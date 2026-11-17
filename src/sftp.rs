use crate::{fs, node::App};
use anyhow::Result;
use russh::{
    Channel, ChannelId,
    server::{Auth, Msg, Server as _, Session},
};
use russh_sftp::{
    protocol::{
        Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version,
    },
    server::Handler as SftpHandler,
};
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::HashMap, future::Future, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};
use tokio::sync::Mutex;

pub async fn serve(app: App, bind: SocketAddr) -> Result<()> {
    let config = russh::server::Config {
        auth_rejection_time: Duration::from_secs(2),
        keys: vec![russh::keys::PrivateKey::random(
            &mut rand::rng(),
            russh::keys::ssh_key::Algorithm::Ed25519,
        )?],
        ..Default::default()
    };
    SshServer { app }
        .run_on_address(Arc::new(config), bind)
        .await?;
    Ok(())
}
#[derive(Clone)]
struct SshServer {
    app: App,
}
impl russh::server::Server for SshServer {
    type Handler = SshSession;
    fn new_client(&mut self, _: Option<SocketAddr>) -> Self::Handler {
        SshSession {
            app: self.app.clone(),
            channels: Arc::new(Mutex::new(HashMap::new())),
            identity: Arc::new(Mutex::new(None)),
        }
    }
}
struct SshSession {
    app: App,
    channels: Arc<Mutex<HashMap<ChannelId, Channel<Msg>>>>,
    identity: Arc<Mutex<Option<Identity>>>,
}
#[derive(Clone)]
struct Identity {
    uuid: String,
    read_only: bool,
}
impl russh::server::Handler for SshSession {
    type Error = anyhow::Error;
    fn auth_password(
        &mut self,
        user: &str,
        password: &str,
    ) -> impl Future<Output = Result<Auth, Self::Error>> + Send {
        let app = self.app.clone();
        let identity = self.identity.clone();
        let parsed = user
            .split_once(':')
            .map(|(a, b)| (a.to_owned(), b.to_owned()));
        let password = password.to_owned();
        async move {
            let Some((uuid, username)) = parsed else {
                return Ok(Auth::reject());
            };
            let cfg = app
                .server_config_public(&uuid)
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let Some(account) = cfg.sftp_users.iter().find(|u| u.username == username) else {
                return Ok(Auth::reject());
            };
            if bcrypt::verify(&password, &account.password_hash).unwrap_or(false) {
                *identity.lock().await = Some(Identity {
                    uuid,
                    read_only: account.read_only,
                });
                Ok(Auth::Accept)
            } else {
                Ok(Auth::reject())
            }
        }
    }
    fn channel_open_session(
        &mut self,
        ch: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _: &mut Session,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.channels.lock().await.insert(ch.id(), ch);
            reply.accept().await;
            Ok(())
        }
    }
    fn subsystem_request(
        &mut self,
        id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let ok = name == "sftp";
        let app = self.app.clone();
        let channels = self.channels.clone();
        let identity = self.identity.clone();
        async move {
            if !ok {
                session.channel_failure(id)?;
                return Ok(());
            }
            let ch = channels
                .lock()
                .await
                .remove(&id)
                .ok_or_else(|| anyhow::anyhow!("channel missing"))?;
            session.channel_success(id)?;
            if let Some(i) = identity.lock().await.clone() {
                russh_sftp::server::run(
                    ch.into_stream(),
                    FsHandler {
                        app,
                        identity: i,
                        handles: HashMap::new(),
                        dirs: HashMap::new(),
                    },
                )
                .await
            }
            Ok(())
        }
    }
}
struct FsHandler {
    app: App,
    identity: Identity,
    handles: HashMap<String, Arc<Mutex<std::fs::File>>>,
    dirs: HashMap<String, (Vec<PathBuf>, usize)>,
}
impl FsHandler {
    fn path(&self, p: &str) -> Result<PathBuf, StatusCode> {
        fs::jailed(&self.app.volumes_root_public(), &self.identity.uuid, p)
            .map_err(|_| StatusCode::PermissionDenied)
    }
    fn writable(&self) -> Result<(), StatusCode> {
        if self.identity.read_only {
            Err(StatusCode::PermissionDenied)
        } else {
            Ok(())
        }
    }
    fn attrs(p: &std::path::Path) -> Result<FileAttributes, StatusCode> {
        let m = std::fs::symlink_metadata(p).map_err(|_| StatusCode::NoSuchFile)?;
        let mut a = FileAttributes {
            size: Some(m.len()),
            uid: Some(1000),
            gid: Some(1000),
            permissions: Some(m.permissions().mode()),
            mtime: Some(
                m.modified()
                    .ok()
                    .and_then(|x| x.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|x| x.as_secs() as u32)
                    .unwrap_or(0),
            ),
            ..Default::default()
        };
        if m.is_dir() {
            a.set_dir(true)
        } else if m.file_type().is_symlink() {
            a.set_symlink(true)
        } else {
            a.set_regular(true)
        }
        Ok(a)
    }
    fn status(id: u32) -> Status {
        Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "ok".into(),
            language_tag: "en".into(),
        }
    }
}
impl SftpHandler for FsHandler {
    type Error = StatusCode;
    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }
    async fn init(&mut self, _: u32, _: HashMap<String, String>) -> Result<Version, Self::Error> {
        Ok(Version::new())
    }
    async fn open(
        &mut self,
        id: u32,
        name: String,
        flags: OpenFlags,
        attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        if flags.intersects(OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE) {
            self.writable()?
        }
        let p = self.path(&name)?;
        if flags.contains(OpenFlags::CREATE) {
            if let Some(x) = p.parent() {
                std::fs::create_dir_all(x).map_err(|_| StatusCode::Failure)?
            }
        }
        let mut o = std::fs::OpenOptions::from(flags);
        if flags.contains(OpenFlags::READ) {
            o.read(true);
        }
        let f = o.open(&p).map_err(|_| StatusCode::NoSuchFile)?;
        if let Some(m) = attrs.permissions {
            let _ = std::fs::set_permissions(&p, std::fs::Permissions::from_mode(m));
        }
        let h = format!("f{}", self.handles.len());
        self.handles.insert(h.clone(), Arc::new(Mutex::new(f)));
        Ok(Handle { id, handle: h })
    }
    async fn close(&mut self, id: u32, h: String) -> Result<Status, Self::Error> {
        self.handles.remove(&h);
        Ok(Self::status(id))
    }
    async fn read(&mut self, id: u32, h: String, off: u64, len: u32) -> Result<Data, Self::Error> {
        let file = self
            .handles
            .get(&h)
            .cloned()
            .ok_or(StatusCode::BadMessage)?;
        let (b, n) = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = file.blocking_lock();
            f.seek(SeekFrom::Start(off))
                .map_err(|_| StatusCode::Failure)?;
            let mut b = vec![0; len as usize];
            let n = f.read(&mut b).map_err(|_| StatusCode::Failure)?;
            Ok::<_, StatusCode>((b, n))
        })
        .await
        .map_err(|_| StatusCode::Failure)??;
        if n == 0 {
            return Err(StatusCode::Eof);
        }
        let mut b = b;
        b.truncate(n);
        Ok(Data { id, data: b })
    }
    async fn write(
        &mut self,
        id: u32,
        h: String,
        off: u64,
        data: Vec<u8>,
    ) -> Result<Status, Self::Error> {
        self.writable()?;
        let file = self
            .handles
            .get(&h)
            .cloned()
            .ok_or(StatusCode::BadMessage)?;
        tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = file.blocking_lock();
            f.seek(SeekFrom::Start(off))
                .map_err(|_| StatusCode::Failure)?;
            f.write_all(&data).map_err(|_| StatusCode::Failure)?;
            f.flush().map_err(|_| StatusCode::Failure)
        })
        .await
        .map_err(|_| StatusCode::Failure)??;
        Ok(Self::status(id))
    }
    async fn stat(&mut self, id: u32, p: String) -> Result<Attrs, Self::Error> {
        Ok(Attrs {
            id,
            attrs: Self::attrs(&self.path(&p)?)?,
        })
    }
    async fn lstat(&mut self, id: u32, p: String) -> Result<Attrs, Self::Error> {
        self.stat(id, p).await
    }
    async fn opendir(&mut self, id: u32, p: String) -> Result<Handle, Self::Error> {
        let v = std::fs::read_dir(self.path(&p)?)
            .map_err(|_| StatusCode::NoSuchFile)?
            .map(|x| x.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| StatusCode::Failure)?;
        let h = format!("d{}", self.dirs.len());
        self.dirs.insert(h.clone(), (v, 0));
        Ok(Handle { id, handle: h })
    }
    async fn readdir(&mut self, id: u32, h: String) -> Result<Name, Self::Error> {
        let (v, i) = self.dirs.get_mut(&h).ok_or(StatusCode::BadMessage)?;
        if *i >= v.len() {
            return Err(StatusCode::Eof);
        }
        let p = &v[*i];
        *i += 1;
        Ok(Name {
            id,
            files: vec![File::new(
                p.file_name().unwrap_or_default().to_string_lossy(),
                Self::attrs(p)?,
            )],
        })
    }
    async fn remove(&mut self, id: u32, p: String) -> Result<Status, Self::Error> {
        self.writable()?;
        std::fs::remove_file(self.path(&p)?).map_err(|_| StatusCode::NoSuchFile)?;
        Ok(Self::status(id))
    }
    async fn mkdir(
        &mut self,
        id: u32,
        p: String,
        _: FileAttributes,
    ) -> Result<Status, Self::Error> {
        self.writable()?;
        std::fs::create_dir_all(self.path(&p)?).map_err(|_| StatusCode::Failure)?;
        Ok(Self::status(id))
    }
    async fn rmdir(&mut self, id: u32, p: String) -> Result<Status, Self::Error> {
        self.writable()?;
        std::fs::remove_dir_all(self.path(&p)?).map_err(|_| StatusCode::Failure)?;
        Ok(Self::status(id))
    }
    async fn realpath(&mut self, id: u32, _: String) -> Result<Name, Self::Error> {
        Ok(Name {
            id,
            files: vec![File::dummy("/")],
        })
    }
    async fn rename(&mut self, id: u32, a: String, b: String) -> Result<Status, Self::Error> {
        self.writable()?;
        std::fs::rename(self.path(&a)?, self.path(&b)?).map_err(|_| StatusCode::Failure)?;
        Ok(Self::status(id))
    }
}
