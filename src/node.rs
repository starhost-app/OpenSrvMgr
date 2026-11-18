use crate::{config::Args, fs};
use anyhow::Result;
use bollard::{
    Docker,
    container::{
        AttachContainerOptions, Config, CreateContainerOptions, RemoveContainerOptions,
        ResizeContainerTtyOptions, StartContainerOptions, StatsOptions, StopContainerOptions,
    },
};
use bollard::{
    image::CreateImageOptions,
    models::{HostConfig, PortBinding},
};
use futures_util::Stream;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::{pin::Pin, sync::Arc};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("opensrvmgr");
}
use proto::{
    node_server::{Node, NodeServer},
    *,
};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
#[derive(Clone)]
pub struct App {
    args: Args,
    docker: Docker,
    panel_url: String,
    panel_key: String,
    _lock: Arc<Mutex<()>>,
}

pub async fn serve(args: Args, docker: Docker) -> Result<()> {
    let app = App {
        args: args.clone(),
        docker,
        panel_url: args.panel_url.clone(),
        panel_key: args.panel_key.clone(),
        _lock: Arc::new(Mutex::new(())),
    };
    let api_key = args.api_key.clone();
    let grpc = Server::builder()
        .add_service(NodeServer::with_interceptor(
            app.clone(),
            move |request: Request<()>| {
                let provided = request
                    .metadata()
                    .get("authorization")
                    .map(|value| value.as_bytes())
                    .unwrap_or(&[]);
                let expected = format!("Bearer {}", api_key);
                if provided.ct_eq(expected.as_bytes()).into() {
                    Ok(request)
                } else {
                    Err(Status::unauthenticated(
                        "invalid or missing Bearer authorization",
                    ))
                }
            },
        ))
        .serve(args.grpc_bind);
    let sftp = crate::sftp::serve(app.clone(), args.sftp_bind);
    let events = event_loop(app.clone());
    tokio::try_join!(
        async { grpc.await.map_err(anyhow::Error::from) },
        sftp,
        events
    )?;
    Ok(())
}
async fn event_loop(app: App) -> Result<()> {
    use bollard::system::EventsOptions;
    let mut events = app.docker.events(Some(EventsOptions::<String> {
        filters: HashMap::from([(String::from("type"), vec![String::from("container")])]),
        ..Default::default()
    }));
    while let Some(item) = events.next().await {
        let Ok(event) = item else { continue };
        let action = event.action.clone().unwrap_or_default();
        let id = event.actor.as_ref().and_then(|a| a.id.clone());
        let attrs = event
            .actor
            .as_ref()
            .and_then(|a| a.attributes.clone())
            .unwrap_or_default();
        let name = attrs.get("name").cloned().unwrap_or_default();
        if !name.starts_with("srv-") || !(action == "die" || action == "oom") {
            continue;
        }
        let uuid = name.trim_start_matches("srv-").to_string();
        let exit_code = attrs
            .get("exitCode")
            .and_then(|x| x.parse().ok())
            .unwrap_or(0);
        let oom = action == "oom";
        app.report(Event {
            uuid: uuid.clone(),
            event: if oom { "oom".into() } else { "crashed".into() },
            exit_code,
            oom_killed: oom,
            timestamp_unix: event.time.unwrap_or_else(|| chrono::Utc::now().timestamp()),
        })
        .await;
        let _ = app
            .docker
            .remove_container(
                id.as_deref().unwrap_or(&name),
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
        if let Ok(cfg) = app.server_config(&uuid).await {
            if cfg.auto_restart && cfg.exists {
                let _ = start_container(&app, &uuid, &cfg).await;
            }
        }
    }
    Ok(())
}
async fn start_container(app: &App, uuid: &str, cfg: &ServerConfig) -> Result<String> {
    let volume = fs::volume(&app.args.volumes_root, uuid)?;
    tokio::fs::create_dir_all(&volume).await?;
    let limits = cfg.limits.clone().unwrap_or_default();
    check_disk_limit(
        &volume,
        &app.args.backups_root.join(uuid),
        limits.disk_bytes,
    )?;
    let name = format!("srv-{uuid}");
    let _ = app
        .docker
        .remove_container(
            &name,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await;
    pull(&app.docker, &cfg.image).await?;
    let (exposed, bindings) = ports(&cfg.ports);
    let id = app
        .docker
        .create_container(
            Some(CreateContainerOptions {
                name: name.clone(),
                ..Default::default()
            }),
            Config::<String> {
                image: Some(cfg.image.clone()),
                cmd: command_vec(&cfg.cmd),
                env: Some(env_map(&cfg.env, limits.memory_bytes)),
                exposed_ports: Some(exposed),
                host_config: Some(HostConfig {
                    binds: Some(vec![format!("{}:/srv/server:rw", volume.display())]),
                    memory: (limits.memory_bytes > 0).then_some(limits.memory_bytes),
                    memory_swap: (limits.swap_bytes > 0).then_some(limits.swap_bytes),
                    nano_cpus: (limits.nano_cpus > 0).then_some(limits.nano_cpus),
                    pids_limit: (limits.pids_limit > 0).then_some(limits.pids_limit),
                    port_bindings: Some(bindings),
                    ..Default::default()
                }),
                open_stdin: Some(true),
                attach_stdin: Some(true),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                tty: Some(true),
                ..Default::default()
            },
        )
        .await?
        .id;
    app.docker
        .start_container(&id, None::<StartContainerOptions<String>>)
        .await?;
    Ok(id)
}
fn err(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
fn empty() -> Empty {
    Empty {}
}

impl App {
    pub(crate) fn volumes_root_public(&self) -> std::path::PathBuf {
        self.args.volumes_root.clone()
    }
    pub(crate) async fn server_config_public(&self, uuid: &str) -> Result<ServerConfig, Status> {
        self.server_config(uuid).await
    }
    async fn panel_client(
        &self,
    ) -> Result<proto::panel_client::PanelClient<tonic::transport::Channel>, Status> {
        let channel = tonic::transport::Channel::from_shared(self.panel_url.clone())
            .map_err(err)?
            .connect()
            .await
            .map_err(err)?;
        Ok(proto::panel_client::PanelClient::new(channel))
    }
    fn panel_auth(&self) -> tonic::metadata::MetadataValue<tonic::metadata::Ascii> {
        tonic::metadata::MetadataValue::try_from(format!("Bearer {}", self.panel_key))
            .expect("panel_key is valid ASCII")
    }
    async fn server_config(&self, uuid: &str) -> Result<ServerConfig, Status> {
        let mut c = self.panel_client().await?;
        let mut req = tonic::Request::new(Uuid { uuid: uuid.into() });
        req.metadata_mut()
            .insert("authorization", self.panel_auth());
        c.get_server_config(req)
            .await
            .map(|r| r.into_inner())
            .map_err(err)
    }
    async fn template_config(&self, uuid: &str) -> Result<TemplateConfig, Status> {
        let mut c = self.panel_client().await?;
        let mut req = tonic::Request::new(Uuid { uuid: uuid.into() });
        req.metadata_mut()
            .insert("authorization", self.panel_auth());
        c.get_template_config(req)
            .await
            .map(|r| r.into_inner())
            .map_err(err)
    }
    async fn report(&self, event: Event) {
        if let Ok(mut c) = self.panel_client().await {
            let mut req = tonic::Request::new(event);
            req.metadata_mut()
                .insert("authorization", self.panel_auth());
            let _ = c.report_container_event(req).await;
        }
    }
}

fn env_map(values: &[Env], memory: i64) -> Vec<String> {
    let mut out = values
        .iter()
        .map(|v| format!("{}={}", v.key, v.value))
        .collect::<Vec<_>>();
    if memory > 0 {
        out.push(format!("MEMORY={}", memory / 1_048_576));
    }
    out
}
fn command_vec(command: &str) -> Option<Vec<String>> {
    if command.trim().is_empty() {
        None
    } else {
        Some(vec!["/bin/sh".into(), "-c".into(), command.into()])
    }
}
fn volume_size(path: &std::path::Path) -> Result<u64> {
    fn walk(path: &std::path::Path, total: &mut u64) -> std::io::Result<()> {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path)? {
                walk(&entry?.path(), total)?;
            }
        } else {
            *total = total.saturating_add(metadata.len());
        }
        Ok(())
    }
    let mut total = 0;
    if path.exists() {
        walk(path, &mut total)?;
    }
    Ok(total)
}
fn check_disk_limit(volume: &std::path::Path, backups: &std::path::Path, limit: i64) -> Result<()> {
    if limit > 0 {
        let used = volume_size(volume)?.saturating_add(volume_size(backups)?);
        if used >= limit as u64 {
            anyhow::bail!(
                "server files and backups use {used} bytes and disk limit is {limit} bytes"
            );
        }
    }
    Ok(())
}
fn ports(cfg: &[Port]) -> (HashMap<String, HashMap<(), ()>>, bollard::models::PortMap) {
    let mut exposed = HashMap::new();
    let mut bindings = HashMap::new();
    for p in cfg {
        for proto in ["tcp", "udp"] {
            let key = format!("{}/{}", p.port, proto);
            exposed.insert(key.clone(), HashMap::new());
            bindings.insert(
                key,
                Some(vec![PortBinding {
                    host_ip: Some(p.ip.clone()),
                    host_port: Some(p.port.to_string()),
                }]),
            );
        }
    }
    (exposed, bindings)
}
async fn pull(docker: &Docker, image: &str) -> Result<()> {
    let mut stream = docker.create_image(
        Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        }),
        None,
        None,
    );
    while let Some(item) = stream.next().await {
        item?;
    }
    Ok(())
}

#[tonic::async_trait]
impl Node for App {
    type RunTemplateStream = BoxStream<BytesChunk>;
    type AttachConsoleStream = BoxStream<BytesChunk>;
    type StreamStatsStream = BoxStream<StatsFrame>;
    type ReadFileStream = BoxStream<BytesChunk>;

    async fn power_action(
        &self,
        r: Request<PowerActionRequest>,
    ) -> Result<Response<ServerState>, Status> {
        let x = r.into_inner();
        if x.uuid.is_empty() {
            return Err(Status::invalid_argument("uuid required"));
        }
        let volume = fs::volume(&self.args.volumes_root, &x.uuid).map_err(err)?;
        tokio::fs::create_dir_all(&volume).await.map_err(err)?;
        fs::chown_tree(&volume).map_err(err)?;
        let name = format!("srv-{}", x.uuid);
        let cfg = self.server_config(&x.uuid).await?;
        if !cfg.exists {
            return Err(Status::not_found("server config does not exist"));
        }
        match PowerActionType::try_from(x.action).unwrap_or(PowerActionType::PowerActionUnspecified)
        {
            PowerActionType::Start => {
                let _ = self
                    .docker
                    .remove_container(
                        &name,
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await;
                pull(&self.docker, &cfg.image).await.map_err(err)?;
                let (exposed, bindings) = ports(&cfg.ports);
                let limits = cfg.limits.clone().unwrap_or_default();
                let memory = limits.memory_bytes;
                check_disk_limit(
                    &volume,
                    &self.args.backups_root.join(&x.uuid),
                    limits.disk_bytes,
                )
                .map_err(err)?;
                let host_config = HostConfig {
                    binds: Some(vec![format!("{}:/srv/server:rw", volume.display())]),
                    memory: (memory > 0).then_some(memory),
                    memory_swap: (limits.swap_bytes > 0).then_some(limits.swap_bytes),
                    nano_cpus: (limits.nano_cpus > 0).then_some(limits.nano_cpus),
                    pids_limit: (limits.pids_limit > 0).then_some(limits.pids_limit),
                    blkio_weight: (limits.io_weight > 0).then_some(limits.io_weight as u16),
                    port_bindings: Some(bindings),
                    ..Default::default()
                };
                let id = self
                    .docker
                    .create_container(
                        Some(CreateContainerOptions {
                            name: name.clone(),
                            ..Default::default()
                        }),
                        Config::<String> {
                            image: Some(cfg.image.clone()),
                            cmd: command_vec(&cfg.cmd),
                            env: Some(env_map(&cfg.env, memory)),
                            exposed_ports: Some(exposed),
                            host_config: Some(host_config),
                            open_stdin: Some(true),
                            attach_stdin: Some(true),
                            attach_stdout: Some(true),
                            attach_stderr: Some(true),
                            tty: Some(true),
                            labels: Some(HashMap::from([(
                                String::from("opensrvmgr.uuid"),
                                x.uuid.clone(),
                            )])),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(err)?
                    .id;
                self.docker
                    .start_container(&id, None::<StartContainerOptions<String>>)
                    .await
                    .map_err(err)?;
                self.report(Event {
                    uuid: x.uuid.clone(),
                    event: "started".into(),
                    timestamp_unix: chrono::Utc::now().timestamp(),
                    ..Default::default()
                })
                .await;
                Ok(Response::new(ServerState {
                    uuid: x.uuid,
                    container_id: id,
                    status: "running".into(),
                    ..Default::default()
                }))
            }
            PowerActionType::Stop => {
                if !cfg.stop_command.trim().is_empty() {
                    if let Ok(exec) = self
                        .docker
                        .create_exec(
                            &name,
                            bollard::exec::CreateExecOptions::<String> {
                                cmd: Some(vec![
                                    "/bin/sh".into(),
                                    "-c".into(),
                                    cfg.stop_command.clone(),
                                ]),
                                attach_stdout: Some(false),
                                attach_stderr: Some(false),
                                attach_stdin: Some(false),
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        let _ = self
                            .docker
                            .start_exec(
                                &exec.id,
                                Some(bollard::exec::StartExecOptions {
                                    detach: true,
                                    tty: false,
                                    output_capacity: None,
                                }),
                            )
                            .await;
                    }
                }
                let timeout = cfg.timeout_sec.max(1) as i64;
                let _ = self
                    .docker
                    .stop_container(&name, Some(StopContainerOptions { t: timeout }))
                    .await;
                let _ = self
                    .docker
                    .remove_container(
                        &name,
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await;
                self.report(Event {
                    uuid: x.uuid.clone(),
                    event: "stopped".into(),
                    timestamp_unix: chrono::Utc::now().timestamp(),
                    ..Default::default()
                })
                .await;
                Ok(Response::new(ServerState {
                    uuid: x.uuid,
                    status: "stopped".into(),
                    ..Default::default()
                }))
            }
            PowerActionType::Kill => {
                let _ = self
                    .docker
                    .kill_container(
                        &name,
                        None::<bollard::container::KillContainerOptions<String>>,
                    )
                    .await;
                let _ = self
                    .docker
                    .remove_container(
                        &name,
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await;
                self.report(Event {
                    uuid: x.uuid.clone(),
                    event: "stopped".into(),
                    timestamp_unix: chrono::Utc::now().timestamp(),
                    ..Default::default()
                })
                .await;
                Ok(Response::new(ServerState {
                    uuid: x.uuid,
                    status: "killed".into(),
                    ..Default::default()
                }))
            }
            _ => Err(Status::invalid_argument(
                "action must be START, STOP, or KILL",
            )),
        }
    }
    async fn delete_server(&self, r: Request<Uuid>) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        let n = format!("srv-{}", x.uuid);
        let _ = self
            .docker
            .remove_container(
                &n,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
        tokio::fs::remove_dir_all(fs::volume(&self.args.volumes_root, &x.uuid).map_err(err)?)
            .await
            .map_err(err)?;
        Ok(Response::new(empty()))
    }
    async fn run_template(
        &self,
        r: Request<RunTemplateRequest>,
    ) -> Result<Response<Self::RunTemplateStream>, Status> {
        let x = r.into_inner();
        let cfg = self.template_config(&x.template_uuid).await?;
        if !cfg.exists {
            return Err(Status::not_found("template config does not exist"));
        }
        let volume = fs::volume(&self.args.volumes_root, &x.server_uuid).map_err(err)?;
        if x.wipe_volume {
            let _ = tokio::fs::remove_dir_all(&volume).await;
        }
        tokio::fs::create_dir_all(&volume).await.map_err(err)?;
        let name = format!("install-{}", x.server_uuid);
        let _ = self
            .docker
            .remove_container(
                &name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
        pull(&self.docker, &cfg.image).await.map_err(err)?;
        let mut env = env_map(&cfg.env, cfg.memory_bytes);
        env.extend(env_map(&x.extra_env, 0));
        let command = vec![cfg.entrypoint.clone(), cfg.script.clone()];
        let id = self
            .docker
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    ..Default::default()
                }),
                Config::<String> {
                    image: Some(cfg.image),
                    cmd: Some(command),
                    env: Some(env),
                    user: Some("0:0".into()),
                    host_config: Some(HostConfig {
                        binds: Some(vec![format!("{}:/mnt/server:rw", volume.display())]),
                        memory: (cfg.memory_bytes > 0).then_some(cfg.memory_bytes),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .map_err(err)?
            .id;
        self.docker
            .start_container(&id, None::<StartContainerOptions<String>>)
            .await
            .map_err(err)?;
        let mut logs = self.docker.logs(
            &id,
            Some(bollard::container::LogsOptions {
                follow: true,
                stdout: true,
                stderr: true,
                since: 0,
                until: 0,
                timestamps: false,
                tail: "all",
            }),
        );
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let docker = self.docker.clone();
        let volume2 = volume.clone();
        tokio::spawn(async move {
            while let Some(item) = logs.next().await {
                match item {
                    Ok(log) => {
                        if tx
                            .send(Ok(BytesChunk {
                                data: log.into_bytes().to_vec(),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        break;
                    }
                }
            }
            let _ = docker
                .remove_container(
                    &name,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await;
            let _ = fs::chown_tree(&volume2);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn attach_console(
        &self,
        r: Request<tonic::Streaming<AttachConsoleRequest>>,
    ) -> Result<Response<Self::AttachConsoleStream>, Status> {
        let mut input = r.into_inner();
        let first =
            input.message().await.map_err(err)?.ok_or_else(|| {
                Status::invalid_argument("first console message must contain uuid")
            })?;
        if first.uuid.is_empty() {
            return Err(Status::invalid_argument("uuid required"));
        }
        let name = format!("srv-{}", first.uuid);
        let attached = self
            .docker
            .attach_container(
                &name,
                Some(AttachContainerOptions::<String> {
                    stdin: Some(true),
                    stdout: Some(true),
                    stderr: Some(true),
                    stream: Some(true),
                    logs: Some(false),
                    detach_keys: None,
                }),
            )
            .await
            .map_err(err)?;
        if let Some(resize) = first.resize {
            let _ = self
                .docker
                .resize_container_tty(
                    &name,
                    ResizeContainerTtyOptions {
                        width: resize.cols.min(u16::MAX as u32) as u16,
                        height: resize.rows.min(u16::MAX as u32) as u16,
                    },
                )
                .await;
        }
        let mut writer = attached.input;
        let mut output = attached.output;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(msg) = input.message().await.transpose() {
                match msg {
                    Ok(msg) => {
                        if let Some(r) = msg.resize {
                            let _ = r;
                        }
                        if !msg.stdin_data.is_empty()
                            && writer.write_all(&msg.stdin_data).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        tokio::spawn(async move {
            while let Some(item) = output.next().await {
                match item {
                    Ok(log) => {
                        if tx
                            .send(Ok(BytesChunk {
                                data: log.into_bytes().to_vec(),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn stream_stats(
        &self,
        r: Request<Uuid>,
    ) -> Result<Response<Self::StreamStatsStream>, Status> {
        let uuid = r.into_inner().uuid;
        let name = format!("srv-{}", uuid);
        let mut stream = self.docker.stats(
            &name,
            Some(StatsOptions {
                stream: true,
                one_shot: false,
            }),
        );
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(s) => {
                        let cpu_delta = s
                            .cpu_stats
                            .cpu_usage
                            .total_usage
                            .saturating_sub(s.precpu_stats.cpu_usage.total_usage)
                            as f64;
                        let sys_delta = s
                            .cpu_stats
                            .system_cpu_usage
                            .unwrap_or(0)
                            .saturating_sub(s.precpu_stats.system_cpu_usage.unwrap_or(0))
                            as f64;
                        let cores = s.cpu_stats.online_cpus.unwrap_or(1) as f64;
                        let cpu = if sys_delta > 0.0 {
                            cpu_delta / sys_delta * cores * 100.0
                        } else {
                            0.0
                        };
                        let mem = s.memory_stats.usage.unwrap_or(0);
                        let limit = s.memory_stats.limit.unwrap_or(0);
                        let (rx_bytes, tx_bytes) = s
                            .networks
                            .as_ref()
                            .map(|n| {
                                n.values()
                                    .fold((0, 0), |(r, t), v| (r + v.rx_bytes, t + v.tx_bytes))
                            })
                            .unwrap_or((0, 0));
                        if tx
                            .send(Ok(StatsFrame {
                                cpu_percent: cpu,
                                memory_bytes: mem as i64,
                                memory_limit: limit as i64,
                                rx_bytes: rx_bytes as i64,
                                tx_bytes: tx_bytes as i64,
                                status: "running".into(),
                                ..Default::default()
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn list_dir(&self, r: Request<PathRequest>) -> Result<Response<ListDirResponse>, Status> {
        let x = r.into_inner();
        let p = fs::jailed(&self.args.volumes_root, &x.uuid, &x.path).map_err(err)?;
        let mut rd = tokio::fs::read_dir(p).await.map_err(err)?;
        let mut entries = Vec::new();
        while let Some(e) = rd.next_entry().await.map_err(err)? {
            entries.push(fs::entry(&e.path()).map_err(err)?);
        }
        Ok(Response::new(ListDirResponse { entries }))
    }
    async fn stat_path(&self, r: Request<PathRequest>) -> Result<Response<DirEntry>, Status> {
        let x = r.into_inner();
        Ok(Response::new(
            fs::entry(&fs::jailed(&self.args.volumes_root, &x.uuid, &x.path).map_err(err)?)
                .map_err(err)?,
        ))
    }
    async fn create_dir(&self, r: Request<PathRequest>) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        let p = fs::jailed(&self.args.volumes_root, &x.uuid, &x.path).map_err(err)?;
        tokio::fs::create_dir_all(&p).await.map_err(err)?;
        fs::chown_tree(&p).map_err(err)?;
        Ok(Response::new(empty()))
    }
    async fn move_paths(&self, r: Request<MovePathsRequest>) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        for m in x.moves {
            let a = fs::jailed(&self.args.volumes_root, &x.uuid, &m.from_path).map_err(err)?;
            let b = fs::jailed(&self.args.volumes_root, &x.uuid, &m.to_path).map_err(err)?;
            if x.overwrite && b.exists() {
                tokio::fs::remove_dir_all(&b).await.ok();
                tokio::fs::remove_file(&b).await.ok();
            }
            tokio::fs::rename(a, b).await.map_err(err)?;
        }
        Ok(Response::new(empty()))
    }
    async fn copy_paths(&self, r: Request<CopyPathsRequest>) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        for c in x.copies {
            let a = fs::jailed(&self.args.volumes_root, &x.uuid, &c.from_path).map_err(err)?;
            let b = fs::jailed(&self.args.volumes_root, &x.uuid, &c.to_path).map_err(err)?;
            if a.is_dir() {
                copy_dir(&a, &b, x.overwrite).await.map_err(err)?;
            } else {
                if x.overwrite == false && b.exists() {
                    return Err(Status::already_exists("destination exists"));
                }
                tokio::fs::copy(a, b).await.map_err(err)?;
            }
        }
        Ok(Response::new(empty()))
    }
    async fn delete_paths(
        &self,
        r: Request<DeletePathsRequest>,
    ) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        for p in x.paths {
            let p = fs::jailed(&self.args.volumes_root, &x.uuid, &p).map_err(err)?;
            if p.is_dir() {
                tokio::fs::remove_dir_all(p).await.map_err(err)?
            } else {
                tokio::fs::remove_file(p).await.map_err(err)?
            }
        }
        Ok(Response::new(empty()))
    }
    async fn read_file(
        &self,
        r: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let x = r.into_inner();
        let p = fs::jailed(&self.args.volumes_root, &x.uuid, &x.path).map_err(err)?;
        let mut f = tokio::fs::File::open(p).await.map_err(err)?;
        f.seek(std::io::SeekFrom::Start(x.offset.max(0) as u64))
            .await
            .map_err(err)?;
        let limit = if x.limit <= 0 {
            u64::MAX
        } else {
            x.limit as u64
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut left = limit;
            let mut buf = vec![0; 65536];
            while left > 0 {
                let read_len = left.min(buf.len() as u64) as usize;
                let n = f.read(&mut buf[..read_len]).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                left -= n as u64;
                if tx
                    .send(Ok(BytesChunk {
                        data: buf[..n].to_vec(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn write_file(
        &self,
        r: Request<tonic::Streaming<WriteFileRequest>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let mut s = r.into_inner();
        let first = s
            .message()
            .await
            .map_err(err)?
            .ok_or_else(|| Status::invalid_argument("first write message required"))?;
        let p = fs::jailed(&self.args.volumes_root, &first.uuid, &first.path).map_err(err)?;
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(err)?
        }
        let mode = first.mode;
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(first.truncate)
            .open(p.clone())
            .await
            .map_err(err)?;
        let mut total = 0;
        f.write_all(&first.data).await.map_err(err)?;
        total += first.data.len() as i64;
        while let Some(x) = s.message().await.map_err(err)? {
            f.write_all(&x.data).await.map_err(err)?;
            total += x.data.len() as i64
        }
        drop(f);
        if mode != 0 {
            tokio::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode))
                .await
                .map_err(err)?;
        }
        fs::chown_tree(&p).map_err(err)?;
        Ok(Response::new(WriteFileResponse {
            bytes_written: total,
        }))
    }
    async fn compress_archive(
        &self,
        r: Request<ArchiveRequest>,
    ) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        let root = fs::volume(&self.args.volumes_root, &x.uuid).map_err(err)?;
        let dest = fs::jailed(&self.args.volumes_root, &x.uuid, &x.dest_archive).map_err(err)?;
        let paths = x
            .paths
            .into_iter()
            .map(|p| fs::jailed(&self.args.volumes_root, &x.uuid, &p))
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        let dest2 = dest.clone();
        tokio::task::spawn_blocking(move || archive_create(&root, &paths, &dest2))
            .await
            .map_err(err)?
            .map_err(err)?;
        fs::chown_tree(&dest).map_err(err)?;
        Ok(Response::new(empty()))
    }
    async fn extract_archive(&self, r: Request<ExtractRequest>) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        let archive = fs::jailed(&self.args.volumes_root, &x.uuid, &x.archive_path).map_err(err)?;
        let dest = fs::jailed(&self.args.volumes_root, &x.uuid, &x.dest_dir).map_err(err)?;
        let a = archive.clone();
        let d = dest.clone();
        tokio::task::spawn_blocking(move || archive_extract(&a, &d))
            .await
            .map_err(err)?
            .map_err(err)?;
        fs::chown_tree(&dest).map_err(err)?;
        Ok(Response::new(empty()))
    }
    async fn create_backup(
        &self,
        r: Request<BackupRequest>,
    ) -> Result<Response<BackupInfo>, Status> {
        let x = r.into_inner();
        let root = fs::volume(&self.args.volumes_root, &x.uuid).map_err(err)?;
        tokio::fs::create_dir_all(&root).await.map_err(err)?;
        let name = backup_name(&x.backup_name).map_err(err)?;
        let dir = self.args.backups_root.join(&x.uuid);
        tokio::fs::create_dir_all(&dir).await.map_err(err)?;
        let dest = dir.join(&name);
        if dest.exists() && !x.overwrite {
            return Err(Status::already_exists("backup already exists"));
        }
        let root2 = root.clone();
        let dest2 = dest.clone();
        tokio::task::spawn_blocking(move || backup_create(&root2, &dest2))
            .await
            .map_err(err)?
            .map_err(err)?;
        fs::chown_tree(&dest).map_err(err)?;
        backup_info(&name, &dest).map(Response::new).map_err(err)
    }
    async fn list_backups(
        &self,
        r: Request<Uuid>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let uuid = r.into_inner().uuid;
        let dir = self.args.backups_root.join(&uuid);
        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(x) => x,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Response::new(ListBackupsResponse { backups: vec![] }));
            }
            Err(e) => return Err(err(e)),
        };
        let mut backups = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(err)? {
            let p = entry.path();
            if p.extension().and_then(|x| x.to_str()) == Some("zst")
                && p.file_name().and_then(|x| x.to_str()).is_some()
            {
                backups.push(
                    backup_info(p.file_name().unwrap().to_string_lossy().as_ref(), &p)
                        .map_err(err)?,
                );
            }
        }
        backups.sort_by(|a, b| b.created_unix.cmp(&a.created_unix));
        Ok(Response::new(ListBackupsResponse { backups }))
    }
    async fn restore_backup(&self, r: Request<BackupRequest>) -> Result<Response<Empty>, Status> {
        let x = r.into_inner();
        let root = fs::volume(&self.args.volumes_root, &x.uuid).map_err(err)?;
        let name = backup_name(&x.backup_name).map_err(err)?;
        let archive = self.args.backups_root.join(&x.uuid).join(&name);
        if !archive.is_file() {
            return Err(Status::not_found("backup does not exist"));
        }
        let root2 = root.clone();
        let archive2 = archive.clone();
        tokio::task::spawn_blocking(move || backup_restore(&root2, &archive2))
            .await
            .map_err(err)?
            .map_err(err)?;
        fs::chown_tree(&root).map_err(err)?;
        Ok(Response::new(empty()))
    }
    async fn get_node_stats(&self, _: Request<Empty>) -> Result<Response<NodeStats>, Status> {
        let mem = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let total = mem
            .lines()
            .find_map(|l| {
                l.strip_prefix("MemTotal:")
                    .and_then(|x| x.split_whitespace().next())
                    .and_then(|x| x.parse::<i64>().ok())
            })
            .unwrap_or(0)
            * 1024;
        let avail = mem
            .lines()
            .find_map(|l| {
                l.strip_prefix("MemAvailable:")
                    .and_then(|x| x.split_whitespace().next())
                    .and_then(|x| x.parse::<i64>().ok())
            })
            .unwrap_or(0)
            * 1024;
        let disk = std::process::Command::new("df")
            .args(["-B1", "/srv"])
            .output()
            .ok();
        let (total_disk, used_disk) = disk
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| {
                s.lines().nth(1).map(|l| {
                    let v: Vec<_> = l.split_whitespace().collect();
                    (
                        v.get(1).and_then(|x| x.parse().ok()).unwrap_or(0),
                        v.get(2).and_then(|x| x.parse().ok()).unwrap_or(0),
                    )
                })
            })
            .unwrap_or((0, 0));
        Ok(Response::new(NodeStats {
            total_ram_bytes: total,
            used_ram_bytes: total.saturating_sub(avail),
            total_disk_bytes: total_disk,
            used_disk_bytes: used_disk,
            cpu_cores: num_cpus::get() as u32,
            daemon_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        }))
    }
}
fn backup_name(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty()
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
    {
        anyhow::bail!("invalid backup name");
    }
    let name = if trimmed.ends_with(".tar.zst") {
        trimmed.to_owned()
    } else {
        format!("{trimmed}.tar.zst")
    };
    Ok(name)
}
fn backup_info(name: &str, path: &std::path::Path) -> Result<BackupInfo> {
    let metadata = std::fs::metadata(path)?;
    let created_unix = metadata
        .created()
        .or_else(|_| metadata.modified())?
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    Ok(BackupInfo {
        backup_name: name.into(),
        size_bytes: metadata.len() as i64,
        created_unix,
    })
}
fn backup_create(root: &std::path::Path, dest: &std::path::Path) -> Result<()> {
    let staging = tempfile::tempdir()?;
    let snapshot = staging.path().join("snapshot");
    std::fs::create_dir_all(&snapshot)?;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.path() == dest {
            continue;
        }
        copy_tree_sync(&entry.path(), &snapshot.join(entry.file_name()))?;
    }
    let file = std::fs::File::create(dest)?;
    let enc = zstd::Encoder::new(file, 3)?;
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all("", &snapshot)?;
    let enc = tar.into_inner()?;
    enc.finish()?;
    Ok(())
}
fn backup_restore(root: &std::path::Path, archive: &std::path::Path) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        remove_tree_sync(&entry.path())?;
    }
    let file = std::fs::File::open(archive)?;
    let dec = zstd::Decoder::new(file)?;
    let mut tar = tar::Archive::new(dec);
    extract_tar(&mut tar, root)?;
    Ok(())
}
fn copy_tree_sync(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    let m = std::fs::symlink_metadata(src)?;
    if m.is_dir() {
        std::fs::create_dir_all(dst)?;
        for e in std::fs::read_dir(src)? {
            let e = e?;
            copy_tree_sync(&e.path(), &dst.join(e.file_name()))?
        }
    } else {
        std::fs::copy(src, dst)?;
    }
    Ok(())
}
fn remove_tree_sync(path: &std::path::Path) -> Result<()> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)?
    } else {
        std::fs::remove_file(path)?
    }
    Ok(())
}
fn archive_create(
    root: &std::path::Path,
    paths: &[std::path::PathBuf],
    dest: &std::path::Path,
) -> Result<()> {
    if dest.extension().and_then(|x| x.to_str()) == Some("zip") {
        let file = std::fs::File::create(dest)?;
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::SimpleFileOptions::default();
        for path in paths {
            add_zip(&mut zip, root, path, opts)?;
        }
        zip.finish()?;
    } else {
        let file = std::fs::File::create(dest)?;
        if dest.to_string_lossy().ends_with(".tar.zst") {
            let enc = zstd::Encoder::new(file, 3)?;
            let mut tar = tar::Builder::new(enc);
            for path in paths {
                tar.append_path_with_name(path, path.strip_prefix(root).unwrap_or(path))?;
            }
            let enc = tar.into_inner()?;
            enc.finish()?;
        } else if dest.to_string_lossy().ends_with(".tar.gz") {
            let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut tar = tar::Builder::new(enc);
            for path in paths {
                tar.append_path_with_name(path, path.strip_prefix(root).unwrap_or(path))?;
            }
            tar.into_inner()?.finish()?;
        } else {
            let mut tar = tar::Builder::new(file);
            for path in paths {
                tar.append_path_with_name(path, path.strip_prefix(root).unwrap_or(path))?;
            }
            tar.finish()?;
        }
    }
    Ok(())
}
fn add_zip(
    zip: &mut zip::ZipWriter<std::fs::File>,
    root: &std::path::Path,
    path: &std::path::Path,
    opts: zip::write::SimpleFileOptions,
) -> Result<()> {
    let name = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    if path.is_dir() {
        zip.add_directory(format!("{name}/"), opts)?;
        for e in std::fs::read_dir(path)? {
            add_zip(zip, root, &e?.path(), opts)?;
        }
    } else {
        zip.start_file(name, opts)?;
        let mut f = std::fs::File::open(path)?;
        std::io::copy(&mut f, zip)?;
    }
    Ok(())
}
fn archive_extract(archive: &std::path::Path, dest: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dest)?;
    let name = archive.to_string_lossy();
    if name.ends_with(".zip") {
        let mut z = zip::ZipArchive::new(std::fs::File::open(archive)?)?;
        for i in 0..z.len() {
            let mut f = z.by_index(i)?;
            let out = safe_archive_path(dest, f.name())?;
            if f.is_dir() {
                std::fs::create_dir_all(&out)?
            } else {
                if let Some(p) = out.parent() {
                    std::fs::create_dir_all(p)?
                }
                let mut o = std::fs::File::create(out)?;
                std::io::copy(&mut f, &mut o)?;
            }
        }
    } else {
        let file = std::fs::File::open(archive)?;
        if name.ends_with(".zst") {
            let dec = zstd::Decoder::new(file)?;
            let mut ar = tar::Archive::new(dec);
            extract_tar(&mut ar, dest)?;
        } else if name.ends_with(".gz") {
            let dec = flate2::read::GzDecoder::new(file);
            let mut ar = tar::Archive::new(dec);
            extract_tar(&mut ar, dest)?;
        } else {
            let mut ar = tar::Archive::new(file);
            extract_tar(&mut ar, dest)?;
        }
    }
    Ok(())
}
fn extract_tar<R: std::io::Read>(ar: &mut tar::Archive<R>, dest: &std::path::Path) -> Result<()> {
    for item in ar.entries()? {
        let mut item = item?;
        let p = item.path()?.to_path_buf();
        let out = safe_archive_path(dest, &p)?;
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?
        }
        item.unpack(out)?;
    }
    Ok(())
}
fn safe_archive_path(
    dest: &std::path::Path,
    name: impl AsRef<std::path::Path>,
) -> Result<std::path::PathBuf> {
    let p = dest.join(name.as_ref());
    let c = p.components().fold(std::path::PathBuf::new(), |mut o, c| {
        match c {
            std::path::Component::ParentDir => {
                o.pop();
            }
            std::path::Component::CurDir => {}
            _ => o.push(c),
        }
        o
    });
    if !c.starts_with(dest) {
        anyhow::bail!("archive path escapes destination")
    }
    Ok(c)
}
#[async_recursion::async_recursion]
async fn copy_dir(a: &std::path::Path, b: &std::path::Path, overwrite: bool) -> Result<()> {
    tokio::fs::create_dir_all(b).await?;
    let mut rd = tokio::fs::read_dir(a).await?;
    while let Some(e) = rd.next_entry().await? {
        let d = b.join(e.file_name());
        if e.path().is_dir() {
            copy_dir(&e.path(), &d, overwrite).await?
        } else if overwrite || !d.exists() {
            tokio::fs::copy(e.path(), d).await?;
        }
    }
    Ok(())
}
