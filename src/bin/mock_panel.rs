use bcrypt::{DEFAULT_COST, hash};
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
mod proto {
    tonic::include_proto!("opensrvmgr");
}
use proto::{
    panel_server::{Panel, PanelServer},
    *,
};

#[derive(Default)]
struct Mock;
#[tonic::async_trait]
impl Panel for Mock {
    async fn get_server_config(&self, r: Request<Uuid>) -> Result<Response<ServerConfig>, Status> {
        let _uuid = r.into_inner().uuid;
        Ok(Response::new(ServerConfig {
            exists: true,
            image: "alpine:latest".into(),
            cmd: "while read line; do echo server:$line; done".into(),
            env: vec![Env {
                key: "TEST_MODE".into(),
                value: "integration".into(),
            }],
            limits: Some(Limits {
                memory_bytes: 134217728,
                ..Default::default()
            }),
            stop_command: "echo stopping".into(),
            timeout_sec: 3,
            auto_restart: false,
            sftp_users: vec![SftpUser {
                username: "tester".into(),
                password_hash: hash("password", DEFAULT_COST).unwrap(),
                read_only: false,
            }],
            ports: vec![],
        }))
    }
    async fn get_template_config(
        &self,
        _: Request<Uuid>,
    ) -> Result<Response<TemplateConfig>, Status> {
        Ok(Response::new(TemplateConfig {
            exists: true,
            image: "alpine:latest".into(),
            entrypoint: "sh".into(),
            script: "echo template-start; echo template-done".into(),
            env: vec![],
            memory_bytes: 67108864,
        }))
    }
    async fn report_container_event(&self, r: Request<Event>) -> Result<Response<Empty>, Status> {
        eprintln!("PANEL_EVENT {:?}", r.into_inner());
        Ok(Response::new(Empty {}))
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = "127.0.0.1:35051".parse()?;
    Server::builder()
        .add_service(PanelServer::new(Mock::default()))
        .serve(addr)
        .await?;
    Ok(())
}
