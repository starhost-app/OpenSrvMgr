use anyhow::{Context, Result, bail};
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "OpenSrvMgr", version)]
pub struct Args {
    #[arg(long, value_parser = parse_addr)]
    pub sftp_bind: SocketAddr,
    #[arg(long, value_parser = parse_addr)]
    pub grpc_bind: SocketAddr,
    #[arg(long, value_name = "URL")]
    pub panel_url: String,
    #[arg(long, value_name = "KEY")]
    pub api_key: String,
    #[arg(long, value_name = "KEY")]
    pub panel_key: String,
    #[arg(long, default_value = "/var/lib/OpenSrvMgr/volumes")]
    pub volumes_root: PathBuf,
    #[arg(long, default_value = "/var/lib/OpenSrvMgr/runtime")]
    pub runtime_dir: PathBuf,
    #[arg(long, default_value = "/var/lib/OpenSrvMgr/backups")]
    pub backups_root: PathBuf,
}

fn parse_addr(value: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|_| format!("invalid IP:port: {value}"))
}

pub fn validate(args: &Args) -> Result<()> {
    if args.sftp_bind == args.grpc_bind {
        bail!("--sftp-bind and --grpc-bind must be different");
    }
    if args.panel_url.is_empty() {
        bail!("--panel-url cannot be empty");
    }
    if args.api_key.trim().is_empty() {
        bail!("--api-key cannot be empty");
    }
    if args.panel_key.trim().is_empty() {
        bail!("--panel-key cannot be empty");
    }
    std::fs::create_dir_all(&args.volumes_root)
        .with_context(|| format!("create {}", args.volumes_root.display()))?;
    std::fs::create_dir_all(&args.runtime_dir)
        .with_context(|| format!("create {}", args.runtime_dir.display()))?;
    std::fs::create_dir_all(&args.backups_root)
        .with_context(|| format!("create {}", args.backups_root.display()))?;
    Ok(())
}
