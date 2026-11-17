mod config;
mod docker;
mod fs;
mod node;
mod sftp;

use anyhow::Result;
use clap::Parser;
use config::Args;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    config::validate(&args)?;
    let docker = docker::preflight().await?;
    node::serve(args, docker).await
}
