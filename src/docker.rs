use anyhow::{Context, Result, bail};
use bollard::Docker;
use std::process::Command;

pub async fn preflight() -> Result<Docker> {
    let path = which_docker().context("Docker CLI was not found in PATH")?;
    let status = Command::new(&path)
        .arg("info")
        .status()
        .context("run docker info")?;
    if !status.success() {
        bail!(
            "Docker exists at {} but the current user cannot access the daemon; check docker.sock permissions/group membership",
            path.display()
        );
    }
    let docker = Docker::connect_with_local_defaults().context("connect to Docker daemon")?;
    docker
        .version()
        .await
        .context("Docker daemon ping failed")?;
    Ok(docker)
}

fn which_docker() -> Option<std::path::PathBuf> {
    std::env::var_os("PATH").and_then(|p| {
        std::env::split_paths(&p)
            .map(|d| d.join("docker"))
            .find(|p| p.is_file())
    })
}
