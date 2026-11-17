use anyhow::{Context, Result, bail};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub fn volume(root: &Path, uuid: &str) -> Result<PathBuf> {
    if uuid.is_empty() || uuid.contains('/') || uuid.contains('\\') || uuid == "." || uuid == ".." {
        bail!("invalid UUID");
    }
    Ok(root.join(uuid))
}
pub fn jailed(root: &Path, uuid: &str, relative: &str) -> Result<PathBuf> {
    let base = volume(root, uuid)?;
    let rel = relative.trim_start_matches('/');
    let path = base.join(rel);
    let normalized = path.components().fold(PathBuf::new(), |mut out, c| {
        match c {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            _ => out.push(c),
        };
        out
    });
    if !normalized.starts_with(&base) {
        bail!("path escapes volume");
    }
    Ok(normalized)
}
pub fn chown_tree(path: &Path) -> Result<()> {
    let status = std::process::Command::new("chown")
        .args(["-R", "1000:1000"])
        .arg(path)
        .status()
        .context("run chown")?;
    if !status.success() {
        bail!("chown failed for {}", path.display());
    }
    Ok(())
}
pub fn entry(path: &Path) -> Result<crate::node::proto::DirEntry> {
    let meta = std::fs::symlink_metadata(path)?;
    Ok(crate::node::proto::DirEntry {
        name: path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into(),
        is_dir: meta.is_dir(),
        is_symlink: meta.file_type().is_symlink(),
        size: meta.size() as i64,
        modified_unix: meta.mtime(),
        mode: meta.permissions().mode(),
    })
}
