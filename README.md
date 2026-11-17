# OpenSrvMgr

OpenSrvMgr is a stateless Rust node daemon. It runs server workloads with Docker, exposes filesystem and console operations over gRPC, and provides jailed SFTP access to server volumes.

The Panel remains the source of truth for server, template, and SFTP configuration. The node does not maintain a database or local server/template configuration files.

## What It Provides

- Docker-backed server lifecycle management
- Template installation and streamed installer output
- Interactive console attachment
- Container resource statistics
- Jailed filesystem operations over gRPC
- Streamed file upload and download
- ZIP, tar, tar.gz, and tar.zst archive operations
- External `.tar.zst` server backups and restore operations
- Crash, OOM, and auto-restart event reporting to the Panel
- Password-authenticated, jailed SFTP
- Soft pre-start disk-limit checks covering server files and external backups
- Bearer-authenticated Node gRPC endpoints

## Documentation

- [Setup Guide](Setup.md)
- [API Specification](API_SPEC.md)
- [Example Panel Proto](proto/panel.proto)
- [Community Use License](LICENSE.md)

