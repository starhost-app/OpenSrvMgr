# OpenSrvMgr Setup Guide

## Requirements

OpenSrvMgr requires:

- Linux
- Rust toolchain
- Docker Engine and Docker CLI
- A separate Panel implementing the example callback service in `proto/panel.proto`
- Permission to access the Docker daemon
- Permission to bind the selected gRPC and SFTP addresses

The daemon performs a Docker preflight before opening either listener. Docker must be installed, running, and accessible by the account that starts OpenSrvMgr.

## Build

Clone the repository and enter it:

```bash
git clone https://github.com/starhost-app/OpenSrvMgr.git
cd OpenSrvMgr
```

Build the release binary:

```bash
cargo build --release --bin OpenSrvMgr
```

The binary is produced at:

```text
target/release/OpenSrvMgr
```

Install it system-wide if desired:

```bash
sudo install -o root -g root -m 0755 target/release/OpenSrvMgr /usr/local/bin/OpenSrvMgr
```

## Persistent Directories

The default persistent layout is:

```text
/var/lib/OpenSrvMgr/
├── volumes/
├── backups/
└── runtime/
```

Create and protect the directories before running as a service:

```bash
sudo install -d -o root -g root -m 0750 \
  /var/lib/OpenSrvMgr \
  /var/lib/OpenSrvMgr/volumes \
  /var/lib/OpenSrvMgr/backups \
  /var/lib/OpenSrvMgr/runtime
```

The paths can be changed with `--volumes-root`, `--backups-root`, and `--runtime-dir`.

## Panel Integration

The separate Panel must implement the `opensrvmgr.Panel` service shown in the example-only `proto/panel.proto` file. The file is not compiled or used by OpenSrvMgr.

- `GetServerConfig`
- `GetTemplateConfig`
- `ReportContainerEvent`

The node calls the Panel when it needs current server or template configuration. The node does not keep a local database or configuration cache.

The Panel should return:

- Docker image and command
- Environment variables
- IP/port mappings
- Resource limits
- Stop command and timeout
- Auto-restart policy
- SFTP users and bcrypt password hashes
- Template entrypoint, script, environment, and memory limit

The Panel endpoint is supplied with `--panel-url`.

## Start OpenSrvMgr

All of these arguments are required:

```bash
OpenSrvMgr \
  --sftp-bind 0.0.0.0:2022 \
  --grpc-bind 0.0.0.0:50051 \
  --panel-url http://panel.example.internal:50052 \
  --api-key 'generate-a-long-random-secret'
```

There are no default values for the network binds. The API key is passed only through `--api-key` and must be kept private.

Every Node gRPC request must include:

```text
authorization: Bearer generate-a-long-random-secret
```

The gRPC API should be restricted to the Panel or private management network. Use TLS or mTLS at the network boundary when traffic crosses an untrusted network.

## SFTP Login

SFTP usernames identify both the server and the Panel account:

```text
<server_uuid>:<panel_username>
```

Example:

```bash
sftp -P 2022 'server-uuid:username'@node.example.com
```

The node fetches the server configuration from the Panel, verifies the password against the returned bcrypt hash, and jails the session to that server volume.

SFTP users marked `read_only` cannot create, modify, rename, or delete files.

## Service Manager

A minimal systemd unit can be created at `/etc/systemd/system/opensrvmgr.service`:

```ini
[Unit]
Description=OpenSrvMgr node
After=docker.service
Requires=docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/OpenSrvMgr \
  --sftp-bind 0.0.0.0:2022 \
  --grpc-bind 0.0.0.0:50051 \
  --panel-url http://panel.example.internal:50052 \
  --api-key CHANGE_ME
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Protect the unit file because it contains the API key, then enable it:

```bash
sudo chmod 0600 /etc/systemd/system/opensrvmgr.service
sudo systemctl daemon-reload
sudo systemctl enable --now opensrvmgr.service
sudo systemctl status opensrvmgr.service
```

For production, use a protected service-manager credential mechanism instead of leaving a long-lived secret directly in a world-readable command configuration.

## Firewall

Allow the gRPC and SFTP ports only from the networks that need them. Example ports:

```text
TCP 50051  gRPC management API
TCP 2022   SFTP
```

Do not expose the management gRPC endpoint publicly without transport security and network restrictions.

## Backups

Backups are created through the `CreateBackup` RPC and stored outside server volumes:

```text
/var/lib/OpenSrvMgr/backups/<server_uuid>/<backup_name>.tar.zst
```

Use the API to:

- create a complete server backup
- list available backups
- restore a backup

Restore removes the server volume contents and replaces them with the selected backup. Backup files count toward the server's soft disk-limit check.

## Soft Disk Limit

`limits.disk_bytes` is a pre-start check, not a kernel quota. Before starting or auto-restarting a server, OpenSrvMgr totals:

- server volume file sizes
- external backup file sizes

If the total is already at or above `disk_bytes`, the server is not started. A running server can still write beyond the limit because its volume is a normal Docker bind mount.

