# OpenSrvMgr API Specification

Protocol definition: [`proto/node.proto`](proto/node.proto)

Transport: gRPC over HTTP/2.

Package: `opensrvmgr`

## Authentication

Every `opensrvmgr.Node` request requires this metadata header:

```text
authorization: Bearer <api-key>
```

Requests without a valid bearer token return:

```text
UNAUTHENTICATED
```

The `opensrvmgr.Panel` service is called by OpenSrvMgr for configuration and event callbacks.

## Common Types

### `Empty`

No fields.

### `Uuid`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server or template UUID. |

### `Env`

| Field | Type | Description |
| --- | --- | --- |
| `key` | `string` | Environment variable name. |
| `value` | `string` | Environment variable value. |

### `Port`

| Field | Type | Description |
| --- | --- | --- |
| `ip` | `string` | Host bind IP. |
| `port` | `uint32` | Host and container port. |

Each configured port is exposed as both TCP and UDP.

### `Limits`

| Field | Type | Description |
| --- | --- | --- |
| `memory_bytes` | `int64` | Memory limit in bytes. |
| `swap_bytes` | `int64` | Swap limit in bytes. |
| `nano_cpus` | `int64` | CPU limit in Docker nano-CPUs. |
| `pids_limit` | `int64` | Maximum process count. |
| `disk_bytes` | `int64` | Soft disk limit in bytes. The node refuses to start or auto-restart a server whose existing volume usage is at or above this value. It does not enforce writes after the container starts. `0` disables the check. |
| `io_weight` | `int32` | Relative block IO weight. |

### `SftpUser`

| Field | Type | Description |
| --- | --- | --- |
| `username` | `string` | SFTP username component. |
| `password_hash` | `string` | Bcrypt password hash. |
| `read_only` | `bool` | Prevents SFTP mutations when true. |

### `DirEntry`

| Field | Type | Description |
| --- | --- | --- |
| `name` | `string` | File or directory name. |
| `is_dir` | `bool` | Whether the entry is a directory. |
| `is_symlink` | `bool` | Whether the entry is a symbolic link. |
| `size` | `int64` | Size in bytes. |
| `modified_unix` | `int64` | Modification time as Unix seconds. |
| `mode` | `uint32` | Unix permission and type bits. |

## Node Service

Service name: `opensrvmgr.Node`

## `PowerAction`

```protobuf
rpc PowerAction(PowerActionRequest) returns (ServerState);
```

Unary RPC.

### Request: `PowerActionRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `action` | `PowerActionType` | `START`, `STOP`, or `KILL`. |

### Response: `ServerState`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `container_id` | `string` | Docker container ID when available. |
| `status` | `string` | Current action/container status. |
| `exit_code` | `int32` | Exit code when available. |
| `oom_killed` | `bool` | Whether the container was killed by OOM. |

### Behavior

`START` fetches current server configuration from the Panel, checks the existing volume usage against `limits.disk_bytes`, prepares the server container, and starts it. If usage is at or above a positive `disk_bytes` limit, startup is rejected. The limit is a pre-start check only; writes made after startup are not blocked by the node.

`STOP` executes the configured stop command when present, waits for the configured timeout, removes the container, and preserves server files.

`KILL` immediately terminates and removes the container.

Invalid actions return `INVALID_ARGUMENT`. Missing Panel configuration returns `NOT_FOUND` or the callback error.

## `DeleteServer`

```protobuf
rpc DeleteServer(Uuid) returns (Empty);
```

Unary RPC.

Force-removes the server container and permanently deletes the server's files. The Panel is responsible for deleting its database record.

## `RunTemplate`

```protobuf
rpc RunTemplate(RunTemplateRequest) returns (stream BytesChunk);
```

Server-streaming RPC.

### Request: `RunTemplateRequest`

| Field | Type | Description |
| --- | --- | --- |
| `server_uuid` | `string` | Destination server UUID. |
| `template_uuid` | `string` | Template UUID. |
| `wipe_volume` | `bool` | Deletes existing server files before execution. |
| `extra_env` | `Env[]` | Additional environment variables. |

### Stream payload: `BytesChunk`

| Field | Type | Description |
| --- | --- | --- |
| `data` | `bytes` | Raw installer stdout/stderr bytes. |

### Behavior

The node fetches template configuration from the Panel, runs the configured entrypoint and script as root in an ephemeral installer container, streams output, removes the installer container, and closes the stream on successful completion.

The command passed to the container is exactly:

```text
[entrypoint, script]
```

A non-zero installer exit or Docker failure terminates the RPC with an error status.

## `AttachConsole`

```protobuf
rpc AttachConsole(stream AttachConsoleRequest) returns (stream BytesChunk);
```

Bidirectional streaming RPC.

### Client messages: `AttachConsoleRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Required on the first message. |
| `stdin_data` | `bytes` | Raw bytes sent to container stdin. |
| `resize` | `Resize` | TTY resize request. |

The first message identifies the server. Subsequent messages may contain stdin bytes or resize data.

### `Resize`

| Field | Type | Description |
| --- | --- | --- |
| `cols` | `uint32` | Terminal width. |
| `rows` | `uint32` | Terminal height. |

### Server stream

Returns raw Docker stdout/stderr bytes as `BytesChunk` messages.

## `StreamStats`

```protobuf
rpc StreamStats(Uuid) returns (stream StatsFrame);
```

Server-streaming RPC.

### `StatsFrame`

| Field | Type | Description |
| --- | --- | --- |
| `cpu_percent` | `double` | Container CPU usage percentage. |
| `memory_bytes` | `int64` | Current memory usage. |
| `memory_limit` | `int64` | Configured memory limit. |
| `disk_bytes` | `int64` | Disk usage when available. |
| `rx_bytes` | `int64` | Network bytes received. |
| `tx_bytes` | `int64` | Network bytes transmitted. |
| `status` | `string` | Container status. |
| `uptime_sec` | `int64` | Container uptime in seconds when available. |

Frames are sent continuously while Docker statistics are available. The stream ends when the client disconnects or the container disappears.

## `ListDir`

```protobuf
rpc ListDir(PathRequest) returns (ListDirResponse);
```

Unary RPC.

Lists direct children of the requested directory.

### Request: `PathRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `path` | `string` | Path relative to the server volume. |

### Response: `ListDirResponse`

| Field | Type | Description |
| --- | --- | --- |
| `entries` | `DirEntry[]` | Direct directory entries. |

## `StatPath`

```protobuf
rpc StatPath(PathRequest) returns (DirEntry);
```

Unary RPC.

Returns metadata for one path.

## `CreateDir`

```protobuf
rpc CreateDir(PathRequest) returns (Empty);
```

Unary RPC. This is the mkdir endpoint.

Recursively creates the requested directory path.

## `MovePaths`

```protobuf
rpc MovePaths(MovePathsRequest) returns (Empty);
```

Unary RPC.

### Request: `MovePathsRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `moves` | `Move[]` | Source/destination pairs. |
| `overwrite` | `bool` | Allows replacing existing destinations. |

### `Move`

| Field | Type | Description |
| --- | --- | --- |
| `from_path` | `string` | Source path. |
| `to_path` | `string` | Destination path. |

Uses atomic rename when possible.

## `CopyPaths`

```protobuf
rpc CopyPaths(CopyPathsRequest) returns (Empty);
```

Unary RPC.

### Request: `CopyPathsRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `copies` | `Copy[]` | Source/destination pairs. |
| `overwrite` | `bool` | Allows replacing existing destinations. |

Copies files and directories recursively.

## `DeletePaths`

```protobuf
rpc DeletePaths(DeletePathsRequest) returns (Empty);
```

Unary RPC.

### Request: `DeletePathsRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `paths` | `string[]` | Files or directories to remove. |

## `ReadFile`

```protobuf
rpc ReadFile(ReadFileRequest) returns (stream BytesChunk);
```

Server-streaming RPC.

### Request: `ReadFileRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `path` | `string` | File path. |
| `offset` | `int64` | Starting byte offset. Negative values are treated as zero. |
| `limit` | `int64` | Maximum bytes. Zero or negative means read to EOF. |

The response contains raw binary chunks, normally approximately 64 KiB each.

## `WriteFile`

```protobuf
rpc WriteFile(stream WriteFileRequest) returns (WriteFileResponse);
```

Client-streaming RPC.

### First client message

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `path` | `string` | Destination file path. |
| `truncate` | `bool` | Truncates the destination before writing. |
| `mode` | `uint32` | Unix mode bits. |
| `data` | `bytes` | Initial file bytes. |

### Subsequent client messages

Only `data` is used. Each message appends the next binary chunk.

### Response: `WriteFileResponse`

| Field | Type | Description |
| --- | --- | --- |
| `bytes_written` | `int64` | Total bytes written. |

An empty client stream returns `INVALID_ARGUMENT`.

## `CompressArchive`

```protobuf
rpc CompressArchive(ArchiveRequest) returns (Empty);
```

Unary RPC.

### Request: `ArchiveRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `paths` | `string[]` | Files/directories to include. |
| `dest_archive` | `string` | Destination archive path. |

Supported archive extensions:

- `.zip`
- `.tar`
- `.tar.gz`
- `.tar.zst`

## `ExtractArchive`

```protobuf
rpc ExtractArchive(ExtractRequest) returns (Empty);
```

Unary RPC.

### Request: `ExtractRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `archive_path` | `string` | Archive to extract. |
| `dest_dir` | `string` | Extraction destination. |

ZIP-slip and archive member path escapes return an error and are not extracted.

## `CreateBackup`

```protobuf
rpc CreateBackup(BackupRequest) returns (BackupInfo);
```

Unary RPC.

Creates a complete server backup as a `.tar.zst` archive. The backup name is normalized to the `.tar.zst` suffix. Backups are stored outside the server volume in the server's backup collection and count toward the configured soft disk limit.

### Request: `BackupRequest`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `backup_name` | `string` | Backup filename or base name. Path separators and traversal are rejected. |
| `overwrite` | `bool` | Replaces an existing backup with the same name. |

### Response: `BackupInfo`

| Field | Type | Description |
| --- | --- | --- |
| `backup_name` | `string` | Stored backup filename. |
| `size_bytes` | `int64` | Archive size. |
| `created_unix` | `int64` | Creation/modification time as Unix seconds. |

The backup is created from the server volume only; the external backup collection is never included in the archive.

## `ListBackups`

```protobuf
rpc ListBackups(Uuid) returns (ListBackupsResponse);
```

Unary RPC.

Lists `.tar.zst` backups for a server, newest first. An empty backup directory returns an empty list.

## `RestoreBackup`

```protobuf
rpc RestoreBackup(BackupRequest) returns (Empty);
```

Unary RPC.

Restores the selected `.tar.zst` backup. Restore first removes all current server contents, then extracts the backup into the server volume. The external backup remains available after restore. Archive member traversal outside the server volume is rejected.

The `overwrite` field is accepted for wire compatibility but does not change restore behavior: restore always replaces the current server contents.

## `GetNodeStats`

```protobuf
rpc GetNodeStats(Empty) returns (NodeStats);
```

Unary RPC.

### `NodeStats`

| Field | Type | Description |
| --- | --- | --- |
| `total_ram_bytes` | `int64` | Host RAM capacity. |
| `used_ram_bytes` | `int64` | Host RAM currently used. |
| `total_disk_bytes` | `int64` | Host filesystem capacity. |
| `used_disk_bytes` | `int64` | Host filesystem usage. |
| `cpu_usage_percent` | `double` | Host CPU usage when available. |
| `cpu_cores` | `uint32` | Host CPU core count. |
| `uptime_sec` | `int64` | Host uptime when available. |
| `daemon_version` | `string` | OpenSrvMgr version. |

## Panel Service

Service name: `opensrvmgr.Panel`

These methods are called by OpenSrvMgr. They are implemented by the Panel.

OpenSrvMgr authenticates every outbound Panel RPC with:

```text
authorization: Bearer <panel-key>
```

The key is supplied at startup with `--panel-key`. The Panel must validate this header and reject requests with an invalid or missing token.

## `GetServerConfig`

```protobuf
rpc GetServerConfig(Uuid) returns (ServerConfig);
```

Unary callback.

Returns the current configuration for a server UUID.

### `ServerConfig`

| Field | Type | Description |
| --- | --- | --- |
| `exists` | `bool` | Whether the server exists. |
| `image` | `string` | Docker image reference. |
| `cmd` | `string` | Server command. |
| `env` | `Env[]` | Environment variables. |
| `ports` | `Port[]` | Host IP/port bindings. |
| `limits` | `Limits` | Resource limits. |
| `stop_command` | `string` | Command sent before stopping. |
| `timeout_sec` | `uint32` | Stop timeout. |
| `auto_restart` | `bool` | Enables crash/OOM restart after the same soft disk-limit pre-check. |
| `sftp_users` | `SftpUser[]` | SFTP accounts. |

## `GetTemplateConfig`

```protobuf
rpc GetTemplateConfig(Uuid) returns (TemplateConfig);
```

Unary callback.

### `TemplateConfig`

| Field | Type | Description |
| --- | --- | --- |
| `exists` | `bool` | Whether the template exists. |
| `image` | `string` | Docker image reference. |
| `entrypoint` | `string` | Executable passed as argument one. |
| `script` | `string` | Argument passed directly after the entrypoint. |
| `env` | `Env[]` | Template environment variables. |
| `memory_bytes` | `int64` | Installer memory limit. |

## `ReportContainerEvent`

```protobuf
rpc ReportContainerEvent(Event) returns (Empty);
```

Unary callback.

### `Event`

| Field | Type | Description |
| --- | --- | --- |
| `uuid` | `string` | Server UUID. |
| `event` | `string` | `started`, `stopped`, `crashed`, or `oom`. |
| `exit_code` | `int32` | Container exit code. |
| `oom_killed` | `bool` | Whether the container was OOM-killed. |
| `timestamp_unix` | `int64` | Event time as Unix seconds. |

## Error Statuses

| Status | Meaning |
| --- | --- |
| `UNAUTHENTICATED` | Missing or invalid bearer token. |
| `INVALID_ARGUMENT` | Malformed request or invalid action/path input. |
| `NOT_FOUND` | Requested server, template, container, file, or directory does not exist. |
| `ALREADY_EXISTS` | Destination exists and overwrite is disabled. |
| `PERMISSION_DENIED` | Operation is forbidden for the authenticated user or path. |
| `FAILED_PRECONDITION` | Operation cannot run in the current container/filesystem state. |
| `INTERNAL` | Docker, filesystem, archive, or callback failure. |
| `UNAVAILABLE` | Required downstream service is unreachable. |
| `CANCELLED` | Client cancelled the RPC or stream. |
