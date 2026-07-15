# Rustium

**Change Data Capture, reimagined in Rust.**

[![CI](https://github.com/ulnit/rustium/actions/workflows/ci.yml/badge.svg)](https://github.com/ulnit/rustium/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)

[English](#english) | [简体中文](#简体中文)

> Rustium is an independent Rust implementation. It uses current Debezium behavior and configuration names as compatibility references, but it is not a fork and does not copy Debezium's Java implementation.

> Rustium 是独立的 Rust 实现。项目以最新版 Debezium 的行为和配置名称作为兼容性参考，但不是其 fork，也不复制其 Java 实现。

---

## English

### Overview

Rustium is a standalone, log-based Change Data Capture service. It reads committed database changes, normalizes them into a typed internal event model, and delivers ordered events to stdout or Kafka without requiring a JVM or Kafka Connect.

The connector priority is fixed:

1. PostgreSQL
2. MySQL
3. SQL Server

Other database connectors will not be added until these three connectors have passed their correctness and recovery gates.

### Current Status

The repository contains a runnable alpha implementation.

| Area | Status |
|---|---|
| Typed `ChangeEvent` model and deterministic event IDs | Implemented |
| Bounded Tokio pipeline and graceful shutdown | Implemented |
| At-least-once sink/checkpoint/source acknowledgement ordering | Implemented |
| SQLite checkpoint v2 with versioned connector state | Implemented and unit tested; v1 JSON remains readable |
| Native JSON and Debezium-compatible JSON, including delete tombstones | Implemented |
| stdout sink | Implemented |
| Kafka sink with idempotent producer settings | Implemented; end-to-end Kafka test pending |
| PostgreSQL 14+ snapshot, `pgoutput`, and persistent schema history | Implemented; destructive-DDL restart gate passes with PostgreSQL 17 |
| MySQL 8+ snapshot, row-binlog streaming, and persistent schema history | Implemented; Docker and external destructive-DDL restart gates pass with MySQL 8.4 |
| SQL Server CDC | Implemented; external integration test passes with SQL Server 2022 Developer CU25 |
| CLI, health, status, stop, and Prometheus endpoints | Implemented |
| Container image, Helm chart, published crates | Not published |

This is not a production-stable release. Persisted state and public configuration may still change before `1.0`.

### Implemented Architecture

```text
 PostgreSQL WAL / MySQL binlog / SQL Server CDC
              |
              v
        Source connector
              |
       bounded channel
              |
              v
   typed ChangeEvent + encoder
              |
              v
       stdout / Kafka sink
              |
       durable acknowledgement
              |
              v
      SQLite checkpoint store
              |
              v
 source acknowledgement / feedback
```

For every batch, Rustium writes to the sink first, persists the source position and versioned connector state in one checkpoint second, and acknowledges the source third. A crash can replay already delivered events, so the guarantee is at-least-once. Deterministic event IDs support downstream deduplication.

### Build and Test

Requirements:

- Rust `1.88.0` or newer
- CMake, OpenSSL, libcurl, and Cyrus SASL development packages for the Kafka client build (`cmake`, `libssl-dev`, `libcurl4-openssl-dev`, and `libsasl2-dev` on Ubuntu)
- Access to PostgreSQL 14+ with logical replication for the ignored PostgreSQL integration test
- Access to SQL Server 2017+ with CDC and SQL Server Agent for the ignored SQL Server external integration test
- Docker for the ignored MySQL and SQL Server container integration tests

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Run the real MySQL 8.4 integration test:

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

This gate forcibly terminates the active binlog dump connection and verifies reconnect from the last safe table-map/commit anchor. It also stops Rustium, writes an old-schema row, applies destructive DDL, writes a new-schema row, and verifies that restart decoding uses the persisted historical schema before applying the binlog DDL in order.

Run the external MySQL 8.0+ integration test without storing credentials in the repository:

```bash
export RUSTIUM_MYSQL_TEST_HOST=mysql.example.com
export RUSTIUM_MYSQL_TEST_PORT=3306
export RUSTIUM_MYSQL_TEST_ADMIN_USER=root
export RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_USER=cdc
export RUSTIUM_MYSQL_TEST_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

The admin account only creates and removes a uniquely named test table. The connector uses the CDC account for snapshot and replication. This gate has passed against MySQL 8.4 with row binlog and GTID enabled.

Run the external PostgreSQL 14+ integration test without storing credentials in the repository:

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture
```

The test creates uniquely named tables, publications, and managed replication slots. It covers snapshot handoff, transaction ordering, checkpoint stop, an old-schema row, destructive drop/add-column DDL, a new-schema row, historical `Relation` replay with schema versions 1 and 2, restart without a repeated snapshot, and cleanup.

Run the external SQL Server 2017+ CDC integration test without storing credentials in the repository:

```bash
export RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com
export RUSTIUM_SQLSERVER_TEST_PORT=1433
export RUSTIUM_SQLSERVER_TEST_USER=sa
export RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me'
export RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

The test creates a uniquely named table and capture instance. It verifies snapshot rows, CDC initialization, ordered transactional create/update/delete events, the commit boundary, checkpoint restart without snapshot replay, and cleanup.

### Embed Rustium in a Rust Project

Running the `rustium` CLI as a separate process is the recommended production boundary. Applications that need in-process lifecycle control or a custom `Sink` can assemble the same public crates used by the CLI.

The crates are not published to crates.io yet, so add the required workspace packages as Git dependencies. Cargo records the resolved commit in `Cargo.lock`; use a `rev` instead of `branch` when your release process requires an explicit source pin.

```toml
[dependencies]
rustium-config = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-core = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-json = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-postgresql = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-sink-stdout = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-state = { git = "https://github.com/ulnit/rustium", branch = "main" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
tokio-util = "0.7"
```

Load the same YAML or Debezium `.properties` file used by the CLI, construct the source, encoder, sink, and checkpoint store, then run `ConnectorRuntime` with a cancellation token:

```rust,no_run
use std::sync::Arc;

use rustium_config::Config;
use rustium_core::{
    CheckpointStore, ConnectorIdentity, ConnectorRuntime, Error, EventEncoder, Result,
    RuntimeConfig, RuntimeStatus,
};
use rustium_format_json::{DebeziumJsonEncoder, JsonEncoderConfig};
use rustium_postgresql::PostgresSource;
use rustium_sink_stdout::StdoutSink;
use rustium_state::SqliteCheckpointStore;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load("rustium.yaml")?;
    let source_config = config.source.as_postgresql().cloned().ok_or_else(|| {
        Error::Configuration("this application expects a PostgreSQL source".into())
    })?;

    let source = Box::new(PostgresSource::new(
        &config.metadata.name,
        source_config,
        config.snapshot.clone(),
    ));
    let encoder: Arc<dyn EventEncoder> = Arc::new(DebeziumJsonEncoder::new(
        JsonEncoderConfig {
            topic_prefix: config.sink.topic_prefix().into(),
            unavailable_value: config.format.unavailable_value.clone(),
            tombstones_on_delete: config.format.tombstones_on_delete,
        },
    ));
    let checkpoints: Arc<dyn CheckpointStore> =
        Arc::new(SqliteCheckpointStore::open(&config.state.path).await?);
    let status = RuntimeStatus::new(&config.metadata.name);
    let runtime = ConnectorRuntime::new(
        ConnectorIdentity::new(&config.metadata.name),
        source,
        encoder,
        Box::new(StdoutSink::default()),
        checkpoints,
        RuntimeConfig {
            channel_capacity: config.runtime.channel_capacity,
            max_batch_size: config.runtime.max_batch_size,
            flush_interval: config.runtime.flush_interval,
            shutdown_timeout: config.runtime.shutdown_timeout,
            config_fingerprint: config.fingerprint(),
        },
        status,
    );

    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal_cancellation.cancel();
    });
    runtime.run(cancellation).await
}
```

The checked source is [crates/rustium-cli/examples/embed_postgresql.rs](crates/rustium-cli/examples/embed_postgresql.rs). Place the connector configuration at `rustium.yaml` and run it with `cargo run -p rustium --example embed_postgresql`.

For MySQL or SQL Server, depend on `rustium-mysql` or `rustium-sqlserver` and construct `MySqlSource` or `SqlServerSource` with the corresponding `SourceConfig` value. Replace `StdoutSink` with `KafkaSink` from `rustium-sink-kafka` for durable Kafka delivery, or implement the async `Sink` trait for application-specific delivery. A custom sink must return from `write` only after the batch is durably accepted; Rustium checkpoints and acknowledges the database source afterward.

### CLI

```bash
# Validate configuration and external dependencies.
cargo run -p rustium -- validate --config examples/postgresql.yaml

# Run one connector in the foreground.
cargo run -p rustium -- run --config examples/postgresql.yaml

# Explicitly remove one connector checkpoint.
cargo run -p rustium -- state reset \
  --config examples/postgresql.yaml \
  --confirm
```

Configuration supports `${NAME}` and `${NAME:-default}` environment interpolation. Credentials are excluded from the semantic configuration fingerprint and are not logged by Rustium.

### PostgreSQL

The PostgreSQL connector uses logical replication with `pgoutput` protocol version 2.

Implemented behavior:

- PostgreSQL 14+ validation, publication validation, and managed or external slot ownership
- exported consistent snapshot and bounded paginated table reads
- insert, update, delete, and truncate events
- transaction ordering and same-LSN event ordinals
- TOAST-unavailable handling
- restart recovery from SQLite checkpoints
- replication feedback only after sink acknowledgement and checkpoint persistence
- schema discovery and table include/exclude regular expressions
- checkpointed PostgreSQL schema history restored before WAL replay
- relation-driven historical column layout, type OID/typmod, key metadata, and schema version increments after table DDL

The source requires `wal_level=logical`, an existing publication, and a user with the required replication and table-read permissions. See [examples/postgresql.yaml](examples/postgresql.yaml).

Checkpoint v1 JSON remains readable, but a completed PostgreSQL v1 checkpoint has no historical Relation baseline and is rejected for resume. Reset it and run one new initial snapshot to establish checkpoint v2 schema history.

Known PostgreSQL gaps include incremental snapshots/signaling, transient-column optionality/default reconstruction beyond what `Relation` carries, broader type and failure fixtures, and Kafka end-to-end recovery coverage.

### MySQL

The MySQL connector uses row-based binary logs through the native replication protocol.

Implemented behavior:

- MySQL 8.0+ validation for `log_bin`, `binlog_format=ROW`, row image, source server ID, and selected tables
- `FLUSH TABLES WITH READ LOCK` plus a repeatable-read consistent snapshot
- captured binlog file, position, GTID state, and source server ID
- write, update, and delete row events, including multi-row events
- transaction GTIDs and total/data-collection ordering
- checkpointed MySQL schema history, restored before binlog replay
- ordered DDL application for `CREATE TABLE`, `ALTER TABLE` add/drop/rename/modify/change column, `DROP TABLE`, and `RENAME TABLE`
- exact restart inside a multi-row event using a replayable table-map anchor and row ordinal
- automatic binlog reconnect from the last safe source position with a finite, observable retry budget
- FULL, MINIMAL, and NOBLOB row images with explicit unavailable values where MySQL omits data
- Docker and external integration coverage against MySQL 8.4, including restart across destructive DDL

Recommended MySQL permissions for the connector user:

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

The MySQL Debezium-style example is [examples/mysql.properties](examples/mysql.properties).

DDL parsing failures stop the connector by default. Debezium-compatible `schema.history.internal.skip.unparseable.ddl=true` can advance past unsupported DDL with a warning, but doing so can leave schema metadata incomplete.

Checkpoint v1 JSON remains readable, but a completed MySQL v1 checkpoint has no historical schema baseline and is rejected for resume. Reset that checkpoint and run a new initial snapshot once to establish checkpoint v2 schema history.

Known MySQL gaps include GTID source include/exclude filters, periodic heartbeat and signaling records, custom trust/key stores, incremental snapshots, and wider DDL/type fixtures. Partial JSON updates are marked unavailable when the server enables `binlog_row_value_options=PARTIAL_JSON`.

### SQL Server

The SQL Server connector is implemented on top of native SQL Server CDC change tables.

Implemented behavior:

- SQL Server 2017+ and database CDC validation
- single-database source ownership and capture-instance discovery
- snapshot handoff at `sys.fn_cdc_get_max_lsn()`
- direct CDC change-table reads ordered by commit LSN, sequence value, and operation
- insert/delete conversion and update operation 3/4 before/after pairing
- transaction ordering, mid-transaction replay, and checkpoint recovery
- explicit failure when CDC cleanup removes the required checkpoint LSN
- bounded CDC queries controlled by `streaming.fetch.size`

The current implementation requires exactly one entry in `database.names`, one active capture instance per selected table, and `data.query.mode=direct`. Snapshot, streaming, transaction ordering, checkpoint restart, and cleanup have been externally integration-tested against SQL Server 2022 Developer RTM-CU25. See [examples/sqlserver.properties](examples/sqlserver.properties).

The database must have CDC enabled, SQL Server Agent must run the capture job, and the connector user needs source-table reads plus direct read access to the `cdc` schema. The separate Docker portability test remains runnable with:

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### Debezium Configuration Compatibility

Rustium accepts strict native YAML and Debezium-style Java `.properties` files. Familiar names are preferred so existing deployments can migrate with smaller configuration changes.

Currently mapped PostgreSQL properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`, `database.dbname`
- `database.sslmode`, `plugin.name`, `slot.name`, `publication.name`
- `schema.include.list`, `schema.exclude.list`, `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Currently mapped MySQL properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`
- `database.server.id`, `database.ssl.mode`
- `database.include.list`, `database.exclude.list`
- `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`, `connect.timeout.ms`
- `connect.keep.alive`, `connect.keep.alive.interval.ms`
- `schema.history.internal.skip.unparseable.ddl`
- `rustium.source.reconnect.max.attempts`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Currently mapped SQL Server properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`
- `database.names`, `database.encrypt`, `database.trustServerCertificate`
- `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`, `snapshot.isolation.mode`
- `data.query.mode=direct`, `streaming.fetch.size`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Common Debezium format properties include `unavailable.value.placeholder` and `tombstones.on.delete`. Tombstones default to enabled in `debezium_json`: each delete envelope is followed in the same delivery batch by the same key with a null value. Set `tombstones.on.delete=false` or native YAML `format.tombstones_on_delete: false` to disable them.

Unsupported properties are reported as compatibility warnings instead of being silently treated as implemented. Rustium-specific source retry, sink, state, server, logging, and Kafka producer settings use the `rustium.*` prefix.

### Formats and Sinks

The internal model preserves null, signed and unsigned integers, decimal text, floating point, binary, date/time/timestamp, UUID, JSON, array, and unavailable values.

Available encoders:

- `rustium_json`: versioned native event payload
- `debezium_json`: `before`, `after`, `source`, `op`, `ts_ms`, and transaction metadata

Available sinks:

- `stdout`: development and protocol inspection
- `kafka`: `librdkafka`, configurable acknowledgements/compression/properties, and idempotence when durable acknowledgements are selected

### Management API

The server binds to `127.0.0.1:8080` by default.

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | Process liveness |
| `GET /health/ready` | Connector readiness |
| `GET /v1/connector/status` | State, position, checkpoint time, queue, and delivery counters |
| `POST /v1/connector/stop` | Graceful stop when mutations are enabled |
| `GET /metrics` | Prometheus exposition |

### Documentation and Contribution Policy

- User-facing documentation is complete English first, followed by complete Simplified Chinese.
- Code, configuration keys, APIs, logs, issues, and commit messages use English.
- Behavioral changes need tests, especially recovery and acknowledgement-order tests.
- Commits must include a DCO `Signed-off-by` line.

See [docs/design.md](docs/design.md) for the normative architecture and connector design.

### License and Independence

Rustium is licensed under the [Apache License 2.0](LICENSE). Rustium is not affiliated with, endorsed by, or a fork of Debezium or Red Hat. Debezium is referenced solely for behavioral and migration compatibility.

---

## 简体中文

### 概述

Rustium 是一个独立运行、基于数据库日志的变更数据捕获服务。它读取数据库已提交变更，规范化为强类型内部事件，并按顺序投递到 stdout 或 Kafka，不依赖 JVM 或 Kafka Connect。

连接器优先级固定如下：

1. PostgreSQL
2. MySQL
3. SQL Server

在这三个连接器全部通过正确性和恢复验证之前，不添加其他数据库连接器。

### 当前状态

仓库已经包含可运行的 alpha 实现。

| 领域 | 状态 |
|---|---|
| 强类型 `ChangeEvent` 与确定性事件 ID | 已实现 |
| 有界 Tokio 流水线与优雅关闭 | 已实现 |
| Sink/checkpoint/Source 确认顺序的 at-least-once 语义 | 已实现 |
| 带版本化连接器状态的 SQLite checkpoint v2 | 已实现并通过单元测试；仍可读取 v1 JSON |
| 原生 JSON 与 Debezium 兼容 JSON，包括 delete tombstone | 已实现 |
| stdout Sink | 已实现 |
| 带幂等 Producer 设置的 Kafka Sink | 已实现；Kafka 端到端测试待补 |
| PostgreSQL 14+ 快照、`pgoutput` 与持久 schema history | 已实现；PostgreSQL 17 破坏性 DDL 重启门槛通过 |
| MySQL 8+ 快照、行级 binlog 与持久 schema history | 已实现；MySQL 8.4 Docker 和外部破坏性 DDL 重启门槛通过 |
| SQL Server CDC | 已实现；SQL Server 2022 Developer CU25 外部集成测试通过 |
| CLI、健康、状态、停止和 Prometheus 端点 | 已实现 |
| 容器镜像、Helm Chart、已发布 crate | 尚未发布 |

当前版本尚未达到生产稳定。`1.0` 之前，持久化状态和公共配置仍可能调整。

### 已实现架构

```text
 PostgreSQL WAL / MySQL binlog / SQL Server CDC
              |
              v
          Source 连接器
              |
           有界 channel
              |
              v
   强类型 ChangeEvent + Encoder
              |
              v
       stdout / Kafka Sink
              |
           持久确认
              |
              v
      SQLite checkpoint 存储
              |
              v
       Source 确认 / 反馈
```

每个批次都先写入 Sink，再在同一个 checkpoint 中持久化源位点和版本化连接器状态，最后确认 Source。崩溃可能重放已经投递的事件，因此保证是 at-least-once。确定性事件 ID 可用于下游去重。

### 构建与测试

环境要求：

- Rust `1.88.0` 或更高版本
- Kafka 客户端构建所需的 CMake、OpenSSL、libcurl 和 Cyrus SASL 开发包（Ubuntu 上为 `cmake`、`libssl-dev`、`libcurl4-openssl-dev` 和 `libsasl2-dev`）
- 运行被忽略的 PostgreSQL 集成测试时，需要可访问已启用逻辑复制的 PostgreSQL 14+
- 运行被忽略的 SQL Server 外部集成测试时，需要可访问已启用 CDC 和 SQL Server Agent 的 SQL Server 2017+
- 运行被忽略的 MySQL 和 SQL Server 容器集成测试时需要 Docker

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

运行真实 MySQL 8.4 集成测试：

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

该门槛会强制终止活动的 binlog dump 连接，并验证 Rustium 从最后安全的 table-map/commit 锚点重连。测试还会停止 Rustium，依次写入旧 schema 行、执行破坏性 DDL、写入新 schema 行，并验证重启后先使用持久化历史 schema 解码，再按 binlog 顺序应用 DDL。

运行外部 MySQL 8.0+ 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_MYSQL_TEST_HOST=mysql.example.com
export RUSTIUM_MYSQL_TEST_PORT=3306
export RUSTIUM_MYSQL_TEST_ADMIN_USER=root
export RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_USER=cdc
export RUSTIUM_MYSQL_TEST_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

管理账号只负责创建和删除唯一命名的测试表；连接器使用 CDC 账号执行快照和复制。该门槛已在启用行级 binlog 和 GTID 的 MySQL 8.4 上通过。

运行外部 PostgreSQL 14+ 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture
```

测试会创建唯一命名的表、publication 和托管 replication slot，覆盖快照切换、事务顺序、checkpoint 停止、旧 schema 行、破坏性删列/加列 DDL、新 schema 行、schema version 1 和 2 的历史 `Relation` 重放、重启不重复快照，以及资源清理。

运行外部 SQL Server 2017+ CDC 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com
export RUSTIUM_SQLSERVER_TEST_PORT=1433
export RUSTIUM_SQLSERVER_TEST_USER=sa
export RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me'
export RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

测试会创建唯一命名的表和 capture instance，验证快照记录、CDC 初始化、同一事务内有序的 create/update/delete 事件、commit 边界、checkpoint 重启不重复快照，以及资源清理。

### 在 Rust 项目中嵌入 Rustium

生产环境优先推荐将 `rustium` CLI 作为独立进程运行。需要进程内生命周期控制或自定义 `Sink` 的应用，可以直接组装 CLI 使用的公开 crate。

这些 crate 尚未发布到 crates.io，因此先通过 Git 依赖引入所需 workspace package。Cargo 会把实际解析的提交记录在 `Cargo.lock` 中；发布流程需要显式锁定源码时，请将 `branch` 改为具体 `rev`。

```toml
[dependencies]
rustium-config = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-core = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-json = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-postgresql = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-sink-stdout = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-state = { git = "https://github.com/ulnit/rustium", branch = "main" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
tokio-util = "0.7"
```

加载 CLI 使用的同一份 YAML 或 Debezium `.properties` 配置，构造 Source、Encoder、Sink 和 checkpoint store，再通过 cancellation token 运行 `ConnectorRuntime`：

```rust,no_run
use std::sync::Arc;

use rustium_config::Config;
use rustium_core::{
    CheckpointStore, ConnectorIdentity, ConnectorRuntime, Error, EventEncoder, Result,
    RuntimeConfig, RuntimeStatus,
};
use rustium_format_json::{DebeziumJsonEncoder, JsonEncoderConfig};
use rustium_postgresql::PostgresSource;
use rustium_sink_stdout::StdoutSink;
use rustium_state::SqliteCheckpointStore;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load("rustium.yaml")?;
    let source_config = config.source.as_postgresql().cloned().ok_or_else(|| {
        Error::Configuration("this application expects a PostgreSQL source".into())
    })?;

    let source = Box::new(PostgresSource::new(
        &config.metadata.name,
        source_config,
        config.snapshot.clone(),
    ));
    let encoder: Arc<dyn EventEncoder> = Arc::new(DebeziumJsonEncoder::new(
        JsonEncoderConfig {
            topic_prefix: config.sink.topic_prefix().into(),
            unavailable_value: config.format.unavailable_value.clone(),
            tombstones_on_delete: config.format.tombstones_on_delete,
        },
    ));
    let checkpoints: Arc<dyn CheckpointStore> =
        Arc::new(SqliteCheckpointStore::open(&config.state.path).await?);
    let status = RuntimeStatus::new(&config.metadata.name);
    let runtime = ConnectorRuntime::new(
        ConnectorIdentity::new(&config.metadata.name),
        source,
        encoder,
        Box::new(StdoutSink::default()),
        checkpoints,
        RuntimeConfig {
            channel_capacity: config.runtime.channel_capacity,
            max_batch_size: config.runtime.max_batch_size,
            flush_interval: config.runtime.flush_interval,
            shutdown_timeout: config.runtime.shutdown_timeout,
            config_fingerprint: config.fingerprint(),
        },
        status,
    );

    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal_cancellation.cancel();
    });
    runtime.run(cancellation).await
}
```

仓库中持续编译校验的源码位于 [crates/rustium-cli/examples/embed_postgresql.rs](crates/rustium-cli/examples/embed_postgresql.rs)。将连接器配置保存为 `rustium.yaml` 后，可通过 `cargo run -p rustium --example embed_postgresql` 运行。

MySQL 或 SQL Server 项目分别依赖 `rustium-mysql` 或 `rustium-sqlserver`，并使用对应 `SourceConfig` 构造 `MySqlSource` 或 `SqlServerSource`。需要持久 Kafka 投递时，用 `rustium-sink-kafka` 的 `KafkaSink` 替换 `StdoutSink`；应用也可以实现异步 `Sink` trait。自定义 Sink 的 `write` 只有在批次已经持久接收后才能返回，之后 Rustium 才会保存 checkpoint 并确认数据库 Source。

### CLI

```bash
# 校验配置与外部依赖。
cargo run -p rustium -- validate --config examples/postgresql.yaml

# 前台运行一个连接器。
cargo run -p rustium -- run --config examples/postgresql.yaml

# 显式删除一个连接器的 checkpoint。
cargo run -p rustium -- state reset \
  --config examples/postgresql.yaml \
  --confirm
```

配置支持 `${NAME}` 和 `${NAME:-default}` 环境变量插值。凭据不参与语义配置指纹，Rustium 也不会主动记录凭据。

### PostgreSQL

PostgreSQL 连接器使用逻辑复制和 `pgoutput` 协议版本 2。

已实现能力：

- PostgreSQL 14+、publication、托管或外部 slot 所有权校验
- 导出一致性快照与有界分页读取
- insert、update、delete、truncate 事件
- 事务顺序与同一 LSN 事件序号
- TOAST 不可用值处理
- 从 SQLite checkpoint 重启恢复
- 仅在 Sink 确认和 checkpoint 持久化后发送复制反馈
- schema 发现与表 include/exclude 正则过滤
- 持久化 PostgreSQL schema history，并在 WAL 重放前恢复
- 表 DDL 后由 Relation 消息驱动历史列布局、类型 OID/typmod、key 元数据和 schema 版本递增

Source 需要 `wal_level=logical`、已存在的 publication，以及具备复制和表读取权限的用户。配置示例见 [examples/postgresql.yaml](examples/postgresql.yaml)。

Checkpoint v1 JSON 仍可读取，但已完成的 PostgreSQL v1 checkpoint 不含历史 Relation 基线，因此会拒绝恢复。升级后需要重置该 checkpoint 并执行一次新的 initial snapshot，以建立 checkpoint v2 schema history。

PostgreSQL 已知缺口包括增量快照/信号、超出 `Relation` 信息范围的短暂历史列可空性/default 重建、更广的类型与故障样例，以及 Kafka 端到端恢复覆盖。

### MySQL

MySQL 连接器通过原生复制协议读取行级二进制日志。

已实现能力：

- MySQL 8.0+ 的 `log_bin`、`binlog_format=ROW`、row image、源 server ID 和选表校验
- `FLUSH TABLES WITH READ LOCK` 加 repeatable-read 一致性快照
- 捕获 binlog 文件、位置、GTID 状态和源 server ID
- write、update、delete 行事件，包括多行事件
- 事务 GTID、全局顺序和集合内顺序
- 持久化 MySQL schema history，并在 binlog 重放前恢复
- 按顺序应用 `CREATE TABLE`、`ALTER TABLE` 增删/重命名/修改/变更列、`DROP TABLE` 和 `RENAME TABLE`
- 使用可重放的 table-map 锚点和行序号，从多行事件内部精确恢复
- 从最后安全源位点自动重连 binlog，并使用有限、可观测的重试预算
- 支持 FULL、MINIMAL、NOBLOB row image；MySQL 未提供的值会明确标记为 unavailable
- MySQL 8.4 Docker 和外部集成测试，包括跨破坏性 DDL 重启

建议给 MySQL 连接器用户授予：

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

MySQL Debezium 风格示例见 [examples/mysql.properties](examples/mysql.properties)。

DDL 默认解析失败即停止连接器。可使用 Debezium 兼容参数 `schema.history.internal.skip.unparseable.ddl=true` 警告后跳过不支持的 DDL，但这可能导致 schema 元数据不完整。

Checkpoint v1 JSON 仍可读取，但已完成的 MySQL v1 checkpoint 不含历史 schema 基线，因此会拒绝恢复。升级后需要重置该 checkpoint 并执行一次新的 initial snapshot，以建立 checkpoint v2 schema history。

MySQL 已知缺口包括 GTID source include/exclude 过滤、周期 heartbeat 和信号记录、自定义 trust/key store、增量快照，以及更广的 DDL/类型样例。当服务端启用 `binlog_row_value_options=PARTIAL_JSON` 时，部分 JSON 更新会标记为 unavailable。

### Debezium 配置兼容

Rustium 同时接受严格的原生 YAML 和 Debezium 风格 Java `.properties`。项目优先采用熟悉的参数名，减少现有部署迁移时的配置改动。

当前已映射的 PostgreSQL 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`、`database.dbname`
- `database.sslmode`、`plugin.name`、`slot.name`、`publication.name`
- `schema.include.list`、`schema.exclude.list`、`table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

当前已映射的 MySQL 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`
- `database.server.id`、`database.ssl.mode`
- `database.include.list`、`database.exclude.list`
- `table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`、`connect.timeout.ms`
- `connect.keep.alive`、`connect.keep.alive.interval.ms`
- `schema.history.internal.skip.unparseable.ddl`
- `rustium.source.reconnect.max.attempts`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

当前已映射的 SQL Server 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`
- `database.names`、`database.encrypt`、`database.trustServerCertificate`
- `table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`、`snapshot.isolation.mode`
- `data.query.mode=direct`、`streaming.fetch.size`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

通用 Debezium 格式参数包括 `unavailable.value.placeholder` 和 `tombstones.on.delete`。在 `debezium_json` 中 tombstone 默认启用：每条 delete envelope 后会在同一个投递批次中追加一条 key 相同、value 为 null 的记录。可通过 `tombstones.on.delete=false` 或原生 YAML 的 `format.tombstones_on_delete: false` 关闭。

未支持的参数会输出兼容性警告，不会被静默伪装成已实现。Rustium 自身的 Source 重试、Sink、状态、Server、日志和 Kafka Producer 设置使用 `rustium.*` 前缀。

### SQL Server

SQL Server 连接器基于原生 SQL Server CDC change table 实现。

已实现能力：

- SQL Server 2017+ 和数据库 CDC 校验
- 单数据库 Source 所有权和 capture instance 发现
- 以 `sys.fn_cdc_get_max_lsn()` 作为快照切换点
- 按 commit LSN、sequence value、operation 排序的 direct CDC change-table 读取
- insert/delete 转换，以及 update operation 3/4 的 before/after 配对
- 事务顺序、事务中间重放和 checkpoint 恢复
- CDC cleanup 删除所需 checkpoint LSN 时明确失败
- 由 `streaming.fetch.size` 控制的有界 CDC 查询

当前实现要求 `database.names` 只有一个数据库、每张选表只有一个活动 capture instance，并使用 `data.query.mode=direct`。快照、流式捕获、事务顺序、checkpoint 重启和清理已在 SQL Server 2022 Developer RTM-CU25 上通过外部集成测试。示例见 [examples/sqlserver.properties](examples/sqlserver.properties)。

数据库必须启用 CDC，SQL Server Agent 必须运行 capture job，连接器用户需要读取源表，并能直接读取 `cdc` schema。独立的 Docker 可移植性测试仍可通过以下命令运行：

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 格式与 Sink

内部模型保留 null、有符号/无符号整数、decimal 文本、浮点数、binary、date/time/timestamp、UUID、JSON、array 和 unavailable 值。

可用 Encoder：

- `rustium_json`：带版本的原生事件
- `debezium_json`：`before`、`after`、`source`、`op`、`ts_ms` 和事务元数据

可用 Sink：

- `stdout`：用于开发和协议检查
- `kafka`：基于 `librdkafka`，支持可配置确认、压缩和属性；选择持久确认时启用幂等能力

### 管理 API

Server 默认绑定 `127.0.0.1:8080`。

| 端点 | 用途 |
|---|---|
| `GET /health/live` | 进程存活 |
| `GET /health/ready` | 连接器就绪状态 |
| `GET /v1/connector/status` | 状态、位点、checkpoint 时间、队列和投递计数 |
| `POST /v1/connector/stop` | 启用变更端点时优雅停止 |
| `GET /metrics` | Prometheus 指标 |

### 文档与贡献策略

- 面向用户的文档必须先提供完整英文，再提供完整简体中文。
- 代码、配置键、API、日志、Issue 和提交信息使用英文。
- 行为变更必须补测试，尤其是恢复和确认顺序测试。
- Commit 必须包含 DCO `Signed-off-by`。

规范架构和连接器设计见 [docs/design.md](docs/design.md)。

### 许可证与独立性

Rustium 使用 [Apache License 2.0](LICENSE)。Rustium 与 Debezium 或 Red Hat 没有关联、背书或 fork 关系。文档引用 Debezium 仅用于行为和迁移兼容。
