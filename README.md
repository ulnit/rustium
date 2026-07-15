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
| SQLite checkpoint store | Implemented and unit tested |
| Native JSON and Debezium-compatible JSON | Implemented |
| stdout sink | Implemented |
| Kafka sink with idempotent producer settings | Implemented; end-to-end Kafka test pending |
| PostgreSQL 14+ snapshot and `pgoutput` streaming | Implemented; external integration test passes with PostgreSQL 17 |
| MySQL 8+ snapshot and row-binlog streaming | Implemented; Docker integration test passes with MySQL 8.4 |
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

For every batch, Rustium writes to the sink first, persists the checkpoint second, and acknowledges the source third. A crash can replay already delivered events, so the guarantee is at-least-once. Deterministic event IDs support downstream deduplication.

### Build and Test

Requirements:

- Rust `1.88.0` or newer
- CMake and OpenSSL development packages for the Kafka client build
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

This gate forcibly terminates the active binlog dump connection and verifies that Rustium reconnects from the last safe table-map/commit anchor without repeating completed events.

Run the external PostgreSQL 14+ integration test without storing credentials in the repository:

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture
```

The test creates uniquely named tables, publications, and managed replication slots. It covers snapshot handoff, transaction ordering and boundaries, relation-driven schema refresh after adding a column, checkpoint restart without a repeated snapshot, and resource cleanup.

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
- relation-driven schema refresh with version increments after table DDL

The source requires `wal_level=logical`, an existing publication, and a user with the required replication and table-read permissions. See [examples/postgresql.yaml](examples/postgresql.yaml).

Known PostgreSQL gaps include incremental snapshots/signaling, tombstones, durable schema history across restarts, broader type and failure fixtures, and Kafka end-to-end recovery coverage.

### MySQL

The MySQL connector uses row-based binary logs through the native replication protocol.

Implemented behavior:

- MySQL 8.0+ validation for `log_bin`, `binlog_format=ROW`, row image, source server ID, and selected tables
- `FLUSH TABLES WITH READ LOCK` plus a repeatable-read consistent snapshot
- captured binlog file, position, GTID state, and source server ID
- write, update, and delete row events, including multi-row events
- transaction GTIDs and total/data-collection ordering
- dynamic schema refresh after table DDL
- exact restart inside a multi-row event using a replayable table-map anchor and row ordinal
- automatic binlog reconnect from the last safe source position with a finite, observable retry budget
- FULL, MINIMAL, and NOBLOB row images with explicit unavailable values where MySQL omits data
- Docker integration coverage against MySQL 8.4

Recommended MySQL permissions for the connector user:

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

The MySQL Debezium-style example is [examples/mysql.properties](examples/mysql.properties).

Known MySQL gaps include persisted historical schemas for replay across destructive DDL, GTID source include/exclude filters, heartbeat records, custom trust/key stores, tombstones, and incremental snapshots. Partial JSON updates are marked unavailable when the server enables `binlog_row_value_options=PARTIAL_JSON`.

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
| SQLite checkpoint 存储 | 已实现并通过单元测试 |
| 原生 JSON 与 Debezium 兼容 JSON | 已实现 |
| stdout Sink | 已实现 |
| 带幂等 Producer 设置的 Kafka Sink | 已实现；Kafka 端到端测试待补 |
| PostgreSQL 14+ 快照与 `pgoutput` 流式捕获 | 已实现；PostgreSQL 17 外部集成测试通过 |
| MySQL 8+ 快照与行级 binlog 流式捕获 | 已实现；MySQL 8.4 Docker 集成测试通过 |
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

每个批次都先写入 Sink，再持久化 checkpoint，最后确认 Source。崩溃可能重放已经投递的事件，因此保证是 at-least-once。确定性事件 ID 可用于下游去重。

### 构建与测试

环境要求：

- Rust `1.88.0` 或更高版本
- Kafka 客户端构建所需的 CMake 和 OpenSSL 开发包
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

该门槛会强制终止活动的 binlog dump 连接，并验证 Rustium 从最后安全的 table-map/commit 锚点重连，且不会重复已完成事件。

运行外部 PostgreSQL 14+ 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture
```

测试会创建唯一命名的表、publication 和托管 replication slot，覆盖快照切换、事务顺序与边界、新增列后的 Relation 驱动 schema 刷新、从 checkpoint 重启且不重复快照，以及资源清理。

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
- 表 DDL 后由 Relation 消息驱动 schema 刷新并递增版本

Source 需要 `wal_level=logical`、已存在的 publication，以及具备复制和表读取权限的用户。配置示例见 [examples/postgresql.yaml](examples/postgresql.yaml)。

PostgreSQL 已知缺口包括增量快照/信号、tombstone、跨重启持久 schema history、更广的类型与故障样例，以及 Kafka 端到端恢复覆盖。

### MySQL

MySQL 连接器通过原生复制协议读取行级二进制日志。

已实现能力：

- MySQL 8.0+ 的 `log_bin`、`binlog_format=ROW`、row image、源 server ID 和选表校验
- `FLUSH TABLES WITH READ LOCK` 加 repeatable-read 一致性快照
- 捕获 binlog 文件、位置、GTID 状态和源 server ID
- write、update、delete 行事件，包括多行事件
- 事务 GTID、全局顺序和集合内顺序
- 表 DDL 后动态刷新 schema
- 使用可重放的 table-map 锚点和行序号，从多行事件内部精确恢复
- 从最后安全源位点自动重连 binlog，并使用有限、可观测的重试预算
- 支持 FULL、MINIMAL、NOBLOB row image；MySQL 未提供的值会明确标记为 unavailable
- MySQL 8.4 Docker 集成测试

建议给 MySQL 连接器用户授予：

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

MySQL Debezium 风格示例见 [examples/mysql.properties](examples/mysql.properties)。

MySQL 已知缺口包括跨破坏性 DDL 回放所需的持久化历史 schema、GTID source include/exclude 过滤、heartbeat、自定义 trust/key store、tombstone 和增量快照。当服务端启用 `binlog_row_value_options=PARTIAL_JSON` 时，部分 JSON 更新会标记为 unavailable。

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
