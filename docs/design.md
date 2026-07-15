# Rustium Architecture and Design

> Status: Pre-alpha design baseline<br>
> Document version: 0.1<br>
> Last updated: 2026-07-15

## Language Policy

Rustium is a global open-source project. Project documentation MUST be published in both English and Simplified Chinese, with English first. The English text is the normative version when the two versions differ. Code, configuration keys, API fields, logs, commit messages, and issue titles SHOULD use English.

This document first presents the complete English design, followed by its Chinese translation.

---

## English

### 1. Project Definition

Rustium is an independently implemented, open-source, log-based Change Data Capture (CDC) platform written in Rust. It reads committed changes from a database replication log, converts them into a stable internal event model, and delivers them to downstream systems.

The first product form is a standalone service distributed as a single binary. Rustium is designed to work without Kafka Connect or a JVM. Kafka is an optional sink, not a runtime dependency.

**Tagline:** Change Data Capture, reimagined in Rust.

#### 1.1 Goals

- Preserve source ordering and database positions correctly.
- Prefer correctness, recoverability, and bounded resource usage over benchmark-only throughput.
- Provide a small standalone deployment with clear operational behavior.
- Support source connectors, event encoders, state stores, and sinks through stable Rust interfaces.
- Offer a tested Debezium-compatible JSON envelope for easier ecosystem adoption.
- Make health, lag, failures, and retained-log risk observable.

#### 1.2 MVP non-goals

- A Kafka Connect plugin or full Kafka Connect worker compatibility.
- End-to-end exactly-once delivery.
- Compatibility with every Debezium connector, option, data type, or Single Message Transform.
- High-availability coordination between multiple Rustium instances.
- User-provided native or WebAssembly transformations.
- Capturing changes by polling application tables.

### 2. Current Status and MVP Scope

The repository currently contains design material and the Apache-2.0 license. It does not yet contain a runnable Rustium implementation, published crate, container image, Helm chart, benchmark, or production release. Every capability in this document is a target until code and tests are merged.

The first usable release is scoped to:

| Capability | MVP target |
|---|---|
| Source | PostgreSQL 14+ logical replication using `pgoutput` |
| Capture modes | Consistent initial snapshot, then continuous streaming |
| Sinks | stdout for development; Kafka for durable production delivery |
| Event format | Versioned JSON and a tested Debezium-compatible JSON encoder |
| State | Local SQLite checkpoint store |
| Runtime | Tokio, one connector per process |
| Management | CLI, health/status HTTP endpoints, Prometheus metrics |
| Delivery | At-least-once with deterministic event identifiers |
| Deployment | Standalone binary and container image |

MySQL, Schema Registry formats, incremental snapshots, a multi-connector daemon, Kubernetes Operator, embedded mode, and additional sinks are later milestones.

### 3. Design Principles and Invariants

The following rules are architectural invariants:

1. **No acknowledged data loss.** A source position MUST NOT be checkpointed until the configured sink has durably acknowledged every event covered by that position.
2. **At-least-once by default.** A crash between sink acknowledgement and checkpoint recovery can produce duplicates. Rustium MUST document this and provide stable event identifiers for deduplication.
3. **Source order is authoritative.** Events from one source partition MUST remain ordered by source position. Parallel work may not reorder them.
4. **Memory is bounded.** Every inter-stage queue MUST have a configured bound. Backpressure propagates to the source instead of allowing unbounded buffering.
5. **Capabilities are explicit.** A connector or sink MUST advertise optional capabilities. The runtime must reject unsupported configurations during validation.
6. **State is versioned.** Checkpoints, event schemas, configuration schemas, and persisted metadata MUST carry versions and support explicit migrations.
7. **Failure is visible.** Rustium MUST stop by default on an event it cannot decode or deliver. Skipping data requires an explicit policy and an observable record.
8. **Secrets stay secret.** Credentials and row payloads MUST NOT appear in logs, metrics, status APIs, or panic output by default.

### 4. System Architecture

Rustium separates the data plane from the control plane.

```text
                         Control plane
  Config validation | lifecycle | status API | health | metrics
                                |
                                v
+-------------------------------------------------------------------+
|                           Data plane                              |
|                                                                   |
|  Source -> Decode -> Normalize -> Filter -> Transform -> Route    |
|     |                                                   |         |
|     |                                                   v         |
|     +-> snapshot / replication position       Encode -> Batch     |
|                                                         |         |
|                                                         v         |
|                                                       Sink        |
|                                                         | ack     |
|                                                         v         |
|                                              Checkpoint store      |
+-------------------------------------------------------------------+
              | schema cache | retry policy | telemetry |
```

#### 4.1 Data plane

- **Source connector:** discovers source schemas, performs snapshots, reads the replication stream, and emits source records with positions.
- **Decoder:** turns protocol messages such as PostgreSQL `pgoutput` messages into typed values.
- **Normalizer:** creates the database-neutral internal `ChangeEvent`.
- **Filter:** applies configured database, schema, and table inclusion/exclusion rules.
- **Transform:** applies built-in, deterministic transformations. The MVP may initially expose only field masking and topic routing.
- **Router:** selects a destination topic or stream from event metadata.
- **Encoder:** serializes the internal event to native versioned JSON or a Debezium-compatible JSON envelope.
- **Batcher and sink:** groups events within ordering constraints and waits for durable delivery acknowledgement.
- **Checkpoint store:** atomically persists the last fully acknowledged source position and snapshot state.

#### 4.2 Control plane

The MVP runs one connector per process. Its control plane loads and validates configuration, manages the connector lifecycle, exposes status and health endpoints, and coordinates graceful shutdown. A future multi-connector daemon must reuse the same connector runtime instead of changing connector semantics.

### 5. Proposed Workspace Boundaries

```text
rustium/
|-- crates/
|   |-- rustium-core/              # Event model, traits, runtime, errors
|   |-- rustium-config/            # Versioned config and validation
|   |-- rustium-state/             # Checkpoint API and SQLite store
|   |-- rustium-postgresql/        # pgoutput source connector
|   |-- rustium-format-json/       # Native and Debezium JSON encoders
|   |-- rustium-sink-stdout/       # Development sink
|   |-- rustium-sink-kafka/        # Kafka producer sink
|   |-- rustium-server/            # HTTP health, status, and metrics
|   `-- rustium-cli/               # rustium executable
|-- docs/
|-- examples/
|-- tests/
|-- deploy/
`-- Cargo.toml
```

Connectors and sinks are statically linked in the MVP. A dynamic plugin ABI is deferred because Rust does not provide a stable native ABI and an early plugin boundary would constrain core types prematurely.

### 6. Internal Event Model

The internal model is database-neutral and must not depend on a sink serialization format.

```rust
pub struct ChangeEvent {
    pub id: EventId,
    pub source: SourceMetadata,
    pub position: SourcePosition,
    pub transaction: Option<TransactionMetadata>,
    pub operation: Operation,
    pub before: Option<Row>,
    pub after: Option<Row>,
    pub schema: EventSchema,
    pub source_time: Option<SystemTime>,
    pub observed_time: SystemTime,
}

pub enum Operation {
    Read,
    Create,
    Update,
    Delete,
    Truncate,
    Message,
}
```

`SourcePosition` is a typed, connector-owned value with a stable serialized representation and total ordering within a source partition. PostgreSQL positions contain at least the WAL LSN and an event ordinal when multiple events share one LSN. MySQL positions will later contain binlog file/position and optional GTID state.

`EventId` is deterministic. It is derived from connector identity, source partition, source position, collection identity, and event ordinal. Replaying the same source record therefore produces the same identifier.

Rows use a typed value model rather than raw JSON so that decimals, timestamps, binary values, arrays, and null remain distinguishable before encoding. An explicit `Unavailable` value represents source values that cannot be reconstructed, such as unchanged PostgreSQL TOAST columns.

### 7. Runtime and Lifecycle

#### 7.1 Connector states

```text
CREATED -> STARTING -> SNAPSHOTTING -> STREAMING
               |             |             |
               +-------------+-------------+-> FAILED

SNAPSHOTTING <-> PAUSED <-> STREAMING
STARTING/SNAPSHOTTING/STREAMING/PAUSED -> STOPPING -> STOPPED
```

A state transition records a timestamp and a machine-readable reason. `FAILED` is terminal until an operator restart or a configured restart policy creates a new run generation.

#### 7.2 Task model

The runtime uses Tokio tasks connected by bounded channels. A connector task owns the database protocol connection. Processor tasks may parallelize CPU work only where an ordering barrier restores source order. A sink worker owns batching and delivery. A checkpoint coordinator serializes acknowledgement and state updates.

#### 7.3 Graceful shutdown

On shutdown, Rustium stops accepting new source records, drains in-flight events up to a configured timeout, flushes the sink, persists the last acknowledged checkpoint, sends final source feedback where applicable, and closes resources. If the timeout expires, the process exits without checkpointing unacknowledged events; those events are replayed on restart.

### 8. Delivery and Checkpoint Semantics

#### 8.1 Default guarantee

Rustium provides **at-least-once delivery** when the source can replay from a stored position and the sink returns a durable acknowledgement. It does not claim end-to-end exactly-once behavior.

The commit sequence is:

1. Read and decode source records.
2. Deliver an ordered batch to the sink.
3. Wait for the sink's durable acknowledgement.
4. Atomically persist the batch's highest contiguous source position.
5. Notify the source that the position may be released, when the protocol supports feedback.

If Rustium crashes after step 3 but before step 4, the acknowledged batch is replayed. If it crashes before step 3, the sink must not report success and the position is not advanced.

#### 8.2 Checkpoint record

A checkpoint contains at least:

- connector identity and run generation;
- source type and versioned source position;
- snapshot phase, snapshot anchor, and completed collections;
- last fully acknowledged transaction or batch boundary;
- compatibility-relevant configuration fingerprint;
- checkpoint schema version and update timestamp.

SQLite is the MVP state store and runs in WAL mode with transactional updates. The state directory must be placed on durable storage. A file being writable does not by itself make it durable in container deployments.

#### 8.3 Sink acknowledgement

`Sink::write(batch)` succeeds only after the sink's configured durability condition is met. For Kafka, the production default is `acks=all` with idempotent producer settings when supported. Kafka producer idempotence reduces producer retries but does not turn source-to-Kafka delivery into an exactly-once transaction.

The stdout sink acknowledges an operating-system write and is intended for development. It cannot provide a durable delivery guarantee.

### 9. PostgreSQL Connector

#### 9.1 Prerequisites

- PostgreSQL 14 or newer for the supported MVP matrix.
- `wal_level=logical` and sufficient replication slots and WAL senders.
- A login with replication permission and `SELECT` access to captured tables.
- A `pgoutput` publication containing the selected tables.
- An appropriate `REPLICA IDENTITY` when old row values are required for updates or deletes.

Rustium validates publication membership, replica identity limitations, slot state, server version, and required privileges before capture begins.

#### 9.2 Consistent initial snapshot

For a Rustium-managed new replication slot, the initial snapshot algorithm is:

1. Create the logical replication slot and export a database snapshot, obtaining a consistent WAL position.
2. Start a `REPEATABLE READ` transaction and import the exported snapshot.
3. Discover table schemas and freeze the selected collection set for this run.
4. Scan tables in deterministic primary-key order where possible and emit `Read` events.
5. Mark the final snapshot record and atomically record snapshot completion.
6. End the snapshot transaction and start logical replication from the consistent WAL position.
7. Process changes retained by the slot while the snapshot was running.

This produces a snapshot followed by all changes after its anchor without an intentional gap. Long snapshots retain WAL, so Rustium must expose retained-WAL size and fail or warn at configured thresholds.

If the process crashes before the exported snapshot completes, PostgreSQL cannot resume that transaction. The MVP restarts the initial snapshot. With an engine-owned slot, Rustium may recreate the slot only under an explicit ownership policy; it never drops a user-managed slot automatically. Re-emitted snapshot rows are duplicates under the at-least-once contract.

Tables without a primary key may be scanned, but deterministic chunking and future incremental snapshots are limited. The runtime reports this limitation.

#### 9.3 Streaming and feedback

The connector decodes `Begin`, `Relation`, `Insert`, `Update`, `Delete`, `Truncate`, `Commit`, and keepalive messages from `pgoutput`. Transactions are emitted in commit order. Relation messages update a versioned schema cache.

PostgreSQL standby status feedback advances only after the corresponding sink acknowledgement has been checkpointed. This ordering favors replay over data loss. Metrics and alerts must make replication-slot WAL retention visible when the sink is unavailable.

The values available in `before` depend on `REPLICA IDENTITY`. Unchanged TOAST values are represented internally as unavailable, never silently converted to `null`.

### 10. Filtering, Transforming, and Routing

Inclusion is evaluated before exclusion. Identifiers use explicit `database.schema.table` semantics and do not depend on locale. Invalid patterns fail configuration validation.

The MVP should keep transformations intentionally small:

- include/exclude collections;
- rename a routed topic;
- remove or mask configured fields;
- add static headers that do not contain secrets.

Transforms are deterministic and side-effect free. They cannot change the source position. A transform failure stops the connector by default.

The default Kafka topic is:

```text
{topic_prefix}.{database}.{schema}.{table}
```

Topic normalization and collision detection happen during validation. Two source collections may not silently resolve to the same topic.

### 11. Event Formats and Debezium Compatibility

The canonical contract is Rustium's versioned internal event model. Encoders provide external representations.

The MVP includes:

1. **Rustium JSON:** a versioned format that can evolve under Rustium's compatibility rules.
2. **Debezium-compatible JSON:** an encoder tested against a documented compatibility matrix.

Example target envelope:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "Alice"
  },
  "source": {
    "version": "0.1.0",
    "connector": "postgresql",
    "name": "orders-cdc",
    "ts_ms": 1784102400000,
    "snapshot": "false",
    "db": "app",
    "sequence": "[\"24023119\",\"24023128\"]",
    "schema": "public",
    "table": "customers",
    "txId": 555,
    "lsn": 24023128
  },
  "op": "c",
  "ts_ms": 1784102400142,
  "transaction": null
}
```

Operation codes are `r` for snapshot read, `c` for create, `u` for update, `d` for delete, and `t` for truncate when supported.

Compatibility is a testable boundary, not a blanket claim. The compatibility matrix must specify envelope fields, snapshot markers, decimal and temporal encodings, unavailable values, tombstones, transaction metadata, schema names, and topic names. Rustium does not claim compatibility for an item until golden tests exist for it.

Schema Registry integration, Avro, and Protobuf are post-MVP. Their eventual schema naming convention should follow the selected compatibility mode and be treated as a public API.

### 12. Configuration Contract

Configuration is YAML with a versioned top-level schema. Unknown fields are errors by default. Environment interpolation uses `${NAME}` and `${NAME:-default}`; resolved secrets are redacted from diagnostics and APIs.

Proposed MVP configuration:

```yaml
api_version: rustium.io/v1alpha1
kind: Connector

metadata:
  name: orders-cdc

source:
  type: postgresql
  hostname: localhost
  port: 5432
  database: app
  username: rustium
  password: ${PG_PASSWORD}
  publication: rustium_pub
  slot_name: rustium_orders
  slot_ownership: managed
  tables:
    include:
      - public.customers
      - public.orders

snapshot:
  mode: initial

format:
  type: debezium_json

sink:
  type: kafka
  bootstrap_servers:
    - localhost:9092
  topic_prefix: app
  acks: all

state:
  type: sqlite
  path: /var/lib/rustium/state.db

runtime:
  channel_capacity: 2048
  shutdown_timeout: 30s

server:
  bind: 127.0.0.1:8080

observability:
  log_format: json
  log_level: info
  metrics: true
```

Startup validation checks syntax, cross-field constraints, connector and sink capabilities, topic collisions, state ownership, and source prerequisites before emitting data.

Changes that alter source identity, slot, selected tables, event format, or topic routing may be checkpoint-incompatible. The CLI must show the incompatibility and require an explicit reset or migration; it must not silently reuse unsafe state.

### 13. Error Handling and Backpressure

Errors are classified as:

- **Configuration:** invalid or unsupported; fail before startup.
- **Authentication/authorization:** fail with a redacted diagnostic; retry only when configured.
- **Transient source/sink:** retry with capped exponential backoff and jitter.
- **Protocol or invariant violation:** stop and mark the connector failed.
- **Data conversion:** stop by default; explicit dead-letter or skip policies are later capabilities.
- **State corruption/incompatibility:** refuse to continue until repaired, migrated, or explicitly reset.

Retry budgets are finite or operator-configured. Status exposes the attempt count, next retry time, and last redacted error.

Bounded channels implement backpressure. For PostgreSQL, stalled delivery eventually stops consuming replication messages while keepalives and safe status feedback continue. Rustium never advances a confirmed position merely to reduce retained WAL. Operators receive lag and disk-risk alerts instead.

### 14. Management API and Observability

The initial HTTP surface is small and versioned:

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | Process liveness |
| `GET /health/ready` | Source, state, and sink readiness |
| `GET /v1/connector/status` | Lifecycle, positions, lag, and last error |
| `POST /v1/connector/pause` | Gracefully pause capture, when enabled |
| `POST /v1/connector/resume` | Resume a paused connector, when enabled |
| `GET /metrics` | Prometheus exposition |

The server binds to loopback by default. Mutating endpoints are disabled unless explicitly enabled and protected. Configuration values returned by an API are always redacted.

Core metric families include:

- `rustium_events_total{operation,table}`;
- `rustium_event_errors_total{stage}`;
- `rustium_source_lag_bytes` and `rustium_source_lag_seconds` where measurable;
- `rustium_checkpoint_lag_seconds`;
- `rustium_pipeline_queue_depth{stage}`;
- `rustium_sink_batch_seconds` and `rustium_sink_errors_total`;
- `rustium_snapshot_rows_total{table}`;
- `rustium_postgresql_retained_wal_bytes`.

Metric labels must be bounded. Row values, SQL text, credentials, and arbitrary error strings are not metric labels. Structured logs include connector name, run generation, stage, source position, and transaction identifier where safe. Row payload logging is off by default.

### 15. Security

- Use TLS for database, Kafka, and HTTP connections when configured.
- Support Kafka SASL mechanisms required by the selected client library.
- Read secrets from environment variables or mounted files; external secret-provider integration is future work.
- Redact credentials in config dumps, URLs, errors, and tracing fields.
- Bind management endpoints locally by default and require explicit remote exposure.
- Run containers as a non-root user with a read-only filesystem except for the state directory.
- Audit dependency licenses and vulnerabilities in CI.
- Publish a `SECURITY.md` before the first public binary release.

### 16. Testing and Release Gates

No feature is “ready” based only on implementation. Release gates include:

- unit tests for event models, configuration, offsets, and type conversion;
- protocol fixture tests for `pgoutput` messages;
- integration tests against supported PostgreSQL and Kafka versions;
- snapshot/stream handoff tests with concurrent writes;
- crash tests before and after sink acknowledgement and checkpoint persistence;
- golden event tests for every claimed Debezium-compatible behavior;
- fuzz tests for protocol decoders and configuration parsers;
- long-running backpressure, reconnect, and WAL-retention tests;
- reproducible benchmarks with workload, hardware, versions, and raw results published.

Performance numbers must not appear in project documentation until the benchmark code and results are public and repeatable. Correctness tests block release; performance regressions use documented budgets after a baseline exists.

### 17. Roadmap

#### Phase 0: Foundation

- Approve architecture and compatibility boundaries.
- Create the Cargo workspace, CI, contribution guide, security policy, and code of conduct.
- Define the event model, connector/sink traits, configuration schema, and SQLite migrations.

#### Phase 1: PostgreSQL vertical slice (`0.1.0` target)

- PostgreSQL `pgoutput` streaming.
- Consistent initial snapshot.
- Native JSON and Debezium-compatible JSON.
- stdout and Kafka sinks.
- SQLite checkpoints, graceful shutdown, retries, health, status, and metrics.
- Container image, examples, integration tests, and compatibility matrix.

#### Phase 2: Reliability and schema (`0.2.x` target)

- Incremental snapshots and signaling.
- Schema change propagation and Schema Registry integration.
- Dead-letter policies, heartbeats, transaction metadata, and broader PostgreSQL type coverage.
- Operational runbooks and published benchmarks.

#### Phase 3: Ecosystem (`0.3+` target)

- MySQL row-based binlog connector.
- Avro and Protobuf encoders.
- Additional durable sinks.
- Embedded runtime evaluation and multi-connector daemon evaluation.

#### Phase 4: Production scale (`1.0` criteria)

- Stable public configuration and event compatibility policies.
- Upgrade and state migration guarantees.
- Security audit, disaster recovery guidance, and production case studies.
- High-availability and Kubernetes operation based on demonstrated user needs.

Version numbers are targets, not promises. A milestone is complete only when its release gates pass.

### 18. Key Decisions

| Area | Decision | Reason |
|---|---|---|
| Runtime | Tokio | Mature async I/O ecosystem and cancellation primitives |
| Deployment | One connector per process for MVP | Fault isolation and simpler state ownership |
| Connector model | Statically linked Rust traits | Type safety without an unstable plugin ABI |
| Source | PostgreSQL `pgoutput` first | Native protocol and a focused correctness surface |
| Delivery | At-least-once | Honest guarantee across heterogeneous sources and sinks |
| State | SQLite by default | Transactional, inspectable, and dependency-light local state |
| Format | Typed internal model; JSON first | Prevent sink formats from defining core semantics |
| Compatibility | Tested encoder and matrix | Compatibility is specific and verifiable |
| Configuration | Strict, versioned YAML | Human-readable with controlled evolution |
| Errors | Stop on unknown data errors | Silent loss is worse than visible interruption |
| Telemetry | `tracing` and Prometheus | Structured diagnostics and standard metrics |

Architecture decisions that change these contracts should be recorded as ADRs under `docs/adr/`.

### 19. Open Design Questions

- Exact boundaries of transaction metadata in per-table Kafka topics.
- Slot ownership and recovery UX for externally managed PostgreSQL slots.
- Native JSON compatibility policy before `1.0`.
- Schema Registry subject naming across native and Debezium modes.
- Snapshot restart behavior for very large tables without primary keys.
- Criteria for adding a distributed state store or multi-instance coordination.

These questions do not block workspace scaffolding, but must be resolved before the affected feature is declared stable.

---

## 中文

### 1. 项目定义

Rustium 是一个使用 Rust 独立实现的开源、基于数据库日志的变更数据捕获（CDC）平台。它从数据库复制日志中读取已提交的变更，将其转换为稳定的内部事件模型，并投递到下游系统。

Rustium 的首个产品形态是以单一二进制发布的独立服务，无需 Kafka Connect 或 JVM。Kafka 是可选的 Sink，而不是运行时依赖。

**标语：** Change Data Capture, reimagined in Rust.（用 Rust 重新构想变更数据捕获。）

#### 1.1 目标

- 正确保留源端顺序和数据库位点。
- 优先保证正确性、可恢复性和资源有界，而不是只追求基准测试吞吐量。
- 提供体积小、行为明确的独立部署方式。
- 通过稳定的 Rust 接口支持 Source Connector、事件编码器、状态存储和 Sink。
- 提供经过测试的 Debezium 兼容 JSON Envelope，降低生态接入成本。
- 让健康状态、延迟、故障和日志保留风险可观测。

#### 1.2 MVP 非目标

- Kafka Connect 插件或完整的 Kafka Connect Worker 兼容性。
- 端到端 exactly-once 投递。
- 兼容 Debezium 的所有连接器、选项、数据类型或单消息转换（SMT）。
- 多个 Rustium 实例之间的高可用协调。
- 用户提供的原生或 WebAssembly 转换逻辑。
- 通过轮询业务表捕获变更。

### 2. 当前状态与 MVP 范围

仓库目前只包含设计材料和 Apache-2.0 许可证，尚无可运行的 Rustium 实现、已发布 crate、容器镜像、Helm Chart、基准测试或生产版本。在代码和测试合并之前，本文中的所有能力都只是设计目标。

第一个可用版本的范围如下：

| 能力 | MVP 目标 |
|---|---|
| Source | PostgreSQL 14+，使用 `pgoutput` 逻辑复制 |
| 捕获模式 | 一致性初始快照，然后持续流式捕获 |
| Sink | 用于开发的 stdout；用于持久投递的 Kafka |
| 事件格式 | 带版本的 JSON，以及经过测试的 Debezium 兼容 JSON 编码器 |
| 状态 | 本地 SQLite checkpoint 存储 |
| 运行时 | Tokio，每个进程运行一个连接器 |
| 管理 | CLI、健康/状态 HTTP 端点、Prometheus 指标 |
| 投递语义 | at-least-once，并提供确定性事件 ID |
| 部署 | 独立二进制和容器镜像 |

MySQL、Schema Registry 格式、增量快照、多连接器守护进程、Kubernetes Operator、嵌入式模式和更多 Sink 属于后续里程碑。

### 3. 设计原则与不变量

以下规则属于架构不变量：

1. **不丢失已确认数据。** 在配置的 Sink 持久确认某个位点覆盖的所有事件之前，不得持久化该源位点。
2. **默认 at-least-once。** Sink 确认后、checkpoint 持久化前发生崩溃可能产生重复。Rustium 必须明确说明这一点，并提供稳定事件 ID 用于去重。
3. **源端顺序权威。** 同一个源分区的事件必须按源位点排序，任何并行处理都不得改变顺序。
4. **内存有界。** 阶段之间的每个队列都必须有配置上限。队列满时向 Source 传播背压，禁止无限缓冲。
5. **能力显式声明。** Connector 或 Sink 必须声明可选能力，运行时在校验阶段拒绝不支持的配置。
6. **状态版本化。** checkpoint、事件 schema、配置 schema 和持久化元数据都必须带版本，并支持显式迁移。
7. **故障可见。** 对无法解码或投递的事件，Rustium 默认必须停止。跳过数据需要显式策略和可观测记录。
8. **保护敏感信息。** 默认情况下，凭据和行数据不得出现在日志、指标、状态 API 或 panic 输出中。

### 4. 系统架构

Rustium 将数据平面和控制平面分离。

```text
                         控制平面
       配置校验 | 生命周期 | 状态 API | 健康检查 | 指标
                                |
                                v
+-------------------------------------------------------------------+
|                           数据平面                                |
|                                                                   |
|  Source -> 解码 -> 规范化 -> 过滤 -> 转换 -> 路由                 |
|     |                                             |               |
|     |                                             v               |
|     +-> 快照 / 复制位点                         编码 -> 批处理     |
|                                                   |               |
|                                                   v               |
|                                                  Sink             |
|                                                   | 确认          |
|                                                   v               |
|                                             Checkpoint 存储        |
+-------------------------------------------------------------------+
                | Schema 缓存 | 重试策略 | 遥测 |
```

#### 4.1 数据平面

- **Source Connector：** 发现源端 schema、执行快照、读取复制流，并输出带源位点的记录。
- **Decoder：** 将 PostgreSQL `pgoutput` 等协议消息转换为强类型值。
- **Normalizer：** 生成与具体数据库无关的内部 `ChangeEvent`。
- **Filter：** 应用数据库、schema、表的包含和排除规则。
- **Transform：** 应用内置且确定性的转换。MVP 初期可以只提供字段脱敏和 Topic 路由。
- **Router：** 根据事件元数据选择目标 Topic 或 Stream。
- **Encoder：** 将内部事件序列化为 Rustium 原生版本化 JSON 或 Debezium 兼容 JSON Envelope。
- **Batcher 与 Sink：** 在不破坏顺序的前提下批量投递，并等待持久确认。
- **Checkpoint Store：** 原子持久化最后一个完全确认的源位点和快照状态。

#### 4.2 控制平面

MVP 中每个进程只运行一个连接器。控制平面负责加载和校验配置、管理连接器生命周期、暴露状态和健康端点，并协调优雅关闭。未来的多连接器守护进程必须复用同一个连接器运行时，不能改变连接器语义。

### 5. 建议的 Workspace 边界

```text
rustium/
|-- crates/
|   |-- rustium-core/              # 事件模型、trait、运行时、错误
|   |-- rustium-config/            # 版本化配置与校验
|   |-- rustium-state/             # Checkpoint API 与 SQLite 存储
|   |-- rustium-postgresql/        # pgoutput Source Connector
|   |-- rustium-format-json/       # 原生和 Debezium JSON 编码器
|   |-- rustium-sink-stdout/       # 开发用 Sink
|   |-- rustium-sink-kafka/        # Kafka Producer Sink
|   |-- rustium-server/            # HTTP 健康、状态和指标
|   `-- rustium-cli/               # rustium 可执行文件
|-- docs/
|-- examples/
|-- tests/
|-- deploy/
`-- Cargo.toml
```

MVP 中 Connector 和 Sink 采用静态链接。由于 Rust 没有稳定的原生 ABI，过早设计动态插件边界还会限制核心类型，因此动态插件 ABI 延后考虑。

### 6. 内部事件模型

内部模型与具体数据库无关，也不能依赖任何 Sink 的序列化格式。

```rust
pub struct ChangeEvent {
    pub id: EventId,
    pub source: SourceMetadata,
    pub position: SourcePosition,
    pub transaction: Option<TransactionMetadata>,
    pub operation: Operation,
    pub before: Option<Row>,
    pub after: Option<Row>,
    pub schema: EventSchema,
    pub source_time: Option<SystemTime>,
    pub observed_time: SystemTime,
}

pub enum Operation {
    Read,
    Create,
    Update,
    Delete,
    Truncate,
    Message,
}
```

`SourcePosition` 是由 Connector 管理的强类型值，拥有稳定的序列化表示，并且在单个源分区中可全序比较。PostgreSQL 位点至少包含 WAL LSN；同一 LSN 存在多个事件时还需包含事件序号。未来的 MySQL 位点包含 binlog 文件/位置和可选的 GTID 状态。

`EventId` 是确定性的，由连接器身份、源分区、源位点、集合身份和事件序号生成。因此重放同一条源记录会得到相同 ID。

行数据使用强类型值模型而不是原始 JSON，使 decimal、timestamp、binary、array 和 null 在编码前保持可区分。对于无法从源端重建的值，例如 PostgreSQL 未变化的 TOAST 列，使用显式的 `Unavailable` 值表示。

### 7. 运行时与生命周期

#### 7.1 连接器状态

```text
CREATED -> STARTING -> SNAPSHOTTING -> STREAMING
               |             |             |
               +-------------+-------------+-> FAILED

SNAPSHOTTING <-> PAUSED <-> STREAMING
STARTING/SNAPSHOTTING/STREAMING/PAUSED -> STOPPING -> STOPPED
```

每次状态转换都记录时间戳和机器可读原因。`FAILED` 是终止状态，直到操作员重启，或配置的重启策略创建新的运行代次。

#### 7.2 任务模型

运行时使用由有界 channel 连接的 Tokio task。Connector task 独占数据库协议连接。Processor task 只能在最终通过排序屏障恢复源端顺序的情况下并行执行 CPU 工作。Sink worker 负责批处理和投递，checkpoint coordinator 串行处理确认和状态更新。

#### 7.3 优雅关闭

关闭时，Rustium 停止接收新的源记录，在配置的超时时间内排空在途事件，刷新 Sink，持久化最后一个已确认 checkpoint，在适用时发送最终源端反馈，然后关闭资源。若超时，进程退出且不 checkpoint 未确认事件；这些事件会在重启后重放。

### 8. 投递与 Checkpoint 语义

#### 8.1 默认保证

当 Source 能从已存位点重放、Sink 能返回持久确认时，Rustium 提供 **at-least-once 投递**。Rustium 不宣称端到端 exactly-once。

提交顺序如下：

1. 读取并解码源记录。
2. 向 Sink 投递有序批次。
3. 等待 Sink 持久确认。
4. 原子持久化该批次最高的连续源位点。
5. 当源协议支持反馈时，通知源端该位点可以释放。

如果 Rustium 在第 3 步之后、第 4 步之前崩溃，已确认批次会被重放。如果在第 3 步之前崩溃，Sink 不得报告成功，位点也不会推进。

#### 8.2 Checkpoint 记录

Checkpoint 至少包含：

- 连接器身份和运行代次；
- Source 类型和版本化源位点；
- 快照阶段、快照锚点和已完成集合；
- 最后一个完全确认的事务或批次边界；
- 与兼容性有关的配置指纹；
- checkpoint schema 版本和更新时间。

SQLite 是 MVP 状态存储，使用 WAL 模式和事务更新。状态目录必须位于持久存储上。在容器部署中，文件可写并不等于数据持久。

#### 8.3 Sink 确认

只有达到 Sink 配置的持久性条件后，`Sink::write(batch)` 才能成功。对于 Kafka，生产默认值是 `acks=all`，并在客户端支持时启用幂等 Producer。Kafka Producer 幂等性可以减少 Producer 重试产生的重复，但不会把 Source 到 Kafka 的投递变成 exactly-once 事务。

stdout Sink 的确认只表示操作系统写入完成，仅适用于开发，不能提供持久投递保证。

### 9. PostgreSQL Connector

#### 9.1 前置条件

- MVP 支持矩阵为 PostgreSQL 14 或更高版本。
- `wal_level=logical`，并配置足够的复制槽和 WAL Sender。
- 登录用户具备复制权限和目标表的 `SELECT` 权限。
- 存在包含目标表的 `pgoutput` Publication。
- 当 update 或 delete 需要旧值时，配置合适的 `REPLICA IDENTITY`。

开始捕获前，Rustium 校验 Publication 成员、Replica Identity 限制、复制槽状态、服务器版本和所需权限。

#### 9.2 一致性初始快照

对于由 Rustium 管理的新复制槽，初始快照算法如下：

1. 创建逻辑复制槽并导出数据库快照，获得一致的 WAL 位点。
2. 启动 `REPEATABLE READ` 事务并导入该快照。
3. 发现表结构，并固定本次运行选择的集合。
4. 在可能的情况下按主键确定性排序扫描表，并输出 `Read` 事件。
5. 标记最后一条快照记录，并原子记录快照完成。
6. 结束快照事务，从一致 WAL 位点开始逻辑复制。
7. 处理快照期间由复制槽保留的变更。

这样可以先输出快照，再输出锚点之后的所有变更，并且不主动留下缺口。长时间快照会保留 WAL，因此 Rustium 必须暴露 WAL 保留量，并在达到配置阈值时告警或失败。

如果进程在导出快照完成前崩溃，PostgreSQL 无法恢复该事务。MVP 会重新执行完整初始快照。对于引擎拥有的复制槽，Rustium 只能在显式所有权策略允许时重建；绝不自动删除用户管理的复制槽。重新输出的快照行属于 at-least-once 契约下的重复数据。

没有主键的表可以扫描，但确定性分块和未来增量快照能力受限，运行时必须报告这一限制。

#### 9.3 流式捕获与反馈

Connector 解码 `pgoutput` 的 `Begin`、`Relation`、`Insert`、`Update`、`Delete`、`Truncate`、`Commit` 和 keepalive 消息。事务按提交顺序输出，Relation 消息用于更新版本化 schema 缓存。

只有相应 Sink 确认已写入 checkpoint 后，PostgreSQL standby status feedback 才推进位点。这一顺序优先选择重放而不是数据丢失。当 Sink 不可用时，指标和告警必须展示复制槽保留 WAL 的风险。

`before` 中可用的值取决于 `REPLICA IDENTITY`。未变化的 TOAST 值在内部表示为 unavailable，绝不能静默转换为 `null`。

### 10. 过滤、转换与路由

先计算包含规则，再计算排除规则。标识符采用明确的 `database.schema.table` 语义，且不依赖 locale。无效模式会导致配置校验失败。

MVP 应刻意限制转换范围：

- 包含或排除集合；
- 重命名路由 Topic；
- 删除或脱敏配置的字段；
- 添加不包含秘密的静态 header。

转换必须是确定性且无副作用的，不能修改源位点。转换失败时默认停止连接器。

默认 Kafka Topic 为：

```text
{topic_prefix}.{database}.{schema}.{table}
```

Topic 规范化和冲突检测在配置校验时完成。两个源集合不得静默映射到同一个 Topic。

### 11. 事件格式与 Debezium 兼容性

Rustium 的规范契约是版本化内部事件模型，Encoder 提供外部表示。

MVP 包含：

1. **Rustium JSON：** 在 Rustium 兼容性规则下演进的版本化格式。
2. **Debezium 兼容 JSON：** 通过明确兼容性矩阵测试的编码器。

目标 Envelope 示例：

```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "Alice"
  },
  "source": {
    "version": "0.1.0",
    "connector": "postgresql",
    "name": "orders-cdc",
    "ts_ms": 1784102400000,
    "snapshot": "false",
    "db": "app",
    "sequence": "[\"24023119\",\"24023128\"]",
    "schema": "public",
    "table": "customers",
    "txId": 555,
    "lsn": 24023128
  },
  "op": "c",
  "ts_ms": 1784102400142,
  "transaction": null
}
```

操作码包括：快照读取 `r`、创建 `c`、更新 `u`、删除 `d`，以及在支持时用于 truncate 的 `t`。

兼容性是可测试的边界，而不是笼统声明。兼容性矩阵必须明确 Envelope 字段、快照标记、decimal 和时间编码、不可用值、tombstone、事务元数据、schema 名称和 Topic 名称。只有存在 golden test 的行为才能宣称兼容。

Schema Registry、Avro 和 Protobuf 属于 MVP 之后的能力。未来的 schema 命名约定应遵循所选兼容模式，并被视为公共 API。

### 12. 配置契约

配置采用带顶层版本的 YAML schema。默认情况下，未知字段属于错误。环境变量插值支持 `${NAME}` 和 `${NAME:-default}`；解析后的秘密会从诊断和 API 中脱敏。

建议的 MVP 配置：

```yaml
api_version: rustium.io/v1alpha1
kind: Connector

metadata:
  name: orders-cdc

source:
  type: postgresql
  hostname: localhost
  port: 5432
  database: app
  username: rustium
  password: ${PG_PASSWORD}
  publication: rustium_pub
  slot_name: rustium_orders
  slot_ownership: managed
  tables:
    include:
      - public.customers
      - public.orders

snapshot:
  mode: initial

format:
  type: debezium_json

sink:
  type: kafka
  bootstrap_servers:
    - localhost:9092
  topic_prefix: app
  acks: all

state:
  type: sqlite
  path: /var/lib/rustium/state.db

runtime:
  channel_capacity: 2048
  shutdown_timeout: 30s

server:
  bind: 127.0.0.1:8080

observability:
  log_format: json
  log_level: info
  metrics: true
```

在输出任何数据前，启动校验会检查语法、字段间约束、Connector 和 Sink 能力、Topic 冲突、状态所有权以及源端前置条件。

修改 Source 身份、复制槽、所选表、事件格式或 Topic 路由可能与 checkpoint 不兼容。CLI 必须显示不兼容原因，并要求显式 reset 或 migration；不得静默复用不安全状态。

### 13. 错误处理与背压

错误分类如下：

- **配置错误：** 无效或不支持，启动前失败。
- **认证/授权错误：** 使用脱敏诊断失败；只有显式配置后才重试。
- **Source/Sink 瞬时错误：** 使用有上限的指数退避和抖动重试。
- **协议或不变量错误：** 停止并将连接器标记为失败。
- **数据转换错误：** 默认停止；死信或跳过策略属于后续能力。
- **状态损坏/不兼容：** 在修复、迁移或显式 reset 之前拒绝继续运行。

重试预算必须有限或由操作员配置。状态接口暴露尝试次数、下次重试时间和最后一个已脱敏错误。

有界 channel 用于实现背压。对 PostgreSQL 而言，投递阻塞最终会暂停消费复制消息，同时继续处理 keepalive 和安全的状态反馈。Rustium 绝不为了减少 WAL 保留量而推进已确认位点，而是通过延迟和磁盘风险告警通知操作员。

### 14. 管理 API 与可观测性

初始 HTTP 接口保持小而且版本化：

| 端点 | 用途 |
|---|---|
| `GET /health/live` | 进程存活状态 |
| `GET /health/ready` | Source、状态存储和 Sink 就绪状态 |
| `GET /v1/connector/status` | 生命周期、位点、延迟和最后错误 |
| `POST /v1/connector/pause` | 启用时优雅暂停捕获 |
| `POST /v1/connector/resume` | 启用时恢复已暂停连接器 |
| `GET /metrics` | Prometheus 指标输出 |

服务器默认绑定 loopback。修改状态的端点只有在显式启用并受到保护时才可用。API 返回的配置值始终经过脱敏。

核心指标包括：

- `rustium_events_total{operation,table}`；
- `rustium_event_errors_total{stage}`；
- 可测量时的 `rustium_source_lag_bytes` 和 `rustium_source_lag_seconds`；
- `rustium_checkpoint_lag_seconds`；
- `rustium_pipeline_queue_depth{stage}`；
- `rustium_sink_batch_seconds` 和 `rustium_sink_errors_total`；
- `rustium_snapshot_rows_total{table}`；
- `rustium_postgresql_retained_wal_bytes`。

指标 label 必须有界。行值、SQL 文本、凭据和任意错误字符串不能作为指标 label。结构化日志在安全时包含连接器名称、运行代次、阶段、源位点和事务 ID。默认关闭行数据日志。

### 15. 安全

- 配置后为数据库、Kafka 和 HTTP 连接启用 TLS。
- 支持所选 Kafka 客户端库提供的必要 SASL 机制。
- 从环境变量或挂载文件读取秘密；外部 Secret Provider 集成属于未来工作。
- 在配置输出、URL、错误和 tracing 字段中脱敏凭据。
- 管理端点默认只绑定本机，远程暴露必须显式开启。
- 容器以非 root 用户运行，除状态目录外使用只读文件系统。
- 在 CI 中审计依赖许可证和漏洞。
- 在首次公开二进制发布前提供 `SECURITY.md`。

### 16. 测试与发布门槛

功能不能仅因代码完成就标记为“可用”。发布门槛包括：

- 事件模型、配置、位点和类型转换的单元测试；
- `pgoutput` 消息的协议 fixture 测试；
- 针对支持的 PostgreSQL 和 Kafka 版本的集成测试；
- 存在并发写入时的快照/流式切换测试；
- Sink 确认和 checkpoint 持久化前后的崩溃测试；
- 每项 Debezium 兼容声明对应的 golden event 测试；
- 协议解码器和配置解析器的 fuzz 测试；
- 长时间背压、重连和 WAL 保留测试；
- 发布工作负载、硬件、版本和原始结果的可复现基准测试。

在基准代码和结果公开且可复现之前，项目文档不得出现性能数字。正确性测试阻塞发布；建立基线后，性能回归使用公开的预算判断。

### 17. 路线图

#### Phase 0：基础

- 审批架构和兼容性边界。
- 创建 Cargo workspace、CI、贡献指南、安全策略和行为准则。
- 定义事件模型、Connector/Sink trait、配置 schema 和 SQLite migration。

#### Phase 1：PostgreSQL 垂直切片（目标 `0.1.0`）

- PostgreSQL `pgoutput` 流式捕获。
- 一致性初始快照。
- 原生 JSON 和 Debezium 兼容 JSON。
- stdout 和 Kafka Sink。
- SQLite checkpoint、优雅关闭、重试、健康检查、状态和指标。
- 容器镜像、示例、集成测试和兼容性矩阵。

#### Phase 2：可靠性与 Schema（目标 `0.2.x`）

- 增量快照和信号机制。
- Schema 变更传播和 Schema Registry 集成。
- 死信策略、心跳、事务元数据和更完整的 PostgreSQL 类型覆盖。
- 运维手册和公开基准测试。

#### Phase 3：生态（目标 `0.3+`）

- MySQL row-based binlog Connector。
- Avro 和 Protobuf Encoder。
- 更多持久化 Sink。
- 评估嵌入式运行时和多连接器守护进程。

#### Phase 4：生产规模（`1.0` 标准）

- 稳定的公开配置和事件兼容性策略。
- 升级和状态迁移保证。
- 安全审计、灾难恢复指南和生产案例。
- 根据真实用户需求实现高可用和 Kubernetes 运维能力。

版本号是目标，不是承诺。只有通过发布门槛后，里程碑才算完成。

### 18. 关键决策

| 领域 | 决策 | 原因 |
|---|---|---|
| 运行时 | Tokio | 成熟的异步 I/O 生态和取消机制 |
| 部署 | MVP 每进程一个连接器 | 故障隔离和更简单的状态所有权 |
| Connector 模型 | 静态链接的 Rust trait | 在不依赖不稳定插件 ABI 的情况下提供类型安全 |
| Source | 优先 PostgreSQL `pgoutput` | 原生协议和聚焦的正确性范围 |
| 投递 | at-least-once | 面对异构 Source 和 Sink 时诚实可实现的保证 |
| 状态 | 默认 SQLite | 支持事务、可检查且本地依赖少 |
| 格式 | 强类型内部模型，JSON 优先 | 避免由 Sink 格式定义核心语义 |
| 兼容性 | 测试过的 Encoder 和矩阵 | 兼容范围具体且可验证 |
| 配置 | 严格、版本化 YAML | 便于阅读且可以受控演进 |
| 错误 | 未知数据错误默认停止 | 静默丢失比可见中断更危险 |
| 遥测 | `tracing` 和 Prometheus | 结构化诊断和标准指标 |

改变这些契约的架构决策应以 ADR 形式记录在 `docs/adr/`。

### 19. 待决设计问题

- 在按表分 Topic 时，事务元数据的准确边界。
- 外部管理 PostgreSQL 复制槽时的所有权和恢复体验。
- `1.0` 之前原生 JSON 的兼容策略。
- 原生模式和 Debezium 模式下的 Schema Registry subject 命名。
- 无主键超大表的快照重启行为。
- 引入分布式状态存储或多实例协调的判断标准。

这些问题不阻塞 workspace 初始化，但必须在受影响功能被标记为稳定前解决。
