# Rustium Architecture and Design

> Status: Implemented alpha baseline
> Document version: 0.2
> Last updated: 2026-07-15

## Language Policy

Rustium documentation is published in complete English first and complete Simplified Chinese second. English is normative when translations differ. Code, configuration keys, APIs, logs, issues, and commit messages use English.

Rustium 文档先提供完整英文，再提供完整简体中文。两种文本不一致时以英文为准。代码、配置键、API、日志、Issue 和提交信息使用英文。

---

## English

### 1. Product Definition

Rustium is an independently implemented, open-source, log-based Change Data Capture platform written in Rust. It reads committed database changes, converts them into a database-neutral typed event, and delivers ordered records to downstream sinks.

Rustium is a standalone service. It does not require Kafka Connect or a JVM. Kafka is a sink, not a runtime dependency.

Rustium uses the latest Debezium architecture, event behavior, and configuration names as compatibility references. Rustium is not a Debezium fork and does not copy Debezium Java source code.

#### 1.1 Connector priority

Connector work follows this strict order:

1. PostgreSQL
2. MySQL
3. SQL Server

Other databases remain out of scope until these three connectors pass their correctness, recovery, type-coverage, and operational release gates.

#### 1.2 Goals

- Preserve source ordering and replayable source positions.
- Never checkpoint data that the sink has not acknowledged.
- Bound memory and propagate backpressure to the source.
- Provide deterministic event identifiers for at-least-once deduplication.
- Keep source protocols, internal events, formats, sinks, and state ownership separate.
- Prefer Debezium property names and event fields where compatibility is practical.
- Expose lifecycle, position, queue, delivery, and failure state operationally.

#### 1.3 Non-goals before 1.0

- End-to-end exactly-once delivery across arbitrary sinks.
- Kafka Connect worker or SMT compatibility.
- Dynamic native plugin ABI.
- Multi-instance high-availability coordination.
- Polling application tables instead of using database CDC facilities.
- Adding lower-priority database connectors before the first three are complete.

### 2. Current Implementation

The workspace contains a runnable alpha service.

| Component | State |
|---|---|
| Core event/position/runtime traits | Implemented |
| Bounded Tokio runtime | Implemented |
| SQLite checkpoint v2 and connector state | Implemented; version 1 JSON remains readable |
| Native and Debezium JSON | Implemented |
| stdout and Kafka sinks | Implemented |
| PostgreSQL source | Implemented; recovery, heartbeat/action-query, writable/read-only incremental-snapshot, and core type-matrix gates pass with PostgreSQL 17 |
| MySQL source | Implemented; Docker and external destructive-DDL restart gates pass with MySQL 8.4 |
| SQL Server source | Implemented and externally integration-tested with SQL Server 2022 Developer CU25 |
| CLI and HTTP management | Implemented |
| Published image/package/Helm chart | Not available |

Alpha means the source and delivery contracts are functional, but configuration and persisted state are not stable yet.

### 3. Architectural Invariants

1. **No acknowledged data loss.** A source position is persisted only after all covered events receive the configured sink acknowledgement.
2. **At-least-once delivery.** A crash after sink acknowledgement but before checkpoint persistence can replay events.
3. **Source order is authoritative.** Records from one connector remain ordered by their typed source position.
4. **Bounded memory.** Every source-to-runtime queue has a configured capacity.
5. **Explicit unavailable values.** Missing TOAST or minimal-row-image values are not converted to null.
6. **Versioned state.** Checkpoints and configuration have explicit schema versions and fingerprints.
7. **Visible failures.** Protocol, conversion, state, and sink errors stop the connector by default.
8. **Secret redaction.** Credentials and row payloads do not appear in status or metrics.
9. **No silent compatibility claims.** Unsupported Debezium properties generate warnings or validation errors.

### 4. Workspace Boundaries

```text
crates/
|-- rustium-core/          typed events, positions, traits, runtime
|-- rustium-config/        YAML, properties parsing, validation, fingerprints
|-- rustium-state/         versioned SQLite checkpoints
|-- rustium-postgresql/    pgoutput source
|-- rustium-mysql/         row-binlog source
|-- rustium-sqlserver/     SQL Server CDC source
|-- rustium-format-json/   native and Debezium JSON
|-- rustium-sink-stdout/   development sink
|-- rustium-sink-kafka/    durable Kafka producer sink
|-- rustium-server/        health, status, lifecycle, metrics
`-- rustium-cli/           rustium binary
```

Connectors and sinks are statically linked behind Rust traits. A stable dynamic plugin ABI is deferred.

### 5. Data Plane

```text
Source protocol
    |
    v
Decode and normalize
    |
    v
bounded SourceRecord channel
    |
    v
encode ordered ChangeEvent records
    |
    v
write ordered DeliveryBatch to sink
    |
    v
persist acknowledged position + connector state
    |
    v
acknowledge source protocol where supported
```

The current runtime uses one source task and one ordered delivery coordinator. This intentionally limits parallelism until ordering barriers and partition contracts are explicit.

#### 5.1 Commit sequence

For every non-empty or position-only batch:

1. Encode all data records.
2. Call `Sink::write` and wait for its acknowledgement.
3. Save the source position and versioned connector state in one SQLite checkpoint transaction.
4. Publish the saved position on the source acknowledgement channel.
5. Update status counters.

Snapshot-complete and transaction-commit boundaries force a flush. Size and time thresholds can also flush inside a transaction; connector positions must therefore support exact mid-transaction replay.

#### 5.2 Shutdown

Cancellation stops source reads, flushes pending events, persists only acknowledged positions, flushes and closes the sink, then closes protocol resources. A timeout fails shutdown without checkpointing unacknowledged records.

### 6. Internal Event Model

`ChangeEvent` contains:

- deterministic `EventId`;
- typed `SourcePosition`;
- connector/database/schema/table metadata;
- optional transaction ID and ordering;
- operation: read, create, update, delete, truncate, or message;
- optional before and after rows;
- versioned field schema;
- source and observed timestamps.

`DataValue` distinguishes null, boolean, signed/unsigned integers, decimal text, float, string, bytes, date, time, timestamp, UUID, JSON, array, and unavailable.

Event IDs hash connector name, source partition, typed position, collection, and ordinal. Replaying the same source row produces the same ID.

### 7. Source Positions and Checkpoints

#### 7.1 PostgreSQL

The position contains WAL LSN, optional commit LSN, transaction ID, same-LSN event serial, and snapshot state. Replication feedback advances only after the matching checkpoint is saved.

#### 7.2 MySQL

The position contains binlog filename, a replayable event anchor, GTID, source server ID, row/event serial, and snapshot state.

For row events, the checkpoint anchor is the preceding `TABLE_MAP_EVENT` start position. This is deliberate. Starting at only the row-event end can lose remaining rows; starting at the row-event body can omit metadata required to decode it. Replaying from the table-map anchor plus a deterministic row ordinal allows Rustium to skip already acknowledged rows and continue inside a multi-row event.

#### 7.3 State compatibility

The configuration fingerprint covers semantic source identity, selected tables, snapshot behavior, format, and routing. Passwords and operational tuning do not affect the fingerprint. An incompatible checkpoint is rejected until reset or migrated.

Checkpoint schema version 2 adds an optional connector-state envelope with a format identifier, payload version, and JSON payload. The field is additive, so version 1 checkpoint JSON without connector state remains readable. The runtime carries the latest state across ordinary data records and persists it atomically with the highest sink-acknowledged source position.

Deserialization compatibility does not invent missing history. Completed PostgreSQL and MySQL version 1 checkpoints have no safe historical schema baseline, so those connectors reject resume and require a checkpoint reset plus one new initial snapshot. SQL Server can continue reading its version 1 checkpoints because its current recovery path does not depend on connector state.

### 8. Configuration

Rustium supports:

- strict, versioned YAML with unknown-field rejection;
- `${NAME}` and `${NAME:-default}` environment interpolation;
- Debezium-style Java `.properties` parsing;
- full-match regular expressions for database/schema/table filters;
- compatibility warnings for recognized but unsupported properties.

The native root schema is `api_version: rustium.io/v1alpha1`, `kind: Connector`.

Debezium names are preferred for migration. Common mappings include `name`, `connector.class`, `topic.prefix`, `database.*`, `table.include.list`, `table.exclude.list`, `snapshot.mode`, `snapshot.fetch.size`, `tombstones.on.delete`, `max.queue.size`, `max.batch.size`, and `poll.interval.ms`.

Rustium-only state, sink, server, logging, and producer extensions use `rustium.*` in properties files.

### 9. PostgreSQL Connector

#### 9.1 Prerequisites

- PostgreSQL 14 or newer.
- `wal_level=logical`.
- Existing `pgoutput` publication.
- Replication and table-read permissions.
- A unique replication slot.

#### 9.2 Snapshot handoff

For a managed initial snapshot:

1. Prepare or recreate an inactive managed slot.
2. create the logical slot with an exported snapshot;
3. open a repeatable-read, read-only SQL transaction;
4. import the exported snapshot;
5. discover selected publication tables and schemas;
6. scan each table in bounded pages;
7. emit snapshot-complete with the baseline schema history at the slot anchor LSN;
8. start `pgoutput` from that LSN.

The slot retains changes committed during the snapshot, so no handoff gap exists.

#### 9.3 Streaming

`pg_walstream` provides replication transport and protocol parsing. Rustium converts begin, insert, update, delete, truncate, commit, and streamed-transaction events. Same-LSN ordinals make positions total and replayable. Missing unchanged TOAST columns become `Unavailable`.

The PostgreSQL connector-state payload persists the snapshot table layout plus each column's type OID and typmod. On restart it restores that baseline before opening the slot. Each `Relation` message then supplies the historical column names, order, type identity, and key flags for the following row events. Exact catalog matches supplement type names and optionality without replacing the WAL layout. Changed schemas increment their version and the updated state is attached to the next checkpointable source record.

PostgreSQL does not put original DDL or column nullability/default metadata into `Relation`. If a transient historical column no longer exists in the current catalog and was not present in the checkpoint baseline, Rustium resolves its type from OID/typmod and conservatively marks it optional. This preserves row decoding and ordering without claiming unavailable metadata.

Snapshot queries project every selected column through PostgreSQL's `::text` output function instead of routing rows through JSON. Snapshot values and `pgoutput` values therefore share one converter and preserve numeric scale/precision, bytea, JSON text, temporal formatting, and array syntax identically. The array parser handles quoted and escaped elements, SQL NULL versus the string `"NULL"`, explicit lower bounds, nested dimensions, and type-aware scalar conversion. Malformed array text is preserved as a string instead of being partially decoded.

`heartbeat.interval.ms` defaults to zero. A positive interval emits a visible heartbeat at the latest SourceRecord position already admitted to the bounded queue, or at the completed snapshot anchor before the first streaming event. No heartbeat is emitted when no source position exists. `heartbeat.action.query` optionally executes first on a reused ordinary SQL connection at each interval. Its WAL is not treated as progress until `pgoutput` returns it, and query failures stop the source with the database error. Heartbeat-table changes can be included in the publication but excluded by the table selector; their transaction commit still advances the safe WAL position without exposing a business-table event.

#### 9.4 Source signaling and incremental snapshots

The signaling implementation follows Debezium's source-table channel. `signal.data.collection` identifies one schema-qualified table with exactly three text-compatible columns named `id`, `type`, and `data`; the table must be in the publication and is always filtered from business snapshots and events. `signal.enabled.channels` accepts `source`. An `execute-snapshot` record accepts `type=incremental`, fully matched regular expressions in `data-collections`, and `additional-conditions` entries containing a case-insensitive collection expression plus a SQL filter. A non-empty surrogate key is rejected until its separate ordering and event-key semantics are implemented.

The controller currently implements `incremental.snapshot.watermarking.strategy=insert_insert`. For each primary-key-ordered chunk it commits an open watermark, captures the current maximum key on the first chunk, reads at most `incremental.snapshot.chunk.size` rows through the shared text converter, and commits a close watermark. WAL creates, updates, and deletes between the watermarks remove matching primary keys from the in-memory window. Rows that remain at close are emitted as read events with the Debezium `incremental` snapshot marker before the close commit boundary.

With `read.only=true`, the chunk connection does not insert watermark rows. It allocates a transaction ID once per chunk, captures `pg_current_snapshot()` before and after the bounded query, and retains the snapshot's `xmin`, `xmax`, and in-progress XID set. WAL transaction IDs open the window at the low `xmin`; the window closes only after the high watermark is visible or the maximum transaction that was in progress has committed. The commit event that closes the window is also the checkpoint boundary. The same transaction ID can safely close subsequent chunks immediately when the watermarks show that no older transaction remains. Restart discards transient watermarks and rereads the current key range.

Connector-state format version 3 stores the signal ID, expanded collections, per-collection conditions, collection index, last key, maximum key, chunk sequence, and pause state. The close commit checkpoints the advanced state atomically with delivered rows. A crash before that checkpoint re-reads the same bounded chunk, which permits duplicates but prevents a gap; a restart after it starts at the next key. Version 1 and 2 schema-history payloads remain readable with defaults for the new fields. The in-memory window is deliberately not persisted.

`pause-snapshot` prevents the next chunk from being prepared after the current close boundary. The paused flag is checkpointed, so restart remains paused. `resume-snapshot` schedules the next chunk after its own signal transaction commits. `stop-snapshot` clears all progress when `data-collections` is absent, or removes only collections matched by its fully matched expressions; stopping the current collection resets its key boundaries and advances safely after the control transaction. Unknown and out-of-order watermark IDs are ignored.

#### 9.5 Remaining PostgreSQL gates

- surrogate-key incremental-snapshot signaling and non-source signal input channels;
- online schema changes during incremental snapshots;
- extension-type and broader failure fixtures;
- Kafka end-to-end recovery tests.

### 10. MySQL Connector

#### 10.1 Prerequisites

- MySQL 8.0 or newer.
- `log_bin=ON` and `binlog_format=ROW`.
- a connector `database.server.id` distinct from the source server ID;
- `SELECT`, `RELOAD`, `FLUSH_TABLES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT` privileges.

#### 10.2 Snapshot handoff

1. Acquire `FLUSH TABLES WITH READ LOCK` on a lock connection.
2. Start a repeatable-read consistent transaction on a snapshot connection.
3. capture current binlog filename, position, GTID state, and source server ID;
4. release the global read lock;
5. discover base-table schemas for the captured databases inside the snapshot;
6. scan selected tables in bounded pages, ordered by primary key when present;
7. commit the snapshot and emit snapshot-complete with the baseline schema history;
8. start binlog streaming from the captured position.

The read lock is held only for transaction establishment and coordinate capture, not for the table scan.

#### 10.3 Streaming

`mysql_async` provides replication transport and binlog parsing. Rustium handles rotate, table-map, GTID, query, write-rows, update-rows, delete-rows, and XID events.

Row events produce typed before/after images. FULL, MINIMAL, and NOBLOB images are accepted; omitted values are explicit `Unavailable` values. GTID transactions carry total order and collection order.

MySQL schema history is a versioned connector-state payload containing the ordered field model for selected tables in captured databases. Snapshot completion establishes the baseline. On restart, Rustium restores that baseline from the checkpoint before opening the binlog, then parses and applies selected-table DDL query events in source order. The updated schema state is attached to the DDL transaction boundary, so sink acknowledgement, source position, and schema state advance together. Discovery and DDL application ignore unselected tables so independent connectors sharing one database cannot mutate each other's history.

The current `sqlparser`-based MySQL DDL path handles `CREATE TABLE`, `ALTER TABLE` add/drop/rename/modify/change column operations, primary-key changes, `DROP TABLE`, `RENAME TABLE`, and schema-neutral `TRUNCATE TABLE`. Parsing or state-application failures stop the connector by default. `schema.history.internal.skip.unparseable.ddl=true` matches Debezium's opt-in skip behavior and logs the metadata-risk warning.

When `connect.keep.alive=true`, an ended or failed binlog stream is reopened from the last SourceRecord successfully sent into the bounded runtime queue. The rewind restores the binlog filename, table-map anchor, GTID transaction counters, row ordinal, and replay filter so completed records are skipped deterministically. `connect.keep.alive.interval.ms` controls the delay between attempts; `rustium.source.reconnect.max.attempts` is Rustium's finite extension and defaults to 10. The attempt number and last error are logged, and the budget resets after new source progress.

`heartbeat.interval.ms` defaults to zero and disables visible heartbeats. A positive interval emits a heartbeat from the latest safe streaming position, so its sink acknowledgement and checkpoint follow the normal at-least-once sequence without inventing binlog progress. Debezium JSON uses the `serverName` key and `ts_ms` value. Topic resolution prefers `topic.heartbeat.name`; otherwise it joins `topic.heartbeat.prefix` (or legacy `heartbeat.topics.prefix`) with `topic.prefix`.

#### 10.4 TLS modes

- `disabled`: plaintext only.
- `preferred`: try encrypted transport, then fall back to plaintext.
- `required`: encrypted transport without CA or hostname verification.
- `verify_ca`: encrypted transport with CA verification, without hostname verification.
- `verify_identity`: encrypted transport with CA and hostname verification.

Custom Debezium truststore/keystore mapping is not implemented yet.

#### 10.5 Verified recovery

The MySQL 8.4 Docker gate covers:

- two-row snapshot and snapshot completion;
- one transaction with multi-row insert, update, and delete;
- transaction order 1 through 5;
- forced termination of the active binlog dump connection;
- reconnect from the last completed transaction without duplicate events;
- capture of the first transaction written while the connection is down;
- checkpoint stop before an old-schema row, destructive drop/add-column DDL, and a new-schema row;
- restart after the database already exposes the final schema, with correct old-schema decoding, DDL state checkpointing, and new-schema decoding.

Unit gates cover checkpoint/state atomicity, version 1/2 checkpoint compatibility, schema-history serialization, incremental progress/control and PostgreSQL snapshot parsing, replay-state rewind, scalar conversions, heartbeat encoding, selected-table isolation, and create/alter/drop/rename DDL application. The external PostgreSQL 17 gate verifies periodic heartbeat emission, successful `heartbeat.action.query`, heartbeat-table filtering, source-signaled chunking, checkpoint restart, additional conditions, concurrent-update deduplication, pause/resume/scoped-stop control, read-only transaction watermarks under a held update, restricted table permissions, zero connector watermark writes, completion cleanup, and signal-table isolation. The external MySQL 8.4 gate verifies periodic heartbeat emission during an idle stream alongside destructive-DDL recovery.

#### 10.6 Remaining MySQL gates

- GTID source include/exclude filters;
- signaling records;
- incremental snapshots;
- custom trust/key stores and wider type fixtures;
- exact reconstruction of server-side partial JSON diffs.

### 11. SQL Server Connector

The SQL Server connector is implemented with SQL Server CDC, not application-table polling. It currently accepts one database per connector, one active capture instance per selected table, and `data.query.mode=direct`.

Mapped Debezium-compatible inputs include `database.hostname`, `database.port`, `database.user`, `database.password`, `database.names`, `database.encrypt`, `database.trustServerCertificate`, `table.include.list`, `table.exclude.list`, `snapshot.mode`, `snapshot.isolation.mode`, `data.query.mode`, `streaming.fetch.size`, `max.queue.size`, `max.batch.size`, and `poll.interval.ms`.

The implemented flow is:

1. validate SQL Server version, database CDC state, capture instances, and CDC LSN availability;
2. capture `sys.fn_cdc_get_max_lsn()` as the snapshot handoff point;
3. read selected source tables in a consistent snapshot transaction;
4. poll each selected capture instance's `cdc` change table directly;
5. pair operation 3 and 4 rows into update before/after images;
6. merge tables by commit LSN and sequence value;
7. checkpoint only after sink acknowledgement;
8. detect CDC cleanup that has removed the required restart LSN and fail visibly.

Update operation 3 and 4 rows are paired into one logical update event. Queries are globally ordered and bounded by `streaming.fetch.size`; transaction boundaries use commit LSN. Mid-transaction recovery replays from the commit LSN, counts skipped records, and preserves transaction ordering.

Multiple database names require explicit partition-aware ordering and checkpoint ownership. Rustium rejects them until that contract is tested. The external gate has verified snapshot handoff, ordered CDC streaming, commit boundaries, checkpoint restart without snapshot replay, and resource cleanup against SQL Server 2022 Developer RTM-CU25. Container portability, retention-failure, wider type, and concurrency fixtures remain open.

### 12. Formats and Sinks

The native JSON encoder exposes the full typed source position and event schema. The Debezium JSON encoder emits `before`, `after`, `source`, `op`, `ts_ms`, and transaction metadata, plus connector-specific position fields.

With `tombstones.on.delete=true`, which is the Debezium-compatible default, encoding one delete produces an ordered pair in the same delivery batch: the delete envelope followed by the same key with a null payload. The tombstone has its own deterministic derived event ID. Sink success, checkpoint persistence, and source acknowledgement cover both records together. Native YAML exposes the same setting as `format.tombstones_on_delete`; the native Rustium JSON format does not emit tombstones.

stdout is best-effort and intended for development; it prints tombstones as a `null` line. Kafka uses `librdkafka`, sends tombstones as a true null value, supports configurable producer properties and durable acknowledgements, and enables idempotence when acknowledgement settings allow it. Producer idempotence does not make source-to-Kafka delivery exactly-once.

### 13. Control Plane and Observability

Lifecycle states are created, starting, snapshotting, streaming, paused, failed, stopping, and stopped. The currently implemented API is:

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | process liveness |
| `GET /health/ready` | connector readiness |
| `GET /v1/connector/status` | lifecycle, position, checkpoint, queue, counters |
| `POST /v1/connector/stop` | graceful stop when enabled |
| `GET /metrics` | Prometheus metrics |

Metrics currently expose connector state, delivered events, failed events, and pipeline queue depth. Database lag and retained-log metrics remain release work.

### 14. Error, Security, and Resource Policy

- Configuration and capability errors fail before capture.
- Unknown protocol/data errors stop the connector.
- General source/sink retry orchestration is not implemented yet. MySQL has connector-local, finite, logged binlog reconnect handling.
- Queues are bounded and backpressure blocks source output.
- Database and Kafka TLS are configuration-controlled.
- The management server binds to loopback by default.
- Mutating HTTP endpoints are disabled by default.
- Secrets are interpolated at load time and excluded from status and semantic fingerprints.

### 15. Testing and Release Gates

A connector is not complete because it compiles. Required gates include:

- unit tests for positions, conversions, filters, configuration, and event envelopes;
- real database snapshot/stream tests;
- concurrent-write snapshot handoff tests;
- crash/restart tests inside transactions and multi-row events;
- sink acknowledgement/checkpoint ordering tests;
- cleanup/retention failure tests;
- DDL and historical schema tests;
- golden Debezium compatibility fixtures;
- long-running backpressure and reconnect tests;
- Kafka end-to-end durability tests.

The ignored MySQL Docker test is runnable with:

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

The gate forcibly kills the active replication connection and verifies reconnect from the last safe source position.

The ignored external MySQL test reads both admin and CDC connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_MYSQL_TEST_HOST=mysql.example.com \
RUSTIUM_MYSQL_TEST_PORT=3306 \
RUSTIUM_MYSQL_TEST_ADMIN_USER=root \
RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_USER=cdc \
RUSTIUM_MYSQL_TEST_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo \
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

This gate creates isolated selected tables with the admin account and runs concurrent CDC sources. It verifies snapshot/replication, checkpointed schema versions 1 and 2 across destructive DDL, selected-table history isolation, periodic idle-stream heartbeats from a safe binlog position, and cleanup. It has passed against MySQL 8.4 with row binlog and GTID enabled.

The ignored external PostgreSQL test reads connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com \
RUSTIUM_POSTGRES_TEST_PORT=5432 \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me' \
RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo \
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture
```

These tests create isolated business-table/signal-table/publication/slot/role names and verify snapshot rows, ordered transactional create/update/delete events, checkpoint stop, an old-schema row, destructive drop/add-column DDL, a new-schema row, historical `Relation` replay with schema versions 1 and 2, restart without snapshot replay, periodic heartbeat records at a safe WAL position, `heartbeat.action.query`, heartbeat-table filtering, checkpointed incremental-snapshot resume, filtered chunks, concurrent-update deduplication, pause/resume/scoped-stop control, read-only transaction watermarks with a held update, zero watermark writes under restricted permissions, signal-table isolation, and identical snapshot/WAL conversion across the core PostgreSQL type matrix. They pass against PostgreSQL 17 with `wal_level=logical`.

The ignored external SQL Server test reads connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com \
RUSTIUM_SQLSERVER_TEST_PORT=1433 \
RUSTIUM_SQLSERVER_TEST_USER=sa \
RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me' \
RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo \
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

This gate creates isolated table/capture-instance names, waits for SQL Agent initialization, and verifies snapshot rows, ordered transactional create/update/delete events, the commit boundary, checkpoint restart without snapshot replay, and cleanup. It has passed against SQL Server 2022 Developer RTM-CU25.

The separate SQL Server Docker portability gate is runnable where the Microsoft image is available:

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 16. Roadmap

1. Close PostgreSQL surrogate-key/non-source signaling, transient-metadata, extension-type/failure-fixture, and Kafka gates.
2. Close MySQL signaling, TLS-store, wider DDL/type, and Kafka gates.
3. Close SQL Server CDC container-portability, retention-failure, concurrency, wider-type, and Kafka gates.
4. Only then consider additional databases.
5. Add Schema Registry formats, packaging, security policy, operational runbooks, and stable upgrade migrations before `1.0`.

---

## 简体中文

### 1. 产品定义

Rustium 是一个使用 Rust 独立实现的开源、基于日志的变更数据捕获平台。它读取数据库已提交变更，转换为与数据库无关的强类型事件，并将有序记录投递到下游 Sink。

Rustium 是独立服务，不依赖 Kafka Connect 或 JVM。Kafka 是 Sink，而不是运行时依赖。

Rustium 以最新版 Debezium 的架构、事件行为和配置名称作为兼容性参考。Rustium 不是 Debezium fork，也不复制 Debezium Java 源码。

#### 1.1 连接器优先级

连接器严格按以下顺序推进：

1. PostgreSQL
2. MySQL
3. SQL Server

在这三个连接器通过正确性、恢复、类型覆盖和运维发布门槛之前，其他数据库不进入范围。

#### 1.2 目标

- 正确保留源端顺序和可重放位点。
- 绝不 checkpoint 尚未获得 Sink 确认的数据。
- 限制内存并将背压传回 Source。
- 为 at-least-once 去重提供确定性事件 ID。
- 分离源协议、内部事件、格式、Sink 和状态所有权。
- 在实际可兼容时优先采用 Debezium 参数名和事件字段。
- 暴露生命周期、位点、队列、投递和故障状态。

#### 1.3 `1.0` 前的非目标

- 跨任意 Sink 的端到端 exactly-once。
- Kafka Connect Worker 或 SMT 兼容。
- 动态原生插件 ABI。
- 多实例高可用协调。
- 通过轮询业务表替代数据库 CDC 能力。
- 在前三个连接器完成前添加低优先级数据库。

### 2. 当前实现

Workspace 已包含可运行的 alpha 服务。

| 组件 | 状态 |
|---|---|
| 核心事件/位点/运行时 trait | 已实现 |
| 有界 Tokio 运行时 | 已实现 |
| SQLite checkpoint v2 与连接器状态 | 已实现；仍可读取 version 1 JSON |
| 原生和 Debezium JSON | 已实现 |
| stdout 和 Kafka Sink | 已实现 |
| PostgreSQL Source | 已实现；PostgreSQL 17 恢复、heartbeat/action-query、可写/只读增量快照和核心类型矩阵门槛通过 |
| MySQL Source | 已实现；MySQL 8.4 Docker 和外部破坏性 DDL 重启门槛通过 |
| SQL Server Source | 已实现并通过 SQL Server 2022 Developer CU25 外部集成测试 |
| CLI 和 HTTP 管理 | 已实现 |
| 已发布镜像/包/Helm Chart | 尚不可用 |

Alpha 表示源端和投递契约可以运行，但配置和持久化状态尚未稳定。

### 3. 架构不变量

1. **不丢失已确认数据。** 只有相关事件全部获得 Sink 确认后才持久化源位点。
2. **At-least-once 投递。** Sink 确认后、checkpoint 持久化前崩溃可能导致重放。
3. **源顺序权威。** 一个连接器的记录按强类型源位点保持顺序。
4. **内存有界。** Source 到运行时的每个队列都有配置容量。
5. **显式 unavailable。** 缺失的 TOAST 或 minimal row image 值不会被错误转换为 null。
6. **状态版本化。** Checkpoint 和配置具有明确 schema 版本与指纹。
7. **故障可见。** 协议、转换、状态和 Sink 错误默认停止连接器。
8. **敏感信息保护。** 凭据和行数据不进入状态或指标。
9. **不静默宣称兼容。** 未支持的 Debezium 参数产生警告或校验错误。

### 4. Workspace 边界

```text
crates/
|-- rustium-core/          强类型事件、位点、trait、运行时
|-- rustium-config/        YAML、properties、校验、指纹
|-- rustium-state/         版本化 SQLite checkpoint
|-- rustium-postgresql/    pgoutput Source
|-- rustium-mysql/         行级 binlog Source
|-- rustium-sqlserver/     SQL Server CDC Source
|-- rustium-format-json/   原生和 Debezium JSON
|-- rustium-sink-stdout/   开发用 Sink
|-- rustium-sink-kafka/    持久 Kafka Producer Sink
|-- rustium-server/        健康、状态、生命周期、指标
`-- rustium-cli/           rustium 二进制
```

Connector 和 Sink 通过 Rust trait 静态链接。稳定动态插件 ABI 延后设计。

### 5. 数据平面

```text
源协议
  |
  v
解码和规范化
  |
  v
有界 SourceRecord channel
  |
  v
编码有序 ChangeEvent
  |
  v
写入有序 DeliveryBatch
  |
  v
持久化已确认位点 + 连接器状态
  |
  v
在协议支持时确认 Source
```

当前运行时使用一个 Source task 和一个有序投递协调器。在明确排序屏障和分区契约之前，项目有意限制并行度。

#### 5.1 提交顺序

每个非空批次或仅位点批次执行：

1. 编码全部数据记录。
2. 调用 `Sink::write` 并等待确认。
3. 在一个 SQLite checkpoint 事务中保存源位点和版本化连接器状态。
4. 通过 Source 确认 channel 发布已保存位点。
5. 更新状态计数器。

快照完成和事务提交边界会强制刷新。大小和时间阈值也可能在事务中间刷新，因此连接器位点必须支持事务内部精确重放。

#### 5.2 关闭

取消操作会停止 Source 读取、刷新待处理事件、只持久化已确认位点、刷新并关闭 Sink，最后关闭协议资源。超时退出时不会 checkpoint 未确认记录。

### 6. 内部事件模型

`ChangeEvent` 包含：

- 确定性 `EventId`；
- 强类型 `SourcePosition`；
- Connector/数据库/schema/表元数据；
- 可选事务 ID 和顺序；
- read、create、update、delete、truncate 或 message 操作；
- 可选 before/after 行；
- 版本化字段 schema；
- 源时间和观测时间。

`DataValue` 区分 null、boolean、有符号/无符号整数、decimal 文本、float、string、bytes、date、time、timestamp、UUID、JSON、array 和 unavailable。

事件 ID 对连接器名称、源分区、强类型位点、集合和序号进行哈希。同一源记录重放会得到相同 ID。

### 7. 源位点与 Checkpoint

#### 7.1 PostgreSQL

位点包含 WAL LSN、可选 commit LSN、事务 ID、同 LSN 事件序号和快照状态。只有对应 checkpoint 保存后才推进复制反馈。

#### 7.2 MySQL

位点包含 binlog 文件名、可重放事件锚点、GTID、源 server ID、行/事件序号和快照状态。

对于行事件，checkpoint 锚点是前置 `TABLE_MAP_EVENT` 的起始位置。这是有意设计：只记录行事件结束位置可能丢失剩余行；直接从行事件体启动又可能缺少解码元数据。从 table-map 锚点重放并使用确定性行序号，可跳过已经确认的行，并从多行事件内部继续。

#### 7.3 状态兼容

配置指纹覆盖源身份、选表、快照行为、格式和路由。密码与运维调优不影响指纹。不兼容 checkpoint 会被拒绝，直到显式重置或迁移。

Checkpoint schema version 2 新增可选 connector-state envelope，其中包含格式标识、payload 版本和 JSON payload。该字段为增量字段，因此不含 connector state 的 version 1 checkpoint JSON 仍可读取。运行时会让最新状态跨普通数据记录延续，并将其与 Sink 已确认的最高源位点原子持久化。

反序列化兼容不会凭空生成缺失历史。已完成的 PostgreSQL 和 MySQL version 1 checkpoint 没有安全的历史 schema 基线，因此这两个连接器会拒绝恢复，并要求重置 checkpoint 后执行一次新的 initial snapshot。SQL Server 当前恢复路径不依赖 connector state，因此仍可继续读取 version 1 checkpoint。

### 8. 配置

Rustium 支持：

- 严格、版本化 YAML，拒绝未知字段；
- `${NAME}` 和 `${NAME:-default}` 环境变量插值；
- Debezium 风格 Java `.properties`；
- 数据库/schema/表完整匹配正则；
- 对已识别但未支持参数输出兼容性警告。

原生根 schema 为 `api_version: rustium.io/v1alpha1`、`kind: Connector`。

项目优先使用 Debezium 名称，包括 `name`、`connector.class`、`topic.prefix`、`database.*`、`table.include.list`、`table.exclude.list`、`snapshot.mode`、`snapshot.fetch.size`、`tombstones.on.delete`、`max.queue.size`、`max.batch.size` 和 `poll.interval.ms`。

Properties 中 Rustium 自身的状态、Sink、Server、日志和 Producer 扩展使用 `rustium.*`。

### 9. PostgreSQL 连接器

#### 9.1 前置条件

- PostgreSQL 14 或更高版本。
- `wal_level=logical`。
- 已存在的 `pgoutput` publication。
- 复制和表读取权限。
- 唯一复制 slot。

#### 9.2 快照切换

托管初始快照流程：

1. 准备或重建非活动托管 slot；
2. 创建逻辑 slot 并导出 snapshot；
3. 打开 repeatable-read、read-only SQL 事务；
4. 导入导出的 snapshot；
5. 发现选中的 publication 表和 schema；
6. 以有界分页扫描每张表；
7. 在 slot 锚点 LSN 携带基线 schema history 发出 snapshot-complete；
8. 从该 LSN 启动 `pgoutput`。

Slot 会保留快照期间提交的变更，因此切换不存在缺口。

#### 9.3 流式捕获

`pg_walstream` 提供复制传输和协议解析。Rustium 转换 begin、insert、update、delete、truncate、commit 和流式事务事件。同 LSN 序号让位点可全序和重放。缺失的未变化 TOAST 列成为 `Unavailable`。

PostgreSQL connector-state payload 持久化快照表布局，以及每列的类型 OID 和 typmod。重启时先恢复该基线，再打开 slot。随后每个 `Relation` 消息提供后续行事件对应的历史列名、顺序、类型身份和 key 标记。只有精确匹配的 catalog 元数据用于补充类型名和可空性，不会覆盖 WAL 列布局。schema 变化时版本递增，更新状态附着到下一条可 checkpoint 的 SourceRecord。

PostgreSQL 不会在 `Relation` 中记录原始 DDL、列可空性或 default。如果短暂历史列已经从当前 catalog 消失，且 checkpoint 基线中也不存在，Rustium 会通过 OID/typmod 解析类型，并保守地标记为 optional。这样可保持行解码和顺序正确，同时不伪造 WAL 未提供的元数据。

快照查询通过 PostgreSQL 的 `::text` 输出函数逐列投影，不再让整行经过 JSON 中间层。快照值和 `pgoutput` 值因此共用同一个转换器，可一致保留 numeric scale/precision、bytea、JSON 文本、时间格式和数组语法。数组解析器支持带引号和转义的元素、SQL NULL 与字符串 `"NULL"` 的区别、显式下界、嵌套维度和按元素类型转换。畸形数组文本会完整保留为字符串，不会被部分解码。

`heartbeat.interval.ms` 默认为零。设置为正数后，会在最新已进入有界队列的 SourceRecord 位点发送可见 heartbeat；首条 streaming event 之前则使用已完成快照的锚点。没有源位点时不会发送 heartbeat。可选的 `heartbeat.action.query` 在每个周期先通过复用的普通 SQL 连接执行。查询产生的 WAL 只有在 `pgoutput` 实际返回后才算进度，查询失败会携带数据库错误停止 Source。heartbeat 表可以加入 publication 但被选表规则排除；其事务 commit 仍可推进安全 WAL 位点，而不会暴露成业务表事件。

#### 9.4 Source 信号与增量快照

信号实现遵循 Debezium source-table channel。`signal.data.collection` 指向一个 schema-qualified 表，该表必须按顺序且仅包含 `id`、`type`、`data` 三个文本兼容列，必须加入 publication，并始终从业务快照和事件中过滤。`signal.enabled.channels` 接受 `source`。`execute-snapshot` 记录接受 `type=incremental`、`data-collections` 中的完整匹配正则，以及包含大小写不敏感集合表达式和 SQL filter 的 `additional-conditions`。在独立实现排序键和事件键语义前，非空 surrogate key 会被明确拒绝。

控制器当前实现 `incremental.snapshot.watermarking.strategy=insert_insert`。对于每个按主键排序的 chunk，它先提交 open watermark，在首个 chunk 捕获当前最大主键，通过共享文本转换器读取不超过 `incremental.snapshot.chunk.size` 行，再提交 close watermark。两个 watermark 之间的 WAL create、update 和 delete 会按主键从内存窗口移除对应行。close 时剩余行在 close commit 边界之前作为 read event 发出，并带 Debezium `incremental` snapshot marker。

当 `read.only=true` 时，chunk 连接不会插入 watermark 行。它为每个 chunk 分配一次 transaction ID，在有界查询前后捕获 `pg_current_snapshot()`，并保留快照的 `xmin`、`xmax` 和进行中 XID 集合。WAL transaction ID 在 low `xmin` 打开窗口；只有 high watermark 可见或当时仍进行中的最大事务已经提交后才关闭窗口。关闭窗口的 commit event 同时作为 checkpoint 边界。如果水位表明没有更旧事务，同一个 transaction ID 可以安全地立即关闭后续 chunk。重启时丢弃瞬时水位并重新读取当前主键范围。

Connector-state format version 3 保存 signal ID、展开后的集合、每集合 condition、集合索引、last key、maximum key、chunk sequence 和 pause 状态。close commit 将推进后的状态与已投递行原子 checkpoint。若在此之前崩溃，会重新读取同一个有界 chunk，可能重复但不会产生缺口；若在此之后重启，则从下一主键开始。Version 1 和 2 schema-history payload 会以新字段默认值继续读取。内存窗口有意不持久化。

`pause-snapshot` 在当前 close 边界后阻止准备下一 chunk。pause 标记会被 checkpoint，因此重启后仍保持暂停。`resume-snapshot` 在自身 signal 事务提交后安排下一 chunk。`stop-snapshot` 在没有 `data-collections` 时清除全部进度，否则只移除完整匹配表达式选中的集合；停止当前集合会重置其主键边界，并在控制事务之后安全推进。未知或乱序 watermark ID 会被忽略。

#### 9.5 PostgreSQL 剩余门槛

- surrogate-key 增量快照信号和非 source 信号输入 channel；
- 增量快照期间的在线 schema 变更；
- 扩展类型和更广故障样例；
- Kafka 端到端恢复测试。

### 10. MySQL 连接器

#### 10.1 前置条件

- MySQL 8.0 或更高版本。
- `log_bin=ON` 和 `binlog_format=ROW`。
- 与源 server ID 不同的 `database.server.id`。
- `SELECT`、`RELOAD`、`FLUSH_TABLES`、`REPLICATION SLAVE` 和 `REPLICATION CLIENT` 权限。

#### 10.2 快照切换

1. 在锁连接执行 `FLUSH TABLES WITH READ LOCK`。
2. 在快照连接启动 repeatable-read 一致性事务。
3. 捕获当前 binlog 文件名、位置、GTID 状态和源 server ID。
4. 释放全局读锁。
5. 在快照内发现已捕获数据库的基础表 schema。
6. 以有界分页扫描选中的表，有主键时按主键排序。
7. 提交快照，并携带基线 schema history 发出 snapshot-complete。
8. 从捕获位置开始 binlog 流式读取。

全局读锁只在建立事务和捕获位点期间持有，不覆盖完整表扫描。

#### 10.3 流式捕获

`mysql_async` 提供复制传输和 binlog 解析。Rustium 处理 rotate、table-map、GTID、query、write-rows、update-rows、delete-rows 和 XID 事件。

行事件生成强类型 before/after。FULL、MINIMAL 和 NOBLOB image 均可接受；省略值使用显式 `Unavailable`。GTID 事务包含全局顺序和集合内顺序。

MySQL schema history 是版本化 connector-state payload，保存已捕获数据库中选中表的有序字段模型。快照完成时建立基线。重启时，Rustium 在打开 binlog 前从 checkpoint 恢复该基线，然后按源顺序解析并应用选中表的 DDL query 事件。更新后的 schema 状态附着在对应 DDL 事务边界上，使 Sink 确认、源位点和 schema 状态一起推进。发现和 DDL 应用会忽略未选表，因此共享同一数据库的独立连接器不会相互修改历史状态。

当前基于 `sqlparser` 的 MySQL DDL 路径支持 `CREATE TABLE`、`ALTER TABLE` 增删/重命名/修改/变更列、主键变更、`DROP TABLE`、`RENAME TABLE`，以及 schema 不变的 `TRUNCATE TABLE`。默认情况下，解析或状态应用失败会停止连接器。`schema.history.internal.skip.unparseable.ddl=true` 与 Debezium 的显式跳过行为一致，并记录元数据风险警告。

当 `connect.keep.alive=true` 时，结束或失败的 binlog stream 会从最后一个成功送入有界运行时队列的 SourceRecord 重新打开。回卷过程恢复 binlog 文件名、table-map 锚点、GTID 事务计数、行序号和重放过滤器，从而确定性跳过已完成记录。`connect.keep.alive.interval.ms` 控制尝试间隔；`rustium.source.reconnect.max.attempts` 是 Rustium 的有限扩展，默认 10。日志会记录尝试次数和最后错误，产生新的源端进度后预算重置。

`heartbeat.interval.ms` 默认为零，即不发送可见 heartbeat。设置为正数后，从最新安全 streaming 位点发送 heartbeat，使其 Sink 确认和 checkpoint 遵循正常 at-least-once 顺序，同时不虚构 binlog 进度。Debezium JSON 使用 `serverName` key 和 `ts_ms` value。topic 优先使用 `topic.heartbeat.name`，否则将 `topic.heartbeat.prefix`（或旧参数 `heartbeat.topics.prefix`）与 `topic.prefix` 连接。

#### 10.4 TLS 模式

- `disabled`：仅明文。
- `preferred`：先尝试加密，失败后回退明文。
- `required`：加密，但不校验 CA 或主机名。
- `verify_ca`：加密并校验 CA，不校验主机名。
- `verify_identity`：加密并同时校验 CA 和主机名。

自定义 Debezium truststore/keystore 映射尚未实现。

#### 10.5 已验证恢复

MySQL 8.4 Docker 门槛覆盖：

- 两行快照和快照完成；
- 一个包含多行 insert、update、delete 的事务；
- 事务顺序 1 到 5；
- 强制终止活动 binlog dump 连接；
- 从最后完成事务重连且不产生重复事件；
- 捕获连接中断期间写入的第一个事务；
- 在旧 schema 行之前停止并建立 checkpoint，随后执行破坏性删列/加列 DDL，再写入新 schema 行；
- 当数据库已经暴露最终 schema 后重启，正确解码旧 schema 行、checkpoint DDL 状态，并解码新 schema 行。

单元门槛覆盖 checkpoint/state 原子性、version 1/2 checkpoint 兼容、schema-history 序列化、增量进度/控制和 PostgreSQL snapshot 解析、重放状态回卷、标量转换、heartbeat 编码、选表隔离，以及 create/alter/drop/rename DDL 应用。外部 PostgreSQL 17 门槛验证周期 heartbeat、成功执行 `heartbeat.action.query`、heartbeat 表过滤、source 信号分块、checkpoint 重启、additional condition、并发更新去重、pause/resume/scoped-stop 控制、保持更新事务时的只读事务水位、受限表权限、零连接器 watermark 写入、完成状态清理和信号表隔离。外部 MySQL 8.4 门槛还会在破坏性 DDL 恢复之外验证空闲 stream 的周期 heartbeat。

#### 10.6 MySQL 剩余门槛

- GTID source include/exclude 过滤；
- 信号记录；
- 增量快照；
- 自定义 trust/key store 和更广类型样例；
- 服务端 partial JSON diff 的精确重建。

### 11. SQL Server 连接器

SQL Server 连接器使用 SQL Server CDC 实现，不轮询业务表。当前每个连接器只接受一个数据库、每张选表只接受一个活动 capture instance，并要求 `data.query.mode=direct`。

已映射的 Debezium 兼容输入包括 `database.hostname`、`database.port`、`database.user`、`database.password`、`database.names`、`database.encrypt`、`database.trustServerCertificate`、`table.include.list`、`table.exclude.list`、`snapshot.mode`、`snapshot.isolation.mode`、`data.query.mode`、`streaming.fetch.size`、`max.queue.size`、`max.batch.size` 和 `poll.interval.ms`。

已实现流程：

1. 校验 SQL Server 版本、数据库 CDC 状态、capture instance 和 CDC LSN 可用性；
2. 捕获 `sys.fn_cdc_get_max_lsn()` 作为快照切换点；
3. 在一致性快照事务中读取选中的源表；
4. 直接轮询每个选中 capture instance 的 `cdc` change table；
5. 将 operation 3 和 4 配对为 update before/after；
6. 按 commit LSN 和 sequence value 合并各表；
7. 仅在 Sink 确认后 checkpoint；
8. 检测 CDC cleanup 已删除所需重启 LSN，并明确失败。

Update operation 3 和 4 会配对为一个逻辑 update 事件。查询按全局顺序排列，并由 `streaming.fetch.size` 限制；事务边界使用 commit LSN。事务中间恢复会从 commit LSN 重放、统计已跳过记录，并保留事务顺序。

多个数据库名称需要显式的分区感知排序和 checkpoint 所有权。在该契约经过测试前，Rustium 会直接拒绝。外部门槛已在 SQL Server 2022 Developer RTM-CU25 上验证快照切换、有序 CDC 流式捕获、commit 边界、checkpoint 重启不重复快照，以及资源清理。容器可移植性、retention 故障、更广类型和并发样例仍待补齐。

### 12. 格式与 Sink

原生 JSON Encoder 暴露完整强类型源位点和事件 schema。Debezium JSON Encoder 输出 `before`、`after`、`source`、`op`、`ts_ms`、事务元数据和连接器特定位点字段。

`tombstones.on.delete=true` 是 Debezium 兼容默认值。此时一条 delete 会在同一投递批次中编码为有序的两条记录：先发送 delete envelope，再发送 key 相同、payload 为 null 的 tombstone。tombstone 使用独立的确定性派生事件 ID；两条记录共同受 Sink 成功、checkpoint 持久化和 Source 确认约束。原生 YAML 使用 `format.tombstones_on_delete`；原生 Rustium JSON 格式不产生 tombstone。

stdout 是 best-effort，仅用于开发，并将 tombstone 输出为一行 `null`。Kafka 使用 `librdkafka`，将 tombstone 发送为真正的 null value，支持可配置 Producer 属性和持久确认，并在确认设置允许时启用幂等。Producer 幂等并不会把 Source 到 Kafka 变为 exactly-once。

### 13. 控制平面与可观测性

生命周期状态包括 created、starting、snapshotting、streaming、paused、failed、stopping 和 stopped。当前 API：

| 端点 | 用途 |
|---|---|
| `GET /health/live` | 进程存活 |
| `GET /health/ready` | 连接器就绪 |
| `GET /v1/connector/status` | 生命周期、位点、checkpoint、队列、计数 |
| `POST /v1/connector/stop` | 启用后优雅停止 |
| `GET /metrics` | Prometheus 指标 |

当前指标包括连接器状态、已投递事件、失败事件和流水线队列深度。数据库 lag 和日志保留指标仍属于发布工作。

### 14. 错误、安全与资源策略

- 配置和能力错误在捕获前失败。
- 未知协议/数据错误停止连接器。
- 通用 Source/Sink 重试协调尚未实现。MySQL 已具备连接器内有限且记录日志的 binlog 重连处理。
- 队列有界，背压会阻塞 Source 输出。
- 数据库和 Kafka TLS 由配置控制。
- 管理 Server 默认绑定 loopback。
- 变更型 HTTP 端点默认禁用。
- Secret 在加载时插值，不进入状态和语义指纹。

### 15. 测试与发布门槛

连接器能编译不代表完成。必要门槛包括：

- 位点、转换、过滤、配置和事件 Envelope 单元测试；
- 真实数据库快照/流式测试；
- 快照切换期间并发写入测试；
- 事务和多行事件内部崩溃/重启测试；
- Sink 确认/checkpoint 顺序测试；
- cleanup/retention 故障测试；
- DDL 和历史 schema 测试；
- Debezium 兼容 golden fixture；
- 长时间背压和重连测试；
- Kafka 端到端持久性测试。

可运行被忽略的 MySQL Docker 测试：

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

该门槛会强制终止活动复制连接，并验证从最后安全源位点重连。

被忽略的外部 MySQL 测试从环境变量读取管理和 CDC 连接设置，仓库中不包含凭据：

```bash
RUSTIUM_MYSQL_TEST_HOST=mysql.example.com \
RUSTIUM_MYSQL_TEST_PORT=3306 \
RUSTIUM_MYSQL_TEST_ADMIN_USER=root \
RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_USER=cdc \
RUSTIUM_MYSQL_TEST_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo \
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

该门槛使用管理账号创建隔离的选中表，并并发运行 CDC Source。测试验证快照/复制、破坏性 DDL 前后 checkpoint 的 schema version 1 和 2、选表历史隔离、从安全 binlog 位点发送的空闲周期 heartbeat，以及资源清理。测试已在启用行级 binlog 和 GTID 的 MySQL 8.4 上通过。

被忽略的 PostgreSQL 外部测试从环境变量读取连接配置，仓库中不包含测试凭据：

```bash
RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com \
RUSTIUM_POSTGRES_TEST_PORT=5432 \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me' \
RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo \
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture
```

这些测试使用隔离的业务表/信号表/publication/slot/role 名称，验证快照记录、同一事务内有序的 create/update/delete 事件、checkpoint 停止、旧 schema 行、破坏性删列/加列 DDL、新 schema 行、schema version 1 和 2 的历史 `Relation` 重放、重启不重复快照、安全 WAL 位点上的周期 heartbeat、`heartbeat.action.query`、heartbeat 表过滤、带 checkpoint 的增量快照恢复、过滤分块、并发更新去重、pause/resume/scoped-stop 控制、保持更新事务时的只读事务水位、受限权限下零 watermark 写入、信号表隔离，以及 PostgreSQL 核心类型矩阵在快照/WAL 路径上的一致转换。测试已在启用 `wal_level=logical` 的 PostgreSQL 17 上通过。

被忽略的 SQL Server 外部测试从环境变量读取连接配置，仓库中不包含测试凭据：

```bash
RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com \
RUSTIUM_SQLSERVER_TEST_PORT=1433 \
RUSTIUM_SQLSERVER_TEST_USER=sa \
RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me' \
RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo \
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

该门槛使用隔离的表/capture-instance 名称，等待 SQL Agent 初始化，并验证快照记录、同一事务内有序的 create/update/delete 事件、commit 边界、checkpoint 重启不重复快照，以及资源清理。测试已在 SQL Server 2022 Developer RTM-CU25 上通过。

在可以访问 Microsoft 镜像的环境中，仍可运行独立的 SQL Server Docker 可移植性门槛：

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 16. 路线图

1. 补齐 PostgreSQL surrogate-key/非 source 信号、短暂元数据、扩展类型/故障样例和 Kafka 门槛。
2. 补齐 MySQL 信号、TLS store、更广 DDL/类型和 Kafka 门槛。
3. 补齐 SQL Server CDC 容器可移植性、retention 故障、并发、更广类型和 Kafka 门槛。
4. 只有完成前三项后才考虑其他数据库。
5. 在 `1.0` 前补 Schema Registry 格式、打包、安全策略、运维手册和稳定升级迁移。
