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
| PostgreSQL 14+ snapshot, `pgoutput`, persistent schema history, heartbeat records, multi-channel signaling, incremental snapshots, and core type matrix | Implemented; external gates pass with PostgreSQL 17 |
| MySQL 8+ snapshot, row-binlog streaming, GTID source filters, persistent schema history, and heartbeat records | Implemented; Docker and external GTID recovery/heartbeat gates pass with MySQL 8.4 |
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
- Access to MySQL 8.0+ with row binlog and GTID for the ignored MySQL external integration test
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

The admin account only creates and removes uniquely named test tables. The CDC account verifies idle periodic heartbeats, `heartbeat.action.query`, snapshot/replication, GTID-filtered startup from the exact server UUID, checkpoint recovery, destructive-DDL recovery, typed keyset restart, durable completed signal IDs, and deduplication of an update committed while an incremental chunk window is open. This gate has passed against MySQL 8.4 with row binlog and GTID enabled.

Run the external PostgreSQL 14+ integration test without storing credentials in the repository:

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

The tests create uniquely named tables, signal tables, publications, replication roles, managed slots, and signal files. They cover snapshot handoff, transaction ordering, checkpoint stop, destructive DDL with historical `Relation` replay, restart without a repeated snapshot, forced termination and automatic recovery of the active replication backend, explicit failure after a checkpoint's replication slot is lost, periodic heartbeat records, `heartbeat.action.query`, heartbeat-table filtering, resumable source/file/in-process-signaled incremental snapshots, immediate external-signal state checkpointing, additional conditions, concurrent-update deduplication, pause/resume/scoped-stop controls, read-only transaction-snapshot watermarking with no signal-table writes, file and in-process read-only signaling without any signal table, validated surrogate-key ordering, and identical snapshot/WAL conversion for high-precision numeric, special values, JSONB, UUID, bytea, temporal, network, range, bit, array, hstore, domain, enum, and tsvector types. These gates pass against PostgreSQL 17 with `wal_level=logical`. An opt-in fixture also temporarily constrains `max_slot_wal_keep_size`, generates WAL, forces checkpoints, and verifies explicit failure after `wal_status=lost`; it restores the original setting before returning. Set `RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST=true` only on an isolated superuser test instance. Another optional fixture verifies vector/halfvec/sparsevec and PostGIS geometry/geography whenever `vector` or `postgis` is already installed; the current PostgreSQL 17 test instance has neither extension installed. A separate librdkafka MockCluster gate verifies Kafka key filtering, single-partition consumption, and offset commit only after durable signal acknowledgement.

Run the WAL-retention fixture separately when the PostgreSQL test account is a superuser and the instance is isolated:

```bash
RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST=true \
cargo test -p rustium-postgresql --test postgresql_external \
  rejects_checkpoint_resume_after_wal_retention_invalidation -- --ignored --nocapture
```

Run the optional real-broker Kafka signal gate against a reachable plaintext Kafka-compatible endpoint. It creates and removes a unique single-partition topic:

```bash
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-signal-kafka --test kafka_external \
  -- --ignored --nocapture --test-threads=1
```

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
serde_json = "1"
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
    let heartbeat_topics_prefix = source_config.heartbeat_topics_prefix.clone();
    let heartbeat_topic_name = source_config.heartbeat_topic_name.clone();

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
            heartbeat_topics_prefix,
            heartbeat_topic_name,
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
    let _signal_sender = runtime.signal_sender();

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

When `signal.enabled.channels` contains `in-process`, retain the sender returned before `run` consumes the runtime and clone it into application request handlers. Sending is bounded by `runtime.channel_capacity` and returns only after the command is admitted to the connector queue:

```rust,no_run
use rustium_core::{Result, SignalRecord, SignalSender};

async fn refresh_orders(sender: &SignalSender) -> Result<()> {
    sender
        .send(SignalRecord::new(
            "orders-refresh-2026-07-16",
            "execute-snapshot",
            serde_json::json!({
                "type": "incremental",
                "data-collections": ["public\\.orders"]
            }),
        ))
        .await
}
```

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
- periodic heartbeat records from the latest safe WAL position, with optional `heartbeat.action.query`
- one shared text conversion path for snapshot and WAL values, including exact numeric precision, arrays, domains, enums, `hstore`, `tsvector`, pgvector values, and spatial EWKB
- source-table, file, in-process, and Kafka `execute-snapshot` signaling with bounded, checkpointed incremental snapshots
- `read.only=true` incremental watermarking without connector writes to the signal table

The source requires `wal_level=logical`, an existing publication, and a user with the required replication and table-read permissions. See [examples/postgresql.yaml](examples/postgresql.yaml).

Checkpoint v1 JSON remains readable, but a completed PostgreSQL v1 checkpoint has no historical Relation baseline and is rejected for resume. Reset it and run one new initial snapshot to establish checkpoint v2 schema history.

Set `heartbeat.interval.ms` to a positive interval to enable PostgreSQL heartbeat records. `heartbeat.action.query` optionally runs on a reused ordinary SQL connection at the same cadence; query failures stop the source, and query-generated WAL does not become checkpoint progress until the replication stream observes it. The default topic is `__debezium-heartbeat.<topic.prefix>`; `topic.heartbeat.prefix`, legacy `heartbeat.topics.prefix`, and full-name override `topic.heartbeat.name` follow Debezium naming. Native YAML uses `source.heartbeat_interval`, `source.heartbeat_action_query`, `source.heartbeat_topics_prefix`, and `source.heartbeat_topic_name`.

PostgreSQL domains are converted through their base type, including domain arrays. Enums, ranges, network types, and `tsvector` retain PostgreSQL's canonical text. Debezium `hstore.handling.mode=json` is the default and produces a JSON value; `map` produces a typed `DataValue::Map`. Dense `vector`/`halfvec` values become float arrays, `sparsevec` becomes a map containing `dimensions` and its indexed vector, and PostGIS `geometry`/`geography` retain complete EWKB bytes. Malformed or unknown extension values fall back to their original string instead of being partially decoded. Native YAML uses `source.hstore_handling_mode`.

To enable incremental snapshots, create a three-column text-compatible signal table, add it to the same publication, and configure the Debezium-compatible source channel:

```properties
signal.data.collection=public.rustium_signal
signal.enabled.channels=source
incremental.snapshot.chunk.size=1024
incremental.snapshot.watermarking.strategy=insert_insert
incremental.snapshot.allow.schema.changes=false
```

Request a snapshot by inserting a Debezium-compatible signal. Each `data-collections` entry is a fully matched regular expression:

```sql
INSERT INTO public.rustium_signal (id, type, data)
VALUES (
  'inventory-refresh-2026-07-16',
  'execute-snapshot',
  '{"type":"incremental","data-collections":["public\\.orders"]}'
);
```

Rustium also implements Debezium's file signaling channel. The file contains one JSON object per line and is cleared after each successful read:

```properties
signal.enabled.channels=file
signal.file=/run/rustium/inventory-signals.jsonl
signal.poll.interval.ms=5000
```

```json
{"id":"inventory-file-refresh","type":"execute-snapshot","data":{"type":"incremental","data-collections":["public\\.orders"]}}
```

The `in-process` channel accepts the same typed JSON envelope through the embedded `SignalSender` or the management API:

```properties
signal.enabled.channels=in-process
read.only=true
```

```bash
curl --fail-with-body -X POST http://127.0.0.1:8080/v1/connector/signals \
  -H 'content-type: application/json' \
  -d '{"id":"inventory-api-refresh","type":"execute-snapshot","data":{"type":"incremental","data-collections":["public\\.orders"]}}'
```

The Kafka channel uses Debezium's topic, group, bootstrap, poll-timeout, and consumer pass-through names:

```properties
signal.enabled.channels=kafka
signal.kafka.bootstrap.servers=kafka-1:9092,kafka-2:9092
signal.kafka.topic=inventory-signals
signal.kafka.groupId=inventory-signal
signal.kafka.poll.timeout.ms=100
signal.consumer.security.protocol=SASL_SSL
read.only=true
```

The topic defaults to `<topic.prefix>-signal`, must have exactly one partition, and each message key must equal `topic.prefix`. The value uses the same JSON envelope shown above. Rustium forces `enable.auto.commit=false` and `enable.auto.offset.store=false`; after accepting a valid command, it commits the Kafka offset synchronously only after the matching connector state passes Sink delivery, SQLite checkpoint persistence, and Source acknowledgement. Replayed `execute-snapshot` records with the active signal ID are idempotently ignored, closing the crash window between the database checkpoint and Kafka offset commit. Native YAML uses `source.signal_kafka_topic`, `source.signal_kafka_bootstrap_servers`, `source.signal_kafka_group_id`, `source.signal_kafka_poll_timeout`, and `source.signal_kafka_consumer_properties`.

The HTTP route additionally requires `rustium.server.enable.mutations=true` or native `server.enable_mutations: true`; it returns `202 Accepted` after bounded queue admission, `403` when mutations are disabled, and `409` when `in-process` is not enabled. `source`, `file`, `in-process`, and `kafka` can be enabled together. A writable incremental snapshot still requires `signal.data.collection` in the publication because Rustium writes its internal open/close watermarks there; `read.only=true` with an external channel does not require a signal table. Valid external control state is checkpointed immediately at the current safe LSN, including on a fresh idle slot. Invalid external records and unsupported actions are logged and skipped, internal watermark action types are rejected, and file delivery has no retry policy.

In Debezium properties, `signal.enabled.channels=jmx` is accepted as a migration alias for `in-process`. Debezium's JMX channel is a JVM MXBean backed by an in-memory queue, so Rustium exposes the equivalent bounded queue through `ConnectorRuntime::signal_sender()` and `POST /v1/connector/signals` instead of pretending to host a JVM MBean. The parser emits a compatibility warning that names this mapping; combining `jmx` and `in-process` creates only one channel.

The execute payload also accepts Debezium `additional-conditions`, each containing a `data-collection` regular expression and a SQL `filter`. The filter constrains both the captured maximum key and every chunk query. Optional `surrogate-key` replaces primary-key chunk ordering when that column is `NOT NULL` and backed by a valid single-column unique index; the table still needs a primary key for WAL deduplication. `pause-snapshot` pauses after the current bounded chunk, `resume-snapshot` continues from its checkpointed key, and `stop-snapshot` stops all work or only collections matched by its optional `data-collections` list.

The signal table must contain exactly `id`, `type`, and `data` in that order and must be part of the publication. Selected snapshot tables must have primary keys. Rustium writes `snapshot-window-open` and `snapshot-window-close` watermarks, removes rows superseded by WAL events while the window is open, emits remaining rows as Debezium `op=r` events with `source.snapshot=incremental`, and checkpoints the next key, conditions, and pause state with the close transaction. Restart can repeat an uncommitted chunk but cannot skip it. Native YAML uses `source.signal_data_collection`, `source.incremental_snapshot_chunk_size`, and `source.incremental_snapshot_watermarking_strategy`.

Set Debezium `read.only=true` or native `source.read_only: true` to replace inserted watermarks with `pg_current_snapshot()` low/high transaction watermarks. Rustium compares WAL transaction IDs against those snapshots, keeps the window open until every transaction visible across the chunk has passed, and applies the same primary-key deduplication. The connector requires only `SELECT` on captured tables plus logical-replication access and writes no watermark records. A source signal table remains necessary only when the `source` channel is used.

Current PostgreSQL signaling supports the Debezium `source`, `file`, `in-process`, and `kafka` channels, the JMX-to-management migration alias, and incremental snapshot actions. Writable mode supports `insert_insert`; read-only mode uses transaction snapshots. With `incremental.snapshot.allow.schema.changes=false`, Rustium compares the catalog after opening every window and also rejects a changed WAL `Relation` for the active table, preserving the previous checkpoint instead of querying or emitting mismatched layouts. A completed checkpoint also requires the original replication slot to exist and report a WAL status that still retains the required history; a missing, `unreserved`, or `lost` slot fails before stream creation with a reset-and-resnapshot instruction rather than silently creating a new slot. Debezium's PostgreSQL connector does not support schema changes during an incremental snapshot, so Rustium rejects `incremental.snapshot.allow.schema.changes=true` instead of exposing unsafe semantics. Historical `Relation` replay now falls back to matching checkpointed type metadata, or a conservative `unknown_oid_*` name when both catalog and history are unavailable. Remaining PostgreSQL gates are live PostGIS/pgvector fixtures on a server with those extensions installed and real-broker Kafka end-to-end recovery coverage.

### MySQL

The MySQL connector uses row-based binary logs through the native replication protocol.

Implemented behavior:

- MySQL 8.0+ validation for `log_bin`, `binlog_format=ROW`, row image, source server ID, and selected tables
- `FLUSH TABLES WITH READ LOCK` plus a repeatable-read consistent snapshot
- captured binlog file, position, GTID state, and source server ID
- write, update, and delete row events, including multi-row events
- transaction GTIDs and total/data-collection ordering
- Debezium-compatible `gtid.source.includes`, `gtid.source.excludes`, and optional source-based DML filtering
- checkpointed MySQL schema history, restored before binlog replay
- ordered DDL application for `CREATE TABLE`, `ALTER TABLE` add/drop/rename/modify/change column, `DROP TABLE`, and `RENAME TABLE`
- exact restart inside a multi-row event using a replayable table-map anchor and row ordinal
- automatic binlog reconnect from the last safe source position with a finite, observable retry budget
- periodic heartbeat records from the latest safe binlog position, disabled by default
- optional `heartbeat.action.query` execution on a separate ordinary MySQL connection at the heartbeat cadence
- FULL, MINIMAL, and NOBLOB row images with explicit unavailable values where MySQL omits data
- PARTIAL_JSON update diffs reconstructed from a complete before image, with unavailable fallback when safe reconstruction is impossible
- UTC-normalized temporal reads plus snapshot/binlog equality for boolean, signed/unsigned integer, decimal, float, bit/binary, temporal, string, JSON, ENUM, SET, and null values
- source-table, file, in-process, and Kafka signaling for incremental snapshot controls, with primary-key keyset progress and completed signal IDs persisted in connector state
- low/high binlog-coordinate windows that remove incrementally read rows superseded by concurrent create, update, or delete events before the chunk commit
- Docker and external integration coverage against MySQL 8.4, including filtered GTID startup, destructive-DDL restart, keyset restart, and concurrent-write deduplication

Recommended MySQL permissions for the connector user:

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

The MySQL Debezium-style example is [examples/mysql.properties](examples/mysql.properties).

Set `heartbeat.interval.ms` to a positive interval to emit heartbeat records. The default topic is `__debezium-heartbeat.<topic.prefix>`; `topic.heartbeat.prefix` changes its prefix, `heartbeat.topics.prefix` remains accepted for migration, and `topic.heartbeat.name` overrides the full topic. Heartbeats use `{"serverName":"<connector-name>"}` as the key and `{"ts_ms":...}` as the value. Native YAML uses `source.heartbeat_interval`, `source.heartbeat_topics_prefix`, and `source.heartbeat_topic_name`.

`heartbeat.action.query` optionally runs first on a separate ordinary MySQL connection at each positive heartbeat interval. Query failures stop the source with the database error; the query's binlog changes are not treated as source progress until the replication stream observes them. Native YAML uses `source.heartbeat_action_query`.

DDL parsing failures stop the connector by default. Debezium-compatible `schema.history.internal.skip.unparseable.ddl=true` can advance past unsupported DDL with a warning, but doing so can leave schema metadata incomplete.

`gtid.source.includes` and `gtid.source.excludes` accept comma-separated, case-insensitive regular expressions matched against the complete GTID source UUID; configure at most one of them. When either property is present, Rustium filters a complete captured executed-GTID SID set and uses GTID-based startup when at least one source remains. If no executed source matches, it logs the condition and falls back to the captured binlog file and position. Streaming checkpoints contain a transaction GTID rather than a complete executed set, so reconnect from those checkpoints deliberately stays on the exact file/position path. With the default `gtid.source.filter.dml.events=true`, row events from a non-matching source are suppressed, but their transaction commit boundary still advances the safe checkpoint and DDL remains visible to schema history. Set the property to `false` to retain all DML while still filtering complete GTID recovery anchors. Native YAML uses `source.gtid_source_includes`, `source.gtid_source_excludes`, and `source.gtid_source_filter_dml_events`.

Checkpoint v1 JSON remains readable, but a completed MySQL v1 checkpoint has no historical schema baseline and is rejected for resume. Reset that checkpoint and run a new initial snapshot once to establish checkpoint v2 schema history.

MySQL supports source-table, file, bounded in-process, and Kafka signaling for Debezium-compatible `execute-snapshot`, `pause-snapshot`, `resume-snapshot`, and `stop-snapshot` controls. Incremental snapshots require a primary key, capture a fixed maximum key per collection, and advance with typed single-column or composite-key keyset queries instead of `LIMIT/OFFSET`. For each chunk, Rustium captures low and high binlog coordinates around the query, buffers the read rows, emits ordinary CDC records while the stream catches up, and removes matching before/after primary keys changed inside `(low, high]`. The remaining rows are emitted with the chunk commit, which also persists the current key, maximum key, collection, pause state, and bounded completed-signal history. Restart resumes after the last checkpointed key; a replayed completed execute signal is ignored, including the crash window between a connector checkpoint and Kafka offset commit. An uncommitted in-memory window is discarded and reread after reconnect, and schema changes observed while a window is open stop the source before mismatched rows are emitted. Only one chunk runs per event-loop turn, so binlog events and control signals are observed between chunks. Java-specific truststore/keystore conversion, spatial types, and less-common DDL fixtures remain follow-up gates. When `binlog_row_value_options=PARTIAL_JSON` is enabled, a diff without a complete before image remains explicitly unavailable rather than being guessed.

For MySQL source-table signaling, set `signal.data.collection=database.signal_table` and include the signal table in the connector user's `SELECT` scope. The table must expose `id`, `type`, and `data` columns; the connector does not write to it. File signaling consumes one JSON envelope per line and clears the file only after reading it. In-process signaling uses the same `SignalSender` and HTTP management route as the other connectors. Kafka signaling uses the existing single-partition, checkpoint-coupled `rustium-signal-kafka` channel and the same Debezium topic/key contract.

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
- bounded CDC queries controlled by `streaming.fetch.size`, including continuation inside one commit LSN
- periodic heartbeat records from the latest safe CDC position, with optional `heartbeat.action.query` on a separate SQL connection
- shared snapshot/CDC SQL projections for consistent core numeric, binary, UUID, temporal, and text conversion
- source-table, file, in-process, and Kafka signal channels with checkpointed primary-key incremental snapshots
- CDC-observed open/close watermarks that buffer each chunk and remove rows superseded by concurrent create, update, or delete events

The current implementation requires exactly one entry in `database.names`, one active capture instance per selected table, and `data.query.mode=direct`. Incremental snapshots require a primary key, capture a fixed maximum key, and persist typed single/composite keyset progress plus a bounded completed-signal history. Every incremental snapshot channel also requires `signal.data.collection`: the table must contain exactly text-compatible `id`, `type`, and `data` columns in that order, `id` must hold at least 42 characters, the table must be CDC-enabled, and the connector user must have `INSERT`. These constraints and object permission are checked during source validation. Rustium waits until CDC observes the open watermark, buffers the chunk, removes primary keys found in CDC before/after images, and emits remaining reads only when CDC observes the close watermark commit. Signal-table rows are never exposed as business events. The event loop processes one chunk at a time outside active CDC transactions, so pause/resume/stop commands are observed between chunks. An uncommitted in-memory window is discarded on restart, and the source verifies the table schema before the query and again at close.

The SQL Server 2022 Developer RTM-CU25 external gate verifies snapshot handoff, fetch-size-one continuation through update before/after pairs, mid-transaction checkpoint restart with preserved transaction ordinals, concurrent transactions ordered by commit LSN, retention fail-closed behavior, heartbeat/action-query, core snapshot/CDC type equality, in-process keyset restart, source-table signaling with additional conditions, CDC-window concurrent-update deduplication, and cleanup. See [examples/sqlserver.properties](examples/sqlserver.properties).

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
- `heartbeat.interval.ms`, `heartbeat.action.query`, `topic.heartbeat.prefix`, `heartbeat.topics.prefix`, `topic.heartbeat.name`
- `signal.data.collection`, `signal.enabled.channels`, `signal.file`, `signal.poll.interval.ms`
- `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, `signal.consumer.*`
- `incremental.snapshot.chunk.size`, `incremental.snapshot.allow.schema.changes`, `incremental.snapshot.watermarking.strategy`
- `read.only`
- `hstore.handling.mode`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Currently mapped MySQL properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`
- `database.server.id`, `database.ssl.mode`, `database.ssl.ca`, `database.ssl.cert`, `database.ssl.key`
- `database.include.list`, `database.exclude.list`
- `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`, `connect.timeout.ms`
- `connect.keep.alive`, `connect.keep.alive.interval.ms`
- `gtid.source.includes`, `gtid.source.excludes`, `gtid.source.filter.dml.events`
- `heartbeat.interval.ms`, `heartbeat.action.query`, `topic.heartbeat.prefix`, `heartbeat.topics.prefix`, `topic.heartbeat.name`
- `signal.data.collection`, `signal.enabled.channels`, `signal.file`, `signal.poll.interval.ms`
- `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, `signal.consumer.*`
- `incremental.snapshot.chunk.size`, `incremental.snapshot.watermarking.strategy`
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
- `heartbeat.interval.ms`, `heartbeat.action.query`, `topic.heartbeat.prefix`, `heartbeat.topics.prefix`, `topic.heartbeat.name`
- `signal.data.collection`, `signal.enabled.channels`, `signal.file`, `signal.poll.interval.ms`
- `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, `signal.consumer.*`
- `incremental.snapshot.chunk.size`, `incremental.snapshot.allow.schema.changes`, `incremental.snapshot.watermarking.strategy`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Common Debezium format properties include `unavailable.value.placeholder` and `tombstones.on.delete`. Tombstones default to enabled in `debezium_json`: each delete envelope is followed in the same delivery batch by the same key with a null value. Set `tombstones.on.delete=false` or native YAML `format.tombstones_on_delete: false` to disable them.

Unsupported properties are reported as compatibility warnings instead of being silently treated as implemented. Rustium-specific source retry, sink, state, server, logging, and Kafka producer settings use the `rustium.*` prefix.

### Formats and Sinks

The internal model preserves null, signed and unsigned integers, decimal text, floating point, binary, date/time/timestamp, UUID, JSON, array, and unavailable values.

Available encoders:

- `rustium_json`: versioned native event payload
- `debezium_json`: `before`, `after`, `source`, `op`, `ts_ms`, transaction metadata, and heartbeat records

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
| `POST /v1/connector/signals` | Submit a Debezium-compatible in-process signal when mutations and the channel are enabled |
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
| PostgreSQL 14+ 快照、`pgoutput`、持久 schema history、heartbeat record、多 channel 信号、增量快照和核心类型矩阵 | 已实现；PostgreSQL 17 外部门槛通过 |
| MySQL 8+ 快照、行级 binlog、GTID source 过滤、持久 schema history 和 heartbeat record | 已实现；MySQL 8.4 Docker、外部 GTID 恢复和 heartbeat 门槛通过 |
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
- 运行被忽略的 MySQL 外部集成测试时，需要可访问且启用行级 binlog 与 GTID 的 MySQL 8.0+
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

管理账号只负责创建和删除唯一命名的测试表；CDC 账号用于验证空闲周期 heartbeat、`heartbeat.action.query`、快照/复制、基于精确 server UUID 的 GTID 过滤启动、checkpoint 恢复、破坏性 DDL 恢复、带类型 keyset 重启、已完成 signal ID 持久化，以及增量 chunk 窗口打开期间提交更新时的去重。该门槛已在启用行级 binlog 和 GTID 的 MySQL 8.4 上通过。

运行外部 PostgreSQL 14+ 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

测试会创建唯一命名的业务表、信号表、publication、复制角色、托管 slot 和信号文件，覆盖快照切换、事务顺序、checkpoint 停止、跨破坏性 DDL 的历史 `Relation` 重放、重启不重复快照、强制终止活动 replication backend 后自动恢复、checkpoint 对应 slot 丢失后的显式失败、周期 heartbeat record、`heartbeat.action.query`、heartbeat 表过滤、可恢复的 source/file/in-process 信号增量快照、外部信号状态即时 checkpoint、additional condition、并发更新去重、pause/resume/scoped-stop 控制、不写信号表的只读事务快照 watermark、完全无信号表的 file 和 in-process 只读信号、经验证的 surrogate-key 排序，以及高精度 numeric、特殊值、JSONB、UUID、bytea、时间、网络、range、bit、数组、hstore、domain、enum 和 tsvector 类型在快照/WAL 路径上的一致转换。这些门槛已在启用 `wal_level=logical` 的 PostgreSQL 17 上通过。可选 WAL retention fixture 会临时限制 `max_slot_wal_keep_size`、生成 WAL 并强制 checkpoint，验证 `wal_status=lost` 后显式失败，结束前恢复原设置；只应在隔离的 superuser 测试实例上设置 `RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST=true` 单独运行。另一个可选 fixture 会在数据库已安装 `vector` 或 `postgis` 时验证 vector/halfvec/sparsevec 与 PostGIS geometry/geography；当前 PostgreSQL 17 测试实例未安装这两个扩展。独立的 librdkafka MockCluster 门槛会验证 Kafka key 过滤、单 partition 消费，以及只有持久信号确认后才提交 offset。

可对可访问的明文 Kafka-compatible endpoint 运行可选的真实 broker 信号门槛。测试会创建并删除唯一命名的单 partition topic：

```bash
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-signal-kafka --test kafka_external \
  -- --ignored --nocapture
```

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
serde_json = "1"
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
    let heartbeat_topics_prefix = source_config.heartbeat_topics_prefix.clone();
    let heartbeat_topic_name = source_config.heartbeat_topic_name.clone();

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
            heartbeat_topics_prefix,
            heartbeat_topic_name,
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
    let _signal_sender = runtime.signal_sender();

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

当 `signal.enabled.channels` 包含 `in-process` 时，请在 `run` 消费 runtime 前保留 sender，并将其 clone 到应用请求处理器。发送操作受 `runtime.channel_capacity` 有界约束，命令进入连接器队列后才返回：

```rust,no_run
use rustium_core::{Result, SignalRecord, SignalSender};

async fn refresh_orders(sender: &SignalSender) -> Result<()> {
    sender
        .send(SignalRecord::new(
            "orders-refresh-2026-07-16",
            "execute-snapshot",
            serde_json::json!({
                "type": "incremental",
                "data-collections": ["public\\.orders"]
            }),
        ))
        .await
}
```

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
- 从最新安全 WAL 位点周期发送 heartbeat record，并支持可选的 `heartbeat.action.query`
- 快照和 WAL 共用同一文本转换路径，包括精确 numeric 精度、数组、domain、enum、`hstore`、`tsvector`、pgvector 值和空间 EWKB
- 通过 source 表、file、in-process 和 Kafka `execute-snapshot` 信号执行有界且可 checkpoint 的增量快照
- 通过 `read.only=true` 执行无需连接器写入信号表的增量 watermark

Source 需要 `wal_level=logical`、已存在的 publication，以及具备复制和表读取权限的用户。配置示例见 [examples/postgresql.yaml](examples/postgresql.yaml)。

Checkpoint v1 JSON 仍可读取，但已完成的 PostgreSQL v1 checkpoint 不含历史 Relation 基线，因此会拒绝恢复。升级后需要重置该 checkpoint 并执行一次新的 initial snapshot，以建立 checkpoint v2 schema history。

将 `heartbeat.interval.ms` 设置为正数即可启用 PostgreSQL heartbeat record。可选的 `heartbeat.action.query` 使用复用的普通 SQL 连接按同一周期执行；查询失败会停止 Source，查询产生的 WAL 只有在复制流实际读到后才能成为 checkpoint 进度。默认 topic 为 `__debezium-heartbeat.<topic.prefix>`；`topic.heartbeat.prefix`、旧参数 `heartbeat.topics.prefix` 和完整名称覆盖参数 `topic.heartbeat.name` 遵循 Debezium 命名。原生 YAML 使用 `source.heartbeat_interval`、`source.heartbeat_action_query`、`source.heartbeat_topics_prefix` 和 `source.heartbeat_topic_name`。

PostgreSQL domain 会按其基础类型转换，包括 domain 数组。Enum、range、网络类型和 `tsvector` 保留 PostgreSQL 规范文本。Debezium `hstore.handling.mode=json` 为默认值并生成 JSON 值；`map` 生成强类型 `DataValue::Map`。稠密 `vector`/`halfvec` 转为浮点数组，`sparsevec` 转为包含 `dimensions` 和索引向量的 map，PostGIS `geometry`/`geography` 保留完整 EWKB 字节。畸形或未知扩展值会回退为原始字符串，不进行部分解码。原生 YAML 使用 `source.hstore_handling_mode`。

要启用增量快照，请创建一个三列文本兼容信号表，将它加入同一 publication，并配置 Debezium 兼容的 source channel：

```properties
signal.data.collection=public.rustium_signal
signal.enabled.channels=source
incremental.snapshot.chunk.size=1024
incremental.snapshot.watermarking.strategy=insert_insert
incremental.snapshot.allow.schema.changes=false
```

插入 Debezium 兼容信号即可发起快照。每个 `data-collections` 项都是完整匹配的正则表达式：

```sql
INSERT INTO public.rustium_signal (id, type, data)
VALUES (
  'inventory-refresh-2026-07-16',
  'execute-snapshot',
  '{"type":"incremental","data-collections":["public\\.orders"]}'
);
```

Rustium 也实现了 Debezium file 信号 channel。文件每行包含一个 JSON 对象，每次成功读取后会清空：

```properties
signal.enabled.channels=file
signal.file=/run/rustium/inventory-signals.jsonl
signal.poll.interval.ms=5000
```

```json
{"id":"inventory-file-refresh","type":"execute-snapshot","data":{"type":"incremental","data-collections":["public\\.orders"]}}
```

`in-process` channel 通过嵌入式 `SignalSender` 或管理 API 接收相同的强类型 JSON envelope：

```properties
signal.enabled.channels=in-process
read.only=true
```

```bash
curl --fail-with-body -X POST http://127.0.0.1:8080/v1/connector/signals \
  -H 'content-type: application/json' \
  -d '{"id":"inventory-api-refresh","type":"execute-snapshot","data":{"type":"incremental","data-collections":["public\\.orders"]}}'
```

Kafka channel 使用 Debezium 的 topic、group、bootstrap、poll timeout 和 consumer 透传参数名：

```properties
signal.enabled.channels=kafka
signal.kafka.bootstrap.servers=kafka-1:9092,kafka-2:9092
signal.kafka.topic=inventory-signals
signal.kafka.groupId=inventory-signal
signal.kafka.poll.timeout.ms=100
signal.consumer.security.protocol=SASL_SSL
read.only=true
```

Topic 默认为 `<topic.prefix>-signal`，必须恰好只有一个 partition；每条消息的 key 必须等于 `topic.prefix`，value 使用上文相同 JSON envelope。Rustium 强制 `enable.auto.commit=false` 和 `enable.auto.offset.store=false`；有效 command 被接受后，只有对应 connector state 完成 Sink 投递、SQLite checkpoint 持久化和 Source 确认，才会同步提交 Kafka offset。若在数据库 checkpoint 与 Kafka offset commit 之间崩溃，重放的相同活动 signal ID 会被幂等忽略。原生 YAML 使用 `source.signal_kafka_topic`、`source.signal_kafka_bootstrap_servers`、`source.signal_kafka_group_id`、`source.signal_kafka_poll_timeout` 和 `source.signal_kafka_consumer_properties`。

HTTP 路由还要求 `rustium.server.enable.mutations=true` 或原生 `server.enable_mutations: true`；命令进入有界队列后返回 `202 Accepted`，禁用变更端点时返回 `403`，未启用 `in-process` 时返回 `409`。`source`、`file`、`in-process` 和 `kafka` 可以同时启用。可写增量快照仍要求 publication 中存在 `signal.data.collection`，因为 Rustium 会在其中写入内部 open/close watermark；`read.only=true` 配合外部 channel 时不需要信号表。有效的外部控制状态会立即在当前安全 LSN checkpoint，包括 fresh idle slot。无效外部 record 和不支持的 action 会记录日志并跳过，内部 watermark action type 会被拒绝；file 投递没有重试策略。

在 Debezium properties 中，`signal.enabled.channels=jmx` 会作为 `in-process` 的迁移别名接受。Debezium JMX channel 是 JVM MXBean 背后的内存队列，因此 Rustium 通过 `ConnectorRuntime::signal_sender()` 和 `POST /v1/connector/signals` 暴露等价的有界队列，不会伪装成 JVM MBean。解析器会发出明确说明该映射的兼容警告；同时配置 `jmx` 与 `in-process` 只会创建一个 channel。

Execute payload 还接受 Debezium `additional-conditions`；每项包含一个 `data-collection` 正则和一个 SQL `filter`。该 filter 同时约束最大捕获主键与每次 chunk 查询。可选 `surrogate-key` 在该列为 `NOT NULL` 且具有有效单列唯一索引时替代主键进行 chunk 排序；目标表仍需主键用于 WAL 去重。`pause-snapshot` 在当前有界 chunk 后暂停，`resume-snapshot` 从已 checkpoint 的主键继续，`stop-snapshot` 可停止全部工作，或只停止可选 `data-collections` 列表匹配的集合。

信号表必须按顺序且仅包含 `id`、`type`、`data`，并加入 publication。被选中的快照表必须有主键。Rustium 写入 `snapshot-window-open` 和 `snapshot-window-close` watermark，在窗口打开期间移除已被 WAL 事件覆盖的行，以 Debezium `op=r`、`source.snapshot=incremental` 发出剩余行，并在 close 事务中 checkpoint 下一主键、condition 和 pause 状态。重启可能重复尚未提交的 chunk，但不会跳过。原生 YAML 使用 `source.signal_data_collection`、`source.incremental_snapshot_chunk_size` 和 `source.incremental_snapshot_watermarking_strategy`。

设置 Debezium `read.only=true` 或原生 `source.read_only: true`，即可用 `pg_current_snapshot()` 低/高事务水位替代插入 watermark。Rustium 将 WAL transaction ID 与这些快照比较，直到跨 chunk 可见的事务全部通过后才关闭窗口，并执行相同的主键去重。连接器只需要捕获表 `SELECT` 和逻辑复制权限，不写入 watermark 记录。只有使用 `source` channel 时才仍需 source 信号表。

当前 PostgreSQL 信号能力支持 Debezium `source`、`file`、`in-process`、`kafka` channel、JMX 到管理通道的迁移别名和增量快照 action。可写模式支持 `insert_insert`，只读模式使用事务快照。当 `incremental.snapshot.allow.schema.changes=false` 时，Rustium 会在每次打开窗口后比较 catalog，并拒绝活动表发生变化的 WAL `Relation`；它保留旧 checkpoint，不会查询或发出布局不匹配的数据。已完成 checkpoint 还要求原 replication slot 存在且 WAL status 仍保留所需历史；slot 缺失、`unreserved` 或 `lost` 时会在建立 stream 前明确失败并要求 reset + resnapshot，不会静默创建新 slot。Debezium PostgreSQL 连接器不支持增量快照期间的 schema change，因此 Rustium 会拒绝 `incremental.snapshot.allow.schema.changes=true`，不会暴露不安全语义。历史 `Relation` 重放现在会优先复用 checkpoint 中匹配的类型元数据；catalog 与历史都不可用时使用保守的 `unknown_oid_*` 名称。PostgreSQL 剩余门槛是在安装相应扩展的服务器上执行 PostGIS/pgvector 实测，以及真实 broker Kafka 端到端恢复覆盖。

### MySQL

MySQL 连接器通过原生复制协议读取行级二进制日志。

已实现能力：

- MySQL 8.0+ 的 `log_bin`、`binlog_format=ROW`、row image、源 server ID 和选表校验
- `FLUSH TABLES WITH READ LOCK` 加 repeatable-read 一致性快照
- 捕获 binlog 文件、位置、GTID 状态和源 server ID
- write、update、delete 行事件，包括多行事件
- 事务 GTID、全局顺序和集合内顺序
- 兼容 Debezium 的 `gtid.source.includes`、`gtid.source.excludes` 和可选 source-based DML 过滤
- 持久化 MySQL schema history，并在 binlog 重放前恢复
- 按顺序应用 `CREATE TABLE`、`ALTER TABLE` 增删/重命名/修改/变更列、`DROP TABLE` 和 `RENAME TABLE`
- 使用可重放的 table-map 锚点和行序号，从多行事件内部精确恢复
- 从最后安全源位点自动重连 binlog，并使用有限、可观测的重试预算
- 从最新安全 binlog 位点周期发送 heartbeat record，默认关闭
- 可选地在 heartbeat 周期通过独立普通 MySQL 连接执行 `heartbeat.action.query`
- 支持 FULL、MINIMAL、NOBLOB row image；MySQL 未提供的值会明确标记为 unavailable
- 根据完整 before image 重建 PARTIAL_JSON 更新 diff；无法安全重建时保守标记为 unavailable
- 统一以 UTC 读取 temporal 值，并验证 boolean、有符号/无符号整数、decimal、float、bit/binary、时间、字符串、JSON、ENUM、SET 和 null 在快照/binlog 路径上一致
- 支持源表、文件、进程内和 Kafka 增量快照信号，并将主键 keyset 进度与已完成 signal ID 持久化到 connector state
- 使用低/高 binlog 坐标窗口，在 chunk commit 前移除已被并发 create、update 或 delete 覆盖的增量 read 行
- MySQL 8.4 Docker 和外部集成测试，包括过滤后的 GTID 启动、破坏性 DDL 重启、keyset 重启和并发写入去重

建议给 MySQL 连接器用户授予：

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

MySQL Debezium 风格示例见 [examples/mysql.properties](examples/mysql.properties)。

将 `heartbeat.interval.ms` 设置为正数即可发送 heartbeat record。默认 topic 为 `__debezium-heartbeat.<topic.prefix>`；`topic.heartbeat.prefix` 可修改前缀，迁移时仍兼容 `heartbeat.topics.prefix`，`topic.heartbeat.name` 可覆盖完整 topic。heartbeat key 为 `{"serverName":"<connector-name>"}`，value 为 `{"ts_ms":...}`。原生 YAML 使用 `source.heartbeat_interval`、`source.heartbeat_topics_prefix` 和 `source.heartbeat_topic_name`。

`heartbeat.action.query` 可选地在每个正 heartbeat 周期先通过独立普通 MySQL 连接执行。查询失败会携带数据库错误停止 Source；查询产生的 binlog 变化只有在复制流实际读到后才算源进度。原生 YAML 使用 `source.heartbeat_action_query`。

DDL 默认解析失败即停止连接器。可使用 Debezium 兼容参数 `schema.history.internal.skip.unparseable.ddl=true` 警告后跳过不支持的 DDL，但这可能导致 schema 元数据不完整。

`gtid.source.includes` 和 `gtid.source.excludes` 接受逗号分隔、大小写不敏感的正则表达式，并对完整 GTID source UUID 进行匹配；两者最多只能配置一个。只要其中一个存在，Rustium 就会过滤完整捕获的 executed-GTID SID 集合，并在至少保留一个 source 时使用基于 GTID 的启动。若没有已执行 source 匹配，则记录日志并回退到已捕获的 binlog 文件和位置。流式 checkpoint 保存的是事务 GTID，而不是完整 executed set，因此从这类 checkpoint 重连时会刻意保持精确 file/position 路径。默认 `gtid.source.filter.dml.events=true` 会抑制不匹配 source 的行事件，但其事务 commit 边界仍会推进安全 checkpoint，DDL 也仍会进入 schema history。设置为 `false` 可保留全部 DML，同时继续过滤完整 GTID 恢复锚点。原生 YAML 使用 `source.gtid_source_includes`、`source.gtid_source_excludes` 和 `source.gtid_source_filter_dml_events`。

Checkpoint v1 JSON 仍可读取，但已完成的 MySQL v1 checkpoint 不含历史 schema 基线，因此会拒绝恢复。升级后需要重置该 checkpoint 并执行一次新的 initial snapshot，以建立 checkpoint v2 schema history。

MySQL 已支持源表、文件、进程内和 Kafka signal，并兼容 Debezium 的 `execute-snapshot`、`pause-snapshot`、`resume-snapshot`、`stop-snapshot` 控制。增量快照要求目标表具有主键；每个集合会固定最大主键，并使用带类型的单列或复合主键 keyset 查询推进，不再使用 `LIMIT/OFFSET`。每个 chunk 都会在查询前后捕获低/高 binlog 坐标，先缓存 read 行，在复制流追到高水位期间正常发出 CDC record，并移除 `(low, high]` 内发生变化的 before/after 主键。剩余行与 chunk commit 一起发出，同时持久化当前主键、最大主键、集合、暂停状态和有界的已完成 signal ID 历史。重启会从最后 checkpoint 的主键之后继续；已完成的 execute signal 重放会被忽略，包括 connector checkpoint 与 Kafka offset commit 之间的崩溃窗口。未提交的内存窗口在重连后会丢弃并重读；窗口打开期间观察到 schema change 时，Source 会在发出布局不匹配的行之前停止。事件循环每轮最多执行一个 chunk，因此可以在 chunk 之间处理 binlog 和控制信号。Kafka 使用现有的单 partition、与 checkpoint 绑定的 `rustium-signal-kafka` channel 以及同一 Debezium topic/key 合约。Java 专用 truststore/keystore 转换、空间类型和少见 DDL fixture 仍是后续门槛。启用 `binlog_row_value_options=PARTIAL_JSON` 时，如果 diff 没有完整 before image，仍会明确标记为 unavailable，而不会猜测结果。

MySQL 源表信号需要配置 `signal.data.collection=database.signal_table`，并让连接器用户对信号表具有 `SELECT` 权限；连接器不会向信号表写入数据。信号表必须提供 `id`、`type`、`data` 三列。文件信号每行一个 JSON envelope，读取后清空文件；进程内信号复用其他连接器的 `SignalSender` 和 HTTP 管理端点；Kafka signal 复用单 partition、与 checkpoint 绑定的 `rustium-signal-kafka` channel。

### Debezium 配置兼容

Rustium 同时接受严格的原生 YAML 和 Debezium 风格 Java `.properties`。项目优先采用熟悉的参数名，减少现有部署迁移时的配置改动。

当前已映射的 PostgreSQL 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`、`database.dbname`
- `database.sslmode`、`plugin.name`、`slot.name`、`publication.name`
- `schema.include.list`、`schema.exclude.list`、`table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`
- `heartbeat.interval.ms`、`heartbeat.action.query`、`topic.heartbeat.prefix`、`heartbeat.topics.prefix`、`topic.heartbeat.name`
- `signal.data.collection`、`signal.enabled.channels`、`signal.file`、`signal.poll.interval.ms`
- `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms`、`signal.consumer.*`
- `incremental.snapshot.chunk.size`、`incremental.snapshot.allow.schema.changes`、`incremental.snapshot.watermarking.strategy`
- `read.only`
- `hstore.handling.mode`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

当前已映射的 MySQL 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`
- `database.server.id`、`database.ssl.mode`、`database.ssl.ca`、`database.ssl.cert`、`database.ssl.key`
- `database.include.list`、`database.exclude.list`
- `table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`、`connect.timeout.ms`
- `connect.keep.alive`、`connect.keep.alive.interval.ms`
- `gtid.source.includes`、`gtid.source.excludes`、`gtid.source.filter.dml.events`
- `heartbeat.interval.ms`、`heartbeat.action.query`、`topic.heartbeat.prefix`、`heartbeat.topics.prefix`、`topic.heartbeat.name`
- `signal.data.collection`、`signal.enabled.channels`、`signal.file`、`signal.poll.interval.ms`
- `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms`、`signal.consumer.*`
- `incremental.snapshot.chunk.size`、`incremental.snapshot.watermarking.strategy`
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
- `heartbeat.interval.ms`、`heartbeat.action.query`、`topic.heartbeat.prefix`、`heartbeat.topics.prefix`、`topic.heartbeat.name`
- `signal.data.collection`、`signal.enabled.channels`、`signal.file`、`signal.poll.interval.ms`
- `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms`、`signal.consumer.*`
- `incremental.snapshot.chunk.size`、`incremental.snapshot.allow.schema.changes`、`incremental.snapshot.watermarking.strategy`
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
- 由 `streaming.fetch.size` 控制的有界 CDC 查询，包括同一 commit LSN 内的继续读取
- 从最新安全 CDC 位点发送周期 heartbeat，并可通过独立 SQL 连接执行 `heartbeat.action.query`
- 快照和 CDC 共用 SQL 投影，保证核心 numeric、binary、UUID、temporal 和 text 转换一致
- 支持 source table、file、in-process 和 Kafka signal channel，并执行带 checkpoint 的主键增量快照
- 通过 CDC 观察 open/close watermark，缓存每个 chunk，并移除已被并发 create、update 或 delete 覆盖的行

当前实现要求 `database.names` 只有一个数据库、每张选表只有一个活动 capture instance，并使用 `data.query.mode=direct`。增量快照要求主键，会固定最大主键，并持久化带类型的单列/复合 keyset 进度和有界的已完成 signal 历史。所有增量快照 channel 还必须配置 `signal.data.collection`：该表必须按顺序且仅包含文本兼容的 `id`、`type`、`data` 列，`id` 至少容纳 42 个字符，表必须启用 CDC，连接器用户必须具有 `INSERT`。这些约束和对象权限会在 Source 校验阶段检查。Rustium 等待 CDC 观察到 open watermark 后缓存 chunk，根据 CDC before/after image 移除主键，并只在 CDC 观察到 close watermark commit 后发出剩余 read。信号表行不会暴露为业务事件。事件循环只在没有活动 CDC 事务时逐次处理一个 chunk，因此 pause/resume/stop 会在 chunk 之间生效。未提交的内存窗口会在重启时丢弃，Source 会在查询前和 close 时再次校验表 schema。

SQL Server 2022 Developer RTM-CU25 外部门槛已验证快照切换、fetch size 为 1 时跨 update before/after 的继续读取、保持事务序号的事务中间 checkpoint 重启、按 commit LSN 排序的并发事务、retention fail-closed、heartbeat/action-query、核心快照/CDC 类型一致性、in-process keyset 重启、带 additional condition 的 source-table signaling、CDC window 并发更新去重和资源清理。示例见 [examples/sqlserver.properties](examples/sqlserver.properties)。

数据库必须启用 CDC，SQL Server Agent 必须运行 capture job，连接器用户需要读取源表，并能直接读取 `cdc` schema。独立的 Docker 可移植性测试仍可通过以下命令运行：

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 格式与 Sink

内部模型保留 null、有符号/无符号整数、decimal 文本、浮点数、binary、date/time/timestamp、UUID、JSON、array 和 unavailable 值。

可用 Encoder：

- `rustium_json`：带版本的原生事件
- `debezium_json`：`before`、`after`、`source`、`op`、`ts_ms`、事务元数据和 heartbeat record

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
| `POST /v1/connector/signals` | 启用变更端点和 channel 时提交 Debezium 兼容 in-process 信号 |
| `GET /metrics` | Prometheus 指标 |

### 文档与贡献策略

- 面向用户的文档必须先提供完整英文，再提供完整简体中文。
- 代码、配置键、API、日志、Issue 和提交信息使用英文。
- 行为变更必须补测试，尤其是恢复和确认顺序测试。
- Commit 必须包含 DCO `Signed-off-by`。

规范架构和连接器设计见 [docs/design.md](docs/design.md)。

### 许可证与独立性

Rustium 使用 [Apache License 2.0](LICENSE)。Rustium 与 Debezium 或 Red Hat 没有关联、背书或 fork 关系。文档引用 Debezium 仅用于行为和迁移兼容。
