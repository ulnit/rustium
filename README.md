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
| Bounded Tokio pipeline and graceful shutdown | Implemented; required 256-cycle runtime backpressure/retry soak enforced by CI |
| At-least-once sink/checkpoint/source acknowledgement ordering | Implemented |
| SQLite checkpoint v2 with versioned connector state | Implemented and unit tested; v1 JSON remains readable |
| Native JSON, Debezium-compatible JSON, Confluent-framed JSON Schema, Avro, and Protobuf, including delete tombstones | Implemented; real Registry/Kafka gates enforced by CI |
| stdout sink | Implemented |
| Kafka sink with idempotent producer settings | Implemented; real-broker delivery and failure gate enforced by CI |
| PostgreSQL 14+ snapshot, `pgoutput`, persistent schema history, heartbeat records, multi-channel signaling, incremental snapshots, and core type matrix | Implemented; external gates pass with PostgreSQL 17 |
| MySQL 8+ snapshot, row-binlog streaming, GTID source filters, persistent schema history, and heartbeat records | Implemented; required Docker CI and external MySQL 8.4 recovery/soak gates pass |
| SQL Server CDC | Implemented; required Docker CI and external SQL Server 2022 recovery/soak gates pass |
| CLI, health, status, stop, and Prometheus endpoints | Implemented |
| Reproducible non-root container image and Helm chart source | Implemented; packaging gate runs in CI |
| Published container image, Helm chart, and crates | Not published yet |

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

Run the required shared runtime soak gate:

```bash
RUSTIUM_RUNTIME_SOAK_CYCLES=256 \
cargo test -p rustium-core --test runtime_soak -- --ignored --nocapture
```

The gate repeatedly holds a capacity-one Source queue behind a retrying Sink and requires byte-identical batch replay, an unchanged checkpoint until delivery succeeds, ordered final progress, bounded queue admission, exact retry metrics, and Sink shutdown. Separate repeated paths exhaust the finite retry budget and cancel an unbounded 60-second retry wait, proving that the Source is cancelled, the Sink is closed, unacknowledged positions remain uncheckpointed, operational exhaustion enters `FAILED`, and cancellation exits promptly in `STOPPED` without counting a failed event. `RUSTIUM_RUNTIME_SOAK_CYCLES` accepts `1..10000`; CI requires 256 cycles.

Build and verify the production container and Helm chart:

```bash
bash scripts/test-packaging.sh
```

The multi-stage image compiles Rustium with the locked workspace dependencies, contains only the runtime libraries needed by the Kafka/TLS clients, runs as UID/GID `65532`, uses `/var/lib/rustium` for the SQLite checkpoint, and exposes the management server on port `8080`. The packaging gate runs `rustium --version`, checks OCI labels and non-root metadata, lints and renders the Chart, verifies its live/ready probes, read-only root filesystem, retained checkpoint PVC, external configuration Secret mode, and rejects more than one replica. Published image and Helm OCI coordinates are intentionally not claimed until a tagged release is created.

Run the real MySQL 8.4 integration test:

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

This required CI gate uses a dynamically assigned host port and a capacity-one Source output. It runs three cycles that fill the bounded channel, forcibly terminate the active binlog dump connection, commit another transaction while disconnected, and require a different replication connection plus every expected row in first-seen source order. Set `RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES=1..1000` for a longer soak. The gate also stops Rustium, writes an old-schema row, applies destructive DDL, writes a new-schema row, and verifies that restart decoding uses the persisted historical schema before applying the binlog DDL in order.

Run the external MySQL 8.0+ integration test without storing credentials in the repository:

```bash
export RUSTIUM_MYSQL_TEST_HOST=mysql.example.com
export RUSTIUM_MYSQL_TEST_PORT=3306
export RUSTIUM_MYSQL_TEST_ADMIN_USER=root
export RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_USER=cdc
export RUSTIUM_MYSQL_TEST_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo
export RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

The admin account only creates and removes uniquely named test tables and terminates the test's replication session. The CDC account verifies three capacity-one backpressure/reconnect cycles by default, idle periodic heartbeats, `heartbeat.action.query`, snapshot/replication, GTID-filtered startup from the exact server UUID, checkpoint recovery, destructive-DDL recovery, typed keyset restart, durable completed signal IDs, deduplication of an update committed while an incremental chunk window is open, and snapshot/binlog equality for scalar and OGC spatial values. When the Kafka variable is set, the same gate also verifies a real connector-level Kafka signal replay after a completed checkpoint but before the original Kafka offset is acknowledged. This gate has passed against MySQL 8.4 with row binlog and GTID enabled.

Run the external PostgreSQL 14+ integration test without storing credentials in the repository:

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
export RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

The tests create uniquely named tables, signal tables, publications, replication roles, managed slots, signal files, and Kafka topics. They cover snapshot handoff, transaction ordering, checkpoint stop, destructive DDL with historical `Relation` replay, restart without a repeated snapshot, repeated forced termination and automatic recovery of the active replication backend while the Source output is backpressured, explicit failure after a checkpoint's replication slot is lost, all four checkpoint/slot mismatch strategies in both LSN directions, all three LSN flush ownership modes including unmonitored WAL keepalive flushing, transactional and non-transactional logical decoding messages with filtering and raw binary content, immediate acknowledgement-driven `confirmed_flush_lsn` feedback with a bounded flush timeout, periodic heartbeat records, `heartbeat.action.query`, heartbeat-table filtering, resumable source/file/in-process/Kafka-signaled incremental snapshots, immediate external-signal state checkpointing, durable completed signal IDs, real-broker replay across the connector-checkpoint/Kafka-offset crash window, additional conditions, concurrent-update deduplication, pause/resume/scoped-stop controls, read-only transaction-snapshot watermarking with no signal-table writes, file and in-process read-only signaling without any signal table, validated surrogate-key ordering, and identical snapshot/WAL conversion for high-precision numeric, special values, JSONB, UUID, bytea, temporal, network, range, bit, array, hstore, domain, enum, and tsvector types. These gates pass against PostgreSQL 17 with `wal_level=logical`. An opt-in fixture also temporarily constrains `max_slot_wal_keep_size`, generates WAL, forces checkpoints, and verifies explicit failure after `wal_status=lost`; it restores the original setting before returning. Set `RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST=true` only on an isolated superuser test instance. The required repository Docker fixture installs pgvector and PostGIS on PostgreSQL 17, verifies vector, halfvec, sparsevec, geometry, and geography equality across snapshot/WAL paths, and runs three capacity-one backpressure/reconnect cycles without losing expected rows or first-seen source order; GitHub CI runs it on every push and pull request. Set `RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES` from `1` through `1000` to increase the cycle count for a longer soak. A separate librdkafka MockCluster gate verifies Kafka key filtering, single-partition consumption, and offset commit only after durable signal acknowledgement.

Run the reproducible PostgreSQL 17 extension gate locally with Docker:

```bash
bash scripts/test-postgresql-extensions.sh
```

The same gate can use a remote Docker context while keeping database traffic on an authenticated SSH tunnel. Select the remote context, set `RUSTIUM_POSTGRES_DOCKER_SSH_HOST` to an SSH host that reaches that daemon, and optionally override the PostgreSQL base image for a trusted registry mirror or preloaded cache. `RUSTIUM_POSTGRES_DOCKER_SSH_LOCAL_PORT` defaults to `55433`.

```bash
docker context use remote-docker
RUSTIUM_POSTGRES_DOCKER_SSH_HOST=docker-host \
RUSTIUM_POSTGRES_EXTENSION_BASE_IMAGE=mirror.example.com/postgres:17 \
bash scripts/test-postgresql-extensions.sh
```

Run the required Kafka Sink and Schema Registry gate with Docker. It starts an isolated Redpanda broker and Confluent-compatible registry, verifies ordered key/payload/header delivery, JSON Schema, Avro, and Protobuf registration/evolution, binary decoding by registered schema ID, Confluent framing and Protobuf message indexes, schema lookup by ID and subject, true null tombstones, and an explicit failure for a missing topic, then removes the container. Set `RUSTIUM_KAFKA_TEST_PORT` or `RUSTIUM_SCHEMA_REGISTRY_TEST_PORT` when port `19092` or `18081` is occupied.

```bash
bash scripts/test-kafka-sink.sh
```

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
export RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

The tests create uniquely named business tables, signal tables, capture instances, and Kafka topics. They verify snapshot rows, CDC initialization, ordered transactional create/update/delete events, commit boundaries, checkpoint restart without snapshot replay, repeated capacity-one backpressure plus forced polling-session termination and recovery from the unchanged typed CDC cursor, heartbeat/action-query, incremental snapshot keyset recovery and concurrent-update deduplication, durable completed signal IDs, real-broker replay across the connector-checkpoint/Kafka-offset crash window, and cleanup. Set `RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES=1..1000` to change the default three recovery cycles.

### Embed Rustium in a Rust Project

Running the `rustium` CLI as a separate process is the recommended production boundary. Applications that need in-process lifecycle control or a custom `Sink` can assemble the same public crates used by the CLI.

The crates are not published to crates.io yet, so add the required workspace packages as Git dependencies. Cargo records the resolved commit in `Cargo.lock`; use a `rev` instead of `branch` when your release process requires an explicit source pin.

```toml
[dependencies]
rustium-config = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-core = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-avro = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-json = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-protobuf = { git = "https://github.com/ulnit/rustium", branch = "main" }
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

    let source = Box::new(
        PostgresSource::new(
            &config.metadata.name,
            source_config,
            config.snapshot.clone(),
        )
        .with_retry_policy(config.runtime.retry_policy()),
    );
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
            errors_max_retries: config.runtime.errors_max_retries,
            errors_retry_delay_initial: config.runtime.errors_retry_delay_initial,
            errors_retry_delay_max: config.runtime.errors_retry_delay_max,
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

- PostgreSQL 14+ validation, Debezium-compatible publication autocreation, managed or external slot ownership, and PostgreSQL 17 failover slots
- exported consistent snapshot and bounded paginated table reads
- Debezium-compatible `snapshot.isolation.mode` with exported and no-export slot handoffs
- Debezium-compatible bounded upfront snapshot locking against concurrent DDL
- insert, update, delete, and truncate events
- transaction ordering and same-LSN event ordinals
- transactional and non-transactional logical decoding messages with prefix filtering
- TOAST-unavailable handling
- Debezium-compatible `schema.refresh.mode` with relation-driven pgoutput schema safety
- restart recovery from SQLite checkpoints
- replication feedback only after sink acknowledgement and checkpoint persistence
- Debezium-compatible replication status cadence and TCP keepalive controls
- explicit checkpoint/slot reconciliation through all Debezium offset mismatch strategies
- Debezium-compatible managed-slot cleanup on orderly stop through `slot.drop.on.stop`
- connector-, external-, or driver-owned LSN flushing through `lsn.flush.mode`
- bounded acknowledgement flush I/O with Debezium `lsn.flush.timeout.ms` and `lsn.flush.timeout.action`
- pgoutput replication-origin filtering through Debezium `slot.stream.params`
- ordered ordinary-connection session setup through `database.initial.statements`
- server-authenticated PostgreSQL TLS with `verify-ca` / `verify-full` and `database.sslrootcert`
- Debezium-compatible unknown datatype omission or opaque bytes through `include.unknown.datatypes`
- Debezium-compatible PostgreSQL MONEY scale and HALF_UP conversion through `money.fraction.digits`
- schema discovery and table include/exclude regular expressions
- transactional `replica.identity.autoset.values` management for selected publication tables
- partition-root snapshot and WAL identity through `publish.via.partition.root`
- checkpointed PostgreSQL schema history restored before WAL replay
- relation-driven historical column layout, type OID/typmod, key metadata, and schema version increments after table DDL
- periodic heartbeat records from the latest safe WAL position, with optional `heartbeat.action.query`
- one shared text conversion path for snapshot and WAL values, including exact numeric precision, arrays, domains, enums, `hstore`, `tsvector`, pgvector values, and spatial EWKB
- Debezium column transformations across initial snapshots, incremental snapshots, and WAL: anchored case-insensitive `schema.table.column` selectors, fixed priority `truncate -> mask -> hash V1 -> hash V2`, NULL-preserving hashes, NULL-replacing fixed masks, and declared-length hash shortening
- source-table, file, in-process, and Kafka `execute-snapshot` signaling with bounded, checkpointed incremental snapshots
- `read.only=true` incremental watermarking without connector writes to the signal table

The source requires `wal_level=logical` plus replication and table-read permissions. Native YAML defaults `source.publication_autocreate_mode` to `disabled`, preserving the existing-publication contract and the semantic fingerprint of older configurations. Debezium properties default `publication.autocreate.mode` to `all_tables`, matching Debezium. Supported values are `disabled`, `all_tables`, `filtered`, and `no_tables`. `filtered` creates or replaces the publication table set from the source filters and always includes a configured signal table; it rejects an existing `FOR ALL TABLES` publication. `no_tables` creates an empty publication that may receive tables later, and Rustium discovers the first streamed table schema without a restart. Autocreation requires PostgreSQL publication privileges; `all_tables` additionally requires superuser authority, table-scoped modes require ownership of the published tables, and updating an existing publication requires ownership of that publication. See [examples/postgresql.yaml](examples/postgresql.yaml).

Debezium `replica.identity.autoset.values` accepts comma-separated `<fully-qualified-table-regex>:<identity>` rules, where identity is `DEFAULT`, `FULL`, `NOTHING`, or `INDEX <index-name>`. Rustium applies fully matched rules only to selected business tables in the publication. It resolves every match before executing DDL, rejects multiple rules matching one table, skips already-correct identities, and applies all required `ALTER TABLE ... REPLICA IDENTITY` statements in one transaction. Validation therefore requires ownership of affected tables and can modify database metadata. PostgreSQL enforces that an `INDEX` target is unique, immediate, non-partial, and covers only `NOT NULL` columns. Native YAML uses structured `source.replica_identity_autoset_values` entries with `table`, `identity`, and an `index` field required only for index mode.

Set Debezium `publish.via.partition.root=true`, or native `source.publish_via_partition_root: true`, to create publications with `WITH (publish_via_partition_root = true)`. Partition snapshots and streamed changes are then attributed to the partition root, keeping collection identity and topic routing stable across leaf partitions. Rustium compares this setting with `pg_publication.pubviaroot` for an existing publication and fails validation on a mismatch; alter or recreate the publication explicitly rather than accepting silent leaf/root routing changes.

Set Debezium `slot.failover=true`, or native `source.slot_failover: true`, to enable PostgreSQL 17 failover synchronization for a managed logical slot. Rustium checks `server_version_num` and `pg_is_in_recovery()` before slot configuration. A PostgreSQL 17+ primary creates or updates the slot with `FAILOVER true`; older servers and standby nodes follow Debezium's fallback behavior, log a warning, and use a regular logical slot. Native `slot_ownership: external` rejects `slot_failover=true` because Rustium does not mutate externally owned slots. The default is false and preserves older semantic fingerprints.

Debezium `slot.drop.on.stop` maps to native `source.drop_slot_on_stop` and defaults to false. When enabled, an orderly Rustium cancellation first stops and releases the replication transport, then removes the managed logical slot through an ordinary database connection. Replication failures, reconnects, output failures, process crashes, and forced shutdowns do not invoke this cleanup, so recoverable history is not discarded on an abnormal exit. External slot ownership rejects the option. The setting is operational and does not change the semantic fingerprint. Enabling it is discouraged for continuously captured production sources because every stopped interval becomes an unrecoverable CDC gap after the next slot is created.

Debezium `snapshot.locking.mode` maps to native `source.snapshot_locking_mode` and defaults to `none`. Set `shared` to acquire deterministic `ACCESS SHARE` locks for every business table admitted by the initial or `when_needed` snapshot filter, plus the configured signal table, before Rustium reads any captured schema or row. These locks permit ordinary DML but block concurrent DDL until the snapshot transaction ends. `snapshot.lock.timeout.ms` maps to native `source.snapshot_lock_timeout`, defaults to 10 seconds, and bounds both captured-table discovery and each table lock; zero preserves PostgreSQL's unlimited-wait behavior. Values above PostgreSQL's `2147483647ms` limit fail validation, and Debezium's Java `custom` SnapshotLock SPI fails explicitly. Both settings are operational and excluded from semantic fingerprints. Use `shared` when schema changes cannot be suspended for the full snapshot window.

`snapshot.isolation.mode` maps to native `source.snapshot_isolation_mode` and accepts Debezium's `serializable` (the default), `repeatable_read`, `read_committed`, and `read_uncommitted` values. Serializable and repeatable-read snapshots use PostgreSQL's exported snapshot handoff, preserving a single consistent baseline and gap-free WAL start. Read-committed and read-uncommitted use a `NOEXPORT_SNAPSHOT` slot and the requested read-only transaction level; PostgreSQL treats `READ UNCOMMITTED` as `READ COMMITTED`. Rustium anchors streaming at the slot's `restart_lsn` in those modes, so changes committed after slot creation are still delivered after the snapshot. The lower modes change the semantic fingerprint and should be reviewed with downstream consumers because they can expose a row set assembled across statement snapshots.

`status.update.interval.ms` controls periodic replication-feedback checks and defaults to 10,000 ms; native YAML uses `source.status_update_interval`. A durable runtime acknowledgement bypasses that cadence and forces an immediate status update, while normal connector mode never reports a flushed/applied LSN ahead of the acknowledged position. `database.tcpKeepAlive` defaults to true and maps to native `source.tcp_keepalive`; it controls libpq TCP keepalive on validation, snapshot, heartbeat, and replication connections. These operational settings do not alter the semantic configuration fingerprint. Explicit PostgreSQL `slot.max.retries` and `slot.retry.delay.ms` act as compatibility fallbacks only when the corresponding shared `errors.*` settings are absent. The slot delay maps to equal initial and maximum delays, preserving Debezium's fixed-delay behavior; `errors.*` remains authoritative when both forms are supplied, and omitted properties retain Rustium's established shared retry defaults.

`xmin.fetch.interval.ms` maps to native `source.xmin_fetch_interval` and defaults to `0`, which disables tracking. With a positive interval, Rustium reads `pg_replication_slots.catalog_xmin` on a dedicated regular connection before the first eligible WAL message, then reuses that value until the interval elapses. The optional `xmin` field is present in PostgreSQL JSON, Avro, and Protobuf source schemas, is null while tracking is disabled or for snapshot records, and is persisted in streaming positions so a checkpoint and its event metadata remain aligned. Query and parse failures stop the source rather than silently publishing stale metadata. A positive interval changes event metadata and therefore enters the semantic fingerprint; the zero default preserves old position serialization and event IDs.

`offset.mismatch.strategy` controls startup when the durable checkpoint and `pg_replication_slots.confirmed_flush_lsn` differ. `no_validation` is the backward-compatible default and starts from the checkpoint. `trust_offset` advances a lagging slot to the checkpoint and rejects a slot that is already ahead. `trust_slot` adopts the slot only when it is ahead, while `trust_greater_lsn` selects the greater position in either direction and advances a lagging slot. Native YAML uses `source.offset_mismatch_strategy`; the deprecated `slot.seek.to.known.offset.on.start=true` maps to `trust_offset`, and the new property takes precedence when both are present. Slot-advancing modes require native `slot_ownership: managed`, and Rustium refuses to advance a mid-transaction checkpoint because PostgreSQL could cross unprocessed transaction records. Adopting an ahead slot intentionally skips local source history, refreshes every selected schema from the catalog, discards any active incremental-snapshot window, and checkpoints that refreshed state with the next record. Non-default strategies therefore change the semantic fingerprint. Use `trust_slot` or `trust_greater_lsn` only when the server-side slot is deliberately authoritative.

`lsn.flush.mode=connector` is the default: only a position acknowledged after Sink delivery and checkpoint persistence can advance PostgreSQL's flushed/applied LSN. `manual` ignores runtime acknowledgements and leaves `confirmed_flush_lsn` under external control; operators must monitor retained WAL and advance the slot themselves. `connector_and_driver` retains connector acknowledgements and also lets replication keepalives flush every LSN received by the transport, including WAL with no published records. Native YAML uses `source.lsn_flush_mode`; deprecated `flush.lsn.source=true` maps to `connector`, false maps to `manual`, and `lsn.flush.mode` takes precedence. Driver flushing can move the slot beyond the local durable checkpoint, so it deliberately weakens Rustium's normal replay guarantee; pair it with `offset.mismatch.strategy=trust_slot` or `trust_greater_lsn` and use it only when bounded WAL retention is more important than replaying an uncommitted local batch. Non-default modes enter the semantic fingerprint.

`lsn.flush.timeout.ms` maps to native `source.lsn_flush_timeout`, must be positive, and defaults to 30,000 ms. It bounds the real standby-status I/O forced by a durable acknowledgement in `connector` and `connector_and_driver` modes. `lsn.flush.timeout.action` maps to native `source.lsn_flush_timeout_action`: `fail` is the default and stops the source, `warn` logs and continues, and `ignore` continues at debug level. The action applies only to elapsed time; a completed I/O error always fails. Manual mode performs no acknowledgement flush. These operational controls do not change the semantic fingerprint.

Debezium `slot.stream.params` accepts semicolon-separated logical-decoder parameters. Rustium uses `pgoutput` exclusively and currently supports its PostgreSQL 16+ `origin=any|none` parameter: `any` includes both local changes and transactions associated with a PostgreSQL replication origin, while `none` includes only local changes. Native YAML uses `source.slot_stream_params: { origin: any }`. Empty parameters preserve the existing fingerprint; a configured origin enters it because changing the filter can change the captured data set. Unsupported names, malformed entries, origin values other than `any` or `none`, and configured origin filtering on PostgreSQL 14/15 fail validation instead of being silently ignored.

`database.initial.statements` is a semicolon-separated list executed in order whenever Rustium establishes an ordinary PostgreSQL connection, including validation, schema discovery, snapshots, heartbeat actions, XMIN metadata, offset reconciliation, incremental snapshots, and orderly slot cleanup. Double a semicolon as `;;` when it belongs inside a statement. Transaction-log replication connections never execute the list, matching Debezium. Native YAML uses `source.database_initial_statements` as a string list and therefore needs no delimiter escaping. Use this property for idempotent session settings such as `SET application_name`, not DML: Rustium can open multiple connections, and statements committed by an earlier connection are not rolled back if a later connection or statement fails. A non-empty list changes the semantic fingerprint.

PostgreSQL `database.sslmode` accepts the libpq values `disable`, `allow`, `prefer`, `require`, `verify-ca`, and `verify-full`; native YAML uses `source.ssl_mode`. `database.sslrootcert`, or native `source.ssl_root_cert`, points to a PEM CA bundle used exclusively by the rustls transport for `verify-ca` and `verify-full`. The same TLS settings apply to validation, snapshots, heartbeat/incremental connections, and the logical replication stream. The required Docker gate proves a correct CA and matching hostname can stream WAL over `verify-full`, while a wrong CA or hostname is rejected. The current `pg_walstream` rustls backend does not support client certificate authentication, so `database.sslcert`, `database.sslkey`, and `database.sslpassword` fail validation rather than being ignored; JVM-specific `database.sslfactory` also fails explicitly.

`include.unknown.datatypes=false` is the Debezium-compatible default and omits columns whose PostgreSQL type Rustium cannot decode. Set it to true, or use native `source.include_unknown_datatypes: true`, to retain each unknown value as a `bytea` field containing the UTF-8 bytes of PostgreSQL's pgoutput/`::text` representation. Initial snapshots, incremental snapshots, and WAL use the same representation. PostgreSQL connector-state version 6 checkpoints OID/typmod and opaque-column state; versions 1 through 5 remain readable and are normalized against exact current-catalog identities before replay. Enabling the property changes the event schema and semantic fingerprint. If Rustium later adds native support for a type, that column can transition from bytes to its logical representation, matching Debezium's compatibility warning.

Debezium `money.fraction.digits` defaults to `2` and maps to native `source.money_fraction_digits`. Rustium removes PostgreSQL's leading currency symbol, comma grouping, sign or accounting parentheses, parses the remaining MONEY value exactly, and applies BigDecimal-compatible `HALF_UP` rounding to the configured signed 16-bit scale. The scale is recorded in the event field type as `money(<scale>)`, and scalar/array values use the same conversion in initial snapshots, incremental snapshots, and WAL. Malformed MONEY text remains the original string instead of producing invalid decimal data. The default preserves existing native fingerprints; a non-default scale changes the event schema, values, and semantic fingerprint.

Debezium `schema.refresh.mode` maps to native `source.schema_refresh_mode` and accepts `columns_diff` (the default) or `columns_diff_exclude_unchanged_toast`. Debezium uses this choice when a decoder row has fewer columns than its cached table schema. Rustium uses `pgoutput` exclusively, where independent `Relation` messages provide the complete column layout and alone drive schema versions; unchanged TOAST markers are row-value omissions, not schema evidence. Both values therefore use the same safe Relation-driven behavior: an omitted value is recovered from a `REPLICA IDENTITY FULL` before image when available, otherwise it becomes `Unavailable`, while the schema version and connector schema state remain unchanged. Invalid values fail configuration. The compatibility choice is operational and does not change the semantic fingerprint.

PostgreSQL column transformations use Debezium's dynamic property names. Selectors are anchored, case-insensitive regular expressions over `schema.table.column`; the first matching rule in the fixed category order `truncate -> fixed mask -> hash V1 -> hash V2` wins. `column.truncate.to.<length>.chars` truncates character or binary values, `column.mask.with.<length>.chars` emits that many `*` characters even for SQL NULL, and both hash forms preserve NULL. Hashes accept the common JCA digest names (`MD2`, `MD5`, `SHA-1`, `SHA-224`, `SHA-256`, `SHA-384`, `SHA-512`, `SHA-512/224`, `SHA-512/256`, and SHA-3 variants); V1 uses Java `ObjectOutputStream` String serialization compatibility bytes, while V2 hashes UTF-8 text. Hash output is lowercase hexadecimal and is shortened to the PostgreSQL declared `char(n)` or `varchar(n)` length. Native YAML uses `source.column_transformations` with `kind: truncate|mask|hash`, `columns`, `length`, and hash `algorithm`, `salt`, and optional `version: v1|v2`. Transformation rules enter the source fingerprint; salts are represented there only by a SHA-256 digest. The PostgreSQL 17 external gate covers snapshot, incremental-snapshot, and WAL values, NULL masking, priority, known Debezium fixtures, bounded hashes, and unchanged non-character columns.

Checkpoint v1 JSON remains readable, but a completed PostgreSQL v1 checkpoint has no historical Relation baseline and is rejected for resume. Reset it and run one new initial snapshot to establish checkpoint v2 schema history.

Set `heartbeat.interval.ms` to a positive interval to enable PostgreSQL heartbeat records. `heartbeat.action.query` optionally runs on a reused ordinary SQL connection at the same cadence; query failures stop the source, and query-generated WAL does not become checkpoint progress until the replication stream observes it. The default topic is `__debezium-heartbeat.<topic.prefix>`; `topic.heartbeat.prefix`, legacy `heartbeat.topics.prefix`, and full-name override `topic.heartbeat.name` follow Debezium naming. Native YAML uses `source.heartbeat_interval`, `source.heartbeat_action_query`, `source.heartbeat_topics_prefix`, and `source.heartbeat_topic_name`.

PostgreSQL domains are converted through their base type, including domain arrays. Enums, ranges, network types, `ltree`, `isbn`, and `tsvector` retain PostgreSQL's canonical text. Debezium `hstore.handling.mode=json` is the default and produces a JSON value; `map` produces a typed `DataValue::Map`. Dense `vector`/`halfvec` values become float arrays, `sparsevec` becomes a map containing `dimensions` and its indexed vector, and PostGIS `geometry`/`geography` retain complete EWKB bytes. Malformed supported extension values fall back to their original string instead of being partially decoded. Native YAML uses `source.hstore_handling_mode`.

Debezium `interval.handling.mode=numeric` is the properties default and emits an `int64` microsecond duration using Debezium's `365.25 / 12` average days per month. `string` emits Debezium's fixed ISO representation such as `P1Y2M3DT4H5M6.789S`. Rustium parses PostgreSQL `postgres`, `postgres_verbose`, `sql_standard`, and `iso_8601` server output styles, including independently signed components and interval arrays. Native YAML defaults `source.interval_handling_mode` to the native-only `postgres` mode so older configurations retain the original PostgreSQL text and semantic fingerprint; selecting `numeric` or `string` changes both. Malformed values remain their original text rather than being partially converted.

PostgreSQL 14+ logical decoding messages emitted by `pg_logical_emit_message` use the Debezium `<topic.prefix>.message` destination, a `{"prefix": ...}` key, and a `message` value block containing the prefix and original content bytes. `message.prefix.include.list` and `message.prefix.exclude.list` are mutually exclusive lists of fully matched regular expressions. Transactional messages preserve the surrounding WAL transaction ID and ordering and become durable only at its commit; non-transactional messages are independently checkpointable. A filtered non-transactional message still advances a position-only safe boundary so it is not replayed forever on an idle source. Debezium properties enable all prefixes by default. Native YAML keeps `source.logical_decoding_messages: false` as the backward-compatible default; enabling it or configuring `source.message_prefix_include_list` / `source.message_prefix_exclude_list` changes the semantic fingerprint. Debezium JSON encodes default bytes as Base64, while Avro and Protobuf retain binary values.

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

The topic defaults to `<topic.prefix>-signal`, must have exactly one partition, and each message key must equal `topic.prefix`. The value uses the same JSON envelope shown above. Rustium forces `enable.auto.commit=false` and `enable.auto.offset.store=false`; after accepting a valid command, it commits the Kafka offset synchronously only after the matching connector state passes Sink delivery, SQLite checkpoint persistence, and Source acknowledgement. Replayed `execute-snapshot` records with an active or recently completed signal ID are idempotently ignored, closing the crash window between the database checkpoint and Kafka offset commit. Native YAML uses `source.signal_kafka_topic`, `source.signal_kafka_bootstrap_servers`, `source.signal_kafka_group_id`, `source.signal_kafka_poll_timeout`, and `source.signal_kafka_consumer_properties`.

The HTTP route additionally requires `rustium.server.enable.mutations=true` or native `server.enable_mutations: true`; it returns `202 Accepted` after bounded queue admission, `403` when mutations are disabled, and `409` when `in-process` is not enabled. `source`, `file`, `in-process`, and `kafka` can be enabled together. A writable incremental snapshot still requires `signal.data.collection` in the publication because Rustium writes its internal open/close watermarks there; `read.only=true` with an external channel does not require a signal table. Valid external control state is checkpointed immediately at the current safe LSN, including on a fresh idle slot. Invalid external records and unsupported actions are logged and skipped, internal watermark action types are rejected, and file delivery has no retry policy.

In Debezium properties, `signal.enabled.channels=jmx` is accepted as a migration alias for `in-process`. Debezium's JMX channel is a JVM MXBean backed by an in-memory queue, so Rustium exposes the equivalent bounded queue through `ConnectorRuntime::signal_sender()` and `POST /v1/connector/signals` instead of pretending to host a JVM MBean. The parser emits a compatibility warning that names this mapping; combining `jmx` and `in-process` creates only one channel.

The execute payload also accepts Debezium `additional-conditions`, each containing a `data-collection` regular expression and a SQL `filter`. The filter constrains both the captured maximum key and every chunk query. Optional `surrogate-key` replaces primary-key chunk ordering when that column is `NOT NULL` and backed by a valid single-column unique index; the table still needs a primary key for WAL deduplication. `pause-snapshot` pauses after the current bounded chunk, `resume-snapshot` continues from its checkpointed key, and `stop-snapshot` stops all work or only collections matched by its optional `data-collections` list.

The signal table must contain exactly `id`, `type`, and `data` in that order and must be part of the publication. Selected snapshot tables must have primary keys. Rustium writes `snapshot-window-open` and `snapshot-window-close` watermarks, removes rows superseded by WAL events while the window is open, emits remaining rows as Debezium `op=r` events with `source.snapshot=incremental`, and checkpoints the next key, conditions, and pause state with the close transaction. Restart can repeat an uncommitted chunk but cannot skip it. Native YAML uses `source.signal_data_collection`, `source.incremental_snapshot_chunk_size`, and `source.incremental_snapshot_watermarking_strategy`.

Set Debezium `read.only=true` or native `source.read_only: true` to replace inserted watermarks with `pg_current_snapshot()` low/high transaction watermarks. Rustium compares WAL transaction IDs against those snapshots, keeps the window open until every transaction visible across the chunk has passed, and applies the same primary-key deduplication. The connector requires only `SELECT` on captured tables plus logical-replication access and writes no watermark records. A source signal table remains necessary only when the `source` channel is used.

Current PostgreSQL signaling supports the Debezium `source`, `file`, `in-process`, and `kafka` channels, the JMX-to-management migration alias, and incremental snapshot actions. Writable mode supports `insert_insert`; read-only mode uses transaction snapshots. Connector-state version 6 retains a bounded history of 1,024 completed or stopped execute signal IDs plus opaque unknown-type column state, so Kafka replay after a completed checkpoint is ignored without repeating rows; versions 1 through 5 remain readable. With `incremental.snapshot.allow.schema.changes=false`, Rustium compares the catalog after opening every window and also rejects a changed WAL `Relation` for the active table, preserving the previous checkpoint instead of querying or emitting mismatched layouts. A completed checkpoint also requires the original replication slot to exist and report a WAL status that still retains the required history; a missing, `unreserved`, or `lost` slot fails before stream creation with a reset-and-resnapshot instruction rather than silently creating a new slot. Transient replication failures use the shared `errors.*` retry policy while the PostgreSQL connector retains ownership of slot and LSN recovery; `0` disables automatic stream recovery and `-1` keeps retrying. Debezium's PostgreSQL connector does not support schema changes during an incremental snapshot, so Rustium rejects `incremental.snapshot.allow.schema.changes=true` instead of exposing unsafe semantics. Historical `Relation` replay falls back to matching checkpointed type metadata, or a conservative `unknown_oid_*` name when both catalog and history are unavailable. PostgreSQL core types, extension types, unknown-type modes, and repeated forced replication-backend recovery under capacity-one output backpressure are reproducible on PostgreSQL 17 and enforced by CI.

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
- automatic binlog reconnect from the last safe source position using the shared Debezium-compatible `errors.*` retry budget
- periodic heartbeat records from the latest safe binlog position, disabled by default
- optional `heartbeat.action.query` execution on a separate ordinary MySQL connection at the heartbeat cadence
- FULL, MINIMAL, and NOBLOB row images with explicit unavailable values where MySQL omits data
- PARTIAL_JSON update diffs reconstructed from a complete before image, with unavailable fallback when safe reconstruction is impossible
- UTC-normalized temporal reads plus snapshot/binlog equality for boolean, signed/unsigned integer, decimal, float, bit/binary, temporal, string, JSON, ENUM, SET, and null values
- source-table, file, in-process, and Kafka signaling for incremental snapshot controls, with primary-key keyset progress and completed signal IDs persisted in connector state
- low/high binlog-coordinate windows that remove incrementally read rows superseded by concurrent create, update, or delete events before the chunk commit
- required Docker CI and external integration coverage against MySQL 8.4, including repeated capacity-one backpressure/reconnect cycles, filtered GTID startup, destructive-DDL restart, keyset restart, and concurrent-write deduplication

Recommended MySQL permissions for the connector user:

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

The MySQL Debezium-style example is [examples/mysql.properties](examples/mysql.properties).

Set `heartbeat.interval.ms` to a positive interval to emit heartbeat records. The default topic is `__debezium-heartbeat.<topic.prefix>`; `topic.heartbeat.prefix` changes its prefix, `heartbeat.topics.prefix` remains accepted for migration, and `topic.heartbeat.name` overrides the full topic. Heartbeats use `{"serverName":"<connector-name>"}` as the key and `{"ts_ms":...}` as the value. Native YAML uses `source.heartbeat_interval`, `source.heartbeat_topics_prefix`, and `source.heartbeat_topic_name`.

`heartbeat.action.query` optionally runs first on a separate ordinary MySQL connection at each positive heartbeat interval. Query failures stop the source with the database error; the query's binlog changes are not treated as source progress until the replication stream observes them. Native YAML uses `source.heartbeat_action_query`.

MySQL reconnect uses `errors.max.retries`, `errors.retry.delay.initial.ms`, and `errors.retry.delay.max.ms`; `0` fails immediately and `-1` retries without a count limit. Backoff resets after new source progress, cancellation interrupts the wait, and `connect.keep.alive=false` still disables automatic recovery. For `.properties` migration, `rustium.source.reconnect.max.attempts` and `connect.keep.alive.interval.ms` remain fallbacks only when the corresponding `errors.*` properties are absent. Direct embedded callers that construct `MySqlSource` without `with_retry_policy` retain those legacy source-config values.

`database.connectionTimeZone` maps to native `source.connection_time_zone` and defaults to `UTC`. Rustium normalizes the accepted `UTC`, `Z`, `Etc/UTC`, and `+00:00` values to a `+00:00` session time zone so snapshot `TIMESTAMP` values match UTC binlog values. Other offsets, named regions, and Debezium's `SERVER` mode fail configuration validation until both capture paths can guarantee identical temporal conversion.

MySQL TLS accepts either PEM/DER material through `database.ssl.ca`, `database.ssl.cert`, and `database.ssl.key`, or Debezium-compatible Java stores through `database.ssl.truststore`, `database.ssl.truststore.password`, `database.ssl.keystore`, and `database.ssl.keystore.password`. Rustium detects JKS and PKCS#12/PFX by content, supports modern PBES2/AES/SHA-256 and legacy PBES1 PKCS#12 files, and converts their certificates and PKCS#8 key directly into in-memory Rustls material. A keystore must contain exactly one private-key entry with a certificate chain. PEM CA and truststore settings are mutually exclusive, as are PEM client identity and keystore settings. Native YAML uses the corresponding `source.ssl_*` names; use environment interpolation for store passwords.

DDL parsing failures stop the connector by default. Debezium-compatible `schema.history.internal.skip.unparseable.ddl=true` can advance past unsupported DDL with a warning, but doing so can leave schema metadata incomplete.

`gtid.source.includes` and `gtid.source.excludes` accept comma-separated, case-insensitive regular expressions matched against the complete GTID source UUID; configure at most one of them. When either property is present, Rustium filters a complete captured executed-GTID SID set and uses GTID-based startup when at least one source remains. If no executed source matches, it logs the condition and falls back to the captured binlog file and position. Streaming checkpoints contain a transaction GTID rather than a complete executed set, so reconnect from those checkpoints deliberately stays on the exact file/position path. With the default `gtid.source.filter.dml.events=true`, row events from a non-matching source are suppressed, but their transaction commit boundary still advances the safe checkpoint and DDL remains visible to schema history. Set the property to `false` to retain all DML while still filtering complete GTID recovery anchors. Native YAML uses `source.gtid_source_includes`, `source.gtid_source_excludes`, and `source.gtid_source_filter_dml_events`.

Checkpoint v1 JSON remains readable, but a completed MySQL v1 checkpoint has no historical schema baseline and is rejected for resume. Reset that checkpoint and run a new initial snapshot once to establish checkpoint v2 schema history.

MySQL supports source-table, file, bounded in-process, and Kafka signaling for Debezium-compatible `execute-snapshot`, `pause-snapshot`, `resume-snapshot`, and `stop-snapshot` controls. Incremental snapshots require a primary key, capture a fixed maximum key per collection, and advance with typed single-column or composite-key keyset queries instead of `LIMIT/OFFSET`. For each chunk, Rustium captures low and high binlog coordinates around the query, buffers the read rows, emits ordinary CDC records while the stream catches up, and removes matching before/after primary keys changed inside `(low, high]`. The remaining rows are emitted with the chunk commit, which also persists the current key, maximum key, collection, pause state, and bounded completed-signal history. Restart resumes after the last checkpointed key; a replayed completed execute signal is ignored, including the crash window between a connector checkpoint and Kafka offset commit. An uncommitted in-memory window is discarded and reread after reconnect, and schema changes observed while a window is open stop the source before mismatched rows are emitted. Only one chunk runs per event-loop turn, so binlog events and control signals are observed between chunks. The external MySQL 8.4 type matrix verifies byte-identical snapshot/binlog handling for `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and `GEOMETRYCOLLECTION`; spatial payloads remain the native MySQL SRID plus WKB bytes. DDL recovery also covers index add/rename/drop and column-default changes without advancing the event-schema version. When configured, the external Kafka gate verifies connector-level signal replay after a completed checkpoint but before the original Kafka offset is acknowledged. When `binlog_row_value_options=PARTIAL_JSON` is enabled, a diff without a complete before image remains explicitly unavailable rather than being guessed.

For MySQL source-table signaling, set `signal.data.collection=database.signal_table` and include the signal table in the connector user's `SELECT` scope. The table must contain exactly three text-compatible columns named `id`, `type`, and `data` in that order; Rustium consumes its binlog inserts as commands, excludes it from business snapshots/events, and does not write to it. File signaling consumes one JSON envelope per line and clears the file only after reading it. In-process signaling uses the same `SignalSender` and HTTP management route as the other connectors. Kafka signaling uses the existing single-partition, checkpoint-coupled `rustium-signal-kafka` channel and the same Debezium topic/key contract.

### SQL Server

The SQL Server connector is implemented on top of native SQL Server CDC change tables.

Implemented behavior:

- SQL Server 2017+ and database CDC validation
- single-database source ownership and capture-instance discovery
- snapshot handoff at `sys.fn_cdc_get_max_lsn()`
- direct CDC change-table reads ordered by commit LSN, sequence value, and operation
- insert/delete conversion and update operation 3/4 before/after pairing
- transaction ordering, mid-transaction replay, and checkpoint recovery
- bounded recovery from transient polling-connection failures without advancing the typed CDC cursor
- explicit failure when CDC cleanup removes the required checkpoint LSN
- bounded CDC queries controlled by `streaming.fetch.size`, including continuation inside one commit LSN
- periodic heartbeat records from the latest safe CDC position, with optional `heartbeat.action.query` on a separate SQL connection
- shared snapshot/CDC SQL projections for consistent numeric, binary, UUID, temporal, text, XML, hierarchyid, geometry, and geography conversion
- source-table, file, in-process, and Kafka signal channels with checkpointed primary-key incremental snapshots
- CDC-observed open/close watermarks that buffer each chunk and remove rows superseded by concurrent create, update, or delete events

The current implementation requires exactly one entry in `database.names`, one active capture instance per selected table, and `data.query.mode=direct`. Incremental snapshots require a primary key, capture a fixed maximum key, and persist typed single/composite keyset progress plus a bounded completed-signal history. Every incremental snapshot channel also requires `signal.data.collection`: the table must contain exactly text-compatible `id`, `type`, and `data` columns in that order, `id` must hold at least 42 characters, the table must be CDC-enabled, and the connector user must have `INSERT`. These constraints and object permission are checked during source validation. Rustium waits until CDC observes the open watermark, buffers the chunk, removes primary keys found in CDC before/after images, and emits remaining reads only when CDC observes the close watermark commit. Signal-table rows are never exposed as business events. The event loop processes one chunk at a time outside active CDC transactions, so pause/resume/stop commands are observed between chunks. An uncommitted in-memory window is discarded on restart, and the source verifies the table schema before the query and again at close.

The SQL Server 2022 Developer RTM-CU25 external gate verifies snapshot handoff, fetch-size-one continuation through update before/after pairs, mid-transaction checkpoint restart with preserved transaction ordinals, three capacity-one polling-session termination cycles with connection-identity replacement, every expected row, and first-seen source order, concurrent transactions ordered by commit LSN, retention fail-closed behavior, heartbeat/action-query, snapshot/CDC equality for the core type matrix plus XML, hierarchyid, geometry, and geography, in-process keyset restart, source-table signaling with additional conditions, CDC-window concurrent-update deduplication, real-broker Kafka replay after a completed connector checkpoint but before the signal offset is committed, and cleanup. Geometry and geography retain the complete native SQL Server `Serialize()` payload as bytes; hierarchyid retains its canonical path and XML retains canonical text. See [examples/sqlserver.properties](examples/sqlserver.properties).

The database must have CDC enabled, SQL Server Agent must run the capture job, and the connector user needs source-table reads plus direct read access to the `cdc` schema. CI runs the separate SQL Server 2022 Docker portability gate on every push and pull request; its capacity-one output performs three session-kill/reconnect cycles by default and requires every subsequent CDC transaction. The gate remains runnable locally with:

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### Debezium Configuration Compatibility

Rustium accepts strict native YAML and Debezium-style Java `.properties` files. Familiar names are preferred so existing deployments can migrate with smaller configuration changes.

Currently mapped PostgreSQL properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`, `database.dbname`, `database.initial.statements`
- `database.sslmode`, `database.sslrootcert`, `database.tcpKeepAlive`, `plugin.name`, `slot.name`, `slot.failover`, `slot.max.retries`, `slot.retry.delay.ms`, `status.update.interval.ms`, `xmin.fetch.interval.ms`
- `slot.stream.params`
- `offset.mismatch.strategy`, deprecated `slot.seek.to.known.offset.on.start`
- `lsn.flush.mode`, `lsn.flush.timeout.ms`, `lsn.flush.timeout.action`, deprecated `flush.lsn.source`
- `publication.name`, `publication.autocreate.mode`, `replica.identity.autoset.values`, `publish.via.partition.root`
- `schema.include.list`, `schema.exclude.list`, `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`, `snapshot.include.collection.list`
- `snapshot.isolation.mode`
- `heartbeat.interval.ms`, `heartbeat.action.query`, `topic.heartbeat.prefix`, `heartbeat.topics.prefix`, `topic.heartbeat.name`
- `signal.data.collection`, `signal.enabled.channels`, `signal.file`, `signal.poll.interval.ms`
- `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, `signal.consumer.*`
- `incremental.snapshot.chunk.size`, `incremental.snapshot.allow.schema.changes`, `incremental.snapshot.watermarking.strategy`
- `read.only`
- `hstore.handling.mode`, `interval.handling.mode`, `include.unknown.datatypes`, `money.fraction.digits`, `schema.refresh.mode`
- `column.truncate.to.<length>.chars`, `column.mask.with.<length>.chars`, `column.mask.hash.<algorithm>.with.salt.<salt>`, `column.mask.hash.v2.<algorithm>.with.salt.<salt>`
- `message.prefix.include.list`, `message.prefix.exclude.list`
- `errors.max.retries`, `errors.retry.delay.initial.ms`, `errors.retry.delay.max.ms`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Currently mapped MySQL properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`
- `database.server.id`, `database.ssl.mode`, `database.ssl.ca`, `database.ssl.cert`, `database.ssl.key`
- `database.ssl.keystore`, `database.ssl.keystore.password`, `database.ssl.truststore`, `database.ssl.truststore.password`
- `database.include.list`, `database.exclude.list`
- `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`, `snapshot.include.collection.list`, `connect.timeout.ms`
- `connect.keep.alive`, `connect.keep.alive.interval.ms`
- `gtid.source.includes`, `gtid.source.excludes`, `gtid.source.filter.dml.events`
- `heartbeat.interval.ms`, `heartbeat.action.query`, `topic.heartbeat.prefix`, `heartbeat.topics.prefix`, `topic.heartbeat.name`
- `signal.data.collection`, `signal.enabled.channels`, `signal.file`, `signal.poll.interval.ms`
- `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, `signal.consumer.*`
- `incremental.snapshot.chunk.size`, `incremental.snapshot.watermarking.strategy`
- `schema.history.internal.skip.unparseable.ddl`
- `rustium.source.reconnect.max.attempts`
- `errors.max.retries`, `errors.retry.delay.initial.ms`, `errors.retry.delay.max.ms`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

Currently mapped SQL Server properties include:

- `name`, `connector.class`, `topic.prefix`
- `database.hostname`, `database.port`, `database.user`, `database.password`
- `database.names`, `database.encrypt`, `database.trustServerCertificate`
- `table.include.list`, `table.exclude.list`
- `snapshot.mode`, `snapshot.fetch.size`, `snapshot.include.collection.list`, `snapshot.isolation.mode`
- `data.query.mode=direct`, `streaming.fetch.size`
- `heartbeat.interval.ms`, `heartbeat.action.query`, `topic.heartbeat.prefix`, `heartbeat.topics.prefix`, `topic.heartbeat.name`
- `signal.data.collection`, `signal.enabled.channels`, `signal.file`, `signal.poll.interval.ms`
- `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, `signal.consumer.*`
- `incremental.snapshot.chunk.size`, `incremental.snapshot.allow.schema.changes`, `incremental.snapshot.watermarking.strategy`
- `errors.max.retries`, `errors.retry.delay.initial.ms`, `errors.retry.delay.max.ms`
- `max.queue.size`, `max.batch.size`, `poll.interval.ms`

`snapshot.include.collection.list` is an anchored, snapshot-only regular-expression filter. An empty list snapshots every table selected by the normal database/schema/table filters. PostgreSQL patterns match `schema.table`, MySQL patterns match `database.table`, and SQL Server patterns match `database.schema.table`. The filter applies to initial snapshots, including recovery snapshots triggered by `snapshot.mode=when_needed`; it does not narrow streaming CDC or incremental snapshots. Native YAML uses `snapshot.include_collections`. Required integration gates create two streaming-selected tables, snapshot only one, and then prove that a new change from the other table is still captured on PostgreSQL 17, MySQL 8.4, and SQL Server 2022.

Common Debezium format properties include `unavailable.value.placeholder` and `tombstones.on.delete`. Tombstones default to enabled in `debezium_json`, `debezium_json_schema`, `debezium_avro`, and `debezium_protobuf`: each delete envelope is followed in the same delivery batch by the same key with a null value. Set `tombstones.on.delete=false` or native YAML `format.tombstones_on_delete: false` to disable them.

Checked-in golden fixtures pin the complete Debezium JSON destination, key, and envelope for PostgreSQL, MySQL, and SQL Server. Companion JSON Schema, Avro, and Protobuf fixtures also pin each connector's destination, Registry subjects, schema type, and complete key/value definitions. CI compares generated contracts against those fixtures so connector-specific source metadata, topic routing, field numbers, and wire-schema drift are reviewed explicitly.

Rustium maps a matching pair of `io.confluent.connect.json.JsonSchemaConverter` values plus `key.converter.schema.registry.url` and `value.converter.schema.registry.url` to `debezium_json_schema`. It also accepts matching `basic.auth.credentials.source=USER_INFO`, `basic.auth.user.info`, and `auto.register.schemas=true`. `rustium.schema.registry.request.timeout.ms` controls each registry request and `rustium.schema.registry.cache.capacity` bounds the successful schema-ID LRU cache. Key and value converters must currently use the same URL list and credentials, the default `TopicNameStrategy`, automatic registration, and explicit generated schemas. Different registries, RecordName strategies, `auto.register.schemas=false`, and `use.latest.version=true` fail validation instead of silently changing compatibility semantics. See [examples/mysql-json-schema.properties](examples/mysql-json-schema.properties).

Matching `io.confluent.connect.avro.AvroConverter` values map to `debezium_avro` with the same Registry URL, authentication, subject strategy, registration, timeout, and bounded-cache contract. Rustium generates valid named Avro key/envelope/source/transaction/row records, serializes raw binary datum with Apache Avro, and leaves Registry registration plus Confluent framing to the Kafka Sink. `schema.name.adjustment.mode` and `field.name.adjustment.mode` may be omitted or set to `avro`; Rustium always performs deterministic Avro adjustment and rejects adjusted field-name collisions. Unsupported adjustment modes fail validation. Unsigned 64-bit integers, decimal values, temporal values, UUIDs, JSON, and otherwise unmodelled extension values use stable string representations; binary database values remain Avro `bytes`, signed integers and floating-point values remain numeric, arrays remain arrays, and hstore/map values remain maps. See [examples/mysql-avro.properties](examples/mysql-avro.properties).

Matching `io.confluent.connect.protobuf.ProtobufConverter` values map to `debezium_protobuf`. Rustium emits one top-level `Key` or `Envelope` message per subject and the Confluent optimized message-index byte `0`. Row fields use generated typed oneof wrappers so native values, explicit null absence, unavailable placeholders, and textual conversion fallbacks remain distinguishable; arrays, multidimensional arrays, maps, binary data, and the full unsigned 64-bit range remain lossless. Protobuf field numbers are deterministically derived from original database column names, remain stable across restart and field reordering, avoid the reserved range, and fail on an active collision. Rustium always adjusts invalid names; `scrub.invalid.names` may be omitted or set to `true`. `optional.for.nullables`, `wrapper.for.nullables`, `generate.struct.for.nulls`, and `flatten.unions` must retain their default `false` values because changing them would select a different wire contract. See [examples/mysql-protobuf.properties](examples/mysql-protobuf.properties).

The native YAML equivalent is:

```yaml
format:
  type: debezium_json_schema
  schema_registry:
    urls: [https://schema-registry:8081]
    username: ${SCHEMA_REGISTRY_USER}
    password: ${SCHEMA_REGISTRY_PASSWORD}
    request_timeout: 10s
    cache_capacity: 1000
```

Use `type: debezium_avro` or `type: debezium_protobuf` with the same `schema_registry` block for native binary format configuration.

Unsupported properties are reported as compatibility warnings instead of being silently treated as implemented. Rustium maps Debezium's `errors.max.retries`, `errors.retry.delay.initial.ms`, and `errors.retry.delay.max.ms` to the shared retry policy used by PostgreSQL replication recovery, MySQL binlog recovery, SQL Server CDC polling recovery, and retryable Sink operations; native YAML uses `runtime.errors_max_retries`, `runtime.errors_retry_delay_initial`, and `runtime.errors_retry_delay_max`. Rustium defaults to 10 retries after the initial attempt, starting at 300 ms and doubling to a 10 s ceiling. Set the retry count to `0` to fail immediately or `-1` for unbounded retries. SQL Server retries only connection and selected transient server failures; permission, conversion, protocol, and CDC-retention errors remain fail-closed. Other Rustium-specific source retry, sink, state, server, logging, and Kafka producer settings use the `rustium.*` prefix.

### Formats and Sinks

The internal model preserves null, signed and unsigned integers, decimal text, floating point, binary, date/time/timestamp, UUID, JSON, array, and unavailable values.

Available encoders:

- `rustium_json`: versioned native event payload
- `debezium_json`: `before`, `after`, `source`, `op`, `ts_ms`, transaction metadata, and heartbeat records
- `debezium_json_schema`: the same Debezium envelope with generated Draft-07 key/value schemas and Confluent Schema Registry wire framing
- `debezium_avro`: named Debezium key/envelope records encoded as Apache Avro binary datum with Confluent Schema Registry wire framing
- `debezium_protobuf`: typed Debezium key/envelope messages encoded as Protocol Buffers with Confluent Schema Registry framing and message indexes

Available sinks:

- `stdout`: development and protocol inspection
- `kafka`: `librdkafka`, replicated acknowledgements, configurable compression/properties, and idempotent delivery

The Kafka Sink accepts only `acks=all` or its `-1` alias because Rustium checkpoints immediately after a successful batch. Allowing `acks=0/1` would acknowledge source progress before Kafka replication and could lose already-checkpointed records. Rustium always enables producer idempotence and owns `bootstrap.servers`, acknowledgement, compression, idempotence, and delivery-timeout properties; those keys cannot be replaced through `sink.properties` or `rustium.kafka.property.*`. Security, batching, and other non-conflicting librdkafka properties remain pass-through. For all schema-aware formats, the Encoder emits a datum plus an immutable schema descriptor; Protobuf also prepends its message-index vector. The Sink registers each distinct subject/definition before Kafka delivery, caches only successful IDs in a bounded LRU, and prefixes key/value bytes with the Confluent magic byte and big-endian schema ID. Network failures remain retryable; compatibility rejections fail closed. This wire path works with Confluent Schema Registry and Apicurio Registry's Confluent compatibility API. The required real-broker CI gate verifies ordered records, all three schema formats and evolution, binary decoding by registered ID, subject lookup, true null tombstones, topic cleanup, and explicit broker failures.

Retryable Sink validation, delivery, and flush failures use cancellation-aware exponential backoff. A delivery retry preserves and replays the same batch, and the checkpoint advances only after that batch succeeds. Cancellation during delivery backoff interrupts the wait, leaves the checkpoint unchanged, performs Source and Sink cleanup, and completes as a graceful stop without incrementing failed-event counters. This prevents skipped source positions, but a failure after partial broker delivery can duplicate records; consumers must therefore remain idempotent. Source reconnection remains connector-owned because WAL slots, binlog table maps, and SQL Server CDC cursors require connector-specific recovery state; PostgreSQL, MySQL, and SQL Server all have connector-specific recovery verified against real databases.

### Management API

The server binds to `127.0.0.1:8080` by default.

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | Process liveness |
| `GET /health/ready` | Connector readiness |
| `GET /v1/connector/status` | State/reason, position, checkpoint and source-event times, lag, queue, and counters |
| `POST /v1/connector/stop` | Graceful stop when mutations are enabled |
| `POST /v1/connector/signals` | Submit a Debezium-compatible in-process signal when mutations and the channel are enabled |
| `GET /metrics` | Prometheus exposition |

Status and metrics advance only after the sink write and checkpoint succeed. `rustium_source_lag_seconds` is the current wall-clock distance from the last durably acknowledged source timestamp and is `NaN` when unavailable; `rustium_checkpoint_age_seconds`, `rustium_last_event_age_seconds`, and `rustium_connector_state_age_seconds` expose low-cardinality operational ages for alerting. `rustium_sink_retry_attempts` counts scheduled Sink retries across validation, delivery, and flush. Encoding and exhausted or non-retryable sink-delivery failures increment `failed_events`, transition the connector to `FAILED`, cancel the Source, and still run Sink shutdown; a Source that ignores cancellation is aborted after `runtime.shutdown_timeout`.

### Container and Kubernetes Deployment

The repository includes a multi-stage `Dockerfile` and a Helm chart at [deploy/helm/rustium](deploy/helm/rustium). The chart intentionally runs one connector replica because SQLite checkpoint ownership and source ordering are single-owner contracts. It defaults to a retained `ReadWriteOnce` PVC, a `Recreate` strategy, non-root UID/GID `65532`, a read-only root filesystem, disabled ServiceAccount token mounting, and `/health/live` plus `/health/ready` probes.

For Kubernetes, create a Secret containing the complete configuration and install the chart:

```bash
kubectl -n rustium create secret generic rustium-config \
  --from-file=rustium.yaml=./rustium.yaml
helm upgrade --install rustium deploy/helm/rustium \
  --namespace rustium --create-namespace \
  --set config.existingSecret=rustium-config
```

The mounted configuration should use `server.bind: 0.0.0.0:8080` and `state.path: /var/lib/rustium/rustium.db`. Interpolate database, Kafka, and Schema Registry credentials from Kubernetes Secret environment variables; do not commit them in values files. See [deploy/helm/rustium/README.md](deploy/helm/rustium/README.md) for the complete English-first bilingual deployment contract.

Tagged releases run [.github/workflows/release.yml](.github/workflows/release.yml). The workflow checks that the tag, Cargo workspace, and Chart versions match, repeats the locked Rust gates, publishes signed multi-architecture (`linux/amd64` and `linux/arm64`) images to GHCR with SBOM/provenance, pushes the Helm chart as an OCI artifact, and creates a GitHub Release with image-digest and SHA-256 checksum files. Crates.io publication remains a separate, reviewed, ordered operation because internal crates must be published from leaves to the CLI.

Crates.io publication is available only through the manual [.github/workflows/publish-crates.yml](.github/workflows/publish-crates.yml) workflow. It requires typing `publish` at dispatch time and configuring the rotated `CARGO_REGISTRY_TOKEN` repository Secret; it verifies the full workspace and publishes core, foundation, connector, and CLI crates in dependency order. The workflow is never triggered by a push or tag.

Every publishable workspace crate carries a concise English-first bilingual README in its package, and the packaging, release, and publication gates verify that the README is included in the Cargo tarball. The same gates require the prioritized connector schema fixtures in each schema-aware format crate.

### Documentation and Contribution Policy

- User-facing documentation is complete English first, followed by complete Simplified Chinese.
- Code, configuration keys, APIs, logs, issues, and commit messages use English.
- Behavioral changes need tests, especially recovery and acknowledgement-order tests.
- Commits must include a DCO `Signed-off-by` line.

See [docs/design.md](docs/design.md) for the normative architecture and connector design. Use [docs/runbook.md](docs/runbook.md) for backup, recovery, alerting, and Kubernetes operations, [docs/upgrades.md](docs/upgrades.md) for checkpoint/configuration migration rules, and [SECURITY.md](SECURITY.md) for vulnerability reporting and secure deployment requirements.

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
| 有界 Tokio 流水线与优雅关闭 | 已实现；CI 强制运行 256 轮 runtime 背压/重试 soak |
| Sink/checkpoint/Source 确认顺序的 at-least-once 语义 | 已实现 |
| 带版本化连接器状态的 SQLite checkpoint v2 | 已实现并通过单元测试；仍可读取 v1 JSON |
| 原生 JSON、Debezium 兼容 JSON、Confluent framing JSON Schema、Avro 和 Protobuf，包括 delete tombstone | 已实现；CI 强制运行真实 Registry/Kafka 门槛 |
| stdout Sink | 已实现 |
| 带幂等 Producer 设置的 Kafka Sink | 已实现；CI 强制运行真实 broker 投递与失败门槛 |
| PostgreSQL 14+ 快照、`pgoutput`、持久 schema history、heartbeat record、多 channel 信号、增量快照和核心类型矩阵 | 已实现；PostgreSQL 17 外部门槛通过 |
| MySQL 8+ 快照、行级 binlog、GTID source 过滤、持久 schema history 和 heartbeat record | 已实现；必选 Docker CI 和外部 MySQL 8.4 恢复/soak 门槛通过 |
| SQL Server CDC | 已实现；必选 Docker CI 和外部 SQL Server 2022 恢复/soak 门槛通过 |
| CLI、健康、状态、停止和 Prometheus 端点 | 已实现 |
| 可复现的非 root 容器镜像与 Helm Chart 源码 | 已实现；CI 强制运行 packaging gate |
| 已发布容器镜像、Helm Chart 与 crate | 尚未发布 |

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

运行必须通过的共享 runtime soak 门槛：

```bash
RUSTIUM_RUNTIME_SOAK_CYCLES=256 \
cargo test -p rustium-core --test runtime_soak -- --ignored --nocapture
```

该门槛会反复让容量为 1 的 Source 队列被重试中的 Sink 阻塞，并要求每次重放的 batch 字节级一致、投递成功前 checkpoint 不变、最终位点有序推进、队列准入有界、重试指标精确且 Sink 完成关闭。独立的重复路径还会耗尽有限重试预算，并取消一次 60 秒的无限重试等待，以证明 Source 被取消、Sink 被关闭、未确认位点不进入 checkpoint、业务性重试耗尽进入 `FAILED`，而取消会迅速进入 `STOPPED` 且不计失败事件。`RUSTIUM_RUNTIME_SOAK_CYCLES` 接受 `1..10000`；CI 固定要求 256 轮。

构建并验证生产容器和 Helm Chart：

```bash
bash scripts/test-packaging.sh
```

多阶段镜像使用 locked workspace 依赖编译 Rustium，只包含 Kafka/TLS client 所需的运行库，以 UID/GID `65532` 运行，使用 `/var/lib/rustium` 保存 SQLite checkpoint，并暴露 `8080` 管理端口。Packaging gate 会执行 `rustium --version`，检查 OCI label 和非 root 元数据，lint/render Chart，验证 live/ready probe、只读根文件系统、保留 checkpoint PVC 和外部配置 Secret 模式，并拒绝多副本。只有创建 tagged release 后才会发布镜像和 Helm OCI 坐标，当前不提前宣称已发布。

运行真实 MySQL 8.4 集成测试：

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

该必选 CI 门槛使用动态分配的主机端口和容量为 1 的 Source 输出。它默认执行 3 轮循环：填满有界 channel、强制终止活动 binlog dump 连接、在断连期间再提交事务，并要求出现不同复制连接、全部预期记录且首次出现顺序保持源端顺序。可设置 `RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES=1..1000` 执行更长 soak。测试还会停止 Rustium，依次写入旧 schema 行、执行破坏性 DDL、写入新 schema 行，并验证重启后先使用持久化历史 schema 解码，再按 binlog 顺序应用 DDL。

运行外部 MySQL 8.0+ 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_MYSQL_TEST_HOST=mysql.example.com
export RUSTIUM_MYSQL_TEST_PORT=3306
export RUSTIUM_MYSQL_TEST_ADMIN_USER=root
export RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_USER=cdc
export RUSTIUM_MYSQL_TEST_PASSWORD='replace-me'
export RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo
export RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

管理账号只负责创建和删除唯一命名的测试表，并终止测试自身的复制 session；CDC 账号默认验证 3 轮容量为 1 的背压/重连循环、空闲周期 heartbeat、`heartbeat.action.query`、快照/复制、基于精确 server UUID 的 GTID 过滤启动、checkpoint 恢复、破坏性 DDL 恢复、带类型 keyset 重启、已完成 signal ID 持久化、增量 chunk 窗口打开期间提交更新时的去重，以及标量和 OGC 空间值在快照/binlog 路径上的一致性。设置 Kafka 变量后，同一门槛还会验证真实连接器级 Kafka signal 在完成 checkpoint 但原始 Kafka offset 尚未确认时重放且不重复执行。该门槛已在启用行级 binlog 和 GTID 的 MySQL 8.4 上通过。

运行外部 PostgreSQL 14+ 集成测试，凭据无需存入仓库：

```bash
export RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com
export RUSTIUM_POSTGRES_TEST_PORT=5432
export RUSTIUM_POSTGRES_TEST_USER=postgres
export RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me'
export RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo
export RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

测试会创建唯一命名的业务表、信号表、publication、复制角色、托管 slot、信号文件和 Kafka topic，覆盖快照切换、事务顺序、checkpoint 停止、跨破坏性 DDL 的历史 `Relation` 重放、重启不重复快照、Source 输出处于背压时重复强制终止活动 replication backend 并自动恢复、checkpoint 对应 slot 丢失后的显式失败、两个 LSN 方向的全部四种 checkpoint/slot mismatch 策略、包含未监控 WAL keepalive flush 的全部三种 LSN flush ownership mode、带过滤和原始二进制 content 的事务内及非事务 logical decoding message、带有界 flush timeout 的 acknowledgement 驱动即时 `confirmed_flush_lsn` feedback、周期 heartbeat record、`heartbeat.action.query`、heartbeat 表过滤、可恢复的 source/file/in-process/Kafka 信号增量快照、外部信号状态即时 checkpoint、已完成 signal ID 持久化、connector checkpoint/Kafka offset 崩溃窗口中的真实 broker 重放、additional condition、并发更新去重、pause/resume/scoped-stop 控制、不写信号表的只读事务快照 watermark、完全无信号表的 file 和 in-process 只读信号、经验证的 surrogate-key 排序，以及高精度 numeric、特殊值、JSONB、UUID、bytea、时间、网络、range、bit、数组、hstore、domain、enum 和 tsvector 类型在快照/WAL 路径上的一致转换。这些门槛已在启用 `wal_level=logical` 的 PostgreSQL 17 上通过。可选 WAL retention fixture 会临时限制 `max_slot_wal_keep_size`、生成 WAL 并强制 checkpoint，验证 `wal_status=lost` 后显式失败，结束前恢复原设置；只应在隔离的 superuser 测试实例上设置 `RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST=true` 单独运行。必须通过的仓库 Docker fixture 会在 PostgreSQL 17 上安装 pgvector 和 PostGIS，验证 vector、halfvec、sparsevec、geometry、geography 在快照/WAL 路径上一致，并执行 3 轮容量为 1 的背压/重连循环，要求预期记录无缺失且首次出现顺序保持源端顺序；GitHub CI 会在每次 push 和 pull request 时运行该门槛。可将 `RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES` 设置为 `1` 到 `1000`，提高循环数以执行更长 soak。独立的 librdkafka MockCluster 门槛会验证 Kafka key 过滤、单 partition 消费，以及只有持久信号确认后才提交 offset。

使用 Docker 在本地运行可复现的 PostgreSQL 17 扩展门槛：

```bash
bash scripts/test-postgresql-extensions.sh
```

同一门槛也可使用远程 Docker context，同时让数据库流量通过已认证的 SSH tunnel。先选择远程 context，将 `RUSTIUM_POSTGRES_DOCKER_SSH_HOST` 设置为可访问该 daemon 的 SSH host；还可以为可信 registry mirror 或预加载缓存覆盖 PostgreSQL 基础镜像。`RUSTIUM_POSTGRES_DOCKER_SSH_LOCAL_PORT` 默认值为 `55433`。

```bash
docker context use remote-docker
RUSTIUM_POSTGRES_DOCKER_SSH_HOST=docker-host \
RUSTIUM_POSTGRES_EXTENSION_BASE_IMAGE=mirror.example.com/postgres:17 \
bash scripts/test-postgresql-extensions.sh
```

使用 Docker 运行必须通过的 Kafka Sink 与 Schema Registry 门槛。它会启动隔离的 Redpanda broker 和 Confluent-compatible registry，验证 key/payload/header 有序投递、JSON Schema、Avro 与 Protobuf 注册/演进、按已注册 schema ID 解码 binary、Confluent framing 与 Protobuf message index、按 ID 和 subject 查询 schema、真正的 null tombstone，以及不存在 topic 的显式投递失败，最后删除容器。端口 `19092` 或 `18081` 已被占用时，分别设置 `RUSTIUM_KAFKA_TEST_PORT` 或 `RUSTIUM_SCHEMA_REGISTRY_TEST_PORT`。

```bash
bash scripts/test-kafka-sink.sh
```

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
export RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

测试会创建唯一命名的业务表、信号表、capture instance 和 Kafka topic，验证快照记录、CDC 初始化、同一事务内有序的 create/update/delete 事件、commit 边界、checkpoint 重启不重复快照、容量为 1 的背压下重复强制终止 polling session 并从未变化的带类型 CDC cursor 恢复、heartbeat/action-query、增量快照 keyset 恢复和并发更新去重、已完成 signal ID 持久化、connector checkpoint/Kafka offset 崩溃窗口中的真实 broker 重放，以及资源清理。可设置 `RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES=1..1000` 修改默认的 3 轮恢复循环。

### 在 Rust 项目中嵌入 Rustium

生产环境优先推荐将 `rustium` CLI 作为独立进程运行。需要进程内生命周期控制或自定义 `Sink` 的应用，可以直接组装 CLI 使用的公开 crate。

这些 crate 尚未发布到 crates.io，因此先通过 Git 依赖引入所需 workspace package。Cargo 会把实际解析的提交记录在 `Cargo.lock` 中；发布流程需要显式锁定源码时，请将 `branch` 改为具体 `rev`。

```toml
[dependencies]
rustium-config = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-core = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-avro = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-json = { git = "https://github.com/ulnit/rustium", branch = "main" }
rustium-format-protobuf = { git = "https://github.com/ulnit/rustium", branch = "main" }
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

    let source = Box::new(
        PostgresSource::new(
            &config.metadata.name,
            source_config,
            config.snapshot.clone(),
        )
        .with_retry_policy(config.runtime.retry_policy()),
    );
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
            errors_max_retries: config.runtime.errors_max_retries,
            errors_retry_delay_initial: config.runtime.errors_retry_delay_initial,
            errors_retry_delay_max: config.runtime.errors_retry_delay_max,
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

- PostgreSQL 14+ 校验、Debezium 兼容 publication 自动创建、托管或外部 slot 所有权校验，以及 PostgreSQL 17 failover slot
- 导出一致性快照与有界分页读取
- 兼容 Debezium `snapshot.isolation.mode`，支持 exported 与 no-export slot handoff
- 兼容 Debezium、带超时且提前获取的 snapshot lock，用于阻止并发 DDL
- insert、update、delete、truncate 事件
- 事务顺序与同一 LSN 事件序号
- 支持 prefix 过滤的事务内与非事务 logical decoding message
- TOAST 不可用值处理
- 兼容 Debezium `schema.refresh.mode`，并保持 pgoutput Relation 驱动的 schema 安全性
- 从 SQLite checkpoint 重启恢复
- 仅在 Sink 确认和 checkpoint 持久化后发送复制反馈
- Debezium 兼容的复制状态周期与 TCP keepalive 控制
- 通过全部 Debezium offset mismatch 策略显式协调 checkpoint 与 slot
- 通过 `slot.drop.on.stop` 在有序停止时执行 Debezium 兼容的 managed slot 清理
- 通过 `lsn.flush.mode` 选择 connector、外部或 driver 拥有 LSN flushing
- 通过 Debezium `lsn.flush.timeout.ms` 与 `lsn.flush.timeout.action` 限制 acknowledgement flush I/O
- 通过 Debezium `slot.stream.params` 过滤 pgoutput replication origin
- 通过 `database.initial.statements` 按顺序初始化普通数据库 connection session
- 通过 `verify-ca` / `verify-full` 和 `database.sslrootcert` 验证 PostgreSQL 服务端 TLS
- 通过 `include.unknown.datatypes` 兼容 Debezium 的未知类型省略或 opaque bytes 输出
- 通过 `money.fraction.digits` 兼容 Debezium 的 PostgreSQL MONEY scale 与 HALF_UP 转换
- schema 发现与表 include/exclude 正则过滤
- 对选中 publication 表事务化管理 `replica.identity.autoset.values`
- 通过 `publish.via.partition.root` 保持分区 root 的快照与 WAL 身份
- 持久化 PostgreSQL schema history，并在 WAL 重放前恢复
- 表 DDL 后由 Relation 消息驱动历史列布局、类型 OID/typmod、key 元数据和 schema 版本递增
- 从最新安全 WAL 位点周期发送 heartbeat record，并支持可选的 `heartbeat.action.query`
- 快照和 WAL 共用同一文本转换路径，包括精确 numeric 精度、数组、domain、enum、`hstore`、`tsvector`、pgvector 值和空间 EWKB
- 通过 source 表、file、in-process 和 Kafka `execute-snapshot` 信号执行有界且可 checkpoint 的增量快照
- 通过 `read.only=true` 执行无需连接器写入信号表的增量 watermark

Source 需要 `wal_level=logical`，以及复制和表读取权限。原生 YAML 的 `source.publication_autocreate_mode` 默认值为 `disabled`，保持既有的“publication 必须预先存在”契约，并保留旧配置的语义指纹。Debezium properties 的 `publication.autocreate.mode` 默认值为 `all_tables`，与 Debezium 一致。支持 `disabled`、`all_tables`、`filtered` 和 `no_tables`。`filtered` 根据 source filter 创建或精确替换 publication 表集合，并始终纳入已配置的 signal table；已有 `FOR ALL TABLES` publication 时会拒绝缩窄。`no_tables` 创建空 publication，之后可以动态加入表，Rustium 无需重启即可发现首个 streaming 表 schema。自动创建需要 PostgreSQL publication 权限；`all_tables` 还需要 superuser 权限，表级模式要求拥有被发布的表，更新既有 publication 还要求拥有该 publication。配置示例见 [examples/postgresql.yaml](examples/postgresql.yaml)。

Debezium `replica.identity.autoset.values` 接受逗号分隔的 `<全限定表正则>:<identity>` 规则，identity 可以是 `DEFAULT`、`FULL`、`NOTHING` 或 `INDEX <index-name>`。Rustium 只对 publication 中被 source filter 选中的业务表执行完整匹配规则。它会先解析全部匹配，再执行 DDL；同一张表匹配多条规则时拒绝启动，identity 已正确时跳过，其余 `ALTER TABLE ... REPLICA IDENTITY` 在一个事务内完成。因此 validation 需要拥有受影响的表，并可能修改数据库 metadata。PostgreSQL 会校验 `INDEX` 目标必须唯一、immediate、非 partial，且只覆盖 `NOT NULL` 列。原生 YAML 使用结构化 `source.replica_identity_autoset_values` 条目，包含 `table`、`identity`，index 模式还必须提供 `index`。

设置 Debezium `publish.via.partition.root=true` 或原生 `source.publish_via_partition_root: true` 后，Rustium 会使用 `WITH (publish_via_partition_root = true)` 创建 publication。分区快照和 streaming change 都归属 partition root，使 collection identity 与 topic routing 在不同 leaf partition 间保持稳定。复用既有 publication 时，Rustium 会将该配置与 `pg_publication.pubviaroot` 对比；不一致时 validation 失败，要求显式 alter 或重建 publication，不接受静默的 leaf/root routing 变化。

设置 Debezium `slot.failover=true` 或原生 `source.slot_failover: true` 后，Rustium 会为 managed logical slot 启用 PostgreSQL 17 failover 同步。配置 slot 前会检查 `server_version_num` 和 `pg_is_in_recovery()`；PostgreSQL 17+ 主库会创建或更新为 `FAILOVER true`，旧版本和 standby 节点按 Debezium 的降级行为记录 warning 并使用普通 logical slot。原生 `slot_ownership: external` 与 `slot_failover=true` 组合会被拒绝，因为 Rustium 不修改外部所有的 slot。默认值为 false，并保持旧 semantic fingerprint。

Debezium `slot.drop.on.stop` 映射为原生 `source.drop_slot_on_stop`，默认值为 false。启用后，Rustium 的有序 cancellation 会先停止并释放 replication transport，再通过普通数据库连接删除 managed logical slot。Replication failure、reconnect、输出失败、进程崩溃和强制 shutdown 都不会执行该清理，因此异常退出不会丢弃可恢复历史。External slot ownership 会拒绝此选项。该设置只影响运维生命周期，不改变 semantic fingerprint。持续采集的生产 source 不建议启用，因为停止期间产生的变更在下一次创建 slot 后无法恢复。

Debezium `snapshot.locking.mode` 映射为原生 `source.snapshot_locking_mode`，默认值为 `none`。设置为 `shared` 后，Rustium 会在读取任何捕获 schema 或 row 前，按确定顺序对 initial 或 `when_needed` snapshot filter 允许的每张业务表及已配置 signal table 获取 `ACCESS SHARE` lock。这些 lock 允许普通 DML，但在 snapshot 事务结束前阻止并发 DDL。`snapshot.lock.timeout.ms` 映射为原生 `source.snapshot_lock_timeout`，默认 10 秒，同时限制 captured-table discovery 和每张表的 lock；零值保留 PostgreSQL 无限等待语义。超过 PostgreSQL `2147483647ms` 上限的值会校验失败，Debezium Java `custom` SnapshotLock SPI 也会明确失败。两个参数都属于运维控制，不进入 semantic fingerprint。如果不能在整个 snapshot 窗口暂停 schema 变更，应使用 `shared`。

`snapshot.isolation.mode` 映射为原生 `source.snapshot_isolation_mode`，接受 Debezium 的 `serializable`（默认）、`repeatable_read`、`read_committed` 和 `read_uncommitted`。Serializable 与 repeatable-read 使用 PostgreSQL exported snapshot handoff，保持单一一致基线和无缺口的 WAL 起点。Read-committed 与 read-uncommitted 使用 `NOEXPORT_SNAPSHOT` slot 及请求的只读事务级别；PostgreSQL 会把 `READ UNCOMMITTED` 当作 `READ COMMITTED`。这两种模式以 slot 的 `restart_lsn` 作为 streaming 起点，因此 slot 创建后提交的变更仍会在 snapshot 后送达。较低模式会改变 semantic fingerprint，并可能跨 statement snapshot 组装出不同时间点的 row set，应先审查下游消费者。

`status.update.interval.ms` 控制周期性 replication feedback 检查，默认 10,000 ms；原生 YAML 使用 `source.status_update_interval`。Runtime durable acknowledgement 会绕过该周期并立即强制发送 status update；普通 connector mode 绝不会报告超过已确认位点的 flushed/applied LSN。`database.tcpKeepAlive` 默认 true，并映射到原生 `source.tcp_keepalive`；它控制 validation、snapshot、heartbeat 和 replication 连接的 libpq TCP keepalive。这些运维参数不改变 semantic configuration fingerprint。显式 PostgreSQL `slot.max.retries` 和 `slot.retry.delay.ms` 只有在对应共享 `errors.*` 未设置时才作为兼容回退。slot delay 会同时映射为相等的初始和最大延迟，以保持 Debezium 固定等待语义；两种形式同时提供时以 `errors.*` 为准，全部省略时继续使用 Rustium 已建立的共享重试默认值。

`xmin.fetch.interval.ms` 映射为原生 `source.xmin_fetch_interval`，默认值为 `0`，即关闭跟踪。设置正周期后，Rustium 会在第一条符合条件的 WAL message 前通过独立普通连接读取 `pg_replication_slots.catalog_xmin`，随后在周期到期前复用缓存值。PostgreSQL JSON、Avro 和 Protobuf source schema 都包含可空 `xmin`；跟踪关闭或 snapshot record 中该字段为 null，streaming position 会持久保存它，使 checkpoint 与 event metadata 保持一致。查询或解析失败会停止 source，不会静默发布陈旧 metadata。正周期会改变 event metadata，因此进入 semantic fingerprint；默认零值保持旧 position serialization 与 event ID。

`offset.mismatch.strategy` 控制 durable checkpoint 与 `pg_replication_slots.confirmed_flush_lsn` 不一致时的启动行为。`no_validation` 是向后兼容默认值，从 checkpoint 启动。`trust_offset` 会把落后的 slot 推进到 checkpoint，并拒绝已经超前的 slot。`trust_slot` 只在 slot 超前时采用 slot；`trust_greater_lsn` 在两个方向都选择较大位点，并推进落后的 slot。原生 YAML 使用 `source.offset_mismatch_strategy`；已废弃的 `slot.seek.to.known.offset.on.start=true` 映射为 `trust_offset`，两者同时提供时以新参数为准。可推进 slot 的模式要求原生 `slot_ownership: managed`；Rustium 会拒绝推进事务中间 checkpoint，因为 PostgreSQL 可能跨过尚未处理的事务记录。采用超前 slot 会有意跳过本地 source history，同时从 catalog 刷新全部选中 schema、丢弃活动 incremental-snapshot window，并在下一条 record 中 checkpoint 新状态。因此非默认策略会改变 semantic fingerprint。只有明确以服务端 slot 为权威时才应使用 `trust_slot` 或 `trust_greater_lsn`。

`lsn.flush.mode=connector` 是默认值：只有 Sink delivery 与 checkpoint 持久化之后确认的位点才能推进 PostgreSQL flushed/applied LSN。`manual` 忽略 runtime acknowledgement，把 `confirmed_flush_lsn` 完全交给外部控制；运维方必须监控保留 WAL 并自行推进 slot。`connector_and_driver` 保留 connector acknowledgement，同时允许 replication keepalive flush 传输层已收到的每个 LSN，包括没有 published record 的 WAL。原生 YAML 使用 `source.lsn_flush_mode`；已废弃的 `flush.lsn.source=true` 映射为 `connector`，false 映射为 `manual`，并以新参数为准。Driver flushing 可能让 slot 超过本地 durable checkpoint，因此会有意削弱 Rustium 的正常 replay 保证；应配合 `offset.mismatch.strategy=trust_slot` 或 `trust_greater_lsn`，且只在限制 WAL retention 比重放本地未提交 batch 更重要时启用。非默认模式进入 semantic fingerprint。

`lsn.flush.timeout.ms` 映射为原生 `source.lsn_flush_timeout`，必须为正值，默认 30,000 ms。它限制 `connector` 与 `connector_and_driver` mode 在 durable acknowledgement 后强制执行的真实 standby-status I/O。`lsn.flush.timeout.action` 映射为原生 `source.lsn_flush_timeout_action`：默认 `fail` 会停止 source，`warn` 会记录 warning 后继续，`ignore` 会在 debug 级别继续。Action 只处理超时；已经完成并返回的 I/O 错误始终使 source 失败。Manual mode 不执行 acknowledgement flush。这些运维控制项不改变 semantic fingerprint。

Debezium `slot.stream.params` 接受以分号分隔的 logical-decoder 参数。Rustium 只使用 `pgoutput`，当前支持 PostgreSQL 16+ 的 `origin=any|none` 参数：`any` 同时包含本地 change 和带 PostgreSQL replication origin 的事务，`none` 只包含本地 change。原生 YAML 使用 `source.slot_stream_params: { origin: any }`。空参数保持既有 fingerprint；配置 origin 后会进入 fingerprint，因为改变过滤器可能改变捕获数据集。不支持的参数名、畸形条目、`any`/`none` 之外的 origin 值，以及在 PostgreSQL 14/15 上配置 origin filter 都会在 validation 阶段失败，不会被静默忽略。

`database.initial.statements` 是按分号分隔并依次执行的语句列表。Rustium 每次建立普通 PostgreSQL connection 时都会执行，包括 validation、schema discovery、snapshot、heartbeat action、XMIN metadata、offset reconciliation、incremental snapshot 和有序 slot cleanup；语句内需要保留的分号写成 `;;`。Transaction-log replication connection 永远不会执行该列表，与 Debezium 一致。原生 YAML 使用字符串列表 `source.database_initial_statements`，因此无需 delimiter escaping。该参数只应配置 `SET application_name` 等幂等 session setting，不应用于 DML：Rustium 可以建立多个 connection，且较早 connection 已提交的语句不会因后续 connection 或语句失败而回滚。非空列表会改变 semantic fingerprint。

PostgreSQL `database.sslmode` 接受 libpq 的 `disable`、`allow`、`prefer`、`require`、`verify-ca` 和 `verify-full`，原生 YAML 使用 `source.ssl_mode`。`database.sslrootcert` 或原生 `source.ssl_root_cert` 指向 PEM CA bundle；rustls transport 在 `verify-ca` 与 `verify-full` 下只使用该 CA 集合。相同 TLS 设置覆盖 validation、snapshot、heartbeat/incremental connection 和 logical replication stream。强制 Docker gate 已证明正确 CA 与匹配 hostname 可以通过 `verify-full` streaming WAL，错误 CA 或 hostname 会被拒绝。当前 `pg_walstream` rustls backend 不支持 client certificate authentication，因此 `database.sslcert`、`database.sslkey` 和 `database.sslpassword` 会在 validation 阶段失败，不会被忽略；JVM 专用 `database.sslfactory` 也会显式失败。

`include.unknown.datatypes=false` 是兼容 Debezium 的默认值，会省略 Rustium 无法解码的 PostgreSQL 类型列。设置为 true，或使用原生 `source.include_unknown_datatypes: true` 后，未知值会以 `bytea` 字段保留，内容是 PostgreSQL pgoutput/`::text` 表示的 UTF-8 bytes。Initial snapshot、incremental snapshot 与 WAL 使用完全相同的表示。PostgreSQL connector-state version 6 会 checkpoint OID/typmod 和 opaque-column 状态；version 1 到 5 仍可读取，并在重放前按当前 catalog 中精确匹配的类型身份归一化。启用该参数会改变 event schema 与 semantic fingerprint。如果 Rustium 后续原生支持某类型，该列可能从 bytes 迁移到 logical representation，与 Debezium 的兼容性警告一致。

Debezium `money.fraction.digits` 默认值为 `2`，原生配置名为 `source.money_fraction_digits`。Rustium 会移除 PostgreSQL MONEY 文本中的前导货币符号、逗号分组、正负号或会计括号，精确解析剩余数值，再按配置的有符号 16 位 scale 执行与 BigDecimal 一致的 `HALF_UP` 舍入。Scale 会记录为 event field type `money(<scale>)`；scalar/array 在 initial snapshot、incremental snapshot 与 WAL 中共用同一转换。畸形 MONEY 文本会保留为原始字符串，不会产生无效 decimal。默认值保持既有原生 fingerprint；非默认 scale 会改变 event schema、值与 semantic fingerprint。

Debezium `schema.refresh.mode` 映射为原生 `source.schema_refresh_mode`，接受默认值 `columns_diff` 或 `columns_diff_exclude_unchanged_toast`。Debezium 会在 decoder row 列数少于缓存表 schema 时使用该选项。Rustium 只使用 `pgoutput`；独立的 `Relation` message 提供完整列布局，并且只有它能驱动 schema version，unchanged TOAST marker 只是行值缺失，不是 schema 变化证据。因此两个值都使用相同且安全的 Relation-driven 行为：存在 `REPLICA IDENTITY FULL` before image 时复用缺失值，否则显式输出 `Unavailable`，同时 schema version 与 connector schema state 保持不变。非法值会使配置失败。该兼容选项属于运维配置，不改变 semantic fingerprint。

PostgreSQL 列转换使用 Debezium 动态参数名。Selector 是作用于 `schema.table.column` 的 anchored、大小写不敏感正则；同一列按固定顺序 `truncate -> 固定 mask -> hash V1 -> hash V2` 选择第一个匹配规则。`column.truncate.to.<length>.chars` 截断字符或二进制值，`column.mask.with.<length>.chars` 即使遇到 SQL NULL 也输出指定数量的 `*`，两种 hash 都保留 NULL。Hash 接受常用 JCA digest 名称（`MD2`、`MD5`、`SHA-1`、`SHA-224`、`SHA-256`、`SHA-384`、`SHA-512`、`SHA-512/224`、`SHA-512/256` 以及 SHA-3 变体）；V1 使用 Java `ObjectOutputStream` String serialization 兼容字节，V2 对 UTF-8 文本计算 hash。输出为小写十六进制，并按 PostgreSQL 声明的 `char(n)` 或 `varchar(n)` 长度截短。原生 YAML 使用 `source.column_transformations`，包含 `kind: truncate|mask|hash`、`columns`、`length`，hash 还包含 `algorithm`、`salt` 和可选的 `version: v1|v2`。转换规则会进入 source fingerprint，但 fingerprint 只保存 salt 的 SHA-256 digest，不保存 salt 原文。PostgreSQL 17 外部门槛覆盖 snapshot、incremental snapshot 和 WAL、NULL mask、优先级、Debezium 已知 fixture、有界 hash 以及不变更非字符列。

Checkpoint v1 JSON 仍可读取，但已完成的 PostgreSQL v1 checkpoint 不含历史 Relation 基线，因此会拒绝恢复。升级后需要重置该 checkpoint 并执行一次新的 initial snapshot，以建立 checkpoint v2 schema history。

将 `heartbeat.interval.ms` 设置为正数即可启用 PostgreSQL heartbeat record。可选的 `heartbeat.action.query` 使用复用的普通 SQL 连接按同一周期执行；查询失败会停止 Source，查询产生的 WAL 只有在复制流实际读到后才能成为 checkpoint 进度。默认 topic 为 `__debezium-heartbeat.<topic.prefix>`；`topic.heartbeat.prefix`、旧参数 `heartbeat.topics.prefix` 和完整名称覆盖参数 `topic.heartbeat.name` 遵循 Debezium 命名。原生 YAML 使用 `source.heartbeat_interval`、`source.heartbeat_action_query`、`source.heartbeat_topics_prefix` 和 `source.heartbeat_topic_name`。

PostgreSQL domain 会按其基础类型转换，包括 domain 数组。Enum、range、网络类型、`ltree`、`isbn` 和 `tsvector` 保留 PostgreSQL 规范文本。Debezium `hstore.handling.mode=json` 为默认值并生成 JSON 值；`map` 生成强类型 `DataValue::Map`。稠密 `vector`/`halfvec` 转为浮点数组，`sparsevec` 转为包含 `dimensions` 和索引向量的 map，PostGIS `geometry`/`geography` 保留完整 EWKB 字节。已支持扩展类型的畸形值会回退为原始字符串，不进行部分解码。原生 YAML 使用 `source.hstore_handling_mode`。

Debezium properties 默认使用 `interval.handling.mode=numeric`，按 Debezium 的每月平均 `365.25 / 12` 天近似输出 `int64` 微秒 duration。`string` 输出固定 ISO 表示，例如 `P1Y2M3DT4H5M6.789S`。Rustium 可解析 PostgreSQL 的 `postgres`、`postgres_verbose`、`sql_standard` 和 `iso_8601` 四种 server output style，包括各分量独立符号和 interval array。原生 YAML 的 `source.interval_handling_mode` 默认使用仅原生支持的 `postgres` 模式，使旧配置继续保留 PostgreSQL 原始文本和 semantic fingerprint；选择 `numeric` 或 `string` 会同时改变两者。畸形值会保留原始文本，不做部分转换。

PostgreSQL 14+ 通过 `pg_logical_emit_message` 写入的 logical decoding message 使用 Debezium 的 `<topic.prefix>.message` destination、`{"prefix": ...}` key，以及包含 prefix 和原始 content bytes 的 `message` value block。`message.prefix.include.list` 与 `message.prefix.exclude.list` 是互斥的完整匹配正则列表。事务消息保留所在 WAL 事务的 ID 和顺序，只有事务 commit 后才可持久化；非事务消息可独立 checkpoint。被过滤的非事务消息仍会推进仅包含位点的安全边界，避免空闲 Source 永久重放。Debezium properties 默认启用全部 prefix。原生 YAML 为保持向后兼容，默认使用 `source.logical_decoding_messages: false`；启用它，或设置 `source.message_prefix_include_list` / `source.message_prefix_exclude_list`，都会改变 semantic fingerprint。Debezium JSON 使用 Base64 表示默认 bytes，Avro 和 Protobuf 保留 binary value。

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

Topic 默认为 `<topic.prefix>-signal`，必须恰好只有一个 partition；每条消息的 key 必须等于 `topic.prefix`，value 使用上文相同 JSON envelope。Rustium 强制 `enable.auto.commit=false` 和 `enable.auto.offset.store=false`；有效 command 被接受后，只有对应 connector state 完成 Sink 投递、SQLite checkpoint 持久化和 Source 确认，才会同步提交 Kafka offset。若在数据库 checkpoint 与 Kafka offset commit 之间崩溃，重放的相同活动或近期已完成 signal ID 会被幂等忽略。原生 YAML 使用 `source.signal_kafka_topic`、`source.signal_kafka_bootstrap_servers`、`source.signal_kafka_group_id`、`source.signal_kafka_poll_timeout` 和 `source.signal_kafka_consumer_properties`。

HTTP 路由还要求 `rustium.server.enable.mutations=true` 或原生 `server.enable_mutations: true`；命令进入有界队列后返回 `202 Accepted`，禁用变更端点时返回 `403`，未启用 `in-process` 时返回 `409`。`source`、`file`、`in-process` 和 `kafka` 可以同时启用。可写增量快照仍要求 publication 中存在 `signal.data.collection`，因为 Rustium 会在其中写入内部 open/close watermark；`read.only=true` 配合外部 channel 时不需要信号表。有效的外部控制状态会立即在当前安全 LSN checkpoint，包括 fresh idle slot。无效外部 record 和不支持的 action 会记录日志并跳过，内部 watermark action type 会被拒绝；file 投递没有重试策略。

在 Debezium properties 中，`signal.enabled.channels=jmx` 会作为 `in-process` 的迁移别名接受。Debezium JMX channel 是 JVM MXBean 背后的内存队列，因此 Rustium 通过 `ConnectorRuntime::signal_sender()` 和 `POST /v1/connector/signals` 暴露等价的有界队列，不会伪装成 JVM MBean。解析器会发出明确说明该映射的兼容警告；同时配置 `jmx` 与 `in-process` 只会创建一个 channel。

Execute payload 还接受 Debezium `additional-conditions`；每项包含一个 `data-collection` 正则和一个 SQL `filter`。该 filter 同时约束最大捕获主键与每次 chunk 查询。可选 `surrogate-key` 在该列为 `NOT NULL` 且具有有效单列唯一索引时替代主键进行 chunk 排序；目标表仍需主键用于 WAL 去重。`pause-snapshot` 在当前有界 chunk 后暂停，`resume-snapshot` 从已 checkpoint 的主键继续，`stop-snapshot` 可停止全部工作，或只停止可选 `data-collections` 列表匹配的集合。

信号表必须按顺序且仅包含 `id`、`type`、`data`，并加入 publication。被选中的快照表必须有主键。Rustium 写入 `snapshot-window-open` 和 `snapshot-window-close` watermark，在窗口打开期间移除已被 WAL 事件覆盖的行，以 Debezium `op=r`、`source.snapshot=incremental` 发出剩余行，并在 close 事务中 checkpoint 下一主键、condition 和 pause 状态。重启可能重复尚未提交的 chunk，但不会跳过。原生 YAML 使用 `source.signal_data_collection`、`source.incremental_snapshot_chunk_size` 和 `source.incremental_snapshot_watermarking_strategy`。

设置 Debezium `read.only=true` 或原生 `source.read_only: true`，即可用 `pg_current_snapshot()` 低/高事务水位替代插入 watermark。Rustium 将 WAL transaction ID 与这些快照比较，直到跨 chunk 可见的事务全部通过后才关闭窗口，并执行相同的主键去重。连接器只需要捕获表 `SELECT` 和逻辑复制权限，不写入 watermark 记录。只有使用 `source` channel 时才仍需 source 信号表。

当前 PostgreSQL 信号能力支持 Debezium `source`、`file`、`in-process`、`kafka` channel、JMX 到管理通道的迁移别名和增量快照 action。可写模式支持 `insert_insert`，只读模式使用事务快照。Connector-state version 6 保留最近 1,024 个已完成或已停止 execute signal ID 以及 opaque unknown-type 列状态；因此完成 checkpoint 后发生 Kafka 重放时不会重复发出行，version 1 到 5 仍可读取。当 `incremental.snapshot.allow.schema.changes=false` 时，Rustium 会在每次打开窗口后比较 catalog，并拒绝活动表发生变化的 WAL `Relation`；它保留旧 checkpoint，不会查询或发出布局不匹配的数据。已完成 checkpoint 还要求原 replication slot 存在且 WAL status 仍保留所需历史；slot 缺失、`unreserved` 或 `lost` 时会在建立 stream 前明确失败并要求 reset + resnapshot，不会静默创建新 slot。暂时性复制故障使用共享 `errors.*` 重试策略，同时由 PostgreSQL 连接器继续拥有 slot 和 LSN 恢复状态；`0` 关闭自动 stream 恢复，`-1` 表示持续重试。Debezium PostgreSQL 连接器不支持增量快照期间的 schema change，因此 Rustium 会拒绝 `incremental.snapshot.allow.schema.changes=true`，不会暴露不安全语义。历史 `Relation` 重放会回退到 checkpoint 中匹配的类型元数据；catalog 与历史都不可用时使用保守的 `unknown_oid_*` 名称。PostgreSQL 核心类型、扩展类型、未知类型模式以及容量为 1 的输出背压下重复强制终止 replication backend 后的恢复现在均可在 PostgreSQL 17 上复现，并由 CI 强制执行。

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
- 从最后安全源位点自动重连 binlog，并使用共享的 Debezium 兼容 `errors.*` 重试预算
- 从最新安全 binlog 位点周期发送 heartbeat record，默认关闭
- 可选地在 heartbeat 周期通过独立普通 MySQL 连接执行 `heartbeat.action.query`
- 支持 FULL、MINIMAL、NOBLOB row image；MySQL 未提供的值会明确标记为 unavailable
- 根据完整 before image 重建 PARTIAL_JSON 更新 diff；无法安全重建时保守标记为 unavailable
- 统一以 UTC 读取 temporal 值，并验证 boolean、有符号/无符号整数、decimal、float、bit/binary、时间、字符串、JSON、ENUM、SET 和 null 在快照/binlog 路径上一致
- 支持源表、文件、进程内和 Kafka 增量快照信号，并将主键 keyset 进度与已完成 signal ID 持久化到 connector state
- 使用低/高 binlog 坐标窗口，在 chunk commit 前移除已被并发 create、update 或 delete 覆盖的增量 read 行
- 必选 Docker CI 和外部 MySQL 8.4 集成测试，包括重复的容量 1 背压/重连循环、过滤后的 GTID 启动、破坏性 DDL 重启、keyset 重启和并发写入去重

建议给 MySQL 连接器用户授予：

```sql
GRANT SELECT, RELOAD, FLUSH_TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'rustium'@'%';
```

MySQL Debezium 风格示例见 [examples/mysql.properties](examples/mysql.properties)。

MySQL 重连使用 `errors.max.retries`、`errors.retry.delay.initial.ms` 和 `errors.retry.delay.max.ms`；`0` 表示立即失败，`-1` 表示不限制重试次数。产生新源端进度后退避会重置，取消信号可中断等待，`connect.keep.alive=false` 仍会关闭自动恢复。为迁移 `.properties`，只有在对应 `errors.*` 缺失时，`rustium.source.reconnect.max.attempts` 和 `connect.keep.alive.interval.ms` 才作为回退。直接构造 `MySqlSource` 且未调用 `with_retry_policy` 的嵌入式调用者继续使用这些旧 source-config 值。

将 `heartbeat.interval.ms` 设置为正数即可发送 heartbeat record。默认 topic 为 `__debezium-heartbeat.<topic.prefix>`；`topic.heartbeat.prefix` 可修改前缀，迁移时仍兼容 `heartbeat.topics.prefix`，`topic.heartbeat.name` 可覆盖完整 topic。heartbeat key 为 `{"serverName":"<connector-name>"}`，value 为 `{"ts_ms":...}`。原生 YAML 使用 `source.heartbeat_interval`、`source.heartbeat_topics_prefix` 和 `source.heartbeat_topic_name`。

`heartbeat.action.query` 可选地在每个正 heartbeat 周期先通过独立普通 MySQL 连接执行。查询失败会携带数据库错误停止 Source；查询产生的 binlog 变化只有在复制流实际读到后才算源进度。原生 YAML 使用 `source.heartbeat_action_query`。

`database.connectionTimeZone` 映射到原生 `source.connection_time_zone`，默认值为 `UTC`。Rustium 会把允许的 `UTC`、`Z`、`Etc/UTC` 和 `+00:00` 规范化为 `+00:00` session time zone，使 `TIMESTAMP` 快照值与 UTC binlog 值一致。其他偏移量、地区名称以及 Debezium 的 `SERVER` 模式会在配置校验阶段失败，直到两条捕获路径能够保证完全一致的时间转换。

MySQL TLS 可以通过 `database.ssl.ca`、`database.ssl.cert`、`database.ssl.key` 使用 PEM/DER 材料，也可以通过 Debezium 兼容参数 `database.ssl.truststore`、`database.ssl.truststore.password`、`database.ssl.keystore`、`database.ssl.keystore.password` 使用 Java 存储。Rustium 按内容识别 JKS 和 PKCS#12/PFX，支持现代 PBES2/AES/SHA-256 以及旧式 PBES1 PKCS#12，并直接在内存中转换为 Rustls 所需的证书和 PKCS#8 私钥。keystore 必须且只能包含一个带证书链的私钥条目。PEM CA 与 truststore 互斥，PEM 客户端身份与 keystore 互斥。原生 YAML 使用对应的 `source.ssl_*` 参数；存储密码应通过环境变量插值提供。

DDL 默认解析失败即停止连接器。可使用 Debezium 兼容参数 `schema.history.internal.skip.unparseable.ddl=true` 警告后跳过不支持的 DDL，但这可能导致 schema 元数据不完整。

`gtid.source.includes` 和 `gtid.source.excludes` 接受逗号分隔、大小写不敏感的正则表达式，并对完整 GTID source UUID 进行匹配；两者最多只能配置一个。只要其中一个存在，Rustium 就会过滤完整捕获的 executed-GTID SID 集合，并在至少保留一个 source 时使用基于 GTID 的启动。若没有已执行 source 匹配，则记录日志并回退到已捕获的 binlog 文件和位置。流式 checkpoint 保存的是事务 GTID，而不是完整 executed set，因此从这类 checkpoint 重连时会刻意保持精确 file/position 路径。默认 `gtid.source.filter.dml.events=true` 会抑制不匹配 source 的行事件，但其事务 commit 边界仍会推进安全 checkpoint，DDL 也仍会进入 schema history。设置为 `false` 可保留全部 DML，同时继续过滤完整 GTID 恢复锚点。原生 YAML 使用 `source.gtid_source_includes`、`source.gtid_source_excludes` 和 `source.gtid_source_filter_dml_events`。

Checkpoint v1 JSON 仍可读取，但已完成的 MySQL v1 checkpoint 不含历史 schema 基线，因此会拒绝恢复。升级后需要重置该 checkpoint 并执行一次新的 initial snapshot，以建立 checkpoint v2 schema history。

MySQL 已支持源表、文件、进程内和 Kafka signal，并兼容 Debezium 的 `execute-snapshot`、`pause-snapshot`、`resume-snapshot`、`stop-snapshot` 控制。增量快照要求目标表具有主键；每个集合会固定最大主键，并使用带类型的单列或复合主键 keyset 查询推进，不再使用 `LIMIT/OFFSET`。每个 chunk 都会在查询前后捕获低/高 binlog 坐标，先缓存 read 行，在复制流追到高水位期间正常发出 CDC record，并移除 `(low, high]` 内发生变化的 before/after 主键。剩余行与 chunk commit 一起发出，同时持久化当前主键、最大主键、集合、暂停状态和有界的已完成 signal ID 历史。重启会从最后 checkpoint 的主键之后继续；已完成的 execute signal 重放会被忽略，包括 connector checkpoint 与 Kafka offset commit 之间的崩溃窗口。未提交的内存窗口在重连后会丢弃并重读；窗口打开期间观察到 schema change 时，Source 会在发出布局不匹配的行之前停止。事件循环每轮最多执行一个 chunk，因此可以在 chunk 之间处理 binlog 和控制信号。Kafka 使用现有的单 partition、与 checkpoint 绑定的 `rustium-signal-kafka` channel 以及同一 Debezium topic/key 合约。MySQL 8.4 外部类型矩阵现已验证 `GEOMETRY`、`POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON` 和 `GEOMETRYCOLLECTION` 在快照/binlog 路径上字节完全一致；空间 payload 保留 MySQL 原生 SRID 加 WKB 字节。DDL 恢复还覆盖索引新增/重命名/删除和列默认值修改，并确保这些操作不会推进 event-schema version。配置真实 Kafka 后，外部门槛会验证完成 checkpoint 但原始 Kafka offset 尚未确认时的连接器级 signal 重放。启用 `binlog_row_value_options=PARTIAL_JSON` 时，如果 diff 没有完整 before image，仍会明确标记为 unavailable，而不会猜测结果。

MySQL 源表信号需要配置 `signal.data.collection=database.signal_table`，并让连接器用户对信号表具有 `SELECT` 权限；连接器不会向信号表写入数据。信号表必须提供 `id`、`type`、`data` 三列。文件信号每行一个 JSON envelope，读取后清空文件；进程内信号复用其他连接器的 `SignalSender` 和 HTTP 管理端点；Kafka signal 复用单 partition、与 checkpoint 绑定的 `rustium-signal-kafka` channel。

### Debezium 配置兼容

Rustium 同时接受严格的原生 YAML 和 Debezium 风格 Java `.properties`。项目优先采用熟悉的参数名，减少现有部署迁移时的配置改动。

当前已映射的 PostgreSQL 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`、`database.dbname`、`database.initial.statements`
- `database.sslmode`、`database.sslrootcert`、`database.tcpKeepAlive`、`plugin.name`、`slot.name`、`slot.failover`、`slot.max.retries`、`slot.retry.delay.ms`、`status.update.interval.ms`、`xmin.fetch.interval.ms`
- `slot.stream.params`
- `offset.mismatch.strategy`、已废弃的 `slot.seek.to.known.offset.on.start`
- `lsn.flush.mode`、`lsn.flush.timeout.ms`、`lsn.flush.timeout.action`、已废弃的 `flush.lsn.source`
- `publication.name`、`publication.autocreate.mode`、`replica.identity.autoset.values`、`publish.via.partition.root`
- `schema.include.list`、`schema.exclude.list`、`table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`、`snapshot.include.collection.list`
- `snapshot.isolation.mode`
- `heartbeat.interval.ms`、`heartbeat.action.query`、`topic.heartbeat.prefix`、`heartbeat.topics.prefix`、`topic.heartbeat.name`
- `signal.data.collection`、`signal.enabled.channels`、`signal.file`、`signal.poll.interval.ms`
- `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms`、`signal.consumer.*`
- `incremental.snapshot.chunk.size`、`incremental.snapshot.allow.schema.changes`、`incremental.snapshot.watermarking.strategy`
- `read.only`
- `hstore.handling.mode`、`interval.handling.mode`、`include.unknown.datatypes`、`money.fraction.digits`、`schema.refresh.mode`
- `column.truncate.to.<length>.chars`、`column.mask.with.<length>.chars`、`column.mask.hash.<algorithm>.with.salt.<salt>`、`column.mask.hash.v2.<algorithm>.with.salt.<salt>`
- `message.prefix.include.list`、`message.prefix.exclude.list`
- `errors.max.retries`、`errors.retry.delay.initial.ms`、`errors.retry.delay.max.ms`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

当前已映射的 MySQL 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`
- `database.server.id`、`database.ssl.mode`、`database.ssl.ca`、`database.ssl.cert`、`database.ssl.key`
- `database.ssl.keystore`、`database.ssl.keystore.password`、`database.ssl.truststore`、`database.ssl.truststore.password`
- `database.include.list`、`database.exclude.list`
- `table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`、`snapshot.include.collection.list`、`connect.timeout.ms`
- `connect.keep.alive`、`connect.keep.alive.interval.ms`
- `gtid.source.includes`、`gtid.source.excludes`、`gtid.source.filter.dml.events`
- `heartbeat.interval.ms`、`heartbeat.action.query`、`topic.heartbeat.prefix`、`heartbeat.topics.prefix`、`topic.heartbeat.name`
- `signal.data.collection`、`signal.enabled.channels`、`signal.file`、`signal.poll.interval.ms`
- `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms`、`signal.consumer.*`
- `incremental.snapshot.chunk.size`、`incremental.snapshot.watermarking.strategy`
- `schema.history.internal.skip.unparseable.ddl`
- `rustium.source.reconnect.max.attempts`
- `errors.max.retries`、`errors.retry.delay.initial.ms`、`errors.retry.delay.max.ms`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

当前已映射的 SQL Server 参数包括：

- `name`、`connector.class`、`topic.prefix`
- `database.hostname`、`database.port`、`database.user`、`database.password`
- `database.names`、`database.encrypt`、`database.trustServerCertificate`
- `table.include.list`、`table.exclude.list`
- `snapshot.mode`、`snapshot.fetch.size`、`snapshot.include.collection.list`、`snapshot.isolation.mode`
- `data.query.mode=direct`、`streaming.fetch.size`
- `heartbeat.interval.ms`、`heartbeat.action.query`、`topic.heartbeat.prefix`、`heartbeat.topics.prefix`、`topic.heartbeat.name`
- `signal.data.collection`、`signal.enabled.channels`、`signal.file`、`signal.poll.interval.ms`
- `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms`、`signal.consumer.*`
- `incremental.snapshot.chunk.size`、`incremental.snapshot.allow.schema.changes`、`incremental.snapshot.watermarking.strategy`
- `errors.max.retries`、`errors.retry.delay.initial.ms`、`errors.retry.delay.max.ms`
- `max.queue.size`、`max.batch.size`、`poll.interval.ms`

`snapshot.include.collection.list` 是使用 anchored matching 的仅快照正则过滤器。空列表会快照普通 database/schema/table 过滤器选中的全部表。PostgreSQL pattern 匹配 `schema.table`，MySQL pattern 匹配 `database.table`，SQL Server pattern 匹配 `database.schema.table`。该过滤器作用于 initial snapshot，包括 `snapshot.mode=when_needed` 触发的恢复快照；它不会缩小 streaming CDC 或 incremental snapshot 范围。原生 YAML 使用 `snapshot.include_collections`。必跑集成门禁会创建两张都被 streaming 选中的表，只快照其中一张，然后证明 PostgreSQL 17、MySQL 8.4 和 SQL Server 2022 仍能捕获另一张表的新变更。

通用 Debezium 格式参数包括 `unavailable.value.placeholder` 和 `tombstones.on.delete`。`debezium_json`、`debezium_json_schema`、`debezium_avro` 和 `debezium_protobuf` 默认启用 tombstone：每条 delete envelope 后会在同一个投递批次中追加一条 key 相同、value 为 null 的记录。可通过 `tombstones.on.delete=false` 或原生 YAML 的 `format.tombstones_on_delete: false` 关闭。

仓库内置 golden fixture，固定 PostgreSQL、MySQL 和 SQL Server 的完整 Debezium JSON destination、key 和 envelope。配套 JSON Schema、Avro 和 Protobuf fixture 还会固定各连接器的 destination、Registry subject、schema type 和完整 key/value 定义。CI 会将生成契约与这些 fixture 比较，使连接器特有 source 元数据、topic routing、field number 或 wire schema 漂移必须经过明确审查。

Rustium 会把成对配置的 `io.confluent.connect.json.JsonSchemaConverter`、`key.converter.schema.registry.url` 和 `value.converter.schema.registry.url` 映射为 `debezium_json_schema`。同时支持两侧一致的 `basic.auth.credentials.source=USER_INFO`、`basic.auth.user.info` 和 `auto.register.schemas=true`。`rustium.schema.registry.request.timeout.ms` 控制单次 registry 请求，`rustium.schema.registry.cache.capacity` 限制成功 schema ID 的 LRU 缓存。当前 key/value converter 必须使用相同 URL 列表与凭据、默认 `TopicNameStrategy`、自动注册和显式生成 schema。不同 registry、RecordName strategy、`auto.register.schemas=false` 和 `use.latest.version=true` 会直接校验失败，不会静默改变兼容语义。示例见 [examples/mysql-json-schema.properties](examples/mysql-json-schema.properties)。

成对的 `io.confluent.connect.avro.AvroConverter` 会映射为 `debezium_avro`，并复用相同的 Registry URL、认证、subject strategy、自动注册、超时和有界缓存契约。Rustium 生成合法的 Avro key/envelope/source/transaction/row 命名 record，使用 Apache Avro 序列化原始 binary datum，由 Kafka Sink 完成 Registry 注册与 Confluent framing。`schema.name.adjustment.mode` 和 `field.name.adjustment.mode` 可省略或设为 `avro`；Rustium 始终执行确定性 Avro 名称调整，并拒绝调整后发生冲突的字段名。不支持的调整模式会直接校验失败。无符号 64 位整数、decimal、时间类型、UUID、JSON 和其他未建模扩展值使用稳定字符串表示；数据库 binary 保持 Avro `bytes`，有符号整数与浮点数保持数值，array 保持 array，hstore/map 保持 map。示例见 [examples/mysql-avro.properties](examples/mysql-avro.properties)。

成对的 `io.confluent.connect.protobuf.ProtobufConverter` 会映射为 `debezium_protobuf`。Rustium 为每个 subject 生成唯一 top-level `Key` 或 `Envelope` message，并输出 Confluent 优化后的 message-index byte `0`。Row 字段使用生成的带类型 oneof wrapper，使原生值、显式 null 缺失、unavailable placeholder 和文本转换 fallback 可区分；array、多维 array、map、binary 和完整无符号 64 位范围都无损保留。Protobuf field number 从原始数据库字段名确定性派生，在重启和字段重排后保持稳定、避开保留区间，并在活动冲突时失败。Rustium 始终调整非法名称；`scrub.invalid.names` 可省略或设为 `true`。`optional.for.nullables`、`wrapper.for.nullables`、`generate.struct.for.nulls` 和 `flatten.unions` 必须保持默认 `false`，因为修改它们会选择不同 wire contract。示例见 [examples/mysql-protobuf.properties](examples/mysql-protobuf.properties)。

对应的原生 YAML 为：

```yaml
format:
  type: debezium_json_schema
  schema_registry:
    urls: [https://schema-registry:8081]
    username: ${SCHEMA_REGISTRY_USER}
    password: ${SCHEMA_REGISTRY_PASSWORD}
    request_timeout: 10s
    cache_capacity: 1000
```

原生 Avro 或 Protobuf 配置使用相同的 `schema_registry` block，并将类型改为 `type: debezium_avro` 或 `type: debezium_protobuf`。

未支持的参数会输出兼容性警告，不会被静默伪装成已实现。Rustium 将 Debezium 的 `errors.max.retries`、`errors.retry.delay.initial.ms` 和 `errors.retry.delay.max.ms` 映射到 PostgreSQL 复制恢复、MySQL binlog 恢复、SQL Server CDC polling 恢复及可重试 Sink 操作共用的重试策略；原生 YAML 使用 `runtime.errors_max_retries`、`runtime.errors_retry_delay_initial` 和 `runtime.errors_retry_delay_max`。Rustium 默认在首次尝试后最多重试 10 次，从 300 ms 开始倍增，最大为 10 s。重试次数设为 `0` 表示立即失败，设为 `-1` 表示无限重试。SQL Server 只重试连接故障和选定的暂时性服务端错误；权限、转换、协议和 CDC retention 错误继续 fail-closed。其他 Rustium 特有的 Source 重试、Sink、状态、Server、日志和 Kafka Producer 设置使用 `rustium.*` 前缀。

### SQL Server

SQL Server 连接器基于原生 SQL Server CDC change table 实现。

已实现能力：

- SQL Server 2017+ 和数据库 CDC 校验
- 单数据库 Source 所有权和 capture instance 发现
- 以 `sys.fn_cdc_get_max_lsn()` 作为快照切换点
- 按 commit LSN、sequence value、operation 排序的 direct CDC change-table 读取
- insert/delete 转换，以及 update operation 3/4 的 before/after 配对
- 事务顺序、事务中间重放和 checkpoint 恢复
- 从暂时性 polling 连接故障中进行有界恢复，且不推进带类型的 CDC cursor
- CDC cleanup 删除所需 checkpoint LSN 时明确失败
- 由 `streaming.fetch.size` 控制的有界 CDC 查询，包括同一 commit LSN 内的继续读取
- 从最新安全 CDC 位点发送周期 heartbeat，并可通过独立 SQL 连接执行 `heartbeat.action.query`
- 快照和 CDC 共用 SQL 投影，保证 numeric、binary、UUID、temporal、text、XML、hierarchyid、geometry 和 geography 转换一致
- 支持 source table、file、in-process 和 Kafka signal channel，并执行带 checkpoint 的主键增量快照
- 通过 CDC 观察 open/close watermark，缓存每个 chunk，并移除已被并发 create、update 或 delete 覆盖的行

当前实现要求 `database.names` 只有一个数据库、每张选表只有一个活动 capture instance，并使用 `data.query.mode=direct`。增量快照要求主键，会固定最大主键，并持久化带类型的单列/复合 keyset 进度和有界的已完成 signal 历史。所有增量快照 channel 还必须配置 `signal.data.collection`：该表必须按顺序且仅包含文本兼容的 `id`、`type`、`data` 列，`id` 至少容纳 42 个字符，表必须启用 CDC，连接器用户必须具有 `INSERT`。这些约束和对象权限会在 Source 校验阶段检查。Rustium 等待 CDC 观察到 open watermark 后缓存 chunk，根据 CDC before/after image 移除主键，并只在 CDC 观察到 close watermark commit 后发出剩余 read。信号表行不会暴露为业务事件。事件循环只在没有活动 CDC 事务时逐次处理一个 chunk，因此 pause/resume/stop 会在 chunk 之间生效。未提交的内存窗口会在重启时丢弃，Source 会在查询前和 close 时再次校验表 schema。

SQL Server 2022 Developer RTM-CU25 外部门槛已验证快照切换、fetch size 为 1 时跨 update before/after 的继续读取、保持事务序号的事务中间 checkpoint 重启、3 轮容量为 1 的 polling-session 终止循环及连接 identity 替换、全部预期记录和首次出现的源端顺序、按 commit LSN 排序的并发事务、retention fail-closed、heartbeat/action-query、核心类型以及 XML、hierarchyid、geometry、geography 的快照/CDC 一致性、in-process keyset 重启、带 additional condition 的 source-table signaling、CDC window 并发更新去重、connector checkpoint 已完成但 signal offset 尚未提交时的真实 broker Kafka 重放，以及资源清理。Geometry 和 geography 以字节保留完整的 SQL Server 原生 `Serialize()` payload；hierarchyid 保留规范路径，XML 保留规范文本。示例见 [examples/sqlserver.properties](examples/sqlserver.properties)。

数据库必须启用 CDC，SQL Server Agent 必须运行 capture job，连接器用户需要读取源表，并能直接读取 `cdc` schema。CI 会在每次 push 和 pull request 时运行独立的 SQL Server 2022 Docker 可移植性门槛；其容量为 1 的输出默认执行 3 轮 session 终止/重连，并要求全部后续 CDC 事务。本地仍可通过以下命令运行：

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 格式与 Sink

内部模型保留 null、有符号/无符号整数、decimal 文本、浮点数、binary、date/time/timestamp、UUID、JSON、array 和 unavailable 值。

可用 Encoder：

- `rustium_json`：带版本的原生事件
- `debezium_json`：`before`、`after`、`source`、`op`、`ts_ms`、事务元数据和 heartbeat record
- `debezium_json_schema`：相同 Debezium envelope，附带生成的 Draft-07 key/value schema 和 Confluent Schema Registry wire framing
- `debezium_avro`：命名的 Debezium key/envelope record，以 Apache Avro binary datum 编码并使用 Confluent Schema Registry wire framing
- `debezium_protobuf`：带类型的 Debezium key/envelope message，以 Protocol Buffers 编码并使用带 message index 的 Confluent Schema Registry framing

可用 Sink：

- `stdout`：用于开发和协议检查
- `kafka`：基于 `librdkafka`，使用副本确认，支持可配置压缩/属性和幂等投递

Kafka Sink 只接受 `acks=all` 或等价别名 `-1`，因为 Rustium 会在 batch 成功后立即 checkpoint。允许 `acks=0/1` 会在 Kafka 完成副本写入前确认源端进度，可能丢失已 checkpoint 的 record。Rustium 始终启用 Producer 幂等，并拥有 `bootstrap.servers`、确认、压缩、幂等和投递超时参数；不能通过 `sink.properties` 或 `rustium.kafka.property.*` 覆盖这些 key。安全、batch 和其他不冲突的 librdkafka 属性仍可透传。所有 schema-aware 格式的 Encoder 都生成 datum 和不可变 schema descriptor；Protobuf 还会预置 message-index vector。Sink 在 Kafka 投递前注册每个不同的 subject/definition，只将成功 ID 放入有界 LRU，并在 key/value 字节前加 Confluent magic byte 和大端 schema ID。网络故障可重试，兼容性拒绝则 fail-closed。该 wire 路径兼容 Confluent Schema Registry 和 Apicurio Registry 的 Confluent compatibility API。必须通过的真实 broker CI 门槛会验证有序记录、三种 schema 格式及其演进、按已注册 ID 解码 binary、subject 查询、真正的 null tombstone、topic 清理和显式 broker 故障。

可重试的 Sink 校验、投递和 flush 失败使用可被取消的指数退避。投递重试会保留并重放同一批次，只有该批次成功后才推进 checkpoint。在投递退避期间取消会中断等待、保持 checkpoint 不变、完成 Source 和 Sink 清理，并作为优雅停止结束且不增加失败事件计数。这可以避免跳过源位点，但 broker 已部分投递后发生失败时可能产生重复记录，因此 Consumer 必须保持幂等。Source 重连仍由连接器负责，因为 WAL slot、binlog table map 和 SQL Server CDC cursor 都需要连接器特有的恢复状态；PostgreSQL、MySQL 和 SQL Server 均已有经过真实数据库验证的连接器特有恢复。

### 管理 API

Server 默认绑定 `127.0.0.1:8080`。

| 端点 | 用途 |
|---|---|
| `GET /health/live` | 进程存活 |
| `GET /health/ready` | 连接器就绪状态 |
| `GET /v1/connector/status` | 状态/原因、位点、checkpoint 与源事件时间、lag、队列和计数 |
| `POST /v1/connector/stop` | 启用变更端点时优雅停止 |
| `POST /v1/connector/signals` | 启用变更端点和 channel 时提交 Debezium 兼容 in-process 信号 |
| `GET /metrics` | Prometheus 指标 |

状态和指标只有在 Sink 写入及 checkpoint 成功后才会推进。`rustium_source_lag_seconds` 表示当前时间与最后一个已持久确认源时间戳之间的距离；不可用时为 `NaN`。`rustium_checkpoint_age_seconds`、`rustium_last_event_age_seconds` 和 `rustium_connector_state_age_seconds` 提供低基数运维 age 指标，适合告警。`rustium_sink_retry_attempts` 统计 Sink 校验、投递和 flush 阶段已调度的重试。编码失败、重试耗尽或不可重试的 Sink 投递失败会增加 `failed_events`、把连接器转为 `FAILED`、取消 Source，并仍然执行 Sink shutdown；若 Source 忽略取消，则会在 `runtime.shutdown_timeout` 后被 abort。

### 容器与 Kubernetes 部署

仓库提供多阶段 `Dockerfile` 和 [deploy/helm/rustium](deploy/helm/rustium) Helm Chart。由于 SQLite checkpoint 所有权和源顺序都是单所有者契约，Chart 有意只运行一个 connector 副本。默认使用保留的 `ReadWriteOnce` PVC、`Recreate` 策略、非 root UID/GID `65532`、只读根文件系统、关闭 ServiceAccount token 挂载，以及 `/health/live` 和 `/health/ready` probe。

在 Kubernetes 中创建包含完整配置的 Secret 并安装 Chart：

```bash
kubectl -n rustium create secret generic rustium-config \
  --from-file=rustium.yaml=./rustium.yaml
helm upgrade --install rustium deploy/helm/rustium \
  --namespace rustium --create-namespace \
  --set config.existingSecret=rustium-config
```

挂载配置应使用 `server.bind: 0.0.0.0:8080` 和 `state.path: /var/lib/rustium/rustium.db`。数据库、Kafka 和 Schema Registry 凭据应通过 Kubernetes Secret 环境变量插入，不要提交到 values 文件。完整英中部署契约见 [deploy/helm/rustium/README.md](deploy/helm/rustium/README.md)。

Tagged release 会运行 [.github/workflows/release.yml](.github/workflows/release.yml)。Workflow 会检查 tag、Cargo workspace 和 Chart 版本一致，重复 locked Rust 门禁，向 GHCR 发布带签名、SBOM/provenance 的多架构（`linux/amd64`、`linux/arm64`）镜像，以 OCI artifact 推送 Helm Chart，并创建带 image digest 与 SHA-256 checksum 文件的 GitHub Release。crates.io 发布仍是单独的人工审查、有序操作，因为内部 crate 必须从叶子 crate 逐步发布到 CLI。

crates.io 发布只能通过手动 [.github/workflows/publish-crates.yml](.github/workflows/publish-crates.yml) workflow 执行。触发时必须输入 `publish`，并配置已轮换的 `CARGO_REGISTRY_TOKEN` repository Secret；workflow 会先验证完整 workspace，再按 core、foundation、connector、CLI 依赖顺序发布。普通 push 或 tag 不会触发该 workflow。

每个可发布 workspace crate 都包含简洁的英中双语 README，packaging、release 和 publication 门禁会验证 README 确实包含在 Cargo tarball 中。同一组门禁还要求每个 schema-aware format crate 包含优先连接器的 schema fixture。

### 文档与贡献策略

- 面向用户的文档必须先提供完整英文，再提供完整简体中文。
- 代码、配置键、API、日志、Issue 和提交信息使用英文。
- 行为变更必须补测试，尤其是恢复和确认顺序测试。
- Commit 必须包含 DCO `Signed-off-by`。

规范架构和连接器设计见 [docs/design.md](docs/design.md)。备份、恢复、告警和 Kubernetes 运维见 [docs/runbook.md](docs/runbook.md)；checkpoint/配置迁移规则见 [docs/upgrades.md](docs/upgrades.md)；漏洞报告和安全部署要求见 [SECURITY.md](SECURITY.md)。

### 许可证与独立性

Rustium 使用 [Apache License 2.0](LICENSE)。Rustium 与 Debezium 或 Red Hat 没有关联、背书或 fork 关系。文档引用 Debezium 仅用于行为和迁移兼容。
