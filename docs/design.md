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
| MySQL source | Implemented; Docker and external GTID/destructive-DDL restart gates pass with MySQL 8.4 |
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
10. **Control offsets follow checkpoints.** Durable external signal offsets advance only after their connector state is checkpointed and acknowledged.

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
|-- rustium-signal-kafka/  checkpoint-coupled Kafka signal input
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
    |
    v
acknowledge durable external signal offsets
```

The current runtime uses one source task and one ordered delivery coordinator. This intentionally limits parallelism until ordering barriers and partition contracts are explicit.

#### 5.1 Commit sequence

For every non-empty or position-only batch:

1. Encode all data records.
2. Call `Sink::write` and wait for its acknowledgement.
3. Save the source position and versioned connector state in one SQLite checkpoint transaction.
4. Publish the saved position on the source acknowledgement channel.
5. Release any external signal acknowledgement attached to that checkpoint.
6. Update status counters.

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

`DataValue` distinguishes null, boolean, signed/unsigned integers, decimal text, float, string, bytes, date, time, timestamp, UUID, JSON, array, string-keyed map, and unavailable.

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

Before resuming a completed checkpoint, Rustium verifies that the original replication slot still exists, still uses `pgoutput`, and has not entered PostgreSQL `wal_status=unreserved` or `lost`. A failed continuity check stops before the replication transport can create a replacement slot and instructs the operator to reset the checkpoint and run a new initial snapshot. This deliberately prefers an explicit recovery operation over a silent WAL gap.

PostgreSQL does not put original DDL or column nullability/default metadata into `Relation`. If a transient historical column no longer exists in the current catalog and was not present in the checkpoint baseline, Rustium resolves its type from OID/typmod and conservatively marks it optional. This preserves row decoding and ordering without claiming unavailable metadata.

Snapshot queries project every selected column through PostgreSQL's `::text` output function instead of routing rows through JSON. Snapshot values and `pgoutput` values therefore share one converter and preserve numeric scale/precision, bytea, JSON text, temporal formatting, and array syntax identically. The array parser handles quoted and escaped elements, SQL NULL versus the string `"NULL"`, explicit lower bounds, nested dimensions, and type-aware scalar conversion. Malformed array text is preserved as a string instead of being partially decoded.

Catalog discovery resolves PostgreSQL domains to their base conversion type while retaining the domain OID/typmod as schema identity; arrays of domains receive the same element conversion. Enum, range, network, and `tsvector` values use canonical PostgreSQL text. `hstore.handling.mode=json` maps hstore output to JSON and `map` maps it to a string-keyed `DataValue::Map`, including nulls, escapes, and hstore arrays. `vector` and `halfvec` become float arrays, `sparsevec` becomes a map containing dimensions and indexed values, and PostGIS geometry/geography becomes complete EWKB bytes. Every specialized parser is all-or-nothing and falls back to the original string on malformed or unknown input.

`heartbeat.interval.ms` defaults to zero. A positive interval emits a visible heartbeat at the latest SourceRecord position already admitted to the bounded queue, or at the completed snapshot anchor before the first streaming event. No heartbeat is emitted when no source position exists. `heartbeat.action.query` optionally executes first on a reused ordinary SQL connection at each interval. Its WAL is not treated as progress until `pgoutput` returns it, and query failures stop the source with the database error. Heartbeat-table changes can be included in the publication but excluded by the table selector; their transaction commit still advances the safe WAL position without exposing a business-table event.

#### 9.4 Source signaling and incremental snapshots

The signaling implementation follows Debezium's `source`, `file`, `in-process`, and `kafka` channels. `signal.data.collection` identifies one schema-qualified table with exactly three text-compatible columns named `id`, `type`, and `data`; the table must be in the publication and is always filtered from business snapshots and events. `signal.enabled.channels` accepts any combination of the four implemented channels. `signal.file` defaults to `file-signals.txt`, and `signal.poll.interval.ms` defaults to 5000 ms. The file reader consumes non-empty JSON Lines and clears the file after a successful read, matching Debezium's no-retry channel semantics. An `execute-snapshot` record accepts `type=incremental`, fully matched regular expressions in `data-collections`, `additional-conditions` entries containing a case-insensitive collection expression plus a SQL filter, and an optional `surrogate-key` column name.

Source-table commands are ignored when the `source` channel is disabled, but connector-generated watermark records remain active for writable externally signaled snapshots. External watermark action types are rejected. After each valid external action, Rustium emits a position-only transaction boundary containing connector state at the latest safe LSN; a fresh stream obtains that initial position from the slot's `confirmed_flush_lsn` or `restart_lsn`. This makes control progress checkpointable even when no business WAL arrives. Read-only external signaling needs no signal table; writable external signaling still needs one for `insert_insert` watermarks.

`ConnectorRuntime::signal_sender()` exposes a cloneable, typed, bounded `SignalSender` before runtime ownership moves into `run`. PostgreSQL consumes commands only outside active WAL transactions and routes file, in-process, and Kafka records through the same action controller and checkpoint path. The CLI attaches this sender to `POST /v1/connector/signals` only when `in-process` is enabled. The route requires management mutations, returns `202` after queue admission, and reports disabled mutations or channels as `403` or `409` respectively.

Debezium's `jmx` signal channel is a JVM MXBean that writes into an in-memory queue. For properties migration, Rustium maps `signal.enabled.channels=jmx` to the same bounded `in-process` channel, emits a compatibility warning, and exposes it through the embedded `SignalSender` and HTTP management route. This preserves queue and action semantics without claiming JVM/RMI protocol compatibility; `jmx,in-process` is deduplicated to one channel.

`rustium-signal-kafka` implements `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, and stripped `signal.consumer.*` pass-through properties. The topic defaults to `<topic.prefix>-signal`, must have exactly one partition, and filters records whose key does not equal `topic.prefix`. Automatic commit and automatic offset storage are forced off. A valid record uses `SignalSender::send_and_wait`; the runtime releases its acknowledgement only after Sink delivery, SQLite checkpoint persistence, and Source acknowledgement, after which the Kafka channel commits offset + 1 synchronously. Invalid or foreign-key records are skipped and committed. A crash after the connector checkpoint but before the Kafka commit can replay the record; an active `execute-snapshot` ID is therefore handled idempotently.

The controller currently implements `incremental.snapshot.watermarking.strategy=insert_insert`. For each key-ordered chunk it commits an open watermark, captures the current maximum key on the first chunk, reads at most `incremental.snapshot.chunk.size` rows through the shared text converter, and commits a close watermark. By default the key is the table primary key. A surrogate key must be `NOT NULL` and backed by a valid, non-partial, single-column unique index; it replaces only chunk boundaries and ordering. The primary key remains mandatory and keys the deduplication window. WAL creates, updates, and deletes between the watermarks remove matching primary keys from that window. Rows that remain at close are emitted as read events with the Debezium `incremental` snapshot marker before the close commit boundary.

With `read.only=true`, the chunk connection does not insert watermark rows. It allocates a transaction ID once per chunk, captures `pg_current_snapshot()` before and after the bounded query, and retains the snapshot's `xmin`, `xmax`, and in-progress XID set. WAL transaction IDs open the window at the low `xmin`; the window closes only after the high watermark is visible or the maximum transaction that was in progress has committed. The commit event that closes the window is also the checkpoint boundary. The same transaction ID can safely close subsequent chunks immediately when the watermarks show that no older transaction remains. Restart discards transient watermarks and rereads the current key range.

Connector-state format version 4 stores the signal ID, expanded collections, per-collection conditions, surrogate key, collection index, last key, maximum key, chunk sequence, and pause state. The close commit checkpoints the advanced state atomically with delivered rows. A crash before that checkpoint re-reads the same bounded chunk, which permits duplicates but prevents a gap; a restart after it starts at the next key. Version 1 through 3 schema-history payloads remain readable with defaults for the new fields. The in-memory window is deliberately not persisted.

`pause-snapshot` prevents the next chunk from being prepared after the current close boundary. The paused flag is checkpointed, so restart remains paused. `resume-snapshot` schedules the next chunk after its own signal transaction commits. `stop-snapshot` clears all progress when `data-collections` is absent, or removes only collections matched by its fully matched expressions; stopping the current collection resets its key boundaries and advances safely after the control transaction. Unknown and out-of-order watermark IDs are ignored.

With `incremental.snapshot.allow.schema.changes=false`, the chunk connection rediscovers the table immediately after the open watermark and compares fields plus PostgreSQL type identity before building SQL. A changed `Relation` for the active table is also rejected while streaming. Either guard stops the source before the old window can be decoded against a new layout, leaving the last acknowledged checkpoint intact. Debezium documents PostgreSQL schema changes during incremental snapshots as unsupported; Rustium therefore rejects `incremental.snapshot.allow.schema.changes=true` rather than claiming unsafe compatibility.

If current catalog discovery fails while replaying a historical `Relation`, Rustium keeps the WAL-provided names, order, OIDs, typmods, and key flags. Type-name lookup first reuses an exact checkpointed column identity; if neither catalog nor history can resolve it, the field receives a conservative `unknown_oid_*` name. Both fallback paths are observable warnings and preserve decoding order without inventing nullability or defaults.

#### 9.5 Remaining PostgreSQL gates

- live PostGIS/pgvector fixtures where installed;
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

`gtid.source.includes` and `gtid.source.excludes` are mutually exclusive lists of case-insensitive regular expressions matched against the complete source UUID. Rustium filters a complete captured executed-GTID SID set before a GTID-based dump request. A non-empty filtered set selects GTID startup; an empty set falls back to the captured file/position with an explicit warning. A streaming position carries the current transaction GTID, not a complete executed set, and is therefore never reused as `COM_BINLOG_DUMP_GTID` replica state; reconnect from it uses the exact file/position anchor. `gtid.source.filter.dml.events=true` additionally marks transactions from non-matching UUIDs and suppresses only their row DML. Query/DDL processing and the transaction commit boundary remain active, so schema history stays ordered and the checkpoint advances without exposing filtered business rows. Setting the property to `false` disables DML suppression without disabling complete recovery-anchor filtering.

MySQL schema history is a versioned connector-state payload containing the ordered field model for selected tables in captured databases. Snapshot completion establishes the baseline. On restart, Rustium restores that baseline from the checkpoint before opening the binlog, then parses and applies selected-table DDL query events in source order. The updated schema state is attached to the DDL transaction boundary, so sink acknowledgement, source position, and schema state advance together. Discovery and DDL application ignore unselected tables so independent connectors sharing one database cannot mutate each other's history.

The current `sqlparser`-based MySQL DDL path handles `CREATE TABLE`, `ALTER TABLE` add/drop/rename/modify/change column operations, primary-key changes, `DROP TABLE`, `RENAME TABLE`, and schema-neutral `TRUNCATE TABLE`. Parsing or state-application failures stop the connector by default. `schema.history.internal.skip.unparseable.ddl=true` matches Debezium's opt-in skip behavior and logs the metadata-risk warning.

When `connect.keep.alive=true`, an ended or failed binlog stream is reopened from the last SourceRecord successfully sent into the bounded runtime queue. The rewind restores the binlog filename, table-map anchor, GTID transaction counters, row ordinal, and replay filter so completed records are skipped deterministically. `connect.keep.alive.interval.ms` controls the delay between attempts; `rustium.source.reconnect.max.attempts` is Rustium's finite extension and defaults to 10. The attempt number and last error are logged, and the budget resets after new source progress.

`heartbeat.interval.ms` defaults to zero and disables visible heartbeats. A positive interval emits a heartbeat from the latest safe streaming position, so its sink acknowledgement and checkpoint follow the normal at-least-once sequence without inventing binlog progress. Debezium JSON uses the `serverName` key and `ts_ms` value. Topic resolution prefers `topic.heartbeat.name`; otherwise it joins `topic.heartbeat.prefix` (or legacy `heartbeat.topics.prefix`) with `topic.prefix`.

`heartbeat.action.query` optionally runs first on a separate ordinary MySQL connection at each positive heartbeat interval. Query failures stop the source with the database error. Changes made by the query do not become source progress until the replication stream observes them.

When MySQL emits `JsonDiff` values under `binlog_row_value_options=PARTIAL_JSON`, Rustium applies replace, insert, and remove paths to the complete before-image JSON and emits the reconstructed after image. If the before image or path is incomplete, it emits `Unavailable` for that field instead of fabricating a value.

MySQL also supports Debezium-compatible source-table, file, bounded in-process, and Kafka signal channels. `signal.data.collection` identifies one `database.table` containing `id`, `type`, and `data`; Rustium reads source-table inserts from the binlog and never writes watermarks to that table. File lines use the same JSON envelope as the runtime `SignalSender`. `execute-snapshot` expands fully matched collection expressions, requires each selected table to have a primary key, captures a fixed maximum key per collection, and advances with typed single-column or composite-key comparisons. It emits `source.snapshot=true` with `rustium.snapshot.kind=incremental` and persists the current key, maximum key, collection, chunk sequence, and pause state in connector state. Version 3 also retains a bounded history of completed or stopped execute signal IDs, making replay idempotent across restart and the connector-checkpoint/Kafka-offset crash window. Version 2 offset progress remains readable and safely restarts its current collection from the beginning rather than risking a skipped row.

The event loop schedules at most one incremental chunk per turn and only outside an active binlog transaction. Each query is enclosed by low and high binlog coordinates. Rustium buffers the chunk, emits ordinary CDC records while the replication stream reaches the high coordinate, and removes keys found in create, update, or delete before/after images inside `(low, high]`. Only the remaining rows are emitted before the chunk commit atomically advances the typed keyset state. Binlog records, pause/resume/stop controls, and external signal acknowledgements are therefore observed between chunks. Schema-history updates preserve the active incremental state instead of replacing it. An in-memory window is intentionally not checkpointed: reconnect discards it and rereads the uncommitted key range, while a schema change observed during an open window fails before any layout-mismatched read is emitted. The Kafka channel reuses the single-partition, checkpoint-coupled `rustium-signal-kafka` implementation and Debezium topic/key contract.

#### 10.4 TLS modes

- `disabled`: plaintext only.
- `preferred`: try encrypted transport, then fall back to plaintext.
- `required`: encrypted transport without CA or hostname verification.
- `verify_ca`: encrypted transport with CA verification, without hostname verification.
- `verify_identity`: encrypted transport with CA and hostname verification.

`database.ssl.ca` supplies a PEM/DER CA file for Rustls. `database.ssl.cert` and `database.ssl.key` supply a PEM/DER client certificate and private key and must be configured together. Java-specific Debezium truststore/keystore conversion remains unsupported; use PEM/DER paths in the Rustium-compatible properties.

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

Unit gates cover checkpoint/state atomicity, version 1/2/3 checkpoint compatibility, schema-history serialization, incremental keyset progress/control, completed-signal replay, PostgreSQL snapshot/file/in-process signal parsing, MySQL signal parsing, durable runtime signal acknowledgement, management gating, replay-state rewind, scalar and extension conversions, heartbeat encoding, selected-table isolation, and create/alter/drop/rename DDL application. A librdkafka MockCluster gate verifies Kafka key filtering, single-partition consumption, and no offset commit before durable signal acknowledgement. The external PostgreSQL 17 gate verifies forced replication-backend termination and automatic recovery, explicit failure after a checkpoint's slot is lost, periodic heartbeat emission, successful `heartbeat.action.query`, heartbeat-table filtering, source/file/in-process-signaled chunking, immediate external-signal checkpointing, file and in-process read-only signaling without a signal table, checkpoint restart, additional conditions, concurrent-update deduplication, pause/resume/scoped-stop control, read-only transaction watermarks under a held update, restricted table permissions, zero connector watermark writes, unique surrogate-key ordering, completion cleanup, signal-table isolation, and snapshot/WAL equality for hstore, domains, enums, and tsvector. An opt-in superuser fixture temporarily limits `max_slot_wal_keep_size`, drives a slot to `wal_status=lost`, verifies fail-closed resume, and restores the original setting; it must be run on an isolated PostgreSQL instance. An optional fixture adds vector/halfvec/sparsevec and PostGIS geometry/geography when those extensions are installed; neither extension is installed on the current PostgreSQL 17 test instance. The external MySQL 8.4 gate verifies periodic heartbeat emission during an idle stream, exact-server-UUID GTID startup, checkpoint recovery, destructive-DDL recovery, in-process incremental snapshot execution, typed keyset restart after deletion/insertion, durable completed signal IDs, and deduplication of a row updated while its chunk window is open; the test requires temporary free space on the MySQL data volume.

#### 10.6 Remaining MySQL gates

- Java-specific truststore/keystore conversion to Rustls materials;
- wider DDL/type fixtures;

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

When `heartbeat.interval.ms` is positive, SQL Server emits a heartbeat at the latest safe CDC commit position. `heartbeat.action.query` runs first on a separate ordinary SQL connection; query failures stop the source and do not advance the CDC cursor until observed by normal polling. Topic prefix and full-name overrides follow the same Debezium names as PostgreSQL and MySQL.

Update operation 3 and 4 rows are paired into one logical update event. Queries are globally ordered and bounded by `streaming.fetch.size`; transaction boundaries use commit LSN. A cursor distinguishes a completed commit from a partial batch at the same commit LSN, so fetch-size limits cannot strand the remaining rows or an update after-image. Mid-transaction recovery replays from the commit LSN, counts skipped records, and preserves transaction ordering.

Snapshot queries use the same per-column SQL projection as CDC change-table queries before entering the shared Rust converter. This avoids driver-specific differences for money, fractional temporal precision, UUID, binary, and text values. Snapshot rows are ordered by primary key when one is present. The external type matrix requires identical `DataValue` rows across snapshot and CDC for bit, integer, decimal/money, real/float, UUID, varbinary, date, time, datetime2, datetimeoffset, and Unicode text.

SQL Server supports Debezium-compatible source-table, file, bounded in-process, and Kafka signal channels. An incremental snapshot from any channel requires a writable source signal table named as `schema.table` or `database.schema.table`; it must contain exactly text-compatible `id`, `type`, and `data` columns in order, `id` must hold at least 42 characters, and the table must have its own active CDC capture instance. Source validation also checks object-level `INSERT` permission. Rustium includes that capture instance even when business table filters exclude it, consumes user create operations as commands, inserts internal open/close watermark rows, and suppresses all signal-table rows from business output. JMX configuration maps to the bounded in-process channel with an explicit compatibility warning.

`execute-snapshot` expands fully matched short or database-qualified collection expressions, requires a primary key, captures a fixed maximum key, and advances typed single/composite keysets. Connector state version 1 persists collection progress, additional conditions, current/maximum keys, chunk sequence, pause state, and a bounded history of completed or stopped signal IDs. One chunk runs per event-loop turn and only at a completed CDC commit boundary. Rustium inserts a unique open watermark and waits for its CDC create event before querying the chunk. It then inserts a distinct close watermark, emits ordinary CDC records while removing matching before/after primary keys from the buffered rows, validates the source schema again, and emits remaining reads at the close watermark commit. The synthetic chunk commit atomically advances the keyset state; source-table controls are checkpointed on their CDC commit, while Kafka offsets remain coupled to the runtime acknowledgement. An in-memory opening or open window is not persisted and is recreated after restart. `pause-snapshot`, `resume-snapshot`, and scoped `stop-snapshot` are idempotent across restart.

Multiple database names require explicit partition-aware ordering and checkpoint ownership. Rustium rejects them until that contract is tested. The external gate has verified snapshot handoff, fetch-size-one update pairing, mid-transaction replay with preserved ordinals, concurrent commit ordering, retention fail-closed behavior, heartbeat/action-query, the core type matrix, in-process keyset restart, source-table signaling with additional conditions, CDC-window concurrent-update deduplication, and resource cleanup against SQL Server 2022 Developer RTM-CU25. Container portability and extended spatial/special-type fixtures remain open.

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
| `POST /v1/connector/signals` | bounded in-process signal submission when mutations and the channel are enabled |
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

This gate creates isolated selected tables with the admin account and uses the CDC account for capture. It verifies snapshot/replication, exact-server-UUID GTID-filtered startup, `heartbeat.action.query`, checkpointed schema versions 1 and 2 across destructive DDL, periodic idle-stream heartbeats from a safe binlog position, and cleanup. It has passed against MySQL 8.4 with row binlog and GTID enabled.

The ignored external PostgreSQL test reads connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com \
RUSTIUM_POSTGRES_TEST_PORT=5432 \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me' \
RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo \
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

These tests create isolated business-table/signal-table/publication/slot/role names and temporary signal files, then verify snapshot rows, ordered transactional create/update/delete events, checkpoint stop, an old-schema row, destructive drop/add-column DDL, a new-schema row, historical `Relation` replay with schema versions 1 and 2, restart without snapshot replay, forced termination and automatic recovery of the active replication backend, fail-closed resume after deleting a checkpoint's slot, periodic heartbeat records at a safe WAL position, `heartbeat.action.query`, heartbeat-table filtering, checkpointed source/file/in-process incremental snapshots, immediate external-signal state checkpointing, filtered chunks, concurrent-update deduplication, pause/resume/scoped-stop control, file and in-process read-only snapshots without a signal table, read-only transaction watermarks with a held update, zero watermark writes under restricted permissions, unique surrogate ordering against a reversed UUID primary-key order, signal-table isolation, and identical snapshot/WAL conversion across the core PostgreSQL type matrix including hstore, domain, enum, and tsvector values. The opt-in superuser fixture has also driven a live slot to `wal_status=lost` and verified fail-closed resume on PostgreSQL 17. An optional fixture exercises pgvector and PostGIS types when installed. The mandatory gates pass against PostgreSQL 17 with `wal_level=logical`; the current instance has neither optional extension installed.

The optional external Kafka signal gate creates a unique single-partition topic, sends a foreign-key record followed by a connector-key record, verifies that only the matching signal is delivered, observes the skipped offset, releases the durable acknowledgement, verifies the next committed offset, and deletes the topic:

```bash
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-signal-kafka --test kafka_external -- --ignored --nocapture
```

The ignored external SQL Server test reads connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com \
RUSTIUM_SQLSERVER_TEST_PORT=1433 \
RUSTIUM_SQLSERVER_TEST_USER=sa \
RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me' \
RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo \
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

This gate creates isolated table/capture-instance names, waits for SQL Agent initialization, and verifies snapshot rows, fetch-size-one transaction continuation, mid-transaction checkpoint replay, concurrent commit ordering, retention failure, heartbeat/action-query, core snapshot/CDC type equality, checkpointed in-process keyset restart, source-table signaling with additional conditions, signal-table isolation, and cleanup. It has passed against SQL Server 2022 Developer RTM-CU25.

The separate SQL Server Docker portability gate is runnable where the Microsoft image is available:

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 16. Roadmap

1. Close PostgreSQL optional live PostGIS/pgvector and real-broker Kafka recovery gates.
2. Close MySQL Java TLS-store conversion, wider DDL/type, and real-broker Kafka recovery gates.
3. Close SQL Server container-portability, extended-type, and real-broker Kafka gates.
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
| MySQL Source | 已实现；MySQL 8.4 Docker 和外部 GTID/破坏性 DDL 重启门槛通过 |
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
10. **控制位点跟随 checkpoint。** 持久外部信号位点只有在其 connector state 完成 checkpoint 和确认后才推进。

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
|-- rustium-signal-kafka/  与 checkpoint 耦合的 Kafka 信号输入
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
  |
  v
确认持久外部信号位点
```

当前运行时使用一个 Source task 和一个有序投递协调器。在明确排序屏障和分区契约之前，项目有意限制并行度。

#### 5.1 提交顺序

每个非空批次或仅位点批次执行：

1. 编码全部数据记录。
2. 调用 `Sink::write` 并等待确认。
3. 在一个 SQLite checkpoint 事务中保存源位点和版本化连接器状态。
4. 通过 Source 确认 channel 发布已保存位点。
5. 释放附着到该 checkpoint 的外部信号确认。
6. 更新状态计数器。

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

`DataValue` 区分 null、boolean、有符号/无符号整数、decimal 文本、float、string、bytes、date、time、timestamp、UUID、JSON、array、字符串键 map 和 unavailable。

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

恢复已完成 checkpoint 之前，Rustium 会验证原 replication slot 仍然存在、仍使用 `pgoutput`，且没有进入 PostgreSQL `wal_status=unreserved` 或 `lost`。连续性检查失败时会在复制传输可能创建替代 slot 之前停止，并要求 reset checkpoint 后重新执行 initial snapshot。该契约有意选择显式恢复，而不是接受静默 WAL 缺口。

PostgreSQL 不会在 `Relation` 中记录原始 DDL、列可空性或 default。如果短暂历史列已经从当前 catalog 消失，且 checkpoint 基线中也不存在，Rustium 会通过 OID/typmod 解析类型，并保守地标记为 optional。这样可保持行解码和顺序正确，同时不伪造 WAL 未提供的元数据。

快照查询通过 PostgreSQL 的 `::text` 输出函数逐列投影，不再让整行经过 JSON 中间层。快照值和 `pgoutput` 值因此共用同一个转换器，可一致保留 numeric scale/precision、bytea、JSON 文本、时间格式和数组语法。数组解析器支持带引号和转义的元素、SQL NULL 与字符串 `"NULL"` 的区别、显式下界、嵌套维度和按元素类型转换。畸形数组文本会完整保留为字符串，不会被部分解码。

Catalog 发现会把 PostgreSQL domain 解析为基础转换类型，同时保留 domain OID/typmod 作为 schema 身份；domain 数组使用相同的元素转换。Enum、range、网络类型和 `tsvector` 使用 PostgreSQL 规范文本。`hstore.handling.mode=json` 把 hstore 输出映射为 JSON，`map` 映射为字符串键 `DataValue::Map`，并支持 null、转义和 hstore 数组。`vector` 与 `halfvec` 转为浮点数组，`sparsevec` 转为包含维度和索引值的 map，PostGIS geometry/geography 转为完整 EWKB 字节。所有专用解析器都是全有或全无；畸形或未知输入会回退为原始字符串。

`heartbeat.interval.ms` 默认为零。设置为正数后，会在最新已进入有界队列的 SourceRecord 位点发送可见 heartbeat；首条 streaming event 之前则使用已完成快照的锚点。没有源位点时不会发送 heartbeat。可选的 `heartbeat.action.query` 在每个周期先通过复用的普通 SQL 连接执行。查询产生的 WAL 只有在 `pgoutput` 实际返回后才算进度，查询失败会携带数据库错误停止 Source。heartbeat 表可以加入 publication 但被选表规则排除；其事务 commit 仍可推进安全 WAL 位点，而不会暴露成业务表事件。

#### 9.4 Source 信号与增量快照

信号实现遵循 Debezium `source`、`file`、`in-process` 和 `kafka` channel。`signal.data.collection` 指向一个 schema-qualified 表，该表必须按顺序且仅包含 `id`、`type`、`data` 三个文本兼容列，必须加入 publication，并始终从业务快照和事件中过滤。`signal.enabled.channels` 接受这四个已实现 channel 的任意组合。`signal.file` 默认为 `file-signals.txt`，`signal.poll.interval.ms` 默认为 5000 ms。File reader 消费非空 JSON Lines，并在成功读取后清空文件，遵循 Debezium 无重试 channel 语义。`execute-snapshot` 记录接受 `type=incremental`、`data-collections` 中的完整匹配正则、包含大小写不敏感集合表达式和 SQL filter 的 `additional-conditions`，以及可选 `surrogate-key` 列名。

禁用 `source` channel 时会忽略 source-table command，但连接器生成的 watermark record 对可写外部信号快照仍然有效。外部 watermark action type 会被拒绝。每个有效外部 action 之后，Rustium 会在最新安全 LSN 发出仅包含 connector state 的位点事务边界；fresh stream 从 slot 的 `confirmed_flush_lsn` 或 `restart_lsn` 获取初始位点。因此，即使没有业务 WAL，控制进度仍可 checkpoint。只读外部信号不需要信号表；可写外部信号仍需要信号表承载 `insert_insert` watermark。

`ConnectorRuntime::signal_sender()` 在 runtime 所有权移入 `run` 前暴露可 clone、强类型且有界的 `SignalSender`。PostgreSQL 只在没有活动 WAL 事务时消费 command，并让 file、in-process 和 Kafka record 共用同一 action controller 和 checkpoint 路径。只有启用 `in-process` 时，CLI 才将 sender 接入 `POST /v1/connector/signals`。该路由要求启用管理变更，命令入队后返回 `202`，禁用变更端点或 channel 时分别返回 `403` 或 `409`。

Debezium 的 `jmx` signal channel 是把记录写入内存队列的 JVM MXBean。为迁移 properties，Rustium 将 `signal.enabled.channels=jmx` 映射到同一个有界 `in-process` channel，发出兼容警告，并通过嵌入式 `SignalSender` 与 HTTP 管理路由暴露。这样保留队列和 action 语义，但不会声称兼容 JVM/RMI 协议；`jmx,in-process` 会去重为一个 channel。

`rustium-signal-kafka` 实现 `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms` 和去掉前缀后的 `signal.consumer.*` 透传参数。Topic 默认为 `<topic.prefix>-signal`，必须恰好只有一个 partition，并过滤 key 不等于 `topic.prefix` 的 record。自动 commit 和自动 offset store 会被强制关闭。有效 record 通过 `SignalSender::send_and_wait` 投递；runtime 只有在完成 Sink 投递、SQLite checkpoint 持久化和 Source 确认后才释放确认，随后 Kafka channel 同步提交 offset + 1。无效 record 或其他 connector key 的 record 会被跳过并提交。如果在 connector checkpoint 后、Kafka commit 前崩溃，record 可能重放，因此活动中的同一 `execute-snapshot` ID 会被幂等处理。

控制器当前实现 `incremental.snapshot.watermarking.strategy=insert_insert`。对于每个按 key 排序的 chunk，它先提交 open watermark，在首个 chunk 捕获当前最大 key，通过共享文本转换器读取不超过 `incremental.snapshot.chunk.size` 行，再提交 close watermark。默认 key 为表主键。surrogate key 必须是 `NOT NULL`，且具有有效、非 partial 的单列唯一索引；它只替代 chunk 边界和排序。主键仍为必需，并作为去重窗口 key。两个 watermark 之间的 WAL create、update 和 delete 会按主键从该窗口移除对应行。close 时剩余行在 close commit 边界之前作为 read event 发出，并带 Debezium `incremental` snapshot marker。

当 `read.only=true` 时，chunk 连接不会插入 watermark 行。它为每个 chunk 分配一次 transaction ID，在有界查询前后捕获 `pg_current_snapshot()`，并保留快照的 `xmin`、`xmax` 和进行中 XID 集合。WAL transaction ID 在 low `xmin` 打开窗口；只有 high watermark 可见或当时仍进行中的最大事务已经提交后才关闭窗口。关闭窗口的 commit event 同时作为 checkpoint 边界。如果水位表明没有更旧事务，同一个 transaction ID 可以安全地立即关闭后续 chunk。重启时丢弃瞬时水位并重新读取当前主键范围。

Connector-state format version 4 保存 signal ID、展开后的集合、每集合 condition、surrogate key、集合索引、last key、maximum key、chunk sequence 和 pause 状态。close commit 将推进后的状态与已投递行原子 checkpoint。若在此之前崩溃，会重新读取同一个有界 chunk，可能重复但不会产生缺口；若在此之后重启，则从下一 key 开始。Version 1 到 3 schema-history payload 会以新字段默认值继续读取。内存窗口有意不持久化。

`pause-snapshot` 在当前 close 边界后阻止准备下一 chunk。pause 标记会被 checkpoint，因此重启后仍保持暂停。`resume-snapshot` 在自身 signal 事务提交后安排下一 chunk。`stop-snapshot` 在没有 `data-collections` 时清除全部进度，否则只移除完整匹配表达式选中的集合；停止当前集合会重置其主键边界，并在控制事务之后安全推进。未知或乱序 watermark ID 会被忽略。

当 `incremental.snapshot.allow.schema.changes=false` 时，chunk 连接会在 open watermark 后立即重新发现表，并在构造 SQL 前比较字段和 PostgreSQL 类型身份。流式阶段若活动表出现变化后的 `Relation` 也会被拒绝。任一保护都会在旧窗口按新布局解码前停止 Source，并保持最后已确认 checkpoint 不变。Debezium 明确记录 PostgreSQL 不支持增量快照期间的 schema change；因此 Rustium 会拒绝 `incremental.snapshot.allow.schema.changes=true`，不会声称不安全的兼容性。

重放历史 `Relation` 时，如果当前 catalog 发现失败，Rustium 会保留 WAL 提供的名称、顺序、OID、typmod 和 key 标志。类型名解析会先复用 checkpoint 中完全匹配的列身份；catalog 与历史都无法解析时，字段使用保守的 `unknown_oid_*` 名称。两条回退路径都会发出可观测 warning，在不伪造 nullable/default 元数据的前提下保持解码顺序。

#### 9.5 PostgreSQL 剩余门槛

- 服务器已安装扩展时的 PostGIS/pgvector 实测；
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

`gtid.source.includes` 和 `gtid.source.excludes` 是互斥列表，内容为大小写不敏感的正则表达式，并对完整 source UUID 进行匹配。Rustium 会在发出基于 GTID 的 dump request 前过滤完整捕获的 executed-GTID SID 集合。过滤后非空则使用 GTID 启动；空集合会记录明确警告，并回退到已捕获的 file/position。流式位点携带当前事务 GTID，而不是完整 executed set，因此绝不会被复用为 `COM_BINLOG_DUMP_GTID` replica state；从该位点重连时使用精确 file/position 锚点。`gtid.source.filter.dml.events=true` 还会标记来自不匹配 UUID 的事务，并只抑制其行级 DML。Query/DDL 处理和事务 commit 边界仍保持活动，因此 schema history 顺序不变，checkpoint 也会继续推进，同时不会暴露被过滤的业务行。将该参数设为 `false` 会关闭 DML 抑制，但不会关闭完整恢复锚点过滤。

MySQL schema history 是版本化 connector-state payload，保存已捕获数据库中选中表的有序字段模型。快照完成时建立基线。重启时，Rustium 在打开 binlog 前从 checkpoint 恢复该基线，然后按源顺序解析并应用选中表的 DDL query 事件。更新后的 schema 状态附着在对应 DDL 事务边界上，使 Sink 确认、源位点和 schema 状态一起推进。发现和 DDL 应用会忽略未选表，因此共享同一数据库的独立连接器不会相互修改历史状态。

当前基于 `sqlparser` 的 MySQL DDL 路径支持 `CREATE TABLE`、`ALTER TABLE` 增删/重命名/修改/变更列、主键变更、`DROP TABLE`、`RENAME TABLE`，以及 schema 不变的 `TRUNCATE TABLE`。默认情况下，解析或状态应用失败会停止连接器。`schema.history.internal.skip.unparseable.ddl=true` 与 Debezium 的显式跳过行为一致，并记录元数据风险警告。

当 `connect.keep.alive=true` 时，结束或失败的 binlog stream 会从最后一个成功送入有界运行时队列的 SourceRecord 重新打开。回卷过程恢复 binlog 文件名、table-map 锚点、GTID 事务计数、行序号和重放过滤器，从而确定性跳过已完成记录。`connect.keep.alive.interval.ms` 控制尝试间隔；`rustium.source.reconnect.max.attempts` 是 Rustium 的有限扩展，默认 10。日志会记录尝试次数和最后错误，产生新的源端进度后预算重置。

`heartbeat.interval.ms` 默认为零，即不发送可见 heartbeat。设置为正数后，从最新安全 streaming 位点发送 heartbeat，使其 Sink 确认和 checkpoint 遵循正常 at-least-once 顺序，同时不虚构 binlog 进度。Debezium JSON 使用 `serverName` key 和 `ts_ms` value。topic 优先使用 `topic.heartbeat.name`，否则将 `topic.heartbeat.prefix`（或旧参数 `heartbeat.topics.prefix`）与 `topic.prefix` 连接。

`heartbeat.action.query` 可选地在每个正 heartbeat 周期先通过独立普通 MySQL 连接执行。查询失败会携带数据库错误停止 Source；查询产生的变化只有在复制流实际读到后才算源进度。

当 MySQL 在 `binlog_row_value_options=PARTIAL_JSON` 下发送 `JsonDiff` 时，Rustium 会把 replace、insert、remove path 应用到完整 before-image JSON，并发出重建后的 after image。如果 before image 或 path 不完整，则将该字段标记为 `Unavailable`，不会伪造值。

MySQL 同时支持 Debezium 兼容的源表、文件、有界进程内和 Kafka signal channel。`signal.data.collection` 指定一个包含 `id`、`type`、`data` 的 `database.table`；Rustium 从 binlog 读取源表插入，但不会向该表写 watermark。文件每行一个 JSON envelope，与 `SignalSender` 使用同一格式。`execute-snapshot` 会展开完整匹配的集合表达式，要求每张目标表具有主键，为每个集合固定最大主键，并使用带类型的单列或复合主键比较推进。它发出 `source.snapshot=true` 和 `rustium.snapshot.kind=incremental`，并在 connector state 中保存当前主键、最大主键、集合、chunk 序号和暂停状态。Version 3 还保留有界的已完成或已停止 execute signal ID 历史，使重启以及 connector checkpoint/Kafka offset 崩溃窗口内的重放保持幂等。Version 2 的 offset 进度仍可读取，但会从当前集合开头安全重读，不会冒跳行风险。

事件循环每轮最多安排一个增量 chunk，并且只在没有活动 binlog 事务时执行。每次查询都会捕获低/高 binlog 坐标；Rustium 先缓存 chunk，在复制流追到高水位期间正常发出 CDC record，并移除 `(low, high]` 内 create、update、delete 的 before/after image 中出现的主键。只有剩余行会在 chunk commit 前发出，commit 同时原子推进带类型的 keyset 状态。因此 binlog record、pause/resume/stop 控制和外部信号确认都可以在 chunk 之间被处理。Schema history 更新会保留活动增量状态，不会覆盖它。内存窗口不会进入 checkpoint：重连会丢弃窗口并重读未提交 key 范围；窗口打开期间观察到 schema change 时，会在发出布局不匹配的 read 之前失败。Kafka 复用单 partition、与 checkpoint 绑定的 `rustium-signal-kafka` 实现及 Debezium topic/key 合约。

#### 10.4 TLS 模式

- `disabled`：仅明文。
- `preferred`：先尝试加密，失败后回退明文。
- `required`：加密，但不校验 CA 或主机名。
- `verify_ca`：加密并校验 CA，不校验主机名。
- `verify_identity`：加密并同时校验 CA 和主机名。

`database.ssl.ca` 指定 Rustls 使用的 PEM/DER CA 文件；`database.ssl.cert` 与 `database.ssl.key` 指定 PEM/DER 客户端证书和私钥，并且必须成对配置。Java 专用 Debezium truststore/keystore 转换仍不支持；请在 Rustium 兼容参数中使用 PEM/DER 路径。

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

单元门槛覆盖 checkpoint/state 原子性、version 1/2/3 checkpoint 兼容、schema-history 序列化、增量 keyset 进度/控制、已完成 signal 重放、PostgreSQL snapshot/file/in-process 信号解析、MySQL 信号解析、持久 runtime 信号确认、管理端点门槛、重放状态回卷、标量与扩展类型转换、heartbeat 编码、选表隔离，以及 create/alter/drop/rename DDL 应用。librdkafka MockCluster 门槛会验证 Kafka key 过滤、单 partition 消费，以及持久信号确认前不提交 offset。外部 PostgreSQL 17 门槛验证强制终止 replication backend 后自动恢复、checkpoint 对应 slot 丢失后的显式失败、周期 heartbeat、成功执行 `heartbeat.action.query`、heartbeat 表过滤、source/file/in-process 信号分块、外部信号即时 checkpoint、完全无信号表的 file 和 in-process 只读信号、checkpoint 重启、additional condition、并发更新去重、pause/resume/scoped-stop 控制、保持更新事务时的只读事务水位、受限表权限、零连接器 watermark 写入、唯一 surrogate-key 排序、完成状态清理、信号表隔离，以及 hstore、domain、enum、tsvector 的快照/WAL 一致性。可选 superuser fixture 会临时限制 `max_slot_wal_keep_size`、让 slot 进入 `wal_status=lost`、验证 fail-closed 恢复并还原原设置，必须在隔离的 PostgreSQL 实例上运行。可选扩展 fixture 会在已安装相应扩展时增加 vector/halfvec/sparsevec 和 PostGIS geometry/geography；当前 PostgreSQL 17 测试实例未安装这两个扩展。外部 MySQL 8.4 门槛还会验证空闲 stream 周期 heartbeat、精确 server UUID 的 GTID 启动、checkpoint 恢复、破坏性 DDL 恢复、删除/插入后的带类型 keyset 重启、已完成 signal ID 持久化，以及 chunk 窗口打开期间更新行的去重。

#### 10.6 MySQL 剩余门槛

- Java 专用 truststore/keystore 到 Rustls 材料的转换；
- 更广 DDL/类型样例；

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

当 `heartbeat.interval.ms` 为正数时，SQL Server 会在最新安全 CDC commit 位点发送 heartbeat。`heartbeat.action.query` 会先通过独立普通 SQL 连接执行；查询失败会停止 Source，且只有正常 CDC 轮询观察到查询产生的变化后才推进 CDC cursor。topic 前缀和完整名称覆盖参数与 PostgreSQL、MySQL 使用相同的 Debezium 命名。

Update operation 3 和 4 会配对为一个逻辑 update 事件。查询按全局顺序排列，并由 `streaming.fetch.size` 限制；事务边界使用 commit LSN。Cursor 会区分已经完成的 commit 与同一 commit LSN 内的部分 batch，因此 fetch-size 限制不会遗留剩余行或 update after-image。事务中间恢复会从 commit LSN 重放、统计已跳过记录，并保留事务顺序。

快照查询会先使用与 CDC change-table 查询相同的逐列 SQL 投影，再进入共享 Rust converter，从而消除 driver 在 money、小数时间精度、UUID、binary 和 text 值上的差异。有主键时快照按主键排序。外部类型矩阵要求 bit、整数、decimal/money、real/float、UUID、varbinary、date、time、datetime2、datetimeoffset 和 Unicode text 在快照与 CDC 路径上产生完全相同的 `DataValue`。

SQL Server 支持 Debezium 兼容的 source table、file、有界 in-process 和 Kafka signal channel。任何 channel 的增量快照都要求可写 source signal table；它可以使用 `schema.table` 或 `database.schema.table`，必须按顺序且仅包含文本兼容的 `id`、`type`、`data` 列，`id` 至少容纳 42 个字符，并具有独立活动 CDC capture instance。Source 校验还会检查对象级 `INSERT` 权限。即使业务 table filter 排除了它，Rustium 也会发现该 capture instance、将用户 create operation 作为 command 消费、插入内部 open/close watermark，并从业务输出中抑制所有 signal-table 行。JMX 配置会映射到有界 in-process channel，并发出明确兼容 warning。

`execute-snapshot` 会展开完整匹配的短名称或 database-qualified 集合表达式，要求主键、固定最大主键，并推进带类型的单列/复合 keyset。Connector state version 1 持久化集合进度、additional condition、当前/最大 key、chunk 序号、pause 状态，以及有界的已完成或已停止 signal ID 历史。事件循环每轮只在完整 CDC commit 边界执行一个 chunk。Rustium 插入唯一 open watermark，并等待其 CDC create event 后才查询 chunk；随后插入不同 ID 的 close watermark，在正常发出 CDC record 的同时根据 before/after 主键从缓存中移除对应行，再次校验源表 schema，并在 close watermark commit 处发出剩余 read。合成 chunk commit 会原子推进 keyset 状态；source-table 控制在自身 CDC commit 上 checkpoint，Kafka offset 继续与 runtime acknowledgement 绑定。内存中的 opening/open window 不持久化，重启后会重新创建。`pause-snapshot`、`resume-snapshot` 和 scoped `stop-snapshot` 在重启后保持幂等。

多个数据库名称需要显式的分区感知排序和 checkpoint 所有权。在该契约经过测试前，Rustium 会直接拒绝。外部门槛已在 SQL Server 2022 Developer RTM-CU25 上验证快照切换、fetch size 为 1 的 update 配对、保持序号的事务中间重放、并发 commit 排序、retention fail-closed、heartbeat/action-query、核心类型矩阵、in-process keyset 重启、带 additional condition 的 source-table signaling、CDC window 并发更新去重和资源清理。容器可移植性和扩展空间/特殊类型 fixture 仍待补齐。

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
| `POST /v1/connector/signals` | 启用变更端点和 channel 时有界提交 in-process 信号 |
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

该门槛使用管理账号创建隔离的选中表，并使用 CDC 账号捕获。测试验证快照/复制、基于精确 server UUID 的 GTID 过滤启动、`heartbeat.action.query`、破坏性 DDL 前后 checkpoint 的 schema version 1 和 2、从安全 binlog 位点发送的空闲周期 heartbeat，以及资源清理。测试已在启用行级 binlog 和 GTID 的 MySQL 8.4 上通过。

被忽略的 PostgreSQL 外部测试从环境变量读取连接配置，仓库中不包含测试凭据：

```bash
RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com \
RUSTIUM_POSTGRES_TEST_PORT=5432 \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me' \
RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo \
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

这些测试使用隔离的业务表/信号表/publication/slot/role 名称和临时信号文件，验证快照记录、同一事务内有序的 create/update/delete 事件、checkpoint 停止、旧 schema 行、破坏性删列/加列 DDL、新 schema 行、schema version 1 和 2 的历史 `Relation` 重放、重启不重复快照、强制终止活动 replication backend 后自动恢复、删除 checkpoint 对应 slot 后 fail-closed 恢复、安全 WAL 位点上的周期 heartbeat、`heartbeat.action.query`、heartbeat 表过滤、带 checkpoint 的 source/file/in-process 增量快照、外部信号状态即时 checkpoint、过滤分块、并发更新去重、pause/resume/scoped-stop 控制、完全无信号表的 file 和 in-process 只读快照、保持更新事务时的只读事务水位、受限权限下零 watermark 写入、与 UUID 主键反向顺序对照的唯一 surrogate 排序、信号表隔离，以及包含 hstore、domain、enum、tsvector 的 PostgreSQL 核心类型矩阵在快照/WAL 路径上的一致转换。可选 superuser fixture 也已在 PostgreSQL 17 上让真实 slot 进入 `wal_status=lost` 并验证 fail-closed 恢复。可选扩展 fixture 会在已安装时实测 pgvector 和 PostGIS 类型。必选门槛已在启用 `wal_level=logical` 的 PostgreSQL 17 上通过；当前实例未安装这两个可选扩展。

可选的外部 Kafka 信号门槛会创建唯一命名的单 partition topic，先发送其他 connector key 的 record，再发送目标 connector key 的 record，验证只投递匹配信号、观察跳过后的 offset、释放持久确认、验证下一已提交 offset，最后删除 topic：

```bash
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-signal-kafka --test kafka_external -- --ignored --nocapture
```

被忽略的 SQL Server 外部测试从环境变量读取连接配置，仓库中不包含测试凭据：

```bash
RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com \
RUSTIUM_SQLSERVER_TEST_PORT=1433 \
RUSTIUM_SQLSERVER_TEST_USER=sa \
RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me' \
RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo \
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

该门槛使用隔离的表/capture-instance 名称，等待 SQL Agent 初始化，并验证快照记录、fetch size 为 1 的事务继续读取、事务中间 checkpoint 重放、并发 commit 排序、retention 失败、heartbeat/action-query、核心快照/CDC 类型一致性、带 checkpoint 的 in-process keyset 重启、带 additional condition 的 source-table signaling、信号表隔离和资源清理。测试已在 SQL Server 2022 Developer RTM-CU25 上通过。

在可以访问 Microsoft 镜像的环境中，仍可运行独立的 SQL Server Docker 可移植性门槛：

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

### 16. 路线图

1. 补齐 PostgreSQL 可选 PostGIS/pgvector 实测和真实 broker Kafka 恢复门槛。
2. 补齐 MySQL TLS store、更广 DDL/类型和真实 broker Kafka 恢复门槛。
3. 补齐 SQL Server 容器可移植性、扩展类型和真实 broker Kafka 门槛。
4. 只有完成前三项后才考虑其他数据库。
5. 在 `1.0` 前补 Schema Registry 格式、打包、安全策略、运维手册和稳定升级迁移。
