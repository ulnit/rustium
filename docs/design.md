# Rustium Architecture and Design

> Status: Implemented alpha baseline
> Document version: 0.4
> Last updated: 2026-07-20

## Language Policy

Rustium documentation is published in complete English first and complete Simplified Chinese second. English is normative when translations differ. Code, configuration keys, APIs, logs, issues, and commit messages use English.

Rustium 文档先提供完整英文，再提供完整简体中文。两种文本不一致时以英文为准。代码、配置键、API、日志、Issue 和提交信息使用英文。

---

## English

### 1. Product Definition

Rustium is an independently implemented, open-source, log-based Change Data Capture platform written in Rust. It reads committed database changes, converts them into a database-neutral typed event, and delivers ordered records to downstream sinks.

Rustium is a standalone Rust service. Native sources do not require Kafka Connect or a JVM. Bridge-backed sources deliberately run a compatible Debezium engine for database protocols that depend on proprietary clients, node-local logs, or distributed stream APIs.

Rustium uses the latest Debezium architecture, event behavior, and configuration names as compatibility references. Rustium is not a Debezium fork and does not copy Debezium Java source code.

#### 1.1 Connector priority

Connector work follows this strict order:

1. PostgreSQL
2. MySQL
3. SQL Server
4. Oracle
5. MongoDB
6. MariaDB
7. Db2
8. Cassandra 3/4/5
9. Vitess
10. Spanner
11. Informix
12. CockroachDB
13. YashanDB

The first five sources are native Rust connectors. All remaining databases in Debezium's current source catalog use the durable Debezium Engine bridge and remain subject to the same correctness, recovery, type-coverage, and operational release contract.

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
- Claiming a native implementation for a proprietary CDC protocol that is actually executed by a bridge engine.

### 2. Current Implementation

The workspace contains a runnable alpha service.

| Component | State |
|---|---|
| Core event/position/runtime traits | Implemented |
| Bounded Tokio runtime | Implemented; required 256-cycle backpressure/retry soak enforced by CI |
| SQLite checkpoint v2 and connector state | Implemented; version 1 JSON remains readable |
| Native JSON, Debezium JSON, Confluent-framed JSON Schema, Avro, and Protobuf | Implemented; real Registry/Kafka gates enforced by CI |
| stdout and Kafka sinks | Implemented; Kafka real-broker delivery/failure gate enforced by CI |
| PostgreSQL source | Implemented; recovery, heartbeat/action-query, writable/read-only incremental-snapshot, and core type-matrix gates pass with PostgreSQL 17 |
| MySQL source | Implemented; required Docker CI and external MySQL 8.4 recovery/soak gates pass |
| SQL Server source | Implemented; required Docker CI and external SQL Server 2022 recovery/soak gates pass |
| Oracle LogMiner source | Implemented; unit/configuration gates pass; external Oracle gate is opt-in |
| MongoDB Change Stream source | Implemented; unit/configuration gates pass; external replica-set gate is opt-in |
| Remaining Debezium database sources | MariaDB, Db2, Cassandra 3/4/5, Vitess, Spanner, Informix, CockroachDB, and YashanDB implemented through the durable HTTP/Kafka bridge |
| CLI and HTTP management | Implemented |
| Reproducible non-root container image and Helm chart source | Implemented; packaging gate enforced by CI |
| Tagged-release image, Helm OCI chart, and GitHub Release automation | Implemented; runs only for matching protected `v*` tags |
| Published crates | All workspace crates and the CLI are published as `0.1.0-alpha.2`, including Oracle, MongoDB, and the durable Debezium bridge |

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
|-- rustium-format-avro/   Debezium-compatible Avro schema and datum encoding
|-- rustium-format-json/   native JSON, Debezium JSON, and JSON Schema descriptors
|-- rustium-format-protobuf/ Debezium-compatible Protobuf schema and message encoding
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

Cancellation stops source reads, flushes pending events when delivery can complete without another retry, persists only acknowledged positions, flushes and closes the sink, then closes protocol resources. Cancellation during Sink retry backoff interrupts the wait, leaves the pending position uncheckpointed for replay, performs the same cleanup, and reaches `STOPPED` without counting an operational failure. Any pipeline error enters the cleanup path: the Source is cancelled, aborted if it exceeds the shutdown timeout, and the Sink shutdown hook still runs. The earliest operational error is preserved and unacknowledged records are never checkpointed.

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

Debezium names are preferred for migration. Common mappings include `name`, `connector.class`, `topic.prefix`, `database.*`, `table.include.list`, `table.exclude.list`, `publication.autocreate.mode`, `snapshot.mode`, `snapshot.fetch.size`, `snapshot.include.collection.list`, `tombstones.on.delete`, `max.queue.size`, `max.batch.size`, and `poll.interval.ms`. Matching Confluent `JsonSchemaConverter`, `AvroConverter`, and `ProtobufConverter` key/value settings map to `debezium_json_schema`, `debezium_avro`, and `debezium_protobuf`. All three use one shared `schema.registry.url` list, `USER_INFO` basic authentication, automatic registration, request timeout, and bounded ID caches. Avro schema/field adjustment is deterministic and adjusted-name collisions fail encoding. Protobuf always scrubs invalid names and rejects options that would select different nullable or union wire contracts. Unsupported converter asymmetry, adjustment mode, or subject/version selection fails validation.

`snapshot.include.collection.list` is stored as native `snapshot.include_collections` and applies anchored regular expressions only while emitting an initial or `when_needed` recovery snapshot. PostgreSQL qualifies collections as `schema.table`, MySQL as `database.table`, and SQL Server as `database.schema.table`. Discovery and connector schema history still cover every table selected for streaming; only snapshot row scans are filtered. Incremental snapshots and subsequent streaming therefore retain the ordinary source filters. Required PostgreSQL 17, MySQL 8.4, and SQL Server 2022 integration gates select two tables for streaming, snapshot one, and require a later create event from the other.

The currently implemented Debezium snapshot modes are `initial`, `when_needed`, `never`, and the legacy alias `no_data`. Modes with different semantics, including `always`, `initial_only`, `schema_only`, `recovery`, and custom modes, fail validation instead of being silently remapped.

Rustium-only state, sink, server, logging, and producer extensions use `rustium.*` in properties files.

### 9. PostgreSQL Connector

#### 9.1 Prerequisites

- PostgreSQL 14 or newer.
- `wal_level=logical`.
- An existing `pgoutput` publication, or privileges to create the configured publication mode.
- Replication and table-read permissions.
- A unique replication slot.

Native YAML uses `source.publication_autocreate_mode` and defaults to `disabled` so existing deployments retain their publication ownership contract and semantic fingerprint. Debezium properties use `publication.autocreate.mode` and default to Debezium's `all_tables`. `disabled` requires the named publication to exist. `all_tables` creates a missing `FOR ALL TABLES` publication. `filtered` creates a missing publication or applies `ALTER PUBLICATION ... SET TABLE` to an existing table-scoped publication using the fully matched source filters; a configured signal table is included even though it is hidden from business events. `filtered` rejects an existing `FOR ALL TABLES` publication rather than silently changing its ownership scope. `no_tables` creates a missing empty publication and leaves an existing publication unchanged. Creation requires database `CREATE` privilege, table-scoped publication requires table ownership, changing an existing publication requires publication ownership, and `FOR ALL TABLES` requires superuser authority.

`replica.identity.autoset.values` is parsed as ordered fully matched table regular expressions with `DEFAULT`, `FULL`, `NOTHING`, or `INDEX <name>` targets. After publication preparation, Rustium reads the current identity and replica index for every publication table admitted by the source filters, excluding the signal table. It rejects overlapping rules before mutation, computes only identities that differ, and executes the complete `ALTER TABLE ... REPLICA IDENTITY` set in one transaction. A statement or privilege failure rolls the transaction back. Native YAML represents each rule as `table`, `identity`, and optional `index`; empty rules preserve prior behavior and fingerprints. Because validation can change database metadata, the connector role must own affected tables. PostgreSQL validates replica-index uniqueness, immediacy, predicate absence, and `NOT NULL` columns.

`publish.via.partition.root` defaults to false and maps to native `source.publish_via_partition_root`. When true, every autocreated publication includes `WITH (publish_via_partition_root = true)`. Existing publications are never silently altered: validation compares `pg_publication.pubviaroot` with the configured value and rejects either direction of mismatch. With root publication enabled, discovery, snapshot records, relation metadata, streamed events, and downstream topic routing use the partitioned root table rather than physical leaf partitions.

`slot.failover` defaults to false and maps to native `source.slot_failover`. It applies only to managed logical slots and is omitted from semantic fingerprint material while false. Before creating or updating a slot, Rustium checks `server_version_num` and `pg_is_in_recovery()`. PostgreSQL 17+ primaries receive `ALTER_REPLICATION_SLOT ... (FAILOVER true)` after the slot and any exported snapshot are established; this uses PostgreSQL 17's option syntax without weakening the snapshot handoff. Older servers and standby nodes match Debezium's fallback behavior by warning and retaining a regular logical slot. External slot ownership rejects the option because Rustium must not mutate externally managed metadata.

`slot.drop.on.stop` defaults to false and maps to native `source.drop_slot_on_stop`. It is valid only with managed ownership and is excluded from semantic fingerprints because it changes lifecycle cleanup, not selected events. On orderly cancellation Rustium sends final replication feedback, releases the replication transport, and retries ordinary-connection `pg_drop_replication_slot` while PostgreSQL still reports the slot active. A missing slot is idempotent success; a persistent active state or permission failure makes orderly shutdown fail visibly. Stream failures, automatic reconnects, channel failures, crashes, and forced task aborts never request deletion. This distinction preserves recovery after abnormal exits while matching Debezium's close-time option. Enabling deletion creates an intentional CDC gap for changes committed while the connector is stopped and is therefore discouraged for continuously captured production sources.

`snapshot.locking.mode` maps to native `source.snapshot_locking_mode` and accepts Debezium's `none` default or `shared`; Java SPI mode `custom` fails configuration. Shared mode starts after the snapshot transaction begins (and after the exported snapshot is imported when applicable), before schema discovery. Rustium sets transaction-local `lock_timeout`, reads the publication's captured tables in schema/table order, retains business tables admitted by `snapshot.include.collection.list`, adds the configured signal table, and acquires one `ACCESS SHARE` lock per table in that order. DML remains available while concurrent DDL waits until snapshot commit. `snapshot.lock.timeout.ms` maps to native `source.snapshot_lock_timeout`, defaults to 10 seconds, and bounds both the publication-view lookup and every explicit lock because PostgreSQL's catalog view can itself request relation locks. Zero disables PostgreSQL's timeout; values above its signed 32-bit millisecond maximum fail validation. Neither operational setting enters the semantic fingerprint. A lock or catalog timeout aborts before any snapshot row is emitted, leaving the managed slot available for an explicit retry.

`snapshot.isolation.mode` maps to native `source.snapshot_isolation_mode` and accepts `serializable` (the Debezium default), `repeatable_read`, `read_committed`, and `read_uncommitted`. Serializable and repeatable-read use `EXPORT_SNAPSHOT` plus a read-only REPEATABLE READ import, preserving the existing gap-free handoff; PostgreSQL's imported-snapshot protocol does not permit READ COMMITTED. The lower-isolation modes therefore create the slot with `NOEXPORT_SNAPSHOT`, open the requested read-only transaction (`READ UNCOMMITTED` is PostgreSQL's READ COMMITTED alias), and anchor `START_REPLICATION` at the slot's `restart_lsn`. This deliberately permits a snapshot assembled across statement snapshots while still delivering every change after slot creation. Serializable and repeatable-read are omitted from the fingerprint because both import the same exported snapshot; the lower modes enter it because they change snapshot consistency and source handoff semantics.

#### 9.2 Snapshot handoff

For a managed initial snapshot using the default `serializable` or `repeatable_read` isolation:

1. Prepare or recreate an inactive managed slot.
2. create the logical slot with an exported snapshot;
3. open a repeatable-read, read-only SQL transaction;
4. import the exported snapshot;
5. discover selected publication tables and schemas;
6. scan each table admitted by the optional snapshot-only collection filter in bounded pages;
7. emit snapshot-complete with the baseline schema history at the slot anchor LSN;
8. start `pgoutput` from that LSN.

The slot retains changes committed during the snapshot, so no handoff gap exists.

For `read_committed` or `read_uncommitted`, step 2 uses `NOEXPORT_SNAPSHOT`, step 3 opens the requested read-only isolation level, and step 7 records the slot's `restart_lsn`. This path trades one globally consistent snapshot for Debezium-compatible lower isolation while retaining all WAL committed after slot creation.

#### 9.3 Streaming

`pg_walstream` provides replication transport and protocol parsing. Rustium converts begin, insert, update, delete, truncate, commit, and streamed-transaction events. Same-LSN ordinals make positions total and replayable. Missing unchanged TOAST columns become `Unavailable`.

Replica identity controls old-row shape. `FULL` provides all old columns. `DEFAULT` and `INDEX` provide replica-key values while PostgreSQL represents unavailable non-key fields as null placeholders in the protocol row. The external PostgreSQL 17 gate verifies catalog modes plus the actual `FULL` and non-leading `INDEX` before images.

PostgreSQL connection establishment and transient stream recovery consume the shared Debezium-compatible `errors.max.retries`, `errors.retry.delay.initial.ms`, and `errors.retry.delay.max.ms` policy. The configured retry count is interpreted after the initial attempt; `0` disables automatic stream recovery and `-1` is effectively unbounded. Backoff and connection attempts stay inside `pg_walstream`, while Rustium retains ownership of the replayable LSN, slot continuity checks, decoded transaction state, and checkpoint acknowledgements.

PostgreSQL also accepts explicit `slot.max.retries` and `slot.retry.delay.ms` as migration fallbacks when the corresponding shared `errors.*` properties are absent. The slot delay becomes both the initial and maximum retry delay so the shared retry engine produces Debezium's fixed wait. Explicit `errors.*` values take precedence, while omission of both forms preserves Rustium's established shared defaults. `status.update.interval.ms` maps to native `source.status_update_interval`, defaults to 10 seconds, and configures periodic `pg_walstream` feedback checks. The feedback atomics advance only from `SourceContext.acknowledged`, after Sink delivery and checkpoint persistence; each connector-owned acknowledgement also forces an immediate status update, so the periodic cadence does not delay durable slot advancement. `database.tcpKeepAlive` maps to native `source.tcp_keepalive`, defaults to true, and adds libpq `keepalives=1` or `keepalives=0` consistently to ordinary and replication URLs. These connection-only controls are excluded from the event-semantic fingerprint.

`xmin.fetch.interval.ms` maps to native `source.xmin_fetch_interval` and defaults to zero. A positive value opens one ordinary libpq connection after the logical slot exists, reads its `catalog_xmin` before the first eligible decoded message, and reuses the cached unsigned XID until the interval expires. Null slot values are queried again on the next message, matching Debezium's no-cache behavior when no XMIN exists. Streaming `PostgresPosition` stores the optional value; JSON, Avro, and Protobuf PostgreSQL source contracts always expose an optional `xmin`, while snapshots and disabled tracking use null. The new position member has serde default/omit rules, so zero-value checkpoints serialize in the old shape and retain old event IDs. Query, connection, and parse errors fail the source rather than silently retaining stale metadata. Because a positive interval changes emitted source metadata, it enters the event-semantic fingerprint.

Before resuming, `offset.mismatch.strategy` reconciles the durable checkpoint with `confirmed_flush_lsn`. `no_validation` preserves the legacy checkpoint start. With checkpoint greater than slot, `trust_offset` and `trust_greater_lsn` call `pg_replication_slot_advance`; `trust_slot` keeps the checkpoint without mutating the slot. With slot greater than checkpoint, `trust_offset` fails closed, while `trust_slot` and `trust_greater_lsn` adopt the slot. The deprecated `slot.seek.to.known.offset.on.start` maps true to `trust_offset` and false to `no_validation`, but the new property has precedence. Native YAML uses `source.offset_mismatch_strategy`. Slot-advancing strategies require managed ownership, and a checkpoint inside a transaction is rejected before mutation because advancing a logical slot could cross unprocessed transaction events. A slot-authoritative jump replaces the replay filter with a clean position at that LSN, reloads selected schemas from the current catalog, drops active incremental-snapshot progress, and marks connector state dirty for the next checkpoint. Completed signal IDs remain retained. Because non-default modes can mutate source state or deliberately skip local history, they enter the semantic fingerprint.

`lsn.flush.mode` assigns feedback ownership. `connector` is the default and copies only `SourceContext.acknowledged` into the stream's flushed/applied tracker. `manual` consumes acknowledgement notifications without changing that tracker, leaving slot advancement external. `connector_and_driver` seeds the shared feedback tracker with its monotonic maximum; `pg_walstream` caps every status update to its internal `last_received_lsn`, reproducing pgjdbc automatic flushing for ordinary events and server keepalives without exposing transport internals. The deprecated `flush.lsn.source` maps true to `connector` and false to `manual`, with the new enum taking precedence. Native YAML uses `source.lsn_flush_mode`. Manual mode can retain WAL indefinitely if the external owner stalls. Driver mode can move `confirmed_flush_lsn` beyond a local durable checkpoint and therefore trades replay safety for bounded retention; it should be paired with a slot-authoritative offset mismatch strategy. Non-default modes enter the semantic fingerprint.

`lsn.flush.timeout.ms` maps to positive native `source.lsn_flush_timeout` and defaults to 30 seconds. The connector wraps the actual asynchronous `send_feedback()` future forced by each durable acknowledgement in that timeout. `lsn.flush.timeout.action` maps to native `source.lsn_flush_timeout_action` and accepts `fail` (default), `warn`, or `ignore`. A timeout fails the source, logs and continues, or continues at debug level respectively; an I/O future that completes with an error always fails regardless of the timeout action. Manual mode never enters this acknowledgement-flush path. These operational settings are excluded from the event-semantic fingerprint.

`slot.stream.params` is parsed as Debezium's semicolon-separated decoder parameter list. Because Rustium supports only `pgoutput`, the implemented parameter surface is its PostgreSQL 16+ `origin=any|none` filter and maps directly to `pg_walstream::OriginFilter`. `any` requests local and replication-origin transactions; `none` requests only local transactions. Native YAML uses the deterministic `source.slot_stream_params` map. Empty parameters preserve the old semantic fingerprint, while configured parameters enter it because they alter source selection. Malformed entries, unknown names, invalid origin values, and configured origin filtering on PostgreSQL 14/15 fail configuration before a replication connection is opened.

`database.initial.statements` uses Debezium's character-level delimiter contract: one semicolon ends a statement and `;;` contributes one literal semicolon. The resulting ordered list runs immediately after each ordinary libpq connection is established, covering validation, catalog lookup, snapshots, heartbeat actions, XMIN metadata, slot/offset inspection, incremental snapshots, and orderly slot cleanup. The replication transport and replication-protocol slot mutation connection intentionally bypass it. Native YAML uses `source.database_initial_statements` as an already-split list. Blank native entries fail validation. Statements are intended for idempotent session configuration because connection creation is discretionary and execution is not atomic across connections. A failure identifies the one-based entry without Rustium echoing its configured SQL, and a non-empty list enters the semantic fingerprint because session settings can change captured values.

PostgreSQL TLS stays in the shared connection URL so ordinary and replication connections cannot drift. `database.sslmode` accepts the libpq modes `disable`, `allow`, `prefer`, `require`, `verify-ca`, and `verify-full`. `database.sslrootcert` maps to native `source.ssl_root_cert` and the rustls transport's exclusive PEM root store. TLS paths and modes are operational rather than event-semantic and therefore do not enter the fingerprint. The current `pg_walstream 0.8` rustls backend does not parse or load `sslcert`, `sslkey`, or `sslpassword`; Rustium rejects those fields before opening a connection instead of silently running without requested mutual TLS. Debezium's `database.sslfactory` is a JVM class extension and is rejected explicitly for the same reason.

Unknown PostgreSQL types follow Debezium's `include.unknown.datatypes` contract. Catalog discovery classifies supported built-ins, enums, ranges/multiranges, supported domain and array bases, and explicitly supported extension types before constructing the event schema. The default false mode retains every column's OID/typmod in schema history but omits unsupported fields from the event schema and rows. True maps those fields to `bytea` and preserves the UTF-8 bytes of the pgoutput text value; snapshot and incremental-snapshot `::text` projections use the same byte conversion. Connector-state version 6 persists the opaque-column set. Versions 1 through 5 deserialize with an empty set, then exact name/OID/typmod matches are normalized from the validation catalog before replay; changed historical layouts are not overwritten. The option enters the semantic fingerprint because it changes both schema and data.

PostgreSQL MONEY follows Debezium's `money.fraction.digits` contract, with native `source.money_fraction_digits` and default scale 2. Catalog and Relation schema construction encode the effective signed 16-bit scale as `money(<scale>)`. One converter removes the leading currency symbol, comma grouping, sign or accounting parentheses from PostgreSQL text, parses an arbitrary-precision decimal, and applies `HALF_UP` rounding. Scalar and array values use that converter for initial snapshots, incremental snapshots, and WAL. Malformed input remains a string. The default is omitted from semantic fingerprint material; a non-default scale is fingerprinted because it changes schema and data.

`schema.refresh.mode` maps to native `source.schema_refresh_mode` and strictly accepts Debezium's `columns_diff` default or `columns_diff_exclude_unchanged_toast`. The option is behaviorally equivalent under Rustium's pgoutput-only transport. pgoutput emits complete `Relation` layouts independently from row tuples, and Rustium increments schema versions only from those layouts; an unchanged TOAST marker is skipped by the transport and cannot be mistaken for dropped schema. Row conversion fills the omission from a `REPLICA IDENTITY FULL` before image when present or with `DataValue::Unavailable` otherwise. Neither mode dirties connector schema state for a row-only update, so the option is excluded from semantic fingerprint material. This preserves Debezium configuration migration without inheriting the stale-schema risk documented for non-pgoutput decoders.

The PostgreSQL connector-state payload persists the snapshot table layout plus each column's type OID and typmod. On restart it restores that baseline before opening the slot. Each `Relation` message then supplies the historical column names, order, type identity, and key flags for the following row events. Exact catalog matches supplement type names and optionality without replacing the WAL layout. Changed schemas increment their version and the updated state is attached to the next checkpointable source record.

`pg_walstream` caches a relation's first protocol message and emits explicit `Relation` events only for later layout changes. When a table is added to a running `no_tables` or table-scoped publication, Rustium therefore detects the first selected DML cache miss, discovers the current catalog schema before decoding that same event, and marks the schema state dirty for the next checkpoint. This permits dynamic publication expansion without dropping the first row or requiring a restart.

Before resuming a completed checkpoint, Rustium verifies that the original replication slot still exists, still uses `pgoutput`, and has not entered PostgreSQL `wal_status=unreserved` or `lost`. A failed continuity check stops before the replication transport can create a replacement slot and instructs the operator to reset the checkpoint and run a new initial snapshot. This deliberately prefers an explicit recovery operation over a silent WAL gap.

With `snapshot.mode=when_needed`, the same continuity failures, or a legacy checkpoint without schema history, trigger a new managed snapshot and clear the obsolete connector state before streaming resumes. `initial` remains fail-closed for those conditions.

PostgreSQL does not put original DDL or column nullability/default metadata into `Relation`. If a transient historical column no longer exists in the current catalog and was not present in the checkpoint baseline, Rustium resolves its type from OID/typmod and conservatively marks it optional. This preserves row decoding and ordering without claiming unavailable metadata.

Snapshot queries project every selected column through PostgreSQL's `::text` output function instead of routing rows through JSON. Snapshot values and `pgoutput` values therefore share one converter and preserve numeric scale/precision, bytea, JSON text, temporal formatting, and array syntax identically. The array parser handles quoted and escaped elements, SQL NULL versus the string `"NULL"`, explicit lower bounds, nested dimensions, and type-aware scalar conversion. Malformed array text is preserved as a string instead of being partially decoded.

PostgreSQL column transformations are a post-conversion emission layer shared by initial snapshots, incremental snapshots, and WAL records. Debezium dynamic properties compile into anchored, case-insensitive `schema.table.column` selectors. Rule selection is deterministic and category-first: truncate, fixed mask, hash V1, then hash V2; the first matching selector in that order wins. Fixed masks replace NULL, hashes preserve NULL, and hash output is lowercase hexadecimal shortened to the declared character typmod. V1 reproduces Java `ObjectOutputStream` String serialization before digesting `salt || bytes`; V2 digests `salt || UTF-8(value)`. The implementation supports the common JCA MD2, MD5, SHA-1/SHA-2, SHA-512 truncated, and SHA-3 names. Transformation is deliberately applied after incremental-snapshot/WAL bookkeeping, so raw primary keys remain available for keyset progress and concurrent-update deduplication. Salts are semantic configuration material but only their SHA-256 digest is stored in fingerprints.

Catalog discovery resolves PostgreSQL domains to their base conversion type while retaining the domain OID/typmod as schema identity; arrays of domains receive the same element conversion. Enum, range, network, `ltree`, `isbn`, and `tsvector` values use canonical PostgreSQL text. `hstore.handling.mode=json` maps hstore output to JSON and `map` maps it to a string-keyed `DataValue::Map`, including nulls, escapes, and hstore arrays. `vector` and `halfvec` become float arrays, `sparsevec` becomes a map containing dimensions and indexed values, and PostGIS geometry/geography becomes complete EWKB bytes. Every specialized parser is all-or-nothing and falls back to the original string on malformed supported input.

`interval.handling.mode` implements Debezium's `numeric` and `string` contracts. Properties default to `numeric`, which produces `DataValue::Int64` microseconds using `365.25 / 12` average days per month and the same floating-point truncation order as Debezium `MicroDuration`. `string` produces Debezium's complete `PnYnMnDTnHnMnS` representation with independently signed components. The parser accepts all PostgreSQL 17 `IntervalStyle` outputs: `postgres`, `postgres_verbose`, `sql_standard`, and `iso_8601`; the same converter handles scalar and array elements in snapshot, incremental-snapshot, and WAL paths. Native `source.interval_handling_mode=postgres` remains the default and returns the original server text, preserving older native behavior and fingerprints. Malformed input is never partially interpreted.

PostgreSQL 14+ logical decoding `Message` records are requested from `pgoutput` only when the source configuration enables them. Debezium properties enable capture by default; native `source.logical_decoding_messages=false` preserves the previous behavior and fingerprint. `message.prefix.include.list` and `message.prefix.exclude.list` map to mutually exclusive, fully matched regular expressions, with native `source.message_prefix_include_list` and `source.message_prefix_exclude_list` aliases. An accepted message produces a `prefix` key and a `message` block on `<topic.prefix>.message`; JSON uses Base64 for the default bytes representation, while Avro and Protobuf retain bytes. Transactional messages participate in the active transaction's ID and total ordering and remain a data boundary until commit. A non-transactional message is both an event and a commit boundary. Filtering a non-transactional message emits a position-only commit boundary so acknowledged WAL can advance without exposing a record.

`heartbeat.interval.ms` defaults to zero. A positive interval emits a visible heartbeat at the latest SourceRecord position already admitted to the bounded queue, or at the completed snapshot anchor before the first streaming event. No heartbeat is emitted when no source position exists. `heartbeat.action.query` optionally executes first on a reused ordinary SQL connection at each interval. Its WAL is not treated as progress until `pgoutput` returns it, and query failures stop the source with the database error. Heartbeat-table changes can be included in the publication but excluded by the table selector; their transaction commit still advances the safe WAL position without exposing a business-table event.

#### 9.4 Source signaling and incremental snapshots

The signaling implementation follows Debezium's `source`, `file`, `in-process`, and `kafka` channels. `signal.data.collection` identifies one schema-qualified table with exactly three text-compatible columns named `id`, `type`, and `data`; the table must be in the publication and is always filtered from business snapshots and events. `signal.enabled.channels` accepts any combination of the four implemented channels. `signal.file` defaults to `file-signals.txt`, and `signal.poll.interval.ms` defaults to 5000 ms. The file reader consumes non-empty JSON Lines and clears the file after a successful read, matching Debezium's no-retry channel semantics. An `execute-snapshot` record accepts `type=incremental`, fully matched regular expressions in `data-collections`, `additional-conditions` entries containing a case-insensitive collection expression plus a SQL filter, and an optional `surrogate-key` column name.

Source-table commands are ignored when the `source` channel is disabled, but connector-generated watermark records remain active for writable externally signaled snapshots. External watermark action types are rejected. After each valid external action, Rustium emits a position-only transaction boundary containing connector state at the latest safe LSN; a fresh stream obtains that initial position from the slot's `confirmed_flush_lsn` or `restart_lsn`. This makes control progress checkpointable even when no business WAL arrives. Read-only external signaling needs no signal table; writable external signaling still needs one for `insert_insert` watermarks.

`ConnectorRuntime::signal_sender()` exposes a cloneable, typed, bounded `SignalSender` before runtime ownership moves into `run`. PostgreSQL consumes commands only outside active WAL transactions and routes file, in-process, and Kafka records through the same action controller and checkpoint path. The CLI attaches this sender to `POST /v1/connector/signals` only when `in-process` is enabled. The route requires management mutations, returns `202` after queue admission, and reports disabled mutations or channels as `403` or `409` respectively.

Debezium's `jmx` signal channel is a JVM MXBean that writes into an in-memory queue. For properties migration, Rustium maps `signal.enabled.channels=jmx` to the same bounded `in-process` channel, emits a compatibility warning, and exposes it through the embedded `SignalSender` and HTTP management route. This preserves queue and action semantics without claiming JVM/RMI protocol compatibility; `jmx,in-process` is deduplicated to one channel.

`rustium-signal-kafka` implements `signal.kafka.topic`, `signal.kafka.groupId`, `signal.kafka.bootstrap.servers`, `signal.kafka.poll.timeout.ms`, and stripped `signal.consumer.*` pass-through properties. The topic defaults to `<topic.prefix>-signal`, must have exactly one partition, and filters records whose key does not equal `topic.prefix`. Automatic commit and automatic offset storage are forced off. A valid record uses `SignalSender::send_and_wait`; the runtime releases its acknowledgement only after Sink delivery, SQLite checkpoint persistence, and Source acknowledgement, after which the Kafka channel commits offset + 1 synchronously. Invalid or foreign-key records are skipped and committed. A crash after the connector checkpoint but before the Kafka commit can replay the record; active and recently completed `execute-snapshot` IDs are therefore handled idempotently.

The controller currently implements `incremental.snapshot.watermarking.strategy=insert_insert`. For each key-ordered chunk it commits an open watermark, captures the current maximum key on the first chunk, reads at most `incremental.snapshot.chunk.size` rows through the shared text converter, and commits a close watermark. By default the key is the table primary key. A surrogate key must be `NOT NULL` and backed by a valid, non-partial, single-column unique index; it replaces only chunk boundaries and ordering. The primary key remains mandatory and keys the deduplication window. WAL creates, updates, and deletes between the watermarks remove matching primary keys from that window. Rows that remain at close are emitted as read events with the Debezium `incremental` snapshot marker before the close commit boundary.

With `read.only=true`, the chunk connection does not insert watermark rows. It allocates a transaction ID once per chunk, captures `pg_current_snapshot()` before and after the bounded query, and retains the snapshot's `xmin`, `xmax`, and in-progress XID set. WAL transaction IDs open the window at the low `xmin`; the window closes only after the high watermark is visible or the maximum transaction that was in progress has committed. The commit event that closes the window is also the checkpoint boundary. The same transaction ID can safely close subsequent chunks immediately when the watermarks show that no older transaction remains. Restart discards transient watermarks and rereads the current key range.

Connector-state format version 6 stores the signal ID, expanded collections, per-collection conditions, surrogate key, collection index, last key, maximum key, chunk sequence, pause state, a bounded history of 1,024 completed or stopped execute signal IDs, and opaque unknown-type columns. The close commit checkpoints the advanced state atomically with delivered rows. A crash before that checkpoint re-reads the same bounded chunk, which permits duplicates but prevents a gap; a restart after it starts at the next key. Versions 1 through 5 schema-history payloads remain readable with defaults and catalog normalization for the new field. A replayed signal in the completed history is acknowledged without starting another snapshot. The in-memory window is deliberately not persisted.

`pause-snapshot` prevents the next chunk from being prepared after the current close boundary. The paused flag is checkpointed, so restart remains paused. `resume-snapshot` schedules the next chunk after its own signal transaction commits. `stop-snapshot` clears all progress when `data-collections` is absent, or removes only collections matched by its fully matched expressions; stopping the current collection resets its key boundaries and advances safely after the control transaction. Unknown and out-of-order watermark IDs are ignored.

With `incremental.snapshot.allow.schema.changes=false`, the chunk connection rediscovers the table immediately after the open watermark and compares fields plus PostgreSQL type identity before building SQL. A changed `Relation` for the active table is also rejected while streaming. Either guard stops the source before the old window can be decoded against a new layout, leaving the last acknowledged checkpoint intact. Debezium documents PostgreSQL schema changes during incremental snapshots as unsupported; Rustium therefore rejects `incremental.snapshot.allow.schema.changes=true` rather than claiming unsafe compatibility.

If current catalog discovery fails while replaying a historical `Relation`, Rustium keeps the WAL-provided names, order, OIDs, typmods, and key flags. Type-name lookup first reuses an exact checkpointed column identity; if neither catalog nor history can resolve it, the field receives a conservative `unknown_oid_*` name. Both fallback paths are observable warnings and preserve decoding order without inventing nullability or defaults.

#### 9.5 PostgreSQL extension fixture

The TLS gate starts PostgreSQL with a generated CA and a `DNS:localhost` server certificate. It requires ordinary validation and logical replication to succeed with `verify-full`, proves the active replication backend uses TLS, and requires wrong-CA plus hostname-mismatch connections to fail. The unknown-type gate uses a real composite type and proves default field omission plus byte-identical snapshot/WAL output when inclusion is enabled.

The schema-refresh gate proves actual TOAST chunks exist, runs both Debezium modes under default and `FULL` replica identity, repeatedly updates only a non-TOAST column, and requires stable schema versions and connector state while producing `Unavailable` or the recovered before value.

The fixture is remote-daemon capable without exposing its ephemeral PostgreSQL port. Docker commands follow the selected Docker context. When `RUSTIUM_POSTGRES_DOCKER_SSH_HOST` is set, the container still publishes PostgreSQL only on the daemon host's loopback address and the script opens an authenticated local SSH forward; `RUSTIUM_POSTGRES_DOCKER_SSH_LOCAL_PORT` defaults to 55433. `RUSTIUM_POSTGRES_EXTENSION_BASE_IMAGE` can select a trusted registry mirror or preloaded PostgreSQL image, while the default remains the official `postgres:<version>` image used by CI.

`scripts/test-postgresql-extensions.sh` builds a PostgreSQL 17 image from the repository Dockerfile, installs matching pgvector and PostGIS packages, enables logical replication, and runs twenty-two required gates. Readiness requires the final PostgreSQL postmaster to be container PID 1 plus a successful query, so the entrypoint's temporary initialization server cannot satisfy the gate. The type gate requires vector, halfvec, sparsevec, geometry, and geography to exist and remain identical across snapshot/WAL paths. The MONEY gate configures `money.fraction.digits=1` and requires the field scale plus positive and negative `HALF_UP` values to be identical across exported snapshot and WAL. The recovery gate uses a capacity-one Source output, commits a multi-row transaction until the bounded channel is full, terminates the active replication backend, commits another transaction while the original connection is down, and requires the new backend to preserve every expected row plus first-seen source order. The snapshot-filter gate selects two publication tables for streaming, snapshots one, and requires a later create event from the other. The publication gate covers missing-publication failure, `FOR ALL TABLES` creation, filtered creation and replacement, filtered conflict rejection, and empty-publication startup followed by dynamic table addition and first-row capture. The replica-identity gate verifies all four catalog modes, actual `FULL` and non-leading `INDEX` WAL before images, overlapping-rule rejection, and transactional non-mutation on conflict. The partition-root gate verifies publication metadata, root-attributed snapshot/WAL records across two leaf partitions, and existing-publication mismatch rejection. The failover-slot gate requires a PostgreSQL 17 primary, verifies `pg_replication_slots.failover`, and requires both exported-snapshot and WAL delivery through that slot. The slot-lifecycle gate proves that orderly stop retains an inactive slot by default and removes it only when `slot.drop.on.stop=true`. The snapshot-lock gate proves deterministic upfront `ACCESS SHARE` coverage for a later unscanned table, concurrent-DDL blocking, post-snapshot lock release, bounded acquisition failure, and zero row emission before all locks succeed. The isolation gate runs all four Debezium modes and requires both the expected snapshot rows and the first post-snapshot WAL create event. The logical-message gate verifies filtered non-transactional checkpoint progress, raw binary content, transaction identity, message/row ordering, and commit boundaries through `pg_logical_emit_message`. The feedback gate disables TCP keepalive, uses a non-default 25 ms status interval, acknowledges a commit, and requires `confirmed_flush_lsn` to reach that exact durable position within three seconds. The offset-mismatch gate creates both checkpoint-ahead and slot-ahead states, verifies slot advancement for `trust_offset` and `trust_greater_lsn`, requires `trust_offset` to reject an ahead slot, and proves `trust_slot` skips pre-slot records while checkpointing refreshed schema state. The origin-filter gate creates a real PostgreSQL replication origin and proves `origin=none` excludes its transaction while `origin=any` emits it. The initial-statement gate holds a backpressured snapshot connection open, verifies its session settings, proves the active replication backend did not inherit them, and observes the same settings through a heartbeat action connection. The LSN-flush gate proves that manual mode ignores a durable acknowledgement, driver mode flushes unacknowledged published records, and one-second server keepalives flush unmonitored WAL only in driver mode. The interval gate uses role-level settings for all four PostgreSQL `IntervalStyle` values and verifies both Debezium modes, scalar values, arrays, exported snapshots, and WAL. CI runs three recovery cycles; `RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES=1..1000` raises the count for longer runs. The dedicated `postgresql-cdc` GitHub CI job runs all twenty-two gates on every push and pull request.

The XMIN gate creates independent enabled and disabled logical slots. It requires two transactions inside a 60-second fetch interval to carry the same positive cached `catalog_xmin`, while the zero default must emit no XMIN metadata. The format golden gates separately require PostgreSQL JSON, Avro, and Protobuf source contracts to carry the optional field.

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

The current `sqlparser`-based MySQL DDL path handles `CREATE TABLE`, `ALTER TABLE` add/drop/rename/modify/change column operations, primary-key changes, `DROP TABLE`, `RENAME TABLE`, and schema-neutral `TRUNCATE TABLE`, index add/drop, and column-default changes. MySQL's standalone `ALTER TABLE ... RENAME INDEX ... TO ...` syntax is recognized with the SQL tokenizer because `sqlparser` 0.62 does not yet parse that form; mixed operations remain fail closed. Parsing or state-application failures stop the connector by default. `schema.history.internal.skip.unparseable.ddl=true` matches Debezium's opt-in skip behavior and logs the metadata-risk warning.

When `connect.keep.alive=true`, an ended or failed binlog stream is reopened from the last SourceRecord successfully sent into the bounded runtime queue. The rewind restores the binlog filename, table-map anchor, GTID transaction counters, row ordinal, and replay filter so completed records are skipped deterministically. CLI recovery consumes the shared Debezium-compatible `errors.max.retries`, `errors.retry.delay.initial.ms`, and `errors.retry.delay.max.ms` policy with cancellation-aware exponential backoff. `0` fails immediately, `-1` removes the retry-count limit, and new source progress resets both the budget and delay. For `.properties` migration, `rustium.source.reconnect.max.attempts` and `connect.keep.alive.interval.ms` remain fallbacks only when the corresponding `errors.*` values are absent. Direct embedded construction without `with_retry_policy` retains those source-config values.

With `snapshot.mode=when_needed`, a completed checkpoint whose binlog file is no longer retained falls back to a new initial snapshot; ordinary `initial` resume continues to fail at the unavailable source position.

`heartbeat.interval.ms` defaults to zero and disables visible heartbeats. A positive interval emits a heartbeat from the latest safe streaming position, so its sink acknowledgement and checkpoint follow the normal at-least-once sequence without inventing binlog progress. Debezium JSON uses the `serverName` key and `ts_ms` value. Topic resolution prefers `topic.heartbeat.name`; otherwise it joins `topic.heartbeat.prefix` (or legacy `heartbeat.topics.prefix`) with `topic.prefix`.

`heartbeat.action.query` optionally runs first on a separate ordinary MySQL connection at each positive heartbeat interval. Query failures stop the source with the database error. Changes made by the query do not become source progress until the replication stream observes them.

When MySQL emits `JsonDiff` values under `binlog_row_value_options=PARTIAL_JSON`, Rustium applies replace, insert, and remove paths to the complete before-image JSON and emits the reconstructed after image. If the before image or path is incomplete, it emits `Unavailable` for that field instead of fabricating a value.

`database.connectionTimeZone` maps to native `source.connection_time_zone` and defaults to `UTC`. Each ordinary MySQL connection normalizes the accepted `UTC`, `Z`, `Etc/UTC`, or `+00:00` value to a `+00:00` session time zone so `TIMESTAMP` snapshot values match UTC binlog values. Other offsets, named regions, and Debezium's `SERVER` mode fail validation until both capture paths implement identical temporal conversion. Binlog ENUM ordinals and SET bit masks are decoded through the captured column definition. MySQL spatial values are preserved as their native SRID plus WKB bytes, with all OGC column families excluded from non-key snapshot ordering. The external type matrix requires snapshot/binlog equality for boolean, signed and unsigned integers, decimal, float/double, bit and binary, date/time/datetime/timestamp, string/text, JSON, ENUM, SET, null, `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and `GEOMETRYCOLLECTION` values.

PostgreSQL, MySQL, and SQL Server use the shared `rustium-column-transform` engine for Debezium-compatible column transformations. Dynamic properties `column.truncate.to.<length>.chars`, `column.mask.with.<length>.chars`, `column.mask.hash.<algorithm>.with.salt.<salt>`, and `column.mask.hash.v2.<algorithm>.with.salt.<salt>` compile into anchored, case-insensitive selectors. PostgreSQL selectors use `schema.table.column`, MySQL selectors use `database.table.column`, and SQL Server accepts both `database.schema.table.column` and `schema.table.column`. The deterministic priority is truncate, fixed mask, hash V1, then hash V2. Fixed masks replace SQL NULL, hashes preserve NULL, and hashes are lowercase hexadecimal shortened to the declared character length when one exists, including SQL Server `nchar(n)` and `nvarchar(n)`. V1 reproduces Java `ObjectOutputStream` String serialization before digesting `salt || bytes`; V2 digests `salt || UTF-8(value)`. Transformations run after snapshot/incremental keyset bookkeeping and before the event is emitted, so primary-key progress and concurrent-update deduplication always use raw values. Salts are never written to semantic fingerprints; only their SHA-256 digests are retained.

MySQL also supports Debezium-compatible source-table, file, bounded in-process, and Kafka signal channels. `signal.data.collection` identifies one `database.table` containing exactly three text-compatible columns named `id`, `type`, and `data` in that order; Rustium reads source-table inserts from the binlog, excludes the signal table from business snapshots and events, and never writes watermarks to it. File lines use the same JSON envelope as the runtime `SignalSender`. `execute-snapshot` expands fully matched collection expressions, requires each selected table to have a primary key, captures a fixed maximum key per collection, and advances with typed single-column or composite-key comparisons. It emits `source.snapshot=true` with `rustium.snapshot.kind=incremental` and persists the current key, maximum key, collection, chunk sequence, and pause state in connector state. Version 3 also retains a bounded history of completed or stopped execute signal IDs, making replay idempotent across restart and the connector-checkpoint/Kafka-offset crash window. Version 2 offset progress remains readable and safely restarts its current collection from the beginning rather than risking a skipped row.

The event loop schedules at most one incremental chunk per turn and only outside an active binlog transaction. Each query is enclosed by low and high binlog coordinates. Rustium buffers the chunk, emits ordinary CDC records while the replication stream reaches the high coordinate, and removes keys found in create, update, or delete before/after images inside `(low, high]`. Only the remaining rows are emitted before the chunk commit atomically advances the typed keyset state. Binlog records, pause/resume/stop controls, and external signal acknowledgements are therefore observed between chunks. Schema-history updates preserve the active incremental state instead of replacing it. An in-memory window is intentionally not checkpointed: reconnect discards it and rereads the uncommitted key range, while a schema change observed during an open window fails before any layout-mismatched read is emitted. The Kafka channel reuses the single-partition, checkpoint-coupled `rustium-signal-kafka` implementation and Debezium topic/key contract.

#### 10.4 TLS modes

- `disabled`: plaintext only.
- `preferred`: try encrypted transport, then fall back to plaintext.
- `required`: encrypted transport without CA or hostname verification.
- `verify_ca`: encrypted transport with CA verification, without hostname verification.
- `verify_identity`: encrypted transport with CA and hostname verification.

`database.ssl.ca` supplies a PEM/DER CA file for Rustls. `database.ssl.cert` and `database.ssl.key` supply a PEM/DER client certificate and private key and must be configured together. Debezium-compatible `database.ssl.truststore`/`database.ssl.truststore.password` and `database.ssl.keystore`/`database.ssl.keystore.password` accept JKS or PKCS#12/PFX stores detected by content. PKCS#12 decoding supports modern PBES2/AES/SHA-256 and legacy PBES1 files. Certificates and the PKCS#8 private key are converted directly to in-memory Rustls inputs. A keystore must expose exactly one private-key entry with a certificate chain; an empty truststore fails validation. PEM CA and truststore inputs are mutually exclusive, as are PEM client identity and keystore inputs. Store paths participate in the connector fingerprint, while store passwords do not.

#### 10.5 Verified recovery

The MySQL 8.4 Docker gate covers:

- two-row snapshot and snapshot completion;
- snapshot-only collection filtering without narrowing the selected binlog stream;
- one transaction with multi-row insert, update, and delete;
- transaction order 1 through 5;
- three capacity-one Source-output backpressure cycles by default;
- forced termination and connection-identity replacement of the active binlog dump session in every cycle;
- reconnect from the last completed table-map/commit anchor with every expected row and first-seen source order preserved;
- checkpoint stop before an old-schema row, destructive drop/add-column DDL, and a new-schema row;
- restart after the database already exposes the final schema, with correct old-schema decoding, DDL state checkpointing, and new-schema decoding.

`RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES=1..1000` raises the cycle count. The gate uses a dynamically assigned host port, has bounded Docker cleanup, and is a required `mysql-cdc` GitHub Actions job on every push and pull request.

Unit gates cover checkpoint/state atomicity, version 1/2/3 checkpoint compatibility, schema-history serialization, incremental keyset progress/control, completed-signal replay, PostgreSQL snapshot/file/in-process signal parsing, MySQL signal parsing, shared MySQL retry-policy boundaries and legacy fallback mapping, durable runtime signal acknowledgement, management gating, replay-state rewind, scalar and extension conversions, heartbeat encoding, selected-table isolation, create/alter/drop/rename DDL application, and JKS plus modern PKCS#12 TLS-store conversion and failure handling. A librdkafka MockCluster gate verifies Kafka key filtering, single-partition consumption, and no offset commit before durable signal acknowledgement. The external PostgreSQL 17 gate verifies forced replication-backend termination and automatic recovery, explicit failure after a checkpoint's slot is lost, periodic heartbeat emission, successful execution of `heartbeat.action.query`, heartbeat-table filtering, source/file/in-process-signaled chunking, immediate external-signal checkpointing, file and in-process read-only signaling without a signal table, checkpoint restart, additional conditions, concurrent-update deduplication, pause/resume/scoped-stop control, read-only transaction watermarks under a held update, restricted table permissions, zero connector watermark writes, unique surrogate-key ordering, completion cleanup, signal-table isolation, and snapshot/WAL equality for hstore, domains, enums, and tsvector. An opt-in superuser fixture temporarily limits `max_slot_wal_keep_size`, drives a slot to `wal_status=lost`, verifies fail-closed resume, and restores the original setting; it must be run on an isolated PostgreSQL instance. The required Docker/CI fixture enforces both vector/halfvec/sparsevec and PostGIS geometry/geography equality plus replication-backend recovery on PostgreSQL 17. The external MySQL 8.4 gate verifies capacity-one reconnect/backpressure cycles with connection-identity replacement, periodic heartbeat emission during an idle stream, exact-server-UUID GTID startup, checkpoint recovery, destructive-DDL recovery including schema-invariant index/default operations, in-process incremental snapshot execution, typed keyset restart after deletion/insertion, durable completed signal IDs, concurrent-window deduplication, the core plus OGC spatial snapshot/binlog type matrix, and real-broker Kafka replay across the connector-checkpoint/offset-commit crash window; the test requires temporary free space on the MySQL data volume.

### 11. SQL Server Connector

The SQL Server connector is implemented with SQL Server CDC, not application-table polling. It currently accepts one database per connector, one active capture instance per selected table, and `data.query.mode=direct`.

Mapped Debezium-compatible inputs include `database.hostname`, `database.port`, `database.user`, `database.password`, `database.names`, `database.encrypt`, `database.trustServerCertificate`, `table.include.list`, `table.exclude.list`, `snapshot.mode`, `snapshot.isolation.mode`, `data.query.mode`, `streaming.fetch.size`, `max.queue.size`, `max.batch.size`, `poll.interval.ms`, and the dynamic column truncate/mask/hash properties.

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

Transient polling failures use the shared Debezium-compatible `errors.*` policy. Rustium fully materializes each max-LSN, retention, and direct change-table query before mutating the typed cursor. An I/O or selected transient SQL Server failure therefore discards the failed client, reconnects, and repeats polling from the unchanged commit/change LSN. Cancellation interrupts backoff, `0` disables automatic recovery, and `-1` permits unbounded retries. Permission, protocol, conversion, and CDC-retention failures are not retried and remain fail-closed.

With `snapshot.mode=when_needed`, a checkpoint older than CDC cleanup is replaced by a new consistent snapshot before polling resumes. Other snapshot modes fail closed when the required CDC history has been removed.

Snapshot queries use the same per-column SQL projection as CDC change-table queries before entering the shared Rust converter. This avoids driver-specific differences for money, fractional temporal precision, UUID, binary, text, and special values. Snapshot rows are ordered by primary key when one is present. The external type matrix requires identical `DataValue` rows across snapshot and CDC for bit, integer, decimal/money, real/float, UUID, varbinary, date, time, datetime2, datetimeoffset, Unicode text, XML, hierarchyid, geometry, and geography. Geometry and geography use SQL Server's native `Serialize()` representation and remain complete binary payloads instead of lossy `ToString()` output; hierarchyid uses its canonical path and XML uses canonical text.

SQL Server applies column transformations after conversion on initial snapshot rows, paired CDC before/after images, and incremental snapshot rows. Four-part selectors include the configured database; three-part selectors support Debezium configurations scoped as `schema.table.column`. Incremental key extraction and CDC-window deduplication happen before transformation, preserving raw key ordering. Signal-table CDC rows are deliberately excluded from transformation until their internal commands and watermark IDs have been parsed, preventing a matching mask rule from corrupting connector control flow. External SQL Server 2022 gates cover a four-part truncation rule across snapshot and CDC and a three-part fixed mask on a source-signaled incremental snapshot.

SQL Server supports Debezium-compatible source-table, file, bounded in-process, and Kafka signal channels. An incremental snapshot from any channel requires a writable source signal table named as `schema.table` or `database.schema.table`; it must contain exactly text-compatible `id`, `type`, and `data` columns in order, `id` must hold at least 42 characters, and the table must have its own active CDC capture instance. Source validation also checks object-level `INSERT` permission. Rustium includes that capture instance even when business table filters exclude it, consumes user create operations as commands, inserts internal open/close watermark rows, and suppresses all signal-table rows from business output. JMX configuration maps to the bounded in-process channel with an explicit compatibility warning.

`execute-snapshot` expands fully matched short or database-qualified collection expressions, requires a primary key, captures a fixed maximum key, and advances typed single/composite keysets. Connector state version 1 persists collection progress, additional conditions, current/maximum keys, chunk sequence, pause state, and a bounded history of completed or stopped signal IDs. One chunk runs per event-loop turn and only at a completed CDC commit boundary. Rustium inserts a unique open watermark and waits for its CDC create event before querying the chunk. It then inserts a distinct close watermark, emits ordinary CDC records while removing matching before/after primary keys from the buffered rows, validates the source schema again, and emits remaining reads at the close watermark commit. The synthetic chunk commit atomically advances the keyset state; source-table controls are checkpointed on their CDC commit, while Kafka offsets remain coupled to the runtime acknowledgement. An in-memory opening or open window is not persisted and is recreated after restart. `pause-snapshot`, `resume-snapshot`, and scoped `stop-snapshot` are idempotent across restart.

Multiple database names require explicit partition-aware ordering and checkpoint ownership. Rustium rejects them until that contract is tested. The external gate has verified snapshot handoff, fetch-size-one update pairing, mid-transaction replay with preserved ordinals, three capacity-one polling-session termination cycles with different connection identities, every expected row, and first-seen source order, concurrent commit ordering, retention fail-closed behavior, heartbeat/action-query, the core and extended type matrices, four-part snapshot/CDC and three-part incremental-snapshot column transformations, raw signal-command isolation, in-process keyset restart, source-table signaling with additional conditions, CDC-window concurrent-update deduplication, real-broker replay across the connector-checkpoint/Kafka-offset crash window, and resource cleanup against SQL Server 2022 Developer RTM-CU25. A required GitHub Actions gate starts the current SQL Server 2022 Linux image with SQL Server Agent, enables CDC, applies the same three-cycle backpressure/reconnect contract, verifies snapshot-only collection filtering without narrowing streaming, and checks snapshot/CDC transaction order, four-part column transformation, plus XML, hierarchyid, geometry, and geography equality through a dynamically assigned host port. `RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES=1..1000` raises the cycle count.

#### 11.1 Oracle LogMiner Source

Oracle uses the pure-Rust `oracle-rs` transport and LogMiner's online catalog. Validation requires ARCHIVELOG mode, minimum supplemental logging, selected-table catalog visibility, and redo retention covering the checkpoint SCN. Initial snapshots capture `CURRENT_SCN` and query selected tables with `AS OF SCN`; streaming starts `DBMS_LOGMNR` with `DICT_FROM_ONLINE_CATALOG` and `COMMITTED_DATA_ONLY`, then polls committed rows in `V$LOGMNR_CONTENTS`.

The durable position is `(scn, commit_scn, transaction_id, event_serial, snapshot)`. DML is parsed only when LogMiner text identifies columns and scalar literals without ambiguity; unsupported expressions remain textual. Commit rows become `TransactionCommit` boundaries. `redo_log_catalog` is rejected until a redo dictionary implementation is available, preserving fail-closed behavior.

#### 11.2 MongoDB Change Streams Source

MongoDB uses the official Rust driver. The connector opens a Change Stream and records `operationTime` before its initial snapshot, then resumes from either the checkpointed opaque token or that operation-time anchor. This ordering prevents concurrent snapshot changes from being skipped.

The durable position is `(resume_token, cluster_time_seconds, cluster_time_increment, event_serial, snapshot)`. BSON documents map recursively into typed Rustium values and `_id` is the event key. `update_lookup` supplies post-update documents; `when_available` or `required` enables pre-images when configured by the server. A replica set or sharded cluster is required.

#### 11.3 Debezium Engine Bridge Sources

MariaDB, Db2, Cassandra 3/4/5, Vitess, Spanner, Informix, CockroachDB, and YashanDB use `rustium-debezium`. The adapter owns Rustium-side ordering, typed normalization, sink delivery, and checkpoint acknowledgement; the upstream Debezium engine owns the database-specific CDC protocol. This boundary is required for Db2 and Informix vendor clients, Cassandra node-local commit-log parsing, Vitess VStream, Spanner partition lifecycle, CockroachDB enriched changefeeds, and YashanDB YStream.

HTTP mode is a synchronous durability boundary. One upstream request is decoded, sent into the bounded runtime, delivered to the configured Sink, checkpointed as `SourcePosition::Debezium`, and acknowledged before HTTP 204 is returned. A request timeout returns failure so Debezium retries. Managed mode generates a mode-0600 Debezium Server configuration, forces unbatched schema-free JSON to the private Rustium endpoint, supports `{config}`/`{endpoint}` command placeholders, and removes the generated file at shutdown. External mode exposes the same endpoint without spawning a process.

Kafka mode sets `enable.auto.commit=false` and `enable.auto.offset.store=false`. It processes one record at a time and synchronously commits the input topic/partition offset only after the Rustium acknowledgement. Regex subscriptions support Cassandra's per-table topics. The Cassandra connector remains one standalone Debezium process per Cassandra node; Rustium does not pretend that a remote CQL query can replace local commit-log CDC.

The durable bridge position is `(connector, source_object, record_id, event_serial, snapshot)`. `source_object` is the complete connector-specific Debezium source metadata, including values such as Db2/Informix LSNs, Cassandra file/position, Vitess VGTID, Spanner partition tokens, CockroachDB resolved timestamps, or YashanDB SCN/LSN fields. `record_id` comes from the upstream CloudEvent when available, from Kafka topic/partition/offset in Kafka mode, or from a deterministic SHA-256 content ID. An exact replay of the final acknowledged HTTP/Kafka record is discarded before a second Sink delivery.

Bridge parsing accepts raw Debezium envelopes, schema/payload envelopes, structured CloudEvents, and HTTP batches. It maps data operations, snapshot-last, transaction-end, heartbeat, and schema-change records into the shared runtime. The native format retains the typed `DebeziumPosition`; compatibility formats carry the lossless source object as `source_offset` JSON together with `record_id` and `event_serial_no`.

### 12. Formats and Sinks

The native JSON encoder exposes the full typed source position and event schema. The Debezium JSON encoder emits `before`, `after`, `source`, `op`, `ts_ms`, and transaction metadata, plus connector-specific position fields. `debezium_json_schema` emits the same JSON values while attaching immutable Draft-07 descriptors for each key and value schema. `debezium_avro` builds named key/envelope/source/transaction/row records, resolves each dynamic value against an Apache Avro schema, and emits raw binary datum. `debezium_protobuf` builds one top-level `Key` or `Envelope` message per subject and uses generated typed oneof wrappers for row values. Table DDL changes alter the value descriptor through `EventSchema.version`; heartbeat records use stable dedicated schemas. Parsed Avro and Protobuf schemas and successful Registry IDs use separate bounded LRU caches.

Avro names are adjusted to `[A-Za-z_][A-Za-z0-9_]*` deterministically. Two database columns that collapse to the same adjusted name are rejected before delivery. Signed integer, floating-point, boolean, binary, array, and hstore/map values retain native Avro categories. Unsigned 64-bit integers, decimals, temporal values, UUIDs, JSON, and otherwise unmodelled extension values use stable strings because the current typed event contract does not carry the precision/logical-type metadata required for lossless Avro logical types. Values outside Avro's signed `long` range fail instead of wrapping or saturating.

Protobuf row wrappers distinguish native values, null through wrapper absence, unavailable placeholders, and textual conversion fallbacks. Arrays, multidimensional arrays, maps, binary values, and the full unsigned 64-bit range retain lossless Protobuf representations. Field numbers are deterministically derived from original database column names, remain stable across restart, reordering, and additive evolution, avoid Protobuf's reserved range, and fail on an active collision. Generated `.proto` definitions are parsed before delivery and dynamic messages are encoded with `prost-reflect`.

With `tombstones.on.delete=true`, which is the Debezium-compatible default, encoding one delete produces an ordered pair in the same delivery batch: the delete envelope followed by the same key with a null payload. The tombstone has its own deterministic derived event ID. Sink success, checkpoint persistence, and source acknowledgement cover both records together. Native YAML exposes the same setting as `format.tombstones_on_delete`; the native Rustium JSON format does not emit tombstones.

Checked-in golden fixtures pin the complete Debezium JSON destination, key, and envelope for PostgreSQL, MySQL, and SQL Server using fixed source positions and timestamps. Companion JSON Schema, Avro, and Protobuf fixtures pin each prioritized connector's destination, key/value subjects, schema type, and complete key/value definitions. JSON-based definitions are compared as structured values, while Protobuf definitions are compared as deterministic source text. Semantically relevant metadata, routing, source fields, field numbers, or wire-schema changes therefore require an intentional fixture update while harmless JSON object-key ordering does not. Each format crate also includes an explicitly ignored regeneration test for controlled fixture updates.

stdout is best-effort and intended for development; it prints tombstones as a `null` line. Kafka uses `librdkafka`, sends tombstones as a true null value, and requires `acks=all` or `-1` so `write` returns only after replica acknowledgement. Producer idempotence is always enabled. Rustium owns bootstrap, acknowledgement, compression, idempotence, and delivery-timeout properties; pass-through properties cannot replace these durability settings. Producer idempotence does not make source-to-Kafka delivery exactly-once.

Schema-aware events are accepted only by Kafka. Before producing an event, the Sink registers its distinct `<topic>-key` and `<topic>-value` definitions through the Confluent Schema Registry API, caches only successful IDs in a configurable bounded LRU, and prefixes each datum with magic byte `0` plus the big-endian four-byte ID. Protobuf adds the Confluent optimized top-level message-index byte `0` before the encoded message. Registration completes before broker delivery, so a registry failure cannot checkpoint or send that event. Network and temporary registry failures are retryable through the shared Sink policy; compatibility and schema rejections fail closed. This path interoperates with Confluent Schema Registry and Apicurio's Confluent compatibility API. Key/value registry endpoints and credentials must currently match, and only `TopicNameStrategy` with automatic registration is accepted.

The shared retry policy maps Debezium's `errors.max.retries`, `errors.retry.delay.initial.ms`, and `errors.retry.delay.max.ms`, with native equivalents under `runtime`; Rustium's finite defaults are 10 retries, 300 ms initial delay, and a 10 s ceiling. PostgreSQL consumes this policy for transient replication recovery, MySQL consumes it for failed binlog-stream recovery, SQL Server consumes it for transient direct-CDC polling recovery, and the runtime consumes it only for failures explicitly classified as retryable by a Sink. Backoff doubles after each attempt and cancellation interrupts the wait. Sink delivery holds and replays the same batch without checkpointing until success; cancellation during delivery backoff leaves that batch unacknowledged for restart and completes normal runtime cleanup without incrementing failed events. This provides at-least-once progress without source-position gaps, while a partially delivered Sink attempt can still produce duplicates. Each Source retains responsibility for reconnect and cursor reconstruction because recovery depends on connector-specific durable state.

### 13. Control Plane and Observability

Lifecycle states are created, starting, snapshotting, streaming, paused, failed, stopping, and stopped. The currently implemented API is:

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | process liveness |
| `GET /health/ready` | connector readiness |
| `GET /v1/connector/status` | lifecycle/reason, position, checkpoint and source-event times, lag, queue, counters |
| `POST /v1/connector/stop` | graceful stop when enabled |
| `POST /v1/connector/signals` | bounded in-process signal submission when mutations and the channel are enabled |
| `GET /metrics` | Prometheus metrics |

Metrics expose connector state, delivered events, failed events, pipeline queue depth, `rustium_sink_retry_attempts`, `rustium_source_lag_seconds`, `rustium_checkpoint_age_seconds`, `rustium_last_event_age_seconds`, and `rustium_connector_state_age_seconds`. Lag is recomputed from wall-clock time and the last durably checkpointed source timestamp; age metrics are `NaN` when their timestamp is unavailable. Encoding failures count one source event, while an exhausted or non-retryable Sink write counts every encoded event in that batch. All currently defined metrics are low-cardinality and suitable for Prometheus scraping.

### 14. Error, Security, and Resource Policy

- Configuration and capability errors fail before capture.
- Unknown protocol/data errors stop the connector.
- Retryable PostgreSQL, MySQL, and SQL Server Source operations plus retryable Sink operations use a shared, finite-by-default retry budget and exponential backoff.
- Source reconnect remains connector-owned; PostgreSQL, MySQL, and SQL Server all have tested connector-specific recovery.
- Queues are bounded, and a blocked or retrying Sink propagates backpressure to source output.
- Database, Kafka, and Schema Registry TLS are configuration-controlled.
- The management server binds to loopback by default.
- Mutating HTTP endpoints are disabled by default.
- Secrets are interpolated at load time and excluded from status and semantic fingerprints.

The security response and deployment baseline are normative in [SECURITY.md](../SECURITY.md). Backup, recovery, alerting, and incident procedures are normative in [docs/runbook.md](runbook.md), and state/configuration compatibility plus rollback rules are normative in [docs/upgrades.md](upgrades.md).

#### 14.1 Container and Kubernetes Packaging

The repository ships a multi-stage `Dockerfile` and `deploy/helm/rustium` chart. The runtime image contains the Rustium binary and only the native Kafka/TLS libraries required at runtime, runs as UID/GID `65532`, uses `/var/lib/rustium` as its writable state directory, and exposes management port `8080`. The chart deliberately enforces one replica and `Recreate` because SQLite checkpoint ownership and source ordering are single-owner contracts. Its default pod uses a retained `ReadWriteOnce` PVC, a read-only root filesystem, dropped Linux capabilities, disabled ServiceAccount token mounting, and live/ready probes. Configuration is mounted from a Secret; production installations should use an externally managed Secret and interpolate database, Kafka, and Registry credentials through environment variables. `scripts/test-packaging.sh` builds the image, executes `rustium --version`, checks OCI/non-root metadata, lints and renders the chart, verifies persistence/probe/security invariants, and rejects multiple replicas. Matching protected `v*` tags run `.github/workflows/release.yml`, which publishes signed multi-architecture GHCR images with SBOM/provenance, a Helm OCI artifact, and a GitHub Release with checksums. Crates.io publication is isolated to the manual `.github/workflows/publish-crates.yml` workflow, which requires an explicitly rotated Secret and publishes from leaf crates to the CLI.

Every publishable workspace crate also carries a concise English-first bilingual README. The packaging, release, and publication gates enumerate each Cargo package and require `README.md` in its tarball, so a published crate remains usable without the monorepo checkout. They also require every prioritized connector schema fixture in each schema-aware format crate, preventing a publication manifest from silently dropping its checked-in wire-contract evidence.

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

The release operator must also run the backup/restore, rollback, container, and Helm procedures in [docs/runbook.md](runbook.md) and [docs/upgrades.md](upgrades.md). A green compile without those operational checks is not a production release.

The ignored MySQL Docker test is runnable with:

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

The required `mysql-cdc` CI gate defaults to three capacity-one backpressure/reconnect cycles and accepts `RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES=1..1000` for longer runs. Each cycle kills the active replication connection, commits while disconnected, and verifies a different connection identity plus every expected row in first-seen source order.

The ignored external MySQL test reads both admin and CDC connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_MYSQL_TEST_HOST=mysql.example.com \
RUSTIUM_MYSQL_TEST_PORT=3306 \
RUSTIUM_MYSQL_TEST_ADMIN_USER=root \
RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_USER=cdc \
RUSTIUM_MYSQL_TEST_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo \
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

This gate creates isolated selected tables with the admin account and uses the CDC account for capture. It verifies capacity-one backpressure/reconnect cycles with replication-connection replacement, snapshot/replication, exact-server-UUID GTID-filtered startup, `heartbeat.action.query`, checkpointed schema versions across destructive and schema-neutral DDL, periodic idle-stream heartbeats from a safe binlog position, OGC spatial equality, and Debezium column transformations across initial snapshots, binlog before/after images, and incremental snapshots. With the Kafka variable, it also verifies real-broker replay after a completed connector checkpoint while the original signal offset remains uncommitted, followed by idempotent Source handling and offset commit only after the recovery checkpoint. It has passed against MySQL 8.4 with row binlog and GTID enabled.

The ignored external PostgreSQL test reads connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com \
RUSTIUM_POSTGRES_TEST_PORT=5432 \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me' \
RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo \
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

These tests create isolated business-table/signal-table/publication/slot/role names and temporary signal files, then verify snapshot rows, ordered transactional create/update/delete events, checkpoint stop, an old-schema row, destructive drop/add-column DDL, a new-schema row, historical `Relation` replay with schema versions 1 and 2, restart without snapshot replay, repeated forced termination and automatic recovery of the active replication backend under capacity-one Source-output backpressure, fail-closed resume after deleting a checkpoint's slot, periodic heartbeat records at a safe WAL position, `heartbeat.action.query`, heartbeat-table filtering, checkpointed source/file/in-process incremental snapshots, immediate external-signal state checkpointing, filtered chunks, concurrent-update deduplication, pause/resume/scoped-stop control, file and in-process read-only snapshots without a signal table, read-only transaction watermarks with a held update, zero watermark writes under restricted permissions, unique surrogate ordering against a reversed UUID primary-key order, signal-table isolation, and identical snapshot/WAL conversion across the core PostgreSQL type matrix including hstore, domain, enum, and tsvector values. The opt-in superuser fixture has also driven a live slot to `wal_status=lost` and verified fail-closed resume on PostgreSQL 17. The Docker fixture additionally requires pgvector and PostGIS snapshot/WAL equality on PostgreSQL 17.

```bash
bash scripts/test-postgresql-extensions.sh
```

The optional external Kafka signal gate creates a unique single-partition topic, sends a foreign-key record followed by a connector-key record, verifies that only the matching signal is delivered, observes the skipped offset, releases the durable acknowledgement, verifies the next committed offset, and deletes the topic:

```bash
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-signal-kafka --test kafka_external -- --ignored --nocapture
```

The required Kafka Sink gate starts an isolated Redpanda broker and Confluent-compatible Schema Registry. It creates unique single-partition topics and verifies ordered keys, payloads, headers, JSON Schema, Avro, and Protobuf key/value registration, additive schema evolution, binary decoding against schemas fetched by ID, Confluent framing and Protobuf message indexes, lookup by ID and latest subject, true null tombstones, successful flush, topic cleanup, and an explicit delivery error for a missing topic with automatic creation disabled:

```bash
bash scripts/test-kafka-sink.sh
```

The shared runtime soak gate is also required by GitHub Actions:

```bash
RUSTIUM_RUNTIME_SOAK_CYCLES=256 \
cargo test -p rustium-core --test runtime_soak -- --ignored --nocapture
```

Across each configured cycle, the gate holds a capacity-one Source queue behind a retrying Sink, checks byte-identical batch replay and checkpoint immobility on every attempt, releases the Sink, and requires ordered final progress. Repeated exhaustion and cancellation paths additionally verify retry metrics, `FAILED` versus `STOPPED` lifecycle semantics, Source cancellation, Sink shutdown, prompt interruption of a 60-second backoff, and retention of the last acknowledged checkpoint. The cycle count accepts `1..10000`; required CI runs 256 cycles.

The ignored external SQL Server test reads connection settings from the environment and does not contain repository credentials:

```bash
RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com \
RUSTIUM_SQLSERVER_TEST_PORT=1433 \
RUSTIUM_SQLSERVER_TEST_USER=sa \
RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me' \
RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo \
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

This gate creates isolated table/capture-instance names and Kafka topics, waits for SQL Agent initialization, and verifies snapshot rows, fetch-size-one transaction continuation, mid-transaction checkpoint replay, repeated capacity-one polling-session termination and recovery from the unchanged typed CDC cursor, connection-identity replacement, every expected row, first-seen source order, concurrent commit ordering, retention failure, heartbeat/action-query, core and extended snapshot/CDC type equality, four-part snapshot/CDC and three-part incremental-snapshot column transformations, raw signal-command isolation, checkpointed in-process keyset restart, source-table signaling with additional conditions, durable completed signal IDs, real-broker replay after connector-state persistence but before Kafka offset commit, and cleanup. It has passed against SQL Server 2022 Developer RTM-CU25.

The separate SQL Server Docker portability gate is required by GitHub Actions and remains runnable where the Microsoft image is available:

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

The gate is remote-daemon capable without exposing the ephemeral SQL Server port. Docker commands follow the selected context. When `RUSTIUM_SQLSERVER_DOCKER_SSH_HOST` is set, the container remains published only on the daemon host's loopback address and the test creates an authenticated local SSH forward; `RUSTIUM_SQLSERVER_DOCKER_SSH_LOCAL_PORT` optionally fixes the local endpoint.

### 16. Roadmap

1. Freeze the public configuration, event, and persisted-state compatibility contracts after the remaining release gates pass.
2. Publish crates in dependency order through the manual crates.io workflow after a reviewed release and a rotated `CARGO_REGISTRY_TOKEN` secret.
3. Consider additional databases only after the current five connectors and shared runtime pass the full `1.0` gates.

---

## 简体中文

### 1. 产品定义

Rustium 是一个使用 Rust 独立实现的开源、基于日志的变更数据捕获平台。它读取数据库已提交变更，转换为与数据库无关的强类型事件，并将有序记录投递到下游 Sink。

Rustium 是独立 Rust 服务。原生 Source 不依赖 Kafka Connect 或 JVM；对于依赖专有客户端、节点本地日志或分布式 stream API 的数据库协议，bridge-backed Source 会明确运行兼容 Debezium engine。

Rustium 以最新版 Debezium 的架构、事件行为和配置名称作为兼容性参考。Rustium 不是 Debezium fork，也不复制 Debezium Java 源码。

#### 1.1 连接器优先级

连接器严格按以下顺序推进：

1. PostgreSQL
2. MySQL
3. SQL Server
4. Oracle
5. MongoDB
6. MariaDB
7. Db2
8. Cassandra 3/4/5
9. Vitess
10. Spanner
11. Informix
12. CockroachDB
13. YashanDB

前五类 Source 使用原生 Rust 连接器。当前 Debezium source catalog 中其余数据库全部使用持久 Debezium Engine bridge，并遵循相同的正确性、恢复、类型覆盖和运维发布门槛。

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
- 把实际由 bridge engine 执行的专有 CDC 协议宣称为原生实现。

### 2. 当前实现

Workspace 已包含可运行的 alpha 服务。

| 组件 | 状态 |
|---|---|
| 核心事件/位点/运行时 trait | 已实现 |
| 有界 Tokio 运行时 | 已实现；CI 强制运行 256 轮背压/重试 soak |
| SQLite checkpoint v2 与连接器状态 | 已实现；仍可读取 version 1 JSON |
| 原生 JSON、Debezium JSON、Confluent framing JSON Schema、Avro 和 Protobuf | 已实现；CI 强制运行真实 Registry/Kafka 门槛 |
| stdout 和 Kafka Sink | 已实现；CI 强制运行 Kafka 真实 broker 投递/失败门槛 |
| PostgreSQL Source | 已实现；PostgreSQL 17 恢复、heartbeat/action-query、可写/只读增量快照和核心类型矩阵门槛通过 |
| MySQL Source | 已实现；必选 Docker CI 和外部 MySQL 8.4 恢复/soak 门槛通过 |
| SQL Server Source | 已实现；必选 Docker CI 和外部 SQL Server 2022 恢复/soak 门槛通过 |
| Oracle LogMiner Source | 已实现；单元/配置门槛通过；真实 Oracle 门槛为显式外部测试 |
| MongoDB Change Stream Source | 已实现；单元/配置门槛通过；真实 replica-set 门槛为显式外部测试 |
| 其余 Debezium 数据库 Source | MariaDB、Db2、Cassandra 3/4/5、Vitess、Spanner、Informix、CockroachDB、YashanDB 已通过持久 HTTP/Kafka bridge 实现 |
| CLI 和 HTTP 管理 | 已实现 |
| 可复现的非 root 容器镜像与 Helm Chart 源码 | 已实现；CI 强制运行 packaging gate |
| Tagged release 镜像、Helm OCI Chart 和 GitHub Release 自动化 | 已实现；仅由匹配的受保护 `v*` tag 触发 |
| 已发布 crates | 全部 workspace crate 与 CLI 已发布为 `0.1.0-alpha.2`，包含 Oracle、MongoDB 和持久 Debezium bridge |

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
|-- rustium-format-avro/   Debezium 兼容 Avro schema 与 datum 编码
|-- rustium-format-json/   原生 JSON、Debezium JSON 和 JSON Schema descriptor
|-- rustium-format-protobuf/ Debezium 兼容 Protobuf schema 与 message 编码
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

取消操作会停止 Source 读取；如果投递无需再次重试即可完成，则刷新待处理事件；只持久化已确认位点；刷新并关闭 Sink，最后关闭协议资源。在 Sink 重试退避期间取消会中断等待，将待处理位点保留为未 checkpoint 状态以供重放，执行相同清理，并在不计业务故障的情况下进入 `STOPPED`。所有流水线错误都进入清理路径：取消 Source，超过关闭超时后将其 abort，并且仍会执行 Sink shutdown hook。系统保留最早的实际错误，绝不会 checkpoint 未确认记录。

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

项目优先使用 Debezium 名称，包括 `name`、`connector.class`、`topic.prefix`、`database.*`、`table.include.list`、`table.exclude.list`、`publication.autocreate.mode`、`snapshot.mode`、`snapshot.fetch.size`、`snapshot.include.collection.list`、`tombstones.on.delete`、`max.queue.size`、`max.batch.size` 和 `poll.interval.ms`。成对的 Confluent `JsonSchemaConverter`、`AvroConverter` 与 `ProtobufConverter` key/value 设置分别映射到 `debezium_json_schema`、`debezium_avro` 与 `debezium_protobuf`。三者都使用一个共享的 `schema.registry.url` 列表、`USER_INFO` 基本认证、自动注册、请求超时和有界 ID 缓存。Avro schema/field 名称执行确定性调整，调整后冲突会编码失败。Protobuf 始终清理非法名称，并拒绝会选择其他 nullable 或 union wire contract 的选项。不支持的 converter 不对称、调整模式或 subject/version 选择会直接校验失败。

`snapshot.include.collection.list` 在原生配置中保存为 `snapshot.include_collections`，只在发出 initial snapshot 或 `when_needed` 恢复快照时执行 anchored regular expression。PostgreSQL 使用 `schema.table`，MySQL 使用 `database.table`，SQL Server 使用 `database.schema.table`。发现与 connector schema history 仍覆盖所有被 streaming 选择的表，只有 snapshot row scan 会被过滤；incremental snapshot 和后续 streaming 因此继续使用普通 source filter。必跑 PostgreSQL 17、MySQL 8.4 和 SQL Server 2022 集成门禁会选择两张表用于 streaming、只快照其中一张，并要求之后仍收到另一张表的 create event。

当前实现的 Debezium snapshot mode 为 `initial`、`when_needed`、`never` 和 legacy alias `no_data`。具有不同语义的 mode，包括 `always`、`initial_only`、`schema_only`、`recovery` 以及 custom mode，会直接校验失败，不会静默映射。

Properties 中 Rustium 自身的状态、Sink、Server、日志和 Producer 扩展使用 `rustium.*`。

### 9. PostgreSQL 连接器

#### 9.1 前置条件

- PostgreSQL 14 或更高版本。
- `wal_level=logical`。
- 已存在的 `pgoutput` publication，或创建所配置 publication 模式所需的权限。
- 复制和表读取权限。
- 唯一复制 slot。

原生 YAML 使用 `source.publication_autocreate_mode`，默认值为 `disabled`，使既有部署保持原 publication 所有权契约和语义指纹。Debezium properties 使用 `publication.autocreate.mode`，默认值为 Debezium 的 `all_tables`。`disabled` 要求命名 publication 已存在。`all_tables` 在缺失时创建 `FOR ALL TABLES` publication。`filtered` 使用完整匹配的 source filter 创建 publication，或对现有表级 publication 执行 `ALTER PUBLICATION ... SET TABLE`；已配置的 signal table 即使不暴露为业务事件也会被纳入。`filtered` 遇到现有 `FOR ALL TABLES` publication 时会拒绝操作，不会静默改变其所有权范围。`no_tables` 在缺失时创建空 publication，并保持现有 publication 不变。创建需要数据库 `CREATE` 权限，表级 publication 要求拥有相关表，更改既有 publication 要求拥有该 publication，`FOR ALL TABLES` 要求 superuser 权限。

`replica.identity.autoset.values` 解析为有顺序、完整匹配的表正则，目标可以是 `DEFAULT`、`FULL`、`NOTHING` 或 `INDEX <name>`。完成 publication 准备后，Rustium 会读取 publication 中被 source filter 允许的每张表的当前 identity 和 replica index，并排除 signal table。它在修改前拒绝重叠规则，只计算确实不同的 identity，并在一个事务内执行完整的 `ALTER TABLE ... REPLICA IDENTITY` 集合。任一语句或权限失败都会回滚事务。原生 YAML 将每条规则表示为 `table`、`identity` 和可选 `index`；空规则保持旧行为和 fingerprint。因为 validation 可能修改数据库 metadata，connector role 必须拥有受影响的表。PostgreSQL 会校验 replica index 的唯一性、immediate、无 predicate 和 `NOT NULL` 列。

`publish.via.partition.root` 默认为 false，并映射为原生 `source.publish_via_partition_root`。设置为 true 时，所有自动创建的 publication 都包含 `WITH (publish_via_partition_root = true)`。既有 publication 不会被静默修改：validation 会将 `pg_publication.pubviaroot` 与配置值比较，任一方向不一致都拒绝启动。启用 root publication 后，discovery、snapshot record、relation metadata、streaming event 和下游 topic routing 都使用 partitioned root table，而不是物理 leaf partition。

`slot.failover` 默认为 false，并映射为原生 `source.slot_failover`；值为 false 时不会进入 semantic fingerprint material。该选项只适用于 managed logical slot。创建或更新 slot 前，Rustium 会检查 `server_version_num` 和 `pg_is_in_recovery()`。PostgreSQL 17+ 主库会在 slot 及 exported snapshot 建立后执行 `ALTER_REPLICATION_SLOT ... (FAILOVER true)`，使用 PostgreSQL 17 option 语法且不削弱 snapshot handoff。旧版本和 standby 节点按 Debezium 的降级行为记录 warning 并保留普通 logical slot。External slot ownership 会拒绝该选项，因为 Rustium 不应修改外部管理的 metadata。

`slot.drop.on.stop` 默认为 false，并映射为原生 `source.drop_slot_on_stop`。它只适用于 managed ownership；该选项改变生命周期清理而不是选中事件，因此不进入 semantic fingerprint。有序 cancellation 时，Rustium 会发送最终 replication feedback、释放 replication transport，并在 PostgreSQL 仍报告 slot active 时有限重试普通连接上的 `pg_drop_replication_slot`。Slot 已不存在视为幂等成功；持续 active 或权限失败会让有序 shutdown 明确失败。Stream failure、自动 reconnect、channel failure、进程崩溃和强制 task abort 都不会请求删除。该区分既保留异常退出后的恢复能力，也匹配 Debezium 的 close-time 选项。启用删除会让 connector 停止期间提交的变更形成有意的 CDC gap，因此持续采集的生产 source 不建议启用。

`snapshot.locking.mode` 映射为原生 `source.snapshot_locking_mode`，接受 Debezium 默认值 `none` 或 `shared`；Java SPI 模式 `custom` 会配置失败。Shared 模式在 snapshot 事务开始后（使用 exported snapshot 时先完成导入）、schema discovery 前启动。Rustium 设置 transaction-local `lock_timeout`，按 schema/table 顺序读取 publication captured table，保留 `snapshot.include.collection.list` 允许的业务表，加入已配置 signal table，再按该顺序为每张表获取一个 `ACCESS SHARE` lock。普通 DML 仍可执行，并发 DDL 会等待 snapshot commit。`snapshot.lock.timeout.ms` 映射为原生 `source.snapshot_lock_timeout`，默认 10 秒，同时限制 publication-view lookup 和每个显式 lock，因为 PostgreSQL catalog view 自身也可能请求 relation lock。零值会禁用 PostgreSQL timeout；超过有符号 32 位毫秒上限的值会校验失败。两个运维参数都不进入 semantic fingerprint。Lock 或 catalog timeout 会在发出任何 snapshot row 前中止，managed slot 会保留以便显式重试。

`snapshot.isolation.mode` 映射为原生 `source.snapshot_isolation_mode`，接受 Debezium 默认的 `serializable`、`repeatable_read`、`read_committed` 和 `read_uncommitted`。Serializable 与 repeatable-read 使用 `EXPORT_SNAPSHOT` 及只读 REPEATABLE READ 导入，保持既有无缺口 handoff；PostgreSQL 的 imported-snapshot 协议不允许 READ COMMITTED。因此较低隔离模式使用 `NOEXPORT_SNAPSHOT` 创建 slot，打开请求的只读事务（`READ UNCOMMITTED` 在 PostgreSQL 中是 READ COMMITTED 别名），并以 slot 的 `restart_lsn` 作为 `START_REPLICATION` 起点。这样允许跨 statement snapshot 组装快照，同时仍会送达 slot 创建后的全部变更。Serializable 与 repeatable-read 都导入同一 exported snapshot，因此不进入 fingerprint；较低模式会改变 snapshot 一致性与 source handoff 语义，所以进入 fingerprint。

#### 9.2 快照切换

使用默认 `serializable` 或 `repeatable_read` 隔离级别的托管初始快照流程：

1. 准备或重建非活动托管 slot；
2. 创建逻辑 slot 并导出 snapshot；
3. 打开 repeatable-read、read-only SQL 事务；
4. 导入导出的 snapshot；
5. 发现选中的 publication 表和 schema；
6. 以有界分页扫描可选 snapshot-only collection filter 允许的每张表；
7. 在 slot 锚点 LSN 携带基线 schema history 发出 snapshot-complete；
8. 从该 LSN 启动 `pgoutput`。

Slot 会保留快照期间提交的变更，因此切换不存在缺口。

对于 `read_committed` 或 `read_uncommitted`，第 2 步使用 `NOEXPORT_SNAPSHOT`，第 3 步打开请求的只读隔离级别，第 7 步记录 slot 的 `restart_lsn`。该路径以一个全局一致 snapshot 换取 Debezium 兼容的较低隔离级别，但仍保留 slot 创建后提交的全部 WAL。

#### 9.3 流式捕获

`pg_walstream` 提供复制传输和协议解析。Rustium 转换 begin、insert、update、delete、truncate、commit 和流式事务事件。同 LSN 序号让位点可全序和重放。缺失的未变化 TOAST 列成为 `Unavailable`。

Replica identity 决定旧行形状。`FULL` 提供全部旧列；`DEFAULT` 和 `INDEX` 提供 replica-key 值，PostgreSQL 在协议行中以 null placeholder 表示不可用的非 key 字段。PostgreSQL 17 外部门槛会同时验证 catalog mode，以及真实 `FULL` 和非首列 `INDEX` before image。

PostgreSQL 建立连接和暂时性 stream 恢复使用共享的 Debezium 兼容 `errors.max.retries`、`errors.retry.delay.initial.ms` 和 `errors.retry.delay.max.ms` 策略。配置的重试次数不包含首次尝试；`0` 关闭自动 stream 恢复，`-1` 表示实际上无界。退避和连接尝试位于 `pg_walstream` 内，Rustium 继续拥有可重放 LSN、slot 连续性检查、已解码事务状态和 checkpoint 确认。

PostgreSQL 还接受显式 `slot.max.retries` 与 `slot.retry.delay.ms`；只有对应共享 `errors.*` 参数缺失时，它们才作为迁移回退。slot delay 会同时成为初始和最大重试延迟，使共享重试引擎保持 Debezium 的固定等待。显式 `errors.*` 优先；两种形式都省略时保留 Rustium 已建立的共享默认值。`status.update.interval.ms` 映射为原生 `source.status_update_interval`，默认 10 秒，并配置 `pg_walstream` 周期性 feedback 检查。Feedback atomic 只会在 Sink delivery 和 checkpoint 持久化之后由 `SourceContext.acknowledged` 推进；每次 connector-owned acknowledgement 还会立即强制 status update，因此 durable slot 推进不受周期延迟。`database.tcpKeepAlive` 映射为原生 `source.tcp_keepalive`，默认 true，并在普通连接与 replication URL 中一致加入 libpq `keepalives=1` 或 `keepalives=0`。这些仅影响连接的控制项不进入 event-semantic fingerprint。

`xmin.fetch.interval.ms` 映射为原生 `source.xmin_fetch_interval`，默认值为零。正值会在 logical slot 存在后打开一个普通 libpq connection，在第一条符合条件的已解码 message 前读取 `catalog_xmin`，并在周期到期前复用缓存的无符号 XID。Slot 返回 null 时会在下一条 message 再查询，与 Debezium 无 XMIN 时不缓存的行为一致。Streaming `PostgresPosition` 会保存该可选值；JSON、Avro、Protobuf 的 PostgreSQL source contract 始终包含可空 `xmin`，snapshot 和关闭跟踪时使用 null。新增 position member 使用 serde default/omit 规则，因此零值 checkpoint 保持旧 serialization shape 与 event ID。查询、连接和解析错误会使 source 失败，不会静默保留陈旧 metadata。正周期会改变输出 source metadata，因此进入 event-semantic fingerprint。

恢复前，`offset.mismatch.strategy` 会协调 durable checkpoint 与 `confirmed_flush_lsn`。`no_validation` 保留旧有 checkpoint 启动行为。当 checkpoint 大于 slot 时，`trust_offset` 和 `trust_greater_lsn` 调用 `pg_replication_slot_advance`，`trust_slot` 保持 checkpoint 且不修改 slot。当 slot 大于 checkpoint 时，`trust_offset` fail-closed，`trust_slot` 与 `trust_greater_lsn` 采用 slot。已废弃的 `slot.seek.to.known.offset.on.start` 将 true 映射为 `trust_offset`、false 映射为 `no_validation`，但新参数优先。原生 YAML 使用 `source.offset_mismatch_strategy`。可推进 slot 的策略要求 managed ownership；事务中间 checkpoint 会在修改前被拒绝，因为推进 logical slot 可能跨过未处理的事务事件。采用权威 slot 后，Rustium 会将 replay filter 替换为该 LSN 的干净位点，重新从当前 catalog 加载选中 schema，丢弃活动 incremental-snapshot progress，并把 connector state 标记为在下一 checkpoint 持久化；已完成 signal ID 继续保留。由于非默认模式可能修改 source state 或有意跳过本地 history，它们会进入 semantic fingerprint。

`lsn.flush.mode` 分配 feedback ownership。`connector` 是默认值，只把 `SourceContext.acknowledged` 复制到 stream 的 flushed/applied tracker。`manual` 消费 acknowledgement 通知但不修改 tracker，把 slot 推进交给外部。`connector_and_driver` 使用单调最大值初始化共享 feedback tracker；`pg_walstream` 会把每次 status update 限制到内部 `last_received_lsn`，因此无需暴露传输内部状态即可复现 pgjdbc 对普通 event 和 server keepalive 的 automatic flushing。已废弃的 `flush.lsn.source` 将 true 映射为 `connector`、false 映射为 `manual`，新 enum 优先。原生 YAML 使用 `source.lsn_flush_mode`。如果外部 owner 停滞，manual mode 可能无限保留 WAL。Driver mode 可能让 `confirmed_flush_lsn` 超过本地 durable checkpoint，因此用 replay safety 换取有界 retention；应配合 slot-authoritative offset mismatch strategy。非默认 mode 进入 semantic fingerprint。

`lsn.flush.timeout.ms` 映射为必须为正值的原生 `source.lsn_flush_timeout`，默认 30 秒。Connector 会用该 timeout 包裹每次 durable acknowledgement 强制触发的真实异步 `send_feedback()` future。`lsn.flush.timeout.action` 映射为原生 `source.lsn_flush_timeout_action`，接受 `fail`（默认）、`warn` 或 `ignore`。超时时会分别使 source 失败、记录 warning 后继续，或在 debug 级别继续；如果 I/O future 已完成并返回错误，则无论 action 如何都使 source 失败。Manual mode 不进入该 acknowledgement-flush 路径。这些运维参数不进入 event-semantic fingerprint。

`slot.stream.params` 按 Debezium 的分号分隔 decoder 参数列表解析。由于 Rustium 只支持 `pgoutput`，已实现参数面是 PostgreSQL 16+ 的 `origin=any|none` 过滤器，并直接映射到 `pg_walstream::OriginFilter`。`any` 请求本地事务与带 replication origin 的事务，`none` 只请求本地事务。原生 YAML 使用确定性 `source.slot_stream_params` map。空参数保持旧 semantic fingerprint；已配置参数会进入 fingerprint，因为它会改变 source selection。畸形条目、未知参数名、非法 origin 值，以及在 PostgreSQL 14/15 上配置 origin filtering 都会在打开 replication connection 前使配置失败。

`database.initial.statements` 使用 Debezium 的逐字符 delimiter 契约：单个分号结束一条语句，`;;` 产生一个字面分号。生成的有序列表会在每个普通 libpq connection 建立后立即执行，覆盖 validation、catalog lookup、snapshot、heartbeat action、XMIN metadata、slot/offset inspection、incremental snapshot 和有序 slot cleanup。Replication transport 与 replication-protocol slot mutation connection 会有意跳过该列表。原生 YAML 使用已拆分的 `source.database_initial_statements` 列表，空白原生条目会使 validation 失败。由于 connection 建立由连接器自行决定，且不同 connection 之间的执行不具备原子性，语句应只用于幂等 session configuration。失败会标识从 1 开始的条目序号，而 Rustium 不回显配置 SQL；非空列表进入 semantic fingerprint，因为 session setting 可能改变捕获值。

PostgreSQL TLS 保持在共享 connection URL 中，使普通 connection 与 replication connection 不会漂移。`database.sslmode` 接受 libpq 的 `disable`、`allow`、`prefer`、`require`、`verify-ca` 和 `verify-full`。`database.sslrootcert` 映射为原生 `source.ssl_root_cert`，以及 rustls transport 独占使用的 PEM root store。TLS path 与 mode 属于运维配置而非 event semantic，因此不进入 fingerprint。当前 `pg_walstream 0.8` rustls backend 不解析或加载 `sslcert`、`sslkey`、`sslpassword`；Rustium 会在打开 connection 前拒绝这些字段，不会在用户要求 mutual TLS 时静默无证书运行。Debezium `database.sslfactory` 是 JVM class extension，同样会被显式拒绝。

未知 PostgreSQL 类型遵循 Debezium `include.unknown.datatypes` 契约。Catalog discovery 会在构造 event schema 前分类已支持的 built-in、enum、range/multirange、可支持的 domain/array base 和显式支持的 extension type。默认 false 模式仍在 schema history 中保留每列 OID/typmod，但从 event schema 与 row 中省略不支持字段。True 会把这些字段映射为 `bytea`，并保留 pgoutput 文本值的 UTF-8 bytes；snapshot 与 incremental snapshot 的 `::text` projection 使用同一 byte conversion。Connector-state version 6 持久化 opaque-column 集合。Version 1 到 5 会以空集合反序列化，再在重放前用 validation catalog 中 name/OID/typmod 完全匹配的列归一化；发生变化的历史布局不会被覆盖。该选项会进入 semantic fingerprint，因为它同时改变 schema 与数据。

PostgreSQL MONEY 遵循 Debezium `money.fraction.digits` 契约，原生配置为 `source.money_fraction_digits`，默认 scale 为 2。Catalog 与 Relation schema 构造会把有效的有符号 16 位 scale 编码为 `money(<scale>)`。统一 converter 会从 PostgreSQL 文本中移除前导货币符号、逗号分组、正负号或会计括号，解析任意精度 decimal，再执行 `HALF_UP` 舍入。Initial snapshot、incremental snapshot 和 WAL 的 scalar/array 共用该 converter。畸形输入保留为字符串。默认值不进入 semantic fingerprint；非默认 scale 会改变 schema 与数据，因此进入 fingerprint。

`schema.refresh.mode` 映射为原生 `source.schema_refresh_mode`，并严格接受 Debezium 默认值 `columns_diff` 或 `columns_diff_exclude_unchanged_toast`。在 Rustium 只使用 pgoutput 的 transport 下，两者行为等价。pgoutput 会独立于 row tuple 发出完整 `Relation` layout，Rustium 也只根据这些 layout 增加 schema version；unchanged TOAST marker 会由 transport 跳过，不可能被误判为 schema 列删除。Row conversion 会在存在 `REPLICA IDENTITY FULL` before image 时补回复用值，否则补为 `DataValue::Unavailable`。纯行更新在两种 mode 下都不会把 connector schema state 标脏，因此该选项不进入 semantic fingerprint。这样既支持 Debezium 配置迁移，也不会继承其为非 pgoutput decoder 记录的 stale-schema 风险。

PostgreSQL connector-state payload 持久化快照表布局，以及每列的类型 OID 和 typmod。重启时先恢复该基线，再打开 slot。随后每个 `Relation` 消息提供后续行事件对应的历史列名、顺序、类型身份和 key 标记。只有精确匹配的 catalog 元数据用于补充类型名和可空性，不会覆盖 WAL 列布局。schema 变化时版本递增，更新状态附着到下一条可 checkpoint 的 SourceRecord。

`pg_walstream` 会缓存 relation 的第一条协议消息，只在后续布局变化时发出显式 `Relation` event。因此，当表被动态加入运行中的 `no_tables` 或表级 publication 时，Rustium 会识别首条选中 DML 的 schema cache miss，在解码同一事件之前从当前 catalog 发现 schema，并将 schema 状态标记为等待下一 checkpoint 持久化。这样无需丢弃首行或重启即可扩展 publication。

恢复已完成 checkpoint 之前，Rustium 会验证原 replication slot 仍然存在、仍使用 `pgoutput`，且没有进入 PostgreSQL `wal_status=unreserved` 或 `lost`。连续性检查失败时会在复制传输可能创建替代 slot 之前停止，并要求 reset checkpoint 后重新执行 initial snapshot。该契约有意选择显式恢复，而不是接受静默 WAL 缺口。

当 `snapshot.mode=when_needed` 时，同样的连续性失败或缺少 schema history 的旧 checkpoint 会触发新的 managed snapshot，并在恢复 streaming 前清理失效 connector state。`initial` 模式对这些情况保持 fail-closed。

PostgreSQL 不会在 `Relation` 中记录原始 DDL、列可空性或 default。如果短暂历史列已经从当前 catalog 消失，且 checkpoint 基线中也不存在，Rustium 会通过 OID/typmod 解析类型，并保守地标记为 optional。这样可保持行解码和顺序正确，同时不伪造 WAL 未提供的元数据。

快照查询通过 PostgreSQL 的 `::text` 输出函数逐列投影，不再让整行经过 JSON 中间层。快照值和 `pgoutput` 值因此共用同一个转换器，可一致保留 numeric scale/precision、bytea、JSON 文本、时间格式和数组语法。数组解析器支持带引号和转义的元素、SQL NULL 与字符串 `"NULL"` 的区别、显式下界、嵌套维度和按元素类型转换。畸形数组文本会完整保留为字符串，不会被部分解码。

PostgreSQL 列转换是 initial snapshot、incremental snapshot 和 WAL record 共用的转换后发出层。Debezium 动态参数会编译为 anchored、大小写不敏感的 `schema.table.column` selector。规则选择确定且按类别优先：truncate、固定 mask、hash V1、hash V2；按该顺序第一个匹配 selector 胜出。固定 mask 替换 NULL，hash 保留 NULL，hash 输出为小写十六进制并按声明的字符 typmod 截短。V1 在计算 `salt || bytes` 前复现 Java `ObjectOutputStream` String serialization；V2 计算 `salt || UTF-8(value)`。实现支持常用 JCA MD2、MD5、SHA-1/SHA-2、截短 SHA-512 和 SHA-3 名称。转换有意在 incremental snapshot/WAL bookkeeping 之后执行，因此 keyset progress 和并发更新去重仍使用原始主键。Salt 属于语义配置，但 fingerprint 只保存其 SHA-256 digest。

Catalog 发现会把 PostgreSQL domain 解析为基础转换类型，同时保留 domain OID/typmod 作为 schema 身份；domain 数组使用相同的元素转换。Enum、range、网络类型、`ltree`、`isbn` 和 `tsvector` 使用 PostgreSQL 规范文本。`hstore.handling.mode=json` 把 hstore 输出映射为 JSON，`map` 映射为字符串键 `DataValue::Map`，并支持 null、转义和 hstore 数组。`vector` 与 `halfvec` 转为浮点数组，`sparsevec` 转为包含维度和索引值的 map，PostGIS geometry/geography 转为完整 EWKB 字节。所有专用解析器都是全有或全无；已支持类型的畸形输入会回退为原始字符串。

`interval.handling.mode` 实现 Debezium 的 `numeric` 和 `string` 契约。Properties 默认使用 `numeric`，按每月平均 `365.25 / 12` 天以及与 Debezium `MicroDuration` 相同的浮点运算和截断顺序生成 `DataValue::Int64` 微秒值。`string` 生成完整 `PnYnMnDTnHnMnS` 表示，并保留各分量独立符号。解析器接受 PostgreSQL 17 的全部 `IntervalStyle` 输出：`postgres`、`postgres_verbose`、`sql_standard` 和 `iso_8601`；snapshot、incremental snapshot、WAL 以及 scalar/array element 共用同一 converter。原生默认值 `source.interval_handling_mode=postgres` 返回 server 原始文本，以保持旧原生行为和 fingerprint。畸形输入不会被部分解释。

只有 source 配置启用时，Rustium 才会从 `pgoutput` 请求 PostgreSQL 14+ logical decoding `Message` record。Debezium properties 默认启用捕获；原生 `source.logical_decoding_messages=false` 保留旧行为和 fingerprint。`message.prefix.include.list` 与 `message.prefix.exclude.list` 映射为互斥的完整匹配正则，原生别名为 `source.message_prefix_include_list` 和 `source.message_prefix_exclude_list`。允许的消息在 `<topic.prefix>.message` 上生成 `prefix` key 和 `message` block；JSON 对默认 bytes 使用 Base64，Avro 与 Protobuf 保留 bytes。事务消息加入活动事务的 ID 和 total order，在 commit 前保持 data boundary。非事务消息本身同时是 event 与 commit boundary。被过滤的非事务消息会生成仅包含位点的 commit boundary，使已确认 WAL 能够推进且不暴露记录。

`heartbeat.interval.ms` 默认为零。设置为正数后，会在最新已进入有界队列的 SourceRecord 位点发送可见 heartbeat；首条 streaming event 之前则使用已完成快照的锚点。没有源位点时不会发送 heartbeat。可选的 `heartbeat.action.query` 在每个周期先通过复用的普通 SQL 连接执行。查询产生的 WAL 只有在 `pgoutput` 实际返回后才算进度，查询失败会携带数据库错误停止 Source。heartbeat 表可以加入 publication 但被选表规则排除；其事务 commit 仍可推进安全 WAL 位点，而不会暴露成业务表事件。

#### 9.4 Source 信号与增量快照

信号实现遵循 Debezium `source`、`file`、`in-process` 和 `kafka` channel。`signal.data.collection` 指向一个 schema-qualified 表，该表必须按顺序且仅包含 `id`、`type`、`data` 三个文本兼容列，必须加入 publication，并始终从业务快照和事件中过滤。`signal.enabled.channels` 接受这四个已实现 channel 的任意组合。`signal.file` 默认为 `file-signals.txt`，`signal.poll.interval.ms` 默认为 5000 ms。File reader 消费非空 JSON Lines，并在成功读取后清空文件，遵循 Debezium 无重试 channel 语义。`execute-snapshot` 记录接受 `type=incremental`、`data-collections` 中的完整匹配正则、包含大小写不敏感集合表达式和 SQL filter 的 `additional-conditions`，以及可选 `surrogate-key` 列名。

禁用 `source` channel 时会忽略 source-table command，但连接器生成的 watermark record 对可写外部信号快照仍然有效。外部 watermark action type 会被拒绝。每个有效外部 action 之后，Rustium 会在最新安全 LSN 发出仅包含 connector state 的位点事务边界；fresh stream 从 slot 的 `confirmed_flush_lsn` 或 `restart_lsn` 获取初始位点。因此，即使没有业务 WAL，控制进度仍可 checkpoint。只读外部信号不需要信号表；可写外部信号仍需要信号表承载 `insert_insert` watermark。

`ConnectorRuntime::signal_sender()` 在 runtime 所有权移入 `run` 前暴露可 clone、强类型且有界的 `SignalSender`。PostgreSQL 只在没有活动 WAL 事务时消费 command，并让 file、in-process 和 Kafka record 共用同一 action controller 和 checkpoint 路径。只有启用 `in-process` 时，CLI 才将 sender 接入 `POST /v1/connector/signals`。该路由要求启用管理变更，命令入队后返回 `202`，禁用变更端点或 channel 时分别返回 `403` 或 `409`。

Debezium 的 `jmx` signal channel 是把记录写入内存队列的 JVM MXBean。为迁移 properties，Rustium 将 `signal.enabled.channels=jmx` 映射到同一个有界 `in-process` channel，发出兼容警告，并通过嵌入式 `SignalSender` 与 HTTP 管理路由暴露。这样保留队列和 action 语义，但不会声称兼容 JVM/RMI 协议；`jmx,in-process` 会去重为一个 channel。

`rustium-signal-kafka` 实现 `signal.kafka.topic`、`signal.kafka.groupId`、`signal.kafka.bootstrap.servers`、`signal.kafka.poll.timeout.ms` 和去掉前缀后的 `signal.consumer.*` 透传参数。Topic 默认为 `<topic.prefix>-signal`，必须恰好只有一个 partition，并过滤 key 不等于 `topic.prefix` 的 record。自动 commit 和自动 offset store 会被强制关闭。有效 record 通过 `SignalSender::send_and_wait` 投递；runtime 只有在完成 Sink 投递、SQLite checkpoint 持久化和 Source 确认后才释放确认，随后 Kafka channel 同步提交 offset + 1。无效 record 或其他 connector key 的 record 会被跳过并提交。如果在 connector checkpoint 后、Kafka commit 前崩溃，record 可能重放，因此活动中或近期已完成的同一 `execute-snapshot` ID 会被幂等处理。

控制器当前实现 `incremental.snapshot.watermarking.strategy=insert_insert`。对于每个按 key 排序的 chunk，它先提交 open watermark，在首个 chunk 捕获当前最大 key，通过共享文本转换器读取不超过 `incremental.snapshot.chunk.size` 行，再提交 close watermark。默认 key 为表主键。surrogate key 必须是 `NOT NULL`，且具有有效、非 partial 的单列唯一索引；它只替代 chunk 边界和排序。主键仍为必需，并作为去重窗口 key。两个 watermark 之间的 WAL create、update 和 delete 会按主键从该窗口移除对应行。close 时剩余行在 close commit 边界之前作为 read event 发出，并带 Debezium `incremental` snapshot marker。

当 `read.only=true` 时，chunk 连接不会插入 watermark 行。它为每个 chunk 分配一次 transaction ID，在有界查询前后捕获 `pg_current_snapshot()`，并保留快照的 `xmin`、`xmax` 和进行中 XID 集合。WAL transaction ID 在 low `xmin` 打开窗口；只有 high watermark 可见或当时仍进行中的最大事务已经提交后才关闭窗口。关闭窗口的 commit event 同时作为 checkpoint 边界。如果水位表明没有更旧事务，同一个 transaction ID 可以安全地立即关闭后续 chunk。重启时丢弃瞬时水位并重新读取当前主键范围。

Connector-state version 6 保存 signal ID、展开后的集合、每集合 condition、surrogate key、集合索引、last key、maximum key、chunk sequence、pause 状态、最近 1,024 个已完成或已停止 execute signal ID 的有界历史，以及 opaque unknown-type 列。close commit 将推进后的状态与已投递行原子 checkpoint。若在此之前崩溃，会重新读取同一个有界 chunk，可能重复但不会产生缺口；若在此之后重启，则从下一 key 开始。Version 1 到 5 schema-history payload 会使用默认值和 catalog normalization 继续读取。重放已完成历史中的 signal 时只确认而不会再次启动快照。内存窗口有意不持久化。

`pause-snapshot` 在当前 close 边界后阻止准备下一 chunk。pause 标记会被 checkpoint，因此重启后仍保持暂停。`resume-snapshot` 在自身 signal 事务提交后安排下一 chunk。`stop-snapshot` 在没有 `data-collections` 时清除全部进度，否则只移除完整匹配表达式选中的集合；停止当前集合会重置其主键边界，并在控制事务之后安全推进。未知或乱序 watermark ID 会被忽略。

当 `incremental.snapshot.allow.schema.changes=false` 时，chunk 连接会在 open watermark 后立即重新发现表，并在构造 SQL 前比较字段和 PostgreSQL 类型身份。流式阶段若活动表出现变化后的 `Relation` 也会被拒绝。任一保护都会在旧窗口按新布局解码前停止 Source，并保持最后已确认 checkpoint 不变。Debezium 明确记录 PostgreSQL 不支持增量快照期间的 schema change；因此 Rustium 会拒绝 `incremental.snapshot.allow.schema.changes=true`，不会声称不安全的兼容性。

重放历史 `Relation` 时，如果当前 catalog 发现失败，Rustium 会保留 WAL 提供的名称、顺序、OID、typmod 和 key 标志。类型名解析会先复用 checkpoint 中完全匹配的列身份；catalog 与历史都无法解析时，字段使用保守的 `unknown_oid_*` 名称。两条回退路径都会发出可观测 warning，在不伪造 nullable/default 元数据的前提下保持解码顺序。

#### 9.5 PostgreSQL 扩展 fixture

TLS 门槛使用生成的 CA 与带 `DNS:localhost` 的服务端证书启动 PostgreSQL。它要求普通 validation 与 logical replication 在 `verify-full` 下成功，证明 active replication backend 使用 TLS，并要求错误 CA 与 hostname mismatch connection 失败。未知类型门槛使用真实 composite type，证明默认省略字段，以及启用包含后 snapshot/WAL 输出 byte-identical。

Schema-refresh 门槛会证明真实 TOAST chunk 存在，在默认和 `FULL` replica identity 下运行两种 Debezium mode，重复只更新非 TOAST 列，并要求 schema version 和 connector state 保持稳定，同时分别输出 `Unavailable` 或恢复的 before value。

该 fixture 支持远程 Docker daemon，并且不会暴露其临时 PostgreSQL 端口。Docker command 使用当前选中的 Docker context。设置 `RUSTIUM_POSTGRES_DOCKER_SSH_HOST` 后，容器仍只把 PostgreSQL 发布到 daemon 主机的 loopback，脚本会建立经过认证的本地 SSH forward；`RUSTIUM_POSTGRES_DOCKER_SSH_LOCAL_PORT` 默认值为 55433。`RUSTIUM_POSTGRES_EXTENSION_BASE_IMAGE` 可选择可信 registry mirror 或预加载 PostgreSQL 镜像，默认值仍是 CI 使用的官方 `postgres:<version>` 镜像。

`scripts/test-postgresql-extensions.sh` 使用仓库 Dockerfile 构建 PostgreSQL 17 镜像，安装同主版本 pgvector 和 PostGIS 包，启用逻辑复制，并执行二十二个强制门槛。就绪条件要求最终 PostgreSQL postmaster 成为容器 PID 1 且查询成功，因此 entrypoint 的临时初始化 server 无法误通过门槛。类型门槛要求 vector、halfvec、sparsevec、geometry、geography 存在且在快照/WAL 路径上一致。MONEY 门槛配置 `money.fraction.digits=1`，要求 field scale 及正负 `HALF_UP` 结果在 exported snapshot 与 WAL 中一致。恢复门槛使用容量为 1 的 Source 输出，提交多行事务直到有界 channel 填满，终止活动 replication backend，在原连接断开期间再提交事务，并要求新 backend 保留全部预期记录及首次出现的源端顺序。snapshot filter 门槛会选择两张 publication 表用于 streaming、只快照其中一张，并要求之后仍收到另一张表的 create event。publication 门槛覆盖 publication 缺失失败、`FOR ALL TABLES` 创建、filtered 创建与替换、filtered 冲突拒绝，以及空 publication 启动后动态加表并捕获首行。replica identity 门槛验证四种 catalog mode、真实 `FULL` 与非首列 `INDEX` WAL before image、重叠规则拒绝，以及冲突时事务化不修改。partition root 门槛验证 publication metadata、跨两个 leaf partition 的 root 归属 snapshot/WAL record，以及既有 publication mismatch 拒绝。failover slot 门槛要求 PostgreSQL 17 主库，验证 `pg_replication_slots.failover`，并要求该 slot 同时完成 exported snapshot 与 WAL delivery。slot lifecycle 门槛证明有序停止默认保留 inactive slot，只有 `slot.drop.on.stop=true` 才删除它。snapshot lock 门槛证明后续尚未扫描的表也会被确定性地提前获取 `ACCESS SHARE`、并发 DDL 被阻止、快照后 lock 被释放、获取失败受 timeout 限制，并且全部 lock 成功前零行输出。isolation 门槛会运行四种 Debezium mode，并要求收到预期 snapshot row 以及 snapshot 后第一条 WAL create event。logical message 门槛通过 `pg_logical_emit_message` 验证被过滤非事务消息的 checkpoint 推进、原始二进制 content、事务身份、message/row 顺序和 commit boundary。feedback 门槛关闭 TCP keepalive，使用非默认 25 ms status interval，确认一个 commit，并要求 `confirmed_flush_lsn` 在三秒内达到该持久位点。offset mismatch 门槛同时制造 checkpoint 超前和 slot 超前状态，验证 `trust_offset` 与 `trust_greater_lsn` 推进 slot，要求 `trust_offset` 拒绝超前 slot，并证明 `trust_slot` 跳过 slot 之前的记录且 checkpoint 刷新后的 schema state。origin filter 门槛创建真实 PostgreSQL replication origin，证明 `origin=none` 排除该事务而 `origin=any` 会发出该事务。initial statement 门槛会保持背压 snapshot connection 存活并验证其 session setting，证明 active replication backend 没有继承这些设置，再通过 heartbeat action connection 观察相同设置。LSN flush 门槛证明 manual mode 会忽略 durable acknowledgement、driver mode 会 flush 未确认的 published record，且一秒 server keepalive 只在 driver mode flush 未监控 WAL。interval 门槛通过 role-level setting 覆盖 PostgreSQL 四种 `IntervalStyle`，并验证两种 Debezium mode、scalar、array、exported snapshot 和 WAL。CI 执行 3 轮恢复循环；`RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES=1..1000` 可提高长时间运行的循环数。独立的 `postgresql-cdc` GitHub CI job 会在每次 push 和 pull request 时运行全部二十二项门槛。

XMIN 门槛会创建相互独立的启用及禁用 logical slot，要求 60 秒 fetch 周期内的两个事务携带相同的正数缓存 `catalog_xmin`，同时默认零值不得输出 XMIN metadata。Format golden 门槛还会分别要求 PostgreSQL JSON、Avro、Protobuf source contract 包含该可选字段。

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

当前基于 `sqlparser` 的 MySQL DDL 路径支持 `CREATE TABLE`、`ALTER TABLE` 增删/重命名/修改/变更列、主键变更、`DROP TABLE`、`RENAME TABLE`，以及 schema 不变的 `TRUNCATE TABLE`、索引新增/删除和列默认值修改。由于 `sqlparser` 0.62 尚不能解析 MySQL 的独立 `ALTER TABLE ... RENAME INDEX ... TO ...` 形式，Rustium 会使用 SQL tokenizer 严格识别该语句；混合操作仍保持 fail closed。默认情况下，解析或状态应用失败会停止连接器。`schema.history.internal.skip.unparseable.ddl=true` 与 Debezium 的显式跳过行为一致，并记录元数据风险警告。

当 `connect.keep.alive=true` 时，结束或失败的 binlog stream 会从最后一个成功送入有界运行时队列的 SourceRecord 重新打开。回卷过程恢复 binlog 文件名、table-map 锚点、GTID 事务计数、行序号和重放过滤器，从而确定性跳过已完成记录。CLI 恢复使用共享的 Debezium 兼容 `errors.max.retries`、`errors.retry.delay.initial.ms` 和 `errors.retry.delay.max.ms` 策略，以及可被取消的指数退避。`0` 表示立即失败，`-1` 取消重试次数上限，产生新源端进度后预算和等待时间都会重置。为迁移 `.properties`，只有在对应 `errors.*` 缺失时，`rustium.source.reconnect.max.attempts` 和 `connect.keep.alive.interval.ms` 才作为回退。直接嵌入且未调用 `with_retry_policy` 时继续使用这些 source-config 值。

当 `snapshot.mode=when_needed` 且已完成 checkpoint 对应的 binlog 文件已经不再保留时，Rustium 会回退到新的 initial snapshot；普通 `initial` 模式会在源位点不可用时继续失败。

`heartbeat.interval.ms` 默认为零，即不发送可见 heartbeat。设置为正数后，从最新安全 streaming 位点发送 heartbeat，使其 Sink 确认和 checkpoint 遵循正常 at-least-once 顺序，同时不虚构 binlog 进度。Debezium JSON 使用 `serverName` key 和 `ts_ms` value。topic 优先使用 `topic.heartbeat.name`，否则将 `topic.heartbeat.prefix`（或旧参数 `heartbeat.topics.prefix`）与 `topic.prefix` 连接。

`heartbeat.action.query` 可选地在每个正 heartbeat 周期先通过独立普通 MySQL 连接执行。查询失败会携带数据库错误停止 Source；查询产生的变化只有在复制流实际读到后才算源进度。

当 MySQL 在 `binlog_row_value_options=PARTIAL_JSON` 下发送 `JsonDiff` 时，Rustium 会把 replace、insert、remove path 应用到完整 before-image JSON，并发出重建后的 after image。如果 before image 或 path 不完整，则将该字段标记为 `Unavailable`，不会伪造值。

`database.connectionTimeZone` 映射到原生 `source.connection_time_zone`，默认值为 `UTC`。每个普通 MySQL 连接都会把允许的 `UTC`、`Z`、`Etc/UTC` 或 `+00:00` 规范化为 `+00:00` session time zone，使 `TIMESTAMP` 快照值与 UTC binlog 值一致。其他偏移量、地区名称以及 Debezium 的 `SERVER` 模式会在校验阶段失败，直到两条捕获路径实现完全一致的时间转换。Binlog ENUM 序号和 SET 位掩码会根据已捕获列定义还原。MySQL 空间值保留原生 SRID 加 WKB 字节，所有 OGC 列类型都会从无主键快照排序字段中排除。外部类型矩阵要求 boolean、有符号/无符号整数、decimal、float/double、bit/binary、date/time/datetime/timestamp、string/text、JSON、ENUM、SET、null、`GEOMETRY`、`POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON` 和 `GEOMETRYCOLLECTION` 在快照/binlog 路径上完全一致。

PostgreSQL、MySQL 和 SQL Server 通过共享的 `rustium-column-transform` engine 实现 Debezium 兼容列转换。动态参数 `column.truncate.to.<length>.chars`、`column.mask.with.<length>.chars`、`column.mask.hash.<algorithm>.with.salt.<salt>` 和 `column.mask.hash.v2.<algorithm>.with.salt.<salt>` 会编译为 anchored、大小写不敏感的 selector。PostgreSQL selector 使用 `schema.table.column`，MySQL 使用 `database.table.column`，SQL Server 同时接受 `database.schema.table.column` 与 `schema.table.column`。确定性的优先级为 truncate、固定 mask、hash V1、hash V2。固定 mask 替换 SQL NULL，hash 保留 NULL；hash 输出为小写十六进制，并在存在声明字符长度时截短，包括 SQL Server 的 `nchar(n)` 和 `nvarchar(n)`。V1 在计算 `salt || bytes` 前复现 Java `ObjectOutputStream` String serialization；V2 计算 `salt || UTF-8(value)`。转换在 snapshot/incremental keyset bookkeeping 之后、事件发出之前执行，因此主键进度和并发更新去重始终使用原始值。Salt 不会写入 semantic fingerprint，只保留其 SHA-256 digest。

MySQL 同时支持 Debezium 兼容的源表、文件、有界进程内和 Kafka signal channel。`signal.data.collection` 指定一个按顺序且仅包含三个文本兼容列 `id`、`type`、`data` 的 `database.table`；Rustium 从 binlog 读取源表插入，将 signal table 从业务快照和事件中排除，并且不会向该表写 watermark。文件每行一个 JSON envelope，与 `SignalSender` 使用同一格式。`execute-snapshot` 会展开完整匹配的集合表达式，要求每张目标表具有主键，为每个集合固定最大主键，并使用带类型的单列或复合主键比较推进。它发出 `source.snapshot=true` 和 `rustium.snapshot.kind=incremental`，并在 connector state 中保存当前主键、最大主键、集合、chunk 序号和暂停状态。Version 3 还保留有界的已完成或已停止 execute signal ID 历史，使重启以及 connector checkpoint/Kafka offset 崩溃窗口内的重放保持幂等。Version 2 的 offset 进度仍可读取，但会从当前集合开头安全重读，不会冒跳行风险。

事件循环每轮最多安排一个增量 chunk，并且只在没有活动 binlog 事务时执行。每次查询都会捕获低/高 binlog 坐标；Rustium 先缓存 chunk，在复制流追到高水位期间正常发出 CDC record，并移除 `(low, high]` 内 create、update、delete 的 before/after image 中出现的主键。只有剩余行会在 chunk commit 前发出，commit 同时原子推进带类型的 keyset 状态。因此 binlog record、pause/resume/stop 控制和外部信号确认都可以在 chunk 之间被处理。Schema history 更新会保留活动增量状态，不会覆盖它。内存窗口不会进入 checkpoint：重连会丢弃窗口并重读未提交 key 范围；窗口打开期间观察到 schema change 时，会在发出布局不匹配的 read 之前失败。Kafka 复用单 partition、与 checkpoint 绑定的 `rustium-signal-kafka` 实现及 Debezium topic/key 合约。

#### 10.4 TLS 模式

- `disabled`：仅明文。
- `preferred`：先尝试加密，失败后回退明文。
- `required`：加密，但不校验 CA 或主机名。
- `verify_ca`：加密并校验 CA，不校验主机名。
- `verify_identity`：加密并同时校验 CA 和主机名。

`database.ssl.ca` 指定 Rustls 使用的 PEM/DER CA 文件；`database.ssl.cert` 与 `database.ssl.key` 指定 PEM/DER 客户端证书和私钥，并且必须成对配置。Debezium 兼容参数 `database.ssl.truststore`/`database.ssl.truststore.password` 与 `database.ssl.keystore`/`database.ssl.keystore.password` 接受按内容识别的 JKS 或 PKCS#12/PFX 存储。PKCS#12 解码同时支持现代 PBES2/AES/SHA-256 和旧式 PBES1 文件。证书与 PKCS#8 私钥会直接转换成内存中的 Rustls 输入。keystore 必须且只能包含一个带证书链的私钥条目；空 truststore 会校验失败。PEM CA 与 truststore 互斥，PEM 客户端身份与 keystore 互斥。存储路径会进入连接器指纹，存储密码不会进入指纹。

#### 10.5 已验证恢复

MySQL 8.4 Docker 门槛覆盖：

- 两行快照和快照完成；
- 只过滤快照集合且不缩小已选择的 binlog stream；
- 一个包含多行 insert、update、delete 的事务；
- 事务顺序 1 到 5；
- 默认执行 3 轮容量为 1 的 Source 输出背压循环；
- 每轮强制终止活动 binlog dump session，并要求替换连接 identity；
- 从最后完成的 table-map/commit 锚点重连，保留全部预期记录和首次出现的源端顺序；
- 在旧 schema 行之前停止并建立 checkpoint，随后执行破坏性删列/加列 DDL，再写入新 schema 行；
- 当数据库已经暴露最终 schema 后重启，正确解码旧 schema 行、checkpoint DDL 状态，并解码新 schema 行。

`RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES=1..1000` 可提高循环数。该门槛使用动态分配的主机端口、带超时的 Docker 清理，并作为必选 `mysql-cdc` GitHub Actions job 在每次 push 和 pull request 时运行。

单元门槛覆盖 checkpoint/state 原子性、version 1/2/3 checkpoint 兼容、schema-history 序列化、增量 keyset 进度/控制、已完成 signal 重放、PostgreSQL snapshot/file/in-process 信号解析、MySQL signal 解析、MySQL 共享重试边界和旧参数回退映射、持久 runtime 信号确认、管理端点门槛、重放状态回卷、标量与扩展类型转换、heartbeat 编码、选表隔离、create/alter/drop/rename DDL 应用，以及 JKS 与现代 PKCS#12 TLS 存储转换和失败处理。librdkafka MockCluster 门槛会验证 Kafka key 过滤、单 partition 消费，以及持久信号确认前不提交 offset。外部 PostgreSQL 17 门槛验证强制终止 replication backend 后自动恢复、checkpoint 对应 slot 丢失后的显式失败、周期 heartbeat、成功执行 `heartbeat.action.query`、heartbeat 表过滤、source/file/in-process 信号分块、外部信号即时 checkpoint、完全无信号表的 file 和 in-process 只读信号、checkpoint 重启、additional condition、并发更新去重、pause/resume/scoped-stop 控制、保持更新事务时的只读事务水位、受限表权限、零连接器 watermark 写入、唯一 surrogate-key 排序、完成状态清理、信号表隔离，以及 hstore、domain、enum、tsvector 的快照/WAL 一致性。可选 superuser fixture 会临时限制 `max_slot_wal_keep_size`、让 slot 进入 `wal_status=lost`、验证 fail-closed 恢复并还原原设置，必须在隔离的 PostgreSQL 实例上运行。必选 Docker/CI fixture 会在 PostgreSQL 17 上验证 vector/halfvec/sparsevec 与 PostGIS geometry/geography。外部 MySQL 8.4 门槛还会验证容量为 1 的背压/重连循环及连接 identity 替换、空闲 stream 周期 heartbeat、精确 server UUID 的 GTID 启动、checkpoint 恢复（包含 schema-invariant 索引/默认值操作的破坏性 DDL 恢复）、删除/插入后的带类型 keyset 重启、已完成 signal ID 持久化、chunk 窗口并发去重、核心加 OGC 空间类型的快照/binlog 类型矩阵，以及 connector checkpoint/offset commit 崩溃窗口中的真实 broker Kafka signal 重放。

### 11. SQL Server 连接器

SQL Server 连接器使用 SQL Server CDC 实现，不轮询业务表。当前每个连接器只接受一个数据库、每张选表只接受一个活动 capture instance，并要求 `data.query.mode=direct`。

已映射的 Debezium 兼容输入包括 `database.hostname`、`database.port`、`database.user`、`database.password`、`database.names`、`database.encrypt`、`database.trustServerCertificate`、`table.include.list`、`table.exclude.list`、`snapshot.mode`、`snapshot.isolation.mode`、`data.query.mode`、`streaming.fetch.size`、`max.queue.size`、`max.batch.size`、`poll.interval.ms`，以及动态列 truncate/mask/hash 参数。

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

暂时性 polling 故障使用共享的 Debezium 兼容 `errors.*` 策略。Rustium 会在改变带类型 cursor 之前完整物化每个 max-LSN、retention 和 direct change-table 查询。因此 I/O 或选定的暂时性 SQL Server 失败会丢弃故障 client、重新连接，并从未变化的 commit/change LSN 重新轮询。取消信号可以中断退避；`0` 关闭自动恢复，`-1` 允许无限重试。权限、协议、转换和 CDC retention 失败不会重试，并继续 fail-closed。

当 `snapshot.mode=when_needed` 且 checkpoint 已早于 CDC cleanup 时，Rustium 会在恢复 polling 前用新的 consistent snapshot 替换它。其他 snapshot mode 在所需 CDC 历史已删除时保持 fail-closed。

快照查询会先使用与 CDC change-table 查询相同的逐列 SQL 投影，再进入共享 Rust converter，从而消除 driver 在 money、小数时间精度、UUID、binary、text 和特殊值上的差异。有主键时快照按主键排序。外部类型矩阵要求 bit、整数、decimal/money、real/float、UUID、varbinary、date、time、datetime2、datetimeoffset、Unicode text、XML、hierarchyid、geometry 和 geography 在快照与 CDC 路径上产生完全相同的 `DataValue`。Geometry 和 geography 使用 SQL Server 原生 `Serialize()` 表示并保留为完整二进制 payload，而不是有损的 `ToString()` 输出；hierarchyid 使用规范路径，XML 使用规范文本。

SQL Server 在转换完成后对 initial snapshot row、已配对的 CDC before/after image 和 incremental snapshot row 执行列转换。四段 selector 包含已配置 database；三段 selector 用于兼容按 `schema.table.column` 限定的 Debezium 配置。Incremental key 提取与 CDC window 去重先于转换执行，从而保留原始 key 排序。Signal-table CDC row 会在解析内部 command 和 watermark ID 前刻意跳过转换，避免匹配的 mask 规则破坏连接器控制流。SQL Server 2022 外部门槛覆盖 snapshot/CDC 上的四段 truncate 规则，以及 source signal 触发 incremental snapshot 上的三段固定 mask。

SQL Server 支持 Debezium 兼容的 source table、file、有界 in-process 和 Kafka signal channel。任何 channel 的增量快照都要求可写 source signal table；它可以使用 `schema.table` 或 `database.schema.table`，必须按顺序且仅包含文本兼容的 `id`、`type`、`data` 列，`id` 至少容纳 42 个字符，并具有独立活动 CDC capture instance。Source 校验还会检查对象级 `INSERT` 权限。即使业务 table filter 排除了它，Rustium 也会发现该 capture instance、将用户 create operation 作为 command 消费、插入内部 open/close watermark，并从业务输出中抑制所有 signal-table 行。JMX 配置会映射到有界 in-process channel，并发出明确兼容 warning。

`execute-snapshot` 会展开完整匹配的短名称或 database-qualified 集合表达式，要求主键、固定最大主键，并推进带类型的单列/复合 keyset。Connector state version 1 持久化集合进度、additional condition、当前/最大 key、chunk 序号、pause 状态，以及有界的已完成或已停止 signal ID 历史。事件循环每轮只在完整 CDC commit 边界执行一个 chunk。Rustium 插入唯一 open watermark，并等待其 CDC create event 后才查询 chunk；随后插入不同 ID 的 close watermark，在正常发出 CDC record 的同时根据 before/after 主键从缓存中移除对应行，再次校验源表 schema，并在 close watermark commit 处发出剩余 read。合成 chunk commit 会原子推进 keyset 状态；source-table 控制在自身 CDC commit 上 checkpoint，Kafka offset 继续与 runtime acknowledgement 绑定。内存中的 opening/open window 不持久化，重启后会重新创建。`pause-snapshot`、`resume-snapshot` 和 scoped `stop-snapshot` 在重启后保持幂等。

多个数据库名称需要显式的分区感知排序和 checkpoint 所有权。在该契约经过测试前，Rustium 会直接拒绝。外部门槛已在 SQL Server 2022 Developer RTM-CU25 上验证快照切换、fetch size 为 1 的 update 配对、保持事务序号的事务中间重放、3 轮容量为 1 的 polling-session 终止循环及不同连接 identity、全部预期记录和首次出现的源端顺序、并发 commit 排序、retention fail-closed、heartbeat/action-query、核心和扩展类型矩阵、四段 snapshot/CDC 与三段 incremental-snapshot 列转换、signal command 原值隔离、in-process keyset 重启、带 additional condition 的 source-table signaling、CDC window 并发更新去重、connector checkpoint/Kafka offset 崩溃窗口中的真实 broker 重放和资源清理。必须通过的 GitHub Actions 门槛会使用动态分配的主机端口启动当前 SQL Server 2022 Linux 镜像和 SQL Server Agent，启用 CDC，执行相同的 3 轮背压/重连契约，验证只过滤快照集合而不缩小 streaming，并检查快照/CDC 事务顺序、四段列转换以及 XML、hierarchyid、geometry 和 geography 一致性。`RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES=1..1000` 可提高循环数。

#### 11.1 Oracle LogMiner Source

Oracle 使用纯 Rust `oracle-rs` 驱动和 LogMiner 在线字典。校验要求 ARCHIVELOG、minimum supplemental logging、选表目录可见，并要求 redo 保留覆盖 checkpoint SCN。initial snapshot 捕获 `CURRENT_SCN`，通过 `AS OF SCN` 查询选表；streaming 使用 `DICT_FROM_ONLINE_CATALOG` 和 `COMMITTED_DATA_ONLY` 启动 `DBMS_LOGMNR`，再轮询 `V$LOGMNR_CONTENTS` 中已提交的行。

持久位点为 `(scn, commit_scn, transaction_id, event_serial, snapshot)`。只有能够无歧义识别列与 scalar literal 的 LogMiner SQL 才会解析；不支持的 expression 保留文本。Commit row 形成 `TransactionCommit` boundary。redo dictionary 尚未实现时会拒绝 `redo_log_catalog`，保持 fail-closed。

#### 11.2 MongoDB Change Streams Source

MongoDB 使用官方 Rust 驱动。连接器在 initial snapshot 前打开 Change Stream 并记录 `operationTime`，随后从 checkpoint 的不透明 token 或 operation-time anchor 恢复，避免 snapshot 并发变更被跳过。

持久位点为 `(resume_token, cluster_time_seconds, cluster_time_increment, event_serial, snapshot)`。BSON document 递归映射为 Rustium typed value，`_id` 是 event key。`update_lookup` 提供更新后的 document；服务端启用后，`when_available` 或 `required` 提供 pre-image。MongoDB 必须运行 replica set 或 sharded cluster。

#### 11.3 Debezium Engine Bridge Source

MariaDB、Db2、Cassandra 3/4/5、Vitess、Spanner、Informix、CockroachDB 和 YashanDB 使用 `rustium-debezium`。Adapter 负责 Rustium 侧排序、typed normalization、Sink 投递和 checkpoint 确认；upstream Debezium engine 负责数据库特有 CDC 协议。该边界用于正确处理 Db2/Informix vendor client、Cassandra 节点本地 commit log、Vitess VStream、Spanner partition lifecycle、CockroachDB enriched changefeed 和 YashanDB YStream。

HTTP 模式是同步持久性边界：每个 upstream request 先被解析并送入有界 runtime，完成 Sink 投递、`SourcePosition::Debezium` checkpoint 与确认后才返回 HTTP 204。超时返回失败以便 Debezium 重试。Managed 模式会生成 mode 0600 的 Debezium Server 配置，强制使用 unbatched schema-free JSON 发往私有 Rustium endpoint，支持 `{config}`/`{endpoint}` command placeholder，并在关闭时删除配置文件。External 模式只提供 endpoint，不启动进程。

Kafka 模式固定 `enable.auto.commit=false` 与 `enable.auto.offset.store=false`，逐条处理，并只在 Rustium 确认后同步提交输入 topic/partition offset。Regex subscription 支持 Cassandra 逐表 topic。Cassandra 仍要求每个数据库节点运行独立 Debezium process；Rustium 不会把远程 CQL 查询冒充节点本地 commit-log CDC。

持久 bridge 位点为 `(connector, source_object, record_id, event_serial, snapshot)`。`source_object` 完整保存 connector 特有的 Debezium source metadata，包括 Db2/Informix LSN、Cassandra file/position、Vitess VGTID、Spanner partition token、CockroachDB resolved timestamp 或 YashanDB SCN/LSN。`record_id` 优先使用 upstream CloudEvent ID；Kafka 模式使用 topic/partition/offset；缺少 ID 的 HTTP 事件使用确定性 SHA-256 content ID。最后一条已确认 HTTP/Kafka record 的精确重放会在再次投递 Sink 前去除。

Bridge parser 接受原始 Debezium envelope、schema/payload envelope、structured CloudEvent 和 HTTP batch，并把 data operation、snapshot-last、transaction-end、heartbeat 与 schema-change record 映射到共享 runtime。原生格式保留 typed `DebeziumPosition`；兼容格式用 `source_offset` JSON、`record_id` 与 `event_serial_no` 无损携带 source object。

### 12. 格式与 Sink

原生 JSON Encoder 暴露完整强类型源位点和事件 schema。Debezium JSON Encoder 输出 `before`、`after`、`source`、`op`、`ts_ms`、事务元数据和连接器特定位点字段。`debezium_json_schema` 输出相同 JSON 值，同时为每个 key/value schema 附加不可变 Draft-07 descriptor。`debezium_avro` 构造命名 key/envelope/source/transaction/row record，将动态值解析到 Apache Avro schema，并输出原始 binary datum。`debezium_protobuf` 为每个 subject 构造一个 top-level `Key` 或 `Envelope` message，并为 row 值使用生成的带类型 oneof wrapper。表 DDL 变化通过 `EventSchema.version` 改变 value descriptor；heartbeat record 使用稳定的独立 schema。已解析 Avro/Protobuf schema 和成功 Registry ID 分别使用独立的有界 LRU。

Avro 名称会被确定性调整为 `[A-Za-z_][A-Za-z0-9_]*`。两个数据库字段调整为相同名称时会在投递前拒绝。有符号整数、浮点、boolean、binary、array 和 hstore/map 保留原生 Avro category。无符号 64 位整数、decimal、时间值、UUID、JSON 和其他未建模扩展值使用稳定字符串，因为当前强类型事件契约不携带无损 Avro logical type 所需的 precision/元数据。超出 Avro 有符号 `long` 范围的值会失败，不会 wrap 或 saturate。

Protobuf row wrapper 可区分原生值、通过 wrapper 缺失表达的 null、unavailable placeholder 和文本转换 fallback。Array、多维 array、map、binary 和完整无符号 64 位范围都保留无损 Protobuf 表示。Field number 从原始数据库字段名确定性派生，在重启、字段重排和增量演进后保持稳定、避开 Protobuf 保留区间，并在活动冲突时失败。生成的 `.proto` 会在投递前解析，dynamic message 使用 `prost-reflect` 编码。

`tombstones.on.delete=true` 是 Debezium 兼容默认值。此时一条 delete 会在同一投递批次中编码为有序的两条记录：先发送 delete envelope，再发送 key 相同、payload 为 null 的 tombstone。tombstone 使用独立的确定性派生事件 ID；两条记录共同受 Sink 成功、checkpoint 持久化和 Source 确认约束。原生 YAML 使用 `format.tombstones_on_delete`；原生 Rustium JSON 格式不产生 tombstone。

仓库内置 golden fixture，使用固定 source position 和 timestamp 固定 PostgreSQL、MySQL 与 SQL Server 的完整 Debezium JSON destination、key 和 envelope。配套 JSON Schema、Avro 和 Protobuf fixture 还会固定每个优先连接器的 destination、key/value subject、schema type 以及完整 key/value 定义。基于 JSON 的定义按结构化值比较，Protobuf 定义按确定性源码文本比较。因此，具有语义影响的元数据、routing、source 字段、field number 或 wire schema 变化都必须显式更新 fixture，而无关的 JSON 对象字段顺序不会造成失败。每个 format crate 还提供一个显式 ignored 的再生成测试，用于受控更新 fixture。

stdout 是 best-effort，仅用于开发，并将 tombstone 输出为一行 `null`。Kafka 使用 `librdkafka`，将 tombstone 发送为真正的 null value，并要求 `acks=all` 或 `-1`，使 `write` 只在副本确认后返回。Producer 幂等始终启用。Rustium 拥有 bootstrap、确认、压缩、幂等和投递超时参数，透传属性不能替换这些持久性设置。Producer 幂等并不会把 Source 到 Kafka 变为 exactly-once。

带 schema 的事件只允许投递到 Kafka。发送事件前，Sink 会通过 Confluent Schema Registry API 注册不同的 `<topic>-key` 和 `<topic>-value` 定义，只把成功 ID 放入可配置的有界 LRU，并在每个 datum 前加 magic byte `0` 和四字节大端 ID。Protobuf 会在编码 message 前增加 Confluent 优化的 top-level message-index byte `0`。注册在 broker 投递前完成，因此 registry 故障不会 checkpoint 或发送该事件。网络和暂时性 registry 故障使用共享 Sink 策略重试；兼容性与 schema 拒绝会 fail-closed。该路径兼容 Confluent Schema Registry 和 Apicurio 的 Confluent compatibility API。当前 key/value registry 端点与凭据必须一致，并且只接受自动注册的 `TopicNameStrategy`。

共享重试策略映射 Debezium 的 `errors.max.retries`、`errors.retry.delay.initial.ms` 和 `errors.retry.delay.max.ms`，原生等价项位于 `runtime` 下；Rustium 的有限默认值为重试 10 次、初始等待 300 ms、上限 10 s。PostgreSQL 使用该策略处理暂时性复制恢复，MySQL 使用它处理失败的 binlog stream 恢复，SQL Server 使用它处理暂时性 direct-CDC polling 恢复，runtime 只对 Sink 明确分类为可重试的失败使用它。每次尝试后的退避时间倍增，取消信号可以中断等待。Sink 投递会保留并重放同一批次，成功前不写 checkpoint；在投递退避期间取消会让该 batch 保持未确认以供重启，并完成正常 runtime 清理且不增加失败事件。这提供不会跳过源位点的 at-least-once 进度，但部分 Sink 投递成功的尝试仍可能产生重复。各 Source 继续负责重连和 cursor 重建，因为恢复依赖连接器特有的持久状态。

### 13. 控制平面与可观测性

生命周期状态包括 created、starting、snapshotting、streaming、paused、failed、stopping 和 stopped。当前 API：

| 端点 | 用途 |
|---|---|
| `GET /health/live` | 进程存活 |
| `GET /health/ready` | 连接器就绪 |
| `GET /v1/connector/status` | 生命周期/原因、位点、checkpoint 与源事件时间、lag、队列、计数 |
| `POST /v1/connector/stop` | 启用后优雅停止 |
| `POST /v1/connector/signals` | 启用变更端点和 channel 时有界提交 in-process 信号 |
| `GET /metrics` | Prometheus 指标 |

当前指标包括连接器状态、已投递事件、失败事件、流水线队列深度、`rustium_sink_retry_attempts`、`rustium_source_lag_seconds`、`rustium_checkpoint_age_seconds`、`rustium_last_event_age_seconds` 和 `rustium_connector_state_age_seconds`。Lag 根据当前时间与最后一个已持久 checkpoint 的源时间戳动态计算；没有对应时间戳时 age 指标为 `NaN`。所有当前指标都是低基数，适合 Prometheus 抓取。编码失败计一个源事件，重试耗尽或不可重试的 Sink 写入失败则统计该批次全部已编码事件。

### 14. 错误、安全与资源策略

- 配置和能力错误在捕获前失败。
- 未知协议/数据错误停止连接器。
- 可重试的 PostgreSQL、MySQL、SQL Server Source 操作和可重试 Sink 操作使用共享的、默认有限的重试预算和指数退避。
- Source 重连仍由连接器负责；PostgreSQL、MySQL 和 SQL Server 都已有经过测试的连接器特有恢复。
- 队列有界，阻塞或重试中的 Sink 会将背压传回 Source 输出。
- 数据库、Kafka 和 Schema Registry TLS 由配置控制。
- 管理 Server 默认绑定 loopback。
- 变更型 HTTP 端点默认禁用。
- Secret 在加载时插值，不进入状态和语义指纹。

安全响应和部署基线以 [SECURITY.md](../SECURITY.md) 为规范；备份、恢复、告警和事故流程以 [docs/runbook.md](runbook.md) 为规范；状态/配置兼容性及 rollback 规则以 [docs/upgrades.md](upgrades.md) 为规范。

#### 14.1 容器与 Kubernetes 打包

仓库提供多阶段 `Dockerfile` 和 `deploy/helm/rustium` Chart。运行时镜像只包含 Rustium 二进制及 Kafka/TLS client 所需的原生运行库，以 UID/GID `65532` 运行，将 `/var/lib/rustium` 作为可写状态目录，并暴露 `8080` 管理端口。由于 SQLite checkpoint 所有权与源顺序都是单所有者契约，Chart 强制单副本和 `Recreate`。默认 Pod 使用保留的 `ReadWriteOnce` PVC、只读根文件系统、删除全部 Linux capabilities、关闭 ServiceAccount token 挂载，以及 live/ready probe。配置从 Secret 挂载；生产部署应使用外部管理的 Secret，并通过环境变量插入数据库、Kafka 和 Registry 凭据。`scripts/test-packaging.sh` 会构建镜像、执行 `rustium --version`、检查 OCI/非 root 元数据、lint/render Chart、验证持久化/probe/安全不变量，并拒绝多副本。匹配的受保护 `v*` tag 会运行 `.github/workflows/release.yml`，发布带签名、SBOM/provenance 的多架构 GHCR 镜像、Helm OCI artifact 和带 checksum 的 GitHub Release。crates.io 仅通过手动 `.github/workflows/publish-crates.yml` workflow 发布，该 workflow 要求显式轮换的 Secret，并按叶子 crate 到 CLI 的顺序执行。

每个可发布 workspace crate 也包含简洁的英中双语 README。Packaging、release 和 publication 门禁会枚举每个 Cargo package，并要求其 tarball 包含 `README.md`，确保发布后的 crate 不依赖 monorepo checkout 才能使用。它们还要求每个 schema-aware format crate 包含全部优先连接器 schema fixture，避免发布 manifest 静默丢失仓库内固定的 wire contract 证据。

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

发布运维人员还必须执行 [docs/runbook.md](runbook.md) 与 [docs/upgrades.md](upgrades.md) 中的 backup/restore、rollback、容器和 Helm 流程。只有编译通过而未完成这些运维检查，不构成生产发布。

可运行被忽略的 MySQL Docker 测试：

```bash
cargo test -p rustium-mysql --test mysql_docker -- --ignored --nocapture
```

必选 `mysql-cdc` CI 门槛默认执行 3 轮容量为 1 的背压/重连循环，并接受 `RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES=1..1000` 进行更长运行。每轮会终止活动复制连接、在断连期间提交事务，并验证不同连接 identity、全部预期记录及首次出现的源端顺序。

被忽略的外部 MySQL 测试从环境变量读取管理和 CDC 连接设置，仓库中不包含凭据：

```bash
RUSTIUM_MYSQL_TEST_HOST=mysql.example.com \
RUSTIUM_MYSQL_TEST_PORT=3306 \
RUSTIUM_MYSQL_TEST_ADMIN_USER=root \
RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_USER=cdc \
RUSTIUM_MYSQL_TEST_PASSWORD='replace-me' \
RUSTIUM_MYSQL_TEST_DATABASE=cdc_demo \
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-mysql --test mysql_external -- --ignored --nocapture
```

该门槛使用管理账号创建隔离的选中表，并使用 CDC 账号捕获。测试验证容量为 1 的背压/重连循环及复制连接替换、快照/复制、基于精确 server UUID 的 GTID 过滤启动、`heartbeat.action.query`、包含 schema-invariant 索引/默认值操作的破坏性 DDL checkpoint、从安全 binlog 位点发送的空闲周期 heartbeat、OGC 空间值一致性、以及 initial snapshot、binlog before/after image、incremental snapshot 三条路径上的 Debezium 列转换。设置 Kafka 变量后，还会验证真实 broker 在 connector checkpoint 已完成但原 signal offset 尚未提交时的重放、幂等 Source 处理，以及仅在恢复 checkpoint 确认后提交 offset。测试已在启用行级 binlog 和 GTID 的 MySQL 8.4 上通过。

被忽略的 PostgreSQL 外部测试从环境变量读取连接配置，仓库中不包含测试凭据：

```bash
RUSTIUM_POSTGRES_TEST_HOST=postgres.example.com \
RUSTIUM_POSTGRES_TEST_PORT=5432 \
RUSTIUM_POSTGRES_TEST_USER=postgres \
RUSTIUM_POSTGRES_TEST_PASSWORD='replace-me' \
RUSTIUM_POSTGRES_TEST_DATABASE=cdc_demo \
cargo test -p rustium-postgresql --test postgresql_external -- --ignored --nocapture --test-threads=1
```

这些测试使用隔离的业务表/信号表/publication/slot/role 名称和临时信号文件，验证快照记录、同一事务内有序的 create/update/delete 事件、checkpoint 停止、旧 schema 行、破坏性删列/加列 DDL、新 schema 行、schema version 1 和 2 的历史 `Relation` 重放、重启不重复快照、容量为 1 的 Source 输出背压下重复强制终止活动 replication backend 并自动恢复、删除 checkpoint 对应 slot 后 fail-closed 恢复、安全 WAL 位点上的周期 heartbeat、`heartbeat.action.query`、heartbeat 表过滤、带 checkpoint 的 source/file/in-process 增量快照、外部信号状态即时 checkpoint、过滤分块、并发更新去重、pause/resume/scoped-stop 控制、完全无信号表的 file 和 in-process 只读快照、保持更新事务时的只读事务水位、受限权限下零 watermark 写入、与 UUID 主键反向顺序对照的唯一 surrogate 排序、信号表隔离，以及包含 hstore、domain、enum、tsvector 的 PostgreSQL 核心类型矩阵在快照/WAL 路径上的一致转换。可选 superuser fixture 也已在 PostgreSQL 17 上让真实 slot 进入 `wal_status=lost` 并验证 fail-closed 恢复。Docker fixture 还会在 PostgreSQL 17 上强制验证 pgvector 和 PostGIS 的快照/WAL 一致性。

```bash
bash scripts/test-postgresql-extensions.sh
```

可选的外部 Kafka 信号门槛会创建唯一命名的单 partition topic，先发送其他 connector key 的 record，再发送目标 connector key 的 record，验证只投递匹配信号、观察跳过后的 offset、释放持久确认、验证下一已提交 offset，最后删除 topic：

```bash
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-signal-kafka --test kafka_external -- --ignored --nocapture
```

必须通过的 Kafka Sink 门槛会启动隔离的 Redpanda broker 和 Confluent-compatible Schema Registry，创建唯一命名的单 partition topic，并验证有序 key、payload、header、JSON Schema、Avro 与 Protobuf key/value 注册、增量 schema 演进、使用按 ID 获取的 schema 解码 binary、Confluent framing 与 Protobuf message index、按 ID 和最新 subject 查询、真正的 null tombstone、成功 flush、topic 清理，以及关闭自动建 topic 时向不存在 topic 投递产生的显式错误：

```bash
bash scripts/test-kafka-sink.sh
```

共享 runtime soak 也是 GitHub Actions 必须通过的门槛：

```bash
RUSTIUM_RUNTIME_SOAK_CYCLES=256 \
cargo test -p rustium-core --test runtime_soak -- --ignored --nocapture
```

每轮都会让容量为 1 的 Source 队列被重试中的 Sink 阻塞，在每次尝试中验证 batch 字节级一致和 checkpoint 静止，然后释放 Sink 并要求最终位点有序推进。重复的耗尽与取消路径还会验证重试指标、`FAILED` 与 `STOPPED` 生命周期语义、Source 取消、Sink shutdown、60 秒退避的快速中断，以及最后已确认 checkpoint 保持不变。轮数接受 `1..10000`；必选 CI 运行 256 轮。

被忽略的 SQL Server 外部测试从环境变量读取连接配置，仓库中不包含测试凭据：

```bash
RUSTIUM_SQLSERVER_TEST_HOST=sqlserver.example.com \
RUSTIUM_SQLSERVER_TEST_PORT=1433 \
RUSTIUM_SQLSERVER_TEST_USER=sa \
RUSTIUM_SQLSERVER_TEST_PASSWORD='replace-me' \
RUSTIUM_SQLSERVER_TEST_DATABASE=cdc_demo \
RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS=kafka.example.com:9092 \
cargo test -p rustium-sqlserver --test sqlserver_external -- --ignored --nocapture
```

该门槛使用隔离的表/capture-instance 名称和 Kafka topic，等待 SQL Agent 初始化，并验证快照记录、fetch size 为 1 的事务继续读取、事务中间 checkpoint 重放、容量为 1 的背压下重复终止 polling session 并从未变化的带类型 CDC cursor 恢复、连接 identity 替换、全部预期记录、首次出现的源端顺序、并发 commit 排序、retention 失败、heartbeat/action-query、核心和扩展快照/CDC 类型一致性、四段 snapshot/CDC 与三段 incremental-snapshot 列转换、signal command 原值隔离、带 checkpoint 的 in-process keyset 重启、带 additional condition 的 source-table signaling、已完成 signal ID 持久化、connector state 已持久化但 Kafka offset 尚未提交时的真实 broker 重放和资源清理。测试已在 SQL Server 2022 Developer RTM-CU25 上通过。

独立的 SQL Server Docker 可移植性门槛是 GitHub Actions 必须通过的测试，在可以访问 Microsoft 镜像的环境中仍可手动运行：

```bash
cargo test -p rustium-sqlserver --test sqlserver_docker -- --ignored --nocapture
```

该门槛支持远程 Docker daemon，且无需暴露临时 SQL Server 端口。Docker 命令使用当前选择的 context。设置 `RUSTIUM_SQLSERVER_DOCKER_SSH_HOST` 后，容器仍只发布到 daemon 主机 loopback，测试会建立本地认证 SSH forward；`RUSTIUM_SQLSERVER_DOCKER_SSH_LOCAL_PORT` 可选地固定本地端点。

### 16. 路线图

1. 剩余重点是冻结公共配置、事件和持久化状态兼容契约；tagged image/Helm/GitHub Release 自动化已实现。
2. 在完成审查并轮换 `CARGO_REGISTRY_TOKEN` secret 后，通过手动 crates.io workflow 按依赖顺序发布各 crate。
3. 只有当当前五个连接器和共享运行时通过完整 `1.0` 门槛后，才考虑更多数据库。
