# rustium-config

Versioned Rustium configuration models, validation, environment interpolation, semantic fingerprints, and Debezium-compatible `.properties` parsing for PostgreSQL, MySQL, SQL Server, sinks, formats, and runtime settings.

Use this crate when an embedded Rustium application needs the same strict configuration contract as the CLI. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md).

`snapshot.include.collection.list` maps to native `snapshot.include_collections` with anchored, connector-qualified, snapshot-only matching.

PostgreSQL `publication.autocreate.mode` supports `disabled`, `all_tables`, `filtered`, and `no_tables`. Debezium properties default to `all_tables`; native `source.publication_autocreate_mode` defaults to `disabled` for backward-compatible ownership and fingerprints.

PostgreSQL `replica.identity.autoset.values` maps to structured native rules with `table`, `identity`, and optional `index`. Non-empty rules are fingerprinted because validation applies transactional table DDL.

PostgreSQL `publish.via.partition.root` maps to native `source.publish_via_partition_root`; existing publication metadata must match the configured value.

PostgreSQL `slot.failover` maps to native `source.slot_failover`. It defaults to false and is fingerprinted only when enabled; failover configuration is valid only for managed slots.

PostgreSQL `slot.drop.on.stop` maps to native `source.drop_slot_on_stop`. It defaults to false, is valid only for managed slots, and is excluded from fingerprints because it affects orderly lifecycle cleanup rather than event selection.

PostgreSQL `snapshot.locking.mode=none|shared` maps to native `source.snapshot_locking_mode`; `snapshot.lock.timeout.ms` maps to the 10-second native `source.snapshot_lock_timeout`. Both are operational and excluded from fingerprints. Java SPI mode `custom` and timeouts above PostgreSQL's signed 32-bit millisecond limit fail validation.

PostgreSQL `snapshot.isolation.mode` maps to native `source.snapshot_isolation_mode` and accepts `serializable`, `repeatable_read`, `read_committed`, and `read_uncommitted`. Serializable and `repeatable_read` preserve existing fingerprints because both import the same exported snapshot; the lower modes are fingerprinted because they alter snapshot consistency and slot handoff.

PostgreSQL `interval.handling.mode` accepts Debezium `numeric` and `string`; properties default to `numeric`. Native `source.interval_handling_mode` additionally accepts the backward-compatible `postgres` default, which is omitted from fingerprint material.

PostgreSQL `money.fraction.digits` maps to native `source.money_fraction_digits` and defaults to `2`. Non-default signed 16-bit scales are fingerprinted because they change MONEY schemas and values.

PostgreSQL `schema.refresh.mode` maps to native `source.schema_refresh_mode` and accepts `columns_diff` or `columns_diff_exclude_unchanged_toast`. Both are operationally equivalent with pgoutput Relation-driven schemas and are excluded from fingerprints.

PostgreSQL Debezium properties enable logical decoding messages by default and map `message.prefix.include.list` / `message.prefix.exclude.list` to anchored native filters. Native `source.logical_decoding_messages` defaults to false; enabling capture or adding filters is fingerprinted.

## 简体中文

Rustium 的版本化配置模型、校验、环境变量插值、语义指纹，以及 PostgreSQL、MySQL、SQL Server、sink、格式和 runtime 的 Debezium 兼容 `.properties` 解析。

嵌入 Rustium 的应用需要与 CLI 相同的严格配置契约时使用此 crate。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)。

`snapshot.include.collection.list` 映射为原生 `snapshot.include_collections`，采用 anchored、连接器限定且仅作用于快照的匹配语义。

PostgreSQL `publication.autocreate.mode` 支持 `disabled`、`all_tables`、`filtered` 和 `no_tables`。Debezium properties 默认使用 `all_tables`；原生 `source.publication_autocreate_mode` 默认使用 `disabled`，以保持向后兼容的所有权和 fingerprint。

PostgreSQL `replica.identity.autoset.values` 映射为带 `table`、`identity` 和可选 `index` 的结构化原生规则。非空规则会进入 fingerprint，因为 validation 会执行事务化表 DDL。

PostgreSQL `publish.via.partition.root` 映射为原生 `source.publish_via_partition_root`；既有 publication metadata 必须与配置值一致。

PostgreSQL `slot.failover` 映射为原生 `source.slot_failover`。默认值为 false，只有启用时才进入 fingerprint；failover 配置只适用于 managed slot。

PostgreSQL `slot.drop.on.stop` 映射为原生 `source.drop_slot_on_stop`。默认值为 false，只适用于 managed slot；它影响有序生命周期清理而不是事件选择，因此不进入 fingerprint。

PostgreSQL `snapshot.locking.mode=none|shared` 映射为原生 `source.snapshot_locking_mode`；`snapshot.lock.timeout.ms` 映射为默认 10 秒的原生 `source.snapshot_lock_timeout`。两者都是运维参数，不进入 fingerprint。Java SPI 模式 `custom` 以及超过 PostgreSQL 有符号 32 位毫秒上限的 timeout 会校验失败。

PostgreSQL `snapshot.isolation.mode` 映射为原生 `source.snapshot_isolation_mode`，接受 `serializable`、`repeatable_read`、`read_committed` 和 `read_uncommitted`。`serializable` 与 `repeatable_read` 都导入同一 exported snapshot，因此保持既有 fingerprint；较低模式会改变 snapshot 一致性和 slot handoff，因此进入 fingerprint。

PostgreSQL `interval.handling.mode` 接受 Debezium 的 `numeric` 和 `string`，properties 默认使用 `numeric`。原生 `source.interval_handling_mode` 还接受向后兼容的默认值 `postgres`，该默认值不会进入 fingerprint material。

PostgreSQL `money.fraction.digits` 映射为原生 `source.money_fraction_digits`，默认值为 `2`。非默认的有符号 16 位 scale 会改变 MONEY schema 与值，因此进入 fingerprint。

PostgreSQL `schema.refresh.mode` 映射为原生 `source.schema_refresh_mode`，接受 `columns_diff` 或 `columns_diff_exclude_unchanged_toast`。在 pgoutput Relation-driven schema 下两者运维行为等价，因此不进入 fingerprint。

PostgreSQL Debezium properties 默认启用 logical decoding message，并把 `message.prefix.include.list` / `message.prefix.exclude.list` 映射为 anchored 原生过滤器。原生 `source.logical_decoding_messages` 默认为 false；启用捕获或增加过滤器都会进入 fingerprint。
