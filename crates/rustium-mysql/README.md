# rustium-mysql

MySQL row-binlog CDC source for Rustium. It supports snapshots, GTID-aware recovery, schema history, typed values, heartbeats, Debezium-compatible signals, incremental snapshots, and bounded reconnects.

Debezium-compatible column transformations apply consistently to initial snapshots, incremental snapshots, and binlog before/after images. Supported dynamic properties are `column.truncate.to.<length>.chars`, `column.mask.with.<length>.chars`, `column.mask.hash.<algorithm>.with.salt.<salt>`, and `column.mask.hash.v2.<algorithm>.with.salt.<salt>`. Selectors are anchored, case-insensitive `database.table.column` regular expressions. Keyset progress and concurrent-update deduplication always use original primary-key values.

The connector targets MySQL 8+ and is validated against MySQL 8.4. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) and [design](https://github.com/ulnit/rustium/blob/main/docs/design.md).

## 简体中文

Rustium 的 MySQL row-binlog CDC source，支持快照、基于 GTID 的恢复、schema history、类型化值、heartbeat、Debezium 兼容信号、增量快照和有界重连。

Debezium 兼容列转换会一致应用于 initial snapshot、incremental snapshot 和 binlog before/after image。支持的动态参数为 `column.truncate.to.<length>.chars`、`column.mask.with.<length>.chars`、`column.mask.hash.<algorithm>.with.salt.<salt>` 和 `column.mask.hash.v2.<algorithm>.with.salt.<salt>`。Selector 是 anchored、大小写不敏感的 `database.table.column` 正则。Keyset progress 和并发更新去重始终使用原始主键值。

连接器面向 MySQL 8+，并已在 MySQL 8.4 上验证。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)和[设计文档](https://github.com/ulnit/rustium/blob/main/docs/design.md)。
