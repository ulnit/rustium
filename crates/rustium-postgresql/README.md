# rustium-postgresql

PostgreSQL `pgoutput` CDC source for Rustium. It supports all Debezium snapshot isolation modes, exported/no-export slot handoff with bounded Debezium-compatible DDL locks, PostgreSQL 17 failover slots, managed-slot cleanup on orderly stop, immediate durable LSN feedback with bounded timeout actions, periodic slot `catalog_xmin` metadata, persistent Relation-driven schema history, WAL recovery, Debezium interval, MONEY scale, schema refresh modes, and PostgreSQL column transformations, safe unchanged-TOAST handling, transactional and non-transactional logical decoding messages, typed core and extension values, heartbeats, signals, and incremental snapshots.

The connector targets PostgreSQL 14+ and is validated against PostgreSQL 17 with logical replication. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) and [design](https://github.com/ulnit/rustium/blob/main/docs/design.md).

## 简体中文

Rustium 的 PostgreSQL `pgoutput` CDC source，支持全部 Debezium snapshot isolation mode、带有界 Debezium 兼容 DDL lock 的 exported/no-export slot handoff、PostgreSQL 17 failover slot、有序停止时 managed slot 清理、带有界 timeout action 的即时 durable LSN feedback、定期 slot `catalog_xmin` metadata、持久 Relation-driven schema history、WAL 恢复、Debezium interval、MONEY scale 与 schema refresh mode、安全的 unchanged-TOAST 处理、事务内及非事务 logical decoding message、核心及扩展类型值、heartbeat、信号和增量快照。
Rustium 的 PostgreSQL `pgoutput` CDC source，支持全部 Debezium snapshot isolation mode、带有界 Debezium 兼容 DDL lock 的 exported/no-export slot handoff、PostgreSQL 17 failover slot、有序停止时 managed slot 清理、带有界 timeout action 的即时 durable LSN feedback、定期 slot `catalog_xmin` metadata、持久 Relation-driven schema history、WAL 恢复、Debezium interval、MONEY scale、schema refresh mode 和 PostgreSQL 列转换、安全的 unchanged-TOAST 处理、事务内及非事务 logical decoding message、核心及扩展类型值、heartbeat、信号和增量快照。

Column transformations use Debezium's dynamic property names on initial, incremental, and streaming records. Selectors are anchored case-insensitive `schema.table.column` patterns; truncate, fixed mask, hash V1, and hash V2 are evaluated in that order. Native YAML uses `source.column_transformations`.

列转换在 initial、incremental 和 streaming record 上使用 Debezium 动态参数名。Selector 是 anchored、大小写不敏感的 `schema.table.column` pattern；按 truncate、固定 mask、hash V1、hash V2 顺序计算。原生 YAML 使用 `source.column_transformations`。

连接器面向 PostgreSQL 14+，并已在启用逻辑复制的 PostgreSQL 17 上验证。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)和[设计文档](https://github.com/ulnit/rustium/blob/main/docs/design.md)。
