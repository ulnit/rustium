# rustium-postgresql

PostgreSQL `pgoutput` CDC source for Rustium. It supports exported-snapshot handoff, PostgreSQL 17 failover slots, persistent Relation-driven schema history, WAL recovery, Debezium interval, MONEY scale, and schema refresh modes, safe unchanged-TOAST handling, transactional and non-transactional logical decoding messages, typed core and extension values, heartbeats, signals, and incremental snapshots.

The connector targets PostgreSQL 14+ and is validated against PostgreSQL 17 with logical replication. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) and [design](https://github.com/ulnit/rustium/blob/main/docs/design.md).

## 简体中文

Rustium 的 PostgreSQL `pgoutput` CDC source，支持 exported snapshot 切换、PostgreSQL 17 failover slot、持久 Relation-driven schema history、WAL 恢复、Debezium interval、MONEY scale 与 schema refresh mode、安全的 unchanged-TOAST 处理、事务内及非事务 logical decoding message、核心及扩展类型值、heartbeat、信号和增量快照。

连接器面向 PostgreSQL 14+，并已在启用逻辑复制的 PostgreSQL 17 上验证。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)和[设计文档](https://github.com/ulnit/rustium/blob/main/docs/design.md)。
