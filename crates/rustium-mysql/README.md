# rustium-mysql

MySQL row-binlog CDC source for Rustium. It supports snapshots, GTID-aware recovery, schema history, typed values, heartbeats, Debezium-compatible signals, incremental snapshots, and bounded reconnects.

The connector targets MySQL 8+ and is validated against MySQL 8.4. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) and [design](https://github.com/ulnit/rustium/blob/main/docs/design.md).

## 简体中文

Rustium 的 MySQL row-binlog CDC source，支持快照、基于 GTID 的恢复、schema history、类型化值、heartbeat、Debezium 兼容信号、增量快照和有界重连。

连接器面向 MySQL 8+，并已在 MySQL 8.4 上验证。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)和[设计文档](https://github.com/ulnit/rustium/blob/main/docs/design.md)。
