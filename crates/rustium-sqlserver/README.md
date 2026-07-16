# rustium-sqlserver

SQL Server CDC source for Rustium. It supports consistent snapshots, direct CDC polling, transaction-ordered update pairing, retention-aware recovery, typed SQL Server values, heartbeats, signals, and incremental snapshots.

The connector targets SQL Server 2017+ CDC and is validated against SQL Server 2022. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) and [design](https://github.com/ulnit/rustium/blob/main/docs/design.md).

## 简体中文

Rustium 的 SQL Server CDC source，支持一致性快照、direct CDC polling、按事务顺序配对 update、感知 retention 的恢复、类型化 SQL Server 值、heartbeat、信号和增量快照。

连接器面向启用 CDC 的 SQL Server 2017+，并已在 SQL Server 2022 上验证。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)和[设计文档](https://github.com/ulnit/rustium/blob/main/docs/design.md)。
