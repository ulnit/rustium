# rustium-oracle

Oracle LogMiner CDC source connector for Rustium.

## English

Rustium uses Oracle LogMiner with the online data dictionary and persists SCN,
commit SCN, transaction ID, and event serial in the checkpoint. The database
must run in ARCHIVELOG mode with supplemental logging enabled and the connector
user must have the LogMiner catalog/query privileges described by Debezium.
Initial snapshots use Oracle Flashback queries at a fixed SCN.

The implementation uses the pure-Rust oracle-rs driver; no Oracle Instant
Client is needed on the Rustium host. Oracle server-side LogMiner privileges
and redo retention remain required.

## 中文

Rustium 使用 Oracle LogMiner 在线字典，并在 checkpoint 中持久化 SCN、提交 SCN、
事务 ID 和事件序号。数据库必须启用 ARCHIVELOG 与 supplemental logging，连接账号
需要具备 Debezium 文档所列的 LogMiner 目录和查询权限。初始快照在固定 SCN 上通过
Oracle Flashback 查询。

实现使用纯 Rust 的 oracle-rs 驱动，Rustium 主机不需要 Oracle Instant Client；
但 Oracle 服务端仍必须具备 LogMiner 权限并保留所需 redo。
