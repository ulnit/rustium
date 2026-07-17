# rustium-sqlserver

SQL Server CDC source for Rustium. It supports consistent snapshots, direct CDC polling, transaction-ordered update pairing, retention-aware recovery, typed SQL Server values, heartbeats, signals, and incremental snapshots.

Debezium-compatible column transformations cover initial snapshots, CDC update before/after images, and incremental snapshots. SQL Server selectors accept both `database.schema.table.column` and `schema.table.column`; truncation, fixed masks, and V1/V2 hashes share the same deterministic priority and salt-safe semantic fingerprinting as the PostgreSQL and MySQL connectors. Internal signal-table commands are parsed from raw values before any configured business-column transformation.

The connector targets SQL Server 2017+ CDC and is validated against SQL Server 2022. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) and [design](https://github.com/ulnit/rustium/blob/main/docs/design.md).

The required Docker gate also supports a remote Docker daemon without exposing its ephemeral SQL Server port:

```bash
docker context use remote-docker
RUSTIUM_SQLSERVER_DOCKER_SSH_HOST=docker-host \
cargo test -p rustium-sqlserver --test sqlserver_docker -- \
  snapshots_and_streams_cdc_changes --ignored --exact --nocapture
```

## 简体中文

Rustium 的 SQL Server CDC source，支持一致性快照、direct CDC polling、按事务顺序配对 update、感知 retention 的恢复、类型化 SQL Server 值、heartbeat、信号和增量快照。

兼容 Debezium 的列转换覆盖 initial snapshot、CDC update before/after image 和 incremental snapshot。SQL Server selector 同时接受 `database.schema.table.column` 与 `schema.table.column`；截断、固定 mask、V1/V2 hash 使用与 PostgreSQL、MySQL 相同的确定性优先级和不暴露 salt 的 semantic fingerprint。内部 signal-table command 会先从原始值解析，再执行任何面向业务列的转换。

连接器面向启用 CDC 的 SQL Server 2017+，并已在 SQL Server 2022 上验证。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)和[设计文档](https://github.com/ulnit/rustium/blob/main/docs/design.md)。

必跑 Docker 门禁也支持远程 Docker daemon，且无需暴露临时 SQL Server 端口：

```bash
docker context use remote-docker
RUSTIUM_SQLSERVER_DOCKER_SSH_HOST=docker-host \
cargo test -p rustium-sqlserver --test sqlserver_docker -- \
  snapshots_and_streams_cdc_changes --ignored --exact --nocapture
```
