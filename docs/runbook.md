# Rustium Operations Runbook

## English

This runbook assumes one Rustium process owns one connector name and one source-position stream. Keep the SQLite checkpoint on durable storage and never run two processes against the same connector state unless an external lock has been added and tested.

### 1. Preflight and startup

1. Store the complete YAML or Debezium `.properties` configuration in an external Secret or protected filesystem. Verify that passwords are supplied through environment interpolation.
2. Run `rustium validate --config /etc/rustium/rustium.yaml`. This validates configuration and the configured source, Sink, and optional Kafka signal dependencies.
3. Confirm database CDC prerequisites, source grants, publication/slot or binlog/GTID settings, SQL Server Agent and capture cleanup, Kafka ACLs, Schema Registry compatibility, and writable checkpoint storage.
4. Start with `rustium run --config /etc/rustium/rustium.yaml` or the Helm chart. Keep `server.enable_mutations=false` unless the control path is authenticated and isolated.
5. Verify `GET /health/live`, then wait for `GET /health/ready` to return HTTP 200. Inspect `GET /v1/connector/status` for the connector state, source position, checkpoint time, and reason.

The container listens on port 8080 for management. The Helm chart uses one replica, `Recreate`, a retained PVC, UID/GID 65532, a read-only root filesystem, and a Secret-mounted configuration.

### 2. Normal stop and restart

Send SIGTERM or use `POST /v1/connector/stop` only when mutations are enabled. Wait for the process to exit and the status to reach `STOPPED`. A normal stop flushes only deliverable events, persists only Sink-acknowledged positions, closes the Sink, and leaves unacknowledged work for replay. Never delete the SQLite file during a normal restart.

After restart, check that the source position resumes from the expected checkpoint, the connector state is `STREAMING` or `SNAPSHOTTING`, and lag decreases. A replay after a crash is expected at-least-once behavior; compare deterministic event IDs before declaring duplication a data-loss incident.

### 3. Health, metrics, and alerts

Use `/health/live` for process liveness and `/health/ready` for traffic readiness. Readiness can be non-200 during startup, snapshot handoff, or failure; it is not a replacement for the status endpoint.

Alert on:

- `FAILED` lifecycle state or a non-empty failure reason;
- readiness non-200 beyond the connector's expected startup or snapshot window;
- increasing `rustium_source_lag_seconds` against the business freshness objective;
- a sustained pipeline queue depth near `runtime.channel_capacity`;
- a growing `rustium_sink_retry_attempts` rate or repeated Registry/Kafka errors;
- checkpoint age that exceeds the source retention safety margin.

`rustium_source_lag_seconds` is `NaN` when no source timestamp exists. `rustium_checkpoint_age_seconds`, `rustium_last_event_age_seconds`, and `rustium_connector_state_age_seconds` provide direct alerting signals for checkpoint freshness, source observation, and lifecycle transitions. Interpret them together with status position and checkpoint time. Metrics and status update after Sink acknowledgement and checkpoint persistence, so a stalled metric is a reason to inspect the Sink and state store, not to reset state.

### 4. State backup and restore

Back up the checkpoint before every binary/configuration upgrade, source reset, or retention operation.

1. Stop Rustium gracefully and confirm the process has exited.
2. On a filesystem deployment, run `PRAGMA integrity_check;` and `PRAGMA wal_checkpoint(TRUNCATE);` with the `sqlite3` tool, then copy the database file to protected storage. Preserve the original file, its permissions, and the connector configuration together.
3. On Kubernetes, prefer a CSI `VolumeSnapshot` of the retained PVC while the Pod is stopped. Record the image digest, chart values, Secret resource version, source position, and slot/binlog/CDC identifiers beside the snapshot.
4. Encrypt backups, restrict access to the connector operators, and test a restore on an isolated path before relying on it.

To restore, stop the process, preserve the current failed state, replace the database from the verified backup, restore ownership for UID/GID 65532, run `PRAGMA integrity_check;`, and start the exact configuration that created the backup. Do not restore a checkpoint into a different connector name, database, publication, server UUID, or SQL Server capture-instance contract without an explicit migration plan.

### 5. Connector recovery

#### PostgreSQL

Check the replication slot, plugin, `confirmed_flush_lsn`, `restart_lsn`, `wal_status`, publication, and `wal_level=logical` before restarting. Rustium fails closed when the checkpoint's slot is missing or WAL is `unreserved`/`lost`. Preserve the original slot and database identity. If continuity is impossible, back up state, reset the connector, recreate or reassign the managed slot according to ownership, and run a new initial snapshot.

#### MySQL

Check `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`, GTID mode, the exact server UUID, binlog retention, and CDC grants. Do not change the server UUID or purge the checkpoint's required binlog range. If the range is gone, back up state, reset, and run a new initial snapshot from a safe current position. Preserve the signal-table and topic contracts when resuming incremental snapshots.

#### SQL Server

Check database and table CDC enablement, capture instances, SQL Agent, CDC cleanup retention, and the maximum available LSN. Rustium fails closed when a checkpoint is older than retained change rows. Preserve capture-instance names during restart. If retention has removed the required LSN, back up state, reset, and perform a new snapshot.

### 6. Checkpoint reset and data-loss prevention

`rustium state reset --config /etc/rustium/rustium.yaml --confirm` is destructive for one connector name. Use it only after preserving the state backup and proving that source continuity cannot be recovered. A reset removes the local position but does not delete database slots, binlogs, CDC tables, Kafka topics, or external signal rows; clean those resources only after confirming ownership and retention policy.

After a reset, force an initial snapshot, verify row counts and first post-snapshot source order, and document the new source anchor. Never reset merely because lag is high or because a Sink retry is in progress.

### 7. Kubernetes maintenance

Use `helm upgrade --install` with the same release name and an immutable image digest. The chart's `Recreate` strategy prevents two SQLite owners. Rotate an externally managed configuration Secret, then run `kubectl rollout restart deployment/<release>` and verify the new Secret resource version in the Pod. Back up the PVC before changing storage class, access mode, or chart release. Keep `serviceMonitor.enabled=false` unless the Prometheus Operator CRD exists.

### 8. Incident collection

Before changing state, collect the sanitized status JSON, metrics, image digest, chart values with secrets removed, connector config fingerprint, source position, checkpoint backup hash, and relevant database slot/binlog/CDC metadata. Capture logs with payloads and credentials redacted. Record whether the Sink acknowledged any batch before the failure. Preserve the original state for replay analysis, then follow the recovery section for the affected connector.

## 简体中文

本 runbook 假设一个 Rustium 进程拥有一个 connector name 和一条源位点流。SQLite checkpoint 必须放在持久存储中；除非增加并验证外部锁，否则不要让两个进程同时访问同一 connector state。

### 1. 预检与启动

1. 将完整 YAML 或 Debezium `.properties` 配置放入外部 Secret 或受保护文件系统，通过环境变量插入密码。
2. 执行 `rustium validate --config /etc/rustium/rustium.yaml`，校验配置、Source、Sink 以及可选 Kafka signal 依赖。
3. 确认数据库 CDC 前置条件、Source 权限、publication/slot 或 binlog/GTID、SQL Server Agent 与 capture cleanup、Kafka ACL、Schema Registry 兼容性和 checkpoint 存储可写。
4. 使用 `rustium run --config /etc/rustium/rustium.yaml` 或 Helm Chart 启动。除非控制路径已认证且隔离，否则保持 `server.enable_mutations=false`。
5. 检查 `GET /health/live`，再等待 `GET /health/ready` 返回 HTTP 200，并查看 `GET /v1/connector/status` 的状态、源位点、checkpoint 时间和 reason。

容器管理端口为 8080。Helm Chart 使用单副本、`Recreate`、保留 PVC、UID/GID 65532、只读根文件系统和 Secret 配置挂载。

### 2. 正常停止与重启

发送 SIGTERM，或仅在开启 mutations 时调用 `POST /v1/connector/stop`。等待进程退出并达到 `STOPPED`。正常停止只 flush 能完成投递的事件，只持久化 Sink 已确认位点，关闭 Sink，未确认工作会保留以供重放。正常重启不要删除 SQLite 文件。

重启后确认 Source 从预期 checkpoint 恢复，状态为 `STREAMING` 或 `SNAPSHOTTING`，lag 正在下降。崩溃后的重放属于 at-least-once 语义；先比较确定性 event ID，再判断是否为重复或数据丢失事故。

### 3. 健康、指标与告警

`/health/live` 用于进程存活，`/health/ready` 用于流量就绪。启动、snapshot handoff 或故障期间 readiness 可能非 200，不能替代 status endpoint。

建议对以下情况告警：

- 生命周期进入 `FAILED` 或 failure reason 非空；
- readiness 在超过预期启动或 snapshot 窗口后仍非 200；
- `rustium_source_lag_seconds` 按业务 freshness 目标持续上升；
- pipeline queue depth 持续接近 `runtime.channel_capacity`；
- `rustium_sink_retry_attempts` 增长或 Registry/Kafka 错误重复出现；
- checkpoint age 超过 Source retention 安全余量。

没有源时间戳时 `rustium_source_lag_seconds` 为 `NaN`；`rustium_checkpoint_age_seconds`、`rustium_last_event_age_seconds` 和 `rustium_connector_state_age_seconds` 分别提供 checkpoint 新鲜度、源事件观察和生命周期状态的直接告警信号，应结合 status 位点和 checkpoint 时间判断。指标和 status 只有 Sink 确认及 checkpoint 持久化后才推进；指标停滞应先检查 Sink 和 state store，不要直接 reset。

### 4. 状态备份与恢复

每次 binary/config 升级、Source reset 或 retention 操作前都要备份 checkpoint。

1. 优雅停止 Rustium，确认进程已退出。
2. 文件系统部署使用 `sqlite3` 执行 `PRAGMA integrity_check;` 和 `PRAGMA wal_checkpoint(TRUNCATE);`，再把数据库文件复制到受保护存储，并一起保存原文件权限和 connector 配置。
3. Kubernetes 优先在 Pod 停止时对保留 PVC 创建 CSI `VolumeSnapshot`。同时记录 image digest、Chart values、Secret resource version、源位点和 slot/binlog/CDC 标识。
4. 加密备份，只允许 connector 运维人员访问，并在隔离路径测试 restore。

恢复时停止进程，保留当前故障状态，用已验证备份替换数据库，恢复 UID/GID 65532 的所有权，执行 `PRAGMA integrity_check;`，再启动创建该备份时的完全相同配置。未经明确迁移计划，不要把 checkpoint 放到不同 connector name、数据库、publication、server UUID 或 SQL Server capture-instance 契约中。

### 5. 连接器恢复

#### PostgreSQL

重启前检查 replication slot、plugin、`confirmed_flush_lsn`、`restart_lsn`、`wal_status`、publication 和 `wal_level=logical`。slot 缺失或 WAL 为 `unreserved`/`lost` 时 Rustium 会 fail-closed。保留原 slot 与数据库身份；无法连续恢复时先备份，按 ownership 重建或重新分配 managed slot，执行新的 initial snapshot。

#### MySQL

检查 `log_bin`、`binlog_format=ROW`、`binlog_row_image=FULL`、GTID、精确 server UUID、binlog retention 和 CDC 权限。不要修改 server UUID，也不要清理 checkpoint 仍需的 binlog 范围。范围已丢失时先备份、reset，然后从安全当前位点执行 initial snapshot。恢复增量快照时保持 signal-table 和 topic 契约。

#### SQL Server

检查数据库与表 CDC、capture instance、SQL Agent、CDC cleanup retention 和可用最大 LSN。checkpoint 早于保留 change rows 时 Rustium 会 fail-closed。重启时保持 capture-instance 名称；retention 已删除所需 LSN 时先备份、reset 并执行新 snapshot。

### 6. Checkpoint reset 与防止数据丢失

`rustium state reset --config /etc/rustium/rustium.yaml --confirm` 会破坏性删除一个 connector name 的本地位点。只有在已保存 state backup 且证明无法恢复 Source 连续性后才能使用。Reset 不会删除数据库 slot、binlog、CDC table、Kafka topic 或外部 signal row；清理这些资源前要确认 ownership 与 retention policy。

Reset 后强制 initial snapshot，验证行数和 snapshot 之后第一批源顺序，并记录新的源锚点。不要因为 lag 高或 Sink 正在重试就 reset。

### 7. Kubernetes 维护

使用相同 release name 和不可变 image digest 执行 `helm upgrade --install`。Chart 的 `Recreate` 防止两个 SQLite owner。轮换外部配置 Secret 后执行 `kubectl rollout restart deployment/<release>`，并确认 Pod 使用新的 Secret resource version。修改 storage class、access mode 或 Chart release 前备份 PVC。只有安装 Prometheus Operator CRD 时才打开 `serviceMonitor.enabled`。

### 8. 事故信息收集

改变状态前收集脱敏 status JSON、metrics、image digest、去掉 secret 的 Chart values、配置 fingerprint、源位点、checkpoint backup hash 以及数据库 slot/binlog/CDC metadata。日志必须去掉 payload 和凭据。记录故障前 Sink 是否确认过 batch；保留原 state 用于重放分析，再按对应 connector recovery 段落操作。
