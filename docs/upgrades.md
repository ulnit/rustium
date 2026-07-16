# Rustium Upgrade and Migration Contract

## English

This document defines the currently supported alpha upgrade behavior. It is intentionally conservative: an unknown future state is rejected rather than guessed, skipped, or overwritten.

### Version inventory

| Contract | Current version | Read behavior |
|---|---:|---|
| Native configuration API | `rustium.io/v1alpha1` | Strict YAML/properties validation; unknown fields fail |
| SQLite storage schema (`PRAGMA user_version`) | `1` | Version `0` initializes to `1`; future versions fail without downgrade |
| JSON `Checkpoint.schema_version` | `2` | Version `1` remains readable when its connector contract permits it |
| PostgreSQL schema-history envelope | `5` | Versions `1` through `5` read; older checkpoints without history fail closed |
| MySQL schema-history envelope | `3` | Versions `1` through `3` read; v2 offset progress is safely replayable |
| SQL Server connector-state envelope | `1` | Version `1` only |

The SQLite storage version is separate from the JSON checkpoint version. A storage migration must never silently rewrite a future `PRAGMA user_version`. Connector-state versions are format-specific and are validated before a source resumes.

### Before every upgrade

1. Read the release notes and compare the version inventory above with the target binary.
2. Stop the connector gracefully and wait for `STOPPED`.
3. Back up the SQLite database or create a CSI VolumeSnapshot. Record its hash, image digest, chart values, configuration fingerprint, source position, and database slot/binlog/CDC identifiers.
4. Run `rustium validate` with the target binary and the unchanged configuration. Resolve capability or compatibility errors before starting.
5. Confirm that the target binary still uses the same connector name, source identity, topic prefix, state path, and sink durability settings.

### Configuration migration

The public native API is `rustium.io/v1alpha1`. Debezium-style properties are parsed into the same versioned model. Rustium does not rewrite configuration files automatically. Additive recognized properties can be introduced with defaults; unknown fields and unsafe compatibility options fail validation. Keep a copy of the pre-upgrade configuration and record any renamed Debezium property mapping in the release notes.

The configuration fingerprint intentionally covers source identity, selected collections, snapshot behavior, format, and routing. Passwords and operational tuning do not change it. Changing a fingerprinted value against an existing checkpoint is rejected. To make such a change, stop the connector, decide whether the source position is still valid under the new contract, and either restore the compatible configuration or perform the documented reset and new snapshot.

The additive `snapshot.include_collections` field defaults to an empty list and is omitted from semantic fingerprint material when empty, preserving fingerprints created before the field existed. A non-empty list changes initial and `when_needed` recovery snapshot behavior, so it is fingerprinted and must be introduced through the normal reviewed configuration-change procedure.

The additive native PostgreSQL `source.publication_autocreate_mode` field defaults to `disabled` and is omitted from semantic fingerprint material at that value, preserving older native fingerprints and publication ownership. Debezium properties intentionally default `publication.autocreate.mode` to `all_tables`, matching Debezium rather than the native compatibility default. Any non-`disabled` mode changes the fingerprint. Before enabling it on an existing connector, validate database `CREATE`, table ownership, and superuser requirements, and review whether `filtered` may replace the current table-scoped publication set.

The additive PostgreSQL `source.replica_identity_autoset_values` list defaults to empty and is omitted from semantic fingerprint material when empty. A non-empty list changes the fingerprint and causes source validation to apply transactional table DDL. Before introducing or changing rules, stop the connector, verify table ownership and any replica-index constraints, review UPDATE/DELETE key and before-image compatibility with downstream consumers, and run validation in a controlled change window. Overlapping rules fail before mutation; SQL or privilege failures roll back the full rule set.

### Checkpoint migration

Checkpoint JSON version 2 adds an optional connector-state envelope. Version 1 JSON without that field remains readable. PostgreSQL and MySQL cannot safely resume a completed version 1 checkpoint without persistent schema history, so they fail closed and require a backup, reset, and new initial snapshot. SQL Server's tested resume path does not depend on connector state and can read version 1 when the source cursor and CDC retention remain valid.

The SQLite storage schema is initialized and migrated by `rustium-state`. Current version 1 only creates the `checkpoints` table and sets `PRAGMA user_version`. A database reporting a version greater than the binary supports is rejected before any downgrade. Do not edit `user_version` by hand and do not copy a checkpoint between connector identities.

### Connector-state migration

- PostgreSQL schema history versions 1 through 5 are deserialized with defaults for additive fields. The current state keeps table layouts, type metadata, incremental keyset progress, pause state, and a bounded completed-signal history.
- MySQL schema history versions 1 through 3 are deserialized. Version 2 offset progress remains readable and is replayed from the collection start; version 3 persists typed keyset progress and completed signal IDs.
- SQL Server connector state currently accepts version 1 only. Unknown versions fail before CDC polling.

Do not manually edit JSON payloads. A malformed, duplicate, unknown-format, or unknown-version envelope is a state error and must be handled by restoring a valid backup or resetting with a new snapshot.

### Rolling upgrade procedure

Rustium is a single-owner process, not an active/active service. Use a stop, backup, replace, and start sequence:

1. Drain or pause upstream operational changes as required by the database retention window.
2. Stop the old binary and verify its final checkpoint was acknowledged.
3. Back up state and record the old image digest.
4. Deploy the new binary or Helm image with the same state PVC and connector configuration.
5. Run `rustium validate`, then start exactly one replica.
6. Verify the source position, connector-state version, sink acknowledgements, lag, and first post-upgrade event order.
7. Keep the backup until the new binary has passed the normal retention and replay observation window.

Do not run two replicas against one state path. A Helm `Recreate` deployment is required for this reason. If the new binary writes a checkpoint, do not roll back in place unless the release explicitly documents reverse-read compatibility. Otherwise stop it, restore the pre-upgrade state backup and image, and resume from the old checkpoint.

### Reset migration

Use `rustium state reset --config <path> --confirm` only after backing up state and proving that source continuity is unavailable. Reset is not a generic upgrade mechanism. After reset, recreate or reassign source resources according to connector ownership, force an initial snapshot, validate row counts and first-seen ordering, and document the new anchor.

### Release author checklist

- Update the version inventory and release notes for every schema or config change.
- Add a read test for every prior supported state version and a fail-closed test for a future version.
- Exercise a real database restart and Sink acknowledgement gate for the affected connector.
- Test backup/restore and rollback on an isolated state file or PVC.
- Update the English section first, then the complete Simplified Chinese translation.
- Require green CI, a DCO-signed commit, and a reviewed migration note before publishing.

## 简体中文

本文定义当前 alpha 阶段支持的升级行为，原则是保守处理：遇到未知未来状态时拒绝打开，而不是猜测、跳过或覆盖。

### 版本清单

| 契约 | 当前版本 | 读取行为 |
|---|---:|---|
| 原生配置 API | `rustium.io/v1alpha1` | 严格校验 YAML/properties，未知字段失败 |
| SQLite storage schema (`PRAGMA user_version`) | `1` | version `0` 初始化到 `1`，未来版本拒绝且不降级 |
| JSON `Checkpoint.schema_version` | `2` | version `1` 在连接器契约允许时仍可读取 |
| PostgreSQL schema-history envelope | `5` | 可读取 `1` 到 `5`；没有 history 的旧 checkpoint fail-closed |
| MySQL schema-history envelope | `3` | 可读取 `1` 到 `3`；v2 offset progress 可安全重放 |
| SQL Server connector-state envelope | `1` | 只接受 version `1` |

SQLite storage version 与 JSON checkpoint version 相互独立。Storage migration 绝不能静默改写未来的 `PRAGMA user_version`。Connector-state version 按格式分别校验，Source 恢复前必须通过验证。

### 每次升级前

1. 阅读 release notes，并将上表版本与目标 binary 对照。
2. 优雅停止 connector，等待 `STOPPED`。
3. 备份 SQLite 数据库或创建 CSI VolumeSnapshot，记录 hash、image digest、Chart values、配置 fingerprint、源位点和数据库 slot/binlog/CDC 标识。
4. 使用目标 binary 和原配置运行 `rustium validate`，先解决能力或兼容性错误。
5. 确认目标 binary 保持相同 connector name、源身份、topic prefix、state path 和 Sink durability 设置。

### 配置迁移

公共原生 API 是 `rustium.io/v1alpha1`。Debezium 风格 properties 会解析到同一个版本化模型。Rustium 不会自动重写配置文件；新增已识别参数可以带默认值，未知字段和不安全兼容选项会校验失败。保存升级前配置，并在 release notes 中记录重命名 Debezium 参数的映射。

配置 fingerprint 有意覆盖源身份、选中集合、snapshot 行为、格式和路由；密码与运维调优不影响 fingerprint。已有 checkpoint 上改变这些 fingerprint 字段会被拒绝。必须先停止 connector，确认新值下源位点仍有效，否则恢复兼容配置，或按文档 reset 并执行新 snapshot。

新增的 `snapshot.include_collections` 字段默认是空列表；为空时不会进入 semantic fingerprint 材料，从而保持该字段出现前生成的 fingerprint。非空列表会改变 initial 和 `when_needed` 恢复快照行为，因此必须计入 fingerprint，并通过正常的配置变更审查流程引入。

新增的 PostgreSQL 原生字段 `source.publication_autocreate_mode` 默认值为 `disabled`，该值不会进入 semantic fingerprint 材料，从而保持旧原生配置的 fingerprint 和 publication 所有权。Debezium properties 的 `publication.autocreate.mode` 有意默认到 `all_tables`，与 Debezium 一致，而不是使用原生兼容默认值。任何非 `disabled` 模式都会改变 fingerprint。在已有 connector 上启用前，需要校验数据库 `CREATE`、表 ownership 和 superuser 要求，并审查 `filtered` 是否可能替换当前表级 publication 集合。

新增的 PostgreSQL `source.replica_identity_autoset_values` 列表默认为空，空列表不会进入 semantic fingerprint 材料。非空列表会改变 fingerprint，并让 source validation 执行事务化表 DDL。引入或修改规则前，应停止 connector，确认 table ownership 和 replica index 约束，审查下游 consumer 对 UPDATE/DELETE key 与 before image 的兼容性，并在受控变更窗口执行 validation。重叠规则会在修改前失败；SQL 或权限失败会回滚整组规则。

### Checkpoint 迁移

Checkpoint JSON version 2 新增可选 connector-state envelope；没有该字段的 version 1 JSON 仍可读。PostgreSQL 和 MySQL 没有持久 schema history 时无法安全恢复已完成的 version 1 checkpoint，因此会 fail-closed，要求备份、reset 和新的 initial snapshot。SQL Server 已验证的恢复路径不依赖 connector state，只要源 cursor 和 CDC retention 有效，仍可读取 version 1。

SQLite storage schema 由 `rustium-state` 初始化和迁移。当前 version 1 只负责创建 `checkpoints` table 并设置 `PRAGMA user_version`。如果数据库报告的 version 高于 binary 支持版本，会在任何降级前拒绝打开。不要手工修改 `user_version`，也不要在不同 connector identity 之间复制 checkpoint。

### Connector-state 迁移

- PostgreSQL schema history 可读取 version 1 到 5，并为新增字段使用默认值；当前状态保存 table layout、类型 metadata、增量 keyset progress、pause 状态和有界 completed-signal history。
- MySQL schema history 可读取 version 1 到 3。Version 2 offset progress 仍可读，并从集合开头安全重放；version 3 持久化带类型 keyset progress 和 completed signal ID。
- SQL Server connector state 当前只接受 version 1，未知 version 会在 CDC polling 前失败。

不要手工编辑 JSON payload。Malformed、重复、未知 format 或未知 version 都是 state error，应恢复有效备份，或 reset 后执行新 snapshot。

### 滚动升级流程

Rustium 是单 owner 进程，不是 active/active service。使用 stop、backup、replace、start 顺序：

1. 按数据库 retention 窗口要求 drain 或暂停上游运维变更。
2. 停止旧 binary，确认最终 checkpoint 已确认。
3. 备份 state，记录旧 image digest。
4. 用相同 state PVC 和 connector 配置部署新 binary 或 Helm image。
5. 执行 `rustium validate`，然后只启动一个副本。
6. 检查源位点、connector-state version、Sink 确认、lag 和升级后第一批事件顺序。
7. 新 binary 经过正常 retention 和 replay 观察窗口后再删除备份。

不要让两个副本访问同一 state path。Helm 的 `Recreate` 策略正是为此设置。如果新 binary 已写入 checkpoint，除非 release 明确保证反向读取兼容，否则不要原地 rollback；应停止新 binary，恢复升级前 state backup 和 image，从旧 checkpoint 继续。

### Reset 迁移

只有先备份 state 并证明源连续性不可恢复后，才使用 `rustium state reset --config <path> --confirm`。Reset 不是通用升级工具。Reset 后按 connector ownership 重建或重新分配源资源，强制 initial snapshot，验证行数和首次顺序，并记录新的 source anchor。

### Release author 清单

- 每次 schema 或配置变更都更新版本清单和 release notes。
- 每个历史支持版本增加 read test，并增加未来版本 fail-closed test。
- 对受影响连接器执行真实数据库重启和 Sink acknowledgement gate。
- 在隔离 state 文件或 PVC 上测试 backup/restore 和 rollback。
- 先更新完整英文，再更新对应的完整简体中文翻译。
- 发布前要求 CI 全绿、commit 带 DCO，并有经过审查的 migration note。
