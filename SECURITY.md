# Rustium Security Policy

## English

### Supported versions

| Version line | Security support |
|---|---|
| Latest tagged release | Supported according to the release notes |
| `main` | Best effort while the project is alpha |
| Older alpha releases | Upgrade to the latest release before requesting a fix |

Rustium is still pre-1.0. A security fix may require a checkpoint, configuration, or event-contract migration. The upgrade procedure in [docs/upgrades.md](docs/upgrades.md) is part of the security response.

### Reporting a vulnerability

Report vulnerabilities privately through [GitHub Security Advisories](https://github.com/ulnit/rustium/security/advisories/new). Do not open a public issue, include credentials in a report, or paste unredacted database/Kafka/Registry logs. Include the affected commit or version, deployment mode, connector, a minimal reproduction, and the impact. Encrypt any sensitive reproduction separately through the private advisory channel.

The maintainers target acknowledgement within three business days and an initial triage decision within fourteen calendar days. These are response targets, not a guarantee. A coordinated disclosure date is agreed with the reporter after a fix or mitigation is available.

### Scope and threat model

Rustium handles database change records, database credentials, Kafka credentials, Schema Registry credentials, source positions, schema history, and management signals. The management API can stop a connector or submit a signal only when mutations are explicitly enabled. Debezium bridge sources also accept CDC envelopes over HTTP or Kafka. A threat actor who can write the SQLite checkpoint or Debezium recovery files, impersonate the source database, publish to a bridge input topic, reach a bridge HTTP endpoint, or reach an enabled mutating management endpoint can alter delivery behavior; filesystem, database, Kafka, HTTP, and Kubernetes boundaries must therefore be protected as trusted infrastructure.

The project does not claim end-to-end encryption for data written to `stdout`, exactly-once delivery across arbitrary sinks, or protection from a compromised host or container runtime. Consumers must protect and deduplicate the records they receive.

### Secure deployment defaults

- Keep `server.bind` on loopback unless the management endpoint is isolated by a firewall or Kubernetes NetworkPolicy.
- Keep `server.enable_mutations=false` unless an authenticated, private control path is required.
- Keep `source.bridge.listen` on loopback for managed Debezium Server. For external HTTP producers, use `source.bridge.authentication_token`, a private network, and a TLS-terminating authenticated reverse proxy; the native bridge endpoint is HTTP and must not be exposed directly to an untrusted network.
- Give Kafka bridge consumer groups read access only to the configured Debezium input topics, and give upstream producers write access only to those topics. Reject untrusted producers with broker authentication and ACLs; a validly shaped forged envelope is treated as source data.
- Use `verify_identity` or the strongest available TLS mode for PostgreSQL, MySQL, SQL Server, Kafka, and Schema Registry. Do not set `trust_server_certificate` or equivalent bypasses in production.
- Put passwords, private keys, truststore passwords, and Schema Registry credentials in an external secret manager or Kubernetes Secret. Use `${NAME}` interpolation and never commit resolved values.
- Grant CDC accounts only the replication, metadata, snapshot, and signal permissions required by the selected connector. Do not use a database superuser for normal capture.
- Run the container as the chart's non-root UID/GID `65532`, keep the root filesystem read-only, and retain the checkpoint PVC with storage encryption and access controls.
- Require Kafka `acks=all` or `-1`, TLS/SASL where applicable, and topic ACLs that restrict the connector to its topic and signal topic.
- Treat the SQLite checkpoint and connector schema history as sensitive operational state. Back it up before reset or upgrade and restrict access to the connector identity.
- For a managed Debezium bridge, place `offset_file` and `schema_history_file` on the same protected persistent volume as SQLite and back up all three atomically. Generated Debezium configuration files are created with mode `0600` on Unix, but the process environment, temporary directory, recovery files, and inherited child-process output still require host-level isolation and log redaction.
- Pin image tags to a digest in production, review lockfile changes, and require passing CI plus a DCO-signed commit before release.

Rustium interpolates secrets at load time and excludes them from configuration fingerprints, status, and metrics. Log level and sink choice still matter: `stdout` is a development sink and can expose row payloads to the container log system.

### Supply-chain and dependency reports

The repository runs locked workspace compilation, Clippy, unit tests, database gates, runtime soak, and container/Helm packaging on every push and pull request. The lockfile is committed. Dependency or base-image updates must be reviewed for licensing, CVEs, TLS behavior, and runtime linkage; a green build alone is not a security approval.

## 简体中文

### 支持版本

| 版本线 | 安全支持 |
|---|---|
| 最新 tagged release | 依据 release notes 提供支持 |
| `main` | 项目处于 alpha 阶段，尽力支持 |
| 更旧的 alpha release | 提交问题前先升级到最新版本 |

Rustium 尚未达到 `1.0`。安全修复可能需要 checkpoint、配置或事件契约迁移；[docs/upgrades.md](docs/upgrades.md) 中的升级流程属于安全响应的一部分。

### 漏洞报告

请通过 [GitHub Security Advisories](https://github.com/ulnit/rustium/security/advisories/new) 私下报告漏洞。不要创建公开 Issue，不要在报告中写入凭据，也不要粘贴未脱敏的数据库、Kafka 或 Registry 日志。请提供受影响的 commit 或版本、部署模式、连接器、最小复现和影响范围；敏感复现材料应通过私有 advisory channel 单独加密传递。

维护者目标是在三个工作日内确认收到，并在十四个自然日内完成首次分级判断。这是响应目标而不是保证。修复或缓解措施可用后，再与报告者协商协调披露日期。

### 范围与威胁模型

Rustium 处理数据库变更记录、数据库凭据、Kafka 凭据、Schema Registry 凭据、源位点、schema history 和管理信号。只有显式开启 mutations 后，管理 API 才能停止 connector 或提交 signal。Debezium bridge source 还会通过 HTTP 或 Kafka 接收 CDC envelope。能够写入 SQLite checkpoint 或 Debezium recovery file、冒充源数据库、向 bridge input topic 发布消息、访问 bridge HTTP endpoint，或访问已开启变更端点的攻击者可以改变投递行为；因此必须保护文件系统、数据库、Kafka、HTTP 和 Kubernetes 边界。

项目不声称为写入 `stdout` 的数据提供端到端加密，不声称任意 Sink 上的 exactly-once，也不防护已被攻陷的宿主机或容器运行时。Consumer 必须自行保护并去重收到的 record。

### 安全部署默认值

- 除非通过防火墙或 Kubernetes NetworkPolicy 隔离，否则保持 `server.bind` 为 loopback。
- 除非确实需要且控制路径已认证，否则保持 `server.enable_mutations=false`。
- Managed Debezium Server 应保持 `source.bridge.listen` 为 loopback。External HTTP producer 应同时使用 `source.bridge.authentication_token`、私有网络和负责 TLS termination 的认证反向代理；原生 bridge endpoint 使用 HTTP，不能直接暴露到不可信网络。
- Kafka bridge consumer group 只能读取配置的 Debezium input topic，上游 producer 也只能写入这些 topic。必须使用 broker authentication 与 ACL 排除不可信 producer；格式合法的伪造 envelope 会被当作 source data。
- PostgreSQL、MySQL、SQL Server、Kafka 和 Schema Registry 使用 `verify_identity` 或可用的最强 TLS 模式；生产环境不要设置 `trust_server_certificate` 或等价绕过。
- 密码、私钥、truststore 密码和 Registry 凭据放入外部 secret manager 或 Kubernetes Secret，使用 `${NAME}` 插值，绝不提交解析后的值。
- CDC 账号只授予选定连接器需要的 replication、元数据、snapshot 和 signal 权限；普通捕获不要使用数据库 superuser。
- 使用 Chart 的非 root UID/GID `65532`，保持根文件系统只读，并使用带加密和访问控制的 checkpoint PVC。
- Kafka 使用 `acks=all` 或 `-1`，按需启用 TLS/SASL，并用 topic ACL 把 connector 限制在业务 topic 和 signal topic。
- SQLite checkpoint 与 connector schema history 属于敏感运维状态；reset 或升级前备份，并限制只允许 connector 身份访问。
- Managed Debezium bridge 应把 `offset_file` 与 `schema_history_file` 放在和 SQLite 相同的受保护持久卷，并原子备份三者。Unix 上生成的 Debezium 配置文件权限为 `0600`，但 process environment、临时目录、recovery file 和继承的 child-process output 仍需宿主机隔离与日志脱敏。
- 生产环境把镜像 tag 固定到 digest，审查 lockfile 变化，并要求 CI 通过且 commit 带 DCO 后才发布。

Rustium 在加载时插入 secret，并将其排除在配置 fingerprint、status 和 metrics 之外。但 log level 和 Sink 仍然重要：`stdout` 是开发 Sink，可能把行 payload 暴露给容器日志系统。

### 供应链与依赖报告

仓库在每次 push 和 pull request 上运行 locked workspace 编译、Clippy、单元测试、数据库门槛、runtime soak 以及容器/Helm packaging。Lockfile 已提交。依赖或基础镜像更新必须审查许可证、CVE、TLS 行为和运行时链接；绿色构建本身不是安全批准。
