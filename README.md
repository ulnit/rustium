# Rustium

**Change Data Capture, reimagined in Rust.**

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![Status: pre-alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)

[English](#english) | [简体中文](#简体中文)

> **Project status:** Rustium is currently in the design and bootstrap stage. This repository does not yet contain a runnable binary or published package. Features described below are targets unless explicitly marked as implemented.

> **Documentation policy:** Rustium documentation is written in English and Simplified Chinese, with English first. See the [architecture and design document](docs/design.md) for the normative engineering baseline.

---

## English

### What is Rustium?

Rustium is an independently implemented, open-source, log-based Change Data Capture (CDC) platform written in Rust. It is designed to read committed changes from database replication logs and deliver ordered change events to downstream systems with explicit recovery and delivery semantics.

The first release is planned as a standalone service distributed as a single binary. It will not require a JVM or Kafka Connect. Kafka is an optional destination rather than a runtime requirement.

Rustium is guided by four priorities:

- **Correctness:** source positions advance only after durable sink acknowledgement.
- **Operational clarity:** lag, failures, checkpoints, backpressure, and retained-log risk are observable.
- **Bounded resources:** queues are bounded and pressure propagates to the source.
- **Ecosystem adoption:** a tested Debezium-compatible JSON encoder reduces migration work without coupling Rustium's core model to Debezium.

### Current Status

Rustium is **pre-alpha**. The architecture is being established before implementation begins.

| Area | Status |
|---|---|
| Architecture and MVP contract | Drafted |
| Apache-2.0 license | Available |
| Cargo workspace and runtime | Planned |
| PostgreSQL connector | Planned |
| stdout and Kafka sinks | Planned |
| SQLite checkpoint store | Planned |
| CLI, HTTP status, and metrics | Planned |
| Packages, images, and releases | Not available |

There are currently no valid `cargo install`, Docker, Helm, or production deployment instructions. They will be added only after the corresponding artifacts are built and tested.

### Planned MVP

The `0.1.0` target is a focused PostgreSQL-to-stdout/Kafka vertical slice:

| Capability | Initial target |
|---|---|
| Source | PostgreSQL 14+ logical replication with `pgoutput` |
| Capture | Consistent initial snapshot followed by streaming |
| Sinks | stdout for development and Kafka for durable delivery |
| Formats | Versioned Rustium JSON and Debezium-compatible JSON |
| State | Transactional SQLite checkpoints |
| Runtime | Tokio; one connector per process |
| Operations | CLI, graceful shutdown, retries, health/status API, Prometheus metrics |
| Delivery | At-least-once with deterministic event identifiers |
| Packaging | Standalone binary and container image |

MySQL, incremental snapshots, Schema Registry, Avro, Protobuf, more sinks, a Kubernetes Operator, embedded mode, and multi-connector operation are later milestones.

### Architecture

```text
                              Control plane
                   config | lifecycle | health | metrics
                                   |
                                   v
  PostgreSQL -> Decode -> Normalize -> Filter -> Transform -> Route
       |                                                        |
       |                                                        v
       +-> snapshot / WAL position                    Encode -> Batch
                                                                |
                                                                v
                                                              Sink
                                                                |
                                                        durable ack
                                                                |
                                                                v
                                                       SQLite checkpoint
```

Rustium keeps a database-neutral, typed `ChangeEvent` inside the pipeline. Connectors own source positions; encoders own external formats; sinks own delivery acknowledgements. This separation prevents a Kafka or JSON-specific contract from defining the entire runtime.

The complete design, including snapshot handoff, PostgreSQL replication feedback, checkpoint ordering, error policy, configuration, security, testing gates, and roadmap, is in [docs/design.md](docs/design.md).

### Delivery Contract

The initial delivery guarantee is **at-least-once**, not exactly-once.

For each ordered batch, Rustium will:

1. deliver the events to the sink;
2. wait for the sink's configured durable acknowledgement;
3. atomically store the highest fully acknowledged source position;
4. then allow the source to release that position.

A crash after sink acknowledgement but before checkpoint persistence can replay events. Event identifiers are deterministic so consumers can deduplicate. Rustium will not advance a source position merely to reduce replication-log retention.

### Debezium Compatibility

Rustium is inspired by the proven event semantics used in the CDC ecosystem and plans a Debezium-compatible JSON encoder with familiar fields such as `before`, `after`, `source`, `op`, and `ts_ms`.

Compatibility is deliberately scoped:

- Rustium will publish a field-by-field compatibility matrix.
- Each compatibility claim must have golden event tests.
- Native Rustium events remain versioned independently.
- Full Debezium configuration, connector, SMT, and Kafka Connect compatibility is not an MVP goal.

Rustium is not affiliated with, endorsed by, or a fork of the Debezium project or Red Hat. Debezium is referenced only to describe interoperability goals.

### Roadmap

#### Phase 0: Foundation

- Create the Cargo workspace, CI, contribution guide, security policy, and code of conduct.
- Implement core event, connector, sink, checkpoint, and configuration contracts.

#### Phase 1: PostgreSQL `0.1.0`

- Add `pgoutput` streaming and a consistent initial snapshot.
- Add JSON encoders, stdout/Kafka sinks, SQLite state, and operational endpoints.
- Publish integration tests, examples, a container image, and a compatibility matrix.

#### Phase 2: Reliability and schema

- Add incremental snapshots, schema evolution, Schema Registry, dead-letter policies, and broader PostgreSQL type coverage.
- Publish reproducible benchmarks and operational runbooks.

#### Phase 3+: Ecosystem and production scale

- Add MySQL, more formats and sinks, and evaluate embedded or multi-connector operation.
- Stabilize upgrade, migration, security, and disaster-recovery guarantees before `1.0`.

Roadmap versions are targets, not promises. A capability is complete only after its release gates pass.

### Contributing

The most useful early contributions are design review, protocol research, compatibility fixtures, and implementation of the Phase 0 foundation. Contributions should follow these rules:

- Discuss changes to delivery semantics, persisted state, public events, or configuration before implementation.
- Add an Architecture Decision Record under `docs/adr/` for changes to an accepted architectural contract.
- Keep user-facing documentation bilingual, with the complete English section first and Simplified Chinese second.
- Use English for code, identifiers, configuration keys, logs, commit messages, and issue titles.
- Add tests for behavior and failure recovery, not only the success path.
- Sign commits with a `Signed-off-by` line under the Developer Certificate of Origin (DCO).

A detailed `CONTRIBUTING.md` will be added during Phase 0.

### Documentation

- [Architecture and Design](docs/design.md)
- [Apache License 2.0](LICENSE)

The files ending in `(参考)` are source references retained during project bootstrap. They are not statements of current project capability. This README and `docs/design.md` are the active documentation baseline.

### License

Rustium is licensed under the [Apache License 2.0](LICENSE).

---

## 简体中文

### Rustium 是什么？

Rustium 是一个使用 Rust 独立实现的开源、基于数据库日志的变更数据捕获（CDC）平台。它计划从数据库复制日志读取已提交变更，并以明确的恢复和投递语义，将有序变更事件发送到下游系统。

第一个版本计划作为单一二进制形式的独立服务发布，不依赖 JVM 或 Kafka Connect。Kafka 是可选目标，而不是运行时必需组件。

Rustium 遵循四个核心优先级：

- **正确性：** 只有 Sink 持久确认后才推进源位点。
- **运维清晰：** 延迟、故障、checkpoint、背压和日志保留风险都可观测。
- **资源有界：** 队列容量有上限，压力会向 Source 传播。
- **生态接入：** 通过经过测试的 Debezium 兼容 JSON Encoder 降低迁移成本，同时避免内部核心模型与 Debezium 耦合。

### 当前状态

Rustium 当前处于 **pre-alpha** 阶段，项目会先建立架构基线，再进入实现。

| 领域 | 状态 |
|---|---|
| 架构与 MVP 契约 | 已形成草案 |
| Apache-2.0 许可证 | 已提供 |
| Cargo workspace 与运行时 | 计划中 |
| PostgreSQL Connector | 计划中 |
| stdout 和 Kafka Sink | 计划中 |
| SQLite checkpoint 存储 | 计划中 |
| CLI、HTTP 状态和指标 | 计划中 |
| 包、镜像和 Release | 尚不可用 |

目前没有有效的 `cargo install`、Docker、Helm 或生产部署说明。只有对应产物完成并通过测试后，项目才会添加这些说明。

### 计划中的 MVP

`0.1.0` 的目标是完成一个聚焦的 PostgreSQL 到 stdout/Kafka 垂直切片：

| 能力 | 初始目标 |
|---|---|
| Source | PostgreSQL 14+，使用 `pgoutput` 逻辑复制 |
| 捕获 | 一致性初始快照，然后进入流式捕获 |
| Sink | 用于开发的 stdout，以及用于持久投递的 Kafka |
| 格式 | 版本化 Rustium JSON 和 Debezium 兼容 JSON |
| 状态 | 事务性 SQLite checkpoint |
| 运行时 | Tokio；每个进程运行一个连接器 |
| 运维 | CLI、优雅关闭、重试、健康/状态 API、Prometheus 指标 |
| 投递 | at-least-once，并提供确定性事件 ID |
| 打包 | 独立二进制和容器镜像 |

MySQL、增量快照、Schema Registry、Avro、Protobuf、更多 Sink、Kubernetes Operator、嵌入式模式和多连接器运行属于后续里程碑。

### 架构

```text
                              控制平面
                     配置 | 生命周期 | 健康 | 指标
                                   |
                                   v
  PostgreSQL -> 解码 -> 规范化 -> 过滤 -> 转换 -> 路由
       |                                              |
       |                                              v
       +-> 快照 / WAL 位点                         编码 -> 批处理
                                                          |
                                                          v
                                                         Sink
                                                          |
                                                       持久确认
                                                          |
                                                          v
                                                   SQLite checkpoint
```

Rustium 在流水线内部使用与数据库无关的强类型 `ChangeEvent`。Connector 管理源位点，Encoder 管理外部格式，Sink 管理投递确认。这种分层可以避免由 Kafka 或 JSON 的特定契约定义整个运行时。

完整设计包括快照切换、PostgreSQL 复制反馈、checkpoint 顺序、错误策略、配置、安全、测试门槛和路线图，详见 [docs/design.md](docs/design.md)。

### 投递契约

初始投递保证是 **at-least-once**，不是 exactly-once。

对于每个有序批次，Rustium 将：

1. 向 Sink 投递事件；
2. 等待 Sink 配置的持久确认；
3. 原子存储最高的、完全确认的源位点；
4. 然后允许 Source 释放该位点。

Sink 确认后、checkpoint 持久化前发生崩溃可能导致事件重放。事件 ID 是确定性的，因此消费者可以去重。Rustium 不会仅为了减少复制日志保留量而推进源位点。

### Debezium 兼容性

Rustium 借鉴 CDC 生态中经过验证的事件语义，并计划提供 Debezium 兼容 JSON Encoder，包含 `before`、`after`、`source`、`op` 和 `ts_ms` 等常用字段。

兼容范围会被严格限定：

- Rustium 将发布逐字段兼容性矩阵。
- 每项兼容声明都必须有 golden event test。
- Rustium 原生事件独立进行版本管理。
- 完整 Debezium 配置、连接器、SMT 和 Kafka Connect 兼容不属于 MVP 目标。

Rustium 与 Debezium 项目或 Red Hat 没有从属、授权或 fork 关系。文档仅为描述互操作目标而引用 Debezium。

### 路线图

#### Phase 0：基础

- 创建 Cargo workspace、CI、贡献指南、安全策略和行为准则。
- 实现核心事件、Connector、Sink、checkpoint 和配置契约。

#### Phase 1：PostgreSQL `0.1.0`

- 实现 `pgoutput` 流式捕获和一致性初始快照。
- 实现 JSON Encoder、stdout/Kafka Sink、SQLite 状态和运维端点。
- 发布集成测试、示例、容器镜像和兼容性矩阵。

#### Phase 2：可靠性与 Schema

- 增加增量快照、Schema 演进、Schema Registry、死信策略和更完整的 PostgreSQL 类型覆盖。
- 发布可复现基准测试和运维手册。

#### Phase 3+：生态与生产规模

- 增加 MySQL、更多格式和 Sink，并评估嵌入式或多连接器运行方式。
- 在 `1.0` 之前稳定升级、迁移、安全和灾难恢复保证。

路线图版本是目标，不是承诺。只有通过发布门槛后，相应能力才算完成。

### 参与贡献

项目早期最有价值的贡献包括设计评审、协议研究、兼容性 fixture 和 Phase 0 基础实现。贡献应遵守以下规则：

- 修改投递语义、持久状态、公开事件或配置前先进行讨论。
- 修改已接受的架构契约时，在 `docs/adr/` 下添加 Architecture Decision Record。
- 所有面向用户的文档都必须提供英文和简体中文，先放完整英文，再放简体中文。
- 代码、标识符、配置键、日志、commit message 和 issue 标题使用英文。
- 测试必须覆盖行为和故障恢复，不能只覆盖成功路径。
- 按 Developer Certificate of Origin（DCO）要求，在 commit 中添加 `Signed-off-by`。

详细的 `CONTRIBUTING.md` 将在 Phase 0 添加。

### 文档

- [架构与设计](docs/design.md)
- [Apache License 2.0](LICENSE)

文件名以 `(参考)` 结尾的文档是项目初始化期间保留的素材，不代表项目当前能力。此 README 和 `docs/design.md` 是当前有效的文档基线。

### 许可证

Rustium 使用 [Apache License 2.0](LICENSE)。
