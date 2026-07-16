# rustium-signal-kafka

Checkpoint-coupled Kafka signal channel compatible with Debezium signal topic and connector-key semantics. Offsets are committed only after the matching connector state is durably acknowledged.

See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) for signal configuration and crash-window behavior.

## 简体中文

与 Debezium signal topic 和 connector key 语义兼容的 Kafka 信号 channel。只有对应 connector state 被持久确认后才提交 offset。

信号配置和崩溃窗口行为请参阅[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)。
