# rustium-sink-kafka

Durable Kafka sink for Rustium with idempotent producers, `acks=all`, retry classification, tombstones, Confluent Schema Registry framing, bounded schema-ID caches, and checkpoint-safe delivery.

See the [project README](https://github.com/ulnit/rustium/blob/main/README.md) for configuration and delivery guarantees.

## 简体中文

Rustium 的持久 Kafka sink，提供幂等 producer、`acks=all`、重试分类、tombstone、Confluent Schema Registry framing、有界 schema-ID cache 和 checkpoint 安全投递。

配置和投递保证请参阅[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)。
