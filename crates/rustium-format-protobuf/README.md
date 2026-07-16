# rustium-format-protobuf

Debezium-compatible Protobuf encoding for Rustium with deterministic field numbers, typed row wrappers, schema evolution, null and unavailable values, and Confluent message framing support.

Use it with `rustium-sink-kafka` and a Confluent-compatible Schema Registry. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md).

Checked-in PostgreSQL, MySQL, and SQL Server fixtures protect complete key/value subjects, field numbers, and Protobuf definitions.

## 简体中文

Rustium 的 Debezium 兼容 Protobuf 编码器，提供确定性字段编号、类型化行 wrapper、schema 演进、null 与 unavailable 值，以及 Confluent message framing 支持。

与 `rustium-sink-kafka` 及 Confluent 兼容 Schema Registry 一起使用。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)。

仓库内置 PostgreSQL、MySQL 和 SQL Server fixture，用于保护完整的 key/value subject、field number 与 Protobuf 定义。
