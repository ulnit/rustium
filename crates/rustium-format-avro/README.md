# rustium-format-avro

Debezium-compatible Avro event encoding for Rustium, including deterministic schemas, source metadata, transactions, schema evolution, and delete tombstones.

Use it with `rustium-sink-kafka` and a Confluent-compatible Schema Registry. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md).

Checked-in PostgreSQL, MySQL, and SQL Server fixtures protect complete key/value subjects and Avro definitions.

## 简体中文

Rustium 的 Debezium 兼容 Avro 事件编码，包含确定性 schema、source 元数据、事务、schema 演进和 delete tombstone。

与 `rustium-sink-kafka` 及 Confluent 兼容 Schema Registry 一起使用。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)。

仓库内置 PostgreSQL、MySQL 和 SQL Server fixture，用于保护完整的 key/value subject 与 Avro 定义。
