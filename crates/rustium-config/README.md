# rustium-config

Versioned Rustium configuration models, validation, environment interpolation, semantic fingerprints, and Debezium-compatible `.properties` parsing for PostgreSQL, MySQL, SQL Server, sinks, formats, and runtime settings.

Use this crate when an embedded Rustium application needs the same strict configuration contract as the CLI. See the [project README](https://github.com/ulnit/rustium/blob/main/README.md).

`snapshot.include.collection.list` maps to native `snapshot.include_collections` with anchored, connector-qualified, snapshot-only matching.

PostgreSQL `publication.autocreate.mode` supports `disabled`, `all_tables`, `filtered`, and `no_tables`. Debezium properties default to `all_tables`; native `source.publication_autocreate_mode` defaults to `disabled` for backward-compatible ownership and fingerprints.

## 简体中文

Rustium 的版本化配置模型、校验、环境变量插值、语义指纹，以及 PostgreSQL、MySQL、SQL Server、sink、格式和 runtime 的 Debezium 兼容 `.properties` 解析。

嵌入 Rustium 的应用需要与 CLI 相同的严格配置契约时使用此 crate。详见[项目 README](https://github.com/ulnit/rustium/blob/main/README.md)。

`snapshot.include.collection.list` 映射为原生 `snapshot.include_collections`，采用 anchored、连接器限定且仅作用于快照的匹配语义。

PostgreSQL `publication.autocreate.mode` 支持 `disabled`、`all_tables`、`filtered` 和 `no_tables`。Debezium properties 默认使用 `all_tables`；原生 `source.publication_autocreate_mode` 默认使用 `disabled`，以保持向后兼容的所有权和 fingerprint。
