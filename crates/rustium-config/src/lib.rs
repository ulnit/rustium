//! Strict, versioned Rustium configuration.

mod debezium;

use std::{collections::BTreeMap, env, fs, path::Path, time::Duration};

use regex::Regex;
use rustium_core::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

const API_VERSION: &str = "rustium.io/v1alpha1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub source: SourceConfig,
    #[serde(default)]
    pub snapshot: SnapshotConfig,
    #[serde(default)]
    pub format: FormatConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub runtime: RuntimeSettings,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(skip)]
    pub compatibility_warnings: Vec<String>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)?;
        if path
            .extension()
            .is_some_and(|extension| extension == "properties")
            || !raw
                .lines()
                .any(|line| line.trim_start().starts_with("api_version:"))
        {
            Self::from_debezium_properties(&raw)
        } else {
            Self::from_yaml(&raw)
        }
    }

    pub fn from_yaml(raw: &str) -> Result<Self> {
        let interpolated = interpolate_environment(raw)?;
        let config: Self = serde_yaml::from_str(&interpolated)
            .map_err(|error| Error::Configuration(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_debezium_properties(raw: &str) -> Result<Self> {
        debezium::parse(raw)
    }

    pub fn validate(&self) -> Result<()> {
        if self.api_version != API_VERSION {
            return Err(Error::Configuration(format!(
                "unsupported api_version {:?}; expected {API_VERSION:?}",
                self.api_version
            )));
        }
        if self.kind != "Connector" {
            return Err(Error::Configuration(
                "kind must be exactly \"Connector\"".into(),
            ));
        }
        validate_name(&self.metadata.name, "metadata.name")?;
        self.source.validate()?;
        self.sink.validate()?;
        if self.runtime.channel_capacity == 0 {
            return Err(Error::Configuration(
                "runtime.channel_capacity must be greater than zero".into(),
            ));
        }
        if self.runtime.max_batch_size == 0
            || self.runtime.max_batch_size > self.runtime.channel_capacity
        {
            return Err(Error::Configuration(
                "runtime.max_batch_size must be between 1 and channel_capacity".into(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn fingerprint(&self) -> String {
        let semantic = serde_json::json!({
            "api_version": self.api_version,
            "name": self.metadata.name,
            "source": self.source.semantic_config(),
            "snapshot": self.snapshot,
            "format": self.format,
            "sink": self.sink.semantic_config(),
        });
        let bytes = serde_json::to_vec(&semantic).expect("configuration serialization cannot fail");
        hex_digest(&bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub name: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Postgresql(PostgresSourceConfig),
    Mysql(MySqlSourceConfig),
    Sqlserver(SqlServerSourceConfig),
}

impl SourceConfig {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Postgresql(config) => config.validate(),
            Self::Mysql(config) => config.validate(),
            Self::Sqlserver(config) => config.validate(),
        }
    }

    #[must_use]
    pub fn as_postgresql(&self) -> Option<&PostgresSourceConfig> {
        match self {
            Self::Postgresql(config) => Some(config),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_mysql(&self) -> Option<&MySqlSourceConfig> {
        match self {
            Self::Mysql(config) => Some(config),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_sqlserver(&self) -> Option<&SqlServerSourceConfig> {
        match self {
            Self::Sqlserver(config) => Some(config),
            _ => None,
        }
    }

    fn semantic_config(&self) -> serde_json::Value {
        match self {
            Self::Postgresql(config) => serde_json::json!({
                "type": "postgresql",
                "hostname": config.hostname,
                "port": config.port,
                "database": config.database,
                "publication": config.publication,
                "slot_name": config.slot_name,
                "tables": config.tables,
            }),
            Self::Mysql(config) => serde_json::json!({
                "type": "mysql",
                "hostname": config.hostname,
                "port": config.port,
                "databases": config.databases,
                "server_id": config.server_id,
                "tables": config.tables,
                "schema_history_skip_unparseable_ddl": config.schema_history_skip_unparseable_ddl,
            }),
            Self::Sqlserver(config) => serde_json::json!({
                "type": "sqlserver",
                "hostname": config.hostname,
                "port": config.port,
                "databases": config.databases,
                "tables": config.tables,
                "encrypt": config.encrypt,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    #[serde(default = "default_hostname")]
    pub hostname: String,
    #[serde(default = "default_postgres_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub publication: String,
    #[serde(default = "default_slot_name")]
    pub slot_name: String,
    #[serde(default)]
    pub slot_ownership: SlotOwnership,
    #[serde(default)]
    pub tables: TableSelection,
    #[serde(default = "default_ssl_mode")]
    pub ssl_mode: String,
    #[serde(default = "default_connect_timeout")]
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
}

impl PostgresSourceConfig {
    fn validate(&self) -> Result<()> {
        for (value, field) in [
            (&self.database, "source.database"),
            (&self.username, "source.username"),
            (&self.publication, "source.publication"),
            (&self.slot_name, "source.slot_name"),
        ] {
            if value.trim().is_empty() {
                return Err(Error::Configuration(format!("{field} must not be empty")));
            }
        }
        validate_name(&self.slot_name, "source.slot_name")?;
        validate_name(&self.publication, "source.publication")?;
        for pattern in self.tables.include.iter().chain(self.tables.exclude.iter()) {
            if Regex::new(pattern).is_err() {
                return Err(Error::Configuration(format!(
                    "table selector {pattern:?} is not a valid regular expression"
                )));
            }
        }
        Ok(())
    }

    pub fn connection_url(&self, replication: bool) -> Result<String> {
        let mut url = Url::parse("postgresql://localhost")
            .map_err(|error| Error::Configuration(error.to_string()))?;
        url.set_host(Some(&self.hostname))
            .map_err(|_| Error::Configuration("invalid source.hostname".into()))?;
        url.set_port(Some(self.port))
            .map_err(|_| Error::Configuration("invalid source.port".into()))?;
        url.set_username(&self.username)
            .map_err(|_| Error::Configuration("invalid source.username".into()))?;
        url.set_password(Some(&self.password))
            .map_err(|_| Error::Configuration("invalid source.password".into()))?;
        url.set_path(&self.database);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("sslmode", &self.ssl_mode);
            query.append_pair(
                "connect_timeout",
                &self.connect_timeout.as_secs().max(1).to_string(),
            );
            if replication {
                query.append_pair("replication", "database");
            }
        }
        Ok(url.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlSourceConfig {
    #[serde(default = "default_hostname")]
    pub hostname: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub databases: Vec<String>,
    #[serde(default = "default_mysql_server_id")]
    pub server_id: u32,
    #[serde(default)]
    pub tables: TableSelection,
    #[serde(default = "default_mysql_ssl_mode")]
    pub ssl_mode: String,
    #[serde(default = "default_connect_timeout")]
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(default = "default_true")]
    pub connect_keep_alive: bool,
    #[serde(default = "default_mysql_keep_alive_interval")]
    #[serde(with = "humantime_serde")]
    pub connect_keep_alive_interval: Duration,
    #[serde(default = "default_mysql_reconnect_max_attempts")]
    pub reconnect_max_attempts: u32,
    #[serde(default)]
    pub schema_history_skip_unparseable_ddl: bool,
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(default = "default_heartbeat_topics_prefix")]
    pub heartbeat_topics_prefix: String,
    #[serde(default)]
    pub heartbeat_topic_name: Option<String>,
}

impl MySqlSourceConfig {
    fn validate(&self) -> Result<()> {
        if self.username.trim().is_empty() {
            return Err(Error::Configuration(
                "source.username must not be empty".into(),
            ));
        }
        if self.server_id == 0 {
            return Err(Error::Configuration(
                "source.server_id must be greater than zero".into(),
            ));
        }
        if self.connect_keep_alive_interval.is_zero() {
            return Err(Error::Configuration(
                "source.connect_keep_alive_interval must be greater than zero".into(),
            ));
        }
        if self.connect_keep_alive && self.reconnect_max_attempts == 0 {
            return Err(Error::Configuration(
                "source.reconnect_max_attempts must be greater than zero when connect_keep_alive is enabled"
                    .into(),
            ));
        }
        if self.heartbeat_topics_prefix.trim().is_empty() {
            return Err(Error::Configuration(
                "source.heartbeat_topics_prefix must not be empty".into(),
            ));
        }
        if self
            .heartbeat_topic_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(Error::Configuration(
                "source.heartbeat_topic_name must not be empty when set".into(),
            ));
        }
        if !matches!(
            self.ssl_mode.as_str(),
            "disabled" | "preferred" | "required" | "verify_ca" | "verify_identity"
        ) {
            return Err(Error::Configuration(
                "source.ssl_mode must be one of disabled, preferred, required, verify_ca, or verify_identity"
                    .into(),
            ));
        }
        if self
            .databases
            .iter()
            .any(|database| database.trim().is_empty())
        {
            return Err(Error::Configuration(
                "source.databases must not contain empty names".into(),
            ));
        }
        validate_table_patterns(&self.tables)
    }

    pub fn connection_url(&self) -> Result<String> {
        let mut url = Url::parse("mysql://localhost")
            .map_err(|error| Error::Configuration(error.to_string()))?;
        url.set_host(Some(&self.hostname))
            .map_err(|_| Error::Configuration("invalid source.hostname".into()))?;
        url.set_port(Some(self.port))
            .map_err(|_| Error::Configuration("invalid source.port".into()))?;
        url.set_username(&self.username)
            .map_err(|_| Error::Configuration("invalid source.username".into()))?;
        url.set_password(Some(&self.password))
            .map_err(|_| Error::Configuration("invalid source.password".into()))?;
        Ok(url.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlServerSourceConfig {
    #[serde(default = "default_hostname")]
    pub hostname: String,
    #[serde(default = "default_sqlserver_port")]
    pub port: u16,
    pub username: String,
    pub password: String,
    pub databases: Vec<String>,
    #[serde(default)]
    pub tables: TableSelection,
    #[serde(default = "default_connect_timeout")]
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(default = "default_true")]
    pub encrypt: bool,
    #[serde(default)]
    pub trust_server_certificate: bool,
    #[serde(default = "default_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    #[serde(default = "default_streaming_fetch_size")]
    pub streaming_fetch_size: usize,
    #[serde(default = "default_sqlserver_snapshot_isolation")]
    pub snapshot_isolation_mode: String,
}

impl SqlServerSourceConfig {
    fn validate(&self) -> Result<()> {
        if self.username.trim().is_empty() || self.databases.len() != 1 {
            return Err(Error::Configuration(
                "SQL Server source currently requires exactly one database per connector".into(),
            ));
        }
        if self.streaming_fetch_size == 0 {
            return Err(Error::Configuration(
                "source.streaming_fetch_size must be greater than zero".into(),
            ));
        }
        if !matches!(
            self.snapshot_isolation_mode.as_str(),
            "exclusive" | "snapshot" | "repeatable_read" | "read_committed" | "read_uncommitted"
        ) {
            return Err(Error::Configuration(
                "source.snapshot_isolation_mode is unsupported".into(),
            ));
        }
        validate_table_patterns(&self.tables)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotOwnership {
    #[default]
    Managed,
    External,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSelection {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl TableSelection {
    #[must_use]
    pub fn includes(&self, schema: &str, table: &str) -> bool {
        let name = format!("{schema}.{table}");
        let included = self.include.is_empty()
            || self
                .include
                .iter()
                .any(|pattern| regex_matches(pattern, &name));
        included
            && !self
                .exclude
                .iter()
                .any(|pattern| regex_matches(pattern, &name))
    }
}

fn validate_table_patterns(tables: &TableSelection) -> Result<()> {
    for pattern in tables.include.iter().chain(tables.exclude.iter()) {
        if Regex::new(pattern).is_err() {
            return Err(Error::Configuration(format!(
                "table selector {pattern:?} is not a valid regular expression"
            )));
        }
    }
    Ok(())
}

fn regex_matches(pattern: &str, value: &str) -> bool {
    Regex::new(&format!("^(?:{pattern})$")).is_ok_and(|pattern| pattern.is_match(value))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    #[serde(default)]
    pub mode: SnapshotMode,
    #[serde(default = "default_snapshot_fetch_size")]
    pub fetch_size: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            fetch_size: default_snapshot_fetch_size(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    #[default]
    Initial,
    Never,
    WhenNeeded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormatConfig {
    #[serde(rename = "type", default)]
    pub kind: FormatType,
    #[serde(default = "default_unavailable_value")]
    pub unavailable_value: String,
    #[serde(default = "default_true")]
    pub tombstones_on_delete: bool,
}

impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            kind: FormatType::DebeziumJson,
            unavailable_value: default_unavailable_value(),
            tombstones_on_delete: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatType {
    RustiumJson,
    #[default]
    DebeziumJson,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkConfig {
    Stdout {
        #[serde(default = "default_topic_prefix")]
        topic_prefix: String,
    },
    Kafka {
        bootstrap_servers: Vec<String>,
        topic_prefix: String,
        #[serde(default = "default_kafka_acks")]
        acks: String,
        #[serde(default = "default_kafka_compression")]
        compression: String,
        #[serde(default = "default_delivery_timeout")]
        #[serde(with = "humantime_serde")]
        delivery_timeout: Duration,
        #[serde(default)]
        properties: BTreeMap<String, String>,
    },
}

impl SinkConfig {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Stdout { topic_prefix } => validate_name(topic_prefix, "sink.topic_prefix"),
            Self::Kafka {
                bootstrap_servers,
                topic_prefix,
                acks,
                ..
            } => {
                if bootstrap_servers.is_empty()
                    || bootstrap_servers
                        .iter()
                        .any(|server| server.trim().is_empty())
                {
                    return Err(Error::Configuration(
                        "sink.bootstrap_servers must contain at least one server".into(),
                    ));
                }
                if !matches!(acks.as_str(), "all" | "-1" | "0" | "1") {
                    return Err(Error::Configuration(
                        "sink.acks must be one of all, -1, 0, or 1".into(),
                    ));
                }
                validate_name(topic_prefix, "sink.topic_prefix")
            }
        }
    }

    #[must_use]
    pub fn topic_prefix(&self) -> &str {
        match self {
            Self::Stdout { topic_prefix } | Self::Kafka { topic_prefix, .. } => topic_prefix,
        }
    }

    fn semantic_config(&self) -> serde_json::Value {
        match self {
            Self::Stdout { topic_prefix } => {
                serde_json::json!({"type": "stdout", "topic_prefix": topic_prefix})
            }
            Self::Kafka {
                bootstrap_servers,
                topic_prefix,
                ..
            } => serde_json::json!({
                "type": "kafka",
                "bootstrap_servers": bootstrap_servers,
                "topic_prefix": topic_prefix,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    #[serde(rename = "type", default)]
    pub kind: StateType,
    #[serde(default = "default_state_path")]
    pub path: String,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            kind: StateType::Sqlite,
            path: default_state_path(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateType {
    #[default]
    Sqlite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSettings {
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_batch_size")]
    pub max_batch_size: usize,
    #[serde(default = "default_flush_interval")]
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    #[serde(default = "default_shutdown_timeout")]
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
            max_batch_size: default_batch_size(),
            flush_interval: default_flush_interval(),
            shutdown_timeout: default_shutdown_timeout(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub enable_mutations: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            enable_mutations: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub log_format: LogFormat,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_true")]
    pub metrics: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Json,
            log_level: default_log_level(),
            metrics: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Json,
    Pretty,
}

fn interpolate_environment(input: &str) -> Result<String> {
    let pattern = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
        .expect("environment interpolation regex is valid");
    let mut missing = Vec::new();
    let output = pattern.replace_all(input, |captures: &regex::Captures<'_>| {
        let name = &captures[1];
        match env::var(name) {
            Ok(value) => value,
            Err(_) => captures.get(2).map_or_else(
                || {
                    missing.push(name.to_string());
                    String::new()
                },
                |value| value.as_str().to_string(),
            ),
        }
    });
    if missing.is_empty() {
        Ok(output.into_owned())
    } else {
        Err(Error::Configuration(format!(
            "missing environment variables: {}",
            missing.join(", ")
        )))
    }
}

fn validate_name(value: &str, field: &str) -> Result<()> {
    let valid = Regex::new(r"^[A-Za-z_][A-Za-z0-9_.-]{0,254}$").expect("name regex is valid");
    if valid.is_match(value) {
        Ok(())
    } else {
        Err(Error::Configuration(format!(
            "{field} contains unsupported characters"
        )))
    }
}

fn hex_digest(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn default_hostname() -> String {
    "localhost".into()
}
const fn default_postgres_port() -> u16 {
    5432
}
const fn default_mysql_port() -> u16 {
    3306
}
const fn default_sqlserver_port() -> u16 {
    1433
}
const fn default_mysql_server_id() -> u32 {
    5_401
}
fn default_mysql_ssl_mode() -> String {
    "preferred".into()
}
fn default_mysql_keep_alive_interval() -> Duration {
    Duration::from_secs(60)
}
const fn default_mysql_reconnect_max_attempts() -> u32 {
    10
}
fn default_poll_interval() -> Duration {
    Duration::from_millis(500)
}
const fn default_streaming_fetch_size() -> usize {
    10_240
}
fn default_sqlserver_snapshot_isolation() -> String {
    "repeatable_read".into()
}
fn default_slot_name() -> String {
    "rustium".into()
}
fn default_ssl_mode() -> String {
    "prefer".into()
}
const fn default_connect_timeout() -> Duration {
    Duration::from_secs(30)
}
const fn default_snapshot_fetch_size() -> usize {
    10_000
}
fn default_unavailable_value() -> String {
    "__rustium_unavailable_value".into()
}
fn default_heartbeat_topics_prefix() -> String {
    "__debezium-heartbeat".into()
}
fn default_topic_prefix() -> String {
    "rustium".into()
}
fn default_kafka_acks() -> String {
    "all".into()
}
fn default_kafka_compression() -> String {
    "lz4".into()
}
const fn default_delivery_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_state_path() -> String {
    "rustium.db".into()
}
const fn default_channel_capacity() -> usize {
    2_048
}
const fn default_batch_size() -> usize {
    512
}
const fn default_flush_interval() -> Duration {
    Duration::from_millis(100)
}
const fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_bind() -> String {
    "127.0.0.1:8080".into()
}
fn default_log_level() -> String {
    "info".into()
}
const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"
api_version: rustium.io/v1alpha1
kind: Connector
metadata:
  name: orders-cdc
source:
  type: postgresql
  database: app
  username: rustium
  password: secret
  publication: rustium_pub
  slot_name: rustium_orders
  tables:
    include: [public.orders]
sink:
  type: stdout
  topic_prefix: app
"#;

    #[test]
    fn parses_minimal_config() {
        let config = Config::from_yaml(CONFIG).unwrap();
        assert_eq!(config.metadata.name, "orders-cdc");
        assert_eq!(config.runtime.max_batch_size, 512);
        assert!(config.format.tombstones_on_delete);
        assert!(
            config
                .source
                .as_postgresql()
                .unwrap()
                .connection_url(true)
                .unwrap()
                .contains("replication=database")
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let error = Config::from_yaml(&format!("{CONFIG}\nunknown: true\n")).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn fingerprint_ignores_password() {
        let first = Config::from_yaml(CONFIG).unwrap();
        let second = Config::from_yaml(&CONFIG.replace("secret", "rotated")).unwrap();
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn parses_native_tombstone_override() {
        let config = Config::from_yaml(
            &CONFIG.replace("sink:\n", "format:\n  tombstones_on_delete: false\nsink:\n"),
        )
        .unwrap();
        assert!(!config.format.tombstones_on_delete);
    }

    #[test]
    fn parses_native_mysql_heartbeat_settings() {
        let config = Config::from_yaml(
            r#"
api_version: rustium.io/v1alpha1
kind: Connector
metadata:
  name: inventory-mysql
source:
  type: mysql
  username: rustium
  password: secret
  databases: [inventory]
  heartbeat_interval: 5s
  heartbeat_topics_prefix: __heartbeat
  heartbeat_topic_name: shared-heartbeat
sink:
  type: stdout
  topic_prefix: inventory
"#,
        )
        .unwrap();
        let source = config.source.as_mysql().unwrap();
        assert_eq!(source.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(source.heartbeat_topics_prefix, "__heartbeat");
        assert_eq!(
            source.heartbeat_topic_name.as_deref(),
            Some("shared-heartbeat")
        );
    }
}
