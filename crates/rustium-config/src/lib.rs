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
        if self.snapshot.fetch_size == 0 {
            return Err(Error::Configuration(
                "snapshot.fetch_size must be greater than zero".into(),
            ));
        }
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
#[allow(clippy::large_enum_variant)]
pub enum SourceConfig {
    Postgresql(Box<PostgresSourceConfig>),
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
            Self::Postgresql(config) => {
                let mut semantic = serde_json::json!({
                    "type": "postgresql",
                    "hostname": config.hostname,
                    "port": config.port,
                    "database": config.database,
                    "publication": config.publication,
                    "slot_name": config.slot_name,
                    "tables": config.tables,
                });
                add_heartbeat_semantics(
                    &mut semantic,
                    config.heartbeat_interval,
                    config.heartbeat_action_query.as_deref(),
                    &config.heartbeat_topics_prefix,
                    config.heartbeat_topic_name.as_deref(),
                );
                if config.signal_data_collection.is_some() || config.read_only {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "incremental_snapshot".into(),
                            serde_json::json!({
                                "signal_data_collection": config.signal_data_collection,
                                "chunk_size": config.incremental_snapshot_chunk_size,
                                "watermarking_strategy": config.incremental_snapshot_watermarking_strategy,
                                "read_only": config.read_only,
                            }),
                        );
                }
                if config.signal_enabled_channels != default_signal_enabled_channels()
                    || config
                        .signal_enabled_channels
                        .iter()
                        .any(|channel| channel == "file")
                {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "signals".into(),
                            serde_json::json!({
                                "enabled_channels": config.signal_enabled_channels,
                                "file": config.signal_file,
                                "poll_interval_ms": config.signal_poll_interval.as_millis(),
                                "kafka_topic": config.signal_kafka_topic,
                                "kafka_group_id": config.signal_kafka_group_id,
                            }),
                        );
                }
                semantic
                    .as_object_mut()
                    .expect("source semantic is an object")
                    .insert(
                        "hstore_handling_mode".into(),
                        config.hstore_handling_mode.clone().into(),
                    );
                semantic
            }
            Self::Mysql(config) => {
                let mut semantic = serde_json::json!({
                    "type": "mysql",
                    "hostname": config.hostname,
                    "port": config.port,
                    "databases": config.databases,
                    "server_id": config.server_id,
                    "tables": config.tables,
                    "ssl_ca": config.ssl_ca,
                    "ssl_cert": config.ssl_cert,
                    "ssl_key": config.ssl_key,
                    "schema_history_skip_unparseable_ddl": config.schema_history_skip_unparseable_ddl,
                    "gtid_source_includes": config.gtid_source_includes,
                    "gtid_source_excludes": config.gtid_source_excludes,
                    "gtid_source_filter_dml_events": config.gtid_source_filter_dml_events,
                });
                add_heartbeat_semantics(
                    &mut semantic,
                    config.heartbeat_interval,
                    config.heartbeat_action_query.as_deref(),
                    &config.heartbeat_topics_prefix,
                    config.heartbeat_topic_name.as_deref(),
                );
                semantic
                    .as_object_mut()
                    .expect("source semantic is an object")
                    .insert(
                        "signals".into(),
                        serde_json::json!({
                            "data_collection": config.signal_data_collection,
                            "enabled_channels": config.signal_enabled_channels,
                            "file": config.signal_file,
                            "poll_interval_ms": config.signal_poll_interval.as_millis(),
                            "chunk_size": config.incremental_snapshot_chunk_size,
                            "watermarking_strategy": config.incremental_snapshot_watermarking_strategy,
                            "kafka_topic": config.signal_kafka_topic,
                            "kafka_bootstrap_servers": config.signal_kafka_bootstrap_servers,
                            "kafka_group_id": config.signal_kafka_group_id,
                        }),
                    );
                semantic
            }
            Self::Sqlserver(config) => {
                let mut semantic = serde_json::json!({
                "type": "sqlserver",
                "hostname": config.hostname,
                "port": config.port,
                "databases": config.databases,
                "tables": config.tables,
                "encrypt": config.encrypt,
                });
                add_heartbeat_semantics(
                    &mut semantic,
                    config.heartbeat_interval,
                    config.heartbeat_action_query.as_deref(),
                    &config.heartbeat_topics_prefix,
                    config.heartbeat_topic_name.as_deref(),
                );
                semantic
                    .as_object_mut()
                    .expect("source semantic is an object")
                    .insert(
                        "signals".into(),
                        serde_json::json!({
                            "data_collection": config.signal_data_collection,
                            "enabled_channels": config.signal_enabled_channels,
                            "file": config.signal_file,
                            "poll_interval_ms": config.signal_poll_interval.as_millis(),
                            "chunk_size": config.incremental_snapshot_chunk_size,
                            "watermarking_strategy": config.incremental_snapshot_watermarking_strategy,
                            "kafka_topic": config.signal_kafka_topic,
                            "kafka_bootstrap_servers": config.signal_kafka_bootstrap_servers,
                            "kafka_group_id": config.signal_kafka_group_id,
                        }),
                    );
                semantic
            }
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
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(default)]
    pub heartbeat_action_query: Option<String>,
    #[serde(default = "default_heartbeat_topics_prefix")]
    pub heartbeat_topics_prefix: String,
    #[serde(default)]
    pub heartbeat_topic_name: Option<String>,
    #[serde(default)]
    pub signal_data_collection: Option<String>,
    #[serde(default = "default_signal_enabled_channels")]
    pub signal_enabled_channels: Vec<String>,
    #[serde(default = "default_signal_file")]
    pub signal_file: String,
    #[serde(default = "default_signal_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub signal_poll_interval: Duration,
    #[serde(default)]
    pub signal_kafka_topic: Option<String>,
    #[serde(default)]
    pub signal_kafka_bootstrap_servers: Vec<String>,
    #[serde(default = "default_signal_kafka_group_id")]
    pub signal_kafka_group_id: String,
    #[serde(default = "default_signal_kafka_poll_timeout")]
    #[serde(with = "humantime_serde")]
    pub signal_kafka_poll_timeout: Duration,
    #[serde(default)]
    pub signal_kafka_consumer_properties: BTreeMap<String, String>,
    #[serde(default = "default_incremental_snapshot_chunk_size")]
    pub incremental_snapshot_chunk_size: usize,
    #[serde(default = "default_incremental_snapshot_watermarking_strategy")]
    pub incremental_snapshot_watermarking_strategy: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default = "default_hstore_handling_mode")]
    pub hstore_handling_mode: String,
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
        validate_heartbeat(
            &self.heartbeat_topics_prefix,
            self.heartbeat_topic_name.as_deref(),
            self.heartbeat_action_query.as_deref(),
        )?;
        if self.incremental_snapshot_chunk_size == 0 {
            return Err(Error::Configuration(
                "source.incremental_snapshot_chunk_size must be greater than zero".into(),
            ));
        }
        if self.incremental_snapshot_watermarking_strategy != "insert_insert" {
            return Err(Error::Configuration(
                "source.incremental_snapshot_watermarking_strategy currently supports only insert_insert"
                    .into(),
            ));
        }
        if !matches!(self.hstore_handling_mode.as_str(), "json" | "map") {
            return Err(Error::Configuration(
                "source.hstore_handling_mode must be json or map".into(),
            ));
        }
        if self.signal_poll_interval.is_zero() {
            return Err(Error::Configuration(
                "source.signal_poll_interval must be greater than zero".into(),
            ));
        }
        if self.signal_enabled_channels.iter().any(|channel| {
            channel.trim().is_empty()
                || !matches!(channel.as_str(), "source" | "file" | "in-process" | "kafka")
        }) {
            return Err(Error::Configuration(
                "source.signal_enabled_channels currently supports source, file, in-process, and kafka"
                    .into(),
            ));
        }
        if self
            .signal_enabled_channels
            .iter()
            .any(|channel| channel == "file")
            && self.signal_file.trim().is_empty()
        {
            return Err(Error::Configuration(
                "source.signal_file must not be empty when the file signal channel is enabled"
                    .into(),
            ));
        }
        if self
            .signal_enabled_channels
            .iter()
            .any(|channel| channel == "kafka")
        {
            if self.signal_kafka_bootstrap_servers.is_empty()
                || self
                    .signal_kafka_bootstrap_servers
                    .iter()
                    .any(|server| server.trim().is_empty())
            {
                return Err(Error::Configuration(
                    "source.signal_kafka_bootstrap_servers must contain at least one server when the kafka signal channel is enabled"
                        .into(),
                ));
            }
            if self.signal_kafka_group_id.trim().is_empty() {
                return Err(Error::Configuration(
                    "source.signal_kafka_group_id must not be empty when the kafka signal channel is enabled"
                        .into(),
                ));
            }
            if self
                .signal_kafka_topic
                .as_deref()
                .is_some_and(|topic| topic.trim().is_empty())
            {
                return Err(Error::Configuration(
                    "source.signal_kafka_topic must not be empty when configured".into(),
                ));
            }
            for property in ["enable.auto.commit", "enable.auto.offset.store"] {
                if self
                    .signal_kafka_consumer_properties
                    .get(property)
                    .is_some_and(|value| !value.eq_ignore_ascii_case("false"))
                {
                    return Err(Error::Configuration(format!(
                        "source.signal_kafka_consumer_properties.{property} must be false so signal offsets follow Rustium checkpoints"
                    )));
                }
            }
        }
        if let Some(collection) = &self.signal_data_collection {
            let mut parts = collection.split('.');
            let schema = parts.next().unwrap_or_default();
            let table = parts.next().unwrap_or_default();
            if schema.is_empty() || table.is_empty() || parts.next().is_some() {
                return Err(Error::Configuration(
                    "source.signal_data_collection must be a schema-qualified PostgreSQL table"
                        .into(),
                ));
            }
            validate_name(schema, "source.signal_data_collection schema")?;
            validate_name(table, "source.signal_data_collection table")?;
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
    #[serde(default = "default_mysql_connection_time_zone")]
    pub connection_time_zone: String,
    #[serde(default)]
    pub ssl_ca: Option<String>,
    #[serde(default)]
    pub ssl_cert: Option<String>,
    #[serde(default)]
    pub ssl_key: Option<String>,
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
    pub gtid_source_includes: Vec<String>,
    #[serde(default)]
    pub gtid_source_excludes: Vec<String>,
    #[serde(default = "default_true")]
    pub gtid_source_filter_dml_events: bool,
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(default)]
    pub heartbeat_action_query: Option<String>,
    #[serde(default = "default_heartbeat_topics_prefix")]
    pub heartbeat_topics_prefix: String,
    #[serde(default)]
    pub heartbeat_topic_name: Option<String>,
    #[serde(default)]
    pub signal_data_collection: Option<String>,
    #[serde(default = "default_signal_enabled_channels")]
    pub signal_enabled_channels: Vec<String>,
    #[serde(default = "default_signal_file")]
    pub signal_file: String,
    #[serde(default = "default_signal_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub signal_poll_interval: Duration,
    #[serde(default = "default_incremental_snapshot_chunk_size")]
    pub incremental_snapshot_chunk_size: usize,
    #[serde(default = "default_incremental_snapshot_watermarking_strategy")]
    pub incremental_snapshot_watermarking_strategy: String,
    #[serde(default)]
    pub signal_kafka_topic: Option<String>,
    #[serde(default)]
    pub signal_kafka_bootstrap_servers: Vec<String>,
    #[serde(default = "default_signal_kafka_group_id")]
    pub signal_kafka_group_id: String,
    #[serde(default = "default_signal_kafka_poll_timeout")]
    #[serde(with = "humantime_serde")]
    pub signal_kafka_poll_timeout: Duration,
    #[serde(default)]
    pub signal_kafka_consumer_properties: BTreeMap<String, String>,
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
        self.session_time_zone()?;
        validate_heartbeat(
            &self.heartbeat_topics_prefix,
            self.heartbeat_topic_name.as_deref(),
            self.heartbeat_action_query.as_deref(),
        )?;
        if self.incremental_snapshot_chunk_size == 0 {
            return Err(Error::Configuration(
                "source.incremental_snapshot_chunk_size must be greater than zero".into(),
            ));
        }
        if self.incremental_snapshot_watermarking_strategy != "insert_insert" {
            return Err(Error::Configuration(
                "source.incremental_snapshot_watermarking_strategy currently supports only insert_insert".into(),
            ));
        }
        if self.signal_poll_interval.is_zero() {
            return Err(Error::Configuration(
                "source.signal_poll_interval must be greater than zero".into(),
            ));
        }
        if self.signal_enabled_channels.iter().any(|channel| {
            channel.trim().is_empty()
                || !matches!(channel.as_str(), "source" | "file" | "in-process" | "kafka")
        }) {
            return Err(Error::Configuration(
                "source.signal_enabled_channels currently supports source, file, in-process, and kafka for MySQL".into(),
            ));
        }
        if self
            .signal_enabled_channels
            .iter()
            .any(|channel| channel == "file")
            && self.signal_file.trim().is_empty()
        {
            return Err(Error::Configuration(
                "source.signal_file must not be empty when the file signal channel is enabled"
                    .into(),
            ));
        }
        if let Some(collection) = &self.signal_data_collection {
            let mut parts = collection.split('.');
            let database = parts.next().unwrap_or_default();
            let table = parts.next().unwrap_or_default();
            if database.is_empty() || table.is_empty() || parts.next().is_some() {
                return Err(Error::Configuration(
                    "source.signal_data_collection must be a database-qualified MySQL table".into(),
                ));
            }
            validate_name(database, "source.signal_data_collection database")?;
            validate_name(table, "source.signal_data_collection table")?;
        }
        if self
            .signal_enabled_channels
            .iter()
            .any(|channel| channel == "kafka")
        {
            if self.signal_kafka_bootstrap_servers.is_empty()
                || self
                    .signal_kafka_bootstrap_servers
                    .iter()
                    .any(|server| server.trim().is_empty())
            {
                return Err(Error::Configuration(
                    "source.signal_kafka_bootstrap_servers must contain at least one server when the kafka signal channel is enabled".into(),
                ));
            }
            if self.signal_kafka_group_id.trim().is_empty() {
                return Err(Error::Configuration(
                    "source.signal_kafka_group_id must not be empty when the kafka signal channel is enabled".into(),
                ));
            }
            if self
                .signal_kafka_topic
                .as_deref()
                .is_some_and(|topic| topic.trim().is_empty())
            {
                return Err(Error::Configuration(
                    "source.signal_kafka_topic must not be empty when configured".into(),
                ));
            }
            for property in ["enable.auto.commit", "enable.auto.offset.store"] {
                if self
                    .signal_kafka_consumer_properties
                    .get(property)
                    .is_some_and(|value| !value.eq_ignore_ascii_case("false"))
                {
                    return Err(Error::Configuration(format!(
                        "source.signal_kafka_consumer_properties.{property} must be false so signal offsets follow Rustium checkpoints"
                    )));
                }
            }
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
        if self.ssl_cert.is_some() != self.ssl_key.is_some() {
            return Err(Error::Configuration(
                "source.ssl_cert and source.ssl_key must be configured together".into(),
            ));
        }
        if self.ssl_mode == "disabled"
            && (self.ssl_ca.is_some() || self.ssl_cert.is_some() || self.ssl_key.is_some())
        {
            return Err(Error::Configuration(
                "source.ssl_ca/ssl_cert/ssl_key require an enabled MySQL TLS mode".into(),
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
        if !self.gtid_source_includes.is_empty() && !self.gtid_source_excludes.is_empty() {
            return Err(Error::Configuration(
                "source.gtid_source_includes and source.gtid_source_excludes cannot both be configured"
                    .into(),
            ));
        }
        for pattern in self
            .gtid_source_includes
            .iter()
            .chain(self.gtid_source_excludes.iter())
        {
            if pattern.trim().is_empty() || Regex::new(pattern).is_err() {
                return Err(Error::Configuration(format!(
                    "GTID source filter {pattern:?} is not a valid regular expression"
                )));
            }
        }
        validate_table_patterns(&self.tables)
    }

    pub fn session_time_zone(&self) -> Result<&'static str> {
        let value = self.connection_time_zone.trim();
        if value == "+00:00"
            || value.eq_ignore_ascii_case("UTC")
            || value.eq_ignore_ascii_case("Z")
            || value.eq_ignore_ascii_case("Etc/UTC")
        {
            return Ok("+00:00");
        }

        Err(Error::Configuration(format!(
            "source.connection_time_zone currently supports only UTC, Z, Etc/UTC, or +00:00; found {:?}",
            self.connection_time_zone
        )))
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
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(default)]
    pub heartbeat_action_query: Option<String>,
    #[serde(default = "default_heartbeat_topics_prefix")]
    pub heartbeat_topics_prefix: String,
    #[serde(default)]
    pub heartbeat_topic_name: Option<String>,
    #[serde(default)]
    pub signal_data_collection: Option<String>,
    #[serde(default = "default_signal_enabled_channels")]
    pub signal_enabled_channels: Vec<String>,
    #[serde(default = "default_signal_file")]
    pub signal_file: String,
    #[serde(default = "default_signal_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub signal_poll_interval: Duration,
    #[serde(default = "default_incremental_snapshot_chunk_size")]
    pub incremental_snapshot_chunk_size: usize,
    #[serde(default = "default_incremental_snapshot_watermarking_strategy")]
    pub incremental_snapshot_watermarking_strategy: String,
    #[serde(default)]
    pub signal_kafka_topic: Option<String>,
    #[serde(default)]
    pub signal_kafka_bootstrap_servers: Vec<String>,
    #[serde(default = "default_signal_kafka_group_id")]
    pub signal_kafka_group_id: String,
    #[serde(default = "default_signal_kafka_poll_timeout")]
    #[serde(with = "humantime_serde")]
    pub signal_kafka_poll_timeout: Duration,
    #[serde(default)]
    pub signal_kafka_consumer_properties: BTreeMap<String, String>,
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
        validate_heartbeat(
            &self.heartbeat_topics_prefix,
            self.heartbeat_topic_name.as_deref(),
            self.heartbeat_action_query.as_deref(),
        )?;
        if self.incremental_snapshot_chunk_size == 0 {
            return Err(Error::Configuration(
                "source.incremental_snapshot_chunk_size must be greater than zero".into(),
            ));
        }
        if self.incremental_snapshot_watermarking_strategy != "insert_insert" {
            return Err(Error::Configuration(
                "source.incremental_snapshot_watermarking_strategy currently supports only insert_insert"
                    .into(),
            ));
        }
        if self.signal_poll_interval.is_zero() {
            return Err(Error::Configuration(
                "source.signal_poll_interval must be greater than zero".into(),
            ));
        }
        if self.signal_enabled_channels.iter().any(|channel| {
            channel.trim().is_empty()
                || !matches!(channel.as_str(), "source" | "file" | "in-process" | "kafka")
        }) {
            return Err(Error::Configuration(
                "source.signal_enabled_channels currently supports source, file, in-process, and kafka for SQL Server"
                    .into(),
            ));
        }
        if self
            .signal_enabled_channels
            .iter()
            .any(|channel| channel == "file")
            && self.signal_file.trim().is_empty()
        {
            return Err(Error::Configuration(
                "source.signal_file must not be empty when the file signal channel is enabled"
                    .into(),
            ));
        }
        if let Some(collection) = &self.signal_data_collection {
            validate_sqlserver_signal_collection(collection, &self.databases[0])?;
        }
        if self
            .signal_enabled_channels
            .iter()
            .any(|channel| channel == "kafka")
        {
            if self.signal_kafka_bootstrap_servers.is_empty()
                || self
                    .signal_kafka_bootstrap_servers
                    .iter()
                    .any(|server| server.trim().is_empty())
            {
                return Err(Error::Configuration(
                    "source.signal_kafka_bootstrap_servers must contain at least one server when the kafka signal channel is enabled"
                        .into(),
                ));
            }
            if self.signal_kafka_group_id.trim().is_empty() {
                return Err(Error::Configuration(
                    "source.signal_kafka_group_id must not be empty when the kafka signal channel is enabled"
                        .into(),
                ));
            }
            if self
                .signal_kafka_topic
                .as_deref()
                .is_some_and(|topic| topic.trim().is_empty())
            {
                return Err(Error::Configuration(
                    "source.signal_kafka_topic must not be empty when configured".into(),
                ));
            }
            for property in ["enable.auto.commit", "enable.auto.offset.store"] {
                if self
                    .signal_kafka_consumer_properties
                    .get(property)
                    .is_some_and(|value| !value.eq_ignore_ascii_case("false"))
                {
                    return Err(Error::Configuration(format!(
                        "source.signal_kafka_consumer_properties.{property} must be false so signal offsets follow Rustium checkpoints"
                    )));
                }
            }
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

fn validate_sqlserver_signal_collection(collection: &str, database: &str) -> Result<()> {
    let parts = collection.split('.').collect::<Vec<_>>();
    let (schema, table) = match parts.as_slice() {
        [schema, table] => (*schema, *table),
        [configured_database, schema, table] if configured_database == &database => {
            (*schema, *table)
        }
        [configured_database, _, _] => {
            return Err(Error::Configuration(format!(
                "source.signal_data_collection database {configured_database:?} does not match {database:?}"
            )));
        }
        _ => {
            return Err(Error::Configuration(
                "source.signal_data_collection must be schema.table or database.schema.table for SQL Server"
                    .into(),
            ));
        }
    };
    validate_name(schema, "source.signal_data_collection schema")?;
    validate_name(table, "source.signal_data_collection table")
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

fn validate_heartbeat(
    topics_prefix: &str,
    topic_name: Option<&str>,
    action_query: Option<&str>,
) -> Result<()> {
    if topics_prefix.trim().is_empty() {
        return Err(Error::Configuration(
            "source.heartbeat_topics_prefix must not be empty".into(),
        ));
    }
    if topic_name.is_some_and(|name| name.trim().is_empty()) {
        return Err(Error::Configuration(
            "source.heartbeat_topic_name must not be empty when set".into(),
        ));
    }
    if action_query.is_some_and(|query| query.trim().is_empty()) {
        return Err(Error::Configuration(
            "source.heartbeat_action_query must not be empty when set".into(),
        ));
    }
    Ok(())
}

fn add_heartbeat_semantics(
    semantic: &mut serde_json::Value,
    interval: Duration,
    action_query: Option<&str>,
    topics_prefix: &str,
    topic_name: Option<&str>,
) {
    if interval.is_zero()
        && action_query.is_none()
        && topics_prefix == "__debezium-heartbeat"
        && topic_name.is_none()
    {
        return;
    }
    semantic
        .as_object_mut()
        .expect("source semantic is an object")
        .insert(
            "heartbeat".into(),
            serde_json::json!({
                "interval": interval,
                "action_query": action_query,
                "topics_prefix": topics_prefix,
                "topic_name": topic_name,
            }),
        );
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
fn default_mysql_connection_time_zone() -> String {
    "UTC".into()
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
const fn default_incremental_snapshot_chunk_size() -> usize {
    1_024
}
fn default_incremental_snapshot_watermarking_strategy() -> String {
    "insert_insert".into()
}
fn default_hstore_handling_mode() -> String {
    "json".into()
}
fn default_signal_enabled_channels() -> Vec<String> {
    vec!["source".into()]
}
fn default_signal_file() -> String {
    "file-signals.txt".into()
}
const fn default_signal_poll_interval() -> Duration {
    Duration::from_secs(5)
}
fn default_signal_kafka_group_id() -> String {
    "kafka-signal".into()
}
const fn default_signal_kafka_poll_timeout() -> Duration {
    Duration::from_millis(100)
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
        assert_eq!(
            config.source.as_postgresql().unwrap().hstore_handling_mode,
            "json"
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let error = Config::from_yaml(&format!("{CONFIG}\nunknown: true\n")).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_zero_snapshot_fetch_size() {
        let error =
            Config::from_yaml(&CONFIG.replace("sink:\n", "snapshot:\n  fetch_size: 0\nsink:\n"))
                .unwrap_err();
        assert!(error.to_string().contains("snapshot.fetch_size"));
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
        let raw = r#"
api_version: rustium.io/v1alpha1
kind: Connector
metadata:
  name: inventory-mysql
source:
  type: mysql
  username: rustium
  password: secret
  databases: [inventory]
  connection_time_zone: Etc/UTC
  heartbeat_interval: 5s
  heartbeat_topics_prefix: __heartbeat
  heartbeat_topic_name: shared-heartbeat
sink:
  type: stdout
  topic_prefix: inventory
"#;
        let config = Config::from_yaml(raw).unwrap();
        let source = config.source.as_mysql().unwrap();
        assert_eq!(source.connection_time_zone, "Etc/UTC");
        assert_eq!(source.session_time_zone().unwrap(), "+00:00");
        assert_eq!(source.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(source.heartbeat_topics_prefix, "__heartbeat");
        assert_eq!(
            source.heartbeat_topic_name.as_deref(),
            Some("shared-heartbeat")
        );
        let implicit_utc =
            Config::from_yaml(&raw.replace("  connection_time_zone: Etc/UTC\n", "")).unwrap();
        assert_eq!(config.fingerprint(), implicit_utc.fingerprint());
    }

    #[test]
    fn parses_native_postgresql_heartbeat_settings() {
        let default = Config::from_yaml(CONFIG).unwrap();
        let config = Config::from_yaml(
            &CONFIG.replace(
                "  password: secret\n",
                "  password: secret\n  heartbeat_interval: 5s\n  heartbeat_action_query: UPDATE public.heartbeat SET touched_at = now()\n  heartbeat_topics_prefix: __heartbeat\n  heartbeat_topic_name: shared-heartbeat\n  read_only: true\n",
            ),
        )
        .unwrap();
        let source = config.source.as_postgresql().unwrap();
        assert_eq!(source.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(
            source.heartbeat_action_query.as_deref(),
            Some("UPDATE public.heartbeat SET touched_at = now()")
        );
        assert_eq!(source.heartbeat_topics_prefix, "__heartbeat");
        assert_eq!(
            source.heartbeat_topic_name.as_deref(),
            Some("shared-heartbeat")
        );
        assert!(source.read_only);
        assert!(default.source.semantic_config().get("heartbeat").is_none());
        assert_ne!(default.fingerprint(), config.fingerprint());
    }
}
