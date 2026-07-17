//! Strict, versioned Rustium configuration.

mod debezium;

use std::{collections::BTreeMap, env, fs, path::Path, time::Duration};

use regex::Regex;
use rustium_core::{Error, Result, RetryPolicy};
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
        self.format.validate(&self.sink)?;
        if self.snapshot.fetch_size == 0 {
            return Err(Error::Configuration(
                "snapshot.fetch_size must be greater than zero".into(),
            ));
        }
        self.snapshot.validate()?;
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
        if self.runtime.errors_max_retries < -1 {
            return Err(Error::Configuration(
                "runtime.errors_max_retries must be -1, 0, or a positive integer".into(),
            ));
        }
        if self.runtime.errors_retry_delay_initial.is_zero() {
            return Err(Error::Configuration(
                "runtime.errors_retry_delay_initial must be greater than zero".into(),
            ));
        }
        if self.runtime.errors_retry_delay_max < self.runtime.errors_retry_delay_initial {
            return Err(Error::Configuration(
                "runtime.errors_retry_delay_max must be greater than or equal to errors_retry_delay_initial"
                    .into(),
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
            "snapshot": self.snapshot.semantic_config(),
            "format": self.format.semantic_config(),
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
                if config.publication_autocreate_mode != PublicationAutoCreateMode::Disabled {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "publication_autocreate_mode".into(),
                            serde_json::json!(config.publication_autocreate_mode),
                        );
                }
                if !config.replica_identity_autoset_values.is_empty() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "replica_identity_autoset_values".into(),
                            serde_json::json!(config.replica_identity_autoset_values),
                        );
                }
                if config.publish_via_partition_root {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert("publish_via_partition_root".into(), true.into());
                }
                if config.slot_failover {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert("slot_failover".into(), true.into());
                }
                if config.offset_mismatch_strategy != PostgresOffsetMismatchStrategy::NoValidation {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "offset_mismatch_strategy".into(),
                            serde_json::json!(config.offset_mismatch_strategy),
                        );
                }
                if config.lsn_flush_mode != PostgresLsnFlushMode::Connector {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "lsn_flush_mode".into(),
                            serde_json::json!(config.lsn_flush_mode),
                        );
                }
                if !config.snapshot_isolation_mode.imports_snapshot() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "snapshot_isolation_mode".into(),
                            serde_json::json!(config.snapshot_isolation_mode),
                        );
                }
                if !config.xmin_fetch_interval.is_zero() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "xmin_fetch_interval".into(),
                            serde_json::json!(config.xmin_fetch_interval),
                        );
                }
                if !config.slot_stream_params.is_empty() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "slot_stream_params".into(),
                            serde_json::json!(config.slot_stream_params),
                        );
                }
                if !config.database_initial_statements.is_empty() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "database_initial_statements".into(),
                            serde_json::json!(config.database_initial_statements),
                        );
                }
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
                if config.interval_handling_mode != default_postgres_interval_handling_mode() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "interval_handling_mode".into(),
                            config.interval_handling_mode.clone().into(),
                        );
                }
                if config.include_unknown_datatypes {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert("include_unknown_datatypes".into(), true.into());
                }
                if config.money_fraction_digits != default_postgres_money_fraction_digits() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "money_fraction_digits".into(),
                            config.money_fraction_digits.into(),
                        );
                }
                if config.captures_logical_decoding_messages() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "logical_decoding_messages".into(),
                            serde_json::json!({
                                "include": config.message_prefix_include_list,
                                "exclude": config.message_prefix_exclude_list,
                            }),
                        );
                }
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
                if config.ssl_keystore.is_some() || config.ssl_truststore.is_some() {
                    semantic
                        .as_object_mut()
                        .expect("source semantic is an object")
                        .insert(
                            "java_ssl_stores".into(),
                            serde_json::json!({
                                "keystore": config.ssl_keystore,
                                "truststore": config.ssl_truststore,
                            }),
                        );
                }
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
    #[serde(default)]
    pub publication_autocreate_mode: PublicationAutoCreateMode,
    #[serde(default)]
    pub replica_identity_autoset_values: Vec<PostgresReplicaIdentityRule>,
    #[serde(default)]
    pub publish_via_partition_root: bool,
    #[serde(default = "default_slot_name")]
    pub slot_name: String,
    #[serde(default)]
    pub drop_slot_on_stop: bool,
    #[serde(default)]
    pub slot_failover: bool,
    #[serde(default)]
    pub slot_ownership: SlotOwnership,
    #[serde(default)]
    pub offset_mismatch_strategy: PostgresOffsetMismatchStrategy,
    #[serde(default)]
    pub lsn_flush_mode: PostgresLsnFlushMode,
    #[serde(default)]
    pub slot_stream_params: BTreeMap<String, String>,
    #[serde(default)]
    pub database_initial_statements: Vec<String>,
    #[serde(default)]
    pub snapshot_locking_mode: PostgresSnapshotLockingMode,
    #[serde(default = "default_postgres_snapshot_lock_timeout")]
    #[serde(with = "humantime_serde")]
    pub snapshot_lock_timeout: Duration,
    #[serde(default)]
    pub snapshot_isolation_mode: PostgresSnapshotIsolationMode,
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub xmin_fetch_interval: Duration,
    #[serde(default)]
    pub tables: TableSelection,
    #[serde(default = "default_ssl_mode")]
    pub ssl_mode: String,
    #[serde(default)]
    pub ssl_root_cert: Option<String>,
    #[serde(default)]
    pub ssl_cert: Option<String>,
    #[serde(default)]
    pub ssl_key: Option<String>,
    #[serde(default)]
    pub ssl_key_password: Option<String>,
    #[serde(default = "default_connect_timeout")]
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(default = "default_postgres_status_update_interval")]
    #[serde(with = "humantime_serde")]
    pub status_update_interval: Duration,
    #[serde(default = "default_true")]
    pub tcp_keepalive: bool,
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
    #[serde(default = "default_postgres_interval_handling_mode")]
    pub interval_handling_mode: String,
    #[serde(default)]
    pub include_unknown_datatypes: bool,
    #[serde(default = "default_postgres_money_fraction_digits")]
    pub money_fraction_digits: i16,
    #[serde(default)]
    pub schema_refresh_mode: PostgresSchemaRefreshMode,
    #[serde(default)]
    pub logical_decoding_messages: bool,
    #[serde(default)]
    pub message_prefix_include_list: Vec<String>,
    #[serde(default)]
    pub message_prefix_exclude_list: Vec<String>,
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
        if self.drop_slot_on_stop && self.slot_ownership == SlotOwnership::External {
            return Err(Error::Configuration(
                "source.drop_slot_on_stop can be enabled only for a managed replication slot"
                    .into(),
            ));
        }
        if self.slot_failover && self.slot_ownership == SlotOwnership::External {
            return Err(Error::Configuration(
                "source.slot_failover can be enabled only for a managed replication slot".into(),
            ));
        }
        if self.slot_ownership == SlotOwnership::External
            && self.offset_mismatch_strategy.advances_slot()
        {
            return Err(Error::Configuration(
                "source.offset_mismatch_strategy=trust_offset or trust_greater_lsn requires slot_ownership=managed because it can advance the replication slot"
                    .into(),
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(Error::Configuration(
                "source.connect_timeout must be greater than zero".into(),
            ));
        }
        if self.status_update_interval.is_zero() {
            return Err(Error::Configuration(
                "source.status_update_interval must be greater than zero".into(),
            ));
        }
        if !matches!(
            self.ssl_mode.as_str(),
            "disable" | "allow" | "prefer" | "require" | "verify-ca" | "verify-full"
        ) {
            return Err(Error::Configuration(
                "source.ssl_mode must be disable, allow, prefer, require, verify-ca, or verify-full"
                    .into(),
            ));
        }
        for (value, field) in [
            (self.ssl_root_cert.as_deref(), "source.ssl_root_cert"),
            (self.ssl_cert.as_deref(), "source.ssl_cert"),
            (self.ssl_key.as_deref(), "source.ssl_key"),
            (self.ssl_key_password.as_deref(), "source.ssl_key_password"),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(Error::Configuration(format!(
                    "{field} must not be blank when configured"
                )));
            }
        }
        if self.ssl_cert.is_some() || self.ssl_key.is_some() || self.ssl_key_password.is_some() {
            return Err(Error::Configuration(
                "source.ssl_cert, source.ssl_key, and source.ssl_key_password are not supported by the current pg_walstream rustls transport; use server certificate verification with source.ssl_root_cert or omit client-certificate authentication"
                    .into(),
            ));
        }
        for (name, value) in &self.slot_stream_params {
            if name != "origin" {
                return Err(Error::Configuration(format!(
                    "source.slot_stream_params currently supports only the pgoutput origin parameter; found {name:?}"
                )));
            }
            if !matches!(value.as_str(), "any" | "none") {
                return Err(Error::Configuration(format!(
                    "source.slot_stream_params.origin must be any or none; found {value:?}"
                )));
            }
        }
        if self
            .database_initial_statements
            .iter()
            .any(|statement| statement.trim().is_empty())
        {
            return Err(Error::Configuration(
                "source.database_initial_statements must not contain blank statements".into(),
            ));
        }
        if self.snapshot_lock_timeout.as_millis() > i32::MAX as u128 {
            return Err(Error::Configuration(format!(
                "source.snapshot_lock_timeout must not exceed {}ms",
                i32::MAX
            )));
        }
        for pattern in self.tables.include.iter().chain(self.tables.exclude.iter()) {
            if Regex::new(pattern).is_err() {
                return Err(Error::Configuration(format!(
                    "table selector {pattern:?} is not a valid regular expression"
                )));
            }
        }
        for rule in &self.replica_identity_autoset_values {
            if rule.table.trim().is_empty() || Regex::new(&rule.table).is_err() {
                return Err(Error::Configuration(format!(
                    "PostgreSQL replica identity table selector {:?} is not a valid regular expression",
                    rule.table
                )));
            }
            match (&rule.identity, rule.index.as_deref()) {
                (PostgresReplicaIdentity::Index, Some(index)) if !index.trim().is_empty() => {}
                (PostgresReplicaIdentity::Index, _) => {
                    return Err(Error::Configuration(
                        "PostgreSQL replica identity index mode requires a non-empty index name"
                            .into(),
                    ));
                }
                (_, Some(_)) => {
                    return Err(Error::Configuration(
                        "PostgreSQL replica identity index is valid only with identity=index"
                            .into(),
                    ));
                }
                (_, None) => {}
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
        if !matches!(
            self.interval_handling_mode.as_str(),
            "postgres" | "numeric" | "string"
        ) {
            return Err(Error::Configuration(
                "source.interval_handling_mode must be postgres, numeric, or string".into(),
            ));
        }
        if !self.message_prefix_include_list.is_empty()
            && !self.message_prefix_exclude_list.is_empty()
        {
            return Err(Error::Configuration(
                "source.message_prefix_include_list and source.message_prefix_exclude_list are mutually exclusive"
                    .into(),
            ));
        }
        for pattern in self
            .message_prefix_include_list
            .iter()
            .chain(&self.message_prefix_exclude_list)
        {
            if Regex::new(pattern).is_err() {
                return Err(Error::Configuration(format!(
                    "PostgreSQL logical decoding message prefix selector {pattern:?} is not a valid regular expression"
                )));
            }
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

    #[must_use]
    pub fn captures_logical_decoding_messages(&self) -> bool {
        self.logical_decoding_messages
            || !self.message_prefix_include_list.is_empty()
            || !self.message_prefix_exclude_list.is_empty()
    }

    #[must_use]
    pub fn includes_message_prefix(&self, prefix: &str) -> bool {
        if !self.captures_logical_decoding_messages() {
            return false;
        }
        let included = self.message_prefix_include_list.is_empty()
            || self
                .message_prefix_include_list
                .iter()
                .any(|pattern| regex_matches(pattern, prefix));
        included
            && !self
                .message_prefix_exclude_list
                .iter()
                .any(|pattern| regex_matches(pattern, prefix))
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
            if let Some(root_cert) = &self.ssl_root_cert {
                query.append_pair("sslrootcert", root_cert);
            }
            query.append_pair(
                "connect_timeout",
                &self.connect_timeout.as_secs().max(1).to_string(),
            );
            query.append_pair("keepalives", if self.tcp_keepalive { "1" } else { "0" });
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
    #[serde(default)]
    pub ssl_keystore: Option<String>,
    #[serde(default)]
    pub ssl_keystore_password: Option<String>,
    #[serde(default)]
    pub ssl_truststore: Option<String>,
    #[serde(default)]
    pub ssl_truststore_password: Option<String>,
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
        if self.ssl_keystore.is_some() && (self.ssl_cert.is_some() || self.ssl_key.is_some()) {
            return Err(Error::Configuration(
                "source.ssl_keystore cannot be combined with source.ssl_cert/source.ssl_key".into(),
            ));
        }
        if self.ssl_truststore.is_some() && self.ssl_ca.is_some() {
            return Err(Error::Configuration(
                "source.ssl_truststore cannot be combined with source.ssl_ca".into(),
            ));
        }
        if self.ssl_keystore.is_none() && self.ssl_keystore_password.is_some() {
            return Err(Error::Configuration(
                "source.ssl_keystore_password requires source.ssl_keystore".into(),
            ));
        }
        if self.ssl_truststore.is_none() && self.ssl_truststore_password.is_some() {
            return Err(Error::Configuration(
                "source.ssl_truststore_password requires source.ssl_truststore".into(),
            ));
        }
        if self.ssl_mode == "disabled"
            && (self.ssl_ca.is_some()
                || self.ssl_cert.is_some()
                || self.ssl_key.is_some()
                || self.ssl_keystore.is_some()
                || self.ssl_truststore.is_some())
        {
            return Err(Error::Configuration(
                "source.ssl_ca/ssl_cert/ssl_key/ssl_keystore/ssl_truststore require an enabled MySQL TLS mode".into(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresOffsetMismatchStrategy {
    #[default]
    NoValidation,
    TrustOffset,
    TrustSlot,
    TrustGreaterLsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresLsnFlushMode {
    #[default]
    Connector,
    Manual,
    ConnectorAndDriver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSchemaRefreshMode {
    #[default]
    ColumnsDiff,
    ColumnsDiffExcludeUnchangedToast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSnapshotLockingMode {
    #[default]
    None,
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSnapshotIsolationMode {
    #[default]
    Serializable,
    RepeatableRead,
    ReadCommitted,
    ReadUncommitted,
}

impl PostgresSnapshotIsolationMode {
    #[must_use]
    pub const fn imports_snapshot(self) -> bool {
        matches!(self, Self::Serializable | Self::RepeatableRead)
    }
}

impl PostgresOffsetMismatchStrategy {
    #[must_use]
    pub const fn advances_slot(self) -> bool {
        matches!(self, Self::TrustOffset | Self::TrustGreaterLsn)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAutoCreateMode {
    #[default]
    Disabled,
    AllTables,
    Filtered,
    NoTables,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresReplicaIdentityRule {
    pub table: String,
    pub identity: PostgresReplicaIdentity,
    #[serde(default)]
    pub index: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresReplicaIdentity {
    Default,
    Full,
    Nothing,
    Index,
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
    #[serde(default)]
    pub include_collections: Vec<String>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            fetch_size: default_snapshot_fetch_size(),
            include_collections: Vec::new(),
        }
    }
}

impl SnapshotConfig {
    fn validate(&self) -> Result<()> {
        for pattern in &self.include_collections {
            if Regex::new(pattern).is_err() {
                return Err(Error::Configuration(format!(
                    "snapshot include selector {pattern:?} is not a valid regular expression"
                )));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn includes_collection(&self, collection: &str) -> bool {
        self.include_collections.is_empty()
            || self
                .include_collections
                .iter()
                .any(|pattern| regex_matches(pattern, collection))
    }

    fn semantic_config(&self) -> serde_json::Value {
        let mut semantic = serde_json::json!({
            "mode": self.mode,
            "fetch_size": self.fetch_size,
        });
        if !self.include_collections.is_empty() {
            semantic
                .as_object_mut()
                .expect("snapshot semantic is an object")
                .insert(
                    "include_collections".into(),
                    serde_json::json!(self.include_collections),
                );
        }
        semantic
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistryConfig>,
}

impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            kind: FormatType::DebeziumJson,
            unavailable_value: default_unavailable_value(),
            tombstones_on_delete: true,
            schema_registry: None,
        }
    }
}

impl FormatConfig {
    fn validate(&self, sink: &SinkConfig) -> Result<()> {
        match (self.kind, &self.schema_registry) {
            (
                FormatType::DebeziumJsonSchema
                | FormatType::DebeziumAvro
                | FormatType::DebeziumProtobuf,
                Some(registry),
            ) => {
                if !matches!(sink, SinkConfig::Kafka { .. }) {
                    return Err(Error::Configuration(format!(
                        "format.type={} requires sink.type=kafka",
                        self.kind.as_str()
                    )));
                }
                registry.validate()
            }
            (
                FormatType::DebeziumJsonSchema
                | FormatType::DebeziumAvro
                | FormatType::DebeziumProtobuf,
                None,
            ) => Err(Error::Configuration(format!(
                "format.type={} requires format.schema_registry",
                self.kind.as_str()
            ))),
            (_, Some(_)) => Err(Error::Configuration(
                "format.schema_registry is only valid with a schema-registry format".into(),
            )),
            (_, None) => Ok(()),
        }
    }

    fn semantic_config(&self) -> serde_json::Value {
        serde_json::json!({
            "type": self.kind,
            "unavailable_value": self.unavailable_value,
            "tombstones_on_delete": self.tombstones_on_delete,
            "schema_registry": self.schema_registry.as_ref().map(SchemaRegistryConfig::semantic_config),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatType {
    RustiumJson,
    #[default]
    DebeziumJson,
    DebeziumJsonSchema,
    DebeziumAvro,
    DebeziumProtobuf,
}

impl FormatType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RustiumJson => "rustium_json",
            Self::DebeziumJson => "debezium_json",
            Self::DebeziumJsonSchema => "debezium_json_schema",
            Self::DebeziumAvro => "debezium_avro",
            Self::DebeziumProtobuf => "debezium_protobuf",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConfig {
    pub urls: Vec<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_schema_registry_timeout")]
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(default = "default_schema_registry_cache_capacity")]
    pub cache_capacity: usize,
}

impl SchemaRegistryConfig {
    fn validate(&self) -> Result<()> {
        if self.urls.is_empty() {
            return Err(Error::Configuration(
                "format.schema_registry.urls must contain at least one URL".into(),
            ));
        }
        for raw in &self.urls {
            let url = Url::parse(raw).map_err(|error| {
                Error::Configuration(format!(
                    "format.schema_registry URL {raw:?} is invalid: {error}"
                ))
            })?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return Err(Error::Configuration(format!(
                    "format.schema_registry URL {raw:?} must be an absolute HTTP(S) URL"
                )));
            }
        }
        if self.password.is_some() && self.username.is_none() {
            return Err(Error::Configuration(
                "format.schema_registry.password requires username".into(),
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(Error::Configuration(
                "format.schema_registry.request_timeout must be greater than zero".into(),
            ));
        }
        if self.cache_capacity == 0 {
            return Err(Error::Configuration(
                "format.schema_registry.cache_capacity must be greater than zero".into(),
            ));
        }
        Ok(())
    }

    fn semantic_config(&self) -> serde_json::Value {
        serde_json::json!({
            "urls": self.urls,
            "username": self.username,
            "request_timeout_ms": self.request_timeout.as_millis(),
            "cache_capacity": self.cache_capacity,
        })
    }
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
                delivery_timeout,
                properties,
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
                if !matches!(acks.as_str(), "all" | "-1") {
                    return Err(Error::Configuration(
                        "sink.acks must be all or -1 so Kafka replicates a batch before checkpointing"
                            .into(),
                    ));
                }
                if delivery_timeout.is_zero() || delivery_timeout.as_millis() > i32::MAX as u128 {
                    return Err(Error::Configuration(format!(
                        "sink.delivery_timeout must be between 1 and {} milliseconds",
                        i32::MAX
                    )));
                }
                const RESERVED: &[&str] = &[
                    "acks",
                    "bootstrap.servers",
                    "compression.codec",
                    "compression.type",
                    "delivery.timeout.ms",
                    "enable.idempotence",
                    "message.timeout.ms",
                    "metadata.broker.list",
                    "request.required.acks",
                ];
                if let Some(property) = properties
                    .keys()
                    .find(|property| RESERVED.contains(&property.as_str()))
                {
                    return Err(Error::Configuration(format!(
                        "sink.properties key {property:?} is managed by Rustium and cannot be overridden"
                    )));
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
    #[serde(default = "default_errors_max_retries")]
    pub errors_max_retries: i32,
    #[serde(default = "default_errors_retry_delay_initial")]
    #[serde(with = "humantime_serde")]
    pub errors_retry_delay_initial: Duration,
    #[serde(default = "default_errors_retry_delay_max")]
    #[serde(with = "humantime_serde")]
    pub errors_retry_delay_max: Duration,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
            max_batch_size: default_batch_size(),
            flush_interval: default_flush_interval(),
            shutdown_timeout: default_shutdown_timeout(),
            errors_max_retries: default_errors_max_retries(),
            errors_retry_delay_initial: default_errors_retry_delay_initial(),
            errors_retry_delay_max: default_errors_retry_delay_max(),
        }
    }
}

impl RuntimeSettings {
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_retries: self.errors_max_retries,
            initial_delay: self.errors_retry_delay_initial,
            max_delay: self.errors_retry_delay_max,
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
const fn default_postgres_status_update_interval() -> Duration {
    Duration::from_secs(10)
}
const fn default_postgres_snapshot_lock_timeout() -> Duration {
    Duration::from_secs(10)
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
fn default_postgres_interval_handling_mode() -> String {
    "postgres".into()
}
const fn default_postgres_money_fraction_digits() -> i16 {
    2
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

const fn default_schema_registry_timeout() -> Duration {
    Duration::from_secs(10)
}

const fn default_schema_registry_cache_capacity() -> usize {
    1_000
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
const fn default_errors_max_retries() -> i32 {
    10
}
const fn default_errors_retry_delay_initial() -> Duration {
    Duration::from_millis(300)
}
const fn default_errors_retry_delay_max() -> Duration {
    Duration::from_secs(10)
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
        assert_eq!(config.runtime.errors_max_retries, 10);
        assert_eq!(
            config.runtime.errors_retry_delay_initial,
            Duration::from_millis(300)
        );
        assert_eq!(
            config.runtime.errors_retry_delay_max,
            Duration::from_secs(10)
        );
        assert_eq!(config.runtime.retry_policy(), RetryPolicy::default());
        assert!(config.format.tombstones_on_delete);
        let postgres = config.source.as_postgresql().unwrap();
        assert_eq!(
            postgres.status_update_interval,
            default_postgres_status_update_interval()
        );
        assert!(postgres.tcp_keepalive);
        assert_eq!(
            postgres.offset_mismatch_strategy,
            PostgresOffsetMismatchStrategy::NoValidation
        );
        assert!(!postgres.drop_slot_on_stop);
        assert_eq!(postgres.lsn_flush_mode, PostgresLsnFlushMode::Connector);
        assert!(postgres.slot_stream_params.is_empty());
        assert!(postgres.database_initial_statements.is_empty());
        assert_eq!(
            postgres.snapshot_locking_mode,
            PostgresSnapshotLockingMode::None
        );
        assert_eq!(
            postgres.snapshot_lock_timeout,
            default_postgres_snapshot_lock_timeout()
        );
        assert_eq!(
            postgres.snapshot_isolation_mode,
            PostgresSnapshotIsolationMode::Serializable
        );
        assert!(postgres.xmin_fetch_interval.is_zero());
        assert!(postgres.ssl_root_cert.is_none());
        assert!(!postgres.include_unknown_datatypes);
        assert_eq!(
            postgres.money_fraction_digits,
            default_postgres_money_fraction_digits()
        );
        assert_eq!(
            postgres.schema_refresh_mode,
            PostgresSchemaRefreshMode::ColumnsDiff
        );
        let connection_url = Url::parse(&postgres.connection_url(true).unwrap()).unwrap();
        assert!(
            connection_url
                .query_pairs()
                .any(|(key, value)| key == "replication" && value == "database")
        );
        assert!(
            connection_url
                .query_pairs()
                .any(|(key, value)| key == "keepalives" && value == "1")
        );
        assert_eq!(
            config.source.as_postgresql().unwrap().hstore_handling_mode,
            "json"
        );
        assert_eq!(
            config
                .source
                .as_postgresql()
                .unwrap()
                .interval_handling_mode,
            "postgres"
        );
        assert!(
            !config
                .source
                .as_postgresql()
                .unwrap()
                .captures_logical_decoding_messages()
        );
        assert_eq!(
            config
                .source
                .as_postgresql()
                .unwrap()
                .publication_autocreate_mode,
            PublicationAutoCreateMode::Disabled
        );
        assert!(
            config.source.semantic_config()["publication_autocreate_mode"].is_null(),
            "the native default must preserve the pre-autocreate fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["replica_identity_autoset_values"].is_null(),
            "empty replica identity rules must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["publish_via_partition_root"].is_null(),
            "disabled partition-root publication must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["slot_failover"].is_null(),
            "disabled failover slots must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["offset_mismatch_strategy"].is_null(),
            "no_validation must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["lsn_flush_mode"].is_null(),
            "connector LSN flushing must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["snapshot_isolation_mode"].is_null(),
            "the default PostgreSQL snapshot isolation mode must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["slot_stream_params"].is_null(),
            "empty slot stream parameters must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["database_initial_statements"].is_null(),
            "empty database initial statements must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["interval_handling_mode"].is_null(),
            "the native PostgreSQL interval mode must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["include_unknown_datatypes"].is_null(),
            "omitting unknown PostgreSQL datatypes must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["money_fraction_digits"].is_null(),
            "the default PostgreSQL money scale must preserve the old fingerprint shape"
        );
        assert!(
            config.source.semantic_config()["schema_refresh_mode"].is_null(),
            "the pgoutput schema refresh compatibility mode must not change fingerprints"
        );
        assert!(
            config.source.semantic_config()["logical_decoding_messages"].is_null(),
            "disabled native logical decoding messages must preserve the old fingerprint shape"
        );
    }

    #[test]
    fn parses_native_postgresql_logical_decoding_message_filters() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  logical_decoding_messages: true\n  message_prefix_include_list: [orders\\..*, audit]\n",
        ))
        .unwrap();
        let source = configured.source.as_postgresql().unwrap();
        assert!(source.includes_message_prefix("orders.created"));
        assert!(source.includes_message_prefix("audit"));
        assert!(!source.includes_message_prefix("orders"));
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let conflicting = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  message_prefix_include_list: [orders]\n  message_prefix_exclude_list: [audit]\n",
        ))
        .unwrap_err();
        assert!(conflicting.to_string().contains("mutually exclusive"));

        let invalid = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  message_prefix_include_list: ['[']\n",
        ))
        .unwrap_err();
        assert!(
            invalid
                .to_string()
                .contains("not a valid regular expression")
        );
    }

    #[test]
    fn validates_native_postgresql_replica_identity_rules() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  replica_identity_autoset_values:\n    - table: public\\.orders\n      identity: full\n    - table: public\\.customers\n      identity: index\n      index: customers_replica_key\n",
        ))
        .unwrap();
        let rules = &configured
            .source
            .as_postgresql()
            .unwrap()
            .replica_identity_autoset_values;
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].identity, PostgresReplicaIdentity::Full);
        assert_eq!(rules[1].index.as_deref(), Some("customers_replica_key"));
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let missing_index = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  replica_identity_autoset_values:\n    - table: public\\.orders\n      identity: index\n",
        ))
        .unwrap_err();
        assert!(
            missing_index
                .to_string()
                .contains("requires a non-empty index")
        );

        let unexpected_index = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  replica_identity_autoset_values:\n    - table: public\\.orders\n      identity: full\n      index: orders_key\n",
        ))
        .unwrap_err();
        assert!(unexpected_index.to_string().contains("valid only"));
    }

    #[test]
    fn parses_native_postgresql_partition_root_publication() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  publication: rustium_pub\n",
            "  publication: rustium_pub\n  publish_via_partition_root: true\n",
        ))
        .unwrap();
        assert!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .publish_via_partition_root
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );
    }

    #[test]
    fn parses_native_postgresql_failover_slot() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  slot_failover: true\n",
        ))
        .unwrap();
        assert!(configured.source.as_postgresql().unwrap().slot_failover);
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let external = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  slot_failover: true\n  slot_ownership: external\n",
        ))
        .unwrap_err();
        assert!(external.to_string().contains("only for a managed"));
    }

    #[test]
    fn parses_native_postgresql_drop_slot_on_stop_without_changing_fingerprint() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  drop_slot_on_stop: true\n",
        ))
        .unwrap();
        assert!(configured.source.as_postgresql().unwrap().drop_slot_on_stop);
        assert_eq!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let external = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  drop_slot_on_stop: true\n  slot_ownership: external\n",
        ))
        .unwrap_err();
        assert!(external.to_string().contains("only for a managed"));
    }

    #[test]
    fn parses_native_postgresql_offset_mismatch_strategy() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  offset_mismatch_strategy: trust_slot\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .offset_mismatch_strategy,
            PostgresOffsetMismatchStrategy::TrustSlot
        );
        assert_eq!(
            configured.source.semantic_config()["offset_mismatch_strategy"],
            "trust_slot"
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let external = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  slot_ownership: external\n  offset_mismatch_strategy: trust_offset\n",
        ))
        .unwrap_err();
        assert!(
            external
                .to_string()
                .contains("requires slot_ownership=managed")
        );
    }

    #[test]
    fn parses_native_postgresql_lsn_flush_mode() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  lsn_flush_mode: connector_and_driver\n",
        ))
        .unwrap();
        assert_eq!(
            configured.source.as_postgresql().unwrap().lsn_flush_mode,
            PostgresLsnFlushMode::ConnectorAndDriver
        );
        assert_eq!(
            configured.source.semantic_config()["lsn_flush_mode"],
            "connector_and_driver"
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );
    }

    #[test]
    fn parses_native_postgresql_slot_stream_origin() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  slot_stream_params:\n    origin: none\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .slot_stream_params
                .get("origin")
                .map(String::as_str),
            Some("none")
        );
        assert_eq!(
            configured.source.semantic_config()["slot_stream_params"]["origin"],
            "none"
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        for params in ["    origin: local\n", "    add-tables: public.orders\n"] {
            let invalid = Config::from_yaml(&CONFIG.replace(
                "  slot_name: rustium_orders\n",
                &format!("  slot_name: rustium_orders\n  slot_stream_params:\n{params}"),
            ))
            .unwrap_err();
            assert!(invalid.to_string().contains("slot_stream_params"));
        }
    }

    #[test]
    fn parses_native_postgresql_database_initial_statements() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  database_initial_statements:\n    - SET application_name = 'rustium-cdc'\n    - SET statement_timeout = '5s'\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .database_initial_statements,
            [
                "SET application_name = 'rustium-cdc'",
                "SET statement_timeout = '5s'"
            ]
        );
        assert_eq!(
            configured.source.semantic_config()["database_initial_statements"][0],
            "SET application_name = 'rustium-cdc'"
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let blank = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  database_initial_statements: [' ']\n",
        ))
        .unwrap_err();
        assert!(blank.to_string().contains("database_initial_statements"));
    }

    #[test]
    fn parses_native_postgresql_snapshot_locking_without_changing_fingerprint() {
        let baseline = Config::from_yaml(CONFIG).unwrap();
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  snapshot_locking_mode: shared\n  snapshot_lock_timeout: 250ms\n",
        ))
        .unwrap();
        let source = configured.source.as_postgresql().unwrap();
        assert_eq!(
            source.snapshot_locking_mode,
            PostgresSnapshotLockingMode::Shared
        );
        assert_eq!(source.snapshot_lock_timeout, Duration::from_millis(250));
        assert_eq!(baseline.fingerprint(), configured.fingerprint());

        let too_large = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  snapshot_lock_timeout: 2147483648ms\n",
        ))
        .unwrap_err();
        assert!(too_large.to_string().contains("must not exceed"));
    }

    #[test]
    fn parses_native_postgresql_snapshot_isolation_modes_into_fingerprint() {
        let baseline = Config::from_yaml(CONFIG).unwrap();
        for mode in [
            "serializable",
            "repeatable_read",
            "read_committed",
            "read_uncommitted",
        ] {
            let configured = Config::from_yaml(&CONFIG.replace(
                "  slot_name: rustium_orders\n",
                &format!("  slot_name: rustium_orders\n  snapshot_isolation_mode: {mode}\n"),
            ))
            .unwrap();
            let expected = match mode {
                "serializable" => PostgresSnapshotIsolationMode::Serializable,
                "repeatable_read" => PostgresSnapshotIsolationMode::RepeatableRead,
                "read_committed" => PostgresSnapshotIsolationMode::ReadCommitted,
                "read_uncommitted" => PostgresSnapshotIsolationMode::ReadUncommitted,
                _ => unreachable!(),
            };
            assert_eq!(
                configured
                    .source
                    .as_postgresql()
                    .unwrap()
                    .snapshot_isolation_mode,
                expected
            );
            if matches!(mode, "serializable" | "repeatable_read") {
                assert_eq!(baseline.fingerprint(), configured.fingerprint());
            } else {
                assert_ne!(baseline.fingerprint(), configured.fingerprint());
            }
        }

        let invalid = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  snapshot_isolation_mode: dirty_read\n",
        ))
        .unwrap_err();
        assert!(invalid.to_string().contains("dirty_read"));
    }

    #[test]
    fn parses_native_postgresql_xmin_fetch_interval_into_fingerprint() {
        let baseline = Config::from_yaml(CONFIG).unwrap();
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  xmin_fetch_interval: 25ms\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .xmin_fetch_interval,
            Duration::from_millis(25)
        );
        assert_ne!(baseline.fingerprint(), configured.fingerprint());

        let explicit_default = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  xmin_fetch_interval: 0ms\n",
        ))
        .unwrap();
        assert_eq!(baseline.fingerprint(), explicit_default.fingerprint());
    }

    #[test]
    fn validates_native_postgresql_tls_material() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  ssl_mode: verify-full\n  ssl_root_cert: /run/secrets/postgres-ca.pem\n",
        ))
        .unwrap();
        let source = configured.source.as_postgresql().unwrap();
        assert_eq!(source.ssl_mode, "verify-full");
        assert_eq!(
            source.ssl_root_cert.as_deref(),
            Some("/run/secrets/postgres-ca.pem")
        );
        let connection_url = Url::parse(&source.connection_url(true).unwrap()).unwrap();
        assert!(connection_url.query_pairs().any(|(key, value)| {
            key == "sslrootcert" && value == "/run/secrets/postgres-ca.pem"
        }));
        assert_eq!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint(),
            "PostgreSQL TLS transport settings must not change event semantics"
        );

        let invalid_mode = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  ssl_mode: verify_identity\n",
        ))
        .unwrap_err();
        assert!(invalid_mode.to_string().contains("source.ssl_mode"));

        let client_key = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  ssl_mode: require\n  ssl_cert: /run/secrets/client.pem\n  ssl_key: /run/secrets/client.key\n",
        ))
        .unwrap_err();
        assert!(
            client_key
                .to_string()
                .contains("pg_walstream rustls transport")
        );
    }

    #[test]
    fn parses_native_postgresql_interval_handling_mode() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  interval_handling_mode: numeric\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .interval_handling_mode,
            "numeric"
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let invalid = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  interval_handling_mode: invalid\n",
        ))
        .unwrap_err();
        assert!(invalid.to_string().contains("postgres, numeric, or string"));
    }

    #[test]
    fn parses_native_postgresql_unknown_datatype_mode() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  include_unknown_datatypes: true\n",
        ))
        .unwrap();
        assert!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .include_unknown_datatypes
        );
        assert_eq!(
            configured.source.semantic_config()["include_unknown_datatypes"],
            true
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );
    }

    #[test]
    fn parses_native_postgresql_money_fraction_digits() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  money_fraction_digits: 4\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .money_fraction_digits,
            4
        );
        assert_eq!(
            configured.source.semantic_config()["money_fraction_digits"],
            4
        );
        assert_ne!(
            Config::from_yaml(CONFIG).unwrap().fingerprint(),
            configured.fingerprint()
        );

        let negative = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  money_fraction_digits: -1\n",
        ))
        .unwrap();
        assert_eq!(
            negative
                .source
                .as_postgresql()
                .unwrap()
                .money_fraction_digits,
            -1
        );
        assert!(
            Config::from_yaml(&CONFIG.replace(
                "  slot_name: rustium_orders\n",
                "  slot_name: rustium_orders\n  money_fraction_digits: 32768\n",
            ))
            .is_err()
        );
    }

    #[test]
    fn parses_native_postgresql_schema_refresh_modes_without_changing_fingerprints() {
        let baseline = Config::from_yaml(CONFIG).unwrap();
        let configured = Config::from_yaml(&CONFIG.replace(
            "  slot_name: rustium_orders\n",
            "  slot_name: rustium_orders\n  schema_refresh_mode: columns_diff_exclude_unchanged_toast\n",
        ))
        .unwrap();
        assert_eq!(
            configured
                .source
                .as_postgresql()
                .unwrap()
                .schema_refresh_mode,
            PostgresSchemaRefreshMode::ColumnsDiffExcludeUnchangedToast
        );
        assert_eq!(baseline.fingerprint(), configured.fingerprint());
        assert!(
            configured.source.semantic_config()["schema_refresh_mode"].is_null(),
            "schema.refresh.mode is operational compatibility under pgoutput"
        );

        assert!(
            Config::from_yaml(&CONFIG.replace(
                "  slot_name: rustium_orders\n",
                "  slot_name: rustium_orders\n  schema_refresh_mode: invalid\n",
            ))
            .is_err()
        );
    }

    #[test]
    fn validates_native_schema_registry_formats() {
        let config = Config::from_yaml(
            r#"
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
format:
  type: debezium_json_schema
  schema_registry:
    urls: [https://registry-1:8081, https://registry-2:8081]
    username: registry-user
    password: registry-secret
    request_timeout: 5s
sink:
  type: kafka
  bootstrap_servers: [kafka:9092]
  topic_prefix: app
"#,
        )
        .unwrap();
        assert_eq!(config.format.kind, FormatType::DebeziumJsonSchema);
        assert_eq!(
            config
                .format
                .schema_registry
                .as_ref()
                .unwrap()
                .request_timeout,
            Duration::from_secs(5)
        );

        let error = Config::from_yaml(
            &CONFIG.replace(
                "sink:\n",
                "format:\n  type: debezium_json_schema\n  schema_registry:\n    urls: [http://registry:8081]\nsink:\n",
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("requires sink.type=kafka"));

        let avro = Config::from_yaml(
            r#"
api_version: rustium.io/v1alpha1
kind: Connector
metadata:
  name: orders-avro
source:
  type: mysql
  databases: [app]
  username: rustium
  password: secret
format:
  type: debezium_avro
  schema_registry:
    urls: [http://registry:8081]
    cache_capacity: 32
sink:
  type: kafka
  bootstrap_servers: [kafka:9092]
  topic_prefix: app
"#,
        )
        .unwrap();
        assert_eq!(avro.format.kind, FormatType::DebeziumAvro);
        assert_eq!(
            avro.format.schema_registry.as_ref().unwrap().cache_capacity,
            32
        );

        let protobuf = Config::from_yaml(
            r#"
api_version: rustium.io/v1alpha1
kind: Connector
metadata:
  name: orders-protobuf
source:
  type: mysql
  databases: [app]
  username: rustium
  password: secret
format:
  type: debezium_protobuf
  schema_registry:
    urls: [http://registry:8081]
sink:
  type: kafka
  bootstrap_servers: [kafka:9092]
  topic_prefix: app
"#,
        )
        .unwrap();
        assert_eq!(protobuf.format.kind, FormatType::DebeziumProtobuf);
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
    fn validates_anchored_snapshot_collection_filters() {
        let config = Config::from_yaml(&CONFIG.replace(
            "sink:\n",
            "snapshot:\n  include_collections: [public\\.orders]\nsink:\n",
        ))
        .unwrap();
        assert!(config.snapshot.includes_collection("public.orders"));
        assert!(!config.snapshot.includes_collection("archive.public.orders"));
        assert!(!config.snapshot.includes_collection("public.orders_history"));

        let error = Config::from_yaml(&CONFIG.replace(
            "sink:\n",
            "snapshot:\n  include_collections: ['[invalid']\nsink:\n",
        ))
        .unwrap_err();
        assert!(error.to_string().contains("snapshot include selector"));
    }

    #[test]
    fn preserves_snapshot_fingerprint_shape_without_a_filter() {
        assert_eq!(
            SnapshotConfig::default().semantic_config(),
            serde_json::json!({
                "mode": "initial",
                "fetch_size": default_snapshot_fetch_size(),
            })
        );

        let filtered = SnapshotConfig {
            include_collections: vec![r"public\.orders".into()],
            ..SnapshotConfig::default()
        };
        assert_eq!(
            filtered.semantic_config()["include_collections"],
            serde_json::json!([r"public\.orders"])
        );
    }

    #[test]
    fn rejects_zero_postgresql_connect_timeout() {
        let error = Config::from_yaml(&CONFIG.replace(
            "  database: app\n",
            "  database: app\n  connect_timeout: 0s\n",
        ))
        .unwrap_err();
        assert!(error.to_string().contains("source.connect_timeout"));
    }

    #[test]
    fn validates_postgresql_connection_tuning() {
        let configured = Config::from_yaml(&CONFIG.replace(
            "  database: app\n",
            "  database: app\n  status_update_interval: 125ms\n  tcp_keepalive: false\n",
        ))
        .unwrap();
        let source = configured.source.as_postgresql().unwrap();
        assert_eq!(source.status_update_interval, Duration::from_millis(125));
        assert!(!source.tcp_keepalive);
        let connection_url = Url::parse(&source.connection_url(false).unwrap()).unwrap();
        assert!(
            connection_url
                .query_pairs()
                .any(|(key, value)| key == "keepalives" && value == "0")
        );

        let error = Config::from_yaml(&CONFIG.replace(
            "  database: app\n",
            "  database: app\n  status_update_interval: 0ms\n",
        ))
        .unwrap_err();
        assert!(error.to_string().contains("source.status_update_interval"));
    }

    #[test]
    fn validates_native_retry_settings() {
        let configured = Config::from_yaml(&format!(
            "{CONFIG}\nruntime:\n  errors_max_retries: -1\n  errors_retry_delay_initial: 25ms\n  errors_retry_delay_max: 1s\n"
        ))
        .unwrap();
        assert_eq!(configured.runtime.errors_max_retries, -1);
        assert_eq!(
            configured.runtime.errors_retry_delay_initial,
            Duration::from_millis(25)
        );
        assert_eq!(
            configured.runtime.retry_policy(),
            RetryPolicy {
                max_retries: -1,
                initial_delay: Duration::from_millis(25),
                max_delay: Duration::from_secs(1),
            }
        );

        for (settings, expected) in [
            ("errors_max_retries: -2", "errors_max_retries"),
            (
                "errors_retry_delay_initial: 0ms",
                "errors_retry_delay_initial",
            ),
            (
                "errors_retry_delay_initial: 2s\n  errors_retry_delay_max: 1s",
                "errors_retry_delay_max",
            ),
        ] {
            let error = Config::from_yaml(&format!("{CONFIG}\nruntime:\n  {settings}\n"))
                .expect_err("invalid retry settings must fail");
            assert!(error.to_string().contains(expected));
        }
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
    fn rejects_non_durable_kafka_sink_settings() {
        let kafka = CONFIG.replace(
            "sink:\n  type: stdout\n  topic_prefix: app",
            "sink:\n  type: kafka\n  bootstrap_servers: [127.0.0.1:9092]\n  topic_prefix: app\n  acks: all\n  delivery_timeout: 3s",
        );
        Config::from_yaml(&kafka).unwrap();

        let acknowledgements = Config::from_yaml(&kafka.replace("acks: all", "acks: '1'"))
            .expect_err("non-durable Kafka acknowledgements must fail");
        assert!(acknowledgements.to_string().contains("sink.acks"));

        let timeout =
            Config::from_yaml(&kafka.replace("delivery_timeout: 3s", "delivery_timeout: 0s"))
                .expect_err("zero Kafka delivery timeout must fail");
        assert!(timeout.to_string().contains("sink.delivery_timeout"));

        let override_property = Config::from_yaml(&format!(
            "{kafka}\n  properties:\n    enable.idempotence: 'false'\n"
        ))
        .expect_err("managed Kafka producer property must fail");
        assert!(
            override_property
                .to_string()
                .contains("cannot be overridden")
        );
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
    fn parses_native_mysql_java_tls_stores_without_fingerprinting_passwords() {
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
  ssl_mode: verify_identity
  ssl_keystore: /run/secrets/client.p12
  ssl_keystore_password: client-secret
  ssl_truststore: /run/secrets/trust.jks
  ssl_truststore_password: trust-secret
sink:
  type: stdout
  topic_prefix: inventory
"#;
        let config = Config::from_yaml(raw).unwrap();
        let source = config.source.as_mysql().unwrap();
        assert_eq!(
            source.ssl_keystore.as_deref(),
            Some("/run/secrets/client.p12")
        );
        assert_eq!(
            source.ssl_truststore.as_deref(),
            Some("/run/secrets/trust.jks")
        );

        let rotated = Config::from_yaml(
            &raw.replace("client-secret", "rotated-client")
                .replace("trust-secret", "rotated-trust"),
        )
        .unwrap();
        assert_eq!(config.fingerprint(), rotated.fingerprint());

        let different_store =
            Config::from_yaml(&raw.replace("client.p12", "replacement.p12")).unwrap();
        assert_ne!(config.fingerprint(), different_store.fingerprint());
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
