use super::*;

pub(super) fn parse(raw: &str) -> Result<Config> {
    let interpolated = interpolate_environment(raw)?;
    let properties = parse_properties(&interpolated)?;
    let connector_class = required(&properties, "connector.class")?;
    if connector_class.contains("MySqlConnector") {
        return parse_mysql(&properties);
    }
    if connector_class.contains("SqlServerConnector") {
        return parse_sqlserver(&properties);
    }
    if !connector_class.contains("PostgresConnector") {
        return Err(Error::Configuration(format!(
            "unsupported Debezium connector.class {connector_class:?}; PostgreSQL, MySQL, and SQL Server are prioritized"
        )));
    }
    if let Some(plugin) = properties.get("plugin.name")
        && plugin != "pgoutput"
    {
        return Err(Error::Configuration(format!(
            "plugin.name={plugin:?} is not supported; Rustium uses pgoutput"
        )));
    }

    let table_include = csv_property(properties.get("table.include.list"));
    let table_exclude = csv_property(properties.get("table.exclude.list"));
    let schema_include = csv_property(properties.get("schema.include.list"));
    let schema_exclude = csv_property(properties.get("schema.exclude.list"));
    let include = if table_include.is_empty() {
        schema_include
            .into_iter()
            .map(|schema| format!("(?:{schema})\\..+"))
            .collect()
    } else {
        table_include
    };
    let mut exclude = table_exclude;
    exclude.extend(
        schema_exclude
            .into_iter()
            .map(|schema| format!("(?:{schema})\\..+")),
    );
    let signal_collections = csv_property(properties.get("signal.data.collection"));
    if signal_collections.len() > 1 {
        return Err(Error::Configuration(
            "Rustium PostgreSQL currently accepts one table in signal.data.collection".into(),
        ));
    }
    let signal_data_collection = signal_collections.into_iter().next();
    let signal_channels = csv_property(properties.get("signal.enabled.channels"));
    let requested_signal_channels = if signal_channels.is_empty() {
        default_signal_enabled_channels()
    } else {
        signal_channels
    };
    let mut signal_enabled_channels = Vec::new();
    for channel in &requested_signal_channels {
        let mapped = match channel.as_str() {
            "source" | "file" | "in-process" | "kafka" => Some(channel.clone()),
            "jmx" => Some("in-process".into()),
            _ => None,
        };
        if let Some(mapped) = mapped
            && !signal_enabled_channels.contains(&mapped)
        {
            signal_enabled_channels.push(mapped);
        }
    }
    if signal_enabled_channels.is_empty() {
        return Err(Error::Configuration(
            "PostgreSQL signal.enabled.channels enables no implemented channel".into(),
        ));
    }
    let mut warnings = Vec::new();
    for channel in &requested_signal_channels {
        match channel.as_str() {
            "jmx" => warnings.push(
                "signal.enabled.channels=jmx is JVM-specific; Rustium maps it to the bounded in-process channel and its SignalSender/HTTP management API"
                    .into(),
            ),
            "source" | "file" | "in-process" | "kafka" => {}
            _ => warnings.push(format!(
                "signal.enabled.channels={channel} is not implemented and was ignored"
            )),
        }
    }
    if bool_value(
        &properties,
        "incremental.snapshot.allow.schema.changes",
        false,
    )? {
        return Err(Error::Configuration(
            "incremental.snapshot.allow.schema.changes=true is not implemented for PostgreSQL"
                .into(),
        ));
    }

    assemble_config(
        &properties,
        SourceConfig::Postgresql(Box::new(PostgresSourceConfig {
            hostname: properties
                .get("database.hostname")
                .cloned()
                .unwrap_or_else(default_hostname),
            port: u16_value(&properties, "database.port", default_postgres_port())?,
            database: required(&properties, "database.dbname")?.to_string(),
            username: required(&properties, "database.user")?.to_string(),
            password: required(&properties, "database.password")?.to_string(),
            publication: properties
                .get("publication.name")
                .cloned()
                .unwrap_or_else(|| "dbz_publication".into()),
            publication_autocreate_mode: publication_autocreate_mode(
                properties
                    .get("publication.autocreate.mode")
                    .map_or("all_tables", String::as_str),
            )?,
            replica_identity_autoset_values: postgres_replica_identity_rules(
                properties.get("replica.identity.autoset.values"),
            )?,
            publish_via_partition_root: bool_value(
                &properties,
                "publish.via.partition.root",
                false,
            )?,
            slot_name: properties
                .get("slot.name")
                .cloned()
                .unwrap_or_else(|| "debezium".into()),
            slot_failover: bool_value(&properties, "slot.failover", false)?,
            slot_ownership: SlotOwnership::Managed,
            tables: TableSelection { include, exclude },
            ssl_mode: properties
                .get("database.sslmode")
                .cloned()
                .unwrap_or_else(default_ssl_mode),
            connect_timeout: duration_ms(
                &properties,
                "connection.validation.timeout.ms",
                default_connect_timeout(),
            )?,
            heartbeat_interval: duration_ms(&properties, "heartbeat.interval.ms", Duration::ZERO)?,
            heartbeat_action_query: properties
                .get("heartbeat.action.query")
                .filter(|query| !query.trim().is_empty())
                .cloned(),
            heartbeat_topics_prefix: properties
                .get("topic.heartbeat.prefix")
                .or_else(|| properties.get("heartbeat.topics.prefix"))
                .cloned()
                .unwrap_or_else(default_heartbeat_topics_prefix),
            heartbeat_topic_name: properties.get("topic.heartbeat.name").cloned(),
            signal_data_collection,
            signal_enabled_channels,
            signal_file: properties
                .get("signal.file")
                .cloned()
                .unwrap_or_else(default_signal_file),
            signal_poll_interval: duration_ms(
                &properties,
                "signal.poll.interval.ms",
                default_signal_poll_interval(),
            )?,
            signal_kafka_topic: properties.get("signal.kafka.topic").cloned(),
            signal_kafka_bootstrap_servers: csv_property(
                properties.get("signal.kafka.bootstrap.servers"),
            ),
            signal_kafka_group_id: properties
                .get("signal.kafka.groupId")
                .cloned()
                .unwrap_or_else(default_signal_kafka_group_id),
            signal_kafka_poll_timeout: duration_ms(
                &properties,
                "signal.kafka.poll.timeout.ms",
                default_signal_kafka_poll_timeout(),
            )?,
            signal_kafka_consumer_properties: properties
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix("signal.consumer.")
                        .map(|key| (key.to_string(), value.clone()))
                })
                .collect(),
            incremental_snapshot_chunk_size: usize_value(
                &properties,
                "incremental.snapshot.chunk.size",
                default_incremental_snapshot_chunk_size(),
            )?,
            incremental_snapshot_watermarking_strategy: properties
                .get("incremental.snapshot.watermarking.strategy")
                .cloned()
                .unwrap_or_else(default_incremental_snapshot_watermarking_strategy),
            read_only: bool_value(&properties, "read.only", false)?,
            hstore_handling_mode: properties
                .get("hstore.handling.mode")
                .map(|mode| mode.to_ascii_lowercase())
                .unwrap_or_else(default_hstore_handling_mode),
            interval_handling_mode: postgres_interval_handling_mode(
                properties
                    .get("interval.handling.mode")
                    .map_or("numeric", String::as_str),
            )?,
        })),
        SnapshotConfig {
            mode: snapshot_mode(
                properties
                    .get("snapshot.mode")
                    .map_or("initial", String::as_str),
            )?,
            fetch_size: usize_value(
                &properties,
                "snapshot.fetch.size",
                default_snapshot_fetch_size(),
            )?,
            include_collections: csv_property(properties.get("snapshot.include.collection.list")),
        },
        warnings,
    )
}

fn parse_mysql(properties: &BTreeMap<String, String>) -> Result<Config> {
    let database_include = csv_property(properties.get("database.include.list"));
    let database_exclude = csv_property(properties.get("database.exclude.list"));
    let mut table_exclude = csv_property(properties.get("table.exclude.list"));
    table_exclude.extend(
        database_exclude
            .into_iter()
            .map(|database| format!("(?:{database})\\..+")),
    );

    let mut warnings = Vec::new();
    if properties.contains_key("database.server.id.offset") {
        warnings.push(
            "database.server.id.offset is accepted but ignored because Rustium runs one source task per connector"
                .into(),
        );
    }
    let signal_collections = csv_property(properties.get("signal.data.collection"));
    if signal_collections.len() > 1 {
        return Err(Error::Configuration(
            "Rustium MySQL currently accepts one table in signal.data.collection".into(),
        ));
    }
    let signal_data_collection = signal_collections.into_iter().next();
    let requested_signal_channels = {
        let channels = csv_property(properties.get("signal.enabled.channels"));
        if channels.is_empty() {
            default_signal_enabled_channels()
        } else {
            channels
        }
    };
    let mut signal_enabled_channels = Vec::new();
    for channel in &requested_signal_channels {
        let mapped = match channel.as_str() {
            "source" | "file" | "in-process" | "kafka" => Some(channel.clone()),
            "jmx" => Some("in-process".into()),
            _ => None,
        };
        if let Some(mapped) = mapped
            && !signal_enabled_channels.contains(&mapped)
        {
            signal_enabled_channels.push(mapped);
        }
    }
    if signal_enabled_channels.is_empty() {
        return Err(Error::Configuration(
            "MySQL signal.enabled.channels enables no implemented channel".into(),
        ));
    }
    for channel in &requested_signal_channels {
        match channel.as_str() {
            "jmx" => warnings.push(
                "signal.enabled.channels=jmx is JVM-specific; Rustium maps it to the bounded in-process channel and its SignalSender/HTTP management API".into(),
            ),
            "source" | "file" | "in-process" | "kafka" => {}
            _ => warnings.push(format!("signal.enabled.channels={channel} is not implemented and was ignored")),
        }
    }
    if bool_value(
        properties,
        "incremental.snapshot.allow.schema.changes",
        false,
    )? {
        return Err(Error::Configuration(
            "incremental.snapshot.allow.schema.changes=true is not implemented for MySQL".into(),
        ));
    }
    let gtid_source_includes = csv_property(properties.get("gtid.source.includes"));
    let gtid_source_excludes = csv_property(properties.get("gtid.source.excludes"));
    if !gtid_source_includes.is_empty() && !gtid_source_excludes.is_empty() {
        return Err(Error::Configuration(
            "gtid.source.includes and gtid.source.excludes cannot both be configured".into(),
        ));
    }

    assemble_config(
        properties,
        SourceConfig::Mysql(MySqlSourceConfig {
            hostname: properties
                .get("database.hostname")
                .cloned()
                .unwrap_or_else(default_hostname),
            port: u16_value(properties, "database.port", default_mysql_port())?,
            username: required(properties, "database.user")?.to_string(),
            password: required(properties, "database.password")?.to_string(),
            databases: database_include,
            server_id: u32_value(properties, "database.server.id", default_mysql_server_id())?,
            tables: TableSelection {
                include: csv_property(properties.get("table.include.list")),
                exclude: table_exclude,
            },
            ssl_mode: properties
                .get("database.ssl.mode")
                .cloned()
                .unwrap_or_else(default_mysql_ssl_mode),
            connection_time_zone: properties
                .get("database.connectionTimeZone")
                .cloned()
                .unwrap_or_else(default_mysql_connection_time_zone),
            ssl_ca: properties.get("database.ssl.ca").cloned(),
            ssl_cert: properties.get("database.ssl.cert").cloned(),
            ssl_key: properties.get("database.ssl.key").cloned(),
            ssl_keystore: properties.get("database.ssl.keystore").cloned(),
            ssl_keystore_password: properties.get("database.ssl.keystore.password").cloned(),
            ssl_truststore: properties.get("database.ssl.truststore").cloned(),
            ssl_truststore_password: properties.get("database.ssl.truststore.password").cloned(),
            connect_timeout: duration_ms(
                properties,
                "connect.timeout.ms",
                default_connect_timeout(),
            )?,
            connect_keep_alive: bool_value(properties, "connect.keep.alive", true)?,
            connect_keep_alive_interval: duration_ms(
                properties,
                "connect.keep.alive.interval.ms",
                default_mysql_keep_alive_interval(),
            )?,
            reconnect_max_attempts: u32_value(
                properties,
                "rustium.source.reconnect.max.attempts",
                default_mysql_reconnect_max_attempts(),
            )?,
            schema_history_skip_unparseable_ddl: bool_value(
                properties,
                "schema.history.internal.skip.unparseable.ddl",
                false,
            )?,
            gtid_source_includes,
            gtid_source_excludes,
            gtid_source_filter_dml_events: bool_value(
                properties,
                "gtid.source.filter.dml.events",
                true,
            )?,
            heartbeat_interval: duration_ms(properties, "heartbeat.interval.ms", Duration::ZERO)?,
            heartbeat_action_query: properties
                .get("heartbeat.action.query")
                .filter(|query| !query.trim().is_empty())
                .cloned(),
            heartbeat_topics_prefix: properties
                .get("topic.heartbeat.prefix")
                .or_else(|| properties.get("heartbeat.topics.prefix"))
                .cloned()
                .unwrap_or_else(default_heartbeat_topics_prefix),
            heartbeat_topic_name: properties.get("topic.heartbeat.name").cloned(),
            signal_data_collection,
            signal_enabled_channels,
            signal_file: properties
                .get("signal.file")
                .cloned()
                .unwrap_or_else(default_signal_file),
            signal_poll_interval: duration_ms(
                properties,
                "signal.poll.interval.ms",
                default_signal_poll_interval(),
            )?,
            incremental_snapshot_chunk_size: usize_value(
                properties,
                "incremental.snapshot.chunk.size",
                default_incremental_snapshot_chunk_size(),
            )?,
            incremental_snapshot_watermarking_strategy: properties
                .get("incremental.snapshot.watermarking.strategy")
                .cloned()
                .unwrap_or_else(default_incremental_snapshot_watermarking_strategy),
            signal_kafka_topic: properties.get("signal.kafka.topic").cloned(),
            signal_kafka_bootstrap_servers: csv_property(
                properties.get("signal.kafka.bootstrap.servers"),
            ),
            signal_kafka_group_id: properties
                .get("signal.kafka.groupId")
                .cloned()
                .unwrap_or_else(default_signal_kafka_group_id),
            signal_kafka_poll_timeout: duration_ms(
                properties,
                "signal.kafka.poll.timeout.ms",
                default_signal_kafka_poll_timeout(),
            )?,
            signal_kafka_consumer_properties: properties
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix("signal.consumer.")
                        .map(|key| (key.to_string(), value.clone()))
                })
                .collect(),
        }),
        SnapshotConfig {
            mode: snapshot_mode(
                properties
                    .get("snapshot.mode")
                    .map_or("initial", String::as_str),
            )?,
            fetch_size: usize_value(
                properties,
                "snapshot.fetch.size",
                default_snapshot_fetch_size(),
            )?,
            include_collections: csv_property(properties.get("snapshot.include.collection.list")),
        },
        warnings,
    )
}

fn parse_sqlserver(properties: &BTreeMap<String, String>) -> Result<Config> {
    let databases = csv_property(properties.get("database.names"));
    if databases.len() != 1 {
        return Err(Error::Configuration(
            "Rustium currently requires exactly one SQL Server database in database.names".into(),
        ));
    }
    if properties
        .get("data.query.mode")
        .is_some_and(|mode| mode != "direct")
    {
        return Err(Error::Configuration(
            "Rustium currently requires data.query.mode=direct for SQL Server CDC".into(),
        ));
    }
    let poll_interval = duration_ms(properties, "poll.interval.ms", default_poll_interval())?;
    let signal_collections = csv_property(properties.get("signal.data.collection"));
    if signal_collections.len() > 1 {
        return Err(Error::Configuration(
            "Rustium SQL Server currently accepts one table in signal.data.collection".into(),
        ));
    }
    let signal_data_collection = signal_collections.into_iter().next();
    let requested_signal_channels = {
        let channels = csv_property(properties.get("signal.enabled.channels"));
        if channels.is_empty() {
            default_signal_enabled_channels()
        } else {
            channels
        }
    };
    let mut signal_enabled_channels = Vec::new();
    let mut warnings = Vec::new();
    for channel in &requested_signal_channels {
        let mapped = match channel.as_str() {
            "source" | "file" | "in-process" | "kafka" => Some(channel.clone()),
            "jmx" => Some("in-process".into()),
            _ => None,
        };
        if let Some(mapped) = mapped
            && !signal_enabled_channels.contains(&mapped)
        {
            signal_enabled_channels.push(mapped);
        }
    }
    if signal_enabled_channels.is_empty() {
        return Err(Error::Configuration(
            "SQL Server signal.enabled.channels enables no implemented channel".into(),
        ));
    }
    for channel in &requested_signal_channels {
        match channel.as_str() {
            "jmx" => warnings.push(
                "signal.enabled.channels=jmx is JVM-specific; Rustium maps it to the bounded in-process channel and its SignalSender/HTTP management API".into(),
            ),
            "source" | "file" | "in-process" | "kafka" => {}
            _ => warnings.push(format!("signal.enabled.channels={channel} is not implemented and was ignored")),
        }
    }
    if bool_value(
        properties,
        "incremental.snapshot.allow.schema.changes",
        false,
    )? {
        return Err(Error::Configuration(
            "incremental.snapshot.allow.schema.changes=true is not implemented for SQL Server"
                .into(),
        ));
    }
    assemble_config(
        properties,
        SourceConfig::Sqlserver(SqlServerSourceConfig {
            hostname: properties
                .get("database.hostname")
                .cloned()
                .unwrap_or_else(default_hostname),
            port: u16_value(properties, "database.port", default_sqlserver_port())?,
            username: required(properties, "database.user")?.to_string(),
            password: required(properties, "database.password")?.to_string(),
            databases,
            tables: TableSelection {
                include: csv_property(properties.get("table.include.list")),
                exclude: csv_property(properties.get("table.exclude.list")),
            },
            connect_timeout: duration_ms(
                properties,
                "database.connection.timeout.ms",
                default_connect_timeout(),
            )?,
            encrypt: bool_value(properties, "database.encrypt", true)?,
            trust_server_certificate: bool_value(
                properties,
                "database.trustServerCertificate",
                false,
            )?,
            poll_interval,
            streaming_fetch_size: usize_value(
                properties,
                "streaming.fetch.size",
                default_streaming_fetch_size(),
            )?,
            snapshot_isolation_mode: properties
                .get("snapshot.isolation.mode")
                .cloned()
                .unwrap_or_else(default_sqlserver_snapshot_isolation),
            heartbeat_interval: duration_ms(properties, "heartbeat.interval.ms", Duration::ZERO)?,
            heartbeat_action_query: properties
                .get("heartbeat.action.query")
                .filter(|query| !query.trim().is_empty())
                .cloned(),
            heartbeat_topics_prefix: properties
                .get("topic.heartbeat.prefix")
                .or_else(|| properties.get("heartbeat.topics.prefix"))
                .cloned()
                .unwrap_or_else(default_heartbeat_topics_prefix),
            heartbeat_topic_name: properties.get("topic.heartbeat.name").cloned(),
            signal_data_collection,
            signal_enabled_channels,
            signal_file: properties
                .get("signal.file")
                .cloned()
                .unwrap_or_else(default_signal_file),
            signal_poll_interval: duration_ms(
                properties,
                "signal.poll.interval.ms",
                default_signal_poll_interval(),
            )?,
            incremental_snapshot_chunk_size: usize_value(
                properties,
                "incremental.snapshot.chunk.size",
                default_incremental_snapshot_chunk_size(),
            )?,
            incremental_snapshot_watermarking_strategy: properties
                .get("incremental.snapshot.watermarking.strategy")
                .cloned()
                .unwrap_or_else(default_incremental_snapshot_watermarking_strategy),
            signal_kafka_topic: properties.get("signal.kafka.topic").cloned(),
            signal_kafka_bootstrap_servers: csv_property(
                properties.get("signal.kafka.bootstrap.servers"),
            ),
            signal_kafka_group_id: properties
                .get("signal.kafka.groupId")
                .cloned()
                .unwrap_or_else(default_signal_kafka_group_id),
            signal_kafka_poll_timeout: duration_ms(
                properties,
                "signal.kafka.poll.timeout.ms",
                default_signal_kafka_poll_timeout(),
            )?,
            signal_kafka_consumer_properties: properties
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix("signal.consumer.")
                        .map(|key| (key.to_string(), value.clone()))
                })
                .collect(),
        }),
        SnapshotConfig {
            mode: snapshot_mode(
                properties
                    .get("snapshot.mode")
                    .map_or("initial", String::as_str),
            )?,
            fetch_size: usize_value(
                properties,
                "snapshot.fetch.size",
                default_snapshot_fetch_size(),
            )?,
            include_collections: csv_property(properties.get("snapshot.include.collection.list")),
        },
        warnings,
    )
}

fn assemble_config(
    properties: &BTreeMap<String, String>,
    source: SourceConfig,
    snapshot: SnapshotConfig,
    mut warnings: Vec<String>,
) -> Result<Config> {
    let topic_prefix = required(properties, "topic.prefix")?.to_string();
    let sink = sink_config(properties, &topic_prefix)?;
    warnings.extend(unsupported_warnings(properties));
    let mysql_source = matches!(&source, SourceConfig::Mysql(_));
    let errors_max_retries = if properties.contains_key("errors.max.retries") {
        i32_value(
            properties,
            "errors.max.retries",
            default_errors_max_retries(),
        )?
    } else if mysql_source && properties.contains_key("rustium.source.reconnect.max.attempts") {
        i32_value(
            properties,
            "rustium.source.reconnect.max.attempts",
            default_errors_max_retries(),
        )?
    } else {
        default_errors_max_retries()
    };
    let errors_retry_delay_initial = if properties.contains_key("errors.retry.delay.initial.ms") {
        duration_ms(
            properties,
            "errors.retry.delay.initial.ms",
            default_errors_retry_delay_initial(),
        )?
    } else if mysql_source && properties.contains_key("connect.keep.alive.interval.ms") {
        duration_ms(
            properties,
            "connect.keep.alive.interval.ms",
            default_errors_retry_delay_initial(),
        )?
    } else {
        default_errors_retry_delay_initial()
    };
    let errors_retry_delay_max = if properties.contains_key("errors.retry.delay.max.ms") {
        duration_ms(
            properties,
            "errors.retry.delay.max.ms",
            default_errors_retry_delay_max(),
        )?
    } else if mysql_source && properties.contains_key("connect.keep.alive.interval.ms") {
        errors_retry_delay_initial
    } else {
        default_errors_retry_delay_max()
    };

    let config = Config {
        api_version: API_VERSION.into(),
        kind: "Connector".into(),
        metadata: Metadata {
            name: required(properties, "name")?.to_string(),
            labels: BTreeMap::new(),
        },
        source,
        snapshot,
        format: format_config(properties)?,
        sink,
        state: StateConfig {
            kind: StateType::Sqlite,
            path: properties
                .get("rustium.state.path")
                .or_else(|| properties.get("offset.storage.file.filename"))
                .cloned()
                .unwrap_or_else(default_state_path),
        },
        runtime: RuntimeSettings {
            channel_capacity: usize_value(
                properties,
                "max.queue.size",
                default_channel_capacity(),
            )?,
            max_batch_size: usize_value(properties, "max.batch.size", default_batch_size())?,
            flush_interval: duration_ms(properties, "poll.interval.ms", default_flush_interval())?,
            shutdown_timeout: default_shutdown_timeout(),
            errors_max_retries,
            errors_retry_delay_initial,
            errors_retry_delay_max,
        },
        server: ServerConfig {
            bind: properties
                .get("rustium.server.bind")
                .cloned()
                .unwrap_or_else(default_bind),
            enable_mutations: properties
                .get("rustium.server.enable.mutations")
                .is_some_and(|value| value == "true"),
        },
        observability: ObservabilityConfig {
            log_format: properties
                .get("rustium.log.format")
                .map_or(LogFormat::Json, |value| {
                    if value == "pretty" {
                        LogFormat::Pretty
                    } else {
                        LogFormat::Json
                    }
                }),
            log_level: properties
                .get("rustium.log.level")
                .cloned()
                .unwrap_or_else(default_log_level),
            metrics: properties
                .get("rustium.metrics.enabled")
                .is_none_or(|value| value == "true"),
        },
        compatibility_warnings: warnings,
    };
    config.validate()?;
    Ok(config)
}

fn format_config(properties: &BTreeMap<String, String>) -> Result<FormatConfig> {
    const JSON_CONVERTER: &str = "org.apache.kafka.connect.json.JsonConverter";
    const JSON_SCHEMA_CONVERTER: &str = "io.confluent.connect.json.JsonSchemaConverter";
    const AVRO_CONVERTER: &str = "io.confluent.connect.avro.AvroConverter";
    const PROTOBUF_CONVERTER: &str = "io.confluent.connect.protobuf.ProtobufConverter";

    let key_converter = properties
        .get("key.converter")
        .map_or(JSON_CONVERTER, String::as_str);
    let value_converter = properties
        .get("value.converter")
        .map_or(JSON_CONVERTER, String::as_str);
    let unavailable_value = properties
        .get("unavailable.value.placeholder")
        .cloned()
        .unwrap_or_else(default_unavailable_value);
    let tombstones_on_delete = bool_value(properties, "tombstones.on.delete", true)?;

    if key_converter == JSON_CONVERTER && value_converter == JSON_CONVERTER {
        return Ok(FormatConfig {
            kind: FormatType::DebeziumJson,
            unavailable_value,
            tombstones_on_delete,
            schema_registry: None,
        });
    }
    let kind = if key_converter == JSON_SCHEMA_CONVERTER && value_converter == JSON_SCHEMA_CONVERTER
    {
        FormatType::DebeziumJsonSchema
    } else if key_converter == AVRO_CONVERTER && value_converter == AVRO_CONVERTER {
        FormatType::DebeziumAvro
    } else if key_converter == PROTOBUF_CONVERTER && value_converter == PROTOBUF_CONVERTER {
        FormatType::DebeziumProtobuf
    } else {
        return Err(Error::Configuration(format!(
            "Rustium requires matching key.converter and value.converter values; supported pairs are {JSON_CONVERTER:?}, {JSON_SCHEMA_CONVERTER:?}, {AVRO_CONVERTER:?}, and {PROTOBUF_CONVERTER:?}, got {key_converter:?} and {value_converter:?}"
        )));
    };

    if kind == FormatType::DebeziumAvro {
        for property in ["schema.name.adjustment.mode", "field.name.adjustment.mode"] {
            if let Some(mode) = properties.get(property)
                && mode != "avro"
            {
                return Err(Error::Configuration(format!(
                    "{property}={mode:?} is not implemented for Avro; Rustium uses deterministic avro adjustment"
                )));
            }
        }
    }
    if kind == FormatType::DebeziumProtobuf {
        for prefix in ["key.converter", "value.converter"] {
            if let Some(value) = properties.get(&format!("{prefix}.scrub.invalid.names"))
                && value != "true"
            {
                return Err(Error::Configuration(format!(
                    "{prefix}.scrub.invalid.names={value:?} is not implemented; Rustium always performs deterministic Protobuf name adjustment"
                )));
            }
            for option in [
                "optional.for.nullables",
                "wrapper.for.nullables",
                "generate.struct.for.nulls",
                "flatten.unions",
            ] {
                if bool_value(properties, &format!("{prefix}.{option}"), false)? {
                    return Err(Error::Configuration(format!(
                        "{prefix}.{option}=true is not implemented by Rustium's typed Protobuf contract"
                    )));
                }
            }
        }
    }

    let key_urls = required(properties, "key.converter.schema.registry.url")?;
    let value_urls = required(properties, "value.converter.schema.registry.url")?;
    if split_csv(key_urls) != split_csv(value_urls) {
        return Err(Error::Configuration(
            "Rustium currently requires key and value converters to use the same schema.registry.url list"
                .into(),
        ));
    }
    for prefix in ["key.converter", "value.converter"] {
        if !bool_value(properties, &format!("{prefix}.auto.register.schemas"), true)? {
            return Err(Error::Configuration(format!(
                "{prefix}.auto.register.schemas=false is not implemented"
            )));
        }
        if bool_value(properties, &format!("{prefix}.use.latest.version"), false)? {
            return Err(Error::Configuration(format!(
                "{prefix}.use.latest.version=true is not implemented"
            )));
        }
        if let Some(strategy) = properties.get(&format!("{prefix}.subject.name.strategy"))
            && !strategy.ends_with("TopicNameStrategy")
        {
            return Err(Error::Configuration(format!(
                "{prefix}.subject.name.strategy={strategy:?} is not implemented; Rustium currently uses TopicNameStrategy"
            )));
        }
        if let Some(source) = properties.get(&format!("{prefix}.basic.auth.credentials.source"))
            && source != "USER_INFO"
        {
            return Err(Error::Configuration(format!(
                "{prefix}.basic.auth.credentials.source={source:?} is not implemented; use USER_INFO"
            )));
        }
    }

    let key_auth = properties
        .get("key.converter.basic.auth.user.info")
        .map(String::as_str);
    let value_auth = properties
        .get("value.converter.basic.auth.user.info")
        .map(String::as_str);
    if key_auth != value_auth {
        return Err(Error::Configuration(
            "Rustium currently requires matching key and value schema registry basic.auth.user.info values"
                .into(),
        ));
    }
    let (username, password) = match key_auth {
        Some(value) => {
            let (username, password) = value.split_once(':').ok_or_else(|| {
                Error::Configuration(
                    "schema registry basic.auth.user.info must use username:password".into(),
                )
            })?;
            if username.is_empty() {
                return Err(Error::Configuration(
                    "schema registry basic.auth.user.info username must not be empty".into(),
                ));
            }
            (Some(username.to_string()), Some(password.to_string()))
        }
        None => (None, None),
    };

    Ok(FormatConfig {
        kind,
        unavailable_value,
        tombstones_on_delete,
        schema_registry: Some(SchemaRegistryConfig {
            urls: split_csv(key_urls),
            username,
            password,
            request_timeout: duration_ms(
                properties,
                "rustium.schema.registry.request.timeout.ms",
                default_schema_registry_timeout(),
            )?,
            cache_capacity: usize_value(
                properties,
                "rustium.schema.registry.cache.capacity",
                default_schema_registry_cache_capacity(),
            )?,
        }),
    })
}

fn sink_config(properties: &BTreeMap<String, String>, topic_prefix: &str) -> Result<SinkConfig> {
    match properties
        .get("rustium.sink.type")
        .map_or("stdout", String::as_str)
    {
        "stdout" => Ok(SinkConfig::Stdout {
            topic_prefix: topic_prefix.into(),
        }),
        "kafka" => {
            let bootstrap_servers = properties
                .get("rustium.kafka.bootstrap.servers")
                .or_else(|| properties.get("bootstrap.servers"))
                .ok_or_else(|| {
                    Error::Configuration(
                        "rustium.sink.type=kafka requires rustium.kafka.bootstrap.servers or bootstrap.servers"
                            .into(),
                    )
                })?;
            Ok(SinkConfig::Kafka {
                bootstrap_servers: split_csv(bootstrap_servers),
                topic_prefix: topic_prefix.into(),
                acks: properties
                    .get("rustium.kafka.acks")
                    .cloned()
                    .unwrap_or_else(default_kafka_acks),
                compression: properties
                    .get("rustium.kafka.compression.type")
                    .cloned()
                    .unwrap_or_else(default_kafka_compression),
                delivery_timeout: duration_ms(
                    properties,
                    "rustium.kafka.delivery.timeout.ms",
                    default_delivery_timeout(),
                )?,
                properties: properties
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix("rustium.kafka.property.")
                            .map(|key| (key.to_string(), value.clone()))
                    })
                    .collect(),
            })
        }
        other => Err(Error::Configuration(format!(
            "unsupported rustium.sink.type {other:?}"
        ))),
    }
}

fn parse_properties(input: &str) -> Result<BTreeMap<String, String>> {
    let mut logical_lines = Vec::new();
    let mut current = String::new();
    for raw_line in input.lines() {
        let trimmed = raw_line.trim_end();
        let slash_count = trimmed
            .chars()
            .rev()
            .take_while(|character| *character == '\\')
            .count();
        if slash_count % 2 == 1 {
            current.push_str(trimmed.strip_suffix('\\').unwrap_or(trimmed));
        } else {
            current.push_str(trimmed);
            logical_lines.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        logical_lines.push(current);
    }

    let mut properties = BTreeMap::new();
    for line in logical_lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let separator = line
            .char_indices()
            .find(|(_, character)| matches!(character, '=' | ':') || character.is_whitespace())
            .map(|(index, _)| index)
            .ok_or_else(|| Error::Configuration(format!("invalid property line {line:?}")))?;
        let key = unescape(&line[..separator])?;
        let value = line[separator..].trim_start_matches(|character: char| {
            matches!(character, '=' | ':') || character.is_whitespace()
        });
        properties.insert(key, unescape(value)?);
    }
    Ok(properties)
}

fn unescape(value: &str) -> Result<String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        match chars.next() {
            Some('n') => output.push('\n'),
            Some('r') => output.push('\r'),
            Some('t') => output.push('\t'),
            Some('f') => output.push('\u{000c}'),
            Some('u') => {
                let code = chars.by_ref().take(4).collect::<String>();
                let number = u32::from_str_radix(&code, 16).map_err(|error| {
                    Error::Configuration(format!("invalid Unicode property escape: {error}"))
                })?;
                output.push(char::from_u32(number).ok_or_else(|| {
                    Error::Configuration("invalid Unicode property code point".into())
                })?);
            }
            Some(other) => output.push(other),
            None => output.push('\\'),
        }
    }
    Ok(output)
}

fn required<'a>(properties: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str> {
    properties
        .get(name)
        .filter(|value| !value.trim().is_empty())
        .map(String::as_str)
        .ok_or_else(|| Error::Configuration(format!("missing required property {name}")))
}

fn usize_value(properties: &BTreeMap<String, String>, name: &str, default: usize) -> Result<usize> {
    properties.get(name).map_or(Ok(default), |value| {
        value.parse().map_err(|error| {
            Error::Configuration(format!("property {name} must be an integer: {error}"))
        })
    })
}

fn u16_value(properties: &BTreeMap<String, String>, name: &str, default: u16) -> Result<u16> {
    properties.get(name).map_or(Ok(default), |value| {
        value.parse().map_err(|error| {
            Error::Configuration(format!("property {name} must be a port number: {error}"))
        })
    })
}

fn u32_value(properties: &BTreeMap<String, String>, name: &str, default: u32) -> Result<u32> {
    properties.get(name).map_or(Ok(default), |value| {
        value.parse().map_err(|error| {
            Error::Configuration(format!("property {name} must be an integer: {error}"))
        })
    })
}

fn i32_value(properties: &BTreeMap<String, String>, name: &str, default: i32) -> Result<i32> {
    properties.get(name).map_or(Ok(default), |value| {
        value
            .parse::<i32>()
            .map_err(|error| Error::Configuration(format!("invalid {name}: {error}")))
    })
}

fn bool_value(properties: &BTreeMap<String, String>, name: &str, default: bool) -> Result<bool> {
    properties.get(name).map_or(Ok(default), |value| {
        value.parse().map_err(|error| {
            Error::Configuration(format!("property {name} must be true or false: {error}"))
        })
    })
}

fn duration_ms(
    properties: &BTreeMap<String, String>,
    name: &str,
    default: Duration,
) -> Result<Duration> {
    properties.get(name).map_or(Ok(default), |value| {
        value
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|error| {
                Error::Configuration(format!("property {name} must be milliseconds: {error}"))
            })
    })
}

fn snapshot_mode(mode: &str) -> Result<SnapshotMode> {
    match mode {
        "initial" => Ok(SnapshotMode::Initial),
        "when_needed" => Ok(SnapshotMode::WhenNeeded),
        "never" | "no_data" => Ok(SnapshotMode::Never),
        other => Err(Error::Configuration(format!(
            "snapshot.mode={other:?} is recognized but not supported by the current connector"
        ))),
    }
}

fn publication_autocreate_mode(mode: &str) -> Result<PublicationAutoCreateMode> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "disabled" => Ok(PublicationAutoCreateMode::Disabled),
        "all_tables" => Ok(PublicationAutoCreateMode::AllTables),
        "filtered" => Ok(PublicationAutoCreateMode::Filtered),
        "no_tables" => Ok(PublicationAutoCreateMode::NoTables),
        _ => Err(Error::Configuration(format!(
            "publication.autocreate.mode={mode:?} must be disabled, all_tables, filtered, or no_tables"
        ))),
    }
}

fn postgres_interval_handling_mode(mode: &str) -> Result<String> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "numeric" => Ok("numeric".into()),
        "string" => Ok("string".into()),
        _ => Err(Error::Configuration(format!(
            "interval.handling.mode={mode:?} must be numeric or string"
        ))),
    }
}

fn postgres_replica_identity_rules(
    value: Option<&String>,
) -> Result<Vec<PostgresReplicaIdentityRule>> {
    value.map_or_else(
        || Ok(Vec::new()),
        |value| {
            if value.trim().is_empty() {
                return Err(Error::Configuration(
                    "replica.identity.autoset.values must not be empty when configured".into(),
                ));
            }
            value
                .split(',')
                .map(|entry| {
                    let entry = entry.trim();
                    let (table, identity) = entry.split_once(':').ok_or_else(|| {
                        Error::Configuration(format!(
                            "replica.identity.autoset.values entry {entry:?} must use <table-regex>:<identity>"
                        ))
                    })?;
                    if table.trim().is_empty()
                        || table.chars().any(char::is_whitespace)
                        || table.contains(':')
                        || Regex::new(table).is_err()
                    {
                        return Err(Error::Configuration(format!(
                            "replica.identity.autoset.values table selector {table:?} is invalid"
                        )));
                    }
                    let mut identity_parts = identity.split_whitespace();
                    let mode = identity_parts.next().unwrap_or_default();
                    let (identity, index) = match mode.to_ascii_uppercase().as_str() {
                        "DEFAULT" if identity_parts.next().is_none() => {
                            (PostgresReplicaIdentity::Default, None)
                        }
                        "FULL" if identity_parts.next().is_none() => {
                            (PostgresReplicaIdentity::Full, None)
                        }
                        "NOTHING" if identity_parts.next().is_none() => {
                            (PostgresReplicaIdentity::Nothing, None)
                        }
                        "INDEX" => {
                            let index = identity_parts.next().filter(|index| !index.is_empty());
                            if index.is_none() || identity_parts.next().is_some() {
                                return Err(Error::Configuration(format!(
                                    "replica.identity.autoset.values entry {entry:?} requires INDEX <index-name>"
                                )));
                            }
                            (PostgresReplicaIdentity::Index, index.map(str::to_string))
                        }
                        _ => {
                            return Err(Error::Configuration(format!(
                                "replica.identity.autoset.values entry {entry:?} must use DEFAULT, FULL, NOTHING, or INDEX <index-name>"
                            )));
                        }
                    };
                    Ok(PostgresReplicaIdentityRule {
                        table: table.to_string(),
                        identity,
                        index,
                    })
                })
                .collect()
        },
    )
}

fn csv_property(value: Option<&String>) -> Vec<String> {
    value.map_or_else(Vec::new, |value| split_csv(value))
}

fn split_csv(value: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for character in value.chars() {
        if escaped {
            current.push('\\');
            current.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == ',' {
            if !current.trim().is_empty() {
                values.push(current.trim().to_string());
            }
            current.clear();
        } else {
            current.push(character);
        }
    }
    if escaped {
        current.push('\\');
    }
    if !current.trim().is_empty() {
        values.push(current.trim().to_string());
    }
    values
}

fn unsupported_warnings(properties: &BTreeMap<String, String>) -> Vec<String> {
    const SUPPORTED: &[&str] = &[
        "name",
        "connector.class",
        "tasks.max",
        "database.hostname",
        "database.port",
        "database.user",
        "database.password",
        "database.dbname",
        "database.sslmode",
        "database.server.id",
        "database.server.id.offset",
        "database.ssl.mode",
        "database.connectionTimeZone",
        "database.ssl.ca",
        "database.ssl.cert",
        "database.ssl.key",
        "database.ssl.keystore",
        "database.ssl.keystore.password",
        "database.ssl.truststore",
        "database.ssl.truststore.password",
        "database.include.list",
        "database.exclude.list",
        "gtid.source.includes",
        "gtid.source.excludes",
        "gtid.source.filter.dml.events",
        "connect.timeout.ms",
        "connect.keep.alive",
        "connect.keep.alive.interval.ms",
        "database.names",
        "database.encrypt",
        "database.trustServerCertificate",
        "database.connection.timeout.ms",
        "snapshot.isolation.mode",
        "data.query.mode",
        "streaming.fetch.size",
        "topic.prefix",
        "plugin.name",
        "slot.name",
        "slot.failover",
        "publication.name",
        "publication.autocreate.mode",
        "replica.identity.autoset.values",
        "publish.via.partition.root",
        "snapshot.mode",
        "snapshot.fetch.size",
        "snapshot.include.collection.list",
        "table.include.list",
        "table.exclude.list",
        "schema.include.list",
        "schema.exclude.list",
        "max.queue.size",
        "max.batch.size",
        "poll.interval.ms",
        "connection.validation.timeout.ms",
        "unavailable.value.placeholder",
        "tombstones.on.delete",
        "key.converter",
        "value.converter",
        "key.converter.schemas.enable",
        "value.converter.schemas.enable",
        "key.converter.schema.registry.url",
        "value.converter.schema.registry.url",
        "key.converter.auto.register.schemas",
        "value.converter.auto.register.schemas",
        "key.converter.use.latest.version",
        "value.converter.use.latest.version",
        "key.converter.subject.name.strategy",
        "value.converter.subject.name.strategy",
        "key.converter.basic.auth.credentials.source",
        "value.converter.basic.auth.credentials.source",
        "key.converter.basic.auth.user.info",
        "value.converter.basic.auth.user.info",
        "schema.name.adjustment.mode",
        "field.name.adjustment.mode",
        "key.converter.scrub.invalid.names",
        "value.converter.scrub.invalid.names",
        "key.converter.optional.for.nullables",
        "value.converter.optional.for.nullables",
        "key.converter.wrapper.for.nullables",
        "value.converter.wrapper.for.nullables",
        "key.converter.generate.struct.for.nulls",
        "value.converter.generate.struct.for.nulls",
        "key.converter.flatten.unions",
        "value.converter.flatten.unions",
        "heartbeat.interval.ms",
        "heartbeat.action.query",
        "heartbeat.topics.prefix",
        "topic.heartbeat.prefix",
        "topic.heartbeat.name",
        "signal.data.collection",
        "signal.enabled.channels",
        "signal.file",
        "signal.poll.interval.ms",
        "signal.kafka.topic",
        "signal.kafka.groupId",
        "signal.kafka.bootstrap.servers",
        "signal.kafka.poll.timeout.ms",
        "incremental.snapshot.chunk.size",
        "incremental.snapshot.allow.schema.changes",
        "incremental.snapshot.watermarking.strategy",
        "read.only",
        "hstore.handling.mode",
        "interval.handling.mode",
        "offset.storage.file.filename",
        "bootstrap.servers",
        "rustium.sink.type",
        "rustium.source.reconnect.max.attempts",
        "schema.history.internal.skip.unparseable.ddl",
        "rustium.kafka.bootstrap.servers",
        "rustium.kafka.acks",
        "rustium.kafka.compression.type",
        "rustium.kafka.delivery.timeout.ms",
        "rustium.schema.registry.request.timeout.ms",
        "rustium.schema.registry.cache.capacity",
        "rustium.state.path",
        "rustium.server.bind",
        "rustium.server.enable.mutations",
        "rustium.log.format",
        "rustium.log.level",
        "rustium.metrics.enabled",
        "errors.max.retries",
        "errors.retry.delay.initial.ms",
        "errors.retry.delay.max.ms",
    ];
    properties
        .keys()
        .filter(|key| {
            !SUPPORTED.contains(&key.as_str())
                && !key.starts_with("rustium.kafka.property.")
                && !key.starts_with("signal.consumer.")
        })
        .map(|key| format!("Debezium property {key} is not implemented and was ignored"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_snapshot_modes_with_different_semantics() {
        assert_eq!(snapshot_mode("initial").unwrap(), SnapshotMode::Initial);
        assert_eq!(
            snapshot_mode("when_needed").unwrap(),
            SnapshotMode::WhenNeeded
        );
        assert_eq!(snapshot_mode("never").unwrap(), SnapshotMode::Never);
        assert_eq!(snapshot_mode("no_data").unwrap(), SnapshotMode::Never);
        for mode in [
            "always",
            "initial_only",
            "schema_only",
            "recovery",
            "custom",
        ] {
            let error = snapshot_mode(mode).unwrap_err();
            assert!(error.to_string().contains("not supported"), "mode={mode}");
        }
    }

    #[test]
    fn parses_postgres_properties_and_regex_filters() {
        let config = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.port=5432
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
plugin.name=pgoutput
slot.name=orders_slot
slot.failover=true
publication.name=orders_pub
publication.autocreate.mode=filtered
replica.identity.autoset.values=public\\.orders:FULL,public\\.customers:INDEX customers_replica_key
publish.via.partition.root=true
table.include.list=public\\.(orders|customers)
snapshot.mode=initial
snapshot.include.collection.list=public\\.orders
tombstones.on.delete=false
heartbeat.interval.ms=2500
heartbeat.action.query=INSERT INTO public.rustium_heartbeat (id) VALUES (1)
topic.heartbeat.prefix=__pg-heartbeat
topic.heartbeat.name=shared-pg-heartbeat
signal.data.collection=public.rustium_signal
signal.enabled.channels=source,file,in-process,kafka
signal.file=/run/rustium/orders-signals.jsonl
signal.poll.interval.ms=250
signal.kafka.topic=orders-signals
signal.kafka.bootstrap.servers=kafka-1:9092,kafka-2:9092
signal.kafka.groupId=orders-signal-group
signal.kafka.poll.timeout.ms=75
signal.consumer.security.protocol=SASL_SSL
signal.consumer.enable.auto.commit=false
incremental.snapshot.chunk.size=128
incremental.snapshot.allow.schema.changes=false
incremental.snapshot.watermarking.strategy=insert_insert
read.only=true
hstore.handling.mode=map
interval.handling.mode=string
max.queue.size=4096
max.batch.size=1000
"#,
        )
        .unwrap();
        assert_eq!(config.metadata.name, "orders-cdc");
        assert_eq!(config.runtime.channel_capacity, 4096);
        let source = config.source.as_postgresql().unwrap();
        assert!(source.tables.includes("public", "orders"));
        assert!(!source.tables.includes("public", "products"));
        assert_eq!(source.heartbeat_interval, Duration::from_millis(2500));
        assert_eq!(
            source.heartbeat_action_query.as_deref(),
            Some("INSERT INTO public.rustium_heartbeat (id) VALUES (1)")
        );
        assert_eq!(source.heartbeat_topics_prefix, "__pg-heartbeat");
        assert_eq!(
            source.heartbeat_topic_name.as_deref(),
            Some("shared-pg-heartbeat")
        );
        assert_eq!(
            source.signal_data_collection.as_deref(),
            Some("public.rustium_signal")
        );
        assert_eq!(
            source.signal_enabled_channels,
            ["source", "file", "in-process", "kafka"]
        );
        assert_eq!(source.signal_file, "/run/rustium/orders-signals.jsonl");
        assert_eq!(source.signal_poll_interval, Duration::from_millis(250));
        assert_eq!(source.signal_kafka_topic.as_deref(), Some("orders-signals"));
        assert_eq!(
            source.signal_kafka_bootstrap_servers,
            ["kafka-1:9092", "kafka-2:9092"]
        );
        assert_eq!(source.signal_kafka_group_id, "orders-signal-group");
        assert_eq!(source.signal_kafka_poll_timeout, Duration::from_millis(75));
        assert_eq!(
            source
                .signal_kafka_consumer_properties
                .get("security.protocol")
                .map(String::as_str),
            Some("SASL_SSL")
        );
        assert_eq!(source.incremental_snapshot_chunk_size, 128);
        assert_eq!(
            source.incremental_snapshot_watermarking_strategy,
            "insert_insert"
        );
        assert!(source.read_only);
        assert_eq!(source.hstore_handling_mode, "map");
        assert_eq!(source.interval_handling_mode, "string");
        assert_eq!(
            source.publication_autocreate_mode,
            PublicationAutoCreateMode::Filtered
        );
        assert_eq!(source.replica_identity_autoset_values.len(), 2);
        assert!(source.publish_via_partition_root);
        assert!(source.slot_failover);
        assert_eq!(
            source.replica_identity_autoset_values[0].table,
            r"public\.orders"
        );
        assert_eq!(
            source.replica_identity_autoset_values[0].identity,
            PostgresReplicaIdentity::Full
        );
        assert_eq!(
            source.replica_identity_autoset_values[1].index.as_deref(),
            Some("customers_replica_key")
        );
        assert_eq!(config.snapshot.include_collections, [r"public\.orders"]);
        assert!(!config.format.tombstones_on_delete);
        assert!(
            config
                .compatibility_warnings
                .iter()
                .all(|warning| !warning.contains("tombstone"))
        );
    }

    #[test]
    fn rejects_invalid_postgres_incremental_snapshot_options() {
        let missing_kafka_bootstrap = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
signal.data.collection=public.rustium_signal
signal.enabled.channels=kafka
"#,
        )
        .unwrap_err();
        assert!(missing_kafka_bootstrap.to_string().contains("bootstrap"));

        let schema_changes = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
incremental.snapshot.allow.schema.changes=true
"#,
        )
        .unwrap_err();
        assert!(schema_changes.to_string().contains("allow.schema.changes"));
    }

    #[test]
    fn validates_postgres_publication_autocreate_modes() {
        for (value, expected) in [
            ("DISABLED", PublicationAutoCreateMode::Disabled),
            ("All_Tables", PublicationAutoCreateMode::AllTables),
            ("FILTERED", PublicationAutoCreateMode::Filtered),
            ("No_Tables", PublicationAutoCreateMode::NoTables),
        ] {
            assert_eq!(publication_autocreate_mode(value).unwrap(), expected);
        }
        assert!(publication_autocreate_mode("invalid").is_err());

        let config = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
"#,
        )
        .unwrap();
        assert_eq!(
            config
                .source
                .as_postgresql()
                .unwrap()
                .publication_autocreate_mode,
            PublicationAutoCreateMode::AllTables
        );
    }

    #[test]
    fn validates_postgres_interval_handling_modes() {
        assert_eq!(
            postgres_interval_handling_mode("numeric").unwrap(),
            "numeric"
        );
        assert_eq!(postgres_interval_handling_mode("STRING").unwrap(), "string");
        assert!(postgres_interval_handling_mode("postgres").is_err());
    }

    #[test]
    fn validates_postgres_replica_identity_autoset_values() {
        let rules = postgres_replica_identity_rules(Some(&
            "public\\.orders:default,public\\.customers:Full,public\\.audit:NOTHING,public\\.accounts:INDEX accounts_key".into(),
        ))
        .unwrap();
        assert_eq!(rules.len(), 4);
        assert_eq!(rules[0].identity, PostgresReplicaIdentity::Default);
        assert_eq!(rules[1].identity, PostgresReplicaIdentity::Full);
        assert_eq!(rules[2].identity, PostgresReplicaIdentity::Nothing);
        assert_eq!(rules[3].identity, PostgresReplicaIdentity::Index);
        assert_eq!(rules[3].index.as_deref(), Some("accounts_key"));

        for invalid in [
            "",
            "public\\.orders",
            "public\\.orders:unknown",
            "public\\.orders:INDEX",
            "public orders:FULL",
            "[invalid:FULL",
        ] {
            assert!(postgres_replica_identity_rules(Some(&invalid.into())).is_err());
        }
    }

    #[test]
    fn accepts_read_only_postgres_file_signaling_without_source_table() {
        let config = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
signal.enabled.channels=file
signal.file=/run/rustium/signals.jsonl
signal.poll.interval.ms=100
read.only=true
"#,
        )
        .unwrap();
        let source = config.source.as_postgresql().unwrap();
        assert_eq!(source.signal_enabled_channels, ["file"]);
        assert!(source.signal_data_collection.is_none());
        assert!(source.read_only);
    }

    #[test]
    fn maps_debezium_engine_retry_settings() {
        let config = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
errors.max.retries=4
errors.retry.delay.initial.ms=125
errors.retry.delay.max.ms=2000
"#,
        )
        .unwrap();
        assert_eq!(config.runtime.errors_max_retries, 4);
        assert_eq!(
            config.runtime.errors_retry_delay_initial,
            Duration::from_millis(125)
        );
        assert_eq!(
            config.runtime.errors_retry_delay_max,
            Duration::from_secs(2)
        );
        assert!(
            config
                .compatibility_warnings
                .iter()
                .all(|warning| !warning.contains("errors."))
        );
    }

    #[test]
    fn maps_postgres_jmx_signaling_to_the_management_channel() {
        let config = parse(
            r#"
name=orders-cdc
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=postgres
database.user=rustium
database.password=secret
database.dbname=app
topic.prefix=app
signal.enabled.channels=jmx,in-process
read.only=true
"#,
        )
        .unwrap();
        let source = config.source.as_postgresql().unwrap();
        assert_eq!(source.signal_enabled_channels, ["in-process"]);
        assert!(config.compatibility_warnings.iter().any(|warning| {
            warning.contains("signal.enabled.channels=jmx")
                && warning.contains("SignalSender/HTTP management API")
        }));
    }

    #[test]
    fn recognizes_prioritized_connectors() {
        let config = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
database.server.id=7001
database.connectionTimeZone=Etc/UTC
database.ssl.ca=/etc/mysql/ca.pem
database.ssl.cert=/etc/mysql/client.pem
database.ssl.key=/etc/mysql/client-key.pem
database.include.list=inventory
table.include.list=inventory\.(orders|customers)
snapshot.include.collection.list=inventory\\.orders
topic.prefix=inventory
connect.keep.alive=false
connect.keep.alive.interval.ms=250
rustium.source.reconnect.max.attempts=3
schema.history.internal.skip.unparseable.ddl=true
gtid.source.includes=8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850,2f6f.*
gtid.source.filter.dml.events=false
heartbeat.interval.ms=1000
heartbeat.action.query=UPDATE inventory.heartbeat SET touched_at = CURRENT_TIMESTAMP
heartbeat.topics.prefix=__legacy-heartbeat
topic.heartbeat.prefix=__custom-heartbeat
topic.heartbeat.name=shared-heartbeat
"#,
        )
        .unwrap();
        let source = config.source.as_mysql().unwrap();
        assert_eq!(source.server_id, 7001);
        assert_eq!(source.connection_time_zone, "Etc/UTC");
        assert_eq!(source.session_time_zone().unwrap(), "+00:00");
        assert_eq!(source.ssl_ca.as_deref(), Some("/etc/mysql/ca.pem"));
        assert_eq!(source.ssl_cert.as_deref(), Some("/etc/mysql/client.pem"));
        assert_eq!(source.ssl_key.as_deref(), Some("/etc/mysql/client-key.pem"));
        assert_eq!(source.databases, ["inventory"]);
        assert!(!source.connect_keep_alive);
        assert_eq!(
            source.connect_keep_alive_interval,
            Duration::from_millis(250)
        );
        assert_eq!(source.reconnect_max_attempts, 3);
        assert_eq!(config.runtime.errors_max_retries, 3);
        assert_eq!(
            config.runtime.errors_retry_delay_initial,
            Duration::from_millis(250)
        );
        assert_eq!(
            config.runtime.errors_retry_delay_max,
            Duration::from_millis(250)
        );
        assert!(source.schema_history_skip_unparseable_ddl);
        assert_eq!(
            source.gtid_source_includes,
            ["8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850", "2f6f.*"]
        );
        assert!(source.gtid_source_excludes.is_empty());
        assert!(!source.gtid_source_filter_dml_events);
        assert_eq!(source.heartbeat_interval, Duration::from_secs(1));
        assert_eq!(
            source.heartbeat_action_query.as_deref(),
            Some("UPDATE inventory.heartbeat SET touched_at = CURRENT_TIMESTAMP")
        );
        assert_eq!(source.heartbeat_topics_prefix, "__custom-heartbeat");
        assert_eq!(
            source.heartbeat_topic_name.as_deref(),
            Some("shared-heartbeat")
        );
        assert!(source.tables.includes("inventory", "orders"));
        assert!(!source.tables.includes("inventory", "products"));
        assert_eq!(config.snapshot.include_collections, [r"inventory\.orders"]);
    }

    #[test]
    fn rejects_invalid_mysql_gtid_source_filters() {
        let both = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
gtid.source.includes=8f5f.*
gtid.source.excludes=2f6f.*
"#,
        )
        .unwrap_err();
        assert!(both.to_string().contains("cannot both be configured"));

        let invalid_regex = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
gtid.source.includes=(unterminated
"#,
        )
        .unwrap_err();
        assert!(
            invalid_regex
                .to_string()
                .contains("valid regular expression")
        );
    }

    #[test]
    fn rejects_unsupported_mysql_connection_time_zone() {
        let error = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
database.connectionTimeZone=America/Los_Angeles
topic.prefix=inventory
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("connection_time_zone"));
    }

    #[test]
    fn parses_mysql_signal_and_incremental_snapshot_properties() {
        let config = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
signal.enabled.channels=file,kafka
signal.file=/run/rustium/signals.jsonl
signal.kafka.topic=orders-signals
signal.kafka.bootstrap.servers=kafka-1:9092,kafka-2:9092
signal.kafka.groupId=orders-signal-group
signal.kafka.poll.timeout.ms=75
incremental.snapshot.chunk.size=128
read.only=true
signal.consumer.security.protocol=SASL_SSL
"#,
        )
        .unwrap();
        let source = config.source.as_mysql().unwrap();
        assert_eq!(source.signal_enabled_channels, ["file", "kafka"]);
        assert_eq!(source.signal_file, "/run/rustium/signals.jsonl");
        assert_eq!(source.signal_kafka_topic.as_deref(), Some("orders-signals"));
        assert_eq!(
            source.signal_kafka_bootstrap_servers,
            ["kafka-1:9092", "kafka-2:9092"]
        );
        assert_eq!(source.signal_kafka_group_id, "orders-signal-group");
        assert_eq!(source.signal_kafka_poll_timeout, Duration::from_millis(75));
        assert_eq!(source.incremental_snapshot_chunk_size, 128);
    }

    #[test]
    fn rejects_incomplete_mysql_tls_material() {
        let config = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
database.ssl.mode=required
database.ssl.cert=/etc/mysql/client.pem
"#,
        )
        .unwrap_err();
        assert!(
            config
                .to_string()
                .contains("source.ssl_cert and source.ssl_key")
        );

        let config = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
database.ssl.mode=disabled
database.ssl.ca=/etc/mysql/ca.pem
"#,
        )
        .unwrap_err();
        assert!(
            config
                .to_string()
                .contains("require an enabled MySQL TLS mode")
        );
    }

    #[test]
    fn maps_mysql_java_tls_stores() {
        let config = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
database.ssl.mode=verify_identity
database.ssl.keystore=/run/secrets/client.p12
database.ssl.keystore.password=client-secret
database.ssl.truststore=/run/secrets/trust.jks
database.ssl.truststore.password=trust-secret
"#,
        )
        .unwrap();
        let source = config.source.as_mysql().unwrap();
        assert_eq!(
            source.ssl_keystore.as_deref(),
            Some("/run/secrets/client.p12")
        );
        assert_eq!(
            source.ssl_keystore_password.as_deref(),
            Some("client-secret")
        );
        assert_eq!(
            source.ssl_truststore.as_deref(),
            Some("/run/secrets/trust.jks")
        );
        assert_eq!(
            source.ssl_truststore_password.as_deref(),
            Some("trust-secret")
        );
        assert!(!config.compatibility_warnings.iter().any(|warning| {
            warning.contains("database.ssl.keystore") || warning.contains("database.ssl.truststore")
        }));
    }

    #[test]
    fn rejects_conflicting_mysql_tls_store_material() {
        let conflicting_identity = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
database.ssl.mode=required
database.ssl.cert=/etc/mysql/client.pem
database.ssl.key=/etc/mysql/client-key.pem
database.ssl.keystore=/etc/mysql/client.p12
"#,
        )
        .unwrap_err();
        assert!(
            conflicting_identity
                .to_string()
                .contains("ssl_keystore cannot be combined")
        );

        let orphan_password = parse(
            r#"
name=mysql
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
database.ssl.mode=required
database.ssl.truststore.password=trust-secret
"#,
        )
        .unwrap_err();
        assert!(
            orphan_password
                .to_string()
                .contains("ssl_truststore_password requires")
        );
    }

    #[test]
    fn parses_sqlserver_properties() {
        let config = parse(
            r#"
name=inventory-sqlserver
connector.class=io.debezium.connector.sqlserver.SqlServerConnector
database.hostname=sqlserver
database.user=rustium
database.password=secret
database.names=inventory
table.include.list=dbo\.orders
topic.prefix=inventory
snapshot.include.collection.list=inventory\\.dbo\\.orders
snapshot.isolation.mode=snapshot
streaming.fetch.size=2048
heartbeat.interval.ms=1000
heartbeat.action.query=UPDATE dbo.heartbeat SET touched_at = SYSUTCDATETIME()
topic.heartbeat.prefix=__sql-heartbeat
topic.heartbeat.name=shared-sql-heartbeat
signal.data.collection=inventory.dbo.rustium_signal
signal.enabled.channels=jmx,file,kafka
signal.file=sql-signals.jsonl
signal.poll.interval.ms=250
signal.kafka.bootstrap.servers=kafka:9092
signal.kafka.groupId=sql-signals
signal.consumer.enable.auto.commit=false
incremental.snapshot.chunk.size=64
incremental.snapshot.allow.schema.changes=false
incremental.snapshot.watermarking.strategy=insert_insert
"#,
        )
        .unwrap();
        let source = config.source.as_sqlserver().unwrap();
        assert_eq!(source.databases, ["inventory"]);
        assert_eq!(source.streaming_fetch_size, 2048);
        assert_eq!(source.snapshot_isolation_mode, "snapshot");
        assert_eq!(source.heartbeat_interval, Duration::from_secs(1));
        assert_eq!(
            source.heartbeat_action_query.as_deref(),
            Some("UPDATE dbo.heartbeat SET touched_at = SYSUTCDATETIME()")
        );
        assert_eq!(source.heartbeat_topics_prefix, "__sql-heartbeat");
        assert_eq!(
            source.heartbeat_topic_name.as_deref(),
            Some("shared-sql-heartbeat")
        );
        assert_eq!(
            source.signal_data_collection.as_deref(),
            Some("inventory.dbo.rustium_signal")
        );
        assert_eq!(
            source.signal_enabled_channels,
            ["in-process", "file", "kafka"]
        );
        assert_eq!(source.signal_file, "sql-signals.jsonl");
        assert_eq!(source.signal_poll_interval, Duration::from_millis(250));
        assert_eq!(source.incremental_snapshot_chunk_size, 64);
        assert_eq!(source.signal_kafka_bootstrap_servers, ["kafka:9092"]);
        assert_eq!(source.signal_kafka_group_id, "sql-signals");
        assert_eq!(
            config.snapshot.include_collections,
            [r"inventory\.dbo\.orders"]
        );
        assert_eq!(
            source
                .signal_kafka_consumer_properties
                .get("enable.auto.commit"),
            Some(&"false".into())
        );
        assert!(
            config
                .compatibility_warnings
                .iter()
                .any(|warning| warning.contains("signal.enabled.channels=jmx"))
        );
        assert!(config.format.tombstones_on_delete);
    }

    #[test]
    fn maps_confluent_json_schema_converters() {
        let config = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
rustium.sink.type=kafka
rustium.kafka.bootstrap.servers=kafka:9092
key.converter=io.confluent.connect.json.JsonSchemaConverter
key.converter.schema.registry.url=https://registry-1:8081,https://registry-2:8081
key.converter.basic.auth.credentials.source=USER_INFO
key.converter.basic.auth.user.info=registry-user:registry-secret
key.converter.auto.register.schemas=true
value.converter=io.confluent.connect.json.JsonSchemaConverter
value.converter.schema.registry.url=https://registry-1:8081,https://registry-2:8081
value.converter.basic.auth.credentials.source=USER_INFO
value.converter.basic.auth.user.info=registry-user:registry-secret
value.converter.auto.register.schemas=true
rustium.schema.registry.request.timeout.ms=2500
rustium.schema.registry.cache.capacity=64
"#,
        )
        .unwrap();

        assert_eq!(config.format.kind, FormatType::DebeziumJsonSchema);
        let registry = config.format.schema_registry.as_ref().unwrap();
        assert_eq!(
            registry.urls,
            ["https://registry-1:8081", "https://registry-2:8081"]
        );
        assert_eq!(registry.username.as_deref(), Some("registry-user"));
        assert_eq!(registry.password.as_deref(), Some("registry-secret"));
        assert_eq!(registry.request_timeout, Duration::from_millis(2500));
        assert_eq!(registry.cache_capacity, 64);
        let mut changed_secret = config.clone();
        changed_secret
            .format
            .schema_registry
            .as_mut()
            .unwrap()
            .password = Some("different-secret".into());
        assert_eq!(config.fingerprint(), changed_secret.fingerprint());
        assert!(!config.compatibility_warnings.iter().any(|warning| {
            warning.contains("converter") || warning.contains("schema.registry")
        }));
    }

    #[test]
    fn maps_confluent_avro_converters_and_rejects_unsafe_adjustment() {
        let config = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
rustium.sink.type=kafka
rustium.kafka.bootstrap.servers=kafka:9092
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://registry:8081
key.converter.auto.register.schemas=true
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://registry:8081
value.converter.auto.register.schemas=true
schema.name.adjustment.mode=avro
field.name.adjustment.mode=avro
"#,
        )
        .unwrap();

        assert_eq!(config.format.kind, FormatType::DebeziumAvro);
        assert_eq!(
            config.format.schema_registry.as_ref().unwrap().urls,
            ["http://registry:8081"]
        );
        assert!(!config.compatibility_warnings.iter().any(|warning| {
            warning.contains("converter") || warning.contains("name.adjustment.mode")
        }));

        let error = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://registry:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://registry:8081
field.name.adjustment.mode=none
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("field.name.adjustment.mode"));
    }

    #[test]
    fn maps_confluent_protobuf_converters_and_rejects_wire_variants() {
        let config = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
rustium.sink.type=kafka
rustium.kafka.bootstrap.servers=kafka:9092
key.converter=io.confluent.connect.protobuf.ProtobufConverter
key.converter.schema.registry.url=http://registry:8081
key.converter.scrub.invalid.names=true
key.converter.optional.for.nullables=false
value.converter=io.confluent.connect.protobuf.ProtobufConverter
value.converter.schema.registry.url=http://registry:8081
value.converter.scrub.invalid.names=true
value.converter.optional.for.nullables=false
"#,
        )
        .unwrap();

        assert_eq!(config.format.kind, FormatType::DebeziumProtobuf);
        assert_eq!(
            config.format.schema_registry.as_ref().unwrap().urls,
            ["http://registry:8081"]
        );
        assert!(!config.compatibility_warnings.iter().any(|warning| {
            warning.contains("converter") || warning.contains("optional.for.nullables")
        }));

        let error = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
key.converter=io.confluent.connect.protobuf.ProtobufConverter
key.converter.schema.registry.url=http://registry:8081
key.converter.optional.for.nullables=true
value.converter=io.confluent.connect.protobuf.ProtobufConverter
value.converter.schema.registry.url=http://registry:8081
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("optional.for.nullables=true"));
    }

    #[test]
    fn rejects_unsafe_json_schema_converter_variants() {
        let mismatched = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=io.confluent.connect.json.JsonSchemaConverter
value.converter.schema.registry.url=http://registry:8081
"#,
        )
        .unwrap_err();
        assert!(mismatched.to_string().contains("matching key.converter"));

        let no_registration = parse(
            r#"
name=inventory
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=mysql
database.user=rustium
database.password=secret
topic.prefix=inventory
key.converter=io.confluent.connect.json.JsonSchemaConverter
key.converter.schema.registry.url=http://registry:8081
key.converter.auto.register.schemas=false
value.converter=io.confluent.connect.json.JsonSchemaConverter
value.converter.schema.registry.url=http://registry:8081
"#,
        )
        .unwrap_err();
        assert!(
            no_registration
                .to_string()
                .contains("auto.register.schemas=false")
        );
    }
}
