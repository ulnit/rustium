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
            slot_name: properties
                .get("slot.name")
                .cloned()
                .unwrap_or_else(|| "debezium".into()),
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
            ssl_ca: properties.get("database.ssl.ca").cloned(),
            ssl_cert: properties.get("database.ssl.cert").cloned(),
            ssl_key: properties.get("database.ssl.key").cloned(),
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
    let warnings = Vec::new();
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

    let config = Config {
        api_version: API_VERSION.into(),
        kind: "Connector".into(),
        metadata: Metadata {
            name: required(properties, "name")?.to_string(),
            labels: BTreeMap::new(),
        },
        source,
        snapshot,
        format: FormatConfig {
            kind: FormatType::DebeziumJson,
            unavailable_value: properties
                .get("unavailable.value.placeholder")
                .cloned()
                .unwrap_or_else(default_unavailable_value),
            tombstones_on_delete: bool_value(properties, "tombstones.on.delete", true)?,
        },
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
        "initial" | "always" | "initial_only" => Ok(SnapshotMode::Initial),
        "when_needed" => Ok(SnapshotMode::WhenNeeded),
        "never" | "no_data" | "schema_only" => Ok(SnapshotMode::Never),
        other => Err(Error::Configuration(format!(
            "snapshot.mode={other:?} is recognized but not supported by the current connector"
        ))),
    }
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
        "database.ssl.ca",
        "database.ssl.cert",
        "database.ssl.key",
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
        "publication.name",
        "snapshot.mode",
        "snapshot.fetch.size",
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
        "offset.storage.file.filename",
        "bootstrap.servers",
        "rustium.sink.type",
        "rustium.source.reconnect.max.attempts",
        "schema.history.internal.skip.unparseable.ddl",
        "rustium.kafka.bootstrap.servers",
        "rustium.kafka.acks",
        "rustium.kafka.compression.type",
        "rustium.kafka.delivery.timeout.ms",
        "rustium.state.path",
        "rustium.server.bind",
        "rustium.server.enable.mutations",
        "rustium.log.format",
        "rustium.log.level",
        "rustium.metrics.enabled",
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
publication.name=orders_pub
table.include.list=public\\.(orders|customers)
snapshot.mode=initial
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
database.ssl.ca=/etc/mysql/ca.pem
database.ssl.cert=/etc/mysql/client.pem
database.ssl.key=/etc/mysql/client-key.pem
database.include.list=inventory
table.include.list=inventory\.(orders|customers)
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
snapshot.isolation.mode=snapshot
streaming.fetch.size=2048
heartbeat.interval.ms=1000
heartbeat.action.query=UPDATE dbo.heartbeat SET touched_at = SYSUTCDATETIME()
topic.heartbeat.prefix=__sql-heartbeat
topic.heartbeat.name=shared-sql-heartbeat
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
        assert!(config.format.tombstones_on_delete);
    }
}
