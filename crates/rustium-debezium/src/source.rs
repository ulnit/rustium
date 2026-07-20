use std::{
    collections::{BTreeMap, HashSet},
    future::IntoFuture,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{DateTime, Utc};
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::OwnedMessage,
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use rustium_config::{
    DebeziumBridgeConfig, DebeziumConnectorKind, DebeziumSourceConfig, SnapshotConfig,
};
use rustium_core::{
    ChangeEvent, DataValue, DebeziumPosition, Error, EventId, EventSchema, FieldSchema, Operation,
    RecordBoundary, Result, RetryPolicy, Row, SourceConnector, SourceContext, SourceMetadata,
    SourcePosition, SourceRecord, TransactionMetadata,
};
use sha2::{Digest, Sha256};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, Command},
    sync::{Mutex, mpsc, watch},
};
use tracing::{info, warn};
use uuid::Uuid;

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct DebeziumSource {
    connector_name: String,
    kind: DebeziumConnectorKind,
    config: DebeziumSourceConfig,
    _snapshot: SnapshotConfig,
    retry_policy: RetryPolicy,
}

impl DebeziumSource {
    #[must_use]
    pub fn new(
        connector_name: impl Into<String>,
        kind: DebeziumConnectorKind,
        config: DebeziumSourceConfig,
        snapshot: SnapshotConfig,
    ) -> Self {
        Self {
            connector_name: connector_name.into(),
            kind,
            config,
            _snapshot: snapshot,
            retry_policy: RetryPolicy::default(),
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    fn kafka_consumer(&self) -> Result<StreamConsumer> {
        let DebeziumBridgeConfig::Kafka {
            bootstrap_servers,
            group_id,
            consumer_properties,
            ..
        } = &self.config.bridge
        else {
            return Err(Error::Invariant(
                "Kafka consumer requested for an HTTP Debezium bridge".into(),
            ));
        };
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers.join(","))
            .set("group.id", group_id)
            .set("client.id", format!("rustium-{}", self.connector_name))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest");
        for (key, value) in consumer_properties {
            config.set(key, value);
        }
        config.create().map_err(|error| {
            Error::Configuration(format!("invalid Debezium Kafka bridge config: {error}"))
        })
    }

    async fn validate_kafka(&self) -> Result<()> {
        let consumer = Arc::new(self.kafka_consumer()?);
        let timeout = match &self.config.bridge {
            DebeziumBridgeConfig::Kafka { poll_timeout, .. } => *poll_timeout,
            DebeziumBridgeConfig::Http { .. } => Duration::from_secs(10),
        };
        tokio::task::spawn_blocking(move || {
            consumer
                .fetch_metadata(None, Timeout::After(timeout))
                .map_err(|error| {
                    Error::Source(format!(
                        "Debezium Kafka bridge metadata request failed: {error}"
                    ))
                })?;
            Ok(())
        })
        .await
        .map_err(|error| Error::Source(format!("Kafka validation task failed: {error}")))?
    }

    async fn run_http(&self, context: SourceContext, checkpoint: BridgeCheckpoint) -> Result<()> {
        let DebeziumBridgeConfig::Http {
            listen,
            path,
            authentication_token,
            request_timeout,
            max_body_size,
        } = &self.config.bridge
        else {
            return Err(Error::Invariant(
                "HTTP bridge requested for a Kafka Debezium bridge".into(),
            ));
        };
        let address: SocketAddr = listen.parse().map_err(|error| {
            Error::Configuration(format!("invalid Debezium HTTP bridge address: {error}"))
        })?;
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|error| Error::Source(format!("bind Debezium HTTP bridge: {error}")))?;
        let local_address = listener
            .local_addr()
            .map_err(|error| Error::Source(format!("read Debezium bridge address: {error}")))?;
        let endpoint = bridge_endpoint(local_address, path);
        let state = Arc::new(HttpState {
            connector_name: self.connector_name.clone(),
            kind: self.kind,
            output: context.output,
            acknowledged: context.acknowledged,
            cancellation: context.cancellation.clone(),
            authentication_token: authentication_token.clone(),
            request_timeout: *request_timeout,
            processing: Mutex::new(ProcessingState {
                next_serial: checkpoint.event_serial,
                last_record_id: checkpoint.record_id,
                pending: None,
                snapshot_completed: checkpoint.snapshot_completed,
            }),
        });
        let router = Router::new()
            .route(path, post(ingest_http))
            .layer(DefaultBodyLimit::max(*max_body_size))
            .with_state(state);
        let mut managed = if self.config.command.is_some() {
            Some(self.launch_managed(&endpoint).await?)
        } else {
            None
        };
        info!(
            connector = %self.connector_name,
            source_type = self.kind.source_type(),
            endpoint = %endpoint,
            managed = managed.is_some(),
            max_retries = self.retry_policy.max_retries,
            "Debezium HTTP bridge started"
        );
        let cancellation = context.cancellation.clone();
        let shutdown = cancellation.clone();
        let server = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                shutdown.cancelled().await;
            })
            .into_future();
        tokio::pin!(server);
        let result = if let Some(process) = managed.as_mut() {
            tokio::select! {
                result = &mut server => result.map_err(|error| Error::Source(format!("Debezium HTTP bridge failed: {error}"))),
                status = process.child.wait() => {
                    let status = status.map_err(|error| Error::Source(format!("wait for Debezium command: {error}")))?;
                    if cancellation.is_cancelled() {
                        Ok(())
                    } else {
                        Err(Error::Source(format!("managed Debezium command exited with {status}")))
                    }
                }
            }
        } else {
            server
                .await
                .map_err(|error| Error::Source(format!("Debezium HTTP bridge failed: {error}")))
        };
        if let Some(mut process) = managed {
            if process.child.id().is_some() {
                let _ = process.child.kill().await;
                let _ = process.child.wait().await;
            }
            if let Err(error) = tokio::fs::remove_file(&process.config_path).await
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %process.config_path.display(), %error, "could not remove generated Debezium configuration");
            }
        }
        result
    }

    async fn launch_managed(&self, endpoint: &str) -> Result<ManagedProcess> {
        let command =
            self.config.command.as_ref().ok_or_else(|| {
                Error::Invariant("managed Debezium command is not configured".into())
            })?;
        let config_path = std::env::temp_dir().join(format!(
            "rustium-debezium-{}-{}-{}.properties",
            std::process::id(),
            sanitize_filename(&self.connector_name),
            Uuid::new_v4()
        ));
        write_managed_config(
            &config_path,
            self.kind,
            &self.config.properties,
            &self.config.offset_file,
            &self.config.schema_history_file,
            endpoint,
        )
        .await?;
        let config_path_text = config_path.to_string_lossy();
        let mut process = Command::new(command);
        process
            .args(self.config.command_args.iter().map(|argument| {
                argument
                    .replace("{config}", &config_path_text)
                    .replace("{endpoint}", endpoint)
            }))
            .env(
                "QUARKUS_CONFIG_LOCATIONS",
                format!("file:{}", config_path.display()),
            )
            .env("RUSTIUM_DEBEZIUM_CONFIG", &config_path)
            .envs(&self.config.command_environment)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let child = match process.spawn() {
            Ok(child) => child,
            Err(error) => {
                if let Err(removal_error) = tokio::fs::remove_file(&config_path).await {
                    warn!(
                        path = %config_path.display(),
                        %removal_error,
                        "could not remove generated Debezium configuration after launch failure"
                    );
                }
                return Err(Error::Source(format!(
                    "start managed Debezium command {command:?}: {error}"
                )));
            }
        };
        Ok(ManagedProcess { child, config_path })
    }

    async fn run_kafka(&self, context: SourceContext, checkpoint: BridgeCheckpoint) -> Result<()> {
        let DebeziumBridgeConfig::Kafka {
            topics,
            poll_timeout,
            ..
        } = &self.config.bridge
        else {
            return Err(Error::Invariant(
                "Kafka bridge requested for an HTTP Debezium bridge".into(),
            ));
        };
        let consumer = self.kafka_consumer()?;
        let topic_refs = topics.iter().map(String::as_str).collect::<Vec<_>>();
        consumer.subscribe(&topic_refs).map_err(|error| {
            Error::Source(format!("subscribe Debezium Kafka bridge topics: {error}"))
        })?;
        let mut event_serial = checkpoint.event_serial;
        let mut last_record_id = checkpoint.record_id;
        let mut snapshot_completed = checkpoint.snapshot_completed;
        info!(
            connector = %self.connector_name,
            source_type = self.kind.source_type(),
            topics = ?topics,
            "Debezium Kafka bridge started"
        );
        loop {
            let message = tokio::select! {
                _ = context.cancellation.cancelled() => return Ok(()),
                received = tokio::time::timeout(*poll_timeout, consumer.recv()) => match received {
                    Ok(message) => message.map(|message| message.detach()).map_err(kafka_error)?,
                    Err(_) => continue,
                }
            };
            let record_id = format!(
                "kafka:{}:{}:{}",
                message.topic(),
                message.partition(),
                message.offset()
            );
            if last_record_id.as_deref() != Some(&record_id)
                && let Some(payload) = message.payload()
                && !payload.is_empty()
            {
                let document: serde_json::Value =
                    serde_json::from_slice(payload).map_err(|error| {
                        Error::Source(format!(
                            "Debezium Kafka bridge payload is not JSON: {error}"
                        ))
                    })?;
                event_serial += 1;
                let decoded = decode_document(
                    &self.connector_name,
                    self.kind,
                    document,
                    record_id.clone(),
                    event_serial,
                    Some(message.topic()),
                    snapshot_completed,
                )?;
                if decoded.completes_snapshot {
                    snapshot_completed = true;
                }
                let position = decoded.record.position.clone();
                context
                    .output
                    .send(Ok(decoded.record))
                    .await
                    .map_err(|_| Error::Cancelled)?;
                wait_for_ack(
                    &position,
                    context.acknowledged.clone(),
                    &context.cancellation,
                    None,
                )
                .await?;
                last_record_id = Some(record_id);
            }
            commit_kafka(&consumer, &message)?;
        }
    }
}

#[async_trait]
impl SourceConnector for DebeziumSource {
    fn source_type(&self) -> &'static str {
        self.kind.source_type()
    }

    async fn validate(&mut self) -> Result<()> {
        match &self.config.bridge {
            DebeziumBridgeConfig::Http { listen, .. } => {
                let address = listen.parse::<SocketAddr>().map_err(|error| {
                    Error::Configuration(format!("invalid Debezium HTTP bridge address: {error}"))
                })?;
                let listener = tokio::net::TcpListener::bind(address)
                    .await
                    .map_err(|error| {
                        Error::Source(format!(
                            "Debezium HTTP bridge address {address} is unavailable: {error}"
                        ))
                    })?;
                drop(listener);
                if let Some(command) = &self.config.command
                    && command.contains(std::path::MAIN_SEPARATOR)
                    && !Path::new(command).is_file()
                {
                    return Err(Error::Configuration(format!(
                        "managed Debezium command {command:?} is not a file"
                    )));
                }
                Ok(())
            }
            DebeziumBridgeConfig::Kafka { .. } => self.validate_kafka().await,
        }
    }

    async fn run(&mut self, context: SourceContext) -> Result<()> {
        let checkpoint = BridgeCheckpoint::from_context(&context, self.kind)?;
        match &self.config.bridge {
            DebeziumBridgeConfig::Http { .. } => self.run_http(context, checkpoint).await,
            DebeziumBridgeConfig::Kafka { .. } => self.run_kafka(context, checkpoint).await,
        }
    }
}

struct ManagedProcess {
    child: Child,
    config_path: PathBuf,
}

struct BridgeCheckpoint {
    event_serial: u64,
    record_id: Option<String>,
    snapshot_completed: bool,
}

impl BridgeCheckpoint {
    fn from_context(context: &SourceContext, kind: DebeziumConnectorKind) -> Result<Self> {
        let Some(checkpoint) = &context.initial_checkpoint else {
            return Ok(Self {
                event_serial: 0,
                record_id: None,
                snapshot_completed: false,
            });
        };
        let SourcePosition::Debezium(position) = &checkpoint.source_position else {
            return Err(Error::State(format!(
                "{} connector cannot resume from another source checkpoint",
                kind.source_type()
            )));
        };
        if position.connector != kind.source_type() {
            return Err(Error::State(format!(
                "{} connector cannot resume from a {:?} Debezium checkpoint",
                kind.source_type(),
                position.connector
            )));
        }
        Ok(Self {
            event_serial: position.event_serial,
            record_id: Some(position.record_id.clone()),
            snapshot_completed: checkpoint.snapshot_completed,
        })
    }
}

struct HttpState {
    connector_name: String,
    kind: DebeziumConnectorKind,
    output: mpsc::Sender<Result<SourceRecord>>,
    acknowledged: watch::Receiver<Option<SourcePosition>>,
    cancellation: tokio_util::sync::CancellationToken,
    authentication_token: Option<String>,
    request_timeout: Duration,
    processing: Mutex<ProcessingState>,
}

struct ProcessingState {
    next_serial: u64,
    last_record_id: Option<String>,
    pending: Option<SourcePosition>,
    snapshot_completed: bool,
}

async fn ingest_http(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&headers, state.authentication_token.as_deref()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match process_http(state, &headers, &body).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(Error::Cancelled) => (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response(),
        Err(error) => {
            warn!(%error, "Debezium HTTP event was rejected");
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("invalid Debezium event: {error}"),
            )
                .into_response()
        }
    }
}

async fn process_http(state: Arc<HttpState>, headers: &HeaderMap, body: &[u8]) -> Result<()> {
    if body.is_empty() || body == b"null" {
        return Ok(());
    }
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| Error::Source(format!("Debezium HTTP body is not JSON: {error}")))?;
    let documents = match value {
        serde_json::Value::Array(values) => values,
        value => vec![value],
    };
    let base_id = header_record_id(headers);
    let topic_hint = header_topic(headers);
    let mut processing = state.processing.lock().await;
    for (index, document) in documents.into_iter().enumerate() {
        let record_id = base_id.clone().map_or_else(
            || document_record_id(&document),
            |id| {
                if index == 0 {
                    id
                } else {
                    format!("{id}:{index}")
                }
            },
        );
        if processing.last_record_id.as_deref() == Some(&record_id) {
            continue;
        }
        if let Some(pending) = processing.pending.clone() {
            let SourcePosition::Debezium(position) = &pending else {
                return Err(Error::Invariant(
                    "Debezium HTTP bridge pending a foreign position".into(),
                ));
            };
            if position.record_id != record_id {
                return Err(Error::Source(
                    "Debezium HTTP source sent a new record while the previous record is unacknowledged"
                        .into(),
                ));
            }
            wait_for_ack(
                &pending,
                state.acknowledged.clone(),
                &state.cancellation,
                Some(state.request_timeout),
            )
            .await?;
            processing.last_record_id = Some(record_id);
            processing.pending = None;
            continue;
        }
        processing.next_serial += 1;
        let decoded = decode_document(
            &state.connector_name,
            state.kind,
            document,
            record_id.clone(),
            processing.next_serial,
            topic_hint.as_deref(),
            processing.snapshot_completed,
        )?;
        if decoded.completes_snapshot {
            processing.snapshot_completed = true;
        }
        let position = decoded.record.position.clone();
        state
            .output
            .send(Ok(decoded.record))
            .await
            .map_err(|_| Error::Cancelled)?;
        processing.pending = Some(position.clone());
        wait_for_ack(
            &position,
            state.acknowledged.clone(),
            &state.cancellation,
            Some(state.request_timeout),
        )
        .await?;
        processing.last_record_id = Some(record_id);
        processing.pending = None;
    }
    Ok(())
}

struct DecodedRecord {
    record: SourceRecord,
    completes_snapshot: bool,
}

fn decode_document(
    connector_name: &str,
    kind: DebeziumConnectorKind,
    document: serde_json::Value,
    record_id: String,
    event_serial: u64,
    topic_hint: Option<&str>,
    snapshot_completed: bool,
) -> Result<DecodedRecord> {
    let payload = unwrap_payload(document)?;
    let source = payload
        .get("source")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(connector) = source.get("connector").and_then(serde_json::Value::as_str)
        && connector != kind.source_type()
    {
        return Err(Error::Source(format!(
            "expected {} event, received connector {connector:?}",
            kind.source_type()
        )));
    }
    let (snapshot, snapshot_last, incremental_snapshot) = snapshot_marker(&source);
    let stable_event_id = bridge_event_id(connector_name, &record_id);
    let source_position = SourcePosition::Debezium(DebeziumPosition {
        connector: kind.source_type().into(),
        source: source.clone().into_iter().collect(),
        record_id,
        event_serial,
        snapshot,
    });
    let completes_snapshot = snapshot_last || !snapshot && !snapshot_completed;
    let transaction_end = payload.get("status").and_then(serde_json::Value::as_str) == Some("END");
    let event_payload = payload.get("op").is_some()
        || payload.get("ddl").is_some()
        || payload.get("tableChanges").is_some();
    let boundary = if transaction_end {
        RecordBoundary::TransactionCommit
    } else if completes_snapshot {
        RecordBoundary::SnapshotComplete
    } else if event_payload {
        RecordBoundary::Data
    } else {
        RecordBoundary::Heartbeat
    };
    if !event_payload {
        return Ok(DecodedRecord {
            record: SourceRecord {
                event: None,
                position: source_position,
                boundary,
                connector_state: None,
                signal_acknowledgements: Vec::new(),
            },
            completes_snapshot,
        });
    }
    let operation = operation(payload.get("op"))?;
    let before = payload.get("before").and_then(row_from_json);
    let after = payload.get("after").and_then(row_from_json).or_else(|| {
        (payload.get("ddl").is_some() || payload.get("tableChanges").is_some())
            .then(|| Row::from([("_debezium".into(), DataValue::Json(payload.clone()))]))
    });
    let database = source_database(kind, &source);
    let schema = source_schema(kind, &source);
    let table = source
        .get("table")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| topic_hint.and_then(topic_table));
    let table = table.unwrap_or_else(|| "_schema_changes".into());
    let mut attributes = BTreeMap::from([(
        "debezium.source".into(),
        serde_json::Value::Object(source.clone()),
    )]);
    if incremental_snapshot {
        attributes.insert("rustium.snapshot.kind".into(), "incremental".into());
    }
    if before.is_none() && after.is_none() {
        attributes.insert("rustium.debezium.message".into(), true.into());
    }
    let row = after.as_ref().or(before.as_ref());
    let fields = row
        .into_iter()
        .flat_map(|row| row.iter())
        .map(|(name, value)| FieldSchema {
            name: name.clone(),
            type_name: value_type(value).into(),
            optional: true,
            primary_key: probable_key(name),
        })
        .collect();
    let transaction = transaction_metadata(&payload, &source);
    let source_time = source_time(&source);
    let event = ChangeEvent {
        id: stable_event_id,
        source: SourceMetadata {
            connector: kind.source_type().into(),
            connector_name: connector_name.into(),
            database,
            schema,
            table: Some(table.clone()),
            snapshot,
            version: CONNECTOR_VERSION.into(),
            attributes,
        },
        position: source_position,
        transaction,
        operation,
        before,
        after,
        schema: EventSchema {
            name: format!("{}.Envelope", table.replace('.', "_")),
            version: 1,
            fields,
        },
        source_time,
        observed_time: Utc::now(),
    };
    let position = event.position.clone();
    Ok(DecodedRecord {
        record: SourceRecord {
            event: Some(event),
            position,
            boundary,
            connector_state: None,
            signal_acknowledgements: Vec::new(),
        },
        completes_snapshot,
    })
}

fn unwrap_payload(mut value: serde_json::Value) -> Result<serde_json::Value> {
    for _ in 0..4 {
        if let serde_json::Value::String(text) = &value {
            value = serde_json::from_str(text).map_err(|error| {
                Error::Source(format!(
                    "Debezium structured event data is not JSON: {error}"
                ))
            })?;
            continue;
        }
        let next = value
            .get("data")
            .filter(|_| value.get("specversion").is_some())
            .cloned()
            .or_else(|| {
                value
                    .get("payload")
                    .filter(|_| value.get("schema").is_some())
                    .cloned()
            });
        if let Some(next) = next {
            value = next;
        } else {
            return Ok(value);
        }
    }
    Err(Error::Source(
        "Debezium event has too many nested data/payload envelopes".into(),
    ))
}

fn operation(value: Option<&serde_json::Value>) -> Result<Operation> {
    match value.and_then(serde_json::Value::as_str) {
        Some("r") => Ok(Operation::Read),
        Some("c") => Ok(Operation::Create),
        Some("u") => Ok(Operation::Update),
        Some("d") => Ok(Operation::Delete),
        Some("t") => Ok(Operation::Truncate),
        Some("m") | None => Ok(Operation::Message),
        Some(value) => Err(Error::Source(format!(
            "unsupported Debezium operation code {value:?}"
        ))),
    }
}

fn row_from_json(value: &serde_json::Value) -> Option<Row> {
    value.as_object().map(|object| {
        object
            .iter()
            .map(|(key, value)| (key.clone(), json_value(value)))
            .collect()
    })
}

fn json_value(value: &serde_json::Value) -> DataValue {
    match value {
        serde_json::Value::Null => DataValue::Null,
        serde_json::Value::Bool(value) => DataValue::Boolean(*value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(DataValue::Int64)
            .or_else(|| value.as_u64().map(DataValue::UInt64))
            .or_else(|| value.as_f64().map(DataValue::Float64))
            .unwrap_or_else(|| DataValue::Decimal(value.to_string())),
        serde_json::Value::String(value) => DataValue::String(value.clone()),
        serde_json::Value::Array(values) => {
            DataValue::Array(values.iter().map(json_value).collect())
        }
        serde_json::Value::Object(values) => DataValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_value(value)))
                .collect(),
        ),
    }
}

fn value_type(value: &DataValue) -> &'static str {
    match value {
        DataValue::Null => "null",
        DataValue::Boolean(_) => "boolean",
        DataValue::Int32(_) => "int32",
        DataValue::Int64(_) => "int64",
        DataValue::UInt64(_) => "uint64",
        DataValue::Float64(_) => "double",
        DataValue::Decimal(_) => "decimal",
        DataValue::String(_) => "string",
        DataValue::Bytes(_) => "bytes",
        DataValue::Date(_) => "date",
        DataValue::Time(_) => "time",
        DataValue::Timestamp(_) => "timestamp",
        DataValue::Uuid(_) => "uuid",
        DataValue::Json(_) => "json",
        DataValue::Array(_) => "array",
        DataValue::Map(_) => "map",
        DataValue::Unavailable => "unavailable",
    }
}

fn probable_key(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "id" || name == "_id" || name.ends_with("_id")
}

fn snapshot_marker(source: &serde_json::Map<String, serde_json::Value>) -> (bool, bool, bool) {
    match source.get("snapshot") {
        Some(serde_json::Value::Bool(value)) => (*value, false, false),
        Some(serde_json::Value::String(value)) if value == "last" => (true, true, false),
        Some(serde_json::Value::String(value)) if value == "incremental" => (true, false, true),
        Some(serde_json::Value::String(value)) if value == "true" => (true, false, false),
        _ => (false, false, false),
    }
}

fn source_database(
    kind: DebeziumConnectorKind,
    source: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let candidates: &[&str] = match kind {
        DebeziumConnectorKind::Cassandra => &["keyspace", "db", "cluster"],
        DebeziumConnectorKind::Spanner => &["database_id", "db"],
        _ => &["db", "database", "keyspace", "database_id", "cluster"],
    };
    candidates
        .iter()
        .find_map(|key| source.get(*key).and_then(serde_json::Value::as_str))
        .unwrap_or("unknown")
        .to_string()
}

fn source_schema(
    kind: DebeziumConnectorKind,
    source: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    if matches!(
        kind,
        DebeziumConnectorKind::Mariadb
            | DebeziumConnectorKind::Cassandra
            | DebeziumConnectorKind::Vitess
            | DebeziumConnectorKind::Spanner
    ) {
        None
    } else {
        source
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }
}

fn source_time(source: &serde_json::Map<String, serde_json::Value>) -> Option<DateTime<Utc>> {
    if let Some(value) = source.get("ts_ns").and_then(serde_json::Value::as_i64) {
        return DateTime::from_timestamp(
            value.div_euclid(1_000_000_000),
            value.rem_euclid(1_000_000_000) as u32,
        );
    }
    if let Some(value) = source.get("ts_us").and_then(serde_json::Value::as_i64) {
        return DateTime::from_timestamp_micros(value);
    }
    source
        .get("ts_ms")
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| {
            if value.unsigned_abs() > 100_000_000_000_000 {
                DateTime::from_timestamp_micros(value)
            } else {
                DateTime::from_timestamp_millis(value)
            }
        })
}

fn transaction_metadata(
    payload: &serde_json::Value,
    source: &serde_json::Map<String, serde_json::Value>,
) -> Option<TransactionMetadata> {
    if let Some(transaction) = payload
        .get("transaction")
        .and_then(serde_json::Value::as_object)
    {
        return transaction
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(|id| TransactionMetadata {
                id: id.into(),
                total_order: json_u64(transaction.get("total_order")),
                collection_order: json_u64(transaction.get("data_collection_order")),
            });
    }
    ["txId", "server_transaction_id", "vgtid"]
        .iter()
        .find_map(|key| source.get(*key))
        .and_then(json_text)
        .map(|id| TransactionMetadata {
            id,
            total_order: None,
            collection_order: None,
        })
}

fn json_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn json_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        value => Some(value.to_string()),
    }
}

fn topic_table(topic: &str) -> Option<String> {
    topic
        .rsplit('.')
        .next()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn header_topic(headers: &HeaderMap) -> Option<String> {
    ["ce-subject", "x-debezium-destination", "x-debezium-topic"]
        .iter()
        .find_map(|name| headers.get(*name))
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn header_record_id(headers: &HeaderMap) -> Option<String> {
    ["ce-id", "x-debezium-id", "x-rustium-event-id"]
        .iter()
        .find_map(|name| headers.get(*name))
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn document_record_id(document: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(document).expect("JSON serialization cannot fail");
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn bridge_event_id(connector_name: &str, record_id: &str) -> EventId {
    let mut digest = Sha256::new();
    digest.update(connector_name.as_bytes());
    digest.update([0]);
    digest.update(record_id.as_bytes());
    EventId(hex::encode(digest.finalize()))
}

fn authorized(headers: &HeaderMap, token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {token}"))
        || headers
            .get("x-rustium-token")
            .and_then(|value| value.to_str().ok())
            == Some(token)
}

async fn wait_for_ack(
    expected: &SourcePosition,
    mut acknowledged: watch::Receiver<Option<SourcePosition>>,
    cancellation: &tokio_util::sync::CancellationToken,
    timeout: Option<Duration>,
) -> Result<()> {
    let wait = async {
        loop {
            if acknowledged
                .borrow()
                .as_ref()
                .is_some_and(|position| position == expected || position.is_after(expected))
            {
                return Ok(());
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                changed = acknowledged.changed() => changed.map_err(|_| Error::Cancelled)?,
            }
        }
    };
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, wait).await.map_err(|_| {
            Error::Source(format!(
                "Rustium did not durably acknowledge the Debezium event within {} ms",
                timeout.as_millis()
            ))
        })?
    } else {
        wait.await
    }
}

fn commit_kafka(consumer: &StreamConsumer, message: &OwnedMessage) -> Result<()> {
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset(
            message.topic(),
            message.partition(),
            Offset::Offset(message.offset() + 1),
        )
        .map_err(|error| Error::Source(format!("build Kafka bridge offset: {error}")))?;
    consumer
        .commit(&offsets, CommitMode::Sync)
        .map_err(|error| Error::Source(format!("commit Kafka bridge offset: {error}")))
}

fn kafka_error(error: rdkafka::error::KafkaError) -> Error {
    Error::Source(format!("Debezium Kafka bridge receive failed: {error}"))
}

fn bridge_endpoint(address: SocketAddr, path: &str) -> String {
    let host = match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "127.0.0.1".into(),
        IpAddr::V6(ip) if ip.is_unspecified() => "[::1]".into(),
        IpAddr::V6(ip) => format!("[{ip}]"),
        IpAddr::V4(ip) => ip.to_string(),
    };
    format!("http://{host}:{}{path}", address.port())
}

async fn write_managed_config(
    path: &Path,
    kind: DebeziumConnectorKind,
    properties: &BTreeMap<String, String>,
    offset_file: &str,
    schema_history_file: &str,
    endpoint: &str,
) -> Result<()> {
    let connector_class = kind.connector_class().ok_or_else(|| {
        Error::Configuration(format!(
            "{} does not run through Debezium Server",
            kind.source_type()
        ))
    })?;
    let mut lines = vec![
        "debezium.sink.type=http".to_string(),
        format!("debezium.sink.http.url={}", properties_value(endpoint)),
        "debezium.sink.http.batch.enabled=false".into(),
        "debezium.format.key=json".into(),
        "debezium.format.key.schemas.enable=false".into(),
        "debezium.format.value=json".into(),
        "debezium.format.value.schemas.enable=false".into(),
        format!(
            "debezium.source.connector.class={}",
            properties_value(connector_class)
        ),
        "debezium.source.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore"
            .into(),
        format!(
            "debezium.source.offset.storage.file.filename={}",
            properties_value(offset_file)
        ),
        "debezium.source.offset.flush.interval.ms=0".into(),
        "debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory"
            .into(),
        format!(
            "debezium.source.schema.history.internal.file.filename={}",
            properties_value(schema_history_file)
        ),
    ];
    let owned = HashSet::from([
        "connector.class",
        "key.converter",
        "value.converter",
        "key.converter.schemas.enable",
        "value.converter.schemas.enable",
        "offset.storage",
        "offset.storage.file.filename",
        "offset.flush.interval.ms",
        "schema.history.internal",
        "schema.history.internal.file.filename",
    ]);
    for (key, value) in properties {
        if owned.contains(key.as_str()) {
            continue;
        }
        lines.push(format!(
            "debezium.source.{}={}",
            properties_key(key),
            properties_value(value)
        ));
    }
    lines.push(String::new());
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(path).await.map_err(|error| {
        Error::Source(format!("create generated Debezium configuration: {error}"))
    })?;
    file.write_all(lines.join("\n").as_bytes())
        .await
        .map_err(|error| Error::Source(format!("write Debezium configuration: {error}")))?;
    file.flush()
        .await
        .map_err(|error| Error::Source(format!("flush Debezium configuration: {error}")))
}

fn properties_key(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('=', "\\=")
        .replace(':', "\\:")
}

fn properties_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn sanitize_filename(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_connector_specific_source_metadata_without_losing_offsets() {
        let document = serde_json::json!({
            "before": null,
            "after": {"ID": 1, "NAME": "test"},
            "source": {
                "connector": "db2",
                "snapshot": "false",
                "db": "inventory",
                "schema": "APP",
                "table": "CUSTOMERS",
                "change_lsn": "0001:0002",
                "commit_lsn": "0001:0003"
            },
            "op": "c",
            "ts_ms": 1_780_000_000_000_i64
        });
        let decoded = decode_document(
            "db2-test",
            DebeziumConnectorKind::Db2,
            document,
            "record-1".into(),
            1,
            None,
            true,
        )
        .unwrap();
        let event = decoded.record.event.unwrap();
        assert_eq!(event.operation, Operation::Create);
        assert_eq!(event.source.database, "inventory");
        assert_eq!(event.source.schema.as_deref(), Some("APP"));
        assert!(event.schema.fields.iter().any(|field| field.primary_key));
        let SourcePosition::Debezium(position) = event.position else {
            panic!("expected Debezium position");
        };
        assert_eq!(position.source["commit_lsn"], "0001:0003");
    }

    #[test]
    fn unwraps_schema_and_structured_cloudevent_envelopes() {
        let value = serde_json::json!({
            "specversion": "1.0",
            "data": {
                "schema": {},
                "payload": {"op": "d", "source": {"connector": "informix"}}
            }
        });
        let payload = unwrap_payload(value).unwrap();
        assert_eq!(payload["op"], "d");
    }

    #[test]
    fn uses_stable_content_hash_when_http_id_is_absent() {
        let value = serde_json::json!({"op": "c", "after": {"id": 1}});
        assert_eq!(document_record_id(&value), document_record_id(&value));
        assert_eq!(
            bridge_event_id("connector", "record-1"),
            bridge_event_id("connector", "record-1")
        );
    }

    #[test]
    fn preserves_schema_change_documents_as_message_events() {
        let document = serde_json::json!({
            "source": {
                "connector": "informix",
                "snapshot": "false",
                "db": "inventory",
                "schema": "app"
            },
            "ddl": "ALTER TABLE app.orders ADD note VARCHAR(64)",
            "tableChanges": []
        });
        let decoded = decode_document(
            "informix-test",
            DebeziumConnectorKind::Informix,
            document,
            "schema-1".into(),
            1,
            None,
            true,
        )
        .unwrap();
        let event = decoded.record.event.unwrap();
        assert_eq!(event.operation, Operation::Message);
        assert!(event.after.unwrap().contains_key("_debezium"));
    }

    #[tokio::test]
    async fn durable_http_ack_waits_for_the_runtime_checkpoint() {
        let position = SourcePosition::Debezium(DebeziumPosition {
            connector: "db2".into(),
            source: BTreeMap::new(),
            record_id: "record-1".into(),
            event_serial: 1,
            snapshot: false,
        });
        let (sender, receiver) = watch::channel(None);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let waiting = tokio::spawn({
            let position = position.clone();
            let cancellation = cancellation.clone();
            async move {
                wait_for_ack(
                    &position,
                    receiver,
                    &cancellation,
                    Some(Duration::from_secs(1)),
                )
                .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        sender.send(Some(position)).unwrap();
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn managed_config_owns_http_and_durable_offset_settings() {
        let path = std::env::temp_dir().join(format!(
            "rustium-managed-config-test-{}-{}.properties",
            std::process::id(),
            Uuid::new_v4()
        ));
        write_managed_config(
            &path,
            DebeziumConnectorKind::Db2,
            &BTreeMap::from([("topic.prefix".into(), "inventory".into())]),
            "/state/db2.offsets",
            "/state/db2-schema.dat",
            "http://127.0.0.1:18080/events",
        )
        .await
        .unwrap();
        let config = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(config.contains("debezium.sink.http.batch.enabled=false"));
        assert!(config.contains("debezium.source.offset.flush.interval.ms=0"));
        assert!(config.contains("debezium.source.topic.prefix=inventory"));
        assert!(config.contains("FileSchemaHistory"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = tokio::fs::metadata(&path)
                .await
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let duplicate = write_managed_config(
            &path,
            DebeziumConnectorKind::Db2,
            &BTreeMap::new(),
            "/state/replacement.offsets",
            "/state/replacement-schema.dat",
            "http://127.0.0.1:18081/events",
        )
        .await
        .unwrap_err();
        assert!(duplicate.to_string().contains("create generated"));
        assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), config);
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn http_request_completes_only_after_its_emitted_record_is_acknowledged() {
        let (output, mut records) = mpsc::channel(1);
        let (acknowledge, acknowledged) = watch::channel(None);
        let state = Arc::new(HttpState {
            connector_name: "db2-http".into(),
            kind: DebeziumConnectorKind::Db2,
            output,
            acknowledged,
            cancellation: tokio_util::sync::CancellationToken::new(),
            authentication_token: None,
            request_timeout: Duration::from_secs(1),
            processing: Mutex::new(ProcessingState {
                next_serial: 0,
                last_record_id: None,
                pending: None,
                snapshot_completed: true,
            }),
        });
        let body = serde_json::to_vec(&serde_json::json!({
            "before": null,
            "after": {"ID": 1},
            "source": {
                "connector": "db2",
                "snapshot": "false",
                "db": "inventory",
                "schema": "APP",
                "table": "CUSTOMERS",
                "commit_lsn": "0001:0002"
            },
            "op": "c"
        }))
        .unwrap();
        let processing =
            tokio::spawn(async move { process_http(state, &HeaderMap::new(), &body).await });
        let record = records.recv().await.unwrap().unwrap();
        assert!(!processing.is_finished());
        acknowledge.send(Some(record.position)).unwrap();
        processing.await.unwrap().unwrap();
    }
}
