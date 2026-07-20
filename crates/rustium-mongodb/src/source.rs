use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use mongodb::{
    Client,
    bson::{Bson, Document, Timestamp, doc},
    change_stream::{
        ChangeStream,
        event::{ChangeNamespace, ChangeStreamEvent, OperationType},
    },
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType},
};
use regex::Regex;
use rustium_config::{MongoDbSourceConfig, SnapshotConfig, SnapshotMode};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, MongoDbPosition, Operation,
    RecordBoundary, Result, RetryPolicy, Row, SourceConnector, SourceContext, SourceMetadata,
    SourcePosition, SourceRecord, TransactionMetadata,
};
use tokio::time::MissedTickBehavior;
use tracing::info;

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct MongoDbSource {
    connector_name: String,
    config: MongoDbSourceConfig,
    snapshot: SnapshotConfig,
    retry_policy: RetryPolicy,
    collection_filters: Vec<Regex>,
    collection_excludes: Vec<Regex>,
}

impl MongoDbSource {
    #[must_use]
    pub fn new(
        connector_name: impl Into<String>,
        config: MongoDbSourceConfig,
        snapshot: SnapshotConfig,
    ) -> Self {
        Self {
            connector_name: connector_name.into(),
            config,
            snapshot,
            retry_policy: RetryPolicy::default(),
            collection_filters: Vec::new(),
            collection_excludes: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    async fn connect(&self) -> Result<Client> {
        let mut options = mongodb::options::ClientOptions::parse(&self.config.connection_string)
            .await
            .map_err(mongo_error)?;
        options.connect_timeout = Some(self.config.connect_timeout);
        options.app_name = Some(format!("rustium/{CONNECTOR_VERSION}"));
        Client::with_options(options).map_err(mongo_error)
    }

    async fn validate_source(&mut self) -> Result<()> {
        self.collection_filters = compile_patterns(&self.config.collections.include, "include")?;
        self.collection_excludes = compile_patterns(&self.config.collections.exclude, "exclude")?;
        let client = self.connect().await?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(mongo_error)?;
        let databases = self.databases(&client).await?;
        if databases.is_empty() {
            return Err(Error::Configuration(
                "MongoDB source selected no databases".into(),
            ));
        }
        for database in databases {
            for collection in client
                .database(&database)
                .list_collection_names()
                .await
                .map_err(mongo_error)?
            {
                if self.collection_selected(&format!("{database}.{collection}")) {
                    return Ok(());
                }
            }
        }
        if self.config.collections.include.is_empty() {
            Ok(())
        } else {
            Err(Error::Configuration(
                "MongoDB collection filters select no collections".into(),
            ))
        }
    }

    async fn databases(&self, client: &Client) -> Result<Vec<String>> {
        if self.config.databases.is_empty() {
            Ok(client
                .list_database_names()
                .await
                .map_err(mongo_error)?
                .into_iter()
                .filter(|database| !matches!(database.as_str(), "admin" | "config" | "local"))
                .collect())
        } else {
            Ok(self.config.databases.clone())
        }
    }

    async fn operation_time(&self, client: &Client) -> Result<Option<Timestamp>> {
        let response = client
            .database("admin")
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(mongo_error)?;
        Ok(response.get_timestamp("operationTime").ok())
    }

    async fn open_stream(
        &self,
        client: &Client,
        resume: Option<&MongoDbPosition>,
        operation_time: Option<Timestamp>,
    ) -> Result<ChangeStream<ChangeStreamEvent<Document>>> {
        let mut options = ChangeStreamOptions::builder()
            .max_await_time(Some(self.config.poll_interval))
            .batch_size(Some(self.config.batch_size as u32))
            .build();
        if self.config.full_document == "update_lookup" {
            options.full_document = Some(FullDocumentType::UpdateLookup);
        }
        options.full_document_before_change = match self.config.full_document_before_change.as_str()
        {
            "when_available" => Some(FullDocumentBeforeChangeType::WhenAvailable),
            "required" => Some(FullDocumentBeforeChangeType::Required),
            _ => None,
        };
        if let Some(position) = resume {
            if let Some(token) = &position.resume_token {
                options.resume_after = Some(serde_json::from_str(token).map_err(|error| {
                    Error::State(format!("invalid MongoDB checkpoint resume token: {error}"))
                })?);
            } else if let (Some(seconds), Some(increment)) = (
                position.cluster_time_seconds,
                position.cluster_time_increment,
            ) {
                options.start_at_operation_time = Some(Timestamp {
                    time: seconds,
                    increment,
                });
            }
        } else if let Some(operation_time) = operation_time {
            options.start_at_operation_time = Some(operation_time);
        }
        if self.config.databases.len() == 1 {
            client
                .database(&self.config.databases[0])
                .watch()
                .with_options(options)
                .await
                .map_err(mongo_error)
        } else {
            client
                .watch()
                .with_options(options)
                .await
                .map_err(mongo_error)
        }
    }

    async fn snapshot(
        &self,
        client: &Client,
        anchor: Option<Timestamp>,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<MongoDbPosition> {
        let mut serial = 0_u64;
        for database in self.databases(client).await? {
            let db = client.database(&database);
            let collections = db.list_collection_names().await.map_err(mongo_error)?;
            for collection in collections {
                let namespace = format!("{database}.{collection}");
                if !self.collection_selected(&namespace) {
                    continue;
                }
                let mut cursor = db
                    .collection::<Document>(&collection)
                    .find(doc! {})
                    .sort(doc! { "_id": 1 })
                    .batch_size(self.config.batch_size as u32)
                    .await
                    .map_err(mongo_error)?;
                while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
                    serial += 1;
                    let position = mongo_position(anchor, None, serial, true);
                    let event = self.event(
                        namespace.clone(),
                        position,
                        Operation::Read,
                        None,
                        Some(document_to_row(&document)),
                        None,
                    );
                    output
                        .send(Ok(SourceRecord::data(event)))
                        .await
                        .map_err(|_| Error::Cancelled)?;
                }
            }
        }
        let position = mongo_position(anchor, None, serial + 1, true);
        output
            .send(Ok(SourceRecord {
                event: None,
                position: SourcePosition::MongoDb(position.clone()),
                boundary: RecordBoundary::SnapshotComplete,
                connector_state: None,
                signal_acknowledgements: Vec::new(),
            }))
            .await
            .map_err(|_| Error::Cancelled)?;
        Ok(position)
    }

    fn collection_selected(&self, namespace: &str) -> bool {
        table_selected(
            namespace,
            &self.collection_filters,
            &self.collection_excludes,
        )
    }

    fn event(
        &self,
        namespace: String,
        position: MongoDbPosition,
        operation: Operation,
        before: Option<Row>,
        after: Option<Row>,
        transaction: Option<TransactionMetadata>,
    ) -> ChangeEvent {
        let (database, collection) = namespace
            .split_once('.')
            .map_or((namespace.as_str(), ""), |parts| parts);
        let mut fields = BTreeMap::new();
        if let Some(row) = before.as_ref().or(after.as_ref()) {
            for (name, value) in row {
                fields.insert(name.clone(), value_type(value).to_string());
            }
        }
        let schema_fields = fields
            .into_iter()
            .map(|(name, type_name)| FieldSchema {
                primary_key: name == "_id",
                name,
                type_name,
                optional: true,
            })
            .collect();
        let id = EventId::deterministic(
            &self.connector_name,
            database,
            &SourcePosition::MongoDb(position.clone()),
            &namespace,
            position.event_serial,
        );
        ChangeEvent {
            id,
            source: SourceMetadata {
                connector: "mongodb".into(),
                connector_name: self.connector_name.clone(),
                database: database.into(),
                schema: None,
                table: Some(collection.into()),
                snapshot: position.snapshot,
                version: CONNECTOR_VERSION.into(),
                attributes: BTreeMap::new(),
            },
            position: SourcePosition::MongoDb(position),
            transaction,
            operation,
            before,
            after,
            schema: EventSchema {
                name: format!("{}.Envelope", namespace.replace('.', "_")),
                version: 1,
                fields: schema_fields,
            },
            source_time: None,
            observed_time: Utc::now(),
        }
    }
}

#[async_trait]
impl SourceConnector for MongoDbSource {
    fn source_type(&self) -> &'static str {
        "mongodb"
    }

    async fn validate(&mut self) -> Result<()> {
        self.validate_source().await
    }

    async fn run(&mut self, context: SourceContext) -> Result<()> {
        let checkpoint = context.initial_checkpoint.clone();
        if checkpoint.as_ref().is_some_and(|checkpoint| {
            !matches!(checkpoint.source_position, SourcePosition::MongoDb(_))
        }) {
            return Err(Error::State(
                "MongoDB connector cannot resume from another source checkpoint".into(),
            ));
        }
        let client = self.connect().await?;
        let anchor = self.operation_time(&client).await?;
        let checkpoint_position =
            checkpoint
                .as_ref()
                .and_then(|checkpoint| match &checkpoint.source_position {
                    SourcePosition::MongoDb(position) => Some(position.clone()),
                    _ => None,
                });
        let snapshot_needed = match self.snapshot.mode {
            SnapshotMode::Never => false,
            SnapshotMode::Initial | SnapshotMode::WhenNeeded => checkpoint
                .as_ref()
                .is_none_or(|checkpoint| !checkpoint.snapshot_completed),
        };
        let mut stream = self
            .open_stream(&client, checkpoint_position.as_ref(), anchor)
            .await?;
        let mut last_position = if snapshot_needed {
            self.snapshot(&client, anchor, &context.output).await?
        } else {
            checkpoint_position
                .clone()
                .unwrap_or_else(|| mongo_position(anchor, None, 0, false))
        };
        let mut heartbeat =
            tokio::time::interval(self.config.heartbeat_interval.max(Duration::from_secs(1)));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        info!(connector = %self.connector_name, "MongoDB Change Stream started");
        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => return Ok(()),
                _ = heartbeat.tick(), if !self.config.heartbeat_interval.is_zero() => {
                    context.output.send(Ok(SourceRecord {
                        event: None,
                        position: SourcePosition::MongoDb(last_position.clone()),
                        boundary: RecordBoundary::Heartbeat,
                        connector_state: None,
                        signal_acknowledgements: Vec::new(),
                    })).await.map_err(|_| Error::Cancelled)?;
                }
                item = stream.try_next() => {
                    let event = item.map_err(mongo_error)?.ok_or_else(|| Error::Source("MongoDB Change Stream ended unexpectedly".into()))?;
                    let Some(namespace) = event.ns.as_ref().and_then(namespace) else { continue; };
                    if !self.collection_selected(&namespace) { continue; }
                    let token = serde_json::to_string(&event.id).map_err(|error| Error::Source(format!("serialize MongoDB resume token: {error}")))?;
                    let position = mongo_position(event.cluster_time, Some(token), last_position.event_serial + 1, false);
                    let transaction = event.txn_number.map(|number| TransactionMetadata {
                        id: format!("{}:{number}", event.lsid.as_ref().map_or_else(|| "unknown".into(), |id| format!("{id:?}"))),
                        total_order: None,
                        collection_order: None,
                    });
                    let (operation, before, after) = change_event_rows(&event);
                    last_position = position.clone();
                    let change = self.event(namespace, position, operation, before, after, transaction);
                    context.output.send(Ok(SourceRecord::data(change))).await.map_err(|_| Error::Cancelled)?;
                }
            }
        }
    }
}

fn mongo_error(error: impl std::fmt::Display) -> Error {
    Error::Source(format!("MongoDB: {error}"))
}

fn compile_patterns(patterns: &[String], kind: &str) -> Result<Vec<Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            Regex::new(pattern).map_err(|error| {
                Error::Configuration(format!(
                    "invalid MongoDB collection {kind} selector {pattern:?}: {error}"
                ))
            })
        })
        .collect()
}

fn table_selected(namespace: &str, includes: &[Regex], excludes: &[Regex]) -> bool {
    (includes.is_empty() || includes.iter().any(|regex| regex.is_match(namespace)))
        && !excludes.iter().any(|regex| regex.is_match(namespace))
}

fn namespace(value: &ChangeNamespace) -> Option<String> {
    value
        .coll
        .as_ref()
        .map(|collection| format!("{}.{}", value.db, collection))
}

fn mongo_position(
    timestamp: Option<Timestamp>,
    resume_token: Option<String>,
    event_serial: u64,
    snapshot: bool,
) -> MongoDbPosition {
    MongoDbPosition {
        resume_token,
        cluster_time_seconds: timestamp.map(|value| value.time),
        cluster_time_increment: timestamp.map(|value| value.increment),
        event_serial,
        snapshot,
    }
}

fn change_event_rows(event: &ChangeStreamEvent<Document>) -> (Operation, Option<Row>, Option<Row>) {
    match event.operation_type {
        OperationType::Insert | OperationType::Replace => (
            if matches!(event.operation_type, OperationType::Insert) {
                Operation::Create
            } else {
                Operation::Update
            },
            event
                .full_document_before_change
                .as_ref()
                .map(document_to_row),
            event.full_document.as_ref().map(document_to_row),
        ),
        OperationType::Update => {
            let after = event
                .full_document
                .as_ref()
                .map(document_to_row)
                .or_else(|| {
                    event.update_description.as_ref().map(|description| {
                        let mut row = description
                            .updated_fields
                            .iter()
                            .map(|(key, value)| (key.clone(), bson_to_value(value)))
                            .collect::<Row>();
                        if let Some(key) = event.document_key.as_ref() {
                            row.extend(
                                key.iter()
                                    .map(|(key, value)| (key.clone(), bson_to_value(value))),
                            );
                        }
                        row
                    })
                });
            (
                Operation::Update,
                event
                    .full_document_before_change
                    .as_ref()
                    .map(document_to_row),
                after,
            )
        }
        OperationType::Delete => (
            Operation::Delete,
            event
                .full_document_before_change
                .as_ref()
                .map(document_to_row)
                .or_else(|| event.document_key.as_ref().map(document_to_row)),
            None,
        ),
        _ => (Operation::Message, None, None),
    }
}

fn document_to_row(document: &Document) -> Row {
    document
        .iter()
        .map(|(key, value)| (key.clone(), bson_to_value(value)))
        .collect()
}

fn bson_to_value(value: &Bson) -> DataValue {
    match value {
        Bson::Double(value) => DataValue::Float64(*value),
        Bson::String(value) => DataValue::String(value.clone()),
        Bson::Array(values) => DataValue::Array(values.iter().map(bson_to_value).collect()),
        Bson::Document(value) => DataValue::Map(
            value
                .iter()
                .map(|(key, value)| (key.clone(), bson_to_value(value)))
                .collect(),
        ),
        Bson::Boolean(value) => DataValue::Boolean(*value),
        Bson::Null => DataValue::Null,
        Bson::RegularExpression(value) => DataValue::String(value.pattern.clone()),
        Bson::JavaScriptCode(value) => DataValue::String(value.clone()),
        Bson::JavaScriptCodeWithScope(value) => {
            DataValue::Json(serde_json::to_value(value).unwrap_or_default())
        }
        Bson::Int32(value) => DataValue::Int32(*value),
        Bson::Int64(value) => DataValue::Int64(*value),
        Bson::Timestamp(value) => DataValue::Map(BTreeMap::from([
            ("t".into(), DataValue::Int64(i64::from(value.time))),
            ("i".into(), DataValue::Int64(i64::from(value.increment))),
        ])),
        Bson::Binary(value) => DataValue::Bytes(value.bytes.clone()),
        Bson::ObjectId(value) => DataValue::String(value.to_hex()),
        Bson::DateTime(value) => DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis())
            .map_or_else(
                || DataValue::Timestamp(value.to_string()),
                |date| DataValue::Timestamp(date.to_rfc3339()),
            ),
        Bson::Symbol(value) => DataValue::String(value.clone()),
        Bson::DbPointer(value) => DataValue::String(format!("{value:?}")),
        Bson::Undefined => DataValue::Null,
        Bson::MaxKey | Bson::MinKey => DataValue::String(value.to_string()),
        Bson::Decimal128(value) => DataValue::Decimal(value.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_nested_bson_without_losing_binary_and_object_id() {
        let document = doc! {
            "_id": mongodb::bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
            "active": true,
            "nested": { "count": 3_i32 },
            "payload": mongodb::bson::Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] },
        };
        let row = document_to_row(&document);
        assert_eq!(
            row.get("_id"),
            Some(&DataValue::String("507f1f77bcf86cd799439011".into()))
        );
        assert_eq!(row.get("active"), Some(&DataValue::Boolean(true)));
        assert!(matches!(row.get("nested"), Some(DataValue::Map(_))));
        assert_eq!(row.get("payload"), Some(&DataValue::Bytes(vec![1, 2, 3])));
    }

    #[test]
    fn filters_mongodb_namespaces_with_debezium_style_regexes() {
        let includes = vec![Regex::new(r"^app\.orders$").unwrap()];
        let excludes = vec![Regex::new(r"_internal$").unwrap()];
        assert!(table_selected("app.orders", &includes, &excludes));
        assert!(!table_selected("app.customers", &includes, &excludes));
        assert!(!table_selected("app.orders_internal", &includes, &excludes));
    }
}
