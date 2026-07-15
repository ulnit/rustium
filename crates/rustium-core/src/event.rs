use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub type Row = IndexMap<String, DataValue>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStateEnvelope {
    pub format: String,
    pub version: u32,
    pub payload: serde_json::Value,
}

impl ConnectorStateEnvelope {
    #[must_use]
    pub fn new(format: impl Into<String>, version: u32, payload: serde_json::Value) -> Self {
        Self {
            format: format.into(),
            version,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum DataValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Time(String),
    Timestamp(String),
    Uuid(Uuid),
    Json(serde_json::Value),
    Array(Vec<DataValue>),
    Map(BTreeMap<String, DataValue>),
    Unavailable,
}

impl DataValue {
    #[must_use]
    pub fn to_json(&self, unavailable_value: &str) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Boolean(value) => (*value).into(),
            Self::Int32(value) => (*value).into(),
            Self::Int64(value) => (*value).into(),
            Self::UInt64(value) => (*value).into(),
            Self::Float64(value) => serde_json::Number::from_f64(*value)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            Self::Decimal(value)
            | Self::String(value)
            | Self::Date(value)
            | Self::Time(value)
            | Self::Timestamp(value) => value.clone().into(),
            Self::Bytes(value) => hex::encode(value).into(),
            Self::Uuid(value) => value.to_string().into(),
            Self::Json(value) => value.clone(),
            Self::Array(values) => values
                .iter()
                .map(|value| value.to_json(unavailable_value))
                .collect::<Vec<_>>()
                .into(),
            Self::Map(values) => values
                .iter()
                .map(|(key, value)| (key.clone(), value.to_json(unavailable_value)))
                .collect::<serde_json::Map<_, _>>()
                .into(),
            Self::Unavailable => unavailable_value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorIdentity {
    pub name: String,
    pub generation: Uuid,
}

impl ConnectorIdentity {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            generation: Uuid::new_v4(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourcePosition {
    Postgres(PostgresPosition),
    MySql(MySqlPosition),
    SqlServer(SqlServerPosition),
}

impl SourcePosition {
    #[must_use]
    pub fn is_after(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Postgres(left), Self::Postgres(right)) => left.sort_key() > right.sort_key(),
            (Self::MySql(left), Self::MySql(right)) => left.sort_key() > right.sort_key(),
            (Self::SqlServer(left), Self::SqlServer(right)) => left.sort_key() > right.sort_key(),
            _ => false,
        }
    }

    #[must_use]
    pub fn is_at_or_before(&self, other: &Self) -> bool {
        self == other || other.is_after(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresPosition {
    pub lsn: u64,
    pub commit_lsn: Option<u64>,
    pub transaction_id: Option<u32>,
    pub event_serial: u64,
    pub snapshot: bool,
}

impl PostgresPosition {
    fn sort_key(&self) -> (u64, u8, u64) {
        (self.lsn, u8::from(!self.snapshot), self.event_serial)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlPosition {
    pub binlog_filename: String,
    pub binlog_position: u64,
    pub gtid_set: Option<String>,
    pub server_id: u32,
    pub event_serial: u64,
    pub snapshot: bool,
}

impl MySqlPosition {
    fn sort_key(&self) -> (&str, u64, u8, u64) {
        (
            &self.binlog_filename,
            self.binlog_position,
            u8::from(!self.snapshot),
            self.event_serial,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlServerPosition {
    pub database: String,
    pub commit_lsn: String,
    pub change_lsn: String,
    pub event_serial: u64,
    pub snapshot: bool,
}

impl SqlServerPosition {
    fn sort_key(&self) -> (&str, &str, &str, u8, u64) {
        (
            &self.database,
            &self.commit_lsn,
            &self.change_lsn,
            u8::from(!self.snapshot),
            self.event_serial,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Read,
    Create,
    Update,
    Delete,
    Truncate,
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub String);

impl EventId {
    #[must_use]
    pub fn deterministic(
        connector_name: &str,
        partition: &str,
        position: &SourcePosition,
        collection: &str,
        ordinal: u64,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(connector_name.as_bytes());
        hasher.update([0]);
        hasher.update(partition.as_bytes());
        hasher.update([0]);
        hasher.update(serde_json::to_vec(position).expect("position serialization is infallible"));
        hasher.update([0]);
        hasher.update(collection.as_bytes());
        hasher.update(ordinal.to_be_bytes());
        Self(hex::encode(hasher.finalize()))
    }

    #[must_use]
    pub fn derived(&self, discriminator: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(self.0.as_bytes());
        hasher.update([0]);
        hasher.update(discriminator.as_bytes());
        Self(hex::encode(hasher.finalize()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub connector: String,
    pub connector_name: String,
    pub database: String,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub snapshot: bool,
    pub version: String,
    pub attributes: BTreeMap<String, serde_json::Value>,
}

impl SourceMetadata {
    #[must_use]
    pub fn collection(&self) -> String {
        match (&self.schema, &self.table) {
            (Some(schema), Some(table)) => format!("{}.{}.{}", self.database, schema, table),
            _ => self.database.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub id: String,
    pub total_order: Option<u64>,
    pub collection_order: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub type_name: String,
    pub optional: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSchema {
    pub name: String,
    pub version: u32,
    pub fields: Vec<FieldSchema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub id: EventId,
    pub source: SourceMetadata,
    pub position: SourcePosition,
    pub transaction: Option<TransactionMetadata>,
    pub operation: Operation,
    pub before: Option<Row>,
    pub after: Option<Row>,
    pub schema: EventSchema,
    pub source_time: Option<DateTime<Utc>>,
    pub observed_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordBoundary {
    Data,
    TransactionCommit,
    SnapshotComplete,
    Heartbeat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceRecord {
    pub event: Option<ChangeEvent>,
    pub position: SourcePosition,
    pub boundary: RecordBoundary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_state: Option<ConnectorStateEnvelope>,
}

impl SourceRecord {
    #[must_use]
    pub fn data(event: ChangeEvent) -> Self {
        Self {
            position: event.position.clone(),
            event: Some(event),
            boundary: RecordBoundary::Data,
            connector_state: None,
        }
    }

    #[must_use]
    pub fn with_connector_state(mut self, connector_state: ConnectorStateEnvelope) -> Self {
        self.connector_state = Some(connector_state);
        self
    }
}

#[derive(Debug, Clone)]
pub struct EncodedEvent {
    pub id: EventId,
    pub destination: String,
    pub key: Option<Bytes>,
    pub payload: Option<Bytes>,
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DeliveryBatch {
    pub events: Vec<EncodedEvent>,
    pub highest_position: SourcePosition,
}
