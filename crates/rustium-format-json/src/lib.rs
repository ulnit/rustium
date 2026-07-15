//! JSON encoders for Rustium's native and Debezium compatibility modes.

use std::collections::BTreeMap;

use bytes::Bytes;
use rustium_core::{
    ChangeEvent, EncodedEvent, Error, EventEncoder, Operation, Result, Row, SourcePosition,
};

#[derive(Debug, Clone)]
pub struct JsonEncoderConfig {
    pub topic_prefix: String,
    pub unavailable_value: String,
    pub tombstones_on_delete: bool,
    pub heartbeat_topics_prefix: String,
    pub heartbeat_topic_name: Option<String>,
}

pub struct RustiumJsonEncoder {
    config: JsonEncoderConfig,
}

impl RustiumJsonEncoder {
    #[must_use]
    pub fn new(config: JsonEncoderConfig) -> Self {
        Self { config }
    }
}

impl EventEncoder for RustiumJsonEncoder {
    fn content_type(&self) -> &'static str {
        "application/vnd.rustium.change+json;version=1"
    }

    fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        let payload = serde_json::json!({
            "specversion": "1.0",
            "id": event.id.0,
            "source": event.source,
            "position": event.position,
            "transaction": event.transaction,
            "op": operation_name(event.operation),
            "before": event.before.as_ref().map(|row| row_to_json(row, &self.config.unavailable_value)),
            "after": event.after.as_ref().map(|row| row_to_json(row, &self.config.unavailable_value)),
            "schema": event.schema,
            "source_time": event.source_time,
            "observed_time": event.observed_time,
        });
        build_encoded(event, &self.config, payload)
    }
}

pub struct DebeziumJsonEncoder {
    config: JsonEncoderConfig,
}

impl DebeziumJsonEncoder {
    #[must_use]
    pub fn new(config: JsonEncoderConfig) -> Self {
        Self { config }
    }
}

impl EventEncoder for DebeziumJsonEncoder {
    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        if is_heartbeat(event) {
            return build_encoded(
                event,
                &self.config,
                serde_json::json!({"ts_ms": event.observed_time.timestamp_millis()}),
            );
        }
        let source_ts = event.source_time.map(|time| time.timestamp_millis());
        let source = debezium_source(event, source_ts)?;
        let payload = serde_json::json!({
            "before": event.before.as_ref().map(|row| row_to_json(row, &self.config.unavailable_value)),
            "after": event.after.as_ref().map(|row| row_to_json(row, &self.config.unavailable_value)),
            "source": source,
            "op": operation_code(event.operation),
            "ts_ms": event.observed_time.timestamp_millis(),
            "transaction": event.transaction.as_ref().map(|transaction| serde_json::json!({
                "id": transaction.id,
                "total_order": transaction.total_order,
                "data_collection_order": transaction.collection_order,
            })),
        });
        build_encoded(event, &self.config, payload)
    }

    fn encode_batch(&self, event: &ChangeEvent) -> Result<Vec<EncodedEvent>> {
        let encoded = self.encode(event)?;
        if event.operation != Operation::Delete || !self.config.tombstones_on_delete {
            return Ok(vec![encoded]);
        }

        let tombstone_id = event.id.derived("debezium-tombstone");
        let mut tombstone = EncodedEvent {
            id: tombstone_id.clone(),
            destination: encoded.destination.clone(),
            key: encoded.key.clone(),
            payload: None,
            headers: encoded.headers.clone(),
        };
        tombstone
            .headers
            .insert("rustium.event.id".into(), tombstone_id.0);
        tombstone
            .headers
            .insert("rustium.record.type".into(), "tombstone".into());
        Ok(vec![encoded, tombstone])
    }
}

fn build_encoded(
    event: &ChangeEvent,
    config: &JsonEncoderConfig,
    payload: serde_json::Value,
) -> Result<EncodedEvent> {
    let heartbeat = is_heartbeat(event);
    let destination = if heartbeat {
        config.heartbeat_topic_name.clone().unwrap_or_else(|| {
            format!("{}.{}", config.heartbeat_topics_prefix, config.topic_prefix)
        })
    } else {
        let table = event
            .source
            .table
            .as_deref()
            .ok_or_else(|| Error::Encoding("event has no source table".into()))?;
        if event.source.connector == "mysql" {
            format!(
                "{}.{}.{}",
                config.topic_prefix, event.source.database, table
            )
        } else {
            let schema = event
                .source
                .schema
                .as_deref()
                .ok_or_else(|| Error::Encoding("event has no source schema".into()))?;
            format!(
                "{}.{}.{}.{}",
                config.topic_prefix, event.source.database, schema, table
            )
        }
    };
    let key = if heartbeat {
        Some(Bytes::from(serde_json::to_vec(&serde_json::json!({
            "serverName": event.source.connector_name,
        }))?))
    } else {
        event
            .after
            .as_ref()
            .or(event.before.as_ref())
            .and_then(|row| event_key(row, event, &config.unavailable_value))
            .map(|value| Bytes::from(serde_json::to_vec(&value).expect("JSON key cannot fail")))
    };
    let mut headers = BTreeMap::new();
    headers.insert("rustium.event.id".into(), event.id.0.clone());
    headers.insert("rustium.content.type".into(), "application/json".into());
    if heartbeat {
        headers.insert("rustium.record.type".into(), "heartbeat".into());
    }
    Ok(EncodedEvent {
        id: event.id.clone(),
        destination,
        key,
        payload: Some(Bytes::from(serde_json::to_vec(&payload)?)),
        headers,
    })
}

fn is_heartbeat(event: &ChangeEvent) -> bool {
    event
        .source
        .attributes
        .get("rustium.heartbeat")
        .is_some_and(|value| value == &serde_json::Value::Bool(true))
}

fn debezium_source(event: &ChangeEvent, source_ts: Option<i64>) -> Result<serde_json::Value> {
    let common = serde_json::json!({
        "version": event.source.version,
        "connector": event.source.connector,
        "name": event.source.connector_name,
        "ts_ms": source_ts,
        "snapshot": snapshot_marker(event),
        "db": event.source.database,
        "schema": event.source.schema,
        "table": event.source.table,
    });
    let mut source = common
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Encoding("source metadata is not an object".into()))?;
    match &event.position {
        SourcePosition::Postgres(position) => {
            source.insert(
                "sequence".into(),
                serde_json::to_string(&[
                    position.commit_lsn.unwrap_or(position.lsn).to_string(),
                    position.lsn.to_string(),
                ])?
                .into(),
            );
            source.insert("txId".into(), position.transaction_id.into());
            source.insert("lsn".into(), position.lsn.into());
        }
        SourcePosition::MySql(position) => {
            source.insert("server_id".into(), position.server_id.into());
            source.insert("gtid".into(), position.gtid_set.clone().into());
            source.insert("file".into(), position.binlog_filename.clone().into());
            source.insert("pos".into(), position.binlog_position.into());
            source.insert("row".into(), position.event_serial.into());
            source.insert("thread".into(), serde_json::Value::Null);
            source.insert("query".into(), serde_json::Value::Null);
        }
        SourcePosition::SqlServer(position) => {
            source.insert("change_lsn".into(), position.change_lsn.clone().into());
            source.insert("commit_lsn".into(), position.commit_lsn.clone().into());
            source.insert("event_serial_no".into(), position.event_serial.into());
        }
    }
    Ok(source.into())
}

fn snapshot_marker(event: &ChangeEvent) -> &'static str {
    if event
        .source
        .attributes
        .get("rustium.snapshot.kind")
        .and_then(serde_json::Value::as_str)
        == Some("incremental")
    {
        "incremental"
    } else if event.source.snapshot {
        "true"
    } else {
        "false"
    }
}

fn event_key(row: &Row, event: &ChangeEvent, unavailable_value: &str) -> Option<serde_json::Value> {
    let key_fields = event.schema.fields.iter().filter(|field| field.primary_key);
    let mut key = serde_json::Map::new();
    for field in key_fields {
        if let Some(value) = row.get(&field.name) {
            key.insert(field.name.clone(), value.to_json(unavailable_value));
        }
    }
    (!key.is_empty()).then_some(key.into())
}

fn row_to_json(row: &Row, unavailable_value: &str) -> serde_json::Value {
    row.iter()
        .map(|(name, value)| (name.clone(), value.to_json(unavailable_value)))
        .collect::<serde_json::Map<_, _>>()
        .into()
}

const fn operation_code(operation: Operation) -> &'static str {
    match operation {
        Operation::Read => "r",
        Operation::Create => "c",
        Operation::Update => "u",
        Operation::Delete => "d",
        Operation::Truncate => "t",
        Operation::Message => "m",
    }
}

const fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Read => "read",
        Operation::Create => "create",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Truncate => "truncate",
        Operation::Message => "message",
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use indexmap::indexmap;
    use rustium_core::{
        DataValue, EventId, EventSchema, FieldSchema, PostgresPosition, SourceMetadata,
        TransactionMetadata,
    };

    use super::*;

    fn event() -> ChangeEvent {
        ChangeEvent {
            id: EventId("event-1".into()),
            source: SourceMetadata {
                connector: "postgresql".into(),
                connector_name: "orders".into(),
                database: "app".into(),
                schema: Some("public".into()),
                table: Some("customers".into()),
                snapshot: false,
                version: "0.1.0".into(),
                attributes: BTreeMap::new(),
            },
            position: SourcePosition::Postgres(PostgresPosition {
                lsn: 42,
                commit_lsn: Some(44),
                transaction_id: Some(7),
                event_serial: 1,
                snapshot: false,
            }),
            transaction: Some(TransactionMetadata {
                id: "7".into(),
                total_order: Some(1),
                collection_order: Some(1),
            }),
            operation: Operation::Create,
            before: None,
            after: Some(indexmap! {
                "id".into() => DataValue::Int64(1),
                "name".into() => DataValue::String("Alice".into()),
            }),
            schema: EventSchema {
                name: "orders.app.public.customers.Envelope".into(),
                version: 1,
                fields: vec![FieldSchema {
                    name: "id".into(),
                    type_name: "int8".into(),
                    optional: false,
                    primary_key: true,
                }],
            },
            source_time: Some(Utc::now()),
            observed_time: Utc::now(),
        }
    }

    #[test]
    fn emits_debezium_envelope_and_key() {
        let encoded = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode(&event())
        .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(encoded.payload.as_ref().unwrap()).unwrap();
        assert_eq!(encoded.destination, "app.app.public.customers");
        assert_eq!(value["op"], "c");
        assert_eq!(value["after"]["name"], "Alice");
        assert!(encoded.key.is_some());
    }

    #[test]
    fn emits_incremental_snapshot_marker() {
        let mut event = event();
        event.operation = Operation::Read;
        event.source.snapshot = true;
        event
            .source
            .attributes
            .insert("rustium.snapshot.kind".into(), "incremental".into());
        let encoded = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode(&event)
        .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(encoded.payload.as_ref().unwrap()).unwrap();
        assert_eq!(value["op"], "r");
        assert_eq!(value["source"]["snapshot"], "incremental");
    }

    #[test]
    fn emits_delete_envelope_and_null_tombstone() {
        let mut event = event();
        event.operation = Operation::Delete;
        event.before = event.after.take();

        let encoded = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode_batch(&event)
        .unwrap();

        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].destination, encoded[1].destination);
        assert_eq!(encoded[0].key, encoded[1].key);
        assert!(encoded[0].payload.is_some());
        assert!(encoded[1].payload.is_none());
        assert_ne!(encoded[0].id, encoded[1].id);
        assert_eq!(
            encoded[1].headers.get("rustium.record.type"),
            Some(&"tombstone".into())
        );
    }

    #[test]
    fn can_disable_delete_tombstones() {
        let mut event = event();
        event.operation = Operation::Delete;
        event.before = event.after.take();

        let encoded = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: false,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode_batch(&event)
        .unwrap();

        assert_eq!(encoded.len(), 1);
        assert!(encoded[0].payload.is_some());
    }

    #[test]
    fn emits_debezium_heartbeat_key_topic_and_value() {
        let mut event = event();
        event.operation = Operation::Message;
        event.before = None;
        event.after = None;
        event.source.table = None;
        event.source.schema = None;
        event
            .source
            .attributes
            .insert("rustium.heartbeat".into(), true.into());

        let encoded = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "inventory".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode(&event)
        .unwrap();

        let key: serde_json::Value = serde_json::from_slice(encoded.key.as_ref().unwrap()).unwrap();
        let payload: serde_json::Value =
            serde_json::from_slice(encoded.payload.as_ref().unwrap()).unwrap();
        assert_eq!(encoded.destination, "__debezium-heartbeat.inventory");
        assert_eq!(key, serde_json::json!({"serverName": "orders"}));
        assert_eq!(payload["ts_ms"], event.observed_time.timestamp_millis());
        assert_eq!(
            encoded
                .headers
                .get("rustium.record.type")
                .map(String::as_str),
            Some("heartbeat")
        );

        let overridden = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "inventory".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: Some("shared-heartbeat".into()),
        })
        .encode(&event)
        .unwrap();
        assert_eq!(overridden.destination, "shared-heartbeat");
    }
}
