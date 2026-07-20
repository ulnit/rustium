//! JSON encoders for Rustium's native and Debezium compatibility modes.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rustium_core::{
    ChangeEvent, DataValue, EncodedEvent, Error, EventEncoder, FieldSchema, Operation, Result, Row,
    SourcePosition, WireSchema, WireSchemaType,
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

pub struct DebeziumJsonSchemaEncoder {
    inner: DebeziumJsonEncoder,
}

impl DebeziumJsonSchemaEncoder {
    #[must_use]
    pub fn new(config: JsonEncoderConfig) -> Self {
        Self {
            inner: DebeziumJsonEncoder::new(config),
        }
    }

    fn attach_schemas(&self, event: &ChangeEvent, records: &mut [EncodedEvent]) -> Result<()> {
        let Some(first) = records.first() else {
            return Ok(());
        };
        let key_schema = first
            .key
            .is_some()
            .then(|| json_key_schema(event, &first.destination, &self.inner.config))
            .transpose()?;
        let payload_schema = json_payload_schema(event, &first.destination, &self.inner.config)?;
        for record in records {
            if record.key.is_some() {
                record.key_schema.clone_from(&key_schema);
            }
            if record.payload.is_some() {
                record.payload_schema = Some(payload_schema.clone());
            }
            record.headers.insert(
                "rustium.content.type".into(),
                "application/json; framing=confluent-schema-registry".into(),
            );
        }
        Ok(())
    }
}

impl EventEncoder for DebeziumJsonSchemaEncoder {
    fn content_type(&self) -> &'static str {
        "application/json; framing=confluent-schema-registry"
    }

    fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        let mut records = vec![self.inner.encode(event)?];
        self.attach_schemas(event, &mut records)?;
        Ok(records.remove(0))
    }

    fn encode_batch(&self, event: &ChangeEvent) -> Result<Vec<EncodedEvent>> {
        let mut records = self.inner.encode_batch(event)?;
        self.attach_schemas(event, &mut records)?;
        Ok(records)
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
        let transaction = event.transaction.as_ref().map(|transaction| {
            serde_json::json!({
                "id": transaction.id,
                "total_order": transaction.total_order,
                "data_collection_order": transaction.collection_order,
            })
        });
        let payload = if is_logical_decoding_message(event) {
            let message = event.after.as_ref().ok_or_else(|| {
                Error::Encoding("PostgreSQL logical decoding message has no content".into())
            })?;
            serde_json::json!({
                "source": source,
                "op": operation_code(event.operation),
                "ts_ms": event.observed_time.timestamp_millis(),
                "transaction": transaction,
                "message": logical_message_to_json(message, &self.config.unavailable_value),
            })
        } else {
            serde_json::json!({
                "before": event.before.as_ref().map(|row| row_to_json(row, &self.config.unavailable_value)),
                "after": event.after.as_ref().map(|row| row_to_json(row, &self.config.unavailable_value)),
                "source": source,
                "op": operation_code(event.operation),
                "ts_ms": event.observed_time.timestamp_millis(),
                "transaction": transaction,
            })
        };
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
            key_schema: encoded.key_schema.clone(),
            payload: None,
            payload_schema: None,
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
    let logical_message = is_logical_decoding_message(event);
    let destination = if heartbeat {
        config.heartbeat_topic_name.clone().unwrap_or_else(|| {
            format!("{}.{}", config.heartbeat_topics_prefix, config.topic_prefix)
        })
    } else if logical_message {
        format!("{}.message", config.topic_prefix)
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
    } else if logical_message {
        headers.insert(
            "rustium.record.type".into(),
            "logical-decoding-message".into(),
        );
    }
    Ok(EncodedEvent {
        id: event.id.clone(),
        destination,
        key,
        key_schema: None,
        payload: Some(Bytes::from(serde_json::to_vec(&payload)?)),
        payload_schema: None,
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

fn is_logical_decoding_message(event: &ChangeEvent) -> bool {
    event
        .source
        .attributes
        .get("rustium.logical_decoding_message")
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
            source.insert("xmin".into(), position.xmin.into());
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
        SourcePosition::Oracle(position) => {
            source.insert("scn".into(), position.scn.into());
            source.insert("commit_scn".into(), position.commit_scn.into());
            source.insert("txId".into(), position.transaction_id.clone().into());
            source.insert("rs_id".into(), position.rs_id.clone().into());
            source.insert("ssn".into(), position.ssn.into());
            source.insert("event_serial_no".into(), position.event_serial.into());
        }
        SourcePosition::MongoDb(position) => {
            source.insert("resume_token".into(), position.resume_token.clone().into());
            source.insert(
                "cluster_time_seconds".into(),
                position.cluster_time_seconds.map(u64::from).into(),
            );
            source.insert(
                "cluster_time_increment".into(),
                position.cluster_time_increment.map(u64::from).into(),
            );
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

fn json_source_schema(event: &ChangeEvent) -> serde_json::Value {
    let mut properties = serde_json::Map::from_iter([
        ("version".into(), serde_json::json!({"type": "string"})),
        ("connector".into(), serde_json::json!({"type": "string"})),
        ("name".into(), serde_json::json!({"type": "string"})),
        (
            "ts_ms".into(),
            serde_json::json!({"type": ["integer", "null"]}),
        ),
        ("snapshot".into(), serde_json::json!({"type": "string"})),
        ("db".into(), serde_json::json!({"type": "string"})),
        (
            "schema".into(),
            serde_json::json!({"type": ["string", "null"]}),
        ),
        (
            "table".into(),
            serde_json::json!({"type": ["string", "null"]}),
        ),
    ]);
    let mut required = vec![
        "version",
        "connector",
        "name",
        "ts_ms",
        "snapshot",
        "db",
        "schema",
        "table",
    ];
    let connector_fields: Vec<(&str, serde_json::Value)> = match &event.position {
        SourcePosition::Postgres(_) => vec![
            ("sequence", serde_json::json!({"type": "string"})),
            ("txId", serde_json::json!({"type": ["integer", "null"]})),
            ("lsn", serde_json::json!({"type": "integer"})),
            ("xmin", serde_json::json!({"type": ["integer", "null"]})),
        ],
        SourcePosition::MySql(_) => vec![
            ("server_id", serde_json::json!({"type": "integer"})),
            ("gtid", serde_json::json!({"type": ["string", "null"]})),
            ("file", serde_json::json!({"type": "string"})),
            ("pos", serde_json::json!({"type": "integer"})),
            ("row", serde_json::json!({"type": "integer"})),
            ("thread", serde_json::json!({"type": ["integer", "null"]})),
            ("query", serde_json::json!({"type": ["string", "null"]})),
        ],
        SourcePosition::SqlServer(_) => vec![
            ("change_lsn", serde_json::json!({"type": "string"})),
            ("commit_lsn", serde_json::json!({"type": "string"})),
            ("event_serial_no", serde_json::json!({"type": "integer"})),
        ],
        SourcePosition::Oracle(_) => vec![
            ("scn", serde_json::json!({"type": "integer"})),
            (
                "commit_scn",
                serde_json::json!({"type": ["integer", "null"]}),
            ),
            ("txId", serde_json::json!({"type": ["string", "null"]})),
            ("rs_id", serde_json::json!({"type": ["string", "null"]})),
            ("ssn", serde_json::json!({"type": ["integer", "null"]})),
            ("event_serial_no", serde_json::json!({"type": "integer"})),
        ],
        SourcePosition::MongoDb(_) => vec![
            (
                "resume_token",
                serde_json::json!({"type": ["string", "null"]}),
            ),
            (
                "cluster_time_seconds",
                serde_json::json!({"type": ["integer", "null"]}),
            ),
            (
                "cluster_time_increment",
                serde_json::json!({"type": ["integer", "null"]}),
            ),
            ("event_serial_no", serde_json::json!({"type": "integer"})),
        ],
    };
    for (name, schema) in connector_fields {
        properties.insert(name.into(), schema);
        required.push(name);
    }
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required,
    })
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

fn logical_message_to_json(row: &Row, unavailable_value: &str) -> serde_json::Value {
    row.iter()
        .map(|(name, value)| {
            let value = match value {
                DataValue::Bytes(value) => STANDARD.encode(value).into(),
                value => value.to_json(unavailable_value),
            };
            (name.clone(), value)
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

fn json_key_schema(
    event: &ChangeEvent,
    destination: &str,
    config: &JsonEncoderConfig,
) -> Result<WireSchema> {
    let definition = if is_heartbeat(event) {
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": format!("{}.HeartbeatKey", event.source.connector_name),
            "type": "object",
            "additionalProperties": false,
            "properties": {"serverName": {"type": "string"}},
            "required": ["serverName"],
        })
    } else {
        let key_fields = event
            .schema
            .fields
            .iter()
            .filter(|field| field.primary_key)
            .collect::<Vec<_>>();
        let properties = key_fields
            .iter()
            .map(|field| {
                (
                    field.name.clone(),
                    json_field_schema(field, &config.unavailable_value, true),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let required = key_fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema_title(event, "Key"),
            "type": "object",
            "additionalProperties": false,
            "properties": properties,
            "required": required,
        })
    };
    wire_schema(format!("{destination}-key"), definition)
}

fn json_payload_schema(
    event: &ChangeEvent,
    destination: &str,
    config: &JsonEncoderConfig,
) -> Result<WireSchema> {
    let source_schema = json_source_schema(event);
    let definition = if is_heartbeat(event) {
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": format!("{}.Heartbeat", event.source.connector_name),
            "type": "object",
            "additionalProperties": false,
            "properties": {"ts_ms": {"type": "integer"}},
            "required": ["ts_ms"],
        })
    } else if is_logical_decoding_message(event) {
        let message_schema = json_row_schema(event, config);
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema_title(event, "Envelope"),
            "$comment": format!("Rustium EventSchema version {}", event.schema.version),
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "source": source_schema.clone(),
                "op": {"type": "string", "enum": ["m"]},
                "ts_ms": {"type": "integer"},
                "transaction": {
                    "anyOf": [
                        {"type": "null"},
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "id": {"type": "string"},
                                "total_order": {"type": ["integer", "null"]},
                                "data_collection_order": {"type": ["integer", "null"]}
                            },
                            "required": ["id", "total_order", "data_collection_order"]
                        }
                    ]
                },
                "message": message_schema
            },
            "required": ["source", "op", "ts_ms", "transaction", "message"]
        })
    } else {
        let row_schema = json_row_schema(event, config);
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema_title(event, "Envelope"),
            "$comment": format!("Rustium EventSchema version {}", event.schema.version),
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "before": {"anyOf": [{"type": "null"}, row_schema.clone()]},
                "after": {"anyOf": [{"type": "null"}, row_schema]},
                "source": source_schema,
                "op": {"type": "string", "enum": ["r", "c", "u", "d", "t", "m"]},
                "ts_ms": {"type": "integer"},
                "transaction": {
                    "anyOf": [
                        {"type": "null"},
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "id": {"type": "string"},
                                "total_order": {"type": ["integer", "null"]},
                                "data_collection_order": {"type": ["integer", "null"]}
                            },
                            "required": ["id", "total_order", "data_collection_order"]
                        }
                    ]
                }
            },
            "required": ["before", "after", "source", "op", "ts_ms", "transaction"]
        })
    };
    wire_schema(format!("{destination}-value"), definition)
}

fn json_row_schema(event: &ChangeEvent, config: &JsonEncoderConfig) -> serde_json::Value {
    let properties = event
        .schema
        .fields
        .iter()
        .map(|field| {
            (
                field.name.clone(),
                json_field_schema(field, &config.unavailable_value, false),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({
        "title": schema_title(event, "Value"),
        "type": "object",
        "additionalProperties": false,
        "properties": properties
    })
}

fn json_field_schema(field: &FieldSchema, unavailable_value: &str, key: bool) -> serde_json::Value {
    let mut variants = vec![json_base_type(&field.type_name)];
    if !key {
        variants.push(serde_json::json!({
            "type": "string",
            "const": unavailable_value
        }));
    }
    if field.optional {
        variants.push(serde_json::json!({"type": "null"}));
    }
    if variants.len() == 1 {
        variants.remove(0)
    } else {
        serde_json::json!({"anyOf": variants})
    }
}

fn json_base_type(type_name: &str) -> serde_json::Value {
    let normalized = type_name.trim().to_ascii_lowercase();
    if let Some(element) = normalized.strip_suffix("[]") {
        return serde_json::json!({
            "type": "array",
            "items": json_base_type(element)
        });
    }
    if normalized == "hstore" || normalized.starts_with("map") {
        return serde_json::json!({
            "type": "object",
            "additionalProperties": {"type": ["string", "null"]}
        });
    }
    if normalized == "json" || normalized == "jsonb" {
        return serde_json::json!({});
    }
    if normalized == "bool" || normalized == "boolean" || normalized == "bit" {
        return serde_json::json!({"type": "boolean"});
    }
    if normalized.contains("decimal")
        || normalized.contains("numeric")
        || normalized.contains("money")
    {
        return serde_json::json!({"type": "string"});
    }
    if [
        "tinyint",
        "smallint",
        "mediumint",
        "integer",
        "bigint",
        "serial",
        "year",
        "int2",
        "int4",
        "int8",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
        || normalized == "int"
    {
        return serde_json::json!({"type": "integer"});
    }
    if ["real", "float", "double"]
        .iter()
        .any(|prefix| normalized.starts_with(prefix))
    {
        return serde_json::json!({"type": ["number", "null"]});
    }
    serde_json::json!({"type": "string"})
}

fn schema_title(event: &ChangeEvent, suffix: &str) -> String {
    let base = event
        .schema
        .name
        .strip_suffix(".Envelope")
        .unwrap_or(&event.schema.name);
    format!("{base}.{suffix}")
}

fn wire_schema(subject: String, definition: serde_json::Value) -> Result<WireSchema> {
    Ok(WireSchema {
        subject,
        schema_type: WireSchemaType::Json,
        definition: serde_json::to_string(&definition)?,
    })
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
    use std::{fs, path::Path};

    use chrono::{TimeZone, Utc};
    use indexmap::indexmap;
    use rustium_core::{
        DataValue, EventId, EventSchema, FieldSchema, MySqlPosition, PostgresPosition,
        SourceMetadata, SqlServerPosition, TransactionMetadata,
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
                xmin: Some(9),
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
                "attributes".into() => DataValue::Map(BTreeMap::from([
                    ("region".into(), DataValue::String("global".into())),
                    ("optional".into(), DataValue::Null),
                ])),
            }),
            schema: EventSchema {
                name: "orders.app.public.customers.Envelope".into(),
                version: 1,
                fields: vec![
                    FieldSchema {
                        name: "id".into(),
                        type_name: "int8".into(),
                        optional: false,
                        primary_key: true,
                    },
                    FieldSchema {
                        name: "name".into(),
                        type_name: "text".into(),
                        optional: false,
                        primary_key: false,
                    },
                    FieldSchema {
                        name: "attributes".into(),
                        type_name: "hstore".into(),
                        optional: true,
                        primary_key: false,
                    },
                ],
            },
            source_time: Some(
                Utc.timestamp_millis_opt(1_700_000_000_123)
                    .single()
                    .unwrap(),
            ),
            observed_time: Utc
                .timestamp_millis_opt(1_700_000_001_456)
                .single()
                .unwrap(),
        }
    }

    fn logical_message_event() -> ChangeEvent {
        let mut event = event();
        event.source.connector = "postgresql".into();
        event.source.schema = Some(String::new());
        event.source.table = Some(String::new());
        event
            .source
            .attributes
            .insert("rustium.logical_decoding_message".into(), true.into());
        event.position = SourcePosition::Postgres(PostgresPosition {
            lsn: 64,
            commit_lsn: Some(64),
            transaction_id: None,
            xmin: None,
            event_serial: 1,
            snapshot: false,
        });
        event.transaction = None;
        event.operation = Operation::Message;
        event.before = None;
        event.after = Some(indexmap! {
            "prefix".into() => DataValue::String("orders.created".into()),
            "content".into() => DataValue::Bytes(b"payload".to_vec()),
        });
        event.schema = EventSchema {
            name: "orders.Message".into(),
            version: 1,
            fields: vec![
                FieldSchema {
                    name: "prefix".into(),
                    type_name: "text".into(),
                    optional: true,
                    primary_key: true,
                },
                FieldSchema {
                    name: "content".into(),
                    type_name: "bytea".into(),
                    optional: true,
                    primary_key: false,
                },
            ],
        };
        event
    }

    fn encoded_fixture(event: &ChangeEvent) -> serde_json::Value {
        let encoded = DebeziumJsonEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode(event)
        .unwrap();
        serde_json::json!({
            "destination": encoded.destination,
            "key": serde_json::from_slice::<serde_json::Value>(encoded.key.as_ref().unwrap()).unwrap(),
            "payload": serde_json::from_slice::<serde_json::Value>(encoded.payload.as_ref().unwrap()).unwrap(),
        })
    }

    fn golden_fixture(path: &str) -> serde_json::Value {
        serde_json::from_str(path).unwrap()
    }

    fn connector_events() -> [(&'static str, ChangeEvent); 3] {
        let postgres = event();

        let mut mysql = event();
        mysql.source.connector = "mysql".into();
        mysql.source.schema = None;
        mysql.schema.name = "orders.app.customers.Envelope".into();
        mysql.position = SourcePosition::MySql(MySqlPosition {
            binlog_filename: "mysql-bin.000123".into(),
            binlog_position: 4_567,
            gtid_set: Some("24bc7850-2c16-11ef-8f32-0242ac120002:1-9".into()),
            server_id: 184,
            event_serial: 2,
            snapshot: false,
        });

        let mut sqlserver = event();
        sqlserver.source.connector = "sqlserver".into();
        sqlserver.source.schema = Some("dbo".into());
        sqlserver.schema.name = "orders.app.dbo.customers.Envelope".into();
        sqlserver.position = SourcePosition::SqlServer(SqlServerPosition {
            database: "app".into(),
            commit_lsn: "0x0000002A000001000001".into(),
            change_lsn: "0x0000002A000001000002".into(),
            event_serial: 3,
            snapshot: false,
        });

        [
            ("postgresql", postgres),
            ("mysql", mysql),
            ("sqlserver", sqlserver),
        ]
    }

    fn schema_fixture(event: &ChangeEvent) -> serde_json::Value {
        let encoded = DebeziumJsonSchemaEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        })
        .encode(event)
        .unwrap();
        let key = encoded.key_schema.unwrap();
        let value = encoded.payload_schema.unwrap();
        serde_json::json!({
            "destination": encoded.destination,
            "key": {
                "subject": key.subject,
                "schema_type": format!("{:?}", key.schema_type),
                "definition": serde_json::from_str::<serde_json::Value>(&key.definition).unwrap(),
            },
            "value": {
                "subject": value.subject,
                "schema_type": format!("{:?}", value.schema_type),
                "definition": serde_json::from_str::<serde_json::Value>(&value.definition).unwrap(),
            },
        })
    }

    fn write_fixture(path: &Path, fixture: &serde_json::Value) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut contents = serde_json::to_string_pretty(fixture).unwrap();
        contents.push('\n');
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn matches_debezium_json_golden_fixtures_for_prioritized_connectors() {
        let [(_, postgres), (_, mysql), (_, sqlserver)] = connector_events();
        assert_eq!(
            encoded_fixture(&postgres),
            golden_fixture(include_str!(
                "../tests/fixtures/debezium-postgresql-create.json"
            ))
        );

        assert_eq!(
            encoded_fixture(&mysql),
            golden_fixture(include_str!("../tests/fixtures/debezium-mysql-create.json"))
        );

        assert_eq!(
            encoded_fixture(&sqlserver),
            golden_fixture(include_str!(
                "../tests/fixtures/debezium-sqlserver-create.json"
            ))
        );
    }

    #[test]
    fn matches_json_schema_golden_fixtures_for_prioritized_connectors() {
        let [(_, postgres), (_, mysql), (_, sqlserver)] = connector_events();
        assert_eq!(
            schema_fixture(&postgres),
            golden_fixture(include_str!(
                "../tests/fixtures/schema-postgresql-create.json"
            ))
        );
        assert_eq!(
            schema_fixture(&mysql),
            golden_fixture(include_str!("../tests/fixtures/schema-mysql-create.json"))
        );
        assert_eq!(
            schema_fixture(&sqlserver),
            golden_fixture(include_str!(
                "../tests/fixtures/schema-sqlserver-create.json"
            ))
        );
    }

    #[test]
    #[ignore = "regenerates checked-in JSON Schema golden fixtures"]
    fn regenerate_json_schema_golden_fixtures() {
        let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        for (connector, event) in connector_events() {
            write_fixture(
                &fixture_dir.join(format!("schema-{connector}-create.json")),
                &schema_fixture(&event),
            );
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
        assert_eq!(value["after"]["attributes"]["region"], "global");
        assert!(value["after"]["attributes"]["optional"].is_null());
        assert_eq!(value["source"]["xmin"], 9);
        assert!(encoded.key.is_some());
    }

    #[test]
    fn emits_postgresql_logical_decoding_message_contracts() {
        let config = JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        };
        let event = logical_message_event();
        let encoded = DebeziumJsonEncoder::new(config.clone())
            .encode(&event)
            .unwrap();
        assert_eq!(encoded.destination, "app.message");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(encoded.key.as_ref().unwrap()).unwrap(),
            serde_json::json!({"prefix": "orders.created"})
        );
        let payload =
            serde_json::from_slice::<serde_json::Value>(encoded.payload.as_ref().unwrap()).unwrap();
        assert_eq!(payload["op"], "m");
        assert_eq!(payload["source"]["schema"], "");
        assert_eq!(payload["source"]["table"], "");
        assert_eq!(payload["message"]["prefix"], "orders.created");
        assert_eq!(payload["message"]["content"], "cGF5bG9hZA==");
        assert!(payload.get("before").is_none());
        assert!(payload.get("after").is_none());

        let schema_encoded = DebeziumJsonSchemaEncoder::new(config)
            .encode(&event)
            .unwrap();
        let definition: serde_json::Value =
            serde_json::from_str(&schema_encoded.payload_schema.unwrap().definition).unwrap();
        assert!(definition["properties"]["message"].is_object());
        assert!(definition["properties"].get("before").is_none());
        assert_eq!(schema_encoded.destination, "app.message");
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

    #[test]
    fn emits_json_schema_registry_descriptors_and_tombstones() {
        let encoder = DebeziumJsonSchemaEncoder::new(JsonEncoderConfig {
            topic_prefix: "app".into(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        });
        let encoded = encoder.encode(&event()).unwrap();
        let key_schema = encoded.key_schema.as_ref().unwrap();
        let payload_schema = encoded.payload_schema.as_ref().unwrap();
        assert_eq!(key_schema.subject, "app.app.public.customers-key");
        assert_eq!(payload_schema.subject, "app.app.public.customers-value");
        assert_eq!(key_schema.schema_type, WireSchemaType::Json);
        let key: serde_json::Value = serde_json::from_str(&key_schema.definition).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload_schema.definition).unwrap();
        assert_eq!(key["required"], serde_json::json!(["id"]));
        assert_eq!(key["properties"]["id"]["type"], "integer");
        assert_eq!(
            payload["properties"]["after"]["anyOf"][1]["properties"]["attributes"]["anyOf"][0]["type"],
            "object"
        );

        let mut changed = event();
        changed.schema.version = 2;
        let changed = encoder.encode(&changed).unwrap();
        assert_ne!(
            payload_schema.definition,
            changed.payload_schema.unwrap().definition
        );

        let mut deleted = event();
        deleted.operation = Operation::Delete;
        deleted.before = deleted.after.take();
        let deleted = encoder.encode_batch(&deleted).unwrap();
        assert_eq!(deleted.len(), 2);
        assert_eq!(deleted[0].key_schema, deleted[1].key_schema);
        assert!(deleted[0].payload_schema.is_some());
        assert!(deleted[1].payload_schema.is_none());
    }
}
