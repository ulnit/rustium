//! Debezium-compatible Apache Avro encoding for Rustium change events.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use apache_avro::{Schema, to_avro_datum, types::Value};
use bytes::Bytes;
use rustium_core::{
    ChangeEvent, DataValue, EncodedEvent, Error, EventEncoder, FieldSchema, Operation, Result, Row,
    SourcePosition, WireSchema, WireSchemaType,
};

#[derive(Debug, Clone)]
pub struct AvroEncoderConfig {
    pub topic_prefix: String,
    pub unavailable_value: String,
    pub tombstones_on_delete: bool,
    pub heartbeat_topics_prefix: String,
    pub heartbeat_topic_name: Option<String>,
    pub schema_cache_capacity: usize,
}

pub struct DebeziumAvroEncoder {
    config: AvroEncoderConfig,
    schemas: Mutex<ParsedSchemaCache>,
}

struct ParsedSchemaCache {
    schemas: HashMap<String, Arc<Schema>>,
    order: VecDeque<String>,
    capacity: usize,
}

struct AdjustedField<'a> {
    source: &'a FieldSchema,
    name: String,
}

#[derive(Debug, Clone)]
enum AvroKind {
    Boolean,
    Int,
    Long,
    Double,
    Bytes,
    String,
    Array(Box<Self>),
    Map,
}

impl DebeziumAvroEncoder {
    pub fn new(config: AvroEncoderConfig) -> Result<Self> {
        if config.schema_cache_capacity == 0 {
            return Err(Error::Configuration(
                "Avro schema cache capacity must be greater than zero".into(),
            ));
        }
        let capacity = config.schema_cache_capacity;
        Ok(Self {
            config,
            schemas: Mutex::new(ParsedSchemaCache::new(capacity)),
        })
    }

    fn encode_datum(&self, definition: &str, value: Value) -> Result<Bytes> {
        let schema = self
            .schemas
            .lock()
            .map_err(|_| Error::Encoding("Avro schema cache lock is poisoned".into()))?
            .get_or_parse(definition)?;
        let resolved = value.resolve(&schema).map_err(|error| {
            Error::Encoding(format!("Avro value does not match its schema: {error}"))
        })?;
        let datum = to_avro_datum(&schema, resolved)
            .map_err(|error| Error::Encoding(format!("Avro serialization failed: {error}")))?;
        Ok(Bytes::from(datum))
    }

    fn encode_one(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        let destination = destination(event, &self.config)?;
        let heartbeat = is_heartbeat(event);
        let fields = adjusted_fields(event)?;

        let (key, key_schema) = if heartbeat {
            let schema = heartbeat_key_schema(event, &destination)?;
            let value = Value::Record(vec![(
                "serverName".into(),
                Value::String(event.source.connector_name.clone()),
            )]);
            let bytes = self.encode_datum(&schema.definition, value)?;
            (Some(bytes), Some(schema))
        } else {
            let key_fields = fields
                .iter()
                .filter(|field| field.source.primary_key)
                .collect::<Vec<_>>();
            if key_fields.is_empty() {
                (None, None)
            } else {
                let row = event
                    .after
                    .as_ref()
                    .or(event.before.as_ref())
                    .ok_or_else(|| {
                        Error::Encoding("keyed event has no before or after row".into())
                    })?;
                let schema = key_schema(event, &destination, &key_fields)?;
                let value = row_value(row, &key_fields, &self.config.unavailable_value, true)?;
                let bytes = self.encode_datum(&schema.definition, value)?;
                (Some(bytes), Some(schema))
            }
        };

        let payload_schema = payload_schema(event, &destination, &fields)?;
        let payload_value = if heartbeat {
            Value::Record(vec![(
                "ts_ms".into(),
                Value::Long(event.observed_time.timestamp_millis()),
            )])
        } else {
            envelope_value(event, &fields, &self.config.unavailable_value)?
        };
        let payload = self.encode_datum(&payload_schema.definition, payload_value)?;

        let mut headers = BTreeMap::new();
        headers.insert("rustium.event.id".into(), event.id.0.clone());
        headers.insert(
            "rustium.content.type".into(),
            "application/avro; framing=confluent-schema-registry".into(),
        );
        if heartbeat {
            headers.insert("rustium.record.type".into(), "heartbeat".into());
        }
        Ok(EncodedEvent {
            id: event.id.clone(),
            destination,
            key,
            key_schema,
            payload: Some(payload),
            payload_schema: Some(payload_schema),
            headers,
        })
    }
}

impl EventEncoder for DebeziumAvroEncoder {
    fn content_type(&self) -> &'static str {
        "application/avro; framing=confluent-schema-registry"
    }

    fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        self.encode_one(event)
    }

    fn encode_batch(&self, event: &ChangeEvent) -> Result<Vec<EncodedEvent>> {
        let encoded = self.encode_one(event)?;
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

impl ParsedSchemaCache {
    fn new(capacity: usize) -> Self {
        Self {
            schemas: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn get_or_parse(&mut self, definition: &str) -> Result<Arc<Schema>> {
        if let Some(schema) = self.schemas.get(definition).cloned() {
            self.touch(definition);
            return Ok(schema);
        }
        let schema = Arc::new(Schema::parse_str(definition).map_err(|error| {
            Error::Encoding(format!("generated Avro schema is invalid: {error}"))
        })?);
        if self.schemas.len() == self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.schemas.remove(&evicted);
        }
        self.order.push_back(definition.to_string());
        self.schemas.insert(definition.to_string(), schema.clone());
        Ok(schema)
    }

    fn touch(&mut self, definition: &str) {
        if let Some(index) = self.order.iter().position(|cached| cached == definition) {
            self.order.remove(index);
        }
        self.order.push_back(definition.to_string());
    }
}

fn destination(event: &ChangeEvent, config: &AvroEncoderConfig) -> Result<String> {
    if is_heartbeat(event) {
        return Ok(config.heartbeat_topic_name.clone().unwrap_or_else(|| {
            format!("{}.{}", config.heartbeat_topics_prefix, config.topic_prefix)
        }));
    }
    let table = event
        .source
        .table
        .as_deref()
        .ok_or_else(|| Error::Encoding("event has no source table".into()))?;
    if event.source.connector == "mysql" {
        Ok(format!(
            "{}.{}.{}",
            config.topic_prefix, event.source.database, table
        ))
    } else {
        let schema = event
            .source
            .schema
            .as_deref()
            .ok_or_else(|| Error::Encoding("event has no source schema".into()))?;
        Ok(format!(
            "{}.{}.{}.{}",
            config.topic_prefix, event.source.database, schema, table
        ))
    }
}

fn is_heartbeat(event: &ChangeEvent) -> bool {
    event
        .source
        .attributes
        .get("rustium.heartbeat")
        .is_some_and(|value| value == &serde_json::Value::Bool(true))
}

fn adjusted_fields(event: &ChangeEvent) -> Result<Vec<AdjustedField<'_>>> {
    let mut owners = BTreeMap::<String, String>::new();
    let mut adjusted = Vec::with_capacity(event.schema.fields.len());
    for field in &event.schema.fields {
        let name = adjust_avro_name(&field.name);
        if let Some(existing) = owners.insert(name.clone(), field.name.clone()) {
            return Err(Error::Encoding(format!(
                "Avro field name adjustment maps both {existing:?} and {:?} to {name:?}",
                field.name
            )));
        }
        adjusted.push(AdjustedField {
            source: field,
            name,
        });
    }
    Ok(adjusted)
}

fn adjust_avro_name(raw: &str) -> String {
    let mut adjusted = String::with_capacity(raw.len().max(1));
    for (index, character) in raw.chars().enumerate() {
        let valid = character == '_'
            || character.is_ascii_alphabetic()
            || (index > 0 && character.is_ascii_digit());
        adjusted.push(if valid { character } else { '_' });
    }
    if adjusted.is_empty() {
        adjusted.push('_');
    }
    adjusted
}

fn avro_namespace(event: &ChangeEvent) -> String {
    let base = event
        .schema
        .name
        .strip_suffix(".Envelope")
        .unwrap_or(&event.schema.name);
    base.split('.')
        .map(adjust_avro_name)
        .collect::<Vec<_>>()
        .join(".")
}

fn heartbeat_namespace(event: &ChangeEvent) -> String {
    format!(
        "{}.heartbeat",
        event
            .source
            .connector_name
            .split('.')
            .map(adjust_avro_name)
            .collect::<Vec<_>>()
            .join(".")
    )
}

fn key_schema(
    event: &ChangeEvent,
    destination: &str,
    fields: &[&AdjustedField<'_>],
) -> Result<WireSchema> {
    let namespace = avro_namespace(event);
    let fields = fields
        .iter()
        .map(|field| avro_field(field, true))
        .collect::<Vec<_>>();
    wire_schema(
        format!("{destination}-key"),
        serde_json::json!({
            "type": "record",
            "name": "Key",
            "namespace": namespace,
            "connect.name": format!("{}.Key", avro_namespace(event)),
            "fields": fields
        }),
    )
}

fn heartbeat_key_schema(event: &ChangeEvent, destination: &str) -> Result<WireSchema> {
    let namespace = heartbeat_namespace(event);
    wire_schema(
        format!("{destination}-key"),
        serde_json::json!({
            "type": "record",
            "name": "HeartbeatKey",
            "namespace": namespace,
            "connect.name": format!("{namespace}.HeartbeatKey"),
            "fields": [{"name": "serverName", "type": "string"}]
        }),
    )
}

fn payload_schema(
    event: &ChangeEvent,
    destination: &str,
    fields: &[AdjustedField<'_>],
) -> Result<WireSchema> {
    if is_heartbeat(event) {
        let namespace = heartbeat_namespace(event);
        return wire_schema(
            format!("{destination}-value"),
            serde_json::json!({
                "type": "record",
                "name": "Heartbeat",
                "namespace": namespace,
                "connect.name": format!("{namespace}.Heartbeat"),
                "fields": [{"name": "ts_ms", "type": "long"}]
            }),
        );
    }

    let namespace = avro_namespace(event);
    let row_fields = fields
        .iter()
        .map(|field| avro_field(field, false))
        .collect::<Vec<_>>();
    let row_schema = serde_json::json!({
        "type": "record",
        "name": "Value",
        "namespace": namespace,
        "connect.name": format!("{namespace}.Value"),
        "fields": row_fields
    });
    let source_schema = source_schema(event, &namespace);
    let transaction_schema = serde_json::json!({
        "type": "record",
        "name": "Transaction",
        "namespace": namespace,
        "connect.name": format!("{namespace}.Transaction"),
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "total_order", "type": ["null", "long"], "default": null},
            {"name": "data_collection_order", "type": ["null", "long"], "default": null}
        ]
    });
    wire_schema(
        format!("{destination}-value"),
        serde_json::json!({
            "type": "record",
            "name": "Envelope",
            "namespace": namespace,
            "connect.name": format!("{namespace}.Envelope"),
            "rustium.event.schema.version": event.schema.version,
            "fields": [
                {"name": "before", "type": ["null", row_schema], "default": null},
                {"name": "after", "type": ["null", format!("{namespace}.Value")], "default": null},
                {"name": "source", "type": source_schema},
                {"name": "op", "type": "string"},
                {"name": "ts_ms", "type": "long"},
                {"name": "transaction", "type": ["null", transaction_schema], "default": null}
            ]
        }),
    )
}

fn source_schema(event: &ChangeEvent, namespace: &str) -> serde_json::Value {
    let mut fields = vec![
        serde_json::json!({"name": "version", "type": "string"}),
        serde_json::json!({"name": "connector", "type": "string"}),
        serde_json::json!({"name": "name", "type": "string"}),
        serde_json::json!({"name": "ts_ms", "type": ["null", "long"], "default": null}),
        serde_json::json!({"name": "snapshot", "type": "string"}),
        serde_json::json!({"name": "db", "type": "string"}),
        serde_json::json!({"name": "schema", "type": ["null", "string"], "default": null}),
        serde_json::json!({"name": "table", "type": ["null", "string"], "default": null}),
    ];
    match &event.position {
        SourcePosition::Postgres(_) => fields.extend([
            serde_json::json!({"name": "sequence", "type": "string"}),
            serde_json::json!({"name": "txId", "type": ["null", "long"], "default": null}),
            serde_json::json!({"name": "lsn", "type": "long"}),
        ]),
        SourcePosition::MySql(_) => fields.extend([
            serde_json::json!({"name": "server_id", "type": "long"}),
            serde_json::json!({"name": "gtid", "type": ["null", "string"], "default": null}),
            serde_json::json!({"name": "file", "type": "string"}),
            serde_json::json!({"name": "pos", "type": "long"}),
            serde_json::json!({"name": "row", "type": "long"}),
            serde_json::json!({"name": "thread", "type": ["null", "long"], "default": null}),
            serde_json::json!({"name": "query", "type": ["null", "string"], "default": null}),
        ]),
        SourcePosition::SqlServer(_) => fields.extend([
            serde_json::json!({"name": "change_lsn", "type": "string"}),
            serde_json::json!({"name": "commit_lsn", "type": "string"}),
            serde_json::json!({"name": "event_serial_no", "type": "long"}),
        ]),
    }
    serde_json::json!({
        "type": "record",
        "name": "Source",
        "namespace": namespace,
        "connect.name": format!("{namespace}.Source"),
        "fields": fields
    })
}

fn avro_field(field: &AdjustedField<'_>, key: bool) -> serde_json::Value {
    let kind = avro_kind(&field.source.type_name);
    let mut definition = serde_json::Map::from_iter([
        ("name".into(), serde_json::Value::String(field.name.clone())),
        (
            "type".into(),
            avro_data_schema(&kind, field.source.optional, !key),
        ),
    ]);
    if field.source.optional {
        definition.insert("default".into(), serde_json::Value::Null);
    }
    serde_json::Value::Object(definition)
}

fn avro_data_schema(kind: &AvroKind, nullable: bool, allow_unavailable: bool) -> serde_json::Value {
    let base = match kind {
        AvroKind::Boolean => serde_json::json!("boolean"),
        AvroKind::Int => serde_json::json!("int"),
        AvroKind::Long => serde_json::json!("long"),
        AvroKind::Double => serde_json::json!("double"),
        AvroKind::Bytes => serde_json::json!("bytes"),
        AvroKind::String => serde_json::json!("string"),
        AvroKind::Array(element) => serde_json::json!({
            "type": "array",
            "items": avro_data_schema(element, true, true)
        }),
        AvroKind::Map => serde_json::json!({
            "type": "map",
            "values": ["null", "string"]
        }),
    };
    let mut variants = Vec::with_capacity(3);
    if nullable {
        variants.push(serde_json::json!("null"));
    }
    variants.push(base);
    if allow_unavailable && !matches!(kind, AvroKind::String) {
        variants.push(serde_json::json!("string"));
    }
    if variants.len() == 1 {
        variants.remove(0)
    } else {
        serde_json::Value::Array(variants)
    }
}

fn avro_kind(type_name: &str) -> AvroKind {
    let normalized = type_name.trim().to_ascii_lowercase();
    if let Some(element) = normalized.strip_suffix("[]") {
        return AvroKind::Array(Box::new(avro_kind(element)));
    }
    let unsigned = normalized.contains("unsigned");
    let base = normalized
        .split('(')
        .next()
        .unwrap_or(&normalized)
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or(&normalized)
        .trim()
        .trim_matches('"');
    let token = base.split_whitespace().next().unwrap_or(base);
    if base == "hstore" || base.starts_with("map") || base == "sparsevec" {
        return AvroKind::Map;
    }
    if normalized.starts_with("tinyint(1)") || matches!(token, "bool" | "boolean") {
        return AvroKind::Boolean;
    }
    if unsigned || matches!(token, "oid") {
        return AvroKind::String;
    }
    if matches!(
        token,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "serial"
            | "year"
            | "int2"
            | "int4"
    ) {
        return AvroKind::Int;
    }
    if matches!(token, "bigint" | "bigserial" | "int8") {
        return AvroKind::Long;
    }
    if matches!(token, "real" | "float" | "double") {
        return AvroKind::Double;
    }
    if matches!(
        token,
        "binary"
            | "varbinary"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
            | "bytea"
            | "image"
            | "rowversion"
            | "bit"
    ) || normalized.contains("geometry")
        || normalized.contains("geography")
    {
        return AvroKind::Bytes;
    }
    AvroKind::String
}

fn row_value(
    row: &Row,
    fields: &[&AdjustedField<'_>],
    unavailable_value: &str,
    key: bool,
) -> Result<Value> {
    let values = fields
        .iter()
        .map(|field| {
            let value = match row.get(&field.source.name) {
                Some(value) => data_value_to_avro(
                    value,
                    &avro_kind(&field.source.type_name),
                    unavailable_value,
                )
                .map_err(|error| {
                    Error::Encoding(format!(
                        "Avro field {:?} cannot be encoded: {error}",
                        field.source.name
                    ))
                })?,
                None if field.source.optional && !key => Value::Null,
                None => {
                    return Err(Error::Encoding(format!(
                        "Avro record is missing required field {:?}",
                        field.source.name
                    )));
                }
            };
            Ok((field.name.clone(), value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::Record(values))
}

fn envelope_value(
    event: &ChangeEvent,
    fields: &[AdjustedField<'_>],
    unavailable_value: &str,
) -> Result<Value> {
    let field_refs = fields.iter().collect::<Vec<_>>();
    let before = event
        .before
        .as_ref()
        .map(|row| row_value(row, &field_refs, unavailable_value, false))
        .transpose()?
        .unwrap_or(Value::Null);
    let after = event
        .after
        .as_ref()
        .map(|row| row_value(row, &field_refs, unavailable_value, false))
        .transpose()?
        .unwrap_or(Value::Null);
    let transaction = event
        .transaction
        .as_ref()
        .map(|transaction| -> Result<Value> {
            Ok(Value::Record(vec![
                ("id".into(), Value::String(transaction.id.clone())),
                (
                    "total_order".into(),
                    transaction.total_order.map_or(Ok(Value::Null), |value| {
                        u64_to_i64(value, "transaction total order").map(Value::Long)
                    })?,
                ),
                (
                    "data_collection_order".into(),
                    transaction
                        .collection_order
                        .map_or(Ok(Value::Null), |value| {
                            u64_to_i64(value, "transaction collection order").map(Value::Long)
                        })?,
                ),
            ]))
        })
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(Value::Record(vec![
        ("before".into(), before),
        ("after".into(), after),
        ("source".into(), source_value(event)?),
        (
            "op".into(),
            Value::String(operation_code(event.operation).into()),
        ),
        (
            "ts_ms".into(),
            Value::Long(event.observed_time.timestamp_millis()),
        ),
        ("transaction".into(), transaction),
    ]))
}

fn source_value(event: &ChangeEvent) -> Result<Value> {
    let mut values = vec![
        (
            "version".into(),
            Value::String(event.source.version.clone()),
        ),
        (
            "connector".into(),
            Value::String(event.source.connector.clone()),
        ),
        (
            "name".into(),
            Value::String(event.source.connector_name.clone()),
        ),
        (
            "ts_ms".into(),
            event
                .source_time
                .map_or(Value::Null, |time| Value::Long(time.timestamp_millis())),
        ),
        (
            "snapshot".into(),
            Value::String(snapshot_marker(event).into()),
        ),
        ("db".into(), Value::String(event.source.database.clone())),
        (
            "schema".into(),
            event
                .source
                .schema
                .clone()
                .map_or(Value::Null, Value::String),
        ),
        (
            "table".into(),
            event
                .source
                .table
                .clone()
                .map_or(Value::Null, Value::String),
        ),
    ];
    match &event.position {
        SourcePosition::Postgres(position) => {
            values.extend([
                (
                    "sequence".into(),
                    Value::String(serde_json::to_string(&[
                        position.commit_lsn.unwrap_or(position.lsn).to_string(),
                        position.lsn.to_string(),
                    ])?),
                ),
                (
                    "txId".into(),
                    position
                        .transaction_id
                        .map_or(Value::Null, |value| Value::Long(i64::from(value))),
                ),
                ("lsn".into(), Value::Long(u64_to_i64(position.lsn, "lsn")?)),
            ]);
        }
        SourcePosition::MySql(position) => {
            values.extend([
                (
                    "server_id".into(),
                    Value::Long(i64::from(position.server_id)),
                ),
                (
                    "gtid".into(),
                    position.gtid_set.clone().map_or(Value::Null, Value::String),
                ),
                (
                    "file".into(),
                    Value::String(position.binlog_filename.clone()),
                ),
                (
                    "pos".into(),
                    Value::Long(u64_to_i64(position.binlog_position, "binlog position")?),
                ),
                (
                    "row".into(),
                    Value::Long(u64_to_i64(position.event_serial, "event serial")?),
                ),
                ("thread".into(), Value::Null),
                ("query".into(), Value::Null),
            ]);
        }
        SourcePosition::SqlServer(position) => {
            values.extend([
                (
                    "change_lsn".into(),
                    Value::String(position.change_lsn.clone()),
                ),
                (
                    "commit_lsn".into(),
                    Value::String(position.commit_lsn.clone()),
                ),
                (
                    "event_serial_no".into(),
                    Value::Long(u64_to_i64(position.event_serial, "event serial")?),
                ),
            ]);
        }
    }
    Ok(Value::Record(values))
}

fn data_value_to_avro(value: &DataValue, kind: &AvroKind, unavailable: &str) -> Result<Value> {
    if matches!(value, DataValue::Null) {
        return Ok(Value::Null);
    }
    if matches!(value, DataValue::Unavailable) {
        return Ok(Value::String(unavailable.into()));
    }
    if let DataValue::String(value) = value
        && !matches!(kind, AvroKind::String)
    {
        return Ok(Value::String(value.clone()));
    }
    match kind {
        AvroKind::Boolean => match value {
            DataValue::Boolean(value) => Ok(Value::Boolean(*value)),
            other => Err(Error::Encoding(format!("expected boolean, got {other:?}"))),
        },
        AvroKind::Int => match value {
            DataValue::Int32(value) => Ok(Value::Int(*value)),
            DataValue::Int64(value) => i32::try_from(*value).map(Value::Int).map_err(|_| {
                Error::Encoding(format!("integer {value} is outside the Avro int range"))
            }),
            other => Err(Error::Encoding(format!("expected integer, got {other:?}"))),
        },
        AvroKind::Long => match value {
            DataValue::Int32(value) => Ok(Value::Long(i64::from(*value))),
            DataValue::Int64(value) => Ok(Value::Long(*value)),
            DataValue::UInt64(value) => i64::try_from(*value).map(Value::Long).map_err(|_| {
                Error::Encoding(format!("integer {value} is outside the Avro long range"))
            }),
            other => Err(Error::Encoding(format!("expected integer, got {other:?}"))),
        },
        AvroKind::Double => match value {
            DataValue::Float64(value) => Ok(Value::Double(*value)),
            DataValue::Int32(value) => Ok(Value::Double(f64::from(*value))),
            DataValue::Int64(value) => Ok(Value::Double(*value as f64)),
            other => Err(Error::Encoding(format!("expected number, got {other:?}"))),
        },
        AvroKind::Bytes => match value {
            DataValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
            other => Err(Error::Encoding(format!("expected bytes, got {other:?}"))),
        },
        AvroKind::String => value_as_string(value, unavailable).map(Value::String),
        AvroKind::Array(element) => match value {
            DataValue::Array(values) => values
                .iter()
                .map(|value| data_value_to_avro(value, element, unavailable))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            other => value_as_string(other, unavailable).map(Value::String),
        },
        AvroKind::Map => match value {
            DataValue::Map(values) => values
                .iter()
                .map(|(key, value)| {
                    let value = if matches!(value, DataValue::Null) {
                        Value::Null
                    } else {
                        Value::String(value_as_string(value, unavailable)?)
                    };
                    Ok((key.clone(), value))
                })
                .collect::<Result<HashMap<_, _>>>()
                .map(Value::Map),
            other => value_as_string(other, unavailable).map(Value::String),
        },
    }
}

fn value_as_string(value: &DataValue, unavailable: &str) -> Result<String> {
    match value {
        DataValue::Null => Ok("null".into()),
        DataValue::Boolean(value) => Ok(value.to_string()),
        DataValue::Int32(value) => Ok(value.to_string()),
        DataValue::Int64(value) => Ok(value.to_string()),
        DataValue::UInt64(value) => Ok(value.to_string()),
        DataValue::Float64(value) => Ok(value.to_string()),
        DataValue::Decimal(value)
        | DataValue::String(value)
        | DataValue::Date(value)
        | DataValue::Time(value)
        | DataValue::Timestamp(value) => Ok(value.clone()),
        DataValue::Bytes(value) => Ok(hex::encode(value)),
        DataValue::Uuid(value) => Ok(value.to_string()),
        DataValue::Json(_) | DataValue::Array(_) | DataValue::Map(_) => {
            Ok(serde_json::to_string(&value.to_json(unavailable))?)
        }
        DataValue::Unavailable => Ok(unavailable.into()),
    }
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::Encoding(format!(
            "{field} value {value} is outside the Avro long range"
        ))
    })
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

fn wire_schema(subject: String, definition: serde_json::Value) -> Result<WireSchema> {
    Ok(WireSchema {
        subject,
        schema_type: WireSchemaType::Avro,
        definition: serde_json::to_string(&definition)?,
    })
}

#[cfg(test)]
mod tests {
    use apache_avro::from_avro_datum;
    use chrono::Utc;
    use indexmap::indexmap;
    use rustium_core::{
        EventId, EventSchema, MySqlPosition, PostgresPosition, SourceMetadata, SqlServerPosition,
        TransactionMetadata,
    };

    use super::*;

    fn encoder(cache_capacity: usize) -> DebeziumAvroEncoder {
        DebeziumAvroEncoder::new(AvroEncoderConfig {
            topic_prefix: "inventory".into(),
            unavailable_value: "__debezium_unavailable_value".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            schema_cache_capacity: cache_capacity,
        })
        .unwrap()
    }

    fn event() -> ChangeEvent {
        ChangeEvent {
            id: EventId("event-1".into()),
            source: SourceMetadata {
                connector: "mysql".into(),
                connector_name: "inventory".into(),
                database: "app".into(),
                schema: None,
                table: Some("customers".into()),
                snapshot: false,
                version: "0.1.0".into(),
                attributes: BTreeMap::new(),
            },
            position: SourcePosition::MySql(MySqlPosition {
                binlog_filename: "binlog.000001".into(),
                binlog_position: 42,
                gtid_set: Some("server:1-2".into()),
                server_id: 1,
                event_serial: 1,
                snapshot: false,
            }),
            transaction: Some(TransactionMetadata {
                id: "server:2".into(),
                total_order: Some(1),
                collection_order: Some(1),
            }),
            operation: Operation::Create,
            before: None,
            after: Some(indexmap! {
                "customer-id".into() => DataValue::Int64(7),
                "name".into() => DataValue::String("Alice".into()),
                "amount".into() => DataValue::Decimal("12.30".into()),
                "score".into() => DataValue::String("NaN".into()),
                "binary_value".into() => DataValue::Bytes(vec![1, 2, 3]),
                "counter".into() => DataValue::Unavailable,
                "attributes".into() => DataValue::Map(BTreeMap::from([
                    ("region".into(), DataValue::String("global".into())),
                    ("optional".into(), DataValue::Null),
                ])),
            }),
            schema: EventSchema {
                name: "inventory.app.customers.Envelope".into(),
                version: 1,
                fields: vec![
                    FieldSchema {
                        name: "customer-id".into(),
                        type_name: "bigint".into(),
                        optional: false,
                        primary_key: true,
                    },
                    FieldSchema {
                        name: "name".into(),
                        type_name: "varchar(255)".into(),
                        optional: false,
                        primary_key: false,
                    },
                    FieldSchema {
                        name: "amount".into(),
                        type_name: "decimal(10,2)".into(),
                        optional: false,
                        primary_key: false,
                    },
                    FieldSchema {
                        name: "score".into(),
                        type_name: "double precision".into(),
                        optional: false,
                        primary_key: false,
                    },
                    FieldSchema {
                        name: "binary_value".into(),
                        type_name: "bytea".into(),
                        optional: false,
                        primary_key: false,
                    },
                    FieldSchema {
                        name: "counter".into(),
                        type_name: "integer".into(),
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
            source_time: Some(Utc::now()),
            observed_time: Utc::now(),
        }
    }

    fn decode(encoded: &[u8], schema: &WireSchema) -> Value {
        let schema = Schema::parse_str(&schema.definition).unwrap();
        let mut input = encoded;
        from_avro_datum(&schema, &mut input, None).unwrap()
    }

    fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
        let Value::Record(fields) = value else {
            panic!("expected record, got {value:?}");
        };
        fields
            .iter()
            .find_map(|(field, value)| (field == name).then_some(value))
            .unwrap_or_else(|| panic!("record has no {name:?} field"))
    }

    fn union(value: &Value) -> &Value {
        let Value::Union(_, value) = value else {
            panic!("expected union, got {value:?}");
        };
        value
    }

    #[test]
    fn emits_and_decodes_debezium_avro_records() {
        let encoded = encoder(16).encode(&event()).unwrap();
        assert_eq!(encoded.destination, "inventory.app.customers");
        let key_schema = encoded.key_schema.as_ref().unwrap();
        let payload_schema = encoded.payload_schema.as_ref().unwrap();
        assert_eq!(key_schema.schema_type, WireSchemaType::Avro);
        assert_eq!(key_schema.subject, "inventory.app.customers-key");
        assert_eq!(payload_schema.subject, "inventory.app.customers-value");
        assert!(key_schema.definition.contains("customer_id"));

        let key = decode(encoded.key.as_ref().unwrap(), key_schema);
        assert_eq!(field(&key, "customer_id"), &Value::Long(7));
        let payload = decode(encoded.payload.as_ref().unwrap(), payload_schema);
        let after = union(field(&payload, "after"));
        assert_eq!(union(field(after, "customer_id")), &Value::Long(7));
        assert_eq!(field(after, "name"), &Value::String("Alice".into()));
        assert_eq!(field(after, "amount"), &Value::String("12.30".into()));
        assert_eq!(union(field(after, "score")), &Value::String("NaN".into()));
        assert_eq!(
            union(field(after, "binary_value")),
            &Value::Bytes(vec![1, 2, 3])
        );
        assert_eq!(
            union(field(after, "counter")),
            &Value::String("__debezium_unavailable_value".into())
        );
        let attributes = union(field(after, "attributes"));
        let Value::Map(attributes) = attributes else {
            panic!("expected attributes map");
        };
        assert_eq!(
            union(&attributes["region"]),
            &Value::String("global".into())
        );
        assert_eq!(field(&payload, "op"), &Value::String("c".into()));
    }

    #[test]
    fn evolves_optional_fields_and_emits_null_tombstones() {
        let encoder = encoder(2);
        let first = encoder.encode(&event()).unwrap();
        let mut changed = event();
        changed.schema.version = 2;
        changed.schema.fields.push(FieldSchema {
            name: "email".into(),
            type_name: "varchar(255)".into(),
            optional: true,
            primary_key: false,
        });
        changed.after.as_mut().unwrap().insert(
            "email".into(),
            DataValue::String("alice@example.com".into()),
        );
        let second = encoder.encode(&changed).unwrap();
        let first_schema = first.payload_schema.as_ref().unwrap();
        let second_schema = second.payload_schema.as_ref().unwrap();
        assert_ne!(first_schema.definition, second_schema.definition);
        let decoded = decode(second.payload.as_ref().unwrap(), second_schema);
        assert_eq!(
            union(field(union(field(&decoded, "after")), "email")),
            &Value::String("alice@example.com".into())
        );

        let writer = Schema::parse_str(&first_schema.definition).unwrap();
        let reader = Schema::parse_str(&second_schema.definition).unwrap();
        let mut first_input = first.payload.as_ref().unwrap().as_ref();
        let evolved = from_avro_datum(&writer, &mut first_input, Some(&reader)).unwrap();
        assert_eq!(
            union(field(union(field(&evolved, "after")), "email")),
            &Value::Null
        );

        changed.operation = Operation::Delete;
        changed.before = changed.after.take();
        let deleted = encoder.encode_batch(&changed).unwrap();
        assert_eq!(deleted.len(), 2);
        assert_eq!(deleted[0].key_schema, deleted[1].key_schema);
        assert!(deleted[0].payload.is_some());
        assert!(deleted[1].payload.is_none());
        assert!(deleted[1].payload_schema.is_none());
        assert!(encoder.schemas.lock().unwrap().schemas.len() <= 2);
    }

    #[test]
    fn rejects_avro_name_collisions_and_invalid_cache_capacity() {
        let error = DebeziumAvroEncoder::new(AvroEncoderConfig {
            schema_cache_capacity: 0,
            ..encoder(1).config.clone()
        })
        .err()
        .expect("zero cache capacity unexpectedly succeeded");
        assert!(error.to_string().contains("greater than zero"));

        let mut event = event();
        event.schema.fields.push(FieldSchema {
            name: "customer_id".into(),
            type_name: "bigint".into(),
            optional: false,
            primary_key: false,
        });
        event
            .after
            .as_mut()
            .unwrap()
            .insert("customer_id".into(), DataValue::Int64(8));
        let error = encoder(16).encode(&event).unwrap_err();
        assert!(error.to_string().contains("maps both"));
    }

    #[test]
    fn encodes_connector_specific_sources_and_heartbeat() {
        let encoder = encoder(16);
        let mut postgres = event();
        postgres.source.connector = "postgresql".into();
        postgres.source.schema = Some("public".into());
        postgres.schema.name = "inventory.app.public.customers.Envelope".into();
        postgres.position = SourcePosition::Postgres(PostgresPosition {
            lsn: 42,
            commit_lsn: Some(44),
            transaction_id: Some(7),
            event_serial: 1,
            snapshot: false,
        });
        let encoded = encoder.encode(&postgres).unwrap();
        let payload = decode(
            encoded.payload.as_ref().unwrap(),
            encoded.payload_schema.as_ref().unwrap(),
        );
        let source = field(&payload, "source");
        assert_eq!(field(source, "lsn"), &Value::Long(42));
        assert_eq!(union(field(source, "txId")), &Value::Long(7));

        let mut sqlserver = postgres.clone();
        sqlserver.source.connector = "sqlserver".into();
        sqlserver.source.schema = Some("dbo".into());
        sqlserver.schema.name = "inventory.app.dbo.customers.Envelope".into();
        sqlserver.position = SourcePosition::SqlServer(SqlServerPosition {
            database: "app".into(),
            commit_lsn: "0001:0002:0003".into(),
            change_lsn: "0001:0002:0004".into(),
            event_serial: 2,
            snapshot: false,
        });
        let encoded = encoder.encode(&sqlserver).unwrap();
        let payload = decode(
            encoded.payload.as_ref().unwrap(),
            encoded.payload_schema.as_ref().unwrap(),
        );
        let source = field(&payload, "source");
        assert_eq!(
            field(source, "commit_lsn"),
            &Value::String("0001:0002:0003".into())
        );

        let mut heartbeat = event();
        heartbeat.operation = Operation::Message;
        heartbeat.before = None;
        heartbeat.after = None;
        heartbeat.source.table = None;
        heartbeat.source.schema = None;
        heartbeat
            .source
            .attributes
            .insert("rustium.heartbeat".into(), true.into());
        let encoded = encoder.encode(&heartbeat).unwrap();
        assert_eq!(encoded.destination, "__debezium-heartbeat.inventory");
        let key = decode(
            encoded.key.as_ref().unwrap(),
            encoded.key_schema.as_ref().unwrap(),
        );
        assert_eq!(
            field(&key, "serverName"),
            &Value::String("inventory".into())
        );
        let payload = decode(
            encoded.payload.as_ref().unwrap(),
            encoded.payload_schema.as_ref().unwrap(),
        );
        assert_eq!(
            field(&payload, "ts_ms"),
            &Value::Long(heartbeat.observed_time.timestamp_millis())
        );
    }
}
