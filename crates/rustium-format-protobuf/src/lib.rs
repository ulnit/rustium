//! Debezium-compatible Protocol Buffers encoding for Rustium change events.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MapKey, MessageDescriptor, ReflectMessage,
    Value as ProtoValue,
};
use rustium_core::{
    ChangeEvent, DataValue, EncodedEvent, Error, EventEncoder, FieldSchema, Operation, Result, Row,
    SourcePosition, WireSchema, WireSchemaType,
};
use sha2::{Digest, Sha256};

const MAX_PROTOBUF_FIELD_NUMBER: u32 = 536_870_911;
const RESERVED_FIELD_START: u32 = 19_000;
const RESERVED_FIELD_END: u32 = 19_999;

#[derive(Debug, Clone)]
pub struct ProtobufEncoderConfig {
    pub topic_prefix: String,
    pub unavailable_value: String,
    pub tombstones_on_delete: bool,
    pub heartbeat_topics_prefix: String,
    pub heartbeat_topic_name: Option<String>,
    pub schema_cache_capacity: usize,
}

pub struct DebeziumProtobufEncoder {
    config: ProtobufEncoderConfig,
    schemas: Mutex<ParsedSchemaCache>,
}

struct ParsedSchemaCache {
    schemas: HashMap<SchemaCacheKey, Arc<CompiledSchema>>,
    order: VecDeque<SchemaCacheKey>,
    capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SchemaCacheKey {
    definition: String,
    root_name: String,
}

struct CompiledSchema {
    root: MessageDescriptor,
}

struct GeneratedSchema {
    wire: WireSchema,
    root_name: String,
}

struct AdjustedField<'a> {
    source: &'a FieldSchema,
    name: String,
    number: u32,
    kind: ProtobufKind,
}

#[derive(Debug, Clone)]
enum ProtobufKind {
    Boolean,
    Int,
    Long,
    UnsignedLong,
    Double,
    Bytes,
    String,
    Array(Box<Self>),
    Map,
}

impl DebeziumProtobufEncoder {
    pub fn new(config: ProtobufEncoderConfig) -> Result<Self> {
        if config.schema_cache_capacity == 0 {
            return Err(Error::Configuration(
                "Protobuf schema cache capacity must be greater than zero".into(),
            ));
        }
        let capacity = config.schema_cache_capacity;
        Ok(Self {
            config,
            schemas: Mutex::new(ParsedSchemaCache::new(capacity)),
        })
    }

    fn compile(&self, schema: &GeneratedSchema) -> Result<Arc<CompiledSchema>> {
        self.schemas
            .lock()
            .map_err(|_| Error::Encoding("Protobuf schema cache lock is poisoned".into()))?
            .get_or_parse(&schema.wire.definition, &schema.root_name)
    }

    fn encode_message(
        &self,
        schema: &GeneratedSchema,
        build: impl FnOnce(&MessageDescriptor) -> Result<DynamicMessage>,
    ) -> Result<Bytes> {
        let compiled = self.compile(schema)?;
        let message = build(&compiled.root)?;
        let encoded = message.encode_to_vec();
        let mut datum = Vec::with_capacity(encoded.len() + 1);
        // Confluent optimizes the top-level message index [0] to one zero varint.
        datum.push(0);
        datum.extend_from_slice(&encoded);
        Ok(Bytes::from(datum))
    }

    fn encode_one(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        let destination = destination(event, &self.config)?;
        let heartbeat = is_heartbeat(event);
        let fields = adjusted_fields(event)?;

        let (key, key_schema) = if heartbeat {
            let schema = heartbeat_key_schema(event, &destination)?;
            let key = self.encode_message(&schema, |descriptor| {
                let mut message = DynamicMessage::new(descriptor.clone());
                set_field(
                    &mut message,
                    "serverName",
                    ProtoValue::String(event.source.connector_name.clone()),
                )?;
                Ok(message)
            })?;
            (Some(key), Some(schema.wire))
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
                let key = self.encode_message(&schema, |descriptor| {
                    build_key_message(row, &key_fields, descriptor, &self.config.unavailable_value)
                })?;
                (Some(key), Some(schema.wire))
            }
        };

        let payload_schema = payload_schema(event, &destination, &fields)?;
        let payload = self.encode_message(&payload_schema, |descriptor| {
            if heartbeat {
                let mut message = DynamicMessage::new(descriptor.clone());
                set_field(
                    &mut message,
                    "ts_ms",
                    ProtoValue::I64(event.observed_time.timestamp_millis()),
                )?;
                Ok(message)
            } else {
                build_envelope_message(event, &fields, descriptor, &self.config.unavailable_value)
            }
        })?;

        let mut headers = BTreeMap::new();
        headers.insert("rustium.event.id".into(), event.id.0.clone());
        headers.insert(
            "rustium.content.type".into(),
            "application/x-protobuf; framing=confluent-schema-registry".into(),
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
            payload_schema: Some(payload_schema.wire),
            headers,
        })
    }
}

impl EventEncoder for DebeziumProtobufEncoder {
    fn content_type(&self) -> &'static str {
        "application/x-protobuf; framing=confluent-schema-registry"
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

    fn get_or_parse(&mut self, definition: &str, root_name: &str) -> Result<Arc<CompiledSchema>> {
        let key = SchemaCacheKey {
            definition: definition.into(),
            root_name: root_name.into(),
        };
        if let Some(schema) = self.schemas.get(&key).cloned() {
            self.touch(&key);
            return Ok(schema);
        }
        let file = protox_parse::parse("rustium.proto", definition).map_err(|error| {
            Error::Encoding(format!("generated Protobuf schema is invalid: {error}"))
        })?;
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file).map_err(|error| {
            Error::Encoding(format!(
                "generated Protobuf descriptors are invalid: {error}"
            ))
        })?;
        let root = pool.get_message_by_name(root_name).ok_or_else(|| {
            Error::Encoding(format!(
                "generated Protobuf schema has no root message {root_name:?}"
            ))
        })?;
        let schema = Arc::new(CompiledSchema { root });
        if self.schemas.len() == self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.schemas.remove(&evicted);
        }
        self.order.push_back(key.clone());
        self.schemas.insert(key, schema.clone());
        Ok(schema)
    }

    fn touch(&mut self, key: &SchemaCacheKey) {
        if let Some(index) = self.order.iter().position(|cached| cached == key) {
            self.order.remove(index);
        }
        self.order.push_back(key.clone());
    }
}

fn destination(event: &ChangeEvent, config: &ProtobufEncoderConfig) -> Result<String> {
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
    let mut name_owners = BTreeMap::<String, String>::new();
    let mut number_owners = BTreeMap::<u32, String>::new();
    let mut fields = Vec::with_capacity(event.schema.fields.len());
    for field in &event.schema.fields {
        let name = adjust_protobuf_name(&field.name);
        if let Some(existing) = name_owners.insert(name.clone(), field.name.clone()) {
            return Err(Error::Encoding(format!(
                "Protobuf field name adjustment maps both {existing:?} and {:?} to {name:?}",
                field.name
            )));
        }
        let number = stable_field_number(&field.name);
        if let Some(existing) = number_owners.insert(number, field.name.clone()) {
            return Err(Error::Encoding(format!(
                "Protobuf field-number derivation maps both {existing:?} and {:?} to {number}",
                field.name
            )));
        }
        fields.push(AdjustedField {
            source: field,
            name,
            number,
            kind: protobuf_kind(&field.type_name),
        });
    }
    Ok(fields)
}

fn stable_field_number(name: &str) -> u32 {
    let digest = Sha256::digest(name.as_bytes());
    let raw = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    let mut number = raw % MAX_PROTOBUF_FIELD_NUMBER + 1;
    if (RESERVED_FIELD_START..=RESERVED_FIELD_END).contains(&number) {
        number += RESERVED_FIELD_END - RESERVED_FIELD_START + 1;
    }
    number
}

fn adjust_protobuf_name(raw: &str) -> String {
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

fn protobuf_package(event: &ChangeEvent) -> String {
    let base = event
        .schema
        .name
        .strip_suffix(".Envelope")
        .unwrap_or(&event.schema.name);
    base.split('.')
        .map(adjust_protobuf_name)
        .collect::<Vec<_>>()
        .join(".")
}

fn heartbeat_package(event: &ChangeEvent) -> String {
    format!(
        "{}.heartbeat",
        event
            .source
            .connector_name
            .split('.')
            .map(adjust_protobuf_name)
            .collect::<Vec<_>>()
            .join(".")
    )
}

fn key_schema(
    event: &ChangeEvent,
    destination: &str,
    fields: &[&AdjustedField<'_>],
) -> Result<GeneratedSchema> {
    let package = protobuf_package(event);
    let mut definition = schema_header(&package);
    push_line(&mut definition, 0, "message Key {");
    for field in fields {
        let field_type = key_protobuf_type(&field.kind);
        push_line(
            &mut definition,
            1,
            &format!("{field_type} {} = {};", field.name, field.number),
        );
    }
    push_line(&mut definition, 0, "}");
    generated_schema(
        format!("{destination}-key"),
        definition,
        format!("{package}.Key"),
    )
}

fn heartbeat_key_schema(event: &ChangeEvent, destination: &str) -> Result<GeneratedSchema> {
    let package = heartbeat_package(event);
    let mut definition = schema_header(&package);
    push_line(&mut definition, 0, "message HeartbeatKey {");
    push_line(&mut definition, 1, "string serverName = 1;");
    push_line(&mut definition, 0, "}");
    generated_schema(
        format!("{destination}-key"),
        definition,
        format!("{package}.HeartbeatKey"),
    )
}

fn payload_schema(
    event: &ChangeEvent,
    destination: &str,
    fields: &[AdjustedField<'_>],
) -> Result<GeneratedSchema> {
    if is_heartbeat(event) {
        let package = heartbeat_package(event);
        let mut definition = schema_header(&package);
        push_line(&mut definition, 0, "message Heartbeat {");
        push_line(&mut definition, 1, "int64 ts_ms = 1;");
        push_line(&mut definition, 0, "}");
        return generated_schema(
            format!("{destination}-value"),
            definition,
            format!("{package}.Heartbeat"),
        );
    }

    let package = protobuf_package(event);
    let mut definition = schema_header(&package);
    push_line(&mut definition, 0, "message Envelope {");
    push_line(&mut definition, 1, "message Value {");
    for field in fields {
        push_field_wrapper(&mut definition, field, 2);
    }
    for field in fields {
        push_line(
            &mut definition,
            2,
            &format!("Field_{} {} = {};", field.name, field.name, field.number),
        );
    }
    push_line(&mut definition, 1, "}");
    push_source_schema(&mut definition, event, 1);
    push_line(&mut definition, 1, "message Transaction {");
    push_line(&mut definition, 2, "string id = 1;");
    push_line(&mut definition, 2, "optional uint64 total_order = 2;");
    push_line(
        &mut definition,
        2,
        "optional uint64 data_collection_order = 3;",
    );
    push_line(&mut definition, 1, "}");
    push_line(&mut definition, 1, "Value before = 1;");
    push_line(&mut definition, 1, "Value after = 2;");
    push_line(&mut definition, 1, "Source source = 3;");
    push_line(&mut definition, 1, "string op = 4;");
    push_line(&mut definition, 1, "int64 ts_ms = 5;");
    push_line(&mut definition, 1, "Transaction transaction = 6;");
    push_line(&mut definition, 0, "}");
    generated_schema(
        format!("{destination}-value"),
        definition,
        format!("{package}.Envelope"),
    )
}

fn schema_header(package: &str) -> String {
    format!("syntax = \"proto3\";\npackage {package};\n\n")
}

fn push_source_schema(output: &mut String, event: &ChangeEvent, indent: usize) {
    push_line(output, indent, "message Source {");
    let fields = [
        "string version = 1;",
        "string connector = 2;",
        "string name = 3;",
        "optional int64 ts_ms = 4;",
        "string snapshot = 5;",
        "string db = 6;",
        "optional string schema = 7;",
        "optional string table = 8;",
    ];
    for field in fields {
        push_line(output, indent + 1, field);
    }
    let connector_fields: &[&str] = match &event.position {
        SourcePosition::Postgres(_) => &[
            "string sequence = 20;",
            "optional uint32 txId = 21;",
            "uint64 lsn = 22;",
        ],
        SourcePosition::MySql(_) => &[
            "uint32 server_id = 20;",
            "optional string gtid = 21;",
            "string file = 22;",
            "uint64 pos = 23;",
            "uint64 row = 24;",
            "optional uint64 thread = 25;",
            "optional string query = 26;",
        ],
        SourcePosition::SqlServer(_) => &[
            "string change_lsn = 20;",
            "string commit_lsn = 21;",
            "uint64 event_serial_no = 22;",
        ],
    };
    for field in connector_fields {
        push_line(output, indent + 1, field);
    }
    push_line(output, indent, "}");
}

fn push_field_wrapper(output: &mut String, field: &AdjustedField<'_>, indent: usize) {
    push_line(output, indent, &format!("message Field_{} {{", field.name));
    if matches!(field.kind, ProtobufKind::Array(_) | ProtobufKind::Map) {
        push_container_message(output, "NativeValue", &field.kind, indent + 1);
    }
    push_line(output, indent + 1, "oneof state {");
    push_line(
        output,
        indent + 2,
        &format!("{} value = 1;", native_type_name(&field.kind)),
    );
    push_line(output, indent + 2, "string unavailable = 2;");
    if !matches!(field.kind, ProtobufKind::String) {
        push_line(output, indent + 2, "string text = 3;");
    }
    push_line(output, indent + 1, "}");
    push_line(output, indent, "}");
}

fn push_container_message(output: &mut String, name: &str, kind: &ProtobufKind, indent: usize) {
    push_line(output, indent, &format!("message {name} {{"));
    match kind {
        ProtobufKind::Array(element) => {
            push_line(output, indent + 1, "message Item {");
            if matches!(element.as_ref(), ProtobufKind::Array(_) | ProtobufKind::Map) {
                push_container_message(output, "NativeValue", element, indent + 2);
            }
            push_line(output, indent + 2, "oneof state {");
            push_line(
                output,
                indent + 3,
                &format!("{} value = 1;", native_type_name(element)),
            );
            push_line(output, indent + 3, "bool null_value = 2;");
            push_line(output, indent + 3, "string unavailable = 3;");
            if !matches!(element.as_ref(), ProtobufKind::String) {
                push_line(output, indent + 3, "string text = 4;");
            }
            push_line(output, indent + 2, "}");
            push_line(output, indent + 1, "}");
            push_line(output, indent + 1, "repeated Item values = 1;");
        }
        ProtobufKind::Map => {
            push_line(output, indent + 1, "message EntryValue {");
            push_line(output, indent + 2, "oneof state {");
            push_line(output, indent + 3, "string value = 1;");
            push_line(output, indent + 3, "bool null_value = 2;");
            push_line(output, indent + 3, "string unavailable = 3;");
            push_line(output, indent + 2, "}");
            push_line(output, indent + 1, "}");
            push_line(output, indent + 1, "map<string, EntryValue> values = 1;");
        }
        _ => {}
    }
    push_line(output, indent, "}");
}

fn native_type_name(kind: &ProtobufKind) -> &'static str {
    match kind {
        ProtobufKind::Boolean => "bool",
        ProtobufKind::Int => "int32",
        ProtobufKind::Long => "int64",
        ProtobufKind::UnsignedLong => "uint64",
        ProtobufKind::Double => "double",
        ProtobufKind::Bytes => "bytes",
        ProtobufKind::String => "string",
        ProtobufKind::Array(_) | ProtobufKind::Map => "NativeValue",
    }
}

fn key_protobuf_type(kind: &ProtobufKind) -> &'static str {
    match kind {
        ProtobufKind::Array(_) | ProtobufKind::Map => "string",
        _ => native_type_name(kind),
    }
}

fn push_line(output: &mut String, indent: usize, line: &str) {
    for _ in 0..indent {
        output.push_str("  ");
    }
    output.push_str(line);
    output.push('\n');
}

fn generated_schema(
    subject: String,
    definition: String,
    root_name: String,
) -> Result<GeneratedSchema> {
    protox_parse::parse("rustium.proto", &definition).map_err(|error| {
        Error::Encoding(format!("generated Protobuf schema is invalid: {error}"))
    })?;
    Ok(GeneratedSchema {
        wire: WireSchema {
            subject,
            schema_type: WireSchemaType::Protobuf,
            definition,
        },
        root_name,
    })
}

fn protobuf_kind(type_name: &str) -> ProtobufKind {
    let normalized = type_name.trim().to_ascii_lowercase();
    if let Some(element) = normalized.strip_suffix("[]") {
        return ProtobufKind::Array(Box::new(protobuf_kind(element)));
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
        return ProtobufKind::Map;
    }
    if normalized.starts_with("tinyint(1)") || matches!(token, "bool" | "boolean") {
        return ProtobufKind::Boolean;
    }
    if unsigned || token == "oid" {
        return ProtobufKind::UnsignedLong;
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
        return ProtobufKind::Int;
    }
    if matches!(token, "bigint" | "bigserial" | "int8") {
        return ProtobufKind::Long;
    }
    if matches!(token, "real" | "float" | "double") {
        return ProtobufKind::Double;
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
        return ProtobufKind::Bytes;
    }
    ProtobufKind::String
}

fn build_key_message(
    row: &Row,
    fields: &[&AdjustedField<'_>],
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    let mut message = DynamicMessage::new(descriptor.clone());
    for field in fields {
        let value = row.get(&field.source.name).ok_or_else(|| {
            Error::Encoding(format!(
                "Protobuf key is missing required field {:?}",
                field.source.name
            ))
        })?;
        if matches!(value, DataValue::Null | DataValue::Unavailable) {
            return Err(Error::Encoding(format!(
                "Protobuf key field {:?} is null or unavailable",
                field.source.name
            )));
        }
        let value = key_value_to_proto(value, &field.kind, unavailable).map_err(|error| {
            Error::Encoding(format!(
                "Protobuf key field {:?} cannot be encoded: {error}",
                field.source.name
            ))
        })?;
        set_field(&mut message, &field.name, value)?;
    }
    Ok(message)
}

fn build_envelope_message(
    event: &ChangeEvent,
    fields: &[AdjustedField<'_>],
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    let mut envelope = DynamicMessage::new(descriptor.clone());
    if let Some(before) = &event.before {
        let value_descriptor = message_field_descriptor(descriptor, "before")?;
        let value = build_row_message(before, fields, &value_descriptor, unavailable)?;
        set_field(&mut envelope, "before", ProtoValue::Message(value))?;
    }
    if let Some(after) = &event.after {
        let value_descriptor = message_field_descriptor(descriptor, "after")?;
        let value = build_row_message(after, fields, &value_descriptor, unavailable)?;
        set_field(&mut envelope, "after", ProtoValue::Message(value))?;
    }
    let source_descriptor = message_field_descriptor(descriptor, "source")?;
    let source = build_source_message(event, &source_descriptor)?;
    set_field(&mut envelope, "source", ProtoValue::Message(source))?;
    set_field(
        &mut envelope,
        "op",
        ProtoValue::String(operation_code(event.operation).into()),
    )?;
    set_field(
        &mut envelope,
        "ts_ms",
        ProtoValue::I64(event.observed_time.timestamp_millis()),
    )?;
    if let Some(transaction) = &event.transaction {
        let descriptor = message_field_descriptor(descriptor, "transaction")?;
        let mut message = DynamicMessage::new(descriptor);
        set_field(
            &mut message,
            "id",
            ProtoValue::String(transaction.id.clone()),
        )?;
        if let Some(value) = transaction.total_order {
            set_field(&mut message, "total_order", ProtoValue::U64(value))?;
        }
        if let Some(value) = transaction.collection_order {
            set_field(
                &mut message,
                "data_collection_order",
                ProtoValue::U64(value),
            )?;
        }
        set_field(&mut envelope, "transaction", ProtoValue::Message(message))?;
    }
    Ok(envelope)
}

fn build_row_message(
    row: &Row,
    fields: &[AdjustedField<'_>],
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    let mut message = DynamicMessage::new(descriptor.clone());
    for field in fields {
        let value = match row.get(&field.source.name) {
            Some(DataValue::Null) if field.source.optional => continue,
            Some(DataValue::Null) => {
                return Err(Error::Encoding(format!(
                    "Protobuf record has null for required field {:?}",
                    field.source.name
                )));
            }
            Some(value) => value,
            None if field.source.optional => continue,
            None => {
                return Err(Error::Encoding(format!(
                    "Protobuf record is missing required field {:?}",
                    field.source.name
                )));
            }
        };
        let wrapper_descriptor = message_field_descriptor(descriptor, &field.name)?;
        let wrapper = build_field_wrapper(value, &field.kind, &wrapper_descriptor, unavailable)
            .map_err(|error| {
                Error::Encoding(format!(
                    "Protobuf field {:?} cannot be encoded: {error}",
                    field.source.name
                ))
            })?;
        set_field(&mut message, &field.name, ProtoValue::Message(wrapper))?;
    }
    Ok(message)
}

fn build_field_wrapper(
    value: &DataValue,
    kind: &ProtobufKind,
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    let mut wrapper = DynamicMessage::new(descriptor.clone());
    if matches!(value, DataValue::Unavailable) {
        set_field(
            &mut wrapper,
            "unavailable",
            ProtoValue::String(unavailable.into()),
        )?;
        return Ok(wrapper);
    }
    if let DataValue::String(value) = value
        && !matches!(kind, ProtobufKind::String)
    {
        set_field(&mut wrapper, "text", ProtoValue::String(value.clone()))?;
        return Ok(wrapper);
    }
    let native = match kind {
        ProtobufKind::Array(_) | ProtobufKind::Map => {
            let descriptor = message_field_descriptor(descriptor, "value")?;
            ProtoValue::Message(build_container_message(
                value,
                kind,
                &descriptor,
                unavailable,
            )?)
        }
        _ => scalar_value_to_proto(value, kind, unavailable)?,
    };
    set_field(&mut wrapper, "value", native)?;
    Ok(wrapper)
}

fn build_container_message(
    value: &DataValue,
    kind: &ProtobufKind,
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    match (kind, value) {
        (ProtobufKind::Array(element), DataValue::Array(values)) => {
            let mut array = DynamicMessage::new(descriptor.clone());
            let values_field = descriptor
                .get_field_by_name("values")
                .ok_or_else(|| Error::Encoding("Protobuf array has no values field".into()))?;
            let item_descriptor = repeated_message_descriptor(&values_field)?;
            let items = values
                .iter()
                .map(|value| {
                    build_array_item(value, element, &item_descriptor, unavailable)
                        .map(ProtoValue::Message)
                })
                .collect::<Result<Vec<_>>>()?;
            array
                .try_set_field(&values_field, ProtoValue::List(items))
                .map_err(|error| {
                    Error::Encoding(format!("invalid Protobuf array values: {error:?}"))
                })?;
            Ok(array)
        }
        (ProtobufKind::Map, DataValue::Map(values)) => {
            let mut map_message = DynamicMessage::new(descriptor.clone());
            let values_field = descriptor
                .get_field_by_name("values")
                .ok_or_else(|| Error::Encoding("Protobuf map has no values field".into()))?;
            let entry_descriptor = map_value_message_descriptor(&values_field)?;
            let entries = values
                .iter()
                .map(|(key, value)| {
                    let entry = build_map_entry(value, &entry_descriptor, unavailable)?;
                    Ok((MapKey::String(key.clone()), ProtoValue::Message(entry)))
                })
                .collect::<Result<HashMap<_, _>>>()?;
            map_message
                .try_set_field(&values_field, ProtoValue::Map(entries))
                .map_err(|error| {
                    Error::Encoding(format!("invalid Protobuf map values: {error:?}"))
                })?;
            Ok(map_message)
        }
        _ => Err(Error::Encoding(format!(
            "expected container {kind:?}, got {value:?}"
        ))),
    }
}

fn build_array_item(
    value: &DataValue,
    kind: &ProtobufKind,
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    let mut item = DynamicMessage::new(descriptor.clone());
    match value {
        DataValue::Null => set_field(&mut item, "null_value", ProtoValue::Bool(true))?,
        DataValue::Unavailable => set_field(
            &mut item,
            "unavailable",
            ProtoValue::String(unavailable.into()),
        )?,
        DataValue::String(value) if !matches!(kind, ProtobufKind::String) => {
            set_field(&mut item, "text", ProtoValue::String(value.clone()))?;
        }
        _ if matches!(kind, ProtobufKind::Array(_) | ProtobufKind::Map) => {
            let descriptor = message_field_descriptor(descriptor, "value")?;
            let nested = build_container_message(value, kind, &descriptor, unavailable)?;
            set_field(&mut item, "value", ProtoValue::Message(nested))?;
        }
        _ => set_field(
            &mut item,
            "value",
            scalar_value_to_proto(value, kind, unavailable)?,
        )?,
    }
    Ok(item)
}

fn build_map_entry(
    value: &DataValue,
    descriptor: &MessageDescriptor,
    unavailable: &str,
) -> Result<DynamicMessage> {
    let mut entry = DynamicMessage::new(descriptor.clone());
    match value {
        DataValue::Null => set_field(&mut entry, "null_value", ProtoValue::Bool(true))?,
        DataValue::Unavailable => set_field(
            &mut entry,
            "unavailable",
            ProtoValue::String(unavailable.into()),
        )?,
        _ => set_field(
            &mut entry,
            "value",
            ProtoValue::String(value_as_string(value, unavailable)?),
        )?,
    }
    Ok(entry)
}

fn build_source_message(
    event: &ChangeEvent,
    descriptor: &MessageDescriptor,
) -> Result<DynamicMessage> {
    let mut source = DynamicMessage::new(descriptor.clone());
    set_field(
        &mut source,
        "version",
        ProtoValue::String(event.source.version.clone()),
    )?;
    set_field(
        &mut source,
        "connector",
        ProtoValue::String(event.source.connector.clone()),
    )?;
    set_field(
        &mut source,
        "name",
        ProtoValue::String(event.source.connector_name.clone()),
    )?;
    if let Some(time) = event.source_time {
        set_field(
            &mut source,
            "ts_ms",
            ProtoValue::I64(time.timestamp_millis()),
        )?;
    }
    set_field(
        &mut source,
        "snapshot",
        ProtoValue::String(snapshot_marker(event).into()),
    )?;
    set_field(
        &mut source,
        "db",
        ProtoValue::String(event.source.database.clone()),
    )?;
    if let Some(schema) = &event.source.schema {
        set_field(&mut source, "schema", ProtoValue::String(schema.clone()))?;
    }
    if let Some(table) = &event.source.table {
        set_field(&mut source, "table", ProtoValue::String(table.clone()))?;
    }
    match &event.position {
        SourcePosition::Postgres(position) => {
            set_field(
                &mut source,
                "sequence",
                ProtoValue::String(serde_json::to_string(&[
                    position.commit_lsn.unwrap_or(position.lsn).to_string(),
                    position.lsn.to_string(),
                ])?),
            )?;
            if let Some(transaction_id) = position.transaction_id {
                set_field(&mut source, "txId", ProtoValue::U32(transaction_id))?;
            }
            set_field(&mut source, "lsn", ProtoValue::U64(position.lsn))?;
        }
        SourcePosition::MySql(position) => {
            set_field(
                &mut source,
                "server_id",
                ProtoValue::U32(position.server_id),
            )?;
            if let Some(gtid) = &position.gtid_set {
                set_field(&mut source, "gtid", ProtoValue::String(gtid.clone()))?;
            }
            set_field(
                &mut source,
                "file",
                ProtoValue::String(position.binlog_filename.clone()),
            )?;
            set_field(
                &mut source,
                "pos",
                ProtoValue::U64(position.binlog_position),
            )?;
            set_field(&mut source, "row", ProtoValue::U64(position.event_serial))?;
        }
        SourcePosition::SqlServer(position) => {
            set_field(
                &mut source,
                "change_lsn",
                ProtoValue::String(position.change_lsn.clone()),
            )?;
            set_field(
                &mut source,
                "commit_lsn",
                ProtoValue::String(position.commit_lsn.clone()),
            )?;
            set_field(
                &mut source,
                "event_serial_no",
                ProtoValue::U64(position.event_serial),
            )?;
        }
    }
    Ok(source)
}

fn key_value_to_proto(
    value: &DataValue,
    kind: &ProtobufKind,
    unavailable: &str,
) -> Result<ProtoValue> {
    if matches!(kind, ProtobufKind::Array(_) | ProtobufKind::Map) {
        return value_as_string(value, unavailable).map(ProtoValue::String);
    }
    scalar_value_to_proto(value, kind, unavailable)
}

fn scalar_value_to_proto(
    value: &DataValue,
    kind: &ProtobufKind,
    unavailable: &str,
) -> Result<ProtoValue> {
    match kind {
        ProtobufKind::Boolean => match value {
            DataValue::Boolean(value) => Ok(ProtoValue::Bool(*value)),
            other => Err(Error::Encoding(format!("expected boolean, got {other:?}"))),
        },
        ProtobufKind::Int => match value {
            DataValue::Int32(value) => Ok(ProtoValue::I32(*value)),
            DataValue::Int64(value) => i32::try_from(*value).map(ProtoValue::I32).map_err(|_| {
                Error::Encoding(format!(
                    "integer {value} is outside the Protobuf int32 range"
                ))
            }),
            other => Err(Error::Encoding(format!("expected integer, got {other:?}"))),
        },
        ProtobufKind::Long => match value {
            DataValue::Int32(value) => Ok(ProtoValue::I64(i64::from(*value))),
            DataValue::Int64(value) => Ok(ProtoValue::I64(*value)),
            other => Err(Error::Encoding(format!("expected integer, got {other:?}"))),
        },
        ProtobufKind::UnsignedLong => match value {
            DataValue::UInt64(value) => Ok(ProtoValue::U64(*value)),
            DataValue::Int32(value) => u64::try_from(*value).map(ProtoValue::U64).map_err(|_| {
                Error::Encoding(format!(
                    "negative integer {value} cannot be encoded as uint64"
                ))
            }),
            DataValue::Int64(value) => u64::try_from(*value).map(ProtoValue::U64).map_err(|_| {
                Error::Encoding(format!(
                    "negative integer {value} cannot be encoded as uint64"
                ))
            }),
            other => Err(Error::Encoding(format!(
                "expected unsigned integer, got {other:?}"
            ))),
        },
        ProtobufKind::Double => match value {
            DataValue::Float64(value) => Ok(ProtoValue::F64(*value)),
            DataValue::Int32(value) => Ok(ProtoValue::F64(f64::from(*value))),
            DataValue::Int64(value) => Ok(ProtoValue::F64(*value as f64)),
            other => Err(Error::Encoding(format!("expected number, got {other:?}"))),
        },
        ProtobufKind::Bytes => match value {
            DataValue::Bytes(value) => Ok(ProtoValue::Bytes(Bytes::copy_from_slice(value))),
            other => Err(Error::Encoding(format!("expected bytes, got {other:?}"))),
        },
        ProtobufKind::String => value_as_string(value, unavailable).map(ProtoValue::String),
        ProtobufKind::Array(_) | ProtobufKind::Map => Err(Error::Encoding(
            "container cannot be encoded as a Protobuf scalar".into(),
        )),
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

fn set_field(message: &mut DynamicMessage, name: &str, value: ProtoValue) -> Result<()> {
    let field = message
        .descriptor()
        .get_field_by_name(name)
        .ok_or_else(|| Error::Encoding(format!("Protobuf message has no field {name:?}")))?;
    message
        .try_set_field(&field, value)
        .map_err(|error| Error::Encoding(format!("invalid Protobuf field {name:?}: {error:?}")))
}

fn message_field_descriptor(
    descriptor: &MessageDescriptor,
    name: &str,
) -> Result<MessageDescriptor> {
    let field = descriptor
        .get_field_by_name(name)
        .ok_or_else(|| Error::Encoding(format!("Protobuf message has no field {name:?}")))?;
    match field.kind() {
        Kind::Message(descriptor) => Ok(descriptor),
        kind => Err(Error::Encoding(format!(
            "Protobuf field {name:?} is {kind:?}, expected a message"
        ))),
    }
}

fn repeated_message_descriptor(
    field: &prost_reflect::FieldDescriptor,
) -> Result<MessageDescriptor> {
    match field.kind() {
        Kind::Message(descriptor) if field.is_list() => Ok(descriptor),
        kind => Err(Error::Encoding(format!(
            "Protobuf repeated field has kind {kind:?}, expected a message list"
        ))),
    }
}

fn map_value_message_descriptor(
    field: &prost_reflect::FieldDescriptor,
) -> Result<MessageDescriptor> {
    let Kind::Message(entry) = field.kind() else {
        return Err(Error::Encoding(
            "Protobuf map field does not use a map-entry message".into(),
        ));
    };
    let value = entry
        .get_field_by_name("value")
        .ok_or_else(|| Error::Encoding("Protobuf map entry has no value field".into()))?;
    match value.kind() {
        Kind::Message(descriptor) => Ok(descriptor),
        kind => Err(Error::Encoding(format!(
            "Protobuf map value has kind {kind:?}, expected a message"
        ))),
    }
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

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use chrono::Utc;
    use indexmap::indexmap;
    use rustium_core::{
        EventId, EventSchema, MySqlPosition, PostgresPosition, SourceMetadata, SqlServerPosition,
        TransactionMetadata,
    };

    use super::*;

    fn encoder(cache_capacity: usize) -> DebeziumProtobufEncoder {
        DebeziumProtobufEncoder::new(ProtobufEncoderConfig {
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
                "unsigned_value".into() => DataValue::UInt64(u64::MAX),
                "score".into() => DataValue::String("NaN".into()),
                "binary_value".into() => DataValue::Bytes(vec![1, 2, 3]),
                "counter".into() => DataValue::Unavailable,
                "tags".into() => DataValue::Array(vec![
                    DataValue::String("one".into()),
                    DataValue::Null,
                ]),
                "matrix".into() => DataValue::Array(vec![
                    DataValue::Array(vec![DataValue::Int32(1), DataValue::Null]),
                    DataValue::Array(vec![DataValue::Int32(2)]),
                ]),
                "attributes".into() => DataValue::Map(BTreeMap::from([
                    ("region".into(), DataValue::String("global".into())),
                    ("optional".into(), DataValue::Null),
                ])),
            }),
            schema: EventSchema {
                name: "inventory.app.customers.Envelope".into(),
                version: 1,
                fields: vec![
                    field("customer-id", "bigint", false, true),
                    field("name", "varchar(255)", false, false),
                    field("unsigned_value", "bigint unsigned", false, false),
                    field("score", "double precision", false, false),
                    field("binary_value", "bytea", false, false),
                    field("counter", "integer", false, false),
                    field("tags", "text[]", true, false),
                    field("matrix", "integer[][]", true, false),
                    field("attributes", "hstore", true, false),
                ],
            },
            source_time: Some(Utc::now()),
            observed_time: Utc::now(),
        }
    }

    fn connector_events() -> [(&'static str, ChangeEvent); 3] {
        let mysql = event();

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

        let mut sqlserver = event();
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

        [
            ("postgresql", postgres),
            ("mysql", mysql),
            ("sqlserver", sqlserver),
        ]
    }

    fn schema_fixture(event: &ChangeEvent) -> serde_json::Value {
        let encoded = encoder(32).encode(event).unwrap();
        let key = encoded.key_schema.unwrap();
        let value = encoded.payload_schema.unwrap();
        serde_json::json!({
            "destination": encoded.destination,
            "key": {
                "subject": key.subject,
                "schema_type": format!("{:?}", key.schema_type),
                "definition": key.definition,
            },
            "value": {
                "subject": value.subject,
                "schema_type": format!("{:?}", value.schema_type),
                "definition": value.definition,
            },
        })
    }

    fn golden_fixture(path: &str) -> serde_json::Value {
        serde_json::from_str(path).unwrap()
    }

    fn write_fixture(path: &Path, fixture: &serde_json::Value) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut contents = serde_json::to_string_pretty(fixture).unwrap();
        contents.push('\n');
        fs::write(path, contents).unwrap();
    }

    fn field(name: &str, type_name: &str, optional: bool, primary_key: bool) -> FieldSchema {
        FieldSchema {
            name: name.into(),
            type_name: type_name.into(),
            optional,
            primary_key,
        }
    }

    fn decode(encoded: &[u8], schema: &WireSchema) -> DynamicMessage {
        assert_eq!(encoded.first(), Some(&0));
        let file = protox_parse::parse("test.proto", &schema.definition).unwrap();
        let package = file.package.clone().unwrap();
        let root = file.message_type[0].name.clone().unwrap();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file).unwrap();
        let descriptor = pool
            .get_message_by_name(&format!("{package}.{root}"))
            .unwrap();
        DynamicMessage::decode(descriptor, &encoded[1..]).unwrap()
    }

    fn message_field(message: &DynamicMessage, name: &str) -> DynamicMessage {
        let value = message.get_field_by_name(name).unwrap().into_owned();
        let ProtoValue::Message(message) = value else {
            panic!("field {name:?} is not a message: {value:?}");
        };
        message
    }

    fn string_field(message: &DynamicMessage, name: &str) -> String {
        let value = message.get_field_by_name(name).unwrap().into_owned();
        let ProtoValue::String(value) = value else {
            panic!("field {name:?} is not a string: {value:?}");
        };
        value
    }

    #[test]
    fn emits_and_decodes_typed_protobuf_records() {
        let encoded = encoder(32).encode(&event()).unwrap();
        assert_eq!(encoded.destination, "inventory.app.customers");
        assert_eq!(encoded.key.as_ref().unwrap()[0], 0);
        let key_schema = encoded.key_schema.as_ref().unwrap();
        let payload_schema = encoded.payload_schema.as_ref().unwrap();
        assert_eq!(key_schema.schema_type, WireSchemaType::Protobuf);
        assert!(key_schema.definition.contains("customer_id"));

        let key = decode(encoded.key.as_ref().unwrap(), key_schema);
        assert_eq!(
            key.get_field_by_name("customer_id").unwrap().as_ref(),
            &ProtoValue::I64(7)
        );
        let payload = decode(encoded.payload.as_ref().unwrap(), payload_schema);
        let after = message_field(&payload, "after");
        let id = message_field(&after, "customer_id");
        assert_eq!(
            id.get_field_by_name("value").unwrap().as_ref(),
            &ProtoValue::I64(7)
        );
        let unsigned = message_field(&after, "unsigned_value");
        assert_eq!(
            unsigned.get_field_by_name("value").unwrap().as_ref(),
            &ProtoValue::U64(u64::MAX)
        );
        let score = message_field(&after, "score");
        assert_eq!(string_field(&score, "text"), "NaN");
        let counter = message_field(&after, "counter");
        assert_eq!(
            string_field(&counter, "unavailable"),
            "__debezium_unavailable_value"
        );
        let tags = message_field(&message_field(&after, "tags"), "value");
        let ProtoValue::List(tags) = tags.get_field_by_name("values").unwrap().into_owned() else {
            panic!("tags are not a list");
        };
        assert_eq!(tags.len(), 2);
        let ProtoValue::Message(first_tag) = &tags[0] else {
            panic!("tag item is not a message");
        };
        assert_eq!(string_field(first_tag, "value"), "one");
        let ProtoValue::Message(second_tag) = &tags[1] else {
            panic!("tag item is not a message");
        };
        assert_eq!(
            second_tag.get_field_by_name("null_value").unwrap().as_ref(),
            &ProtoValue::Bool(true)
        );
        let matrix = message_field(&message_field(&after, "matrix"), "value");
        let ProtoValue::List(rows) = matrix.get_field_by_name("values").unwrap().into_owned()
        else {
            panic!("matrix is not a list");
        };
        let ProtoValue::Message(first_row) = &rows[0] else {
            panic!("matrix row is not a message");
        };
        let first_row = message_field(first_row, "value");
        let ProtoValue::List(first_row) =
            first_row.get_field_by_name("values").unwrap().into_owned()
        else {
            panic!("matrix row values are not a list");
        };
        let ProtoValue::Message(first_value) = &first_row[0] else {
            panic!("matrix value is not a message");
        };
        assert_eq!(
            first_value.get_field_by_name("value").unwrap().as_ref(),
            &ProtoValue::I32(1)
        );
        assert_eq!(string_field(&payload, "op"), "c");
    }

    #[test]
    fn matches_protobuf_schema_golden_fixtures_for_prioritized_connectors() {
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
    #[ignore = "regenerates checked-in Protobuf schema golden fixtures"]
    fn regenerate_protobuf_schema_golden_fixtures() {
        let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        for (connector, event) in connector_events() {
            write_fixture(
                &fixture_dir.join(format!("schema-{connector}-create.json")),
                &schema_fixture(&event),
            );
        }
    }

    #[test]
    fn preserves_field_numbers_across_reordering_and_additive_evolution() {
        let encoder = encoder(4);
        let first_event = event();
        let first = encoder.encode(&first_event).unwrap();
        let mut changed = event();
        changed.schema.version = 2;
        changed.schema.fields.reverse();
        changed
            .schema
            .fields
            .push(field("email", "text", true, false));
        changed.after.as_mut().unwrap().insert(
            "email".into(),
            DataValue::String("alice@example.com".into()),
        );
        let second = encoder.encode(&changed).unwrap();
        let first_schema = first.payload_schema.as_ref().unwrap();
        let second_schema = second.payload_schema.as_ref().unwrap();
        assert_ne!(first_schema.definition, second_schema.definition);

        let first_file = protox_parse::parse("first.proto", &first_schema.definition).unwrap();
        let second_file = protox_parse::parse("second.proto", &second_schema.definition).unwrap();
        let first_value = &first_file.message_type[0].nested_type[0];
        let second_value = &second_file.message_type[0].nested_type[0];
        let first_id = first_value
            .field
            .iter()
            .find(|field| field.name.as_deref() == Some("customer_id"))
            .unwrap()
            .number;
        let second_id = second_value
            .field
            .iter()
            .find(|field| field.name.as_deref() == Some("customer_id"))
            .unwrap()
            .number;
        assert_eq!(first_id, second_id);

        let first_payload = first.payload.as_ref().unwrap();
        let first_with_new_schema = {
            let file = protox_parse::parse("reader.proto", &second_schema.definition).unwrap();
            let package = file.package.clone().unwrap();
            let mut pool = DescriptorPool::new();
            pool.add_file_descriptor_proto(file).unwrap();
            let descriptor = pool
                .get_message_by_name(&format!("{package}.Envelope"))
                .unwrap();
            DynamicMessage::decode(descriptor, &first_payload[1..]).unwrap()
        };
        let after = message_field(&first_with_new_schema, "after");
        assert!(!after.has_field_by_name("email"));

        changed.operation = Operation::Delete;
        changed.before = changed.after.take();
        let deleted = encoder.encode_batch(&changed).unwrap();
        assert_eq!(deleted.len(), 2);
        assert!(deleted[0].payload.is_some());
        assert!(deleted[1].payload.is_none());
        assert!(encoder.schemas.lock().unwrap().schemas.len() <= 4);
    }

    #[test]
    fn rejects_name_collisions_and_zero_cache_capacity() {
        let error = DebeziumProtobufEncoder::new(ProtobufEncoderConfig {
            schema_cache_capacity: 0,
            ..encoder(1).config.clone()
        })
        .err()
        .expect("zero cache capacity unexpectedly succeeded");
        assert!(error.to_string().contains("greater than zero"));

        let mut event = event();
        event
            .schema
            .fields
            .push(field("customer_id", "bigint", false, false));
        event
            .after
            .as_mut()
            .unwrap()
            .insert("customer_id".into(), DataValue::Int64(8));
        let error = encoder(16).encode(&event).unwrap_err();
        assert!(error.to_string().contains("maps both"));
    }

    #[test]
    fn encodes_connector_sources_and_heartbeat() {
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
        let source = message_field(&payload, "source");
        assert_eq!(
            source.get_field_by_name("lsn").unwrap().as_ref(),
            &ProtoValue::U64(42)
        );

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
        assert_eq!(
            string_field(&message_field(&payload, "source"), "commit_lsn"),
            "0001:0002:0003"
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
        let payload = decode(
            encoded.payload.as_ref().unwrap(),
            encoded.payload_schema.as_ref().unwrap(),
        );
        assert_eq!(
            payload.get_field_by_name("ts_ms").unwrap().as_ref(),
            &ProtoValue::I64(heartbeat.observed_time.timestamp_millis())
        );
    }
}
