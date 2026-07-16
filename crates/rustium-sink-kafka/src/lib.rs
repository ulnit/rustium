//! Durable Kafka sink.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::{
    ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use rustium_core::{DeliveryBatch, Durability, Error, Result, Sink, WireSchema, WireSchemaType};
use schema_registry_converter::{
    async_impl::schema_registry::{SrSettings, post_schema},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use url::Url;

const RESERVED_PROPERTIES: &[&str] = &[
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

pub struct KafkaSink {
    producer: FutureProducer,
    metadata_timeout: Duration,
    delivery_timeout: Duration,
    schema_registry: Option<SchemaRegistryFramer>,
}

#[derive(Debug, Clone)]
pub struct SchemaRegistrySettings {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub request_timeout: Duration,
    pub cache_capacity: usize,
}

struct SchemaRegistryFramer {
    settings: SrSettings,
    schema_ids: SchemaIdCache,
}

struct SchemaIdCache {
    ids: HashMap<WireSchema, u32>,
    order: VecDeque<WireSchema>,
    capacity: usize,
}

impl KafkaSink {
    pub fn new(
        bootstrap_servers: &[String],
        acks: &str,
        compression: &str,
        delivery_timeout: Duration,
        properties: &BTreeMap<String, String>,
    ) -> Result<Self> {
        validate_delivery_contract(bootstrap_servers, acks, delivery_timeout, properties)?;
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers.join(","))
            .set("acks", acks)
            .set("compression.type", compression)
            .set("enable.idempotence", "true")
            .set(
                "message.timeout.ms",
                delivery_timeout.as_millis().to_string(),
            );
        for (key, value) in properties {
            config.set(key, value);
        }
        let producer = config
            .create()
            .map_err(|error| Error::Configuration(format!("invalid Kafka config: {error}")))?;
        Ok(Self {
            producer,
            metadata_timeout: Duration::from_secs(10),
            delivery_timeout,
            schema_registry: None,
        })
    }

    pub fn with_schema_registry(mut self, settings: SchemaRegistrySettings) -> Result<Self> {
        self.schema_registry = Some(SchemaRegistryFramer::new(settings)?);
        Ok(self)
    }
}

impl SchemaRegistryFramer {
    fn new(settings: SchemaRegistrySettings) -> Result<Self> {
        let urls = settings
            .urls
            .iter()
            .map(|url| url.trim_end_matches('/').to_string())
            .collect::<Vec<_>>();
        let Some(first_url) = urls.first() else {
            return Err(Error::Configuration(
                "Schema Registry requires at least one URL".into(),
            ));
        };
        for raw in &urls {
            let url = Url::parse(raw).map_err(|error| {
                Error::Configuration(format!("invalid Schema Registry URL {raw:?}: {error}"))
            })?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return Err(Error::Configuration(format!(
                    "Schema Registry URL {raw:?} must be an absolute HTTP(S) URL"
                )));
            }
        }
        if settings.password.is_some() && settings.username.is_none() {
            return Err(Error::Configuration(
                "Schema Registry password requires a username".into(),
            ));
        }
        if settings.request_timeout.is_zero() {
            return Err(Error::Configuration(
                "Schema Registry request timeout must be greater than zero".into(),
            ));
        }
        if settings.cache_capacity == 0 {
            return Err(Error::Configuration(
                "Schema Registry cache capacity must be greater than zero".into(),
            ));
        }

        let mut builder = SrSettings::new_builder(first_url.clone());
        for url in urls.iter().skip(1) {
            builder.add_url(url.clone());
        }
        if let Some(username) = &settings.username {
            builder.set_basic_authorization(username, settings.password.as_deref());
        }
        builder.set_timeout(settings.request_timeout);
        let cache_capacity = settings.cache_capacity;
        let settings = builder.build().map_err(|error| {
            Error::Configuration(format!("invalid Schema Registry settings: {error}"))
        })?;
        Ok(Self {
            settings,
            schema_ids: SchemaIdCache::new(cache_capacity),
        })
    }

    async fn frame(&mut self, value: &Bytes, schema: &WireSchema) -> Result<Bytes> {
        let schema_id = if let Some(schema_id) = self.schema_ids.get(schema) {
            schema_id
        } else {
            let supplied = SuppliedSchema {
                name: None,
                schema_type: registry_schema_type(schema.schema_type),
                schema: schema.definition.clone(),
                references: Vec::new(),
                properties: None,
                tags: None,
            };
            let registered = post_schema(&self.settings, schema.subject.clone(), supplied)
                .await
                .map_err(schema_registry_error)?;
            self.schema_ids.insert(schema.clone(), registered.id);
            registered.id
        };
        Ok(Bytes::from(confluent_frame(schema_id, value)))
    }
}

impl SchemaIdCache {
    fn new(capacity: usize) -> Self {
        Self {
            ids: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn get(&mut self, schema: &WireSchema) -> Option<u32> {
        let id = *self.ids.get(schema)?;
        if let Some(index) = self.order.iter().position(|cached| cached == schema) {
            self.order.remove(index);
        }
        self.order.push_back(schema.clone());
        Some(id)
    }

    fn insert(&mut self, schema: WireSchema, id: u32) {
        if self.ids.contains_key(&schema) {
            self.get(&schema);
            self.ids.insert(schema, id);
            return;
        }
        if self.ids.len() == self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.ids.remove(&evicted);
        }
        self.order.push_back(schema.clone());
        self.ids.insert(schema, id);
    }
}

const fn registry_schema_type(schema_type: WireSchemaType) -> SchemaType {
    match schema_type {
        WireSchemaType::Avro => SchemaType::Avro,
        WireSchemaType::Json => SchemaType::Json,
        WireSchemaType::Protobuf => SchemaType::Protobuf,
    }
}

fn schema_registry_error(error: schema_registry_converter::error::SRCError) -> Error {
    if error.retriable {
        Error::RetryableSink(format!("Schema Registry request failed: {error}"))
    } else {
        Error::Sink(format!("Schema Registry rejected the schema: {error}"))
    }
}

fn confluent_frame(schema_id: u32, value: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + value.len());
    framed.push(0);
    framed.extend_from_slice(&schema_id.to_be_bytes());
    framed.extend_from_slice(value);
    framed
}

async fn frame_component(
    registry: Option<&mut SchemaRegistryFramer>,
    value: Option<&Bytes>,
    schema: Option<&WireSchema>,
    component: &str,
) -> Result<Option<Bytes>> {
    match (value, schema, registry) {
        (None, _, _) => Ok(None),
        (Some(value), None, _) => Ok(Some(value.clone())),
        (Some(value), Some(schema), Some(registry)) => {
            registry.frame(value, schema).await.map(Some)
        }
        (Some(_), Some(_), None) => Err(Error::Sink(format!(
            "Kafka {component} requires Schema Registry settings"
        ))),
    }
}

fn validate_delivery_contract(
    bootstrap_servers: &[String],
    acks: &str,
    delivery_timeout: Duration,
    properties: &BTreeMap<String, String>,
) -> Result<()> {
    if bootstrap_servers.is_empty()
        || bootstrap_servers
            .iter()
            .any(|server| server.trim().is_empty())
    {
        return Err(Error::Configuration(
            "Kafka bootstrap servers must contain at least one non-empty address".into(),
        ));
    }
    if !matches!(acks, "all" | "-1") {
        return Err(Error::Configuration(
            "Kafka Sink requires acks=all (or -1) so a batch is replicated before checkpointing"
                .into(),
        ));
    }
    let timeout_millis = delivery_timeout.as_millis();
    if timeout_millis == 0 || timeout_millis > i32::MAX as u128 {
        return Err(Error::Configuration(format!(
            "Kafka delivery timeout must be between 1 and {} milliseconds",
            i32::MAX
        )));
    }
    if let Some(property) = properties
        .keys()
        .find(|property| RESERVED_PROPERTIES.contains(&property.as_str()))
    {
        return Err(Error::Configuration(format!(
            "Kafka property {property:?} is managed by Rustium and cannot be overridden"
        )));
    }
    Ok(())
}

#[async_trait]
impl Sink for KafkaSink {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn durability(&self) -> Durability {
        Durability::Durable
    }

    async fn validate(&mut self) -> Result<()> {
        let producer = self.producer.clone();
        let timeout = self.metadata_timeout;
        tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(None, Timeout::After(timeout))
                .map(|_| ())
                .map_err(|error| {
                    Error::RetryableSink(format!("Kafka metadata request failed: {error}"))
                })
        })
        .await
        .map_err(|error| Error::Sink(format!("Kafka validation task failed: {error}")))?
    }

    async fn write(&mut self, batch: &DeliveryBatch) -> Result<()> {
        for event in &batch.events {
            let key = frame_component(
                self.schema_registry.as_mut(),
                event.key.as_ref(),
                event.key_schema.as_ref(),
                "key",
            )
            .await?;
            let payload = frame_component(
                self.schema_registry.as_mut(),
                event.payload.as_ref(),
                event.payload_schema.as_ref(),
                "value",
            )
            .await?;
            let mut headers = OwnedHeaders::new();
            for (key, value) in &event.headers {
                headers = headers.insert(Header {
                    key,
                    value: Some(value),
                });
            }
            let mut record = FutureRecord::to(&event.destination).headers(headers);
            if let Some(payload) = &payload {
                record = record.payload(payload.as_ref());
            }
            if let Some(key) = &key {
                record = record.key(key.as_ref());
            }
            self.producer
                .send(record, Timeout::After(self.delivery_timeout))
                .await
                .map_err(|(error, _)| {
                    Error::RetryableSink(format!("Kafka delivery failed: {error}"))
                })?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.producer
            .flush(Timeout::After(self.delivery_timeout))
            .map_err(|error| Error::RetryableSink(format!("Kafka flush failed: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use mockito::Matcher;

    use super::*;

    fn bootstrap_servers() -> Vec<String> {
        vec!["127.0.0.1:9092".into()]
    }

    #[test]
    fn rejects_non_durable_acknowledgements() {
        for acks in ["0", "1"] {
            let error = KafkaSink::new(
                &bootstrap_servers(),
                acks,
                "none",
                Duration::from_secs(1),
                &BTreeMap::new(),
            )
            .err()
            .expect("non-durable Kafka acknowledgements must fail");
            assert!(error.to_string().contains("requires acks=all"));
        }
    }

    #[test]
    fn rejects_reserved_property_overrides() {
        for property in RESERVED_PROPERTIES {
            let properties = BTreeMap::from([(property.to_string(), "override".into())]);
            let error = KafkaSink::new(
                &bootstrap_servers(),
                "all",
                "none",
                Duration::from_secs(1),
                &properties,
            )
            .err()
            .expect("reserved Kafka property must fail");
            assert!(error.to_string().contains("cannot be overridden"));
        }
    }

    #[test]
    fn rejects_invalid_delivery_timeouts() {
        for timeout in [Duration::ZERO, Duration::from_millis(i32::MAX as u64 + 1)] {
            let error = KafkaSink::new(
                &bootstrap_servers(),
                "all",
                "none",
                timeout,
                &BTreeMap::new(),
            )
            .err()
            .expect("invalid Kafka delivery timeout must fail");
            assert!(error.to_string().contains("delivery timeout"));
        }
    }

    #[test]
    fn reports_durable_delivery_for_valid_configuration() {
        let sink = KafkaSink::new(
            &bootstrap_servers(),
            "-1",
            "none",
            Duration::from_secs(1),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(sink.durability(), Durability::Durable);
    }

    #[test]
    fn builds_confluent_schema_registry_framing() {
        assert_eq!(
            confluent_frame(0x0102_0304, b"{}"),
            [0, 1, 2, 3, 4, b'{', b'}']
        );
    }

    #[test]
    fn rejects_invalid_schema_registry_settings() {
        let sink = KafkaSink::new(
            &bootstrap_servers(),
            "all",
            "none",
            Duration::from_secs(1),
            &BTreeMap::new(),
        )
        .unwrap();
        let error = sink
            .with_schema_registry(SchemaRegistrySettings {
                urls: Vec::new(),
                username: None,
                password: None,
                request_timeout: Duration::from_secs(1),
                cache_capacity: 1,
            })
            .err()
            .expect("empty Schema Registry URL list must fail");
        assert!(error.to_string().contains("at least one URL"));
    }

    #[tokio::test]
    async fn registers_json_schema_once_and_frames_cached_values() {
        let mut server = mockito::Server::new_async().await;
        let definition =
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
        let registration = server
            .mock("POST", "/subjects/orders-value/versions")
            .match_header("content-type", "application/vnd.schemaregistry.v1+json")
            .match_header("authorization", "Basic dXNlcjpwYXNz")
            .match_body(Matcher::PartialJson(serde_json::json!({
                "schema": definition,
                "schemaType": "JSON"
            })))
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":23}"#)
            .expect(1)
            .create_async()
            .await;
        let mut framer = SchemaRegistryFramer::new(SchemaRegistrySettings {
            urls: vec![format!("{}/", server.url())],
            username: Some("user".into()),
            password: Some("pass".into()),
            request_timeout: Duration::from_secs(1),
            cache_capacity: 8,
        })
        .unwrap();
        let schema = WireSchema {
            subject: "orders-value".into(),
            schema_type: WireSchemaType::Json,
            definition: definition.into(),
        };

        let first = framer
            .frame(&Bytes::from_static(br#"{"id":1}"#), &schema)
            .await
            .unwrap();
        let second = framer
            .frame(&Bytes::from_static(br#"{"id":2}"#), &schema)
            .await
            .unwrap();

        assert_eq!(&first[..5], &[0, 0, 0, 0, 23]);
        assert_eq!(&first[5..], br#"{"id":1}"#);
        assert_eq!(&second[..5], &[0, 0, 0, 0, 23]);
        assert_eq!(&second[5..], br#"{"id":2}"#);
        registration.assert_async().await;
    }

    #[tokio::test]
    async fn registers_avro_schema_and_preserves_binary_datum() {
        let mut server = mockito::Server::new_async().await;
        let definition = r#"{"type":"record","name":"Key","fields":[{"name":"id","type":"long"}]}"#;
        let registration = server
            .mock("POST", "/subjects/orders-key/versions")
            .match_body(Matcher::PartialJson(serde_json::json!({
                "schema": definition,
                "schemaType": "AVRO"
            })))
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":41}"#)
            .expect(1)
            .create_async()
            .await;
        let mut framer = SchemaRegistryFramer::new(SchemaRegistrySettings {
            urls: vec![server.url()],
            username: None,
            password: None,
            request_timeout: Duration::from_secs(1),
            cache_capacity: 8,
        })
        .unwrap();
        let framed = framer
            .frame(
                &Bytes::from_static(&[14]),
                &WireSchema {
                    subject: "orders-key".into(),
                    schema_type: WireSchemaType::Avro,
                    definition: definition.into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(framed.as_ref(), &[0, 0, 0, 0, 41, 14]);
        registration.assert_async().await;
    }

    #[tokio::test]
    async fn registers_protobuf_schema_and_preserves_message_index() {
        let mut server = mockito::Server::new_async().await;
        let definition = r#"syntax = "proto3"; message Key { int64 id = 1; }"#;
        let registration = server
            .mock("POST", "/subjects/orders-key/versions")
            .match_body(Matcher::PartialJson(serde_json::json!({
                "schema": definition,
                "schemaType": "PROTOBUF"
            })))
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":53}"#)
            .expect(1)
            .create_async()
            .await;
        let mut framer = SchemaRegistryFramer::new(SchemaRegistrySettings {
            urls: vec![server.url()],
            username: None,
            password: None,
            request_timeout: Duration::from_secs(1),
            cache_capacity: 8,
        })
        .unwrap();
        let framed = framer
            .frame(
                &Bytes::from_static(&[0, 8, 14]),
                &WireSchema {
                    subject: "orders-key".into(),
                    schema_type: WireSchemaType::Protobuf,
                    definition: definition.into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(framed.as_ref(), &[0, 0, 0, 0, 53, 0, 8, 14]);
        registration.assert_async().await;
    }

    #[tokio::test]
    async fn treats_registry_compatibility_rejections_as_non_retryable() {
        let mut server = mockito::Server::new_async().await;
        let rejection = server
            .mock("POST", "/subjects/orders-value/versions")
            .with_status(409)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":409,"message":"incompatible schema"}"#)
            .create_async()
            .await;
        let mut framer = SchemaRegistryFramer::new(SchemaRegistrySettings {
            urls: vec![server.url()],
            username: None,
            password: None,
            request_timeout: Duration::from_secs(1),
            cache_capacity: 8,
        })
        .unwrap();
        let error = framer
            .frame(
                &Bytes::from_static(b"{}"),
                &WireSchema {
                    subject: "orders-value".into(),
                    schema_type: WireSchemaType::Json,
                    definition: r#"{"type":"object"}"#.into(),
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Sink(_)));
        assert!(error.to_string().contains("rejected the schema"));
        rejection.assert_async().await;
    }

    #[test]
    fn bounds_schema_id_cache_with_lru_eviction() {
        let schema = |subject: &str| WireSchema {
            subject: subject.into(),
            schema_type: WireSchemaType::Json,
            definition: format!(r#"{{"title":"{subject}"}}"#),
        };
        let first = schema("first-value");
        let second = schema("second-value");
        let third = schema("third-value");
        let mut cache = SchemaIdCache::new(2);
        cache.insert(first.clone(), 1);
        cache.insert(second.clone(), 2);
        assert_eq!(cache.get(&first), Some(1));
        cache.insert(third.clone(), 3);

        assert_eq!(cache.get(&second), None);
        assert_eq!(cache.get(&first), Some(1));
        assert_eq!(cache.get(&third), Some(3));
        assert_eq!(cache.ids.len(), 2);
    }
}
