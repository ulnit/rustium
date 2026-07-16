use std::{collections::BTreeMap, env, io, time::Duration};

use apache_avro::{Schema, from_avro_datum, types::Value as AvroValue};
use chrono::Utc;
use prost_reflect::{DescriptorPool, DynamicMessage, Value as ProtobufValue};
use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    message::{Headers, OwnedMessage},
};
use rustium_core::{
    ChangeEvent, DataValue, DeliveryBatch, EncodedEvent, EventEncoder, EventId, EventSchema,
    FieldSchema, MySqlPosition, Operation, Sink, SourceMetadata, SourcePosition,
};
use rustium_format_avro::{AvroEncoderConfig, DebeziumAvroEncoder};
use rustium_format_json::{DebeziumJsonSchemaEncoder, JsonEncoderConfig};
use rustium_format_protobuf::{DebeziumProtobufEncoder, ProtobufEncoderConfig};
use rustium_sink_kafka::{KafkaSink, SchemaRegistrySettings};
use schema_registry_converter::{
    async_impl::schema_registry::{SrSettings, get_schema_by_id, get_schema_by_subject},
    schema_registry_common::{SchemaType, SubjectNameStrategy},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external Kafka-compatible broker"]
async fn delivers_records_tombstones_and_broker_failures() -> TestResult {
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic = format!("rustium-sink-{}", &suffix[..12]);
    let missing_topic = format!("rustium-sink-missing-{}", &suffix[..12]);
    let group_id = format!("rustium-sink-observer-{}", &suffix[..12]);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let topic_spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let created = admin
        .create_topics(
            [&topic_spec],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?;
    require(
        matches!(created.as_slice(), [Ok(created)] if created == &topic),
        "Kafka Sink test topic was not created",
    )?;

    let outcome = async {
        let properties = BTreeMap::from([("allow.auto.create.topics".into(), "false".into())]);
        let mut sink = KafkaSink::new(
            &[bootstrap_servers.clone()],
            "all",
            "none",
            Duration::from_secs(3),
            &properties,
        )?;
        sink.validate().await?;

        let batch = DeliveryBatch {
            events: vec![
                encoded_event("event-1", &topic, Some(b"created")),
                encoded_event("event-2", &topic, None),
            ],
            highest_position: position(2),
        };
        sink.write(&batch).await?;
        sink.flush().await?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let first = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
            .await??
            .detach();
        let second = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
            .await??
            .detach();

        require(
            first.partition() == 0,
            "Kafka Sink record used the wrong partition",
        )?;
        require(
            first.offset() == 0,
            "Kafka Sink record used the wrong offset",
        )?;
        require(
            first.key() == Some(b"order-1"),
            "Kafka Sink record lost its key",
        )?;
        require(
            first.payload() == Some(b"created"),
            "Kafka Sink record lost its payload",
        )?;
        require_header(&first, "event-1")?;

        require(
            second.partition() == 0,
            "Kafka tombstone used the wrong partition",
        )?;
        require(
            second.offset() == 1,
            "Kafka tombstone used the wrong offset",
        )?;
        require(
            second.key() == Some(b"order-1"),
            "Kafka tombstone lost its key",
        )?;
        require(
            second.payload().is_none(),
            "Kafka tombstone was not a null value",
        )?;
        require_header(&second, "event-2")?;
        drop(consumer);

        let failed = sink
            .write(&DeliveryBatch {
                events: vec![encoded_event("event-3", &missing_topic, Some(b"rejected"))],
                highest_position: position(3),
            })
            .await
            .expect_err("delivery to a missing topic unexpectedly succeeded");
        require(
            failed.to_string().contains("Kafka delivery failed"),
            "Kafka Sink did not expose the broker delivery failure",
        )?;
        sink.shutdown().await?;
        TestResult::Ok(())
    }
    .await;

    let deleted = admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await;
    if outcome.is_ok() {
        let deleted = deleted?;
        require(
            matches!(deleted.as_slice(), [Ok(deleted)] if deleted == &topic),
            "Kafka Sink test topic was not deleted",
        )?;
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external Kafka-compatible broker and Schema Registry"]
async fn registers_evolves_and_delivers_json_schema_records() -> TestResult {
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let registry_url = required_env("RUSTIUM_SCHEMA_REGISTRY_TEST_URL")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic_prefix = format!("rustium-schema-registry-{}", &suffix[..12]);
    let topic = format!("{topic_prefix}.app.customers");
    let group_id = format!("rustium-schema-observer-{}", &suffix[..12]);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let topic_spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let created = admin
        .create_topics(
            [&topic_spec],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?;
    require(
        matches!(created.as_slice(), [Ok(created)] if created == &topic),
        "Schema Registry test topic was not created",
    )?;

    let outcome = async {
        let properties = BTreeMap::from([("allow.auto.create.topics".into(), "false".into())]);
        let mut sink = KafkaSink::new(
            &[bootstrap_servers.clone()],
            "all",
            "none",
            Duration::from_secs(5),
            &properties,
        )?
        .with_schema_registry(SchemaRegistrySettings {
            urls: vec![registry_url.clone()],
            username: None,
            password: None,
            request_timeout: Duration::from_secs(5),
            cache_capacity: 128,
        })?;
        sink.validate().await?;

        let encoder = DebeziumJsonSchemaEncoder::new(JsonEncoderConfig {
            topic_prefix: topic_prefix.clone(),
            unavailable_value: "__unavailable".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        });
        let first = encoder.encode(&schema_change_event(&topic_prefix, 1, 1, None))?;
        let second = encoder.encode(&schema_change_event(
            &topic_prefix,
            2,
            2,
            Some("bob@example.com"),
        ))?;
        let mut deleted_event = schema_change_event(&topic_prefix, 2, 3, Some("bob@example.com"));
        deleted_event.operation = Operation::Delete;
        deleted_event.before = deleted_event.after.take();
        let deleted = encoder.encode_batch(&deleted_event)?;
        require(
            deleted.len() == 2,
            "JSON Schema delete did not produce envelope plus tombstone",
        )?;
        let mut events = vec![first, second];
        events.extend(deleted);
        sink.write(&DeliveryBatch {
            events,
            highest_position: position(4),
        })
        .await?;
        sink.flush().await?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let mut messages = Vec::new();
        for _ in 0..4 {
            messages.push(
                tokio::time::timeout(Duration::from_secs(10), consumer.recv())
                    .await??
                    .detach(),
            );
        }

        let first_key = messages[0]
            .key()
            .ok_or_else(|| test_error("first schema record lost its key"))?;
        let second_key = messages[1]
            .key()
            .ok_or_else(|| test_error("second schema record lost its key"))?;
        let first_value = messages[0]
            .payload()
            .ok_or_else(|| test_error("first schema record lost its payload"))?;
        let second_value = messages[1]
            .payload()
            .ok_or_else(|| test_error("second schema record lost its payload"))?;
        let delete_key = messages[2]
            .key()
            .ok_or_else(|| test_error("schema delete envelope lost its key"))?;
        let tombstone_key = messages[3]
            .key()
            .ok_or_else(|| test_error("schema tombstone lost its key"))?;
        require(
            messages[3].payload().is_none(),
            "schema tombstone is not null",
        )?;
        let key_id = framed_schema_id(first_key)?;
        require(
            framed_schema_id(second_key)? == key_id
                && framed_schema_id(delete_key)? == key_id
                && framed_schema_id(tombstone_key)? == key_id,
            "stable key schema did not reuse its registry ID",
        )?;
        let value_v1_id = framed_schema_id(first_value)?;
        let value_v2_id = framed_schema_id(second_value)?;
        let delete_value = messages[2]
            .payload()
            .ok_or_else(|| test_error("schema delete envelope lost its payload"))?;
        require(
            value_v1_id != value_v2_id,
            "evolved value schema did not receive a new registry ID",
        )?;
        require(
            framed_schema_id(delete_value)? == value_v2_id,
            "delete envelope did not reuse the latest value schema",
        )?;
        require(
            framed_json(first_value)?["after"]
                == serde_json::json!({"id": 1, "name": "Customer 1"}),
            "first framed JSON value changed",
        )?;
        require(
            framed_json(second_value)?["after"]
                == serde_json::json!({
                    "id": 2,
                    "name": "Customer 2",
                    "email": "bob@example.com"
                }),
            "second framed JSON value changed",
        )?;
        require(
            framed_json(delete_value)?["op"] == "d",
            "delete envelope operation changed",
        )?;

        let settings = SrSettings::new(registry_url.clone());
        let registered_key = get_schema_by_id(key_id, &settings).await?;
        let registered_v1 = get_schema_by_id(value_v1_id, &settings).await?;
        let registered_v2 = get_schema_by_id(value_v2_id, &settings).await?;
        require(
            registered_key.schema_type == SchemaType::Json
                && registered_v1.schema_type == SchemaType::Json
                && registered_v2.schema_type == SchemaType::Json,
            "registry did not preserve JSON Schema types",
        )?;
        let latest = get_schema_by_subject(
            &settings,
            &SubjectNameStrategy::TopicNameStrategy(topic.clone(), false),
        )
        .await?;
        require(
            latest.id == value_v2_id,
            "latest value subject does not point at the evolved schema",
        )?;
        sink.shutdown().await?;
        TestResult::Ok(())
    }
    .await;

    let deleted = admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await;
    if outcome.is_ok() {
        let deleted = deleted?;
        require(
            matches!(deleted.as_slice(), [Ok(deleted)] if deleted == &topic),
            "Schema Registry test topic was not deleted",
        )?;
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external Kafka-compatible broker and Schema Registry"]
async fn registers_evolves_and_delivers_avro_records() -> TestResult {
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let registry_url = required_env("RUSTIUM_SCHEMA_REGISTRY_TEST_URL")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic_prefix = format!("rustium-avro-{}", &suffix[..12]);
    let topic = format!("{topic_prefix}.app.customers");
    let group_id = format!("rustium-avro-observer-{}", &suffix[..12]);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let topic_spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let created = admin
        .create_topics(
            [&topic_spec],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?;
    require(
        matches!(created.as_slice(), [Ok(created)] if created == &topic),
        "Avro test topic was not created",
    )?;

    let outcome = async {
        let properties = BTreeMap::from([("allow.auto.create.topics".into(), "false".into())]);
        let mut sink = KafkaSink::new(
            &[bootstrap_servers.clone()],
            "all",
            "none",
            Duration::from_secs(5),
            &properties,
        )?
        .with_schema_registry(SchemaRegistrySettings {
            urls: vec![registry_url.clone()],
            username: None,
            password: None,
            request_timeout: Duration::from_secs(5),
            cache_capacity: 128,
        })?;
        sink.validate().await?;

        let encoder = DebeziumAvroEncoder::new(AvroEncoderConfig {
            topic_prefix: topic_prefix.clone(),
            unavailable_value: "__debezium_unavailable_value".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            schema_cache_capacity: 128,
        })?;
        let first = encoder.encode(&schema_change_event(&topic_prefix, 1, 11, None))?;
        let second = encoder.encode(&schema_change_event(
            &topic_prefix,
            2,
            12,
            Some("bob@example.com"),
        ))?;
        let mut deleted_event = schema_change_event(&topic_prefix, 2, 13, Some("bob@example.com"));
        deleted_event.operation = Operation::Delete;
        deleted_event.before = deleted_event.after.take();
        let deleted = encoder.encode_batch(&deleted_event)?;
        require(
            deleted.len() == 2,
            "Avro delete did not produce envelope plus tombstone",
        )?;
        let mut events = vec![first, second];
        events.extend(deleted);
        sink.write(&DeliveryBatch {
            events,
            highest_position: position(14),
        })
        .await?;
        sink.flush().await?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let mut messages = Vec::new();
        for _ in 0..4 {
            messages.push(
                tokio::time::timeout(Duration::from_secs(10), consumer.recv())
                    .await??
                    .detach(),
            );
        }

        let first_key = messages[0]
            .key()
            .ok_or_else(|| test_error("first Avro record lost its key"))?;
        let second_key = messages[1]
            .key()
            .ok_or_else(|| test_error("second Avro record lost its key"))?;
        let first_value = messages[0]
            .payload()
            .ok_or_else(|| test_error("first Avro record lost its payload"))?;
        let second_value = messages[1]
            .payload()
            .ok_or_else(|| test_error("second Avro record lost its payload"))?;
        let delete_key = messages[2]
            .key()
            .ok_or_else(|| test_error("Avro delete envelope lost its key"))?;
        let tombstone_key = messages[3]
            .key()
            .ok_or_else(|| test_error("Avro tombstone lost its key"))?;
        require(
            messages[3].payload().is_none(),
            "Avro tombstone is not null",
        )?;
        let key_id = framed_schema_id(first_key)?;
        require(
            framed_schema_id(second_key)? == key_id
                && framed_schema_id(delete_key)? == key_id
                && framed_schema_id(tombstone_key)? == key_id,
            "stable Avro key schema did not reuse its registry ID",
        )?;
        let value_v1_id = framed_schema_id(first_value)?;
        let value_v2_id = framed_schema_id(second_value)?;
        let delete_value = messages[2]
            .payload()
            .ok_or_else(|| test_error("Avro delete envelope lost its payload"))?;
        require(
            value_v1_id != value_v2_id,
            "evolved Avro value schema did not receive a new registry ID",
        )?;
        require(
            framed_schema_id(delete_value)? == value_v2_id,
            "Avro delete envelope did not reuse the latest value schema",
        )?;

        let settings = SrSettings::new(registry_url.clone());
        let registered_key = get_schema_by_id(key_id, &settings).await?;
        let registered_v1 = get_schema_by_id(value_v1_id, &settings).await?;
        let registered_v2 = get_schema_by_id(value_v2_id, &settings).await?;
        require(
            registered_key.schema_type == SchemaType::Avro
                && registered_v1.schema_type == SchemaType::Avro
                && registered_v2.schema_type == SchemaType::Avro,
            "registry did not preserve Avro schema types",
        )?;
        let first_decoded = framed_avro(first_value, &registered_v1.schema)?;
        let second_decoded = framed_avro(second_value, &registered_v2.schema)?;
        let delete_decoded = framed_avro(delete_value, &registered_v2.schema)?;
        require(
            avro_union(avro_field(
                avro_union(avro_field(&first_decoded, "after")?)?,
                "id",
            )?)? == &AvroValue::Long(11),
            "first framed Avro value changed",
        )?;
        require(
            avro_union(avro_field(
                avro_union(avro_field(&second_decoded, "after")?)?,
                "email",
            )?)? == &AvroValue::String("bob@example.com".into()),
            "evolved framed Avro value changed",
        )?;
        require(
            avro_field(&delete_decoded, "op")? == &AvroValue::String("d".into()),
            "Avro delete envelope operation changed",
        )?;
        let latest = get_schema_by_subject(
            &settings,
            &SubjectNameStrategy::TopicNameStrategy(topic.clone(), false),
        )
        .await?;
        require(
            latest.id == value_v2_id,
            "latest Avro value subject does not point at the evolved schema",
        )?;
        sink.shutdown().await?;
        TestResult::Ok(())
    }
    .await;

    let deleted = admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await;
    if outcome.is_ok() {
        let deleted = deleted?;
        require(
            matches!(deleted.as_slice(), [Ok(deleted)] if deleted == &topic),
            "Avro test topic was not deleted",
        )?;
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external Kafka-compatible broker and Schema Registry"]
async fn registers_evolves_and_delivers_protobuf_records() -> TestResult {
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let registry_url = required_env("RUSTIUM_SCHEMA_REGISTRY_TEST_URL")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic_prefix = format!("rustium-protobuf-{}", &suffix[..12]);
    let topic = format!("{topic_prefix}.app.customers");
    let group_id = format!("rustium-protobuf-observer-{}", &suffix[..12]);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let topic_spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let created = admin
        .create_topics(
            [&topic_spec],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?;
    require(
        matches!(created.as_slice(), [Ok(created)] if created == &topic),
        "Protobuf test topic was not created",
    )?;

    let outcome = async {
        let properties = BTreeMap::from([("allow.auto.create.topics".into(), "false".into())]);
        let mut sink = KafkaSink::new(
            &[bootstrap_servers.clone()],
            "all",
            "none",
            Duration::from_secs(5),
            &properties,
        )?
        .with_schema_registry(SchemaRegistrySettings {
            urls: vec![registry_url.clone()],
            username: None,
            password: None,
            request_timeout: Duration::from_secs(5),
            cache_capacity: 128,
        })?;
        sink.validate().await?;

        let encoder = DebeziumProtobufEncoder::new(ProtobufEncoderConfig {
            topic_prefix: topic_prefix.clone(),
            unavailable_value: "__debezium_unavailable_value".into(),
            tombstones_on_delete: true,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            schema_cache_capacity: 128,
        })?;
        let first = encoder.encode(&schema_change_event(&topic_prefix, 1, 21, None))?;
        let second = encoder.encode(&schema_change_event(
            &topic_prefix,
            2,
            22,
            Some("bob@example.com"),
        ))?;
        let mut deleted_event = schema_change_event(&topic_prefix, 2, 23, Some("bob@example.com"));
        deleted_event.operation = Operation::Delete;
        deleted_event.before = deleted_event.after.take();
        let deleted = encoder.encode_batch(&deleted_event)?;
        require(
            deleted.len() == 2,
            "Protobuf delete did not produce envelope plus tombstone",
        )?;
        let mut events = vec![first, second];
        events.extend(deleted);
        sink.write(&DeliveryBatch {
            events,
            highest_position: position(24),
        })
        .await?;
        sink.flush().await?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let mut messages = Vec::new();
        for _ in 0..4 {
            messages.push(
                tokio::time::timeout(Duration::from_secs(10), consumer.recv())
                    .await??
                    .detach(),
            );
        }

        let first_key = messages[0]
            .key()
            .ok_or_else(|| test_error("first Protobuf record lost its key"))?;
        let second_key = messages[1]
            .key()
            .ok_or_else(|| test_error("second Protobuf record lost its key"))?;
        let first_value = messages[0]
            .payload()
            .ok_or_else(|| test_error("first Protobuf record lost its payload"))?;
        let second_value = messages[1]
            .payload()
            .ok_or_else(|| test_error("second Protobuf record lost its payload"))?;
        let delete_key = messages[2]
            .key()
            .ok_or_else(|| test_error("Protobuf delete envelope lost its key"))?;
        let tombstone_key = messages[3]
            .key()
            .ok_or_else(|| test_error("Protobuf tombstone lost its key"))?;
        require(
            messages[3].payload().is_none(),
            "Protobuf tombstone is not null",
        )?;
        let key_id = framed_schema_id(first_key)?;
        require(
            framed_schema_id(second_key)? == key_id
                && framed_schema_id(delete_key)? == key_id
                && framed_schema_id(tombstone_key)? == key_id,
            "stable Protobuf key schema did not reuse its registry ID",
        )?;
        let value_v1_id = framed_schema_id(first_value)?;
        let value_v2_id = framed_schema_id(second_value)?;
        let delete_value = messages[2]
            .payload()
            .ok_or_else(|| test_error("Protobuf delete envelope lost its payload"))?;
        require(
            value_v1_id != value_v2_id,
            "evolved Protobuf value schema did not receive a new registry ID",
        )?;
        require(
            framed_schema_id(delete_value)? == value_v2_id,
            "Protobuf delete envelope did not reuse the latest value schema",
        )?;

        let settings = SrSettings::new(registry_url.clone());
        let registered_key = get_schema_by_id(key_id, &settings).await?;
        let registered_v1 = get_schema_by_id(value_v1_id, &settings).await?;
        let registered_v2 = get_schema_by_id(value_v2_id, &settings).await?;
        require(
            registered_key.schema_type == SchemaType::Protobuf
                && registered_v1.schema_type == SchemaType::Protobuf
                && registered_v2.schema_type == SchemaType::Protobuf,
            "registry did not preserve Protobuf schema types",
        )?;
        let first_decoded = framed_protobuf(first_value, &registered_v1.schema)?;
        let second_decoded = framed_protobuf(second_value, &registered_v2.schema)?;
        let delete_decoded = framed_protobuf(delete_value, &registered_v2.schema)?;
        let first_after = protobuf_message_field(&first_decoded, "after")?;
        let first_id = protobuf_message_field(&first_after, "id")?;
        require(
            first_id.get_field_by_name("value").as_deref() == Some(&ProtobufValue::I64(21)),
            "first framed Protobuf value changed",
        )?;
        let second_after = protobuf_message_field(&second_decoded, "after")?;
        let email = protobuf_message_field(&second_after, "email")?;
        require(
            email.get_field_by_name("value").as_deref()
                == Some(&ProtobufValue::String("bob@example.com".into())),
            "evolved framed Protobuf value changed",
        )?;
        require(
            delete_decoded.get_field_by_name("op").as_deref()
                == Some(&ProtobufValue::String("d".into())),
            "Protobuf delete envelope operation changed",
        )?;
        let latest = get_schema_by_subject(
            &settings,
            &SubjectNameStrategy::TopicNameStrategy(topic.clone(), false),
        )
        .await?;
        require(
            latest.id == value_v2_id,
            "latest Protobuf value subject does not point at the evolved schema",
        )?;
        sink.shutdown().await?;
        TestResult::Ok(())
    }
    .await;

    let deleted = admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await;
    if outcome.is_ok() {
        let deleted = deleted?;
        require(
            matches!(deleted.as_slice(), [Ok(deleted)] if deleted == &topic),
            "Protobuf test topic was not deleted",
        )?;
    }
    outcome
}

fn encoded_event(id: &str, destination: &str, payload: Option<&'static [u8]>) -> EncodedEvent {
    EncodedEvent {
        id: EventId(id.into()),
        destination: destination.into(),
        key: Some(Vec::from(b"order-1").into()),
        key_schema: None,
        payload: payload.map(|payload| payload.to_vec().into()),
        payload_schema: None,
        headers: BTreeMap::from([("rustium.event.id".into(), id.into())]),
    }
}

fn framed_schema_id(value: &[u8]) -> TestResult<u32> {
    require(
        value.len() >= 5 && value[0] == 0,
        "record does not use Confluent Schema Registry framing",
    )?;
    Ok(u32::from_be_bytes(value[1..5].try_into()?))
}

fn schema_change_event(
    topic_prefix: &str,
    schema_version: u32,
    serial: u64,
    email: Option<&str>,
) -> ChangeEvent {
    let position = position(serial);
    let mut after = rustium_core::Row::new();
    after.insert("id".into(), DataValue::Int64(serial as i64));
    after.insert(
        "name".into(),
        DataValue::String(format!("Customer {serial}")),
    );
    if let Some(email) = email {
        after.insert("email".into(), DataValue::String(email.into()));
    }
    let mut fields = vec![
        FieldSchema {
            name: "id".into(),
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
    ];
    if schema_version >= 2 {
        fields.push(FieldSchema {
            name: "email".into(),
            type_name: "varchar(255)".into(),
            optional: true,
            primary_key: false,
        });
    }
    ChangeEvent {
        id: EventId::deterministic(
            "schema-registry-test",
            "mysql",
            &position,
            "app.customers",
            serial,
        ),
        source: SourceMetadata {
            connector: "mysql".into(),
            connector_name: topic_prefix.into(),
            database: "app".into(),
            schema: None,
            table: Some("customers".into()),
            snapshot: false,
            version: "test".into(),
            attributes: BTreeMap::new(),
        },
        position,
        transaction: None,
        operation: Operation::Create,
        before: None,
        after: Some(after),
        schema: EventSchema {
            name: format!("{topic_prefix}.app.customers.Envelope"),
            version: schema_version,
            fields,
        },
        source_time: Some(Utc::now()),
        observed_time: Utc::now(),
    }
}

fn framed_json(value: &[u8]) -> TestResult<serde_json::Value> {
    framed_schema_id(value)?;
    Ok(serde_json::from_slice(&value[5..])?)
}

fn framed_avro(value: &[u8], definition: &str) -> TestResult<AvroValue> {
    framed_schema_id(value)?;
    let schema = Schema::parse_str(definition)?;
    Ok(from_avro_datum(&schema, &mut &value[5..], None)?)
}

fn avro_field<'a>(value: &'a AvroValue, name: &str) -> TestResult<&'a AvroValue> {
    let AvroValue::Record(fields) = value else {
        return Err(test_error("decoded Avro value is not a record"));
    };
    fields
        .iter()
        .find_map(|(field, value)| (field == name).then_some(value))
        .ok_or_else(|| test_error(&format!("decoded Avro record has no {name:?} field")))
}

fn avro_union(value: &AvroValue) -> TestResult<&AvroValue> {
    let AvroValue::Union(_, value) = value else {
        return Err(test_error("decoded Avro value is not a union"));
    };
    Ok(value)
}

fn framed_protobuf(value: &[u8], definition: &str) -> TestResult<DynamicMessage> {
    framed_schema_id(value)?;
    require(
        value.get(5) == Some(&0),
        "Protobuf record does not use the top-level message index",
    )?;
    let file = protox_parse::parse("registry.proto", definition)?;
    let package = file
        .package
        .clone()
        .ok_or_else(|| test_error("registered Protobuf schema has no package"))?;
    let root = file
        .message_type
        .first()
        .and_then(|message| message.name.clone())
        .ok_or_else(|| test_error("registered Protobuf schema has no root message"))?;
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(file)?;
    let descriptor = pool
        .get_message_by_name(&format!("{package}.{root}"))
        .ok_or_else(|| test_error("registered Protobuf root descriptor is missing"))?;
    Ok(DynamicMessage::decode(descriptor, &value[6..])?)
}

fn protobuf_message_field(message: &DynamicMessage, name: &str) -> TestResult<DynamicMessage> {
    let value = message
        .get_field_by_name(name)
        .ok_or_else(|| test_error(&format!("decoded Protobuf record has no {name:?} field")))?
        .into_owned();
    let ProtobufValue::Message(message) = value else {
        return Err(test_error(&format!(
            "decoded Protobuf field {name:?} is not a message"
        )));
    };
    Ok(message)
}

fn position(event_serial: u64) -> SourcePosition {
    SourcePosition::MySql(MySqlPosition {
        binlog_filename: "binlog.000001".into(),
        binlog_position: event_serial,
        gtid_set: None,
        server_id: 1,
        event_serial,
        snapshot: false,
    })
}

fn require_header(message: &OwnedMessage, expected: &str) -> TestResult {
    let headers = message
        .headers()
        .ok_or_else(|| test_error("Kafka Sink record lost its headers"))?;
    require(
        headers.count() == 1,
        "Kafka Sink record has unexpected headers",
    )?;
    let header = headers.get(0);
    require(
        header.key == "rustium.event.id" && header.value == Some(expected.as_bytes()),
        "Kafka Sink record changed its header",
    )
}

fn required_env(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("required environment variable {name} is not set"),
        )
        .into()
    })
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(test_error(message))
    }
}

fn test_error(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::other(message).into()
}
