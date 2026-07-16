use std::{collections::BTreeMap, env, io, time::Duration};

use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    message::{Headers, OwnedMessage},
};
use rustium_core::{DeliveryBatch, EncodedEvent, EventId, MySqlPosition, Sink, SourcePosition};
use rustium_sink_kafka::KafkaSink;

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

fn encoded_event(id: &str, destination: &str, payload: Option<&'static [u8]>) -> EncodedEvent {
    EncodedEvent {
        id: EventId(id.into()),
        destination: destination.into(),
        key: Some(Vec::from(b"order-1").into()),
        payload: payload.map(|payload| payload.to_vec().into()),
        headers: BTreeMap::from([("rustium.event.id".into(), id.into())]),
    }
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
