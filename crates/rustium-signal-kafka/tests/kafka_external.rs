use std::{collections::BTreeMap, env, io, time::Duration};

use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use rustium_core::signal_channel;
use rustium_signal_kafka::KafkaSignalChannel;
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external Kafka-compatible broker"]
async fn consumes_signals_and_commits_only_after_acknowledgement() -> TestResult {
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic = format!("rustium-signal-{}", &suffix[..12]);
    let group_id = format!("rustium-signal-group-{}", &suffix[..12]);
    let connector_key = format!("inventory-{}", &suffix[..12]);
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
        "Kafka signal test topic was not created",
    )?;

    let outcome = async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()?;
        for (key, id) in [("other-connector", "ignored"), (&connector_key, "snapshot-1")] {
            producer
                .send(
                    FutureRecord::to(&topic).key(key).payload(&format!(
                        r#"{{"id":"{id}","type":"execute-snapshot","data":{{"type":"incremental"}}}}"#
                    )),
                    Timeout::After(Duration::from_secs(10)),
                )
                .await
                .map_err(|(error, _)| error)?;
        }

        let channel = KafkaSignalChannel::new(
            &[bootstrap_servers.clone()],
            &connector_key,
            &topic,
            &group_id,
            Duration::from_millis(100),
            &BTreeMap::new(),
        )?;
        let observer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .create()?;
        let (sender, mut receiver) = signal_channel(1);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(channel.run(sender, cancellation.clone()));
        let delivery = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await?
            .ok_or_else(|| test_error("Kafka signal channel closed before delivery"))?;
        require(delivery.record().id == "snapshot-1", "wrong Kafka signal delivered")?;
        wait_for_offset(&observer, &topic, Offset::Offset(1)).await?;
        delivery.acknowledge();
        wait_for_offset(&observer, &topic, Offset::Offset(2)).await?;
        cancellation.cancel();
        task.await??;
        TestResult::Ok(())
    }
    .await;

    let deleted = admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await;
    if let Err(error) = deleted
        && outcome.is_ok()
    {
        return Err(error.into());
    }
    outcome
}

async fn wait_for_offset(consumer: &StreamConsumer, topic: &str, expected: Offset) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if committed_offset(consumer, topic)? == expected {
                return TestResult::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

fn committed_offset(consumer: &StreamConsumer, topic: &str) -> TestResult<Offset> {
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    Ok(consumer
        .committed_offsets(partitions, Timeout::After(Duration::from_secs(2)))?
        .find_partition(topic, 0)
        .ok_or_else(|| test_error("Kafka committed offset has no signal partition"))?
        .offset())
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
