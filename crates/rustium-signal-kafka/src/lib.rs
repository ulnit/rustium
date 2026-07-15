//! Debezium-compatible Kafka signal input channel.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::OwnedMessage,
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use rustium_core::{Error, Result, SignalRecord, SignalSender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub struct KafkaSignalChannel {
    consumer: Arc<StreamConsumer>,
    connector_key: String,
    topic: String,
    poll_timeout: Duration,
    metadata_timeout: Duration,
}

impl KafkaSignalChannel {
    pub fn new(
        bootstrap_servers: &[String],
        connector_key: impl Into<String>,
        topic: impl Into<String>,
        group_id: &str,
        poll_timeout: Duration,
        properties: &BTreeMap<String, String>,
    ) -> Result<Self> {
        if bootstrap_servers.is_empty()
            || bootstrap_servers
                .iter()
                .any(|server| server.trim().is_empty())
        {
            return Err(Error::Configuration(
                "Kafka signal bootstrap servers must not be empty".into(),
            ));
        }
        let connector_key = connector_key.into();
        let topic = topic.into();
        if connector_key.trim().is_empty() || topic.trim().is_empty() || group_id.trim().is_empty()
        {
            return Err(Error::Configuration(
                "Kafka signal connector key, topic, and group ID must not be empty".into(),
            ));
        }

        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers.join(","))
            .set("client.id", uuid::Uuid::new_v4().to_string())
            .set("group.id", group_id)
            .set("fetch.min.bytes", "1")
            .set("session.timeout.ms", "10000")
            .set("auto.offset.reset", "earliest");
        for (key, value) in properties {
            if matches!(
                key.as_str(),
                "enable.auto.commit" | "enable.auto.offset.store"
            ) && !value.eq_ignore_ascii_case("false")
            {
                return Err(Error::Configuration(format!(
                    "Kafka signal consumer property {key} must be false so offsets follow Rustium checkpoints"
                )));
            }
            config.set(key, value);
        }
        config
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");

        let consumer = config.create().map_err(|error| {
            Error::Configuration(format!("invalid Kafka signal consumer config: {error}"))
        })?;
        Ok(Self {
            consumer: Arc::new(consumer),
            connector_key,
            topic,
            poll_timeout,
            metadata_timeout: Duration::from_secs(10),
        })
    }

    pub async fn validate(&self) -> Result<()> {
        let consumer = self.consumer.clone();
        let topic = self.topic.clone();
        let timeout = self.metadata_timeout;
        let partition = tokio::task::spawn_blocking(move || {
            let metadata = consumer
                .fetch_metadata(Some(&topic), Timeout::After(timeout))
                .map_err(|error| {
                    Error::Source(format!("Kafka signal metadata request failed: {error}"))
                })?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|metadata| metadata.name() == topic)
                .ok_or_else(|| {
                    Error::Source(format!("Kafka signal topic {topic:?} was not found"))
                })?;
            if topic_metadata.partitions().len() != 1 {
                return Err(Error::Configuration(format!(
                    "Kafka signal topic {topic:?} must have exactly one partition, found {}",
                    topic_metadata.partitions().len()
                )));
            }
            Ok(topic_metadata.partitions()[0].id())
        })
        .await
        .map_err(|error| {
            Error::Source(format!("Kafka signal validation task failed: {error}"))
        })??;

        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(&self.topic, partition, Offset::Stored)
            .map_err(|error| {
                Error::Source(format!(
                    "Kafka signal assignment could not be built: {error}"
                ))
            })?;
        self.consumer.assign(&assignment).map_err(|error| {
            Error::Source(format!("Kafka signal topic assignment failed: {error}"))
        })?;
        Ok(())
    }

    pub async fn run(self, sender: SignalSender, cancellation: CancellationToken) -> Result<()> {
        self.validate().await?;
        info!(topic = %self.topic, connector_key = %self.connector_key, "Kafka signal channel started");
        loop {
            let message = match self.receive(&cancellation).await? {
                ReceiveOutcome::Message(message) => message,
                ReceiveOutcome::Timeout => continue,
                ReceiveOutcome::Cancelled => {
                    info!(topic = %self.topic, "Kafka signal channel stopped");
                    return Ok(());
                }
            };
            let signal = match decode_signal(&self.connector_key, message.key(), message.payload())
            {
                Ok(Some(signal)) => Some(signal),
                Ok(None) => {
                    debug!(
                        topic = %message.topic(),
                        partition = message.partition(),
                        offset = message.offset(),
                        "Kafka signal ignored because its key does not match topic.prefix"
                    );
                    None
                }
                Err(error) => {
                    warn!(
                        topic = %message.topic(),
                        partition = message.partition(),
                        offset = message.offset(),
                        %error,
                        "invalid Kafka signal ignored"
                    );
                    None
                }
            };
            if let Some(signal) = signal {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(()),
                    result = sender.send_and_wait(signal) => result?,
                }
            }
            self.commit(&message)?;
        }
    }

    async fn receive(&self, cancellation: &CancellationToken) -> Result<ReceiveOutcome> {
        if self.poll_timeout.is_zero() {
            return tokio::select! {
                _ = cancellation.cancelled() => Ok(ReceiveOutcome::Cancelled),
                message = self.consumer.recv() => message
                    .map(|message| ReceiveOutcome::Message(message.detach()))
                    .map_err(kafka_receive_error),
            };
        }
        tokio::select! {
            _ = cancellation.cancelled() => Ok(ReceiveOutcome::Cancelled),
            result = tokio::time::timeout(self.poll_timeout, self.consumer.recv()) => match result {
                Ok(message) => message
                    .map(|message| ReceiveOutcome::Message(message.detach()))
                    .map_err(kafka_receive_error),
                Err(_) => Ok(ReceiveOutcome::Timeout),
            },
        }
    }

    fn commit(&self, message: &OwnedMessage) -> Result<()> {
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(
                message.topic(),
                message.partition(),
                Offset::Offset(message.offset() + 1),
            )
            .map_err(|error| {
                Error::Source(format!("Kafka signal offset could not be built: {error}"))
            })?;
        self.consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(|error| Error::Source(format!("Kafka signal offset commit failed: {error}")))
    }
}

enum ReceiveOutcome {
    Message(OwnedMessage),
    Timeout,
    Cancelled,
}

fn decode_signal(
    connector_key: &str,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
) -> Result<Option<SignalRecord>> {
    if key != Some(connector_key.as_bytes()) {
        return Ok(None);
    }
    let payload = payload.ok_or_else(|| Error::Source("Kafka signal payload is null".into()))?;
    if payload.is_empty() {
        return Err(Error::Source("Kafka signal payload is empty".into()));
    }
    let signal: SignalRecord = serde_json::from_slice(payload).map_err(|error| {
        Error::Source(format!("Kafka signal payload has invalid JSON: {error}"))
    })?;
    signal.validate()?;
    Ok(Some(signal))
}

fn kafka_receive_error(error: rdkafka::error::KafkaError) -> Error {
    Error::Source(format!("Kafka signal receive failed: {error}"))
}

#[cfg(test)]
mod tests {
    use rdkafka::{
        mocking::MockCluster,
        producer::{FutureProducer, FutureRecord},
    };
    use rustium_core::signal_channel;

    use super::*;

    #[test]
    fn filters_connector_keys_and_decodes_signal_values() {
        let payload =
            br#"{"id":"snapshot-1","type":"execute-snapshot","data":{"type":"incremental"}}"#;
        assert!(
            decode_signal("inventory", Some(b"other"), Some(payload))
                .unwrap()
                .is_none()
        );
        let signal = decode_signal("inventory", Some(b"inventory"), Some(payload))
            .unwrap()
            .unwrap();
        assert_eq!(signal.id, "snapshot-1");
        assert_eq!(signal.signal_type, "execute-snapshot");
        assert!(decode_signal("inventory", Some(b"inventory"), Some(b"{")).is_err());
    }

    #[test]
    fn rejects_automatic_offset_advancement() {
        let mut properties = BTreeMap::new();
        properties.insert("enable.auto.commit".into(), "true".into());
        let error = KafkaSignalChannel::new(
            &["localhost:9092".into()],
            "inventory",
            "inventory-signal",
            "kafka-signal",
            Duration::from_millis(100),
            &properties,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("must be false"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commits_a_mock_kafka_offset_only_after_signal_acknowledgement() {
        const TOPIC: &str = "inventory-signal";
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic(TOPIC, 1, 1).unwrap();
        let bootstrap_servers = vec![cluster.bootstrap_servers()];
        let channel = KafkaSignalChannel::new(
            &bootstrap_servers,
            "inventory",
            TOPIC,
            "rustium-signal-test",
            Duration::from_millis(25),
            &BTreeMap::new(),
        )
        .unwrap();
        let consumer = channel.consumer.clone();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers[0])
            .create()
            .unwrap();
        producer
            .send(
                FutureRecord::to(TOPIC)
                    .key("inventory")
                    .payload(
                        r#"{"id":"snapshot-1","type":"execute-snapshot","data":{"type":"incremental"}}"#,
                    ),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        let (sender, mut receiver) = signal_channel(1);
        let cancellation = CancellationToken::new();
        let task_cancel = cancellation.clone();
        let task = tokio::spawn(channel.run(sender, task_cancel));
        let delivery = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.record().id, "snapshot-1");
        assert_ne!(committed_offset(&consumer, TOPIC), Offset::Offset(1));

        delivery.acknowledge();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if committed_offset(&consumer, TOPIC) == Offset::Offset(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejects_a_multi_partition_signal_topic() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("multi-signal", 2, 1).unwrap();
        let channel = KafkaSignalChannel::new(
            &[cluster.bootstrap_servers()],
            "inventory",
            "multi-signal",
            "rustium-signal-multi-test",
            Duration::from_millis(25),
            &BTreeMap::new(),
        )
        .unwrap();
        let error = channel.validate().await.unwrap_err();
        assert!(error.to_string().contains("exactly one partition"));
    }

    fn committed_offset(consumer: &StreamConsumer, topic: &str) -> Offset {
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(topic, 0);
        consumer
            .committed_offsets(partitions, Timeout::After(Duration::from_secs(1)))
            .unwrap()
            .find_partition(topic, 0)
            .unwrap()
            .offset()
    }
}
