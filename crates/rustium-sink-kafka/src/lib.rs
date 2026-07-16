//! Durable Kafka sink.

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use rdkafka::{
    ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use rustium_core::{DeliveryBatch, Durability, Error, Result, Sink};

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
        })
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
            let mut headers = OwnedHeaders::new();
            for (key, value) in &event.headers {
                headers = headers.insert(Header {
                    key,
                    value: Some(value),
                });
            }
            let mut record = FutureRecord::to(&event.destination).headers(headers);
            if let Some(payload) = &event.payload {
                record = record.payload(payload.as_ref());
            }
            if let Some(key) = &event.key {
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
}
