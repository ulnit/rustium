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
                .map_err(|error| Error::Sink(format!("Kafka metadata request failed: {error}")))
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
                .map_err(|(error, _)| Error::Sink(format!("Kafka delivery failed: {error}")))?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.producer
            .flush(Timeout::After(self.delivery_timeout))
            .map_err(|error| Error::Sink(format!("Kafka flush failed: {error}")))
    }
}
