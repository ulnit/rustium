use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, mpsc, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, CheckpointStore, ConnectorIdentity,
    ConnectorStateEnvelope, DeliveryBatch, EncodedEvent, Error, EventEncoder, RecordBoundary,
    Result, Sink, SourceConnector, SourceContext, SourcePosition, SourceRecord,
};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub flush_interval: Duration,
    pub shutdown_timeout: Duration,
    pub config_fingerprint: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 2_048,
            max_batch_size: 512,
            flush_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(30),
            config_fingerprint: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnectorState {
    Created,
    Starting,
    Snapshotting,
    Streaming,
    Paused,
    Failed,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub connector_name: String,
    pub state: ConnectorState,
    pub state_since: DateTime<Utc>,
    pub state_reason: Option<String>,
    pub last_position: Option<SourcePosition>,
    pub last_checkpoint_at: Option<DateTime<Utc>>,
    pub delivered_events: u64,
    pub failed_events: u64,
    pub queue_depth: usize,
}

#[derive(Clone)]
pub struct RuntimeStatus(Arc<RwLock<StatusSnapshot>>);

impl RuntimeStatus {
    #[must_use]
    pub fn new(connector_name: impl Into<String>) -> Self {
        Self(Arc::new(RwLock::new(StatusSnapshot {
            connector_name: connector_name.into(),
            state: ConnectorState::Created,
            state_since: Utc::now(),
            state_reason: None,
            last_position: None,
            last_checkpoint_at: None,
            delivered_events: 0,
            failed_events: 0,
            queue_depth: 0,
        })))
    }

    pub async fn snapshot(&self) -> StatusSnapshot {
        self.0.read().await.clone()
    }

    pub async fn transition(&self, state: ConnectorState, reason: Option<String>) {
        let mut status = self.0.write().await;
        status.state = state;
        status.state_since = Utc::now();
        status.state_reason = reason;
    }

    async fn set_queue_depth(&self, depth: usize) {
        self.0.write().await.queue_depth = depth;
    }

    async fn checkpointed(&self, position: SourcePosition, delivered: usize) {
        let mut status = self.0.write().await;
        status.last_position = Some(position);
        status.last_checkpoint_at = Some(Utc::now());
        status.delivered_events += delivered as u64;
    }
}

pub struct ConnectorRuntime {
    identity: ConnectorIdentity,
    source: Option<Box<dyn SourceConnector>>,
    encoder: Arc<dyn EventEncoder>,
    sink: Box<dyn Sink>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    config: RuntimeConfig,
    status: RuntimeStatus,
}

impl ConnectorRuntime {
    #[must_use]
    pub fn new(
        identity: ConnectorIdentity,
        source: Box<dyn SourceConnector>,
        encoder: Arc<dyn EventEncoder>,
        sink: Box<dyn Sink>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        config: RuntimeConfig,
        status: RuntimeStatus,
    ) -> Self {
        Self {
            identity,
            source: Some(source),
            encoder,
            sink,
            checkpoint_store,
            config,
            status,
        }
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        self.status.transition(ConnectorState::Starting, None).await;
        let mut source = self
            .source
            .take()
            .ok_or_else(|| Error::Invariant("source connector already started".into()))?;
        source.validate().await?;
        self.sink.validate().await?;

        let initial_checkpoint = self.checkpoint_store.load(&self.identity.name).await?;
        if let Some(checkpoint) = &initial_checkpoint
            && checkpoint.config_fingerprint != self.config.config_fingerprint
        {
            return Err(Error::Configuration(
                "persisted checkpoint does not match the active configuration".into(),
            ));
        }

        let (source_tx, mut source_rx) = mpsc::channel(self.config.channel_capacity);
        let (ack_tx, ack_rx) = watch::channel(
            initial_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.source_position.clone()),
        );

        let snapshot_completed = initial_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.snapshot_completed);
        let mut connector_state = initial_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.connector_state.clone());
        let source_cancel = cancellation.child_token();
        let source_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            source
                .run(SourceContext {
                    output: source_tx,
                    acknowledged: ack_rx,
                    initial_checkpoint,
                    cancellation: source_cancel,
                })
                .await
        });

        self.status
            .transition(ConnectorState::Streaming, None)
            .await;
        let mut interval = tokio::time::interval_at(
            Instant::now() + self.config.flush_interval,
            self.config.flush_interval,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut encoded = Vec::with_capacity(self.config.max_batch_size);
        let mut highest_position: Option<SourcePosition> = None;
        let mut snapshot_completed = snapshot_completed;

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    self.status.transition(ConnectorState::Stopping, None).await;
                    break;
                }
                _ = interval.tick() => {
                    self.flush_batch(
                        &mut encoded,
                        &mut highest_position,
                        snapshot_completed,
                        &connector_state,
                        &ack_tx,
                    ).await?;
                }
                record = source_rx.recv() => {
                    let Some(record) = record else { break };
                    let record = record?;
                    self.status.set_queue_depth(source_rx.len()).await;
                    self.consume_record(
                        record,
                        &mut encoded,
                        &mut highest_position,
                        &mut snapshot_completed,
                        &mut connector_state,
                        &ack_tx,
                    ).await?;
                }
            }
        }

        self.flush_batch(
            &mut encoded,
            &mut highest_position,
            snapshot_completed,
            &connector_state,
            &ack_tx,
        )
        .await?;
        self.sink.flush().await?;
        cancellation.cancel();

        match tokio::time::timeout(self.config.shutdown_timeout, source_handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) if !matches!(error, Error::Cancelled) => return Err(error),
            Ok(Err(error)) => return Err(Error::Source(format!("source task failed: {error}"))),
            Err(_) => return Err(Error::Source("source shutdown timed out".into())),
            _ => {}
        }

        self.sink.shutdown().await?;
        self.status.transition(ConnectorState::Stopped, None).await;
        info!(connector = %self.identity.name, "connector stopped");
        Ok(())
    }

    async fn consume_record(
        &mut self,
        record: SourceRecord,
        encoded: &mut Vec<EncodedEvent>,
        highest_position: &mut Option<SourcePosition>,
        snapshot_completed: &mut bool,
        connector_state: &mut Option<ConnectorStateEnvelope>,
        ack_tx: &watch::Sender<Option<SourcePosition>>,
    ) -> Result<()> {
        if let Some(previous) = highest_position.as_ref()
            && !record.position.is_after(previous)
            && record.position != *previous
        {
            return Err(Error::Invariant(format!(
                "source position moved backwards: {:?} -> {:?}",
                previous, record.position
            )));
        }

        if let Some(event) = &record.event {
            let incremental_snapshot = event
                .source
                .attributes
                .get("rustium.snapshot.kind")
                .and_then(serde_json::Value::as_str)
                == Some("incremental");
            if event.source.snapshot
                && !incremental_snapshot
                && self.status.snapshot().await.state != ConnectorState::Snapshotting
            {
                self.status
                    .transition(ConnectorState::Snapshotting, None)
                    .await;
            }
            encoded.extend(self.encoder.encode_batch(event)?);
        }
        *highest_position = Some(record.position);
        if let Some(state) = record.connector_state {
            *connector_state = Some(state);
        }

        match record.boundary {
            RecordBoundary::SnapshotComplete => {
                *snapshot_completed = true;
                self.status
                    .transition(ConnectorState::Streaming, None)
                    .await;
                self.flush_batch(
                    encoded,
                    highest_position,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                )
                .await?;
            }
            RecordBoundary::TransactionCommit => {
                self.flush_batch(
                    encoded,
                    highest_position,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                )
                .await?;
            }
            RecordBoundary::Data if encoded.len() >= self.config.max_batch_size => {
                self.flush_batch(
                    encoded,
                    highest_position,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                )
                .await?;
            }
            RecordBoundary::Heartbeat if encoded.is_empty() => {
                self.flush_batch(
                    encoded,
                    highest_position,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn flush_batch(
        &mut self,
        encoded: &mut Vec<EncodedEvent>,
        highest_position: &mut Option<SourcePosition>,
        snapshot_completed: bool,
        connector_state: &Option<ConnectorStateEnvelope>,
        ack_tx: &watch::Sender<Option<SourcePosition>>,
    ) -> Result<()> {
        let Some(position) = highest_position.take() else {
            return Ok(());
        };
        let delivered = encoded.len();

        if !encoded.is_empty() {
            let batch = DeliveryBatch {
                events: std::mem::take(encoded),
                highest_position: position.clone(),
            };
            if let Err(error) = self.sink.write(&batch).await {
                error!(connector = %self.identity.name, %error, "sink delivery failed");
                self.status
                    .transition(ConnectorState::Failed, Some(error.to_string()))
                    .await;
                return Err(error);
            }
        }

        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: self.identity.name.clone(),
            generation: self.identity.generation,
            source_position: position.clone(),
            snapshot_completed,
            config_fingerprint: self.config.config_fingerprint.clone(),
            updated_at: std::time::SystemTime::now(),
            connector_state: connector_state.clone(),
        };
        self.checkpoint_store.save(&checkpoint).await?;
        ack_tx
            .send(Some(position.clone()))
            .map_err(|_| Error::Source("source acknowledgement channel closed".into()))?;
        self.status.checkpointed(position, delivered).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Utc;

    use super::*;
    use crate::{
        ChangeEvent, ConnectorStateEnvelope, Durability, EventId, EventSchema, MySqlPosition,
        Operation, RecordBoundary, SourceMetadata,
    };

    struct StateSource {
        record: SourceRecord,
    }

    #[async_trait]
    impl SourceConnector for StateSource {
        fn source_type(&self) -> &'static str {
            "test"
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn run(&mut self, mut context: SourceContext) -> Result<()> {
            context
                .output
                .send(Ok(self.record.clone()))
                .await
                .map_err(|_| Error::Cancelled)?;
            context
                .acknowledged
                .changed()
                .await
                .map_err(|_| Error::Cancelled)?;
            Ok(())
        }
    }

    struct UnusedEncoder;

    impl EventEncoder for UnusedEncoder {
        fn content_type(&self) -> &'static str {
            "application/test"
        }

        fn encode(&self, _event: &ChangeEvent) -> Result<EncodedEvent> {
            Err(Error::Invariant(
                "position-only runtime test unexpectedly encoded an event".into(),
            ))
        }
    }

    struct NoopSink;

    #[async_trait]
    impl Sink for NoopSink {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn durability(&self) -> Durability {
            Durability::Durable
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write(&mut self, _batch: &DeliveryBatch) -> Result<()> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct BatchEncoder;

    impl EventEncoder for BatchEncoder {
        fn content_type(&self) -> &'static str {
            "application/test"
        }

        fn encode(&self, _event: &ChangeEvent) -> Result<EncodedEvent> {
            Err(Error::Invariant(
                "batch encoder must be invoked through encode_batch".into(),
            ))
        }

        fn encode_batch(&self, event: &ChangeEvent) -> Result<Vec<EncodedEvent>> {
            let encoded = |id: EventId, payload| EncodedEvent {
                id,
                destination: "orders".into(),
                key: Some(Bytes::from_static(b"1")),
                payload,
                headers: BTreeMap::new(),
            };
            Ok(vec![
                encoded(event.id.clone(), Some(Bytes::from_static(br#"{"op":"d"}"#))),
                encoded(event.id.derived("tombstone"), None),
            ])
        }
    }

    struct RecordingSink {
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn durability(&self) -> Durability {
            Durability::Durable
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write(&mut self, batch: &DeliveryBatch) -> Result<()> {
            self.batch_sizes.lock().unwrap().push(batch.events.len());
            assert!(batch.events[0].payload.is_some());
            assert!(batch.events[1].payload.is_none());
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryCheckpointStore {
        checkpoint: Mutex<Option<Checkpoint>>,
    }

    #[async_trait]
    impl CheckpointStore for MemoryCheckpointStore {
        async fn load(&self, _connector_name: &str) -> Result<Option<Checkpoint>> {
            Ok(self.checkpoint.lock().unwrap().clone())
        }

        async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
            *self.checkpoint.lock().unwrap() = Some(checkpoint.clone());
            Ok(())
        }

        async fn delete(&self, _connector_name: &str) -> Result<()> {
            *self.checkpoint.lock().unwrap() = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn checkpoints_connector_state_with_its_source_position() {
        let position = SourcePosition::MySql(MySqlPosition {
            binlog_filename: "mysql-bin.000001".into(),
            binlog_position: 128,
            gtid_set: None,
            server_id: 1,
            event_serial: 0,
            snapshot: false,
        });
        let connector_state =
            ConnectorStateEnvelope::new("rustium.test", 1, serde_json::json!({"table": "orders"}));
        let source = StateSource {
            record: SourceRecord {
                event: None,
                position: position.clone(),
                boundary: RecordBoundary::Heartbeat,
                connector_state: Some(connector_state.clone()),
            },
        };
        let store = Arc::new(MemoryCheckpointStore::default());
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-state-test"),
            Box::new(source),
            Arc::new(UnusedEncoder),
            Box::new(NoopSink),
            store.clone(),
            RuntimeConfig {
                flush_interval: Duration::from_secs(60),
                ..RuntimeConfig::default()
            },
            RuntimeStatus::new("runtime-state-test"),
        );

        runtime.run(CancellationToken::new()).await.unwrap();

        let checkpoint = store.checkpoint.lock().unwrap().clone().unwrap();
        assert_eq!(checkpoint.schema_version, CHECKPOINT_SCHEMA_VERSION);
        assert_eq!(checkpoint.source_position, position);
        assert_eq!(checkpoint.connector_state, Some(connector_state));
    }

    #[tokio::test]
    async fn checkpoints_after_all_encoded_records_are_delivered_together() {
        let position = SourcePosition::MySql(MySqlPosition {
            binlog_filename: "mysql-bin.000001".into(),
            binlog_position: 256,
            gtid_set: None,
            server_id: 1,
            event_serial: 1,
            snapshot: false,
        });
        let event = ChangeEvent {
            id: EventId::deterministic("orders", "mysql", &position, "app.orders", 1),
            source: SourceMetadata {
                connector: "mysql".into(),
                connector_name: "orders".into(),
                database: "app".into(),
                schema: None,
                table: Some("orders".into()),
                snapshot: false,
                version: "test".into(),
                attributes: BTreeMap::new(),
            },
            position: position.clone(),
            transaction: None,
            operation: Operation::Delete,
            before: None,
            after: None,
            schema: EventSchema {
                name: "orders".into(),
                version: 1,
                fields: Vec::new(),
            },
            source_time: None,
            observed_time: Utc::now(),
        };
        let source = StateSource {
            record: SourceRecord {
                event: Some(event),
                position: position.clone(),
                boundary: RecordBoundary::TransactionCommit,
                connector_state: None,
            },
        };
        let store = Arc::new(MemoryCheckpointStore::default());
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let status = RuntimeStatus::new("runtime-batch-test");
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-batch-test"),
            Box::new(source),
            Arc::new(BatchEncoder),
            Box::new(RecordingSink {
                batch_sizes: batch_sizes.clone(),
            }),
            store.clone(),
            RuntimeConfig::default(),
            status.clone(),
        );

        runtime.run(CancellationToken::new()).await.unwrap();

        assert_eq!(*batch_sizes.lock().unwrap(), [2]);
        assert_eq!(
            store
                .checkpoint
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .source_position,
            position
        );
        assert_eq!(status.snapshot().await.delivered_events, 2);
    }
}
