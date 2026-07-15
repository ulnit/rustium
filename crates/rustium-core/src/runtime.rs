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
    Checkpoint, CheckpointStore, ConnectorIdentity, DeliveryBatch, EncodedEvent, Error,
    EventEncoder, RecordBoundary, Result, Sink, SourceConnector, SourceContext, SourcePosition,
    SourceRecord,
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
                    self.flush_batch(&mut encoded, &mut highest_position, snapshot_completed, &ack_tx).await?;
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
                        &ack_tx,
                    ).await?;
                }
            }
        }

        self.flush_batch(
            &mut encoded,
            &mut highest_position,
            snapshot_completed,
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
            if event.source.snapshot
                && self.status.snapshot().await.state != ConnectorState::Snapshotting
            {
                self.status
                    .transition(ConnectorState::Snapshotting, None)
                    .await;
            }
            encoded.push(self.encoder.encode(event)?);
        }
        *highest_position = Some(record.position);

        match record.boundary {
            RecordBoundary::SnapshotComplete => {
                *snapshot_completed = true;
                self.status
                    .transition(ConnectorState::Streaming, None)
                    .await;
                self.flush_batch(encoded, highest_position, *snapshot_completed, ack_tx)
                    .await?;
            }
            RecordBoundary::TransactionCommit => {
                self.flush_batch(encoded, highest_position, *snapshot_completed, ack_tx)
                    .await?;
            }
            RecordBoundary::Data if encoded.len() >= self.config.max_batch_size => {
                self.flush_batch(encoded, highest_position, *snapshot_completed, ack_tx)
                    .await?;
            }
            RecordBoundary::Heartbeat if encoded.is_empty() => {
                self.flush_batch(encoded, highest_position, *snapshot_completed, ack_tx)
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
            schema_version: 1,
            connector_name: self.identity.name.clone(),
            generation: self.identity.generation,
            source_position: position.clone(),
            snapshot_completed,
            config_fingerprint: self.config.config_fingerprint.clone(),
            updated_at: std::time::SystemTime::now(),
        };
        self.checkpoint_store.save(&checkpoint).await?;
        ack_tx
            .send(Some(position.clone()))
            .map_err(|_| Error::Source("source acknowledgement channel closed".into()))?;
        self.status.checkpointed(position, delivered).await;
        Ok(())
    }
}
