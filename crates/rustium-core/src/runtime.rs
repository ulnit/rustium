use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, mpsc, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, CheckpointStore, ConnectorIdentity,
    ConnectorStateEnvelope, DeliveryBatch, EncodedEvent, Error, EventEncoder, RecordBoundary,
    Result, SignalSender, Sink, SourceConnector, SourceContext, SourcePosition, SourceRecord,
    signal_channel,
};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub flush_interval: Duration,
    pub shutdown_timeout: Duration,
    pub errors_max_retries: i32,
    pub errors_retry_delay_initial: Duration,
    pub errors_retry_delay_max: Duration,
    pub config_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: i32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_delay: Duration::from_millis(300),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RuntimeConfig {
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_retries: self.errors_max_retries,
            initial_delay: self.errors_retry_delay_initial,
            max_delay: self.errors_retry_delay_max,
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 2_048,
            max_batch_size: 512,
            flush_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(30),
            errors_max_retries: 10,
            errors_retry_delay_initial: Duration::from_millis(300),
            errors_retry_delay_max: Duration::from_secs(10),
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
    pub last_source_event_at: Option<DateTime<Utc>>,
    pub last_event_observed_at: Option<DateTime<Utc>>,
    pub source_lag_millis: Option<u64>,
    pub delivered_events: u64,
    pub failed_events: u64,
    pub sink_retry_attempts: u64,
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
            last_source_event_at: None,
            last_event_observed_at: None,
            source_lag_millis: None,
            delivered_events: 0,
            failed_events: 0,
            sink_retry_attempts: 0,
            queue_depth: 0,
        })))
    }

    pub async fn snapshot(&self) -> StatusSnapshot {
        let mut snapshot = self.0.read().await.clone();
        snapshot.source_lag_millis = snapshot
            .last_source_event_at
            .map(|source_time| event_lag_millis(source_time, Utc::now()));
        snapshot
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

    async fn increment_failed_events(&self, failed: usize) {
        self.0.write().await.failed_events += failed as u64;
    }

    async fn increment_sink_retry_attempts(&self) {
        self.0.write().await.sink_retry_attempts += 1;
    }

    async fn checkpointed(
        &self,
        position: SourcePosition,
        delivered: usize,
        event_timing: Option<EventTiming>,
    ) {
        let mut status = self.0.write().await;
        status.last_position = Some(position);
        status.last_checkpoint_at = Some(Utc::now());
        status.delivered_events += delivered as u64;
        if let Some(event_timing) = event_timing {
            status.last_source_event_at = event_timing.source_time;
            status.last_event_observed_at = Some(event_timing.observed_time);
            status.source_lag_millis = event_timing
                .source_time
                .map(|source_time| event_lag_millis(source_time, Utc::now()));
        }
    }
}

fn event_lag_millis(source_time: DateTime<Utc>, observed_time: DateTime<Utc>) -> u64 {
    u64::try_from(
        observed_time
            .signed_duration_since(source_time)
            .num_milliseconds()
            .max(0),
    )
    .unwrap_or(u64::MAX)
}

pub struct ConnectorRuntime {
    identity: ConnectorIdentity,
    source: Option<Box<dyn SourceConnector>>,
    encoder: Arc<dyn EventEncoder>,
    sink: Box<dyn Sink>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    config: RuntimeConfig,
    status: RuntimeStatus,
    signal_sender: SignalSender,
    signal_receiver: Option<mpsc::Receiver<crate::SignalDelivery>>,
}

struct PendingBatch {
    encoded: Vec<EncodedEvent>,
    highest_position: Option<SourcePosition>,
    latest_event_timing: Option<EventTiming>,
    signal_acknowledgements: Vec<crate::SignalAcknowledgement>,
}

#[derive(Debug, Clone, Copy)]
struct EventTiming {
    source_time: Option<DateTime<Utc>>,
    observed_time: DateTime<Utc>,
}

struct RetryState {
    attempts: u64,
    delay: Duration,
}

impl RetryState {
    fn new(initial_delay: Duration) -> Self {
        Self {
            attempts: 0,
            delay: initial_delay,
        }
    }
}

impl PendingBatch {
    fn new(capacity: usize) -> Self {
        Self {
            encoded: Vec::with_capacity(capacity),
            highest_position: None,
            latest_event_timing: None,
            signal_acknowledgements: Vec::new(),
        }
    }
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
        let (signal_sender, signal_receiver) = signal_channel(config.channel_capacity);
        Self {
            identity,
            source: Some(source),
            encoder,
            sink,
            checkpoint_store,
            config,
            status,
            signal_sender,
            signal_receiver: Some(signal_receiver),
        }
    }

    #[must_use]
    pub fn signal_sender(&self) -> SignalSender {
        self.signal_sender.clone()
    }

    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let status = self.status.clone();
        let result = self.run_inner(cancellation).await;
        if let Err(error) = &result
            && !matches!(error, Error::Cancelled)
        {
            status
                .transition(ConnectorState::Failed, Some(error.to_string()))
                .await;
        }
        result
    }

    async fn run_inner(mut self, cancellation: CancellationToken) -> Result<()> {
        self.status.transition(ConnectorState::Starting, None).await;
        let mut source = self
            .source
            .take()
            .ok_or_else(|| Error::Invariant("source connector already started".into()))?;
        source.validate().await?;
        self.validate_sink_with_retry(&cancellation).await?;

        let initial_checkpoint = self.checkpoint_store.load(&self.identity.name).await?;
        if let Some(checkpoint) = &initial_checkpoint
            && checkpoint.config_fingerprint != self.config.config_fingerprint
        {
            return Err(Error::Configuration(
                "persisted checkpoint does not match the active configuration".into(),
            ));
        }

        let (source_tx, mut source_rx) = mpsc::channel(self.config.channel_capacity);
        let signals = self
            .signal_receiver
            .take()
            .ok_or_else(|| Error::Invariant("runtime signal channel already started".into()))?;
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
        let mut source_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            source
                .run(SourceContext {
                    output: source_tx,
                    acknowledged: ack_rx,
                    initial_checkpoint,
                    signals,
                    cancellation: source_cancel,
                })
                .await
        });

        let pipeline_result: Result<()> = async {
            self.status
                .transition(ConnectorState::Streaming, None)
                .await;
            let mut interval = tokio::time::interval_at(
                Instant::now() + self.config.flush_interval,
                self.config.flush_interval,
            );
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            let mut pending = PendingBatch::new(self.config.max_batch_size);
            let mut snapshot_completed = snapshot_completed;

            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        self.flush_batch(
                            &mut pending,
                            snapshot_completed,
                            &connector_state,
                            &ack_tx,
                            &cancellation,
                        ).await?;
                    }
                    record = source_rx.recv() => {
                        let Some(record) = record else { break };
                        let record = record?;
                        self.status.set_queue_depth(source_rx.len()).await;
                        self.consume_record(
                            record,
                            &mut pending,
                            &mut snapshot_completed,
                            &mut connector_state,
                            &ack_tx,
                            &cancellation,
                        ).await?;
                    }
                }
            }

            self.flush_batch(
                &mut pending,
                snapshot_completed,
                &connector_state,
                &ack_tx,
                &cancellation,
            )
            .await?;
            self.flush_sink_with_retry(&cancellation).await
        }
        .await;
        let pipeline_result = match pipeline_result {
            Err(Error::Cancelled) if cancellation.is_cancelled() => Ok(()),
            result => result,
        };

        if pipeline_result.is_ok() {
            self.status.transition(ConnectorState::Stopping, None).await;
        }
        cancellation.cancel();

        let source_result =
            match tokio::time::timeout(self.config.shutdown_timeout, &mut source_handle).await {
                Ok(Ok(Ok(()))) | Ok(Ok(Err(Error::Cancelled))) => Ok(()),
                Ok(Ok(Err(error))) => Err(error),
                Ok(Err(error)) => Err(Error::Source(format!("source task failed: {error}"))),
                Err(_) => {
                    source_handle.abort();
                    let _ = source_handle.await;
                    Err(Error::Source("source shutdown timed out".into()))
                }
            };

        let shutdown_result = self.sink.shutdown().await;
        pipeline_result?;
        source_result?;
        shutdown_result?;
        self.status.transition(ConnectorState::Stopped, None).await;
        info!(connector = %self.identity.name, "connector stopped");
        Ok(())
    }

    async fn consume_record(
        &mut self,
        record: SourceRecord,
        pending: &mut PendingBatch,
        snapshot_completed: &mut bool,
        connector_state: &mut Option<ConnectorStateEnvelope>,
        ack_tx: &watch::Sender<Option<SourcePosition>>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if let Some(previous) = pending.highest_position.as_ref()
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
            match self.encoder.encode_batch(event) {
                Ok(encoded) => pending.encoded.extend(encoded),
                Err(error) => {
                    self.status.increment_failed_events(1).await;
                    return Err(error);
                }
            }
            pending.latest_event_timing = Some(EventTiming {
                source_time: event.source_time,
                observed_time: event.observed_time,
            });
        }
        pending.highest_position = Some(record.position);
        if let Some(state) = record.connector_state {
            *connector_state = Some(state);
        }
        pending
            .signal_acknowledgements
            .extend(record.signal_acknowledgements);

        match record.boundary {
            RecordBoundary::SnapshotComplete => {
                *snapshot_completed = true;
                self.status
                    .transition(ConnectorState::Streaming, None)
                    .await;
                self.flush_batch(
                    pending,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                    cancellation,
                )
                .await?;
            }
            RecordBoundary::TransactionCommit => {
                self.flush_batch(
                    pending,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                    cancellation,
                )
                .await?;
            }
            RecordBoundary::Data if pending.encoded.len() >= self.config.max_batch_size => {
                self.flush_batch(
                    pending,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                    cancellation,
                )
                .await?;
            }
            RecordBoundary::Heartbeat if pending.encoded.is_empty() => {
                self.flush_batch(
                    pending,
                    *snapshot_completed,
                    connector_state,
                    ack_tx,
                    cancellation,
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn flush_batch(
        &mut self,
        pending: &mut PendingBatch,
        snapshot_completed: bool,
        connector_state: &Option<ConnectorStateEnvelope>,
        ack_tx: &watch::Sender<Option<SourcePosition>>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let Some(position) = pending.highest_position.take() else {
            return Ok(());
        };
        let delivered = pending.encoded.len();

        if !pending.encoded.is_empty() {
            let batch = DeliveryBatch {
                events: std::mem::take(&mut pending.encoded),
                highest_position: position.clone(),
            };
            if let Err(error) = self.write_sink_with_retry(&batch, cancellation).await {
                if matches!(error, Error::Cancelled) {
                    return Err(error);
                }
                error!(connector = %self.identity.name, %error, "sink delivery failed");
                self.status.increment_failed_events(delivered).await;
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
        for acknowledgement in pending.signal_acknowledgements.drain(..) {
            acknowledgement.acknowledge();
        }
        self.status
            .checkpointed(position, delivered, pending.latest_event_timing.take())
            .await;
        Ok(())
    }

    async fn validate_sink_with_retry(&mut self, cancellation: &CancellationToken) -> Result<()> {
        let mut retry = RetryState::new(self.config.errors_retry_delay_initial);
        loop {
            match self.sink.validate().await {
                Ok(()) => return Ok(()),
                Err(error @ Error::RetryableSink(_)) => {
                    if !self
                        .wait_for_sink_retry("validation", &error, &mut retry, cancellation)
                        .await?
                    {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn write_sink_with_retry(
        &mut self,
        batch: &DeliveryBatch,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut retry = RetryState::new(self.config.errors_retry_delay_initial);
        loop {
            match self.sink.write(batch).await {
                Ok(()) => return Ok(()),
                Err(error @ Error::RetryableSink(_)) => {
                    if !self
                        .wait_for_sink_retry("delivery", &error, &mut retry, cancellation)
                        .await?
                    {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn flush_sink_with_retry(&mut self, cancellation: &CancellationToken) -> Result<()> {
        let mut retry = RetryState::new(self.config.errors_retry_delay_initial);
        loop {
            match self.sink.flush().await {
                Ok(()) => return Ok(()),
                Err(error @ Error::RetryableSink(_)) => {
                    if !self
                        .wait_for_sink_retry("flush", &error, &mut retry, cancellation)
                        .await?
                    {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn wait_for_sink_retry(
        &mut self,
        operation: &'static str,
        error: &Error,
        retry: &mut RetryState,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        if self.config.errors_max_retries >= 0
            && retry.attempts >= self.config.errors_max_retries as u64
        {
            return Ok(false);
        }
        retry.attempts += 1;
        self.status.increment_sink_retry_attempts().await;
        warn!(
            connector = %self.identity.name,
            sink = self.sink.name(),
            operation,
            retry = retry.attempts,
            max_retries = self.config.errors_max_retries,
            delay_ms = retry.delay.as_millis(),
            %error,
            "retryable Sink operation failed; scheduling retry"
        );
        tokio::select! {
            () = cancellation.cancelled() => return Err(Error::Cancelled),
            () = tokio::time::sleep(retry.delay) => {}
        }
        retry.delay = retry
            .delay
            .saturating_mul(2)
            .min(self.config.errors_retry_delay_max);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Utc;
    use tokio::sync::Notify;

    use super::*;
    use crate::{
        ChangeEvent, ConnectorStateEnvelope, Durability, EventId, EventSchema, MySqlPosition,
        Operation, RecordBoundary, SourceMetadata,
    };

    struct StateSource {
        record: SourceRecord,
    }

    struct SignalSource {
        received: Arc<Mutex<Option<crate::SignalRecord>>>,
    }

    struct CheckpointSignalSource {
        position: SourcePosition,
    }

    struct CancellableStateSource {
        record: SourceRecord,
        cancelled: Arc<AtomicBool>,
    }

    struct StuckSource {
        dropped: Arc<AtomicBool>,
    }

    struct BurstSource {
        sent: Arc<AtomicUsize>,
        total: usize,
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
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

    #[async_trait]
    impl SourceConnector for SignalSource {
        fn source_type(&self) -> &'static str {
            "signal-test"
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn run(&mut self, mut context: SourceContext) -> Result<()> {
            let signal = context.signals.recv().await.ok_or(Error::Cancelled)?;
            *self.received.lock().unwrap() = Some(signal.record().clone());
            signal.acknowledge();
            Ok(())
        }
    }

    #[async_trait]
    impl SourceConnector for CheckpointSignalSource {
        fn source_type(&self) -> &'static str {
            "checkpoint-signal-test"
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn run(&mut self, mut context: SourceContext) -> Result<()> {
            let signal = context.signals.recv().await.ok_or(Error::Cancelled)?;
            context
                .output
                .send(Ok(SourceRecord {
                    event: None,
                    position: self.position.clone(),
                    boundary: RecordBoundary::TransactionCommit,
                    connector_state: None,
                    signal_acknowledgements: signal.into_acknowledgement().into_iter().collect(),
                }))
                .await
                .map_err(|_| Error::Cancelled)?;
            context
                .acknowledged
                .changed()
                .await
                .map_err(|_| Error::Cancelled)
        }
    }

    #[async_trait]
    impl SourceConnector for CancellableStateSource {
        fn source_type(&self) -> &'static str {
            "cancellable-test"
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn run(&mut self, context: SourceContext) -> Result<()> {
            context
                .output
                .send(Ok(self.record.clone()))
                .await
                .map_err(|_| Error::Cancelled)?;
            context.cancellation.cancelled().await;
            self.cancelled.store(true, Ordering::SeqCst);
            Err(Error::Cancelled)
        }
    }

    #[async_trait]
    impl SourceConnector for StuckSource {
        fn source_type(&self) -> &'static str {
            "stuck-test"
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn run(&mut self, _context: SourceContext) -> Result<()> {
            let _drop_flag = DropFlag(self.dropped.clone());
            std::future::pending().await
        }
    }

    #[async_trait]
    impl SourceConnector for BurstSource {
        fn source_type(&self) -> &'static str {
            "burst-test"
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn run(&mut self, mut context: SourceContext) -> Result<()> {
            let mut last_position = None;
            for serial in 1..=self.total {
                let event = test_event(serial as u64);
                last_position = Some(event.position.clone());
                context
                    .output
                    .send(Ok(SourceRecord::data(event)))
                    .await
                    .map_err(|_| Error::Cancelled)?;
                self.sent.fetch_add(1, Ordering::SeqCst);
            }
            while context.acknowledged.borrow().as_ref() != last_position.as_ref() {
                context
                    .acknowledged
                    .changed()
                    .await
                    .map_err(|_| Error::Cancelled)?;
            }
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
                key_schema: None,
                payload,
                payload_schema: None,
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

    struct FailingSink {
        shutdown: Arc<AtomicBool>,
    }

    struct RetryingSink {
        failures: usize,
        attempts: Arc<AtomicUsize>,
    }

    struct BlockingSink {
        blocked: bool,
        entered: Arc<Notify>,
        release: Arc<Notify>,
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

    #[async_trait]
    impl Sink for FailingSink {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn durability(&self) -> Durability {
            Durability::Durable
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write(&mut self, _batch: &DeliveryBatch) -> Result<()> {
            Err(Error::Sink("intentional delivery failure".into()))
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<()> {
            self.shutdown.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl Sink for RetryingSink {
        fn name(&self) -> &'static str {
            "retrying"
        }

        fn durability(&self) -> Durability {
            Durability::Durable
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write(&mut self, _batch: &DeliveryBatch) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures {
                Err(Error::RetryableSink(format!(
                    "intentional retryable failure {}",
                    attempt + 1
                )))
            } else {
                Ok(())
            }
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Sink for BlockingSink {
        fn name(&self) -> &'static str {
            "blocking"
        }

        fn durability(&self) -> Durability {
            Durability::Durable
        }

        async fn validate(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write(&mut self, _batch: &DeliveryBatch) -> Result<()> {
            if !self.blocked {
                self.blocked = true;
                self.entered.notify_one();
                self.release.notified().await;
            }
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

    fn test_event(event_serial: u64) -> ChangeEvent {
        let position = SourcePosition::MySql(MySqlPosition {
            binlog_filename: "mysql-bin.000001".into(),
            binlog_position: 1_000 + event_serial,
            gtid_set: None,
            server_id: 1,
            event_serial,
            snapshot: false,
        });
        ChangeEvent {
            id: EventId::deterministic(
                "runtime-test",
                "mysql",
                &position,
                "app.orders",
                event_serial,
            ),
            source: SourceMetadata {
                connector: "mysql".into(),
                connector_name: "runtime-test".into(),
                database: "app".into(),
                schema: None,
                table: Some("orders".into()),
                snapshot: false,
                version: "test".into(),
                attributes: BTreeMap::new(),
            },
            position,
            transaction: None,
            operation: Operation::Create,
            before: None,
            after: None,
            schema: EventSchema {
                name: "orders".into(),
                version: 1,
                fields: Vec::new(),
            },
            source_time: Some(Utc::now()),
            observed_time: Utc::now(),
        }
    }

    struct SingleEncoder;

    impl EventEncoder for SingleEncoder {
        fn content_type(&self) -> &'static str {
            "application/test"
        }

        fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
            Ok(EncodedEvent {
                id: event.id.clone(),
                destination: "orders".into(),
                key: Some(Bytes::from_static(b"1")),
                key_schema: None,
                payload: Some(Bytes::from_static(b"value")),
                payload_schema: None,
                headers: BTreeMap::new(),
            })
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
                signal_acknowledgements: Vec::new(),
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
    async fn exposes_a_typed_signal_sender_before_runtime_start() {
        let received = Arc::new(Mutex::new(None));
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-signal-test"),
            Box::new(SignalSource {
                received: received.clone(),
            }),
            Arc::new(UnusedEncoder),
            Box::new(NoopSink),
            Arc::new(MemoryCheckpointStore::default()),
            RuntimeConfig::default(),
            RuntimeStatus::new("runtime-signal-test"),
        );
        let sender = runtime.signal_sender();
        let signal = crate::SignalRecord::new(
            "snapshot-1",
            "execute-snapshot",
            serde_json::json!({"type": "incremental"}),
        );
        sender.send(signal.clone()).await.unwrap();

        runtime.run(CancellationToken::new()).await.unwrap();

        assert_eq!(*received.lock().unwrap(), Some(signal));
    }

    #[tokio::test]
    async fn acknowledges_a_signal_only_after_its_checkpoint_is_saved() {
        let position = SourcePosition::MySql(MySqlPosition {
            binlog_filename: "binlog.000001".into(),
            binlog_position: 120,
            gtid_set: None,
            server_id: 1,
            event_serial: 0,
            snapshot: false,
        });
        let store = Arc::new(MemoryCheckpointStore::default());
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-durable-signal-test"),
            Box::new(CheckpointSignalSource {
                position: position.clone(),
            }),
            Arc::new(UnusedEncoder),
            Box::new(NoopSink),
            store.clone(),
            RuntimeConfig::default(),
            RuntimeStatus::new("runtime-durable-signal-test"),
        );
        let sender = runtime.signal_sender();
        let send = tokio::spawn(async move {
            sender
                .send_and_wait(crate::SignalRecord::new(
                    "snapshot-2",
                    "execute-snapshot",
                    serde_json::json!({"type": "incremental"}),
                ))
                .await
        });

        runtime.run(CancellationToken::new()).await.unwrap();
        send.await.unwrap().unwrap();

        assert_eq!(
            store
                .load("runtime-durable-signal-test")
                .await
                .unwrap()
                .unwrap()
                .source_position,
            position
        );
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
        let source_time = Utc::now() - chrono::Duration::seconds(2);
        let observed_time = Utc::now();
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
            source_time: Some(source_time),
            observed_time,
        };
        let source = StateSource {
            record: SourceRecord {
                event: Some(event),
                position: position.clone(),
                boundary: RecordBoundary::TransactionCommit,
                connector_state: None,
                signal_acknowledgements: Vec::new(),
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
        let status = status.snapshot().await;
        assert_eq!(status.delivered_events, 2);
        assert_eq!(status.last_source_event_at, Some(source_time));
        assert_eq!(status.last_event_observed_at, Some(observed_time));
        assert!(status.source_lag_millis.is_some_and(|lag| lag >= 2_000));
    }

    #[tokio::test]
    async fn retries_the_same_sink_batch_before_checkpointing() {
        let event = test_event(10);
        let position = event.position.clone();
        let store = Arc::new(MemoryCheckpointStore::default());
        let attempts = Arc::new(AtomicUsize::new(0));
        let status = RuntimeStatus::new("runtime-retry-test");
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-retry-test"),
            Box::new(StateSource {
                record: SourceRecord {
                    event: Some(event),
                    position: position.clone(),
                    boundary: RecordBoundary::TransactionCommit,
                    connector_state: None,
                    signal_acknowledgements: Vec::new(),
                },
            }),
            Arc::new(SingleEncoder),
            Box::new(RetryingSink {
                failures: 2,
                attempts: attempts.clone(),
            }),
            store.clone(),
            RuntimeConfig {
                errors_max_retries: 2,
                errors_retry_delay_initial: Duration::from_millis(1),
                errors_retry_delay_max: Duration::from_millis(2),
                ..RuntimeConfig::default()
            },
            status.clone(),
        );

        runtime.run(CancellationToken::new()).await.unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
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
        let status = status.snapshot().await;
        assert_eq!(status.sink_retry_attempts, 2);
        assert_eq!(status.delivered_events, 1);
        assert_eq!(status.failed_events, 0);
    }

    #[tokio::test]
    async fn exhausts_sink_retries_without_advancing_the_checkpoint() {
        let event = test_event(11);
        let position = event.position.clone();
        let store = Arc::new(MemoryCheckpointStore::default());
        let attempts = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let status = RuntimeStatus::new("runtime-retry-exhaustion-test");
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-retry-exhaustion-test"),
            Box::new(CancellableStateSource {
                record: SourceRecord {
                    event: Some(event),
                    position,
                    boundary: RecordBoundary::TransactionCommit,
                    connector_state: None,
                    signal_acknowledgements: Vec::new(),
                },
                cancelled: cancelled.clone(),
            }),
            Arc::new(SingleEncoder),
            Box::new(RetryingSink {
                failures: usize::MAX,
                attempts: attempts.clone(),
            }),
            store.clone(),
            RuntimeConfig {
                errors_max_retries: 2,
                errors_retry_delay_initial: Duration::from_millis(1),
                errors_retry_delay_max: Duration::from_millis(2),
                ..RuntimeConfig::default()
            },
            status.clone(),
        );

        let error = runtime.run(CancellationToken::new()).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("intentional retryable failure 3")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(store.checkpoint.lock().unwrap().is_none());
        assert!(cancelled.load(Ordering::SeqCst));
        let status = status.snapshot().await;
        assert_eq!(status.sink_retry_attempts, 2);
        assert_eq!(status.delivered_events, 0);
        assert_eq!(status.failed_events, 1);
        assert_eq!(status.state, ConnectorState::Failed);
    }

    #[tokio::test]
    async fn applies_bounded_backpressure_while_the_sink_is_blocked() {
        let total = 16;
        let sent = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let store = Arc::new(MemoryCheckpointStore::default());
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-backpressure-test"),
            Box::new(BurstSource {
                sent: sent.clone(),
                total,
            }),
            Arc::new(SingleEncoder),
            Box::new(BlockingSink {
                blocked: false,
                entered: entered.clone(),
                release: release.clone(),
            }),
            store.clone(),
            RuntimeConfig {
                channel_capacity: 2,
                max_batch_size: 1,
                flush_interval: Duration::from_secs(60),
                ..RuntimeConfig::default()
            },
            RuntimeStatus::new("runtime-backpressure-test"),
        );
        let task = tokio::spawn(runtime.run(CancellationToken::new()));

        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(sent.load(Ordering::SeqCst) <= 3);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(sent.load(Ordering::SeqCst), total);
        let checkpoint_position = store
            .checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .source_position
            .clone();
        let SourcePosition::MySql(position) = checkpoint_position else {
            panic!("backpressure checkpoint used the wrong source position");
        };
        assert_eq!(position.event_serial, total as u64);
    }

    #[tokio::test]
    async fn cancels_source_shuts_down_sink_and_counts_failed_batches() {
        let position = SourcePosition::MySql(MySqlPosition {
            binlog_filename: "mysql-bin.000001".into(),
            binlog_position: 512,
            gtid_set: None,
            server_id: 1,
            event_serial: 2,
            snapshot: false,
        });
        let event = ChangeEvent {
            id: EventId::deterministic("orders", "mysql", &position, "app.orders", 2),
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
            source_time: Some(Utc::now()),
            observed_time: Utc::now(),
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let status = RuntimeStatus::new("runtime-failure-test");
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-failure-test"),
            Box::new(CancellableStateSource {
                record: SourceRecord {
                    event: Some(event),
                    position,
                    boundary: RecordBoundary::TransactionCommit,
                    connector_state: None,
                    signal_acknowledgements: Vec::new(),
                },
                cancelled: cancelled.clone(),
            }),
            Arc::new(BatchEncoder),
            Box::new(FailingSink {
                shutdown: shutdown.clone(),
            }),
            Arc::new(MemoryCheckpointStore::default()),
            RuntimeConfig::default(),
            status.clone(),
        );

        let error = runtime.run(CancellationToken::new()).await.unwrap_err();
        assert!(error.to_string().contains("intentional delivery failure"));
        assert!(cancelled.load(Ordering::SeqCst));
        assert!(shutdown.load(Ordering::SeqCst));
        let status = status.snapshot().await;
        assert_eq!(status.state, ConnectorState::Failed);
        assert_eq!(status.failed_events, 2);
        assert!(
            status
                .state_reason
                .is_some_and(|reason| reason.contains("intentional delivery failure"))
        );
    }

    #[tokio::test]
    async fn aborts_a_source_that_exceeds_the_shutdown_timeout() {
        let dropped = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let status = RuntimeStatus::new("runtime-timeout-test");
        let runtime = ConnectorRuntime::new(
            ConnectorIdentity::new("runtime-timeout-test"),
            Box::new(StuckSource {
                dropped: dropped.clone(),
            }),
            Arc::new(UnusedEncoder),
            Box::new(FailingSink {
                shutdown: shutdown.clone(),
            }),
            Arc::new(MemoryCheckpointStore::default()),
            RuntimeConfig {
                shutdown_timeout: Duration::from_millis(10),
                ..RuntimeConfig::default()
            },
            status.clone(),
        );
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = runtime.run(cancellation).await.unwrap_err();
        assert!(error.to_string().contains("source shutdown timed out"));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(shutdown.load(Ordering::SeqCst));
        let status = status.snapshot().await;
        assert_eq!(status.state, ConnectorState::Failed);
        assert_eq!(status.failed_events, 0);
    }
}
