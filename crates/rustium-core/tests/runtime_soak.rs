use std::{
    collections::BTreeMap,
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, ChangeEvent, Checkpoint, CheckpointStore, ConnectorIdentity,
    ConnectorRuntime, ConnectorState, DeliveryBatch, Durability, EncodedEvent, Error, EventEncoder,
    EventId, EventSchema, MySqlPosition, Operation, RecordBoundary, Result, RuntimeConfig,
    RuntimeStatus, Sink, SourceConnector, SourceContext, SourceMetadata, SourcePosition,
    SourceRecord,
};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

const DEFAULT_SOAK_CYCLES: usize = 64;
const MAX_SOAK_CYCLES: usize = 10_000;

#[derive(Default)]
struct ProbeCheckpointStore {
    checkpoint: Mutex<Option<Checkpoint>>,
    saves: AtomicUsize,
}

impl ProbeCheckpointStore {
    fn with_checkpoint(checkpoint: Checkpoint) -> Self {
        Self {
            checkpoint: Mutex::new(Some(checkpoint)),
            saves: AtomicUsize::new(0),
        }
    }

    fn serial(&self) -> Option<usize> {
        self.checkpoint
            .lock()
            .unwrap()
            .as_ref()
            .map(|checkpoint| position_serial(&checkpoint.source_position))
    }
}

#[async_trait]
impl CheckpointStore for ProbeCheckpointStore {
    async fn load(&self, _connector_name: &str) -> Result<Option<Checkpoint>> {
        Ok(self.checkpoint.lock().unwrap().clone())
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        *self.checkpoint.lock().unwrap() = Some(checkpoint.clone());
        self.saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn delete(&self, _connector_name: &str) -> Result<()> {
        *self.checkpoint.lock().unwrap() = None;
        Ok(())
    }
}

struct SequenceSource {
    first_serial: usize,
    records: usize,
    sent: Arc<AtomicUsize>,
}

#[async_trait]
impl SourceConnector for SequenceSource {
    fn source_type(&self) -> &'static str {
        "runtime-soak"
    }

    async fn validate(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self, mut context: SourceContext) -> Result<()> {
        let last_serial = self.first_serial + self.records - 1;
        for serial in self.first_serial..=last_serial {
            let record = transaction_record(serial);
            tokio::select! {
                () = context.cancellation.cancelled() => return Err(Error::Cancelled),
                result = context.output.send(Ok(record)) => {
                    result.map_err(|_| Error::Cancelled)?;
                    self.sent.fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        while context.acknowledged.borrow().as_ref().map(position_serial) != Some(last_serial) {
            tokio::select! {
                () = context.cancellation.cancelled() => return Err(Error::Cancelled),
                result = context.acknowledged.changed() => {
                    result.map_err(|_| Error::Cancelled)?;
                }
            }
        }
        Ok(())
    }
}

struct OneRecordUntilCancelledSource {
    serial: usize,
    cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl SourceConnector for OneRecordUntilCancelledSource {
    fn source_type(&self) -> &'static str {
        "runtime-soak"
    }

    async fn validate(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self, context: SourceContext) -> Result<()> {
        context
            .output
            .send(Ok(transaction_record(self.serial)))
            .await
            .map_err(|_| Error::Cancelled)?;
        context.cancellation.cancelled().await;
        self.cancelled.store(true, Ordering::SeqCst);
        Err(Error::Cancelled)
    }
}

struct SoakEncoder;

impl EventEncoder for SoakEncoder {
    fn content_type(&self) -> &'static str {
        "application/runtime-soak"
    }

    fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent> {
        let serial = position_serial(&event.position);
        Ok(EncodedEvent {
            id: event.id.clone(),
            destination: "runtime-soak".into(),
            key: Some(Bytes::from(serial.to_string())),
            key_schema: None,
            payload: Some(Bytes::from(format!(r#"{{"serial":{serial}}}"#))),
            payload_schema: None,
            headers: BTreeMap::from([("runtime-soak".into(), serial.to_string())]),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BatchFingerprint {
    position: SourcePosition,
    events: Vec<EventFingerprint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EventFingerprint {
    id: EventId,
    destination: String,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    headers: BTreeMap<String, String>,
}

impl From<&DeliveryBatch> for BatchFingerprint {
    fn from(batch: &DeliveryBatch) -> Self {
        Self {
            position: batch.highest_position.clone(),
            events: batch
                .events
                .iter()
                .map(|event| EventFingerprint {
                    id: event.id.clone(),
                    destination: event.destination.clone(),
                    key: event.key.as_ref().map(|value| value.to_vec()),
                    payload: event.payload.as_ref().map(|value| value.to_vec()),
                    headers: event.headers.clone(),
                })
                .collect(),
        }
    }
}

struct CyclingRetrySink {
    checkpoint_store: Arc<ProbeCheckpointStore>,
    attempts: Arc<Mutex<Vec<usize>>>,
    fingerprints: Vec<Option<BatchFingerprint>>,
    successful: Arc<Mutex<Vec<usize>>>,
    blocked: mpsc::UnboundedSender<usize>,
    releases: Arc<Semaphore>,
    shutdown: Arc<AtomicBool>,
}

#[async_trait]
impl Sink for CyclingRetrySink {
    fn name(&self) -> &'static str {
        "cycling-retry"
    }

    fn durability(&self) -> Durability {
        Durability::Durable
    }

    async fn validate(&mut self) -> Result<()> {
        Ok(())
    }

    async fn write(&mut self, batch: &DeliveryBatch) -> Result<()> {
        let serial = position_serial(&batch.highest_position);
        assert_eq!(self.checkpoint_store.serial(), previous_serial(serial));

        let fingerprint = BatchFingerprint::from(batch);
        let expected = self.fingerprints[serial].get_or_insert_with(|| fingerprint.clone());
        assert_eq!(&fingerprint, expected, "retry changed batch {serial}");

        let attempt = {
            let mut attempts = self.attempts.lock().unwrap();
            attempts[serial] += 1;
            attempts[serial]
        };
        match attempt {
            1 => {
                self.blocked
                    .send(serial)
                    .map_err(|_| Error::Invariant("runtime soak observer closed".into()))?;
                self.releases
                    .acquire()
                    .await
                    .map_err(|_| Error::Cancelled)?
                    .forget();
                Err(Error::RetryableSink(format!(
                    "runtime soak retry {attempt} for batch {serial}"
                )))
            }
            2 => Err(Error::RetryableSink(format!(
                "runtime soak retry {attempt} for batch {serial}"
            ))),
            3 => {
                self.successful.lock().unwrap().push(serial);
                Ok(())
            }
            _ => Err(Error::Invariant(format!(
                "runtime soak attempted batch {serial} more than three times"
            ))),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct AlwaysRetrySink {
    checkpoint_store: Arc<ProbeCheckpointStore>,
    checkpoint_serial: usize,
    fingerprint: Option<BatchFingerprint>,
    attempts: Arc<AtomicUsize>,
    attempted: Option<mpsc::UnboundedSender<()>>,
    shutdown: Arc<AtomicBool>,
}

#[async_trait]
impl Sink for AlwaysRetrySink {
    fn name(&self) -> &'static str {
        "always-retry"
    }

    fn durability(&self) -> Durability {
        Durability::Durable
    }

    async fn validate(&mut self) -> Result<()> {
        Ok(())
    }

    async fn write(&mut self, batch: &DeliveryBatch) -> Result<()> {
        assert_eq!(self.checkpoint_store.serial(), Some(self.checkpoint_serial));
        let fingerprint = BatchFingerprint::from(batch);
        if let Some(expected) = &self.fingerprint {
            assert_eq!(&fingerprint, expected, "retry changed exhausted batch");
        } else {
            self.fingerprint = Some(fingerprint);
        }

        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 1
            && let Some(attempted) = self.attempted.take()
        {
            attempted
                .send(())
                .map_err(|_| Error::Invariant("runtime cancellation observer closed".into()))?;
        }
        Err(Error::RetryableSink(format!(
            "runtime soak persistent failure {attempt}"
        )))
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "required dedicated runtime soak gate"]
async fn replays_same_batches_and_bounds_queues_across_retry_cycles() {
    let cycles = soak_cycles();
    let sent = Arc::new(AtomicUsize::new(0));
    let checkpoint_store = Arc::new(ProbeCheckpointStore::default());
    let attempts = Arc::new(Mutex::new(vec![0; cycles + 1]));
    let successful = Arc::new(Mutex::new(Vec::with_capacity(cycles)));
    let (blocked_tx, mut blocked_rx) = mpsc::unbounded_channel();
    let releases = Arc::new(Semaphore::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    let status = RuntimeStatus::new("runtime-retry-soak");
    let runtime = ConnectorRuntime::new(
        ConnectorIdentity::new("runtime-retry-soak"),
        Box::new(SequenceSource {
            first_serial: 1,
            records: cycles,
            sent: sent.clone(),
        }),
        Arc::new(SoakEncoder),
        Box::new(CyclingRetrySink {
            checkpoint_store: checkpoint_store.clone(),
            attempts: attempts.clone(),
            fingerprints: vec![None; cycles + 1],
            successful: successful.clone(),
            blocked: blocked_tx,
            releases: releases.clone(),
            shutdown: shutdown.clone(),
        }),
        checkpoint_store.clone(),
        retry_runtime_config(2),
        status.clone(),
    );
    let runtime_task = tokio::spawn(runtime.run(CancellationToken::new()));

    for serial in 1..=cycles {
        let blocked_serial = tokio::time::timeout(Duration::from_secs(5), blocked_rx.recv())
            .await
            .expect("runtime did not enter the expected blocked Sink attempt")
            .expect("runtime soak Sink closed its observer");
        assert_eq!(blocked_serial, serial);

        let maximum_sent = (serial + 1).min(cycles);
        tokio::time::timeout(Duration::from_secs(2), async {
            while sent.load(Ordering::SeqCst) < maximum_sent {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Source did not fill the bounded runtime channel");
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            sent.load(Ordering::SeqCst),
            maximum_sent,
            "Source escaped capacity-one backpressure at batch {serial}"
        );
        assert_eq!(checkpoint_store.serial(), previous_serial(serial));
        releases.add_permits(1);
    }

    tokio::time::timeout(Duration::from_secs(30), runtime_task)
        .await
        .expect("runtime retry soak timed out")
        .expect("runtime retry soak task panicked")
        .expect("runtime retry soak failed");

    assert_eq!(sent.load(Ordering::SeqCst), cycles);
    assert_eq!(checkpoint_store.serial(), Some(cycles));
    assert_eq!(checkpoint_store.saves.load(Ordering::SeqCst), cycles);
    assert!(
        attempts.lock().unwrap()[1..]
            .iter()
            .all(|attempts| *attempts == 3)
    );
    assert_eq!(
        *successful.lock().unwrap(),
        (1..=cycles).collect::<Vec<_>>()
    );
    assert!(shutdown.load(Ordering::SeqCst));

    let status = status.snapshot().await;
    assert_eq!(status.state, ConnectorState::Stopped);
    assert_eq!(status.sink_retry_attempts, (cycles * 2) as u64);
    assert_eq!(status.delivered_events, cycles as u64);
    assert_eq!(status.failed_events, 0);
    assert!(status.queue_depth <= 1);
}

#[tokio::test]
#[ignore = "required dedicated runtime soak gate"]
async fn preserves_checkpoints_and_cleans_up_after_retry_exhaustion() {
    for cycle in 1..=soak_cycles() {
        let identity = ConnectorIdentity::new(format!("runtime-exhaustion-soak-{cycle}"));
        let checkpoint_serial = cycle * 10;
        let checkpoint_store = Arc::new(ProbeCheckpointStore::with_checkpoint(checkpoint(
            &identity,
            checkpoint_serial,
        )));
        let cancelled = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let status = RuntimeStatus::new(&identity.name);
        let runtime = ConnectorRuntime::new(
            identity,
            Box::new(OneRecordUntilCancelledSource {
                serial: checkpoint_serial + 1,
                cancelled: cancelled.clone(),
            }),
            Arc::new(SoakEncoder),
            Box::new(AlwaysRetrySink {
                checkpoint_store: checkpoint_store.clone(),
                checkpoint_serial,
                fingerprint: None,
                attempts: attempts.clone(),
                attempted: None,
                shutdown: shutdown.clone(),
            }),
            checkpoint_store.clone(),
            retry_runtime_config(2),
            status.clone(),
        );

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.run(CancellationToken::new()),
        )
        .await
        .expect("runtime retry-exhaustion cycle timed out")
        .expect_err("runtime retry-exhaustion cycle unexpectedly succeeded");

        assert!(error.to_string().contains("persistent failure 3"));
        assert_eq!(checkpoint_store.serial(), Some(checkpoint_serial));
        assert_eq!(checkpoint_store.saves.load(Ordering::SeqCst), 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(cancelled.load(Ordering::SeqCst));
        assert!(shutdown.load(Ordering::SeqCst));

        let status = status.snapshot().await;
        assert_eq!(status.state, ConnectorState::Failed);
        assert_eq!(status.sink_retry_attempts, 2);
        assert_eq!(status.delivered_events, 0);
        assert_eq!(status.failed_events, 1);
    }
}

#[tokio::test]
#[ignore = "required dedicated runtime soak gate"]
async fn cancels_retry_backoff_and_stops_cleanly() {
    for cycle in 1..=soak_cycles() {
        let identity = ConnectorIdentity::new(format!("runtime-cancellation-soak-{cycle}"));
        let checkpoint_serial = cycle * 10;
        let checkpoint_store = Arc::new(ProbeCheckpointStore::with_checkpoint(checkpoint(
            &identity,
            checkpoint_serial,
        )));
        let cancelled = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let status = RuntimeStatus::new(&identity.name);
        let cancellation = CancellationToken::new();
        let runtime = ConnectorRuntime::new(
            identity,
            Box::new(OneRecordUntilCancelledSource {
                serial: checkpoint_serial + 1,
                cancelled: cancelled.clone(),
            }),
            Arc::new(SoakEncoder),
            Box::new(AlwaysRetrySink {
                checkpoint_store: checkpoint_store.clone(),
                checkpoint_serial,
                fingerprint: None,
                attempts: attempts.clone(),
                attempted: Some(attempted_tx),
                shutdown: shutdown.clone(),
            }),
            checkpoint_store.clone(),
            RuntimeConfig {
                channel_capacity: 1,
                max_batch_size: 1,
                flush_interval: Duration::from_secs(60),
                shutdown_timeout: Duration::from_secs(2),
                errors_max_retries: -1,
                errors_retry_delay_initial: Duration::from_secs(60),
                errors_retry_delay_max: Duration::from_secs(60),
                config_fingerprint: String::new(),
            },
            status.clone(),
        );
        let runtime_task = tokio::spawn(runtime.run(cancellation.clone()));

        tokio::time::timeout(Duration::from_secs(5), attempted_rx.recv())
            .await
            .expect("runtime did not enter retry backoff")
            .expect("runtime cancellation observer closed");
        assert_eq!(checkpoint_store.serial(), Some(checkpoint_serial));
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), runtime_task)
            .await
            .expect("runtime did not interrupt retry backoff")
            .expect("runtime cancellation task panicked")
            .expect("runtime cancellation returned an operational error");

        assert_eq!(checkpoint_store.serial(), Some(checkpoint_serial));
        assert_eq!(checkpoint_store.saves.load(Ordering::SeqCst), 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(cancelled.load(Ordering::SeqCst));
        assert!(shutdown.load(Ordering::SeqCst));

        let status = status.snapshot().await;
        assert_eq!(status.state, ConnectorState::Stopped);
        assert_eq!(status.sink_retry_attempts, 1);
        assert_eq!(status.delivered_events, 0);
        assert_eq!(status.failed_events, 0);
    }
}

fn retry_runtime_config(errors_max_retries: i32) -> RuntimeConfig {
    RuntimeConfig {
        channel_capacity: 1,
        max_batch_size: 1,
        flush_interval: Duration::from_secs(60),
        shutdown_timeout: Duration::from_secs(2),
        errors_max_retries,
        errors_retry_delay_initial: Duration::from_millis(1),
        errors_retry_delay_max: Duration::from_millis(1),
        config_fingerprint: String::new(),
    }
}

fn soak_cycles() -> usize {
    let cycles = env::var("RUSTIUM_RUNTIME_SOAK_CYCLES")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("RUSTIUM_RUNTIME_SOAK_CYCLES must be an integer")
        })
        .unwrap_or(DEFAULT_SOAK_CYCLES);
    assert!(
        (1..=MAX_SOAK_CYCLES).contains(&cycles),
        "RUSTIUM_RUNTIME_SOAK_CYCLES must be between 1 and {MAX_SOAK_CYCLES}"
    );
    cycles
}

fn checkpoint(identity: &ConnectorIdentity, serial: usize) -> Checkpoint {
    Checkpoint {
        schema_version: CHECKPOINT_SCHEMA_VERSION,
        connector_name: identity.name.clone(),
        generation: identity.generation,
        source_position: mysql_position(serial),
        snapshot_completed: true,
        config_fingerprint: String::new(),
        updated_at: SystemTime::now(),
        connector_state: None,
    }
}

fn transaction_record(serial: usize) -> SourceRecord {
    SourceRecord {
        event: Some(test_event(serial)),
        position: mysql_position(serial),
        boundary: RecordBoundary::TransactionCommit,
        connector_state: None,
        signal_acknowledgements: Vec::new(),
    }
}

fn test_event(serial: usize) -> ChangeEvent {
    let position = mysql_position(serial);
    ChangeEvent {
        id: EventId::deterministic(
            "runtime-soak",
            "mysql",
            &position,
            "app.runtime_soak",
            serial as u64,
        ),
        source: SourceMetadata {
            connector: "mysql".into(),
            connector_name: "runtime-soak".into(),
            database: "app".into(),
            schema: None,
            table: Some("runtime_soak".into()),
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
            name: "runtime_soak".into(),
            version: 1,
            fields: Vec::new(),
        },
        source_time: Some(Utc::now()),
        observed_time: Utc::now(),
    }
}

fn mysql_position(serial: usize) -> SourcePosition {
    SourcePosition::MySql(MySqlPosition {
        binlog_filename: "mysql-bin.runtime-soak".into(),
        binlog_position: 1_000 + serial as u64,
        gtid_set: None,
        server_id: 1,
        event_serial: serial as u64,
        snapshot: false,
    })
}

fn position_serial(position: &SourcePosition) -> usize {
    let SourcePosition::MySql(position) = position else {
        panic!("runtime soak expected a MySQL test position");
    };
    position.event_serial as usize
}

fn previous_serial(serial: usize) -> Option<usize> {
    (serial > 1).then_some(serial - 1)
}
