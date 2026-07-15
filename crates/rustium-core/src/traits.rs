use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{ChangeEvent, DeliveryBatch, EncodedEvent, Result, SourcePosition, SourceRecord};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub schema_version: u32,
    pub connector_name: String,
    pub generation: uuid::Uuid,
    pub source_position: SourcePosition,
    pub snapshot_completed: bool,
    pub config_fingerprint: String,
    pub updated_at: SystemTime,
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, connector_name: &str) -> Result<Option<Checkpoint>>;
    async fn save(&self, checkpoint: &Checkpoint) -> Result<()>;
    async fn delete(&self, connector_name: &str) -> Result<()>;
}

pub struct SourceContext {
    pub output: mpsc::Sender<Result<SourceRecord>>,
    pub acknowledged: watch::Receiver<Option<SourcePosition>>,
    pub initial_checkpoint: Option<Checkpoint>,
    pub cancellation: CancellationToken,
}

#[async_trait]
pub trait SourceConnector: Send {
    fn source_type(&self) -> &'static str;
    async fn validate(&mut self) -> Result<()>;
    async fn run(&mut self, context: SourceContext) -> Result<()>;
}

pub trait EventEncoder: Send + Sync {
    fn content_type(&self) -> &'static str;
    fn encode(&self, event: &ChangeEvent) -> Result<EncodedEvent>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    BestEffort,
    Durable,
}

#[async_trait]
pub trait Sink: Send {
    fn name(&self) -> &'static str;
    fn durability(&self) -> Durability;
    async fn validate(&mut self) -> Result<()>;
    async fn write(&mut self, batch: &DeliveryBatch) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn shutdown(&mut self) -> Result<()> {
        self.flush().await
    }
}
