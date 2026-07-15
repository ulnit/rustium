//! Core contracts and runtime for Rustium.

mod error;
mod event;
mod runtime;
mod signal;
mod traits;

pub use error::{Error, Result};
pub use event::{
    ChangeEvent, ConnectorIdentity, ConnectorStateEnvelope, DataValue, DeliveryBatch, EncodedEvent,
    EventId, EventSchema, FieldSchema, MySqlPosition, Operation, PostgresPosition, RecordBoundary,
    Row, SourceMetadata, SourcePosition, SourceRecord, SqlServerPosition, TransactionMetadata,
};
pub use runtime::{ConnectorRuntime, ConnectorState, RuntimeConfig, RuntimeStatus, StatusSnapshot};
pub use signal::{SignalRecord, SignalSender, signal_channel};
pub use traits::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, CheckpointStore, Durability, EventEncoder, Sink,
    SourceConnector, SourceContext,
};
