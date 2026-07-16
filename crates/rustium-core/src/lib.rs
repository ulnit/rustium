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
    WireSchema, WireSchemaType,
};
pub use runtime::{
    ConnectorRuntime, ConnectorState, RetryPolicy, RuntimeConfig, RuntimeStatus, StatusSnapshot,
};
pub use signal::{
    SignalAcknowledgement, SignalDelivery, SignalRecord, SignalSender, signal_channel,
};
pub use traits::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, CheckpointStore, Durability, EventEncoder, Sink,
    SourceConnector, SourceContext,
};
