use std::sync::Arc;

use rustium_config::Config;
use rustium_core::{
    CheckpointStore, ConnectorIdentity, ConnectorRuntime, Error, EventEncoder, Result,
    RuntimeConfig, RuntimeStatus,
};
use rustium_format_json::{DebeziumJsonEncoder, JsonEncoderConfig};
use rustium_postgresql::PostgresSource;
use rustium_sink_stdout::StdoutSink;
use rustium_state::SqliteCheckpointStore;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load("rustium.yaml")?;
    let source_config = config.source.as_postgresql().cloned().ok_or_else(|| {
        Error::Configuration("this application expects a PostgreSQL source".into())
    })?;
    let heartbeat_topics_prefix = source_config.heartbeat_topics_prefix.clone();
    let heartbeat_topic_name = source_config.heartbeat_topic_name.clone();

    let source = Box::new(
        PostgresSource::new(
            &config.metadata.name,
            source_config,
            config.snapshot.clone(),
        )
        .with_retry_policy(config.runtime.retry_policy()),
    );
    let encoder: Arc<dyn EventEncoder> = Arc::new(DebeziumJsonEncoder::new(JsonEncoderConfig {
        topic_prefix: config.sink.topic_prefix().into(),
        unavailable_value: config.format.unavailable_value.clone(),
        tombstones_on_delete: config.format.tombstones_on_delete,
        heartbeat_topics_prefix,
        heartbeat_topic_name,
    }));
    let checkpoints: Arc<dyn CheckpointStore> =
        Arc::new(SqliteCheckpointStore::open(&config.state.path).await?);
    let status = RuntimeStatus::new(&config.metadata.name);
    let runtime = ConnectorRuntime::new(
        ConnectorIdentity::new(&config.metadata.name),
        source,
        encoder,
        Box::new(StdoutSink::default()),
        checkpoints,
        RuntimeConfig {
            channel_capacity: config.runtime.channel_capacity,
            max_batch_size: config.runtime.max_batch_size,
            flush_interval: config.runtime.flush_interval,
            shutdown_timeout: config.runtime.shutdown_timeout,
            errors_max_retries: config.runtime.errors_max_retries,
            errors_retry_delay_initial: config.runtime.errors_retry_delay_initial,
            errors_retry_delay_max: config.runtime.errors_retry_delay_max,
            config_fingerprint: config.fingerprint(),
        },
        status,
    );
    let _signal_sender = runtime.signal_sender();

    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal_cancellation.cancel();
    });
    runtime.run(cancellation).await
}
