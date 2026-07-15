use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use rustium_config::{Config, FormatType, LogFormat, SinkConfig, SourceConfig};
use rustium_core::{
    CheckpointStore, ConnectorIdentity, ConnectorRuntime, EventEncoder, Result, RuntimeConfig,
    RuntimeStatus, Sink, SourceConnector,
};
use rustium_format_json::{DebeziumJsonEncoder, JsonEncoderConfig, RustiumJsonEncoder};
use rustium_mysql::MySqlSource;
use rustium_postgresql::PostgresSource;
use rustium_sink_kafka::KafkaSink;
use rustium_sink_stdout::StdoutSink;
use rustium_sqlserver::SqlServerSource;
use rustium_state::SqliteCheckpointStore;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "rustium",
    version,
    about = "Change Data Capture, reimagined in Rust"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(short, long, env = "RUSTIUM_CONFIG")]
        config: PathBuf,
    },
    Validate {
        #[arg(short, long, env = "RUSTIUM_CONFIG")]
        config: PathBuf,
    },
    State {
        #[command(subcommand)]
        command: StateCommand,
    },
}

#[derive(Debug, Subcommand)]
enum StateCommand {
    Reset {
        #[arg(short, long, env = "RUSTIUM_CONFIG")]
        config: PathBuf,
        #[arg(long)]
        confirm: bool,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = execute().await {
        eprintln!("rustium: {error}");
        std::process::exit(1);
    }
}

async fn execute() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run { config } => run(Config::load(config)?).await,
        Command::Validate { config } => validate(Config::load(config)?).await,
        Command::State {
            command: StateCommand::Reset { config, confirm },
        } => reset_state(Config::load(config)?, confirm).await,
    }
}

async fn run(config: Config) -> Result<()> {
    initialize_tracing(&config);
    log_compatibility_warnings(&config);
    let cancellation = CancellationToken::new();
    install_signal_handler(cancellation.clone());

    let status = RuntimeStatus::new(&config.metadata.name);
    let bind: SocketAddr = config.server.bind.parse().map_err(|error| {
        rustium_core::Error::Configuration(format!("invalid server.bind: {error}"))
    })?;
    let server_cancel = cancellation.child_token();
    let server_status = status.clone();
    let enable_mutations = config.server.enable_mutations;
    let server = tokio::spawn(async move {
        rustium_server::serve(bind, server_status, server_cancel, enable_mutations).await
    });

    let runtime = build_runtime(&config, status).await?;
    let runtime_result = runtime.run(cancellation.clone()).await;
    cancellation.cancel();
    let server_result = server.await.map_err(|error| {
        rustium_core::Error::Source(format!("management server task failed: {error}"))
    })?;
    runtime_result?;
    server_result
}

async fn validate(config: Config) -> Result<()> {
    initialize_tracing(&config);
    log_compatibility_warnings(&config);
    let mut source = build_source(&config)?;
    source.validate().await?;
    let mut sink = build_sink(&config)?;
    sink.validate().await?;
    println!("configuration and external dependencies are valid");
    Ok(())
}

async fn reset_state(config: Config, confirm: bool) -> Result<()> {
    if !confirm {
        return Err(rustium_core::Error::Configuration(
            "state reset requires --confirm".into(),
        ));
    }
    let store = SqliteCheckpointStore::open(&config.state.path).await?;
    store.delete(&config.metadata.name).await?;
    println!("deleted checkpoint for {}", config.metadata.name);
    Ok(())
}

async fn build_runtime(config: &Config, status: RuntimeStatus) -> Result<ConnectorRuntime> {
    let identity = ConnectorIdentity::new(&config.metadata.name);
    let source = build_source(config)?;
    let encoder = build_encoder(config);
    let sink = build_sink(config)?;
    let checkpoint_store: Arc<dyn CheckpointStore> =
        Arc::new(SqliteCheckpointStore::open(&config.state.path).await?);
    let runtime = RuntimeConfig {
        channel_capacity: config.runtime.channel_capacity,
        max_batch_size: config.runtime.max_batch_size,
        flush_interval: config.runtime.flush_interval,
        shutdown_timeout: config.runtime.shutdown_timeout,
        config_fingerprint: config.fingerprint(),
    };
    Ok(ConnectorRuntime::new(
        identity,
        source,
        encoder,
        sink,
        checkpoint_store,
        runtime,
        status,
    ))
}

fn build_source(config: &Config) -> Result<Box<dyn SourceConnector>> {
    match &config.source {
        SourceConfig::Postgresql(source) => Ok(Box::new(PostgresSource::new(
            &config.metadata.name,
            source.clone(),
            config.snapshot.clone(),
        ))),
        SourceConfig::Mysql(source) => Ok(Box::new(MySqlSource::new(
            &config.metadata.name,
            source.clone(),
            config.snapshot.clone(),
        ))),
        SourceConfig::Sqlserver(source) => Ok(Box::new(SqlServerSource::new(
            &config.metadata.name,
            source.clone(),
            config.snapshot.clone(),
        ))),
    }
}

fn build_encoder(config: &Config) -> Arc<dyn EventEncoder> {
    let (heartbeat_topics_prefix, heartbeat_topic_name) = config.source.as_mysql().map_or_else(
        || ("__debezium-heartbeat".into(), None),
        |source| {
            (
                source.heartbeat_topics_prefix.clone(),
                source.heartbeat_topic_name.clone(),
            )
        },
    );
    let encoder_config = JsonEncoderConfig {
        topic_prefix: config.sink.topic_prefix().into(),
        unavailable_value: config.format.unavailable_value.clone(),
        tombstones_on_delete: config.format.tombstones_on_delete,
        heartbeat_topics_prefix,
        heartbeat_topic_name,
    };
    match config.format.kind {
        FormatType::RustiumJson => Arc::new(RustiumJsonEncoder::new(encoder_config)),
        FormatType::DebeziumJson => Arc::new(DebeziumJsonEncoder::new(encoder_config)),
    }
}

fn build_sink(config: &Config) -> Result<Box<dyn Sink>> {
    match &config.sink {
        SinkConfig::Stdout { .. } => Ok(Box::new(StdoutSink::default())),
        SinkConfig::Kafka {
            bootstrap_servers,
            acks,
            compression,
            delivery_timeout,
            properties,
            ..
        } => Ok(Box::new(KafkaSink::new(
            bootstrap_servers,
            acks,
            compression,
            *delivery_timeout,
            properties,
        )?)),
    }
}

fn initialize_tracing(config: &Config) {
    let filter = EnvFilter::try_new(&config.observability.log_level)
        .unwrap_or_else(|_| EnvFilter::new("info"));
    match config.observability.log_format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .with_current_span(true)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .compact()
                .init();
        }
    }
}

fn log_compatibility_warnings(config: &Config) {
    for warning in &config.compatibility_warnings {
        tracing::warn!(%warning, "Debezium compatibility warning");
    }
}

fn install_signal_handler(cancellation: CancellationToken) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
        cancellation.cancel();
    });
}
