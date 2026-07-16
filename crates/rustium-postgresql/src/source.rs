use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_walstream::{
    ChangeEvent as WalEvent, ColumnValue, EventType, LogicalReplicationStream,
    PgReplicationConnection, RelationColumn, ReplicationSlotOptions, ReplicationStreamConfig,
    RetryConfig, RowData, StreamingMode, parse_lsn,
    sql_builder::{quote_ident, quote_literal},
};
use regex::Regex;
use rustium_config::{
    PostgresReplicaIdentity, PostgresSourceConfig, PublicationAutoCreateMode, SlotOwnership,
    SnapshotConfig, SnapshotMode,
};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, Operation, PostgresPosition,
    RecordBoundary, Result, RetryPolicy, Row, SignalAcknowledgement, SourceConnector,
    SourceContext, SourceMetadata, SourcePosition, SourceRecord, TransactionMetadata,
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
    file_signal,
    incremental_snapshot::{ClosedWindow, IncrementalSnapshotController, Signal},
    schema_history::{
        PostgresColumnType, TableSchema, decode_connector_state, encode_connector_state,
        encode_schema_history, schema_from_relation,
    },
};

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
struct ActiveTransaction {
    id: u32,
    source_time: DateTime<Utc>,
    total_order: u64,
    collection_order: HashMap<(String, String), u64>,
}

#[derive(Debug)]
struct SnapshotOutcome {
    anchor_lsn: u64,
    schemas: HashMap<(String, String), TableSchema>,
}

pub struct PostgresSource {
    connector_name: String,
    config: PostgresSourceConfig,
    snapshot: SnapshotConfig,
    schemas: HashMap<(String, String), TableSchema>,
    retry_policy: RetryPolicy,
}

impl PostgresSource {
    #[must_use]
    pub fn new(
        connector_name: impl Into<String>,
        config: PostgresSourceConfig,
        snapshot: SnapshotConfig,
    ) -> Self {
        Self {
            connector_name: connector_name.into(),
            config,
            snapshot,
            schemas: HashMap::new(),
            retry_policy: RetryPolicy::default(),
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    async fn validate_source(&mut self) -> Result<()> {
        let connection_url = self.config.connection_url(false)?;
        let config = self.config.clone();
        let schemas = tokio::task::spawn_blocking(move || {
            let mut connection = connect(&connection_url)?;
            let version = scalar(&mut connection, "SHOW server_version_num")?
                .parse::<u32>()
                .map_err(|error| Error::Source(format!("invalid PostgreSQL version: {error}")))?;
            if version < 140_000 {
                return Err(Error::Configuration(format!(
                    "PostgreSQL 14 or newer is required; server_version_num={version}"
                )));
            }
            let wal_level = scalar(&mut connection, "SHOW wal_level")?;
            if wal_level != "logical" {
                return Err(Error::Configuration(format!(
                    "PostgreSQL wal_level must be logical; found {wal_level:?}"
                )));
            }
            ensure_publication(&mut connection, &config)?;
            apply_replica_identity(&mut connection, &config)?;
            validate_signal_table(&mut connection, &config)?;

            let slot = quote_literal(&config.slot_name).map_err(pg_error)?;
            let slot_result = connection
                .exec(&format!(
                    "SELECT plugin, active::text FROM pg_replication_slots WHERE slot_name = {slot}"
                ))
                .map_err(pg_error)?;
            if slot_result.ntuples() > 0 {
                let plugin = required_value(&slot_result, 0, 0, "slot plugin")?;
                if plugin != "pgoutput" {
                    return Err(Error::Configuration(format!(
                        "replication slot {:?} uses plugin {plugin:?}, expected pgoutput",
                        config.slot_name
                    )));
                }
            } else if config.slot_ownership == SlotOwnership::External {
                return Err(Error::Configuration(format!(
                    "externally managed replication slot {:?} does not exist",
                    config.slot_name
                )));
            }
            discover_tables(&mut connection, &config)
        })
        .await
        .map_err(|error| Error::Source(format!("PostgreSQL validation task failed: {error}")))??;

        if schemas.is_empty()
            && self.config.publication_autocreate_mode != PublicationAutoCreateMode::NoTables
        {
            return Err(Error::Configuration(
                "the publication and table filters select no tables".into(),
            ));
        }
        self.schemas = schemas;
        Ok(())
    }

    async fn prepare_snapshot_slot(&self) -> Result<()> {
        if self.config.slot_ownership == SlotOwnership::External {
            return Err(Error::Configuration(
                "an initial snapshot requires slot_ownership=managed".into(),
            ));
        }
        let connection_url = self.config.connection_url(false)?;
        let slot_name = self.config.slot_name.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connect(&connection_url)?;
            let slot = quote_literal(&slot_name).map_err(pg_error)?;
            let result = connection
                .exec(&format!(
                    "SELECT active::text FROM pg_replication_slots WHERE slot_name = {slot}"
                ))
                .map_err(pg_error)?;
            if result.ntuples() == 0 {
                return Ok(());
            }
            let active = required_value(&result, 0, 0, "slot active state")?;
            if active == "t" {
                return Err(Error::Source(format!(
                    "managed replication slot {slot_name:?} is active and cannot be recreated"
                )));
            }
            connection
                .exec(&format!("SELECT pg_drop_replication_slot({slot})"))
                .map_err(pg_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::Source(format!("slot preparation task failed: {error}")))?
    }

    async fn relation_table_schema(
        &self,
        schema: String,
        table: String,
        columns: Vec<RelationColumn>,
        previous: Option<TableSchema>,
    ) -> Result<TableSchema> {
        let connection_url = self.config.connection_url(false)?;
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connect(&connection_url)?;
            let catalog = match query_table_schema(&mut connection, &config, &schema, &table) {
                Ok(catalog) => catalog,
                Err(error) => {
                    warn!(
                        schema = %schema,
                        table = %table,
                        %error,
                        "PostgreSQL relation catalog metadata is unavailable; using WAL relation metadata"
                    );
                    None
                }
            };
            let resolved_type_names = columns
                .iter()
                .map(|column| {
                    scalar(
                        &mut connection,
                        &format!(
                            "SELECT format_type({}::oid, {})",
                            column.type_id, column.type_modifier
                        ),
                    )
                    .unwrap_or_else(|error| {
                        let fallback = relation_type_name_fallback(previous.as_ref(), column);
                        warn!(
                            schema = %schema,
                            table = %table,
                            column = %column.name,
                            %error,
                            fallback = %fallback,
                            "PostgreSQL relation type metadata is unavailable; retaining a conservative type name"
                        );
                        fallback
                    })
                })
                .collect::<Vec<_>>();
            schema_from_relation(
                &schema,
                &table,
                format!(
                    "{}.{}.{}.{}.Envelope",
                    config.slot_name, config.database, schema, table
                ),
                &columns,
                &resolved_type_names,
                previous.as_ref(),
                catalog.as_ref(),
            )
        })
        .await
        .map_err(|error| Error::Source(format!("relation schema task failed: {error}")))?
    }

    async fn discover_runtime_table_schema(
        &self,
        schema: String,
        table: String,
    ) -> Result<TableSchema> {
        let connection_url = self.config.connection_url(false)?;
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connect(&connection_url)?;
            discover_table_schema(&mut connection, &config, &schema, &table)
        })
        .await
        .map_err(|error| Error::Source(format!("runtime schema task failed: {error}")))?
    }

    async fn run_snapshot(
        &self,
        snapshot_name: String,
        output: mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<SnapshotOutcome> {
        let connection_url = self.config.connection_url(false)?;
        let config = self.config.clone();
        let connector_name = self.connector_name.clone();
        let snapshot_config = self.snapshot.clone();
        let fetch_size = snapshot_config.fetch_size;
        tokio::task::spawn_blocking(move || {
            let mut connection = connect(&connection_url)?;
            connection
                .exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .map_err(pg_error)?;
            let snapshot = quote_literal(&snapshot_name).map_err(pg_error)?;
            connection
                .exec(&format!("SET TRANSACTION SNAPSHOT {snapshot}"))
                .map_err(pg_error)?;

            let schemas = discover_tables(&mut connection, &config)?;
            let slot = quote_literal(&config.slot_name).map_err(pg_error)?;
            let anchor_text = scalar(
                &mut connection,
                &format!(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = {slot}"
                ),
            )?;
            let anchor_lsn = parse_lsn(&anchor_text).map_err(pg_error)?;

            let mut ordinal = 0_u64;
            let mut ordered_schemas = schemas.values().cloned().collect::<Vec<_>>();
            ordered_schemas.sort_by_key(TableSchema::key);
            for schema in &ordered_schemas {
                if !snapshot_includes(&snapshot_config, &schema.schema, &schema.table) {
                    continue;
                }
                snapshot_table(
                    &mut connection,
                    &config,
                    &connector_name,
                    schema,
                    anchor_lsn,
                    fetch_size,
                    &mut ordinal,
                    &output,
                )?;
            }

            connection.exec("COMMIT").map_err(pg_error)?;
            ordinal += 1;
            output
                .blocking_send(Ok(SourceRecord {
                    event: None,
                    position: SourcePosition::Postgres(PostgresPosition {
                        lsn: anchor_lsn,
                        commit_lsn: Some(anchor_lsn),
                        transaction_id: None,
                        event_serial: ordinal,
                        snapshot: true,
                    }),
                    boundary: RecordBoundary::SnapshotComplete,
                    connector_state: Some(encode_schema_history(&schemas)?),
                    signal_acknowledgements: Vec::new(),
                }))
                .map_err(|_| Error::Cancelled)?;
            Ok(SnapshotOutcome {
                anchor_lsn,
                schemas,
            })
        })
        .await
        .map_err(|error| Error::Source(format!("snapshot task failed: {error}")))?
    }
}

fn relation_type_name_fallback(previous: Option<&TableSchema>, column: &RelationColumn) -> String {
    previous
        .and_then(|schema| {
            schema.column_types.iter().find_map(|candidate| {
                (candidate.name == column.name.as_ref()
                    && candidate.type_oid == column.type_id
                    && candidate.type_modifier == column.type_modifier)
                    .then(|| {
                        schema
                            .event_schema
                            .fields
                            .iter()
                            .find(|field| field.name == candidate.name)
                    })
                    .flatten()
            })
        })
        .map(|field| field.type_name.clone())
        .unwrap_or_else(|| format!("unknown_oid_{}", column.type_id))
}

#[async_trait]
impl SourceConnector for PostgresSource {
    fn source_type(&self) -> &'static str {
        "postgresql"
    }

    async fn validate(&mut self) -> Result<()> {
        self.validate_source().await
    }

    async fn run(&mut self, mut context: SourceContext) -> Result<()> {
        let checkpoint = context.initial_checkpoint.clone();
        let mut snapshot_needed = match self.snapshot.mode {
            SnapshotMode::Never => false,
            SnapshotMode::Initial | SnapshotMode::WhenNeeded => checkpoint
                .as_ref()
                .is_none_or(|checkpoint| !checkpoint.snapshot_completed),
        };

        let mut incremental_progress = None;
        let mut completed_signal_ids = Vec::new();
        if !snapshot_needed {
            if let Some(connector_state) = checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.connector_state.as_ref())
            {
                let state = decode_connector_state(connector_state)?;
                self.schemas = state.schemas;
                incremental_progress = state.incremental_snapshot;
                completed_signal_ids = state.completed_signal_ids;
            } else if checkpoint.is_some() {
                if self.snapshot.mode == SnapshotMode::WhenNeeded {
                    warn!(
                        connector = %self.connector_name,
                        "PostgreSQL checkpoint has no schema history; taking a recovery snapshot"
                    );
                    snapshot_needed = true;
                    self.schemas.clear();
                    incremental_progress = None;
                    completed_signal_ids.clear();
                } else {
                    return Err(Error::State(
                        "PostgreSQL checkpoint predates persistent schema history and cannot safely replay relation changes; reset the checkpoint and run a new initial snapshot"
                            .into(),
                    ));
                }
            }
        }

        if snapshot_needed {
            self.prepare_snapshot_slot().await?;
        } else if checkpoint.is_some() {
            if let Err(error) = validate_resume_slot(&self.config).await {
                if self.snapshot.mode == SnapshotMode::WhenNeeded
                    && matches!(&error, Error::State(_))
                {
                    warn!(
                        connector = %self.connector_name,
                        %error,
                        "PostgreSQL checkpoint history is unavailable; taking a recovery snapshot"
                    );
                    snapshot_needed = true;
                    self.schemas.clear();
                    incremental_progress = None;
                    completed_signal_ids.clear();
                    self.prepare_snapshot_slot().await?;
                } else {
                    return Err(error);
                }
            }
        }

        let replication_url = self.config.connection_url(true)?;
        let slot_options = ReplicationSlotOptions {
            snapshot: Some(if snapshot_needed { "export" } else { "nothing" }.into()),
            ..ReplicationSlotOptions::default()
        };
        let retry_config = postgres_retry_config(self.retry_policy, self.config.connect_timeout);
        let stream_config = ReplicationStreamConfig::new(
            self.config.slot_name.clone(),
            self.config.publication.clone(),
            2,
            StreamingMode::On,
            Duration::from_secs(10),
            retry_config.max_duration,
            Duration::from_secs(30),
            retry_config,
        )
        .with_messages(false)
        .with_slot_options(slot_options);

        let mut stream = LogicalReplicationStream::new(&replication_url, stream_config)
            .await
            .map_err(pg_error)?;
        let mut start_lsn = match checkpoint.as_ref() {
            Some(checkpoint) => match &checkpoint.source_position {
                SourcePosition::Postgres(position) => Some(position.lsn),
                SourcePosition::MySql(_) | SourcePosition::SqlServer(_) => {
                    return Err(Error::State(
                        "PostgreSQL connector cannot resume from a MySQL checkpoint".into(),
                    ));
                }
            },
            None => None,
        };
        let mut resume_position = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.source_position.clone());

        if snapshot_needed {
            stream.ensure_replication_slot().await.map_err(pg_error)?;
            let snapshot_name = stream
                .exported_snapshot_name()
                .ok_or_else(|| {
                    Error::Source(
                        "PostgreSQL did not export a snapshot for the managed replication slot"
                            .into(),
                    )
                })?
                .to_string();
            let outcome = self
                .run_snapshot(snapshot_name, context.output.clone())
                .await?;
            self.schemas = outcome.schemas;
            start_lsn = Some(outcome.anchor_lsn);
            resume_position = None;
        }

        stream.start(start_lsn).await.map_err(pg_error)?;
        if start_lsn.is_none() {
            start_lsn = Some(query_slot_safe_lsn(&self.config).await?);
        }
        let feedback = Arc::clone(&stream.shared_lsn_feedback);
        if let Some(SourcePosition::Postgres(position)) = context.acknowledged.borrow().clone() {
            feedback.update_applied_lsn(position.commit_lsn.unwrap_or(position.lsn));
        }

        let mut last_safe_position = resume_position
            .as_ref()
            .map(|position| match position {
                SourcePosition::Postgres(position) if position.snapshot => {
                    postgres_streaming_position(position.lsn)
                }
                _ => position.clone(),
            })
            .or_else(|| start_lsn.map(postgres_streaming_position));
        let mut state = StreamingState::new(
            self.schemas.clone(),
            resume_position,
            !snapshot_needed && checkpoint.is_none(),
        );
        let mut heartbeat = heartbeat_timer(self.config.heartbeat_interval);
        let mut file_signal_poll = file_signal_timer(&self.config);
        let mut heartbeat_connection = open_heartbeat_connection(&self.config).await?;
        let mut incremental =
            IncrementalSnapshotController::new(incremental_progress, completed_signal_ids);
        incremental.resume(&self.config, &state.schemas).await?;
        info!(
            connector = %self.connector_name,
            slot = %self.config.slot_name,
            max_retries = self.retry_policy.max_retries,
            initial_retry_delay_ms = self.retry_policy.initial_delay.as_millis(),
            max_retry_delay_ms = self.retry_policy.max_delay.as_millis(),
            "PostgreSQL streaming started"
        );

        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    stream.stop().await.map_err(pg_error)?;
                    return Ok(());
                }
                changed = context.acknowledged.changed() => {
                    if changed.is_err() {
                        return Err(Error::Cancelled);
                    }
                    if let Some(SourcePosition::Postgres(position)) = context.acknowledged.borrow().clone() {
                        feedback.update_applied_lsn(position.commit_lsn.unwrap_or(position.lsn));
                    }
                }
                () = next_heartbeat(&mut heartbeat) => {
                    if let Some(query) = self.config.heartbeat_action_query.clone() {
                        let connection = heartbeat_connection.take().ok_or_else(|| {
                            Error::Source(
                                "PostgreSQL heartbeat action connection is unavailable".into(),
                            )
                        })?;
                        heartbeat_connection = Some(
                            execute_heartbeat_action(connection, query).await?,
                        );
                    }
                    if let Some(position) = last_safe_position.clone() {
                        context
                            .output
                            .send(Ok(heartbeat_record(
                                &self.connector_name,
                                &self.config.database,
                                position,
                            )))
                            .await
                            .map_err(|_| Error::Cancelled)?;
                    }
                }
                () = next_file_signal_poll(&mut file_signal_poll) => {
                    if state.transaction.is_some() {
                        debug!("PostgreSQL file signal polling deferred until the active transaction commits");
                        continue;
                    }
                    for line in file_signal::read_and_clear(&self.config.signal_file).await? {
                        let signal = match IncrementalSnapshotController::parse_external_signal(&line) {
                            Ok(signal) => signal,
                            Err(error) => {
                                warn!(%error, "invalid PostgreSQL file signal ignored");
                                continue;
                            }
                        };
                        if let Signal::Unsupported { id, signal_type } = &signal {
                            warn!(%id, %signal_type, "unsupported PostgreSQL file signal ignored");
                        }
                        apply_external_signal(
                            &mut incremental,
                            signal,
                            &self.config,
                            &state.schemas,
                        ).await?;
                    }
                    checkpoint_external_signal_state(
                        &mut state,
                        &mut incremental,
                        &context.output,
                        last_safe_position.as_ref(),
                        None,
                    ).await?;
                }
                delivery = context.signals.recv(),
                    if (signal_channel_enabled(&self.config, "in-process")
                        || signal_channel_enabled(&self.config, "kafka"))
                        && state.transaction.is_none() =>
                {
                    let delivery = delivery.ok_or_else(|| {
                        Error::Source("PostgreSQL runtime signal channel closed".into())
                    })?;
                    let signal = match IncrementalSnapshotController::parse_external_record(delivery.record()) {
                        Ok(signal) => signal,
                        Err(error) => {
                            warn!(%error, "invalid PostgreSQL runtime signal ignored");
                            delivery.acknowledge();
                            continue;
                        }
                    };
                    if let Signal::Unsupported { id, signal_type } = &signal {
                        warn!(%id, %signal_type, "unsupported PostgreSQL runtime signal ignored");
                    }
                    apply_external_signal(
                        &mut incremental,
                        signal,
                        &self.config,
                        &state.schemas,
                    ).await?;
                    checkpoint_external_signal_state(
                        &mut state,
                        &mut incremental,
                        &context.output,
                        last_safe_position.as_ref(),
                        delivery.into_acknowledgement(),
                    ).await?;
                }
                event = next_postgres_event(
                    &mut stream,
                    &context.cancellation,
                    self.retry_policy.max_retries != 0,
                ) => {
                    let event = match event {
                        Ok(event) => event,
                        Err(pg_walstream::ReplicationError::Cancelled(_))
                            if context.cancellation.is_cancelled() =>
                        {
                            stream.stop().await.map_err(pg_error)?;
                            return Ok(());
                        }
                        Err(error) => return Err(pg_error(error)),
                    };
                    let relation = match &event.event_type {
                        EventType::Relation {
                            namespace,
                            relation_name,
                            columns,
                            ..
                        } if self.config.tables.includes(namespace, relation_name)
                            && !is_signal_table(&self.config, namespace, relation_name) => {
                            Some((
                                namespace.to_string(),
                                relation_name.to_string(),
                                columns.clone(),
                            ))
                        }
                        _ => None,
                    };
                    if let Some((schema_name, table_name, columns)) = relation {
                        let previous = state
                            .schemas
                            .get(&(schema_name.clone(), table_name.clone()))
                            .cloned();
                        let refreshed = self
                            .relation_table_schema(
                                schema_name.clone(),
                                table_name.clone(),
                                columns,
                                previous,
                            )
                            .await?;
                        if let Some(version) = state.refresh_schema(refreshed) {
                            incremental.reject_active_schema_change(
                                &schema_name,
                                &table_name,
                            )?;
                            info!(
                                schema = %schema_name,
                                table = %table_name,
                                version,
                                "PostgreSQL table schema refreshed"
                            );
                        }
                    }
                    let uncached_table = match &event.event_type {
                        EventType::Insert { schema, table, .. }
                        | EventType::Update { schema, table, .. }
                        | EventType::Delete { schema, table, .. }
                            if self.config.tables.includes(schema, table)
                                && !is_signal_table(&self.config, schema, table)
                                && !state
                                    .schemas
                                    .contains_key(&(schema.to_string(), table.to_string())) =>
                        {
                            Some((schema.to_string(), table.to_string()))
                        }
                        _ => None,
                    };
                    if let Some((schema_name, table_name)) = uncached_table {
                        let discovered = self
                            .discover_runtime_table_schema(
                                schema_name.clone(),
                                table_name.clone(),
                            )
                            .await?;
                        let version = state
                            .refresh_schema(discovered)
                            .expect("an uncached PostgreSQL table changes schema state");
                        info!(
                            schema = %schema_name,
                            table = %table_name,
                            version,
                            "PostgreSQL table schema discovered from the streaming catalog"
                        );
                    }
                    let event_lsn = event.lsn.value();
                    let transaction_id = match &event.event_type {
                        EventType::Begin { transaction_id, .. }
                        | EventType::StreamStart { transaction_id, .. }
                        | EventType::StreamCommit { transaction_id, .. } => {
                            Some(u64::from(*transaction_id))
                        }
                        _ => state
                            .transaction
                            .as_ref()
                            .map(|transaction| u64::from(transaction.id)),
                    };
                    let transaction_commit = matches!(
                        &event.event_type,
                        EventType::Commit { .. } | EventType::StreamCommit { .. }
                    );
                    if self.config.read_only {
                        if transaction_commit {
                            if let Some(transaction_id) = transaction_id {
                                loop {
                                    if let Some(closed) =
                                        incremental.observe_read_only_commit(transaction_id)?
                                    {
                                        send_incremental_window(
                                            &mut state,
                                            &context.output,
                                            event_lsn,
                                            &self.connector_name,
                                            &self.config,
                                            closed,
                                        )
                                        .await?;
                                    }
                                    if !incremental
                                        .prepare_read_only_continuation(
                                            &self.config,
                                            &state.schemas,
                                        )
                                        .await?
                                    {
                                        break;
                                    }
                                }
                            }
                        } else if let Some(transaction_id) = transaction_id
                            && let Some(closed) =
                                incremental.observe_read_only_event(transaction_id)?
                        {
                            send_incremental_window(
                                &mut state,
                                &context.output,
                                event_lsn,
                                &self.connector_name,
                                &self.config,
                                closed,
                            )
                            .await?;
                        }
                    }
                    if let EventType::Insert { schema, table, data, .. } = &event.event_type
                        && is_signal_table(&self.config, schema, table)
                    {
                        let signal = IncrementalSnapshotController::parse_signal(data)?;
                        let enabled = signal_channel_enabled(&self.config, "source")
                            || matches!(
                                &signal,
                                Signal::WindowOpen { .. } | Signal::WindowClose { .. }
                            );
                        if !enabled {
                            debug!("PostgreSQL source-table signal ignored because the source channel is disabled");
                            continue;
                        }
                        if let Signal::Unsupported { id, signal_type } = &signal {
                            warn!(%id, %signal_type, "unsupported PostgreSQL signal ignored");
                        }
                        if let Some(closed) = incremental
                            .handle_signal(signal, &self.config, &state.schemas)
                            .await?
                        {
                            send_incremental_window(
                                &mut state,
                                &context.output,
                                event_lsn,
                                &self.connector_name,
                                &self.config,
                                closed,
                            )
                            .await?;
                        }
                        continue;
                    }
                    if let Some(mut record) = state.convert(
                        event,
                        &self.connector_name,
                        &self.config,
                    )? {
                        if let Some(event) = &record.event {
                            incremental.deduplicate(event);
                        }
                        let incremental_dirty = incremental.take_state_dirty();
                        if state.schema_dirty || incremental_dirty {
                            record.connector_state = Some(encode_connector_state(
                                &state.schemas,
                                incremental.progress(),
                                incremental.completed_signal_ids(),
                            )?);
                            state.schema_dirty = false;
                        }
                        let committed = record.boundary == RecordBoundary::TransactionCommit;
                        let position = record.position.clone();
                        context.output.send(Ok(record)).await.map_err(|_| Error::Cancelled)?;
                        last_safe_position = Some(position);
                        if committed {
                            incremental.after_commit(&self.config, &state.schemas).await?;
                        }
                    }
                }
            }
        }
    }
}

const EFFECTIVELY_UNBOUNDED_RETRY_DURATION: Duration = Duration::from_secs(4_294_967_295);

fn postgres_retry_config(policy: RetryPolicy, connect_timeout: Duration) -> RetryConfig {
    let max_attempts = if policy.max_retries < 0 {
        u32::MAX
    } else {
        u32::try_from(policy.max_retries)
            .unwrap_or(u32::MAX)
            .saturating_add(1)
    };
    let max_duration = if policy.max_retries < 0 {
        EFFECTIVELY_UNBOUNDED_RETRY_DURATION
    } else {
        connect_timeout
            .saturating_mul(max_attempts)
            .saturating_add(retry_delay_budget(policy))
    };
    RetryConfig {
        max_attempts,
        initial_delay: policy.initial_delay,
        max_delay: policy.max_delay,
        multiplier: 2.0,
        max_duration,
        jitter: false,
    }
}

fn retry_delay_budget(policy: RetryPolicy) -> Duration {
    let retries = u32::try_from(policy.max_retries).unwrap_or_default();
    let mut remaining = retries;
    let mut delay = policy.initial_delay;
    let mut total = Duration::ZERO;
    while remaining > 0 && delay < policy.max_delay {
        total = total.saturating_add(delay);
        delay = delay.saturating_mul(2).min(policy.max_delay);
        remaining -= 1;
    }
    total.saturating_add(policy.max_delay.saturating_mul(remaining))
}

async fn next_postgres_event(
    stream: &mut LogicalReplicationStream,
    cancellation: &tokio_util::sync::CancellationToken,
    retry_enabled: bool,
) -> pg_walstream::Result<WalEvent> {
    if retry_enabled {
        stream.next_event_with_retry(cancellation).await
    } else {
        stream.next_event(cancellation).await
    }
}

async fn send_incremental_window(
    state: &mut StreamingState,
    output: &mpsc::Sender<Result<SourceRecord>>,
    lsn: u64,
    connector_name: &str,
    config: &PostgresSourceConfig,
    closed: ClosedWindow,
) -> Result<()> {
    for record in state.incremental_snapshot_records(lsn, connector_name, config, closed) {
        output
            .send(Ok(record))
            .await
            .map_err(|_| Error::Cancelled)?;
    }
    Ok(())
}

async fn checkpoint_external_signal_state(
    state: &mut StreamingState,
    incremental: &mut IncrementalSnapshotController,
    output: &mpsc::Sender<Result<SourceRecord>>,
    position: Option<&SourcePosition>,
    acknowledgement: Option<SignalAcknowledgement>,
) -> Result<()> {
    let incremental_dirty = incremental.take_state_dirty();
    if !state.schema_dirty && !incremental_dirty {
        if let Some(acknowledgement) = acknowledgement {
            acknowledgement.acknowledge();
        }
        return Ok(());
    }
    let position = position.cloned().ok_or_else(|| {
        Error::Invariant("PostgreSQL external signal has no safe source position".into())
    })?;
    output
        .send(Ok(SourceRecord {
            event: None,
            position,
            boundary: RecordBoundary::TransactionCommit,
            connector_state: Some(encode_connector_state(
                &state.schemas,
                incremental.progress(),
                incremental.completed_signal_ids(),
            )?),
            signal_acknowledgements: acknowledgement.into_iter().collect(),
        }))
        .await
        .map_err(|_| Error::Cancelled)?;
    state.schema_dirty = false;
    Ok(())
}

async fn apply_external_signal(
    incremental: &mut IncrementalSnapshotController,
    signal: Signal,
    config: &PostgresSourceConfig,
    schemas: &HashMap<(String, String), TableSchema>,
) -> Result<()> {
    incremental.handle_signal(signal, config, schemas).await?;
    if config.read_only {
        incremental
            .prepare_read_only_continuation(config, schemas)
            .await?;
    } else {
        incremental.after_commit(config, schemas).await?;
    }
    Ok(())
}

fn heartbeat_timer(interval: Duration) -> Option<tokio::time::Interval> {
    if interval.is_zero() {
        return None;
    }
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    Some(timer)
}

fn file_signal_timer(config: &PostgresSourceConfig) -> Option<tokio::time::Interval> {
    signal_channel_enabled(config, "file").then(|| {
        let mut timer = tokio::time::interval_at(
            tokio::time::Instant::now() + config.signal_poll_interval,
            config.signal_poll_interval,
        );
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer
    })
}

fn signal_channel_enabled(config: &PostgresSourceConfig, name: &str) -> bool {
    config
        .signal_enabled_channels
        .iter()
        .any(|channel| channel == name)
}

async fn next_heartbeat(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn next_file_signal_poll(timer: &mut Option<tokio::time::Interval>) {
    next_heartbeat(timer).await;
}

async fn open_heartbeat_connection(
    config: &PostgresSourceConfig,
) -> Result<Option<PgReplicationConnection>> {
    if config.heartbeat_interval.is_zero() || config.heartbeat_action_query.is_none() {
        return Ok(None);
    }
    let connection_url = config.connection_url(false)?;
    let connection = tokio::task::spawn_blocking(move || connect(&connection_url))
        .await
        .map_err(|error| {
            Error::Source(format!(
                "PostgreSQL heartbeat connection task failed: {error}"
            ))
        })??;
    Ok(Some(connection))
}

async fn query_slot_safe_lsn(config: &PostgresSourceConfig) -> Result<u64> {
    let connection_url = config.connection_url(false)?;
    let slot_name = config.slot_name.clone();
    tokio::task::spawn_blocking(move || {
        let mut connection = connect(&connection_url)?;
        let slot = quote_literal(&slot_name).map_err(pg_error)?;
        let lsn = scalar(
            &mut connection,
            &format!(
                "SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text \
                 FROM pg_replication_slots WHERE slot_name = {slot}"
            ),
        )?;
        parse_lsn(&lsn).map_err(pg_error)
    })
    .await
    .map_err(|error| Error::Source(format!("slot LSN query task failed: {error}")))?
}

async fn validate_resume_slot(config: &PostgresSourceConfig) -> Result<()> {
    let connection_url = config.connection_url(false)?;
    let slot_name = config.slot_name.clone();
    tokio::task::spawn_blocking(move || {
        let mut connection = connect(&connection_url)?;
        let slot = quote_literal(&slot_name).map_err(pg_error)?;
        let result = connection
            .exec(&format!(
                "SELECT plugin, COALESCE(wal_status, '') \
                 FROM pg_replication_slots WHERE slot_name = {slot}"
            ))
            .map_err(pg_error)?;
        if result.ntuples() == 0 {
            return Err(Error::State(format!(
                "PostgreSQL replication slot {slot_name:?} is missing; the checkpoint cannot be resumed without a possible WAL gap; reset the checkpoint and run a new initial snapshot"
            )));
        }
        let plugin = required_value(&result, 0, 0, "slot plugin")?;
        if plugin != "pgoutput" {
            return Err(Error::State(format!(
                "PostgreSQL replication slot {slot_name:?} uses plugin {plugin:?}; the checkpoint requires pgoutput"
            )));
        }
        let wal_status = required_value(&result, 0, 1, "slot WAL status")?;
        if matches!(wal_status.as_str(), "unreserved" | "lost") {
            return Err(Error::State(format!(
                "PostgreSQL replication slot {slot_name:?} has wal_status={wal_status:?}; required WAL may be unavailable, so reset the checkpoint and run a new initial snapshot"
            )));
        }
        Ok(())
    })
    .await
    .map_err(|error| Error::Source(format!("slot continuity task failed: {error}")))?
}

async fn execute_heartbeat_action(
    mut connection: PgReplicationConnection,
    query: String,
) -> Result<PgReplicationConnection> {
    tokio::task::spawn_blocking(move || {
        connection.exec(&query).map_err(|error| {
            Error::Source(format!("PostgreSQL heartbeat.action.query failed: {error}"))
        })?;
        Ok(connection)
    })
    .await
    .map_err(|error| Error::Source(format!("PostgreSQL heartbeat action task failed: {error}")))?
}

fn postgres_streaming_position(lsn: u64) -> SourcePosition {
    SourcePosition::Postgres(PostgresPosition {
        lsn,
        commit_lsn: Some(lsn),
        transaction_id: None,
        event_serial: 0,
        snapshot: false,
    })
}

fn heartbeat_record(
    connector_name: &str,
    database: &str,
    position: SourcePosition,
) -> SourceRecord {
    let observed_time = Utc::now();
    let timestamp = observed_time.timestamp_millis();
    let mut attributes = BTreeMap::new();
    attributes.insert("rustium.heartbeat".into(), true.into());
    let mut after = Row::new();
    after.insert("ts_ms".into(), DataValue::Int64(timestamp));
    let event = ChangeEvent {
        id: EventId::deterministic(
            connector_name,
            database,
            &position,
            "__heartbeat",
            u64::try_from(observed_time.timestamp_micros()).unwrap_or_default(),
        ),
        source: SourceMetadata {
            connector: "postgresql".into(),
            connector_name: connector_name.into(),
            database: database.into(),
            schema: None,
            table: None,
            snapshot: false,
            version: CONNECTOR_VERSION.into(),
            attributes,
        },
        position: position.clone(),
        transaction: None,
        operation: Operation::Message,
        before: None,
        after: Some(after),
        schema: EventSchema {
            name: format!("{connector_name}.Heartbeat"),
            version: 1,
            fields: vec![FieldSchema {
                name: "ts_ms".into(),
                type_name: "int64".into(),
                optional: false,
                primary_key: false,
            }],
        },
        source_time: None,
        observed_time,
    };
    SourceRecord {
        event: Some(event),
        position,
        boundary: RecordBoundary::Heartbeat,
        connector_state: None,
        signal_acknowledgements: Vec::new(),
    }
}

struct StreamingState {
    schemas: HashMap<(String, String), TableSchema>,
    schema_dirty: bool,
    transaction: Option<ActiveTransaction>,
    previous_lsn: Option<u64>,
    event_serial: u64,
    resume_position: Option<SourcePosition>,
}

impl StreamingState {
    fn new(
        schemas: HashMap<(String, String), TableSchema>,
        resume_position: Option<SourcePosition>,
        schema_dirty: bool,
    ) -> Self {
        Self {
            schemas,
            schema_dirty,
            transaction: None,
            previous_lsn: None,
            event_serial: 0,
            resume_position,
        }
    }

    fn convert(
        &mut self,
        wal: WalEvent,
        connector_name: &str,
        config: &PostgresSourceConfig,
    ) -> Result<Option<SourceRecord>> {
        let lsn = wal.lsn.value();
        match wal.event_type {
            EventType::Begin {
                transaction_id,
                commit_timestamp,
                ..
            } => {
                self.transaction = Some(ActiveTransaction {
                    id: transaction_id,
                    source_time: commit_timestamp,
                    total_order: 0,
                    collection_order: HashMap::new(),
                });
                Ok(None)
            }
            EventType::Insert {
                schema,
                table,
                relation_oid,
                data,
            } => self.data_record(
                lsn,
                connector_name,
                config,
                &schema,
                &table,
                relation_oid,
                Operation::Create,
                None,
                Some(data),
                None,
            ),
            EventType::Update {
                schema,
                table,
                relation_oid,
                old_data,
                new_data,
                key_columns,
                ..
            } => self.data_record(
                lsn,
                connector_name,
                config,
                &schema,
                &table,
                relation_oid,
                Operation::Update,
                old_data,
                Some(new_data),
                Some(key_columns.iter().map(ToString::to_string).collect()),
            ),
            EventType::Delete {
                schema,
                table,
                relation_oid,
                old_data,
                key_columns,
                ..
            } => self.data_record(
                lsn,
                connector_name,
                config,
                &schema,
                &table,
                relation_oid,
                Operation::Delete,
                Some(old_data),
                None,
                Some(key_columns.iter().map(ToString::to_string).collect()),
            ),
            EventType::Commit {
                commit_lsn,
                end_lsn,
                ..
            } => self.commit_record(commit_lsn.value(), end_lsn.value()),
            EventType::StreamStart { transaction_id, .. } => {
                self.transaction.get_or_insert(ActiveTransaction {
                    id: transaction_id,
                    source_time: Utc::now(),
                    total_order: 0,
                    collection_order: HashMap::new(),
                });
                Ok(None)
            }
            EventType::StreamCommit {
                transaction_id,
                commit_lsn,
                end_lsn,
                commit_timestamp,
            } => {
                self.transaction.get_or_insert(ActiveTransaction {
                    id: transaction_id,
                    source_time: commit_timestamp,
                    total_order: 0,
                    collection_order: HashMap::new(),
                });
                self.commit_record(commit_lsn.value(), end_lsn.value())
            }
            EventType::StreamAbort { .. } | EventType::RollbackPrepared { .. } => {
                self.transaction = None;
                Ok(None)
            }
            EventType::Truncate(tables) => {
                let Some(table) = tables.first() else {
                    return Ok(None);
                };
                let matching = self
                    .schemas
                    .values()
                    .find(|schema| schema.table == table.as_ref())
                    .cloned();
                let Some(schema) = matching else {
                    return Ok(None);
                };
                self.data_record(
                    lsn,
                    connector_name,
                    config,
                    &schema.schema,
                    &schema.table,
                    0,
                    Operation::Truncate,
                    None,
                    None,
                    None,
                )
            }
            EventType::Message { .. }
            | EventType::Relation { .. }
            | EventType::Type { .. }
            | EventType::Origin { .. }
            | EventType::StreamStop
            | EventType::BeginPrepare { .. }
            | EventType::Prepare { .. }
            | EventType::CommitPrepared { .. }
            | EventType::StreamPrepare { .. } => Ok(None),
        }
    }

    fn refresh_schema(&mut self, mut refreshed: TableSchema) -> Option<u32> {
        let key = refreshed.key();
        let changed = match self.schemas.get(&key) {
            Some(current)
                if current.event_schema.fields == refreshed.event_schema.fields
                    && current.column_types == refreshed.column_types =>
            {
                refreshed.event_schema.version = current.event_schema.version;
                false
            }
            Some(current) => {
                refreshed.event_schema.version = current.event_schema.version.saturating_add(1);
                true
            }
            None => true,
        };
        let version = refreshed.event_schema.version;
        self.schemas.insert(key, refreshed);
        self.schema_dirty |= changed;
        changed.then_some(version)
    }

    fn incremental_snapshot_records(
        &mut self,
        lsn: u64,
        connector_name: &str,
        config: &PostgresSourceConfig,
        closed: ClosedWindow,
    ) -> Vec<SourceRecord> {
        closed
            .rows
            .into_iter()
            .map(|after| {
                let event_serial = self.next_serial(lsn);
                let position = SourcePosition::Postgres(PostgresPosition {
                    lsn,
                    commit_lsn: None,
                    transaction_id: None,
                    event_serial,
                    snapshot: true,
                });
                let mut source = source_metadata(
                    connector_name,
                    config,
                    &closed.schema.schema,
                    &closed.schema.table,
                    true,
                    0,
                );
                source
                    .attributes
                    .insert("rustium.snapshot.kind".into(), "incremental".into());
                let event = ChangeEvent {
                    id: EventId::deterministic(
                        connector_name,
                        &config.database,
                        &position,
                        &format!(
                            "{}.{}.{}",
                            config.database, closed.schema.schema, closed.schema.table
                        ),
                        event_serial,
                    ),
                    source,
                    position,
                    transaction: None,
                    operation: Operation::Read,
                    before: None,
                    after: Some(after),
                    schema: closed.schema.event_schema.clone(),
                    source_time: None,
                    observed_time: Utc::now(),
                };
                SourceRecord::data(event)
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn data_record(
        &mut self,
        lsn: u64,
        connector_name: &str,
        config: &PostgresSourceConfig,
        schema_name: &str,
        table_name: &str,
        relation_oid: u32,
        operation: Operation,
        old_data: Option<RowData>,
        new_data: Option<RowData>,
        key_columns: Option<Vec<String>>,
    ) -> Result<Option<SourceRecord>> {
        if !config.tables.includes(schema_name, table_name)
            || is_signal_table(config, schema_name, table_name)
        {
            return Ok(None);
        }
        let key = (schema_name.to_string(), table_name.to_string());
        let event_schema = {
            let schema = self.schemas.get_mut(&key).ok_or_else(|| {
                Error::Source(format!(
                    "received an event for unknown table {schema_name}.{table_name}; restart after schema refresh"
                ))
            })?;
            if let Some(key_columns) = key_columns {
                for field in &mut schema.event_schema.fields {
                    field.primary_key = key_columns.iter().any(|column| column == &field.name);
                }
            }
            schema.event_schema.clone()
        };

        let event_serial = self.next_serial(lsn);
        let transaction_id = self.transaction.as_ref().map(|transaction| transaction.id);
        let position = SourcePosition::Postgres(PostgresPosition {
            lsn,
            commit_lsn: None,
            transaction_id,
            event_serial,
            snapshot: false,
        });
        if self.should_skip(&position) {
            return Ok(None);
        }

        let before = old_data
            .as_ref()
            .map(|row| convert_row(row, &event_schema, &config.hstore_handling_mode));
        let mut after = new_data
            .as_ref()
            .map(|row| convert_row(row, &event_schema, &config.hstore_handling_mode));
        if let Some(after) = &mut after {
            fill_unavailable(after, before.as_ref(), &event_schema);
        }

        let transaction = self.transaction.as_mut().map(|transaction| {
            transaction.total_order += 1;
            let collection_order = transaction.collection_order.entry(key.clone()).or_insert(0);
            *collection_order += 1;
            TransactionMetadata {
                id: transaction.id.to_string(),
                total_order: Some(transaction.total_order),
                collection_order: Some(*collection_order),
            }
        });
        let source_time = self
            .transaction
            .as_ref()
            .map(|transaction| transaction.source_time);
        let source = source_metadata(
            connector_name,
            config,
            schema_name,
            table_name,
            false,
            relation_oid,
        );
        let event = ChangeEvent {
            id: EventId::deterministic(
                connector_name,
                &config.database,
                &position,
                &format!("{}.{}.{}", config.database, schema_name, table_name),
                event_serial,
            ),
            source,
            position,
            transaction,
            operation,
            before,
            after,
            schema: event_schema,
            source_time,
            observed_time: Utc::now(),
        };
        Ok(Some(SourceRecord::data(event)))
    }

    fn commit_record(&mut self, commit_lsn: u64, end_lsn: u64) -> Result<Option<SourceRecord>> {
        let transaction_id = self.transaction.as_ref().map(|transaction| transaction.id);
        let event_serial = self.next_serial(end_lsn);
        let position = SourcePosition::Postgres(PostgresPosition {
            lsn: end_lsn,
            commit_lsn: Some(commit_lsn.max(end_lsn)),
            transaction_id,
            event_serial,
            snapshot: false,
        });
        self.transaction = None;
        if self.should_skip(&position) {
            return Ok(None);
        }
        Ok(Some(SourceRecord {
            event: None,
            position,
            boundary: RecordBoundary::TransactionCommit,
            connector_state: None,
            signal_acknowledgements: Vec::new(),
        }))
    }

    fn next_serial(&mut self, lsn: u64) -> u64 {
        if self.previous_lsn == Some(lsn) {
            self.event_serial += 1;
        } else {
            self.previous_lsn = Some(lsn);
            self.event_serial = 1;
        }
        self.event_serial
    }

    fn should_skip(&mut self, position: &SourcePosition) -> bool {
        let Some(resume) = &self.resume_position else {
            return false;
        };
        if position.is_at_or_before(resume) {
            debug!(?position, ?resume, "skipping replayed PostgreSQL event");
            true
        } else {
            self.resume_position = None;
            false
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn snapshot_table(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
    connector_name: &str,
    schema: &TableSchema,
    anchor_lsn: u64,
    fetch_size: usize,
    ordinal: &mut u64,
    output: &mpsc::Sender<Result<SourceRecord>>,
) -> Result<()> {
    let qualified = format!(
        "{}.{}",
        quote_ident(&schema.schema).map_err(pg_error)?,
        quote_ident(&schema.table).map_err(pg_error)?
    );
    let ordering = schema
        .event_schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .map(|field| quote_ident(&field.name).map_err(pg_error))
        .collect::<Result<Vec<_>>>()?;
    let ordering = if ordering.is_empty() {
        "ctid".to_string()
    } else {
        ordering.join(", ")
    };
    let projection = schema
        .event_schema
        .fields
        .iter()
        .map(|field| {
            quote_ident(&field.name)
                .map(|name| format!("{name}::text"))
                .map_err(pg_error)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let mut offset = 0_usize;
    loop {
        let query = format!(
            "SELECT {projection} FROM {qualified} ORDER BY {ordering} LIMIT {fetch_size} OFFSET {offset}"
        );
        let result = connection.exec(&query).map_err(pg_error)?;
        if result.ntuples() == 0 {
            break;
        }
        let row_count = result.ntuples();
        for row_index in 0..row_count {
            *ordinal += 1;
            let position = SourcePosition::Postgres(PostgresPosition {
                lsn: anchor_lsn,
                commit_lsn: Some(anchor_lsn),
                transaction_id: None,
                event_serial: *ordinal,
                snapshot: true,
            });
            let after = schema
                .event_schema
                .fields
                .iter()
                .enumerate()
                .map(|(column_index, field)| {
                    let column_index = i32::try_from(column_index).map_err(|_| {
                        Error::Invariant("PostgreSQL snapshot has too many columns".into())
                    })?;
                    let value = result.get_value(row_index, column_index).map_or(
                        DataValue::Null,
                        |value| {
                            convert_text_with_hstore_mode(
                                &value,
                                &field.type_name,
                                &config.hstore_handling_mode,
                            )
                        },
                    );
                    Ok((field.name.clone(), value))
                })
                .collect::<Result<Row>>()?;
            let event = ChangeEvent {
                id: EventId::deterministic(
                    connector_name,
                    &config.database,
                    &position,
                    &format!("{}.{}.{}", config.database, schema.schema, schema.table),
                    *ordinal,
                ),
                source: source_metadata(
                    connector_name,
                    config,
                    &schema.schema,
                    &schema.table,
                    true,
                    0,
                ),
                position,
                transaction: None,
                operation: Operation::Read,
                before: None,
                after: Some(after),
                schema: schema.event_schema.clone(),
                source_time: None,
                observed_time: Utc::now(),
            };
            output
                .blocking_send(Ok(SourceRecord::data(event)))
                .map_err(|_| Error::Cancelled)?;
        }
        offset += row_count as usize;
        if (row_count as usize) < fetch_size {
            break;
        }
    }
    info!(table = %format!("{}.{}", schema.schema, schema.table), rows = offset, "snapshot table completed");
    Ok(())
}

fn discover_tables(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
) -> Result<HashMap<(String, String), TableSchema>> {
    let publication = quote_literal(&config.publication).map_err(pg_error)?;
    let tables = connection
        .exec(&format!(
            "SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = {publication} ORDER BY schemaname, tablename"
        ))
        .map_err(pg_error)?;
    let mut schemas = HashMap::new();
    for index in 0..tables.ntuples() {
        let schema = required_value(&tables, index, 0, "publication schema")?;
        let table = required_value(&tables, index, 1, "publication table")?;
        if !config.tables.includes(&schema, &table) || is_signal_table(config, &schema, &table) {
            continue;
        }
        let table_schema = discover_table_schema(connection, config, &schema, &table)?;
        schemas.insert(table_schema.key(), table_schema);
    }
    Ok(schemas)
}

fn ensure_publication(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
) -> Result<()> {
    let publication_literal = quote_literal(&config.publication).map_err(pg_error)?;
    let publication_identifier = quote_ident(&config.publication).map_err(pg_error)?;
    let result = connection
        .exec(&format!(
            "SELECT puballtables::text FROM pg_catalog.pg_publication WHERE pubname = {publication_literal}"
        ))
        .map_err(pg_error)?;

    if result.ntuples() == 0 {
        let statement = match config.publication_autocreate_mode {
            PublicationAutoCreateMode::Disabled => {
                return Err(Error::Configuration(format!(
                    "publication {:?} does not exist and publication autocreation is disabled",
                    config.publication
                )));
            }
            PublicationAutoCreateMode::AllTables => {
                format!("CREATE PUBLICATION {publication_identifier} FOR ALL TABLES")
            }
            PublicationAutoCreateMode::Filtered => {
                let tables = selected_publication_tables(connection, config)?;
                format!(
                    "CREATE PUBLICATION {publication_identifier} FOR TABLE {}",
                    qualified_publication_tables(&tables)?
                )
            }
            PublicationAutoCreateMode::NoTables => {
                format!("CREATE PUBLICATION {publication_identifier}")
            }
        };
        connection.exec(&statement).map_err(pg_error)?;
        return Ok(());
    }

    if config.publication_autocreate_mode == PublicationAutoCreateMode::Filtered {
        let all_tables = required_value(&result, 0, 0, "publication all-tables state")?;
        if all_tables == "true" || all_tables == "t" {
            return Err(Error::Configuration(format!(
                "publication {:?} is FOR ALL TABLES and cannot be updated in filtered mode",
                config.publication
            )));
        }
        let tables = selected_publication_tables(connection, config)?;
        connection
            .exec(&format!(
                "ALTER PUBLICATION {publication_identifier} SET TABLE {}",
                qualified_publication_tables(&tables)?
            ))
            .map_err(pg_error)?;
    }
    Ok(())
}

fn apply_replica_identity(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
) -> Result<()> {
    if config.replica_identity_autoset_values.is_empty() {
        return Ok(());
    }

    let publication = quote_literal(&config.publication).map_err(pg_error)?;
    let result = connection
        .exec(&format!(
            "SELECT p.schemaname, p.tablename, c.relreplident::text, COALESCE(i.relname, '') \
             FROM pg_catalog.pg_publication_tables p \
             JOIN pg_catalog.pg_namespace n ON n.nspname = p.schemaname \
             JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename \
             LEFT JOIN pg_catalog.pg_index x ON x.indrelid = c.oid AND x.indisreplident \
             LEFT JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid \
             WHERE p.pubname = {publication} \
             ORDER BY p.schemaname, p.tablename"
        ))
        .map_err(pg_error)?;
    let mut changes = Vec::new();
    for row in 0..result.ntuples() {
        let schema = required_value(&result, row, 0, "replica identity table schema")?;
        let table = required_value(&result, row, 1, "replica identity table name")?;
        if !config.tables.includes(&schema, &table) || is_signal_table(config, &schema, &table) {
            continue;
        }
        let qualified = format!("{schema}.{table}");
        let mut matched = Vec::new();
        for rule in &config.replica_identity_autoset_values {
            let selector = Regex::new(&format!("^(?:{})$", rule.table)).map_err(|error| {
                Error::Configuration(format!(
                    "PostgreSQL replica identity table selector {:?} is invalid: {error}",
                    rule.table
                ))
            })?;
            if selector.is_match(&qualified) {
                matched.push(rule);
            }
        }
        if matched.len() > 1 {
            return Err(Error::Configuration(format!(
                "more than one replica.identity.autoset.values rule matches PostgreSQL table {qualified}"
            )));
        }
        let Some(rule) = matched.first() else {
            continue;
        };
        let current = required_value(&result, row, 2, "replica identity mode")?;
        let current_index = required_value(&result, row, 3, "replica identity index")?;
        let unchanged = match rule.identity {
            PostgresReplicaIdentity::Default => current == "d",
            PostgresReplicaIdentity::Full => current == "f",
            PostgresReplicaIdentity::Nothing => current == "n",
            PostgresReplicaIdentity::Index => {
                current == "i" && rule.index.as_deref() == Some(current_index.as_str())
            }
        };
        if !unchanged {
            changes.push((
                schema,
                table,
                rule.identity,
                rule.index.as_deref().map(str::to_string),
            ));
        }
    }

    if changes.is_empty() {
        return Ok(());
    }
    connection.exec("BEGIN").map_err(pg_error)?;
    let applied = (|| -> Result<()> {
        for (schema, table, identity, index) in &changes {
            let qualified = format!(
                "{}.{}",
                quote_ident(schema).map_err(pg_error)?,
                quote_ident(table).map_err(pg_error)?
            );
            let identity = match identity {
                PostgresReplicaIdentity::Default => "DEFAULT".into(),
                PostgresReplicaIdentity::Full => "FULL".into(),
                PostgresReplicaIdentity::Nothing => "NOTHING".into(),
                PostgresReplicaIdentity::Index => format!(
                    "USING INDEX {}",
                    quote_ident(index.as_deref().ok_or_else(|| {
                        Error::Configuration(format!(
                            "PostgreSQL replica identity index is missing for {schema}.{table}"
                        ))
                    })?)
                    .map_err(pg_error)?
                ),
            };
            connection
                .exec(&format!(
                    "ALTER TABLE {qualified} REPLICA IDENTITY {identity}"
                ))
                .map_err(|error| {
                    Error::Configuration(format!(
                        "failed to set PostgreSQL replica identity for {schema}.{table}: {error}"
                    ))
                })?;
        }
        Ok(())
    })();
    if let Err(error) = applied {
        let _ = connection.exec("ROLLBACK");
        return Err(error);
    }
    connection.exec("COMMIT").map_err(pg_error)?;
    for (schema, table, identity, index) in changes {
        info!(
            schema = %schema,
            table = %table,
            ?identity,
            ?index,
            "PostgreSQL replica identity updated"
        );
    }
    Ok(())
}

fn selected_publication_tables(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
) -> Result<Vec<(String, String)>> {
    let result = connection
        .exec(
            "SELECT n.nspname, c.relname \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') \
               AND n.nspname <> 'information_schema' \
               AND n.nspname !~ '^pg_' \
             ORDER BY n.nspname, c.relname",
        )
        .map_err(pg_error)?;
    let mut tables = Vec::new();
    for index in 0..result.ntuples() {
        let schema = required_value(&result, index, 0, "publication table schema")?;
        let table = required_value(&result, index, 1, "publication table name")?;
        if config.tables.includes(&schema, &table) || is_signal_table(config, &schema, &table) {
            tables.push((schema, table));
        }
    }
    if tables.is_empty() {
        return Err(Error::Configuration(format!(
            "publication.autocreate.mode=filtered selected no PostgreSQL tables for publication {:?}",
            config.publication
        )));
    }
    Ok(tables)
}

fn qualified_publication_tables(tables: &[(String, String)]) -> Result<String> {
    tables
        .iter()
        .map(|(schema, table)| {
            Ok(format!(
                "{}.{}",
                quote_ident(schema).map_err(pg_error)?,
                quote_ident(table).map_err(pg_error)?
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|tables| tables.join(", "))
}

fn validate_signal_table(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
) -> Result<()> {
    let Some(collection) = &config.signal_data_collection else {
        return Ok(());
    };
    let (schema, table) = collection.split_once('.').ok_or_else(|| {
        Error::Configuration("signal_data_collection must be schema-qualified".into())
    })?;
    let publication = quote_literal(&config.publication).map_err(pg_error)?;
    let schema_literal = quote_literal(schema).map_err(pg_error)?;
    let table_literal = quote_literal(table).map_err(pg_error)?;
    let published = scalar(
        connection,
        &format!(
            "SELECT EXISTS (\
                SELECT 1 FROM pg_catalog.pg_publication_tables \
                WHERE pubname = {publication} \
                  AND schemaname = {schema_literal} \
                  AND tablename = {table_literal}\
             )"
        ),
    )?;
    if published != "t" {
        return Err(Error::Configuration(format!(
            "PostgreSQL signal table {collection:?} is not part of publication {:?}",
            config.publication
        )));
    }
    let columns = connection
        .exec(&format!(
            "SELECT column_name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = {schema_literal} AND table_name = {table_literal} \
             ORDER BY ordinal_position"
        ))
        .map_err(pg_error)?;
    if columns.ntuples() != 3 {
        return Err(Error::Configuration(format!(
            "PostgreSQL signal table {collection:?} must contain exactly id, type, and data columns"
        )));
    }
    for (index, expected) in ["id", "type", "data"].iter().enumerate() {
        let index = i32::try_from(index)
            .map_err(|_| Error::Invariant("signal column index overflow".into()))?;
        let name = required_value(&columns, index, 0, "signal column name")?;
        let data_type = required_value(&columns, index, 1, "signal column type")?;
        if name != *expected
            || !matches!(
                data_type.as_str(),
                "text" | "character varying" | "character"
            )
        {
            return Err(Error::Configuration(format!(
                "PostgreSQL signal table {collection:?} column {} must be text-compatible {expected}",
                index + 1
            )));
        }
    }
    Ok(())
}

fn is_signal_table(config: &PostgresSourceConfig, schema: &str, table: &str) -> bool {
    config
        .signal_data_collection
        .as_deref()
        .and_then(|collection| collection.split_once('.'))
        .is_some_and(|(signal_schema, signal_table)| {
            signal_schema == schema && signal_table == table
        })
}

fn snapshot_includes(snapshot: &SnapshotConfig, schema: &str, table: &str) -> bool {
    snapshot.includes_collection(&format!("{schema}.{table}"))
}

fn discover_table_schema(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
    schema: &str,
    table: &str,
) -> Result<TableSchema> {
    query_table_schema(connection, config, schema, table)?
        .ok_or_else(|| Error::Source(format!("could not discover columns for {schema}.{table}")))
}

pub(crate) fn query_table_schema(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
    schema: &str,
    table: &str,
) -> Result<Option<TableSchema>> {
    let schema_literal = quote_literal(schema).map_err(pg_error)?;
    let table_literal = quote_literal(table).map_err(pg_error)?;
    let result = connection
        .exec(&format!(
            r#"SELECT a.attname,
                      CASE WHEN t.typtype = 'd'
                           THEN format_type(
                                  t.typbasetype,
                                  CASE WHEN a.atttypmod >= 0 THEN a.atttypmod
                                       ELSE t.typtypmod END
                                )
                           WHEN element_type.typtype = 'd'
                           THEN format_type(element_type.typbasetype, element_type.typtypmod) || '[]'
                           ELSE format_type(a.atttypid, a.atttypmod)
                      END,
                      NOT a.attnotnull,
                      EXISTS (
                        SELECT 1
                        FROM pg_index i
                        WHERE i.indrelid = c.oid
                          AND i.indisprimary
                          AND a.attnum = ANY(i.indkey::smallint[])
                      ),
                      a.atttypid::oid::text,
                      a.atttypmod::text
               FROM pg_attribute a
               JOIN pg_class c ON c.oid = a.attrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               JOIN pg_type t ON t.oid = a.atttypid
               LEFT JOIN pg_type element_type ON element_type.oid = t.typelem
               WHERE n.nspname = {schema_literal}
                 AND c.relname = {table_literal}
                 AND a.attnum > 0
                 AND NOT a.attisdropped
               ORDER BY a.attnum"#
        ))
        .map_err(pg_error)?;
    let mut fields = Vec::with_capacity(result.ntuples() as usize);
    let mut column_types = Vec::with_capacity(result.ntuples() as usize);
    for index in 0..result.ntuples() {
        let name = required_value(&result, index, 0, "column name")?;
        fields.push(FieldSchema {
            name: name.clone(),
            type_name: required_value(&result, index, 1, "column type")?,
            optional: required_value(&result, index, 2, "column optionality")? == "t",
            primary_key: required_value(&result, index, 3, "column key state")? == "t",
        });
        column_types.push(PostgresColumnType {
            name,
            type_oid: required_value(&result, index, 4, "column type OID")?
                .parse()
                .map_err(|error| Error::Source(format!("invalid column type OID: {error}")))?,
            type_modifier: required_value(&result, index, 5, "column type modifier")?
                .parse()
                .map_err(|error| Error::Source(format!("invalid column type modifier: {error}")))?,
        });
    }
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(TableSchema {
        schema: schema.into(),
        table: table.into(),
        event_schema: EventSchema {
            name: format!(
                "{}.{}.{}.{}.Envelope",
                config.slot_name, config.database, schema, table
            ),
            version: 1,
            fields,
        },
        column_types,
    }))
}

fn convert_row(row: &RowData, schema: &EventSchema, hstore_handling_mode: &str) -> Row {
    row.iter()
        .map(|(name, value)| {
            let type_name = schema
                .fields
                .iter()
                .find(|field| field.name == name.as_ref())
                .map_or("text", |field| field.type_name.as_str());
            (
                name.to_string(),
                convert_value(value, type_name, hstore_handling_mode),
            )
        })
        .collect()
}

fn convert_value(value: &ColumnValue, type_name: &str, hstore_handling_mode: &str) -> DataValue {
    match value {
        ColumnValue::Null => DataValue::Null,
        ColumnValue::Binary(value) => DataValue::Bytes(value.to_vec()),
        ColumnValue::Text(value) => {
            let Ok(value) = std::str::from_utf8(value) else {
                return DataValue::Bytes(value.to_vec());
            };
            convert_text_with_hstore_mode(value, type_name, hstore_handling_mode)
        }
    }
}

#[cfg(test)]
pub(crate) fn convert_text(value: &str, type_name: &str) -> DataValue {
    convert_text_with_hstore_mode(value, type_name, "json")
}

pub(crate) fn convert_text_with_hstore_mode(
    value: &str,
    type_name: &str,
    hstore_handling_mode: &str,
) -> DataValue {
    if let Some(element_type) = type_name.trim().strip_suffix("[]") {
        return parse_postgres_array(value, element_type, hstore_handling_mode)
            .unwrap_or_else(|| DataValue::String(value.into()));
    }
    let base_type = unqualified_base_type(type_name);
    match base_type {
        "boolean" => DataValue::Boolean(matches!(value, "t" | "true" | "1")),
        "smallint" | "integer" => value
            .parse::<i32>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Int32),
        "bigint" => value
            .parse::<i64>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Int64),
        "real" | "double precision" => match value {
            "NaN" | "Infinity" | "-Infinity" => DataValue::String(value.into()),
            _ => value
                .parse::<f64>()
                .map_or_else(|_| DataValue::String(value.into()), DataValue::Float64),
        },
        "numeric" | "decimal" | "money" => DataValue::Decimal(value.into()),
        "json" | "jsonb" => serde_json::from_str(value)
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Json),
        "uuid" => uuid::Uuid::parse_str(value)
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Uuid),
        "oid" => value
            .parse::<u64>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::UInt64),
        "date" => DataValue::Date(value.into()),
        "time" | "time without time zone" | "time with time zone" => DataValue::Time(value.into()),
        "timestamp" | "timestamp without time zone" | "timestamp with time zone" => {
            DataValue::Timestamp(value.into())
        }
        "bytea" if value.starts_with("\\x") => hex_decode(&value[2..])
            .map_or_else(|| DataValue::String(value.into()), DataValue::Bytes),
        "hstore" => parse_hstore(value).map_or_else(
            || DataValue::String(value.into()),
            |values| hstore_value(values, hstore_handling_mode),
        ),
        "vector" | "halfvec" => parse_dense_vector(value)
            .map_or_else(|| DataValue::String(value.into()), DataValue::Array),
        "sparsevec" => parse_sparse_vector(value)
            .map_or_else(|| DataValue::String(value.into()), DataValue::Map),
        "geometry" | "geography" => hex_decode(value.strip_prefix("\\x").unwrap_or(value))
            .map_or_else(|| DataValue::String(value.into()), DataValue::Bytes),
        _ => DataValue::String(value.into()),
    }
}

fn unqualified_base_type(type_name: &str) -> &str {
    type_name
        .split('(')
        .next()
        .unwrap_or(type_name)
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or(type_name)
        .trim()
        .trim_matches('"')
}

fn hstore_value(values: BTreeMap<String, Option<String>>, hstore_handling_mode: &str) -> DataValue {
    if hstore_handling_mode == "map" {
        return DataValue::Map(
            values
                .into_iter()
                .map(|(key, value)| (key, value.map_or(DataValue::Null, DataValue::String)))
                .collect(),
        );
    }
    DataValue::Json(
        values
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    value.map_or(serde_json::Value::Null, serde_json::Value::String),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into(),
    )
}

fn parse_hstore(value: &str) -> Option<BTreeMap<String, Option<String>>> {
    let mut parser = HstoreParser {
        input: value,
        index: 0,
    };
    let mut values = BTreeMap::new();
    parser.skip_whitespace();
    if parser.peek().is_none() {
        return Some(values);
    }
    loop {
        let key = parser.parse_string()?;
        parser.skip_whitespace();
        parser.consume('=')?;
        parser.consume('>')?;
        parser.skip_whitespace();
        let entry_value = if parser.remaining_starts_with_null() {
            parser.index += 4;
            None
        } else {
            Some(parser.parse_string()?)
        };
        values.insert(key, entry_value);
        parser.skip_whitespace();
        match parser.peek() {
            None => return Some(values),
            Some(',') => {
                parser.consume(',')?;
                parser.skip_whitespace();
            }
            Some(_) => return None,
        }
    }
}

struct HstoreParser<'a> {
    input: &'a str,
    index: usize,
}

impl HstoreParser<'_> {
    fn parse_string(&mut self) -> Option<String> {
        if self.peek()? == '"' {
            self.consume('"')?;
            let mut output = String::new();
            loop {
                match self.peek()? {
                    '"' => {
                        self.consume('"')?;
                        return Some(output);
                    }
                    '\\' => {
                        self.consume('\\')?;
                        output.push(self.take()?);
                    }
                    _ => output.push(self.take()?),
                }
            }
        }
        let start = self.index;
        while self
            .peek()
            .is_some_and(|character| !character.is_whitespace() && !matches!(character, '=' | ','))
        {
            self.take()?;
        }
        (self.index > start).then(|| self.input[start..self.index].to_string())
    }

    fn remaining_starts_with_null(&self) -> bool {
        self.input[self.index..]
            .strip_prefix("NULL")
            .is_some_and(|remaining| {
                remaining
                    .chars()
                    .next()
                    .is_none_or(|character| character.is_whitespace() || character == ',')
            })
    }

    fn consume(&mut self, expected: char) -> Option<()> {
        (self.take()? == expected).then_some(())
    }

    fn take(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.index += character.len_utf8();
        Some(character)
    }

    fn peek(&self) -> Option<char> {
        self.input[self.index..].chars().next()
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.take();
        }
    }
}

fn parse_dense_vector(value: &str) -> Option<Vec<DataValue>> {
    let values = value.trim().strip_prefix('[')?.strip_suffix(']')?.trim();
    if values.is_empty() {
        return Some(Vec::new());
    }
    values
        .split(',')
        .map(|value| value.trim().parse::<f64>().ok().map(DataValue::Float64))
        .collect()
}

fn parse_sparse_vector(value: &str) -> Option<BTreeMap<String, DataValue>> {
    let (entries, dimensions) = value.trim().split_once('/')?;
    let dimensions = dimensions.trim().parse::<i32>().ok()?;
    let entries = entries.trim().strip_prefix('{')?.strip_suffix('}')?.trim();
    let vector = if entries.is_empty() {
        BTreeMap::new()
    } else {
        entries
            .split(',')
            .map(|entry| {
                let (index, value) = entry.trim().split_once(':')?;
                let index = index.trim().parse::<i16>().ok()?.to_string();
                let value = value.trim().parse::<f64>().ok()?;
                Some((index, DataValue::Float64(value)))
            })
            .collect::<Option<BTreeMap<_, _>>>()?
    };
    Some(BTreeMap::from([
        ("dimensions".into(), DataValue::Int32(dimensions)),
        ("vector".into(), DataValue::Map(vector)),
    ]))
}

fn parse_postgres_array(
    value: &str,
    element_type: &str,
    hstore_handling_mode: &str,
) -> Option<DataValue> {
    let start = value.find('{')?;
    let dimensions = &value[..start];
    if !(dimensions.is_empty() || dimensions.starts_with('[') && dimensions.ends_with("]=")) {
        return None;
    }
    let delimiter = if element_type
        .split('(')
        .next()
        .is_some_and(|name| name.trim() == "box")
    {
        b';'
    } else {
        b','
    };
    let mut parser = PostgresArrayParser {
        input: value.as_bytes(),
        index: start,
        delimiter,
        element_type,
        hstore_handling_mode,
    };
    let parsed = parser.parse_array()?;
    parser.skip_whitespace();
    (parser.index == parser.input.len()).then_some(parsed)
}

struct PostgresArrayParser<'a> {
    input: &'a [u8],
    index: usize,
    delimiter: u8,
    element_type: &'a str,
    hstore_handling_mode: &'a str,
}

impl PostgresArrayParser<'_> {
    fn parse_array(&mut self) -> Option<DataValue> {
        self.consume(b'{')?;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.peek()? == b'}' {
            self.index += 1;
            return Some(DataValue::Array(values));
        }
        loop {
            self.skip_whitespace();
            let value = if self.peek()? == b'{' {
                self.parse_array()?
            } else {
                self.parse_element()?
            };
            values.push(value);
            self.skip_whitespace();
            match self.peek()? {
                byte if byte == self.delimiter => self.index += 1,
                b'}' => {
                    self.index += 1;
                    return Some(DataValue::Array(values));
                }
                _ => return None,
            }
        }
    }

    fn parse_element(&mut self) -> Option<DataValue> {
        if self.peek()? == b'"' {
            self.index += 1;
            let mut value = Vec::new();
            loop {
                match self.peek()? {
                    b'"' => {
                        self.index += 1;
                        let value = std::str::from_utf8(&value).ok()?;
                        return Some(convert_text_with_hstore_mode(
                            value,
                            self.element_type,
                            self.hstore_handling_mode,
                        ));
                    }
                    b'\\' => {
                        self.index += 1;
                        value.push(self.peek()?);
                        self.index += 1;
                    }
                    byte => {
                        value.push(byte);
                        self.index += 1;
                    }
                }
            }
        }

        let mut value = Vec::new();
        while let Some(byte) = self.peek() {
            if byte == self.delimiter || byte == b'}' {
                break;
            }
            if byte == b'\\' {
                self.index += 1;
                value.push(self.peek()?);
                self.index += 1;
            } else {
                value.push(byte);
                self.index += 1;
            }
        }
        let value = std::str::from_utf8(&value).ok()?.trim();
        if value.is_empty() {
            return None;
        }
        if value == "NULL" {
            Some(DataValue::Null)
        } else {
            Some(convert_text_with_hstore_mode(
                value,
                self.element_type,
                self.hstore_handling_mode,
            ))
        }
    }

    fn consume(&mut self, expected: u8) -> Option<()> {
        (self.peek()? == expected).then(|| self.index += 1)
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.index).copied()
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.index += 1;
        }
    }
}

fn fill_unavailable(after: &mut Row, before: Option<&Row>, schema: &EventSchema) {
    for field in &schema.fields {
        if !after.contains_key(&field.name) {
            let value = before
                .and_then(|row| row.get(&field.name))
                .cloned()
                .unwrap_or(DataValue::Unavailable);
            after.insert(field.name.clone(), value);
        }
    }
}

fn source_metadata(
    connector_name: &str,
    config: &PostgresSourceConfig,
    schema: &str,
    table: &str,
    snapshot: bool,
    relation_oid: u32,
) -> SourceMetadata {
    let mut attributes = BTreeMap::new();
    if relation_oid != 0 {
        attributes.insert("relation_oid".into(), relation_oid.into());
    }
    SourceMetadata {
        connector: "postgresql".into(),
        connector_name: connector_name.into(),
        database: config.database.clone(),
        schema: Some(schema.into()),
        table: Some(table.into()),
        snapshot,
        version: CONNECTOR_VERSION.into(),
        attributes,
    }
}

fn connect(connection_url: &str) -> Result<PgReplicationConnection> {
    PgReplicationConnection::connect(connection_url).map_err(pg_error)
}

fn scalar(connection: &mut PgReplicationConnection, query: &str) -> Result<String> {
    let result = connection.exec(query).map_err(pg_error)?;
    required_value(&result, 0, 0, "query result")
}

fn required_value(
    result: &pg_walstream::PgResult,
    row: i32,
    column: i32,
    label: &str,
) -> Result<String> {
    result
        .get_value(row, column)
        .ok_or_else(|| Error::Source(format!("missing {label}")))
}

fn pg_error(error: impl std::fmt::Display) -> Error {
    Error::Source(error.to_string())
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use indexmap::IndexMap;
    use rustium_config::TableSelection;
    use rustium_core::{Checkpoint, ConnectorIdentity};
    use tokio::sync::{mpsc, watch};
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[test]
    fn qualifies_postgresql_snapshot_collection_filters() {
        let snapshot = SnapshotConfig {
            include_collections: vec![r"public\.orders".into()],
            ..SnapshotConfig::default()
        };
        assert!(snapshot_includes(&snapshot, "public", "orders"));
        assert!(!snapshot_includes(&snapshot, "archive", "orders"));
        assert!(!snapshot_includes(&snapshot, "public", "orders_history"));
    }

    #[test]
    fn builds_heartbeat_at_the_safe_postgresql_position() {
        let position = postgres_streaming_position(512);
        let record = heartbeat_record("inventory-postgresql", "inventory", position.clone());
        let event = record.event.unwrap();

        assert_eq!(record.boundary, RecordBoundary::Heartbeat);
        assert_eq!(record.position, position);
        assert_eq!(event.operation, Operation::Message);
        assert_eq!(event.source.table, None);
        assert_eq!(event.source.schema, None);
        assert_eq!(
            event.source.attributes.get("rustium.heartbeat"),
            Some(&true.into())
        );
        assert!(matches!(
            event.after.unwrap().get("ts_ms"),
            Some(DataValue::Int64(_))
        ));
    }

    #[test]
    fn maps_shared_retry_policy_to_postgresql_recovery() {
        let disabled = postgres_retry_config(
            RetryPolicy {
                max_retries: 0,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_millis(250),
            },
            Duration::from_secs(2),
        );
        assert_eq!(disabled.max_attempts, 1);
        assert_eq!(disabled.max_duration, Duration::from_secs(2));

        let finite = postgres_retry_config(
            RetryPolicy {
                max_retries: 3,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_millis(250),
            },
            Duration::from_secs(2),
        );
        assert_eq!(finite.max_attempts, 4);
        assert_eq!(finite.initial_delay, Duration::from_millis(100));
        assert_eq!(finite.max_delay, Duration::from_millis(250));
        assert_eq!(finite.max_duration, Duration::from_millis(8_550));
        assert_eq!(finite.multiplier, 2.0);
        assert!(!finite.jitter);

        let unlimited = postgres_retry_config(
            RetryPolicy {
                max_retries: -1,
                initial_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(1),
            },
            Duration::from_secs(2),
        );
        assert_eq!(unlimited.max_attempts, u32::MAX);
        assert_eq!(unlimited.max_duration, EFFECTIVELY_UNBOUNDED_RETRY_DURATION);
    }

    #[test]
    fn converts_postgres_scalar_types() {
        assert_eq!(convert_text("42", "integer"), DataValue::Int32(42));
        assert_eq!(convert_text("t", "boolean"), DataValue::Boolean(true));
        assert_eq!(
            convert_text("12.30", "numeric(10,2)"),
            DataValue::Decimal("12.30".into())
        );
        assert_eq!(
            convert_text("NaN", "double precision"),
            DataValue::String("NaN".into())
        );
        assert_eq!(
            convert_text("4294967295", "oid"),
            DataValue::UInt64(4_294_967_295)
        );
    }

    #[test]
    fn converts_postgres_text_and_multidimensional_arrays() {
        assert_eq!(
            convert_text(
                r#"{alpha,"comma,value","NULL",NULL,"quote\"value","slash\\value"}"#,
                "text[]",
            ),
            DataValue::Array(vec![
                DataValue::String("alpha".into()),
                DataValue::String("comma,value".into()),
                DataValue::String("NULL".into()),
                DataValue::Null,
                DataValue::String("quote\"value".into()),
                DataValue::String("slash\\value".into()),
            ])
        );
        assert_eq!(
            convert_text("[0:1][2:3]={{1,2},{3,NULL}}", "integer[]"),
            DataValue::Array(vec![
                DataValue::Array(vec![DataValue::Int32(1), DataValue::Int32(2)]),
                DataValue::Array(vec![DataValue::Int32(3), DataValue::Null]),
            ])
        );
    }

    #[test]
    fn preserves_malformed_postgres_arrays_as_strings() {
        for value in ["{one,two", "garbage{one,two}", "{one,,two}"] {
            assert_eq!(
                convert_text(value, "text[]"),
                DataValue::String(value.into())
            );
        }
    }

    #[test]
    fn converts_hstore_json_map_and_arrays() {
        let value = r#""alpha"=>"one", "escaped\"key"=>"slash\\value", "nothing"=>NULL"#;
        assert_eq!(
            convert_text(value, "\"extensions\".\"hstore\""),
            DataValue::Json(serde_json::json!({
                "alpha": "one",
                "escaped\"key": "slash\\value",
                "nothing": null
            }))
        );
        assert_eq!(
            convert_text_with_hstore_mode(value, "hstore", "map"),
            DataValue::Map(BTreeMap::from([
                ("alpha".into(), DataValue::String("one".into())),
                (
                    "escaped\"key".into(),
                    DataValue::String("slash\\value".into())
                ),
                ("nothing".into(), DataValue::Null),
            ]))
        );
        assert!(matches!(
            convert_text(r#"{"\"alpha\"=>\"one\"",NULL}"#, "hstore[]"),
            DataValue::Array(values)
                if matches!(values.as_slice(), [DataValue::Json(_), DataValue::Null])
        ));
    }

    #[test]
    fn converts_postgres_vectors_and_spatial_binary() {
        assert_eq!(
            convert_text("[1, 2.5, -3]", "pgvector.vector(3)"),
            DataValue::Array(vec![
                DataValue::Float64(1.0),
                DataValue::Float64(2.5),
                DataValue::Float64(-3.0),
            ])
        );
        assert_eq!(
            convert_text("{1:1.5, 9:-2}/12", "sparsevec(12)"),
            DataValue::Map(BTreeMap::from([
                ("dimensions".into(), DataValue::Int32(12)),
                (
                    "vector".into(),
                    DataValue::Map(BTreeMap::from([
                        ("1".into(), DataValue::Float64(1.5)),
                        ("9".into(), DataValue::Float64(-2.0)),
                    ])),
                ),
            ]))
        );
        assert_eq!(
            convert_text(
                "0101000000000000000000F03F0000000000000040",
                "postgis.geometry(Point,4326)",
            ),
            DataValue::Bytes(hex_decode("0101000000000000000000F03F0000000000000040").unwrap())
        );
    }

    #[test]
    fn fills_missing_toast_columns() {
        let schema = EventSchema {
            name: "test".into(),
            version: 1,
            fields: vec![
                FieldSchema {
                    name: "id".into(),
                    type_name: "integer".into(),
                    optional: false,
                    primary_key: true,
                },
                FieldSchema {
                    name: "body".into(),
                    type_name: "text".into(),
                    optional: true,
                    primary_key: false,
                },
            ],
        };
        let mut after = IndexMap::from([("id".into(), DataValue::Int32(1))]);
        fill_unavailable(&mut after, None, &schema);
        assert_eq!(after["body"], DataValue::Unavailable);
    }

    #[test]
    fn versions_relation_driven_schema_changes() {
        let original = TableSchema {
            schema: "public".into(),
            table: "orders".into(),
            event_schema: EventSchema {
                name: "test.public.orders.Envelope".into(),
                version: 1,
                fields: vec![FieldSchema {
                    name: "id".into(),
                    type_name: "bigint".into(),
                    optional: false,
                    primary_key: true,
                }],
            },
            column_types: vec![PostgresColumnType {
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
            }],
        };
        let mut state = StreamingState::new(
            HashMap::from([(original.key(), original.clone())]),
            None,
            false,
        );

        assert_eq!(state.refresh_schema(original.clone()), None);
        let mut changed = original;
        changed.event_schema.fields.push(FieldSchema {
            name: "status".into(),
            type_name: "text".into(),
            optional: false,
            primary_key: false,
        });
        changed.column_types.push(PostgresColumnType {
            name: "status".into(),
            type_oid: 25,
            type_modifier: -1,
        });
        assert_eq!(state.refresh_schema(changed), Some(2));
        assert_eq!(
            state.schemas[&("public".into(), "orders".into())]
                .event_schema
                .version,
            2
        );
    }

    #[test]
    fn reuses_historical_type_name_when_catalog_type_resolution_fails() {
        let previous = TableSchema {
            schema: "public".into(),
            table: "orders".into(),
            event_schema: EventSchema {
                name: "test.public.orders.Envelope".into(),
                version: 2,
                fields: vec![FieldSchema {
                    name: "legacy_amount".into(),
                    type_name: "numeric(20,4)".into(),
                    optional: true,
                    primary_key: false,
                }],
            },
            column_types: vec![PostgresColumnType {
                name: "legacy_amount".into(),
                type_oid: 1_640_001,
                type_modifier: 1_310_728,
            }],
        };
        let relation_column = RelationColumn {
            name: Arc::from("legacy_amount"),
            type_id: 1_640_001,
            type_modifier: 1_310_728,
            is_key: false,
        };

        assert_eq!(
            relation_type_name_fallback(Some(&previous), &relation_column),
            "numeric(20,4)"
        );
    }

    #[test]
    fn conservatively_names_unknown_relation_type_without_history() {
        let relation_column = RelationColumn {
            name: Arc::from("removed_extension_value"),
            type_id: 1_640_002,
            type_modifier: -1,
            is_key: false,
        };

        assert_eq!(
            relation_type_name_fallback(None, &relation_column),
            "unknown_oid_1640002"
        );
    }

    #[tokio::test]
    async fn rejects_legacy_postgres_checkpoint_without_schema_history() {
        let config = PostgresSourceConfig {
            hostname: "localhost".into(),
            port: 5432,
            database: "inventory".into(),
            username: "rustium".into(),
            password: "secret".into(),
            publication: "orders_publication".into(),
            publication_autocreate_mode: PublicationAutoCreateMode::Disabled,
            replica_identity_autoset_values: Vec::new(),
            slot_name: "orders_slot".into(),
            slot_ownership: SlotOwnership::Managed,
            tables: TableSelection::default(),
            ssl_mode: "disable".into(),
            connect_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::ZERO,
            heartbeat_action_query: None,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            signal_data_collection: None,
            signal_enabled_channels: vec!["source".into()],
            signal_file: "file-signals.txt".into(),
            signal_poll_interval: Duration::from_secs(5),
            signal_kafka_topic: None,
            signal_kafka_bootstrap_servers: Vec::new(),
            signal_kafka_group_id: "kafka-signal".into(),
            signal_kafka_poll_timeout: Duration::from_millis(100),
            signal_kafka_consumer_properties: BTreeMap::new(),
            incremental_snapshot_chunk_size: 1_024,
            incremental_snapshot_watermarking_strategy: "insert_insert".into(),
            read_only: false,
            hstore_handling_mode: "json".into(),
        };
        let mut source = PostgresSource::new(
            "inventory-postgresql",
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        let position = SourcePosition::Postgres(PostgresPosition {
            lsn: 128,
            commit_lsn: Some(128),
            transaction_id: None,
            event_serial: 1,
            snapshot: false,
        });
        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: "inventory-postgresql".into(),
            generation: ConnectorIdentity::new("inventory-postgresql").generation,
            source_position: position.clone(),
            snapshot_completed: true,
            config_fingerprint: "legacy".into(),
            updated_at: SystemTime::now(),
            connector_state: None,
        };
        let (output, _records) = mpsc::channel(1);
        let (_acknowledged, acknowledgements) = watch::channel(Some(position));

        let error = source
            .run(SourceContext {
                output,
                acknowledged: acknowledgements,
                initial_checkpoint: Some(checkpoint),
                signals: rustium_core::signal_channel(1).1,
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::State(message) if message.contains("predates persistent schema history"))
        );
    }
}
