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
use rustium_config::{PostgresSourceConfig, SlotOwnership, SnapshotConfig, SnapshotMode};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, Operation, PostgresPosition,
    RecordBoundary, Result, Row, SourceConnector, SourceContext, SourceMetadata, SourcePosition,
    SourceRecord, TransactionMetadata,
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
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
        }
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
            let publication = quote_literal(&config.publication).map_err(pg_error)?;
            let publication_exists = scalar(
                &mut connection,
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = {publication})"
                ),
            )?;
            if publication_exists != "t" {
                return Err(Error::Configuration(format!(
                    "publication {:?} does not exist",
                    config.publication
                )));
            }
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

        if schemas.is_empty() {
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
            let catalog = query_table_schema(&mut connection, &config, &schema, &table)?;
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
                })
                .collect::<Result<Vec<_>>>()?;
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

    async fn run_snapshot(
        &self,
        snapshot_name: String,
        output: mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<SnapshotOutcome> {
        let connection_url = self.config.connection_url(false)?;
        let config = self.config.clone();
        let connector_name = self.connector_name.clone();
        let fetch_size = self.snapshot.fetch_size;
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
        let snapshot_needed = match self.snapshot.mode {
            SnapshotMode::Never => false,
            SnapshotMode::Initial | SnapshotMode::WhenNeeded => checkpoint
                .as_ref()
                .is_none_or(|checkpoint| !checkpoint.snapshot_completed),
        };

        let mut incremental_progress = None;
        if !snapshot_needed {
            if let Some(connector_state) = checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.connector_state.as_ref())
            {
                let state = decode_connector_state(connector_state)?;
                self.schemas = state.schemas;
                incremental_progress = state.incremental_snapshot;
            } else if checkpoint.is_some() {
                return Err(Error::State(
                    "PostgreSQL checkpoint predates persistent schema history and cannot safely replay relation changes; reset the checkpoint and run a new initial snapshot"
                        .into(),
                ));
            }
        }

        if snapshot_needed {
            self.prepare_snapshot_slot().await?;
        }

        let replication_url = self.config.connection_url(true)?;
        let slot_options = ReplicationSlotOptions {
            snapshot: Some(if snapshot_needed { "export" } else { "nothing" }.into()),
            ..ReplicationSlotOptions::default()
        };
        let stream_config = ReplicationStreamConfig::new(
            self.config.slot_name.clone(),
            self.config.publication.clone(),
            2,
            StreamingMode::On,
            Duration::from_secs(10),
            self.config.connect_timeout,
            Duration::from_secs(30),
            RetryConfig::default(),
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
        let mut heartbeat_connection = open_heartbeat_connection(&self.config).await?;
        let mut incremental = IncrementalSnapshotController::new(incremental_progress);
        incremental.resume(&self.config, &state.schemas).await?;
        info!(
            connector = %self.connector_name,
            slot = %self.config.slot_name,
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
                event = stream.next_event_with_retry(&context.cancellation) => {
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
                            info!(
                                schema = %schema_name,
                                table = %table_name,
                                version,
                                "PostgreSQL table schema refreshed"
                            );
                        }
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

fn heartbeat_timer(interval: Duration) -> Option<tokio::time::Interval> {
    if interval.is_zero() {
        return None;
    }
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    Some(timer)
}

async fn next_heartbeat(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
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

        let before = old_data.as_ref().map(|row| convert_row(row, &event_schema));
        let mut after = new_data.as_ref().map(|row| convert_row(row, &event_schema));
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
                    let value = result
                        .get_value(row_index, column_index)
                        .map_or(DataValue::Null, |value| {
                            convert_text(&value, &field.type_name)
                        });
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

fn discover_table_schema(
    connection: &mut PgReplicationConnection,
    config: &PostgresSourceConfig,
    schema: &str,
    table: &str,
) -> Result<TableSchema> {
    query_table_schema(connection, config, schema, table)?
        .ok_or_else(|| Error::Source(format!("could not discover columns for {schema}.{table}")))
}

fn query_table_schema(
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
                      format_type(a.atttypid, a.atttypmod),
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

fn convert_row(row: &RowData, schema: &EventSchema) -> Row {
    row.iter()
        .map(|(name, value)| {
            let type_name = schema
                .fields
                .iter()
                .find(|field| field.name == name.as_ref())
                .map_or("text", |field| field.type_name.as_str());
            (name.to_string(), convert_value(value, type_name))
        })
        .collect()
}

fn convert_value(value: &ColumnValue, type_name: &str) -> DataValue {
    match value {
        ColumnValue::Null => DataValue::Null,
        ColumnValue::Binary(value) => DataValue::Bytes(value.to_vec()),
        ColumnValue::Text(value) => {
            let Ok(value) = std::str::from_utf8(value) else {
                return DataValue::Bytes(value.to_vec());
            };
            convert_text(value, type_name)
        }
    }
}

pub(crate) fn convert_text(value: &str, type_name: &str) -> DataValue {
    if let Some(element_type) = type_name.trim().strip_suffix("[]") {
        return parse_postgres_array(value, element_type)
            .unwrap_or_else(|| DataValue::String(value.into()));
    }
    let base_type = type_name.split('(').next().unwrap_or(type_name).trim();
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
        _ => DataValue::String(value.into()),
    }
}

fn parse_postgres_array(value: &str, element_type: &str) -> Option<DataValue> {
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
                        return Some(convert_text(value, self.element_type));
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
            Some(convert_text(value, self.element_type))
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

    #[tokio::test]
    async fn rejects_legacy_postgres_checkpoint_without_schema_history() {
        let config = PostgresSourceConfig {
            hostname: "localhost".into(),
            port: 5432,
            database: "inventory".into(),
            username: "rustium".into(),
            password: "secret".into(),
            publication: "orders_publication".into(),
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
            incremental_snapshot_chunk_size: 1_024,
            incremental_snapshot_watermarking_strategy: "insert_insert".into(),
            read_only: false,
        };
        let mut source = PostgresSource::new(
            "inventory-postgresql",
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
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
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::State(message) if message.contains("predates persistent schema history"))
        );
    }
}
