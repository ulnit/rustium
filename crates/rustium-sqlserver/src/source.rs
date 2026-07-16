use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::TryStreamExt;
use rustium_config::{SnapshotConfig, SnapshotMode, SqlServerSourceConfig};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, Operation, RecordBoundary,
    Result, RetryPolicy, Row, SignalAcknowledgement, SignalRecord, SourceConnector, SourceContext,
    SourceMetadata, SourcePosition, SourceRecord, SqlServerPosition, TransactionMetadata,
};
use tiberius::{
    AuthMethod, Client, ColumnData, Config as TdsConfig, EncryptionLevel, Row as TdsRow,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{info, warn};

use crate::state::{
    IncrementalSnapshotProgress, SqlServerKeyValue, decode_connector_state, encode_connector_state,
};

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");
const LSN_SIZE: usize = 10;
const RAW_DELETE: i32 = 1;
const RAW_INSERT: i32 = 2;
const RAW_UPDATE_BEFORE: i32 = 3;
const RAW_UPDATE_AFTER: i32 = 4;
const COMMIT_SERIAL: u64 = 5;
const MAX_COMPLETED_SIGNAL_IDS: usize = 1_024;
const WINDOW_OPEN_SIGNAL: &str = "snapshot-window-open";
const WINDOW_CLOSE_SIGNAL: &str = "snapshot-window-close";

type SqlClient = Client<Compat<TcpStream>>;

#[derive(Debug, Clone)]
struct CaptureTable {
    schema: String,
    table: String,
    capture_instance: String,
    source_object_id: i32,
    event_schema: EventSchema,
}

impl CaptureTable {
    fn key(&self) -> (String, String) {
        (self.schema.clone(), self.table.clone())
    }
}

#[derive(Debug, Clone)]
struct CdcCursor {
    commit_lsn: Vec<u8>,
    change_lsn: Vec<u8>,
    raw_operation: i32,
}

impl CdcCursor {
    fn at_snapshot(commit_lsn: Vec<u8>) -> Self {
        Self {
            commit_lsn,
            change_lsn: max_lsn_bytes(),
            raw_operation: i32::try_from(COMMIT_SERIAL).unwrap_or(i32::MAX),
        }
    }

    fn from_position(
        position: &SqlServerPosition,
        connector_state_boundary: bool,
    ) -> Result<(Self, Option<SourcePosition>)> {
        if position.snapshot || position.event_serial == COMMIT_SERIAL || connector_state_boundary {
            return Ok((Self::at_snapshot(parse_lsn(&position.commit_lsn)?), None));
        }
        Ok((
            Self {
                commit_lsn: parse_lsn(&position.commit_lsn)?,
                change_lsn: zero_lsn_bytes(),
                raw_operation: 0,
            },
            Some(SourcePosition::SqlServer(position.clone())),
        ))
    }

    fn commit_complete(&self) -> bool {
        self.raw_operation == i32::try_from(COMMIT_SERIAL).unwrap_or(i32::MAX)
            && self.change_lsn == max_lsn_bytes()
    }
}

#[derive(Debug)]
struct RawChange {
    commit_lsn: Vec<u8>,
    change_lsn: Vec<u8>,
    raw_operation: i32,
    capture_instance: String,
    row: Row,
    source_time: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct ActiveTransaction {
    commit_lsn: Vec<u8>,
    source_time: Option<DateTime<Utc>>,
    total_order: u64,
    collection_order: HashMap<(String, String), u64>,
}

#[derive(Debug, Clone)]
enum SnapshotSignal {
    Execute {
        id: String,
        data_collections: Vec<String>,
        additional_conditions: BTreeMap<String, String>,
    },
    Stop {
        id: String,
        data_collections: Vec<String>,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Unsupported {
        id: String,
        signal_type: String,
    },
}

struct SqlServerIncrementalSnapshot {
    progress: Option<IncrementalSnapshotProgress>,
    opening_window_id: Option<String>,
    window: Option<SqlServerIncrementalWindow>,
    completed_signal_ids: Vec<String>,
    event_serial: u64,
    state_dirty: bool,
}

impl SqlServerIncrementalSnapshot {
    fn new(
        progress: Option<IncrementalSnapshotProgress>,
        completed_signal_ids: Vec<String>,
    ) -> Self {
        Self {
            progress,
            opening_window_id: None,
            window: None,
            completed_signal_ids,
            event_serial: 0,
            state_dirty: false,
        }
    }

    fn progress(&self) -> Option<&IncrementalSnapshotProgress> {
        self.progress.as_ref()
    }

    fn completed_signal_ids(&self) -> &[String] {
        &self.completed_signal_ids
    }

    fn is_active(&self) -> bool {
        self.progress
            .as_ref()
            .is_some_and(|progress| !progress.paused)
            && self.opening_window_id.is_none()
            && self.window.is_none()
    }

    fn discard_window(&mut self) {
        self.opening_window_id = None;
        self.window = None;
    }

    fn state_dirty(&self) -> bool {
        self.state_dirty
    }

    fn mark_checkpointed(&mut self) {
        self.state_dirty = false;
    }

    fn remember_completed(&mut self, id: String) {
        if let Some(index) = self
            .completed_signal_ids
            .iter()
            .position(|candidate| candidate == &id)
        {
            self.completed_signal_ids.remove(index);
        }
        self.completed_signal_ids.push(id);
        if self.completed_signal_ids.len() > MAX_COMPLETED_SIGNAL_IDS {
            self.completed_signal_ids.remove(0);
        }
    }

    fn parse_external_record(record: &SignalRecord) -> Result<SnapshotSignal> {
        if record.id.trim().is_empty() || record.signal_type.trim().is_empty() {
            return Err(Error::Source(
                "SQL Server external signal requires non-empty id and type".into(),
            ));
        }
        parse_snapshot_signal(&record.id, &record.signal_type, &record.data)
    }

    fn parse_row(row: &Row) -> Result<SnapshotSignal> {
        let id = signal_text(row, "id")?;
        let signal_type = signal_text(row, "type")?;
        let data = signal_text(row, "data")?;
        let value = serde_json::from_str::<serde_json::Value>(&data).map_err(|error| {
            Error::Source(format!(
                "SQL Server signal {id:?} has invalid JSON data: {error}"
            ))
        })?;
        parse_snapshot_signal(&id, &signal_type, &value)
    }

    fn handle_signal(&mut self, signal: SnapshotSignal, source: &SqlServerSource) -> Result<()> {
        match signal {
            SnapshotSignal::Execute {
                id,
                data_collections,
                additional_conditions,
            } => {
                if self.completed_signal_ids.contains(&id)
                    || self
                        .progress
                        .as_ref()
                        .is_some_and(|progress| progress.signal_id == id)
                {
                    return Ok(());
                }
                if self.progress.is_some() {
                    tracing::warn!(%id, "SQL Server incremental snapshot is already active; execute signal ignored");
                    return Ok(());
                }
                source.signal_capture().ok_or_else(|| {
                    Error::Configuration(
                        "SQL Server incremental snapshots require a CDC-enabled signal.data.collection so Rustium can emit open/close watermarks"
                            .into(),
                    )
                })?;
                self.progress = Some(IncrementalSnapshotProgress {
                    signal_id: id,
                    data_collections: source.expand_data_collections(&data_collections)?,
                    additional_conditions,
                    current_collection: 0,
                    last_key: None,
                    maximum_key: None,
                    chunk_sequence: 1,
                    paused: false,
                });
                self.state_dirty = true;
                Ok(())
            }
            SnapshotSignal::Stop {
                id,
                data_collections,
            } => {
                let Some(mut progress) = self.progress.take() else {
                    return Ok(());
                };
                if data_collections.is_empty() {
                    self.discard_window();
                    self.remember_completed(progress.signal_id);
                    self.state_dirty = true;
                    info!(%id, "SQL Server incremental snapshot stopped");
                    return Ok(());
                }
                let patterns = compile_collection_patterns(&data_collections)?;
                let original = progress.data_collections.clone();
                let current = original.get(progress.current_collection).cloned();
                let retained_before = original
                    .iter()
                    .take(progress.current_collection)
                    .filter(|collection| !collection_matches_any(collection, &patterns))
                    .count();
                progress
                    .data_collections
                    .retain(|collection| !collection_matches_any(collection, &patterns));
                if progress.data_collections.len() == original.len() {
                    self.progress = Some(progress);
                    return Ok(());
                }
                self.discard_window();
                if current
                    .as_ref()
                    .is_some_and(|collection| collection_matches_any(collection, &patterns))
                {
                    progress.last_key = None;
                    progress.maximum_key = None;
                }
                progress.current_collection = retained_before;
                if progress.current_collection >= progress.data_collections.len() {
                    self.remember_completed(progress.signal_id);
                } else {
                    self.progress = Some(progress);
                }
                self.state_dirty = true;
                info!(%id, "SQL Server incremental snapshot collections stopped");
                Ok(())
            }
            SnapshotSignal::Pause { id } => {
                if let Some(progress) = &mut self.progress
                    && !progress.paused
                {
                    progress.paused = true;
                    self.state_dirty = true;
                    info!(%id, "SQL Server incremental snapshot paused");
                }
                Ok(())
            }
            SnapshotSignal::Resume { id } => {
                if let Some(progress) = &mut self.progress
                    && progress.paused
                {
                    progress.paused = false;
                    self.state_dirty = true;
                    info!(%id, "SQL Server incremental snapshot resumed");
                }
                Ok(())
            }
            SnapshotSignal::Unsupported { id, signal_type } => {
                tracing::warn!(%id, %signal_type, "unsupported SQL Server runtime signal ignored");
                Ok(())
            }
        }
    }

    async fn start_next_chunk(&mut self, source: &SqlServerSource) -> Result<()> {
        if self.opening_window_id.is_some() || self.window.is_some() {
            return Ok(());
        }
        let Some(progress) = self.progress.clone() else {
            return Ok(());
        };
        if progress.paused {
            return Ok(());
        }
        let window_id = uuid::Uuid::new_v4().to_string();
        source
            .emit_incremental_watermark(&format!("{window_id}-open"), WINDOW_OPEN_SIGNAL)
            .await?;
        self.opening_window_id = Some(window_id);
        Ok(())
    }

    async fn open_window(&mut self, source: &SqlServerSource, window_id: String) -> Result<()> {
        let Some(progress) = self.progress.clone() else {
            return Ok(());
        };
        let collection = progress
            .data_collections
            .get(progress.current_collection)
            .cloned()
            .ok_or_else(|| {
                Error::State(format!(
                    "SQL Server incremental snapshot collection index {} is outside {} collections",
                    progress.current_collection,
                    progress.data_collections.len()
                ))
            })?;
        let capture = source.capture_for_collection(&collection).ok_or_else(|| {
            Error::Source(format!(
                "SQL Server incremental snapshot collection {collection:?} is not captured"
            ))
        })?;
        let chunk = source.read_incremental_chunk(&capture, &progress).await?;
        let row_count = chunk.rows.len();
        let last_key = chunk.rows.last().map(|(_, key)| key.clone());
        let remaining_keys = chunk
            .rows
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<HashSet<_>>();
        self.window = Some(SqlServerIncrementalWindow {
            id: window_id.clone(),
            collection,
            capture,
            rows: chunk.rows,
            remaining_keys,
            maximum_key: chunk.maximum_key,
            last_key,
            row_count,
            close_commit_lsn: None,
        });
        source
            .emit_incremental_watermark(&format!("{window_id}-close"), WINDOW_CLOSE_SIGNAL)
            .await?;
        Ok(())
    }

    async fn handle_internal_signal(
        &mut self,
        record: &SourceRecord,
        row: &Row,
        source: &SqlServerSource,
    ) -> Result<bool> {
        let signal_type = signal_text(row, "type")?;
        if !matches!(
            signal_type.as_str(),
            WINDOW_OPEN_SIGNAL | WINDOW_CLOSE_SIGNAL
        ) {
            return Ok(false);
        }
        let id = signal_text(row, "id")?;
        let opening_matches = self
            .opening_window_id
            .as_ref()
            .is_some_and(|window_id| id == format!("{window_id}-open"));
        let closing_matches = self
            .window
            .as_ref()
            .is_some_and(|window| id == format!("{}-close", window.id));
        if signal_type == WINDOW_OPEN_SIGNAL && opening_matches {
            let window_id = self.opening_window_id.take().ok_or_else(|| {
                Error::State("SQL Server incremental opening window disappeared".into())
            })?;
            self.open_window(source, window_id).await?;
        } else if signal_type == WINDOW_CLOSE_SIGNAL && closing_matches {
            let SourcePosition::SqlServer(position) = &record.position else {
                return Err(Error::State(
                    "SQL Server watermark record has a non-SQL Server position".into(),
                ));
            };
            let capture = self
                .window
                .as_ref()
                .map(|window| window.capture.clone())
                .ok_or_else(|| Error::State("SQL Server incremental window disappeared".into()))?;
            source.validate_incremental_schema(&capture).await?;
            if let Some(window) = &mut self.window {
                window.close_commit_lsn = Some(position.commit_lsn.clone());
            }
        }
        Ok(true)
    }

    fn observe_record(&mut self, record: &SourceRecord) -> Result<()> {
        let Some(window) = &mut self.window else {
            return Ok(());
        };
        if window.close_commit_lsn.is_some() {
            return Ok(());
        }
        let Some(event) = &record.event else {
            return Ok(());
        };
        let collection = event
            .source
            .schema
            .as_deref()
            .zip(event.source.table.as_deref())
            .map(|(schema, table)| format!("{schema}.{table}"));
        if collection.as_deref() != Some(window.collection.as_str()) {
            return Ok(());
        }
        for row in [event.before.as_ref(), event.after.as_ref()]
            .into_iter()
            .flatten()
        {
            let key = sqlserver_key_from_row(row, &window.capture.event_schema)?;
            window.remaining_keys.remove(&key);
        }
        Ok(())
    }

    fn closes_at(&self, record: &SourceRecord) -> bool {
        let SourcePosition::SqlServer(position) = &record.position else {
            return false;
        };
        record.boundary == RecordBoundary::TransactionCommit
            && self.window.as_ref().is_some_and(|window| {
                window.close_commit_lsn.as_deref() == Some(position.commit_lsn.as_str())
            })
    }

    async fn finish_window(
        &mut self,
        source: &SqlServerSource,
        base_position: &SourcePosition,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<SourcePosition> {
        let window = self.window.take().ok_or_else(|| {
            Error::State("SQL Server incremental snapshot has no pending window".into())
        })?;
        let progress = self.progress.clone().ok_or_else(|| {
            Error::State("SQL Server incremental snapshot window has no progress".into())
        })?;
        for (row, key) in window.rows {
            if !window.remaining_keys.contains(&key) {
                continue;
            }
            self.event_serial = self.event_serial.saturating_add(1);
            let position = sqlserver_incremental_position(base_position, self.event_serial)?;
            let mut attributes = BTreeMap::new();
            attributes.insert("rustium.snapshot.kind".into(), "incremental".into());
            let event = ChangeEvent {
                id: EventId::deterministic(
                    &source.connector_name,
                    source.database(),
                    &position,
                    &window.collection,
                    self.event_serial,
                ),
                source: SourceMetadata {
                    connector: "sqlserver".into(),
                    connector_name: source.connector_name.clone(),
                    database: source.database().into(),
                    schema: Some(window.capture.schema.clone()),
                    table: Some(window.capture.table.clone()),
                    snapshot: true,
                    version: CONNECTOR_VERSION.into(),
                    attributes,
                },
                position: position.clone(),
                transaction: None,
                operation: Operation::Read,
                before: None,
                after: Some(row),
                schema: window.capture.event_schema.clone(),
                source_time: None,
                observed_time: Utc::now(),
            };
            output
                .send(Ok(SourceRecord::data(event)))
                .await
                .map_err(|_| Error::Cancelled)?;
        }

        let mut next = progress;
        if next.maximum_key.is_none() {
            next.maximum_key.clone_from(&window.maximum_key);
        }
        next.last_key = window.last_key;
        let collection_complete = next.maximum_key.is_none()
            || next.last_key.as_ref() == next.maximum_key.as_ref()
            || window.row_count < source.config.incremental_snapshot_chunk_size;
        if collection_complete {
            next.current_collection = next.current_collection.saturating_add(1);
            next.last_key = None;
            next.maximum_key = None;
        }
        next.chunk_sequence = next.chunk_sequence.saturating_add(1);
        if next.current_collection >= next.data_collections.len() {
            self.remember_completed(next.signal_id);
            self.progress = None;
        } else {
            self.progress = Some(next);
        }

        self.event_serial = self.event_serial.saturating_add(1);
        let position = sqlserver_incremental_position(base_position, self.event_serial)?;
        output
            .send(Ok(SourceRecord {
                event: None,
                position: position.clone(),
                boundary: RecordBoundary::TransactionCommit,
                connector_state: Some(encode_connector_state(
                    self.progress.as_ref(),
                    &self.completed_signal_ids,
                )?),
                signal_acknowledgements: Vec::new(),
            }))
            .await
            .map_err(|_| Error::Cancelled)?;
        self.mark_checkpointed();
        Ok(position)
    }
}

struct IncrementalChunk {
    rows: Vec<(Row, Vec<SqlServerKeyValue>)>,
    maximum_key: Option<Vec<SqlServerKeyValue>>,
}

struct SqlServerIncrementalWindow {
    id: String,
    collection: String,
    capture: CaptureTable,
    rows: Vec<(Row, Vec<SqlServerKeyValue>)>,
    remaining_keys: HashSet<Vec<SqlServerKeyValue>>,
    maximum_key: Option<Vec<SqlServerKeyValue>>,
    last_key: Option<Vec<SqlServerKeyValue>>,
    row_count: usize,
    close_commit_lsn: Option<String>,
}

pub struct SqlServerSource {
    connector_name: String,
    config: SqlServerSourceConfig,
    snapshot: SnapshotConfig,
    captures: Vec<CaptureTable>,
    retry_policy: RetryPolicy,
}

impl SqlServerSource {
    #[must_use]
    pub fn new(
        connector_name: impl Into<String>,
        config: SqlServerSourceConfig,
        snapshot: SnapshotConfig,
    ) -> Self {
        Self {
            connector_name: connector_name.into(),
            config,
            snapshot,
            captures: Vec::new(),
            retry_policy: RetryPolicy::default(),
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    fn database(&self) -> &str {
        &self.config.databases[0]
    }

    async fn validate_source(&mut self) -> Result<()> {
        let mut client = connect(&self.config, self.database()).await?;
        let row = client
            .simple_query(
                "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int) AS major_version, \
                 CAST(is_cdc_enabled AS bit) AS is_cdc_enabled, \
                 CAST(snapshot_isolation_state AS int) AS snapshot_isolation_state \
                 FROM sys.databases WHERE name = DB_NAME()",
            )
            .await
            .map_err(sqlserver_error)?
            .into_row()
            .await
            .map_err(sqlserver_error)?
            .ok_or_else(|| Error::Source("SQL Server did not return database metadata".into()))?;
        let major_version = required::<i32>(&row, "major_version")?;
        let cdc_enabled = required::<bool>(&row, "is_cdc_enabled")?;
        let snapshot_isolation_state = required::<i32>(&row, "snapshot_isolation_state")?;
        if major_version < 14 {
            return Err(Error::Configuration(format!(
                "SQL Server 2017 or newer is required; major version is {major_version}"
            )));
        }
        if !cdc_enabled {
            return Err(Error::Configuration(format!(
                "CDC is not enabled for SQL Server database {:?}",
                self.database()
            )));
        }
        if self.config.snapshot_isolation_mode == "snapshot" && snapshot_isolation_state != 1 {
            return Err(Error::Configuration(
                "snapshot.isolation.mode=snapshot requires ALLOW_SNAPSHOT_ISOLATION ON".into(),
            ));
        }

        let captures = discover_captures(&mut client, &self.config, &self.connector_name).await?;
        let signal_table = signal_table_key(&self.config);
        if captures
            .iter()
            .all(|capture| signal_table.as_ref() == Some(&capture.key()))
        {
            return Err(Error::Configuration(
                "the SQL Server CDC capture instances and table filters select no tables".into(),
            ));
        }
        if let Some(signal_table) = signal_table {
            let capture = captures
                .iter()
                .find(|capture| capture.key() == signal_table)
                .ok_or_else(|| {
                    Error::Configuration(format!(
                        "SQL Server signal table {}.{} is not CDC-enabled",
                        signal_table.0, signal_table.1
                    ))
                })?;
            validate_signal_schema(capture)?;
            validate_signal_insert_permission(&mut client, capture).await?;
        }
        current_max_lsn(&mut client).await?;
        client.close().await.map_err(sqlserver_error)?;
        self.captures = captures;
        Ok(())
    }

    async fn run_snapshot(
        &self,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<Vec<u8>> {
        let mut client = connect(&self.config, self.database()).await?;
        client
            .simple_query(snapshot_begin_sql(&self.config.snapshot_isolation_mode))
            .await
            .map_err(sqlserver_error)?
            .into_results()
            .await
            .map_err(sqlserver_error)?;
        let anchor = current_max_lsn(&mut client).await?;
        let mut ordinal = 0_u64;
        let mut captures = self.captures.clone();
        let signal_table = signal_table_key(&self.config);
        captures.retain(|capture| signal_table.as_ref() != Some(&capture.key()));
        captures.sort_by_key(CaptureTable::key);
        for capture in &captures {
            snapshot_table(
                &mut client,
                self.database(),
                &self.connector_name,
                capture,
                &anchor,
                &mut ordinal,
                output,
            )
            .await?;
        }
        client
            .simple_query("COMMIT TRANSACTION")
            .await
            .map_err(sqlserver_error)?
            .into_results()
            .await
            .map_err(sqlserver_error)?;
        ordinal += 1;
        output
            .send(Ok(SourceRecord {
                event: None,
                position: sqlserver_position(
                    self.database(),
                    &anchor,
                    &zero_lsn_bytes(),
                    ordinal,
                    true,
                ),
                boundary: RecordBoundary::SnapshotComplete,
                connector_state: None,
                signal_acknowledgements: Vec::new(),
            }))
            .await
            .map_err(|_| Error::Cancelled)?;
        client.close().await.map_err(sqlserver_error)?;
        Ok(anchor)
    }

    async fn current_anchor(&self) -> Result<Vec<u8>> {
        let mut client = connect(&self.config, self.database()).await?;
        let anchor = current_max_lsn(&mut client).await?;
        client.close().await.map_err(sqlserver_error)?;
        Ok(anchor)
    }

    fn signal_capture(&self) -> Option<CaptureTable> {
        let signal = signal_table_key(&self.config)?;
        self.captures
            .iter()
            .find(|capture| capture.key() == signal)
            .cloned()
    }

    async fn emit_incremental_watermark(&self, id: &str, signal_type: &str) -> Result<()> {
        let capture = self.signal_capture().ok_or_else(|| {
            Error::Configuration(
                "SQL Server incremental snapshots require a CDC-enabled signal.data.collection"
                    .into(),
            )
        })?;
        let table = format!(
            "{}.{}",
            quote_identifier(&capture.schema),
            quote_identifier(&capture.table)
        );
        let data = "{}";
        let mut client = connect(&self.config, self.database()).await?;
        client
            .execute(
                format!("INSERT INTO {table} ([id], [type], [data]) VALUES (@P1, @P2, @P3)"),
                &[&id, &signal_type, &data],
            )
            .await
            .map_err(sqlserver_error)?;
        client.close().await.map_err(sqlserver_error)
    }

    async fn validate_incremental_schema(&self, capture: &CaptureTable) -> Result<()> {
        let mut client = connect(&self.config, self.database()).await?;
        let fields = discover_fields(&mut client, capture.source_object_id).await?;
        client.close().await.map_err(sqlserver_error)?;
        if fields != capture.event_schema.fields {
            return Err(Error::Source(format!(
                "SQL Server schema changed while incremental snapshot window for {}.{} was open",
                capture.schema, capture.table
            )));
        }
        Ok(())
    }

    fn capture_for_collection(&self, collection: &str) -> Option<CaptureTable> {
        self.captures
            .iter()
            .find(|capture| format!("{}.{}", capture.schema, capture.table) == collection)
            .cloned()
    }

    fn expand_data_collections(&self, patterns: &[String]) -> Result<Vec<String>> {
        let patterns = compile_collection_patterns(patterns)?;
        let signal_table = signal_table_key(&self.config);
        let mut collections = self
            .captures
            .iter()
            .filter(|capture| signal_table.as_ref() != Some(&capture.key()))
            .filter_map(|capture| {
                let short = format!("{}.{}", capture.schema, capture.table);
                let qualified = format!("{}.{}", self.database(), short);
                patterns
                    .iter()
                    .any(|pattern| pattern.is_match(&short) || pattern.is_match(&qualified))
                    .then_some(short)
            })
            .collect::<Vec<_>>();
        collections.sort();
        collections.dedup();
        if collections.is_empty() {
            return Err(Error::Source(
                "SQL Server incremental snapshot patterns select no captured tables".into(),
            ));
        }
        Ok(collections)
    }

    async fn read_incremental_chunk(
        &self,
        capture: &CaptureTable,
        progress: &IncrementalSnapshotProgress,
    ) -> Result<IncrementalChunk> {
        let key_fields = capture
            .event_schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.primary_key)
            .collect::<Vec<_>>();
        if key_fields.is_empty() {
            return Err(Error::Source(format!(
                "SQL Server incremental snapshot table {}.{} requires a primary key",
                capture.schema, capture.table
            )));
        }
        let key_indices = key_fields
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let key_columns = key_fields
            .iter()
            .map(|(_, field)| format!("ct.{}", quote_identifier(&field.name)))
            .collect::<Vec<_>>();
        let collection = format!("{}.{}", capture.schema, capture.table);
        let qualified_collection = format!("{}.{}", self.database(), collection);
        let condition = progress
            .additional_conditions
            .iter()
            .find_map(|(pattern, filter)| {
                regex::Regex::new(&format!(r"^(?:{pattern})$"))
                    .ok()
                    .and_then(|pattern| {
                        (pattern.is_match(&collection) || pattern.is_match(&qualified_collection))
                            .then_some(filter)
                    })
            });
        let table = format!(
            "{}.{}",
            quote_identifier(&capture.schema),
            quote_identifier(&capture.table)
        );
        let mut client = connect(&self.config, self.database()).await?;
        let current_fields = discover_fields(&mut client, capture.source_object_id).await?;
        if current_fields != capture.event_schema.fields {
            return Err(Error::Source(format!(
                "SQL Server schema changed before incremental snapshot query for {}.{}",
                capture.schema, capture.table
            )));
        }
        let maximum_key = match &progress.maximum_key {
            Some(key) => Some(key.clone()),
            None => {
                let projections = key_fields
                    .iter()
                    .enumerate()
                    .map(|(projection_index, (_, field))| {
                        change_value_expression(projection_index, field)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let where_clause = condition
                    .map(|condition| format!(" WHERE ({condition})"))
                    .unwrap_or_default();
                let ordering = key_columns
                    .iter()
                    .map(|column| format!("{column} DESC"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let row = client
                    .simple_query(format!(
                        "SELECT TOP (1) {projections} FROM {table} AS ct{where_clause} ORDER BY {ordering}"
                    ))
                    .await
                    .map_err(sqlserver_error)?
                    .into_row()
                    .await
                    .map_err(sqlserver_error)?;
                row.map(|row| sqlserver_key_from_projection(&row, &key_fields))
                    .transpose()?
            }
        };
        let Some(maximum_key) = maximum_key else {
            client.close().await.map_err(sqlserver_error)?;
            return Ok(IncrementalChunk {
                rows: Vec::new(),
                maximum_key: None,
            });
        };

        validate_key_width(&key_columns, &maximum_key, "maximum")?;
        let mut predicates = Vec::new();
        if let Some(condition) = condition {
            predicates.push(format!("({condition})"));
        }
        if let Some(last_key) = &progress.last_key {
            validate_key_width(&key_columns, last_key, "last")?;
            predicates.push(sqlserver_key_predicate(&key_columns, last_key, true)?);
        }
        predicates.push(sqlserver_key_predicate(&key_columns, &maximum_key, false)?);
        let projections = capture
            .event_schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| change_value_expression(index, field))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT TOP ({}) {projections} FROM {table} AS ct WHERE {} ORDER BY {}",
            self.config.incremental_snapshot_chunk_size,
            predicates.join(" AND "),
            key_columns.join(", ")
        );
        let rows = client
            .simple_query(query)
            .await
            .map_err(sqlserver_error)?
            .into_first_result()
            .await
            .map_err(sqlserver_error)?;
        let rows = rows
            .iter()
            .map(|row| {
                let converted = convert_tds_row(row, &capture.event_schema)?;
                let key = key_indices
                    .iter()
                    .map(|index| {
                        let field = &capture.event_schema.fields[*index];
                        converted
                            .get(&field.name)
                            .ok_or_else(|| {
                                Error::Source(format!(
                                    "SQL Server incremental row is missing key {:?}",
                                    field.name
                                ))
                            })
                            .and_then(sqlserver_key_from_data_value)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((converted, key))
            })
            .collect::<Result<Vec<_>>>()?;
        client.close().await.map_err(sqlserver_error)?;
        Ok(IncrementalChunk {
            rows,
            maximum_key: Some(maximum_key),
        })
    }
}

#[async_trait]
impl SourceConnector for SqlServerSource {
    fn source_type(&self) -> &'static str {
        "sqlserver"
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
        let checkpoint_position = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.source_position.clone());
        if checkpoint_position
            .as_ref()
            .is_some_and(|position| !matches!(position, SourcePosition::SqlServer(_)))
        {
            return Err(Error::State(
                "SQL Server connector cannot resume from another source checkpoint".into(),
            ));
        }

        let checkpoint_has_connector_state = !snapshot_needed
            && checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.connector_state.as_ref())
                .is_some();
        let (incremental_progress, completed_signal_ids) = if !snapshot_needed {
            checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.connector_state.as_ref())
                .map(decode_connector_state)
                .transpose()?
                .unwrap_or_default()
        } else {
            (None, Vec::new())
        };

        let (cursor, resume_position) = if snapshot_needed {
            (
                CdcCursor::at_snapshot(self.run_snapshot(&context.output).await?),
                None,
            )
        } else if let Some(SourcePosition::SqlServer(position)) = &checkpoint_position {
            if position.database != self.database() {
                return Err(Error::State(format!(
                    "SQL Server checkpoint belongs to database {:?}, not {:?}",
                    position.database,
                    self.database()
                )));
            }
            CdcCursor::from_position(position, checkpoint_has_connector_state)?
        } else {
            (CdcCursor::at_snapshot(self.current_anchor().await?), None)
        };

        let mut client = connect(&self.config, self.database()).await?;
        validate_retention(&mut client, &self.captures, &cursor.commit_lsn).await?;
        let mut state = StreamingState::new(cursor, resume_position);
        let mut incremental =
            SqlServerIncrementalSnapshot::new(incremental_progress, completed_signal_ids);
        let mut last_safe_position = checkpoint_position
            .as_ref()
            .filter(|position| {
                matches!(position, SourcePosition::SqlServer(position) if !position.snapshot)
            })
            .cloned()
            .unwrap_or_else(|| state.safe_position(self.database()));
        let mut interval = tokio::time::interval(self.config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut heartbeat = heartbeat_timer(self.config.heartbeat_interval);
        let mut file_signal_poll = file_signal_timer(&self.config);
        let mut incremental_tick = tokio::time::interval(std::time::Duration::from_millis(1));
        incremental_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat_connection = if self.config.heartbeat_interval.is_zero()
            || self.config.heartbeat_action_query.is_none()
        {
            None
        } else {
            Some(connect(&self.config, self.database()).await?)
        };
        info!(
            connector = %self.connector_name,
            database = %self.database(),
            commit_lsn = %format_lsn(&state.cursor.commit_lsn),
            max_retries = self.retry_policy.max_retries,
            initial_retry_delay_ms = self.retry_policy.initial_delay.as_millis(),
            max_retry_delay_ms = self.retry_policy.max_delay.as_millis(),
            "SQL Server CDC streaming started"
        );

        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    client.close().await.map_err(sqlserver_error)?;
                    if let Some(connection) = heartbeat_connection {
                        connection.close().await.map_err(sqlserver_error)?;
                    }
                    return Ok(());
                }
                changed = context.acknowledged.changed() => {
                    if changed.is_err() {
                        return Err(Error::Cancelled);
                    }
                }
                _ = incremental_tick.tick(),
                    if incremental.is_active()
                        && state.transaction.is_none()
                        && state.cursor.commit_complete() => {
                    incremental.start_next_chunk(self).await?;
                }
                () = next_file_signal_poll(&mut file_signal_poll),
                    if signal_channel_enabled(&self.config, "file")
                        && state.transaction.is_none()
                        && state.cursor.commit_complete() => {
                    for line in crate::file_signal::read_and_clear(&self.config.signal_file).await? {
                        let record = match serde_json::from_str::<SignalRecord>(&line) {
                            Ok(record) => record,
                            Err(error) => {
                                tracing::warn!(%error, "invalid SQL Server file signal ignored");
                                continue;
                            }
                        };
                        let signal = match SqlServerIncrementalSnapshot::parse_external_record(&record) {
                            Ok(signal) => signal,
                            Err(error) => {
                                tracing::warn!(%error, "invalid SQL Server file signal ignored");
                                continue;
                            }
                        };
                        incremental.handle_signal(signal, self)?;
                    }
                    if incremental.state_dirty() {
                        emit_incremental_checkpoint(
                            &context.output,
                            incremental.progress(),
                            incremental.completed_signal_ids(),
                            &last_safe_position,
                            None,
                        ).await?;
                        incremental.mark_checkpointed();
                    }
                }
                delivery = context.signals.recv(),
                    if (signal_channel_enabled(&self.config, "in-process")
                        || signal_channel_enabled(&self.config, "kafka"))
                        && state.transaction.is_none()
                        && state.cursor.commit_complete() => {
                    let delivery = delivery.ok_or_else(|| {
                        Error::Source("SQL Server runtime signal channel closed".into())
                    })?;
                    let signal = match SqlServerIncrementalSnapshot::parse_external_record(delivery.record()) {
                        Ok(signal) => signal,
                        Err(error) => {
                            tracing::warn!(%error, "invalid SQL Server runtime signal ignored");
                            delivery.acknowledge();
                            continue;
                        }
                    };
                    incremental.handle_signal(signal, self)?;
                    emit_incremental_checkpoint(
                        &context.output,
                        incremental.progress(),
                        incremental.completed_signal_ids(),
                        &last_safe_position,
                        delivery.into_acknowledgement(),
                    ).await?;
                    incremental.mark_checkpointed();
                }
                _ = interval.tick() => {
                    let Some(records) = poll_change_batch_with_retry(
                        self,
                        &mut client,
                        &mut state,
                        &context.cancellation,
                    ).await? else {
                        continue;
                    };
                    for mut record in records {
                        if let Some(signal_row) = source_signal_row(&record, &self.config) {
                            if let Some(signal_row) = signal_row {
                                if incremental
                                    .handle_internal_signal(&record, signal_row, self)
                                    .await?
                                {
                                    continue;
                                }
                                if signal_channel_enabled(&self.config, "source") {
                                    let signal =
                                        SqlServerIncrementalSnapshot::parse_row(signal_row)?;
                                    incremental.handle_signal(signal, self)?;
                                }
                            }
                            continue;
                        }
                        incremental.observe_record(&record)?;
                        if incremental.closes_at(&record) {
                            let base = record.position.clone();
                            last_safe_position = incremental
                                .finish_window(self, &base, &context.output)
                                .await?;
                            continue;
                        }
                        if record.boundary == RecordBoundary::TransactionCommit
                            && incremental.state_dirty()
                        {
                            record.connector_state = Some(encode_connector_state(
                                incremental.progress(),
                                incremental.completed_signal_ids(),
                            )?);
                        }
                        let checkpointed = record.connector_state.is_some();
                        let position = record.position.clone();
                        context.output.send(Ok(record)).await.map_err(|_| Error::Cancelled)?;
                        if position.is_after(&last_safe_position)
                            || position == last_safe_position
                        {
                            last_safe_position = position;
                        }
                        if checkpointed {
                            incremental.mark_checkpointed();
                        }
                    }
                }
                () = next_heartbeat(&mut heartbeat) => {
                    if !state.cursor.commit_complete() {
                        continue;
                    }
                    if let Some(query) = self.config.heartbeat_action_query.clone() {
                        let connection = heartbeat_connection.take().ok_or_else(|| {
                            Error::Source("SQL Server heartbeat action connection is unavailable".into())
                        })?;
                        heartbeat_connection = Some(execute_heartbeat_action(connection, query).await?);
                    }
                    context.output.send(Ok(sqlserver_heartbeat_record(
                        &self.connector_name,
                        self.database(),
                        last_safe_position.clone(),
                    ))).await.map_err(|_| Error::Cancelled)?;
                }
            }
        }
    }
}

async fn poll_change_batch_with_retry(
    source: &SqlServerSource,
    client: &mut SqlClient,
    state: &mut StreamingState,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<Option<Vec<SourceRecord>>> {
    let mut retries = 0_u64;
    let mut delay = source.retry_policy.initial_delay;
    let mut reconnect = false;
    loop {
        if reconnect {
            match connect(&source.config, source.database()).await {
                Ok(reconnected) => {
                    *client = reconnected;
                    info!(
                        connector = %source.connector_name,
                        database = %source.database(),
                        retries,
                        "SQL Server CDC polling connection recovered"
                    );
                }
                Err(error @ Error::RetryableSource(_)) => {
                    if !wait_for_sqlserver_retry(source, retries, &mut delay, &error, cancellation)
                        .await?
                    {
                        return Err(error);
                    }
                    retries += 1;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        match poll_change_batch_once(source, client, state).await {
            Ok(records) => return Ok(records),
            Err(error @ Error::RetryableSource(_)) => {
                if !wait_for_sqlserver_retry(source, retries, &mut delay, &error, cancellation)
                    .await?
                {
                    return Err(error);
                }
                retries += 1;
                reconnect = true;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_sqlserver_retry(
    source: &SqlServerSource,
    retries: u64,
    delay: &mut std::time::Duration,
    error: &Error,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<bool> {
    if !sqlserver_retry_allowed(&source.retry_policy, retries) {
        return Ok(false);
    }
    warn!(
        connector = %source.connector_name,
        database = %source.database(),
        retry = retries + 1,
        max_retries = source.retry_policy.max_retries,
        delay_ms = delay.as_millis(),
        %error,
        "retryable SQL Server CDC polling failure; reconnecting"
    );
    tokio::select! {
        () = cancellation.cancelled() => return Err(Error::Cancelled),
        () = tokio::time::sleep(*delay) => {}
    }
    *delay = delay.saturating_mul(2).min(source.retry_policy.max_delay);
    Ok(true)
}

fn sqlserver_retry_allowed(policy: &RetryPolicy, retries: u64) -> bool {
    policy.max_retries < 0 || retries < policy.max_retries as u64
}

async fn poll_change_batch_once(
    source: &SqlServerSource,
    client: &mut SqlClient,
    state: &mut StreamingState,
) -> Result<Option<Vec<SourceRecord>>> {
    let max_lsn = current_max_lsn(client).await?;
    if max_lsn < state.cursor.commit_lsn
        || (max_lsn == state.cursor.commit_lsn && state.cursor.commit_complete())
    {
        return Ok(None);
    }
    validate_retention(client, &source.captures, &state.cursor.commit_lsn).await?;
    read_change_batch(
        client,
        source.database(),
        &source.connector_name,
        &source.captures,
        state,
        &max_lsn,
        source.config.streaming_fetch_size,
    )
    .await
    .map(Some)
}

fn heartbeat_timer(interval: std::time::Duration) -> Option<tokio::time::Interval> {
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

fn signal_channel_enabled(config: &SqlServerSourceConfig, channel: &str) -> bool {
    config
        .signal_enabled_channels
        .iter()
        .any(|enabled| enabled == channel)
}

fn signal_table_key(config: &SqlServerSourceConfig) -> Option<(String, String)> {
    let parts = config
        .signal_data_collection
        .as_deref()?
        .split('.')
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [schema, table] | [_, schema, table] => Some(((*schema).into(), (*table).into())),
        _ => None,
    }
}

fn validate_signal_schema(capture: &CaptureTable) -> Result<()> {
    let expected = ["id", "type", "data"];
    if capture.event_schema.fields.len() != expected.len()
        || capture
            .event_schema
            .fields
            .iter()
            .zip(expected)
            .any(|(field, expected)| {
                field.name != expected
                    || !matches!(
                        base_type(&field.type_name),
                        "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext"
                    )
            })
    {
        return Err(Error::Configuration(format!(
            "SQL Server signal table {}.{} must contain exactly text-compatible id, type, and data columns in that order",
            capture.schema, capture.table
        )));
    }
    let minimum_lengths = [
        uuid::Uuid::nil().to_string().len() + "-close".len(),
        WINDOW_OPEN_SIGNAL.len().max(WINDOW_CLOSE_SIGNAL.len()),
        2,
    ];
    for (field, minimum) in capture.event_schema.fields.iter().zip(minimum_lengths) {
        if signal_text_capacity(&field.type_name).is_some_and(|capacity| capacity < minimum) {
            return Err(Error::Configuration(format!(
                "SQL Server signal table column {:?} must hold at least {minimum} characters for incremental snapshot watermarks; found {}",
                field.name, field.type_name
            )));
        }
    }
    Ok(())
}

fn signal_text_capacity(type_name: &str) -> Option<usize> {
    let normalized = type_name.trim().to_ascii_lowercase();
    if matches!(base_type(&normalized), "text" | "ntext") || normalized.ends_with("(max)") {
        return None;
    }
    normalized
        .split_once('(')
        .and_then(|(_, length)| length.strip_suffix(')'))
        .and_then(|length| length.parse().ok())
}

async fn validate_signal_insert_permission(
    client: &mut SqlClient,
    capture: &CaptureTable,
) -> Result<()> {
    let object_name = format!("{}.{}", capture.schema, capture.table);
    let row = client
        .query(
            "SELECT CAST(HAS_PERMS_BY_NAME(@P1, 'OBJECT', 'INSERT') AS int) AS can_insert",
            &[&object_name],
        )
        .await
        .map_err(sqlserver_error)?
        .into_row()
        .await
        .map_err(sqlserver_error)?
        .ok_or_else(|| {
            Error::Source("SQL Server did not return signal-table permissions".into())
        })?;
    if required::<i32>(&row, "can_insert")? != 1 {
        return Err(Error::Configuration(format!(
            "SQL Server connector user requires INSERT on signal table {}.{} for incremental snapshot watermarks",
            capture.schema, capture.table
        )));
    }
    Ok(())
}

fn source_signal_row<'a>(
    record: &'a SourceRecord,
    config: &SqlServerSourceConfig,
) -> Option<Option<&'a Row>> {
    let signal = signal_table_key(config)?;
    let event = record.event.as_ref()?;
    if event.source.schema.as_deref() != Some(signal.0.as_str())
        || event.source.table.as_deref() != Some(signal.1.as_str())
    {
        return None;
    }
    Some(
        (event.operation == Operation::Create)
            .then_some(event.after.as_ref())
            .flatten(),
    )
}

fn file_signal_timer(config: &SqlServerSourceConfig) -> Option<tokio::time::Interval> {
    if !signal_channel_enabled(config, "file") {
        return None;
    }
    let mut timer = tokio::time::interval_at(
        tokio::time::Instant::now() + config.signal_poll_interval,
        config.signal_poll_interval,
    );
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    Some(timer)
}

async fn next_file_signal_poll(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn signal_text(row: &Row, name: &str) -> Result<String> {
    match row.get(name) {
        Some(DataValue::String(value)) => Ok(value.clone()),
        Some(DataValue::Json(value)) => Ok(value.to_string()),
        Some(DataValue::Bytes(value)) => String::from_utf8(value.clone()).map_err(|error| {
            Error::Source(format!(
                "SQL Server signal column {name} is not UTF-8: {error}"
            ))
        }),
        Some(value) => Ok(value.to_json("__rustium_unavailable").to_string()),
        None => Err(Error::Source(format!(
            "SQL Server signal table is missing column {name:?}"
        ))),
    }
}

fn parse_snapshot_signal(
    id: &str,
    signal_type: &str,
    data: &serde_json::Value,
) -> Result<SnapshotSignal> {
    match signal_type {
        "execute-snapshot" => {
            let snapshot_type = data.get("type").and_then(serde_json::Value::as_str);
            if snapshot_type.is_some_and(|kind| !kind.eq_ignore_ascii_case("incremental")) {
                return Err(Error::Source(format!(
                    "SQL Server execute-snapshot signal {id:?} supports only type=incremental"
                )));
            }
            let collections = data_collections(data);
            if collections.is_empty() {
                return Err(Error::Source(format!(
                    "SQL Server execute-snapshot signal {id:?} has no data-collections"
                )));
            }
            compile_collection_patterns(&collections)?;
            let mut additional_conditions = BTreeMap::new();
            if let Some(values) = data
                .get("additional-conditions")
                .and_then(serde_json::Value::as_array)
            {
                for value in values {
                    let collection = value
                        .get("data-collection")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            Error::Source(format!(
                                "SQL Server execute-snapshot signal {id:?} has an invalid additional-condition"
                            ))
                        })?;
                    let filter = value
                        .get("filter")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            Error::Source(format!(
                                "SQL Server execute-snapshot signal {id:?} has an invalid additional-condition"
                            ))
                        })?;
                    compile_collection_patterns(&[collection.into()])?;
                    additional_conditions.insert(collection.into(), filter.into());
                }
            }
            Ok(SnapshotSignal::Execute {
                id: id.into(),
                data_collections: collections,
                additional_conditions,
            })
        }
        "stop-snapshot" => Ok(SnapshotSignal::Stop {
            id: id.into(),
            data_collections: data_collections(data),
        }),
        "pause-snapshot" => Ok(SnapshotSignal::Pause { id: id.into() }),
        "resume-snapshot" => Ok(SnapshotSignal::Resume { id: id.into() }),
        other => Ok(SnapshotSignal::Unsupported {
            id: id.into(),
            signal_type: other.into(),
        }),
    }
}

fn data_collections(data: &serde_json::Value) -> Vec<String> {
    data.get("data-collections")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn compile_collection_patterns(patterns: &[String]) -> Result<Vec<regex::Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            regex::Regex::new(&format!(r"^(?:{pattern})$")).map_err(|error| {
                Error::Source(format!(
                    "invalid SQL Server snapshot collection pattern {pattern:?}: {error}"
                ))
            })
        })
        .collect()
}

fn collection_matches_any(collection: &str, patterns: &[regex::Regex]) -> bool {
    patterns.iter().any(|pattern| pattern.is_match(collection))
}

fn validate_key_width(columns: &[String], key: &[SqlServerKeyValue], boundary: &str) -> Result<()> {
    if columns.len() != key.len() {
        return Err(Error::State(format!(
            "SQL Server incremental snapshot {boundary} key has {} values for {} primary-key columns",
            key.len(),
            columns.len()
        )));
    }
    Ok(())
}

fn sqlserver_key_from_projection(
    row: &TdsRow,
    key_fields: &[(usize, &FieldSchema)],
) -> Result<Vec<SqlServerKeyValue>> {
    key_fields
        .iter()
        .enumerate()
        .map(|(projection_index, (_, field))| {
            convert_tds_value(row, projection_index, &field.type_name)
                .and_then(|value| sqlserver_key_from_data_value(&value))
        })
        .collect()
}

fn sqlserver_key_from_row(row: &Row, schema: &EventSchema) -> Result<Vec<SqlServerKeyValue>> {
    let key = schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .map(|field| {
            row.get(&field.name)
                .ok_or_else(|| {
                    Error::Source(format!(
                        "SQL Server CDC row is missing primary-key column {:?}",
                        field.name
                    ))
                })
                .and_then(sqlserver_key_from_data_value)
        })
        .collect::<Result<Vec<_>>>()?;
    if key.is_empty() {
        return Err(Error::Source(
            "SQL Server incremental snapshot table has no primary-key fields".into(),
        ));
    }
    Ok(key)
}

fn sqlserver_key_from_data_value(value: &DataValue) -> Result<SqlServerKeyValue> {
    match value {
        DataValue::Boolean(value) => Ok(SqlServerKeyValue::Boolean(*value)),
        DataValue::Int32(value) => Ok(SqlServerKeyValue::Int32(*value)),
        DataValue::Int64(value) => Ok(SqlServerKeyValue::Int64(*value)),
        DataValue::UInt64(value) => Ok(SqlServerKeyValue::UInt64(*value)),
        DataValue::Float64(value) if value.is_finite() => {
            Ok(SqlServerKeyValue::Float64(value.to_bits()))
        }
        DataValue::Decimal(value) => Ok(SqlServerKeyValue::Decimal(value.clone())),
        DataValue::String(value) => Ok(SqlServerKeyValue::String(value.clone())),
        DataValue::Bytes(value) => Ok(SqlServerKeyValue::Bytes(value.clone())),
        DataValue::Date(value) => Ok(SqlServerKeyValue::Date(value.clone())),
        DataValue::Time(value) => Ok(SqlServerKeyValue::Time(value.clone())),
        DataValue::Timestamp(value) => Ok(SqlServerKeyValue::Timestamp(value.clone())),
        DataValue::Uuid(value) => Ok(SqlServerKeyValue::Uuid(*value)),
        DataValue::Null => Err(Error::Source(
            "SQL Server incremental snapshot primary key contains NULL".into(),
        )),
        other => Err(Error::Source(format!(
            "SQL Server incremental snapshot primary key has unsupported value {other:?}"
        ))),
    }
}

fn sqlserver_key_literal(value: &SqlServerKeyValue) -> Result<String> {
    match value {
        SqlServerKeyValue::Boolean(value) => Ok(u8::from(*value).to_string()),
        SqlServerKeyValue::Int32(value) => Ok(value.to_string()),
        SqlServerKeyValue::Int64(value) => Ok(value.to_string()),
        SqlServerKeyValue::UInt64(value) => Ok(value.to_string()),
        SqlServerKeyValue::Float64(bits) => {
            let value = f64::from_bits(*bits);
            if value.is_finite() {
                Ok(value.to_string())
            } else {
                Err(Error::State(
                    "SQL Server incremental snapshot key contains a non-finite float".into(),
                ))
            }
        }
        SqlServerKeyValue::Decimal(value) => {
            let unsigned = value
                .strip_prefix('-')
                .or_else(|| value.strip_prefix('+'))
                .unwrap_or(value);
            let mut parts = unsigned.split('.');
            let whole = parts.next().unwrap_or_default();
            let fraction = parts.next();
            if whole.is_empty()
                || !whole.chars().all(|character| character.is_ascii_digit())
                || fraction.is_some_and(|fraction| {
                    fraction.is_empty()
                        || !fraction.chars().all(|character| character.is_ascii_digit())
                })
                || parts.next().is_some()
            {
                return Err(Error::State(format!(
                    "SQL Server incremental snapshot has invalid decimal key {value:?}"
                )));
            }
            Ok(value.clone())
        }
        SqlServerKeyValue::Bytes(value) => Ok(format!("0x{}", hex::encode(value))),
        SqlServerKeyValue::String(value)
        | SqlServerKeyValue::Date(value)
        | SqlServerKeyValue::Time(value)
        | SqlServerKeyValue::Timestamp(value) => Ok(format!("N'{}'", quote_literal(value))),
        SqlServerKeyValue::Uuid(value) => Ok(format!("N'{value}'")),
    }
}

fn sqlserver_key_predicate(
    columns: &[String],
    key: &[SqlServerKeyValue],
    greater: bool,
) -> Result<String> {
    validate_key_width(columns, key, if greater { "last" } else { "maximum" })?;
    let literals = key
        .iter()
        .map(sqlserver_key_literal)
        .collect::<Result<Vec<_>>>()?;
    let mut terms = Vec::new();
    for index in 0..columns.len() {
        let mut comparisons = (0..index)
            .map(|prefix| format!("{} = {}", columns[prefix], literals[prefix]))
            .collect::<Vec<_>>();
        comparisons.push(format!(
            "{} {} {}",
            columns[index],
            if greater { ">" } else { "<" },
            literals[index]
        ));
        terms.push(format!("({})", comparisons.join(" AND ")));
    }
    if !greater {
        terms.push(format!(
            "({})",
            columns
                .iter()
                .zip(&literals)
                .map(|(column, literal)| format!("{column} = {literal}"))
                .collect::<Vec<_>>()
                .join(" AND ")
        ));
    }
    Ok(format!("({})", terms.join(" OR ")))
}

fn sqlserver_incremental_position(
    base: &SourcePosition,
    event_serial: u64,
) -> Result<SourcePosition> {
    let SourcePosition::SqlServer(position) = base else {
        return Err(Error::State(
            "SQL Server incremental snapshot requires a SQL Server position".into(),
        ));
    };
    Ok(SourcePosition::SqlServer(SqlServerPosition {
        database: position.database.clone(),
        commit_lsn: position.commit_lsn.clone(),
        change_lsn: position.change_lsn.clone(),
        event_serial: position.event_serial.saturating_add(event_serial),
        snapshot: false,
    }))
}

async fn emit_incremental_checkpoint(
    output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    progress: Option<&IncrementalSnapshotProgress>,
    completed_signal_ids: &[String],
    position: &SourcePosition,
    acknowledgement: Option<SignalAcknowledgement>,
) -> Result<()> {
    output
        .send(Ok(SourceRecord {
            event: None,
            position: position.clone(),
            boundary: RecordBoundary::Heartbeat,
            connector_state: Some(encode_connector_state(progress, completed_signal_ids)?),
            signal_acknowledgements: acknowledgement.into_iter().collect(),
        }))
        .await
        .map_err(|_| Error::Cancelled)
}

async fn execute_heartbeat_action(mut connection: SqlClient, query: String) -> Result<SqlClient> {
    connection
        .simple_query(&query)
        .await
        .map_err(sqlserver_error)?
        .into_results()
        .await
        .map_err(sqlserver_error)?;
    Ok(connection)
}

fn sqlserver_heartbeat_record(
    connector_name: &str,
    database: &str,
    position: SourcePosition,
) -> SourceRecord {
    let observed_time = Utc::now();
    let mut attributes = BTreeMap::new();
    attributes.insert("rustium.heartbeat".into(), true.into());
    let mut after = Row::new();
    after.insert(
        "ts_ms".into(),
        DataValue::Int64(observed_time.timestamp_millis()),
    );
    let event = ChangeEvent {
        id: EventId::deterministic(
            connector_name,
            database,
            &position,
            "__heartbeat",
            u64::try_from(observed_time.timestamp_micros()).unwrap_or_default(),
        ),
        source: SourceMetadata {
            connector: "sqlserver".into(),
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
    cursor: CdcCursor,
    transaction: Option<ActiveTransaction>,
    pending_updates: HashMap<(Vec<u8>, Vec<u8>, String), Row>,
    resume_position: Option<SourcePosition>,
}

impl StreamingState {
    fn new(cursor: CdcCursor, resume_position: Option<SourcePosition>) -> Self {
        Self {
            cursor,
            transaction: None,
            pending_updates: HashMap::new(),
            resume_position,
        }
    }

    fn transaction_metadata(
        &mut self,
        change: &RawChange,
        table: &CaptureTable,
    ) -> TransactionMetadata {
        if self
            .transaction
            .as_ref()
            .is_none_or(|transaction| transaction.commit_lsn != change.commit_lsn)
        {
            self.transaction = Some(ActiveTransaction {
                commit_lsn: change.commit_lsn.clone(),
                source_time: change.source_time,
                total_order: 0,
                collection_order: HashMap::new(),
            });
        }
        let transaction = self.transaction.as_mut().expect("transaction exists");
        transaction.total_order += 1;
        let collection_order = transaction.collection_order.entry(table.key()).or_insert(0);
        *collection_order += 1;
        TransactionMetadata {
            id: format_lsn(&change.commit_lsn),
            total_order: Some(transaction.total_order),
            collection_order: Some(*collection_order),
        }
    }

    fn commit_record(&mut self, database: &str, commit_lsn: &[u8]) -> Option<SourceRecord> {
        self.transaction = None;
        let record = SourceRecord {
            event: None,
            position: sqlserver_position(
                database,
                commit_lsn,
                &max_lsn_bytes(),
                COMMIT_SERIAL,
                false,
            ),
            boundary: RecordBoundary::TransactionCommit,
            connector_state: None,
            signal_acknowledgements: Vec::new(),
        };
        if self.should_skip(&record.position) {
            None
        } else {
            Some(record)
        }
    }

    fn should_skip(&mut self, position: &SourcePosition) -> bool {
        let Some(resume) = &self.resume_position else {
            return false;
        };
        if position.is_at_or_before(resume) {
            true
        } else {
            self.resume_position = None;
            false
        }
    }

    fn safe_position(&self, database: &str) -> SourcePosition {
        sqlserver_position(
            database,
            &self.cursor.commit_lsn,
            &self.cursor.change_lsn,
            COMMIT_SERIAL,
            false,
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_change_batch(
    client: &mut SqlClient,
    database: &str,
    connector_name: &str,
    captures: &[CaptureTable],
    state: &mut StreamingState,
    max_lsn: &[u8],
    fetch_size: usize,
) -> Result<Vec<SourceRecord>> {
    let query = change_query(captures, fetch_size.saturating_add(1));
    let rows = client
        .query(
            query,
            &[
                &state.cursor.commit_lsn,
                &state.cursor.change_lsn,
                &state.cursor.raw_operation,
                &max_lsn.to_vec(),
            ],
        )
        .await
        .map_err(sqlserver_error)?
        .into_first_result()
        .await
        .map_err(sqlserver_error)?;

    if rows.is_empty() {
        if state.pending_updates.is_empty() {
            state.cursor = CdcCursor::at_snapshot(max_lsn.to_vec());
            return Ok(vec![SourceRecord {
                event: None,
                position: sqlserver_position(
                    database,
                    max_lsn,
                    &max_lsn_bytes(),
                    COMMIT_SERIAL,
                    false,
                ),
                boundary: RecordBoundary::Heartbeat,
                connector_state: None,
                signal_acknowledgements: Vec::new(),
            }]);
        }
        return Ok(Vec::new());
    }

    let has_lookahead = rows.len() > fetch_size;
    let process_count = rows.len().min(fetch_size);
    let mut raw_changes = Vec::with_capacity(process_count);
    for row in rows.iter().take(process_count) {
        raw_changes.push(raw_change(row, captures)?);
    }
    let lookahead_commit = has_lookahead
        .then(|| required_binary(&rows[process_count], "commit_lsn"))
        .transpose()?;
    let complete_last_commit = lookahead_commit.as_ref().is_none_or(|lookahead| {
        raw_changes
            .last()
            .is_some_and(|last| last.commit_lsn != *lookahead)
    });
    let capture_map = captures
        .iter()
        .map(|capture| (capture.capture_instance.as_str(), capture))
        .collect::<HashMap<_, _>>();
    let mut records = Vec::new();

    for (index, change) in raw_changes.iter().enumerate() {
        let table = capture_map
            .get(change.capture_instance.as_str())
            .copied()
            .ok_or_else(|| {
                Error::Source(format!(
                    "unknown SQL Server capture instance {:?}",
                    change.capture_instance
                ))
            })?;
        let update_key = (
            change.commit_lsn.clone(),
            change.change_lsn.clone(),
            change.capture_instance.clone(),
        );
        let (operation, before, after, event_serial) = match change.raw_operation {
            RAW_DELETE => (Operation::Delete, Some(change.row.clone()), None, 1),
            RAW_INSERT => (Operation::Create, None, Some(change.row.clone()), 2),
            RAW_UPDATE_BEFORE => {
                state.pending_updates.insert(update_key, change.row.clone());
                state.cursor = CdcCursor {
                    commit_lsn: change.commit_lsn.clone(),
                    change_lsn: change.change_lsn.clone(),
                    raw_operation: change.raw_operation,
                };
                continue;
            }
            RAW_UPDATE_AFTER => {
                let before = state.pending_updates.remove(&update_key).ok_or_else(|| {
                    Error::Source(format!(
                        "SQL Server update after-image at {}:{} has no before-image",
                        format_lsn(&change.commit_lsn),
                        format_lsn(&change.change_lsn)
                    ))
                })?;
                (Operation::Update, Some(before), Some(change.row.clone()), 4)
            }
            operation => {
                return Err(Error::Source(format!(
                    "unsupported SQL Server CDC operation {operation}"
                )));
            }
        };
        let position = sqlserver_position(
            database,
            &change.commit_lsn,
            &change.change_lsn,
            event_serial,
            false,
        );
        let transaction = state.transaction_metadata(change, table);
        let source_time = state
            .transaction
            .as_ref()
            .and_then(|transaction| transaction.source_time)
            .or(change.source_time);
        let event = ChangeEvent {
            id: EventId::deterministic(
                connector_name,
                database,
                &position,
                &format!("{database}.{}.{}", table.schema, table.table),
                event_serial,
            ),
            source: source_metadata(connector_name, database, table, false),
            position,
            transaction: Some(transaction),
            operation,
            before,
            after,
            schema: table.event_schema.clone(),
            source_time,
            observed_time: Utc::now(),
        };
        let record = SourceRecord::data(event);
        if !state.should_skip(&record.position) {
            records.push(record);
        }
        state.cursor = CdcCursor {
            commit_lsn: change.commit_lsn.clone(),
            change_lsn: change.change_lsn.clone(),
            raw_operation: change.raw_operation,
        };

        let next_commit = raw_changes.get(index + 1).map(|next| &next.commit_lsn);
        let commit_complete = next_commit.is_some_and(|next| next != &change.commit_lsn)
            || (index + 1 == raw_changes.len() && complete_last_commit);
        if commit_complete {
            if let Some(record) = state.commit_record(database, &change.commit_lsn) {
                records.push(record);
            }
            state.cursor = CdcCursor::at_snapshot(change.commit_lsn.clone());
        }
    }
    Ok(records)
}

async fn connect(config: &SqlServerSourceConfig, database: &str) -> Result<SqlClient> {
    let mut tds = TdsConfig::new();
    tds.host(&config.hostname);
    tds.port(config.port);
    tds.database(database);
    tds.application_name("rustium");
    tds.authentication(AuthMethod::sql_server(&config.username, &config.password));
    if config.encrypt {
        tds.encryption(EncryptionLevel::Required);
    } else {
        tds.encryption(EncryptionLevel::NotSupported);
    }
    if config.trust_server_certificate {
        tds.trust_cert();
    }
    let address = tds.get_addr();
    let tcp = tokio::time::timeout(config.connect_timeout, TcpStream::connect(&address))
        .await
        .map_err(|_| Error::RetryableSource("timed out connecting to SQL Server".into()))?
        .map_err(sqlserver_io_error)?;
    tcp.set_nodelay(true).map_err(sqlserver_io_error)?;
    tokio::time::timeout(
        config.connect_timeout,
        Client::connect(tds, tcp.compat_write()),
    )
    .await
    .map_err(|_| Error::RetryableSource("timed out negotiating SQL Server TDS".into()))?
    .map_err(sqlserver_error)
}

async fn discover_captures(
    client: &mut SqlClient,
    config: &SqlServerSourceConfig,
    connector_name: &str,
) -> Result<Vec<CaptureTable>> {
    let rows = client
        .simple_query(
            "SELECT s.name AS schema_name, t.name AS table_name, ct.capture_instance, \
             CAST(ct.source_object_id AS int) AS source_object_id \
             FROM cdc.change_tables ct \
             JOIN sys.tables t ON t.object_id = ct.source_object_id \
             JOIN sys.schemas s ON s.schema_id = t.schema_id \
             ORDER BY s.name, t.name, ct.capture_instance",
        )
        .await
        .map_err(sqlserver_error)?
        .into_first_result()
        .await
        .map_err(sqlserver_error)?;
    let mut captures = Vec::new();
    let mut selected_tables = HashMap::new();
    let signal_table = signal_table_key(config);
    for row in rows {
        let schema = required_string(&row, "schema_name")?;
        let table = required_string(&row, "table_name")?;
        let is_signal = signal_table.as_ref() == Some(&(schema.clone(), table.clone()));
        if !config.tables.includes(&schema, &table) && !is_signal {
            continue;
        }
        if selected_tables
            .insert((schema.clone(), table.clone()), ())
            .is_some()
        {
            return Err(Error::Configuration(format!(
                "SQL Server table {schema}.{table} has multiple CDC capture instances; Rustium requires an explicit single active instance"
            )));
        }
        let source_object_id = required::<i32>(&row, "source_object_id")?;
        let fields = discover_fields(client, source_object_id).await?;
        captures.push(CaptureTable {
            schema: schema.clone(),
            table: table.clone(),
            capture_instance: required_string(&row, "capture_instance")?,
            source_object_id,
            event_schema: EventSchema {
                name: format!("{connector_name}.{schema}.{table}.Envelope"),
                version: 1,
                fields,
            },
        });
    }
    Ok(captures)
}

async fn discover_fields(client: &mut SqlClient, object_id: i32) -> Result<Vec<FieldSchema>> {
    let rows = client
        .query(
            "SELECT c.name AS column_name, ty.name AS type_name, CAST(c.max_length AS int) AS max_length, \
             CAST(c.precision AS int) AS precision, CAST(c.scale AS int) AS scale, \
             CAST(c.is_nullable AS bit) AS is_nullable, \
             CAST(CASE WHEN pk.column_id IS NULL THEN 0 ELSE 1 END AS bit) AS is_primary_key \
             FROM sys.columns c \
             JOIN sys.types ty ON ty.user_type_id = c.user_type_id \
             LEFT JOIN ( \
               SELECT ic.object_id, ic.column_id FROM sys.indexes i \
               JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
               WHERE i.is_primary_key = 1 \
             ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id \
             WHERE c.object_id = @P1 ORDER BY c.column_id",
            &[&object_id],
        )
        .await
        .map_err(sqlserver_error)?
        .into_first_result()
        .await
        .map_err(sqlserver_error)?;
    let mut fields = Vec::new();
    for row in rows {
        let base = required_string(&row, "type_name")?;
        fields.push(FieldSchema {
            name: required_string(&row, "column_name")?,
            type_name: format_sql_type(
                &base,
                required::<i32>(&row, "max_length")?,
                required::<i32>(&row, "precision")?,
                required::<i32>(&row, "scale")?,
            ),
            optional: required::<bool>(&row, "is_nullable")?,
            primary_key: required::<bool>(&row, "is_primary_key")?,
        });
    }
    if fields.is_empty() {
        return Err(Error::Source(format!(
            "could not discover SQL Server columns for object_id {object_id}"
        )));
    }
    Ok(fields)
}

async fn current_max_lsn(client: &mut SqlClient) -> Result<Vec<u8>> {
    let row = client
        .simple_query("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn")
        .await
        .map_err(sqlserver_error)?
        .into_row()
        .await
        .map_err(sqlserver_error)?
        .ok_or_else(|| Error::Source("SQL Server returned no maximum CDC LSN".into()))?;
    required_binary(&row, "max_lsn")
}

async fn validate_retention(
    client: &mut SqlClient,
    captures: &[CaptureTable],
    restart_lsn: &[u8],
) -> Result<()> {
    for capture in captures {
        let row = client
            .query(
                "SELECT sys.fn_cdc_get_min_lsn(@P1) AS min_lsn",
                &[&capture.capture_instance],
            )
            .await
            .map_err(sqlserver_error)?
            .into_row()
            .await
            .map_err(sqlserver_error)?
            .ok_or_else(|| Error::Source("SQL Server returned no minimum CDC LSN".into()))?;
        let min_lsn = required_binary(&row, "min_lsn")?;
        if restart_lsn != zero_lsn_bytes() && restart_lsn < min_lsn.as_slice() {
            return Err(Error::State(format!(
                "SQL Server CDC cleanup advanced capture instance {:?} to {}; checkpoint {} is no longer available",
                capture.capture_instance,
                format_lsn(&min_lsn),
                format_lsn(restart_lsn)
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn snapshot_table(
    client: &mut SqlClient,
    database: &str,
    connector_name: &str,
    capture: &CaptureTable,
    anchor: &[u8],
    ordinal: &mut u64,
    output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
) -> Result<()> {
    let values = capture
        .event_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| change_value_expression(index, field))
        .collect::<Vec<_>>()
        .join(", ");
    let primary_key = capture
        .event_schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .map(|field| format!("ct.{}", quote_identifier(&field.name)))
        .collect::<Vec<_>>();
    let ordering = if primary_key.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", primary_key.join(", "))
    };
    let query = format!(
        "SELECT {values} FROM {}.{} AS ct{ordering}",
        quote_identifier(&capture.schema),
        quote_identifier(&capture.table)
    );
    let mut rows = client
        .simple_query(query)
        .await
        .map_err(sqlserver_error)?
        .into_row_stream();
    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await.map_err(sqlserver_error)? {
        *ordinal += 1;
        count += 1;
        let position = sqlserver_position(database, anchor, &zero_lsn_bytes(), *ordinal, true);
        let event = ChangeEvent {
            id: EventId::deterministic(
                connector_name,
                database,
                &position,
                &format!("{database}.{}.{}", capture.schema, capture.table),
                *ordinal,
            ),
            source: source_metadata(connector_name, database, capture, true),
            position,
            transaction: None,
            operation: Operation::Read,
            before: None,
            after: Some(convert_tds_row(&row, &capture.event_schema)?),
            schema: capture.event_schema.clone(),
            source_time: None,
            observed_time: Utc::now(),
        };
        output
            .send(Ok(SourceRecord::data(event)))
            .await
            .map_err(|_| Error::Cancelled)?;
    }
    drop(rows);
    info!(table = %format!("{}.{}", capture.schema, capture.table), rows = count, "SQL Server snapshot table completed");
    Ok(())
}

fn change_query(captures: &[CaptureTable], limit: usize) -> String {
    let unions = captures
        .iter()
        .map(|capture| {
            let values = capture
                .event_schema
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| change_value_expression(index, field))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT ct.__$start_lsn AS commit_lsn, ct.__$seqval AS change_lsn, \
                 CAST(ct.__$operation AS int) AS operation, \
                 CAST(N'{}' AS nvarchar(128)) AS capture_instance, \
                 (SELECT {values} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES) AS row_data, \
                 sys.fn_cdc_map_lsn_to_time(ct.__$start_lsn) AS source_time \
                 FROM cdc.{} ct",
                quote_literal(&capture.capture_instance),
                quote_identifier(&format!("{}_CT", capture.capture_instance)),
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    format!(
        "SELECT TOP ({limit}) commit_lsn, change_lsn, operation, capture_instance, row_data, source_time \
         FROM ({unions}) AS changes \
         WHERE (commit_lsn > @P1 OR (commit_lsn = @P1 AND \
                (change_lsn > @P2 OR (change_lsn = @P2 AND operation > @P3)))) \
           AND commit_lsn <= @P4 \
         ORDER BY commit_lsn, change_lsn, operation"
    )
}

fn change_value_expression(index: usize, field: &FieldSchema) -> String {
    let column = format!("ct.{}", quote_identifier(&field.name));
    let base = base_type(&field.type_name);
    let value = match base {
        "binary" | "varbinary" | "image" | "rowversion" | "timestamp" => {
            format!("CONVERT(varchar(max), {column}, 2)")
        }
        "date" | "time" | "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset" => {
            format!("CONVERT(nvarchar(64), {column}, 126)")
        }
        "geometry" | "geography" => {
            format!("CONVERT(varchar(max), {column}.Serialize(), 2)")
        }
        "hierarchyid" => format!("{column}.ToString()"),
        _ => format!("CONVERT(nvarchar(max), {column})"),
    };
    format!("{value} AS [c{index}]")
}

fn raw_change(row: &TdsRow, captures: &[CaptureTable]) -> Result<RawChange> {
    let capture_instance = required_string(row, "capture_instance")?;
    let capture = captures
        .iter()
        .find(|capture| capture.capture_instance == capture_instance)
        .ok_or_else(|| {
            Error::Source(format!(
                "SQL Server returned unknown capture instance {capture_instance:?}"
            ))
        })?;
    let json = required_string(row, "row_data")?;
    Ok(RawChange {
        commit_lsn: required_binary(row, "commit_lsn")?,
        change_lsn: required_binary(row, "change_lsn")?,
        raw_operation: required::<i32>(row, "operation")?,
        capture_instance,
        row: convert_json_row(&json, &capture.event_schema)?,
        source_time: row
            .try_get::<NaiveDateTime, _>("source_time")
            .map_err(sqlserver_error)?
            .map(|time| DateTime::from_naive_utc_and_offset(time, Utc)),
    })
}

fn convert_json_row(json: &str, schema: &EventSchema) -> Result<Row> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    let object = value
        .as_object()
        .ok_or_else(|| Error::Source("SQL Server CDC row JSON is not an object".into()))?;
    Ok(schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = object
                .get(&format!("c{index}"))
                .map_or(DataValue::Null, |value| {
                    json_text_value(value, &field.type_name)
                });
            (field.name.clone(), value)
        })
        .collect())
}

fn json_text_value(value: &serde_json::Value, type_name: &str) -> DataValue {
    match value {
        serde_json::Value::Null => DataValue::Null,
        serde_json::Value::String(value) => convert_sqlserver_text(value, type_name),
        serde_json::Value::Bool(value) => DataValue::Boolean(*value),
        serde_json::Value::Number(value) => DataValue::Decimal(value.to_string()),
        _ => DataValue::Json(value.clone()),
    }
}

fn convert_sqlserver_text(value: &str, type_name: &str) -> DataValue {
    match base_type(type_name) {
        "bit" => DataValue::Boolean(matches!(value, "1" | "true" | "TRUE")),
        "tinyint" | "smallint" | "int" => value
            .parse::<i32>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Int32),
        "bigint" => value
            .parse::<i64>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Int64),
        "real" | "float" => value
            .parse::<f64>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Float64),
        "decimal" | "numeric" | "money" | "smallmoney" => DataValue::Decimal(value.into()),
        "binary" | "varbinary" | "image" | "rowversion" | "timestamp" | "geometry"
        | "geography" => {
            hex::decode(value).map_or_else(|_| DataValue::String(value.into()), DataValue::Bytes)
        }
        "date" => DataValue::Date(value.into()),
        "time" => DataValue::Time(value.into()),
        "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset" => {
            DataValue::Timestamp(value.into())
        }
        "uniqueidentifier" => uuid::Uuid::parse_str(value)
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Uuid),
        _ => DataValue::String(value.into()),
    }
}

fn convert_tds_row(row: &TdsRow, schema: &EventSchema) -> Result<Row> {
    if row.len() != schema.fields.len() {
        return Err(Error::Source(format!(
            "SQL Server snapshot row has {} values but schema has {} fields",
            row.len(),
            schema.fields.len()
        )));
    }
    schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Ok((
                field.name.clone(),
                convert_tds_value(row, index, &field.type_name)?,
            ))
        })
        .collect()
}

fn convert_tds_value(row: &TdsRow, index: usize, type_name: &str) -> Result<DataValue> {
    let cell = row
        .cells()
        .nth(index)
        .map(|(_, value)| value)
        .ok_or_else(|| Error::Source("SQL Server row cell is missing".into()))?;
    let value = match cell {
        ColumnData::U8(value) => {
            value.map_or(DataValue::Null, |value| DataValue::Int32(value.into()))
        }
        ColumnData::I16(value) => {
            value.map_or(DataValue::Null, |value| DataValue::Int32(value.into()))
        }
        ColumnData::I32(value) => value.map_or(DataValue::Null, DataValue::Int32),
        ColumnData::I64(value) => value.map_or(DataValue::Null, DataValue::Int64),
        ColumnData::F32(value) => {
            value.map_or(DataValue::Null, |value| DataValue::Float64(value.into()))
        }
        ColumnData::F64(value) => value.map_or(DataValue::Null, DataValue::Float64),
        ColumnData::Bit(value) => value.map_or(DataValue::Null, DataValue::Boolean),
        ColumnData::String(value) => value.as_ref().map_or(DataValue::Null, |value| {
            convert_sqlserver_text(value, type_name)
        }),
        ColumnData::Guid(value) => value.map_or(DataValue::Null, DataValue::Uuid),
        ColumnData::Binary(value) => value
            .as_ref()
            .map_or(DataValue::Null, |value| DataValue::Bytes(value.to_vec())),
        ColumnData::Numeric(value) => value.map_or(DataValue::Null, |value| {
            DataValue::Decimal(value.to_string())
        }),
        ColumnData::Xml(value) => value.as_ref().map_or(DataValue::Null, |value| {
            DataValue::String(value.to_string())
        }),
        ColumnData::DateTime(None)
        | ColumnData::SmallDateTime(None)
        | ColumnData::Time(None)
        | ColumnData::Date(None)
        | ColumnData::DateTime2(None)
        | ColumnData::DateTimeOffset(None) => DataValue::Null,
        ColumnData::DateTime(Some(_))
        | ColumnData::SmallDateTime(Some(_))
        | ColumnData::DateTime2(Some(_)) => row
            .try_get::<NaiveDateTime, _>(index)
            .map_err(sqlserver_error)?
            .map_or(DataValue::Null, |value| {
                DataValue::Timestamp(sqlserver_datetime(value))
            }),
        ColumnData::Time(Some(_)) => row
            .try_get::<NaiveTime, _>(index)
            .map_err(sqlserver_error)?
            .map_or(DataValue::Null, |value| DataValue::Time(value.to_string())),
        ColumnData::Date(Some(_)) => row
            .try_get::<NaiveDate, _>(index)
            .map_err(sqlserver_error)?
            .map_or(DataValue::Null, |value| DataValue::Date(value.to_string())),
        ColumnData::DateTimeOffset(Some(_)) => row
            .try_get::<DateTime<FixedOffset>, _>(index)
            .map_err(sqlserver_error)?
            .map_or(DataValue::Null, |value| {
                DataValue::Timestamp(value.to_rfc3339())
            }),
    };
    Ok(value)
}

fn sqlserver_datetime(value: NaiveDateTime) -> String {
    value.to_string().replacen(' ', "T", 1)
}

fn source_metadata(
    connector_name: &str,
    database: &str,
    capture: &CaptureTable,
    snapshot: bool,
) -> SourceMetadata {
    let mut attributes = BTreeMap::new();
    attributes.insert(
        "capture_instance".into(),
        capture.capture_instance.clone().into(),
    );
    attributes.insert("source_object_id".into(), capture.source_object_id.into());
    SourceMetadata {
        connector: "sqlserver".into(),
        connector_name: connector_name.into(),
        database: database.into(),
        schema: Some(capture.schema.clone()),
        table: Some(capture.table.clone()),
        snapshot,
        version: CONNECTOR_VERSION.into(),
        attributes,
    }
}

fn sqlserver_position(
    database: &str,
    commit_lsn: &[u8],
    change_lsn: &[u8],
    event_serial: u64,
    snapshot: bool,
) -> SourcePosition {
    SourcePosition::SqlServer(SqlServerPosition {
        database: database.into(),
        commit_lsn: format_lsn(commit_lsn),
        change_lsn: format_lsn(change_lsn),
        event_serial,
        snapshot,
    })
}

fn snapshot_begin_sql(mode: &str) -> &'static str {
    match mode {
        "exclusive" => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION",
        "snapshot" => "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; BEGIN TRANSACTION",
        "read_committed" => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
        "read_uncommitted" => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; BEGIN TRANSACTION",
        _ => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
    }
}

fn format_sql_type(base: &str, max_length: i32, precision: i32, scale: i32) -> String {
    match base {
        "decimal" | "numeric" => format!("{base}({precision},{scale})"),
        "char" | "varchar" | "binary" | "varbinary" => {
            if max_length < 0 {
                format!("{base}(max)")
            } else {
                format!("{base}({max_length})")
            }
        }
        "nchar" | "nvarchar" => {
            if max_length < 0 {
                format!("{base}(max)")
            } else {
                format!("{base}({})", max_length / 2)
            }
        }
        "datetime2" | "datetimeoffset" | "time" => format!("{base}({scale})"),
        _ => base.into(),
    }
}

fn required<'a, T>(row: &'a TdsRow, name: &str) -> Result<T>
where
    T: tiberius::FromSql<'a>,
{
    row.try_get(name)
        .map_err(sqlserver_error)?
        .ok_or_else(|| Error::Source(format!("SQL Server result {name:?} is null")))
}

fn required_string(row: &TdsRow, name: &str) -> Result<String> {
    required::<&str>(row, name).map(str::to_string)
}

fn required_binary(row: &TdsRow, name: &str) -> Result<Vec<u8>> {
    let value = required::<&[u8]>(row, name)?.to_vec();
    if value.len() != LSN_SIZE {
        return Err(Error::Source(format!(
            "SQL Server {name} has {} bytes, expected {LSN_SIZE}",
            value.len()
        )));
    }
    Ok(value)
}

fn quote_identifier(identifier: &str) -> String {
    format!("[{}]", identifier.replace(']', "]]"))
}

fn quote_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn base_type(type_name: &str) -> &str {
    type_name.split('(').next().unwrap_or(type_name)
}

fn format_lsn(lsn: &[u8]) -> String {
    format!("0x{}", hex::encode_upper(lsn))
}

fn parse_lsn(lsn: &str) -> Result<Vec<u8>> {
    let bytes = hex::decode(lsn.strip_prefix("0x").unwrap_or(lsn)).map_err(|error| {
        Error::State(format!(
            "invalid SQL Server checkpoint LSN {lsn:?}: {error}"
        ))
    })?;
    if bytes.len() != LSN_SIZE {
        return Err(Error::State(format!(
            "invalid SQL Server checkpoint LSN length {}; expected {LSN_SIZE}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn zero_lsn_bytes() -> Vec<u8> {
    vec![0; LSN_SIZE]
}

fn max_lsn_bytes() -> Vec<u8> {
    vec![u8::MAX; LSN_SIZE]
}

fn sqlserver_error(error: tiberius::error::Error) -> Error {
    let retryable = matches!(error, tiberius::error::Error::Io { .. })
        || error.code().is_some_and(is_transient_sqlserver_code);
    if retryable {
        Error::RetryableSource(error.to_string())
    } else {
        Error::Source(error.to_string())
    }
}

fn sqlserver_io_error(error: std::io::Error) -> Error {
    Error::RetryableSource(error.to_string())
}

const fn is_transient_sqlserver_code(code: u32) -> bool {
    matches!(
        code,
        233 | 1205
            | 1222
            | 10_053
            | 10_054
            | 10_060
            | 10_928
            | 10_929
            | 40_197
            | 40_501
            | 40_613
            | 49_918
            | 49_919
            | 49_920
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_sqlserver_lsn() {
        let bytes = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 255];
        assert_eq!(parse_lsn(&format_lsn(&bytes)).unwrap(), bytes);
    }

    #[test]
    fn distinguishes_complete_and_mid_transaction_cursors() {
        let complete = CdcCursor::at_snapshot(vec![1; LSN_SIZE]);
        assert!(complete.commit_complete());

        let incomplete = CdcCursor {
            commit_lsn: vec![1; LSN_SIZE],
            change_lsn: vec![2; LSN_SIZE],
            raw_operation: RAW_UPDATE_BEFORE,
        };
        assert!(!incomplete.commit_complete());
    }

    #[test]
    fn classifies_only_transient_sqlserver_failures_for_retry() {
        let io_error = tiberius::error::Error::Io {
            kind: std::io::ErrorKind::ConnectionReset,
            message: "connection reset".into(),
        };
        assert!(matches!(
            sqlserver_error(io_error),
            Error::RetryableSource(message) if message.contains("connection reset")
        ));
        assert!(is_transient_sqlserver_code(1205));
        assert!(is_transient_sqlserver_code(40_613));
        assert!(!is_transient_sqlserver_code(208));

        assert!(matches!(
            sqlserver_error(tiberius::error::Error::Protocol("invalid token".into())),
            Error::Source(message) if message.contains("invalid token")
        ));
    }

    #[test]
    fn enforces_sqlserver_retry_policy_boundaries() {
        let disabled = RetryPolicy {
            max_retries: 0,
            ..RetryPolicy::default()
        };
        assert!(!sqlserver_retry_allowed(&disabled, 0));

        let finite = RetryPolicy {
            max_retries: 2,
            ..RetryPolicy::default()
        };
        assert!(sqlserver_retry_allowed(&finite, 0));
        assert!(sqlserver_retry_allowed(&finite, 1));
        assert!(!sqlserver_retry_allowed(&finite, 2));

        let unbounded = RetryPolicy {
            max_retries: -1,
            ..RetryPolicy::default()
        };
        assert!(sqlserver_retry_allowed(&unbounded, u64::MAX));
    }

    #[test]
    fn converts_sqlserver_values() {
        assert_eq!(
            convert_sqlserver_text("12.30", "decimal(10,2)"),
            DataValue::Decimal("12.30".into())
        );
        assert_eq!(convert_sqlserver_text("1", "bit"), DataValue::Boolean(true));
        assert_eq!(
            convert_sqlserver_text("00FF", "varbinary(2)"),
            DataValue::Bytes(vec![0, 255])
        );
        assert_eq!(
            convert_sqlserver_text("E6100000010C0000000000000040000000000000F03F", "geometry"),
            DataValue::Bytes(hex::decode("E6100000010C0000000000000040000000000000F03F").unwrap())
        );
        assert_eq!(
            change_value_expression(
                0,
                &FieldSchema {
                    name: "location".into(),
                    type_name: "geography".into(),
                    optional: false,
                    primary_key: false,
                }
            ),
            "CONVERT(varchar(max), ct.[location].Serialize(), 2) AS [c0]"
        );
        assert_eq!(
            sqlserver_datetime(
                NaiveDate::from_ymd_opt(2026, 7, 16)
                    .unwrap()
                    .and_hms_micro_opt(9, 30, 45, 123_400)
                    .unwrap()
            ),
            "2026-07-16T09:30:45.123400"
        );
    }

    #[test]
    fn builds_bounded_direct_cdc_query() {
        let capture = CaptureTable {
            schema: "dbo".into(),
            table: "orders".into(),
            capture_instance: "dbo_orders".into(),
            source_object_id: 42,
            event_schema: EventSchema {
                name: "orders".into(),
                version: 1,
                fields: vec![FieldSchema {
                    name: "id".into(),
                    type_name: "bigint".into(),
                    optional: false,
                    primary_key: true,
                }],
            },
        };
        let query = change_query(&[capture], 513);
        assert!(query.contains("TOP (513)"));
        assert!(query.contains("cdc.[dbo_orders_CT]"));
        assert!(query.contains("ORDER BY commit_lsn, change_lsn, operation"));
    }

    #[test]
    fn builds_composite_sqlserver_keyset_predicates() {
        let columns = vec!["ct.[tenant_id]".into(), "ct.[id]".into()];
        let key = vec![SqlServerKeyValue::Int32(7), SqlServerKeyValue::Int64(42)];
        assert_eq!(
            sqlserver_key_predicate(&columns, &key, true).unwrap(),
            "((ct.[tenant_id] > 7) OR (ct.[tenant_id] = 7 AND ct.[id] > 42))"
        );
        assert_eq!(
            sqlserver_key_predicate(&columns, &key, false).unwrap(),
            "((ct.[tenant_id] < 7) OR (ct.[tenant_id] = 7 AND ct.[id] < 42) OR (ct.[tenant_id] = 7 AND ct.[id] = 42))"
        );
    }

    #[test]
    fn validates_sqlserver_signal_table_layout() {
        let mut capture = CaptureTable {
            schema: "dbo".into(),
            table: "rustium_signal".into(),
            capture_instance: "dbo_rustium_signal".into(),
            source_object_id: 42,
            event_schema: EventSchema {
                name: "signal".into(),
                version: 1,
                fields: ["id", "type", "data"]
                    .into_iter()
                    .map(|name| FieldSchema {
                        name: name.into(),
                        type_name: "nvarchar(max)".into(),
                        optional: false,
                        primary_key: name == "id",
                    })
                    .collect(),
            },
        };
        assert!(validate_signal_schema(&capture).is_ok());
        capture.event_schema.fields[0].type_name = "nvarchar(20)".into();
        assert!(matches!(
            validate_signal_schema(&capture),
            Err(Error::Configuration(message)) if message.contains("at least 42 characters")
        ));
    }

    #[test]
    fn builds_sqlserver_heartbeat_record_at_commit_position() {
        let position = sqlserver_position(
            "inventory",
            &[1; LSN_SIZE],
            &max_lsn_bytes(),
            COMMIT_SERIAL,
            false,
        );
        let record =
            sqlserver_heartbeat_record("inventory-sqlserver", "inventory", position.clone());
        let event = record.event.unwrap();
        assert_eq!(record.boundary, RecordBoundary::Heartbeat);
        assert_eq!(record.position, position);
        assert_eq!(event.operation, Operation::Message);
        assert_eq!(event.source.table, None);
        assert_eq!(
            event.source.attributes.get("rustium.heartbeat"),
            Some(&true.into())
        );
        assert!(matches!(
            event.after.unwrap().get("ts_ms"),
            Some(DataValue::Int64(_))
        ));
    }
}
