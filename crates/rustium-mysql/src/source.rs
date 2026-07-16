use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use mysql_async::{
    BinlogStream, BinlogStreamRequest, ClientIdentity, Conn, Opts, OptsBuilder, Row as MySqlRow,
    Sid, SslOpts, Value,
    binlog::{
        events::{Event as BinlogEvent, EventData, RowsEventData, TableMapEvent},
        row::BinlogRow,
        value::BinlogValue,
    },
    prelude::Queryable,
};
use regex::RegexBuilder;
use rustium_config::{MySqlSourceConfig, SnapshotConfig, SnapshotMode};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, MySqlPosition, Operation,
    RecordBoundary, Result, Row, SignalAcknowledgement, SignalRecord, SourceConnector,
    SourceContext, SourceMetadata, SourcePosition, SourceRecord, TransactionMetadata,
};
use tracing::{debug, info, warn};

use crate::schema_history::{
    IncrementalSnapshotProgress, TableSchema, apply_ddl, decode_connector_state,
    encode_connector_state, encode_schema_history,
};

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
struct BinlogCoordinates {
    filename: String,
    position: u64,
    gtid_set: Option<String>,
    gtid_set_is_complete: bool,
    source_server_id: u32,
}

#[derive(Debug)]
struct GtidSourceFilter {
    patterns: Vec<regex::Regex>,
    include: bool,
}

impl GtidSourceFilter {
    fn from_config(config: &MySqlSourceConfig) -> Result<Option<Self>> {
        let (patterns, include) = if !config.gtid_source_includes.is_empty() {
            (&config.gtid_source_includes, true)
        } else if !config.gtid_source_excludes.is_empty() {
            (&config.gtid_source_excludes, false)
        } else {
            return Ok(None);
        };
        let patterns = patterns
            .iter()
            .map(|pattern| {
                RegexBuilder::new(&format!(r"\A(?:{pattern})\z"))
                    .case_insensitive(true)
                    .build()
                    .map_err(|error| {
                        Error::Configuration(format!(
                            "invalid MySQL GTID source filter {pattern:?}: {error}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self { patterns, include }))
    }

    fn matches(&self, source_uuid: &str) -> bool {
        let matched = self
            .patterns
            .iter()
            .any(|pattern| pattern.is_match(source_uuid));
        if self.include { matched } else { !matched }
    }

    fn filter_sids(&self, gtid_set: &str) -> Result<Vec<Sid<'static>>> {
        gtid_set
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(|entry| {
                let sid = entry.trim().parse::<Sid<'static>>().map_err(|error| {
                    Error::Source(format!(
                        "invalid MySQL gtid_executed entry {entry:?}: {error}"
                    ))
                })?;
                let source_uuid = uuid::Uuid::from_bytes(sid.uuid()).to_string();
                Ok((self.matches(&source_uuid)).then_some(sid))
            })
            .filter_map(|result| match result {
                Ok(Some(sid)) => Some(Ok(sid)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }
}

#[derive(Debug)]
struct SnapshotOutcome {
    coordinates: BinlogCoordinates,
    schemas: HashMap<(String, String), TableSchema>,
}

#[derive(Debug, Clone)]
struct ActiveTransaction {
    id: String,
    source_time: Option<DateTime<Utc>>,
    total_order: u64,
    collection_order: HashMap<(String, String), u64>,
    ignore_dml: bool,
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

struct MySqlIncrementalSnapshot {
    progress: Option<IncrementalSnapshotProgress>,
    event_serial: u64,
    completed_signal_ids: HashSet<String>,
}

impl MySqlIncrementalSnapshot {
    fn new(progress: Option<IncrementalSnapshotProgress>) -> Self {
        Self {
            progress,
            event_serial: 0,
            completed_signal_ids: HashSet::new(),
        }
    }

    fn progress(&self) -> Option<&IncrementalSnapshotProgress> {
        self.progress.as_ref()
    }

    fn latest_position(&self, base: &SourcePosition) -> Result<Option<SourcePosition>> {
        if self.event_serial == 0 {
            Ok(None)
        } else {
            incremental_position(base, self.event_serial).map(Some)
        }
    }

    fn parse_external_record(record: &SignalRecord) -> Result<SnapshotSignal> {
        if record.id.trim().is_empty() || record.signal_type.trim().is_empty() {
            return Err(Error::Source(
                "MySQL external signal requires non-empty id and type".into(),
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
                "MySQL signal {id:?} has invalid JSON data: {error}"
            ))
        })?;
        parse_snapshot_signal(&id, &signal_type, &value)
    }

    async fn handle_signal(
        &mut self,
        signal: SnapshotSignal,
        source: &MySqlSource,
        base_position: &SourcePosition,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<()> {
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
                    warn!(%id, "MySQL incremental snapshot is already active; execute signal ignored");
                    return Ok(());
                }
                self.progress = Some(IncrementalSnapshotProgress {
                    signal_id: id,
                    data_collections,
                    additional_conditions,
                    current_collection: 0,
                    offset: 0,
                    chunk_sequence: 1,
                    paused: false,
                });
                self.run_to_completion(source, base_position, output).await
            }
            SnapshotSignal::Stop {
                id,
                data_collections,
            } => {
                if let Some(progress) = &self.progress {
                    if data_collections.is_empty()
                        || data_collections.iter().any(|collection| {
                            progress
                                .data_collections
                                .iter()
                                .any(|active| active == collection)
                        })
                    {
                        info!(%id, "MySQL incremental snapshot stopped");
                        self.progress = None;
                    }
                }
                Ok(())
            }
            SnapshotSignal::Pause { id } => {
                if let Some(progress) = &mut self.progress {
                    progress.paused = true;
                    info!(%id, "MySQL incremental snapshot paused");
                }
                Ok(())
            }
            SnapshotSignal::Resume { id } => {
                let should_resume = if let Some(progress) = &mut self.progress {
                    progress.paused = false;
                    info!(%id, "MySQL incremental snapshot resumed");
                    true
                } else {
                    false
                };
                if should_resume {
                    self.run_to_completion(source, base_position, output)
                        .await?;
                }
                Ok(())
            }
            SnapshotSignal::Unsupported { id, signal_type } => {
                warn!(%id, %signal_type, "unsupported MySQL runtime signal ignored");
                Ok(())
            }
        }
    }

    async fn run_to_completion(
        &mut self,
        source: &MySqlSource,
        base_position: &SourcePosition,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<()> {
        while self
            .progress
            .as_ref()
            .is_some_and(|progress| !progress.paused)
        {
            let Some(progress) = self.progress.clone() else {
                break;
            };
            let Some(collection) = progress
                .data_collections
                .get(progress.current_collection)
                .cloned()
            else {
                self.progress = None;
                break;
            };
            let Some(schema) = source.schema_for_collection(&collection) else {
                return Err(Error::Source(format!(
                    "MySQL incremental snapshot collection {collection:?} is not captured"
                )));
            };
            let rows = source.read_incremental_chunk(&schema, &progress).await?;
            let row_count = rows.len();
            for row in rows {
                self.event_serial = self.event_serial.saturating_add(1);
                let position = incremental_position(base_position, self.event_serial)?;
                let mut attributes = BTreeMap::new();
                attributes.insert("rustium.snapshot.kind".into(), "incremental".into());
                let event = ChangeEvent {
                    id: EventId::deterministic(
                        &source.connector_name,
                        &schema.database,
                        &position,
                        &collection,
                        self.event_serial,
                    ),
                    source: SourceMetadata {
                        connector: "mysql".into(),
                        connector_name: source.connector_name.clone(),
                        database: schema.database.clone(),
                        schema: None,
                        table: Some(schema.table.clone()),
                        snapshot: true,
                        version: CONNECTOR_VERSION.into(),
                        attributes,
                    },
                    position: position.clone(),
                    transaction: None,
                    operation: Operation::Read,
                    before: None,
                    after: Some(convert_snapshot_row(row, &schema.event_schema)?),
                    schema: schema.event_schema.clone(),
                    source_time: None,
                    observed_time: Utc::now(),
                };
                output
                    .send(Ok(SourceRecord::data(event)))
                    .await
                    .map_err(|_| Error::Cancelled)?;
            }
            let mut next = progress;
            if row_count < source.config.incremental_snapshot_chunk_size {
                next.current_collection = next.current_collection.saturating_add(1);
                next.offset = 0;
            } else {
                next.offset = next.offset.saturating_add(row_count as u64);
            }
            next.chunk_sequence = next.chunk_sequence.saturating_add(1);
            self.progress = Some(next);
            self.event_serial = self.event_serial.saturating_add(1);
            let position = incremental_position(base_position, self.event_serial)?;
            output
                .send(Ok(SourceRecord {
                    event: None,
                    position,
                    boundary: RecordBoundary::TransactionCommit,
                    connector_state: Some(encode_connector_state(
                        &source.schemas,
                        self.progress.as_ref(),
                    )?),
                    signal_acknowledgements: Vec::new(),
                }))
                .await
                .map_err(|_| Error::Cancelled)?;
        }
        if self
            .progress
            .as_ref()
            .is_some_and(|progress| progress.current_collection >= progress.data_collections.len())
        {
            if let Some(progress) = &self.progress {
                self.completed_signal_ids.insert(progress.signal_id.clone());
            }
            self.progress = None;
            self.event_serial = self.event_serial.saturating_add(1);
            let position = incremental_position(base_position, self.event_serial)?;
            output
                .send(Ok(SourceRecord {
                    event: None,
                    position,
                    boundary: RecordBoundary::TransactionCommit,
                    connector_state: Some(encode_connector_state(&source.schemas, None)?),
                    signal_acknowledgements: Vec::new(),
                }))
                .await
                .map_err(|_| Error::Cancelled)?;
        }
        Ok(())
    }
}

pub struct MySqlSource {
    connector_name: String,
    config: MySqlSourceConfig,
    snapshot: SnapshotConfig,
    schemas: HashMap<(String, String), TableSchema>,
    source_server_id: u32,
    gtid_source_filter: Option<GtidSourceFilter>,
}

impl MySqlSource {
    #[must_use]
    pub fn new(
        connector_name: impl Into<String>,
        config: MySqlSourceConfig,
        snapshot: SnapshotConfig,
    ) -> Self {
        Self {
            connector_name: connector_name.into(),
            config,
            snapshot,
            schemas: HashMap::new(),
            source_server_id: 0,
            gtid_source_filter: None,
        }
    }

    async fn validate_source(&mut self) -> Result<()> {
        self.gtid_source_filter = GtidSourceFilter::from_config(&self.config)?;
        let mut connection = connect(&self.config).await?;
        let version = connection.server_version();
        if version < (8, 0, 0) {
            return Err(Error::Configuration(format!(
                "MySQL 8.0 or newer is required; server version is {}.{}.{}",
                version.0, version.1, version.2
            )));
        }

        let variables: Option<(String, String, String, u32)> = connection
            .query_first(
                "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, \
                 @@GLOBAL.binlog_row_image, @@GLOBAL.server_id",
            )
            .await
            .map_err(mysql_error)?;
        let (log_bin, binlog_format, row_image, source_server_id) = variables
            .ok_or_else(|| Error::Source("MySQL did not return binary log settings".into()))?;
        if !matches!(log_bin.to_ascii_uppercase().as_str(), "1" | "ON") {
            return Err(Error::Configuration(
                "MySQL binary logging must be enabled (log_bin=ON)".into(),
            ));
        }
        if !binlog_format.eq_ignore_ascii_case("ROW") {
            return Err(Error::Configuration(format!(
                "MySQL binlog_format must be ROW; found {binlog_format:?}"
            )));
        }
        if !matches!(
            row_image.to_ascii_uppercase().as_str(),
            "FULL" | "MINIMAL" | "NOBLOB"
        ) {
            return Err(Error::Configuration(format!(
                "unsupported MySQL binlog_row_image {row_image:?}"
            )));
        }
        if source_server_id == self.config.server_id {
            return Err(Error::Configuration(format!(
                "database.server.id={} conflicts with the source server_id",
                self.config.server_id
            )));
        }

        current_binlog_coordinates(&mut connection, source_server_id).await?;
        let schemas = discover_tables(&mut connection, &self.config, &self.connector_name).await?;
        if let Some(signal_key) = signal_table_key(&self.config) {
            let signal_schema = schemas.get(&signal_key).ok_or_else(|| {
                Error::Configuration(format!(
                    "MySQL signal.data.collection {}.{} does not exist or is not visible",
                    signal_key.0, signal_key.1
                ))
            })?;
            let columns = signal_schema
                .event_schema
                .fields
                .iter()
                .map(|field| field.name.to_ascii_lowercase())
                .collect::<Vec<_>>();
            if columns.len() < 3 || columns[..3] != ["id", "type", "data"] {
                return Err(Error::Configuration(
                    "MySQL signal.data.collection must expose id, type, data as its first three columns".into(),
                ));
            }
        }
        if !schemas
            .values()
            .any(|schema| self.config.tables.includes(&schema.database, &schema.table))
        {
            return Err(Error::Configuration(
                "the database and table filters select no MySQL tables".into(),
            ));
        }
        connection.disconnect().await.map_err(mysql_error)?;
        self.schemas = schemas;
        self.source_server_id = source_server_id;
        Ok(())
    }

    async fn run_snapshot(
        &self,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<SnapshotOutcome> {
        let mut lock_connection = connect(&self.config).await?;
        let mut snapshot_connection = connect(&self.config).await?;

        lock_connection
            .query_drop("FLUSH TABLES WITH READ LOCK")
            .await
            .map_err(mysql_error)?;
        let setup_result: Result<BinlogCoordinates> = async {
            snapshot_connection
                .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await
                .map_err(mysql_error)?;
            snapshot_connection
                .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await
                .map_err(mysql_error)?;
            current_binlog_coordinates(&mut lock_connection, self.source_server_id).await
        }
        .await;
        let unlock_result = lock_connection
            .query_drop("UNLOCK TABLES")
            .await
            .map_err(mysql_error);
        let coordinates = setup_result?;
        unlock_result?;

        let snapshot_result: Result<HashMap<(String, String), TableSchema>> = async {
            let schemas =
                discover_tables(&mut snapshot_connection, &self.config, &self.connector_name)
                    .await?;
            let mut ordered = schemas
                .values()
                .filter(|schema| self.config.tables.includes(&schema.database, &schema.table))
                .cloned()
                .collect::<Vec<_>>();
            ordered.sort_by_key(TableSchema::key);
            let mut ordinal = 0_u64;
            for schema in &ordered {
                snapshot_table(
                    &mut snapshot_connection,
                    &self.connector_name,
                    schema,
                    &coordinates,
                    self.snapshot.fetch_size,
                    &mut ordinal,
                    output,
                )
                .await?;
            }

            snapshot_connection
                .query_drop("COMMIT")
                .await
                .map_err(mysql_error)?;
            ordinal += 1;
            output
                .send(Ok(SourceRecord {
                    event: None,
                    position: mysql_position(
                        &coordinates.filename,
                        coordinates.position,
                        coordinates.gtid_set.clone(),
                        coordinates.source_server_id,
                        ordinal,
                        true,
                    ),
                    boundary: RecordBoundary::SnapshotComplete,
                    connector_state: Some(encode_schema_history(&schemas)?),
                    signal_acknowledgements: Vec::new(),
                }))
                .await
                .map_err(|_| Error::Cancelled)?;
            Ok(schemas)
        }
        .await;

        if snapshot_result.is_err() {
            let _ = snapshot_connection.query_drop("ROLLBACK").await;
        }
        let schemas = snapshot_result?;
        let _ = lock_connection.disconnect().await;
        let _ = snapshot_connection.disconnect().await;
        Ok(SnapshotOutcome {
            coordinates,
            schemas,
        })
    }

    async fn current_coordinates(&self) -> Result<BinlogCoordinates> {
        let mut connection = connect(&self.config).await?;
        let coordinates =
            current_binlog_coordinates(&mut connection, self.source_server_id).await?;
        connection.disconnect().await.map_err(mysql_error)?;
        Ok(coordinates)
    }

    async fn open_binlog_stream(&self, coordinates: &BinlogCoordinates) -> Result<BinlogStream> {
        let mut connection = connect(&self.config).await?;
        let row_value_options = connection
            .query_first::<String, _>("SELECT @@SESSION.binlog_row_value_options")
            .await
            .map_err(mysql_error)?
            .unwrap_or_default();
        if !row_value_options.is_empty() {
            warn!(
                value = %row_value_options,
                "partial JSON row values are enabled; changed JSON fields may be marked unavailable"
            );
        }
        let filename = coordinates.filename.as_bytes().to_vec();
        let mut request = BinlogStreamRequest::new(self.config.server_id)
            .with_filename(&filename)
            .with_pos(coordinates.position);
        if let (Some(filter), Some(gtid_set)) = (
            &self.gtid_source_filter,
            coordinates
                .gtid_set_is_complete
                .then_some(coordinates.gtid_set.as_deref())
                .flatten(),
        ) {
            let filtered_sids = filter.filter_sids(gtid_set)?;
            if filtered_sids.is_empty() {
                warn!(
                    gtid_set,
                    "configured MySQL GTID source filters matched no executed sources; falling back to binlog file and position"
                );
            } else {
                debug!(
                    source_count = filtered_sids.len(),
                    "opening MySQL stream with a filtered GTID set"
                );
                request = request.with_gtid().with_gtid_set(filtered_sids);
            }
        }
        connection
            .get_binlog_stream(request)
            .await
            .map_err(mysql_error)
    }

    fn schema_for_collection(&self, collection: &str) -> Option<TableSchema> {
        self.schemas
            .values()
            .find(|schema| {
                let name = format!("{}.{}", schema.database, schema.table);
                regex::Regex::new(&format!(r"^(?:{collection})$"))
                    .is_ok_and(|pattern| pattern.is_match(&name))
            })
            .cloned()
    }

    async fn read_incremental_chunk(
        &self,
        schema: &TableSchema,
        progress: &IncrementalSnapshotProgress,
    ) -> Result<Vec<MySqlRow>> {
        let qualified = format!(
            "{}.{}",
            quote_identifier(&schema.database),
            quote_identifier(&schema.table)
        );
        let ordering = schema
            .event_schema
            .fields
            .iter()
            .filter(|field| field.primary_key)
            .map(|field| quote_identifier(&field.name))
            .collect::<Vec<_>>();
        let ordering = if ordering.is_empty() {
            schema
                .event_schema
                .fields
                .iter()
                .map(|field| quote_identifier(&field.name))
                .collect::<Vec<_>>()
        } else {
            ordering
        };
        let collection_name = format!("{}.{}", schema.database, schema.table);
        let condition = progress
            .additional_conditions
            .iter()
            .find_map(|(pattern, filter)| {
                regex::Regex::new(&format!(r"^(?:{pattern})$"))
                    .ok()
                    .and_then(|pattern| pattern.is_match(&collection_name).then_some(filter))
            });
        let where_clause =
            condition.map_or_else(String::new, |condition| format!(" WHERE ({condition})"));
        let order_clause = if ordering.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", ordering.join(", "))
        };
        let query = format!(
            "SELECT * FROM {qualified}{where_clause}{order_clause} LIMIT {} OFFSET {}",
            self.config.incremental_snapshot_chunk_size, progress.offset
        );
        let mut connection = connect(&self.config).await?;
        let rows = connection.query(query).await.map_err(mysql_error)?;
        connection.disconnect().await.map_err(mysql_error)?;
        Ok(rows)
    }

    async fn process_binlog_event(
        &mut self,
        event: BinlogEvent,
        stream: &BinlogStream,
        state: &mut StreamingState,
        incremental: &mut MySqlIncrementalSnapshot,
        context: &mut SourceContext,
    ) -> Result<Option<SourcePosition>> {
        let header = event.header();
        let event_start =
            u64::from(header.log_pos()).saturating_sub(u64::from(header.event_size()));
        let source_time = mysql_source_time(header.timestamp());
        let Some(data) = event.read_data().map_err(mysql_error)? else {
            return Ok(None);
        };

        let records = match data {
            EventData::RotateEvent(rotate) => {
                state.rotate(rotate.name().into_owned());
                Vec::new()
            }
            EventData::TableMapEvent(table_map) => {
                state.register_table_map(table_map.table_id(), event_start);
                Vec::new()
            }
            EventData::GtidEvent(gtid) => {
                let source_uuid = uuid::Uuid::from_bytes(gtid.sid()).to_string();
                let ignore_dml = self.config.gtid_source_filter_dml_events
                    && self
                        .gtid_source_filter
                        .as_ref()
                        .is_some_and(|filter| !filter.matches(&source_uuid));
                state.begin_gtid(&gtid, source_time, ignore_dml);
                Vec::new()
            }
            EventData::RowsEvent(rows) => {
                let table_map = stream.get_tme(rows.table_id()).ok_or_else(|| {
                    Error::Source(format!(
                        "missing TABLE_MAP_EVENT for MySQL table id {} at {}:{}",
                        rows.table_id(),
                        state.current_filename,
                        event_start
                    ))
                })?;
                let signal_table = signal_table_key(&self.config);
                let table_name = (
                    table_map.database_name().into_owned(),
                    table_map.table_name().into_owned(),
                );
                if signal_channel_enabled(&self.config, "source")
                    && signal_table.as_ref() == Some(&table_name)
                {
                    let schema = self.schemas.get(&table_name).cloned().ok_or_else(|| {
                        Error::Source(format!(
                            "MySQL signal table {}.{} is not available in schema history",
                            table_name.0, table_name.1
                        ))
                    })?;
                    for pair in rows.rows(table_map) {
                        let (_before, after) = pair.map_err(mysql_error)?;
                        let Some(after) = after else { continue };
                        let signal_row = convert_binlog_row(&after, &schema.event_schema, None);
                        let signal = MySqlIncrementalSnapshot::parse_row(&signal_row)?;
                        let base = mysql_position(
                            &state.current_filename,
                            event_start,
                            state
                                .transaction
                                .as_ref()
                                .map(|transaction| transaction.id.clone()),
                            header.server_id(),
                            0,
                            false,
                        );
                        incremental
                            .handle_signal(signal, self, &base, &context.output)
                            .await?;
                    }
                    let signal_position = mysql_position(
                        &state.current_filename,
                        event_start,
                        state
                            .transaction
                            .as_ref()
                            .map(|transaction| transaction.id.clone()),
                        header.server_id(),
                        1,
                        false,
                    );
                    return Ok(Some(
                        incremental
                            .latest_position(&signal_position)?
                            .unwrap_or(signal_position),
                    ));
                }
                state.convert_rows(
                    &rows,
                    table_map,
                    event_start,
                    header.server_id(),
                    source_time,
                    &self.connector_name,
                    &self.config,
                )?
            }
            EventData::XidEvent(xid) => state
                .commit_record(event_start, header.server_id(), Some(xid.xid.to_string()))
                .into_iter()
                .collect(),
            EventData::QueryEvent(query) => {
                let query_text = query.query().into_owned();
                let current_database = query.schema().into_owned();
                let schema_change = is_schema_change(&query_text);
                let mut record =
                    state.handle_query(&query_text, event_start, header.server_id(), source_time);
                if schema_change && let Some(record) = &mut record {
                    if let Err(error) = apply_ddl(
                        &mut state.schemas,
                        &query_text,
                        &current_database,
                        &self.config,
                        &self.connector_name,
                    ) {
                        if self.config.schema_history_skip_unparseable_ddl {
                            warn!(ddl = %query_text, %error, "skipping unparseable MySQL DDL");
                        } else {
                            return Err(error);
                        }
                    }
                    self.schemas.clone_from(&state.schemas);
                    record.connector_state = Some(encode_schema_history(&state.schemas)?);
                }
                record.into_iter().collect()
            }
            _ => Vec::new(),
        };

        let mut last_position = None;
        for record in records {
            last_position = Some(record.position.clone());
            context
                .output
                .send(Ok(record))
                .await
                .map_err(|_| Error::Cancelled)?;
        }
        Ok(last_position)
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_binlog_stream(
        &mut self,
        stream: &mut BinlogStream,
        state: &mut StreamingState,
        incremental: &mut MySqlIncrementalSnapshot,
        context: &mut SourceContext,
        last_safe_position: &mut Option<SourcePosition>,
        reconnect_attempts: &mut u32,
        heartbeat_connection: &mut Option<Conn>,
    ) -> Result<Option<Error>> {
        let mut heartbeat = heartbeat_timer(self.config.heartbeat_interval);
        let mut file_signal_poll = file_signal_timer(&self.config);
        if incremental
            .progress()
            .is_some_and(|progress| !progress.paused)
        {
            if let Some(base) = last_safe_position.clone() {
                incremental
                    .run_to_completion(self, &base, &context.output)
                    .await?;
                if let Some(position) = incremental.latest_position(&base)? {
                    *last_safe_position = Some(position);
                }
            }
        }
        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => return Ok(None),
                () = next_file_signal_poll(&mut file_signal_poll),
                    if signal_channel_enabled(&self.config, "file") && state.transaction.is_none() => {
                    for line in crate::file_signal::read_and_clear(&self.config.signal_file).await? {
                        let record = match serde_json::from_str::<SignalRecord>(&line) {
                            Ok(record) => record,
                            Err(error) => { warn!(%error, "invalid MySQL file signal ignored"); continue; }
                        };
                        let signal = match MySqlIncrementalSnapshot::parse_external_record(&record) {
                            Ok(signal) => signal,
                            Err(error) => { warn!(%error, "invalid MySQL file signal ignored"); continue; }
                        };
                        let base = last_safe_position.clone().unwrap_or_else(|| mysql_position(
                            &state.current_filename, 4, None, self.source_server_id, 0, false,
                        ));
                        incremental.handle_signal(signal, self, &base, &context.output).await?;
                        if let Some(position) = incremental.latest_position(&base)? {
                            *last_safe_position = Some(position);
                        }
                    }
                    emit_incremental_checkpoint(
                        &context.output, &self.schemas, incremental.progress(),
                        last_safe_position.as_ref(), None,
                    ).await?;
                }
                delivery = context.signals.recv(),
                    if signal_channel_enabled(&self.config, "in-process") && state.transaction.is_none() => {
                    let delivery = delivery.ok_or_else(|| Error::Source("MySQL runtime signal channel closed".into()))?;
                    let signal = match MySqlIncrementalSnapshot::parse_external_record(delivery.record()) {
                        Ok(signal) => signal,
                        Err(error) => { warn!(%error, "invalid MySQL runtime signal ignored"); delivery.acknowledge(); continue; }
                    };
                    let base = last_safe_position.clone().unwrap_or_else(|| mysql_position(
                        &state.current_filename, 4, None, self.source_server_id, 0, false,
                    ));
                    incremental.handle_signal(signal, self, &base, &context.output).await?;
                    if let Some(position) = incremental.latest_position(&base)? {
                        *last_safe_position = Some(position);
                    }
                    emit_incremental_checkpoint(
                        &context.output, &self.schemas, incremental.progress(),
                        last_safe_position.as_ref(), delivery.into_acknowledgement(),
                    ).await?;
                }
                changed = context.acknowledged.changed() => {
                    if changed.is_err() {
                        return Err(Error::Cancelled);
                    }
                }
                event = stream.next() => {
                    let event = match event {
                        Some(Ok(event)) => event,
                        Some(Err(error)) => return Ok(Some(mysql_error(error))),
                        None => {
                            return Ok(Some(Error::Source(
                                "MySQL binary log stream ended unexpectedly".into(),
                            )));
                        }
                    };
                    if let Some(position) = self
                        .process_binlog_event(event, stream, state, incremental, context)
                        .await?
                    {
                        *last_safe_position = Some(position);
                        *reconnect_attempts = 0;
                    }
                }
                () = next_heartbeat(&mut heartbeat) => {
                    if let Some(query) = self.config.heartbeat_action_query.clone() {
                        let connection = heartbeat_connection.take().ok_or_else(|| {
                            Error::Source(
                                "MySQL heartbeat action connection is unavailable".into(),
                            )
                        })?;
                        *heartbeat_connection = Some(execute_heartbeat_action(connection, query).await?);
                    }
                    if let Some(position) = last_safe_position.clone() {
                        let database = self.config.databases.first().map_or("", String::as_str);
                        context
                            .output
                            .send(Ok(heartbeat_record(
                                &self.connector_name,
                                database,
                                position,
                            )))
                            .await
                            .map_err(|_| Error::Cancelled)?;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SourceConnector for MySqlSource {
    fn source_type(&self) -> &'static str {
        "mysql"
    }

    async fn validate(&mut self) -> Result<()> {
        self.validate_source().await
    }

    async fn run(&mut self, mut context: SourceContext) -> Result<()> {
        self.gtid_source_filter = GtidSourceFilter::from_config(&self.config)?;
        let checkpoint = context.initial_checkpoint.clone();
        let snapshot_needed = match self.snapshot.mode {
            SnapshotMode::Never => false,
            SnapshotMode::Initial | SnapshotMode::WhenNeeded => checkpoint
                .as_ref()
                .is_none_or(|checkpoint| !checkpoint.snapshot_completed),
        };

        let mut resume_position = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.source_position.clone());
        if resume_position
            .as_ref()
            .is_some_and(|position| !matches!(position, SourcePosition::MySql(_)))
        {
            return Err(Error::State(
                "MySQL connector cannot resume from a PostgreSQL checkpoint".into(),
            ));
        }

        let mut incremental_progress = None;
        let checkpoint_has_schema_history = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.connector_state.as_ref())
            .is_some();
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
                    "MySQL checkpoint predates persistent schema history and cannot safely replay destructive DDL; reset the checkpoint and run a new initial snapshot"
                        .into(),
                ));
            }
        }

        let coordinates = if snapshot_needed {
            let outcome = self.run_snapshot(&context.output).await?;
            self.schemas = outcome.schemas;
            resume_position = None;
            outcome.coordinates
        } else if let Some(SourcePosition::MySql(position)) = &resume_position {
            BinlogCoordinates {
                filename: position.binlog_filename.clone(),
                position: position.binlog_position,
                gtid_set: position.gtid_set.clone(),
                gtid_set_is_complete: position.snapshot,
                source_server_id: position.server_id,
            }
        } else {
            self.current_coordinates().await?
        };

        if !snapshot_needed && !checkpoint_has_schema_history {
            let position = resume_position.clone().unwrap_or_else(|| {
                mysql_position(
                    &coordinates.filename,
                    coordinates.position,
                    coordinates.gtid_set.clone(),
                    coordinates.source_server_id,
                    0,
                    false,
                )
            });
            context
                .output
                .send(Ok(SourceRecord {
                    event: None,
                    position,
                    boundary: RecordBoundary::Heartbeat,
                    connector_state: Some(encode_schema_history(&self.schemas)?),
                    signal_acknowledgements: Vec::new(),
                }))
                .await
                .map_err(|_| Error::Cancelled)?;
        }

        let mut state = StreamingState::new(
            coordinates.filename.clone(),
            self.schemas.clone(),
            resume_position.clone(),
        );
        let mut heartbeat_connection = open_heartbeat_connection(&self.config).await?;
        let mut incremental = MySqlIncrementalSnapshot::new(incremental_progress);
        let mut stream_coordinates = coordinates.clone();
        let mut last_safe_position = Some(
            resume_position
                .as_ref()
                .filter(|position| {
                    matches!(position, SourcePosition::MySql(position) if !position.snapshot)
                })
                .cloned()
                .unwrap_or_else(|| {
                    mysql_position(
                        &coordinates.filename,
                        coordinates.position,
                        coordinates.gtid_set.clone(),
                        coordinates.source_server_id,
                        0,
                        false,
                    )
                }),
        );
        let mut reconnect_attempts = 0_u32;

        loop {
            let open_result = tokio::select! {
                _ = context.cancellation.cancelled() => return Ok(()),
                result = self.open_binlog_stream(&stream_coordinates) => result,
            };
            let mut stream = match open_result {
                Ok(stream) => stream,
                Err(error) => {
                    if !wait_for_reconnect(
                        &self.config,
                        &mut context,
                        &mut reconnect_attempts,
                        error,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    stream_coordinates = last_safe_position
                        .as_ref()
                        .and_then(binlog_coordinates_from_position)
                        .unwrap_or_else(|| coordinates.clone());
                    state.rewind(&stream_coordinates, last_safe_position.clone());
                    continue;
                }
            };

            info!(
                connector = %self.connector_name,
                file = %stream_coordinates.filename,
                position = stream_coordinates.position,
                reconnect_attempts,
                "MySQL streaming started"
            );

            let failure = self
                .consume_binlog_stream(
                    &mut stream,
                    &mut state,
                    &mut incremental,
                    &mut context,
                    &mut last_safe_position,
                    &mut reconnect_attempts,
                    &mut heartbeat_connection,
                )
                .await?;

            let close_result = stream.close().await.map_err(mysql_error);
            let Some(failure) = failure else {
                close_result?;
                return Ok(());
            };
            if let Err(error) = close_result {
                debug!(%error, "failed to close disconnected MySQL binlog stream");
            }
            if !wait_for_reconnect(&self.config, &mut context, &mut reconnect_attempts, failure)
                .await?
            {
                return Ok(());
            }
            stream_coordinates = last_safe_position
                .as_ref()
                .and_then(binlog_coordinates_from_position)
                .unwrap_or_else(|| coordinates.clone());
            state.rewind(&stream_coordinates, last_safe_position.clone());
        }
    }
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

fn signal_table_key(config: &MySqlSourceConfig) -> Option<(String, String)> {
    let collection = config.signal_data_collection.as_deref()?.trim();
    let (database, table) = collection.split_once('.')?;
    if database.is_empty() || table.is_empty() {
        return None;
    }
    Some((database.to_owned(), table.to_owned()))
}

fn signal_channel_enabled(config: &MySqlSourceConfig, channel: &str) -> bool {
    config
        .signal_enabled_channels
        .iter()
        .any(|enabled| enabled == channel)
}

fn file_signal_timer(config: &MySqlSourceConfig) -> Option<tokio::time::Interval> {
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
            Error::Source(format!("MySQL signal column {name} is not UTF-8: {error}"))
        }),
        Some(value) => Ok(value.to_json("__rustium_unavailable").to_string()),
        None => Err(Error::Source(format!(
            "MySQL signal table is missing column {name:?}"
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
                    "MySQL execute-snapshot signal {id:?} supports only type=incremental"
                )));
            }
            let collections = data
                .get("data-collections")
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if collections.is_empty() {
                return Err(Error::Source(format!(
                    "MySQL execute-snapshot signal {id:?} has no data-collections"
                )));
            }
            for collection in &collections {
                regex::Regex::new(collection).map_err(|error| {
                    Error::Source(format!(
                        "MySQL execute-snapshot signal {id:?} has invalid data-collection pattern {collection:?}: {error}"
                    ))
                })?;
            }
            let mut additional_conditions = BTreeMap::new();
            if let Some(values) = data
                .get("additional-conditions")
                .and_then(serde_json::Value::as_array)
            {
                for value in values {
                    let collection = value
                        .get("data-collection")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| Error::Source(format!("MySQL execute-snapshot signal {id:?} has an invalid additional-condition")))?;
                    let filter = value
                        .get("filter")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| Error::Source(format!("MySQL execute-snapshot signal {id:?} has an invalid additional-condition")))?;
                    regex::Regex::new(collection).map_err(|error| {
                        Error::Source(format!(
                            "MySQL execute-snapshot signal {id:?} has invalid additional-condition collection {collection:?}: {error}"
                        ))
                    })?;
                    additional_conditions.insert(collection.to_owned(), filter.to_owned());
                }
            }
            Ok(SnapshotSignal::Execute {
                id: id.to_owned(),
                data_collections: collections,
                additional_conditions,
            })
        }
        "stop-snapshot" => Ok(SnapshotSignal::Stop {
            id: id.to_owned(),
            data_collections: data_collections(data),
        }),
        "pause-snapshot" => Ok(SnapshotSignal::Pause { id: id.to_owned() }),
        "resume-snapshot" => Ok(SnapshotSignal::Resume { id: id.to_owned() }),
        other => Ok(SnapshotSignal::Unsupported {
            id: id.to_owned(),
            signal_type: other.to_owned(),
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

fn incremental_position(base: &SourcePosition, event_serial: u64) -> Result<SourcePosition> {
    let SourcePosition::MySql(position) = base else {
        return Err(Error::State(
            "MySQL incremental snapshot requires a MySQL position".into(),
        ));
    };
    Ok(SourcePosition::MySql(MySqlPosition {
        binlog_filename: position.binlog_filename.clone(),
        binlog_position: position.binlog_position,
        gtid_set: position.gtid_set.clone(),
        server_id: position.server_id,
        event_serial: position.event_serial.saturating_add(event_serial),
        snapshot: false,
    }))
}

async fn emit_incremental_checkpoint(
    output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    schemas: &HashMap<(String, String), TableSchema>,
    progress: Option<&IncrementalSnapshotProgress>,
    position: Option<&SourcePosition>,
    acknowledgement: Option<SignalAcknowledgement>,
) -> Result<()> {
    let Some(position) = position else {
        return Ok(());
    };
    output
        .send(Ok(SourceRecord {
            event: None,
            position: position.clone(),
            boundary: RecordBoundary::Heartbeat,
            connector_state: Some(encode_connector_state(schemas, progress)?),
            signal_acknowledgements: acknowledgement.into_iter().collect(),
        }))
        .await
        .map_err(|_| Error::Cancelled)
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
            connector: "mysql".into(),
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
    current_filename: String,
    schemas: HashMap<(String, String), TableSchema>,
    table_anchors: HashMap<u64, u64>,
    transaction: Option<ActiveTransaction>,
    previous_position: Option<(String, u64)>,
    event_serial: u64,
    resume_position: Option<SourcePosition>,
}

impl StreamingState {
    fn new(
        current_filename: String,
        schemas: HashMap<(String, String), TableSchema>,
        resume_position: Option<SourcePosition>,
    ) -> Self {
        let transaction = transaction_from_resume(&resume_position);
        Self {
            current_filename,
            schemas,
            table_anchors: HashMap::new(),
            transaction,
            previous_position: None,
            event_serial: 0,
            resume_position,
        }
    }

    fn rewind(&mut self, coordinates: &BinlogCoordinates, resume_position: Option<SourcePosition>) {
        self.current_filename = coordinates.filename.clone();
        self.table_anchors.clear();
        self.transaction = transaction_from_resume(&resume_position);
        self.previous_position = None;
        self.event_serial = 0;
        self.resume_position = resume_position;
    }

    fn rotate(&mut self, filename: String) {
        self.current_filename = filename;
        self.table_anchors.clear();
        self.previous_position = None;
        self.event_serial = 0;
    }

    fn register_table_map(&mut self, table_id: u64, event_start: u64) {
        self.table_anchors.insert(table_id, event_start);
    }

    fn begin_gtid(
        &mut self,
        event: &mysql_async::binlog::events::GtidEvent,
        source_time: Option<DateTime<Utc>>,
        ignore_dml: bool,
    ) {
        let sid = uuid::Uuid::from_bytes(event.sid());
        let id = event.tag().map_or_else(
            || format!("{sid}:{}", event.gno()),
            |tag| format!("{sid}:{tag}:{}", event.gno()),
        );
        self.transaction = Some(ActiveTransaction {
            id,
            source_time,
            total_order: 0,
            collection_order: HashMap::new(),
            ignore_dml,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn convert_rows(
        &mut self,
        rows: &RowsEventData<'_>,
        table_map: &TableMapEvent<'_>,
        event_start: u64,
        source_server_id: u32,
        source_time: Option<DateTime<Utc>>,
        connector_name: &str,
        config: &MySqlSourceConfig,
    ) -> Result<Vec<SourceRecord>> {
        let database = table_map.database_name().into_owned();
        let table = table_map.table_name().into_owned();
        if !config.tables.includes(&database, &table)
            || (!config.databases.is_empty() && !config.databases.contains(&database))
        {
            return Ok(Vec::new());
        }
        if self
            .transaction
            .as_ref()
            .is_some_and(|transaction| transaction.ignore_dml)
        {
            return Ok(Vec::new());
        }

        let key = (database.clone(), table.clone());
        let schema = self.schemas.get(&key).cloned().ok_or_else(|| {
            Error::Source(format!(
                "received an event for unknown MySQL table {database}.{table}"
            ))
        })?;
        let operation = rows_operation(rows);
        let anchor = self
            .table_anchors
            .get(&rows.table_id())
            .copied()
            .unwrap_or(event_start);
        let fallback_transaction_id = format!("{}:{anchor}", self.current_filename);
        let transaction = self.transaction.get_or_insert_with(|| ActiveTransaction {
            id: fallback_transaction_id,
            source_time,
            total_order: 0,
            collection_order: HashMap::new(),
            ignore_dml: false,
        });
        if transaction.source_time.is_none() {
            transaction.source_time = source_time;
        }

        let mut records = Vec::new();
        for pair in rows.rows(table_map) {
            let (before_row, after_row) = pair.map_err(mysql_error)?;
            let mut before = before_row
                .as_ref()
                .map(|row| convert_binlog_row(row, &schema.event_schema, None));
            let mut after = after_row
                .as_ref()
                .map(|row| convert_binlog_row(row, &schema.event_schema, before.as_ref()));
            if let Some(before) = &mut before {
                fill_unavailable(before, None, &schema.event_schema);
            }
            if let Some(after) = &mut after {
                fill_unavailable(after, before.as_ref(), &schema.event_schema);
            }

            let event_serial = self.next_serial(anchor);
            let gtid = self
                .transaction
                .as_ref()
                .map(|transaction| transaction.id.clone());
            let position = mysql_position(
                &self.current_filename,
                anchor,
                gtid,
                source_server_id,
                event_serial,
                false,
            );
            let transaction_metadata = self.transaction.as_mut().map(|transaction| {
                transaction.total_order += 1;
                let collection_order = transaction.collection_order.entry(key.clone()).or_insert(0);
                *collection_order += 1;
                TransactionMetadata {
                    id: transaction.id.clone(),
                    total_order: Some(transaction.total_order),
                    collection_order: Some(*collection_order),
                }
            });
            let event_source_time = self
                .transaction
                .as_ref()
                .and_then(|transaction| transaction.source_time)
                .or(source_time);
            if self.should_skip(&position) {
                continue;
            }

            let event = ChangeEvent {
                id: EventId::deterministic(
                    connector_name,
                    &database,
                    &position,
                    &format!("{database}.{table}"),
                    event_serial,
                ),
                source: source_metadata(connector_name, &database, &table, false, source_server_id),
                position,
                transaction: transaction_metadata,
                operation,
                before,
                after,
                schema: schema.event_schema.clone(),
                source_time: event_source_time,
                observed_time: Utc::now(),
            };
            records.push(SourceRecord::data(event));
        }
        Ok(records)
    }

    fn handle_query(
        &mut self,
        query: &str,
        event_start: u64,
        source_server_id: u32,
        source_time: Option<DateTime<Utc>>,
    ) -> Option<SourceRecord> {
        let normalized = query.trim().trim_end_matches(';').to_ascii_uppercase();
        if matches!(normalized.as_str(), "BEGIN" | "START TRANSACTION") {
            let transaction = self.transaction.get_or_insert_with(|| ActiveTransaction {
                id: format!("{}:{event_start}", self.current_filename),
                source_time,
                total_order: 0,
                collection_order: HashMap::new(),
                ignore_dml: false,
            });
            if transaction.source_time.is_none() {
                transaction.source_time = source_time;
            }
            return None;
        }
        if normalized == "ROLLBACK" {
            self.transaction = None;
            return None;
        }
        if normalized == "COMMIT" || is_schema_change(query) {
            return self.commit_record(event_start, source_server_id, None);
        }
        None
    }

    fn commit_record(
        &mut self,
        event_start: u64,
        source_server_id: u32,
        fallback_id: Option<String>,
    ) -> Option<SourceRecord> {
        let gtid = self
            .transaction
            .as_ref()
            .map(|transaction| transaction.id.clone())
            .or(fallback_id);
        let event_serial = self.next_serial(event_start);
        let position = mysql_position(
            &self.current_filename,
            event_start,
            gtid,
            source_server_id,
            event_serial,
            false,
        );
        self.transaction = None;
        if self.should_skip(&position) {
            return None;
        }
        Some(SourceRecord {
            event: None,
            position,
            boundary: RecordBoundary::TransactionCommit,
            connector_state: None,
            signal_acknowledgements: Vec::new(),
        })
    }

    fn next_serial(&mut self, position: u64) -> u64 {
        let key = (self.current_filename.clone(), position);
        if self.previous_position.as_ref() == Some(&key) {
            self.event_serial += 1;
        } else {
            self.previous_position = Some(key);
            self.event_serial = 1;
        }
        self.event_serial
    }

    fn should_skip(&mut self, position: &SourcePosition) -> bool {
        let Some(resume) = &self.resume_position else {
            return false;
        };
        if position.is_at_or_before(resume) {
            debug!(?position, ?resume, "skipping replayed MySQL event");
            true
        } else {
            self.resume_position = None;
            false
        }
    }
}

fn transaction_from_resume(resume_position: &Option<SourcePosition>) -> Option<ActiveTransaction> {
    match resume_position {
        Some(SourcePosition::MySql(position)) => (!position.snapshot)
            .then_some(position.gtid_set.as_ref())
            .flatten()
            .map(|gtid| ActiveTransaction {
                id: gtid.clone(),
                source_time: None,
                total_order: 0,
                collection_order: HashMap::new(),
                ignore_dml: false,
            }),
        _ => None,
    }
}

fn binlog_coordinates_from_position(position: &SourcePosition) -> Option<BinlogCoordinates> {
    match position {
        SourcePosition::MySql(position) => Some(BinlogCoordinates {
            filename: position.binlog_filename.clone(),
            position: position.binlog_position,
            gtid_set: position.gtid_set.clone(),
            gtid_set_is_complete: position.snapshot,
            source_server_id: position.server_id,
        }),
        SourcePosition::Postgres(_) | SourcePosition::SqlServer(_) => None,
    }
}

async fn wait_for_reconnect(
    config: &MySqlSourceConfig,
    context: &mut SourceContext,
    attempts: &mut u32,
    error: Error,
) -> Result<bool> {
    if !config.connect_keep_alive {
        return Err(error);
    }
    if *attempts >= config.reconnect_max_attempts {
        return Err(Error::Source(format!(
            "MySQL reconnect budget exhausted after {} attempts; last error: {error}",
            config.reconnect_max_attempts
        )));
    }
    *attempts += 1;
    warn!(
        attempt = *attempts,
        max_attempts = config.reconnect_max_attempts,
        delay_ms = config.connect_keep_alive_interval.as_millis(),
        %error,
        "MySQL binlog stream disconnected; scheduling reconnect"
    );

    let delay = tokio::time::sleep(config.connect_keep_alive_interval);
    tokio::pin!(delay);
    loop {
        tokio::select! {
            _ = context.cancellation.cancelled() => return Ok(false),
            changed = context.acknowledged.changed() => {
                if changed.is_err() {
                    return Err(Error::Cancelled);
                }
            }
            () = &mut delay => return Ok(true),
        }
    }
}

async fn connect(config: &MySqlSourceConfig) -> Result<Conn> {
    let builder = OptsBuilder::default()
        .ip_or_hostname(config.hostname.clone())
        .tcp_port(config.port)
        .user(Some(config.username.clone()))
        .pass(Some(config.password.clone()))
        .prefer_socket(false);
    let builder = match config.ssl_mode.as_str() {
        "disabled" => builder,
        "preferred" => {
            let tls = builder
                .clone()
                .ssl_opts(Some(mysql_tls_options(config, true, true)));
            match connect_with_options(config, tls).await {
                Ok(connection) => return Ok(connection),
                Err(error) => {
                    debug!(%error, "preferred MySQL TLS connection failed; falling back to plaintext");
                    builder
                }
            }
        }
        "required" => builder.ssl_opts(Some(mysql_tls_options(config, true, true))),
        "verify_ca" => builder.ssl_opts(Some(mysql_tls_options(config, true, false))),
        "verify_identity" => builder.ssl_opts(Some(mysql_tls_options(config, false, false))),
        mode => {
            return Err(Error::Configuration(format!(
                "unsupported MySQL database.ssl.mode {mode:?}"
            )));
        }
    };
    connect_with_options(config, builder).await
}

fn mysql_tls_options(
    config: &MySqlSourceConfig,
    skip_domain_validation: bool,
    accept_invalid_certs: bool,
) -> SslOpts {
    let mut options = SslOpts::default();
    if let Some(ca) = &config.ssl_ca {
        options = options.with_root_certs(vec![PathBuf::from(ca).into()]);
    }
    if let (Some(cert), Some(key)) = (&config.ssl_cert, &config.ssl_key) {
        options = options.with_client_identity(Some(ClientIdentity::new(
            PathBuf::from(cert).into(),
            PathBuf::from(key).into(),
        )));
    }
    options
        .with_danger_skip_domain_validation(skip_domain_validation)
        .with_danger_accept_invalid_certs(accept_invalid_certs)
}

async fn open_heartbeat_connection(config: &MySqlSourceConfig) -> Result<Option<Conn>> {
    if config.heartbeat_interval.is_zero() || config.heartbeat_action_query.is_none() {
        return Ok(None);
    }
    Ok(Some(connect(config).await?))
}

async fn execute_heartbeat_action(mut connection: Conn, query: String) -> Result<Conn> {
    connection
        .query_drop(query)
        .await
        .map_err(|error| Error::Source(format!("MySQL heartbeat.action.query failed: {error}")))?;
    Ok(connection)
}

async fn connect_with_options(config: &MySqlSourceConfig, builder: OptsBuilder) -> Result<Conn> {
    let opts = Opts::from(builder);
    tokio::time::timeout(config.connect_timeout, Conn::new(opts))
        .await
        .map_err(|_| Error::Source("timed out connecting to MySQL".into()))?
        .map_err(mysql_error)
}

async fn current_binlog_coordinates(
    connection: &mut Conn,
    source_server_id: u32,
) -> Result<BinlogCoordinates> {
    let row = match connection
        .query_first::<MySqlRow, _>("SHOW BINARY LOG STATUS")
        .await
    {
        Ok(row) => row,
        Err(_) => connection
            .query_first::<MySqlRow, _>("SHOW MASTER STATUS")
            .await
            .map_err(mysql_error)?,
    }
    .ok_or_else(|| Error::Source("MySQL returned no current binary log status".into()))?;
    let filename = row
        .get::<String, _>(0)
        .ok_or_else(|| Error::Source("MySQL binary log status has no filename".into()))?;
    let position = row
        .get::<u64, _>(1)
        .ok_or_else(|| Error::Source("MySQL binary log status has no position".into()))?;
    let gtid_set = if row.len() > 4 {
        row.get::<String, _>(4).filter(|value| !value.is_empty())
    } else {
        connection
            .query_first::<String, _>("SELECT @@GLOBAL.gtid_executed")
            .await
            .map_err(mysql_error)?
            .filter(|value| !value.is_empty())
    };
    Ok(BinlogCoordinates {
        filename,
        position,
        gtid_set,
        gtid_set_is_complete: true,
        source_server_id,
    })
}

async fn discover_tables(
    connection: &mut Conn,
    config: &MySqlSourceConfig,
    connector_name: &str,
) -> Result<HashMap<(String, String), TableSchema>> {
    let tables: Vec<(String, String)> = connection
        .query(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_TYPE = 'BASE TABLE' \
             AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY TABLE_SCHEMA, TABLE_NAME",
        )
        .await
        .map_err(mysql_error)?;
    let mut schemas = HashMap::new();
    for (database, table) in tables {
        let is_signal =
            signal_table_key(config).as_ref() == Some(&(database.clone(), table.clone()));
        if (!config.databases.is_empty() && !config.databases.contains(&database))
            || (!config.tables.includes(&database, &table) && !is_signal)
        {
            continue;
        }
        let schema = discover_table_schema(connection, connector_name, &database, &table).await?;
        schemas.insert(schema.key(), schema);
    }
    Ok(schemas)
}

async fn discover_table_schema(
    connection: &mut Conn,
    connector_name: &str,
    database: &str,
    table: &str,
) -> Result<TableSchema> {
    let columns: Vec<(String, String, String, String)> = connection
        .exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .map_err(mysql_error)?;
    if columns.is_empty() {
        return Err(Error::Source(format!(
            "could not discover columns for {database}.{table}"
        )));
    }
    let fields = columns
        .into_iter()
        .map(|(name, type_name, nullable, key)| FieldSchema {
            name,
            type_name,
            optional: nullable == "YES",
            primary_key: key == "PRI",
        })
        .collect();
    Ok(TableSchema {
        database: database.into(),
        table: table.into(),
        event_schema: EventSchema {
            name: format!("{connector_name}.{database}.{table}.Envelope"),
            version: 1,
            fields,
        },
    })
}

#[allow(clippy::too_many_arguments)]
async fn snapshot_table(
    connection: &mut Conn,
    connector_name: &str,
    schema: &TableSchema,
    coordinates: &BinlogCoordinates,
    fetch_size: usize,
    ordinal: &mut u64,
    output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
) -> Result<()> {
    let qualified = format!(
        "{}.{}",
        quote_identifier(&schema.database),
        quote_identifier(&schema.table)
    );
    let primary_key = schema
        .event_schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .map(|field| quote_identifier(&field.name))
        .collect::<Vec<_>>();
    let ordering_columns = if primary_key.is_empty() {
        schema
            .event_schema
            .fields
            .iter()
            .filter(|field| base_type(&field.type_name) != "geometry")
            .map(|field| quote_identifier(&field.name))
            .collect::<Vec<_>>()
    } else {
        primary_key
    };
    let ordering = if ordering_columns.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", ordering_columns.join(", "))
    };

    let mut offset = 0_usize;
    loop {
        let query =
            format!("SELECT * FROM {qualified}{ordering} LIMIT {fetch_size} OFFSET {offset}");
        let rows: Vec<MySqlRow> = connection.query(query).await.map_err(mysql_error)?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        for row in rows {
            *ordinal += 1;
            let position = mysql_position(
                &coordinates.filename,
                coordinates.position,
                coordinates.gtid_set.clone(),
                coordinates.source_server_id,
                *ordinal,
                true,
            );
            let event = ChangeEvent {
                id: EventId::deterministic(
                    connector_name,
                    &schema.database,
                    &position,
                    &format!("{}.{}", schema.database, schema.table),
                    *ordinal,
                ),
                source: source_metadata(
                    connector_name,
                    &schema.database,
                    &schema.table,
                    true,
                    coordinates.source_server_id,
                ),
                position,
                transaction: None,
                operation: Operation::Read,
                before: None,
                after: Some(convert_snapshot_row(row, &schema.event_schema)?),
                schema: schema.event_schema.clone(),
                source_time: None,
                observed_time: Utc::now(),
            };
            output
                .send(Ok(SourceRecord::data(event)))
                .await
                .map_err(|_| Error::Cancelled)?;
        }
        offset += row_count;
        if row_count < fetch_size {
            break;
        }
    }
    info!(table = %format!("{}.{}", schema.database, schema.table), rows = offset, "snapshot table completed");
    Ok(())
}

fn convert_snapshot_row(row: MySqlRow, schema: &EventSchema) -> Result<Row> {
    let values = row.unwrap();
    if values.len() != schema.fields.len() {
        return Err(Error::Source(format!(
            "snapshot row has {} values but schema has {} fields",
            values.len(),
            schema.fields.len()
        )));
    }
    Ok(values
        .into_iter()
        .zip(&schema.fields)
        .map(|(value, field)| (field.name.clone(), convert_value(&value, &field.type_name)))
        .collect())
}

fn convert_binlog_row(row: &BinlogRow, schema: &EventSchema, base: Option<&Row>) -> Row {
    row.columns_ref()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let raw_name = column.name_str();
            let field = raw_name
                .strip_prefix('@')
                .and_then(|index| index.parse::<usize>().ok())
                .and_then(|index| schema.fields.get(index))
                .or_else(|| schema.fields.iter().find(|field| field.name == raw_name));
            let field = field?;
            let value = row.as_ref(index)?;
            Some((
                field.name.clone(),
                convert_binlog_value(
                    value,
                    &field.type_name,
                    base.and_then(|base| base.get(&field.name)),
                ),
            ))
        })
        .collect()
}

fn convert_binlog_value(
    value: &BinlogValue<'_>,
    type_name: &str,
    base: Option<&DataValue>,
) -> DataValue {
    match value {
        BinlogValue::Value(value) => convert_value(value, type_name),
        BinlogValue::Jsonb(value) => serde_json::Value::try_from(value.clone())
            .map_or(DataValue::Unavailable, DataValue::Json),
        BinlogValue::JsonDiff(diffs) => apply_json_diffs(base, diffs),
    }
}

fn apply_json_diffs(
    base: Option<&DataValue>,
    diffs: &[mysql_async::binlog::jsondiff::JsonDiff<'_>],
) -> DataValue {
    let Some(DataValue::Json(mut value)) = base.cloned() else {
        return DataValue::Unavailable;
    };
    for diff in diffs {
        let Some(path) = parse_json_path(diff.path_str().as_ref()) else {
            return DataValue::Unavailable;
        };
        let replacement = diff
            .value()
            .and_then(|value| serde_json::Value::try_from(value.clone()).ok());
        let applied = match diff.operation() {
            mysql_async::binlog::jsondiff::JsonDiffOperation::REPLACE => replacement
                .is_some_and(|replacement| set_json_path(&mut value, &path, replacement, false)),
            mysql_async::binlog::jsondiff::JsonDiffOperation::INSERT => replacement
                .is_some_and(|replacement| set_json_path(&mut value, &path, replacement, true)),
            mysql_async::binlog::jsondiff::JsonDiffOperation::REMOVE => {
                remove_json_path(&mut value, &path)
            }
        };
        if !applied {
            return DataValue::Unavailable;
        }
    }
    DataValue::Json(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonPathSegment {
    Key(String),
    Index(usize),
}

fn parse_json_path(path: &str) -> Option<Vec<JsonPathSegment>> {
    let mut chars = path.chars().peekable();
    if chars.next()? != '$' {
        return None;
    }
    let mut segments = Vec::new();
    while let Some(character) = chars.next() {
        match character {
            '.' => {
                let mut key = String::new();
                if chars.peek() == Some(&'"') {
                    chars.next();
                    while let Some(character) = chars.next() {
                        match character {
                            '"' => break,
                            '\\' => key.push(chars.next()?),
                            character => key.push(character),
                        }
                    }
                } else {
                    while let Some(&character) = chars.peek() {
                        if character == '.' || character == '[' {
                            break;
                        }
                        key.push(character);
                        chars.next();
                    }
                }
                (!key.is_empty()).then_some(())?;
                segments.push(JsonPathSegment::Key(key));
            }
            '[' => {
                let mut value = String::new();
                for character in chars.by_ref() {
                    if character == ']' {
                        break;
                    }
                    value.push(character);
                }
                if value.chars().all(|character| character.is_ascii_digit()) {
                    segments.push(JsonPathSegment::Index(value.parse().ok()?));
                } else if value.starts_with('"') && value.ends_with('"') {
                    segments.push(JsonPathSegment::Key(value[1..value.len() - 1].into()));
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(segments)
}

fn set_json_path(
    value: &mut serde_json::Value,
    path: &[JsonPathSegment],
    replacement: serde_json::Value,
    allow_insert: bool,
) -> bool {
    let Some((segment, rest)) = path.split_first() else {
        *value = replacement;
        return true;
    };
    if rest.is_empty() {
        return match segment {
            JsonPathSegment::Key(key) => value.as_object_mut().is_some_and(|object| {
                if !allow_insert && !object.contains_key(key) {
                    return false;
                }
                if allow_insert && object.contains_key(key) {
                    return false;
                }
                object.insert(key.clone(), replacement);
                true
            }),
            JsonPathSegment::Index(index) => value.as_array_mut().is_some_and(|array| {
                if allow_insert {
                    if *index > array.len() {
                        return false;
                    }
                    array.insert(*index, replacement);
                    true
                } else {
                    array.get_mut(*index).is_some_and(|value| {
                        *value = replacement;
                        true
                    })
                }
            }),
        };
    }
    match segment {
        JsonPathSegment::Key(key) => value
            .as_object_mut()
            .and_then(|object| object.get_mut(key))
            .is_some_and(|value| set_json_path(value, rest, replacement, allow_insert)),
        JsonPathSegment::Index(index) => value
            .as_array_mut()
            .and_then(|array| array.get_mut(*index))
            .is_some_and(|value| set_json_path(value, rest, replacement, allow_insert)),
    }
}

fn remove_json_path(value: &mut serde_json::Value, path: &[JsonPathSegment]) -> bool {
    let Some((segment, rest)) = path.split_first() else {
        return false;
    };
    if rest.is_empty() {
        return match segment {
            JsonPathSegment::Key(key) => value
                .as_object_mut()
                .and_then(|object| object.remove(key))
                .is_some(),
            JsonPathSegment::Index(index) => value.as_array_mut().is_some_and(|array| {
                *index < array.len() && {
                    array.remove(*index);
                    true
                }
            }),
        };
    }
    match segment {
        JsonPathSegment::Key(key) => value
            .as_object_mut()
            .and_then(|object| object.get_mut(key))
            .is_some_and(|value| remove_json_path(value, rest)),
        JsonPathSegment::Index(index) => value
            .as_array_mut()
            .and_then(|array| array.get_mut(*index))
            .is_some_and(|value| remove_json_path(value, rest)),
    }
}

fn convert_value(value: &Value, type_name: &str) -> DataValue {
    match value {
        Value::NULL => DataValue::Null,
        Value::Int(value) => {
            if type_name.to_ascii_lowercase().starts_with("tinyint(1)") {
                DataValue::Boolean(*value != 0)
            } else if i32::try_from(*value).is_ok() {
                DataValue::Int32(*value as i32)
            } else {
                DataValue::Int64(*value)
            }
        }
        Value::UInt(value) => DataValue::UInt64(*value),
        Value::Float(value) => DataValue::Float64(f64::from(*value)),
        Value::Double(value) => DataValue::Float64(*value),
        Value::Bytes(value) => convert_bytes(value, type_name),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            let date = format!("{year:04}-{month:02}-{day:02}");
            if *hour == 0
                && *minute == 0
                && *second == 0
                && *micros == 0
                && base_type(type_name) == "date"
            {
                DataValue::Date(date)
            } else {
                DataValue::Timestamp(format_mysql_datetime(
                    *year, *month, *day, *hour, *minute, *second, *micros,
                ))
            }
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let total_hours = days.saturating_mul(24) + u32::from(*hours);
            let sign = if *negative { "-" } else { "" };
            let fraction = if *micros == 0 {
                String::new()
            } else {
                format!(".{micros:06}")
            };
            DataValue::Time(format!(
                "{sign}{total_hours:02}:{minutes:02}:{seconds:02}{fraction}"
            ))
        }
    }
}

fn convert_bytes(value: &[u8], type_name: &str) -> DataValue {
    let base = base_type(type_name);
    if matches!(
        base,
        "binary"
            | "varbinary"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
            | "bit"
            | "geometry"
    ) {
        return DataValue::Bytes(value.to_vec());
    }
    let Ok(value) = std::str::from_utf8(value) else {
        return DataValue::Bytes(value.to_vec());
    };
    if type_name.to_ascii_lowercase().starts_with("tinyint(1)") {
        return value.parse::<i64>().map_or_else(
            |_| DataValue::String(value.into()),
            |value| DataValue::Boolean(value != 0),
        );
    }
    match base {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" => {
            if type_name.to_ascii_lowercase().contains("unsigned") {
                value
                    .parse::<u64>()
                    .map_or_else(|_| DataValue::String(value.into()), DataValue::UInt64)
            } else {
                value
                    .parse::<i32>()
                    .map_or_else(|_| DataValue::String(value.into()), DataValue::Int32)
            }
        }
        "bigint" => {
            if type_name.to_ascii_lowercase().contains("unsigned") {
                value
                    .parse::<u64>()
                    .map_or_else(|_| DataValue::String(value.into()), DataValue::UInt64)
            } else {
                value
                    .parse::<i64>()
                    .map_or_else(|_| DataValue::String(value.into()), DataValue::Int64)
            }
        }
        "float" | "double" | "real" => value
            .parse::<f64>()
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Float64),
        "decimal" | "numeric" => DataValue::Decimal(value.into()),
        "json" => serde_json::from_str(value)
            .map_or_else(|_| DataValue::String(value.into()), DataValue::Json),
        "date" => DataValue::Date(value.into()),
        "time" => DataValue::Time(value.into()),
        "datetime" => DataValue::Timestamp(value.into()),
        "timestamp" => mysql_timestamp(value)
            .map(DataValue::Timestamp)
            .unwrap_or_else(|| DataValue::Timestamp(value.into())),
        _ => DataValue::String(value.into()),
    }
}

fn fill_unavailable(row: &mut Row, fallback: Option<&Row>, schema: &EventSchema) {
    for field in &schema.fields {
        if !row.contains_key(&field.name) {
            row.insert(
                field.name.clone(),
                fallback
                    .and_then(|row| row.get(&field.name))
                    .cloned()
                    .unwrap_or(DataValue::Unavailable),
            );
        }
    }
}

fn rows_operation(rows: &RowsEventData<'_>) -> Operation {
    match rows {
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => Operation::Create,
        RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::UpdateRowsEvent(_)
        | RowsEventData::PartialUpdateRowsEvent(_) => Operation::Update,
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
            Operation::Delete
        }
    }
}

fn mysql_position(
    filename: &str,
    position: u64,
    gtid_set: Option<String>,
    server_id: u32,
    event_serial: u64,
    snapshot: bool,
) -> SourcePosition {
    SourcePosition::MySql(MySqlPosition {
        binlog_filename: filename.into(),
        binlog_position: position,
        gtid_set,
        server_id,
        event_serial,
        snapshot,
    })
}

fn source_metadata(
    connector_name: &str,
    database: &str,
    table: &str,
    snapshot: bool,
    server_id: u32,
) -> SourceMetadata {
    let mut attributes = BTreeMap::new();
    attributes.insert("server_id".into(), server_id.into());
    SourceMetadata {
        connector: "mysql".into(),
        connector_name: connector_name.into(),
        database: database.into(),
        schema: None,
        table: Some(table.into()),
        snapshot,
        version: CONNECTOR_VERSION.into(),
        attributes,
    }
}

fn mysql_source_time(timestamp: u32) -> Option<DateTime<Utc>> {
    (timestamp != 0)
        .then(|| Utc.timestamp_opt(i64::from(timestamp), 0).single())
        .flatten()
}

fn mysql_timestamp(value: &str) -> Option<String> {
    let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
    let seconds = seconds.parse::<i64>().ok()?;
    let mut micros = fraction.chars().take(6).collect::<String>();
    while micros.len() < 6 {
        micros.push('0');
    }
    let micros = if micros.is_empty() {
        0
    } else {
        micros.parse::<u32>().ok()?
    };
    let timestamp = Utc.timestamp_opt(seconds, micros * 1_000).single()?;
    let format = if micros == 0 {
        "%Y-%m-%d %H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S%.6f"
    };
    Some(timestamp.format(format).to_string())
}

fn is_schema_change(query: &str) -> bool {
    let query = query.trim_start().to_ascii_uppercase();
    [
        "ALTER TABLE",
        "CREATE TABLE",
        "DROP TABLE",
        "RENAME TABLE",
        "TRUNCATE TABLE",
    ]
    .iter()
    .any(|prefix| query.starts_with(prefix))
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn base_type(type_name: &str) -> &str {
    type_name
        .split(['(', ' '])
        .next()
        .unwrap_or(type_name)
        .trim()
}

fn format_mysql_datetime(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    if micros == 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}")
    }
}

fn mysql_error(error: impl std::fmt::Display) -> Error {
    Error::Source(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use mysql_async::binlog::events::GtidEvent;
    use rustium_config::TableSelection;
    use rustium_core::{Checkpoint, ConnectorIdentity};
    use tokio::sync::{mpsc, watch};
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn test_mysql_config() -> MySqlSourceConfig {
        MySqlSourceConfig {
            hostname: "localhost".into(),
            port: 3306,
            username: "rustium".into(),
            password: "secret".into(),
            databases: vec!["inventory".into()],
            server_id: 5_401,
            tables: TableSelection::default(),
            ssl_mode: "disabled".into(),
            ssl_ca: None,
            ssl_cert: None,
            ssl_key: None,
            connect_timeout: std::time::Duration::from_secs(1),
            connect_keep_alive: true,
            connect_keep_alive_interval: std::time::Duration::from_secs(1),
            reconnect_max_attempts: 1,
            schema_history_skip_unparseable_ddl: false,
            gtid_source_includes: Vec::new(),
            gtid_source_excludes: Vec::new(),
            gtid_source_filter_dml_events: true,
            heartbeat_interval: std::time::Duration::ZERO,
            heartbeat_action_query: None,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            signal_data_collection: None,
            signal_enabled_channels: vec!["source".into(), "file".into(), "in-process".into()],
            signal_file: "signals.jsonl".into(),
            signal_poll_interval: std::time::Duration::from_millis(500),
            incremental_snapshot_chunk_size: 1_024,
            incremental_snapshot_watermarking_strategy: "insert_insert".into(),
        }
    }

    #[test]
    fn builds_heartbeat_at_the_safe_mysql_position() {
        let position = mysql_position(
            "mysql-bin.000004",
            512,
            Some("8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850:42".into()),
            184,
            0,
            false,
        );
        let record = heartbeat_record("inventory-mysql", "inventory", position.clone());
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
    fn converts_mysql_scalar_types() {
        assert_eq!(
            convert_bytes(b"12.30", "decimal(10,2)"),
            DataValue::Decimal("12.30".into())
        );
        assert_eq!(
            convert_bytes(b"42", "bigint unsigned"),
            DataValue::UInt64(42)
        );
        assert_eq!(
            convert_value(&Value::Int(1), "tinyint(1)"),
            DataValue::Boolean(true)
        );
        assert_eq!(convert_bytes(b"1", "tinyint(1)"), DataValue::Boolean(true));
        assert_eq!(
            mysql_timestamp("1784114317.113641"),
            Some("2026-07-15 11:18:37.113641".into())
        );
    }

    #[test]
    fn applies_mysql_json_diff_paths_without_partial_state() {
        assert_eq!(
            parse_json_path("$.customer.address[0].city"),
            Some(vec![
                JsonPathSegment::Key("customer".into()),
                JsonPathSegment::Key("address".into()),
                JsonPathSegment::Index(0),
                JsonPathSegment::Key("city".into()),
            ])
        );
        let mut value = serde_json::json!({"customer": {"name": "Alice", "tags": ["new"]}});
        let path = parse_json_path("$.customer.name").unwrap();
        assert!(set_json_path(
            &mut value,
            &path,
            serde_json::json!("Bob"),
            false
        ));
        let path = parse_json_path("$.customer.tags[1]").unwrap();
        assert!(set_json_path(
            &mut value,
            &path,
            serde_json::json!("vip"),
            true
        ));
        let path = parse_json_path("$.customer.name").unwrap();
        assert!(remove_json_path(&mut value, &path));
        assert_eq!(
            value,
            serde_json::json!({"customer": {"tags": ["new", "vip"]}})
        );
    }

    #[test]
    fn detects_mysql_schema_changes() {
        assert!(is_schema_change("ALTER TABLE orders ADD COLUMN note text"));
        assert!(is_schema_change(" truncate table orders"));
        assert!(!is_schema_change("BEGIN"));
    }

    #[test]
    fn matches_included_mysql_gtid_sources_by_uuid_and_regex() {
        let exact_uuid = "8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850";
        let mut config = test_mysql_config();
        config.gtid_source_includes = vec![exact_uuid.into()];
        let filter = GtidSourceFilter::from_config(&config).unwrap().unwrap();
        assert!(filter.matches(exact_uuid));
        assert!(filter.matches(&exact_uuid.to_ascii_uppercase()));
        assert!(!filter.matches("8f5f4a9a-6b2d-4dd5-915e-1df9d53d2851"));

        config.gtid_source_includes = vec![r"8f5f4a9a-[0-9a-f-]+".into()];
        let filter = GtidSourceFilter::from_config(&config).unwrap().unwrap();
        assert!(filter.matches(exact_uuid));
        assert!(!filter.matches(&format!("prefix-{exact_uuid}")));
    }

    #[test]
    fn excludes_matching_mysql_gtid_sources_and_filters_the_executed_set() {
        let excluded_uuid = "8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850";
        let retained_uuid = "2f6f4a9a-6b2d-4dd5-915e-1df9d53d2850";
        let mut config = test_mysql_config();
        config.gtid_source_excludes = vec![excluded_uuid.into()];
        let filter = GtidSourceFilter::from_config(&config).unwrap().unwrap();
        assert!(!filter.matches(excluded_uuid));
        assert!(filter.matches(retained_uuid));

        let sids = filter
            .filter_sids(&format!("{excluded_uuid}:1-8,\n{retained_uuid}:1-3:7"))
            .unwrap();
        assert_eq!(sids.len(), 1);
        assert_eq!(
            uuid::Uuid::from_bytes(sids[0].uuid()).to_string(),
            retained_uuid
        );
        assert_eq!(sids[0].to_string(), format!("{retained_uuid}:1-3:7"));
    }

    #[test]
    fn filtered_gtid_transactions_preserve_commit_progress() {
        let source_uuid = uuid::Uuid::parse_str("8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850").unwrap();
        let event = GtidEvent::new(*source_uuid.as_bytes(), 42);
        let mut state = StreamingState::new("mysql-bin.000001".into(), HashMap::new(), None);

        state.begin_gtid(&event, None, true);
        assert!(
            state
                .transaction
                .as_ref()
                .is_some_and(|transaction| transaction.ignore_dml)
        );

        let commit = state.commit_record(512, 184, None).unwrap();
        assert_eq!(commit.boundary, RecordBoundary::TransactionCommit);
        assert!(matches!(
            commit.position,
            SourcePosition::MySql(position)
                if position.gtid_set.as_deref()
                    == Some("8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850:42")
        ));
        assert!(state.transaction.is_none());
    }

    #[test]
    fn rewinds_streaming_state_to_a_safe_position() {
        let position = mysql_position(
            "mysql-bin.000002",
            128,
            Some("8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850:42".into()),
            223_344,
            2,
            false,
        );
        let coordinates = binlog_coordinates_from_position(&position).unwrap();
        assert!(!coordinates.gtid_set_is_complete);
        let mut state = StreamingState::new("mysql-bin.000001".into(), HashMap::new(), None);
        state.table_anchors.insert(7, 64);
        state.previous_position = Some(("mysql-bin.000001".into(), 64));
        state.event_serial = 3;

        state.rewind(&coordinates, Some(position.clone()));

        assert_eq!(state.current_filename, "mysql-bin.000002");
        assert!(state.table_anchors.is_empty());
        assert_eq!(state.previous_position, None);
        assert_eq!(state.event_serial, 0);
        assert_eq!(state.resume_position, Some(position));
        assert_eq!(
            state
                .transaction
                .as_ref()
                .map(|transaction| transaction.id.as_str()),
            Some("8f5f4a9a-6b2d-4dd5-915e-1df9d53d2850:42")
        );
    }

    #[tokio::test]
    async fn rejects_legacy_mysql_checkpoint_without_schema_history() {
        let config = test_mysql_config();
        let mut source = MySqlSource::new(
            "inventory-mysql",
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
            },
        );
        let position = mysql_position("mysql-bin.000001", 128, None, 184, 1, false);
        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: "inventory-mysql".into(),
            generation: ConnectorIdentity::new("inventory-mysql").generation,
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

    #[test]
    fn parses_debezium_snapshot_control_signals() {
        let signal = MySqlIncrementalSnapshot::parse_external_record(&SignalRecord::new(
            "snapshot-1",
            "execute-snapshot",
            serde_json::json!({
                "type": "incremental",
                "data-collections": ["inventory\\.orders"],
                "additional-conditions": [{"data-collection": "inventory\\.orders", "filter": "status = 'open'"}]
            }),
        ))
        .unwrap();
        assert!(
            matches!(signal, SnapshotSignal::Execute { data_collections, additional_conditions, .. }
            if data_collections == ["inventory\\.orders"]
                && additional_conditions.get("inventory\\.orders") == Some(&"status = 'open'".into()))
        );
        let pause = MySqlIncrementalSnapshot::parse_external_record(&SignalRecord::new(
            "pause-1",
            "pause-snapshot",
            serde_json::json!({}),
        ))
        .unwrap();
        assert!(matches!(pause, SnapshotSignal::Pause { .. }));
    }
}
