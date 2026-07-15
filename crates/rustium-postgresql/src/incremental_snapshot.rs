use std::collections::{BTreeMap, HashMap};

use indexmap::IndexMap;
use pg_walstream::{
    PgReplicationConnection, RowData,
    sql_builder::{quote_ident, quote_literal},
};
use rustium_config::PostgresSourceConfig;
use rustium_core::{ChangeEvent, DataValue, Error, Result, Row};
use serde::Deserialize;

use crate::{
    schema_history::{IncrementalSnapshotProgress, TableSchema},
    source::convert_text,
};

const EXECUTE_SNAPSHOT: &str = "execute-snapshot";
const STOP_SNAPSHOT: &str = "stop-snapshot";
const PAUSE_SNAPSHOT: &str = "pause-snapshot";
const RESUME_SNAPSHOT: &str = "resume-snapshot";
const WINDOW_OPEN: &str = "snapshot-window-open";
const WINDOW_CLOSE: &str = "snapshot-window-close";

#[derive(Debug)]
pub(crate) enum Signal {
    Execute {
        id: String,
        data_collections: Vec<String>,
        additional_conditions: Vec<AdditionalCondition>,
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
    WindowOpen {
        id: String,
    },
    WindowClose {
        id: String,
    },
    Unsupported {
        id: String,
        signal_type: String,
    },
}

#[derive(Debug)]
pub(crate) struct ClosedWindow {
    pub(crate) schema: TableSchema,
    pub(crate) rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PgSnapshot {
    xmin: u64,
    xmax: u64,
    in_progress: Vec<u64>,
}

#[derive(Debug)]
struct ReadOnlyWatermarks {
    low: PgSnapshot,
    high: PgSnapshot,
}

pub(crate) struct IncrementalSnapshotController {
    progress: Option<IncrementalSnapshotProgress>,
    connection: Option<PgReplicationConnection>,
    window: IndexMap<String, Row>,
    current_schema: Option<TableSchema>,
    current_chunk_end: Option<Vec<String>>,
    current_chunk_complete: bool,
    current_chunk_id: Option<String>,
    window_open: bool,
    state_dirty: bool,
    prepare_after_commit: bool,
    read_only_watermarks: Option<ReadOnlyWatermarks>,
}

impl IncrementalSnapshotController {
    pub(crate) fn new(progress: Option<IncrementalSnapshotProgress>) -> Self {
        Self {
            progress,
            connection: None,
            window: IndexMap::new(),
            current_schema: None,
            current_chunk_end: None,
            current_chunk_complete: false,
            current_chunk_id: None,
            window_open: false,
            state_dirty: false,
            prepare_after_commit: false,
            read_only_watermarks: None,
        }
    }

    pub(crate) fn progress(&self) -> Option<&IncrementalSnapshotProgress> {
        self.progress.as_ref()
    }

    pub(crate) fn take_state_dirty(&mut self) -> bool {
        std::mem::take(&mut self.state_dirty)
    }

    pub(crate) fn parse_signal(row: &RowData) -> Result<Signal> {
        let id = signal_column(row, "id")?;
        let signal_type = signal_column(row, "type")?;
        match signal_type.as_str() {
            EXECUTE_SNAPSHOT => {
                let data = signal_column(row, "data")?;
                let request: ExecuteSnapshotData = serde_json::from_str(&data).map_err(|error| {
                    Error::Source(format!(
                        "PostgreSQL execute-snapshot signal {id:?} has invalid JSON data: {error}"
                    ))
                })?;
                if request
                    .snapshot_type
                    .as_deref()
                    .is_some_and(|kind| !kind.eq_ignore_ascii_case("incremental"))
                {
                    return Err(Error::Source(format!(
                        "PostgreSQL execute-snapshot signal {id:?} supports only type=incremental"
                    )));
                }
                if request.data_collections.is_empty() {
                    return Err(Error::Source(format!(
                        "PostgreSQL execute-snapshot signal {id:?} has no data-collections"
                    )));
                }
                if request
                    .surrogate_key
                    .as_deref()
                    .is_some_and(|key| !key.trim().is_empty())
                {
                    return Err(Error::Source(format!(
                        "PostgreSQL execute-snapshot signal {id:?} does not yet support surrogate-key"
                    )));
                }
                for condition in &request.additional_conditions {
                    if condition.data_collection.trim().is_empty()
                        || condition.filter.trim().is_empty()
                    {
                        return Err(Error::Source(format!(
                            "PostgreSQL execute-snapshot signal {id:?} has an empty additional-condition"
                        )));
                    }
                }
                Ok(Signal::Execute {
                    id,
                    data_collections: request.data_collections,
                    additional_conditions: request.additional_conditions,
                })
            }
            STOP_SNAPSHOT => {
                let request = control_snapshot_data(row, &id)?;
                Ok(Signal::Stop {
                    id,
                    data_collections: request.data_collections,
                })
            }
            PAUSE_SNAPSHOT => {
                control_snapshot_data(row, &id)?;
                Ok(Signal::Pause { id })
            }
            RESUME_SNAPSHOT => {
                control_snapshot_data(row, &id)?;
                Ok(Signal::Resume { id })
            }
            WINDOW_OPEN => Ok(Signal::WindowOpen { id }),
            WINDOW_CLOSE => Ok(Signal::WindowClose { id }),
            _ => Ok(Signal::Unsupported { id, signal_type }),
        }
    }

    pub(crate) async fn handle_signal(
        &mut self,
        signal: Signal,
        config: &PostgresSourceConfig,
        schemas: &HashMap<(String, String), TableSchema>,
    ) -> Result<Option<ClosedWindow>> {
        match signal {
            Signal::Execute {
                id,
                data_collections,
                additional_conditions,
            } => {
                if self.progress.is_some() {
                    return Err(Error::Source(format!(
                        "PostgreSQL execute-snapshot signal {id:?} arrived while another incremental snapshot is active"
                    )));
                }
                let (expanded, additional_conditions) =
                    expand_data_collections(&data_collections, &additional_conditions, schemas)?;
                self.progress = Some(IncrementalSnapshotProgress {
                    signal_id: id,
                    data_collections: expanded,
                    additional_conditions,
                    current_collection: 0,
                    last_key: None,
                    maximum_key: None,
                    chunk_sequence: 1,
                    paused: false,
                });
                self.state_dirty = true;
                self.prepare_current_chunk(config, schemas).await?;
                Ok(None)
            }
            Signal::Stop {
                id,
                data_collections,
            } => {
                self.stop_snapshot(&id, &data_collections)?;
                Ok(None)
            }
            Signal::Pause { id } => {
                if let Some(progress) = self.progress.as_mut()
                    && !progress.paused
                {
                    progress.paused = true;
                    self.prepare_after_commit = false;
                    self.state_dirty = true;
                    tracing::info!(%id, "PostgreSQL incremental snapshot paused");
                }
                Ok(None)
            }
            Signal::Resume { id } => {
                if let Some(progress) = self.progress.as_mut()
                    && progress.paused
                {
                    progress.paused = false;
                    self.prepare_after_commit = self.current_schema.is_none();
                    self.state_dirty = true;
                    tracing::info!(%id, "PostgreSQL incremental snapshot resumed");
                }
                Ok(None)
            }
            Signal::WindowOpen { id } => {
                if config.read_only {
                    return Ok(None);
                }
                if self.expected_watermark_id("open").as_deref() == Some(id.as_str()) {
                    self.window_open = true;
                }
                Ok(None)
            }
            Signal::WindowClose { id } => {
                if config.read_only {
                    return Ok(None);
                }
                if self.expected_watermark_id("close").as_deref() != Some(id.as_str())
                    || !self.window_open
                {
                    return Ok(None);
                }
                self.close_current_window().map(Some)
            }
            Signal::Unsupported { .. } => Ok(None),
        }
    }

    pub(crate) fn deduplicate(&mut self, event: &ChangeEvent) {
        if !self.window_open {
            return;
        }
        let Some(schema) = &self.current_schema else {
            return;
        };
        if event.source.schema.as_deref() != Some(schema.schema.as_str())
            || event.source.table.as_deref() != Some(schema.table.as_str())
        {
            return;
        }
        for row in [event.before.as_ref(), event.after.as_ref()]
            .into_iter()
            .flatten()
        {
            if let Some(key) = row_key(row, schema) {
                self.window.shift_remove(&key);
            }
        }
    }

    pub(crate) async fn after_commit(
        &mut self,
        config: &PostgresSourceConfig,
        schemas: &HashMap<(String, String), TableSchema>,
    ) -> Result<()> {
        if !config.read_only && std::mem::take(&mut self.prepare_after_commit) {
            self.prepare_current_chunk(config, schemas).await?;
        }
        Ok(())
    }

    pub(crate) fn observe_read_only_event(
        &mut self,
        transaction_id: u64,
    ) -> Result<Option<ClosedWindow>> {
        let Some(watermarks) = self.read_only_watermarks.as_ref() else {
            return Ok(None);
        };
        if !self.window_open && transaction_id >= watermarks.low.xmin {
            self.window_open = true;
        }
        if self.window_open && transaction_id > watermarks.high.xmax.max(watermarks.low.xmax) {
            return self.close_current_window().map(Some);
        }
        Ok(None)
    }

    pub(crate) fn observe_read_only_commit(
        &mut self,
        transaction_id: u64,
    ) -> Result<Option<ClosedWindow>> {
        let Some(watermarks) = self.read_only_watermarks.as_ref() else {
            return Ok(None);
        };
        if transaction_id == watermarks.high.xmax || transaction_id <= watermarks.high.xmin {
            return self.close_current_window().map(Some);
        }
        Ok(None)
    }

    pub(crate) async fn prepare_read_only_continuation(
        &mut self,
        config: &PostgresSourceConfig,
        schemas: &HashMap<(String, String), TableSchema>,
    ) -> Result<bool> {
        if config.read_only && std::mem::take(&mut self.prepare_after_commit) {
            self.prepare_current_chunk(config, schemas).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn resume(
        &mut self,
        config: &PostgresSourceConfig,
        schemas: &HashMap<(String, String), TableSchema>,
    ) -> Result<()> {
        if self
            .progress
            .as_ref()
            .is_some_and(|progress| !progress.paused)
        {
            self.prepare_current_chunk(config, schemas).await?;
        }
        Ok(())
    }

    async fn prepare_current_chunk(
        &mut self,
        config: &PostgresSourceConfig,
        schemas: &HashMap<(String, String), TableSchema>,
    ) -> Result<()> {
        let progress = self.progress.as_ref().ok_or_else(|| {
            Error::Invariant("incremental snapshot has no persisted progress".into())
        })?;
        let collection = progress
            .data_collections
            .get(progress.current_collection)
            .ok_or_else(|| Error::Invariant("incremental snapshot collection is missing".into()))?;
        let (schema_name, table_name) = split_collection(collection)?;
        let schema = schemas
            .get(&(schema_name.into(), table_name.into()))
            .cloned()
            .ok_or_else(|| {
                Error::Source(format!(
                    "incremental snapshot table {collection:?} is not a captured PostgreSQL table"
                ))
            })?;
        if !schema
            .event_schema
            .fields
            .iter()
            .any(|field| field.primary_key)
        {
            return Err(Error::Source(format!(
                "incremental snapshot table {collection:?} has no primary key"
            )));
        }
        let chunk_id = format!("{}-{}", progress.signal_id, progress.chunk_sequence);
        let input = PrepareChunkInput {
            connection_url: config.connection_url(false)?,
            signal_data_collection: config
                .signal_data_collection
                .clone()
                .filter(|_| !config.read_only),
            schema: schema.clone(),
            last_key: progress.last_key.clone(),
            maximum_key: progress.maximum_key.clone(),
            additional_condition: progress.additional_conditions.get(collection).cloned(),
            chunk_size: config.incremental_snapshot_chunk_size,
            chunk_id: chunk_id.clone(),
            read_only: config.read_only,
        };
        let connection = self.connection.take();
        let (connection, prepared) =
            tokio::task::spawn_blocking(move || prepare_chunk(connection, input))
                .await
                .map_err(|error| {
                    Error::Source(format!(
                        "PostgreSQL incremental snapshot task failed: {error}"
                    ))
                })??;
        self.connection = Some(connection);
        if self
            .progress
            .as_mut()
            .is_some_and(|progress| progress.maximum_key.is_none())
        {
            self.progress.as_mut().expect("progress exists").maximum_key =
                prepared.maximum_key.clone();
            self.state_dirty = true;
        }
        self.window = prepared
            .rows
            .into_iter()
            .filter_map(|row| row_key(&row, &schema).map(|key| (key, row)))
            .collect();
        self.current_chunk_end = prepared.chunk_end;
        self.current_chunk_complete = prepared.complete;
        self.current_chunk_id = Some(chunk_id);
        self.current_schema = Some(schema);
        self.window_open = false;
        self.read_only_watermarks = prepared.read_only_watermarks;
        Ok(())
    }

    fn expected_watermark_id(&self, suffix: &str) -> Option<String> {
        self.current_chunk_id
            .as_ref()
            .map(|id| format!("{id}-{suffix}"))
    }

    fn advance_progress(&mut self) {
        let Some(progress) = self.progress.as_mut() else {
            return;
        };
        if self.current_chunk_complete {
            progress.current_collection += 1;
            progress.last_key = None;
            progress.maximum_key = None;
        } else {
            progress.last_key = self.current_chunk_end.take();
        }
        progress.chunk_sequence += 1;
        if progress.current_collection >= progress.data_collections.len() {
            self.progress = None;
        }
    }

    fn close_current_window(&mut self) -> Result<ClosedWindow> {
        let schema = self.current_schema.take().ok_or_else(|| {
            Error::Invariant("incremental snapshot close has no current schema".into())
        })?;
        let rows = self.window.drain(..).map(|(_, row)| row).collect();
        self.window_open = false;
        self.current_chunk_id = None;
        self.read_only_watermarks = None;
        self.advance_progress();
        self.state_dirty = true;
        self.prepare_after_commit = self
            .progress
            .as_ref()
            .is_some_and(|progress| !progress.paused);
        Ok(ClosedWindow { schema, rows })
    }

    fn stop_snapshot(&mut self, signal_id: &str, patterns: &[String]) -> Result<()> {
        let Some(progress) = self.progress.as_mut() else {
            return Ok(());
        };
        let patterns = compile_patterns(patterns, "stop-snapshot data-collections")?;
        let stop_all = patterns.is_empty();
        let current_index = progress.current_collection;
        let current = progress.data_collections.get(current_index).cloned();
        let mut retained_before_current = 0;
        let mut removed_current = false;
        let mut removed = false;
        let mut retained = Vec::with_capacity(progress.data_collections.len());

        for (index, collection) in progress.data_collections.iter().enumerate() {
            let matches = stop_all || patterns.iter().any(|pattern| pattern.is_match(collection));
            if matches {
                removed = true;
                removed_current |= index == current_index;
            } else {
                if index < current_index {
                    retained_before_current += 1;
                }
                retained.push(collection.clone());
            }
        }
        if !removed {
            return Ok(());
        }

        progress
            .additional_conditions
            .retain(|collection, _| retained.contains(collection));
        progress.data_collections = retained;
        if progress.data_collections.is_empty() {
            self.progress = None;
            self.clear_current_chunk();
        } else {
            progress.current_collection =
                retained_before_current.min(progress.data_collections.len() - 1);
            if removed_current || current.as_deref().is_none() {
                progress.last_key = None;
                progress.maximum_key = None;
                progress.chunk_sequence += 1;
                let paused = progress.paused;
                self.clear_current_chunk();
                self.prepare_after_commit = !paused;
            }
        }
        self.state_dirty = true;
        tracing::info!(%signal_id, "PostgreSQL incremental snapshot collections stopped");
        Ok(())
    }

    fn clear_current_chunk(&mut self) {
        self.window.clear();
        self.current_schema = None;
        self.current_chunk_end = None;
        self.current_chunk_complete = false;
        self.current_chunk_id = None;
        self.window_open = false;
        self.prepare_after_commit = false;
        self.read_only_watermarks = None;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ExecuteSnapshotData {
    #[serde(default, rename = "type")]
    snapshot_type: Option<String>,
    data_collections: Vec<String>,
    #[serde(default)]
    additional_conditions: Vec<AdditionalCondition>,
    #[serde(default)]
    surrogate_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct AdditionalCondition {
    data_collection: String,
    filter: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ControlSnapshotData {
    #[serde(default, rename = "type")]
    snapshot_type: Option<String>,
    #[serde(default)]
    data_collections: Vec<String>,
}

struct PrepareChunkInput {
    connection_url: String,
    signal_data_collection: Option<String>,
    schema: TableSchema,
    last_key: Option<Vec<String>>,
    maximum_key: Option<Vec<String>>,
    additional_condition: Option<String>,
    chunk_size: usize,
    chunk_id: String,
    read_only: bool,
}

struct PreparedChunk {
    rows: Vec<Row>,
    chunk_end: Option<Vec<String>>,
    maximum_key: Option<Vec<String>>,
    complete: bool,
    read_only_watermarks: Option<ReadOnlyWatermarks>,
}

fn prepare_chunk(
    connection: Option<PgReplicationConnection>,
    input: PrepareChunkInput,
) -> Result<(PgReplicationConnection, PreparedChunk)> {
    let mut connection = match connection {
        Some(connection) => connection,
        None => PgReplicationConnection::connect(&input.connection_url)
            .map_err(|error| Error::Source(error.to_string()))?,
    };
    let (signal_table, low_watermark) = if input.read_only {
        connection
            .exec("SELECT pg_current_xact_id()")
            .map_err(pg_error)?;
        (None, Some(query_current_snapshot(&mut connection)?))
    } else {
        let signal_table =
            qualified_name(input.signal_data_collection.as_deref().ok_or_else(|| {
                Error::Configuration("signal_data_collection is not set".into())
            })?)?;
        insert_watermark(
            &mut connection,
            &signal_table,
            &format!("{}-open", input.chunk_id),
            WINDOW_OPEN,
        )?;
        (Some(signal_table), None)
    };

    let key_fields = input
        .schema
        .event_schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .collect::<Vec<_>>();
    let key_columns = key_fields
        .iter()
        .map(|field| quote_ident(&field.name).map_err(pg_error))
        .collect::<Result<Vec<_>>>()?;
    let table = qualified_table(&input.schema.schema, &input.schema.table)?;
    let condition = input
        .additional_condition
        .as_deref()
        .map(|condition| format!("({condition})"));
    let maximum_key = match input.maximum_key {
        Some(maximum_key) => Some(maximum_key),
        None => query_key(
            &mut connection,
            &format!(
                "SELECT {} FROM {table}{} ORDER BY {} DESC LIMIT 1",
                text_projection(&key_fields)?,
                condition
                    .as_ref()
                    .map_or_else(String::new, |condition| format!(" WHERE {condition}")),
                key_columns.join(", ")
            ),
            key_fields.len(),
        )?,
    };

    let projection = text_projection(&input.schema.event_schema.fields.iter().collect::<Vec<_>>())?;
    let mut predicates = condition.into_iter().collect::<Vec<_>>();
    if let Some(last_key) = &input.last_key {
        predicates.push(row_predicate(&key_columns, ">", last_key)?);
    }
    if let Some(maximum_key) = &maximum_key {
        predicates.push(row_predicate(&key_columns, "<=", maximum_key)?);
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let query = format!(
        "SELECT {projection} FROM {table}{where_clause} ORDER BY {} LIMIT {}",
        key_columns.join(", "),
        input.chunk_size
    );
    let result = connection.exec(&query).map_err(pg_error)?;
    let mut rows = Vec::with_capacity(result.ntuples() as usize);
    for row_index in 0..result.ntuples() {
        let row = input
            .schema
            .event_schema
            .fields
            .iter()
            .enumerate()
            .map(|(column_index, field)| {
                let column_index = i32::try_from(column_index).map_err(|_| {
                    Error::Invariant("incremental snapshot has too many columns".into())
                })?;
                let value = result
                    .get_value(row_index, column_index)
                    .map_or(DataValue::Null, |value| {
                        convert_text(&value, &field.type_name)
                    });
                Ok((field.name.clone(), value))
            })
            .collect::<Result<Row>>()?;
        rows.push(row);
    }
    let chunk_end = rows
        .last()
        .map(|row| key_text_values(row, &input.schema))
        .transpose()?;
    let complete = chunk_end.is_none() || chunk_end == maximum_key || rows.len() < input.chunk_size;

    let read_only_watermarks = if let Some(low) = low_watermark {
        Some(ReadOnlyWatermarks {
            low,
            high: query_current_snapshot(&mut connection)?,
        })
    } else {
        insert_watermark(
            &mut connection,
            signal_table.as_deref().ok_or_else(|| {
                Error::Invariant("incremental snapshot signal table is missing".into())
            })?,
            &format!("{}-close", input.chunk_id),
            WINDOW_CLOSE,
        )?;
        None
    };
    Ok((
        connection,
        PreparedChunk {
            rows,
            chunk_end,
            maximum_key,
            complete,
            read_only_watermarks,
        },
    ))
}

fn query_current_snapshot(connection: &mut PgReplicationConnection) -> Result<PgSnapshot> {
    let result = connection
        .exec("SELECT pg_current_snapshot()::text")
        .map_err(pg_error)?;
    let value = result
        .get_value(0, 0)
        .ok_or_else(|| Error::Source("pg_current_snapshot() returned no value".into()))?;
    parse_pg_snapshot(&value)
}

fn parse_pg_snapshot(value: &str) -> Result<PgSnapshot> {
    let mut parts = value.split(':');
    let xmin = parse_snapshot_xid(parts.next(), value, "xmin")?;
    let xmax = parse_snapshot_xid(parts.next(), value, "xmax")?;
    let in_progress = parts
        .next()
        .unwrap_or_default()
        .split(',')
        .filter(|xid| !xid.is_empty())
        .map(|xid| {
            xid.parse::<u64>().map_err(|error| {
                Error::Source(format!(
                    "invalid PostgreSQL snapshot in-progress xid {xid:?} in {value:?}: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if parts.next().is_some() {
        return Err(Error::Source(format!(
            "invalid PostgreSQL snapshot {value:?}: too many fields"
        )));
    }
    Ok(PgSnapshot {
        xmin,
        xmax,
        in_progress,
    })
}

fn parse_snapshot_xid(value: Option<&str>, snapshot: &str, field: &str) -> Result<u64> {
    value
        .ok_or_else(|| {
            Error::Source(format!(
                "invalid PostgreSQL snapshot {snapshot:?}: missing {field}"
            ))
        })?
        .parse()
        .map_err(|error| {
            Error::Source(format!(
                "invalid PostgreSQL snapshot {field} in {snapshot:?}: {error}"
            ))
        })
}

fn signal_column(row: &RowData, name: &str) -> Result<String> {
    row.get(name)
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::Source(format!("PostgreSQL signal has no text {name} column")))
}

fn control_snapshot_data(row: &RowData, id: &str) -> Result<ControlSnapshotData> {
    let data = signal_column(row, "data")?;
    let request: ControlSnapshotData = serde_json::from_str(&data).map_err(|error| {
        Error::Source(format!(
            "PostgreSQL snapshot control signal {id:?} has invalid JSON data: {error}"
        ))
    })?;
    if request
        .snapshot_type
        .as_deref()
        .is_some_and(|kind| !kind.eq_ignore_ascii_case("incremental"))
    {
        return Err(Error::Source(format!(
            "PostgreSQL snapshot control signal {id:?} supports only type=incremental"
        )));
    }
    Ok(request)
}

fn expand_data_collections(
    patterns: &[String],
    additional_conditions: &[AdditionalCondition],
    schemas: &HashMap<(String, String), TableSchema>,
) -> Result<(Vec<String>, BTreeMap<String, String>)> {
    let patterns = compile_patterns(patterns, "execute-snapshot data-collections")?;
    let conditions = additional_conditions
        .iter()
        .map(|condition| {
            regex::RegexBuilder::new(&format!("^(?:{})$", condition.data_collection))
                .case_insensitive(true)
                .build()
                .map(|pattern| (pattern, condition.filter.clone()))
                .map_err(|error| {
                    Error::Source(format!(
                        "invalid additional-condition data-collection pattern {:?}: {error}",
                        condition.data_collection
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut matches = schemas
        .values()
        .map(|schema| format!("{}.{}", schema.schema, schema.table))
        .filter(|collection| patterns.iter().any(|pattern| pattern.is_match(collection)))
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    if matches.is_empty() {
        return Err(Error::Source(
            "execute-snapshot signal selected no captured PostgreSQL tables".into(),
        ));
    }
    let expanded_conditions = matches
        .iter()
        .filter_map(|collection| {
            conditions
                .iter()
                .find(|(pattern, _)| pattern.is_match(collection))
                .map(|(_, filter)| (collection.clone(), filter.clone()))
        })
        .collect();
    Ok((matches, expanded_conditions))
}

fn compile_patterns(patterns: &[String], context: &str) -> Result<Vec<regex::Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|error| {
                Error::Source(format!("invalid {context} pattern {pattern:?}: {error}"))
            })
        })
        .collect()
}

fn row_key(row: &Row, schema: &TableSchema) -> Option<String> {
    let key = schema
        .event_schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .map(|field| row.get(&field.name))
        .collect::<Option<Vec<_>>>()?;
    serde_json::to_string(&key).ok()
}

fn key_text_values(row: &Row, schema: &TableSchema) -> Result<Vec<String>> {
    schema
        .event_schema
        .fields
        .iter()
        .filter(|field| field.primary_key)
        .map(|field| {
            let value = row.get(&field.name).ok_or_else(|| {
                Error::Invariant(format!(
                    "incremental snapshot row has no key {}",
                    field.name
                ))
            })?;
            data_value_text(value).ok_or_else(|| {
                Error::Source(format!(
                    "incremental snapshot key {} cannot be converted to PostgreSQL text",
                    field.name
                ))
            })
        })
        .collect()
}

fn data_value_text(value: &DataValue) -> Option<String> {
    match value {
        DataValue::Boolean(value) => Some(if *value { "t" } else { "f" }.into()),
        DataValue::Int32(value) => Some(value.to_string()),
        DataValue::Int64(value) => Some(value.to_string()),
        DataValue::UInt64(value) => Some(value.to_string()),
        DataValue::Float64(value) => Some(value.to_string()),
        DataValue::Decimal(value)
        | DataValue::String(value)
        | DataValue::Date(value)
        | DataValue::Time(value)
        | DataValue::Timestamp(value) => Some(value.clone()),
        DataValue::Uuid(value) => Some(value.to_string()),
        DataValue::Null
        | DataValue::Bytes(_)
        | DataValue::Json(_)
        | DataValue::Array(_)
        | DataValue::Unavailable => None,
    }
}

fn text_projection(fields: &[&rustium_core::FieldSchema]) -> Result<String> {
    fields
        .iter()
        .map(|field| {
            quote_ident(&field.name)
                .map(|name| format!("{name}::text"))
                .map_err(pg_error)
        })
        .collect::<Result<Vec<_>>>()
        .map(|projection| projection.join(", "))
}

fn query_key(
    connection: &mut PgReplicationConnection,
    query: &str,
    columns: usize,
) -> Result<Option<Vec<String>>> {
    let result = connection.exec(query).map_err(pg_error)?;
    if result.ntuples() == 0 {
        return Ok(None);
    }
    (0..columns)
        .map(|index| {
            i32::try_from(index)
                .ok()
                .and_then(|index| result.get_value(0, index))
                .ok_or_else(|| Error::Source("incremental snapshot maximum key is null".into()))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn row_predicate(columns: &[String], operator: &str, values: &[String]) -> Result<String> {
    if columns.len() != values.len() {
        return Err(Error::Invariant(
            "incremental snapshot key boundary has the wrong arity".into(),
        ));
    }
    let values = values
        .iter()
        .map(|value| quote_literal(value).map_err(pg_error))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(
        "ROW({}) {operator} ROW({})",
        columns.join(", "),
        values.join(", ")
    ))
}

fn insert_watermark(
    connection: &mut PgReplicationConnection,
    signal_table: &str,
    id: &str,
    signal_type: &str,
) -> Result<()> {
    let id = quote_literal(id).map_err(pg_error)?;
    let signal_type = quote_literal(signal_type).map_err(pg_error)?;
    connection
        .exec(&format!(
            "INSERT INTO {signal_table} (id, type, data) VALUES ({id}, {signal_type}, '{{}}')"
        ))
        .map_err(pg_error)?;
    Ok(())
}

fn qualified_name(collection: &str) -> Result<String> {
    let (schema, table) = split_collection(collection)?;
    qualified_table(schema, table)
}

fn qualified_table(schema: &str, table: &str) -> Result<String> {
    Ok(format!(
        "{}.{}",
        quote_ident(schema).map_err(pg_error)?,
        quote_ident(table).map_err(pg_error)?
    ))
}

fn split_collection(collection: &str) -> Result<(&str, &str)> {
    collection.split_once('.').ok_or_else(|| {
        Error::Configuration(format!(
            "PostgreSQL collection {collection:?} must be schema-qualified"
        ))
    })
}

fn pg_error(error: impl std::fmt::Display) -> Error {
    Error::Source(error.to_string())
}

#[cfg(test)]
mod tests {
    use pg_walstream::ColumnValue;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn parses_execute_snapshot_signal() {
        let row = signal_row(
            "snapshot-1",
            EXECUTE_SNAPSHOT,
            r#"{"type":"incremental","data-collections":["public\\.orders"],"additional-conditions":[{"data-collection":"public\\.orders","filter":"status = 'open'"}]}"#,
        );
        assert!(matches!(
            IncrementalSnapshotController::parse_signal(&row).unwrap(),
            Signal::Execute {
                id,
                data_collections,
                additional_conditions,
            }
                if id == "snapshot-1"
                    && data_collections == ["public\\.orders"]
                    && additional_conditions.len() == 1
                    && additional_conditions[0].filter == "status = 'open'"
        ));
    }

    #[test]
    fn parses_snapshot_control_signals_and_rejects_surrogate_keys() {
        assert!(matches!(
            IncrementalSnapshotController::parse_signal(&signal_row(
                "pause-1",
                PAUSE_SNAPSHOT,
                r#"{"type":"INCREMENTAL"}"#,
            ))
            .unwrap(),
            Signal::Pause { id } if id == "pause-1"
        ));
        assert!(matches!(
            IncrementalSnapshotController::parse_signal(&signal_row(
                "stop-1",
                STOP_SNAPSHOT,
                r#"{"data-collections":["public\\.orders"]}"#,
            ))
            .unwrap(),
            Signal::Stop { data_collections, .. }
                if data_collections == ["public\\.orders"]
        ));
        let error = IncrementalSnapshotController::parse_signal(&signal_row(
            "snapshot-2",
            EXECUTE_SNAPSHOT,
            r#"{"data-collections":["public\\.orders"],"surrogate-key":"created_at"}"#,
        ))
        .unwrap_err();
        assert!(error.to_string().contains("surrogate-key"));
    }

    #[test]
    fn scoped_stop_removes_current_collection_and_preserves_remaining_work() {
        let progress = IncrementalSnapshotProgress {
            signal_id: "snapshot-1".into(),
            data_collections: vec!["public.orders".into(), "public.customers".into()],
            additional_conditions: BTreeMap::from([
                ("public.orders".into(), "status = 'open'".into()),
                ("public.customers".into(), "active".into()),
            ]),
            current_collection: 0,
            last_key: Some(vec!["42".into()]),
            maximum_key: Some(vec!["100".into()]),
            chunk_sequence: 3,
            paused: false,
        };
        let mut controller = IncrementalSnapshotController::new(Some(progress));

        controller
            .stop_snapshot("stop-1", &[r"public\.orders".into()])
            .unwrap();

        let progress = controller.progress().unwrap();
        assert_eq!(progress.data_collections, ["public.customers"]);
        assert_eq!(progress.current_collection, 0);
        assert_eq!(progress.last_key, None);
        assert_eq!(progress.maximum_key, None);
        assert_eq!(progress.chunk_sequence, 4);
        assert_eq!(
            progress.additional_conditions,
            BTreeMap::from([("public.customers".into(), "active".into())])
        );
        assert!(controller.prepare_after_commit);
        assert!(controller.take_state_dirty());
    }

    #[test]
    fn parses_postgres_transaction_snapshots() {
        assert_eq!(
            parse_pg_snapshot("100:105:101,103").unwrap(),
            PgSnapshot {
                xmin: 100,
                xmax: 105,
                in_progress: vec![101, 103],
            }
        );
        assert_eq!(
            parse_pg_snapshot("200:200:").unwrap(),
            PgSnapshot {
                xmin: 200,
                xmax: 200,
                in_progress: Vec::new(),
            }
        );
        assert!(parse_pg_snapshot("invalid").is_err());
    }

    #[test]
    fn quotes_composite_key_boundaries() {
        assert_eq!(
            row_predicate(
                &["\"tenant\"".into(), "\"id\"".into()],
                ">",
                &["acme".into(), "it's-safe".into()]
            )
            .unwrap(),
            "ROW(\"tenant\", \"id\") > ROW('acme', 'it''s-safe')"
        );
    }

    fn signal_row(id: &str, signal_type: &str, data: &str) -> RowData {
        let mut row = RowData::new();
        row.push(Arc::from("id"), ColumnValue::text(id));
        row.push(Arc::from("type"), ColumnValue::text(signal_type));
        row.push(Arc::from("data"), ColumnValue::text(data));
        row
    }
}
