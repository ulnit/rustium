use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::TryStreamExt;
use rustium_config::{SnapshotConfig, SnapshotMode, SqlServerSourceConfig};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, Operation, RecordBoundary,
    Result, Row, SourceConnector, SourceContext, SourceMetadata, SourcePosition, SourceRecord,
    SqlServerPosition, TransactionMetadata,
};
use tiberius::{
    AuthMethod, Client, ColumnData, Config as TdsConfig, EncryptionLevel, Row as TdsRow,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::info;

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");
const LSN_SIZE: usize = 10;
const RAW_DELETE: i32 = 1;
const RAW_INSERT: i32 = 2;
const RAW_UPDATE_BEFORE: i32 = 3;
const RAW_UPDATE_AFTER: i32 = 4;
const COMMIT_SERIAL: u64 = 5;

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

    fn from_position(position: &SqlServerPosition) -> Result<(Self, Option<SourcePosition>)> {
        if position.snapshot || position.event_serial == COMMIT_SERIAL {
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

pub struct SqlServerSource {
    connector_name: String,
    config: SqlServerSourceConfig,
    snapshot: SnapshotConfig,
    captures: Vec<CaptureTable>,
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
        }
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
        if captures.is_empty() {
            return Err(Error::Configuration(
                "the SQL Server CDC capture instances and table filters select no tables".into(),
            ));
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
            CdcCursor::from_position(position)?
        } else {
            (CdcCursor::at_snapshot(self.current_anchor().await?), None)
        };

        let mut client = connect(&self.config, self.database()).await?;
        validate_retention(&mut client, &self.captures, &cursor.commit_lsn).await?;
        let mut state = StreamingState::new(cursor, resume_position);
        let mut interval = tokio::time::interval(self.config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            connector = %self.connector_name,
            database = %self.database(),
            commit_lsn = %format_lsn(&state.cursor.commit_lsn),
            "SQL Server CDC streaming started"
        );

        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    client.close().await.map_err(sqlserver_error)?;
                    return Ok(());
                }
                changed = context.acknowledged.changed() => {
                    if changed.is_err() {
                        return Err(Error::Cancelled);
                    }
                }
                _ = interval.tick() => {
                    let max_lsn = current_max_lsn(&mut client).await?;
                    if max_lsn <= state.cursor.commit_lsn {
                        continue;
                    }
                    validate_retention(&mut client, &self.captures, &state.cursor.commit_lsn).await?;
                    let records = read_change_batch(
                        &mut client,
                        self.database(),
                        &self.connector_name,
                        &self.captures,
                        &mut state,
                        &max_lsn,
                        self.config.streaming_fetch_size,
                    ).await?;
                    for record in records {
                        context.output.send(Ok(record)).await.map_err(|_| Error::Cancelled)?;
                    }
                }
            }
        }
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
        .map_err(|_| Error::Source("timed out connecting to SQL Server".into()))?
        .map_err(sqlserver_error)?;
    tcp.set_nodelay(true).map_err(sqlserver_error)?;
    tokio::time::timeout(
        config.connect_timeout,
        Client::connect(tds, tcp.compat_write()),
    )
    .await
    .map_err(|_| Error::Source("timed out negotiating SQL Server TDS".into()))?
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
    for row in rows {
        let schema = required_string(&row, "schema_name")?;
        let table = required_string(&row, "table_name")?;
        if !config.tables.includes(&schema, &table) {
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
    let query = format!(
        "SELECT * FROM {}.{}",
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
        "geometry" | "geography" | "hierarchyid" => format!("{column}.ToString()"),
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
        "binary" | "varbinary" | "image" | "rowversion" | "timestamp" => {
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
                DataValue::Timestamp(value.to_string())
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

fn sqlserver_error(error: impl std::fmt::Display) -> Error {
    Error::Source(error.to_string())
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
}
