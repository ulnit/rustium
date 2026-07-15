use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use mysql_async::{
    BinlogStreamRequest, Conn, Opts, OptsBuilder, Row as MySqlRow, SslOpts, Value,
    binlog::{
        events::{EventData, RowsEventData, TableMapEvent},
        row::BinlogRow,
        value::BinlogValue,
    },
    prelude::Queryable,
};
use rustium_config::{MySqlSourceConfig, SnapshotConfig, SnapshotMode};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, MySqlPosition, Operation,
    RecordBoundary, Result, Row, SourceConnector, SourceContext, SourceMetadata, SourcePosition,
    SourceRecord, TransactionMetadata,
};
use tracing::{debug, info, warn};

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
struct TableSchema {
    database: String,
    table: String,
    event_schema: EventSchema,
}

impl TableSchema {
    fn key(&self) -> (String, String) {
        (self.database.clone(), self.table.clone())
    }
}

#[derive(Debug, Clone)]
struct BinlogCoordinates {
    filename: String,
    position: u64,
    gtid_set: Option<String>,
    source_server_id: u32,
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
}

pub struct MySqlSource {
    connector_name: String,
    config: MySqlSourceConfig,
    snapshot: SnapshotConfig,
    schemas: HashMap<(String, String), TableSchema>,
    source_server_id: u32,
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
        }
    }

    async fn validate_source(&mut self) -> Result<()> {
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
        if schemas.is_empty() {
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
            let mut ordered = schemas.values().cloned().collect::<Vec<_>>();
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

    async fn refresh_schemas(&self) -> Result<HashMap<(String, String), TableSchema>> {
        let mut connection = connect(&self.config).await?;
        let schemas = discover_tables(&mut connection, &self.config, &self.connector_name).await?;
        connection.disconnect().await.map_err(mysql_error)?;
        Ok(schemas)
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
                source_server_id: position.server_id,
            }
        } else {
            self.current_coordinates().await?
        };

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
        let request = BinlogStreamRequest::new(self.config.server_id)
            .with_filename(&filename)
            .with_pos(coordinates.position);
        let mut stream = connection
            .get_binlog_stream(request)
            .await
            .map_err(mysql_error)?;
        let mut state = StreamingState::new(
            coordinates.filename.clone(),
            self.schemas.clone(),
            resume_position,
        );

        info!(
            connector = %self.connector_name,
            file = %coordinates.filename,
            position = coordinates.position,
            "MySQL streaming started"
        );

        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    stream.close().await.map_err(mysql_error)?;
                    return Ok(());
                }
                changed = context.acknowledged.changed() => {
                    if changed.is_err() {
                        return Err(Error::Cancelled);
                    }
                }
                event = stream.next() => {
                    let Some(event) = event else {
                        return Err(Error::Source("MySQL binary log stream ended unexpectedly".into()));
                    };
                    let event = event.map_err(mysql_error)?;
                    let header = event.header();
                    let event_start = u64::from(header.log_pos()).saturating_sub(u64::from(header.event_size()));
                    let source_time = mysql_source_time(header.timestamp());
                    let data = event.read_data().map_err(mysql_error)?;
                    let Some(data) = data else { continue };

                    match data {
                        EventData::RotateEvent(rotate) => {
                            state.rotate(rotate.name().into_owned());
                        }
                        EventData::TableMapEvent(table_map) => {
                            state.register_table_map(table_map.table_id(), event_start);
                        }
                        EventData::GtidEvent(gtid) => {
                            state.begin_gtid(&gtid, source_time);
                        }
                        EventData::RowsEvent(rows) => {
                            let table_map = stream.get_tme(rows.table_id()).ok_or_else(|| {
                                Error::Source(format!(
                                    "missing TABLE_MAP_EVENT for MySQL table id {} at {}:{}",
                                    rows.table_id(), state.current_filename, event_start
                                ))
                            })?;
                            let records = state.convert_rows(
                                &rows,
                                table_map,
                                event_start,
                                header.server_id(),
                                source_time,
                                &self.connector_name,
                                &self.config,
                            )?;
                            for record in records {
                                context.output.send(Ok(record)).await.map_err(|_| Error::Cancelled)?;
                            }
                        }
                        EventData::XidEvent(xid) => {
                            if let Some(record) = state.commit_record(
                                event_start,
                                header.server_id(),
                                Some(xid.xid.to_string()),
                            ) {
                                context.output.send(Ok(record)).await.map_err(|_| Error::Cancelled)?;
                            }
                        }
                        EventData::QueryEvent(query) => {
                            let query_text = query.query().into_owned();
                            if is_schema_change(&query_text) {
                                let schemas = self.refresh_schemas().await?;
                                self.schemas = schemas.clone();
                                state.schemas = schemas;
                            }
                            if let Some(record) = state.handle_query(
                                &query_text,
                                event_start,
                                header.server_id(),
                                source_time,
                            ) {
                                context.output.send(Ok(record)).await.map_err(|_| Error::Cancelled)?;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
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
        let transaction = match &resume_position {
            Some(SourcePosition::MySql(position)) => {
                position.gtid_set.as_ref().map(|gtid| ActiveTransaction {
                    id: gtid.clone(),
                    source_time: None,
                    total_order: 0,
                    collection_order: HashMap::new(),
                })
            }
            _ => None,
        };
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
    ) {
        let sid = uuid::Uuid::from_bytes(event.sid());
        self.transaction = Some(ActiveTransaction {
            id: format!("{sid}:{}", event.gno()),
            source_time,
            total_order: 0,
            collection_order: HashMap::new(),
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
        });
        if transaction.source_time.is_none() {
            transaction.source_time = source_time;
        }

        let mut records = Vec::new();
        for pair in rows.rows(table_map) {
            let (before_row, after_row) = pair.map_err(mysql_error)?;
            let mut before = before_row
                .as_ref()
                .map(|row| convert_binlog_row(row, &schema.event_schema));
            let mut after = after_row
                .as_ref()
                .map(|row| convert_binlog_row(row, &schema.event_schema));
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
            let tls = builder.clone().ssl_opts(Some(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ));
            match connect_with_options(config, tls).await {
                Ok(connection) => return Ok(connection),
                Err(error) => {
                    debug!(%error, "preferred MySQL TLS connection failed; falling back to plaintext");
                    builder
                }
            }
        }
        "required" => builder.ssl_opts(Some(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        )),
        "verify_ca" => builder.ssl_opts(Some(
            SslOpts::default().with_danger_skip_domain_validation(true),
        )),
        "verify_identity" => builder.ssl_opts(Some(SslOpts::default())),
        mode => {
            return Err(Error::Configuration(format!(
                "unsupported MySQL database.ssl.mode {mode:?}"
            )));
        }
    };
    connect_with_options(config, builder).await
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
        if (!config.databases.is_empty() && !config.databases.contains(&database))
            || !config.tables.includes(&database, &table)
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

fn convert_binlog_row(row: &BinlogRow, schema: &EventSchema) -> Row {
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
                convert_binlog_value(value, &field.type_name),
            ))
        })
        .collect()
}

fn convert_binlog_value(value: &BinlogValue<'_>, type_name: &str) -> DataValue {
    match value {
        BinlogValue::Value(value) => convert_value(value, type_name),
        BinlogValue::Jsonb(value) => serde_json::Value::try_from(value.clone())
            .map_or(DataValue::Unavailable, DataValue::Json),
        BinlogValue::JsonDiff(_) => DataValue::Unavailable,
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
    use super::*;

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
    fn detects_mysql_schema_changes() {
        assert!(is_schema_change("ALTER TABLE orders ADD COLUMN note text"));
        assert!(is_schema_change(" truncate table orders"));
        assert!(!is_schema_change("BEGIN"));
    }
}
