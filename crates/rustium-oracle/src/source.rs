use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use oracle_rs::{Connection, Value};
use regex::Regex;
use rustium_config::{OracleSourceConfig, SnapshotConfig, SnapshotMode};
use rustium_core::{
    ChangeEvent, DataValue, Error, EventId, EventSchema, FieldSchema, Operation, OraclePosition,
    RecordBoundary, Result, RetryPolicy, Row, SourceConnector, SourceContext, SourceMetadata,
    SourcePosition, SourceRecord, TransactionMetadata,
};
use tokio::time::MissedTickBehavior;
use tracing::info;

const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
struct OracleTable {
    owner: String,
    name: String,
    columns: Vec<String>,
}

impl OracleTable {
    fn namespace(&self) -> String {
        format!("{}.{}", self.owner, self.name)
    }
}

pub struct OracleSource {
    connector_name: String,
    config: OracleSourceConfig,
    snapshot: SnapshotConfig,
    retry_policy: RetryPolicy,
    tables: Vec<OracleTable>,
}

impl OracleSource {
    #[must_use]
    pub fn new(
        connector_name: impl Into<String>,
        config: OracleSourceConfig,
        snapshot: SnapshotConfig,
    ) -> Self {
        Self {
            connector_name: connector_name.into(),
            config,
            snapshot,
            retry_policy: RetryPolicy::default(),
            tables: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    async fn connect(&self) -> Result<Connection> {
        let connection = if let Some(pdb) = &self.config.pdb_name {
            oracle_rs::Config::new(
                &self.config.hostname,
                self.config.port,
                pdb,
                &self.config.username,
                &self.config.password,
            )
        } else {
            oracle_rs::Config::new(
                &self.config.hostname,
                self.config.port,
                &self.config.database,
                &self.config.username,
                &self.config.password,
            )
        }
        .connect_timeout(self.config.connect_timeout);
        Connection::connect_with_config(connection)
            .await
            .map_err(oracle_error)
    }

    async fn validate_source(&mut self) -> Result<()> {
        let connection = self.connect().await?;
        let mode = query_scalar(&connection, "SELECT LOG_MODE FROM V$DATABASE").await?;
        if mode != "ARCHIVELOG" {
            return Err(Error::Configuration(format!(
                "Oracle LOG_MODE must be ARCHIVELOG; found {mode:?}"
            )));
        }
        let supplemental = query_scalar(
            &connection,
            "SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE",
        )
        .await?;
        if !matches!(supplemental.as_str(), "YES" | "IMPLICIT") {
            return Err(Error::Configuration(format!(
                "Oracle supplemental logging is disabled; SUPPLEMENTAL_LOG_DATA_MIN={supplemental:?}"
            )));
        }
        let version = query_scalar(&connection, "SELECT VERSION FROM V$INSTANCE").await?;
        info!(connector = %self.connector_name, oracle_version = %version, "Oracle source validated");
        self.tables = discover_tables(&connection, &self.config).await?;
        if self.tables.is_empty() {
            return Err(Error::Configuration(
                "Oracle table filters select no tables".into(),
            ));
        }
        Ok(())
    }

    async fn snapshot(
        &self,
        connection: &Connection,
        anchor: u64,
        output: &tokio::sync::mpsc::Sender<Result<SourceRecord>>,
    ) -> Result<OraclePosition> {
        let mut serial = 0_u64;
        for table in &self.tables {
            let sql = format!(
                "SELECT {} FROM {} AS OF SCN {}",
                table
                    .columns
                    .iter()
                    .map(|column| format!("\"{}\"", column.replace('"', "\"\"")))
                    .collect::<Vec<_>>()
                    .join(", "),
                qualified_name(&table.owner, &table.name),
                anchor
            );
            for row in query_all(connection, &sql, self.config.batch_size).await? {
                serial += 1;
                let after = row_to_rustium(&table.columns, &row);
                let position = OraclePosition {
                    scn: anchor,
                    commit_scn: Some(anchor),
                    transaction_id: None,
                    rs_id: None,
                    ssn: None,
                    event_serial: serial,
                    snapshot: true,
                };
                let event = self.event(table, position, Operation::Read, None, Some(after), None);
                output
                    .send(Ok(SourceRecord::data(event)))
                    .await
                    .map_err(|_| Error::Cancelled)?;
            }
        }
        let position = OraclePosition {
            scn: anchor,
            commit_scn: Some(anchor),
            transaction_id: None,
            rs_id: None,
            ssn: None,
            event_serial: serial + 1,
            snapshot: true,
        };
        output
            .send(Ok(SourceRecord {
                event: None,
                position: SourcePosition::Oracle(position.clone()),
                boundary: RecordBoundary::SnapshotComplete,
                connector_state: None,
                signal_acknowledgements: Vec::new(),
            }))
            .await
            .map_err(|_| Error::Cancelled)?;
        Ok(position)
    }

    fn event(
        &self,
        table: &OracleTable,
        position: OraclePosition,
        operation: Operation,
        before: Option<Row>,
        after: Option<Row>,
        transaction: Option<TransactionMetadata>,
    ) -> ChangeEvent {
        let row = before.as_ref().or(after.as_ref());
        let fields = row
            .into_iter()
            .flat_map(|row| row.iter())
            .map(|(name, value)| FieldSchema {
                name: name.clone(),
                type_name: value_type(value).into(),
                optional: true,
                primary_key: is_probable_key(name),
            })
            .collect();
        let source_position = SourcePosition::Oracle(position.clone());
        let mut attributes =
            BTreeMap::from([("scn".into(), serde_json::Value::from(position.scn))]);
        if let Some(rs_id) = &position.rs_id {
            attributes.insert("rs_id".into(), rs_id.clone().into());
        }
        if let Some(ssn) = position.ssn {
            attributes.insert("ssn".into(), ssn.into());
        }
        ChangeEvent {
            id: EventId::deterministic(
                &self.connector_name,
                &table.owner,
                &source_position,
                &table.namespace(),
                position.event_serial,
            ),
            source: SourceMetadata {
                connector: "oracle".into(),
                connector_name: self.connector_name.clone(),
                database: self.config.database.clone(),
                schema: Some(table.owner.clone()),
                table: Some(table.name.clone()),
                snapshot: position.snapshot,
                version: CONNECTOR_VERSION.into(),
                attributes,
            },
            position: source_position,
            transaction,
            operation,
            before,
            after,
            schema: EventSchema {
                name: format!("{}.Envelope", table.namespace()),
                version: 1,
                fields,
            },
            source_time: None,
            observed_time: Utc::now(),
        }
    }
}

#[async_trait]
impl SourceConnector for OracleSource {
    fn source_type(&self) -> &'static str {
        "oracle"
    }

    async fn validate(&mut self) -> Result<()> {
        self.validate_source().await
    }

    async fn run(&mut self, context: SourceContext) -> Result<()> {
        let checkpoint = context.initial_checkpoint.clone();
        let checkpoint_position =
            checkpoint
                .as_ref()
                .and_then(|checkpoint| match &checkpoint.source_position {
                    SourcePosition::Oracle(position) => Some(position.clone()),
                    _ => None,
                });
        if checkpoint.is_some() && checkpoint_position.is_none() {
            return Err(Error::State(
                "Oracle connector cannot resume from another source checkpoint".into(),
            ));
        }
        let connection = self.connect().await?;
        let current_scn =
            query_scalar_u64(&connection, "SELECT CURRENT_SCN FROM V$DATABASE").await?;
        let snapshot_needed = match self.snapshot.mode {
            SnapshotMode::Never => false,
            SnapshotMode::Initial | SnapshotMode::WhenNeeded => checkpoint
                .as_ref()
                .is_none_or(|checkpoint| !checkpoint.snapshot_completed),
        };
        let mut cursor_scn = checkpoint_position
            .as_ref()
            .map(|position| position.scn)
            .unwrap_or(current_scn);
        let mut cursor_rs_id = checkpoint_position
            .as_ref()
            .and_then(|position| position.rs_id.clone());
        let mut cursor_ssn = checkpoint_position
            .as_ref()
            .and_then(|position| position.ssn);
        if snapshot_needed {
            self.snapshot(&connection, current_scn, &context.output)
                .await?;
            cursor_scn = current_scn;
            cursor_rs_id = None;
            cursor_ssn = None;
        }
        start_logminer(&connection, cursor_scn, self.config.archive_log_only_mode).await?;
        let mut event_serial = checkpoint_position
            .as_ref()
            .map_or(0, |position| position.event_serial);
        let mut heartbeat =
            tokio::time::interval(self.config.heartbeat_interval.max(Duration::from_secs(1)));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        info!(
            connector = %self.connector_name,
            start_scn = cursor_scn,
            max_retries = self.retry_policy.max_retries,
            "Oracle LogMiner streaming started"
        );
        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    stop_logminer(&connection).await?;
                    return Ok(());
                }
                _ = heartbeat.tick(), if !self.config.heartbeat_interval.is_zero() => {
                    let position = OraclePosition { scn: cursor_scn, commit_scn: Some(cursor_scn), transaction_id: None, rs_id: cursor_rs_id.clone(), ssn: cursor_ssn, event_serial, snapshot: false };
                    context.output.send(Ok(SourceRecord { event: None, position: SourcePosition::Oracle(position), boundary: RecordBoundary::Heartbeat, connector_state: None, signal_acknowledgements: Vec::new() })).await.map_err(|_| Error::Cancelled)?;
                }
                result = poll_logminer(&connection, &self.tables, cursor_scn, cursor_rs_id.as_deref(), cursor_ssn, self.config.batch_size) => {
                    let rows = result?;
                    if rows.is_empty() {
                        tokio::time::sleep(self.config.poll_interval).await;
                        continue;
                    }
                    for change in rows {
                        event_serial += 1;
                        let position = OraclePosition {
                            scn: change.scn,
                            commit_scn: Some(change.commit_scn),
                            transaction_id: Some(change.transaction_id.clone()),
                            rs_id: change.rs_id.clone(),
                            ssn: Some(change.ssn),
                            event_serial,
                            snapshot: false,
                        };
                        cursor_scn = change.scn;
                        cursor_rs_id = change.rs_id.clone();
                        cursor_ssn = Some(change.ssn);
                        if change.operation == Operation::Message && change.is_commit {
                            context.output.send(Ok(SourceRecord { event: None, position: SourcePosition::Oracle(position), boundary: RecordBoundary::TransactionCommit, connector_state: None, signal_acknowledgements: Vec::new() })).await.map_err(|_| Error::Cancelled)?;
                            continue;
                        }
                        let table = self.tables.iter().find(|table| table.owner.eq_ignore_ascii_case(&change.owner) && table.name.eq_ignore_ascii_case(&change.table)).cloned().unwrap_or(OracleTable { owner: change.owner.clone(), name: change.table.clone(), columns: change.after.as_ref().or(change.before.as_ref()).map_or_else(Vec::new, |row| row.keys().cloned().collect()) });
                        let event = self.event(&table, position, change.operation, change.before, change.after, Some(TransactionMetadata { id: change.transaction_id, total_order: None, collection_order: None }));
                        context.output.send(Ok(SourceRecord::data(event))).await.map_err(|_| Error::Cancelled)?;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct LogMinerChange {
    owner: String,
    table: String,
    operation: Operation,
    before: Option<Row>,
    after: Option<Row>,
    scn: u64,
    commit_scn: u64,
    transaction_id: String,
    rs_id: Option<String>,
    ssn: u64,
    is_commit: bool,
}

async fn discover_tables(
    connection: &Connection,
    config: &OracleSourceConfig,
) -> Result<Vec<OracleTable>> {
    let rows = query_all(
        connection,
        "SELECT OWNER, TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS ORDER BY OWNER, TABLE_NAME, COLUMN_ID",
        10_000,
    )
    .await?;
    let mut tables = Vec::new();
    for row in rows {
        let owner = text(&row, 0)?;
        let name = text(&row, 1)?;
        let namespace = format!("{owner}.{name}");
        if !config.schemas.is_empty()
            && !config
                .schemas
                .iter()
                .any(|schema| schema.eq_ignore_ascii_case(&owner))
        {
            continue;
        }
        if !selected(&namespace, &config.tables) {
            continue;
        }
        let column = text(&row, 2)?;
        if let Some(table) = tables
            .iter_mut()
            .find(|table: &&mut OracleTable| table.owner == owner && table.name == name)
        {
            table.columns.push(column);
        } else {
            tables.push(OracleTable {
                owner,
                name,
                columns: vec![column],
            });
        }
    }
    Ok(tables)
}

async fn start_logminer(connection: &Connection, start_scn: u64, archive_only: bool) -> Result<()> {
    let log_query = if archive_only {
        format!(
            "SELECT NAME FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL AND DELETED = 'NO' AND FIRST_CHANGE# <= {start_scn} ORDER BY FIRST_CHANGE#"
        )
    } else {
        "SELECT MEMBER FROM V$LOGFILE WHERE MEMBER IS NOT NULL".into()
    };
    let logs = query_all(connection, &log_query, 1_000).await?;
    for (index, row) in logs.iter().enumerate() {
        let member = text(row, 0)?;
        let option = if index == 0 {
            "DBMS_LOGMNR.NEW"
        } else {
            "DBMS_LOGMNR.ADDFILE"
        };
        let sql = format!(
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '{}', OPTIONS => {}); END;",
            member.replace('\'', "''"),
            option
        );
        connection.execute(&sql, &[]).await.map_err(oracle_error)?;
    }
    let options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY";
    let sql = format!(
        "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => {start_scn}, OPTIONS => {options}); END;"
    );
    connection.execute(&sql, &[]).await.map_err(oracle_error)?;
    Ok(())
}

async fn stop_logminer(connection: &Connection) -> Result<()> {
    connection
        .execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;", &[])
        .await
        .map_err(oracle_error)?;
    Ok(())
}

async fn poll_logminer(
    connection: &Connection,
    tables: &[OracleTable],
    cursor_scn: u64,
    cursor_rs_id: Option<&str>,
    cursor_ssn: Option<u64>,
    batch_size: usize,
) -> Result<Vec<LogMinerChange>> {
    let mut filter = String::new();
    if !tables.is_empty() {
        let predicates = tables
            .iter()
            .map(|table| {
                format!(
                    "(SEG_OWNER = '{}' AND TABLE_NAME = '{}')",
                    table.owner.replace('\'', "''"),
                    table.name.replace('\'', "''")
                )
            })
            .collect::<Vec<_>>();
        filter = format!(
            " AND (OPERATION_CODE IN (6,7) OR ({}))",
            predicates.join(" OR ")
        );
    }
    let cursor = cursor_rs_id.map_or_else(
        || format!("SCN > {cursor_scn}"),
        |rs_id| {
            let rs_id = rs_id.replace('\'', "''");
            let ssn = cursor_ssn.unwrap_or_default();
            format!(
                "(SCN > {cursor_scn} OR (SCN = {cursor_scn} AND (RS_ID > '{rs_id}' OR (RS_ID = '{rs_id}' AND SSN > {ssn}))))"
            )
        },
    );
    let sql = format!(
        "SELECT SCN, COMMIT_SCN, OPERATION_CODE, SQL_REDO, SQL_UNDO, SEG_OWNER, TABLE_NAME, XIDUSN, XIDSLT, XIDSQN, RS_ID, SSN FROM V$LOGMNR_CONTENTS WHERE {cursor} AND COMMIT_SCN IS NOT NULL AND OPERATION_CODE IN (1,2,3,5,6,7){filter} ORDER BY SCN, RS_ID, SSN FETCH FIRST {batch_size} ROWS ONLY"
    );
    let rows = query_all(connection, &sql, batch_size).await?;
    let mut changes = Vec::new();
    for row in rows {
        let scn = number(&row, 0)?;
        let commit_scn = number(&row, 1)?;
        let operation_code = number(&row, 2)?;
        let redo = text_optional(&row, 3);
        let undo = text_optional(&row, 4);
        let owner = text_optional(&row, 5).unwrap_or_default();
        let table = text_optional(&row, 6).unwrap_or_default();
        let transaction_id = format!(
            "{}.{}.{}",
            number(&row, 7).unwrap_or_default(),
            number(&row, 8).unwrap_or_default(),
            number(&row, 9).unwrap_or_default()
        );
        let rs_id = text_optional(&row, 10);
        let ssn = number(&row, 11)?;
        let is_commit = operation_code == 7;
        let (operation, before, after) = match operation_code {
            1 => (
                Operation::Create,
                None,
                redo.as_deref().and_then(parse_sql_row).map(|(_, row)| row),
            ),
            2 => (
                Operation::Delete,
                undo.as_deref().and_then(parse_sql_row).map(|(_, row)| row),
                None,
            ),
            3 => (
                Operation::Update,
                undo.as_deref().and_then(parse_sql_row).map(|(_, row)| row),
                redo.as_deref().and_then(parse_sql_row).map(|(_, row)| row),
            ),
            5 => (
                Operation::Message,
                None,
                redo.clone()
                    .map(|sql| Row::from([("_sql_redo".into(), DataValue::String(sql))])),
            ),
            _ => (Operation::Message, None, None),
        };
        let (owner, table) =
            if let Some((parsed_table, _)) = redo.as_deref().and_then(parse_sql_row) {
                parsed_table
                    .split_once('.')
                    .map_or((owner, table), |(owner, table)| {
                        (owner.to_string(), table.to_string())
                    })
            } else {
                (owner, table)
            };
        changes.push(LogMinerChange {
            owner,
            table,
            operation,
            before,
            after,
            scn,
            commit_scn,
            transaction_id,
            rs_id,
            ssn,
            is_commit,
        });
    }
    Ok(changes)
}

async fn query_all(
    connection: &Connection,
    sql: &str,
    fetch_size: usize,
) -> Result<Vec<oracle_rs::Row>> {
    let mut result = connection.query(sql, &[]).await.map_err(oracle_error)?;
    let columns = result.columns.clone();
    let mut rows = result.rows;
    while result.has_more_rows {
        result = connection
            .fetch_more(result.cursor_id, &columns, fetch_size as u32)
            .await
            .map_err(oracle_error)?;
        rows.extend(result.rows.clone());
    }
    Ok(rows)
}

async fn query_scalar(connection: &Connection, sql: &str) -> Result<String> {
    let rows = query_all(connection, sql, 1).await?;
    rows.first()
        .and_then(|row| row.get(0))
        .map(value_string)
        .ok_or_else(|| Error::Source(format!("Oracle query returned no value: {sql}")))
}

async fn query_scalar_u64(connection: &Connection, sql: &str) -> Result<u64> {
    let value = query_scalar(connection, sql).await?;
    value
        .parse()
        .map_err(|error| Error::Source(format!("invalid Oracle SCN {value:?}: {error}")))
}

fn row_to_rustium(columns: &[String], row: &oracle_rs::Row) -> Row {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            (
                column.clone(),
                row.get(index).map_or(DataValue::Null, oracle_value),
            )
        })
        .collect()
}

fn oracle_value(value: &Value) -> DataValue {
    match value {
        Value::Null => DataValue::Null,
        Value::String(value) => DataValue::String(value.clone()),
        Value::Bytes(value) => DataValue::Bytes(value.clone()),
        Value::Integer(value) => DataValue::Int64(*value),
        Value::Float(value) => DataValue::Float64(*value),
        Value::Number(value) => DataValue::Decimal(value.as_str().into()),
        Value::Date(value) => DataValue::Timestamp(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            value.year, value.month, value.day, value.hour, value.minute, value.second
        )),
        Value::Timestamp(value) => DataValue::Timestamp(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond
        )),
        Value::Boolean(value) => DataValue::Boolean(*value),
        Value::Json(value) => DataValue::Json(value.clone()),
        Value::RowId(value) => DataValue::String(format!("{value:?}")),
        Value::Lob(value) => DataValue::String(format!("{value:?}")),
        Value::Vector(value) => DataValue::String(format!("{value:?}")),
        Value::Cursor(value) => DataValue::String(format!("{value:?}")),
        Value::Collection(value) => DataValue::String(format!("{value:?}")),
    }
}

fn value_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Integer(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Number(value) => value.as_str().into(),
        _ => format!("{value:?}"),
    }
}

fn text(row: &oracle_rs::Row, index: usize) -> Result<String> {
    row.get(index)
        .map(value_string)
        .ok_or_else(|| Error::Source(format!("Oracle result column {index} is null")))
}

fn text_optional(row: &oracle_rs::Row, index: usize) -> Option<String> {
    row.get(index)
        .filter(|value| !value.is_null())
        .map(value_string)
}

fn number(row: &oracle_rs::Row, index: usize) -> Result<u64> {
    text(row, index)?.parse().map_err(|error| {
        Error::Source(format!(
            "Oracle result column {index} is not an unsigned number: {error}"
        ))
    })
}

fn selected(namespace: &str, tables: &rustium_config::TableSelection) -> bool {
    let includes = tables
        .include
        .iter()
        .filter_map(|pattern| Regex::new(pattern).ok());
    let excludes = tables
        .exclude
        .iter()
        .filter_map(|pattern| Regex::new(pattern).ok());
    (tables.include.is_empty() || includes.into_iter().any(|regex| regex.is_match(namespace)))
        && !excludes.into_iter().any(|regex| regex.is_match(namespace))
}

fn qualified_name(owner: &str, table: &str) -> String {
    format!(
        "\"{}\".\"{}\"",
        owner.replace('\"', "\"\""),
        table.replace('\"', "\"\"")
    )
}

fn parse_sql_row(sql: &str) -> Option<(String, Row)> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let action = Regex::new(
        r#"(?i)^(?:insert\s+into|update|delete\s+from)\s+((?:"[^"]+"|[A-Za-z0-9_$#]+)(?:\.(?:"[^"]+"|[A-Za-z0-9_$#]+))?)"#,
    )
    .ok()?
    .captures(sql)?
    .get(1)?
    .as_str()
    .to_string();
    let table = action
        .split('.')
        .map(|part| part.trim_matches('\"').to_string())
        .collect::<Vec<_>>();
    let table = if table.len() == 1 {
        table[0].clone()
    } else {
        format!("{}.{}", table[table.len() - 2], table[table.len() - 1])
    };
    let lower = sql.to_ascii_lowercase();
    if lower.starts_with("insert") {
        let columns_start = sql.find('(')?;
        let columns_end = find_matching(sql, columns_start)?;
        let values_start = lower[columns_end..].find("values")? + columns_end;
        let values_open = sql[values_start..].find('(')? + values_start;
        let values_end = find_matching(sql, values_open)?;
        let columns = split_sql_list(&sql[columns_start + 1..columns_end]);
        let values = split_sql_list(&sql[values_open + 1..values_end]);
        return Some((
            table,
            columns
                .into_iter()
                .zip(values)
                .map(|(key, value)| (normalize_column(&key), parse_literal(&value)))
                .collect(),
        ));
    }
    if lower.starts_with("update") {
        let set_start = lower.find(" set ")? + 5;
        let where_start = lower[set_start..]
            .find(" where ")
            .map(|offset| offset + set_start);
        let assignments = &sql[set_start..where_start.unwrap_or(sql.len())];
        let mut row = Row::new();
        for assignment in split_sql_list(assignments) {
            if let Some((key, value)) = assignment.split_once('=') {
                row.insert(normalize_column(key), parse_literal(value));
            }
        }
        if let Some(where_start) = where_start {
            for predicate in sql[where_start + 7..].split("AND") {
                if let Some((key, value)) = predicate.split_once('=') {
                    row.insert(normalize_column(key), parse_literal(value));
                }
            }
        }
        return Some((table, row));
    }
    None
}

fn split_sql_list(value: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut quote = false;
    let mut depth = 0_i32;
    for character in value.chars() {
        match character {
            '\'' => {
                quote = !quote;
                current.push(character);
            }
            '(' if !quote => {
                depth += 1;
                current.push(character);
            }
            ')' if !quote => {
                depth -= 1;
                current.push(character);
            }
            ',' if !quote && depth == 0 => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(character),
        }
    }
    if !current.trim().is_empty() {
        values.push(current.trim().to_string());
    }
    values
}

fn find_matching(value: &str, start: usize) -> Option<usize> {
    let mut depth = 0_i32;
    let mut quote = false;
    for (index, character) in value.char_indices().skip_while(|(index, _)| *index < start) {
        match character {
            '\'' => quote = !quote,
            '(' if !quote => depth += 1,
            ')' if !quote => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn normalize_column(value: &str) -> String {
    value.trim().trim_matches('\"').to_string()
}

fn parse_literal(value: &str) -> DataValue {
    let value = value.trim();
    if value.eq_ignore_ascii_case("null") {
        return DataValue::Null;
    }
    if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        return DataValue::String(value[1..value.len() - 1].replace("''", "'"));
    }
    if let Ok(integer) = value.parse::<i64>() {
        return DataValue::Int64(integer);
    }
    if let Ok(float) = value.parse::<f64>() {
        return DataValue::Float64(float);
    }
    DataValue::String(value.to_string())
}

fn is_probable_key(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized == "id" || normalized.ends_with("_id") || normalized == "_rowid"
}

fn value_type(value: &DataValue) -> &'static str {
    match value {
        DataValue::Null => "null",
        DataValue::Boolean(_) => "boolean",
        DataValue::Int32(_) => "int32",
        DataValue::Int64(_) => "int64",
        DataValue::UInt64(_) => "uint64",
        DataValue::Float64(_) => "double",
        DataValue::Decimal(_) => "decimal",
        DataValue::String(_) => "string",
        DataValue::Bytes(_) => "bytes",
        DataValue::Date(_) => "date",
        DataValue::Time(_) => "time",
        DataValue::Timestamp(_) => "timestamp",
        DataValue::Uuid(_) => "uuid",
        DataValue::Json(_) => "json",
        DataValue::Array(_) => "array",
        DataValue::Map(_) => "map",
        DataValue::Unavailable => "unavailable",
    }
}

fn oracle_error(error: impl std::fmt::Display) -> Error {
    Error::Source(format!("Oracle: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_logminer_insert_and_update_rows() {
        let (_, insert) =
            parse_sql_row(r#"insert into "APP"."ORDERS" ("ID", "NAME") values (42, 'new')"#)
                .expect("insert should parse");
        assert_eq!(insert.get("ID"), Some(&DataValue::Int64(42)));
        assert_eq!(insert.get("NAME"), Some(&DataValue::String("new".into())));

        let (_, update) =
            parse_sql_row(r#"update "APP"."ORDERS" set "NAME" = 'new' where "ID" = 42"#)
                .expect("update should parse");
        assert_eq!(update.get("ID"), Some(&DataValue::Int64(42)));
        assert_eq!(update.get("NAME"), Some(&DataValue::String("new".into())));
    }

    #[test]
    fn preserves_oracle_literals_without_guessing_functions() {
        assert_eq!(parse_literal("NULL"), DataValue::Null);
        assert_eq!(parse_literal("3.15"), DataValue::Float64(3.15));
        assert_eq!(
            parse_literal("TO_TIMESTAMP('2026-01-01', 'YYYY-MM-DD')"),
            DataValue::String("TO_TIMESTAMP('2026-01-01', 'YYYY-MM-DD')".into())
        );
    }
}
