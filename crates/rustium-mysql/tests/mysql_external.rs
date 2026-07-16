use std::{
    error::Error as StdError,
    io,
    time::{Duration, SystemTime},
};

use mysql_async::{Conn, OptsBuilder, prelude::Queryable};
use rustium_config::{MySqlSourceConfig, SnapshotConfig, SnapshotMode, TableSelection};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, ConnectorStateEnvelope, DataValue, Operation,
    RecordBoundary, SignalRecord, SignalSender, SourceConnector, SourceContext, SourcePosition,
    SourceRecord,
};
use rustium_mysql::MySqlSource;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);

struct TestSettings {
    host: String,
    port: u16,
    admin_user: String,
    admin_password: String,
    cdc_user: String,
    cdc_password: String,
    database: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external MySQL 8.0+ server with row binlog and GTID enabled"]
async fn runs_incremental_snapshot_from_mysql_signal_channel() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_incremental_{}", &suffix[..12]);
    let signal_table = format!("rustium_signal_{}", &suffix[..12]);
    let connector_name = format!("mysql-incremental-{}", &suffix[..12]);
    let qualified_table = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&table_name)
    );
    let qualified_signal = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&signal_table)
    );
    let mut admin = connect_admin(&settings).await?;
    let outcome: TestResult = async {
        admin.query_drop(format!("CREATE TABLE {qualified_table} (id BIGINT PRIMARY KEY, value VARCHAR(50) NOT NULL)")).await?;
        admin.query_drop(format!("CREATE TABLE {qualified_signal} (id VARCHAR(200) PRIMARY KEY, type VARCHAR(64) NOT NULL, data JSON NOT NULL)")).await?;
        admin.query_drop(format!("INSERT INTO {qualified_table} VALUES (1,'one'),(2,'two')")).await?;
        let mut config = settings.source_config(&table_name);
        config.signal_data_collection = Some(format!("{}.{}", settings.database, signal_table));
        config.signal_enabled_channels = vec!["source".into(), "in-process".into()];
        config.incremental_snapshot_chunk_size = 2;
        let (snapshot_position, schema_history) = capture_snapshot(&connector_name, config.clone()).await?;
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: snapshot_position.clone(),
            snapshot_completed: true,
            config_fingerprint: "mysql-incremental-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history),
        };
        let mut source = MySqlSource::new(&connector_name, config, SnapshotConfig { mode: SnapshotMode::Never, fetch_size: 1 });
        source.validate().await?;
        let (mut output, signal_sender, cancellation, source_task) = start_source_with_signals(source, Some(checkpoint), Some(snapshot_position));
        signal_sender.send(SignalRecord::new(
            "mysql-incremental-1",
            "execute-snapshot",
            serde_json::json!({"type":"incremental","data-collections":[format!("{}.{}", settings.database, table_name)]}),
        )).await?;
        let mut rows = 0;
        let capture_result: TestResult = async {
            loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else { continue };
                if event.source.attributes.get("rustium.snapshot.kind").and_then(serde_json::Value::as_str) != Some("incremental") { continue; }
                require(event.operation == Operation::Read, "MySQL incremental event is not a read")?;
                rows += 1;
                if rows == 2 { break Ok(()) }
            }
        }.await;
        finish_source(cancellation, source_task, capture_result).await?;
        require(rows == 2, "MySQL incremental snapshot did not emit all rows")
    }.await;
    let cleanup = async {
        admin
            .query_drop(format!("DROP TABLE IF EXISTS {qualified_signal}"))
            .await?;
        admin
            .query_drop(format!("DROP TABLE IF EXISTS {qualified_table}"))
            .await?;
        admin.disconnect().await.map_err(boxed_error)
    }
    .await;
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => {
            eprintln!("MySQL incremental cleanup failed: {cleanup_error}");
            Err(error)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external MySQL 8.0+ server with row binlog and GTID enabled"]
async fn resumes_incremental_snapshot_with_persisted_keyset() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_keyset_{}", &suffix[..12]);
    let connector_name = format!("mysql-keyset-{}", &suffix[..12]);
    let signal_id = format!("mysql-keyset-signal-{}", &suffix[..12]);
    let qualified_table = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&table_name)
    );
    let mut admin = connect_admin(&settings).await?;
    let outcome: TestResult = async {
        admin
            .query_drop(format!(
                "CREATE TABLE {qualified_table} (id BIGINT PRIMARY KEY, value VARCHAR(50) NOT NULL)"
            ))
            .await?;
        admin
            .query_drop(format!(
                "INSERT INTO {qualified_table} VALUES (1,'one'),(2,'two')"
            ))
            .await?;
        let mut config = settings.source_config(&table_name);
        config.signal_enabled_channels = vec!["in-process".into()];
        config.incremental_snapshot_chunk_size = 1;
        let (snapshot_position, schema_history) =
            capture_snapshot(&connector_name, config.clone()).await?;
        admin
            .query_drop(format!("INSERT INTO {qualified_table} VALUES (3,'three')"))
            .await?;
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: snapshot_position.clone(),
            snapshot_completed: true,
            config_fingerprint: "mysql-keyset-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history),
        };

        let mut source = MySqlSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
            },
        );
        source.validate().await?;
        let (mut output, signal_sender, cancellation, source_task) =
            start_source_with_signals(source, Some(checkpoint), Some(snapshot_position));
        signal_sender
            .send(SignalRecord::new(
                &signal_id,
                "execute-snapshot",
                serde_json::json!({
                    "type":"incremental",
                    "data-collections":[format!("{}\\.{}", settings.database, table_name)]
                }),
            ))
            .await?;

        let first_checkpoint: TestResult<(SourcePosition, ConnectorStateEnvelope)> = async {
            let mut saw_first_row = false;
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = &record.event
                    && event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                {
                    let id = event
                        .after
                        .as_ref()
                        .and_then(|row| row.get("id"))
                        .and_then(mysql_integer);
                    require(
                        id == Some(1),
                        format!(
                            "first MySQL keyset chunk did not contain primary key 1; got {id:?}"
                        ),
                    )?;
                    saw_first_row = true;
                }
                if saw_first_row && record.boundary == RecordBoundary::TransactionCommit {
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("MySQL keyset chunk checkpoint has no connector state")
                    })?;
                    break Ok((record.position, state));
                }
            }
        }
        .await;
        let first_checkpoint = finish_source(cancellation, source_task, first_checkpoint).await?;

        admin
            .query_drop(format!("DELETE FROM {qualified_table} WHERE id = 2"))
            .await?;
        admin
            .query_drop(format!("INSERT INTO {qualified_table} VALUES (0,'zero')"))
            .await?;
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: first_checkpoint.0.clone(),
            snapshot_completed: true,
            config_fingerprint: "mysql-keyset-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(first_checkpoint.1),
        };
        let mut source = MySqlSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source(source, Some(checkpoint), Some(first_checkpoint.0));
        let resumed: TestResult = async {
            let mut saw_last_row = false;
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = &record.event
                    && event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                {
                    let id = event
                        .after
                        .as_ref()
                        .and_then(|row| row.get("id"))
                        .and_then(mysql_integer);
                    require(
                        id == Some(3),
                        format!(
                            "resumed MySQL keyset snapshot reread or skipped the wrong primary key; got {id:?}"
                        ),
                    )?;
                    saw_last_row = true;
                }
                if saw_last_row && record.boundary == RecordBoundary::TransactionCommit {
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("completed MySQL keyset checkpoint has no connector state")
                    })?;
                    let completed = state
                        .payload
                        .get("completed_signal_ids")
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(|| {
                            test_error("completed MySQL keyset checkpoint has no signal IDs")
                        })?;
                    require(
                        completed.iter().any(|id| id.as_str() == Some(&signal_id)),
                        "completed MySQL keyset checkpoint did not persist the signal ID",
                    )?;
                    break Ok(());
                }
            }
        }
        .await;
        finish_source(cancellation, source_task, resumed).await
    }
    .await;
    let cleanup = async {
        admin
            .query_drop(format!("DROP TABLE IF EXISTS {qualified_table}"))
            .await?;
        admin.disconnect().await.map_err(boxed_error)
    }
    .await;
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => {
            eprintln!("MySQL keyset cleanup failed: {cleanup_error}");
            Err(error)
        }
    }
}

impl TestSettings {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            host: required_env("RUSTIUM_MYSQL_TEST_HOST")?,
            port: required_env("RUSTIUM_MYSQL_TEST_PORT")?.parse()?,
            admin_user: required_env("RUSTIUM_MYSQL_TEST_ADMIN_USER")?,
            admin_password: required_env("RUSTIUM_MYSQL_TEST_ADMIN_PASSWORD")?,
            cdc_user: required_env("RUSTIUM_MYSQL_TEST_USER")?,
            cdc_password: required_env("RUSTIUM_MYSQL_TEST_PASSWORD")?,
            database: required_env("RUSTIUM_MYSQL_TEST_DATABASE")?,
        })
    }

    fn source_config(&self, table_name: &str) -> MySqlSourceConfig {
        MySqlSourceConfig {
            hostname: self.host.clone(),
            port: self.port,
            username: self.cdc_user.clone(),
            password: self.cdc_password.clone(),
            databases: vec![self.database.clone()],
            server_id: 50_000
                + table_name
                    .bytes()
                    .fold(0_u32, |hash, byte| hash.wrapping_mul(31) + u32::from(byte))
                    % 10_000,
            tables: TableSelection {
                include: vec![format!(r"{}\.{table_name}", self.database)],
                exclude: Vec::new(),
            },
            ssl_mode: "disabled".into(),
            ssl_ca: None,
            ssl_cert: None,
            ssl_key: None,
            connect_timeout: Duration::from_secs(10),
            connect_keep_alive: true,
            connect_keep_alive_interval: Duration::from_millis(250),
            reconnect_max_attempts: 5,
            schema_history_skip_unparseable_ddl: false,
            gtid_source_includes: Vec::new(),
            gtid_source_excludes: Vec::new(),
            gtid_source_filter_dml_events: true,
            heartbeat_interval: Duration::ZERO,
            heartbeat_action_query: None,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            signal_data_collection: None,
            signal_enabled_channels: vec!["source".into(), "file".into(), "in-process".into()],
            signal_file: "signals.jsonl".into(),
            signal_poll_interval: Duration::from_millis(500),
            incremental_snapshot_chunk_size: 1_024,
            incremental_snapshot_watermarking_strategy: "insert_insert".into(),
            signal_kafka_topic: None,
            signal_kafka_bootstrap_servers: Vec::new(),
            signal_kafka_group_id: "kafka-signal".into(),
            signal_kafka_poll_timeout: Duration::from_millis(100),
            signal_kafka_consumer_properties: std::collections::BTreeMap::new(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external MySQL 8.0+ server with row binlog and GTID enabled"]
async fn emits_periodic_heartbeat_from_safe_binlog_position() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_heartbeat_{}", &suffix[..12]);
    let action_table_name = format!("rustium_heartbeat_action_{}", &suffix[..12]);
    let connector_name = format!("mysql-heartbeat-{}", &suffix[..12]);
    let qualified_table = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&table_name)
    );
    let qualified_action_table = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&action_table_name)
    );
    let mut admin = connect_admin(&settings).await?;

    let outcome: TestResult = async {
        admin
            .query_drop(format!(
                "CREATE TABLE {qualified_table} (id BIGINT NOT NULL PRIMARY KEY)"
            ))
            .await?;
        admin
            .query_drop(format!(
                "CREATE TABLE {qualified_action_table} (id BIGINT NOT NULL PRIMARY KEY, touched_at TIMESTAMP NULL)"
            ))
            .await?;
        admin
            .query_drop(format!(
                "INSERT INTO {qualified_action_table} (id) VALUES (1)"
            ))
            .await?;
        let cdc_user = settings.cdc_user.replace('\'', "''");
        admin
            .query_drop(format!(
                "GRANT UPDATE ON {qualified_action_table} TO '{cdc_user}'@'%'"
            ))
            .await?;
        let mut config = settings.source_config(&table_name);
        config.heartbeat_interval = Duration::from_millis(100);
        config.heartbeat_action_query = Some(format!(
            "UPDATE {qualified_action_table} SET touched_at = CURRENT_TIMESTAMP WHERE id = 1"
        ));
        config.heartbeat_topics_prefix = "__rustium-test-heartbeat".into();
        let mut source = MySqlSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None, None);

        let capture_result: TestResult = async {
            loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                if event.operation != Operation::Message {
                    continue;
                }
                require(
                    record.boundary == RecordBoundary::Heartbeat,
                    "MySQL heartbeat has the wrong record boundary",
                )?;
                require(
                    event.source.table.is_none() && event.source.schema.is_none(),
                    "MySQL heartbeat was exposed as a table event",
                )?;
                require(
                    event.source.attributes.get("rustium.heartbeat") == Some(&true.into()),
                    "MySQL heartbeat marker is missing",
                )?;
                require(
                    matches!(record.position, SourcePosition::MySql(ref position) if !position.snapshot),
                    "MySQL heartbeat does not carry a streaming binlog position",
                )?;
                break Ok(());
            }
        }
        .await;

        finish_source(cancellation, source_task, capture_result).await?;
        let touched: Option<bool> = admin
            .query_first(format!(
                "SELECT touched_at IS NOT NULL FROM {qualified_action_table} WHERE id = 1"
            ))
            .await?;
        require(
            touched == Some(true),
            "MySQL heartbeat.action.query did not update the action table",
        )
    }
    .await;

    let cleanup_result: TestResult = async {
        let cdc_user = settings.cdc_user.replace('\'', "''");
        admin
            .query_drop(format!(
                "REVOKE UPDATE ON {qualified_action_table} FROM '{cdc_user}'@'%'"
            ))
            .await?;
        admin
            .query_drop(format!("DROP TABLE IF EXISTS {qualified_table}"))
            .await?;
        admin
            .query_drop(format!("DROP TABLE IF EXISTS {qualified_action_table}"))
            .await?;
        Ok(())
    }
    .await;
    let close_result = admin.disconnect().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("MySQL heartbeat test cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("MySQL heartbeat test connection close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external MySQL 8.4 server with PARTIAL_JSON support"]
async fn reconstructs_partial_json_updates_from_before_images() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_partial_json_{}", &suffix[..12]);
    let connector_name = format!("mysql-partial-json-{}", &suffix[..12]);
    let qualified_table = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&table_name)
    );
    let mut admin = connect_admin(&settings).await?;
    let original_options: String = admin
        .query_first("SELECT @@GLOBAL.binlog_row_value_options")
        .await?
        .ok_or_else(|| test_error("MySQL did not return binlog_row_value_options"))?;

    let outcome: TestResult = async {
        admin
            .query_drop(format!(
                "CREATE TABLE {qualified_table} (id BIGINT PRIMARY KEY, payload JSON NOT NULL)"
            ))
            .await?;
        admin
            .query_drop(format!(
                "INSERT INTO {qualified_table} VALUES (1, '{{\"name\":\"Alice\",\"tags\":[\"new\"]}}')"
            ))
            .await?;
        admin
            .query_drop(format!(
                "INSERT INTO {qualified_table} VALUES (2, '{{\"name\":\"Carol\",\"tags\":[\"new\"]}}')"
            ))
            .await?;
        admin
            .query_drop("SET GLOBAL binlog_row_value_options = 'PARTIAL_JSON'")
            .await?;

        let config = settings.source_config(&table_name);
        let (snapshot_position, schema_history) =
            capture_snapshot(&connector_name, config.clone()).await?;
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: snapshot_position.clone(),
            snapshot_completed: true,
            config_fingerprint: "mysql-partial-json-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history),
        };

        admin
            .query_drop(format!(
                "UPDATE {qualified_table} SET payload = JSON_SET(payload, '$.name', 'Bob', '$.tags[1]', 'vip') WHERE id = 1"
            ))
            .await?;

        let mut source = MySqlSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source(source, Some(checkpoint), Some(snapshot_position));
        let capture_result: TestResult = async {
            loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                if event.source.table.as_deref() != Some(table_name.as_str()) {
                    continue;
                }
                require(
                    event.operation == Operation::Update,
                    "partial JSON row was not emitted as an update",
                )?;
                require(
                    event.after.as_ref().and_then(|row| row.get("payload"))
                        == Some(&DataValue::Json(serde_json::json!({
                            "name": "Bob",
                            "tags": ["new", "vip"]
                        }))),
                    "partial JSON after image was not reconstructed",
                )?;
                break Ok(());
            }
        }
        .await;
        finish_source(cancellation, source_task, capture_result).await
    }
    .await;

    let restore_result = admin
        .query_drop(format!(
            "SET GLOBAL binlog_row_value_options = '{}'",
            original_options.replace('\'', "''")
        ))
        .await
        .map_err(boxed_error);
    let cleanup_result = admin
        .query_drop(format!("DROP TABLE IF EXISTS {qualified_table}"))
        .await
        .map_err(boxed_error);
    let close_result = admin.disconnect().await.map_err(boxed_error);
    match (outcome, restore_result, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), restore, cleanup, close) => {
            if let Err(restore_error) = restore {
                eprintln!("MySQL partial JSON test restore also failed: {restore_error}");
            }
            if let Err(cleanup_error) = cleanup {
                eprintln!("MySQL partial JSON test cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("MySQL partial JSON test connection close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _, _)
        | (Ok(()), Ok(()), Err(error), _)
        | (Ok(()), Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external MySQL 8.0+ server with row binlog and GTID enabled"]
async fn snapshots_and_replays_destructive_ddl_from_checkpoint() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_mysql_{}", &suffix[..12]);
    let connector_name = format!("mysql-external-{}", &suffix[..12]);
    let qualified_table = format!(
        "{}.{}",
        quote_identifier(&settings.database),
        quote_identifier(&table_name)
    );
    let mut admin = connect_admin(&settings).await?;

    let outcome: TestResult = async {
        admin
            .query_drop(format!(
                "CREATE TABLE {qualified_table} (\
                    id BIGINT PRIMARY KEY, \
                    customer VARCHAR(100) NOT NULL, \
                    amount DECIMAL(10,2) NOT NULL\
                 )"
            ))
            .await?;
        admin
            .query_drop(format!(
                "INSERT INTO {qualified_table} VALUES \
                    (1, 'Alice', 12.30), \
                    (2, 'Bob', 45.60)"
            ))
            .await?;

        let source_uuid = admin
            .query_first::<String, _>("SELECT @@GLOBAL.server_uuid")
            .await?
            .ok_or_else(|| test_error("MySQL did not return server_uuid"))?;
        let mut config = settings.source_config(&table_name);
        config.gtid_source_includes = vec![source_uuid];
        let (snapshot_position, schema_history) =
            capture_snapshot(&connector_name, config.clone()).await?;
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: snapshot_position.clone(),
            snapshot_completed: true,
            config_fingerprint: "mysql-external-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history),
        };

        admin
            .query_drop(format!(
                "INSERT INTO {qualified_table} (id, customer, amount) \
                 VALUES (3, 'Cara', 10.25)"
            ))
            .await?;
        admin
            .query_drop(format!(
                "ALTER TABLE {qualified_table} \
                 DROP COLUMN customer, \
                 ADD COLUMN status VARCHAR(20) NOT NULL AFTER amount"
            ))
            .await?;
        admin
            .query_drop(format!(
                "INSERT INTO {qualified_table} (id, amount, status) \
                 VALUES (4, 20.50, 'ready')"
            ))
            .await?;

        capture_replay(
            &connector_name,
            &table_name,
            config,
            checkpoint,
            snapshot_position,
        )
        .await
    }
    .await;

    let cleanup_result = admin
        .query_drop(format!("DROP TABLE IF EXISTS {qualified_table}"))
        .await
        .map_err(boxed_error);
    let close_result = admin.disconnect().await.map_err(boxed_error);

    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("MySQL test cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("MySQL test connection close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

async fn capture_snapshot(
    connector_name: &str,
    config: MySqlSourceConfig,
) -> TestResult<(SourcePosition, ConnectorStateEnvelope)> {
    let mut source = MySqlSource::new(
        connector_name,
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
        },
    );
    source.validate().await?;
    let (mut output, cancellation, source_task) = start_source(source, None, None);

    let capture_result: TestResult<(SourcePosition, ConnectorStateEnvelope)> = async {
        let mut snapshot_rows = 0;
        loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                require(
                    snapshot_rows == 2,
                    "MySQL snapshot did not emit exactly two rows",
                )?;
                let state = record
                    .connector_state
                    .ok_or_else(|| test_error("snapshot completion has no MySQL schema history"))?;
                break Ok((record.position, state));
            }
            let event = record
                .event
                .ok_or_else(|| test_error("snapshot data record has no event"))?;
            require(
                event.operation == Operation::Read,
                "snapshot event is not a read",
            )?;
            snapshot_rows += 1;
        }
    }
    .await;

    finish_source(cancellation, source_task, capture_result).await
}

async fn capture_replay(
    connector_name: &str,
    table_name: &str,
    config: MySqlSourceConfig,
    checkpoint: Checkpoint,
    acknowledged_position: SourcePosition,
) -> TestResult {
    let mut source = MySqlSource::new(
        connector_name,
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
        },
    );
    source.validate().await?;
    let (mut output, cancellation, source_task) =
        start_source(source, Some(checkpoint), Some(acknowledged_position));

    let capture_result: TestResult = async {
        let mut saw_old_schema = false;
        let mut saw_ddl_state = false;
        loop {
            let record = receive(&mut output).await?;
            if let Some(event) = record.event {
                if event.source.table.as_deref() != Some(table_name) {
                    continue;
                }
                require(
                    event.operation == Operation::Create,
                    "unexpected MySQL replay operation",
                )?;
                let after = event
                    .after
                    .ok_or_else(|| test_error("MySQL create event has no after image"))?;
                if after.get("id") == Some(&DataValue::Int32(3)) {
                    require(
                        event.schema.version == 1,
                        "old row used the wrong schema version",
                    )?;
                    require(
                        after.get("customer") == Some(&DataValue::String("Cara".into())),
                        "old row was not decoded with the historical customer column",
                    )?;
                    require(
                        !after.contains_key("status"),
                        "old row contains the new status column",
                    )?;
                    saw_old_schema = true;
                } else if after.get("id") == Some(&DataValue::Int32(4)) {
                    require(saw_old_schema, "new row arrived before the old-schema row")?;
                    require(
                        saw_ddl_state,
                        "new row arrived before a checkpointable DDL state",
                    )?;
                    require(
                        event.schema.version == 2,
                        "new row used the wrong schema version",
                    )?;
                    require(
                        after.get("status") == Some(&DataValue::String("ready".into())),
                        "new row was not decoded with the status column",
                    )?;
                    require(
                        !after.contains_key("customer"),
                        "new row still contains the dropped customer column",
                    )?;
                    break;
                }
            } else if saw_old_schema && record.connector_state.is_some() {
                saw_ddl_state = true;
            }
        }
        Ok(())
    }
    .await;

    finish_source(cancellation, source_task, capture_result).await
}

fn start_source(
    source: MySqlSource,
    initial_checkpoint: Option<Checkpoint>,
    acknowledged_position: Option<SourcePosition>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    let (output_tx, output_rx) = mpsc::channel(64);
    let (ack_tx, ack_rx) = watch::channel(acknowledged_position);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        let _ack_tx = ack_tx;
        let mut source = source;
        source
            .run(SourceContext {
                output: output_tx,
                acknowledged: ack_rx,
                initial_checkpoint,
                signals: rustium_core::signal_channel(1).1,
                cancellation: source_cancel,
            })
            .await
    });
    (output_rx, cancellation, source_task)
}

fn start_source_with_signals(
    source: MySqlSource,
    initial_checkpoint: Option<Checkpoint>,
    acknowledged_position: Option<SourcePosition>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    SignalSender,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    let (output_tx, output_rx) = mpsc::channel(64);
    let (signal_sender, signals) = rustium_core::signal_channel(16);
    let (ack_tx, ack_rx) = watch::channel(acknowledged_position);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        let _ack_tx = ack_tx;
        let mut source = source;
        source
            .run(SourceContext {
                output: output_tx,
                acknowledged: ack_rx,
                initial_checkpoint,
                signals,
                cancellation: source_cancel,
            })
            .await
    });
    (output_rx, signal_sender, cancellation, source_task)
}

async fn finish_source<T>(
    cancellation: CancellationToken,
    source_task: JoinHandle<rustium_core::Result<()>>,
    capture_result: TestResult<T>,
) -> TestResult<T> {
    cancellation.cancel();
    let source_result = source_task.await.map_err(boxed_error)?;
    match (capture_result, source_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(capture_error), Ok(())) => Err(capture_error),
        (Ok(_), Err(source_error)) => Err(boxed_error(source_error)),
        (Err(capture_error), Err(source_error)) => Err(test_error(format!(
            "{capture_error}; MySQL source also failed: {source_error}"
        ))),
    }
}

async fn receive(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult<SourceRecord> {
    tokio::time::timeout(RECEIVE_TIMEOUT, output.recv())
        .await
        .map_err(|_| test_error("timed out waiting for a MySQL source record"))?
        .ok_or_else(|| test_error("MySQL source closed before the test completed"))?
        .map_err(boxed_error)
}

async fn connect_admin(settings: &TestSettings) -> TestResult<Conn> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(settings.host.clone())
        .tcp_port(settings.port)
        .user(Some(settings.admin_user.clone()))
        .pass(Some(settings.admin_password.clone()))
        .db_name(Some(settings.database.clone()))
        .prefer_socket(false);
    Conn::new(opts).await.map_err(boxed_error)
}

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| test_error(format!("required environment variable {name} is not set")))
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(test_error(message))
    }
}

fn mysql_integer(value: &DataValue) -> Option<i128> {
    match value {
        DataValue::Int32(value) => Some(i128::from(*value)),
        DataValue::Int64(value) => Some(i128::from(*value)),
        DataValue::UInt64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn test_error(message: impl Into<String>) -> Box<dyn StdError + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

fn boxed_error(error: impl StdError + Send + Sync + 'static) -> Box<dyn StdError + Send + Sync> {
    Box::new(error)
}
