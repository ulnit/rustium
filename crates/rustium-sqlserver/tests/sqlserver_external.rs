use std::{
    collections::BTreeMap,
    error::Error as StdError,
    io,
    time::{Duration, SystemTime},
};

use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use rustium_config::{SnapshotConfig, SnapshotMode, SqlServerSourceConfig, TableSelection};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, ConnectorStateEnvelope, DataValue, Operation,
    RecordBoundary, RetryPolicy, SignalRecord, SignalSender, SourceConnector, SourceContext,
    SourcePosition, SourceRecord, SqlServerPosition,
};
use rustium_signal_kafka::KafkaSignalChannel;
use rustium_sqlserver::SqlServerSource;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::{compat::TokioAsyncWriteCompatExt, sync::CancellationToken};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
type SqlClient = Client<tokio_util::compat::Compat<TcpStream>>;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(90);

struct TestSettings {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl TestSettings {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            host: required_env("RUSTIUM_SQLSERVER_TEST_HOST")?,
            port: required_env("RUSTIUM_SQLSERVER_TEST_PORT")?.parse()?,
            user: required_env("RUSTIUM_SQLSERVER_TEST_USER")?,
            password: required_env("RUSTIUM_SQLSERVER_TEST_PASSWORD")?,
            database: required_env("RUSTIUM_SQLSERVER_TEST_DATABASE")?,
        })
    }

    fn source_config(&self, table_name: &str) -> SqlServerSourceConfig {
        SqlServerSourceConfig {
            hostname: self.host.clone(),
            port: self.port,
            username: self.user.clone(),
            password: self.password.clone(),
            databases: vec![self.database.clone()],
            tables: TableSelection {
                include: vec![format!(r"dbo\.{table_name}")],
                exclude: Vec::new(),
            },
            connect_timeout: Duration::from_secs(15),
            encrypt: true,
            trust_server_certificate: true,
            poll_interval: Duration::from_millis(250),
            streaming_fetch_size: 128,
            snapshot_isolation_mode: "repeatable_read".into(),
            heartbeat_interval: Duration::ZERO,
            heartbeat_action_query: None,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            signal_data_collection: None,
            signal_enabled_channels: vec!["source".into()],
            signal_file: "file-signals.txt".into(),
            signal_poll_interval: Duration::from_secs(5),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external SQL Server CDC and a Kafka-compatible broker"]
async fn recovers_completed_snapshot_before_kafka_signal_offset_commit() -> TestResult {
    let settings = TestSettings::from_env()?;
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_kafka_{}", &suffix[..12]);
    let signal_table = format!("rustium_kafka_sig_{}", &suffix[..12]);
    let capture_instance = format!("rustium_kafka_cap_{}", &suffix[..12]);
    let signal_capture = format!("rustium_kafka_sig_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-kafka-{}", &suffix[..12]);
    let signal_id = format!("sqlserver-kafka-signal-{}", &suffix[..12]);
    let topic = format!("rustium-sqlserver-signal-{}", &suffix[..12]);
    let group_id = format!("rustium-sqlserver-signal-group-{}", &suffix[..12]);
    let kafka_admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let topic_spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let created = kafka_admin
        .create_topics(
            [&topic_spec],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?;
    require(
        matches!(created.as_slice(), [Ok(created)] if created == &topic),
        "SQL Server Kafka signal topic was not created",
    )?;
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY, value nvarchar(50) NOT NULL); \
                 CREATE TABLE dbo.{signal_table} (id nvarchar(200) NOT NULL PRIMARY KEY, [type] nvarchar(64) NOT NULL, data nvarchar(max) NOT NULL); \
                 INSERT INTO dbo.{table_name} VALUES (1, N'one'), (2, N'two'); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', @role_name=NULL, @supports_net_changes=0; \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', @source_name=N'{signal_table}', \
                    @capture_instance=N'{signal_capture}', @role_name=NULL, @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;
        wait_for_capture_instance(&mut client, &signal_capture).await?;

        let mut base_config = settings.source_config(&table_name);
        base_config.signal_data_collection =
            Some(format!("{}.dbo.{}", settings.database, signal_table));
        base_config.incremental_snapshot_chunk_size = 1;
        let mut snapshot_source = SqlServerSource::new(
            &connector_name,
            base_config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        snapshot_source.validate().await?;
        let (mut snapshot_output, cancellation, source_task) =
            start_source(snapshot_source, None);
        let snapshot_capture: TestResult<SourcePosition> = async {
            let mut snapshot_rows = 0;
            loop {
                let record = receive(&mut snapshot_output).await?;
                if record.boundary == RecordBoundary::SnapshotComplete {
                    require(
                        snapshot_rows == 2,
                        "SQL Server Kafka recovery fixture snapshot did not emit two rows",
                    )?;
                    break Ok(record.position);
                }
                require(
                    record.event.as_ref().is_some_and(|event| {
                        event.operation == Operation::Read && event.source.snapshot
                    }),
                    "SQL Server Kafka recovery fixture emitted a non-snapshot record",
                )?;
                snapshot_rows += 1;
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let snapshot_position = combine_capture_and_stop(snapshot_capture, stop_result)?;
        let initial_checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: snapshot_position,
            snapshot_completed: true,
            config_fingerprint: "sqlserver-kafka-recovery-test".into(),
            updated_at: SystemTime::now(),
            connector_state: None,
        };

        let mut config = base_config;
        config.signal_enabled_channels = vec!["kafka".into()];
        config.signal_kafka_topic = Some(topic.clone());
        config.signal_kafka_bootstrap_servers = vec![bootstrap_servers.clone()];
        config.signal_kafka_group_id = group_id.clone();
        config.signal_kafka_poll_timeout = Duration::from_millis(50);
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()?;
        let offset_observer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .create()?;
        let payload = serde_json::to_string(&SignalRecord::new(
            &signal_id,
            "execute-snapshot",
            serde_json::json!({
                "type": "incremental",
                "data-collections": [format!("dbo.{}", table_name)],
            }),
        ))?;

        let mut source = SqlServerSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, signal_sender, source_cancellation, source_task) =
            start_source_with_signals(source, Some(initial_checkpoint));
        let kafka_cancellation = CancellationToken::new();
        let channel = KafkaSignalChannel::new(
            std::slice::from_ref(&bootstrap_servers),
            &connector_name,
            &topic,
            &group_id,
            Duration::from_millis(50),
            &BTreeMap::new(),
        )?;
        let kafka_task = tokio::spawn(channel.run(signal_sender, kafka_cancellation.clone()));
        for key in ["other-connector", connector_name.as_str()] {
            producer
                .send(
                    FutureRecord::to(&topic).key(key).payload(&payload),
                    Timeout::After(Duration::from_secs(10)),
                )
                .await
                .map_err(|(error, _)| error)?;
        }
        wait_for_kafka_offset(&offset_observer, &topic, Offset::Offset(1)).await?;

        let mut withheld_acknowledgement = None;
        let mut incremental_rows = 0;
        let completed_checkpoint = loop {
            let mut record = receive(&mut output).await?;
            if withheld_acknowledgement.is_none() && !record.signal_acknowledgements.is_empty() {
                withheld_acknowledgement = record.signal_acknowledgements.pop();
            }
            if record.event.as_ref().is_some_and(|event| {
                event
                    .source
                    .attributes
                    .get("rustium.snapshot.kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("incremental")
            }) {
                incremental_rows += 1;
            }
            let Some(state) = record.connector_state.clone() else {
                continue;
            };
            let completed = state
                .payload
                .get("completed_signal_ids")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&signal_id)));
            if completed && record.boundary == RecordBoundary::TransactionCommit {
                break Checkpoint {
                    schema_version: CHECKPOINT_SCHEMA_VERSION,
                    connector_name: connector_name.clone(),
                    generation: uuid::Uuid::new_v4(),
                    source_position: record.position,
                    snapshot_completed: true,
                    config_fingerprint: "sqlserver-kafka-recovery-test".into(),
                    updated_at: SystemTime::now(),
                    connector_state: Some(state),
                };
            }
        };
        require(
            incremental_rows == 2,
            "Kafka signal did not snapshot both SQL Server rows",
        )?;
        require(
            withheld_acknowledgement.is_some(),
            "SQL Server Kafka signal checkpoint did not carry an acknowledgement",
        )?;
        require(
            committed_kafka_offset(&offset_observer, &topic)? == Offset::Offset(1),
            "SQL Server Kafka signal offset advanced before checkpoint acknowledgement",
        )?;

        stop_source(source_cancellation, source_task).await?;
        kafka_cancellation.cancel();
        kafka_task.await??;
        drop(withheld_acknowledgement);
        tokio::time::sleep(Duration::from_secs(12)).await;

        let (probe_sender, mut probe_receiver) = rustium_core::signal_channel(1);
        let probe_cancellation = CancellationToken::new();
        let probe_channel = KafkaSignalChannel::new(
            std::slice::from_ref(&bootstrap_servers),
            &connector_name,
            &topic,
            &group_id,
            Duration::from_millis(50),
            &BTreeMap::new(),
        )?;
        let probe_task =
            tokio::spawn(probe_channel.run(probe_sender, probe_cancellation.clone()));
        let probe_delivery = tokio::time::timeout(Duration::from_secs(10), probe_receiver.recv())
            .await?
            .ok_or_else(|| test_error("SQL Server Kafka replay probe channel closed"))?;
        require(
            probe_delivery.record().id == signal_id,
            "Kafka did not replay the completed SQL Server signal",
        )?;

        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, signal_sender, source_cancellation, source_task) =
            start_source_with_signals(source, Some(completed_checkpoint));
        let replayed_signal = probe_delivery.record().clone();
        let replay_sender = signal_sender.clone();
        let source_acknowledgement =
            tokio::spawn(async move { replay_sender.send_and_wait(replayed_signal).await });
        loop {
            let mut record = receive(&mut output).await?;
            require(
                !record.event.as_ref().is_some_and(|event| {
                    event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                }),
                "replayed completed Kafka signal emitted duplicate SQL Server incremental rows",
            )?;
            let completed = record
                .connector_state
                .as_ref()
                .and_then(|state| state.payload.get("completed_signal_ids"))
                .and_then(serde_json::Value::as_array)
                .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&signal_id)));
            if completed && record.boundary == RecordBoundary::Heartbeat {
                require(
                    record.signal_acknowledgements.len() == 1,
                    "replayed SQL Server signal checkpoint has no acknowledgement",
                )?;
                record
                    .signal_acknowledgements
                    .pop()
                    .expect("acknowledgement length was checked")
                    .acknowledge();
                break;
            }
        }
        source_acknowledgement.await??;
        probe_delivery.acknowledge();
        wait_for_kafka_offset(&offset_observer, &topic, Offset::Offset(2)).await?;
        stop_source(source_cancellation, source_task).await?;
        probe_cancellation.cancel();
        probe_task.await??;
        Ok(())
    }
    .await;

    let sqlserver_cleanup = async {
        cleanup(&mut client, &signal_table, &signal_capture).await?;
        cleanup(&mut client, &table_name, &capture_instance).await
    }
    .await;
    let close_result = client.close().await.map_err(boxed_error);
    let kafka_cleanup = kafka_admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .map(|_| ())
        .map_err(|error| -> Box<dyn StdError + Send + Sync> { Box::new(error) });
    match (outcome, sqlserver_cleanup, close_result, kafka_cleanup) {
        (Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), _, _, _) => Err(error),
        (Ok(()), Err(error), _, _)
        | (Ok(()), Ok(()), Err(error), _)
        | (Ok(()), Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn snapshots_streams_and_resumes_from_checkpoint() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_sql_{}", &suffix[..12]);
    let capture_instance = format!("rustium_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-external-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (\
                    id bigint NOT NULL PRIMARY KEY, \
                    customer nvarchar(100) NOT NULL, \
                    amount decimal(10,2) NOT NULL\
                 ); \
                 INSERT INTO dbo.{table_name} VALUES \
                    (1, N'Alice', 12.30), \
                    (2, N'Bob', 45.60); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;

        let mut config = settings.source_config(&table_name);
        config.streaming_fetch_size = 1;
        let checkpoint_position =
            run_initial_capture(&mut client, &connector_name, &table_name, config.clone()).await?;

        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: checkpoint_position,
            snapshot_completed: true,
            config_fingerprint: "sqlserver-external-test".into(),
            updated_at: SystemTime::now(),
            connector_state: None,
        };
        run_resumed_capture(
            &mut client,
            &connector_name,
            &table_name,
            config,
            checkpoint,
        )
        .await
    }
    .await;

    let cleanup_result = cleanup(&mut client, &table_name, &capture_instance).await;
    let close_result = client.close().await.map_err(boxed_error);

    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server test cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server test connection close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn reconnects_after_polling_session_termination() -> TestResult {
    let settings = TestSettings::from_env()?;
    let soak_cycles = reconnect_soak_cycles()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_reconnect_{}", &suffix[..12]);
    let capture_instance = format!("rustium_reconnect_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-reconnect-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY, value nvarchar(50) NOT NULL); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;
        run_forced_polling_recovery(
            &mut client,
            &settings,
            &connector_name,
            &table_name,
            soak_cycles,
        )
        .await
    }
    .await;

    let cleanup_result = cleanup(&mut client, &table_name, &capture_instance).await;
    let close_result = client.close().await.map_err(boxed_error);

    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server reconnect test cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server reconnect test connection close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn emits_heartbeat_and_executes_action_query() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_heartbeat_{}", &suffix[..12]);
    let action_table = format!("rustium_hb_action_{}", &suffix[..12]);
    let capture_instance = format!("rustium_hb_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-heartbeat-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY); \
                 CREATE TABLE dbo.{action_table} (id int NOT NULL PRIMARY KEY, executions int NOT NULL); \
                 INSERT INTO dbo.{action_table} VALUES (1, 0); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;

        let mut config = settings.source_config(&table_name);
        config.heartbeat_interval = Duration::from_millis(100);
        config.heartbeat_action_query = Some(format!(
            "UPDATE dbo.{action_table} SET executions = executions + 1 WHERE id = 1"
        ));
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
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
                    "SQL Server heartbeat has the wrong boundary",
                )?;
                require(
                    event.source.table.is_none() && event.source.schema.is_none(),
                    "SQL Server heartbeat was exposed as a table event",
                )?;
                require(
                    event.source.attributes.get("rustium.heartbeat") == Some(&true.into()),
                    "SQL Server heartbeat marker is missing",
                )?;
                require(
                    matches!(
                        event.after.as_ref().and_then(|row| row.get("ts_ms")),
                        Some(DataValue::Int64(_))
                    ),
                    "SQL Server heartbeat timestamp is missing",
                )?;
                require(
                    matches!(
                        record.position,
                        SourcePosition::SqlServer(ref position) if !position.snapshot
                    ),
                    "SQL Server heartbeat does not carry a streaming CDC position",
                )?;
                break Ok(());
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)?;

        let row = client
            .simple_query(format!(
                "SELECT executions FROM dbo.{action_table} WHERE id = 1"
            ))
            .await?
            .into_row()
            .await?
            .ok_or_else(|| test_error("SQL Server heartbeat action returned no row"))?;
        require(
            row.get::<i32, _>("executions").unwrap_or_default() > 0,
            "SQL Server heartbeat.action.query did not update the action table",
        )
    }
    .await;

    let cleanup_result = async {
        cleanup(&mut client, &table_name, &capture_instance).await?;
        execute_batch(
            &mut client,
            &format!(
                "IF OBJECT_ID(N'dbo.{action_table}', N'U') IS NOT NULL \
                    DROP TABLE dbo.{action_table}"
            ),
        )
        .await
    }
    .await;
    let close_result = client.close().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server heartbeat cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server heartbeat close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn rejects_checkpoint_older_than_cdc_retention() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_retention_{}", &suffix[..12]);
    let capture_instance = format!("rustium_ret_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-retention-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY); \
                 INSERT INTO dbo.{table_name} VALUES (1); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;
        let min_lsn = client
            .query(
                "SELECT sys.fn_cdc_get_min_lsn(@P1) AS min_lsn",
                &[&capture_instance],
            )
            .await?
            .into_row()
            .await?
            .and_then(|row| row.get::<&[u8], _>("min_lsn").map(<[u8]>::to_vec))
            .ok_or_else(|| test_error("SQL Server retention test has no minimum LSN"))?;
        require(
            min_lsn.iter().any(|byte| *byte != 0),
            "SQL Server retention test minimum LSN is not initialized",
        )?;

        let config = settings.source_config(&table_name);
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: SourcePosition::SqlServer(SqlServerPosition {
                database: settings.database.clone(),
                commit_lsn: "0x00000000000000000001".into(),
                change_lsn: "0xFFFFFFFFFFFFFFFFFFFF".into(),
                event_serial: 5,
                snapshot: false,
            }),
            snapshot_completed: true,
            config_fingerprint: "sqlserver-retention-test".into(),
            updated_at: SystemTime::now(),
            connector_state: None,
        };
        let (output_tx, _output_rx) = mpsc::channel(1);
        let (_ack_tx, ack_rx) = watch::channel(None);
        let error = source
            .run(SourceContext {
                output: output_tx,
                acknowledged: ack_rx,
                initial_checkpoint: Some(checkpoint),
                signals: rustium_core::signal_channel(1).1,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("SQL Server accepted a checkpoint older than CDC retention");
        require(
            matches!(error, rustium_core::Error::State(ref message) if message.contains("no longer available")),
            "SQL Server retention failure did not return a state error",
        )
    }
    .await;

    let cleanup_result = cleanup(&mut client, &table_name, &capture_instance).await;
    let close_result = client.close().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server retention cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server retention close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn keeps_snapshot_and_cdc_type_conversion_identical() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_types_{}", &suffix[..12]);
    let capture_instance = format!("rustium_types_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-types-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (\
                    id bigint NOT NULL PRIMARY KEY, \
                    flag bit NOT NULL, \
                    tiny_value tinyint NOT NULL, \
                    small_value smallint NOT NULL, \
                    int_value int NOT NULL, \
                    bigint_value bigint NOT NULL, \
                    decimal_value decimal(20,4) NOT NULL, \
                    money_value money NOT NULL, \
                    real_value real NOT NULL, \
                    float_value float NOT NULL, \
                    guid_value uniqueidentifier NOT NULL, \
                    binary_value varbinary(4) NOT NULL, \
                    date_value date NOT NULL, \
                    time_value time(4) NOT NULL, \
                    datetime2_value datetime2(4) NOT NULL, \
                    offset_value datetimeoffset(4) NOT NULL, \
                    text_value nvarchar(100) NULL, \
                    xml_value xml NOT NULL, \
                    hierarchy_value hierarchyid NOT NULL, \
                    geometry_value geometry NOT NULL, \
                    geography_value geography NOT NULL\
                 ); \
                 INSERT INTO dbo.{table_name} VALUES (\
                    1, 1, 255, -1234, 123456, 9876543210, 12345.6789, 42.5000, \
                    1.25, 9.5, '550e8400-e29b-41d4-a716-446655440000', 0x00FF10AA, \
                    '2026-07-16', '09:30:45.1234', '2026-07-16T09:30:45.1234', \
                    '2026-07-16T09:30:45.1234+08:00', N'Rustium type matrix', \
                    CONVERT(xml, N'<root><value>Rustium</value></root>'), \
                    hierarchyid::Parse('/1/2/'), \
                    geometry::STGeomFromText('POINT (1 2)', 4326), \
                    geography::STGeomFromText('POINT (1 2)', 4326)\
                 ); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;

        let config = settings.source_config(&table_name);
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
        let capture_result: TestResult = async {
            let snapshot_row = loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event
                    && event.operation == Operation::Read
                {
                    break event.after.ok_or_else(|| {
                        test_error("SQL Server type snapshot row has no after image")
                    })?;
                }
            };
            loop {
                if receive(&mut output).await?.boundary == RecordBoundary::SnapshotComplete {
                    break;
                }
            }
            execute_batch(
                &mut client,
                &format!(
                    "INSERT INTO dbo.{table_name} VALUES (\
                        2, 1, 255, -1234, 123456, 9876543210, 12345.6789, 42.5000, \
                        1.25, 9.5, '550e8400-e29b-41d4-a716-446655440000', 0x00FF10AA, \
                        '2026-07-16', '09:30:45.1234', '2026-07-16T09:30:45.1234', \
                        '2026-07-16T09:30:45.1234+08:00', N'Rustium type matrix', \
                        CONVERT(xml, N'<root><value>Rustium</value></root>'), \
                        hierarchyid::Parse('/1/2/'), \
                        geometry::STGeomFromText('POINT (1 2)', 4326), \
                        geography::STGeomFromText('POINT (1 2)', 4326)\
                     )"
                ),
            )
            .await?;
            let cdc_row = loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                if event.operation == Operation::Create {
                    break event.after.ok_or_else(|| {
                        test_error("SQL Server type CDC row has no after image")
                    })?;
                }
            };
            let mut snapshot_values = snapshot_row;
            let mut cdc_values = cdc_row;
            snapshot_values.shift_remove("id");
            cdc_values.shift_remove("id");
            require(
                snapshot_values == cdc_values,
                &format!(
                    "SQL Server snapshot/CDC type conversion differs: snapshot={snapshot_values:?}, cdc={cdc_values:?}"
                ),
            )?;
            require(
                matches!(snapshot_values.get("geometry_value"), Some(DataValue::Bytes(value)) if !value.is_empty()),
                "SQL Server geometry did not retain native serialization bytes",
            )?;
            require(
                matches!(snapshot_values.get("geography_value"), Some(DataValue::Bytes(value)) if !value.is_empty()),
                "SQL Server geography did not retain native serialization bytes",
            )?;
            require(
                snapshot_values.get("hierarchy_value")
                    == Some(&DataValue::String("/1/2/".into())),
                "SQL Server hierarchyid did not retain its canonical path",
            )?;
            require(
                snapshot_values.get("xml_value")
                    == Some(&DataValue::String(
                        "<root><value>Rustium</value></root>".into(),
                    )),
                "SQL Server XML did not retain its canonical text",
            )
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(&mut client, &table_name, &capture_instance).await;
    let close_result = client.close().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server type cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server type close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn orders_concurrent_transactions_by_commit_lsn() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_concurrent_{}", &suffix[..12]);
    let capture_instance = format!("rustium_con_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-concurrent-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;

        let config = settings.source_config(&table_name);
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);

        let mut slow_client = connect(&settings).await?;
        let slow_table = table_name.clone();
        let slow_task = tokio::spawn(async move {
            let result = execute_batch(
                &mut slow_client,
                &format!(
                    "BEGIN TRANSACTION; \
                     INSERT INTO dbo.{slow_table} VALUES (1); \
                     WAITFOR DELAY '00:00:01'; \
                     COMMIT TRANSACTION;"
                ),
            )
            .await;
            let close = slow_client.close().await.map_err(boxed_error);
            match (result, close) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(error), _) | (Ok(()), Err(error)) => Err(error),
            }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut fast_client = connect(&settings).await?;
        execute_batch(
            &mut fast_client,
            &format!("INSERT INTO dbo.{table_name} VALUES (2)"),
        )
        .await?;
        fast_client.close().await.map_err(boxed_error)?;
        slow_task.await??;

        let capture_result: TestResult = async {
            let mut ids = Vec::new();
            while ids.len() < 2 {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                if event.operation != Operation::Create {
                    continue;
                }
                let id = event
                    .after
                    .as_ref()
                    .and_then(|row| row.get("id"))
                    .and_then(sqlserver_integer)
                    .ok_or_else(|| test_error("SQL Server concurrent event has no integer id"))?;
                ids.push(id);
            }
            require(
                ids == [2, 1],
                &format!(
                    "SQL Server concurrent transactions were not ordered by commit LSN: {ids:?}"
                ),
            )
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(&mut client, &table_name, &capture_instance).await;
    let close_result = client.close().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server concurrency cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server concurrency close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn resumes_incremental_snapshot_with_persisted_keyset() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_incremental_{}", &suffix[..12]);
    let signal_table = format!("rustium_inc_signal_{}", &suffix[..12]);
    let capture_instance = format!("rustium_inc_cap_{}", &suffix[..12]);
    let signal_capture = format!("rustium_inc_sig_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-incremental-{}", &suffix[..12]);
    let signal_id = format!("sqlserver-signal-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY, value nvarchar(50) NOT NULL); \
                 CREATE TABLE dbo.{signal_table} (id nvarchar(200) NOT NULL PRIMARY KEY, [type] nvarchar(64) NOT NULL, data nvarchar(max) NOT NULL); \
                 INSERT INTO dbo.{table_name} VALUES (1, N'one'), (2, N'two'), (3, N'three'); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', \
                    @role_name=NULL, \
                    @supports_net_changes=0; \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{signal_table}', \
                    @capture_instance=N'{signal_capture}', \
                    @role_name=NULL, \
                    @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;
        wait_for_capture_instance(&mut client, &signal_capture).await?;

        let mut config = settings.source_config(&table_name);
        config.signal_data_collection = Some(format!(
            "{}.dbo.{}",
            settings.database, signal_table
        ));
        config.signal_enabled_channels = vec!["in-process".into()];
        config.incremental_snapshot_chunk_size = 1;
        let mut source = SqlServerSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, signal_sender, cancellation, source_task) =
            start_source_with_signals(source, None);
        signal_sender
            .send(SignalRecord::new(
                &signal_id,
                "execute-snapshot",
                serde_json::json!({
                    "type": "incremental",
                    "data-collections": [format!("dbo\\.{}", table_name)]
                }),
            ))
            .await?;
        let first_checkpoint: TestResult<(SourcePosition, ConnectorStateEnvelope)> = async {
            let mut saw_first = false;
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
                        .and_then(sqlserver_integer);
                    require(
                        id == Some(1),
                        &format!("first SQL Server keyset chunk has id {id:?}, expected 1"),
                    )?;
                    saw_first = true;
                }
                if saw_first && record.boundary == RecordBoundary::TransactionCommit {
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("SQL Server keyset chunk checkpoint has no connector state")
                    })?;
                    break Ok((record.position, state));
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let first_checkpoint = combine_capture_and_stop(first_checkpoint, stop_result)?;

        execute_batch(
            &mut client,
            &format!(
                "DELETE FROM dbo.{table_name} WHERE id = 2; \
                 INSERT INTO dbo.{table_name} VALUES (0, N'zero')"
            ),
        )
        .await?;
        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: first_checkpoint.0.clone(),
            snapshot_completed: true,
            config_fingerprint: "sqlserver-keyset-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(first_checkpoint.1),
        };
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, Some(checkpoint));
        let resumed: TestResult = async {
            let mut saw_last = false;
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
                        .and_then(sqlserver_integer);
                    require(
                        id == Some(3),
                        &format!(
                            "resumed SQL Server keyset chunk has id {id:?}, expected only 3"
                        ),
                    )?;
                    saw_last = true;
                }
                if saw_last && record.boundary == RecordBoundary::TransactionCommit {
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("completed SQL Server keyset checkpoint has no state")
                    })?;
                    let completed = state
                        .payload
                        .get("completed_signal_ids")
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(|| {
                            test_error("completed SQL Server keyset state has no signal IDs")
                        })?;
                    require(
                        completed.iter().any(|id| id.as_str() == Some(&signal_id)),
                        "SQL Server keyset state did not persist the completed signal ID",
                    )?;
                    break Ok(());
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(resumed, stop_result)
    }
    .await;

    let cleanup_result = async {
        cleanup(&mut client, &signal_table, &signal_capture).await?;
        cleanup(&mut client, &table_name, &capture_instance).await
    }
    .await;
    let close_result = client.close().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server incremental cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server incremental close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn deduplicates_incremental_rows_changed_inside_the_cdc_window() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_window_{}", &suffix[..12]);
    let signal_table = format!("rustium_window_sig_{}", &suffix[..12]);
    let capture_instance = format!("rustium_win_cap_{}", &suffix[..12]);
    let signal_capture = format!("rustium_win_sig_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-window-{}", &suffix[..12]);
    let signal_id = format!("sqlserver-window-signal-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;
    let mut observer = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY, value nvarchar(50) NOT NULL); \
                 CREATE TABLE dbo.{signal_table} (id nvarchar(200) NOT NULL PRIMARY KEY, [type] nvarchar(64) NOT NULL, data nvarchar(max) NOT NULL); \
                 INSERT INTO dbo.{table_name} VALUES (1, N'before'), (2, N'stable'); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', @role_name=NULL, @supports_net_changes=0; \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', @source_name=N'{signal_table}', \
                    @capture_instance=N'{signal_capture}', @role_name=NULL, @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;
        wait_for_capture_instance(&mut client, &signal_capture).await?;

        let mut config = settings.source_config(&table_name);
        config.signal_data_collection = Some(format!(
            "{}.dbo.{}",
            settings.database, signal_table
        ));
        config.signal_enabled_channels = vec!["in-process".into()];
        config.incremental_snapshot_chunk_size = 2;
        config.poll_interval = Duration::from_millis(50);
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;

        execute_batch(
            &mut client,
            &format!(
                "BEGIN TRANSACTION; \
                 UPDATE dbo.{table_name} SET value = N'during-window' WHERE id = 1;"
            ),
        )
        .await?;
        let (mut output, signal_sender, cancellation, source_task) =
            start_source_with_signals(source, None);
        signal_sender
            .send(SignalRecord::new(
                &signal_id,
                "execute-snapshot",
                serde_json::json!({
                    "type": "incremental",
                    "data-collections": [format!("dbo\\.{}", table_name)]
                }),
            ))
            .await?;
        wait_for_signal_type(&mut observer, &signal_table, "snapshot-window-open").await?;
        execute_batch(&mut client, "COMMIT TRANSACTION").await?;

        let capture_result: TestResult<(usize, Vec<i128>)> = async {
            let mut update_events = 0;
            let mut snapshot_ids = Vec::new();
            let mut last_position: Option<SourcePosition> = None;
            loop {
                let record = receive(&mut output).await?;
                if let Some(previous) = &last_position {
                    require(
                        previous.is_at_or_before(&record.position),
                        &format!(
                            "SQL Server incremental window moved backwards from {previous:?} to {:?}",
                            record.position
                        ),
                    )?;
                }
                last_position = Some(record.position.clone());
                if let Some(event) = &record.event
                    && event.source.table.as_deref() == Some(table_name.as_str())
                {
                    let id = event
                        .after
                        .as_ref()
                        .or(event.before.as_ref())
                        .and_then(|row| row.get("id"))
                        .and_then(sqlserver_integer);
                    if event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                    {
                        if let Some(id) = id {
                            snapshot_ids.push(id);
                        }
                    } else if event.operation == Operation::Update && id == Some(1) {
                        require(
                            event.after.as_ref().and_then(|row| row.get("value"))
                                == Some(&DataValue::String("during-window".into())),
                            "SQL Server window update has the wrong after image",
                        )?;
                        update_events += 1;
                    }
                }
                let completed = record
                    .connector_state
                    .as_ref()
                    .and_then(|state| state.payload.get("completed_signal_ids"))
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&signal_id)));
                if completed {
                    break Ok((update_events, snapshot_ids));
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let (update_events, snapshot_ids) =
            combine_capture_and_stop(capture_result, stop_result)?;
        require(
            update_events == 1,
            &format!("expected one SQL Server CDC update, got {update_events}"),
        )?;
        require(
            snapshot_ids == [2],
            &format!(
                "SQL Server CDC window emitted changed or missing rows: {snapshot_ids:?}"
            ),
        )
    }
    .await;

    let rollback_result =
        execute_batch(&mut client, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await;
    let cleanup_result = async {
        cleanup(&mut client, &signal_table, &signal_capture).await?;
        cleanup(&mut client, &table_name, &capture_instance).await
    }
    .await;
    let observer_close = observer.close().await.map_err(boxed_error);
    let close_result = client.close().await.map_err(boxed_error);
    match (
        outcome,
        rollback_result,
        cleanup_result,
        observer_close,
        close_result,
    ) {
        (Ok(()), Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), rollback, cleanup, observer_close, close) => {
            if let Err(rollback_error) = rollback {
                eprintln!("SQL Server window rollback also failed: {rollback_error}");
            }
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server window cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = observer_close {
                eprintln!("SQL Server window observer close also failed: {close_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server window close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _, _, _)
        | (Ok(()), Ok(()), Err(error), _, _)
        | (Ok(()), Ok(()), Ok(()), Err(error), _)
        | (Ok(()), Ok(()), Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external SQL Server 2017+ instance with CDC and SQL Server Agent enabled"]
async fn runs_incremental_snapshot_from_source_signal_table() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_source_inc_{}", &suffix[..12]);
    let signal_table = format!("rustium_signal_{}", &suffix[..12]);
    let capture_instance = format!("rustium_src_cap_{}", &suffix[..12]);
    let signal_capture = format!("rustium_sig_cap_{}", &suffix[..12]);
    let connector_name = format!("sqlserver-source-signal-{}", &suffix[..12]);
    let mut client = connect(&settings).await?;

    let outcome: TestResult = async {
        execute_batch(
            &mut client,
            &format!(
                "CREATE TABLE dbo.{table_name} (id bigint NOT NULL PRIMARY KEY, value nvarchar(50) NOT NULL); \
                 CREATE TABLE dbo.{signal_table} (id nvarchar(200) NOT NULL PRIMARY KEY, [type] nvarchar(64) NOT NULL, data nvarchar(max) NOT NULL); \
                 INSERT INTO dbo.{table_name} VALUES (1, N'one'), (2, N'two'); \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}', @role_name=NULL, @supports_net_changes=0; \
                 EXEC sys.sp_cdc_enable_table \
                    @source_schema=N'dbo', @source_name=N'{signal_table}', \
                    @capture_instance=N'{signal_capture}', @role_name=NULL, @supports_net_changes=0;"
            ),
        )
        .await?;
        wait_for_capture_instance(&mut client, &capture_instance).await?;
        wait_for_capture_instance(&mut client, &signal_capture).await?;

        let mut config = settings.source_config(&table_name);
        config.signal_data_collection = Some(format!(
            "{}.dbo.{}",
            settings.database, signal_table
        ));
        config.signal_enabled_channels = vec!["source".into()];
        config.incremental_snapshot_chunk_size = 1;
        let mut source = SqlServerSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
        let payload = serde_json::json!({
            "type": "incremental",
            "data-collections": [format!("dbo\\.{}", table_name)],
            "additional-conditions": [{
                "data-collection": format!("dbo\\.{}", table_name),
                "filter": "id >= 2"
            }]
        })
        .to_string()
        .replace('\'', "''");
        execute_batch(
            &mut client,
            &format!(
                "INSERT INTO dbo.{signal_table} (id, [type], data) VALUES (\
                    N'source-snapshot-1', N'execute-snapshot', \
                    N'{payload}'\
                 )"
            ),
        )
        .await?;

        let capture_result: TestResult = async {
            let mut ids = Vec::new();
            while ids.is_empty() {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                require(
                    event.source.table.as_deref() != Some(signal_table.as_str()),
                    "SQL Server source signal row leaked as a business event",
                )?;
                if event
                    .source
                    .attributes
                    .get("rustium.snapshot.kind")
                    .and_then(serde_json::Value::as_str)
                    != Some("incremental")
                {
                    continue;
                }
                ids.push(
                    event
                        .after
                        .as_ref()
                        .and_then(|row| row.get("id"))
                        .and_then(sqlserver_integer)
                        .ok_or_else(|| {
                            test_error("SQL Server source-signaled snapshot row has no id")
                        })?,
                );
            }
            require(
                ids == [2],
                &format!("SQL Server source-signaled snapshot rows are incorrect: {ids:?}"),
            )
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = async {
        cleanup(&mut client, &signal_table, &signal_capture).await?;
        cleanup(&mut client, &table_name, &capture_instance).await
    }
    .await;
    let close_result = client.close().await.map_err(boxed_error);
    match (outcome, cleanup_result, close_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup, close) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("SQL Server source signal cleanup also failed: {cleanup_error}");
            }
            if let Err(close_error) = close {
                eprintln!("SQL Server source signal close also failed: {close_error}");
            }
            Err(error)
        }
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

async fn run_forced_polling_recovery(
    client: &mut SqlClient,
    settings: &TestSettings,
    connector_name: &str,
    table_name: &str,
    soak_cycles: u32,
) -> TestResult {
    let mut config = settings.source_config(table_name);
    config.poll_interval = Duration::from_millis(100);
    let mut source = SqlServerSource::new(
        connector_name,
        config,
        SnapshotConfig {
            mode: SnapshotMode::Never,
            fetch_size: 1,
            include_collections: Vec::new(),
        },
    )
    .with_retry_policy(RetryPolicy {
        max_retries: 10,
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(250),
    });
    source.validate().await?;
    let (mut output, cancellation, source_task) =
        start_source_with_output_capacity(source, None, 1);

    let capture_result: TestResult = async {
        for cycle in 0..soak_cycles {
            let first_id = 1_i128 + i128::from(cycle) * 3;
            let expected_ids = [first_id, first_id + 1, first_id + 2];
            let (session_id, original_connection_id) =
                wait_for_source_connection(client, &settings.user, None).await?;
            execute_batch(
                client,
                &format!(
                    "INSERT INTO dbo.{table_name} (id, value) VALUES \
                        ({first_id}, N'backpressure-{cycle}-a'), \
                        ({}, N'backpressure-{cycle}-b')",
                    first_id + 1
                ),
            )
            .await?;
            wait_for_output_backpressure(&output).await?;
            execute_batch(client, &format!("KILL {session_id}")).await?;
            execute_batch(
                client,
                &format!(
                    "INSERT INTO dbo.{table_name} (id, value) VALUES \
                        ({}, N'after-reconnect-{cycle}')",
                    first_id + 2
                ),
            )
            .await?;

            let mut first_seen = Vec::new();
            let mut seen = BTreeMap::<i128, usize>::new();
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::Data {
                    let event = record
                        .event
                        .ok_or_else(|| test_error("SQL Server reconnect record has no event"))?;
                    if event.operation == Operation::Create
                        && let Some(id) = event
                            .after
                            .as_ref()
                            .and_then(|row| row.get("id"))
                            .and_then(sqlserver_integer)
                        && expected_ids.contains(&id)
                    {
                        if !seen.contains_key(&id) {
                            first_seen.push(id);
                        }
                        *seen.entry(id).or_default() += 1;
                    }
                }
                if record.boundary == RecordBoundary::TransactionCommit
                    && expected_ids.iter().all(|id| seen.contains_key(id))
                {
                    break;
                }
            }
            require(
                first_seen == expected_ids,
                "SQL Server reconnect soak did not preserve first-seen source order",
            )?;
            let (_, reconnected_connection_id) =
                wait_for_source_connection(client, &settings.user, Some(&original_connection_id))
                    .await?;
            require(
                reconnected_connection_id != original_connection_id,
                "SQL Server polling connection identity did not change after KILL",
            )?;
        }
        Ok(())
    }
    .await;

    let stop_result = stop_source(cancellation, source_task).await;
    combine_capture_and_stop(capture_result, stop_result)
}

async fn run_initial_capture(
    client: &mut SqlClient,
    connector_name: &str,
    table_name: &str,
    config: SqlServerSourceConfig,
) -> TestResult<SourcePosition> {
    let mut source = SqlServerSource::new(
        connector_name,
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
            include_collections: Vec::new(),
        },
    );
    source.validate().await?;

    let (mut output, cancellation, source_task) = start_source(source, None);
    let capture_result: TestResult<SourcePosition> = async {
        let mut snapshot_rows = 0;
        loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                break;
            }
            require(
                record.boundary == RecordBoundary::Data,
                "unexpected SQL Server snapshot boundary",
            )?;
            let event = record
                .event
                .ok_or_else(|| test_error("SQL Server snapshot data record has no event"))?;
            require(
                event.operation == Operation::Read,
                "SQL Server snapshot event is not a read",
            )?;
            snapshot_rows += 1;
        }
        require(
            snapshot_rows == 2,
            "SQL Server snapshot did not emit exactly two rows",
        )?;

        execute_batch(
            client,
            &format!(
                "BEGIN TRANSACTION; \
                 INSERT INTO dbo.{table_name} VALUES (3, N'Cara', 10.25); \
                 UPDATE dbo.{table_name} SET amount = 13.30 WHERE id = 1; \
                 DELETE FROM dbo.{table_name} WHERE id = 2; \
                 COMMIT TRANSACTION;"
            ),
        )
        .await?;

        loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::Heartbeat {
                continue;
            }
            require(
                record.boundary == RecordBoundary::Data,
                "SQL Server did not expose the first transaction row before checkpoint",
            )?;
            let event = record
                .event
                .ok_or_else(|| test_error("SQL Server checkpoint row has no event"))?;
            require(
                event.operation == Operation::Create,
                "SQL Server first transaction row is not the create event",
            )?;
            require(
                event
                    .transaction
                    .and_then(|transaction| transaction.total_order)
                    == Some(1),
                "SQL Server first transaction row has the wrong total_order",
            )?;
            break Ok(record.position);
        }
    }
    .await;

    let stop_result = stop_source(cancellation, source_task).await;
    combine_capture_and_stop(capture_result, stop_result)
}

async fn run_resumed_capture(
    client: &mut SqlClient,
    connector_name: &str,
    table_name: &str,
    config: SqlServerSourceConfig,
    checkpoint: Checkpoint,
) -> TestResult {
    let mut source = SqlServerSource::new(
        connector_name,
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
            include_collections: Vec::new(),
        },
    );
    source.validate().await?;

    let (mut output, cancellation, source_task) = start_source(source, Some(checkpoint));
    let capture_result: TestResult = async {
        let (operations, transaction_orders, _) = receive_transaction(&mut output).await?;
        require(
            operations == [Operation::Update, Operation::Delete],
            "SQL Server mid-transaction resume did not emit only the remaining rows",
        )?;
        require(
            transaction_orders == [2, 3],
            "SQL Server mid-transaction resume did not preserve transaction order",
        )?;

        execute_batch(
            client,
            &format!(
                "INSERT INTO dbo.{table_name} (id, customer, amount) \
                 VALUES (4, N'Dora', 67.80)"
            ),
        )
        .await?;

        let (operations, transaction_orders, _) = receive_transaction(&mut output).await?;
        require(
            operations == [Operation::Create],
            "SQL Server resume did not emit only the new create event",
        )?;
        require(
            transaction_orders == [1],
            "SQL Server resumed transaction order is incorrect",
        )?;
        Ok(())
    }
    .await;

    let stop_result = stop_source(cancellation, source_task).await;
    combine_capture_and_stop(capture_result, stop_result)
}

async fn receive_transaction(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult<(Vec<Operation>, Vec<u64>, SourcePosition)> {
    let mut operations = Vec::new();
    let mut transaction_orders = Vec::new();
    loop {
        let record = receive(output).await?;
        require(
            record.boundary != RecordBoundary::SnapshotComplete,
            "SQL Server snapshot repeated after streaming started",
        )?;
        if record.boundary == RecordBoundary::Heartbeat {
            continue;
        }
        if record.boundary == RecordBoundary::TransactionCommit {
            return Ok((operations, transaction_orders, record.position));
        }
        require(
            record.boundary == RecordBoundary::Data,
            "unexpected SQL Server streaming boundary",
        )?;
        let event = record
            .event
            .ok_or_else(|| test_error("SQL Server streaming data record has no event"))?;
        require(
            event.operation != Operation::Read,
            "SQL Server snapshot row repeated during streaming",
        )?;
        operations.push(event.operation);
        transaction_orders.push(
            event
                .transaction
                .ok_or_else(|| test_error("SQL Server event has no transaction metadata"))?
                .total_order
                .ok_or_else(|| test_error("SQL Server event has no transaction order"))?,
        );
    }
}

fn start_source(
    source: SqlServerSource,
    initial_checkpoint: Option<Checkpoint>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    start_source_with_output_capacity(source, initial_checkpoint, 64)
}

fn start_source_with_output_capacity(
    mut source: SqlServerSource,
    initial_checkpoint: Option<Checkpoint>,
    output_capacity: usize,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    let (output_tx, output_rx) = mpsc::channel(output_capacity);
    let (ack_tx, ack_rx) = watch::channel(None);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        let _ack_tx = ack_tx;
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
    mut source: SqlServerSource,
    initial_checkpoint: Option<Checkpoint>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    SignalSender,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    let (output_tx, output_rx) = mpsc::channel(64);
    let (signal_sender, signals) = rustium_core::signal_channel(16);
    let (ack_tx, ack_rx) = watch::channel(None);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        let _ack_tx = ack_tx;
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

async fn stop_source(
    cancellation: CancellationToken,
    mut source_task: JoinHandle<rustium_core::Result<()>>,
) -> TestResult {
    cancellation.cancel();
    let result = match tokio::time::timeout(Duration::from_secs(15), &mut source_task).await {
        Ok(result) => result?,
        Err(_) => {
            source_task.abort();
            let _ = source_task.await;
            return Err(test_error(
                "SQL Server source did not stop after cancellation",
            ));
        }
    };
    result?;
    Ok(())
}

async fn receive(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult<SourceRecord> {
    let record = tokio::time::timeout(RECEIVE_TIMEOUT, output.recv())
        .await
        .map_err(|_| test_error("timed out waiting for a SQL Server source record"))?
        .ok_or_else(|| test_error("SQL Server source output closed unexpectedly"))??;
    Ok(record)
}

fn reconnect_soak_cycles() -> TestResult<u32> {
    let cycles = std::env::var("RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES")
        .unwrap_or_else(|_| "3".into())
        .parse::<u32>()?;
    require(
        (1..=1_000).contains(&cycles),
        "RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES must be between 1 and 1000",
    )?;
    Ok(cycles)
}

async fn wait_for_output_backpressure(
    output: &mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult {
    for _ in 0..100 {
        if output.capacity() == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(
        "SQL Server source output did not reach bounded backpressure",
    ))
}

async fn connect(settings: &TestSettings) -> TestResult<SqlClient> {
    let mut config = Config::new();
    config.host(&settings.host);
    config.port(settings.port);
    config.database(&settings.database);
    config.application_name("rustium-external-test");
    config.authentication(AuthMethod::sql_server(&settings.user, &settings.password));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let address = config.get_addr();
    let tcp = tokio::time::timeout(Duration::from_secs(15), TcpStream::connect(&address)).await??;
    tcp.set_nodelay(true)?;
    let client = tokio::time::timeout(
        Duration::from_secs(15),
        Client::connect(config, tcp.compat_write()),
    )
    .await??;
    Ok(client)
}

async fn execute_batch(client: &mut SqlClient, statement: &str) -> TestResult {
    client.simple_query(statement).await?.into_results().await?;
    Ok(())
}

async fn wait_for_capture_instance(client: &mut SqlClient, capture_instance: &str) -> TestResult {
    for _ in 0..300 {
        let row = client
            .query(
                "SELECT CAST(COUNT(*) AS int) AS capture_count, \
                        sys.fn_cdc_get_min_lsn(@P1) AS min_lsn \
                 FROM cdc.change_tables WHERE capture_instance = @P1",
                &[&capture_instance],
            )
            .await?
            .into_row()
            .await?;
        let count = row
            .as_ref()
            .and_then(|row| row.get::<i32, _>("capture_count"))
            .unwrap_or_default();
        let min_lsn_ready = row
            .as_ref()
            .and_then(|row| row.get::<&[u8], _>("min_lsn"))
            .is_some_and(|lsn| lsn.len() == 10);
        if count == 1 && min_lsn_ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(
        "SQL Server CDC capture instance did not finish initialization",
    ))
}

async fn wait_for_source_connection(
    client: &mut SqlClient,
    login_name: &str,
    excluded_connection_id: Option<&str>,
) -> TestResult<(i32, String)> {
    for _ in 0..300 {
        let row = client
            .query(
                "SELECT TOP (1) CAST(s.session_id AS int) AS session_id, \
                        CONVERT(nvarchar(36), c.connection_id) AS connection_id \
                 FROM sys.dm_exec_sessions s \
                 JOIN sys.dm_exec_connections c ON c.session_id = s.session_id \
                 WHERE s.program_name = N'rustium' \
                   AND s.login_name = @P1 \
                   AND s.database_id = DB_ID() \
                 ORDER BY s.login_time DESC",
                &[&login_name],
            )
            .await?
            .into_row()
            .await?;
        if let Some(row) = row {
            let session_id = row
                .get::<i32, _>("session_id")
                .ok_or_else(|| test_error("SQL Server source session has no session ID"))?;
            let connection_id = row
                .get::<&str, _>("connection_id")
                .ok_or_else(|| test_error("SQL Server source session has no connection ID"))?
                .to_owned();
            if excluded_connection_id.is_none_or(|excluded| excluded != connection_id) {
                return Ok((session_id, connection_id));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(
        "SQL Server source polling connection did not become available",
    ))
}

async fn wait_for_signal_type(
    client: &mut SqlClient,
    signal_table: &str,
    signal_type: &str,
) -> TestResult {
    for _ in 0..300 {
        let row = client
            .query(
                &format!(
                    "SELECT CAST(COUNT(*) AS int) AS signal_count \
                     FROM dbo.{signal_table} WHERE [type] = @P1"
                ),
                &[&signal_type],
            )
            .await?
            .into_row()
            .await?;
        if row
            .as_ref()
            .and_then(|row| row.get::<i32, _>("signal_count"))
            .unwrap_or_default()
            > 0
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(&format!(
        "SQL Server signal table did not receive {signal_type}"
    )))
}

async fn cleanup(client: &mut SqlClient, table_name: &str, capture_instance: &str) -> TestResult {
    execute_batch(
        client,
        &format!(
            "IF EXISTS (\
                SELECT 1 FROM cdc.change_tables \
                WHERE capture_instance = N'{capture_instance}'\
             ) \
             BEGIN \
                EXEC sys.sp_cdc_disable_table \
                    @source_schema=N'dbo', \
                    @source_name=N'{table_name}', \
                    @capture_instance=N'{capture_instance}'; \
             END; \
             IF OBJECT_ID(N'dbo.{table_name}', N'U') IS NOT NULL \
                DROP TABLE dbo.{table_name};"
        ),
    )
    .await?;

    let row = client
        .query(
            &format!(
                "SELECT CAST((SELECT COUNT(*) FROM cdc.change_tables \
                    WHERE capture_instance = @P1) AS int) AS capture_count, \
                    CAST(CASE WHEN OBJECT_ID(N'dbo.{table_name}', N'U') IS NULL \
                        THEN 0 ELSE 1 END AS int) AS table_count"
            ),
            &[&capture_instance],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| test_error("SQL Server returned no cleanup verification row"))?;
    require(
        row.get::<i32, _>("capture_count").unwrap_or_default() == 0
            && row.get::<i32, _>("table_count").unwrap_or_default() == 0,
        "SQL Server external test resources were not fully removed",
    )
}

async fn wait_for_kafka_offset(
    consumer: &StreamConsumer,
    topic: &str,
    expected: Offset,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if committed_kafka_offset(consumer, topic)? == expected {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

fn committed_kafka_offset(consumer: &StreamConsumer, topic: &str) -> TestResult<Offset> {
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    Ok(consumer
        .committed_offsets(partitions, Timeout::After(Duration::from_secs(2)))?
        .find_partition(topic, 0)
        .ok_or_else(|| test_error("Kafka committed offset has no SQL Server signal partition"))?
        .offset())
}

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| test_error(&format!("required environment variable {name} is not set")))
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(test_error(message))
    }
}

fn sqlserver_integer(value: &DataValue) -> Option<i128> {
    match value {
        DataValue::Int32(value) => Some(i128::from(*value)),
        DataValue::Int64(value) => Some(i128::from(*value)),
        DataValue::UInt64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn combine_capture_and_stop<T>(capture: TestResult<T>, stop: TestResult) -> TestResult<T> {
    match (capture, stop) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(capture_error), Ok(())) => Err(capture_error),
        (Ok(_), Err(stop_error)) => Err(stop_error),
        (Err(capture_error), Err(stop_error)) => Err(test_error(&format!(
            "{capture_error}; SQL Server source task failed: {stop_error}"
        ))),
    }
}

fn boxed_error(error: tiberius::error::Error) -> Box<dyn StdError + Send + Sync> {
    error.into()
}

fn test_error(message: &str) -> Box<dyn StdError + Send + Sync> {
    io::Error::other(message).into()
}
