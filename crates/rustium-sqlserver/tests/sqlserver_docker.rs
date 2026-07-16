use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use futures::FutureExt;
use rustium_config::{SnapshotConfig, SnapshotMode, SqlServerSourceConfig, TableSelection};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, DataValue, Operation, RecordBoundary, RetryPolicy, Row,
    SourceConnector, SourceContext, SourcePosition,
};
use rustium_sqlserver::SqlServerSource;
use tiberius::{AuthMethod, Client, Config};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
};
use tokio_util::{compat::TokioAsyncWriteCompatExt, sync::CancellationToken};

const SA_PASSWORD: &str = "Rustium_Strong_2026!";
const CONNECTOR_PASSWORD: &str = "Cdc_Connector#2026!";
type SqlClient = Client<tokio_util::compat::Compat<TcpStream>>;

struct DockerContainer(String);

impl Drop for DockerContainer {
    fn drop(&mut self) {
        let Ok(mut child) = Command::new("docker")
            .args(["rm", "-f", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(100));
                }
                Ok(None) | Err(_) => {
                    let _ = child.kill();
                    break;
                }
            }
        }
    }
}

async fn connect(
    port: u16,
    database: &str,
    user: &str,
    password: &str,
) -> tiberius::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server(user, password));
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

fn reconnect_soak_cycles() -> u32 {
    let cycles = std::env::var("RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES")
        .unwrap_or_else(|_| "3".into())
        .parse::<u32>()
        .expect("RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES is numeric");
    assert!(
        (1..=1_000).contains(&cycles),
        "RUSTIUM_SQLSERVER_RECONNECT_SOAK_CYCLES must be between 1 and 1000"
    );
    cycles
}

async fn wait_for_output_backpressure(
    output: &mpsc::Receiver<rustium_core::Result<rustium_core::SourceRecord>>,
) {
    for _ in 0..100 {
        if output.capacity() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("SQL Server source output did not reach bounded backpressure");
}

fn sqlserver_integer(value: Option<&DataValue>) -> Option<i64> {
    match value {
        Some(DataValue::Int32(value)) => Some(i64::from(*value)),
        Some(DataValue::Int64(value)) => Some(*value),
        Some(DataValue::UInt64(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

async fn source_connection(
    admin: &mut SqlClient,
    excluded_connection_id: Option<&str>,
) -> (i32, String) {
    for _ in 0..300 {
        let row = admin
            .simple_query(
                "SELECT TOP (1) CAST(s.session_id AS int) AS session_id, \
                        CONVERT(nvarchar(36), c.connection_id) AS connection_id \
                 FROM sys.dm_exec_sessions s \
                 JOIN sys.dm_exec_connections c ON c.session_id = s.session_id \
                 WHERE s.program_name = N'rustium' AND s.login_name = N'rustium' \
                 ORDER BY s.login_time DESC",
            )
            .await
            .unwrap()
            .into_row()
            .await
            .unwrap();
        if let Some(row) = row {
            let session_id = row
                .get::<i32, _>("session_id")
                .expect("active Rustium SQL Server polling session ID");
            let connection_id = row
                .get::<&str, _>("connection_id")
                .expect("active Rustium SQL Server polling connection ID")
                .to_owned();
            if excluded_connection_id.is_none_or(|excluded| excluded != connection_id) {
                return (session_id, connection_id);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("SQL Server polling connection did not become available");
}

fn mapped_port(container: &str) -> u16 {
    let output = Command::new("docker")
        .args(["port", container, "1433/tcp"])
        .output()
        .expect("inspect SQL Server Docker port");
    assert!(
        output.status.success(),
        "docker port failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("SQL Server Docker port is UTF-8")
        .trim()
        .rsplit(':')
        .next()
        .expect("SQL Server Docker port mapping has a port")
        .parse()
        .expect("SQL Server Docker port is numeric")
}

fn require_extended_values(row: &Row) {
    assert!(
        matches!(row.get("geometry_value"), Some(DataValue::Bytes(value)) if !value.is_empty())
    );
    assert!(
        matches!(row.get("geography_value"), Some(DataValue::Bytes(value)) if !value.is_empty())
    );
    assert_eq!(
        row.get("hierarchy_value"),
        Some(&DataValue::String("/1/2/".into()))
    );
    assert_eq!(
        row.get("xml_value"),
        Some(&DataValue::String(
            "<root><value>Rustium</value></root>".into()
        ))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the SQL Server 2022 image"]
async fn snapshots_and_streams_cdc_changes() {
    let soak_cycles = reconnect_soak_cycles();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("rustium-sqlserver-{suffix}");
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--platform",
            "linux/amd64",
            "--name",
            &name,
            "-p",
            "127.0.0.1::1433",
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            &format!("MSSQL_SA_PASSWORD={SA_PASSWORD}"),
            "-e",
            "MSSQL_AGENT_ENABLED=true",
            "mcr.microsoft.com/mssql/server:2022-latest",
        ])
        .output()
        .expect("start SQL Server Docker container");
    assert!(
        output.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _container = DockerContainer(name);
    let port = mapped_port(&_container.0);

    let mut admin = None;
    for _ in 0..180 {
        match connect(port, "master", "sa", SA_PASSWORD).await {
            Ok(client) => {
                admin = Some(client);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    let mut admin = admin.expect("SQL Server container did not become ready");
    admin
        .simple_query(&format!(
            "CREATE DATABASE inventory; \
                 ALTER DATABASE inventory SET ALLOW_SNAPSHOT_ISOLATION ON; \
                 CREATE LOGIN rustium WITH PASSWORD = '{CONNECTOR_PASSWORD}';"
        ))
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    admin.close().await.unwrap();

    let mut admin = connect(port, "inventory", "sa", SA_PASSWORD).await.unwrap();
    admin
        .simple_query(
            "EXEC sys.sp_cdc_enable_db; \
             CREATE TABLE dbo.orders (\
                id bigint NOT NULL PRIMARY KEY, \
                customer nvarchar(100) NOT NULL, \
                amount decimal(10,2) NOT NULL, \
                xml_value xml NOT NULL, \
                hierarchy_value hierarchyid NOT NULL, \
                geometry_value geometry NOT NULL, \
                geography_value geography NOT NULL\
             ); \
             INSERT INTO dbo.orders VALUES \
                (1, N'Alice', 12.30, CONVERT(xml, N'<root><value>Rustium</value></root>'), \
                 hierarchyid::Parse('/1/2/'), geometry::STGeomFromText('POINT (1 2)', 4326), \
                 geography::STGeomFromText('POINT (1 2)', 4326)), \
                (2, N'Bob', 45.60, CONVERT(xml, N'<root><value>Rustium</value></root>'), \
                 hierarchyid::Parse('/1/2/'), geometry::STGeomFromText('POINT (1 2)', 4326), \
                 geography::STGeomFromText('POINT (1 2)', 4326)); \
             EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'orders', @role_name=NULL, @supports_net_changes=0; \
             CREATE TABLE dbo.cdc_probe (id int NOT NULL PRIMARY KEY); \
             EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'cdc_probe', @role_name=NULL, @supports_net_changes=0; \
             INSERT INTO dbo.cdc_probe VALUES (1); \
             CREATE USER rustium FOR LOGIN rustium; \
             GRANT SELECT TO rustium; \
             GRANT VIEW DATABASE STATE TO rustium;",
        )
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();

    let mut capture_ready = false;
    for _ in 0..180 {
        let ready = match admin
            .simple_query(
                "SELECT sys.fn_cdc_get_min_lsn(N'dbo_orders') AS min_lsn, \
                        sys.fn_cdc_get_max_lsn() AS max_lsn",
            )
            .await
        {
            Ok(stream) => stream
                .into_row()
                .await
                .ok()
                .flatten()
                .and_then(|row| {
                    let min_lsn = row.get::<&[u8], _>("min_lsn")?;
                    let max_lsn = row.get::<&[u8], _>("max_lsn")?;
                    Some(
                        [min_lsn, max_lsn]
                            .into_iter()
                            .all(|lsn| lsn.len() == 10 && lsn.iter().any(|byte| *byte != 0)),
                    )
                })
                .unwrap_or(false),
            Err(_) => false,
        };
        if ready {
            capture_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(capture_ready, "SQL Server CDC capture did not become ready");

    let config = SqlServerSourceConfig {
        hostname: "127.0.0.1".into(),
        port,
        username: "rustium".into(),
        password: CONNECTOR_PASSWORD.into(),
        databases: vec!["inventory".into()],
        tables: TableSelection {
            include: vec![r"dbo\.orders".into()],
            exclude: Vec::new(),
        },
        connect_timeout: Duration::from_secs(15),
        encrypt: true,
        trust_server_certificate: true,
        poll_interval: Duration::from_millis(250),
        streaming_fetch_size: 128,
        snapshot_isolation_mode: "snapshot".into(),
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
    };
    let mut source = SqlServerSource::new(
        "inventory-sqlserver",
        config.clone(),
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 128,
        },
    )
    .with_retry_policy(RetryPolicy {
        max_retries: 20,
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(250),
    });
    source.validate().await.unwrap();

    let (output_tx, mut output_rx) = mpsc::channel(1);
    let (_ack_tx, ack_rx) = watch::channel(None);
    let mut cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let mut source_task = tokio::spawn(async move {
        source
            .run(SourceContext {
                output: output_tx,
                acknowledged: ack_rx,
                initial_checkpoint: None,
                signals: rustium_core::signal_channel(1).1,
                cancellation: source_cancel,
            })
            .await
    });

    let capture_result = std::panic::AssertUnwindSafe(async {
        let mut snapshot_rows = 0;
        let mut snapshot_extended = None;
        let snapshot_position = loop {
            let record = tokio::time::timeout(Duration::from_secs(30), output_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if record.boundary == RecordBoundary::SnapshotComplete {
                break record.position;
            }
            let event = record.event.unwrap();
            assert_eq!(event.operation, Operation::Read);
            if snapshot_extended.is_none() {
                snapshot_extended = event.after;
            }
            snapshot_rows += 1;
        };
        assert_eq!(snapshot_rows, 2);
        let snapshot_extended = snapshot_extended.expect("SQL Server snapshot row exists");
        require_extended_values(&snapshot_extended);

        cancellation.cancel();
        (&mut source_task).await.unwrap().unwrap();

        let SourcePosition::SqlServer(mut recovery_position) = snapshot_position else {
            panic!("SQL Server snapshot position has the wrong source type");
        };
        recovery_position.commit_lsn = "0x00000000000000000001".into();
        recovery_position.change_lsn = "0x00000000000000000000".into();
        recovery_position.event_serial = 0;
        recovery_position.snapshot = false;
        let recovery_checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: "inventory-sqlserver".into(),
            generation: uuid::Uuid::new_v4(),
            source_position: SourcePosition::SqlServer(recovery_position),
            snapshot_completed: true,
            config_fingerprint: "sqlserver-when-needed-recovery-test".into(),
            updated_at: std::time::SystemTime::now(),
            connector_state: None,
        };
        let mut recovery_source = SqlServerSource::new(
            "inventory-sqlserver",
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::WhenNeeded,
                fetch_size: 128,
            },
        )
        .with_retry_policy(RetryPolicy {
            max_retries: 20,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(250),
        });
        recovery_source.validate().await.unwrap();
        let (recovery_output_tx, recovery_output_rx) = mpsc::channel(1);
        output_rx = recovery_output_rx;
        let (_recovery_ack_tx, recovery_ack_rx) = watch::channel(None);
        cancellation = CancellationToken::new();
        let recovery_cancel = cancellation.clone();
        source_task = tokio::spawn(async move {
            recovery_source
                .run(SourceContext {
                    output: recovery_output_tx,
                    acknowledged: recovery_ack_rx,
                    initial_checkpoint: Some(recovery_checkpoint),
                    signals: rustium_core::signal_channel(1).1,
                    cancellation: recovery_cancel,
                })
                .await
        });
        let mut recovery_snapshot_rows = 0;
        loop {
            let record = tokio::time::timeout(Duration::from_secs(30), output_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if record.boundary == RecordBoundary::SnapshotComplete {
                break;
            }
            if let Some(event) = record.event {
                assert_eq!(event.operation, Operation::Read);
                recovery_snapshot_rows += 1;
            }
        }
        assert_eq!(recovery_snapshot_rows, 2);

        admin
            .simple_query(
                "BEGIN TRANSACTION; \
             INSERT INTO dbo.orders VALUES (\
                3, N'Cara', 10.25, CONVERT(xml, N'<root><value>Rustium</value></root>'), \
                hierarchyid::Parse('/1/2/'), geometry::STGeomFromText('POINT (1 2)', 4326), \
                geography::STGeomFromText('POINT (1 2)', 4326)\
             ); \
             UPDATE dbo.orders SET amount = 13.30 WHERE id = 1; \
             DELETE FROM dbo.orders WHERE id = 2; \
             COMMIT TRANSACTION;",
            )
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();

        let mut operations = Vec::new();
        let mut create_extended = None;
        loop {
            let record = tokio::time::timeout(Duration::from_secs(60), output_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if record.boundary == RecordBoundary::TransactionCommit {
                break;
            }
            if let Some(event) = record.event {
                if event.operation == Operation::Create {
                    create_extended = event.after.clone();
                }
                operations.push(event.operation);
            }
        }
        assert_eq!(
            operations,
            [Operation::Create, Operation::Update, Operation::Delete]
        );
        let create_extended = create_extended.expect("SQL Server CDC create row exists");
        require_extended_values(&create_extended);
        for field in [
            "xml_value",
            "hierarchy_value",
            "geometry_value",
            "geography_value",
        ] {
            assert_eq!(snapshot_extended.get(field), create_extended.get(field));
        }

        for cycle in 0..soak_cycles {
            let first_id = 4_i64 + i64::from(cycle) * 3;
            let expected_ids = [first_id, first_id + 1, first_id + 2];
            let (original_session_id, original_connection_id) =
                source_connection(&mut admin, None).await;
            admin
                .simple_query(format!(
                    "INSERT INTO dbo.orders VALUES \
                    ({first_id}, N'Backpressure {cycle} A', 99.75, CONVERT(xml, N'<root><value>Reconnect</value></root>'), \
                     hierarchyid::Parse('/2/'), geometry::STGeomFromText('POINT (3 4)', 4326), \
                     geography::STGeomFromText('POINT (3 4)', 4326)), \
                    ({}, N'Backpressure {cycle} B', 100.75, CONVERT(xml, N'<root><value>Reconnect</value></root>'), \
                     hierarchyid::Parse('/3/'), geometry::STGeomFromText('POINT (4 5)', 4326), \
                     geography::STGeomFromText('POINT (4 5)', 4326));",
                    first_id + 1
                ))
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();
            wait_for_output_backpressure(&output_rx).await;
            admin
                .simple_query(format!("KILL {original_session_id}"))
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();
            admin
                .simple_query(format!(
                    "INSERT INTO dbo.orders VALUES (\
                    {}, N'After reconnect {cycle}', 101.75, CONVERT(xml, N'<root><value>Reconnect</value></root>'), \
                    hierarchyid::Parse('/4/'), geometry::STGeomFromText('POINT (5 6)', 4326), \
                    geography::STGeomFromText('POINT (5 6)', 4326)\
                 );",
                    first_id + 2
                ))
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();

            let mut first_seen = Vec::new();
            let mut seen = BTreeMap::<i64, usize>::new();
            loop {
                let record = tokio::time::timeout(Duration::from_secs(60), output_rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if record.boundary == RecordBoundary::Data {
                    let event = record.event.unwrap();
                    if event.operation == Operation::Create
                        && let Some(id) =
                            sqlserver_integer(event.after.as_ref().and_then(|row| row.get("id")))
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
            assert_eq!(first_seen, expected_ids);
            let (_, reconnected_connection_id) =
                source_connection(&mut admin, Some(&original_connection_id)).await;
            assert_ne!(reconnected_connection_id, original_connection_id);
        }
    })
    .catch_unwind()
    .await;

    cancellation.cancel();
    let source_result = match tokio::time::timeout(Duration::from_secs(5), &mut source_task).await {
        Ok(result) => Some(result),
        Err(_) => {
            source_task.abort();
            let _ = source_task.await;
            None
        }
    };
    let _ = admin.close().await;

    if let Err(panic) = capture_result {
        std::panic::resume_unwind(panic);
    }
    source_result
        .expect("SQL Server source stopped after cancellation")
        .expect("SQL Server source task joined")
        .expect("SQL Server source completed without error");
}
