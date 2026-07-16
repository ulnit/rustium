use std::{process::Command, time::Duration};

use rustium_config::{SnapshotConfig, SnapshotMode, SqlServerSourceConfig, TableSelection};
use rustium_core::{Operation, RecordBoundary, SourceConnector, SourceContext};
use rustium_sqlserver::SqlServerSource;
use tiberius::{AuthMethod, Client, Config};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
};
use tokio_util::{compat::TokioAsyncWriteCompatExt, sync::CancellationToken};

struct DockerContainer(String);

impl Drop for DockerContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the SQL Server 2022 image"]
async fn snapshots_and_streams_cdc_changes() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("rustium-sqlserver-{suffix}");
    let port = 35_000 + u16::try_from(std::process::id() % 1_000).unwrap();
    let port_mapping = format!("{port}:1433");
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--platform",
            "linux/amd64",
            "--name",
            &name,
            "-p",
            &port_mapping,
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            "MSSQL_SA_PASSWORD=Rustium_Strong_2026!",
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

    let mut admin = None;
    for _ in 0..120 {
        match connect(port, "master", "sa", "Rustium_Strong_2026!").await {
            Ok(client) => {
                admin = Some(client);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    let mut admin = admin.expect("SQL Server container did not become ready");
    admin
        .simple_query(
            "CREATE DATABASE inventory; \
             ALTER DATABASE inventory SET ALLOW_SNAPSHOT_ISOLATION ON; \
             CREATE LOGIN rustium WITH PASSWORD = 'Rustium_Connector_2026!';",
        )
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    admin.close().await.unwrap();

    let mut admin = connect(port, "inventory", "sa", "Rustium_Strong_2026!")
        .await
        .unwrap();
    admin
        .simple_query(
            "EXEC sys.sp_cdc_enable_db; \
             CREATE TABLE dbo.orders (id bigint NOT NULL PRIMARY KEY, customer nvarchar(100) NOT NULL, amount decimal(10,2) NOT NULL); \
             INSERT INTO dbo.orders VALUES (1, N'Alice', 12.30), (2, N'Bob', 45.60); \
             EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'orders', @role_name=NULL, @supports_net_changes=0; \
             CREATE USER rustium FOR LOGIN rustium; \
             GRANT SELECT TO rustium; \
             GRANT VIEW DATABASE STATE TO rustium;",
        )
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();

    let config = SqlServerSourceConfig {
        hostname: "127.0.0.1".into(),
        port,
        username: "rustium".into(),
        password: "Rustium_Connector_2026!".into(),
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
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 128,
        },
    );
    source.validate().await.unwrap();

    let (output_tx, mut output_rx) = mpsc::channel(64);
    let (_ack_tx, ack_rx) = watch::channel(None);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
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

    let mut snapshot_rows = 0;
    loop {
        let record = tokio::time::timeout(Duration::from_secs(30), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::SnapshotComplete {
            break;
        }
        assert_eq!(record.event.unwrap().operation, Operation::Read);
        snapshot_rows += 1;
    }
    assert_eq!(snapshot_rows, 2);

    admin
        .simple_query(
            "BEGIN TRANSACTION; \
             INSERT INTO dbo.orders VALUES (3, N'Cara', 10.25); \
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
            operations.push(event.operation);
        }
    }
    assert_eq!(
        operations,
        [Operation::Create, Operation::Update, Operation::Delete]
    );

    cancellation.cancel();
    source_task.await.unwrap().unwrap();
    admin.close().await.unwrap();
}
