use std::{process::Command, time::Duration};

use mysql_async::{Conn, OptsBuilder, prelude::Queryable};
use rustium_config::{MySqlSourceConfig, SnapshotConfig, SnapshotMode, TableSelection};
use rustium_core::{Operation, RecordBoundary, SourceConnector, SourceContext};
use rustium_mysql::MySqlSource;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

struct DockerContainer(String);

impl Drop for DockerContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

async fn replication_connection_id(root: &mut Conn) -> u64 {
    for _ in 0..100 {
        let connection_id = root
            .query_first::<u64, _>(
                "SELECT ID FROM information_schema.PROCESSLIST \
                 WHERE USER = 'rustium' AND COMMAND LIKE 'Binlog Dump%'",
            )
            .await
            .unwrap();
        if let Some(connection_id) = connection_id {
            return connection_id;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("MySQL replication connection did not become visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the mysql:8.4 image"]
async fn snapshots_streams_reconnects_and_preserves_transaction_order() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("rustium-mysql-{suffix}");
    let port = 34_000 + u16::try_from(std::process::id() % 1_000).unwrap();
    let port_mapping = format!("{port}:3306");
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &name,
            "-p",
            &port_mapping,
            "-e",
            "MYSQL_ROOT_PASSWORD=root",
            "-e",
            "MYSQL_ROOT_HOST=%",
            "-e",
            "MYSQL_DATABASE=inventory",
            "mysql:8.4",
            "--server-id=223344",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
        .output()
        .expect("start MySQL Docker container");
    assert!(
        output.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _container = DockerContainer(name);

    let root_opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("root"))
        .prefer_socket(false);
    let mut root = None;
    for _ in 0..90 {
        match Conn::new(root_opts.clone()).await {
            Ok(connection) => {
                root = Some(connection);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    let mut root = root.expect("MySQL container did not become ready");
    for query in [
        "CREATE USER 'rustium'@'%' IDENTIFIED BY 'secret'",
        "GRANT SELECT, RELOAD, FLUSH_TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rustium'@'%'",
        "CREATE TABLE inventory.orders (id BIGINT PRIMARY KEY, customer VARCHAR(100) NOT NULL, amount DECIMAL(10,2) NOT NULL)",
        "INSERT INTO inventory.orders VALUES (1, 'Alice', 12.30), (2, 'Bob', 45.60)",
    ] {
        root.query_drop(query).await.unwrap();
    }

    let config = MySqlSourceConfig {
        hostname: "127.0.0.1".into(),
        port,
        username: "rustium".into(),
        password: "secret".into(),
        databases: vec!["inventory".into()],
        server_id: 5_401,
        tables: TableSelection {
            include: vec![r"inventory\.orders".into()],
            exclude: Vec::new(),
        },
        ssl_mode: "disabled".into(),
        connect_timeout: Duration::from_secs(10),
        connect_keep_alive: true,
        connect_keep_alive_interval: Duration::from_millis(100),
        reconnect_max_attempts: 5,
    };
    let mut source = MySqlSource::new(
        "inventory-mysql",
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
        },
    );
    source.validate().await.unwrap();

    let (output_tx, mut output_rx) = mpsc::channel(32);
    let (_ack_tx, ack_rx) = watch::channel(None);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        source
            .run(SourceContext {
                output: output_tx,
                acknowledged: ack_rx,
                initial_checkpoint: None,
                cancellation: source_cancel,
            })
            .await
    });

    let mut snapshot_rows = 0;
    loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::SnapshotComplete {
            break;
        }
        let event = record.event.unwrap();
        assert_eq!(event.operation, Operation::Read);
        snapshot_rows += 1;
    }
    assert_eq!(snapshot_rows, 2);

    for query in [
        "START TRANSACTION",
        "INSERT INTO inventory.orders VALUES (3, 'Cara', 10.25), (4, 'Dan', 20.50)",
        "UPDATE inventory.orders SET amount = amount + 1.00 WHERE id IN (1, 3)",
        "DELETE FROM inventory.orders WHERE id = 2",
        "COMMIT",
    ] {
        root.query_drop(query).await.unwrap();
    }

    let mut operations = Vec::new();
    let mut orders = Vec::new();
    loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::TransactionCommit {
            break;
        }
        let event = record.event.unwrap();
        operations.push(event.operation);
        orders.push(event.transaction.unwrap().total_order.unwrap());
    }
    assert_eq!(
        operations,
        [
            Operation::Create,
            Operation::Create,
            Operation::Update,
            Operation::Update,
            Operation::Delete,
        ]
    );
    assert_eq!(orders, [1, 2, 3, 4, 5]);

    let replication_connection = replication_connection_id(&mut root).await;
    root.query_drop(format!("KILL CONNECTION {replication_connection}"))
        .await
        .unwrap();
    root.query_drop(
        "INSERT INTO inventory.orders (id, customer, amount) VALUES (5, 'Erin', 89.10)",
    )
    .await
    .unwrap();

    let mut operations = Vec::new();
    let mut orders = Vec::new();
    loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::TransactionCommit {
            break;
        }
        let event = record.event.unwrap();
        operations.push(event.operation);
        orders.push(event.transaction.unwrap().total_order.unwrap());
    }
    assert_eq!(operations, [Operation::Create]);
    assert_eq!(orders, [1]);

    cancellation.cancel();
    source_task.await.unwrap().unwrap();
    root.disconnect().await.unwrap();
}
