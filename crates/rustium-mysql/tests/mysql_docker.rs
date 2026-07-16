use std::{
    process::Command,
    time::{Duration, SystemTime},
};

use mysql_async::{Conn, OptsBuilder, prelude::Queryable};
use rustium_config::{MySqlSourceConfig, SnapshotConfig, SnapshotMode, TableSelection};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, DataValue, Operation, RecordBoundary, SourceConnector,
    SourceContext,
};
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
        ssl_ca: None,
        ssl_cert: None,
        ssl_key: None,
        connect_timeout: Duration::from_secs(10),
        connect_keep_alive: true,
        connect_keep_alive_interval: Duration::from_millis(100),
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
    };
    let mut source = MySqlSource::new(
        "inventory-mysql",
        config.clone(),
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
                signals: rustium_core::signal_channel(1).1,
                cancellation: source_cancel,
            })
            .await
    });

    let mut snapshot_rows = 0;
    let snapshot_schema_history = loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::SnapshotComplete {
            break record
                .connector_state
                .expect("snapshot completion should carry MySQL schema history");
        }
        let event = record.event.unwrap();
        assert_eq!(event.operation, Operation::Read);
        snapshot_rows += 1;
    };
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
    let checkpoint_position = loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::TransactionCommit {
            break record.position;
        }
        let event = record.event.unwrap();
        operations.push(event.operation);
        orders.push(event.transaction.unwrap().total_order.unwrap());
    };
    assert_eq!(operations, [Operation::Create]);
    assert_eq!(orders, [1]);

    cancellation.cancel();
    source_task.await.unwrap().unwrap();

    let checkpoint = Checkpoint {
        schema_version: CHECKPOINT_SCHEMA_VERSION,
        connector_name: "inventory-mysql".into(),
        generation: uuid::Uuid::new_v4(),
        source_position: checkpoint_position.clone(),
        snapshot_completed: true,
        config_fingerprint: "mysql-docker-test".into(),
        updated_at: SystemTime::now(),
        connector_state: Some(snapshot_schema_history),
    };

    root.query_drop(
        "INSERT INTO inventory.orders (id, customer, amount) VALUES (6, 'Finn', 32.10)",
    )
    .await
    .unwrap();
    root.query_drop(
        "ALTER TABLE inventory.orders DROP COLUMN customer, ADD COLUMN status VARCHAR(20) NOT NULL AFTER amount",
    )
    .await
    .unwrap();
    root.query_drop("INSERT INTO inventory.orders (id, amount, status) VALUES (7, 64.20, 'ready')")
        .await
        .unwrap();

    let mut resumed_source = MySqlSource::new(
        "inventory-mysql",
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
        },
    );
    resumed_source.validate().await.unwrap();
    let (resumed_tx, mut resumed_rx) = mpsc::channel(32);
    let (_resumed_ack_tx, resumed_ack_rx) = watch::channel(Some(checkpoint_position));
    let resumed_cancellation = CancellationToken::new();
    let resumed_cancel = resumed_cancellation.clone();
    let resumed_task = tokio::spawn(async move {
        resumed_source
            .run(SourceContext {
                output: resumed_tx,
                acknowledged: resumed_ack_rx,
                initial_checkpoint: Some(checkpoint),
                signals: rustium_core::signal_channel(1).1,
                cancellation: resumed_cancel,
            })
            .await
    });

    let old_record = tokio::time::timeout(Duration::from_secs(20), resumed_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let old_event = old_record.event.expect("old-schema row event");
    assert_eq!(old_event.operation, Operation::Create);
    assert_eq!(old_event.schema.version, 1);
    let old_after = old_event.after.unwrap();
    assert_eq!(
        old_after.get("customer"),
        Some(&DataValue::String("Finn".into()))
    );
    assert!(!old_after.contains_key("status"));

    let old_commit = tokio::time::timeout(Duration::from_secs(20), resumed_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(old_commit.boundary, RecordBoundary::TransactionCommit);

    let ddl_commit = tokio::time::timeout(Duration::from_secs(20), resumed_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ddl_commit.boundary, RecordBoundary::TransactionCommit);
    let ddl_state = ddl_commit
        .connector_state
        .expect("DDL commit should carry updated MySQL schema history");
    assert_eq!(ddl_state.format, "rustium.mysql.schema-history");
    assert_eq!(ddl_state.version, 1);

    let new_record = tokio::time::timeout(Duration::from_secs(20), resumed_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let new_event = new_record.event.expect("new-schema row event");
    assert_eq!(new_event.operation, Operation::Create);
    assert_eq!(new_event.schema.version, 2);
    let new_after = new_event.after.unwrap();
    assert_eq!(
        new_after.get("status"),
        Some(&DataValue::String("ready".into()))
    );
    assert!(!new_after.contains_key("customer"));

    let new_commit = tokio::time::timeout(Duration::from_secs(20), resumed_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(new_commit.boundary, RecordBoundary::TransactionCommit);

    resumed_cancellation.cancel();
    resumed_task.await.unwrap().unwrap();
    root.disconnect().await.unwrap();
}
