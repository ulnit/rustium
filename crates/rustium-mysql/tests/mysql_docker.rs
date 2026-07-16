use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime},
};

use mysql_async::{Conn, OptsBuilder, prelude::Queryable};
use rustium_config::{MySqlSourceConfig, SnapshotConfig, SnapshotMode, TableSelection};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, DataValue, Operation, RecordBoundary, RetryPolicy,
    SourceConnector, SourceContext, SourcePosition,
};
use rustium_mysql::MySqlSource;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

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

fn mapped_port(container: &str) -> u16 {
    let output = Command::new("docker")
        .args(["port", container, "3306/tcp"])
        .output()
        .expect("inspect MySQL Docker port");
    assert!(
        output.status.success(),
        "docker port failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("MySQL Docker port is UTF-8")
        .trim()
        .rsplit(':')
        .next()
        .expect("MySQL Docker port mapping has a port")
        .parse()
        .expect("MySQL Docker port is numeric")
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

async fn different_replication_connection_id(root: &mut Conn, previous: u64) -> u64 {
    for _ in 0..300 {
        let connection_id = root
            .query_first::<u64, _>(
                "SELECT ID FROM information_schema.PROCESSLIST \
                 WHERE USER = 'rustium' AND COMMAND LIKE 'Binlog Dump%'",
            )
            .await
            .unwrap();
        if let Some(connection_id) = connection_id
            && connection_id != previous
        {
            return connection_id;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("MySQL replication connection ID did not change after recovery");
}

fn reconnect_soak_cycles() -> u32 {
    let cycles = std::env::var("RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES")
        .unwrap_or_else(|_| "3".into())
        .parse::<u32>()
        .expect("RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES is numeric");
    assert!(
        (1..=1_000).contains(&cycles),
        "RUSTIUM_MYSQL_RECONNECT_SOAK_CYCLES must be between 1 and 1000"
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
    panic!("MySQL source output did not reach bounded backpressure");
}

fn integer_value(value: Option<&DataValue>) -> Option<i64> {
    match value {
        Some(DataValue::Int32(value)) => Some(i64::from(*value)),
        Some(DataValue::Int64(value)) => Some(*value),
        Some(DataValue::UInt64(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the mysql:8.4 image"]
async fn snapshots_streams_reconnects_and_preserves_transaction_order() {
    let soak_cycles = reconnect_soak_cycles();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("rustium-mysql-{suffix}");
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &name,
            "-p",
            "127.0.0.1::3306",
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
    let port = mapped_port(&_container.0);

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
        "CREATE TABLE inventory.audit (id BIGINT PRIMARY KEY, note VARCHAR(100) NOT NULL)",
        "INSERT INTO inventory.audit VALUES (100, 'excluded from initial snapshot')",
        "CREATE TABLE inventory.rustium_signal (id VARCHAR(255) PRIMARY KEY, type VARCHAR(64) NOT NULL, data TEXT NOT NULL)",
        "INSERT INTO inventory.rustium_signal VALUES ('seed', 'execute-snapshot', '{\"type\":\"incremental\",\"data-collections\":[\"inventory\\\\.orders\"]}')",
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
            include: vec![r"inventory\.(orders|audit)".into()],
            exclude: Vec::new(),
        },
        ssl_mode: "disabled".into(),
        connection_time_zone: "UTC".into(),
        ssl_ca: None,
        ssl_cert: None,
        ssl_key: None,
        ssl_keystore: None,
        ssl_keystore_password: None,
        ssl_truststore: None,
        ssl_truststore_password: None,
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
        signal_data_collection: Some("inventory.rustium_signal".into()),
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
    };
    let mut source = MySqlSource::new(
        "inventory-mysql",
        config.clone(),
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
            include_collections: vec![r"inventory\.orders".into()],
        },
    )
    .with_retry_policy(RetryPolicy {
        max_retries: 20,
        initial_delay: Duration::from_millis(25),
        max_delay: Duration::from_millis(250),
    });
    source.validate().await.unwrap();

    let (output_tx, mut output_rx) = mpsc::channel(1);
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
    let (snapshot_position, snapshot_schema_history) = loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::SnapshotComplete {
            break (
                record.position,
                record
                    .connector_state
                    .expect("snapshot completion should carry MySQL schema history"),
            );
        }
        let event = record.event.unwrap();
        assert_eq!(event.operation, Operation::Read);
        snapshot_rows += 1;
    };
    assert_eq!(snapshot_rows, 2);

    cancellation.cancel();
    source_task.await.unwrap().unwrap();

    let SourcePosition::MySql(mut recovery_position) = snapshot_position.clone() else {
        panic!("MySQL snapshot position has the wrong source type");
    };
    recovery_position.binlog_filename = "rustium-missing-binlog.999999".into();
    let recovery_checkpoint = Checkpoint {
        schema_version: CHECKPOINT_SCHEMA_VERSION,
        connector_name: "inventory-mysql".into(),
        generation: uuid::Uuid::new_v4(),
        source_position: SourcePosition::MySql(recovery_position),
        snapshot_completed: true,
        config_fingerprint: "mysql-when-needed-recovery-test".into(),
        updated_at: SystemTime::now(),
        connector_state: Some(snapshot_schema_history.clone()),
    };
    let mut recovery_source = MySqlSource::new(
        "inventory-mysql",
        config.clone(),
        SnapshotConfig {
            mode: SnapshotMode::WhenNeeded,
            fetch_size: 1,
            include_collections: vec![r"inventory\.orders".into()],
        },
    )
    .with_retry_policy(RetryPolicy {
        max_retries: 20,
        initial_delay: Duration::from_millis(25),
        max_delay: Duration::from_millis(250),
    });
    recovery_source.validate().await.unwrap();
    let (recovery_output_tx, mut output_rx) = mpsc::channel(1);
    let (_recovery_ack_tx, recovery_ack_rx) = watch::channel(None);
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        recovery_source
            .run(SourceContext {
                output: recovery_output_tx,
                acknowledged: recovery_ack_rx,
                initial_checkpoint: Some(recovery_checkpoint),
                signals: rustium_core::signal_channel(1).1,
                cancellation: source_cancel,
            })
            .await
    });
    let mut recovery_snapshot_rows = 0;
    loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if record.boundary == RecordBoundary::SnapshotComplete {
            break;
        }
        assert_eq!(record.event.as_ref().unwrap().operation, Operation::Read);
        recovery_snapshot_rows += 1;
    }
    assert_eq!(recovery_snapshot_rows, 2);

    root.query_drop("INSERT INTO inventory.audit VALUES (101, 'streamed after snapshot')")
        .await
        .unwrap();
    let mut streamed_audit = false;
    loop {
        let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(event) = record.event {
            if event.source.table.as_deref() == Some("audit") {
                assert_eq!(event.operation, Operation::Create);
                assert!(!event.source.snapshot);
                assert_eq!(
                    integer_value(event.after.as_ref().and_then(|row| row.get("id"))),
                    Some(101)
                );
                streamed_audit = true;
            }
        }
        if record.boundary == RecordBoundary::TransactionCommit {
            break;
        }
    }
    assert!(
        streamed_audit,
        "snapshot.include.collection.list narrowed MySQL streaming"
    );

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

    let mut checkpoint_position = None;
    for cycle in 0..soak_cycles {
        let first_id = 5_i64 + i64::from(cycle) * 3;
        let expected_ids = [first_id, first_id + 1, first_id + 2];
        let replication_connection = replication_connection_id(&mut root).await;
        root.query_drop("START TRANSACTION").await.unwrap();
        root.query_drop(format!(
            "INSERT INTO inventory.orders (id, customer, amount) VALUES \
                ({first_id}, 'Backpressure {cycle} A', 10.00), \
                ({}, 'Backpressure {cycle} B', 20.00)",
            first_id + 1
        ))
        .await
        .unwrap();
        root.query_drop("COMMIT").await.unwrap();
        wait_for_output_backpressure(&output_rx).await;
        root.query_drop(format!("KILL CONNECTION {replication_connection}"))
            .await
            .unwrap();
        root.query_drop(format!(
            "INSERT INTO inventory.orders (id, customer, amount) VALUES \
                ({}, 'After reconnect {cycle}', 30.00)",
            first_id + 2
        ))
        .await
        .unwrap();

        let mut first_seen = Vec::new();
        let mut seen = BTreeMap::<i64, usize>::new();
        loop {
            let record = tokio::time::timeout(Duration::from_secs(20), output_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if record.boundary == RecordBoundary::Data {
                let event = record.event.unwrap();
                if event.operation == Operation::Create
                    && let Some(id) =
                        integer_value(event.after.as_ref().and_then(|row| row.get("id")))
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
                checkpoint_position = Some(record.position);
                break;
            }
        }
        assert_eq!(first_seen, expected_ids);
        assert_ne!(
            different_replication_connection_id(&mut root, replication_connection).await,
            replication_connection
        );
    }
    let checkpoint_position = checkpoint_position.expect("MySQL reconnect checkpoint position");

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

    let old_schema_id = 5_i64 + i64::from(soak_cycles) * 3;
    let new_schema_id = old_schema_id + 1;
    root.query_drop(format!(
        "INSERT INTO inventory.orders (id, customer, amount) VALUES \
            ({old_schema_id}, 'Finn', 32.10)"
    ))
    .await
    .unwrap();
    root.query_drop(
        "ALTER TABLE inventory.orders DROP COLUMN customer, ADD COLUMN status VARCHAR(20) NOT NULL AFTER amount",
    )
    .await
    .unwrap();
    root.query_drop(format!(
        "INSERT INTO inventory.orders (id, amount, status) VALUES \
            ({new_schema_id}, 64.20, 'ready')"
    ))
    .await
    .unwrap();

    let mut resumed_source = MySqlSource::new(
        "inventory-mysql",
        config,
        SnapshotConfig {
            mode: SnapshotMode::Initial,
            fetch_size: 1,
            include_collections: Vec::new(),
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
    assert_eq!(ddl_state.version, 3);

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
