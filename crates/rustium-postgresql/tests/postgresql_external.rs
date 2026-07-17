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
use rustium_config::{
    PostgresLsnFlushMode, PostgresOffsetMismatchStrategy, PostgresReplicaIdentity,
    PostgresReplicaIdentityRule, PostgresSourceConfig, PublicationAutoCreateMode, SlotOwnership,
    SnapshotConfig, SnapshotMode, TableSelection,
};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, ConnectorStateEnvelope, DataValue, Operation,
    PostgresPosition, RecordBoundary, RetryPolicy, Row, SignalRecord, SourceConnector,
    SourceContext, SourcePosition, SourceRecord,
};
use rustium_postgresql::PostgresSource;
use rustium_signal_kafka::KafkaSignalChannel;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_postgres::{Client, Config, NoTls};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);

struct AcknowledgingSource {
    output: mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    cancellation: CancellationToken,
    task: JoinHandle<rustium_core::Result<()>>,
    acknowledgement: watch::Sender<Option<SourcePosition>>,
}

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
            host: required_env("RUSTIUM_POSTGRES_TEST_HOST")?,
            port: required_env("RUSTIUM_POSTGRES_TEST_PORT")?.parse()?,
            user: required_env("RUSTIUM_POSTGRES_TEST_USER")?,
            password: required_env("RUSTIUM_POSTGRES_TEST_PASSWORD")?,
            database: required_env("RUSTIUM_POSTGRES_TEST_DATABASE")?,
        })
    }

    fn source_config(
        &self,
        publication: &str,
        slot_name: &str,
        table_name: &str,
    ) -> PostgresSourceConfig {
        PostgresSourceConfig {
            hostname: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            username: self.user.clone(),
            password: self.password.clone(),
            publication: publication.into(),
            publication_autocreate_mode: PublicationAutoCreateMode::Disabled,
            replica_identity_autoset_values: Vec::new(),
            publish_via_partition_root: false,
            slot_name: slot_name.into(),
            slot_failover: false,
            slot_ownership: SlotOwnership::Managed,
            offset_mismatch_strategy: PostgresOffsetMismatchStrategy::NoValidation,
            lsn_flush_mode: PostgresLsnFlushMode::Connector,
            slot_stream_params: BTreeMap::new(),
            database_initial_statements: Vec::new(),
            tables: TableSelection {
                include: vec![format!(r"public\.{table_name}")],
                exclude: Vec::new(),
            },
            ssl_mode: "disable".into(),
            connect_timeout: Duration::from_secs(10),
            status_update_interval: Duration::from_secs(10),
            tcp_keepalive: true,
            heartbeat_interval: Duration::ZERO,
            heartbeat_action_query: None,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
            signal_data_collection: None,
            signal_enabled_channels: vec!["source".into()],
            signal_file: "file-signals.txt".into(),
            signal_poll_interval: Duration::from_secs(5),
            signal_kafka_topic: None,
            signal_kafka_bootstrap_servers: Vec::new(),
            signal_kafka_group_id: "kafka-signal".into(),
            signal_kafka_poll_timeout: Duration::from_millis(100),
            signal_kafka_consumer_properties: BTreeMap::new(),
            incremental_snapshot_chunk_size: 1_024,
            incremental_snapshot_watermarking_strategy: "insert_insert".into(),
            read_only: false,
            hstore_handling_mode: "json".into(),
            interval_handling_mode: "postgres".into(),
            logical_decoding_messages: false,
            message_prefix_include_list: Vec::new(),
            message_prefix_exclude_list: Vec::new(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL logical replication and a Kafka-compatible broker"]
async fn recovers_completed_snapshot_before_kafka_signal_offset_commit() -> TestResult {
    let settings = TestSettings::from_env()?;
    let bootstrap_servers = required_env("RUSTIUM_KAFKA_TEST_BOOTSTRAP_SERVERS")?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_kafka_{}", &suffix[..12]);
    let signal_table = format!("rustium_pg_kafka_signal_{}", &suffix[..12]);
    let publication = format!("rustium_pg_kafka_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_pg_kafka_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-kafka-{}", &suffix[..12]);
    let signal_id = format!("postgresql-kafka-signal-{}", &suffix[..12]);
    let topic = format!("rustium-postgresql-signal-{}", &suffix[..12]);
    let group_id = format!("rustium-postgresql-signal-group-{}", &suffix[..12]);
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
        "PostgreSQL Kafka signal topic was not created",
    )?;
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 INSERT INTO public.{table_name} VALUES (1, 'one'), (2, 'two'); \
                 CREATE TABLE public.{signal_table} (\
                    id VARCHAR(64), type VARCHAR(32), data VARCHAR(2048)\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE \
                    public.{table_name}, public.{signal_table};"
            ))
            .await?;

        let base_config = settings.source_config(&publication, &slot_name, &table_name);
        let mut snapshot_source = PostgresSource::new(
            &connector_name,
            base_config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        snapshot_source.validate().await?;
        let (mut snapshot_output, cancellation, source_task) = start_source(snapshot_source, None);
        let snapshot_capture: TestResult<(SourcePosition, ConnectorStateEnvelope)> = async {
            let mut snapshot_rows = 0;
            loop {
                let record = receive_with_context(
                    &mut snapshot_output,
                    "waiting for the PostgreSQL Kafka fixture initial snapshot",
                )
                .await?;
                if record.boundary == RecordBoundary::SnapshotComplete {
                    require(
                        snapshot_rows == 2,
                        "PostgreSQL Kafka recovery fixture snapshot did not emit two rows",
                    )?;
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("PostgreSQL Kafka snapshot completion has no connector state")
                    })?;
                    break Ok((record.position, state));
                }
                require(
                    record.event.as_ref().is_some_and(|event| {
                        event.operation == Operation::Read && event.source.snapshot
                    }),
                    "PostgreSQL Kafka recovery fixture emitted a non-snapshot record",
                )?;
                snapshot_rows += 1;
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let (snapshot_position, schema_history) =
            combine_capture_and_stop(snapshot_capture, stop_result)?;
        let initial_checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: snapshot_position.clone(),
            snapshot_completed: true,
            config_fingerprint: "postgresql-kafka-recovery-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history),
        };

        let mut config = base_config;
        config.signal_enabled_channels = vec!["kafka".into()];
        config.signal_kafka_topic = Some(topic.clone());
        config.signal_kafka_bootstrap_servers = vec![bootstrap_servers.clone()];
        config.signal_kafka_group_id = group_id.clone();
        config.signal_kafka_poll_timeout = Duration::from_millis(50);
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.incremental_snapshot_chunk_size = 1;

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
                "data-collections": [format!("public.{}", table_name)],
            }),
        ))?;

        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, source_cancellation, source_task, signal_sender) =
            start_source_with_signals(source, Some(initial_checkpoint));
        wait_for_active_slot(&client, &slot_name).await?;
        tokio::time::timeout(
            Duration::from_secs(10),
            signal_sender.send_and_wait(SignalRecord::new(
                format!("postgresql-kafka-ready-{suffix}"),
                "pause-snapshot",
                serde_json::json!({"type": "incremental"}),
            )),
        )
        .await??;
        let kafka_cancellation = CancellationToken::new();
        let channel = KafkaSignalChannel::new(
            std::slice::from_ref(&bootstrap_servers),
            &connector_name,
            &topic,
            &group_id,
            Duration::from_millis(50),
            &BTreeMap::new(),
        )?;
        let (kafka_sender, mut kafka_receiver) = rustium_core::signal_channel(1);
        let kafka_task = tokio::spawn(channel.run(kafka_sender, kafka_cancellation.clone()));
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(&connector_name)
                    .payload(&payload),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(error, _)| error)?;

        let kafka_delivery = tokio::time::timeout(Duration::from_secs(10), kafka_receiver.recv())
            .await?
            .ok_or_else(|| test_error("PostgreSQL Kafka signal channel closed before delivery"))?;
        require(
            kafka_delivery.record().id == signal_id,
            "PostgreSQL Kafka channel delivered the wrong signal",
        )?;
        signal_sender.send(kafka_delivery.record().clone()).await?;

        let mut incremental_rows = 0;
        let completed_checkpoint = loop {
            let record = receive_with_context(
                &mut output,
                "waiting for the PostgreSQL Kafka incremental snapshot checkpoint",
            )
            .await?;
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
                    config_fingerprint: "postgresql-kafka-recovery-test".into(),
                    updated_at: SystemTime::now(),
                    connector_state: Some(state),
                };
            }
        };
        require(
            incremental_rows == 2,
            "Kafka signal did not snapshot both PostgreSQL rows",
        )?;
        require(
            committed_kafka_offset(&offset_observer, &topic)? != Offset::Offset(1),
            "PostgreSQL Kafka signal offset advanced before checkpoint acknowledgement",
        )?;

        stop_source(source_cancellation, source_task).await?;
        kafka_cancellation.cancel();
        kafka_task.await??;
        drop(kafka_delivery);
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
        let probe_task = tokio::spawn(probe_channel.run(probe_sender, probe_cancellation.clone()));
        let probe_delivery = tokio::time::timeout(Duration::from_secs(10), probe_receiver.recv())
            .await?
            .ok_or_else(|| test_error("PostgreSQL Kafka replay probe channel closed"))?;
        require(
            probe_delivery.record().id == signal_id,
            "Kafka did not replay the completed PostgreSQL signal",
        )?;

        let mut source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, source_cancellation, source_task, signal_sender) =
            start_source_with_signals(source, Some(completed_checkpoint));
        signal_sender
            .send_and_wait(probe_delivery.record().clone())
            .await?;
        let duplicate = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let record = receive_with_context(
                    &mut output,
                    "checking for duplicate PostgreSQL Kafka incremental rows",
                )
                .await?;
                if record.event.as_ref().is_some_and(|event| {
                    event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                }) {
                    return TestResult::Ok(true);
                }
            }
        })
        .await;
        require(
            !matches!(duplicate, Ok(Ok(true))),
            "replayed completed Kafka signal emitted duplicate PostgreSQL incremental rows",
        )?;
        probe_delivery.acknowledge();
        wait_for_kafka_offset(&offset_observer, &topic, Offset::Offset(1)).await?;
        stop_source(source_cancellation, source_task).await?;
        probe_cancellation.cancel();
        probe_task.await??;
        Ok(())
    }
    .await;

    let postgres_cleanup = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    connection_task.abort();
    let kafka_cleanup = kafka_admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .map(|_| ())
        .map_err(|error| -> Box<dyn StdError + Send + Sync> { Box::new(error) });
    match (outcome, postgres_cleanup, kafka_cleanup) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), _, _) => Err(error),
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn snapshots_streams_and_resumes_from_checkpoint() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_{}", &suffix[..12]);
    let publication = format!("rustium_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-external-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, \
                    customer TEXT NOT NULL, \
                    amount NUMERIC(10,2) NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES \
                    (1, 'Alice', 12.30), \
                    (2, 'Bob', 45.60); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let config = settings.source_config(&publication, &slot_name, &table_name);
        let (commit_position, schema_history) = run_initial_capture(
            &client,
            &connector_name,
            &table_name,
            &slot_name,
            config.clone(),
        )
        .await?;

        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: commit_position,
            snapshot_completed: true,
            config_fingerprint: "postgresql-external-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history),
        };
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount) \
                     VALUES (4, 'Dora', 67.80)"
                ),
                &[],
            )
            .await?;
        client
            .batch_execute(&format!(
                "ALTER TABLE public.{table_name} \
                 DROP COLUMN customer, \
                 ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
            ))
            .await?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, amount, status) \
                     VALUES (5, 89.10, 'ready')"
                ),
                &[],
            )
            .await?;
        run_resumed_capture(&connector_name, config, checkpoint).await
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn manages_debezium_publication_autocreate_modes() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let first_table = format!("rustium_pg_pub_first_{}", &suffix[..10]);
    let second_table = format!("rustium_pg_pub_second_{}", &suffix[..10]);
    let dynamic_table = format!("rustium_pg_pub_dynamic_{}", &suffix[..10]);
    let disabled_publication = format!("rustium_pg_pub_disabled_{}", &suffix[..10]);
    let all_publication = format!("rustium_pg_pub_all_{}", &suffix[..10]);
    let filtered_publication = format!("rustium_pg_pub_filtered_{}", &suffix[..10]);
    let conflicting_publication = format!("rustium_pg_pub_conflict_{}", &suffix[..10]);
    let dynamic_publication = format!("rustium_pg_pub_empty_{}", &suffix[..10]);
    let dynamic_slot = format!("rustium_pg_pub_slot_{}", &suffix[..10]);
    let connector_name = format!("postgresql-publication-{}", &suffix[..10]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{first_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE public.{second_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE public.{dynamic_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL);"
            ))
            .await?;

        let mut disabled_config =
            settings.source_config(&disabled_publication, "unused_disabled_slot", &first_table);
        disabled_config.publication_autocreate_mode = PublicationAutoCreateMode::Disabled;
        let mut disabled_source = PostgresSource::new(
            format!("{connector_name}-disabled"),
            disabled_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        let disabled_error = disabled_source.validate().await.unwrap_err();
        require(
            disabled_error
                .to_string()
                .contains("autocreation is disabled"),
            "disabled mode did not reject a missing publication",
        )?;

        let mut all_config =
            settings.source_config(&all_publication, "unused_all_slot", &first_table);
        all_config.publication_autocreate_mode = PublicationAutoCreateMode::AllTables;
        let mut all_source = PostgresSource::new(
            format!("{connector_name}-all"),
            all_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        all_source.validate().await?;
        let all_tables = client
            .query_one(
                "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = $1",
                &[&all_publication],
            )
            .await?
            .get::<_, bool>(0);
        require(all_tables, "all_tables mode did not create FOR ALL TABLES")?;

        let mut filtered_config =
            settings.source_config(&filtered_publication, "unused_filtered_slot", &first_table);
        filtered_config.publication_autocreate_mode = PublicationAutoCreateMode::Filtered;
        let mut filtered_source = PostgresSource::new(
            format!("{connector_name}-filtered-create"),
            filtered_config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        filtered_source.validate().await?;
        require(
            publication_tables(&client, &filtered_publication).await?
                == [format!("public.{first_table}")],
            "filtered mode did not create the exact selected publication table set",
        )?;

        filtered_config.tables = TableSelection {
            include: vec![format!(r"public\.{second_table}")],
            exclude: Vec::new(),
        };
        let mut updated_source = PostgresSource::new(
            format!("{connector_name}-filtered-update"),
            filtered_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        updated_source.validate().await?;
        require(
            publication_tables(&client, &filtered_publication).await?
                == [format!("public.{second_table}")],
            "filtered mode did not replace an existing publication table set",
        )?;

        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {conflicting_publication} FOR ALL TABLES"
            ))
            .await?;
        let mut conflicting_config = settings.source_config(
            &conflicting_publication,
            "unused_conflicting_slot",
            &first_table,
        );
        conflicting_config.publication_autocreate_mode = PublicationAutoCreateMode::Filtered;
        let mut conflicting_source = PostgresSource::new(
            format!("{connector_name}-filtered-conflict"),
            conflicting_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        let conflicting_error = conflicting_source.validate().await.unwrap_err();
        require(
            conflicting_error
                .to_string()
                .contains("is FOR ALL TABLES and cannot be updated"),
            "filtered mode did not reject an existing FOR ALL TABLES publication",
        )?;

        let mut dynamic_config =
            settings.source_config(&dynamic_publication, &dynamic_slot, &dynamic_table);
        dynamic_config.publication_autocreate_mode = PublicationAutoCreateMode::NoTables;
        let mut dynamic_source = PostgresSource::new(
            format!("{connector_name}-no-tables"),
            dynamic_config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        dynamic_source.validate().await?;
        require(
            publication_tables(&client, &dynamic_publication)
                .await?
                .is_empty(),
            "no_tables mode did not create an empty publication",
        )?;

        let (mut output, cancellation, source_task) = start_source(dynamic_source, None);
        let capture_result: TestResult = async {
            let completion = receive(&mut output).await?;
            require(
                completion.boundary == RecordBoundary::SnapshotComplete,
                "no_tables mode emitted snapshot data for an empty publication",
            )?;
            wait_for_active_slot(&client, &dynamic_slot).await?;
            client
                .batch_execute(&format!(
                    "ALTER PUBLICATION {dynamic_publication} ADD TABLE public.{dynamic_table}; \
                     INSERT INTO public.{dynamic_table} VALUES (1, 'dynamic');"
                ))
                .await?;

            loop {
                let record = receive(&mut output).await?;
                if record.boundary != RecordBoundary::Data {
                    continue;
                }
                let event = record
                    .event
                    .ok_or_else(|| test_error("dynamic publication data record has no event"))?;
                require(
                    event.operation == Operation::Create,
                    "dynamic publication event was not a create",
                )?;
                require(
                    event.after.as_ref().and_then(|row| row.get("value"))
                        == Some(&DataValue::String("dynamic".into())),
                    "dynamic publication event payload is incorrect",
                )?;
                break;
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let publication_cleanup = client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {disabled_publication}; \
             DROP PUBLICATION IF EXISTS {all_publication}; \
             DROP PUBLICATION IF EXISTS {filtered_publication}; \
             DROP PUBLICATION IF EXISTS {conflicting_publication};"
        ))
        .await
        .map_err(|error| -> Box<dyn StdError + Send + Sync> { Box::new(error) });
    let resource_cleanup = cleanup(
        &client,
        &dynamic_publication,
        &dynamic_slot,
        &[&first_table, &second_table, &dynamic_table],
    )
    .await;
    connection_task.abort();

    match (outcome, publication_cleanup, resource_cleanup) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), _, _) => Err(error),
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn applies_debezium_replica_identity_autoset_values_atomically() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let default_table = format!("rustium_pg_identity_default_{}", &suffix[..8]);
    let full_table = format!("rustium_pg_identity_full_{}", &suffix[..8]);
    let nothing_table = format!("rustium_pg_identity_nothing_{}", &suffix[..8]);
    let index_table = format!("rustium_pg_identity_index_{}", &suffix[..8]);
    let index_name = format!("rustium_pg_identity_key_{}", &suffix[..8]);
    let publication = format!("rustium_pg_identity_pub_{}", &suffix[..8]);
    let slot_name = format!("rustium_pg_identity_slot_{}", &suffix[..8]);
    let connector_name = format!("postgresql-identity-{}", &suffix[..8]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{default_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 ALTER TABLE public.{default_table} REPLICA IDENTITY FULL; \
                 CREATE TABLE public.{full_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE public.{nothing_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE public.{index_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE UNIQUE INDEX {index_name} ON public.{index_table} (value);"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &full_table);
        config.publication_autocreate_mode = PublicationAutoCreateMode::Filtered;
        config.tables = TableSelection {
            include: vec![format!(
                r"public\.(?:{default_table}|{full_table}|{nothing_table}|{index_table})"
            )],
            exclude: Vec::new(),
        };
        config.replica_identity_autoset_values = vec![
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{default_table}"),
                identity: PostgresReplicaIdentity::Default,
                index: None,
            },
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{full_table}"),
                identity: PostgresReplicaIdentity::Full,
                index: None,
            },
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{nothing_table}"),
                identity: PostgresReplicaIdentity::Nothing,
                index: None,
            },
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{index_table}"),
                identity: PostgresReplicaIdentity::Index,
                index: Some(index_name.clone()),
            },
        ];
        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;

        require(
            replica_identity(&client, &default_table).await? == ("d".into(), None),
            "DEFAULT replica identity was not applied",
        )?;
        require(
            replica_identity(&client, &full_table).await? == ("f".into(), None),
            "FULL replica identity was not applied",
        )?;
        require(
            replica_identity(&client, &nothing_table).await? == ("n".into(), None),
            "NOTHING replica identity was not applied",
        )?;
        require(
            replica_identity(&client, &index_table).await?
                == ("i".into(), Some(index_name.clone())),
            "INDEX replica identity was not applied",
        )?;

        let (mut output, cancellation, source_task) = start_source(source, None);
        let capture_result: TestResult = async {
            let completion = receive(&mut output).await?;
            require(
                completion.boundary == RecordBoundary::SnapshotComplete,
                "empty replica identity fixture emitted unexpected snapshot data",
            )?;
            wait_for_active_slot(&client, &slot_name).await?;
            client
                .batch_execute(&format!(
                    "BEGIN; \
                     INSERT INTO public.{full_table} VALUES (1, 'full-old'); \
                     INSERT INTO public.{index_table} VALUES (1, 'index-old'); \
                     UPDATE public.{full_table} SET value = 'full-new' WHERE id = 1; \
                     UPDATE public.{index_table} SET value = 'index-new' WHERE id = 1; \
                     COMMIT;"
                ))
                .await?;

            let mut full_before = None;
            let mut index_before = None;
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::TransactionCommit {
                    break;
                }
                let Some(event) = record.event else {
                    continue;
                };
                if event.operation != Operation::Update {
                    continue;
                }
                match event.source.table.as_deref() {
                    Some(table) if table == full_table => full_before = event.before,
                    Some(table) if table == index_table => index_before = event.before,
                    _ => {}
                }
            }
            let full_before = full_before
                .ok_or_else(|| test_error("FULL replica identity update has no before image"))?;
            require(
                full_before.get("id") == Some(&DataValue::Int64(1))
                    && full_before.get("value") == Some(&DataValue::String("full-old".into())),
                "FULL replica identity did not emit the complete old row",
            )?;
            let index_before = index_before
                .ok_or_else(|| test_error("INDEX replica identity update has no before image"))?;
            require(
                index_before.get("id") == Some(&DataValue::Null)
                    && index_before.get("value") == Some(&DataValue::String("index-old".into())),
                "INDEX replica identity did not emit its old key with null non-key placeholders",
            )
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)?;

        let mut invalid_index_config = config.clone();
        invalid_index_config.replica_identity_autoset_values = vec![
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{default_table}"),
                identity: PostgresReplicaIdentity::Full,
                index: None,
            },
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{index_table}"),
                identity: PostgresReplicaIdentity::Index,
                index: Some(format!("missing_replica_index_{}", &suffix[..8])),
            },
        ];
        let mut invalid_index_source = PostgresSource::new(
            format!("{connector_name}-invalid-index"),
            invalid_index_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        let invalid_index_error = invalid_index_source.validate().await.unwrap_err();
        require(
            invalid_index_error.to_string().contains("failed to set")
                && invalid_index_error.to_string().contains(&index_table),
            "invalid replica identity index did not fail validation",
        )?;
        require(
            replica_identity(&client, &default_table).await? == ("d".into(), None)
                && replica_identity(&client, &index_table).await?
                    == ("i".into(), Some(index_name.clone())),
            "replica identity SQL failure did not roll back the complete DDL transaction",
        )?;

        config.replica_identity_autoset_values = vec![
            PostgresReplicaIdentityRule {
                table: format!(r"public\.{full_table}"),
                identity: PostgresReplicaIdentity::Full,
                index: None,
            },
            PostgresReplicaIdentityRule {
                table: r"public\.rustium_pg_identity_.*".into(),
                identity: PostgresReplicaIdentity::Nothing,
                index: None,
            },
        ];
        let mut conflicting_source = PostgresSource::new(
            format!("{connector_name}-conflict"),
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        let error = conflicting_source.validate().await.unwrap_err();
        require(
            error.to_string().contains("more than one") && error.to_string().contains(&full_table),
            "overlapping replica identity rules were not rejected",
        )?;
        require(
            replica_identity(&client, &default_table).await? == ("d".into(), None)
                && replica_identity(&client, &full_table).await? == ("f".into(), None),
            "overlapping replica identity rules applied a partial update",
        )
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&default_table, &full_table, &nothing_table, &index_table],
    )
    .await;
    connection_task.abort();
    match (outcome, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn publishes_partition_changes_via_the_partition_root() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let root_table = format!("rustium_pg_partition_root_{}", &suffix[..8]);
    let first_partition = format!("rustium_pg_partition_first_{}", &suffix[..8]);
    let second_partition = format!("rustium_pg_partition_second_{}", &suffix[..8]);
    let publication = format!("rustium_pg_partition_pub_{}", &suffix[..8]);
    let slot_name = format!("rustium_pg_partition_slot_{}", &suffix[..8]);
    let connector_name = format!("postgresql-partition-root-{}", &suffix[..8]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{root_table} (\
                    id BIGINT NOT NULL, \
                    value TEXT NOT NULL, \
                    PRIMARY KEY (id)\
                 ) PARTITION BY RANGE (id); \
                 CREATE TABLE public.{first_partition} PARTITION OF public.{root_table} \
                    FOR VALUES FROM (0) TO (100); \
                 CREATE TABLE public.{second_partition} PARTITION OF public.{root_table} \
                    FOR VALUES FROM (100) TO (200); \
                 INSERT INTO public.{root_table} VALUES (1, 'snapshot-root');"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &root_table);
        config.publication_autocreate_mode = PublicationAutoCreateMode::Filtered;
        config.publish_via_partition_root = true;
        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let via_root = client
            .query_one(
                "SELECT pubviaroot FROM pg_catalog.pg_publication WHERE pubname = $1",
                &[&publication],
            )
            .await?
            .get::<_, bool>(0);
        require(
            via_root,
            "publication was not configured via the partition root",
        )?;

        let (mut output, cancellation, source_task) = start_source(source, None);
        let capture_result: TestResult = async {
            let snapshot = receive(&mut output).await?;
            let snapshot_event = snapshot
                .event
                .ok_or_else(|| test_error("partition snapshot record has no event"))?;
            require(
                snapshot_event.operation == Operation::Read
                    && snapshot_event.source.table.as_deref() == Some(root_table.as_str()),
                "partition snapshot was not attributed to the root table",
            )?;
            let completion = receive(&mut output).await?;
            require(
                completion.boundary == RecordBoundary::SnapshotComplete,
                "partition snapshot did not complete after its root row",
            )?;
            wait_for_active_slot(&client, &slot_name).await?;
            client
                .execute(
                    &format!("INSERT INTO public.{root_table} VALUES (150, 'stream-root')"),
                    &[],
                )
                .await?;
            loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                require(
                    event.operation == Operation::Create
                        && event.source.table.as_deref() == Some(root_table.as_str())
                        && event.after.as_ref().and_then(|row| row.get("value"))
                            == Some(&DataValue::String("stream-root".into())),
                    "partition WAL event was not attributed to the root table",
                )?;
                break;
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)?;

        config.publish_via_partition_root = false;
        let mut mismatched_source = PostgresSource::new(
            format!("{connector_name}-mismatch"),
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                ..SnapshotConfig::default()
            },
        );
        let mismatch = mismatched_source.validate().await.unwrap_err();
        require(
            mismatch
                .to_string()
                .contains("publish_via_partition_root=true")
                && mismatch
                    .to_string()
                    .contains("source.publish_via_partition_root=false"),
            "existing publication partition-root mismatch was not rejected",
        )
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&first_partition, &second_partition, &root_table],
    )
    .await;
    connection_task.abort();
    match (outcome, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 17+ primary with logical replication enabled"]
async fn creates_postgresql_17_failover_slot() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_failover_{}", &suffix[..12]);
    let publication = format!("rustium_pg_failover_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_pg_failover_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-failover-slot-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        let server_version = client
            .query_one("SHOW server_version_num", &[])
            .await?
            .get::<_, String>(0)
            .parse::<u32>()?;
        require(
            server_version >= 170_000,
            "slot.failover integration requires PostgreSQL 17 or newer",
        )?;
        let in_recovery = client
            .query_one("SELECT pg_catalog.pg_is_in_recovery()", &[])
            .await?
            .get::<_, bool>(0);
        require(
            !in_recovery,
            "slot.failover integration requires a PostgreSQL primary",
        )?;

        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES (1, 'snapshot-failover'); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.slot_failover = true;
        let mut source = PostgresSource::new(
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
            let snapshot = receive(&mut output).await?;
            require(
                snapshot.event.as_ref().is_some_and(|event| {
                    event.operation == Operation::Read
                        && event.after.as_ref().and_then(|row| row.get("value"))
                            == Some(&DataValue::String("snapshot-failover".into()))
                }),
                "failover slot initial snapshot row is incorrect",
            )?;
            let completion = receive(&mut output).await?;
            require(
                completion.boundary == RecordBoundary::SnapshotComplete,
                "failover slot initial snapshot did not complete",
            )?;
            wait_for_active_slot(&client, &slot_name).await?;
            let failover = client
                .query_one(
                    "SELECT failover FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&slot_name],
                )
                .await?
                .get::<_, bool>(0);
            require(
                failover,
                "Rustium did not create a PostgreSQL failover slot",
            )?;

            client
                .execute(
                    &format!("INSERT INTO public.{table_name} VALUES (2, 'stream-failover')"),
                    &[],
                )
                .await?;
            loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                require(
                    event.operation == Operation::Create
                        && event.after.as_ref().and_then(|row| row.get("value"))
                            == Some(&DataValue::String("stream-failover".into())),
                    "failover slot did not stream the inserted row",
                )?;
                break;
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    match (outcome, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn filters_initial_snapshot_without_narrowing_streaming() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let snapshot_table = format!("rustium_pg_snapshot_filter_{}", &suffix[..10]);
    let stream_table = format!("rustium_pg_stream_filter_{}", &suffix[..10]);
    let publication = format!("rustium_pg_filter_pub_{}", &suffix[..10]);
    let slot_name = format!("rustium_pg_filter_slot_{}", &suffix[..10]);
    let connector_name = format!("postgresql-filter-{}", &suffix[..10]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{snapshot_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE public.{stream_table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 INSERT INTO public.{snapshot_table} VALUES (1, 'one'), (2, 'two'); \
                 INSERT INTO public.{stream_table} VALUES (100, 'excluded from initial snapshot'); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{snapshot_table}, public.{stream_table};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &snapshot_table);
        config.tables.include = vec![format!(
            r"public\.({snapshot_table}|{stream_table})"
        )];
        let mut source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: vec![format!(r"public\.{snapshot_table}")],
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);

        let capture: TestResult = async {
            let mut snapshot_rows = 0;
            loop {
                let record = receive_with_context(
                    &mut output,
                    "waiting for the filtered PostgreSQL initial snapshot",
                )
                .await?;
                if record.boundary == RecordBoundary::SnapshotComplete {
                    break;
                }
                let event = record
                    .event
                    .ok_or_else(|| test_error("filtered snapshot record has no event"))?;
                require(
                    event.operation == Operation::Read
                        && event.source.snapshot
                        && event.source.table.as_deref() == Some(snapshot_table.as_str()),
                    "PostgreSQL snapshot collection filter emitted an unexpected table",
                )?;
                snapshot_rows += 1;
            }
            require(
                snapshot_rows == 2,
                "PostgreSQL snapshot collection filter emitted the wrong row count",
            )?;

            client
                .execute(
                    &format!(
                        "INSERT INTO public.{stream_table} (id, value) VALUES (101, 'streamed after snapshot')"
                    ),
                    &[],
                )
                .await?;
            loop {
                let record = receive_with_context(
                    &mut output,
                    "waiting for PostgreSQL streaming outside the snapshot filter",
                )
                .await?;
                if let Some(event) = record.event
                    && event.source.table.as_deref() == Some(stream_table.as_str())
                {
                    require(
                        event.operation == Operation::Create && !event.source.snapshot,
                        "PostgreSQL snapshot collection filter changed streaming semantics",
                    )?;
                    require(
                        event.after.as_ref().and_then(|row| row.get("id"))
                            == Some(&DataValue::Int64(101)),
                        "PostgreSQL streaming row outside the snapshot filter changed",
                    )?;
                    break;
                }
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture, stop_result)
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&snapshot_table, &stream_table],
    )
    .await;
    connection_task.abort();
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL snapshot filter cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn emits_heartbeat_and_executes_action_query() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_heartbeat_{}", &suffix[..12]);
    let action_table = format!("rustium_pg_heartbeat_action_{}", &suffix[..12]);
    let publication = format!("rustium_hb_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_hb_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-heartbeat-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES (1, 'initial'); \
                 CREATE TABLE public.{action_table} (\
                    id INTEGER PRIMARY KEY, beats BIGINT NOT NULL\
                 ); \
                 INSERT INTO public.{action_table} VALUES (1, 0); \
                 CREATE PUBLICATION {publication} FOR TABLE \
                    public.{table_name}, public.{action_table};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.heartbeat_interval = Duration::from_millis(100);
        config.heartbeat_action_query = Some(format!(
            "UPDATE public.{action_table} SET beats = beats + 1 WHERE id = 1"
        ));
        config.heartbeat_topics_prefix = "__rustium-test-heartbeat".into();
        let mut source = PostgresSource::new(
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
            let mut snapshot_rows = 0;
            let mut first_heartbeat_position = None;
            let mut saw_action_commit = false;
            loop {
                let record = receive(&mut output).await?;
                match record.boundary {
                    RecordBoundary::Data => {
                        let event = record.event.ok_or_else(|| {
                            test_error("PostgreSQL snapshot data record has no event")
                        })?;
                        require(
                            event.operation == Operation::Read,
                            "unexpected data event before PostgreSQL heartbeat",
                        )?;
                        snapshot_rows += 1;
                    }
                    RecordBoundary::SnapshotComplete => {
                        require(
                            snapshot_rows == 1,
                            "PostgreSQL heartbeat snapshot row count is incorrect",
                        )?;
                        wait_for_active_slot(&client, &slot_name).await?;
                    }
                    RecordBoundary::Heartbeat => {
                        let event = record.event.ok_or_else(|| {
                            test_error("PostgreSQL heartbeat record has no event")
                        })?;
                        require(
                            event.operation == Operation::Message,
                            "PostgreSQL heartbeat is not a message event",
                        )?;
                        require(
                            event.source.schema.is_none() && event.source.table.is_none(),
                            "PostgreSQL heartbeat was exposed as a table event",
                        )?;
                        require(
                            event.source.attributes.get("rustium.heartbeat") == Some(&true.into()),
                            "PostgreSQL heartbeat marker is missing",
                        )?;
                        require(
                            matches!(
                                &record.position,
                                SourcePosition::Postgres(position) if !position.snapshot
                            ),
                            "PostgreSQL heartbeat does not carry a streaming WAL position",
                        )?;
                        require(
                            matches!(
                                event.after.as_ref().and_then(|row| row.get("ts_ms")),
                                Some(DataValue::Int64(_))
                            ),
                            "PostgreSQL heartbeat timestamp is missing",
                        )?;
                        if let Some(first_position) = &first_heartbeat_position {
                            require(
                                saw_action_commit,
                                "heartbeat.action.query transaction commit was not observed",
                            )?;
                            require(
                                record.position.is_after(first_position),
                                "PostgreSQL heartbeat did not advance after heartbeat.action.query",
                            )?;
                            break;
                        }
                        first_heartbeat_position = Some(record.position);
                    }
                    RecordBoundary::TransactionCommit => {
                        saw_action_commit |= first_heartbeat_position.is_some();
                    }
                }
            }

            let row = client
                .query_one(
                    &format!("SELECT beats FROM public.{action_table} WHERE id = 1"),
                    &[],
                )
                .await?;
            require(
                row.get::<_, i64>(0) > 0,
                "heartbeat.action.query did not update the PostgreSQL heartbeat table",
            )
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &action_table],
    )
    .await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL heartbeat cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn keeps_snapshot_and_streaming_type_conversion_identical() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_types_{}", &suffix[..12]);
    let publication = format!("rustium_types_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_types_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-types-{}", &suffix[..12]);
    let domain_name = format!("rustium_amount_{}", &suffix[..12]);
    let enum_name = format!("rustium_state_{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE EXTENSION IF NOT EXISTS hstore; \
                 CREATE DOMAIN public.{domain_name} AS NUMERIC(12,4); \
                 CREATE TYPE public.{enum_name} AS ENUM ('ready', 'done'); \
                 CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, \
                    flag BOOLEAN NOT NULL, \
                    small_value SMALLINT NOT NULL, \
                    integer_value INTEGER NOT NULL, \
                    big_value BIGINT NOT NULL, \
                    real_value REAL NOT NULL, \
                    double_value DOUBLE PRECISION NOT NULL, \
                    special_double DOUBLE PRECISION NOT NULL, \
                    amount NUMERIC(30,8) NOT NULL, \
                    special_numeric NUMERIC NOT NULL, \
                    payload JSONB NOT NULL, \
                    token UUID NOT NULL, \
                    event_date DATE NOT NULL, \
                    event_time TIME(6) NOT NULL, \
                    event_time_tz TIME(6) WITH TIME ZONE NOT NULL, \
                    event_timestamp TIMESTAMP(6) NOT NULL, \
                    event_timestamp_tz TIMESTAMP(6) WITH TIME ZONE NOT NULL, \
                    binary_value BYTEA NOT NULL, \
                    text_values TEXT[] NOT NULL, \
                    integer_values INTEGER[] NOT NULL, \
                    uuid_values UUID[] NOT NULL, \
                    network INET NOT NULL, \
                    network_block CIDR NOT NULL, \
                    mac MACADDR NOT NULL, \
                    bits BIT(4) NOT NULL, \
                    duration INTERVAL NOT NULL, \
                    int_range INT4RANGE NOT NULL, \
                    attributes HSTORE NOT NULL, \
                    attribute_values HSTORE[] NOT NULL, \
                    domain_value public.{domain_name} NOT NULL, \
                    domain_values public.{domain_name}[] NOT NULL, \
                    state public.{enum_name} NOT NULL, \
                    search_vector TSVECTOR NOT NULL, \
                    nullable_value TEXT\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;
        insert_type_row(&client, &table_name, 1).await?;

        let config = settings.source_config(&publication, &slot_name, &table_name);
        let mut source = PostgresSource::new(
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
        let capture_result: TestResult<(Row, Row)> = async {
            let mut snapshot_row = None;
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::SnapshotComplete {
                    break;
                }
                require(
                    record.boundary == RecordBoundary::Data,
                    "unexpected boundary in PostgreSQL type snapshot",
                )?;
                let event = record
                    .event
                    .ok_or_else(|| test_error("PostgreSQL type snapshot record has no event"))?;
                require(
                    event.operation == Operation::Read,
                    "PostgreSQL type snapshot event is not a read",
                )?;
                snapshot_row = event.after;
            }
            let snapshot_row = snapshot_row
                .ok_or_else(|| test_error("PostgreSQL type snapshot row was not emitted"))?;

            wait_for_active_slot(&client, &slot_name).await?;
            insert_type_row(&client, &table_name, 2).await?;
            let streaming_row = loop {
                let record = receive(&mut output).await?;
                if record.boundary != RecordBoundary::Data {
                    continue;
                }
                let event = record
                    .event
                    .ok_or_else(|| test_error("PostgreSQL type streaming record has no event"))?;
                require(
                    event.operation == Operation::Create,
                    "PostgreSQL type streaming event is not a create",
                )?;
                break event.after.ok_or_else(|| {
                    test_error("PostgreSQL type streaming event has no after row")
                })?;
            };
            Ok((snapshot_row, streaming_row))
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        let (mut snapshot_row, mut streaming_row) =
            combine_capture_and_stop(capture_result, stop_result)?;
        require(
            snapshot_row.shift_remove("id") == Some(DataValue::Int64(1)),
            "PostgreSQL type snapshot id is incorrect",
        )?;
        require(
            streaming_row.shift_remove("id") == Some(DataValue::Int64(2)),
            "PostgreSQL type streaming id is incorrect",
        )?;
        require(
            snapshot_row == streaming_row,
            "PostgreSQL snapshot and streaming type conversion differ",
        )?;
        verify_type_row(&snapshot_row)
    }
    .await;

    let cleanup_result: TestResult = async {
        cleanup(&client, &publication, &slot_name, &[&table_name]).await?;
        client
            .batch_execute(&format!(
                "DROP DOMAIN IF EXISTS public.{domain_name}; \
                 DROP TYPE IF EXISTS public.{enum_name};"
            ))
            .await?;
        Ok(())
    }
    .await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL type test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL logical replication"]
async fn captures_debezium_logical_decoding_messages() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_message_{}", &suffix[..12]);
    let publication = format!("rustium_pg_message_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_pg_message_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-message-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let capture_result = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id integer PRIMARY KEY, note text NOT NULL); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.logical_decoding_messages = true;
        config.message_prefix_include_list = vec![r"allowed\..*".into()];
        let source = PostgresSource::new(
            connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 32,
                include_collections: Vec::new(),
            },
        );
        let (mut output, cancellation, source_task) = start_source(source, None);
        let result: TestResult = async {
            wait_for_active_slot(&client, &slot_name).await?;

            client
                .batch_execute(&format!(
                    "SELECT pg_logical_emit_message(false, 'ignored.prefix', 'ignored'); \
                 SELECT pg_logical_emit_message(false, 'allowed.binary', decode('00ff10', 'hex')); \
                 BEGIN; \
                 SELECT pg_logical_emit_message(true, 'allowed.transactional', 'transaction-content'); \
                 INSERT INTO public.{table_name} VALUES (1, 'same-transaction'); \
                 COMMIT;"
                ))
                .await?;

            let filtered = receive(&mut output).await?;
            require(
                filtered.event.is_none()
                    && filtered.boundary == RecordBoundary::TransactionCommit,
                "filtered non-transactional logical message did not advance a safe checkpoint boundary",
            )?;

            let non_transactional = receive(&mut output).await?;
            require(
                non_transactional.boundary == RecordBoundary::TransactionCommit,
                "non-transactional logical message was not independently committable",
            )?;
            let non_transactional_event = non_transactional
                .event
                .ok_or_else(|| test_error("non-transactional logical message has no event"))?;
            require(
                non_transactional_event.operation == Operation::Message
                    && non_transactional_event.transaction.is_none()
                    && non_transactional_event.source.schema.as_deref() == Some("")
                    && non_transactional_event.source.table.as_deref() == Some("")
                    && non_transactional_event
                        .after
                        .as_ref()
                        .and_then(|row| row.get("prefix"))
                        == Some(&DataValue::String("allowed.binary".into()))
                    && non_transactional_event
                        .after
                        .as_ref()
                        .and_then(|row| row.get("content"))
                        == Some(&DataValue::Bytes(vec![0x00, 0xff, 0x10])),
                "non-transactional logical message metadata or binary content is incorrect",
            )?;

            let transactional = receive(&mut output).await?;
            require(
                transactional.boundary == RecordBoundary::Data,
                "transactional logical message was committed before its transaction",
            )?;
            let transactional_event = transactional
                .event
                .ok_or_else(|| test_error("transactional logical message has no event"))?;
            let message_transaction_id = match &transactional_event.position {
                SourcePosition::Postgres(position) => position.transaction_id,
                _ => None,
            };
            require(
                transactional_event.operation == Operation::Message
                    && message_transaction_id.is_some()
                    && transactional_event
                        .transaction
                        .as_ref()
                        .is_some_and(|transaction| transaction.total_order == Some(1))
                    && transactional_event
                        .after
                        .as_ref()
                        .and_then(|row| row.get("prefix"))
                        == Some(&DataValue::String("allowed.transactional".into())),
                "transactional logical message metadata is incorrect",
            )?;

            let insert = receive(&mut output).await?;
            let insert_event = insert
                .event
                .ok_or_else(|| test_error("same-transaction insert has no event"))?;
            let insert_transaction_id = match &insert_event.position {
                SourcePosition::Postgres(position) => position.transaction_id,
                _ => None,
            };
            require(
                insert_event.operation == Operation::Create
                    && insert_transaction_id == message_transaction_id
                    && insert_event
                        .transaction
                        .as_ref()
                        .is_some_and(|transaction| transaction.total_order == Some(2)),
                "logical message and row event did not preserve source transaction order",
            )?;

            let commit = receive(&mut output).await?;
            require(
                commit.event.is_none()
                    && commit.boundary == RecordBoundary::TransactionCommit
                    && match &commit.position {
                        SourcePosition::Postgres(position) => {
                            position.transaction_id == message_transaction_id
                        }
                        _ => false,
                    },
                "logical message transaction did not end at the expected commit boundary",
            )?;
            Ok(())
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    let _ = connection_task.await;
    capture_result?;
    cleanup_result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL logical replication"]
async fn advances_confirmed_flush_lsn_on_the_configured_feedback_interval() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_feedback_{}", &suffix[..12]);
    let publication = format!("rustium_pg_feedback_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_pg_feedback_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-feedback-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let capture_result = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id integer PRIMARY KEY, note text NOT NULL); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.status_update_interval = Duration::from_millis(25);
        config.tcp_keepalive = false;
        let mut source = PostgresSource::new(
            connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 32,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let running = start_source_with_acknowledgement(source, None, 512);
        let mut output = running.output;
        let cancellation = running.cancellation;
        let source_task = running.task;
        let acknowledgement = running.acknowledgement;
        let result: TestResult = async {
            wait_for_active_slot(&client, &slot_name).await?;
            client
                .batch_execute(&format!(
                    "INSERT INTO public.{table_name} VALUES (1, 'acknowledged');"
                ))
                .await?;

            let commit = loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::TransactionCommit {
                    break record;
                }
                require(
                    record
                        .event
                        .as_ref()
                        .is_some_and(|event| event.operation == Operation::Create),
                    "feedback fixture emitted an unexpected record before commit",
                )?;
            };
            let acknowledged_lsn = match &commit.position {
                SourcePosition::Postgres(position) => position.commit_lsn.unwrap_or(position.lsn),
                SourcePosition::MySql(_) | SourcePosition::SqlServer(_) => {
                    return Err(test_error(
                        "feedback fixture emitted a non-PostgreSQL position",
                    ));
                }
            };
            acknowledgement
                .send(Some(commit.position))
                .map_err(|error| {
                    test_error(&format!("failed to acknowledge PostgreSQL LSN: {error}"))
                })?;

            tokio::time::sleep(Duration::from_millis(75)).await;
            client
                .batch_execute(&format!(
                    "INSERT INTO public.{table_name} (id, note) \
                     SELECT generated.id, 'feedback' \
                     FROM generate_series(2, 170) AS generated(id);"
                ))
                .await?;
            wait_for_confirmed_flush_lsn(&client, &slot_name, acknowledged_lsn).await
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    let _ = connection_task.await;
    capture_result?;
    cleanup_result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL logical replication"]
async fn reconciles_debezium_checkpoint_slot_mismatch_strategies() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_offset_{}", &suffix[..12]);
    let publication = format!("rustium_pg_offset_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_pg_offset_slot_{}", &suffix[..12]);
    let greater_slot = format!("rustium_pg_greater_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-offset-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let capture_result: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, \
                    customer TEXT NOT NULL, \
                    amount NUMERIC(10,2) NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES \
                    (1, 'Alice', 12.30), \
                    (2, 'Bob', 45.60); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let base_config = settings.source_config(&publication, &slot_name, &table_name);
        let (checkpoint_position, schema_history) = run_initial_capture(
            &client,
            &connector_name,
            &table_name,
            &slot_name,
            base_config.clone(),
        )
        .await?;
        let checkpoint_lsn = match &checkpoint_position {
            SourcePosition::Postgres(position) => position.commit_lsn.unwrap_or(position.lsn),
            SourcePosition::MySql(_) | SourcePosition::SqlServer(_) => {
                return Err(test_error(
                    "offset fixture produced a non-PostgreSQL checkpoint",
                ));
            }
        };
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: checkpoint_position,
            snapshot_completed: true,
            config_fingerprint: "postgresql-offset-strategy-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(schema_history.clone()),
        };
        wait_for_inactive_slot(&client, &slot_name).await?;
        require(
            replication_slot_confirmed_lsn(&client, &slot_name).await? < checkpoint_lsn,
            "offset fixture did not create a checkpoint ahead of the replication slot",
        )?;

        let mut trust_offset_config = base_config.clone();
        trust_offset_config.offset_mismatch_strategy = PostgresOffsetMismatchStrategy::TrustOffset;
        let mut source = PostgresSource::new(
            &connector_name,
            trust_offset_config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source(source, Some(checkpoint.clone()));
        wait_for_active_slot(&client, &slot_name).await?;
        require(
            replication_slot_confirmed_lsn(&client, &slot_name).await? >= checkpoint_lsn,
            "trust_offset did not advance the slot to the checkpoint",
        )?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount) \
                     VALUES (4, 'Dora', 67.80)"
                ),
                &[],
            )
            .await?;
        require(
            receive_postgres_create_id(&mut output).await? == 4,
            "trust_offset replayed a record before the checkpoint",
        )?;
        stop_source(cancellation, source_task).await?;
        wait_for_inactive_slot(&client, &slot_name).await?;

        client
            .batch_execute(&format!(
                "ALTER TABLE public.{table_name} \
                 ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
            ))
            .await?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount, status) \
                     VALUES (5, 'Eve', 78.90, 'skipped')"
                ),
                &[],
            )
            .await?;
        let slot_ahead_lsn = current_wal_lsn(&client).await?;
        advance_replication_slot(&client, &slot_name, slot_ahead_lsn).await?;
        require(
            replication_slot_confirmed_lsn(&client, &slot_name).await? > checkpoint_lsn,
            "offset fixture did not advance the slot ahead of the checkpoint",
        )?;

        let mut source = PostgresSource::new(
            &connector_name,
            trust_offset_config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (_output, _cancellation, source_task) = start_source(source, Some(checkpoint.clone()));
        let source_result = tokio::time::timeout(Duration::from_secs(10), source_task).await??;
        let source_error =
            source_result.expect_err("trust_offset must reject a slot ahead of its checkpoint");
        require(
            source_error.to_string().contains("ahead of checkpoint"),
            "trust_offset returned the wrong slot-ahead error",
        )?;

        let mut trust_slot_config = base_config.clone();
        trust_slot_config.offset_mismatch_strategy = PostgresOffsetMismatchStrategy::TrustSlot;
        let mut source = PostgresSource::new(
            &connector_name,
            trust_slot_config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source(source, Some(checkpoint.clone()));
        wait_for_active_slot(&client, &slot_name).await?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount, status) \
                     VALUES (6, 'Finn', 89.10, 'ready')"
                ),
                &[],
            )
            .await?;
        let trust_slot_record = receive_postgres_create(&mut output).await?;
        require(
            postgres_create_id(&trust_slot_record)? == 6,
            "trust_slot replayed a record behind the authoritative slot",
        )?;
        let trust_slot_event = trust_slot_record
            .event
            .as_ref()
            .ok_or_else(|| test_error("trust_slot create record has no event"))?;
        require(
            trust_slot_event
                .after
                .as_ref()
                .and_then(|row| row.get("status"))
                == Some(&DataValue::String("ready".into()))
                && trust_slot_event
                    .schema
                    .fields
                    .iter()
                    .any(|field| field.name == "status"),
            "trust_slot did not decode the schema change skipped by the authoritative slot",
        )?;
        let refreshed_schema_history =
            trust_slot_record.connector_state.clone().ok_or_else(|| {
                test_error("trust_slot did not checkpoint its refreshed PostgreSQL schema state")
            })?;
        stop_source(cancellation, source_task).await?;
        wait_for_inactive_slot(&client, &slot_name).await?;

        client
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&greater_slot],
            )
            .await?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount, status) \
                     VALUES (7, 'Gia', 91.20, 'processed')"
                ),
                &[],
            )
            .await?;
        let greater_checkpoint_lsn = current_wal_lsn(&client).await?;
        require(
            replication_slot_confirmed_lsn(&client, &greater_slot).await? < greater_checkpoint_lsn,
            "greater-LSN fixture did not create a slot behind the checkpoint",
        )?;
        let greater_checkpoint = Checkpoint {
            source_position: SourcePosition::Postgres(PostgresPosition {
                lsn: greater_checkpoint_lsn,
                commit_lsn: Some(greater_checkpoint_lsn),
                transaction_id: None,
                event_serial: 0,
                snapshot: false,
            }),
            connector_state: Some(refreshed_schema_history),
            ..checkpoint
        };
        let mut greater_config = base_config;
        greater_config.slot_name = greater_slot.clone();
        greater_config.offset_mismatch_strategy = PostgresOffsetMismatchStrategy::TrustGreaterLsn;
        let mut source = PostgresSource::new(
            &connector_name,
            greater_config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source(source, Some(greater_checkpoint));
        wait_for_active_slot(&client, &greater_slot).await?;
        require(
            replication_slot_confirmed_lsn(&client, &greater_slot).await? >= greater_checkpoint_lsn,
            "trust_greater_lsn did not advance a lagging slot",
        )?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount, status) \
                     VALUES (8, 'Hana', 92.30, 'greater')"
                ),
                &[],
            )
            .await?;
        require(
            receive_postgres_create_id(&mut output).await? == 8,
            "trust_greater_lsn replayed a record before the selected greater LSN",
        )?;
        stop_source(cancellation, source_task).await
    }
    .await;

    let greater_slot_cleanup = drop_replication_slot(&client, &greater_slot).await;
    let primary_cleanup = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    let _ = connection_task.await;
    capture_result?;
    greater_slot_cleanup?;
    primary_cleanup
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL logical replication"]
async fn applies_debezium_lsn_flush_ownership_modes() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_flush_{}", &suffix[..12]);
    let unmonitored_table = format!("rustium_pg_flush_other_{}", &suffix[..12]);
    let publication = format!("rustium_pg_flush_pub_{}", &suffix[..12]);
    let manual_slot = format!("rustium_pg_manual_slot_{}", &suffix[..12]);
    let driver_slot = format!("rustium_pg_driver_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-flush-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let capture_result: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, note TEXT NOT NULL); \
                 CREATE TABLE public.{unmonitored_table} (id BIGINT PRIMARY KEY); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut manual_config = settings.source_config(&publication, &manual_slot, &table_name);
        manual_config.status_update_interval = Duration::from_millis(25);
        manual_config.lsn_flush_mode = PostgresLsnFlushMode::Manual;
        let mut source = PostgresSource::new(
            &connector_name,
            manual_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 32,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let running = start_source_with_acknowledgement(source, None, 512);
        let mut output = running.output;
        let cancellation = running.cancellation;
        let source_task = running.task;
        let acknowledgement = running.acknowledgement;
        wait_for_active_slot(&client, &manual_slot).await?;

        client
            .batch_execute(&format!(
                "INSERT INTO public.{table_name} VALUES (1, 'manual-ack');"
            ))
            .await?;
        let manual_commit = receive_postgres_transaction_commit(&mut output).await?;
        let manual_ack_lsn = postgres_record_commit_lsn(&manual_commit)?;
        acknowledgement
            .send(Some(manual_commit.position))
            .map_err(|error| test_error(&format!("failed to send manual LSN ack: {error}")))?;
        tokio::time::sleep(Duration::from_millis(75)).await;
        client
            .batch_execute(&format!(
                "INSERT INTO public.{table_name} (id, note) \
                 SELECT generated.id, 'manual-drive' \
                 FROM generate_series(2, 170) AS generated(id);"
            ))
            .await?;
        receive_postgres_transaction_commit(&mut output).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        require(
            replication_slot_confirmed_lsn(&client, &manual_slot).await? < manual_ack_lsn,
            "lsn.flush.mode=manual reported a runtime acknowledgement to PostgreSQL",
        )?;
        stop_source(cancellation, source_task).await?;

        let mut driver_config = settings.source_config(&publication, &driver_slot, &table_name);
        driver_config.status_update_interval = Duration::from_millis(25);
        driver_config.lsn_flush_mode = PostgresLsnFlushMode::ConnectorAndDriver;
        let mut source = PostgresSource::new(
            &connector_name,
            driver_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 32,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source_with_output_capacity(source, None, 512);
        wait_for_active_slot(&client, &driver_slot).await?;

        client
            .batch_execute(&format!(
                "INSERT INTO public.{table_name} VALUES (1000, 'driver-unacknowledged');"
            ))
            .await?;
        let driver_commit = receive_postgres_transaction_commit(&mut output).await?;
        let driver_target_lsn = postgres_record_commit_lsn(&driver_commit)?;
        tokio::time::sleep(Duration::from_millis(75)).await;
        client
            .batch_execute(&format!(
                "INSERT INTO public.{table_name} (id, note) \
                 SELECT generated.id, 'driver-drive' \
                 FROM generate_series(1001, 1170) AS generated(id);"
            ))
            .await?;
        receive_postgres_transaction_commit(&mut output).await?;
        wait_for_confirmed_flush_lsn(&client, &driver_slot, driver_target_lsn).await?;

        if optional_env_bool("RUSTIUM_POSTGRES_REQUIRE_FAST_KEEPALIVE")? {
            let before = replication_slot_confirmed_lsn(&client, &driver_slot).await?;
            client
                .batch_execute(&format!(
                    "INSERT INTO public.{unmonitored_table} VALUES (1);"
                ))
                .await?;
            let unmonitored_target = current_wal_lsn(&client).await?;
            require(
                unmonitored_target > before,
                "unmonitored WAL fixture did not advance the server LSN",
            )?;
            wait_for_confirmed_flush_lsn(&client, &driver_slot, unmonitored_target).await?;
        }
        stop_source(cancellation, source_task).await
    }
    .await;

    let driver_slot_cleanup = drop_replication_slot(&client, &driver_slot).await;
    let primary_cleanup = cleanup(
        &client,
        &publication,
        &manual_slot,
        &[&table_name, &unmonitored_table],
    )
    .await;
    connection_task.abort();
    let _ = connection_task.await;
    capture_result?;
    driver_slot_cleanup?;
    primary_cleanup
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL 16+ logical replication and replication-origin administration"]
async fn applies_pgoutput_origin_slot_stream_parameter() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_origin_{}", &suffix[..12]);
    let publication = format!("rustium_pg_origin_pub_{}", &suffix[..12]);
    let none_slot = format!("rustium_pg_origin_none_{}", &suffix[..12]);
    let any_slot = format!("rustium_pg_origin_any_{}", &suffix[..12]);
    let origin_name = format!("rustium_pg_origin_{}", &suffix[..12]);
    let connector_name = format!("postgresql-origin-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let capture_result: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, note TEXT NOT NULL); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;
        client
            .query_one("SELECT pg_replication_origin_create($1)", &[&origin_name])
            .await?;

        let mut none_config = settings.source_config(&publication, &none_slot, &table_name);
        none_config
            .slot_stream_params
            .insert("origin".into(), "none".into());
        let mut source = PostgresSource::new(
            &connector_name,
            none_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 32,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
        let none_capture: TestResult = async {
            wait_for_active_slot(&client, &none_slot).await?;
            insert_with_replication_origin(&client, &table_name, &origin_name, 1).await?;
            client
                .batch_execute(&format!(
                    "INSERT INTO public.{table_name} VALUES (2, 'local-none');"
                ))
                .await?;
            require(
                receive_postgres_create_id(&mut output).await? == 2,
                "slot.stream.params=origin=none emitted a replicated-origin row",
            )
        }
        .await;
        let none_stop = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(none_capture, none_stop)?;
        drop_replication_slot(&client, &none_slot).await?;

        let mut any_config = settings.source_config(&publication, &any_slot, &table_name);
        any_config
            .slot_stream_params
            .insert("origin".into(), "any".into());
        let mut source = PostgresSource::new(
            &connector_name,
            any_config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 32,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
        let any_capture: TestResult = async {
            wait_for_active_slot(&client, &any_slot).await?;
            insert_with_replication_origin(&client, &table_name, &origin_name, 3).await?;
            require(
                receive_postgres_create_id(&mut output).await? == 3,
                "slot.stream.params=origin=any did not emit a replicated-origin row",
            )
        }
        .await;
        let any_stop = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(any_capture, any_stop)
    }
    .await;

    let _ = client
        .simple_query("SELECT pg_replication_origin_session_reset()")
        .await;
    let any_slot_cleanup = drop_replication_slot(&client, &any_slot).await;
    let primary_cleanup = cleanup(&client, &publication, &none_slot, &[&table_name]).await;
    let origin_cleanup = client
        .query_one("SELECT pg_replication_origin_drop($1)", &[&origin_name])
        .await;
    connection_task.abort();
    let _ = connection_task.await;
    capture_result?;
    any_slot_cleanup?;
    primary_cleanup?;
    origin_cleanup?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires external PostgreSQL logical replication and pg_stat_activity visibility"]
async fn applies_database_initial_statements_only_to_regular_connections() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_initial_{}", &suffix[..12]);
    let publication = format!("rustium_pg_initial_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_pg_initial_slot_{}", &suffix[..12]);
    let application_name = format!("rustium-initial-{}", &suffix[..12]);
    let connector_name = format!("postgresql-initial-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let capture_result: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, note TEXT NOT NULL); \
                 INSERT INTO public.{table_name} \
                 SELECT id, 'snapshot' FROM generate_series(1, 128) AS generated(id); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.database_initial_statements = vec![
            format!("SET application_name = '{application_name}'"),
            "SET statement_timeout = '30s'".into(),
        ];
        config.heartbeat_interval = Duration::from_millis(25);
        config.heartbeat_action_query = Some(format!(
            "INSERT INTO public.{table_name} VALUES \
             (1000, current_setting('application_name')) ON CONFLICT DO NOTHING"
        ));
        let mut source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 16,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source_with_output_capacity(source, None, 1);

        let source_capture: TestResult = async {
            wait_for_application_session(&client, &settings.database, &application_name).await?;
            loop {
                if receive(&mut output).await?.boundary == RecordBoundary::SnapshotComplete {
                    break;
                }
            }
            wait_for_active_slot(&client, &slot_name).await?;
            let replication_application_name = client
                .query_one(
                    "SELECT activity.application_name \
                     FROM pg_replication_slots AS slot \
                     JOIN pg_stat_activity AS activity ON activity.pid = slot.active_pid \
                     WHERE slot.slot_name = $1",
                    &[&slot_name],
                )
                .await?
                .get::<_, String>(0);
            require(
                replication_application_name != application_name,
                "database.initial.statements ran on the transaction-log replication connection",
            )?;

            loop {
                let record = receive(&mut output).await?;
                let Some(after) = record.event.as_ref().and_then(|event| event.after.as_ref())
                else {
                    continue;
                };
                if after.get("id") != Some(&DataValue::Int64(1000)) {
                    continue;
                }
                require(
                    after.get("note") == Some(&DataValue::String(application_name.clone())),
                    "heartbeat connection did not apply database.initial.statements",
                )?;
                break;
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(source_capture, stop_result)
    }
    .await;

    let primary_cleanup = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    let _ = connection_task.await;
    capture_result?;
    primary_cleanup
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ role administrator with logical replication enabled"]
async fn converts_debezium_interval_modes_across_postgresql_styles() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let role_name = format!("rustium_pg_interval_role_{}", &suffix[..8]);
    let role_password = format!("rustium-interval-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE ROLE {role_name} LOGIN REPLICATION PASSWORD '{role_password}'; \
                 GRANT USAGE ON SCHEMA public TO {role_name};"
            ))
            .await?;

        for (style_index, style) in ["postgres", "postgres_verbose", "sql_standard", "iso_8601"]
            .into_iter()
            .enumerate()
        {
            client
                .batch_execute(&format!(
                    "ALTER ROLE {role_name} SET IntervalStyle TO '{style}'"
                ))
                .await?;
            for (mode_index, mode) in ["numeric", "string"].into_iter().enumerate() {
                run_interval_mode_case(
                    &settings,
                    &client,
                    IntervalTestCase {
                        role_name: &role_name,
                        role_password: &role_password,
                        suffix: &suffix,
                        style_index,
                        mode_index,
                        mode,
                    },
                )
                .await?;
            }
        }
        Ok(())
    }
    .await;

    let role_cleanup = client
        .batch_execute(&format!(
            "DROP OWNED BY {role_name}; DROP ROLE IF EXISTS {role_name};"
        ))
        .await;
    connection_task.abort();
    if let Err(cleanup_error) = role_cleanup {
        if outcome.is_ok() {
            return Err(cleanup_error.into());
        }
        eprintln!("PostgreSQL interval role cleanup also failed: {cleanup_error}");
    }
    outcome
}

struct IntervalTestCase<'a> {
    role_name: &'a str,
    role_password: &'a str,
    suffix: &'a str,
    style_index: usize,
    mode_index: usize,
    mode: &'a str,
}

async fn run_interval_mode_case(
    settings: &TestSettings,
    client: &Client,
    case: IntervalTestCase<'_>,
) -> TestResult {
    let IntervalTestCase {
        role_name,
        role_password,
        suffix,
        style_index,
        mode_index,
        mode,
    } = case;
    let table_name = format!(
        "rustium_pg_interval_{style_index}_{mode_index}_{}",
        &suffix[..8]
    );
    let publication = format!(
        "rustium_interval_pub_{style_index}_{mode_index}_{}",
        &suffix[..8]
    );
    let slot_name = format!(
        "rustium_interval_slot_{style_index}_{mode_index}_{}",
        &suffix[..8]
    );
    let connector_name = format!(
        "postgresql-interval-{style_index}-{mode_index}-{}",
        &suffix[..8]
    );

    let outcome: TestResult = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, \
                    duration INTERVAL NOT NULL, \
                    durations INTERVAL[] NOT NULL\
                 ); \
                 GRANT SELECT ON public.{table_name} TO {role_name}; \
                 INSERT INTO public.{table_name} VALUES (\
                    1, \
                    '1 year 2 mons 3 days 04:05:06.789'::interval, \
                    ARRAY['1 day'::interval, '2 days'::interval]\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.username = role_name.into();
        config.password = role_password.into();
        config.interval_handling_mode = mode.into();
        let mut source = PostgresSource::new(
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
            let snapshot = receive(&mut output).await?;
            require(
                snapshot
                    .event
                    .as_ref()
                    .is_some_and(|event| interval_event_matches(event, mode, 1)),
                "PostgreSQL interval snapshot conversion is incorrect",
            )?;
            let completion = receive(&mut output).await?;
            require(
                completion.boundary == RecordBoundary::SnapshotComplete,
                "PostgreSQL interval snapshot did not complete",
            )?;
            wait_for_active_slot(client, &slot_name).await?;
            client
                .execute(
                    &format!(
                        "INSERT INTO public.{table_name} VALUES (\
                            2, \
                            '1 year 2 mons 3 days 04:05:06.789'::interval, \
                            ARRAY['1 day'::interval, '2 days'::interval]\
                         )"
                    ),
                    &[],
                )
                .await?;
            loop {
                let record = receive(&mut output).await?;
                let Some(event) = record.event else {
                    continue;
                };
                require(
                    interval_event_matches(&event, mode, 2),
                    "PostgreSQL interval WAL conversion is incorrect",
                )?;
                break;
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(client, &publication, &slot_name, &[&table_name]).await;
    match (outcome, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

fn interval_event_matches(event: &rustium_core::ChangeEvent, mode: &str, id: i64) -> bool {
    let Some(row) = event.after.as_ref() else {
        return false;
    };
    let expected_duration = if mode == "numeric" {
        DataValue::Int64(37_091_106_789_000)
    } else {
        DataValue::String("P1Y2M3DT4H5M6.789S".into())
    };
    let expected_durations = if mode == "numeric" {
        DataValue::Array(vec![
            DataValue::Int64(86_400_000_000),
            DataValue::Int64(172_800_000_000),
        ])
    } else {
        DataValue::Array(vec![
            DataValue::String("P0Y0M1DT0H0M0S".into()),
            DataValue::String("P0Y0M2DT0H0M0S".into()),
        ])
    };
    event.operation
        == if id == 1 {
            Operation::Read
        } else {
            Operation::Create
        }
        && row.get("id") == Some(&DataValue::Int64(id))
        && row.get("duration") == Some(&expected_duration)
        && row.get("durations") == Some(&expected_durations)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with installed extension types"]
async fn keeps_installed_extension_types_identical_across_snapshot_and_wal() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_extensions_{}", &suffix[..12]);
    let publication = format!("rustium_ext_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_ext_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-extensions-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        let require_all_extension_types = std::env::var(
            "RUSTIUM_POSTGRES_REQUIRE_EXTENSION_TYPES",
        )
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
        let vector_schema = installed_extension_schema(&client, "vector").await?;
        let postgis_schema = installed_extension_schema(&client, "postgis").await?;
        if require_all_extension_types {
            require(
                vector_schema.is_some() && postgis_schema.is_some(),
                "PostgreSQL extension gate requires both vector and postgis",
            )?;
        }
        if vector_schema.is_none() && postgis_schema.is_none() {
            eprintln!(
                "PostgreSQL extension gate skipped: neither vector nor postgis is installed"
            );
            return Ok(());
        }

        let vector_name = vector_schema
            .as_deref()
            .map(|schema| qualified_name(schema, "vector"));
        let halfvec_name = vector_schema
            .as_deref()
            .map(|schema| qualified_name(schema, "halfvec"));
        let sparsevec_name = vector_schema
            .as_deref()
            .map(|schema| qualified_name(schema, "sparsevec"));
        let geometry_name = postgis_schema
            .as_deref()
            .map(|schema| qualified_name(schema, "geometry"));
        let geography_name = postgis_schema
            .as_deref()
            .map(|schema| qualified_name(schema, "geography"));
        let vector_type = type_name_exists(&client, vector_name.as_deref()).await?;
        let halfvec_type = type_name_exists(&client, halfvec_name.as_deref()).await?;
        let sparsevec_type = type_name_exists(&client, sparsevec_name.as_deref()).await?;
        let geometry_type = type_name_exists(&client, geometry_name.as_deref()).await?;
        let geography_type = type_name_exists(&client, geography_name.as_deref()).await?;
        if require_all_extension_types {
            require(
                vector_type
                    && halfvec_type
                    && sparsevec_type
                    && geometry_type
                    && geography_type,
                "PostgreSQL extension gate requires vector, halfvec, sparsevec, geometry, and geography",
            )?;
        }
        require(
            vector_type || halfvec_type || sparsevec_type || geometry_type || geography_type,
            "installed PostgreSQL extension has no discoverable test type",
        )?;

        let mut column_names = vec!["id"];
        let mut columns = vec!["id BIGINT PRIMARY KEY".to_string()];
        let mut values = vec!["1".to_string()];
        if vector_type {
            let vector_name = vector_name.as_deref().expect("vector type name is present");
            column_names.push("vector_value");
            columns.push(format!("vector_value {vector_name}(3) NOT NULL"));
            values.push(format!("'[1, 2.5, -3]'::{vector_name}"));
        }
        if halfvec_type {
            let halfvec_name = halfvec_name
                .as_deref()
                .expect("halfvec type name is present");
            column_names.push("halfvec_value");
            columns.push(format!("halfvec_value {halfvec_name}(3) NOT NULL"));
            values.push(format!("'[1, 2.5, -3]'::{halfvec_name}"));
        }
        if sparsevec_type {
            let sparsevec_name = sparsevec_name
                .as_deref()
                .expect("sparsevec type name is present");
            column_names.push("sparsevec_value");
            columns.push(format!("sparsevec_value {sparsevec_name}(12) NOT NULL"));
            values.push(format!("'{{1:1.5, 9:-2}}/12'::{sparsevec_name}"));
        }
        if geometry_type {
            let geometry_name = geometry_name
                .as_deref()
                .expect("geometry type name is present");
            let postgis_schema = postgis_schema
                .as_deref()
                .expect("postgis extension schema is present");
            column_names.push("geometry_value");
            columns.push(format!(
                "geometry_value {geometry_name}(Point,4326) NOT NULL"
            ));
            values.push(format!(
                "{}.ST_GeomFromText('POINT(1 2)', 4326)",
                quote_identifier(postgis_schema)
            ));
        }
        if geography_type {
            let geography_name = geography_name
                .as_deref()
                .expect("geography type name is present");
            let postgis_schema = postgis_schema
                .as_deref()
                .expect("postgis extension schema is present");
            column_names.push("geography_value");
            columns.push(format!(
                "geography_value {geography_name}(Point,4326) NOT NULL"
            ));
            values.push(format!(
                "{}.ST_GeogFromText('SRID=4326;POINT(1 2)')",
                quote_identifier(postgis_schema)
            ));
        }
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} ({}); \
                 INSERT INTO public.{table_name} ({}) VALUES ({}); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};",
                columns.join(", "),
                column_names.join(", "),
                values.join(", ")
            ))
            .await?;

        let config = settings.source_config(&publication, &slot_name, &table_name);
        let mut source = PostgresSource::new(
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
        let capture_result: TestResult<(Row, Row)> = async {
            let mut snapshot_row = None;
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::SnapshotComplete {
                    break;
                }
                require(
                    record.boundary == RecordBoundary::Data,
                    "unexpected boundary in PostgreSQL extension snapshot",
                )?;
                let event = record
                    .event
                    .ok_or_else(|| test_error("extension snapshot record has no event"))?;
                require(
                    event.operation == Operation::Read,
                    "extension snapshot event is not a read",
                )?;
                snapshot_row = event.after;
            }
            let snapshot_row = snapshot_row
                .ok_or_else(|| test_error("extension snapshot row was not emitted"))?;
            wait_for_active_slot(&client, &slot_name).await?;
            let mut update_values = vec!["2".to_string()];
            if vector_type {
                let vector_name = vector_name.as_deref().expect("vector type name is present");
                update_values.push(format!("'[1, 2.5, -3]'::{vector_name}"));
            }
            if halfvec_type {
                let halfvec_name = halfvec_name
                    .as_deref()
                    .expect("halfvec type name is present");
                update_values.push(format!("'[1, 2.5, -3]'::{halfvec_name}"));
            }
            if sparsevec_type {
                let sparsevec_name = sparsevec_name
                    .as_deref()
                    .expect("sparsevec type name is present");
                update_values.push(format!("'{{1:1.5, 9:-2}}/12'::{sparsevec_name}"));
            }
            if geometry_type {
                let postgis_schema = postgis_schema
                    .as_deref()
                    .expect("postgis extension schema is present");
                update_values.push(format!(
                    "{}.ST_GeomFromText('POINT(1 2)', 4326)",
                    quote_identifier(postgis_schema)
                ));
            }
            if geography_type {
                let postgis_schema = postgis_schema
                    .as_deref()
                    .expect("postgis extension schema is present");
                update_values.push(format!(
                    "{}.ST_GeogFromText('SRID=4326;POINT(1 2)')",
                    quote_identifier(postgis_schema)
                ));
            }
            client
                .execute(
                    &format!(
                        "INSERT INTO public.{table_name} ({}) VALUES ({})",
                        column_names.join(", "),
                        update_values.join(", ")
                    ),
                    &[],
                )
                .await?;
            let streaming_row = loop {
                let record = receive(&mut output).await?;
                if record.boundary != RecordBoundary::Data {
                    continue;
                }
                let event = record
                    .event
                    .ok_or_else(|| test_error("extension streaming record has no event"))?;
                if event.operation != Operation::Create {
                    continue;
                }
                break event
                    .after
                    .ok_or_else(|| test_error("extension streaming row has no after value"))?;
            };
            Ok((snapshot_row, streaming_row))
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let (mut snapshot_row, mut streaming_row) =
            combine_capture_and_stop(capture_result, stop_result)?;
        require(
            snapshot_row.shift_remove("id") == Some(DataValue::Int64(1)),
            "extension snapshot id is incorrect",
        )?;
        require(
            streaming_row.shift_remove("id") == Some(DataValue::Int64(2)),
            "extension streaming id is incorrect",
        )?;
        require(
            snapshot_row == streaming_row,
            "installed PostgreSQL extension conversion differs between snapshot and WAL",
        )?;
        if vector_type || halfvec_type {
            for field in ["vector_value", "halfvec_value"] {
                if snapshot_row.contains_key(field) {
                    require(
                        snapshot_row.get(field)
                            == Some(&DataValue::Array(vec![
                                DataValue::Float64(1.0),
                                DataValue::Float64(2.5),
                                DataValue::Float64(-3.0),
                            ])),
                        "dense pgvector conversion failed",
                    )?;
                }
            }
        }
        if sparsevec_type {
            require(
                matches!(snapshot_row.get("sparsevec_value"), Some(DataValue::Map(_))),
                "sparse pgvector conversion failed",
            )?;
        }
        for field in ["geometry_value", "geography_value"] {
            if snapshot_row.contains_key(field) {
                require(
                    matches!(snapshot_row.get(field), Some(DataValue::Bytes(bytes)) if !bytes.is_empty()),
                    "PostGIS EWKB conversion failed",
                )?;
            }
        }
        Ok(())
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL extension test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn reconnects_after_replication_backend_termination() -> TestResult {
    let settings = TestSettings::from_env()?;
    let soak_cycles = reconnect_soak_cycles()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_reconnect_{}", &suffix[..12]);
    let publication = format!("rustium_reconnect_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_reconnect_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-reconnect-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 INSERT INTO public.{table_name} VALUES (1, 'before'); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;
        let config = settings.source_config(&publication, &slot_name, &table_name);
        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        )
        .with_retry_policy(RetryPolicy {
            max_retries: 20,
            initial_delay: Duration::from_millis(25),
            max_delay: Duration::from_millis(250),
        });
        source.validate().await?;
        let (mut output, cancellation, source_task) =
            start_source_with_output_capacity(source, None, 1);
        let snapshot_checkpoint = loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                break Checkpoint {
                    schema_version: CHECKPOINT_SCHEMA_VERSION,
                    connector_name: connector_name.clone(),
                    generation: uuid::Uuid::new_v4(),
                    source_position: record.position,
                    snapshot_completed: true,
                    config_fingerprint: "postgresql-when-needed-recovery-test".into(),
                    updated_at: SystemTime::now(),
                    connector_state: record.connector_state,
                };
            }
        };
        stop_source(cancellation, source_task).await?;

        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
            .await?;

        let mut recovery_source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::WhenNeeded,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        )
        .with_retry_policy(RetryPolicy {
            max_retries: 20,
            initial_delay: Duration::from_millis(25),
            max_delay: Duration::from_millis(250),
        });
        recovery_source.validate().await?;
        let (mut output, cancellation, source_task) = start_source_with_output_capacity(
            recovery_source,
            Some(snapshot_checkpoint),
            1,
        );
        let capture_result: TestResult = async {
            let mut recovery_snapshot_rows = 0;
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::SnapshotComplete {
                    break;
                }
                require(
                    record.event.as_ref().is_some_and(|event| {
                        event.operation == Operation::Read && event.source.snapshot
                    }),
                    "PostgreSQL when_needed recovery emitted a non-snapshot record",
                )?;
                recovery_snapshot_rows += 1;
            }
            require(
                recovery_snapshot_rows == 1,
                "PostgreSQL when_needed recovery did not resnapshot the table",
            )?;
            for cycle in 0..soak_cycles {
                let first_id = 2_i64 + i64::from(cycle) * 3;
                let expected_ids = [first_id, first_id + 1, first_id + 2];
                let original_pid = wait_for_active_pid(&client, &slot_name).await?;
                client
                    .batch_execute(&format!(
                        "INSERT INTO public.{table_name} VALUES \
                            ({first_id}, 'backpressure-{cycle}-a'), \
                            ({}, 'backpressure-{cycle}-b');",
                        first_id + 1
                    ))
                    .await?;
                wait_for_output_backpressure(&output).await?;
                require(
                    client
                        .query_one("SELECT pg_terminate_backend($1)", &[&original_pid])
                        .await?
                        .get::<_, bool>(0),
                    "PostgreSQL did not terminate the replication backend",
                )?;
                client
                    .execute(
                        &format!(
                            "INSERT INTO public.{table_name} VALUES ({}, 'after-reconnect-{cycle}')",
                            first_id + 2
                        ),
                        &[],
                    )
                    .await?;

                let mut first_seen = Vec::new();
                let mut seen = BTreeMap::<i64, usize>::new();
                loop {
                    let record = receive(&mut output).await?;
                    if record.boundary == RecordBoundary::Data {
                        let event = record
                            .event
                            .ok_or_else(|| test_error("reconnect record has no event"))?;
                        if event.operation == Operation::Create
                            && let Some(DataValue::Int64(id)) =
                                event.after.as_ref().and_then(|row| row.get("id"))
                            && expected_ids.contains(id)
                        {
                            if !seen.contains_key(id) {
                                first_seen.push(*id);
                            }
                            *seen.entry(*id).or_default() += 1;
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
                    "PostgreSQL reconnect soak did not preserve first-seen source order",
                )?;
                let reconnected_pid =
                    wait_for_different_active_pid(&client, &slot_name, original_pid).await?;
                require(
                    reconnected_pid != original_pid,
                    "PostgreSQL replication backend PID did not change after recovery",
                )?;
            }
            Ok(())
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL reconnect test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn rejects_checkpoint_resume_after_replication_slot_loss() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_slot_loss_{}", &suffix[..12]);
    let publication = format!("rustium_slot_loss_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_slot_loss_{}", &suffix[..12]);
    let connector_name = format!("postgresql-slot-loss-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 INSERT INTO public.{table_name} VALUES (1, 'checkpointed'); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;
        let config = settings.source_config(&publication, &slot_name, &table_name);
        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
        let (position, connector_state) = loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                break (
                    record.position,
                    record.connector_state.ok_or_else(|| {
                        test_error("slot-loss snapshot completion has no connector state")
                    })?,
                );
            }
        };
        stop_source(cancellation, source_task).await?;

        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
            .await?;
        client
            .execute(
                &format!("INSERT INTO public.{table_name} VALUES (2, 'would-be-lost')"),
                &[],
            )
            .await?;
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: position,
            snapshot_completed: true,
            config_fingerprint: "postgresql-slot-loss-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(connector_state),
        };
        let mut resumed_source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        resumed_source.validate().await?;
        let (_output, _cancellation, source_task) = start_source(resumed_source, Some(checkpoint));
        let source_result = tokio::time::timeout(Duration::from_secs(10), source_task)
            .await
            .map_err(|_| test_error("slot-loss resume did not fail promptly"))??;
        let error = source_result
            .expect_err("PostgreSQL checkpoint resume unexpectedly recreated a missing slot");
        let message = error.to_string();
        require(
            message.contains("replication slot")
                && message.contains("is missing")
                && message.contains("reset the checkpoint"),
            "PostgreSQL slot-loss failure did not explain the required recovery action",
        )?;
        let slot_exists = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot_name],
            )
            .await?
            .get::<_, bool>(0);
        require(
            !slot_exists,
            "PostgreSQL slot-loss guard recreated the missing replication slot",
        )
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL slot-loss test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ superuser with WAL retention controls"]
async fn rejects_checkpoint_resume_after_wal_retention_invalidation() -> TestResult {
    if !std::env::var("RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST")
        .is_ok_and(|value| value.eq_ignore_ascii_case("true"))
    {
        eprintln!(
            "PostgreSQL WAL retention gate skipped: set RUSTIUM_POSTGRES_RUN_WAL_RETENTION_TEST=true to allow temporary ALTER SYSTEM"
        );
        return Ok(());
    }
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_wal_loss_{}", &suffix[..12]);
    let publication = format!("rustium_wal_loss_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_wal_loss_{}", &suffix[..12]);
    let connector_name = format!("postgresql-wal-loss-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;
    let original_retention = client
        .query_one("SHOW max_slot_wal_keep_size", &[])
        .await?
        .get::<_, String>(0);
    let configure_result = set_max_slot_wal_keep_size(&client, "1MB").await;
    if let Err(error) = configure_result {
        let _ = set_max_slot_wal_keep_size(&client, &original_retention).await;
        connection_task.abort();
        return Err(error);
    }

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (id BIGINT PRIMARY KEY, value TEXT NOT NULL); \
                 INSERT INTO public.{table_name} VALUES (1, 'checkpointed'); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;
        let config = settings.source_config(&publication, &slot_name, &table_name);
        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);
        let (position, connector_state) = loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                break (
                    record.position,
                    record.connector_state.ok_or_else(|| {
                        test_error("WAL-loss snapshot completion has no connector state")
                    })?,
                );
            }
        };
        stop_source(cancellation, source_task).await?;
        wait_for_inactive_slot(&client, &slot_name).await?;

        let mut wal_status = replication_slot_wal_status(&client, &slot_name).await?;
        for round in 0_i64..6 {
            let first_id = 2 + round * 48;
            let last_id = first_id + 47;
            client
                .execute(
                    &format!(
                        "INSERT INTO public.{table_name} (id, value) \
                         SELECT id, repeat('wal-retention-', 100000) \
                         FROM generate_series({first_id}, {last_id}) AS ids(id)"
                    ),
                    &[],
                )
                .await?;
            client.query_one("SELECT pg_switch_wal()", &[]).await?;
            client.execute("CHECKPOINT", &[]).await?;
            wal_status = replication_slot_wal_status(&client, &slot_name).await?;
            if wal_status == "lost" {
                break;
            }
        }
        require(
            wal_status == "lost",
            &format!(
                "PostgreSQL WAL retention fixture did not invalidate the replication slot; final wal_status={wal_status:?}"
            ),
        )?;

        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: position,
            snapshot_completed: true,
            config_fingerprint: "postgresql-wal-loss-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(connector_state),
        };
        let mut resumed_source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Initial,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        resumed_source.validate().await?;
        let (_output, _cancellation, source_task) = start_source(resumed_source, Some(checkpoint));
        let source_result = tokio::time::timeout(Duration::from_secs(10), source_task)
            .await
            .map_err(|_| test_error("WAL-loss resume did not fail promptly"))??;
        let error = match source_result {
            Err(error) => error,
            Ok(()) => return Err(test_error("WAL-loss resume unexpectedly continued")),
        };
        require(
            error.to_string().contains("wal_status=\"lost\"")
                && error.to_string().contains("reset the checkpoint"),
            "WAL-loss failure did not explain the required recovery action",
        )
    }
    .await;

    let restore_result = set_max_slot_wal_keep_size(&client, &original_retention).await;
    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    restore_result?;
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL WAL-loss test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn runs_incremental_snapshot_from_file_signal() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_file_{}", &suffix[..12]);
    let signal_table = format!("rustium_file_signal_{}", &suffix[..12]);
    let publication = format!("rustium_file_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_file_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-file-{}", &suffix[..12]);
    let signal_directory = tempfile::tempdir()?;
    let signal_path = signal_directory.path().join("signals.jsonl");
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES \
                    (1, 'one'), (2, 'two'), (3, 'three'); \
                 CREATE TABLE public.{signal_table} (\
                    id VARCHAR(64), type VARCHAR(32), data VARCHAR(2048)\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE \
                    public.{table_name}, public.{signal_table};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.signal_enabled_channels = vec!["file".into()];
        config.signal_file = signal_path.to_string_lossy().into_owned();
        config.signal_poll_interval = Duration::from_millis(20);
        config.incremental_snapshot_chunk_size = 2;
        let mut source = PostgresSource::new(
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

        wait_for_active_slot(&client, &slot_name).await?;
        let signal = serde_json::json!({
            "id": format!("file-{suffix}"),
            "type": "execute-snapshot",
            "data": {
                "type": "incremental",
                "data-collections": [format!(r"public\.{table_name}")]
            }
        });
        std::fs::write(&signal_path, format!("{signal}\n"))?;

        let capture_result: TestResult<Vec<i64>> = async {
            let mut ids = Vec::new();
            let mut saw_external_checkpoint = false;
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::TransactionCommit
                    && ids.is_empty()
                    && record
                        .connector_state
                        .as_ref()
                        .and_then(|state| state.payload.get("incremental_snapshot"))
                        .is_some()
                {
                    saw_external_checkpoint = true;
                }
                if record.boundary == RecordBoundary::Data {
                    let event = record.event.ok_or_else(|| {
                        test_error("file incremental snapshot record has no event")
                    })?;
                    require_incremental_event(&event, &table_name)?;
                    ids.push(row_id(event.after.as_ref())?);
                }
                if ids.len() == 3
                    && record
                        .connector_state
                        .as_ref()
                        .is_some_and(|state| state.payload.get("incremental_snapshot").is_none())
                {
                    require(
                        saw_external_checkpoint,
                        "file signal progress was not checkpointed at the safe source position",
                    )?;
                    break Ok(ids);
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let ids = combine_capture_and_stop(capture_result, stop_result)?;
        require(
            ids == [1, 2, 3],
            "file-signaled snapshot rows are incorrect",
        )?;
        require(
            std::fs::read_to_string(&signal_path)?.is_empty(),
            "file signal was not cleared after polling",
        )?;
        let signal_counts = client
            .query_one(
                &format!(
                    "SELECT \
                         count(*) FILTER (WHERE type = 'execute-snapshot'), \
                         count(*) FILTER (WHERE type LIKE 'snapshot-window-%') \
                     FROM public.{signal_table}"
                ),
                &[],
            )
            .await?;
        require(
            signal_counts.get::<_, i64>(0) == 0,
            "file signal was unexpectedly written to the source signal table",
        )?;
        require(
            signal_counts.get::<_, i64>(1) == 4,
            "file-signaled snapshot did not emit two bounded watermark pairs",
        )
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    connection_task.abort();
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL file signal cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn runs_read_only_external_snapshots_without_signal_table() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_ro_file_{}", &suffix[..12]);
    let publication = format!("rustium_ro_file_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_ro_file_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-ro-file-{}", &suffix[..12]);
    let signal_directory = tempfile::tempdir()?;
    let signal_path = signal_directory.path().join("signals.jsonl");
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES \
                    (1, 'one'), (2, 'two'), (3, 'three'); \
                 CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.signal_enabled_channels = vec!["file".into(), "in-process".into()];
        config.signal_file = signal_path.to_string_lossy().into_owned();
        config.signal_poll_interval = Duration::from_millis(20);
        config.incremental_snapshot_chunk_size = 2;
        config.read_only = true;
        let mut source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task, signal_sender) =
            start_source_with_signals(source, None);

        wait_for_active_slot(&client, &slot_name).await?;
        let signal = serde_json::json!({
            "id": format!("read-only-file-{suffix}"),
            "type": "execute-snapshot",
            "data": {
                "type": "incremental",
                "data-collections": [format!(r"public\.{table_name}")]
            }
        });
        std::fs::write(&signal_path, format!("{signal}\n"))?;

        loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::TransactionCommit
                && record
                    .connector_state
                    .as_ref()
                    .and_then(|state| state.payload.get("incremental_snapshot"))
                    .is_some()
            {
                break;
            }
        }
        client
            .execute(
                &format!("UPDATE public.{table_name} SET value = 'one-updated' WHERE id = 1"),
                &[],
            )
            .await?;

        let capture_result: TestResult<(Vec<i64>, Vec<i64>)> = async {
            let mut file_ids = Vec::new();
            let mut saw_streamed_update = false;
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event {
                    if event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                    {
                        file_ids.push(row_id(event.after.as_ref())?);
                    } else if event.operation == Operation::Update
                        && row_id(event.after.as_ref())? == 1
                    {
                        saw_streamed_update = true;
                    }
                }
                if file_ids.len() == 2
                    && record
                        .connector_state
                        .as_ref()
                        .is_some_and(|state| state.payload.get("incremental_snapshot").is_none())
                {
                    require(
                        saw_streamed_update,
                        "read-only file snapshot did not stream the concurrent update",
                    )?;
                    break;
                }
            }
            require(
                file_ids == [2, 3],
                "read-only file snapshot did not deduplicate the updated key",
            )?;
            require(
                std::fs::read_to_string(&signal_path)?.is_empty(),
                "read-only file signal was not cleared after polling",
            )?;

            signal_sender
                .send(rustium_core::SignalRecord::new(
                    format!("read-only-in-process-{suffix}"),
                    "execute-snapshot",
                    serde_json::json!({
                        "type": "incremental",
                        "data-collections": [format!(r"public\.{table_name}")]
                    }),
                ))
                .await?;
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::TransactionCommit
                    && record
                        .connector_state
                        .as_ref()
                        .and_then(|state| state.payload.get("incremental_snapshot"))
                        .is_some()
                {
                    break;
                }
            }
            client
                .execute(
                    &format!("UPDATE public.{table_name} SET value = 'two-updated' WHERE id = 2"),
                    &[],
                )
                .await?;

            let mut in_process_ids = Vec::new();
            let mut saw_in_process_update = false;
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event {
                    if event
                        .source
                        .attributes
                        .get("rustium.snapshot.kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("incremental")
                    {
                        in_process_ids.push(row_id(event.after.as_ref())?);
                    } else if event.operation == Operation::Update
                        && row_id(event.after.as_ref())? == 2
                    {
                        saw_in_process_update = true;
                    }
                }
                if in_process_ids.len() == 2
                    && record
                        .connector_state
                        .as_ref()
                        .is_some_and(|state| state.payload.get("incremental_snapshot").is_none())
                {
                    require(
                        saw_in_process_update,
                        "in-process snapshot did not stream the concurrent update",
                    )?;
                    break;
                }
            }
            Ok((file_ids, in_process_ids))
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let (_, in_process_ids) = combine_capture_and_stop(capture_result, stop_result)?;
        require(
            in_process_ids == [1, 3],
            "read-only in-process snapshot did not deduplicate the updated key",
        )
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &[&table_name]).await;
    connection_task.abort();
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL read-only file signal cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn resumes_signal_incremental_snapshot_from_chunk_checkpoint() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_incremental_{}", &suffix[..12]);
    let signal_table = format!("rustium_pg_signal_{}", &suffix[..12]);
    let publication = format!("rustium_inc_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_inc_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-incremental-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} \
                    SELECT id, 'value-' || id::text FROM generate_series(1, 5) AS id; \
                 CREATE TABLE public.{signal_table} (\
                    id VARCHAR(64), type VARCHAR(32), data VARCHAR(2048)\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE \
                    public.{table_name}, public.{signal_table};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.incremental_snapshot_chunk_size = 2;
        let mut source = PostgresSource::new(
            &connector_name,
            config.clone(),
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);

        wait_for_active_slot(&client, &slot_name).await?;
        insert_execute_snapshot_signal(&client, &signal_table, &table_name).await?;
        let first_capture: TestResult<(SourcePosition, ConnectorStateEnvelope, Vec<i64>)> = async {
            let mut ids = Vec::new();
            loop {
                let record = receive(&mut output).await?;
                if record.boundary == RecordBoundary::Data {
                    let event = record.event.ok_or_else(|| {
                        test_error("incremental snapshot data record has no event")
                    })?;
                    require_incremental_event(&event, &table_name)?;
                    ids.push(row_id(event.after.as_ref())?);
                }
                if record.boundary == RecordBoundary::TransactionCommit && ids.len() >= 2 {
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("incremental snapshot chunk commit has no connector state")
                    })?;
                    require(
                        state.payload.get("incremental_snapshot").is_some(),
                        "incremental snapshot progress was not checkpointed",
                    )?;
                    break Ok((record.position, state, ids));
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let (position, connector_state, first_ids) =
            combine_capture_and_stop(first_capture, stop_result)?;
        require(
            first_ids == [1, 2],
            "first incremental snapshot chunk is incorrect",
        )?;

        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: position,
            snapshot_completed: true,
            config_fingerprint: "postgresql-incremental-test".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(connector_state),
        };
        let mut resumed_source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        resumed_source.validate().await?;
        let (mut resumed_output, cancellation, source_task) =
            start_source(resumed_source, Some(checkpoint));
        let resumed_capture: TestResult<Vec<i64>> = async {
            let mut ids = Vec::new();
            loop {
                let record = receive(&mut resumed_output).await?;
                if record.boundary == RecordBoundary::Data {
                    let event = record.event.ok_or_else(|| {
                        test_error("resumed incremental snapshot record has no event")
                    })?;
                    require_incremental_event(&event, &table_name)?;
                    ids.push(row_id(event.after.as_ref())?);
                }
                if record.boundary == RecordBoundary::TransactionCommit && ids.len() >= 3 {
                    let state = record.connector_state.ok_or_else(|| {
                        test_error("completed incremental snapshot has no connector state")
                    })?;
                    require(
                        state.version == 5,
                        "PostgreSQL connector state did not upgrade to version 5",
                    )?;
                    require(
                        state.payload.get("incremental_snapshot").is_none(),
                        "completed incremental snapshot retained active progress",
                    )?;
                    break Ok(ids);
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let resumed_ids = combine_capture_and_stop(resumed_capture, stop_result)?;
        require(
            resumed_ids == [3, 4, 5],
            "incremental snapshot did not resume at the next chunk",
        )
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL incremental snapshot cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn controls_and_deduplicates_filtered_incremental_snapshot() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_control_{}", &suffix[..12]);
    let signal_table = format!("rustium_pg_control_signal_{}", &suffix[..12]);
    let trigger_function = format!("rustium_pg_control_fn_{}", &suffix[..12]);
    let publication = format!("rustium_control_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_control_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-control-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                r#"CREATE TABLE public.{table_name} (
                       id BIGINT PRIMARY KEY, value TEXT NOT NULL
                   );
                   INSERT INTO public.{table_name}
                       SELECT id, 'value-' || id::text FROM generate_series(1, 10) AS id;
                   CREATE TABLE public.{signal_table} (
                       id VARCHAR(128), type VARCHAR(32), data VARCHAR(4096)
                   );
                   CREATE FUNCTION public.{trigger_function}() RETURNS trigger AS $function$
                   BEGIN
                       IF NEW.type = 'snapshot-window-open' AND NEW.id LIKE '%-1-open' THEN
                           UPDATE public.{table_name}
                           SET value = value || '-updated'
                           WHERE id = 2;
                       ELSIF NEW.type = 'snapshot-window-close' AND NEW.id LIKE '%-1-close' THEN
                           INSERT INTO public.{signal_table} (id, type, data)
                           VALUES ('pause-from-trigger', 'pause-snapshot', '{{"type":"incremental"}}');
                       ELSIF NEW.type = 'snapshot-window-close' AND NEW.id LIKE '%-2-close' THEN
                           INSERT INTO public.{signal_table} (id, type, data)
                           VALUES (
                               'stop-from-trigger',
                               'stop-snapshot',
                               '{{"type":"incremental","data-collections":["public\\.{table_name}"]}}'
                           );
                       END IF;
                       RETURN NEW;
                   END;
                   $function$ LANGUAGE plpgsql;
                   CREATE TRIGGER rustium_control_signal_trigger
                   AFTER INSERT ON public.{signal_table}
                   FOR EACH ROW EXECUTE FUNCTION public.{trigger_function}();
                   CREATE PUBLICATION {publication} FOR TABLE
                       public.{table_name}, public.{signal_table};"#
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.incremental_snapshot_chunk_size = 2;
        let mut source = PostgresSource::new(
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

        wait_for_active_slot(&client, &slot_name).await?;
        let execute_data = serde_json::json!({
            "type": "incremental",
            "data-collections": [format!(r"public\.{table_name}")],
            "additional-conditions": [{
                "data-collection": format!(r"public\.{table_name}"),
                "filter": "id % 2 = 0"
            }]
        })
        .to_string();
        insert_signal(
            &client,
            &signal_table,
            "controlled-snapshot",
            "execute-snapshot",
            &execute_data,
        )
        .await?;

        let capture_result: TestResult = async {
            let mut snapshot_ids = Vec::new();
            let mut streaming_update_ids = Vec::new();
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event {
                    require(
                        event.source.table.as_deref() == Some(table_name.as_str()),
                        "PostgreSQL control signal table leaked as a business event",
                    )?;
                    if event.source.attributes.get("rustium.snapshot.kind")
                        == Some(&"incremental".into())
                    {
                        snapshot_ids.push(row_id(event.after.as_ref())?);
                    } else {
                        require(
                            event.operation == Operation::Update,
                            "unexpected streaming event during incremental snapshot",
                        )?;
                        streaming_update_ids.push(row_id(event.after.as_ref())?);
                    }
                }
                if record.boundary == RecordBoundary::TransactionCommit
                    && record.connector_state.as_ref().is_some_and(|state| {
                        state
                            .payload
                            .pointer("/incremental_snapshot/paused")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                    })
                {
                    break;
                }
            }
            require(
                snapshot_ids == [4],
                "first filtered chunk was not deduplicated against its WAL update",
            )?;
            require(
                streaming_update_ids == [2],
                "concurrent WAL update was not emitted exactly once",
            )?;

            require(
                tokio::time::timeout(Duration::from_millis(300), output.recv())
                    .await
                    .is_err(),
                "paused PostgreSQL incremental snapshot continued reading chunks",
            )?;
            insert_signal(
                &client,
                &signal_table,
                "resume-from-test",
                "resume-snapshot",
                r#"{"type":"incremental"}"#,
            )
            .await?;

            let mut resumed_ids = Vec::new();
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event {
                    require_incremental_event(&event, &table_name)?;
                    resumed_ids.push(row_id(event.after.as_ref())?);
                }
                if record.boundary == RecordBoundary::TransactionCommit
                    && record.connector_state.as_ref().is_some_and(|state| {
                        state.version == 5
                            && state.payload.get("incremental_snapshot").is_none()
                    })
                {
                    break;
                }
            }
            require(
                resumed_ids == [6, 8],
                "resume or scoped stop produced the wrong filtered rows",
            )?;
            require(
                tokio::time::timeout(Duration::from_millis(300), output.recv())
                    .await
                    .is_err(),
                "stopped PostgreSQL incremental snapshot emitted another chunk",
            )
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let function_cleanup = client
        .batch_execute(&format!(
            "DROP FUNCTION IF EXISTS public.{trigger_function}() CASCADE"
        ))
        .await;
    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    connection_task.abort();

    if let Err(cleanup_error) = function_cleanup {
        if outcome.is_ok() {
            return Err(cleanup_error.into());
        }
        eprintln!("PostgreSQL control function cleanup also failed: {cleanup_error}");
    }
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL control test cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn runs_incremental_snapshot_with_read_only_table_permissions() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_readonly_{}", &suffix[..12]);
    let signal_table = format!("rustium_pg_readonly_signal_{}", &suffix[..12]);
    let publication = format!("rustium_readonly_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_readonly_slot_{}", &suffix[..12]);
    let role_name = format!("rustium_readonly_{}", &suffix[..12]);
    let role_password = format!("RustiumReadOnly{}", &suffix[..16]);
    let connector_name = format!("postgresql-readonly-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id BIGINT PRIMARY KEY, value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} \
                    SELECT id, 'value-' || id::text FROM generate_series(1, 5) AS id; \
                 CREATE TABLE public.{signal_table} (\
                    id VARCHAR(128), type VARCHAR(32), data VARCHAR(4096)\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE \
                    public.{table_name}, public.{signal_table}; \
                 CREATE ROLE {role_name} WITH LOGIN REPLICATION PASSWORD '{role_password}'; \
                 GRANT USAGE ON SCHEMA public TO {role_name}; \
                 GRANT SELECT ON TABLE public.{table_name}, public.{signal_table} TO {role_name};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.username = role_name.clone();
        config.password = role_password.clone();
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.incremental_snapshot_chunk_size = 2;
        config.read_only = true;
        let mut source = PostgresSource::new(
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

        wait_for_active_slot(&client, &slot_name).await?;
        let (mut concurrent_client, concurrent_connection_task) = connect(&settings).await?;
        let concurrent_transaction = concurrent_client.transaction().await?;
        concurrent_transaction
            .execute(
                &format!(
                    "UPDATE public.{table_name} SET value = value || '-concurrent' WHERE id = 2"
                ),
                &[],
            )
            .await?;
        insert_execute_snapshot_signal(&client, &signal_table, &table_name).await?;
        let capture_result: TestResult = async {
            loop {
                let record = receive(&mut output).await?;
                require(
                    record.event.is_none(),
                    "read-only window closed before its in-progress transaction committed",
                )?;
                if record.boundary == RecordBoundary::TransactionCommit
                    && record
                        .connector_state
                        .as_ref()
                        .is_some_and(|state| state.payload.get("incremental_snapshot").is_some())
                {
                    break;
                }
            }
            concurrent_transaction.commit().await?;

            let mut snapshot_ids = Vec::new();
            let mut streaming_update_ids = Vec::new();
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event {
                    if event.source.attributes.get("rustium.snapshot.kind")
                        == Some(&"incremental".into())
                    {
                        require_incremental_event(&event, &table_name)?;
                        snapshot_ids.push(row_id(event.after.as_ref())?);
                    } else {
                        require(
                            event.operation == Operation::Update,
                            "unexpected read-only streaming event",
                        )?;
                        streaming_update_ids.push(row_id(event.after.as_ref())?);
                    }
                }
                if record.boundary == RecordBoundary::TransactionCommit
                    && record.connector_state.as_ref().is_some_and(|state| {
                        state.version == 5 && state.payload.get("incremental_snapshot").is_none()
                    })
                {
                    break;
                }
            }
            require(
                snapshot_ids == [1, 3, 4, 5],
                "read-only snapshot did not deduplicate its concurrent update",
            )?;
            require(
                streaming_update_ids == [2],
                "read-only snapshot concurrent WAL update was not emitted exactly once",
            )?;
            let signal_counts = client
                .query_one(
                    &format!(
                        "SELECT count(*)::bigint, \
                                count(*) FILTER (WHERE type LIKE 'snapshot-window-%')::bigint \
                         FROM public.{signal_table}"
                    ),
                    &[],
                )
                .await?;
            require(
                signal_counts.get::<_, i64>(0) == 1,
                "read-only incremental snapshot wrote unexpected signal records",
            )?;
            require(
                signal_counts.get::<_, i64>(1) == 0,
                "read-only incremental snapshot wrote watermark records",
            )
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        concurrent_connection_task.abort();
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    let role_cleanup = client
        .batch_execute(&format!(
            "DROP OWNED BY {role_name}; DROP ROLE IF EXISTS {role_name};"
        ))
        .await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL read-only cleanup also failed: {cleanup_error}");
    }
    if let Err(cleanup_error) = role_cleanup {
        if outcome.is_ok() {
            return Err(cleanup_error.into());
        }
        eprintln!("PostgreSQL read-only role cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn orders_incremental_chunks_by_unique_surrogate_key() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_surrogate_{}", &suffix[..12]);
    let signal_table = format!("rustium_pg_surrogate_signal_{}", &suffix[..12]);
    let publication = format!("rustium_surrogate_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_surrogate_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-surrogate-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                "CREATE TABLE public.{table_name} (\
                    id UUID PRIMARY KEY, \
                    snapshot_order BIGINT NOT NULL UNIQUE, \
                    value TEXT NOT NULL\
                 ); \
                 INSERT INTO public.{table_name} VALUES \
                    ('f0000000-0000-0000-0000-000000000001', 1, 'value-1'), \
                    ('e0000000-0000-0000-0000-000000000002', 2, 'value-2'), \
                    ('d0000000-0000-0000-0000-000000000003', 3, 'value-3'), \
                    ('c0000000-0000-0000-0000-000000000004', 4, 'value-4'), \
                    ('b0000000-0000-0000-0000-000000000005', 5, 'value-5'); \
                 CREATE TABLE public.{signal_table} (\
                    id VARCHAR(128), type VARCHAR(32), data VARCHAR(4096)\
                 ); \
                 CREATE PUBLICATION {publication} FOR TABLE \
                    public.{table_name}, public.{signal_table};"
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.incremental_snapshot_chunk_size = 2;
        let mut source = PostgresSource::new(
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

        wait_for_active_slot(&client, &slot_name).await?;
        let signal_data = serde_json::json!({
            "type": "incremental",
            "data-collections": [format!(r"public\.{table_name}")],
            "surrogate-key": "snapshot_order"
        })
        .to_string();
        insert_signal(
            &client,
            &signal_table,
            "surrogate-snapshot",
            "execute-snapshot",
            &signal_data,
        )
        .await?;

        let capture_result: TestResult = async {
            let mut ordering = Vec::new();
            loop {
                let record = receive(&mut output).await?;
                if let Some(event) = record.event {
                    require_incremental_event(&event, &table_name)?;
                    ordering.push(row_int64(event.after.as_ref(), "snapshot_order")?);
                }
                if record.boundary == RecordBoundary::TransactionCommit
                    && record.connector_state.as_ref().is_some_and(|state| {
                        state.version == 5 && state.payload.get("incremental_snapshot").is_none()
                    })
                {
                    break;
                }
            }
            require(
                ordering == [1, 2, 3, 4, 5],
                "PostgreSQL incremental snapshot did not use surrogate-key ordering",
            )
        }
        .await;

        let stop_result = stop_source(cancellation, source_task).await;
        combine_capture_and_stop(capture_result, stop_result)
    }
    .await;

    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL surrogate-key cleanup also failed: {cleanup_error}");
    }
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an external PostgreSQL 14+ server with logical replication enabled"]
async fn rejects_schema_change_inside_incremental_window() -> TestResult {
    let settings = TestSettings::from_env()?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table_name = format!("rustium_pg_schema_guard_{}", &suffix[..12]);
    let signal_table = format!("rustium_pg_schema_signal_{}", &suffix[..12]);
    let trigger_function = format!("rustium_pg_schema_fn_{}", &suffix[..12]);
    let publication = format!("rustium_schema_pub_{}", &suffix[..12]);
    let slot_name = format!("rustium_schema_slot_{}", &suffix[..12]);
    let connector_name = format!("postgresql-schema-guard-{}", &suffix[..12]);
    let (client, connection_task) = connect(&settings).await?;

    let outcome = async {
        client
            .batch_execute(&format!(
                r#"CREATE TABLE public.{table_name} (
                       id BIGINT PRIMARY KEY, value TEXT NOT NULL
                   );
                   INSERT INTO public.{table_name} VALUES (1, 'value-1'), (2, 'value-2');
                   CREATE TABLE public.{signal_table} (
                       id VARCHAR(128), type VARCHAR(32), data VARCHAR(4096)
                   );
                   CREATE FUNCTION public.{trigger_function}() RETURNS trigger AS $function$
                   BEGIN
                       IF NEW.type = 'snapshot-window-open' AND NEW.id LIKE '%-1-open' THEN
                           EXECUTE 'ALTER TABLE public.{table_name} ADD COLUMN added TEXT NOT NULL DEFAULT ''initial''';
                           UPDATE public.{table_name} SET value = value || '-changed' WHERE id = 1;
                       END IF;
                       RETURN NEW;
                   END;
                   $function$ LANGUAGE plpgsql;
                   CREATE TRIGGER rustium_schema_signal_trigger
                   AFTER INSERT ON public.{signal_table}
                   FOR EACH ROW EXECUTE FUNCTION public.{trigger_function}();
                   CREATE PUBLICATION {publication} FOR TABLE
                       public.{table_name}, public.{signal_table};"#
            ))
            .await?;

        let mut config = settings.source_config(&publication, &slot_name, &table_name);
        config.signal_data_collection = Some(format!("public.{signal_table}"));
        config.incremental_snapshot_chunk_size = 2;
        let mut source = PostgresSource::new(
            &connector_name,
            config,
            SnapshotConfig {
                mode: SnapshotMode::Never,
                fetch_size: 1,
                include_collections: Vec::new(),
            },
        );
        source.validate().await?;
        let (_output, cancellation, mut source_task) = start_source(source, None);

        wait_for_active_slot(&client, &slot_name).await?;
        insert_execute_snapshot_signal(&client, &signal_table, &table_name).await?;
        let result = tokio::time::timeout(Duration::from_secs(10), &mut source_task).await;
        cancellation.cancel();
        let source_result = match result {
            Ok(result) => result?,
            Err(_) => {
                source_task.abort();
                let _ = source_task.await;
                return Err(test_error(
                    "PostgreSQL source did not reject an active-window schema change",
                ));
            }
        };
        let error = source_result.expect_err(
            "PostgreSQL source unexpectedly accepted an active-window schema change",
        );
        require(
            error.to_string().contains("incremental snapshot window was active"),
            "PostgreSQL source returned the wrong schema-change failure",
        )
    }
    .await;

    let function_cleanup = client
        .batch_execute(&format!(
            "DROP FUNCTION IF EXISTS public.{trigger_function}() CASCADE"
        ))
        .await;
    let cleanup_result = cleanup(
        &client,
        &publication,
        &slot_name,
        &[&table_name, &signal_table],
    )
    .await;
    connection_task.abort();

    if let Err(cleanup_error) = function_cleanup {
        if outcome.is_ok() {
            return Err(cleanup_error.into());
        }
        eprintln!("PostgreSQL schema guard function cleanup also failed: {cleanup_error}");
    }
    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL schema guard cleanup also failed: {cleanup_error}");
    }
    outcome
}

async fn insert_execute_snapshot_signal(
    client: &Client,
    signal_table: &str,
    table_name: &str,
) -> TestResult {
    let id = uuid::Uuid::new_v4().to_string();
    let signal_type = "execute-snapshot";
    let data = serde_json::json!({
        "type": "incremental",
        "data-collections": [format!(r"public\.{table_name}")],
    })
    .to_string();
    insert_signal(client, signal_table, &id, signal_type, &data).await
}

async fn insert_signal(
    client: &Client,
    signal_table: &str,
    id: &str,
    signal_type: &str,
    data: &str,
) -> TestResult {
    client
        .execute(
            &format!("INSERT INTO public.{signal_table} (id, type, data) VALUES ($1, $2, $3)"),
            &[&id, &signal_type, &data],
        )
        .await?;
    Ok(())
}

fn require_incremental_event(event: &rustium_core::ChangeEvent, table_name: &str) -> TestResult {
    require(
        event.operation == Operation::Read,
        "incremental snapshot event is not a read",
    )?;
    require(
        event.source.snapshot,
        "incremental snapshot event has no snapshot marker",
    )?;
    require(
        event.source.table.as_deref() == Some(table_name),
        "signal table was exposed as a business event",
    )?;
    require(
        event.source.attributes.get("rustium.snapshot.kind") == Some(&"incremental".into()),
        "incremental snapshot kind is missing",
    )?;
    let id = event
        .schema
        .fields
        .iter()
        .find(|field| field.name == "id")
        .ok_or_else(|| test_error("incremental snapshot schema has no id field"))?;
    require(
        id.primary_key,
        "PostgreSQL catalog did not discover the id primary key",
    )?;
    require(
        !id.optional,
        "PostgreSQL catalog marked the id primary key nullable",
    )
}

fn row_id(row: Option<&Row>) -> TestResult<i64> {
    row_int64(row, "id")
}

fn row_int64(row: Option<&Row>, field: &str) -> TestResult<i64> {
    match row.and_then(|row| row.get(field)) {
        Some(DataValue::Int64(id)) => Ok(*id),
        _ => Err(test_error(&format!(
            "incremental snapshot row has no bigint {field}"
        ))),
    }
}

async fn insert_type_row(client: &Client, table_name: &str, id: i64) -> TestResult {
    client
        .batch_execute(&format!(
            r#"INSERT INTO public.{table_name} (
                   id, flag, small_value, integer_value, big_value,
                   real_value, double_value, special_double, amount, special_numeric,
                   payload, token, event_date, event_time, event_time_tz,
                   event_timestamp, event_timestamp_tz, binary_value,
                   text_values, integer_values, uuid_values,
                   network, network_block, mac, bits, duration, int_range,
                   attributes, attribute_values, domain_value, domain_values,
                   state, search_vector, nullable_value
               ) VALUES (
                   {id}, TRUE, -12, 345678, 9223372036854770000,
                   1.25, -12.5, 'Infinity'::double precision,
                   1234567890123456789012.34000000, 'NaN'::numeric,
                   '{{"nested":[1,true],"text":"value"}}'::jsonb,
                   '550e8400-e29b-41d4-a716-446655440000'::uuid,
                   '2026-07-15'::date, '12:34:56.123456'::time,
                   '12:34:56.123456+02:30'::timetz,
                   '2026-07-15 12:34:56.123456'::timestamp,
                   '2026-07-15 12:34:56.123456+02:30'::timestamptz,
                   decode('00ff10', 'hex'),
                   ARRAY['alpha', 'comma,value', 'NULL', NULL, 'quote"value', E'slash\\value'],
                   ARRAY[1, NULL, 3],
                   ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid, NULL,
                         '123e4567-e89b-12d3-a456-426614174000'::uuid],
                   '192.0.2.10/24'::inet, '2001:db8::/48'::cidr,
                   '08:00:2b:01:02:03'::macaddr, B'1010',
                   '2 days 03:04:05.006'::interval, '[1,10)'::int4range,
                   '"alpha"=>"one", "nothing"=>NULL'::hstore,
                   ARRAY[hstore('alpha', 'one'), hstore('nothing', NULL)],
                   12345678.2300, ARRAY[1.2300, 2.3400],
                   'ready', to_tsvector('english', 'Rustium captures global changes'), NULL
               )"#
        ))
        .await?;
    Ok(())
}

fn verify_type_row(row: &Row) -> TestResult {
    require(
        row.get("flag") == Some(&DataValue::Boolean(true)),
        "boolean conversion failed",
    )?;
    require(
        row.get("small_value") == Some(&DataValue::Int32(-12)),
        "smallint conversion failed",
    )?;
    require(
        row.get("integer_value") == Some(&DataValue::Int32(345_678)),
        "integer conversion failed",
    )?;
    require(
        row.get("big_value") == Some(&DataValue::Int64(9_223_372_036_854_770_000)),
        "bigint conversion failed",
    )?;
    require(
        row.get("special_double") == Some(&DataValue::String("Infinity".into())),
        "special double conversion failed",
    )?;
    require(
        row.get("amount")
            == Some(&DataValue::Decimal(
                "1234567890123456789012.34000000".into(),
            )),
        "numeric precision was not preserved",
    )?;
    require(
        row.get("special_numeric") == Some(&DataValue::Decimal("NaN".into())),
        "special numeric conversion failed",
    )?;
    require(
        row.get("payload")
            == Some(&DataValue::Json(
                serde_json::json!({"nested": [1, true], "text": "value"}),
            )),
        "JSONB conversion failed",
    )?;
    require(
        matches!(row.get("token"), Some(DataValue::Uuid(_))),
        "UUID conversion failed",
    )?;
    require(
        row.get("binary_value") == Some(&DataValue::Bytes(vec![0, 255, 16])),
        "bytea conversion failed",
    )?;
    require(
        row.get("text_values")
            == Some(&DataValue::Array(vec![
                DataValue::String("alpha".into()),
                DataValue::String("comma,value".into()),
                DataValue::String("NULL".into()),
                DataValue::Null,
                DataValue::String("quote\"value".into()),
                DataValue::String("slash\\value".into()),
            ])),
        "text array conversion failed",
    )?;
    require(
        row.get("integer_values")
            == Some(&DataValue::Array(vec![
                DataValue::Int32(1),
                DataValue::Null,
                DataValue::Int32(3),
            ])),
        "integer array conversion failed",
    )?;
    require(
        matches!(
            row.get("uuid_values"),
            Some(DataValue::Array(values))
                if matches!(values.as_slice(), [DataValue::Uuid(_), DataValue::Null, DataValue::Uuid(_)])
        ),
        "UUID array conversion failed",
    )?;
    for field in ["event_date", "event_time", "event_time_tz"] {
        require(
            matches!(
                row.get(field),
                Some(DataValue::Date(_) | DataValue::Time(_))
            ),
            "date/time conversion failed",
        )?;
    }
    for field in ["event_timestamp", "event_timestamp_tz"] {
        require(
            matches!(row.get(field), Some(DataValue::Timestamp(_))),
            "timestamp conversion failed",
        )?;
    }
    for field in [
        "network",
        "network_block",
        "mac",
        "bits",
        "duration",
        "int_range",
    ] {
        require(
            matches!(row.get(field), Some(DataValue::String(_))),
            "string-backed PostgreSQL type conversion failed",
        )?;
    }
    require(
        row.get("attributes")
            == Some(&DataValue::Json(
                serde_json::json!({"alpha": "one", "nothing": null}),
            )),
        "hstore JSON conversion failed",
    )?;
    require(
        row.get("attribute_values")
            == Some(&DataValue::Array(vec![
                DataValue::Json(serde_json::json!({"alpha": "one"})),
                DataValue::Json(serde_json::json!({"nothing": null})),
            ])),
        "hstore array conversion failed",
    )?;
    require(
        row.get("domain_value") == Some(&DataValue::Decimal("12345678.2300".into())),
        "numeric domain conversion failed",
    )?;
    require(
        row.get("domain_values")
            == Some(&DataValue::Array(vec![
                DataValue::Decimal("1.2300".into()),
                DataValue::Decimal("2.3400".into()),
            ])),
        "numeric domain array conversion failed",
    )?;
    require(
        row.get("state") == Some(&DataValue::String("ready".into())),
        "enum conversion failed",
    )?;
    require(
        matches!(row.get("search_vector"), Some(DataValue::String(value)) if value.contains("rustium")),
        "tsvector conversion failed",
    )?;
    require(
        row.get("nullable_value") == Some(&DataValue::Null),
        "SQL NULL conversion failed",
    )
}

async fn run_initial_capture(
    client: &Client,
    connector_name: &str,
    table_name: &str,
    slot_name: &str,
    config: PostgresSourceConfig,
) -> TestResult<(SourcePosition, ConnectorStateEnvelope)> {
    let mut source = PostgresSource::new(
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
    let capture_result: TestResult<(SourcePosition, ConnectorStateEnvelope)> = async {
        let mut snapshot_rows = 0;
        let schema_history = loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                break record.connector_state.ok_or_else(|| {
                    test_error("snapshot completion has no PostgreSQL schema history")
                })?;
            }
            require(
                record.boundary == RecordBoundary::Data,
                "unexpected snapshot boundary",
            )?;
            let event = record
                .event
                .ok_or_else(|| test_error("snapshot data record has no event"))?;
            require(
                event.operation == Operation::Read,
                "snapshot event is not a read",
            )?;
            snapshot_rows += 1;
        };
        require(snapshot_rows == 2, "snapshot did not emit exactly two rows")?;

        wait_for_active_slot(client, slot_name).await?;
        let slot = client
            .query_one(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?;
        require(
            slot.get::<_, String>(0) == "pgoutput",
            "managed slot does not use pgoutput",
        )?;

        client
            .batch_execute(&format!(
                "BEGIN; \
                 INSERT INTO public.{table_name} VALUES (3, 'Cara', 10.25); \
                 UPDATE public.{table_name} SET amount = 13.30 WHERE id = 1; \
                 DELETE FROM public.{table_name} WHERE id = 2; \
                 COMMIT;"
            ))
            .await?;

        let mut operations = Vec::new();
        let mut transaction_orders = Vec::new();
        let commit_position = loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::TransactionCommit {
                break record.position;
            }
            require(
                record.boundary == RecordBoundary::Data,
                "unexpected streaming boundary",
            )?;
            let event = record
                .event
                .ok_or_else(|| test_error("streaming data record has no event"))?;
            operations.push(event.operation);
            transaction_orders.push(
                event
                    .transaction
                    .ok_or_else(|| test_error("streaming event has no transaction metadata"))?
                    .total_order
                    .ok_or_else(|| test_error("streaming event has no transaction order"))?,
            );
        };
        require(
            operations == [Operation::Create, Operation::Update, Operation::Delete],
            "transaction operations are incomplete or out of order",
        )?;
        require(
            transaction_orders == [1, 2, 3],
            "transaction total_order values are incorrect",
        )?;

        Ok((commit_position, schema_history))
    }
    .await;

    let stop_result = stop_source(cancellation, source_task).await;
    combine_capture_and_stop(capture_result, stop_result)
}

async fn run_resumed_capture(
    connector_name: &str,
    config: PostgresSourceConfig,
    checkpoint: Checkpoint,
) -> TestResult {
    let mut source = PostgresSource::new(
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
        let mut old_event = None;
        let mut new_event = None;
        let mut saw_new_schema_state = false;
        loop {
            let record = receive(&mut output).await?;
            require(
                record.boundary != RecordBoundary::SnapshotComplete,
                "snapshot repeated after a completed checkpoint",
            )?;
            if record.boundary == RecordBoundary::TransactionCommit {
                if new_event.is_some() {
                    break;
                }
                continue;
            }
            require(
                record.boundary == RecordBoundary::Data,
                "unexpected resumed boundary",
            )?;
            let event = record
                .event
                .ok_or_else(|| test_error("resumed data record has no event"))?;
            require(
                event.operation != Operation::Read,
                "snapshot row repeated after a completed checkpoint",
            )?;
            require(
                event.operation == Operation::Create,
                "unexpected resumed operation",
            )?;
            if old_event.is_none() {
                old_event = Some(event);
            } else {
                saw_new_schema_state = record.connector_state.is_some();
                new_event = Some(event);
            }
        }

        let old_event = old_event.ok_or_else(|| test_error("old-schema event was not emitted"))?;
        require(
            old_event.schema.version == 1,
            "old row did not use schema version 1",
        )?;
        require(
            old_event.after.as_ref().and_then(|row| row.get("customer"))
                == Some(&DataValue::String("Dora".into())),
            "old row was not decoded with the historical customer column",
        )?;
        require(
            old_event
                .after
                .as_ref()
                .is_some_and(|row| !row.contains_key("status")),
            "old row contains the future status column",
        )?;

        let new_event = new_event.ok_or_else(|| test_error("new-schema event was not emitted"))?;
        require(
            saw_new_schema_state,
            "new relation schema was not attached to a checkpointable record",
        )?;
        require(
            new_event.schema.version == 2,
            "new row did not use schema version 2",
        )?;
        require(
            new_event.after.as_ref().and_then(|row| row.get("status"))
                == Some(&DataValue::String("ready".into())),
            "new row was not decoded with the status column",
        )?;
        require(
            new_event
                .after
                .as_ref()
                .is_some_and(|row| !row.contains_key("customer")),
            "new row still contains the dropped customer column",
        )?;
        Ok(())
    }
    .await;

    let stop_result = stop_source(cancellation, source_task).await;
    combine_capture_and_stop(capture_result, stop_result)
}

fn start_source(
    source: PostgresSource,
    initial_checkpoint: Option<Checkpoint>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    start_source_with_output_capacity(source, initial_checkpoint, 64)
}

fn start_source_with_output_capacity(
    source: PostgresSource,
    initial_checkpoint: Option<Checkpoint>,
    output_capacity: usize,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    let running = start_source_with_acknowledgement(source, initial_checkpoint, output_capacity);
    (running.output, running.cancellation, running.task)
}

fn start_source_with_acknowledgement(
    mut source: PostgresSource,
    initial_checkpoint: Option<Checkpoint>,
    output_capacity: usize,
) -> AcknowledgingSource {
    let (output_tx, output_rx) = mpsc::channel(output_capacity);
    let (_signal_sender, signals) = rustium_core::signal_channel(64);
    let acknowledged_position = initial_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.source_position.clone());
    let (ack_tx, ack_rx) = watch::channel(acknowledged_position);
    let source_ack_tx = ack_tx.clone();
    let cancellation = CancellationToken::new();
    let source_cancel = cancellation.clone();
    let source_task = tokio::spawn(async move {
        let _ack_tx = source_ack_tx;
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
    AcknowledgingSource {
        output: output_rx,
        cancellation,
        task: source_task,
        acknowledgement: ack_tx,
    }
}

fn start_source_with_signals(
    mut source: PostgresSource,
    initial_checkpoint: Option<Checkpoint>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
    rustium_core::SignalSender,
) {
    let (output_tx, output_rx) = mpsc::channel(64);
    let (signal_sender, signals) = rustium_core::signal_channel(64);
    let acknowledged_position = initial_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.source_position.clone());
    let (ack_tx, ack_rx) = watch::channel(acknowledged_position);
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
    (output_rx, cancellation, source_task, signal_sender)
}

async fn stop_source(
    cancellation: CancellationToken,
    mut source_task: JoinHandle<rustium_core::Result<()>>,
) -> TestResult {
    cancellation.cancel();
    let result = match tokio::time::timeout(Duration::from_secs(10), &mut source_task).await {
        Ok(result) => result?,
        Err(_) => {
            source_task.abort();
            let _ = source_task.await;
            return Err(test_error(
                "PostgreSQL source did not stop after cancellation",
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
        .map_err(|_| test_error("timed out waiting for a PostgreSQL source record"))?
        .ok_or_else(|| test_error("PostgreSQL source output closed unexpectedly"))??;
    Ok(record)
}

fn reconnect_soak_cycles() -> TestResult<u32> {
    let cycles = std::env::var("RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES")
        .unwrap_or_else(|_| "3".into())
        .parse::<u32>()?;
    require(
        (1..=1_000).contains(&cycles),
        "RUSTIUM_POSTGRES_RECONNECT_SOAK_CYCLES must be between 1 and 1000",
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
        "PostgreSQL source output did not reach bounded backpressure",
    ))
}

async fn receive_with_context(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    context: &str,
) -> TestResult<SourceRecord> {
    receive(output)
        .await
        .map_err(|error| test_error(&format!("{context}: {error}")))
}

async fn receive_postgres_create(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult<SourceRecord> {
    loop {
        let record = receive(output).await?;
        let Some(event) = record.event.as_ref() else {
            require(
                record.boundary == RecordBoundary::TransactionCommit,
                "offset strategy fixture emitted an unexpected position-only record",
            )?;
            continue;
        };
        require(
            event.operation == Operation::Create,
            "offset strategy fixture emitted a non-create event",
        )?;
        return Ok(record);
    }
}

async fn receive_postgres_transaction_commit(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult<SourceRecord> {
    loop {
        let record = receive(output).await?;
        if record.boundary == RecordBoundary::TransactionCommit {
            return Ok(record);
        }
        require(
            record
                .event
                .as_ref()
                .is_some_and(|event| event.operation == Operation::Create),
            "LSN flush fixture emitted an unexpected record before commit",
        )?;
    }
}

fn postgres_record_commit_lsn(record: &SourceRecord) -> TestResult<u64> {
    match &record.position {
        SourcePosition::Postgres(position) => Ok(position.commit_lsn.unwrap_or(position.lsn)),
        SourcePosition::MySql(_) | SourcePosition::SqlServer(_) => Err(test_error(
            "LSN flush fixture emitted a non-PostgreSQL position",
        )),
    }
}

async fn insert_with_replication_origin(
    client: &Client,
    table_name: &str,
    origin_name: &str,
    id: i64,
) -> TestResult {
    client
        .query_one(
            "SELECT pg_replication_origin_session_setup($1)",
            &[&origin_name],
        )
        .await?;
    let insert_result = client
        .batch_execute(&format!(
            "SELECT pg_replication_origin_xact_setup('0/12345', now()); \
             INSERT INTO public.{table_name} VALUES ({id}, 'replicated-origin');"
        ))
        .await;
    let reset_result = client
        .simple_query("SELECT pg_replication_origin_session_reset()")
        .await;
    insert_result?;
    reset_result?;
    Ok(())
}

async fn receive_postgres_create_id(
    output: &mut mpsc::Receiver<rustium_core::Result<SourceRecord>>,
) -> TestResult<i64> {
    let record = receive_postgres_create(output).await?;
    postgres_create_id(&record)
}

fn postgres_create_id(record: &SourceRecord) -> TestResult<i64> {
    match record
        .event
        .as_ref()
        .and_then(|event| event.after.as_ref())
        .and_then(|row| row.get("id"))
    {
        Some(DataValue::Int64(id)) => Ok(*id),
        _ => Err(test_error(
            "offset strategy fixture create event has no BIGINT id",
        )),
    }
}

async fn wait_for_active_slot(client: &Client, slot_name: &str) -> TestResult {
    for _ in 0..100 {
        let active = client
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?
            .is_some_and(|row| row.get(0));
        if active {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error("replication slot did not become active"))
}

async fn wait_for_application_session(
    client: &Client,
    database: &str,
    application_name: &str,
) -> TestResult {
    for _ in 0..100 {
        let active = client
            .query_one(
                "SELECT EXISTS( \
                    SELECT 1 FROM pg_stat_activity \
                    WHERE datname = $1 AND application_name = $2 \
                )",
                &[&database, &application_name],
            )
            .await?
            .get::<_, bool>(0);
        if active {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(
        "PostgreSQL regular connection did not apply database.initial.statements",
    ))
}

async fn wait_for_confirmed_flush_lsn(
    client: &Client,
    slot_name: &str,
    expected_lsn: u64,
) -> TestResult {
    let expected_lsn = format_postgres_lsn(expected_lsn);
    for _ in 0..150 {
        let reached = client
            .query_opt(
                "SELECT COALESCE(confirmed_flush_lsn >= ($2::text)::pg_lsn, false) \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name, &expected_lsn],
            )
            .await?
            .is_some_and(|row| row.get(0));
        if reached {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(test_error(
        "replication slot confirmed_flush_lsn did not reach the acknowledged LSN within 3 seconds",
    ))
}

async fn current_wal_lsn(client: &Client) -> TestResult<u64> {
    let lsn = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get::<_, String>(0);
    parse_postgres_lsn(&lsn)
}

async fn replication_slot_confirmed_lsn(client: &Client, slot_name: &str) -> TestResult<u64> {
    let lsn = client
        .query_one(
            "SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?
        .get::<_, String>(0);
    parse_postgres_lsn(&lsn)
}

async fn advance_replication_slot(client: &Client, slot_name: &str, target_lsn: u64) -> TestResult {
    let target = format_postgres_lsn(target_lsn);
    let advanced = client
        .query_one(
            "SELECT end_lsn::text \
             FROM pg_replication_slot_advance($1, ($2::text)::pg_lsn)",
            &[&slot_name, &target],
        )
        .await?
        .get::<_, String>(0);
    require(
        parse_postgres_lsn(&advanced)? >= target_lsn,
        "PostgreSQL did not advance the replication slot to the requested LSN",
    )
}

async fn drop_replication_slot(client: &Client, slot_name: &str) -> TestResult {
    client
        .execute(
            "SELECT pg_drop_replication_slot($1) \
             WHERE EXISTS (\
                SELECT 1 FROM pg_replication_slots \
                WHERE slot_name = $1 AND NOT active\
             )",
            &[&slot_name],
        )
        .await?;
    Ok(())
}

fn parse_postgres_lsn(lsn: &str) -> TestResult<u64> {
    let (high, low) = lsn
        .split_once('/')
        .ok_or_else(|| test_error("PostgreSQL returned an invalid LSN"))?;
    Ok((u64::from_str_radix(high, 16)? << 32) | u64::from_str_radix(low, 16)?)
}

fn format_postgres_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

async fn wait_for_inactive_slot(client: &Client, slot_name: &str) -> TestResult {
    for _ in 0..100 {
        let active = client
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?
            .is_some_and(|row| row.get(0));
        if !active {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error("replication slot did not become inactive"))
}

async fn replication_slot_wal_status(client: &Client, slot_name: &str) -> TestResult<String> {
    client
        .query_one(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?
        .get::<_, Option<String>>(0)
        .ok_or_else(|| test_error("replication slot has no WAL status"))
}

async fn set_max_slot_wal_keep_size(client: &Client, value: &str) -> TestResult {
    let sql = format!(
        "ALTER SYSTEM SET max_slot_wal_keep_size = '{}'",
        value.replace('\'', "''")
    );
    client.execute(&sql, &[]).await?;
    let reloaded = client
        .query_one("SELECT pg_reload_conf()", &[])
        .await?
        .get::<_, bool>(0);
    require(reloaded, "PostgreSQL did not reload max_slot_wal_keep_size")?;
    for _ in 0..100 {
        let current = client
            .query_one("SHOW max_slot_wal_keep_size", &[])
            .await?
            .get::<_, String>(0);
        if current == value {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(&format!(
        "PostgreSQL max_slot_wal_keep_size did not become {value:?}"
    )))
}

async fn wait_for_active_pid(client: &Client, slot_name: &str) -> TestResult<i32> {
    for _ in 0..100 {
        let active_pid = client
            .query_opt(
                "SELECT active_pid FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active",
                &[&slot_name],
            )
            .await?
            .and_then(|row| row.get::<_, Option<i32>>(0));
        if let Some(active_pid) = active_pid {
            return Ok(active_pid);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error("replication slot has no active backend PID"))
}

async fn wait_for_different_active_pid(
    client: &Client,
    slot_name: &str,
    previous_pid: i32,
) -> TestResult<i32> {
    for _ in 0..300 {
        let active_pid = client
            .query_opt(
                "SELECT active_pid FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active",
                &[&slot_name],
            )
            .await?
            .and_then(|row| row.get::<_, Option<i32>>(0));
        if let Some(active_pid) = active_pid
            && active_pid != previous_pid
        {
            return Ok(active_pid);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(test_error(
        "replication slot did not reconnect with a different backend PID",
    ))
}

async fn installed_extension_schema(client: &Client, name: &str) -> TestResult<Option<String>> {
    Ok(client
        .query_opt(
            "SELECT n.nspname FROM pg_extension e \
             JOIN pg_namespace n ON n.oid = e.extnamespace \
             WHERE e.extname = $1",
            &[&name],
        )
        .await?
        .map(|row| row.get(0)))
}

async fn type_name_exists(client: &Client, name: Option<&str>) -> TestResult<bool> {
    let Some(name) = name else {
        return Ok(false);
    };
    Ok(client
        .query_one("SELECT to_regtype($1) IS NOT NULL", &[&name])
        .await?
        .get(0))
}

async fn publication_tables(client: &Client, publication: &str) -> TestResult<Vec<String>> {
    Ok(client
        .query(
            "SELECT schemaname || '.' || tablename \
             FROM pg_catalog.pg_publication_tables \
             WHERE pubname = $1 \
             ORDER BY schemaname, tablename",
            &[&publication],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

async fn replica_identity(client: &Client, table: &str) -> TestResult<(String, Option<String>)> {
    let row = client
        .query_one(
            "SELECT c.relreplident::text, i.relname \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             LEFT JOIN pg_catalog.pg_index x ON x.indrelid = c.oid AND x.indisreplident \
             LEFT JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid \
             WHERE n.nspname = 'public' AND c.relname = $1",
            &[&table],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

fn qualified_name(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(name))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn connect(
    settings: &TestSettings,
) -> TestResult<(Client, JoinHandle<Result<(), tokio_postgres::Error>>)> {
    let mut config = Config::new();
    config
        .host(&settings.host)
        .port(settings.port)
        .user(&settings.user)
        .password(&settings.password)
        .dbname(&settings.database)
        .connect_timeout(Duration::from_secs(10));
    let (client, connection) = config.connect(NoTls).await?;
    Ok((client, tokio::spawn(connection)))
}

async fn cleanup(
    client: &Client,
    publication: &str,
    slot_name: &str,
    table_names: &[&str],
) -> TestResult {
    let mut first_error = None;
    if let Err(error) = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication}"))
        .await
    {
        first_error = Some(error);
    }
    if let Err(error) = client
        .execute(
            "SELECT pg_drop_replication_slot($1) \
             WHERE EXISTS (\
                SELECT 1 FROM pg_replication_slots \
                WHERE slot_name = $1 AND NOT active\
             )",
            &[&slot_name],
        )
        .await
        && first_error.is_none()
    {
        first_error = Some(error);
    }
    for table_name in table_names {
        if let Err(error) = client
            .batch_execute(&format!("DROP TABLE IF EXISTS public.{table_name}"))
            .await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
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
        .ok_or_else(|| test_error("Kafka committed offset has no PostgreSQL signal partition"))?
        .offset())
}

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| test_error(&format!("required environment variable {name} is not set")))
}

fn optional_env_bool(name: &str) -> TestResult<bool> {
    match std::env::var(name) {
        Ok(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Ok(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Ok(value) => Err(test_error(&format!(
            "environment variable {name} must be true, false, 1, or 0; found {value:?}"
        ))),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(test_error(&format!(
            "environment variable {name} is invalid: {error}"
        ))),
    }
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(test_error(message))
    }
}

fn combine_capture_and_stop<T>(capture: TestResult<T>, stop: TestResult) -> TestResult<T> {
    match (capture, stop) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(capture_error), Ok(())) => Err(capture_error),
        (Ok(_), Err(stop_error)) => Err(stop_error),
        (Err(capture_error), Err(stop_error)) => Err(test_error(&format!(
            "{capture_error}; PostgreSQL source task failed: {stop_error}"
        ))),
    }
}

fn test_error(message: &str) -> Box<dyn StdError + Send + Sync> {
    io::Error::other(message).into()
}
