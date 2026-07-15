use std::{
    error::Error as StdError,
    io,
    time::{Duration, SystemTime},
};

use rustium_config::{
    PostgresSourceConfig, SlotOwnership, SnapshotConfig, SnapshotMode, TableSelection,
};
use rustium_core::{
    CHECKPOINT_SCHEMA_VERSION, Checkpoint, ConnectorStateEnvelope, DataValue, Operation,
    RecordBoundary, Row, SourceConnector, SourceContext, SourcePosition, SourceRecord,
};
use rustium_postgresql::PostgresSource;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_postgres::{Client, Config, NoTls};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);

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
            slot_name: slot_name.into(),
            slot_ownership: SlotOwnership::Managed,
            tables: TableSelection {
                include: vec![format!(r"public\.{table_name}")],
                exclude: Vec::new(),
            },
            ssl_mode: "disable".into(),
            connect_timeout: Duration::from_secs(10),
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
            read_only: false,
            hstore_handling_mode: "json".into(),
        }
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
async fn runs_read_only_file_snapshot_without_signal_table() -> TestResult {
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
        config.signal_enabled_channels = vec!["file".into()];
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
            },
        );
        source.validate().await?;
        let (mut output, cancellation, source_task) = start_source(source, None);

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

        let capture_result: TestResult<Vec<i64>> = async {
            let mut ids = Vec::new();
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
                        ids.push(row_id(event.after.as_ref())?);
                    } else if event.operation == Operation::Update
                        && row_id(event.after.as_ref())? == 1
                    {
                        saw_streamed_update = true;
                    }
                }
                if ids.len() == 2
                    && record
                        .connector_state
                        .as_ref()
                        .is_some_and(|state| state.payload.get("incremental_snapshot").is_none())
                {
                    require(
                        saw_streamed_update,
                        "read-only file snapshot did not stream the concurrent update",
                    )?;
                    break Ok(ids);
                }
            }
        }
        .await;
        let stop_result = stop_source(cancellation, source_task).await;
        let ids = combine_capture_and_stop(capture_result, stop_result)?;
        require(
            ids == [2, 3],
            "read-only file snapshot did not deduplicate the updated key",
        )?;
        require(
            std::fs::read_to_string(&signal_path)?.is_empty(),
            "read-only file signal was not cleared after polling",
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
                        state.version == 4,
                        "PostgreSQL connector state did not upgrade to version 4",
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
                        state.version == 4
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
                        state.version == 4 && state.payload.get("incremental_snapshot").is_none()
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
                        state.version == 4 && state.payload.get("incremental_snapshot").is_none()
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
    mut source: PostgresSource,
    initial_checkpoint: Option<Checkpoint>,
) -> (
    mpsc::Receiver<rustium_core::Result<SourceRecord>>,
    CancellationToken,
    JoinHandle<rustium_core::Result<()>>,
) {
    let (output_tx, output_rx) = mpsc::channel(64);
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
                cancellation: source_cancel,
            })
            .await
    });
    (output_rx, cancellation, source_task)
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
