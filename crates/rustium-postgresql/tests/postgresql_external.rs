use std::{
    error::Error as StdError,
    io,
    time::{Duration, SystemTime},
};

use rustium_config::{
    PostgresSourceConfig, SlotOwnership, SnapshotConfig, SnapshotMode, TableSelection,
};
use rustium_core::{
    Checkpoint, DataValue, Operation, RecordBoundary, SourceConnector, SourceContext,
    SourcePosition, SourceRecord,
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
        let commit_position = run_initial_capture(
            &client,
            &connector_name,
            &table_name,
            &slot_name,
            config.clone(),
        )
        .await?;

        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: commit_position,
            snapshot_completed: true,
            config_fingerprint: "postgresql-external-test".into(),
            updated_at: SystemTime::now(),
            connector_state: None,
        };
        run_resumed_capture(
            &client,
            &connector_name,
            &table_name,
            &slot_name,
            config,
            checkpoint,
        )
        .await
    }
    .await;

    let cleanup_result = cleanup(&client, &publication, &slot_name, &table_name).await;
    connection_task.abort();

    if let Err(cleanup_error) = cleanup_result {
        if outcome.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("PostgreSQL test cleanup also failed: {cleanup_error}");
    }
    outcome
}

async fn run_initial_capture(
    client: &Client,
    connector_name: &str,
    table_name: &str,
    slot_name: &str,
    config: PostgresSourceConfig,
) -> TestResult<SourcePosition> {
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
    let capture_result: TestResult<SourcePosition> = async {
        let mut snapshot_rows = 0;
        loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::SnapshotComplete {
                break;
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
        }
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
        loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::TransactionCommit {
                break;
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
        }
        require(
            operations == [Operation::Create, Operation::Update, Operation::Delete],
            "transaction operations are incomplete or out of order",
        )?;
        require(
            transaction_orders == [1, 2, 3],
            "transaction total_order values are incorrect",
        )?;

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
                     VALUES (4, 'Dora', 67.80, 'ready')"
                ),
                &[],
            )
            .await?;

        let mut refreshed_event = None;
        let commit_position = loop {
            let record = receive(&mut output).await?;
            if record.boundary == RecordBoundary::TransactionCommit {
                break record.position;
            }
            require(
                record.boundary == RecordBoundary::Data,
                "unexpected DDL boundary",
            )?;
            let event = record
                .event
                .ok_or_else(|| test_error("DDL data record has no event"))?;
            require(
                event.operation == Operation::Create,
                "DDL follow-up event is not a create",
            )?;
            refreshed_event = Some(event);
        };
        let event = refreshed_event
            .ok_or_else(|| test_error("DDL follow-up transaction emitted no data event"))?;
        require(
            event.schema.version == 2,
            "relation-driven schema version did not advance",
        )?;
        let status_field = event
            .schema
            .fields
            .iter()
            .find(|field| field.name == "status")
            .ok_or_else(|| test_error("refreshed schema does not contain status"))?;
        require(
            status_field.type_name == "text" && !status_field.optional,
            "refreshed status field metadata is incorrect",
        )?;
        require(
            event.after.as_ref().and_then(|row| row.get("status"))
                == Some(&DataValue::String("ready".into())),
            "DDL follow-up event does not contain the new status value",
        )?;
        Ok(commit_position)
    }
    .await;

    let stop_result = stop_source(cancellation, source_task).await;
    combine_capture_and_stop(capture_result, stop_result)
}

async fn run_resumed_capture(
    client: &Client,
    connector_name: &str,
    table_name: &str,
    slot_name: &str,
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
        wait_for_active_slot(client, slot_name).await?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, customer, amount, status) \
                     VALUES (5, 'Erin', 89.10, 'resumed')"
                ),
                &[],
            )
            .await?;

        let mut operations = Vec::new();
        let mut transaction_orders = Vec::new();
        loop {
            let record = receive(&mut output).await?;
            require(
                record.boundary != RecordBoundary::SnapshotComplete,
                "snapshot repeated after a completed checkpoint",
            )?;
            if record.boundary == RecordBoundary::TransactionCommit {
                break;
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
            operations.push(event.operation);
            transaction_orders.push(
                event
                    .transaction
                    .ok_or_else(|| test_error("resumed event has no transaction metadata"))?
                    .total_order
                    .ok_or_else(|| test_error("resumed event has no transaction order"))?,
            );
        }
        require(
            operations == [Operation::Create],
            "resume did not emit only the new create event",
        )?;
        require(
            transaction_orders == [1],
            "resumed transaction order is incorrect",
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
    table_name: &str,
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
    if let Err(error) = client
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{table_name}"))
        .await
        && first_error.is_none()
    {
        first_error = Some(error);
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
