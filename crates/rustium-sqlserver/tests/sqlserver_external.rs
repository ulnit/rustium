use std::{
    error::Error as StdError,
    io,
    time::{Duration, SystemTime},
};

use rustium_config::{SnapshotConfig, SnapshotMode, SqlServerSourceConfig, TableSelection};
use rustium_core::{
    Checkpoint, Operation, RecordBoundary, SourceConnector, SourceContext, SourcePosition,
    SourceRecord,
};
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
        }
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

        let config = settings.source_config(&table_name);
        let commit_position =
            run_initial_capture(&mut client, &connector_name, &table_name, config.clone()).await?;

        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: connector_name.clone(),
            generation: uuid::Uuid::new_v4(),
            source_position: commit_position,
            snapshot_completed: true,
            config_fingerprint: "sqlserver-external-test".into(),
            updated_at: SystemTime::now(),
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

        let (operations, transaction_orders, commit_position) =
            receive_transaction(&mut output).await?;
        require(
            operations == [Operation::Create, Operation::Update, Operation::Delete],
            "SQL Server transaction operations are incomplete or out of order",
        )?;
        require(
            transaction_orders == [1, 2, 3],
            "SQL Server transaction total_order values are incorrect",
        )?;
        Ok(commit_position)
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
        },
    );
    source.validate().await?;

    let (mut output, cancellation, source_task) = start_source(source, Some(checkpoint));
    let capture_result: TestResult = async {
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
    mut source: SqlServerSource,
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
