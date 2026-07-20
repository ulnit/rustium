use std::{env, error::Error as StdError, time::Duration};

use rustium_config::{OracleSourceConfig, SnapshotConfig, TableSelection};
use rustium_core::SourceConnector;
use rustium_oracle::OracleSource;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test]
#[ignore = "requires an external Oracle instance with ARCHIVELOG, supplemental logging, and LogMiner privileges"]
async fn validates_oracle_logminer_source() -> TestResult {
    let config = OracleSourceConfig {
        hostname: env::var("RUSTIUM_ORACLE_TEST_HOST")?,
        port: env::var("RUSTIUM_ORACLE_TEST_PORT")?.parse()?,
        username: env::var("RUSTIUM_ORACLE_TEST_USER")?,
        password: env::var("RUSTIUM_ORACLE_TEST_PASSWORD")?,
        database: env::var("RUSTIUM_ORACLE_TEST_SERVICE")?,
        pdb_name: env::var("RUSTIUM_ORACLE_TEST_PDB").ok(),
        schemas: env::var("RUSTIUM_ORACLE_TEST_SCHEMA")
            .ok()
            .into_iter()
            .collect(),
        tables: TableSelection::default(),
        connect_timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(250),
        batch_size: 256,
        log_mining_strategy: "online_catalog".into(),
        archive_log_only_mode: false,
        heartbeat_interval: Duration::ZERO,
        heartbeat_action_query: None,
        heartbeat_topics_prefix: "__debezium-heartbeat".into(),
        heartbeat_topic_name: None,
    };
    OracleSource::new("oracle-external", config, SnapshotConfig::default())
        .validate()
        .await?;
    Ok(())
}
