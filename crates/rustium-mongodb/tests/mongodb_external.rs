use std::{env, error::Error as StdError, time::Duration};

use rustium_config::{MongoDbSourceConfig, SnapshotConfig, TableSelection};
use rustium_core::SourceConnector;
use rustium_mongodb::MongoDbSource;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test]
#[ignore = "requires an external MongoDB replica set or sharded cluster"]
async fn validates_mongodb_change_stream_source() -> TestResult {
    let config = MongoDbSourceConfig {
        connection_string: env::var("RUSTIUM_MONGODB_TEST_URI")?,
        databases: vec![env::var("RUSTIUM_MONGODB_TEST_DATABASE")?],
        collections: TableSelection::default(),
        connect_timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(250),
        batch_size: 256,
        full_document: "update_lookup".into(),
        full_document_before_change: "when_available".into(),
        heartbeat_interval: Duration::ZERO,
        heartbeat_topics_prefix: "__debezium-heartbeat".into(),
        heartbeat_topic_name: None,
    };
    MongoDbSource::new("mongodb-external", config, SnapshotConfig::default())
        .validate()
        .await?;
    Ok(())
}
