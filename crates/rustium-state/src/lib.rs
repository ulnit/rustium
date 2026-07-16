//! Persistent checkpoint stores.

use std::{path::PathBuf, time::UNIX_EPOCH};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use rustium_core::{Checkpoint, CheckpointStore, Error, Result};

/// SQLite storage schema version, independent from the JSON checkpoint version.
pub const SQLITE_STORAGE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct SqliteCheckpointStore {
    path: PathBuf,
}

impl SqliteCheckpointStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let store = Self { path: path.into() };
        let path = store.path.clone();
        tokio::task::spawn_blocking(move || initialize(&path))
            .await
            .map_err(|error| {
                Error::State(format!("state initialization task failed: {error}"))
            })??;
        Ok(store)
    }
}

#[async_trait]
impl CheckpointStore for SqliteCheckpointStore {
    async fn load(&self, connector_name: &str) -> Result<Option<Checkpoint>> {
        let path = self.path.clone();
        let connector_name = connector_name.to_string();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let payload: Option<String> = connection
                .query_row(
                    "SELECT payload FROM checkpoints WHERE connector_name = ?1",
                    params![connector_name],
                    |row| row.get(0),
                )
                .optional()
                .map_err(state_error)?;
            payload
                .map(|payload| serde_json::from_str(&payload).map_err(Error::from))
                .transpose()
        })
        .await
        .map_err(|error| Error::State(format!("state load task failed: {error}")))?
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        let path = self.path.clone();
        let checkpoint = checkpoint.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = open_connection(&path)?;
            let transaction = connection.transaction().map_err(state_error)?;
            let payload = serde_json::to_string(&checkpoint)?;
            let updated_at_ms = checkpoint
                .updated_at
                .duration_since(UNIX_EPOCH)
                .map_err(|error| Error::State(error.to_string()))?
                .as_millis() as i64;
            transaction
                .execute(
                    r#"INSERT INTO checkpoints
                         (connector_name, schema_version, payload, updated_at_ms)
                       VALUES (?1, ?2, ?3, ?4)
                       ON CONFLICT(connector_name) DO UPDATE SET
                         schema_version = excluded.schema_version,
                         payload = excluded.payload,
                         updated_at_ms = excluded.updated_at_ms"#,
                    params![
                        checkpoint.connector_name,
                        checkpoint.schema_version,
                        payload,
                        updated_at_ms
                    ],
                )
                .map_err(state_error)?;
            transaction.commit().map_err(state_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::State(format!("state save task failed: {error}")))?
    }

    async fn delete(&self, connector_name: &str) -> Result<()> {
        let path = self.path.clone();
        let connector_name = connector_name.to_string();
        tokio::task::spawn_blocking(move || {
            open_connection(&path)?
                .execute(
                    "DELETE FROM checkpoints WHERE connector_name = ?1",
                    params![connector_name],
                )
                .map_err(state_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::State(format!("state delete task failed: {error}")))?
    }
}

fn initialize(path: &PathBuf) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let connection = open_connection(path)?;
    let storage_version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
        .map_err(state_error)?;
    if storage_version > SQLITE_STORAGE_SCHEMA_VERSION {
        return Err(Error::State(format!(
            "SQLite state schema version {storage_version} is newer than the supported version {SQLITE_STORAGE_SCHEMA_VERSION}; upgrade Rustium before opening this state file"
        )));
    }

    connection
        .execute_batch(
            r#"CREATE TABLE IF NOT EXISTS checkpoints (
                 connector_name TEXT PRIMARY KEY NOT NULL,
                 schema_version INTEGER NOT NULL,
                 payload TEXT NOT NULL,
                 updated_at_ms INTEGER NOT NULL
               );"#,
        )
        .map_err(state_error)?;

    if storage_version < SQLITE_STORAGE_SCHEMA_VERSION {
        connection
            .pragma_update(None, "user_version", SQLITE_STORAGE_SCHEMA_VERSION)
            .map_err(state_error)?;
    }
    Ok(())
}

fn open_connection(path: &PathBuf) -> Result<Connection> {
    let connection = Connection::open(path).map_err(state_error)?;
    connection
        .execute_batch(
            r#"PRAGMA journal_mode = WAL;
               PRAGMA synchronous = FULL;
               PRAGMA foreign_keys = ON;
               PRAGMA busy_timeout = 5000;"#,
        )
        .map_err(state_error)?;
    Ok(connection)
}

fn state_error(error: rusqlite::Error) -> Error {
    Error::State(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use rustium_core::{
        CHECKPOINT_SCHEMA_VERSION, ConnectorStateEnvelope, PostgresPosition, SourcePosition,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn round_trips_checkpoint() {
        let directory = tempdir().unwrap();
        let store = SqliteCheckpointStore::open(directory.path().join("state.db"))
            .await
            .unwrap();
        let checkpoint = Checkpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            connector_name: "orders".into(),
            generation: Uuid::new_v4(),
            source_position: SourcePosition::Postgres(PostgresPosition {
                lsn: 42,
                commit_lsn: Some(42),
                transaction_id: Some(7),
                event_serial: 1,
                snapshot: false,
            }),
            snapshot_completed: true,
            config_fingerprint: "abc".into(),
            updated_at: SystemTime::now(),
            connector_state: Some(ConnectorStateEnvelope::new(
                "rustium.test",
                1,
                serde_json::json!({"schema": "v1"}),
            )),
        };
        store.save(&checkpoint).await.unwrap();
        assert_eq!(store.load("orders").await.unwrap(), Some(checkpoint));
        store.delete("orders").await.unwrap();
        assert!(store.load("orders").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_a_future_storage_schema_without_downgrading_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("future.db");
        let connection = Connection::open(&path).unwrap();
        let future_version = SQLITE_STORAGE_SCHEMA_VERSION + 1;
        connection
            .pragma_update(None, "user_version", future_version)
            .unwrap();
        drop(connection);

        let error = SqliteCheckpointStore::open(&path).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("newer than the supported version")
        );

        let connection = Connection::open(path).unwrap();
        let stored_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(stored_version, future_version);
    }

    #[test]
    fn loads_checkpoint_without_connector_state() {
        let checkpoint = Checkpoint {
            schema_version: 1,
            connector_name: "legacy".into(),
            generation: Uuid::new_v4(),
            source_position: SourcePosition::Postgres(PostgresPosition {
                lsn: 7,
                commit_lsn: Some(7),
                transaction_id: None,
                event_serial: 1,
                snapshot: false,
            }),
            snapshot_completed: true,
            config_fingerprint: "legacy".into(),
            updated_at: SystemTime::now(),
            connector_state: None,
        };
        let serialized = serde_json::to_value(&checkpoint).unwrap();
        assert!(serialized.get("connector_state").is_none());

        let loaded: Checkpoint = serde_json::from_value(serialized).unwrap();
        assert_eq!(loaded.connector_state, None);
    }
}
