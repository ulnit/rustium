use std::collections::BTreeMap;

use rustium_core::{ConnectorStateEnvelope, Error, Result};
use serde::{Deserialize, Serialize};

pub(crate) const SQLSERVER_STATE_FORMAT: &str = "rustium.sqlserver.connector-state";
const SQLSERVER_STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum SqlServerKeyValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float64(u64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Time(String),
    Timestamp(String),
    Uuid(uuid::Uuid),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IncrementalSnapshotProgress {
    pub(crate) signal_id: String,
    pub(crate) data_collections: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) additional_conditions: BTreeMap<String, String>,
    pub(crate) current_collection: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) last_key: Option<Vec<SqlServerKeyValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) maximum_key: Option<Vec<SqlServerKeyValue>>,
    #[serde(default)]
    pub(crate) chunk_sequence: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct SqlServerConnectorState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incremental_snapshot: Option<IncrementalSnapshotProgress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    completed_signal_ids: Vec<String>,
}

pub(crate) fn encode_connector_state(
    incremental_snapshot: Option<&IncrementalSnapshotProgress>,
    completed_signal_ids: &[String],
) -> Result<ConnectorStateEnvelope> {
    Ok(ConnectorStateEnvelope::new(
        SQLSERVER_STATE_FORMAT,
        SQLSERVER_STATE_VERSION,
        serde_json::to_value(SqlServerConnectorState {
            incremental_snapshot: incremental_snapshot.cloned(),
            completed_signal_ids: completed_signal_ids.to_vec(),
        })?,
    ))
}

pub(crate) fn decode_connector_state(
    envelope: &ConnectorStateEnvelope,
) -> Result<(Option<IncrementalSnapshotProgress>, Vec<String>)> {
    if envelope.format != SQLSERVER_STATE_FORMAT {
        return Err(Error::State(format!(
            "SQL Server checkpoint has connector state format {:?}, expected {:?}",
            envelope.format, SQLSERVER_STATE_FORMAT
        )));
    }
    if envelope.version != SQLSERVER_STATE_VERSION {
        return Err(Error::State(format!(
            "unsupported SQL Server connector state version {}; expected {}",
            envelope.version, SQLSERVER_STATE_VERSION
        )));
    }
    let state: SqlServerConnectorState = serde_json::from_value(envelope.payload.clone())?;
    Ok((state.incremental_snapshot, state.completed_signal_ids))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_sqlserver_incremental_state() {
        let progress = IncrementalSnapshotProgress {
            signal_id: "snapshot-1".into(),
            data_collections: vec!["dbo.orders".into()],
            additional_conditions: BTreeMap::from([(
                "dbo\\.orders".into(),
                "status = 'open'".into(),
            )]),
            current_collection: 0,
            last_key: Some(vec![SqlServerKeyValue::Int64(42)]),
            maximum_key: Some(vec![SqlServerKeyValue::Int64(100)]),
            chunk_sequence: 3,
            paused: true,
        };
        let completed = vec!["snapshot-0".into()];
        let envelope = encode_connector_state(Some(&progress), &completed).unwrap();
        let decoded = decode_connector_state(&envelope).unwrap();
        assert_eq!(decoded, (Some(progress), completed));
    }
}
