use std::collections::HashMap;

use pg_walstream::RelationColumn;
use rustium_core::{ConnectorStateEnvelope, Error, EventSchema, FieldSchema, Result};
use serde::{Deserialize, Serialize};

pub(crate) const POSTGRES_SCHEMA_HISTORY_FORMAT: &str = "rustium.postgresql.schema-history";
const POSTGRES_SCHEMA_HISTORY_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostgresColumnType {
    pub(crate) name: String,
    pub(crate) type_oid: u32,
    pub(crate) type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableSchema {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) event_schema: EventSchema,
    pub(crate) column_types: Vec<PostgresColumnType>,
}

impl TableSchema {
    pub(crate) fn key(&self) -> (String, String) {
        (self.schema.clone(), self.table.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IncrementalSnapshotProgress {
    pub(crate) signal_id: String,
    pub(crate) data_collections: Vec<String>,
    pub(crate) current_collection: usize,
    pub(crate) last_key: Option<Vec<String>>,
    pub(crate) maximum_key: Option<Vec<String>>,
    pub(crate) chunk_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostgresConnectorState {
    pub(crate) schemas: HashMap<(String, String), TableSchema>,
    pub(crate) incremental_snapshot: Option<IncrementalSnapshotProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresSchemaHistoryState {
    tables: Vec<TableSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incremental_snapshot: Option<IncrementalSnapshotProgress>,
}

pub(crate) fn encode_schema_history(
    schemas: &HashMap<(String, String), TableSchema>,
) -> Result<ConnectorStateEnvelope> {
    encode_connector_state(schemas, None)
}

pub(crate) fn encode_connector_state(
    schemas: &HashMap<(String, String), TableSchema>,
    incremental_snapshot: Option<&IncrementalSnapshotProgress>,
) -> Result<ConnectorStateEnvelope> {
    let mut tables = schemas.values().cloned().collect::<Vec<_>>();
    tables.sort_by_key(TableSchema::key);
    let payload = serde_json::to_value(PostgresSchemaHistoryState {
        tables,
        incremental_snapshot: incremental_snapshot.cloned(),
    })?;
    Ok(ConnectorStateEnvelope::new(
        POSTGRES_SCHEMA_HISTORY_FORMAT,
        POSTGRES_SCHEMA_HISTORY_VERSION,
        payload,
    ))
}

#[cfg(test)]
pub(crate) fn decode_schema_history(
    envelope: &ConnectorStateEnvelope,
) -> Result<HashMap<(String, String), TableSchema>> {
    Ok(decode_connector_state(envelope)?.schemas)
}

pub(crate) fn decode_connector_state(
    envelope: &ConnectorStateEnvelope,
) -> Result<PostgresConnectorState> {
    if envelope.format != POSTGRES_SCHEMA_HISTORY_FORMAT {
        return Err(Error::State(format!(
            "PostgreSQL checkpoint has connector state format {:?}, expected {:?}",
            envelope.format, POSTGRES_SCHEMA_HISTORY_FORMAT
        )));
    }
    if !matches!(envelope.version, 1 | POSTGRES_SCHEMA_HISTORY_VERSION) {
        return Err(Error::State(format!(
            "unsupported PostgreSQL schema history version {}; expected 1 or {}",
            envelope.version, POSTGRES_SCHEMA_HISTORY_VERSION,
        )));
    }
    let state: PostgresSchemaHistoryState = serde_json::from_value(envelope.payload.clone())?;
    let mut schemas = HashMap::with_capacity(state.tables.len());
    for table in state.tables {
        let key = table.key();
        if schemas.insert(key.clone(), table).is_some() {
            return Err(Error::State(format!(
                "PostgreSQL schema history contains duplicate table {}.{}",
                key.0, key.1
            )));
        }
    }
    Ok(PostgresConnectorState {
        schemas,
        incremental_snapshot: state.incremental_snapshot,
    })
}

pub(crate) fn schema_from_relation(
    namespace: &str,
    relation_name: &str,
    schema_name: String,
    columns: &[RelationColumn],
    resolved_type_names: &[String],
    previous: Option<&TableSchema>,
    catalog: Option<&TableSchema>,
) -> Result<TableSchema> {
    if columns.len() != resolved_type_names.len() {
        return Err(Error::Invariant(format!(
            "PostgreSQL relation has {} columns but {} resolved type names",
            columns.len(),
            resolved_type_names.len()
        )));
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut column_types = Vec::with_capacity(columns.len());
    for (column, resolved_type_name) in columns.iter().zip(resolved_type_names) {
        let previous_field = matching_field(previous, column);
        let catalog_field = matching_field(catalog, column);
        let metadata = catalog_field.or(previous_field);
        fields.push(FieldSchema {
            name: column.name.to_string(),
            type_name: metadata.map_or_else(
                || resolved_type_name.clone(),
                |field| field.type_name.clone(),
            ),
            optional: metadata.is_none_or(|field| field.optional),
            primary_key: column.is_key,
        });
        column_types.push(PostgresColumnType {
            name: column.name.to_string(),
            type_oid: column.type_id,
            type_modifier: column.type_modifier,
        });
    }

    Ok(TableSchema {
        schema: namespace.into(),
        table: relation_name.into(),
        event_schema: EventSchema {
            name: schema_name,
            version: 1,
            fields,
        },
        column_types,
    })
}

fn matching_field<'a>(
    schema: Option<&'a TableSchema>,
    column: &RelationColumn,
) -> Option<&'a FieldSchema> {
    let schema = schema?;
    let column_type = schema.column_types.iter().find(|candidate| {
        candidate.name == column.name.as_ref()
            && candidate.type_oid == column.type_id
            && candidate.type_modifier == column.type_modifier
    })?;
    schema
        .event_schema
        .fields
        .iter()
        .find(|field| field.name == column_type.name)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn baseline() -> TableSchema {
        TableSchema {
            schema: "public".into(),
            table: "orders".into(),
            event_schema: EventSchema {
                name: "test.public.orders.Envelope".into(),
                version: 1,
                fields: vec![
                    FieldSchema {
                        name: "id".into(),
                        type_name: "bigint".into(),
                        optional: false,
                        primary_key: true,
                    },
                    FieldSchema {
                        name: "customer".into(),
                        type_name: "text".into(),
                        optional: false,
                        primary_key: false,
                    },
                ],
            },
            column_types: vec![
                PostgresColumnType {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                },
                PostgresColumnType {
                    name: "customer".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    #[test]
    fn round_trips_versioned_postgres_schema_history() {
        let table = baseline();
        let schemas = HashMap::from([(table.key(), table)]);
        let envelope = encode_schema_history(&schemas).unwrap();

        assert_eq!(envelope.format, POSTGRES_SCHEMA_HISTORY_FORMAT);
        assert_eq!(envelope.version, 2);
        assert!(envelope.payload.get("incremental_snapshot").is_none());
        assert_eq!(decode_schema_history(&envelope).unwrap(), schemas);
    }

    #[test]
    fn reads_version_one_schema_history_without_incremental_progress() {
        let table = baseline();
        let schemas = HashMap::from([(table.key(), table.clone())]);
        let envelope = ConnectorStateEnvelope::new(
            POSTGRES_SCHEMA_HISTORY_FORMAT,
            1,
            serde_json::json!({ "tables": [table] }),
        );

        let decoded = decode_connector_state(&envelope).unwrap();
        assert_eq!(decoded.schemas, schemas);
        assert_eq!(decoded.incremental_snapshot, None);
    }

    #[test]
    fn round_trips_version_two_incremental_snapshot_progress() {
        let table = baseline();
        let schemas = HashMap::from([(table.key(), table)]);
        let progress = IncrementalSnapshotProgress {
            signal_id: "snapshot-1".into(),
            data_collections: vec!["public.orders".into(), "public.customers".into()],
            current_collection: 1,
            last_key: Some(vec!["acme".into(), "42".into()]),
            maximum_key: Some(vec!["zenith".into(), "9000".into()]),
            chunk_sequence: 7,
        };

        let envelope = encode_connector_state(&schemas, Some(&progress)).unwrap();
        let decoded = decode_connector_state(&envelope).unwrap();

        assert_eq!(envelope.version, 2);
        assert_eq!(decoded.schemas, schemas);
        assert_eq!(decoded.incremental_snapshot, Some(progress));
    }

    #[test]
    fn rebuilds_historical_column_layout_from_relation() {
        let previous = baseline();
        let catalog = TableSchema {
            event_schema: EventSchema {
                fields: vec![
                    previous.event_schema.fields[0].clone(),
                    FieldSchema {
                        name: "status".into(),
                        type_name: "text".into(),
                        optional: false,
                        primary_key: false,
                    },
                ],
                ..previous.event_schema.clone()
            },
            column_types: vec![
                previous.column_types[0].clone(),
                PostgresColumnType {
                    name: "status".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
            ..previous.clone()
        };
        let columns = vec![
            RelationColumn {
                name: Arc::from("id"),
                type_id: 20,
                type_modifier: -1,
                is_key: true,
            },
            RelationColumn {
                name: Arc::from("status"),
                type_id: 25,
                type_modifier: -1,
                is_key: false,
            },
        ];

        let evolved = schema_from_relation(
            "public",
            "orders",
            previous.event_schema.name.clone(),
            &columns,
            &["bigint".into(), "text".into()],
            Some(&previous),
            Some(&catalog),
        )
        .unwrap();

        assert_eq!(
            evolved
                .event_schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "status"]
        );
        assert!(!evolved.event_schema.fields[1].optional);
    }
}
