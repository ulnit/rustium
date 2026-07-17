use std::collections::{BTreeMap, HashMap};

use pg_walstream::RelationColumn;
use rustium_core::{ConnectorStateEnvelope, Error, EventSchema, FieldSchema, Result};
use serde::{Deserialize, Serialize};

pub(crate) const POSTGRES_SCHEMA_HISTORY_FORMAT: &str = "rustium.postgresql.schema-history";
const POSTGRES_SCHEMA_HISTORY_VERSION: u32 = 6;

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) opaque_columns: Vec<String>,
}

impl TableSchema {
    pub(crate) fn key(&self) -> (String, String) {
        (self.schema.clone(), self.table.clone())
    }
}

pub(crate) fn postgres_field_type_name(type_name: &str, money_fraction_digits: i16) -> String {
    let trimmed = type_name.trim();
    let (scalar, array) = trimmed
        .strip_suffix("[]")
        .map_or((trimmed, false), |scalar| (scalar, true));
    let base = scalar
        .split('(')
        .next()
        .unwrap_or(scalar)
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or(scalar)
        .trim()
        .trim_matches('"');
    if base == "money" {
        format!(
            "money({money_fraction_digits}){}",
            if array { "[]" } else { "" }
        )
    } else {
        type_name.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IncrementalSnapshotProgress {
    pub(crate) signal_id: String,
    pub(crate) data_collections: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) additional_conditions: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) surrogate_key: Option<String>,
    pub(crate) current_collection: usize,
    pub(crate) last_key: Option<Vec<String>>,
    pub(crate) maximum_key: Option<Vec<String>>,
    pub(crate) chunk_sequence: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostgresConnectorState {
    pub(crate) schemas: HashMap<(String, String), TableSchema>,
    pub(crate) incremental_snapshot: Option<IncrementalSnapshotProgress>,
    pub(crate) completed_signal_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresSchemaHistoryState {
    tables: Vec<TableSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incremental_snapshot: Option<IncrementalSnapshotProgress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    completed_signal_ids: Vec<String>,
}

pub(crate) fn encode_schema_history(
    schemas: &HashMap<(String, String), TableSchema>,
) -> Result<ConnectorStateEnvelope> {
    encode_connector_state(schemas, None, &[])
}

pub(crate) fn encode_connector_state(
    schemas: &HashMap<(String, String), TableSchema>,
    incremental_snapshot: Option<&IncrementalSnapshotProgress>,
    completed_signal_ids: &[String],
) -> Result<ConnectorStateEnvelope> {
    let mut tables = schemas.values().cloned().collect::<Vec<_>>();
    tables.sort_by_key(TableSchema::key);
    let payload = serde_json::to_value(PostgresSchemaHistoryState {
        tables,
        incremental_snapshot: incremental_snapshot.cloned(),
        completed_signal_ids: completed_signal_ids.to_vec(),
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
    if !(1..=POSTGRES_SCHEMA_HISTORY_VERSION).contains(&envelope.version) {
        return Err(Error::State(format!(
            "unsupported PostgreSQL schema history version {}; expected 1 through {}",
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
        completed_signal_ids: state.completed_signal_ids,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn schema_from_relation(
    namespace: &str,
    relation_name: &str,
    schema_name: String,
    columns: &[RelationColumn],
    resolved_types: &[(String, bool)],
    include_unknown_datatypes: bool,
    money_fraction_digits: i16,
    previous: Option<&TableSchema>,
    catalog: Option<&TableSchema>,
) -> Result<TableSchema> {
    if columns.len() != resolved_types.len() {
        return Err(Error::Invariant(format!(
            "PostgreSQL relation has {} columns but {} resolved type names",
            columns.len(),
            resolved_types.len()
        )));
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut column_types = Vec::with_capacity(columns.len());
    let mut opaque_columns = Vec::new();
    for (column, (resolved_type_name, supported)) in columns.iter().zip(resolved_types) {
        let catalog_field = matching_field(catalog, column);
        let previous_field = matching_field(previous, column);
        let metadata = catalog_field.or(previous_field);
        if *supported {
            fields.push(FieldSchema {
                name: column.name.to_string(),
                type_name: catalog_field
                    .or_else(|| {
                        previous_field.filter(|_| {
                            previous.is_none_or(|schema| {
                                !schema
                                    .opaque_columns
                                    .iter()
                                    .any(|name| name == column.name.as_ref())
                            })
                        })
                    })
                    .map_or_else(
                        || postgres_field_type_name(resolved_type_name, money_fraction_digits),
                        |field| postgres_field_type_name(&field.type_name, money_fraction_digits),
                    ),
                optional: metadata.is_none_or(|field| field.optional),
                primary_key: column.is_key,
            });
        } else if include_unknown_datatypes {
            fields.push(FieldSchema {
                name: column.name.to_string(),
                type_name: "bytea".into(),
                optional: metadata.is_none_or(|field| field.optional),
                primary_key: column.is_key,
            });
            opaque_columns.push(column.name.to_string());
        }
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
        opaque_columns,
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
            opaque_columns: Vec::new(),
        }
    }

    #[test]
    fn round_trips_versioned_postgres_schema_history() {
        let table = baseline();
        let schemas = HashMap::from([(table.key(), table)]);
        let envelope = encode_schema_history(&schemas).unwrap();

        assert_eq!(envelope.format, POSTGRES_SCHEMA_HISTORY_FORMAT);
        assert_eq!(envelope.version, 6);
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
        assert!(decoded.completed_signal_ids.is_empty());
    }

    #[test]
    fn round_trips_version_six_incremental_snapshot_progress() {
        let table = baseline();
        let schemas = HashMap::from([(table.key(), table)]);
        let progress = IncrementalSnapshotProgress {
            signal_id: "snapshot-1".into(),
            data_collections: vec!["public.orders".into(), "public.customers".into()],
            additional_conditions: BTreeMap::from([(
                "public.orders".into(),
                "status = 'open'".into(),
            )]),
            surrogate_key: Some("sequence_id".into()),
            current_collection: 1,
            last_key: Some(vec!["acme".into(), "42".into()]),
            maximum_key: Some(vec!["zenith".into(), "9000".into()]),
            chunk_sequence: 7,
            paused: true,
        };

        let envelope =
            encode_connector_state(&schemas, Some(&progress), &["snapshot-0".into()]).unwrap();
        let decoded = decode_connector_state(&envelope).unwrap();

        assert_eq!(envelope.version, 6);
        assert_eq!(decoded.schemas, schemas);
        assert_eq!(decoded.incremental_snapshot, Some(progress));
        assert_eq!(decoded.completed_signal_ids, ["snapshot-0"]);
    }

    #[test]
    fn reads_version_two_incremental_progress_with_new_defaults() {
        let table = baseline();
        let envelope = ConnectorStateEnvelope::new(
            POSTGRES_SCHEMA_HISTORY_FORMAT,
            2,
            serde_json::json!({
                "tables": [table],
                "incremental_snapshot": {
                    "signal_id": "snapshot-2",
                    "data_collections": ["public.orders"],
                    "current_collection": 0,
                    "last_key": ["42"],
                    "maximum_key": ["100"],
                    "chunk_sequence": 3
                }
            }),
        );

        let progress = decode_connector_state(&envelope)
            .unwrap()
            .incremental_snapshot
            .unwrap();
        assert!(progress.additional_conditions.is_empty());
        assert_eq!(progress.surrogate_key, None);
        assert!(!progress.paused);
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
            &[("bigint".into(), true), ("text".into(), true)],
            false,
            2,
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

    #[test]
    fn omits_or_captures_unknown_relation_columns() {
        let columns = vec![
            RelationColumn {
                name: Arc::from("id"),
                type_id: 20,
                type_modifier: -1,
                is_key: true,
            },
            RelationColumn {
                name: Arc::from("payload"),
                type_id: 1_640_123,
                type_modifier: -1,
                is_key: false,
            },
        ];
        let resolved = [("bigint".into(), true), ("public.payload".into(), false)];

        let omitted = schema_from_relation(
            "public",
            "events",
            "test.public.events.Envelope".into(),
            &columns,
            &resolved,
            false,
            2,
            None,
            None,
        )
        .unwrap();
        assert_eq!(omitted.event_schema.fields.len(), 1);
        assert!(omitted.opaque_columns.is_empty());
        assert_eq!(omitted.column_types.len(), 2);

        let captured = schema_from_relation(
            "public",
            "events",
            "test.public.events.Envelope".into(),
            &columns,
            &resolved,
            true,
            2,
            None,
            None,
        )
        .unwrap();
        assert_eq!(captured.event_schema.fields.len(), 2);
        assert_eq!(captured.event_schema.fields[1].type_name, "bytea");
        assert_eq!(captured.opaque_columns, ["payload"]);
    }

    #[test]
    fn applies_configured_money_scale_to_relation_schemas() {
        let columns = [RelationColumn {
            name: Arc::from("amount"),
            type_id: 790,
            type_modifier: -1,
            is_key: false,
        }];
        let schema = schema_from_relation(
            "public",
            "payments",
            "test.public.payments.Envelope".into(),
            &columns,
            &[("money".into(), true)],
            false,
            4,
            None,
            None,
        )
        .unwrap();
        assert_eq!(schema.event_schema.fields[0].type_name, "money(4)");
        assert_eq!(
            postgres_field_type_name("pg_catalog.money[]", 1),
            "money(1)[]"
        );
    }

    #[test]
    fn upgrades_historical_opaque_columns_when_the_type_becomes_supported() {
        let mut previous = baseline();
        previous.event_schema.fields[1].type_name = "bytea".into();
        previous.opaque_columns.push("customer".into());
        let column = RelationColumn {
            name: Arc::from("customer"),
            type_id: 25,
            type_modifier: -1,
            is_key: false,
        };

        let upgraded = schema_from_relation(
            "public",
            "orders",
            previous.event_schema.name.clone(),
            &[column],
            &[("text".into(), true)],
            true,
            2,
            Some(&previous),
            None,
        )
        .unwrap();

        assert_eq!(upgraded.event_schema.fields[0].type_name, "text");
        assert!(upgraded.opaque_columns.is_empty());
    }
}
