use std::collections::HashMap;

use rustium_config::ColumnTransformRule;
use rustium_core::{ChangeEvent, DataValue, Result};

use crate::schema_history::{PostgresColumnType, TableSchema};

#[derive(Debug, Clone)]
pub(crate) struct ColumnTransformer(rustium_column_transform::ColumnTransformer);

impl ColumnTransformer {
    pub(crate) fn new(rules: &[ColumnTransformRule]) -> Result<Self> {
        rustium_column_transform::ColumnTransformer::new(rules).map(Self)
    }

    pub(crate) fn transform_event(&self, event: &mut ChangeEvent, schema: &TableSchema) {
        self.0.transform_event(
            event,
            &schema.schema,
            &schema.table,
            &declared_lengths(schema),
        );
    }

    pub(crate) fn transform_value(
        &self,
        schema: &TableSchema,
        column: &str,
        value: DataValue,
    ) -> DataValue {
        let declared_length = schema
            .column_types
            .iter()
            .find(|column_type| column_type.name == column)
            .and_then(declared_character_length);
        self.0.transform_value(
            &schema.schema,
            &schema.table,
            &schema.event_schema,
            column,
            value,
            declared_length,
        )
    }
}

fn declared_lengths(schema: &TableSchema) -> HashMap<String, usize> {
    schema
        .column_types
        .iter()
        .filter_map(|column_type| {
            declared_character_length(column_type).map(|length| (column_type.name.clone(), length))
        })
        .collect()
}

fn declared_character_length(column_type: &PostgresColumnType) -> Option<usize> {
    match column_type.type_oid {
        18 => Some(1),
        1042 | 1043 if column_type.type_modifier >= 4 => {
            usize::try_from(column_type.type_modifier - 4).ok()
        }
        _ => None,
    }
}
