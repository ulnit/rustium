use std::collections::HashMap;

use rustium_config::ColumnTransformRule;
use rustium_core::{ChangeEvent, Result};

use crate::schema_history::TableSchema;

#[derive(Debug, Clone)]
pub(crate) struct ColumnTransformer(rustium_column_transform::ColumnTransformer);

impl ColumnTransformer {
    pub(crate) fn new(rules: &[ColumnTransformRule]) -> Result<Self> {
        rustium_column_transform::ColumnTransformer::new(rules).map(Self)
    }

    pub(crate) fn transform_event(&self, event: &mut ChangeEvent, schema: &TableSchema) {
        self.0
            .transform_event(event, &schema.database, &schema.table, &HashMap::new());
    }

    pub(crate) fn transform_row(&self, row: &mut rustium_core::Row, schema: &TableSchema) {
        self.0.transform_row(
            row,
            &schema.database,
            &schema.table,
            &schema.event_schema,
            &HashMap::new(),
        );
    }
}
