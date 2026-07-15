use std::collections::{HashMap, HashSet};

use rustium_config::MySqlSourceConfig;
use rustium_core::{ConnectorStateEnvelope, Error, EventSchema, FieldSchema, Result};
use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, CreateTable,
        CreateTableLikeKind, DataType, Expr, MySQLColumnPosition, ObjectName, ObjectType,
        RenameTableNameKind, Statement, TableConstraint,
    },
    dialect::MySqlDialect,
    parser::Parser,
};

pub(crate) const MYSQL_SCHEMA_HISTORY_FORMAT: &str = "rustium.mysql.schema-history";
const MYSQL_SCHEMA_HISTORY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableSchema {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) event_schema: EventSchema,
}

impl TableSchema {
    pub(crate) fn key(&self) -> (String, String) {
        (self.database.clone(), self.table.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MySqlSchemaHistoryState {
    tables: Vec<TableSchema>,
}

pub(crate) fn encode_schema_history(
    schemas: &HashMap<(String, String), TableSchema>,
) -> Result<ConnectorStateEnvelope> {
    let mut tables = schemas.values().cloned().collect::<Vec<_>>();
    tables.sort_by_key(TableSchema::key);
    let payload = serde_json::to_value(MySqlSchemaHistoryState { tables })?;
    Ok(ConnectorStateEnvelope::new(
        MYSQL_SCHEMA_HISTORY_FORMAT,
        MYSQL_SCHEMA_HISTORY_VERSION,
        payload,
    ))
}

pub(crate) fn decode_schema_history(
    envelope: &ConnectorStateEnvelope,
) -> Result<HashMap<(String, String), TableSchema>> {
    if envelope.format != MYSQL_SCHEMA_HISTORY_FORMAT {
        return Err(Error::State(format!(
            "MySQL checkpoint has connector state format {:?}, expected {:?}",
            envelope.format, MYSQL_SCHEMA_HISTORY_FORMAT
        )));
    }
    if envelope.version != MYSQL_SCHEMA_HISTORY_VERSION {
        return Err(Error::State(format!(
            "unsupported MySQL schema history version {}; expected {}",
            envelope.version, MYSQL_SCHEMA_HISTORY_VERSION
        )));
    }
    let state: MySqlSchemaHistoryState = serde_json::from_value(envelope.payload.clone())?;
    let mut schemas = HashMap::with_capacity(state.tables.len());
    for table in state.tables {
        let key = table.key();
        if schemas.insert(key.clone(), table).is_some() {
            return Err(Error::State(format!(
                "MySQL schema history contains duplicate table {}.{}",
                key.0, key.1
            )));
        }
    }
    Ok(schemas)
}

pub(crate) fn apply_ddl(
    schemas: &mut HashMap<(String, String), TableSchema>,
    ddl: &str,
    current_database: &str,
    config: &MySqlSourceConfig,
    connector_name: &str,
) -> Result<bool> {
    let statements = Parser::parse_sql(&MySqlDialect {}, ddl)
        .map_err(|error| Error::Source(format!("could not parse MySQL DDL {ddl:?}: {error}")))?;
    let mut staged = schemas.clone();
    let mut changed = false;
    for statement in statements {
        changed |= apply_statement(
            &mut staged,
            statement,
            current_database,
            config,
            connector_name,
        )?;
    }
    *schemas = staged;
    Ok(changed)
}

fn apply_statement(
    schemas: &mut HashMap<(String, String), TableSchema>,
    statement: Statement,
    current_database: &str,
    config: &MySqlSourceConfig,
    connector_name: &str,
) -> Result<bool> {
    match statement {
        Statement::CreateTable(create) => {
            apply_create_table(schemas, create, current_database, config, connector_name)
        }
        Statement::AlterTable(alter) => apply_alter_table(
            schemas,
            alter.name,
            alter.operations,
            current_database,
            config,
            connector_name,
        ),
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            if_exists,
            ..
        } => {
            let mut changed = false;
            for name in names {
                let key = table_key(&name, current_database)?;
                if !tracks_table(config, &key) {
                    continue;
                }
                let removed = schemas.remove(&key).is_some();
                if !removed && !if_exists {
                    return Err(unknown_table(&key));
                }
                changed |= removed;
            }
            Ok(changed)
        }
        Statement::RenameTable(renames) => {
            let mut changed = false;
            for rename in renames {
                changed |= rename_table(
                    schemas,
                    table_key(&rename.old_name, current_database)?,
                    table_key(&rename.new_name, current_database)?,
                    config,
                    connector_name,
                )?;
            }
            Ok(changed)
        }
        Statement::Truncate(_) => Ok(false),
        statement => Err(Error::Source(format!(
            "unsupported MySQL schema change statement: {statement}"
        ))),
    }
}

fn apply_create_table(
    schemas: &mut HashMap<(String, String), TableSchema>,
    create: CreateTable,
    current_database: &str,
    config: &MySqlSourceConfig,
    connector_name: &str,
) -> Result<bool> {
    let key = table_key(&create.name, current_database)?;
    if !tracks_table(config, &key) {
        return Ok(false);
    }
    if create.if_not_exists && schemas.contains_key(&key) {
        return Ok(false);
    }
    if schemas.contains_key(&key) {
        return Err(Error::Source(format!(
            "MySQL schema history already contains table {}.{}",
            key.0, key.1
        )));
    }

    let mut table = if let Some(like) = create.like {
        let like = match like {
            CreateTableLikeKind::Parenthesized(like) | CreateTableLikeKind::Plain(like) => like,
        };
        let source_key = table_key(&like.name, current_database)?;
        let mut table = schemas
            .get(&source_key)
            .cloned()
            .ok_or_else(|| unknown_table(&source_key))?;
        table.database.clone_from(&key.0);
        table.table.clone_from(&key.1);
        table.event_schema.name = event_schema_name(connector_name, &key);
        table.event_schema.version = 1;
        table
    } else {
        if create.columns.is_empty() || create.query.is_some() {
            return Err(Error::Source(format!(
                "cannot reconstruct MySQL schema for CREATE TABLE {} without explicit columns",
                create.name
            )));
        }
        let mut fields = create
            .columns
            .iter()
            .map(field_from_column_def)
            .collect::<Result<Vec<_>>>()?;
        apply_primary_key_constraints(&mut fields, &create.constraints)?;
        TableSchema {
            database: key.0.clone(),
            table: key.1.clone(),
            event_schema: EventSchema {
                name: event_schema_name(connector_name, &key),
                version: 1,
                fields,
            },
        }
    };
    table.database.clone_from(&key.0);
    table.table.clone_from(&key.1);
    schemas.insert(key, table);
    Ok(true)
}

fn apply_alter_table(
    schemas: &mut HashMap<(String, String), TableSchema>,
    name: ObjectName,
    operations: Vec<AlterTableOperation>,
    current_database: &str,
    config: &MySqlSourceConfig,
    connector_name: &str,
) -> Result<bool> {
    let original_key = table_key(&name, current_database)?;
    if !tracks_table(config, &original_key) {
        return Ok(false);
    }
    let mut table = schemas
        .remove(&original_key)
        .ok_or_else(|| unknown_table(&original_key))?;
    let original_schema = table.event_schema.clone();
    let mut target_key = original_key.clone();

    for operation in operations {
        match operation {
            AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            } => {
                let field = field_from_column_def(&column_def)?;
                if table
                    .event_schema
                    .fields
                    .iter()
                    .any(|existing| existing.name == field.name)
                {
                    if if_not_exists {
                        continue;
                    }
                    return Err(Error::Source(format!(
                        "MySQL schema history cannot add duplicate column {}.{}.{}",
                        target_key.0, target_key.1, field.name
                    )));
                }
                insert_field(&mut table.event_schema.fields, field, column_position)?;
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                for name in column_names {
                    let removed =
                        remove_field(&mut table.event_schema.fields, &name.value).is_some();
                    if !removed && !if_exists {
                        return Err(unknown_column(&target_key, &name.value));
                    }
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let field = find_field_mut(
                    &mut table.event_schema.fields,
                    &target_key,
                    &old_column_name.value,
                )?;
                field.name = new_column_name.value;
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                replace_field(
                    &mut table.event_schema.fields,
                    &target_key,
                    &old_name.value,
                    &new_name.value,
                    &data_type,
                    &options,
                    column_position,
                )?;
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                replace_field(
                    &mut table.event_schema.fields,
                    &target_key,
                    &col_name.value,
                    &col_name.value,
                    &data_type,
                    &options,
                    column_position,
                )?;
            }
            AlterTableOperation::AddConstraint { constraint, .. } => {
                apply_primary_key_constraints(
                    &mut table.event_schema.fields,
                    std::slice::from_ref(&constraint),
                )?;
            }
            AlterTableOperation::DropPrimaryKey { .. } => {
                for field in &mut table.event_schema.fields {
                    field.primary_key = false;
                }
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let field = find_field_mut(
                    &mut table.event_schema.fields,
                    &target_key,
                    &column_name.value,
                )?;
                match op {
                    AlterColumnOperation::SetNotNull => field.optional = false,
                    AlterColumnOperation::DropNotNull => field.optional = true,
                    AlterColumnOperation::SetDataType { data_type, .. } => {
                        field.type_name = type_name(&data_type)?;
                    }
                    AlterColumnOperation::SetDefault { .. }
                    | AlterColumnOperation::DropDefault
                    | AlterColumnOperation::AddGenerated { .. } => {}
                }
            }
            AlterTableOperation::RenameTable { table_name } => {
                let name = match table_name {
                    RenameTableNameKind::As(name) | RenameTableNameKind::To(name) => name,
                };
                target_key = table_key(&name, current_database)?;
                table.database.clone_from(&target_key.0);
                table.table.clone_from(&target_key.1);
                table.event_schema.name = event_schema_name(connector_name, &target_key);
            }
            AlterTableOperation::DropConstraint { .. }
            | AlterTableOperation::DropForeignKey { .. }
            | AlterTableOperation::DropIndex { .. }
            | AlterTableOperation::Algorithm { .. }
            | AlterTableOperation::Lock { .. }
            | AlterTableOperation::AutoIncrement { .. } => {}
            _ => {}
        }
    }

    let changed = table.event_schema != original_schema || target_key != original_key;
    if table.event_schema != original_schema {
        table.event_schema.version = original_schema.version.saturating_add(1);
    }
    if tracks_table(config, &target_key) {
        if target_key != original_key && schemas.contains_key(&target_key) {
            return Err(Error::Source(format!(
                "MySQL schema history cannot rename {}.{} over existing table {}.{}",
                original_key.0, original_key.1, target_key.0, target_key.1
            )));
        }
        schemas.insert(target_key, table);
    }
    Ok(changed)
}

fn rename_table(
    schemas: &mut HashMap<(String, String), TableSchema>,
    old_key: (String, String),
    new_key: (String, String),
    config: &MySqlSourceConfig,
    connector_name: &str,
) -> Result<bool> {
    if !tracks_table(config, &old_key) {
        return Ok(false);
    }
    let mut table = schemas
        .remove(&old_key)
        .ok_or_else(|| unknown_table(&old_key))?;
    if tracks_table(config, &new_key) && schemas.contains_key(&new_key) {
        return Err(Error::Source(format!(
            "MySQL schema history cannot rename {}.{} over existing table {}.{}",
            old_key.0, old_key.1, new_key.0, new_key.1
        )));
    }
    table.database.clone_from(&new_key.0);
    table.table.clone_from(&new_key.1);
    table.event_schema.name = event_schema_name(connector_name, &new_key);
    table.event_schema.version = table.event_schema.version.saturating_add(1);
    if tracks_table(config, &new_key) {
        schemas.insert(new_key, table);
    }
    Ok(true)
}

fn field_from_column_def(column: &ColumnDef) -> Result<FieldSchema> {
    let options = column
        .options
        .iter()
        .map(|option| &option.option)
        .collect::<Vec<_>>();
    field_from_parts(
        &column.name.value,
        &column.data_type,
        options.into_iter(),
        false,
    )
}

fn field_from_parts<'a>(
    name: &str,
    data_type: &DataType,
    options: impl Iterator<Item = &'a ColumnOption>,
    preserve_primary_key: bool,
) -> Result<FieldSchema> {
    let mut optional = true;
    let mut primary_key = preserve_primary_key;
    for option in options {
        match option {
            ColumnOption::Null => optional = true,
            ColumnOption::NotNull => optional = false,
            ColumnOption::PrimaryKey(_) => {
                optional = false;
                primary_key = true;
            }
            _ => {}
        }
    }
    if primary_key {
        optional = false;
    }
    Ok(FieldSchema {
        name: name.into(),
        type_name: type_name(data_type)?,
        optional,
        primary_key,
    })
}

fn apply_primary_key_constraints(
    fields: &mut [FieldSchema],
    constraints: &[TableConstraint],
) -> Result<()> {
    let mut primary_keys = HashSet::new();
    for constraint in constraints {
        let TableConstraint::PrimaryKey(primary_key) = constraint else {
            continue;
        };
        for column in &primary_key.columns {
            let Expr::Identifier(identifier) = &column.column.expr else {
                return Err(Error::Source(format!(
                    "unsupported expression in MySQL primary key: {}",
                    column.column.expr
                )));
            };
            primary_keys.insert(identifier.value.as_str());
        }
    }
    for name in primary_keys {
        let field = fields
            .iter_mut()
            .find(|field| field.name == name)
            .ok_or_else(|| {
                Error::Source(format!(
                    "MySQL primary key references unknown column {name}"
                ))
            })?;
        field.primary_key = true;
        field.optional = false;
    }
    Ok(())
}

fn replace_field(
    fields: &mut Vec<FieldSchema>,
    table_key: &(String, String),
    old_name: &str,
    new_name: &str,
    data_type: &DataType,
    options: &[ColumnOption],
    position: Option<MySQLColumnPosition>,
) -> Result<()> {
    let index = fields
        .iter()
        .position(|field| field.name == old_name)
        .ok_or_else(|| unknown_column(table_key, old_name))?;
    let old = fields.remove(index);
    let field = field_from_parts(new_name, data_type, options.iter(), old.primary_key)?;
    if let Some(position) = position {
        insert_field(fields, field, Some(position))?;
    } else {
        fields.insert(index.min(fields.len()), field);
    }
    Ok(())
}

fn insert_field(
    fields: &mut Vec<FieldSchema>,
    field: FieldSchema,
    position: Option<MySQLColumnPosition>,
) -> Result<()> {
    let index = match position {
        None => fields.len(),
        Some(MySQLColumnPosition::First) => 0,
        Some(MySQLColumnPosition::After(column)) => fields
            .iter()
            .position(|field| field.name == column.value)
            .map(|index| index + 1)
            .ok_or_else(|| {
                Error::Source(format!(
                    "MySQL column position references unknown column {}",
                    column.value
                ))
            })?,
    };
    fields.insert(index, field);
    Ok(())
}

fn remove_field(fields: &mut Vec<FieldSchema>, name: &str) -> Option<FieldSchema> {
    fields
        .iter()
        .position(|field| field.name == name)
        .map(|index| fields.remove(index))
}

fn find_field_mut<'a>(
    fields: &'a mut [FieldSchema],
    table_key: &(String, String),
    name: &str,
) -> Result<&'a mut FieldSchema> {
    fields
        .iter_mut()
        .find(|field| field.name == name)
        .ok_or_else(|| unknown_column(table_key, name))
}

fn table_key(name: &ObjectName, current_database: &str) -> Result<(String, String)> {
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|identifier| identifier.value.clone())
                .ok_or_else(|| {
                    Error::Source(format!("unsupported dynamic MySQL object name {name}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    match parts.as_slice() {
        [table] if !current_database.is_empty() => Ok((current_database.into(), table.clone())),
        [database, table] => Ok((database.clone(), table.clone())),
        [table] => Err(Error::Source(format!(
            "MySQL DDL table {table:?} has no database context"
        ))),
        _ => Err(Error::Source(format!(
            "unsupported MySQL table name {name}"
        ))),
    }
}

fn type_name(data_type: &DataType) -> Result<String> {
    if *data_type == DataType::Unspecified {
        return Err(Error::Source(
            "MySQL schema history encountered a column without a data type".into(),
        ));
    }
    Ok(data_type.to_string().to_ascii_lowercase())
}

fn event_schema_name(connector_name: &str, key: &(String, String)) -> String {
    format!("{connector_name}.{}.{}.Envelope", key.0, key.1)
}

fn tracks_database(config: &MySqlSourceConfig, database: &str) -> bool {
    !matches!(
        database,
        "information_schema" | "mysql" | "performance_schema" | "sys"
    ) && (config.databases.is_empty() || config.databases.iter().any(|name| name == database))
}

fn tracks_table(config: &MySqlSourceConfig, key: &(String, String)) -> bool {
    tracks_database(config, &key.0) && config.tables.includes(&key.0, &key.1)
}

fn unknown_table(key: &(String, String)) -> Error {
    Error::Source(format!(
        "MySQL schema history has no table {}.{}",
        key.0, key.1
    ))
}

fn unknown_column(key: &(String, String), column: &str) -> Error {
    Error::Source(format!(
        "MySQL schema history has no column {}.{}.{}",
        key.0, key.1, column
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rustium_config::TableSelection;

    use super::*;

    fn config() -> MySqlSourceConfig {
        MySqlSourceConfig {
            hostname: "localhost".into(),
            port: 3306,
            username: "rustium".into(),
            password: "secret".into(),
            databases: vec!["inventory".into()],
            server_id: 5_401,
            tables: TableSelection::default(),
            ssl_mode: "disabled".into(),
            ssl_ca: None,
            ssl_cert: None,
            ssl_key: None,
            connect_timeout: Duration::from_secs(10),
            connect_keep_alive: true,
            connect_keep_alive_interval: Duration::from_secs(1),
            reconnect_max_attempts: 3,
            schema_history_skip_unparseable_ddl: false,
            gtid_source_includes: Vec::new(),
            gtid_source_excludes: Vec::new(),
            gtid_source_filter_dml_events: true,
            heartbeat_interval: Duration::ZERO,
            heartbeat_action_query: None,
            heartbeat_topics_prefix: "__debezium-heartbeat".into(),
            heartbeat_topic_name: None,
        }
    }

    #[test]
    fn ignores_ddl_for_unselected_tables() {
        let mut config = config();
        config.tables.include = vec![r"inventory\.orders".into()];
        let mut schemas = HashMap::new();

        let changed = apply_ddl(
            &mut schemas,
            "CREATE TABLE customers (id BIGINT PRIMARY KEY)",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();

        assert!(!changed);
        assert!(schemas.is_empty());
    }

    #[test]
    fn applies_destructive_mysql_ddl_in_order() {
        let config = config();
        let mut schemas = HashMap::new();
        apply_ddl(
            &mut schemas,
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer VARCHAR(100) NOT NULL, amount DECIMAL(10,2) NOT NULL)",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();
        apply_ddl(
            &mut schemas,
            "ALTER TABLE orders DROP COLUMN customer, ADD COLUMN status VARCHAR(20) NOT NULL AFTER amount",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();

        let schema = &schemas
            .get(&("inventory".into(), "orders".into()))
            .unwrap()
            .event_schema;
        assert_eq!(schema.version, 2);
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "amount", "status"]
        );
        assert_eq!(schema.fields[0].type_name, "bigint");
        assert!(schema.fields[0].primary_key);
        assert!(!schema.fields[2].optional);
    }

    #[test]
    fn round_trips_versioned_schema_history() {
        let config = config();
        let mut schemas = HashMap::new();
        apply_ddl(
            &mut schemas,
            "CREATE TABLE inventory.orders (id BIGINT PRIMARY KEY, amount DECIMAL(10,2))",
            "",
            &config,
            "inventory-mysql",
        )
        .unwrap();

        let envelope = encode_schema_history(&schemas).unwrap();
        assert_eq!(envelope.format, MYSQL_SCHEMA_HISTORY_FORMAT);
        assert_eq!(decode_schema_history(&envelope).unwrap(), schemas);
    }

    #[test]
    fn applies_mysql_column_and_table_renames() {
        let config = config();
        let mut schemas = HashMap::new();
        apply_ddl(
            &mut schemas,
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer VARCHAR(100), amount DECIMAL(10,2))",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();
        apply_ddl(
            &mut schemas,
            "ALTER TABLE orders ADD COLUMN note TEXT FIRST, CHANGE COLUMN customer buyer VARCHAR(120) NOT NULL, MODIFY COLUMN amount DECIMAL(12,3), RENAME COLUMN buyer TO customer_name",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();
        apply_ddl(
            &mut schemas,
            "RENAME TABLE orders TO purchases",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();

        let schema = &schemas
            .get(&("inventory".into(), "purchases".into()))
            .unwrap()
            .event_schema;
        assert_eq!(schema.version, 3);
        assert_eq!(schema.name, "inventory-mysql.inventory.purchases.Envelope");
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["note", "id", "customer_name", "amount"]
        );
        assert_eq!(schema.fields[2].type_name, "varchar(120)");
        assert!(!schema.fields[2].optional);
        assert_eq!(schema.fields[3].type_name, "decimal(12,3)");

        apply_ddl(
            &mut schemas,
            "DROP TABLE purchases",
            "inventory",
            &config,
            "inventory-mysql",
        )
        .unwrap();
        assert!(schemas.is_empty());
    }
}
