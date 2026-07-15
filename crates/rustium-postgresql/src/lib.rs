//! PostgreSQL logical replication source for Rustium.

mod incremental_snapshot;
mod schema_history;
mod source;

pub use source::PostgresSource;
