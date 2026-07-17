//! PostgreSQL logical replication source for Rustium.

mod column_transform;
mod file_signal;
mod incremental_snapshot;
mod schema_history;
mod source;

pub use source::PostgresSource;
