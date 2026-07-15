//! PostgreSQL logical replication source for Rustium.

mod schema_history;
mod source;

pub use source::PostgresSource;
