//! MySQL binary log source for Rustium.

mod file_signal;
mod schema_history;
mod source;
mod tls;

pub use source::MySqlSource;
