//! MySQL binary log source for Rustium.

mod file_signal;
mod schema_history;
mod source;

pub use source::MySqlSource;
