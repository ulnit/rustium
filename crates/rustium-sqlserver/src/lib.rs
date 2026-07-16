//! SQL Server Change Data Capture source for Rustium.

mod file_signal;
mod source;
mod state;

pub use source::SqlServerSource;
