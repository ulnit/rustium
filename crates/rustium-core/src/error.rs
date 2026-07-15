use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("source error: {0}")]
    Source(String),
    #[error("encoding error: {0}")]
    Encoding(String),
    #[error("sink error: {0}")]
    Sink(String),
    #[error("state error: {0}")]
    State(String),
    #[error("runtime invariant violated: {0}")]
    Invariant(String),
    #[error("connector cancelled")]
    Cancelled,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
