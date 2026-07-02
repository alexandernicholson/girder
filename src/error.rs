use thiserror::Error;

#[derive(Debug, Error)]
pub enum GirderError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt {what}: {detail}")]
    Corrupt { what: &'static str, detail: String },
    #[error("encode: {0}")]
    Encode(String),
    #[error("engine shut down")]
    ShutDown,
    #[error("config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, GirderError>;
