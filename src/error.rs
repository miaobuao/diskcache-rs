#[derive(Debug, thiserror::Error)]
pub enum DiskCacheError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
    #[error("invalid namespace name: {0}")]
    InvalidNamespaceName(String),
}

pub type Result<T> = std::result::Result<T, DiskCacheError>;
