use std::path::PathBuf;

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
    #[error("blob is missing: {0}")]
    BlobMissing(PathBuf),
    #[error("corrupt blob data at {path:?}: expected checksum {expected}, got {actual}")]
    BlobChecksumMismatch {
        path: PathBuf,
        expected: u32,
        actual: u32,
    },
}

pub type Result<T> = std::result::Result<T, DiskCacheError>;
