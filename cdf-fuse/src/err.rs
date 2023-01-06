use log::error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("Cognite Error {0}")]
    Cognite(#[from] cognite::Error),
    #[error("Directory not found")]
    DirectoryNotFound,
    #[error("File not found")]
    FileNotFound,
    #[error("IOerror from cache files")]
    Io(#[from] std::io::Error),
    #[error("Invalid path")]
    InvalidPath,
    #[error("Directory not empty")]
    NotEmpty,
    #[error("Node with name already exists")]
    Conflict,
}

impl FsError {
    pub fn as_code(&self) -> i32 {
        error!("Failure! {}", self);
        // Just enoent for now, we can figure out better errors in the future...
        libc::EFAULT
    }
}
