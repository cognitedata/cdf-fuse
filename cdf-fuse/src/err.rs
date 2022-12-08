#[derive(Debug)]
pub enum FsError {
    Cognite(cognite::Error),
    DirectoryNotFound,
}

impl From<cognite::Error> for FsError {
    fn from(e: cognite::Error) -> Self {
        Self::Cognite(e)
    }
}

impl FsError {
    pub fn as_code(&self) -> i32 {
        println!("Failure! {:?}", self);
        // Just enoent for now, we can figure out better errors in the future...
        libc::EFAULT
    }
}
