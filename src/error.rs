use thiserror::Error;

#[derive(Error, Debug)]
#[repr(i32)]
pub enum DataServerError {
    #[error("path not exists")]
    PathNotExists,
    #[error("path is directory")]
    IsDir,

    #[error("base error")]
    BaseError,
}
