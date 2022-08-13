use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("no such file")]
    NoSuchFile,
    #[error("file already exists")]
    FileExists,
    #[error("file not exists")]
    FileNotExists,

    #[error("wrong file path")]
    WrongFilePath,

    #[error("failed to open file")]
    FailedToOpen,
    #[error("failed to read file")]
    FailedToRead,
    #[error("failed to write file")]
    FailedToWrite,
    #[error("failed to seek file")]
    FailedToSeek,
    #[error("failed to create new file")]
    FailedToCreateFile,
    #[error("failed to remove file")]
    FailedToRemoveFile,

    #[error("invalid parameter")]
    InvalidParameter,

    #[error("emtry wal files")]
    EmptyWalFiles,

    #[error("to many entries")]
    TooManyEntries,
}
