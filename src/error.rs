use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataServerError {
    #[error("")]
    FailedToStopTcpServer,
    #[error("")]
    ConnectionClosed,
    #[error("")]
    FailedToReceive,
    #[error("")]
    FailedToSend,
    #[error("")]
    FailedToConnect,
    #[error("")]
    InvalidToml,
    #[error("")]
    FailedToCreate,
    #[error("")]
    FailedToOpen,
    #[error("")]
    FailedToRead,
    #[error("")]
    FailedToWrite,
    #[error("")]
    FailedToSeek,
    #[error("")]
    TooManyConnections,

    #[error("")]
    JournalFileFull,
    #[error("")]
    JournalUnUsed,
    #[error("")]
    JournalFileAllFlushed,
    #[error("")]
    JournalExists,
    #[error("")]
    OpenDirFailed,
    #[error("")]
    InvalidJournalDir,
    #[error("")]
    NoJournalDir,
    #[error("")]
    InvalidOffset,

    #[error("")]
    Unknown,
}
