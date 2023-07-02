use thiserror::Error;

#[derive(Error, Debug)]
#[repr(i32)]
pub enum DataServerError {
    #[error("failed to stop tcp server")]
    FailedToStopTcpServer = 1,
    #[error("connection closed")]
    ConnectionClosed,
    #[error("failed to receive")]
    FailedToReceive,
    #[error("failed to send")]
    FailedToSend,
    #[error("failed to connect")]
    FailedToConnect,
    #[error("invalid toml file")]
    InvalidToml,
    #[error("failed to create")]
    FailedToCreate,
    #[error("failed to open")]
    FailedToOpen,
    #[error("failed to read")]
    FailedToRead,
    #[error("failed to write")]
    FailedToWrite,
    #[error("failed to seek")]
    FailedToSeek,
    #[error("too many connections")]
    TooManyConnections,

    #[error("journal file full")]
    JournalFileFull,
    #[error("journal unused")]
    JournalUnUsed,
    #[error("journal file all flushed")]
    JournalFileAllFlushed,
    #[error("jounal exists")]
    JournalExists,
    #[error("failed to open directory")]
    OpenDirFailed,
    #[error("invalid journal directory")]
    InvalidJournalDir,
    #[error("no such journal directory")]
    NoJournalDir,
    #[error("invalid offset")]
    InvalidOffset,

    #[error("path not exists")]
    PathNotExists,
    #[error("path is directory")]
    IsDir,

    #[error("unknown")]
    Unknown,
}
