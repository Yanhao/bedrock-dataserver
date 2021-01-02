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
    InvalidToml,
    #[error("")]
    FailedToRead,
    #[error("")]
    FailedToWrite,
}
