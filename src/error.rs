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
}

pub type Result<T> = std::result::Result<T, DataServerError>;