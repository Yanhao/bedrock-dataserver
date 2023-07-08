use anyhow::Result;
use async_trait::async_trait;
use thiserror::Error;

use idl_gen::replog_pb::Entry;

mod mem_log;
mod wal;
mod wal_file;

pub use wal::Wal;

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

    #[error("empty wal files")]
    EmptyWalFiles,
    #[error("wal file full")]
    WalFileFull,

    #[error("to many entries")]
    TooManyEntries,
}

#[async_trait]
pub trait WalTrait {
    async fn entries(&mut self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>>;
    async fn append(&mut self, ents: Vec<Entry>, discard: bool) -> Result<u64 /* last_index */>;
    async fn compact(&mut self, compact_index: u64) -> Result<()>;

    fn first_index(&self) -> u64;
    fn next_index(&self) -> u64;
}
