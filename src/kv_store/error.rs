use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("db internal error")]
    DbInternalError,
    #[error("no such key")]
    NoSuchkey,
}
