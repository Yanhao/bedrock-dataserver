use thiserror::Error;

mod locks;
mod mvcc;
mod sledkv;

pub use locks::{LockItem, RangeLock};
pub use sledkv::{KeyValue, SledStore, StoreIter};

#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("db internal error")]
    DbInternalError,
    #[error("no such key")]
    NoSuchkey,
}
