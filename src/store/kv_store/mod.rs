use anyhow::Result;
use bytes::Bytes;
use thiserror::Error;

mod sledkv;

pub use sledkv::{KeyValue, SledStore, StoreIter};

#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("db internal error")]
    DbInternalError,
    #[error("no such key")]
    NoSuchkey,
}

pub trait KvStore: Clone {
    fn kv_get(&self, key: &str) -> Result<Option<Bytes>>;
    fn kv_set(&self, key: &str, value: Bytes) -> Result<()>;
    fn kv_delete(&self, key: &str) -> Result<Option<Bytes>>;
    fn kv_delete_range(&self, start_key: &str, _end_key: &str) -> Result<()>;

    fn kv_get_prev_or_eq(&self, key: &str) -> Result<Option<(String, Bytes)>>;
    fn kv_get_next_or_eq(&self, key: &str) -> Result<Option<(String, Bytes)>>;

    fn kv_scan(&self, prefix: &str) -> Result<impl Iterator<Item = (String, Bytes)>>;
}
