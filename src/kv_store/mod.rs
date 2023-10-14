mod sledkv;

use anyhow::Result;
use bytes::Bytes;

pub use sledkv::{KeyValue, SledStore, StoreIter};

pub trait KvStore: Clone {
    fn kv_get(&self, key: Bytes) -> Result<Option<Bytes>>;
    fn kv_set(&self, key: Bytes, value: Bytes) -> Result<()>;
    fn kv_delete(&self, key: Bytes) -> Result<Option<Bytes>>;
    fn kv_delete_range(&self, start_key: Bytes, _end_key: Bytes) -> Result<()>;

    fn kv_get_prev_or_eq(&self, key: Bytes) -> Result<Option<(Bytes, Bytes)>>;
    fn kv_get_next_or_eq(&self, key: Bytes) -> Result<Option<(Bytes, Bytes)>>;

    fn kv_scan(&self, prefix: Bytes) -> Result<impl Iterator<Item = (Bytes, Bytes)>>;
}
