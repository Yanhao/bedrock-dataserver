use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tracing::info;

use crate::{
    shard::{self, Shard},
    store::KvStore,
};

#[derive(Clone)]
pub struct Dstore {
    shard: Arc<Shard>,
}

impl Dstore {
    pub fn new(shard: Arc<Shard>) -> Self {
        Self { shard }
    }

    pub fn kv_get(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.shard.kv_store.kv_get(key)
    }

    pub fn kv_get_prev(&self, key: Bytes) -> Result<Option<(Bytes, Bytes)>> {
        self.shard.kv_store.kv_get_prev_or_eq(key)
    }

    pub fn kv_get_next(&self, key: Bytes) -> Result<Option<(Bytes, Bytes)>> {
        self.shard.kv_store.kv_get_next_or_eq(key)
    }

    pub async fn kv_set(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut entry_with_notifier = self
            .shard
            .process_write(shard::Operation::Set, &key, &value.to_vec())
            .await?;

        info!("start wait result");
        let _ = entry_with_notifier.wait_result().await?;
        let _ = self.shard.apply_entry(&entry_with_notifier.entry).await?;

        Ok(())
    }

    pub async fn kv_delete(&self, key: Bytes) -> Result<Option<Bytes>> {
        let mut entry_with_notifier = self
            .shard
            .process_write(shard::Operation::Del, &key, &vec![])
            .await?;

        info!("start wait result");
        let _ = entry_with_notifier.wait_result().await?;

        self.shard.apply_entry(&entry_with_notifier.entry).await
    }

    pub fn kv_scan(&self, prefix: Bytes) -> Result<impl Iterator<Item = (Bytes, Bytes)> + '_> {
        self.shard.kv_store.kv_scan(prefix)
    }
}
