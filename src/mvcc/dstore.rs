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

    pub fn kv_get(&self, key: &str) -> Result<Option<Bytes>> {
        self.shard.kv_store.kv_get(key)
    }

    pub fn kv_get_prev(&self, key: &str) -> Result<Option<(String, Bytes)>> {
        self.shard.kv_store.kv_get_prev(key)
    }

    pub fn kv_get_next(&self, key: &str) -> Result<Option<(String, Bytes)>> {
        self.shard.kv_store.kv_get_next(key)
    }

    pub async fn kv_set(&self, key: &str, value: Bytes) -> Result<()> {
        let mut entry_with_notifier = self
            .shard
            .process_write(shard::Operation::Set, key.as_bytes(), &value.to_vec())
            .await?;

        info!("start wait result");
        let _ = entry_with_notifier.wait_result().await?;
        let _ = self.shard.apply_entry(&entry_with_notifier.entry).await?;

        Ok(())
    }

    pub async fn kv_delete(&self, key: &str) -> Result<Option<Bytes>> {
        let mut entry_with_notifier = self
            .shard
            .process_write(shard::Operation::Del, key.as_bytes(), &vec![])
            .await?;

        info!("start wait result");
        let _ = entry_with_notifier.wait_result().await?;

        self.shard.apply_entry(&entry_with_notifier.entry).await
    }

    pub fn kv_scan<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Result<impl Iterator<Item = (String, Bytes)> + 'a> {
        self.shard.kv_store.kv_scan(prefix)
    }
}
