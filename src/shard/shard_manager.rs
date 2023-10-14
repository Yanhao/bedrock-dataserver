use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::Lazy;

use super::Shard;
use crate::kv_store::KvStore;
use crate::wal::Wal;

pub static SHARD_MANAGER: Lazy<ShardManager> = Lazy::new(ShardManager::new);

pub struct ShardManager {
    shards: parking_lot::RwLock<HashMap<u64, Arc<Shard>>>,
}

impl ShardManager {
    fn new() -> Self {
        ShardManager {
            shards: Default::default(),
        }
    }

    async fn load_shard(&self, shard_id: u64) -> Result<Arc<Shard>> {
        let shard = Arc::new(Shard::load_shard(shard_id).await.unwrap());

        shard.clone().start_role().await?;

        self.shards.write().insert(shard_id, shard.clone());

        Ok(shard)
    }

    pub async fn get_shard(&self, shard_id: u64) -> Result<Arc<Shard>> {
        if let Some(shard) = self.shards.read().get(&shard_id) {
            return Ok(shard.clone());
        }

        self.load_shard(shard_id).await
    }

    pub async fn remove_shard(&self, shard_id: u64) -> Result<()> {
        if !self.shards.read().contains_key(&shard_id) {
            return Ok(());
        }

        self.shards.write().remove(&shard_id);

        Ok(())
    }

    pub async fn split_shard(&self, shard_id: u64, new_shard_id: u64) -> Result<()> {
        let shard = self.load_shard(shard_id).await?;

        let (start_key, end_key) = (shard.middle_key(), shard.max_key());

        let new_shard = Arc::new(
            Shard::create_shard_for_split(
                new_shard_id,
                shard.leader(),
                shard.replicates(),
                shard.next_index().await,
                start_key.clone()..end_key.clone(),
            )
            .await?,
        );

        Wal::create_wal_dir(shard_id).await?;

        self.shards.write().insert(new_shard_id, new_shard.clone());

        let iter = shard.create_split_iter(&start_key, &end_key).await?;
        for (key, value) in iter.into_iter() {
            new_shard.kv_store.kv_set(key, value)?;
        }

        shard
            .kv_store
            .kv_delete_range(start_key.into(), end_key.into())?;

        new_shard.start_role().await?;

        Ok(())
    }

    pub async fn merge_shard(&self, shard_id_a: u64, shard_id_b: u64) -> Result<()> {
        let shard_a = self.load_shard(shard_id_a).await?;
        let shard_b = self.load_shard(shard_id_b).await?;

        let iter = shard_b.kv_store.take_snapshot()?;

        for (key, value) in iter.into_iter() {
            shard_a.kv_store.kv_set(key, value)?;
        }

        shard_b.stop_role().await?;

        self.remove_shard(shard_id_b).await?;
        Shard::remove_shard(shard_b.shard_id()).await?;

        Ok(())
    }
}
