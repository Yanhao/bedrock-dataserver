use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::Lazy;

use super::Shard;
use crate::wal::Wal;

const DEFAULT_SHARD_CAPACITY: u64 = 10240;

pub static SHARD_MANAGER: Lazy<ShardManager> = Lazy::new(|| ShardManager::new());

pub struct ShardManager {
    shards: parking_lot::RwLock<HashMap<u64, Arc<Shard>>>,
}

impl ShardManager {
    fn new() -> Self {
        return ShardManager {
            shards: Default::default(),
        };
    }

    async fn load_shard(&self, shard_id: u64) -> Result<Arc<Shard>> {
        let shard = Arc::new(Shard::load_shard(shard_id).await.unwrap());

        shard.clone().switch_role_to_leader().await.unwrap();

        self.shards.write().insert(shard_id, shard.clone());

        Ok(shard)
    }

    pub async fn get_shard(&self, id: u64) -> Result<Arc<Shard>> {
        if let Some(shard) = self.shards.read().get(&id) {
            return Ok(shard.clone());
        }

        self.load_shard(id).await
    }

    pub async fn remove_shard(&self, id: u64) -> Result<()> {
        if !self.shards.read().contains_key(&id) {
            return Ok(());
        }

        self.shards.write().remove(&id);

        Ok(())
    }

    pub async fn split_shard(&self, shard_id: u64, new_shard_id: u64) -> Result<()> {
        let shard = self.load_shard(shard_id).await.unwrap();

        let replicates = shard.get_replicates();
        let leader = shard.get_leader();
        let last_wal_index = shard.get_last_index().await;

        let start_key = shard.middle_key();
        let end_key = shard.max_key();

        let mut new_shard = Shard::create_shard_for_split(
            new_shard_id,
            leader,
            replicates,
            last_wal_index,
            start_key.clone()..end_key.clone(),
        )
        .await?;

        let new_shard = Arc::new(new_shard);

        Wal::create_wal_dir(shard_id).await.unwrap();

        self.shards.write().insert(new_shard_id, new_shard.clone());

        let iter = shard.create_split_iter(&start_key, &end_key).await.unwrap();
        for kv in iter.into_iter() {
            let key = kv.0.to_owned();
            let value = kv.1.to_owned();

            new_shard.kv_store.kv_set(&key, &value).await?;
        }

        shard
            .kv_store
            .kv_delete_range(&start_key, &end_key)
            .await
            .unwrap();

        new_shard.switch_role_to_leader().await?;

        Ok(())
    }

    pub async fn merge_shard(&self, shard_id_a: u64, shard_id_b: u64) -> Result<()> {
        let shard_a = self.load_shard(shard_id_a).await?;
        let shard_b = self.load_shard(shard_id_b).await?;

        let iter = shard_b.kv_store.create_snapshot_iter().await?;

        for kv in iter.into_iter() {
            let key = kv.0.to_owned();
            let value = kv.1.to_owned();

            shard_a.kv_store.kv_set(&key, &value).await?;
        }

        shard_b.stop_role().await?;

        self.remove_shard(shard_id_b).await.unwrap();
        Shard::remove_shard(shard_b.get_shard_id()).await?;

        Ok(())
    }
}
