use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tonic::Request;
use tracing::info;

use crate::service_pb::CreateShardRequest;
use crate::wal::Wal;

use super::{Shard, ShardError};

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

    pub async fn create_shard(req: &Request<CreateShardRequest>) -> Result<()> {
        let shard_id = req.get_ref().shard_id;
        Shard::create_shard(req).await?;
        Wal::create_wal_dir(shard_id).await?;

        Ok(())
    }

    async fn load_shard(&self, shard_id: u64) -> Result<Arc<Shard>> {
        let mut shard = Shard::load_shard(shard_id).await.unwrap();
        let (rx, input_rx) = shard.setup_ch().await?;

        let shard = Arc::new(shard);

        shard.clone().start(rx, input_rx).await.unwrap();

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

        // self.shards.read().await.remove(&id);

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
        let (rx, input_rx) = new_shard.setup_ch().await?;

        let new_shard = Arc::new(new_shard);

        Wal::create_wal_dir(shard_id).await.unwrap();

        self.shards.write().insert(new_shard_id, new_shard.clone());

        let iter = shard.create_split_iter(&start_key, &end_key).await.unwrap();
        for kv in iter.into_iter() {
            let key = kv.0.to_owned();
            let value = kv.1.to_owned();

            new_shard.put(&key, &value).await?;
        }

        shard.delete_range(&start_key, &end_key).await.unwrap();

        new_shard.start(rx, input_rx).await?;

        Ok(())
    }

    pub async fn merge_shard(&self, shard_id_a: u64, shard_id_b: u64) {
        let shard_a = self.load_shard(shard_id_a).await.unwrap();
        let shard_b = self.load_shard(shard_id_b).await.unwrap();

        let iter = shard_b.create_snapshot_iter().await.unwrap();

        for kv in iter.into_iter() {
            let key = kv.0.to_owned();
            let value = kv.1.to_owned();

            shard_a.put(&key, &value).await.unwrap();
        }

        shard_b.stop().await;

        self.remove_shard(shard_id_b).await.unwrap();
        shard_b.remove_shard().await.unwrap();
    }
}
