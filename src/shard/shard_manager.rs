use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use super::error::ShardError;
use super::shard::Shard;

const DEFAULT_SHARD_CAPACITY: u64 = 10240;

pub static SHARD_MANAGER: Lazy<RwLock<ShardManager>> =
    Lazy::new(|| RwLock::new(ShardManager::new()));

pub struct ShardManager {
    shards: RwLock<HashMap<u64, Arc<RwLock<Shard>>>>,
}

impl ShardManager {
    fn new() -> Self {
        return ShardManager {
            shards: Default::default(),
        };
    }

    pub async fn get_shard(&self, id: u64) -> Result<Arc<RwLock<Shard>>> {
        match self.shards.read().await.get(&id) {
            None => bail!(ShardError::NoSuchShard),
            Some(shard) => {
                return Ok(shard.to_owned());
            }
        }
    }

    pub async fn remove_shard(&mut self, id: u64) -> Result<()> {
        self.shards.write().await.remove(&id);

        Ok(())
    }

    pub async fn create_shard(&mut self, shard: Shard) -> Result<()> {
        if self.shards.read().await.contains_key(&(shard.shard_id)) {
            bail!(ShardError::ShardExists);
        }

        self.shards
            .write()
            .await
            .insert(shard.shard_id, Arc::new(RwLock::new(shard)));

        Ok(())
    }
}
