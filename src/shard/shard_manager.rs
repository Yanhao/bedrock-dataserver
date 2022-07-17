use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use super::{error::ShardError, fsm::Fsm};

const DEFAULT_SHARD_CAPACITY: u64 = 10240;

pub static SHARD_MANAGER: Lazy<RwLock<ShardManager>> =
    Lazy::new(|| RwLock::new(ShardManager::new()));

pub struct ShardManager {
    shards: RwLock<HashMap<u64, Arc<RwLock<Fsm>>>>,
}

impl ShardManager {
    fn new() -> Self {
        return ShardManager {
            shards: Default::default(),
        };
    }

    pub async fn get_shard_fsm(&self, id: u64) -> Result<Arc<RwLock<Fsm>>> {
        match self.shards.read().await.get(&id) {
            None => bail!(ShardError::NoSuchShard),
            Some(shard) => {
                return Ok(shard.to_owned());
            }
        }
    }

    pub async fn remove_shard_fsm(&mut self, id: u64) -> Result<()> {
        self.shards.write().await.remove(&id);

        Ok(())
    }

    pub async fn create_shard_fsm(&mut self, fsm: Fsm) -> Result<()> {
        if self
            .shards
            .read()
            .await
            .contains_key(&(fsm.shard.read().await.shard_id))
        {
            bail!(ShardError::ShardExists);
        }

        let shard_id = fsm.shard.read().await.shard_id;
        self.shards
            .write()
            .await
            .insert(shard_id, Arc::new(RwLock::new(fsm)));

        Ok(())
    }
}
