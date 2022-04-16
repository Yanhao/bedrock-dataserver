use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Result};
use once_cell::sync::Lazy;

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

    pub fn get_shard(&self, id: u64) -> Result<Arc<RwLock<Shard>>> {
        match self.shards.read().unwrap().get(&id) {
            None => bail!(ShardError::NoSuchShard),
            Some(shard) => {
                return Ok(shard.to_owned());
            }
        }
    }

    pub fn remove_shard(&mut self, id: u64) -> Result<()> {
        self.shards.write().unwrap().remove(&id);

        Ok(())
    }

    pub fn create_shard(&mut self, shard: Shard) -> Result<()> {
        if self.shards.read().unwrap().contains_key(&(shard.shard_id)) {
            bail!(ShardError::ShardExists);
        }

        self.shards
            .write()
            .unwrap()
            .insert(shard.shard_id, Arc::new(RwLock::new(shard)));

        Ok(())
    }
}
