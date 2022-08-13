use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::wal::Wal;

use super::{error::ShardError, fsm::Fsm, Shard};

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

    pub async fn load_shard_fsm(&mut self, shard_id: u64) -> Result<Arc<RwLock<Fsm>>> {
        let shard = Shard::load_shard(shard_id).await;
        let fsm = Arc::new(RwLock::new(Fsm::new(
            shard,
            Arc::new(RwLock::new(
                Wal::load_wal_by_shard_id(shard_id).await.unwrap(),
            )),
        )));

        fsm.write().await.start().await.unwrap();

        self.shards.write().await.insert(shard_id, fsm.clone());

        Ok(fsm)
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
        let shard_id = fsm.get_shard_id().await;

        if self.shards.read().await.contains_key(&shard_id) {
            bail!(ShardError::ShardExists);
        }

        self.shards
            .write()
            .await
            .insert(shard_id, Arc::new(RwLock::new(fsm)));

        Ok(())
    }
}
