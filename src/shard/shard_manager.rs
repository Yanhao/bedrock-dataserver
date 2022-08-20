use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use log::info;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::wal::{Wal, WalTrait};

use super::{error::ShardError, fsm::Fsm, Shard};

const DEFAULT_SHARD_CAPACITY: u64 = 10240;

pub static SHARD_MANAGER: Lazy<RwLock<ShardManager>> =
    Lazy::new(|| RwLock::new(ShardManager::new()));

pub struct ShardManager {
    shard_fsms: RwLock<HashMap<u64, Arc<RwLock<Fsm>>>>,
}

impl ShardManager {
    fn new() -> Self {
        return ShardManager {
            shard_fsms: Default::default(),
        };
    }

    pub async fn load_shard_fsm(&mut self, shard_id: u64) -> Result<Arc<RwLock<Fsm>>> {
        if let Ok(v) = self.get_shard_fsm(shard_id).await {
            return Ok(v);
        }

        let shard = Shard::load_shard(shard_id).await.unwrap();
        let wal = Wal::load_wal_by_shard_id(shard_id).await.unwrap();
        let mut fsm = Fsm::new(shard, wal);

        fsm.start().await.unwrap();

        let fsm = Arc::new(RwLock::new(fsm));

        self.shard_fsms.write().await.insert(shard_id, fsm.clone());

        Ok(fsm)
    }

    pub async fn get_shard_fsm(&self, id: u64) -> Result<Arc<RwLock<Fsm>>> {
        match self.shard_fsms.read().await.get(&id) {
            None => bail!(ShardError::NoSuchShard),
            Some(shard) => {
                return Ok(shard.to_owned());
            }
        }
    }

    pub async fn remove_shard_fsm(&mut self, id: u64) -> Result<()> {
        self.shard_fsms.write().await.remove(&id);

        Ok(())
    }

    pub async fn create_shard_fsm(&mut self, fsm: Fsm) -> Result<()> {
        let shard_id = fsm.get_shard_id().await;

        if self.shard_fsms.read().await.contains_key(&shard_id) {
            bail!(ShardError::ShardExists);
        }

        self.shard_fsms
            .write()
            .await
            .insert(shard_id, Arc::new(RwLock::new(fsm)))
            .unwrap();

        Ok(())
    }
}
