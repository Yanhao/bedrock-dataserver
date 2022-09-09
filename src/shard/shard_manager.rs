use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use log::info;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::wal::Wal;

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
            .insert(shard_id, Arc::new(RwLock::new(fsm)));

        info!("insert shard to cache, shard id: 0x{:016x}", shard_id);

        Ok(())
    }

    pub async fn split_shard(&mut self, shard_id: u64, new_shard_id: u64) {
        let fsm = self.load_shard_fsm(shard_id).await.unwrap();
        let shard = fsm.read().await.get_shard();

        let replicates = shard.read().await.get_replicates();
        let leader = shard.read().await.get_leader();
        let last_wal_index = shard.read().await.get_last_wal_index();

        let start_key = shard.read().await.middle_key();
        let end_key = shard.read().await.max_key();

        let new_shard = Shard::create_shard_for_split(
            new_shard_id,
            leader,
            replicates,
            last_wal_index,
            start_key.clone()..end_key.clone(),
        )
        .await;

        Wal::create_wal_dir(shard_id).await.unwrap();
        let new_fsm = Arc::new(RwLock::new(Fsm::new(
            new_shard,
            Wal::load_wal_by_shard_id(new_shard_id).await.unwrap(),
        )));

        self.shard_fsms
            .write()
            .await
            .insert(new_shard_id, new_fsm.clone());

        let new_shard = new_fsm.read().await.get_shard();

        let iter = shard
            .read()
            .await
            .create_split_iter(&start_key, &end_key)
            .await
            .unwrap();
        for kv in iter.into_iter() {
            let key = kv.0.to_owned();
            let value = kv.1.to_owned();

            new_shard.write().await.put(&key, &value).await;
        }

        shard
            .write()
            .await
            .delete_range(&start_key, &end_key)
            .await
            .unwrap();

        new_fsm.write().await.start().await.unwrap();
    }

    pub async fn merge_shard(&mut self, shard_id_a: u64, shard_id_b: u64) {
        let fsm_a = self.load_shard_fsm(shard_id_a).await.unwrap();
        let fsm_b = self.load_shard_fsm(shard_id_b).await.unwrap();

        let shard_a = fsm_a.read().await.get_shard();
        let shard_b = fsm_b.read().await.get_shard();

        let iter = shard_b.write().await.create_snapshot_iter().await.unwrap();

        for kv in iter.into_iter() {
            let key = kv.0.to_owned();
            let value = kv.1.to_owned();

            shard_a.write().await.put(&key, &value).await.unwrap();
        }

        fsm_b.write().await.stop().await;

        self.remove_shard_fsm(shard_id_b).await.unwrap();
        shard_b.write().await.remove_shard().await.unwrap();
    }
}
