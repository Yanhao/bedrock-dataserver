use std::str::from_utf8;
use std::time;
use std::vec::Vec;
use std::{collections::HashMap, net::SocketAddr};

use anyhow::{anyhow, bail, Result};
use log::info;
use tokio::sync::RwLock;

use crate::shard::error;

pub struct Shard {
    pub shard_id: u64,
    pub storage_id: u64,
    pub create_ts: time::SystemTime,
    pub replicates: Vec<SocketAddr>,
    pub replicates_update_ts: time::SystemTime,
    pub leader: SocketAddr,
    pub leader_change_ts: time::SystemTime,

    pub kv_data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Shard {
    fn new(shard_id: u64, storage_id: u64) -> Self {
        return Shard {
            shard_id,
            storage_id,
            create_ts: time::SystemTime::now(),
            replicates: Vec::new(),
            replicates_update_ts: time::SystemTime::now(),
            leader: "0.0.0.0:1024".parse().unwrap(),
            leader_change_ts: time::SystemTime::now(),

            kv_data: RwLock::new(HashMap::new()),
        };
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = key.to_vec();
        let value = value.to_vec();

        info!(
            "shard put: key={}, value={}",
            from_utf8(&key).unwrap(),
            from_utf8(&value).unwrap()
        );

        self.kv_data.write().await.insert(key, value);

        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let kv = self.kv_data.read().await;

        let value = match kv.get(key.as_ref()) {
            None => {
                bail!(error::ShardError::NoSuchKey)
            }
            Some(v) => v,
        };

        info!(
            "shard get: key={}, value={}",
            from_utf8(key).unwrap(),
            from_utf8(value.as_slice()).unwrap()
        );

        Ok(value.to_vec())
    }

    pub async fn clear(&self) {
        *self.kv_data.write().await = HashMap::new();
    }

    pub fn get_replicates(&self) -> Vec<SocketAddr> {
        self.replicates.clone()
    }
}
