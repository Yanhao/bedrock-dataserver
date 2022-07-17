use std::str::from_utf8;
use std::time;
use std::vec::Vec;
use std::{collections::HashMap, net::SocketAddr};

use anyhow::{bail, Result};
use log::info;
use tokio::sync::RwLock;
use tonic::async_trait;

use crate::shard::error;
use crate::shard::snapshoter::SnapShoter;

pub struct Shard {
    pub shard_id: u64,
    pub storage_id: u64,
    pub create_ts: time::SystemTime,
    pub replicates: Vec<SocketAddr>,
    pub replicates_update_ts: time::SystemTime,
    pub leader: Option<SocketAddr>,
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
            leader: Some("0.0.0.0:1024".parse().unwrap()),
            leader_change_ts: time::SystemTime::now(),

            kv_data: RwLock::new(HashMap::new()),
        };
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = key.to_vec();
        let value = value.to_vec();

        info!(
            "shard put: key={:?}, value={:?}",
            key,
            value,
            // from_utf8(&key).unwrap(),
            // from_utf8(&value).unwrap()
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

    pub fn set_replicates(&mut self, replicats: &[SocketAddr]) -> Result<()> {
        self.replicates = replicats.to_owned();
        Ok(())
    }
}

#[async_trait]
impl SnapShoter for Shard {
    async fn create_snapshot(&self) -> Result<Vec<Vec<u8>>> {
        let mut ret: Vec<Vec<u8>> = Default::default();

        for kv in self.kv_data.read().await.iter() {
            let mut key = kv.0.to_owned();
            let mut value = kv.1.to_owned();

            let mut item = vec![];
            item.append(&mut key);
            item.append(&mut vec!['\n' as u8]);
            item.append(&mut value);

            ret.push(item);
        }

        Ok(ret)
    }

    async fn install_snapshot(&mut self, piece: &[u8]) -> Result<()> {
        let piece = piece.to_owned();
        let mut ps = piece.split(|b| *b == '\n' as u8);

        let key = ps.next().unwrap().to_owned();
        let value = ps.next().unwrap().to_owned();
        assert!(ps.next().is_none());

        self.kv_data.write().await.insert(key, value);

        Ok(())
    }
}
