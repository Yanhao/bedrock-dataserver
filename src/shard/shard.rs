use std::net::SocketAddr;
use std::time;
use std::vec::Vec;

use anyhow::{anyhow, bail, Result};
use log::info;

pub struct Shard {
    pub shard_id: u64,
    pub storage_id: u64,
    pub create_ts: time::SystemTime,
    pub replicates: Vec<SocketAddr>,
    pub replicates_update_ts: time::SystemTime,
    pub leader: SocketAddr,
    pub leader_change_ts: time::SystemTime,
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
        };
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        info!("shard put");

        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        info!("shard get");

        Ok(Vec::new())
    }
}
