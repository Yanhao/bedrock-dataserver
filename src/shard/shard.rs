use std::net::SocketAddr;
use std::vec::Vec;
use std::time;

use anyhow::{anyhow, bail, Result};

pub struct Shard {
    shard_id: u64,
    storage_id: u64,
    create_ts: time::Instant,
    replicates: Vec<SocketAddr>,
    replicates_update_ts: time::Instant,
    leader: SocketAddr,
    leader_change_ts: time::Instant,
}

impl Shard {
    fn new(shard_id: u64, storage_id: u64) -> Self {
        return Shard {
            shard_id,
            storage_id,
            create_ts: time::Instant::now(),
            replicates: Vec::new(),
            replicates_update_ts: time::Instant::now(),
            leader: "0.0.0.0:1024".parse().unwrap(),
            leader_change_ts: time::Instant::now(),
        };
    }

     pub async fn Put() -> Result<()> {
        Ok(())
    }

    pub async fn Get() -> Result<()> {
        Ok(())
    }
}
