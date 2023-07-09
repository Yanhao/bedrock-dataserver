use std::fs::{read_to_string, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, ensure, Result};
use once_cell::sync::Lazy;
use prost::Message;
use serde::{Deserialize, Serialize};
use sled;
use tracing::{debug, info, warn};

use idl_gen::service_pb::ShardMeta;

const METADATA_PATH: &str = "metadata.json";
const METADATA_DIRECTORY: &str = "metadata";

pub static METADATA: Lazy<parking_lot::RwLock<MetaData>> = Lazy::new(|| {
    debug!("parse json metadata");

    let json_raw = read_to_string(METADATA_PATH).expect("read metadata failed");
    let m: DsMeta = serde_json::from_str(&json_raw).expect("parse metadata failed");

    parking_lot::RwLock::new(MetaData::new(m, METADATA_DIRECTORY).unwrap())
});

pub trait Meta<T>
where
    T: Iterator<Item = ShardMeta>,
{
    fn is_shard_exists(&self, shard_id: u64) -> Result<bool>;
    fn put_shard(&self, shard_id: u64, meta: ShardMeta) -> Result<()>;
    fn remove_shard(&self, shard_id: u64) -> Result<()>;
    fn shard_iter(&self) -> T;
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DsMeta {
    pub cluster_name: String,

    pub metaserver_addrs: Vec<String>,
    pub metaserver_addrs_update_ts: String,

    pub metaserver_leader: String,
    pub metaserver_leader_update_ts: String,
}

pub struct MetaData {
    data: DsMeta,
    meta_db: sled::Db,
}

impl MetaData {
    pub fn new(data: DsMeta, meta_dir: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(meta_dir.as_ref()).unwrap();
        Ok(Self { data, meta_db: db })
    }

    pub fn get_meta(&self) -> DsMeta {
        self.data.clone()
    }

    pub fn save_meta(&mut self, json_meta: DsMeta) -> Result<()> {
        let json_raw = serde_json::to_string(&json_meta).unwrap();

        let mut mfile = OpenOptions::new()
            .create(true)
            .append(true)
            .open(METADATA_PATH)?;

        let data = json_raw.as_bytes();
        let sz = mfile.write(data).unwrap();
        ensure!(sz == data.len(), "write partial data");

        self.data = json_meta;

        Ok(())
    }

    fn shard_key(shard_id: u64) -> Vec<u8> {
        let key = format!("/shard/0x{:08x}", shard_id);
        key.as_bytes().to_owned()
    }
}

impl Meta<ShardMetaIter> for MetaData {
    fn is_shard_exists(&self, shard_id: u64) -> Result<bool> {
        let key = Self::shard_key(shard_id);
        Ok(self.meta_db.get(key)?.is_some())
    }

    fn put_shard(&self, shard_id: u64, meta: ShardMeta) -> Result<()> {
        let key = Self::shard_key(shard_id);

        let mut buf = Vec::<u8>::new();
        meta.encode(&mut buf).unwrap();

        self.meta_db.insert(key, buf).map_err(|_| anyhow!(""))?;
        Ok(())
    }

    fn remove_shard(&self, shard_id: u64) -> Result<()> {
        let key = Self::shard_key(shard_id);

        self.meta_db.remove(key).map_err(|_| anyhow!(""))?;
        Ok(())
    }

    fn shard_iter(&self) -> ShardMetaIter {
        let prefix_key = "/shard/";
        let iter = self.meta_db.scan_prefix(prefix_key.as_bytes());

        ShardMetaIter::new(iter)
    }
}

pub struct ShardMetaIter {
    iter: sled::Iter,
}

impl ShardMetaIter {
    fn new(iter: sled::Iter) -> Self {
        Self { iter }
    }
}

impl Iterator for ShardMetaIter {
    type Item = ShardMeta;

    fn next(&mut self) -> Option<Self::Item> {
        let kv = self.iter.next()?;

        match kv {
            Err(e) => {
                // FIXME: error handling
                warn!("failed to read data from kv {}", e);
                None
            }
            Ok(v) => {
                let meta = ShardMeta::decode(v.1.as_ref()).unwrap();
                info!("shard meta read from metadata: {meta:#?}");
                Some(meta)
            }
        }
    }
}
