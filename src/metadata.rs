use std::path::Path;

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use prost::Message;
use sled;
use tracing::{debug, warn};

use idl_gen::service_pb::ShardMeta;

const METADATA_PATH: &str = "metadata.json";
const METADATA_DIRECTORY: &str = "metadata";

pub static METADATA: Lazy<parking_lot::RwLock<MetaData>> =
    Lazy::new(|| parking_lot::RwLock::new(MetaData::new(METADATA_DIRECTORY).unwrap()));

pub trait Meta<T>
where
    T: Iterator<Item = ShardMeta>,
{
    fn is_shard_exists(&self, shard_id: u64) -> Result<bool>;
    fn put_shard(&self, shard_id: u64, meta: ShardMeta) -> Result<()>;
    fn get_shard(&self, shard_id: u64) -> Result<ShardMeta>;
    fn remove_shard(&self, shard_id: u64) -> Result<()>;
    fn shard_iter(&self) -> T;
}

pub struct MetaData {
    meta_db: sled::Db,
}

impl MetaData {
    pub fn new(meta_dir: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(meta_dir.as_ref()).unwrap();
        Ok(Self { meta_db: db })
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

    fn get_shard(&self, shard_id: u64) -> Result<ShardMeta> {
        let key = Self::shard_key(shard_id);
        match self.meta_db.get(key)? {
            Some(value) => Ok(ShardMeta::decode(value.as_ref()).unwrap()),
            None => Err(anyhow!("no such shard")),
        }
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
                debug!("shard meta read from metadata: {meta:#?}");
                Some(meta)
            }
        }
    }
}
