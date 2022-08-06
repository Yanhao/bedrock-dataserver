use std::path::PathBuf;

use anyhow::{bail, Result};
use sled;

use crate::config::CONFIG;

pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    fn store_path(shard_id: u64) -> PathBuf {
        let wal_dir: PathBuf = CONFIG
            .read()
            .unwrap()
            .work_directory
            .as_ref()
            .unwrap()
            .into();

        let storage_id: u32 = ((shard_id & 0xFFFF0000) >> 32) as u32;
        let shard_id: u32 = (shard_id & 0x0000FFFF) as u32;

        let kv_data_dir = wal_dir
            .join::<String>("data".into())
            .join::<String>(format!("{:#04x}", storage_id))
            .join::<String>(format!("{:#04x}", shard_id));

        kv_data_dir
    }

    pub async fn load(shard_id: u64) -> Result<Self> {
        let path = Self::store_path(shard_id);
        let store = sled::open(path).unwrap();
        Ok(Self { db: store })
    }

    pub async fn create(shard_id: u64) -> Result<Self> {
        let path = Self::store_path(shard_id);
        let store = sled::open(path).unwrap();
        Ok(Self { db: store })
    }

    pub async fn remove(shard_id: u64) -> Result<()> {
        let path = Self::store_path(shard_id);

        std::fs::remove_file(path).unwrap();

        Ok(())
    }

    pub async fn kv_get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let value_ref = self.db.get(key).unwrap().unwrap();
        let value: &[u8] = value_ref.as_ref();

        Ok(value.to_owned())
    }

    pub async fn kv_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value);
        Ok(())
    }
}
