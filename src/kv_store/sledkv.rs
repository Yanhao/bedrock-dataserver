use std::iter::Iterator;
use std::path::PathBuf;

use anyhow::{bail, Result};
use async_trait::async_trait;
use log::debug;
use sled;
use tokio::fs::remove_dir_all;

use crate::{config::CONFIG, shard::SnapShoter};

use super::error;

pub struct SledStore {
    pub db: sled::Db,
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
        debug!("remove directory: {}", path.display());

        remove_dir_all(path).await;

        Ok(())
    }

    pub async fn kv_get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let iv = match self.db.get(key) {
            Err(e) => {
                bail!(error::KvStoreError::DbInternalError);
            }
            Ok(v) => v,
        };
        if iv.is_none() {
            bail!(error::KvStoreError::NoSuchkey);
        }
        let v = iv.unwrap();
        Ok(v.as_ref().to_owned())
    }

    pub async fn kv_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value);
        Ok(())
    }
}

pub struct StoreIter {
    pub iter: sled::Iter,
}

impl Iterator for StoreIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(v) => {
                let v = v.unwrap();
                let key = v.0;
                let value = v.1;

                Some((key.as_ref().to_owned(), value.as_ref().to_owned()))
            }
        }
    }
}

impl SledStore {
    pub async fn create_snapshot_iter(&self) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        Ok(StoreIter {
            iter: self.db.iter(),
        })
    }

    pub async fn install_snapshot(&mut self, piece: &[u8]) -> Result<()> {
        let piece = piece.to_owned();
        let mut ps = piece.split(|b| *b == '\n' as u8);

        let key = ps.next().unwrap().to_owned();
        let value = ps.next().unwrap().to_owned();
        assert!(ps.next().is_none());

        self.kv_set(&key, &value).await.unwrap();

        Ok(())
    }
}
