use std::iter::Iterator;
use std::path::PathBuf;

use anyhow::{bail, Result};
use idl_gen::service_pb::shard_install_snapshot_request;
use sled;
use tokio::fs::remove_dir_all;
use tracing::{debug, error, info};

use crate::{config::CONFIG, shard::Shard};

use super::KvStoreError;

pub struct SledStore {
    pub db: sled::Db,
}

pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl SledStore {
    fn db_path(shard_id: u64) -> PathBuf {
        let data_dir: PathBuf = CONFIG.read().data_directory.as_ref().unwrap().into();

        let storage_id = Shard::shard_sid(shard_id);
        let shard_isn = Shard::shard_isn(shard_id);
        data_dir
            .join::<String>("data".into())
            .join::<String>(format!("{:08x}", storage_id))
            .join::<String>(format!("{:08x}", shard_isn))
    }

    pub async fn load(shard_id: u64) -> Result<Self> {
        info!("load shard sledkv db 0x{:016x}", shard_id);

        let path = Self::db_path(shard_id);
        let store = sled::open(path).unwrap();
        Ok(Self { db: store })
    }

    pub async fn create(shard_id: u64) -> Result<Self> {
        info!("create shard sledkv db 0x{:016x}", shard_id);

        let path = Self::db_path(shard_id);
        let store = sled::open(path).unwrap();
        Ok(Self { db: store })
    }

    pub async fn remove(shard_id: u64) -> Result<()> {
        let path = Self::db_path(shard_id);
        debug!("remove directory: {:?}", path);

        remove_dir_all(path).await?;

        Ok(())
    }

    pub async fn kv_get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let iv = match self.db.get(key) {
            Err(e) => {
                error!("sledkv get failed, err: {e}");
                bail!(KvStoreError::DbInternalError);
            }
            Ok(v) => v,
        };
        if iv.is_none() {
            bail!(KvStoreError::NoSuchkey);
        }
        let v = iv.unwrap();
        Ok(v.as_ref().to_owned())
    }

    pub async fn kv_set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value)?;

        Ok(())
    }

    pub async fn kv_scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: i32,
    ) -> Result<Vec<KeyValue>> {
        let iter = self.db.range(start_key..end_key);

        let mut kvs = vec![];

        for (count, kv) in iter.enumerate() {
            if count >= limit as usize {
                break;
            }

            let kv = kv.unwrap();

            kvs.push(KeyValue {
                key: kv.0.as_ref().to_owned(),
                value: kv.1.as_ref().to_owned(),
            });
        }

        Ok(kvs)
    }

    pub async fn kv_delete_range(&self, start_key: &[u8], _end_key: &[u8]) -> Result<()> {
        // FIXME:
        let start_key: Vec<u8> = start_key.into();

        loop {
            let (key, _) = self.db.pop_max().unwrap().unwrap();
            let key: Vec<u8> = key.as_ref().to_owned();
            if key < start_key {
                self.db.remove(key)?;
                break;
            }
        }

        Ok(())
    }

    pub async fn clear_data(&self) {
        todo!()
    }
}

pub struct StoreIter {
    pub iter: sled::Iter,
}

impl Iterator for StoreIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?.unwrap();
        let (key, value) = item;

        Some((key.as_ref().to_owned(), value.as_ref().to_owned()))
    }
}

impl SledStore {
    pub async fn create_snapshot_iter(&self) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        info!("creat snapshot iterator ...");

        for i in self.db.iter() {
            match i {
                Err(e) => {
                    error!("wrong item, err: {e}");
                }
                Ok(v) => {
                    let (k, v) = v;
                    info!("iterator: key: {:?}, value: {:?}", k.as_ref(), v.as_ref());
                }
            }
        }

        Ok(StoreIter {
            iter: self.db.iter(),
        })
    }

    pub async fn install_snapshot(
        &self,
        entries: Vec<shard_install_snapshot_request::Entry>,
    ) -> Result<()> {
        for ent in entries.iter() {
            info!(
                "install_snapshot, key: {:?}, value: {:?}",
                ent.key, ent.value
            );

            self.kv_set(&ent.key, &ent.value).await.inspect_err(|e| {
                error!(
                    "install_snapshot, kv_set failed, err: {e}, key: {:?}, value: {:?}",
                    ent.key, ent.value
                )
            })?;
        }

        Ok(())
    }

    // pub async fn create_split_iter(
    //     &self,
    //     start_key: &[u8],
    //     end_key: &[u8],
    // ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
    //     Ok(StoreIter {
    //         iter: self.db.range(start_key..end_key),
    //     })
    // }
}
