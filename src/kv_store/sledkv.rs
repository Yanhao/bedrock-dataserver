use std::iter::Iterator;
use std::path::PathBuf;

use anyhow::{bail, Result};
use idl_gen::service_pb::shard_install_snapshot_request;
use sled;
use tokio::fs::remove_dir_all;
use tracing::{debug, error, info};

use crate::config::CONFIG;

use super::KvStoreError;

pub struct SledStore {
    pub db: sled::Db,
}

pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl SledStore {
    fn store_path(shard_id: u64) -> PathBuf {
        let data_dir: PathBuf = CONFIG.read().data_directory.as_ref().unwrap().into();

        info!("shard_id: 0x{:016x}", shard_id);
        let storage_id: u32 = ((shard_id & 0xFFFFFFFF_00000000) >> 32) as u32;
        let shard_isn: u32 = (shard_id & 0x00000000_FFFFFFFF) as u32;
        info!(
            "storage_id: 0x{:08x}, shard_isn: 0x{:08x}",
            storage_id, shard_isn
        );
        data_dir
            .join::<String>("data".into())
            .join::<String>(format!("{:08x}", storage_id))
            .join::<String>(format!("{:08x}", shard_isn))
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
        self.db.insert(key, value);
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
        let mut count = 0;
        for kv in iter {
            if count >= limit {
                break;
            }

            let kv = kv.unwrap();

            kvs.push(KeyValue {
                key: kv.0.as_ref().to_owned(),
                value: kv.1.as_ref().to_owned(),
            });

            count += 1;
        }

        Ok(kvs)
    }

    pub async fn kv_delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        let start_key: Vec<u8> = start_key.into();

        loop {
            let (key, _) = self.db.pop_max().unwrap().unwrap();
            let key: Vec<u8> = key.as_ref().to_owned();
            if key < start_key {
                self.db.remove(key);
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
        // info!("piece: {}", String::from_utf8_lossy(piece));

        // let piece = piece.to_owned();
        // let mut ps = piece.split(|b| *b == '\n' as u8);
        // info!("split : {ps:?}");

        // let key = ps.next().unwrap().to_owned();
        // let value = ps.next().unwrap().to_owned();
        // assert!(ps.next().is_none());
        // info!(
        //     "kv piece: key: {}, value: {}",
        //     String::from_utf8_lossy(&key),
        //     String::from_utf8_lossy(&value)
        // );

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
