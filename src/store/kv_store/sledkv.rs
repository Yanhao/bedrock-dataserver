use std::iter::Iterator;
use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use sled;
use tokio::fs::remove_dir_all;
use tracing::{debug, error, info};

use crate::{config::CONFIG, shard::Shard};

use super::KvStore;

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

    pub async fn create(shard_id: u64) -> Result<()> {
        info!("create shard sledkv db 0x{:016x}", shard_id);

        let path = Self::db_path(shard_id);
        let _ = sled::open(path).unwrap();

        Ok(())
    }

    pub async fn remove(shard_id: u64) -> Result<()> {
        let path = Self::db_path(shard_id);
        debug!("remove directory: {:?}", path);

        remove_dir_all(path).await?;

        Ok(())
    }
}

impl SledStore {
    // pub async fn kv_scan(
    //     &self,
    //     start_key: &[u8],
    //     end_key: &[u8],
    //     limit: i32,
    // ) -> Result<Vec<KeyValue>> {
    //     let iter = self.db.range(start_key..end_key);

    //     let mut kvs = vec![];

    //     for (count, kv) in iter.enumerate() {
    //         if count >= limit as usize {
    //             break;
    //         }

    //         let kv = kv.unwrap();

    //         kvs.push(KeyValue {
    //             key: kv.0.as_ref().to_owned(),
    //             value: kv.1.as_ref().to_owned(),
    //         });
    //     }

    //     Ok(kvs)
    // }

    pub fn take_snapshot(&self) -> Result<impl Iterator<Item = (String, bytes::Bytes)>> {
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

    pub fn install_snapshot(&self, entries: Vec<(String, bytes::Bytes)>) -> Result<()> {
        for ent in entries.iter() {
            info!("install_snapshot, key: {:?}, value: {:?}", ent.0, ent.1);

            self.kv_set(&ent.0, ent.1.clone()).inspect_err(|e| {
                error!(
                    "install_snapshot, kv_set failed, err: {e}, key: {:?}, value: {:?}",
                    ent.0, ent.1
                )
            })?;
        }

        Ok(())
    }
}

impl Clone for SledStore {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
        }
    }
}

impl KvStore for SledStore {
    fn kv_get(&self, key: &str) -> Result<Option<bytes::Bytes>> {
        let iv = self.db.get(key)?;
        Ok(iv.map(|v| v.as_ref().to_owned().into()))
    }

    fn kv_set(&self, key: &str, value: bytes::Bytes) -> Result<()> {
        self.db.insert(key, value.to_vec())?;
        Ok(())
    }

    fn kv_delete(&self, _key: &str) -> Result<Option<bytes::Bytes>> {
        todo!()
    }

    fn kv_delete_range(&self, start_key: &str, _end_key: &str) -> Result<()> {
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

    fn kv_get_prev(&self, _key: &str) -> Result<Option<(String, bytes::Bytes)>> {
        todo!()
    }

    fn kv_get_next(&self, _key: &str) -> Result<Option<(String, bytes::Bytes)>> {
        todo!()
    }

    fn kv_scan(&self, prefix: &str) -> Result<impl Iterator<Item = (String, bytes::Bytes)>> {
        Ok(self.db.scan_prefix(prefix).filter_map(|x| {
            x.ok().map(|(k, v)| {
                (
                    unsafe { String::from_utf8_unchecked(k.as_ref().to_owned()) },
                    v.as_ref().to_owned().into(),
                )
            })
        }))
    }
}

pub struct StoreIter {
    pub iter: sled::Iter,
}

impl Iterator for StoreIter {
    type Item = (String, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?.unwrap();
        let (key, value) = item;

        Some((
            unsafe { String::from_utf8_unchecked(key.as_ref().to_owned()) },
            value.as_ref().to_owned().into(),
        ))
    }
}
