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
    pub key: Bytes,
    pub value: Bytes,
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
    pub fn take_snapshot(&self) -> Result<impl Iterator<Item = (Bytes, bytes::Bytes)>> {
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

    pub fn install_snapshot(&self, entries: Vec<(Bytes, bytes::Bytes)>) -> Result<()> {
        for ent in entries.iter() {
            info!("install_snapshot, key: {:?}, value: {:?}", ent.0, ent.1);

            self.kv_set(ent.0.clone(), ent.1.clone()).inspect_err(|e| {
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
    fn kv_get(&self, key: Bytes) -> Result<Option<Bytes>> {
        let iv = self.db.get(key)?;
        Ok(iv.map(|v| v.as_ref().to_owned().into()))
    }

    fn kv_set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.db.insert(key, value.to_vec())?;
        Ok(())
    }

    fn kv_delete(&self, key: Bytes) -> Result<Option<Bytes>> {
        let a = self.db.remove(key)?;
        if a.is_none() {
            return Ok(None);
        }

        Ok(Some(Bytes::copy_from_slice(&a.unwrap())))
    }

    fn kv_delete_range(&self, start_key: Bytes, _end_key: Bytes) -> Result<()> {
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

    fn kv_get_prev_or_eq(&self, key: Bytes) -> Result<Option<(Bytes, bytes::Bytes)>> {
        if let Some(a) = self.kv_get(key.clone())? {
            return Ok(Some((key, a)));
        }

        let a = self.db.get_lt(key)?;
        if a.is_none() {
            return Ok(None);
        }
        let a = a.unwrap();

        return Ok(Some((a.0.to_vec().into(), Bytes::copy_from_slice(&a.1))));
    }

    fn kv_get_next_or_eq(&self, key: Bytes) -> Result<Option<(Bytes, bytes::Bytes)>> {
        if let Some(a) = self.kv_get(key.clone())? {
            return Ok(Some((key, a)));
        }

        let a = self.db.get_gt(key)?;
        if a.is_none() {
            return Ok(None);
        }
        let a = a.unwrap();

        return Ok(Some((a.0.to_vec().into(), Bytes::copy_from_slice(&a.1))));
    }

    fn kv_scan(&self, prefix: Bytes) -> Result<impl Iterator<Item = (Bytes, bytes::Bytes)>> {
        Ok(self.db.scan_prefix(prefix).filter_map(|x| {
            x.ok()
                .map(|(k, v)| (k.as_ref().to_vec().into(), v.as_ref().to_owned().into()))
        }))
    }
}

pub struct StoreIter {
    pub iter: sled::Iter,
}

impl Iterator for StoreIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?.unwrap();
        let (key, value) = item;

        Some((
            key.as_ref().to_vec().into(),
            value.as_ref().to_owned().into(),
        ))
    }
}
