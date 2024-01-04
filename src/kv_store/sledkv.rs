use std::iter::Iterator;
use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use sled;
use tokio::fs::remove_dir_all;
use tracing::{debug, error, info};

use crate::config::CONFIG;

use super::KvStore;

pub struct SledStore {
    pub db: sled::Db,
}

pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

impl SledStore {
    fn data_dir_path(shard_id: u64) -> PathBuf {
        let data_dir: PathBuf = CONFIG.read().data_dir.as_ref().unwrap().into();

        data_dir
            .join::<String>(format!(
                "{:08x}",
                ((shard_id & 0xFFFFFFFF_00000000) >> 32) as u32
            ))
            .join::<String>(format!("{:08x}", (shard_id & 0x00000000_FFFFFFFF) as u32))
    }

    pub async fn load(shard_id: u64) -> Result<Self> {
        info!(
            "load shard sledkv db 0x{:016x} from path: {:?}",
            shard_id,
            Self::data_dir_path(shard_id)
        );
        let store = sled::open(Self::data_dir_path(shard_id))?;

        Ok(Self { db: store })
    }

    pub async fn create(shard_id: u64) -> Result<()> {
        info!("create shard sledkv db 0x{:016x}", shard_id);
        let _ = sled::open(Self::data_dir_path(shard_id))?;

        Ok(())
    }

    pub async fn remove(shard_id: u64) -> Result<()> {
        info!("remove shard sledkv db 0x{:016x}", shard_id);
        remove_dir_all(Self::data_dir_path(shard_id)).await?;

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
        let sz = self.db.flush()?;
        info!("flush size: {sz}");

        Ok(())
    }

    fn kv_delete(&self, key: Bytes) -> Result<Option<Bytes>> {
        let Some(value) = self.db.remove(key)? else {
            return Ok(None);
        };

        let _ = self.db.flush()?;

        Ok(Some(Bytes::copy_from_slice(&value)))
    }

    fn kv_delete_range(&self, start_key: Bytes, end_key: Bytes) -> Result<()> {
        let (start_key, end_key): (&[u8], &[u8]) = (&start_key, &end_key);
        for kv_result in self.db.range(start_key..end_key) {
            let Ok((key, _value)) = kv_result else {
                continue;
            };

            debug!("remove key {}", String::from_utf8_lossy(&key));

            self.db.remove(key)?;
        }

        let _ = self.db.flush()?;

        Ok(())
    }

    fn kv_get_prev_or_eq(&self, key: Bytes) -> Result<Option<(Bytes, bytes::Bytes)>> {
        if let Some(v) = self.kv_get(key.clone())? {
            return Ok(Some((key, v)));
        }

        let Some((key, value)) = self.db.get_lt(key)? else {
            return Ok(None);
        };

        Ok(Some((key.to_vec().into(), Bytes::copy_from_slice(&value))))
    }

    fn kv_get_next_or_eq(&self, key: Bytes) -> Result<Option<(Bytes, bytes::Bytes)>> {
        if let Some(v) = self.kv_get(key.clone())? {
            return Ok(Some((key, v)));
        }

        let Some((key, value)) = self.db.get_gt(key)? else {
            return Ok(None);
        };

        Ok(Some((key.to_vec().into(), Bytes::copy_from_slice(&value))))
    }

    fn kv_scan(&self, prefix: Bytes) -> Result<impl Iterator<Item = (Bytes, Bytes)>> {
        Ok(self.db.scan_prefix(prefix).filter_map(|x| {
            x.ok()
                .map(|(k, v)| (k.as_ref().to_vec().into(), v.as_ref().to_owned().into()))
        }))
    }

    fn kv_range(
        &self,
        start_key: Bytes,
        end_key: Bytes,
    ) -> impl Iterator<Item = (Bytes, Bytes)> + Send {
        self.db
            .range(start_key..end_key)
            .into_iter()
            .filter_map(|kv| {
                kv.ok()
                    .map(|(k, v)| (k.as_ref().to_vec().into(), v.as_ref().to_owned().into()))
            })
    }

    fn take_snapshot(&self) -> Result<impl Iterator<Item = (Bytes, Bytes)>> {
        info!("creat snapshot iterator ...");

        for i in self.db.iter() {
            if let Ok((k, v)) = i {
                info!("iterator: key: {:?}, value: {:?}", k.as_ref(), v.as_ref());
            }
        }

        Ok(StoreIter {
            iter: self.db.iter(),
        })
    }

    fn install_snapshot(&self, entries: Vec<(Bytes, bytes::Bytes)>) -> Result<()> {
        for (key, value) in entries.into_iter() {
            info!("install_snapshot, key: {:?}, value: {:?}", key, value);

            self.kv_set(key.clone(), value.clone()).inspect_err(|e| {
                error!(
                    "install_snapshot, kv_set failed, err: {e}, key: {:?}, value: {:?}",
                    key, value,
                )
            })?;
        }

        Ok(())
    }
}

pub struct StoreIter {
    pub iter: sled::Iter,
}

impl Iterator for StoreIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.iter.next()?.unwrap();

        Some((
            key.as_ref().to_vec().into(),
            value.as_ref().to_owned().into(),
        ))
    }
}
