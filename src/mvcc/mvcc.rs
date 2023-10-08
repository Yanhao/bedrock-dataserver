use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::shard::{self, Shard};

use super::{dstore::Dstore, lock_table::LockTable};

const TX_TABLE_KEY_PREFIX: &'static str = "/tx_ids/";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    key: String,
    value: Vec<u8>,
    version: u64,
    commited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxTableItem {
    tx_id: u64,
    keys: Vec<KeyVersion>,
    lock_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyVersion {
    key: String,
    version: u64,
}

#[derive(Clone)]
pub struct MvccStore {
    dstore: Dstore,
    locks: LockTable,
}

impl MvccStore {
    fn make_tx_table_key(key: &str) -> String {
        format!("{TX_TABLE_KEY_PREFIX}{key}")
    }

    fn make_versioned_key(key: &str, version_str: &str) -> String {
        format!("{key}@{version_str}")
    }

    async fn commit_version(&self, key: &str, version: u64) -> Result<()> {
        let key = Self::make_versioned_key(key, &version.to_string());

        let item = self.dstore.kv_get(&key)?.ok_or(anyhow!("no such key"))?;

        let mut item: Item = serde_json::from_slice(&item)?;

        item.commited = true;

        let _ = self
            .dstore
            .kv_set(&key, serde_json::to_vec(&item)?.into())
            .await?;

        Ok(())
    }
}

// for read
impl MvccStore {
    pub fn new(shard: std::sync::Arc<Shard>) -> Self {
        let s = Dstore::new(shard);
        Self {
            dstore: s.clone(),
            locks: LockTable::new(s),
        }
    }

    pub fn get_until_version(&self, key: &str, version: u64) -> Result<Option<Bytes>> {
        let mut key = Self::make_versioned_key(key, &version.to_string());

        loop {
            let item = self.dstore.kv_get_prev(&key)?.map(|(_k, v)| v.clone());
            if item.is_none() {
                return Ok(None);
            }
            let item: Item = serde_json::from_slice(&item.unwrap())?;

            if item.commited {
                return Ok(Some(item.value.into()));
            }

            key = Self::make_versioned_key(&item.key, &item.version.to_string())
        }
    }

    pub fn scan_util_version(
        &self,
        prefix: &str,
        version: u64,
        limit: usize,
    ) -> Result<Vec<(String, Bytes)>> {
        Ok(self
            .dstore
            .kv_scan(prefix)?
            .filter_map(|(key, item)| {
                let item: Item = serde_json::from_slice(&item).unwrap(); // FIXME: remove this unwrap

                if !item.commited || item.version > version {
                    return None;
                }

                Some((key, item.value.into()))
            })
            .take(limit)
            .collect::<Vec<_>>())
    }
}

// for write
impl MvccStore {
    pub async fn set_with_version(&self, tx_id: u64, key: &str, value: Bytes) -> Result<()> {
        self.locks.lock_record(key).await?;

        self.dstore
            .kv_set(
                &Self::make_versioned_key(key, &tx_id.to_string()),
                serde_json::to_vec(&Item {
                    key: key.to_string(),
                    version: tx_id,
                    value: value.to_vec(),
                    commited: false,
                })
                .unwrap()
                .into(),
            )
            .await?;

        // FIXME:
        self.dstore
            .kv_set(
                &Self::make_tx_table_key(&tx_id.to_string()),
                serde_json::to_vec(&TxTableItem {
                    tx_id,
                    keys: vec![KeyVersion {
                        key: key.to_string(),
                        version: tx_id,
                    }],
                    lock_ids: vec![key.to_string()],
                })
                .unwrap()
                .into(),
            )
            .await?;

        Ok(())
    }

    pub async fn commit_tx(&self, tx_id: u64) -> Result<()> {
        let tx_table = self
            .dstore
            .kv_get(&Self::make_tx_table_key(&tx_id.to_string()))?
            .ok_or(anyhow!("no such tx_id"))?;

        let tx_table: TxTableItem = serde_json::from_slice(&tx_table)?;

        for i in tx_table.keys.iter() {
            self.commit_version(&i.key, i.version).await?;
        }

        for id in tx_table.lock_ids.iter() {
            self.locks.unlock(id).await?;
        }

        Ok(())
    }

    pub async fn abort_tx(&self, tx_id: u64) -> Result<()> {
        let tx_table = self
            .dstore
            .kv_delete(&Self::make_tx_table_key(&tx_id.to_string()))
            .await?
            .ok_or(anyhow!("no such tx_id"))?;

        let tx_table: TxTableItem = serde_json::from_slice(&tx_table)?;

        for id in tx_table.lock_ids.iter() {
            self.locks.unlock(id).await?;
        }

        Ok(())
    }
}

// for locks
impl MvccStore {
    pub async fn lock_record(&self, key: &str) -> Result<()> {
        self.locks.lock_record(key).await
    }

    pub async fn lock_range(&self, start: &str, end: &str) -> Result<()> {
        self.locks.lock_range(start, end).await
    }
}
