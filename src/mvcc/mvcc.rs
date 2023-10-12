use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::shard::Shard;

use super::{dstore::Dstore, lock_table::LockTable};

const TX_TABLE_KEY_PREFIX: &'static str = "/tx_ids/";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
    version: u64,
    tombstone: bool,
    commited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxTableItem {
    tx_id: u64,
    timestamp: u64,
    keys: Vec<KeyVersion>,
    lock_ids: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyVersion {
    key: Vec<u8>,
    version: u64,
}

#[derive(Clone)]
pub struct MvccStore {
    dstore: Dstore,
    locks: LockTable,
}

impl MvccStore {
    fn make_tx_table_key(txid: u64) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(TX_TABLE_KEY_PREFIX.as_bytes());
        b.extend_from_slice(format!("/{txid}").as_bytes());
        b.freeze()
    }

    fn make_versioned_key(key: Bytes, version: u64) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(&key);
        b.extend_from_slice(format!("@{version}").as_bytes());
        b.freeze()
    }

    async fn commit_version(&self, key: Bytes, version: u64) -> Result<()> {
        let key = Self::make_versioned_key(key, version);

        let item = self
            .dstore
            .kv_get(key.clone())?
            .ok_or(anyhow!("no such key"))?;

        let mut item: Item = serde_json::from_slice(&item)?;

        item.commited = true;

        let _ = self
            .dstore
            .kv_set(key, serde_json::to_vec(&item)?.into())
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

    pub fn get_until_version(&self, key: Bytes, version: u64) -> Result<Option<Bytes>> {
        let mut key = Self::make_versioned_key(key, version);

        loop {
            let item = self
                .dstore
                .kv_get_prev(key.clone())?
                .map(|(_k, v)| v.clone());
            if item.is_none() {
                return Ok(None);
            }
            let item: Item = serde_json::from_slice(&item.unwrap())?;
            if item.commited {
                if item.tombstone {
                    return Ok(None);
                }
                return Ok(Some(item.value.into()));
            }

            key = Self::make_versioned_key(item.key.into(), item.version)
        }
    }

    pub fn scan_util_version(
        &self,
        prefix: Bytes,
        version: u64,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        Ok(self
            .dstore
            .kv_scan(prefix)?
            .filter_map(|(key, item)| {
                let item: Item = serde_json::from_slice(&item).unwrap(); // FIXME: remove this unwrap

                if !item.commited || item.version > version || item.tombstone {
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
    pub async fn set_with_version(&self, tx_id: u64, key: Bytes, value: Bytes) -> Result<()> {
        self.locks.lock_record(key.clone()).await?;

        self.dstore
            .kv_set(
                Self::make_versioned_key(key.clone(), tx_id),
                serde_json::to_vec(&Item {
                    key: key.to_vec(),
                    version: tx_id,
                    value: value.to_vec(),
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    tombstone: false,
                    commited: false,
                })
                .unwrap()
                .into(),
            )
            .await?;

        // FIXME:
        self.dstore
            .kv_set(
                Self::make_tx_table_key(tx_id),
                serde_json::to_vec(&TxTableItem {
                    tx_id,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    keys: vec![KeyVersion {
                        key: key.to_vec(),
                        version: tx_id,
                    }],
                    lock_ids: vec![key.to_vec()],
                })
                .unwrap()
                .into(),
            )
            .await?;

        Ok(())
    }

    pub async fn del_with_version(&self, tx_id: u64, key: Bytes) -> Result<()> {
        self.locks.lock_record(key.clone()).await?;

        self.dstore
            .kv_set(
                Self::make_versioned_key(key.clone(), tx_id),
                serde_json::to_vec(&Item {
                    key: key.to_vec(),
                    version: tx_id,
                    value: vec![],
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    tombstone: true,
                    commited: false,
                })
                .unwrap()
                .into(),
            )
            .await?;

        // FIXME:
        self.dstore
            .kv_set(
                Self::make_tx_table_key(tx_id),
                serde_json::to_vec(&TxTableItem {
                    tx_id,
                    timestamp: chrono::Utc::now().timestamp() as u64,
                    keys: vec![KeyVersion {
                        key: key.to_vec(),
                        version: tx_id,
                    }],
                    lock_ids: vec![key.to_vec()],
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
            .kv_get(Self::make_tx_table_key(tx_id))?
            .ok_or(anyhow!("no such tx_id"))?;

        let tx_table: TxTableItem = serde_json::from_slice(&tx_table)?;

        for i in tx_table.keys.iter() {
            self.commit_version(Bytes::from(i.key.clone()), i.version)
                .await?;
        }

        for id in tx_table.lock_ids.iter() {
            self.locks.unlock(Bytes::from(id.clone())).await?;
        }

        Ok(())
    }

    pub async fn abort_tx(&self, tx_id: u64) -> Result<()> {
        let tx_table = self
            .dstore
            .kv_delete(Self::make_tx_table_key(tx_id))
            .await?
            .ok_or(anyhow!("no such tx_id"))?;

        let tx_table: TxTableItem = serde_json::from_slice(&tx_table)?;

        for id in tx_table.lock_ids.iter() {
            self.locks.unlock(Bytes::from(id.clone())).await?;
        }

        Ok(())
    }
}

// for locks
impl MvccStore {
    pub async fn lock_record(&self, key: Bytes) -> Result<()> {
        self.locks.lock_record(key).await
    }

    pub async fn lock_range(&self, start: Bytes, end: Bytes) -> Result<()> {
        self.locks.lock_range(start, end).await
    }
}
