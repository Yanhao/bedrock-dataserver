use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

use crate::shard::Shard;

use super::{dstore::Dstore, lock_table::LockTable};

const TX_TABLE_KEY_PREFIX: &'static str = "/txids/";

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
pub struct MvccStore<'a> {
    dstore: Dstore<'a>,
    locks: LockTable<'a>,
}

impl<'a> MvccStore<'a> {
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

        let item_data = self
            .dstore
            .kv_get(key.clone())?
            .ok_or(anyhow!("no such key"))?;

        let item = {
            let mut it: Item = serde_json::from_slice(&item_data)?;
            it.commited = true;
            it
        };

        self.dstore
            .kv_set(key, serde_json::to_vec(&item)?.into())
            .await
    }
}

// for read
impl<'a> MvccStore<'a> {
    pub fn new(shard: &'a Shard) -> Self {
        let s = Dstore::new(shard);
        Self {
            dstore: s.clone(),
            locks: LockTable::new(s),
        }
    }

    pub fn get_until_version(&self, key: Bytes, version: u64) -> Result<Option<Bytes>> {
        let mut key = Self::make_versioned_key(key, version);

        loop {
            let Some(item) = self
                .dstore
                .kv_get_prev(key.clone())?
                .map(|(_, v)| v.clone())
            else {
                return Ok(None);
            };

            let item: Item = serde_json::from_slice(&item)?;
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
impl<'a> MvccStore<'a> {
    pub async fn set_with_version(&self, txid: u64, key: Bytes, value: Bytes) -> Result<()> {
        self.locks.lock_record(key.clone()).await?;

        self.dstore
            .kv_set(
                Self::make_versioned_key(key.clone(), txid),
                serde_json::to_vec(&Item {
                    key: key.to_vec(),
                    version: txid,
                    value: value.to_vec(),
                    timestamp: Utc::now().timestamp() as u64,
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
                Self::make_tx_table_key(txid),
                serde_json::to_vec(&TxTableItem {
                    tx_id: txid,
                    timestamp: Utc::now().timestamp() as u64,
                    keys: vec![KeyVersion {
                        key: key.to_vec(),
                        version: txid,
                    }],
                    lock_ids: vec![key.to_vec()],
                })
                .unwrap()
                .into(),
            )
            .await
    }

    pub async fn del_with_version(&self, txid: u64, key: Bytes) -> Result<()> {
        self.locks.lock_record(key.clone()).await?;

        self.dstore
            .kv_set(
                Self::make_versioned_key(key.clone(), txid),
                serde_json::to_vec(&Item {
                    key: key.to_vec(),
                    version: txid,
                    value: vec![],
                    timestamp: Utc::now().timestamp() as u64,
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
                Self::make_tx_table_key(txid),
                serde_json::to_vec(&TxTableItem {
                    tx_id: txid,
                    timestamp: Utc::now().timestamp() as u64,
                    keys: vec![KeyVersion {
                        key: key.to_vec(),
                        version: txid,
                    }],
                    lock_ids: vec![key.to_vec()],
                })
                .unwrap()
                .into(),
            )
            .await
    }

    pub async fn commit_tx(&self, txid: u64) -> Result<()> {
        let tx_table: TxTableItem = {
            let tx_table = self
                .dstore
                .kv_get(Self::make_tx_table_key(txid))?
                .ok_or(anyhow!("no such txid"))?;
            serde_json::from_slice(&tx_table)?
        };

        for kv in tx_table.keys.iter() {
            self.commit_version(Bytes::from(kv.key.clone()), kv.version)
                .await?;
        }

        for lock_key in tx_table.lock_ids.into_iter() {
            self.locks.unlock(Bytes::from(lock_key)).await?;
        }

        Ok(())
    }

    pub async fn abort_tx(&self, txid: u64) -> Result<()> {
        let tx_table: TxTableItem = {
            let tx_table = self
                .dstore
                .kv_get(Self::make_tx_table_key(txid))?
                .ok_or(anyhow!("no such txid"))?;
            serde_json::from_slice(&tx_table)?
        };

        for id in tx_table.lock_ids.into_iter() {
            self.locks.unlock(Bytes::from(id)).await?;
        }

        Ok(())
    }
}

// for locks
impl<'a> MvccStore<'a> {
    pub async fn lock_record(&self, key: Bytes) -> Result<()> {
        self.locks.lock_record(key).await
    }

    pub async fn lock_range(&self, start: Bytes, end: Bytes) -> Result<()> {
        self.locks.lock_range(start, end).await
    }
}
