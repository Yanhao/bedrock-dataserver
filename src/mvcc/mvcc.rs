use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;

use crate::shard::Shard;

use super::{dstore::Dstore, lock_table::LockTable, model::KvItem, tx_table::TxTable};

#[derive(Clone)]
pub struct MvccStore<'a> {
    dstore: Dstore<'a>,
    locks: LockTable<'a>,
    tx_table: TxTable<'a>,
}

impl<'a> MvccStore<'a> {
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
            let mut it: KvItem = serde_json::from_slice(&item_data)?;
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
            locks: LockTable::new(s.clone()),
            tx_table: TxTable::new(s),
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

            let item: KvItem = serde_json::from_slice(&item)?;
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
                let item: KvItem = serde_json::from_slice(&item).unwrap(); // TODO: remove this unwrap

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
        self.locks.lock_record(txid, key.clone()).await?;

        self.dstore
            .kv_set(
                Self::make_versioned_key(key.clone(), txid),
                serde_json::to_vec(&KvItem {
                    key: key.to_vec(),
                    version: txid,
                    value: value.to_vec(),
                    timestamp: Utc::now().timestamp(),
                    tombstone: false,
                    commited: false,
                })
                .unwrap()
                .into(),
            )
            .await?;

        self.tx_table.put_tx_item(txid, key).await
    }

    pub async fn del_with_version(&self, txid: u64, key: Bytes) -> Result<()> {
        self.locks.lock_record(txid, key.clone()).await?;

        self.dstore
            .kv_set(
                Self::make_versioned_key(key.clone(), txid),
                serde_json::to_vec(&KvItem {
                    key: key.to_vec(),
                    version: txid,
                    value: vec![],
                    timestamp: Utc::now().timestamp(),
                    tombstone: true,
                    commited: false,
                })
                .unwrap()
                .into(),
            )
            .await?;

        self.tx_table.put_tx_item(txid, key).await
    }

    pub async fn commit_tx(&self, txid: u64) -> Result<()> {
        let tx_record = self.tx_table.get_tx_record(txid)?;

        for key in tx_record.keys.iter() {
            self.commit_version(Bytes::from(key.clone()), tx_record.txid)
                .await?;
        }

        for lock_key in tx_record.keys.into_iter() {
            self.locks.unlock(Bytes::from(lock_key)).await?;
        }

        Ok(())
    }

    pub async fn abort_tx(&self, txid: u64) -> Result<()> {
        let tx_record = self.tx_table.get_tx_record(txid)?;

        for lock_key in tx_record.keys.into_iter() {
            self.locks.unlock(Bytes::from(lock_key)).await?;
        }

        Ok(())
    }
}

// for locks
impl<'a> MvccStore<'a> {
    pub async fn lock_record(&self, txid: u64, key: Bytes) -> Result<()> {
        self.locks.lock_record(txid, key).await
    }

    pub async fn lock_range(&self, txid: u64, start: Bytes, end: Bytes) -> Result<()> {
        self.locks.lock_range(txid, start, end).await
    }
}
