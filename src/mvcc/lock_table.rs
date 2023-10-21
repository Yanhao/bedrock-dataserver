use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};

use super::{dstore::Dstore, model::LockItem};

const LOCK_KEY_PREFIX: &'static str = "/locks/";

#[derive(Clone)]
pub struct LockTable<'a> {
    dstore: Dstore<'a>,
}

impl<'a> LockTable<'a> {
    pub fn new(s: Dstore<'a>) -> Self {
        Self { dstore: s }
    }

    fn make_lock_key(orig_key: Bytes) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(LOCK_KEY_PREFIX.as_bytes());
        b.extend_from_slice("/".as_bytes());
        b.extend_from_slice(&orig_key);
        b.freeze()
    }

    pub async fn lock_record(&self, txid: u64, key: Bytes) -> Result<()> {
        let key = Self::make_lock_key(key);

        let Some((start, data)) = self.dstore.kv_get_prev(key.clone())? else {
            self.dstore
                .kv_set(key.clone(), LockItem::record_lock_data(txid))
                .await?;

            return Ok(());
        };

        let lock_item = LockItem::from_data(data);
        if lock_item.is_outdated() {
            let _ = self.dstore.kv_delete(start.clone()).await?;
        } else {
            if lock_item.txid == txid {
                return Ok(());
            }

            if lock_item.is_record_lock() && start == key || lock_item.range_end > key {
                bail!("lock failed, resource busy");
            }
        }

        self.dstore
            .kv_set(key, LockItem::record_lock_data(txid))
            .await
    }

    pub async fn lock_range(&self, txid: u64, start: Bytes, end: Bytes) -> Result<()> {
        let (start, end) = (Self::make_lock_key(start), Self::make_lock_key(end));

        let lprev = self.dstore.kv_get_prev(start.clone())?;
        let lnext = self.dstore.kv_get_prev(end.clone())?;

        if lprev.is_none() && lnext.is_none() {
            self.dstore
                .kv_set(start.clone(), LockItem::range_lock_data(txid, end))
                .await?;

            return Ok(());
        }

        if let Some((key, data)) = lprev {
            let lock_item = LockItem::from_data(data);
            if lock_item.is_outdated() {
                let _ = self.dstore.kv_delete(key.clone()).await?;
            } else {
                if lock_item.txid != txid
                    && (lock_item.is_record_lock() && key == start || lock_item.range_end > start)
                {
                    bail!("lock failed, resource busy");
                }
            }
        }

        if let Some((key, data)) = lnext {
            let lock_item = LockItem::from_data(data);
            if lock_item.is_outdated() {
                let _ = self.dstore.kv_delete(key.clone()).await?;
            } else {
                if lock_item.txid == txid {
                    return Ok(());
                }

                if !(lock_item.is_record_lock() && key == end) {
                    bail!("lock failed, resource busy");
                }
            }
        }

        self.dstore
            .kv_set(start, LockItem::range_lock_data(txid, end))
            .await
    }

    pub async fn unlock(&self, key: Bytes) -> Result<()> {
        let _ = self.dstore.kv_delete(Self::make_lock_key(key)).await?;

        Ok(())
    }
}
