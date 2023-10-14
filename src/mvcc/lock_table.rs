use anyhow::{bail, ensure, Result};
use bytes::{Bytes, BytesMut};

use super::dstore::Dstore;

const LOCK_KEY_PREFIX: &'static str = "/lock/";

#[derive(Clone)]
pub struct LockTable {
    dstore: Dstore,
}

impl LockTable {
    pub fn new(s: Dstore) -> Self {
        Self { dstore: s }
    }

    fn make_lock_key(orig_key: Bytes) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(LOCK_KEY_PREFIX.as_bytes());
        b.extend_from_slice("/".as_bytes());
        b.extend_from_slice(&orig_key);
        b.freeze()
    }

    pub async fn lock_record(&self, key: Bytes) -> Result<()> {
        let key = Self::make_lock_key(key);

        let Some((start, end)) = self.dstore.kv_get_prev(key.clone())? else {
            self.dstore.kv_set(key.clone(), Bytes::new()).await?;

            return Ok(());
        };

        ensure!(
            (!end.is_empty() || start != key) && end <= key,
            "lock failed"
        );

        self.dstore.kv_set(key, Bytes::new()).await?;

        Ok(())
    }

    pub async fn lock_range(&self, start: Bytes, end: Bytes) -> Result<()> {
        let (start, end) = (Self::make_lock_key(start), Self::make_lock_key(end));

        let lprev = self.dstore.kv_get_prev(start.clone())?;
        let lnext = self.dstore.kv_get_next(start.clone())?;

        if lprev.is_none() && lnext.is_none() {
            self.dstore
                .kv_set(start.clone(), end.clone().to_vec().into())
                .await?;

            return Ok(());
        }

        if let Some(prev) = lprev {
            if prev.1.is_empty() && prev.0 == *start || prev.1 > start {
                bail!("lock failed");
            }
        }

        if let Some(next) = lnext {
            if next.1.is_empty() && next.0 == *start || next.0 > *start {
                bail!("lock failed");
            }
        }

        self.dstore.kv_set(start, end.to_vec().into()).await
    }

    pub async fn unlock(&self, key: Bytes) -> Result<()> {
        let _ = self.dstore.kv_delete(Self::make_lock_key(key)).await?;

        Ok(())
    }
}
