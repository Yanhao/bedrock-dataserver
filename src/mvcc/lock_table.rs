use anyhow::{bail, Result};
use bytes::Bytes;

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

    fn make_lock_key(orig_key: &str) -> String {
        format!("{LOCK_KEY_PREFIX}{orig_key}")
    }

    pub async fn lock_record(&self, key: &str) -> Result<()> {
        let key = &Self::make_lock_key(key);

        let l = self.dstore.kv_get_prev(key)?;
        if l.is_none() {
            self.dstore.kv_set(key, Bytes::new()).await?;

            return Ok(());
        }
        let l = l.unwrap();

        if l.1.is_empty() && l.0 == *key {
            bail!("lock failed");
        }
        let (_start, end) = l;
        if end > key {
            bail!("lock failed");
        }

        self.dstore.kv_set(key, Bytes::new()).await?;

        Ok(())
    }

    pub async fn lock_range(&self, start: &str, end: &str) -> Result<()> {
        let (start, end) = (&Self::make_lock_key(start), &Self::make_lock_key(end));

        let lprev = self.dstore.kv_get_prev(start)?;
        let lnext = self.dstore.kv_get_next(start)?;

        if lprev.is_none() && lnext.is_none() {
            self.dstore
                .kv_set(start, end.as_bytes().to_vec().into())
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

        self.dstore
            .kv_set(start, end.as_bytes().to_vec().into())
            .await?;

        Ok(())
    }

    pub async fn unlock(&self, key: &str) -> Result<()> {
        let _ = self.dstore.kv_delete(&Self::make_lock_key(key)).await?;

        Ok(())
    }
}
