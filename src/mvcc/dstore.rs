use anyhow::Result;
use bytes::Bytes;
use tracing::error;

use crate::kv_store::KvStore;
use crate::shard::{self, Shard};

#[derive(Clone)]
pub struct Dstore<'a> {
    shard: &'a Shard,
}

impl<'a> Dstore<'a> {
    pub fn new(shard: &'a Shard) -> Self {
        Self { shard }
    }

    pub fn kv_get(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.shard
            .get_kv_store()
            .kv_get(Shard::data_key(&key).into())
            .inspect_err(|e| {
                error!(
                    msg = "kv get failed.",
                    err = ?e,
                    op = "kv_get",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })
    }

    pub fn kv_get_prev(&self, key: Bytes) -> Result<Option<(Bytes, Bytes)>> {
        self.shard
            .get_kv_store()
            .kv_get_prev_or_eq(Shard::data_key(&key).into())
            .inspect_err(|e| {
                error!(
                    msg = "kv_get_prev failed.",
                    err = ?e,
                    op = "kv_get_prev",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })
    }

    #[allow(dead_code)]
    pub fn kv_get_next(&self, key: Bytes) -> Result<Option<(Bytes, Bytes)>> {
        self.shard
            .get_kv_store()
            .kv_get_next_or_eq(Shard::data_key(&key).into())
            .inspect_err(|e| {
                error!(
                    msg = "kv_get_next failed.",
                    err = ?e,
                    op = "kv_get_next",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })
    }

    pub fn kv_scan(&self, prefix: Bytes) -> Result<impl Iterator<Item = (Bytes, Bytes)> + '_> {
        self.shard
            .get_kv_store()
            .kv_scan(Shard::data_key(&prefix).into())
            .inspect_err(|e| {
                error!(
                    msg = "kv_scan failed.",
                    err = ?e,
                    op = "kv_scan",
                    prefix = unsafe { String::from_utf8_unchecked(prefix.to_vec()) },
                )
            })
    }

    pub async fn kv_set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.shard
            .process_write(shard::Operation::Set, &key, &value.to_vec())
            .await
            .inspect_err(|e| {
                error!(
                    msg = "process wirte failed.",
                    err = ?e,
                    op = "kv_set",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })?;

        Ok(())
    }

    pub async fn kv_set_ns(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.shard
            .process_write(shard::Operation::Set, &key, &value.to_vec())
            .await
            .inspect_err(|e| {
                error!(
                    msg = "process wirte failed.",
                    err = ?e,
                    op = "kv_setns",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })?;

        Ok(())
    }

    pub async fn kv_delete(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.shard
            .process_write(shard::Operation::Del, &key, &vec![])
            .await
            .inspect_err(|e| {
                error!(
                    msg = "process wirte failed.",
                    err = ?e,
                    op = "kv_delete",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })
    }
}
