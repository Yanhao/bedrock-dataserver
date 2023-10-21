use anyhow::Result;
use bytes::Bytes;
use tracing::{debug, error};

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
        self.shard.kv_store.kv_get(key.clone()).inspect_err(|e| {
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
            .kv_store
            .kv_get_prev_or_eq(key.clone())
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
            .kv_store
            .kv_get_next_or_eq(key.clone())
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
            .kv_store
            .kv_scan(prefix.clone())
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
        let mut entry_with_notifier = self
            .shard
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

        debug!("start wait result");
        entry_with_notifier.wait_result().await.inspect_err(|e| {
            error!(
                msg = "write wait result failed.",
                err = ?e,
                op = "kv_set",
                key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
            )
        })?;
        let _ = self
            .shard
            .apply_entry(&entry_with_notifier.entry)
            .await
            .inspect_err(|e| {
                error!(
                    msg = "apply entry failed.",
                    err = ?e,
                    op = "kv_set",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })?;

        debug!(" wait result successed");

        Ok(())
    }

    pub async fn kv_set_ns(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut entry_with_notifier = self
            .shard
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

        debug!("start wait result");
        entry_with_notifier.wait_result().await.inspect_err(|e| {
            error!(
                msg = "write wait result failed.",
                err = ?e,
                op = "kv_set",
                key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
            )
        })?;
        let _ = self
            .shard
            .apply_entry(&entry_with_notifier.entry)
            .await
            .inspect_err(|e| {
                error!(
                    msg = "apply entry failed.",
                    err = ?e,
                    op = "kv_set",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })?;

        debug!(" wait result successed");

        Ok(())
    }

    pub async fn kv_delete(&self, key: Bytes) -> Result<Option<Bytes>> {
        let mut entry_with_notifier = self
            .shard
            .process_write(shard::Operation::Del, &key, &vec![])
            .await
            .inspect_err(|e| {
                error!(
                    msg = "process wirte failed.",
                    err = ?e,
                    op = "kv_delete",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })?;

        debug!("start wait result");
        entry_with_notifier.wait_result().await.inspect_err(|e| {
            error!(
                msg = "write wait result failed.",
                err = ?e,
                op = "kv_delete",
                key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
            )
        })?;

        self.shard
            .apply_entry(&entry_with_notifier.entry)
            .await
            .inspect(|_| debug!(" wait result successed"))
            .inspect_err(|e| {
                error!(
                    msg = "apply entry failed.",
                    err = ?e,
                    op = "kv_delete",
                    key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
                )
            })
    }
}
