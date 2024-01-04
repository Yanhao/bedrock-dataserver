use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use tokio::sync::broadcast;

use super::dstore::Dstore;
use super::model::KvItem;
use super::mvcc::KEY_INDEX_PREFIX;
use crate::kv_store::KvStore;
use crate::shard::Shard;

#[allow(dead_code)]
pub struct GarbageCollector {}

impl GarbageCollector {
    fn make_versioned_key_prefix(key: Bytes) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(&key);
        b.extend_from_slice(format!("@").as_bytes());
        b.freeze()
    }

    pub async fn do_gc(shard: Arc<Shard>, stop_ch: &mut broadcast::Receiver<()>) {
        let it = shard
            .get_kv_store()
            .kv_scan(KEY_INDEX_PREFIX.as_bytes().into())
            .unwrap();

        for kv in it {
            if stop_ch.try_recv().is_ok() {
                break;
            }

            Self::do_gc_one_key(shard.clone(), kv.0.as_ref().to_owned().into()).await;
        }
    }

    async fn do_gc_one_key(shard: Arc<Shard>, key: Bytes) {
        let it = shard
            .get_kv_store()
            .kv_scan(Self::make_versioned_key_prefix(key.clone()))
            .unwrap();

        for kv in it {
            let Ok(item) = serde_json::from_slice::<KvItem>(&kv.1) else {
                continue;
            };

            if !item.commited {
                continue;
            }

            if Utc.timestamp_opt(item.timestamp, 0).unwrap()
                + chrono::Duration::from_std(std::time::Duration::from_secs(60)).unwrap()
                < Utc::now()
            {
                let _ = Dstore::new(&shard).kv_delete(item.key.into()).await;
            }
        }
    }
}
