use std::sync::atomic::AtomicU64;

use anyhow::{bail, Result};

use dataserver::replog_pb::Entry;

use super::{replicate_log::ReplicateLog, Shard, order_keeper::OrderKeeper};

pub struct Fsm {
    pub shard: Shard,
    rep_log: ReplicateLog,

    next_index: AtomicU64,

    order_keeper: OrderKeeper,

}

impl Fsm {
    pub fn new(shard: Shard, rep_log: ReplicateLog) -> Self {
        Self {
            shard,
            rep_log,


            next_index: 0.into(),

            order_keeper: OrderKeeper::new(),
        }
    }

    pub async fn apply(&mut self, entry: Entry) -> Result<Entry> {
        let mut entry = entry.clone();
        entry.index = self
            .next_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.order_keeper.ensure_order(entry.index).await;

        self.rep_log.append(vec![entry.clone()]).unwrap();

        self.order_keeper.pass_order(entry.index).await;


        if entry.op == "get" {
            self.shard
                .put(entry.key.as_slice(), entry.value.as_slice())
                .await
                .unwrap();
        }

        Ok(entry)
    }

    pub fn last_index(&self) -> u64 {
        self.rep_log.last_index()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.shard.get(key).await
    }
}
