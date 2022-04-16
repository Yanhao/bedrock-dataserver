use anyhow::{bail, Result};

use dataserver::replog_pb::Entry;

use super::{replicate_log::ReplicateLog, Shard};

pub struct Fsm {
    pub shard: Shard,
    rep_log: ReplicateLog,
}

impl Fsm {
    pub fn new(shard: Shard, rep_log: ReplicateLog) -> Self {
        Self { shard, rep_log }
    }

    pub async fn apply(&mut self, entry: Entry) {
        self.rep_log.append(vec![entry.clone()]).unwrap();

        if entry.op == "get" {
            self.shard
                .put(entry.key.as_slice(), entry.value.as_slice())
                .await
                .unwrap();
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.shard.get(key).await
    }
}
