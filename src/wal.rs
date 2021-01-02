use raft::prelude::*;

use anyhow::{bail, Result};

pub struct Wal {}

impl Wal {
    fn new() -> Self {
        todo!()
    }

    fn try_batch() {}
    fn append() {}
    fn snapshot() {}
    fn gc() {}
    fn write_back() {}
}

struct WalFile {}

struct ChunkSnapshot {}

struct RaftMeta {}

pub struct LogStorage {
    wal: Wal,
    snap: ChunkSnapshot,
    config: RaftMeta,
}

impl Storage for LogStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        todo!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        todo!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        todo!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        todo!()
    }
}


impl LogStorage {
    pub async fn append_entries(&self, entries: &Vec<Entry>) -> Result<()> {
        todo!()
    }

    pub async fn save_hardstate(&self, hardstat: &HardState) -> Result<()> {
        todo!()
    }

    pub async fn save_snapshot(&self, snap: &Snapshot) -> Result<()> {
        todo!()
    }

    pub async fn save_configration(&self, conf: &ConfState) -> Result<()> {
        todo!()
    }
}
