use std::slice;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use raft::prelude::*;

use anyhow::{bail, Result};
use prost::Message;

use crate::chunk::{Chunk, ChunkID};
use crate::journal::Journal;

#[derive(Clone)]
struct ReplicateItem {
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,

    // data location in chunk file
    pub offset: u64,
    pub length: u64,

    // data location in wal
    pub wal_nu: u32,
    pub record_index: u32,

    pub commit: bool, // TODO: is this really needed
}

pub struct RaftLog {
    id: ChunkID,

    chunk: Arc<Chunk>,
    journal: Arc<Journal>,

    replog: Vec<ReplicateItem>,

    first_log_index: AtomicU64,
    last_log_index: AtomicU64,

    snap: raft::eraftpb::Snapshot,
}

impl RaftLog {
    fn new(journal: Arc<Journal>, chunk: Arc<Chunk>) -> Self {
        Self {
            id: chunk.id,
            chunk,
            journal,

            replog: Vec::new(),

            first_log_index: AtomicU64::new(chunk.version),
            last_log_index: AtomicU64::new(chunk.version),

            snap: raft::eraftpb::Snapshot::default(),
        }
    }

    fn append_entries(&self, entries: Vec<ReplicateItem>) -> Result<()> {
        for e in entries.iter() {
            e.index = self.last_log_index;
            e.term = self.chunk.raft_state.hard_stat.term;
            e.commit = false;

            self.replog.append(e);
            self.last_log_index += 1;
        }

        Ok(())
    }

    fn compact_entries_before(&self, idx: u64) -> Result<()> {
        let vector_idx = idx - self.first_log_index;

        self.replog = self.replog[vector_idx as usize..].clone();

        Ok(())
    }

    fn save_snapshot(&mut self, snap: raft::eraftpb::Snapshot) -> Result<()> {
        self.snap = snap;
        Ok(())
    }

    fn save_hardstate(&mut self, hs: HardState) -> Result<()> {
        todo!()
    }
}

impl Storage for RaftLog {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.chunk.raft_state.read().unwrap().clone())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        let first = self.first_log_index.load(Ordering::Relaxed);
        let last = self.last_log_index.load(Ordering::Relaxed);

        if low < first {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if high > last + 1 {
            panic!("index out of bound (last: {}, high: {})", last + 1, high)
        }

        let mut ret = Vec::new();

        for i in (low - first)..(high - first) {
            let item = self.replog[i as usize];

            let data = unsafe {
                slice::from_raw_parts(
                    &item as *const _ as *const u8,
                    std::mem::size_of::<ReplicateItem>(),
                )
            };

            let entry = Entry {
                entry_type: item.entry_type,
                term: item.term,
                index: item.index,
                data: data.to_owned(),

                // the following parts are not used
                context: vec![],
                sync_log: false,
            };

            ret.push(entry);
        }

        Ok(ret)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_log_index.load(Ordering::Relaxed))
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_log_index.load(Ordering::Relaxed))
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let first = self.first_log_index.load(Ordering::Relaxed);
        if idx < first {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let offset = self.replog[0].index;
        assert!(offset <= idx);
        if idx - offset >= self.replog.len() as u64 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        Ok(self.replog[(idx - first) as usize].term)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        Ok(self.snap.clone())
    }
}

struct RaftLogManager {}
