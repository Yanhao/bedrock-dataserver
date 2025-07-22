//! mem_log.rs
//! In-memory log implementation, used as a WAL cache layer or lightweight implementation for testing.
//! Supports log entry append, query, compaction, and basic error handling.

use std::cmp::Ordering;

use anyhow::{bail, Result};
use prost::Message;
use thiserror::Error;
use tracing::warn;

use idl_gen::replog_pb::Entry;

use super::WalTrait;

#[derive(Error, Debug)]
pub enum MemLogError {
    #[error("entry has been compacted")]
    EntryCompacted,
    #[error("index of out bound")]
    IndexOutOfBound,
    #[error("empty replicate log")]
    EmptyRepLog,
    #[error("compact failed")]
    FailedToCompact,

    #[error("base error")]
    BaseError,
}

/// In-memory log struct, holds an array of log entries
pub struct MemLog {
    ents: Vec<Entry>,
}

impl MemLog {
    /// Create a new empty in-memory log
    #[allow(unused)]
    pub fn new() -> Self {
        MemLog { ents: Vec::new() }
    }

    /// Clear all log entries
    #[allow(unused)]
    pub fn clear(&mut self) {
        self.ents = vec![];
    }

    /// Limit the total bytes of returned log entries to max_size
    fn limit_size(ents: Vec<Entry>, max_size: u64) -> Vec<Entry> {
        if ents.is_empty() {
            return Vec::new();
        }

        let mut size = ents[0].encoded_len();
        let mut limit: usize = 1;

        while limit < ents.len() {
            size += ents[limit].encoded_len();
            if size > max_size as usize {
                break;
            }

            limit += 1;
        }

        ents[..limit].to_owned()
    }
}

impl WalTrait for MemLog {
    /// Get log entries in range [lo, hi], up to max_size bytes
    async fn entries(&mut self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>> {
        if self.ents.is_empty() {
            bail!(MemLogError::EmptyRepLog)
        }
        let offset = self.ents[0].index;

        if lo <= offset {
            bail!(MemLogError::EntryCompacted)
        }

        if hi > self.next_index().await {
            bail!(MemLogError::IndexOutOfBound)
        }

        if self.ents.len() == 1 {
            bail!(MemLogError::EmptyRepLog)
        }

        let lo_idx = (lo - offset) as usize;
        let hi_idx = (hi - offset) as usize;
        if lo_idx >= self.ents.len() || hi_idx > self.ents.len() || lo_idx >= hi_idx {
            bail!(MemLogError::IndexOutOfBound)
        }
        let ret = &self.ents[lo_idx..hi_idx];

        Ok(Self::limit_size(ret.to_vec(), max_size))
    }

    /// Append log entries, return the last log index
    async fn append(&mut self, ents: Vec<Entry>, _discard: bool) -> Result<u64> {
        if ents.is_empty() {
            bail!(MemLogError::BaseError);
        }

        if self.ents.is_empty() {
            self.ents = ents;
            return Ok(self.next_index().await - 1);
        }

        let first = self.first_index().await;
        let last = self.next_index().await - 1;
        let ents_first_index = ents[0].index;
        let skip = if first > ents_first_index {
            (first - ents_first_index) as usize
        } else {
            0
        };
        let mut new_ents: Vec<Entry> = if skip < ents.len() {
            ents[skip..].to_owned()
        } else {
            vec![]
        };
        let offset = if self.ents[0].index > ents_first_index {
            0
        } else {
            (ents_first_index - self.ents[0].index) as usize
        };

        match self.ents.len().cmp(&offset) {
            Ordering::Greater => {
                self.ents = self.ents[..offset].to_owned();
                self.ents.append(&mut new_ents);
            }
            Ordering::Equal => {
                self.ents.append(&mut new_ents);
            }
            Ordering::Less => {
                warn!("");
                self.ents.append(&mut new_ents);
            }
        };

        Ok(self.next_index().await - 1)
    }

    /// Log compaction, discard all before compact_index
    async fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.ents[0].index;
        if compact_index <= offset {
            bail!(MemLogError::FailedToCompact);
        }
        if compact_index > self.next_index().await - 1 {
            bail!(MemLogError::IndexOutOfBound);
        }
        let i = (compact_index - offset) as usize;
        if i >= self.ents.len() {
            bail!(MemLogError::IndexOutOfBound);
        }
        let mut ents = vec![Entry {
            index: self.ents[i].index,
            items: Vec::new(),
        }];
        ents.extend_from_slice(&self.ents[i + 1..]);
        self.ents = ents;

        Ok(())
    }

    /// Get the index of the first log entry
    async fn first_index(&self) -> u64 {
        if self.ents.is_empty() {
            0
        } else {
            self.ents[0].index
        }
    }

    /// Get the index of the next available log entry
    async fn next_index(&self) -> u64 {
        if self.ents.is_empty() {
            return 0;
        }
        self.ents[0].index + self.ents.len() as u64
    }
}
