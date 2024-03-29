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

pub struct MemLog {
    ents: Vec<Entry>,
}

impl MemLog {
    #[allow(unused)]
    pub fn new() -> Self {
        MemLog { ents: Vec::new() }
    }

    #[allow(unused)]
    pub fn clear(&mut self) {
        self.ents = vec![];
    }

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
    async fn entries(&mut self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>> {
        let offset = self.ents[0].index;

        if lo <= offset {
            bail!(MemLogError::EntryCompacted)
        }

        if hi > self.next_index() {
            bail!(MemLogError::IndexOutOfBound)
        }

        if self.ents.len() == 1 {
            bail!(MemLogError::EmptyRepLog)
        }

        let ret = &self.ents[(lo - offset) as usize..(hi - offset) as usize];

        Ok(Self::limit_size(ret.to_owned(), max_size))
    }

    async fn append(&mut self, ents: Vec<Entry>, _discard: bool) -> Result<u64> {
        if ents.is_empty() {
            bail!(MemLogError::BaseError);
        }

        let first = self.first_index();
        let last = self.next_index() - 1;

        if last < first {
            bail!(MemLogError::BaseError);
        }

        let mut new_ents: Vec<Entry> = vec![];

        if first > ents[0].index {
            new_ents = ents[(first - ents[0].index) as usize..].to_owned()
        }

        let offset = (ents[0].index - self.ents[0].index) as usize;

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

        Ok(self.next_index() - 1)
    }

    async fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.ents[0].index;
        if compact_index <= offset {
            bail!(MemLogError::FailedToCompact);
        }
        if compact_index > self.next_index() - 1 {
            bail!(MemLogError::IndexOutOfBound);
        }
        let i = (compact_index - offset) as usize;
        let mut ents = vec![Entry {
            op: "".to_string(),
            key: Vec::new(),
            value: Vec::new(),
            index: self.ents[i].index,
        }];
        ents.append(self.ents[i + 1..].to_owned().as_mut());

        self.ents = ents;

        Ok(())
    }

    fn first_index(&self) -> u64 {
        self.ents.len() as u64 + 1
    }

    fn next_index(&self) -> u64 {
        if self.ents.is_empty() {
            return 0;
        }
        self.ents[0].index + self.ents.len() as u64
    }
}
