use anyhow::{bail, Result};

use dataserver::replog_pb::Entry;
use log::warn;
use prost::Message;

use super::error::ShardError;

pub struct ReplicatLog {
    ents: Vec<Entry>,
}

impl ReplicatLog {
    pub fn new() -> Self {
        ReplicatLog { ents: Vec::new() }
    }

    pub fn entries(&self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>> {
        let offset = self.ents[0].index;

        if lo <= offset {
            bail!(ShardError::EntryCompacted)
        }

        if hi > self.last_index() + 1 {
            bail!(ShardError::IndexOutOfBound)
        }

        if self.ents.len() == 1 {
            bail!(ShardError::EmptyRepLog)
        }

        let ret = &self.ents[(lo - offset) as usize..(hi - offset) as usize];

        Ok(limit_size(ret.to_owned(), max_size))
    }

    pub fn last_index(&self) -> u64 {
        self.ents[0].index + self.ents.len() as u64 - 1
    }

    pub fn first_index(&self) -> u64 {
        self.ents.len() as u64 + 1
    }

    pub fn append(&mut self, ents: Vec<Entry>) -> Result<()> {
        if ents.len() == 0 {
            return Ok(());
        }

        let first = self.first_index();
        let last = self.last_index();

        if last < first {
            return Ok(());
        }

        let mut new_ents: Vec<Entry> = vec![];

        if first > ents[0].index {
            new_ents = ents[(first - ents[0].index) as usize..].to_owned()
        }

        let offset = (ents[0].index - self.ents[0].index) as usize;

        if self.ents.len() > offset {
            self.ents = self.ents[..offset].to_owned();
            self.ents.append(&mut new_ents);
        } else if self.ents.len() == offset {
            self.ents.append(&mut new_ents);
        } else {
            warn!("");
            self.ents.append(&mut new_ents);
        }

        Ok(())
    }

    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.ents[0].index;
        if compact_index <= offset {
            bail!(ShardError::FailedToCompact);
        }
        if compact_index > self.last_index() {
            bail!(ShardError::IndexOutOfBound);
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

    pub fn clear(&mut self) {
        self.ents = vec![];
    }
}

fn limit_size(ents: Vec<Entry>, max_size: u64) -> Vec<Entry> {
    if ents.len() == 0 {
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

    return ents[..limit].to_owned();
}
