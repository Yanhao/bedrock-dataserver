use std::fmt;
use std::path::PathBuf;
use std::sync::RwLock;

use anyhow::{bail, Result};
use raft::prelude::*;

use crate::config::CONFIG;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ChunkID {
    pub id: u64,
}

impl fmt::Display for ChunkID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let volume_id = self.id >> 32;
        let volume_index = self.id & 0xFFFF0000;

        write!(f, "{:08x}.{:08x}", volume_id, volume_index)
    }
}

impl ChunkID {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn chunk_path(&self) -> PathBuf {
        let work_dir: PathBuf = CONFIG
            .read()
            .unwrap()
            .work_directory
            .as_ref()
            .unwrap()
            .into();
        work_dir.join(format!("{}", *self))
    }
}

pub struct Chunk {
    pub id: ChunkID,

    pub version: u64,
    pub version_in_chunk_file: u64,

    pub raft_state: RwLock<RaftState>,
}

impl Chunk {
    pub fn create() -> Result<()> {
        todo!()
    }

    pub fn remove() -> Result<()> {
        todo!()
    }

    pub fn load() -> Self {
        todo!()
    }

    pub fn read(&self) {
        todo!()
    }

    pub fn write(&self) {
        todo!()
    }

    pub fn snapshot(&self) {
        todo!()
    }

    pub async fn apply_snapshot(&self, snap: &Snapshot) -> Result<()> {
        todo!()
    }

    pub async fn save_config(&self, conf: &ConfState) -> Result<()> {
        todo!()
    }
}
