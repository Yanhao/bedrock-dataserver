use std::fmt;
use std::path::PathBuf;

use anyhow::{bail, Result};
use raft::prelude::*;

use crate::config::CONFIG;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ChunkID {
    volume_id: u32,
    index: u32,
}

impl fmt::Display for ChunkID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}.{:08x}", self.volume_id, self.index)
    }
}

impl ChunkID {
    pub fn new(volume_id: u32, index: u32) -> Self {
        Self { volume_id, index }
    }

    pub fn chunk_path(&self) -> PathBuf {
        let work_dir: PathBuf = CONFIG.read().unwrap().work_directory.as_ref().unwrap().into();
        work_dir.join(format!("{}", *self))
    }
}

pub struct Chunk {
    pub id: ChunkID,
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

    pub async fn apply_entry(&self, entry: &Entry) {}

    pub async fn apply_snapshot(&self, snap: &Snapshot) -> Result<()> {
        todo!()
    }
}
