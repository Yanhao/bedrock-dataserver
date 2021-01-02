use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::{bail, Result};
use lazy_static::lazy_static;

use crate::chunk::{Chunk, ChunkID};

lazy_static! {
    pub static ref CHUNK_MANAGER: RwLock<ChunkManager> =
        unsafe { MaybeUninit::uninit().assume_init() };
}

pub struct ChunkManager {
    chunks: RwLock<HashMap<ChunkID, Arc<Chunk>>>,
}

impl ChunkManager {
    pub fn new() -> Self {
        Self {
            chunks: RwLock::new(HashMap::new()),
        }
    }

    pub fn put(&self, chunk: Arc<Chunk>) -> Result<()> {
        self.chunks.write().unwrap().insert(chunk.id, chunk);
        Ok(())
    }

    pub fn get(&self, chunkid: ChunkID) -> Result<Arc<Chunk>> {
        let ret = self.chunks.read().unwrap().get(&chunkid).unwrap().clone();

        Ok(ret)
    }

    pub fn remove(&self, chunkid: ChunkID) -> Result<()> {
        self.chunks.write().unwrap().remove(&chunkid).unwrap();
        Ok(())
    }
}
