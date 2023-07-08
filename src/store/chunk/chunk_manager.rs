use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::{bail, ensure, Result};
use lazy_static::lazy_static;

use crate::chunk::{Chunk, ChunkID};

// lazy_static! {
//     pub static ref CHUNK_MANAGER: RwLock<ChunkManager> =
//         unsafe { MaybeUninit::uninit().assume_init() };
// }

lazy_static! {
    pub static ref CHUNK_MANAGER: RwLock<ChunkManager> = RwLock::new(ChunkManager::new().unwrap());
}

// pub struct ChunkManager {
//     chunks: RwLock<HashMap<ChunkID, Arc<Chunk>>>,
// }

// impl ChunkManager {
//     pub fn new() -> Self {
//         Self {
//             chunks: RwLock::new(HashMap::new()),
//         }
//     }

//     pub fn put(&self, chunk: Arc<Chunk>) -> Result<()> {
//         self.chunks.write().unwrap().insert(chunk.id, chunk);
//         Ok(())
//     }

//     pub async fn get_chunk(&self, chunkid: ChunkID) -> Result<Arc<Chunk>> {
//         let ret = self.chunks.read().unwrap().get(&chunkid).unwrap().clone();

//         Ok(ret)
//     }

//     pub fn remove(&self, chunkid: ChunkID) -> Result<()> {
//         self.chunks.write().unwrap().remove(&chunkid).unwrap();
//         Ok(())
//     }
// }
pub enum ChunkMangerStatus {
    Normal,
    Closed,
}

pub struct ChunkManager {
    chunk_pool: HashMap<ChunkID, Arc<Chunk>>,
    status: ChunkMangerStatus,
}

impl ChunkManager {
    pub fn new() -> Result<Self> {
        Ok(Self {
            chunk_pool: HashMap::<ChunkID, Arc<Chunk>>::new(),
            status: ChunkMangerStatus::Normal,
        })
    }

    pub async fn load_all_chunks(&mut self) -> Result<()> {
        todo!()
    }

    pub async fn close_all_chunks(&mut self) -> Result<()> {
        self.status = ChunkMangerStatus::Closed;
        self.chunk_pool.clear();
        Ok(())
    }

    pub async fn create_chunk(
        &mut self,
        chunk_id: ChunkID,
        replicates: Vec<SocketAddrV4>,
    ) -> Result<()> {
        todo!()
        // // TODO: get rid of io when holding the lock
        // ChunkFile::create_new_chunk_file(chunk_id, replicates).await?;

        // ensure!(Chunk::exists(chunk_id).await, BlockServerError::ChunkExists);

        // METADATA.write().await.new_chunk(chunk_id)?;
        // let chunk = Chunk::load(chunk_id).await?;
        // self.chunk_pool.insert(chunk_id, Arc::new(chunk));

        // Ok(())
    }

    pub async fn remove_chunk(&mut self, chunk_id: ChunkID) -> Result<()> {
        todo!()
        // ensure!(
        //     Chunk::exists(chunk_id).await,
        //     DataServerError::ChunkNotExists
        // );

        // METADATA.write().await.remove_chunk(chunk_id)?;

        // Chunk::delete_file(chunk_id).await.unwrap();
        // if self.chunk_pool.contains_key(&chunk_id) {
        //     self.chunk_pool.remove(&chunk_id);
        // }

        // Ok(())
    }

    pub async fn get_chunk(chunk_id: ChunkID) -> Result<Arc<Chunk>> {
        todo!()
        // if CHUNK_MANAGER
        //     .read()
        //     .unwrap()
        //     .chunk_pool
        //     .contains_key(&chunk_id)
        // {
        //     return Ok(CHUNK_MANAGER
        //         .read()
        //         .unwrap()
        //         .chunk_pool
        //         .get(&chunk_id)
        //         .unwrap()
        //         .to_owned());
        // }

        // ensure!(
        //     METADATA.read().await.check_exists(chunk_id).unwrap(),
        //     DataServerError::ChunkNotExists
        // );
        // ensure!(
        //     Chunk::exists(chunk_id).await,
        //     DataServerError::ChunkNotExists
        // );

        // let chunk = Chunk::load(chunk_id).await?;
        // CHUNK_MANAGER
        //     .write()
        //     .unwrap()
        //     .chunk_pool
        //     .insert(chunk_id, Arc::new(chunk));
        // return Ok(CHUNK_MANAGER
        //     .read()
        //     .unwrap()
        //     .chunk_pool
        //     .get(&chunk_id)
        //     .unwrap()
        //     .clone());
    }
}
