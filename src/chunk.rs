use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::slice;
use std::sync::RwLock;

use anyhow::{bail, Result};
use raft::prelude::*;
use tokio::fs::{remove_file, File, OpenOptions};

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

    pub term: u64,
    pub term_in_chunk_file: u64,

    pub vote_for: u64,

    pub peers: Vec<SocketAddrV4>,

    // pub raft_state: RwLock<RaftState>,
    pub chunk_file: File,
}

#[derive(Default, Debug)]
#[repr(C, packed)]
pub struct ChunkHeader {
    pub chunk_id: u64,
    pub version: u64, // version in file/disk
    pub term: u64,
    pub vote_for: u64,

    pub peers: [u64; 5],
}

const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024; // 64M

impl Chunk {
    pub async fn create(chunk_id: ChunkID, peers: Vec<SocketAddrV4>) -> Result<()> {
        todo!()
        // let chunk_path = chunk_id.chunk_path();
        // let mut chunk_file = OpenOptions::new()
        //     .create(true)
        //     .write(true)
        //     .open(chunk_path)
        //     .await?;

        // chunk_file.truncate(DEFAULT_CHUNK_SIZE).await?;

        // let peers_u64: [u64; 5];
        // for (i, x) in peers.iter().enumerate() {
        //     let ip: u32 = x.ip.into();
        //     let port = x.port;

        //     peers_u64[i] = (ip as u64) << 32 & port as u64;
        // }

        // let header = ChunkHeader {
        //     chunk_id: chunk_id.id,
        //     version: 0,
        //     term: 0,
        //     vote_for: 0,

        //     peers: peers_u64,
        // };

        // chunk_file
        //     .write(unsafe {
        //         slice::from_raw_parts(
        //             &header as *const _ as *const u8,
        //             std::mem::size_of::<ChunkHeader>(),
        //         )
        //     })
        //     .await?;
        // chunk_file.sync_all().await?;

        // Ok(())
    }

    pub async fn remove(chunk_id: ChunkID) -> Result<()> {
        todo!()
        // let chunk_path = chunk_id.chunk_path();

        // remove_file(chunk_path).await?;

        // Ok(())
    }

    pub async fn load(chunk_id: ChunkID) -> Self {
        todo!()
        // let chunk_path = chunk_id.chunk_path();

        // let mut chunk_file = OpenOptions::new()
        //     .write(true)
        //     .read(true)
        //     .open(chunk_path)
        //     .await?;

        // let mut header = ChunkHeader::default();

        // chunk_file
        //     .read(unsafe {
        //         slice::from_raw_parts_mut(
        //             &header as *mut _ as *mut u8,
        //             std::mem::size_of::<ChunkHeader>(),
        //         )
        //     })
        //     .await?;

        // let mut peers = Vec::new();
        // for i in header.peers.iter() {
        //     let ip: Ipv4Addr = ((i >> 32) as u32).into();
        //     let port = i & 0x00000000FFFFFFFF as u16;

        //     peers.push(SocketAddrV4::new(ip, port));
        // }

        // Self {
        //     id: chunk_id,
        //     version: header.version,
        //     version_in_chunk_file: header.version,

        //     term: header.term,
        //     term_in_chunk_file: header.term,

        //     vote_for: header.vote_for,

        //     peers,

        //     chunk_file,
        // }
    }

    pub async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        todo!()
        // let real_offset = offset + std::mem::size_of::<ChunkHeader>;

        // if cfg!(target_os = "linux") {
        //     Ok(self.chunk_file.read_at(real_offset, length).await?)
        // } else {
        //     Ok(vec![])
        // }
    }

    pub async fn write(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        todo!()
        // let real_offset = offset + std::mem::size_of::<ChunkHeader>;

        // if cfg!(target_os = "linux") {
        //     self.chunk_file.write_at(real_offset, data).await?;
        // } else {
        //     Ok(())
        // }
    }

    pub fn write_version(&self, version: u64) -> Result<()> {
        todo!()
    }

    pub fn write_term(&self, term: u64) -> Result<()> {
        todo!()
    }

    pub fn write_vote(&self, vote_for: u64) -> Result<()> {
        todo!()
    }

    pub fn snapshot(&self) {
        todo!()
    }

    pub async fn apply_snapshot(&self, snap: &Snapshot) -> Result<()> {
        todo!()
    }

    pub async fn apply_snapshot_data(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        todo!()
    }

    pub async fn save_config(&self, conf: &ConfState) -> Result<()> {
        todo!()
    }
}
