use std::io::SeekFrom;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::slice;

use anyhow::{anyhow, Result};
use raft::prelude::*;
use tokio::fs::{remove_file, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::DataServerError;

const DEFAULT_CHUNK_SIZE: i64 = 64 * 1024 * 1024; // 64M

pub struct Chunk {
    pub shard_id: u64,

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
    pub version: u64, // version in file/disk
    pub term: u64,
    pub vote_for: u64,

    pub peers: [u64; 5],
}

impl Chunk {
    pub async fn create(chunk_id: ChunkID, peers: Vec<SocketAddrV4>) -> Result<()> {
        let chunk_path = chunk_id.chunk_path();
        let mut chunk_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(chunk_path)
            .await?;

        // unsafe {
        //     libc::fallocate(chunk_file.as_raw_fd(), 0, 0, DEFAULT_CHUNK_SIZE);
        // }

        let mut peers_u64: [u64; 5] = [0; 5];
        for (i, x) in peers.iter().enumerate() {
            let ip: u32 = x.ip().to_owned().into();
            let port = x.port();

            peers_u64[i] = (ip as u64) << 32 & port as u64;
        }

        let header = ChunkHeader {
            chunk_id: chunk_id.id,
            version: 0,
            term: 0,
            vote_for: 0,

            peers: peers_u64,
        };

        chunk_file
            .write(unsafe {
                slice::from_raw_parts(
                    &header as *const _ as *const u8,
                    std::mem::size_of::<ChunkHeader>(),
                )
            })
            .await?;
        chunk_file.sync_all().await?;

        Ok(())
    }

    pub async fn remove(chunk_id: ChunkID) -> Result<()> {
        let chunk_path = chunk_id.chunk_path();

        remove_file(chunk_path).await?;

        Ok(())
    }

    pub async fn load(chunk_id: ChunkID) -> Result<Self> {
        let chunk_path = chunk_id.chunk_path();

        let mut chunk_file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(chunk_path)
            .await?;

        let mut header = ChunkHeader::default();

        chunk_file
            .read(unsafe {
                slice::from_raw_parts_mut(
                    &mut header as *mut _ as *mut u8,
                    std::mem::size_of::<ChunkHeader>(),
                )
            })
            .await?;

        let mut peers = Vec::new();
        for i in header.peers.iter() {
            let ip: Ipv4Addr = ((i >> 32) as u32).into();
            let port = (i & 0x00000000FFFFFFFF) as u16;

            peers.push(SocketAddrV4::new(ip, port));
        }

        Ok(Self {
            id: chunk_id,
            version: header.version,
            version_in_chunk_file: header.version,

            term: header.term,
            term_in_chunk_file: header.term,

            vote_for: header.vote_for,

            peers,

            chunk_file,
        })
    }
}

impl Chunk {
    pub async fn read(&mut self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let real_offset = offset + std::mem::size_of::<ChunkHeader> as u64;

        if cfg!(target_os = "linux") {
            self.chunk_file
                .seek(SeekFrom::Start(real_offset))
                .await
                .map_err(|_| anyhow!(DataServerError::FailedToSeek))?;

            let mut buf = Vec::new();
            buf.reserve_exact(length as usize);
            unsafe { buf.set_len(length as usize) }
            self.chunk_file.read(&mut buf).await?;
            Ok(buf)
        } else {
            Ok(vec![])
        }
    }

    pub async fn write(&mut self, offset: u64, data: Vec<u8>) -> Result<()> {
        let real_offset = offset + std::mem::size_of::<ChunkHeader> as u64;

        if cfg!(target_os = "linux") {
            self.chunk_file
                .seek(SeekFrom::Start(real_offset))
                .await
                .map_err(|_| anyhow!(DataServerError::FailedToSeek))?;

            self.chunk_file.write(&data[..]).await?;
        }
        Ok(())
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

    pub async fn save_config(&self, conf: &ConfState) -> Result<()> {
        todo!()
    }
}

impl Chunk {
    pub fn snapshot(&self) {
        todo!()
    }

    pub async fn apply_snapshot(&self, snap: &Snapshot) -> Result<()> {
        todo!()
    }

    pub async fn apply_snapshot_data(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        todo!()
    }
}
