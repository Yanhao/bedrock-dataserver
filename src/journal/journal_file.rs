use std::io::SeekFrom;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::prelude::*;
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use regex::Regex;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::chunk::ChunkID;
use crate::chunk_manager::ChunkManager;
use crate::error::DataServerError;

const JOURNAL_MAGIC: u64 = 0x6565;
const JOURNAL_FILE_DEFAULT_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const JOURNAL_DEFAULT_MAX_RECORD_COUNT: u32 = 1024 * 64;
const JOURNAL_HEADER_RESERVED_SIZE: u64 = 4096;
const JOURNAL_DEFAULT_EXPAND_SIZE: u64 = 512 * 1024 * 1024;

#[derive(Copy, Clone)]
#[repr(C, packed)]
pub struct JournalEntryMeta {
    pub chunk_id: u64,

    pub offset_in_chunk: u64,
    pub offset_in_journal: u64,
    pub length: u64,

    pub time: u64,

    pub version: u64,
    pub term: u64,
    pub commit: u8,
}

#[repr(C, packed)]
pub struct JournalHeader {
    magic: u64,
    format: u8,

    pub file_size: u64,

    max_entry_count: u32,
    meta_offset: u64,
    data_offset: u64,

    pub checkpoint_index: u32,
    pub committed_index: u32,

    next_entry_index: u32,
    next_data_offset: u64,
}

pub struct JournalFile {
    pub header: Box<JournalHeader>,
    entry_metas: Vec<JournalEntryMeta>,

    pub base_offset: u64,
    first_usable_index: u32, // the fist unused meta since we last restart

    pub file_lock: Mutex<()>,
    pub file: File,
    pub path: PathBuf,

    lock: Arc<Mutex<()>>,
}

impl JournalFile {
    pub async fn fsync(&self) {}

    pub async fn read_at(&mut self, buf: &mut [u8], pos: u64) -> Result<()> {
        let _lg = self.file_lock.lock().await;
        self.file
            .seek(SeekFrom::Start(pos))
            .await
            .map_err(|_| anyhow!(DataServerError::FailedToSeek))?;
        self.file
            .read(buf)
            .await
            .map_err(|_| anyhow!(DataServerError::FailedToRead))?;

        Ok(())
    }
    pub async fn write_at(&mut self, data: &[u8], pos: u64) -> Result<()> {
        let _lg = self.file_lock.lock().await;
        self.file
            .seek(SeekFrom::Start(pos))
            .await
            .map_err(|_| anyhow!(DataServerError::FailedToSeek))?;
        self.file
            .write(data)
            .await
            .map_err(|_| anyhow!(DataServerError::FailedToWrite))?;

        Ok(())
    }

    pub fn allocate_new_entry(
        &mut self,
        chunk_id: ChunkID,
        offset: u64,
        size: u64,
        version: u64,
        term: u64,
    ) -> Result<(
        u32, /* record_meta_index */
        u64, /* data_offset_in_journal_file */
    )> {
        if self.header.next_entry_index >= JOURNAL_DEFAULT_MAX_RECORD_COUNT {
            bail!(DataServerError::JournalFileFull);
        }

        let allocate_index = self.header.next_entry_index;
        let entry_meta = &mut self.entry_metas[allocate_index as usize];

        entry_meta.chunk_id = chunk_id.id;
        entry_meta.version = version;
        entry_meta.term = term;
        entry_meta.time = 0;
        entry_meta.offset_in_chunk = offset;
        entry_meta.length = size;
        entry_meta.offset_in_journal = self.header.next_data_offset;

        if self.header.file_size <= entry_meta.offset_in_journal + size {
            unsafe {
                // libc::fallocate(
                //     self.file.as_raw_fd(),
                //     0,
                //     self.header.file_size as i64,
                //     JOURNAL_DEFAULT_EXPAND_SIZE as i64,
                // );
            }

            self.header.file_size += JOURNAL_DEFAULT_EXPAND_SIZE;
        }

        self.header.next_entry_index += 1;
        self.header.next_data_offset += size;

        Ok((allocate_index, entry_meta.offset_in_journal))
    }

    pub fn get_entry_meta(&self, meta_index: u32) -> Result<JournalEntryMeta> {
        Ok(self.entry_metas[meta_index as usize])
    }

    pub async fn write_header(&mut self) -> Result<()> {
        self.write_at(
            unsafe {
                slice::from_raw_parts(
                    &self.header as *const _ as *const u8,
                    std::mem::size_of::<JournalHeader>(),
                )
            },
            0,
        )
        .await?;

        Ok(())
    }

    async fn write_entry_meta(&mut self, index: u32) -> Result<()> {
        let offset =
            self.header.meta_offset + index as u64 * std::mem::size_of::<JournalEntryMeta>() as u64;
        let data = unsafe {
            slice::from_raw_parts(
                &self.entry_metas[index as usize] as *const _ as *const u8,
                std::mem::size_of::<JournalEntryMeta>(),
            )
        };

        self.write_at(data, offset).await?;
        Ok(())
    }

    pub async fn commit_entry(&mut self, index: u32) -> Result<()> {
        self.entry_metas[index as usize].commit = 1;

        self.write_entry_meta(index).await
    }

    pub async fn replay(&mut self) -> Result<()> {
        todo!()
    }
    // pub async fn replay(&mut self) -> Result<()> {
    //     let cp /* checkpoint */ = self.header.checkpoint_index;

    //     if cp == std::u32::MAX {
    //         bail!(DataServerError::JournalUnUsed);
    //     }

    //     if cp >= JOURNAL_DEFAULT_MAX_RECORD_COUNT {
    //         // TODO: should we just delete this journal file?
    //         bail!(DataServerError::JournalFileAllFlushed);
    //     }

    //     let mut new_next_index = self.header.next_entry_index;
    //     let mut new_next_data_offset = self.header.next_data_offset;
    //     let mut new_committed_index = self.header.committed_index;

    //     for i in cp..JOURNAL_DEFAULT_MAX_RECORD_COUNT {
    //         let meta = self.entry_metas[i as usize];
    //         if meta.commit != 1 {
    //             continue;
    //         }
    //         new_committed_index = i;

    //         ChunkManager::get_chunk(ChunkID { id: meta.chunk_id })
    //             .await?
    //             .journal_index
    //             .write()
    //             .await
    //             .insert(
    //                 meta.offset_in_chunk,
    //                 self.base_offset + meta.offset_in_journal,
    //                 meta.length,
    //                 meta.version,
    //                 meta.term,
    //             );
    //         new_next_index = i + 1;
    //         new_next_data_offset = meta.offset_in_journal + meta.length;
    //     }

    //     self.header.next_entry_index = new_next_index;
    //     self.header.next_data_offset = new_next_data_offset;
    //     self.header.committed_index = new_committed_index;
    //     self.first_usable_index = new_next_index;

    //     Ok(())
    // }

    pub fn is_need_flush(&self) -> bool {
        self.header.committed_index > self.header.checkpoint_index
    }

    pub fn is_full_and_all_flushed(&self) -> bool {
        self.header.next_entry_index >= JOURNAL_DEFAULT_MAX_RECORD_COUNT && !self.is_need_flush()
    }

    pub fn parse_base_offset(path: impl AsRef<Path>) -> Result<u64> {
        let re = Regex::new(r"").unwrap();
        let caps = re.captures(path.as_ref().to_str().unwrap()).unwrap();
        let offset_str = caps.get(1).unwrap().as_str();

        Ok(offset_str.parse::<u64>().unwrap())
    }

    pub async fn load_journal_file(path: impl AsRef<Path>) -> Result<JournalFile> {
        let mut file = match OpenOptions::new()
            .create_new(false)
            .write(true)
            .read(true)
            .open(path.as_ref())
            .await
        {
            Err(e) => {
                eprintln!("failed to open journal file {:?}", path.as_ref());
                bail!(DataServerError::FailedToOpen);
            }
            Ok(v) => v,
        };

        let mut header: JournalHeader = unsafe { std::mem::MaybeUninit::uninit().assume_init() };

        let data = unsafe {
            slice::from_raw_parts_mut(
                &mut header as *mut _ as *mut u8,
                std::mem::size_of::<JournalHeader>(),
            )
        };
        if let Err(e) = file.read(data).await {
            eprintln!("failed to read header from journal");
            bail!(DataServerError::FailedToRead);
        }

        let mut metas = Vec::<JournalEntryMeta>::new();

        for i in 0..JOURNAL_DEFAULT_MAX_RECORD_COUNT {
            let offset: u64 =
                header.meta_offset + i as u64 * std::mem::size_of::<JournalEntryMeta>() as u64;

            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|_| anyhow!(DataServerError::FailedToSeek))?;

            let data = unsafe {
                slice::from_raw_parts_mut(
                    &mut metas[i as usize] as *mut _ as *mut u8,
                    std::mem::size_of::<JournalEntryMeta>(),
                )
            };

            if let Err(e) = file.read(data).await {
                eprintln!("failed to read jorunal record metas");
                bail!(DataServerError::FailedToRead);
            }
        }

        let base_offset = Self::parse_base_offset(path.as_ref()).unwrap();

        Ok(Self {
            header: Box::new(header),
            entry_metas: metas,

            base_offset,
            first_usable_index: 0,

            file_lock: Mutex::new(()),
            file,
            path: path.as_ref().to_path_buf(),
            lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn create_journal_with_offset(
        journal_dir: impl AsRef<Path>,
        base_offset: u64,
    ) -> Result<()> {
        let journal_path = journal_dir.as_ref().join(format!("{:x}", base_offset));
        if journal_path.exists() {
            eprintln!("the journal patch already exists");
            bail!(DataServerError::JournalExists);
        }

        let mut file = match OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(journal_path)
            .await
        {
            Err(e) => {
                bail!(DataServerError::FailedToCreate);
            }
            Ok(v) => v,
        };

        let metas_size =
            std::mem::size_of::<JournalEntryMeta>() as u32 * JOURNAL_DEFAULT_MAX_RECORD_COUNT;
        let file_size =
            JOURNAL_HEADER_RESERVED_SIZE + metas_size as u64 + JOURNAL_FILE_DEFAULT_SIZE;

        unsafe {
            // libc::fallocate(file.as_raw_fd(), 0, 0, file_size as i64);
        }

        let journal_header = JournalHeader {
            magic: JOURNAL_MAGIC,
            format: 0x1,

            file_size,

            max_entry_count: JOURNAL_DEFAULT_MAX_RECORD_COUNT,
            meta_offset: JOURNAL_HEADER_RESERVED_SIZE,
            data_offset: JOURNAL_HEADER_RESERVED_SIZE + metas_size as u64,

            checkpoint_index: 0,
            committed_index: std::u32::MAX,

            next_entry_index: 0,
            next_data_offset: JOURNAL_HEADER_RESERVED_SIZE + metas_size as u64,
        };

        let journal_header_s = unsafe {
            slice::from_raw_parts(
                &journal_header as *const _ as *const u8,
                std::mem::size_of::<JournalHeader>(),
            )
        };

        if let Err(e) = file.write(journal_header_s).await {
            eprintln!("failed to write journal header");
            bail!(DataServerError::FailedToWrite);
        }

        // zero record meta zone
        unsafe {
            // libc::fallocate(
            //     file.as_raw_fd(),
            //     0,
            //     JOURNAL_HEADER_RESERVED_SIZE as i64,
            //     metas_size as i64,
            // );
        }

        file.sync_all();

        Ok(())
    }

    // TODO: may be we don't need this because the journal file is wrap by Arc (shared ptr)
    pub async fn delete_lock(&self) -> OwnedMutexGuard<()> {
        self.lock.clone().lock_owned().await
    }
}
