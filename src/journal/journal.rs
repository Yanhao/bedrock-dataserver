use std::collections::LinkedList;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use lazy_static::lazy_static;
use regex::Regex;
use tokio::fs::remove_file;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tokio::task::JoinHandle;

use crate::chunk::ChunkID;
use crate::chunk_manager::ChunkManager;
use crate::error::DataServerError;
use crate::journal_file::JournalFile;

lazy_static! {
    pub static ref G_JOURNAL: RwLock<Journal> =
        unsafe { std::mem::MaybeUninit::uninit().assume_init() };
}

pub async fn init_journal(journal_dir: impl AsRef<Path>) -> Result<()> {
    *G_JOURNAL.write().await = Journal::load_journal(journal_dir).await.unwrap();
    Ok(())
}

pub struct Journal {
    stop_flush_back_flag: AtomicBool,
    flush_back_all_flag: AtomicBool,
    journal_directory: PathBuf,

    journal_files: RwLock<LinkedList<Arc<RwLock<JournalFile>>>>,

    flush_thread: Mutex<JoinHandle<()>>,

    inflight_io: AtomicU32,
}

impl Journal {
    fn invalidate_journal_path(paths: Vec<String>) -> Vec<String> {
        let re = Regex::new(r"").unwrap();
        let mut ret_paths = Vec::<String>::new();

        for i in paths {
            if re.is_match(&i) {
                ret_paths.push(i.clone());
            }
        }

        ret_paths
    }

    pub async fn load_journal(journal_dir: impl AsRef<Path>) -> Result<Journal> {
        if !journal_dir.as_ref().is_dir() {
            bail!(DataServerError::NoJournalDir);
        }

        let mut journal_paths = Vec::<String>::new();
        let journal_files = RwLock::new(LinkedList::<Arc<RwLock<JournalFile>>>::new());

        let js = match fs::read_dir(journal_dir.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                bail!(DataServerError::OpenDirFailed);
            }
        };

        for e in js {
            let e = e.unwrap();
            let path = e.path();
            journal_paths.push(path.into_os_string().into_string().unwrap());
        }

        journal_paths = Journal::invalidate_journal_path(journal_paths);
        if journal_paths.is_empty() {
            bail!(DataServerError::InvalidJournalDir);
        }

        for i in journal_paths {
            journal_files.write().await.push_back(Arc::new(RwLock::new(
                JournalFile::load_journal_file(i).await?,
            )));
        }

        Ok(Journal {
            stop_flush_back_flag: AtomicBool::new(false),
            flush_back_all_flag: AtomicBool::new(false),

            journal_directory: journal_dir.as_ref().to_path_buf(),
            journal_files,
            flush_thread: Mutex::new(tokio::spawn(async {})),

            inflight_io: AtomicU32::new(0),
        })
    }

    async fn find_journal_file_by_offset(
        &self,
        logic_offset: u64,
    ) -> Result<(Arc<RwLock<JournalFile>>, u64, OwnedMutexGuard<()>)> {
        let j_files = self.journal_files.read().await;

        if j_files.front().unwrap().read().await.base_offset < logic_offset {
            bail!(DataServerError::InvalidOffset);
        }

        if j_files.back().unwrap().read().await.base_offset
            + j_files.back().unwrap().read().await.header.file_size
            > logic_offset
        {
            bail!(DataServerError::InvalidOffset);
        }

        for i in j_files.iter() {
            if i.read().await.base_offset + i.read().await.header.file_size > logic_offset {
                return Ok((
                    i.clone(),
                    i.read().await.base_offset + i.read().await.header.file_size - logic_offset,
                    i.clone().read().await.delete_lock().await,
                ));
            }
        }

        bail!(DataServerError::Unknown);
    }

    pub async fn read_at(&mut self, chunk_id: ChunkID, buf: &mut [u8], offset: u64) -> Result<()> {
        todo!()
    }
    // pub async fn read_at(&mut self, chunk_id: ChunkID, buf: &mut [u8], offset: u64) -> Result<()> {
    //     let segs = ChunkManager::get_chunk(chunk_id)
    //         .await
    //         .unwrap()
    //         .journal_index
    //         .read()
    //         .await
    //         .search(offset, buf.len() as u64)?;

    //     let total_size = buf.len() as u64;
    //     let mut need_read_size = buf.len() as u64;

    //     let mut seg_index: usize = 0;
    //     while need_read_size > 0 {
    //         let read_len: u64;

    //         if segs[seg_index].offset <= offset + (offset + buf.len() as u64) - need_read_size {
    //             // read from journal

    //             read_len = segs[seg_index].end_offset_in_chunk()
    //                 - (offset + (offset + buf.len() as u64) - need_read_size);
    //             let read_offset = segs[seg_index].end_offset_in_journal() - read_len;

    //             // FIXME: the journal file maybe already deleted here
    //             let (j_file, offset_in_journal, jf_lock_handle) =
    //                 self.find_journal_file_by_offset(read_offset).await?;

    //             j_file
    //                 .write()
    //                 .await
    //                 .read_at(
    //                     &mut buf[(total_size - need_read_size) as usize..],
    //                     read_offset,
    //                 )
    //                 .await?;

    //             seg_index += 1;
    //         } else {
    //             // read from chunk file

    //             read_len = segs[seg_index].offset
    //                 - (offset + (offset + buf.len() as u64) - need_read_size);
    //             let read_offset = offset + (offset + buf.len() as u64) - need_read_size;

    //             ChunkManager::get_chunk(chunk_id)
    //                 .await
    //                 .unwrap()
    //                 .chunk_file
    //                 .write()
    //                 .await
    //                 .read_at(
    //                     &mut buf[(total_size - need_read_size) as usize..],
    //                     read_offset,
    //                 )
    //                 .await?;
    //         }

    //         need_read_size -= read_len;
    //     }

    //     Ok(())
    // }

    async fn try_allocate_new_journal_entry(
        &mut self,
        chunk_id: ChunkID,
        offset: u64,
        data_len: u64,
        version: u64,
        term: u64,
    ) -> Result<(
        u32, /* journal record meta */
        u64, /* data offset in journal */
    )> {
        let mut j_files = self.journal_files.write().await;

        if j_files.len() == 0 {
            // create new journal file
            JournalFile::create_journal_with_offset(&self.journal_directory, 0).await?;
            let mut j_file =
                JournalFile::load_journal_file(format!("{:?}/0.journal", self.journal_directory))
                    .await?;
            let ret = j_file
                .allocate_new_entry(chunk_id, offset, data_len, version, term)
                .unwrap();
            j_files.push_back(Arc::new(RwLock::new(j_file)));

            return Ok(ret);
        }

        let r = j_files
            .back()
            .unwrap()
            .write()
            .await
            .allocate_new_entry(chunk_id, offset, data_len, version, term);

        return match r {
            Ok(v) => Ok(v),

            Err(e) => match e.downcast_ref::<DataServerError>() {
                Some(DataServerError::JournalFileFull) => {
                    let last_j_file = j_files.back().unwrap();
                    let new_base_offset = last_j_file.read().await.base_offset
                        + last_j_file.read().await.header.as_ref().file_size;

                    JournalFile::create_journal_with_offset(&self.journal_directory, 0).await?;
                    let mut j_file = JournalFile::load_journal_file(format!(
                        "{:?}/0.journal",
                        self.journal_directory
                    ))
                    .await?;
                    let ret = j_file
                        .allocate_new_entry(chunk_id, offset, data_len, version, term)
                        .unwrap();
                    j_files.push_back(Arc::new(RwLock::new(j_file)));
                    Ok(ret)
                }
                _ => Err(anyhow!(DataServerError::Unknown)),
            },
        };
    }

    pub async fn write_at(
        &mut self,
        chunk_id: ChunkID,
        data: &[u8],
        offset: u64,
        version: u64,
        term: u64,
    ) -> Result<()> {
        todo!()
    }
    // pub async fn write_at(
    //     &mut self,
    //     chunk_id: ChunkID,
    //     data: &[u8],
    //     offset: u64,
    //     version: u64,
    //     term: u64,
    // ) -> Result<()> {
    //     self.inflight_io.fetch_add(1, Ordering::Relaxed);
    //     // TODO: defer

    //     ChunkManager::get_chunk(chunk_id)
    //         .await?
    //         .write_wait_queue
    //         .write()
    //         .await
    //         .wait(version)
    //         .await?;

    //     let (meta_index, offset_in_journal) = self
    //         .try_allocate_new_journal_entry(chunk_id, offset, data.len() as u64, version, term)
    //         .await?;

    //     let mut j_list = self.journal_files.write().await;
    //     let j_file = j_list.back_mut().unwrap();
    //     j_file
    //         .write()
    //         .await
    //         .write_at(data, offset_in_journal)
    //         .await?;
    //     j_file.write().await.fsync().await;

    //     let logic_offset = j_file.read().await.base_offset + offset_in_journal;
    //     ChunkManager::get_chunk(chunk_id)
    //         .await?
    //         .journal_index
    //         .write()
    //         .await
    //         .insert(offset, logic_offset, data.len() as u64, version, term);

    //     ChunkManager::get_chunk(chunk_id)
    //         .await?
    //         .write_wait_queue
    //         .write()
    //         .await
    //         .wake_next(version + 1)
    //         .await?;

    //     j_file.write().await.commit_entry(meta_index).await?;

    //     Ok(())
    // }

    pub async fn stop_flush_back(&mut self) {
        self.stop_flush_back_flag.store(true, Ordering::Relaxed);
        self.flush_thread.lock().await;
    }

    pub async fn flush_one_journal_entry(
        &self,
        j_file: Arc<RwLock<JournalFile>>,
        entry_index: u32,
    ) -> Result<()> {
        todo!()
    }
    // pub async fn flush_one_journal_entry(
    //     &self,
    //     j_file: Arc<RwLock<JournalFile>>,
    //     entry_index: u32,
    // ) -> Result<()> {
    //     let entry = j_file.read().await.get_entry_meta(entry_index).unwrap();
    //     let segs = ChunkManager::get_chunk(ChunkID { id: entry.chunk_id })
    //         .await?
    //         .journal_index
    //         .read()
    //         .await
    //         .search(entry.offset_in_journal, entry.length)
    //         .unwrap();

    //     for i in segs.iter() {
    //         let mut buf = vec![0u8; i.length as usize];
    //         j_file.write().await.read_at(&mut buf, i.offset);

    //         let chunk = ChunkManager::get_chunk(ChunkID { id: entry.chunk_id }).await?;

    //         chunk.chunk_file.write().await.write_at(&buf, 0).await?;

    //         let mut jf = j_file.write().await;
    //         jf.header.checkpoint_index += 1;
    //         jf.write_header().await?;
    //     }

    //     ChunkManager::get_chunk(ChunkID { id: entry.chunk_id })
    //         .await?
    //         .journal_index
    //         .write()
    //         .await
    //         .clear(entry.offset_in_journal, entry.length, entry.version);
    //     Ok(())
    // }

    pub async fn flush_one_journal_file(&self, j_file: Arc<RwLock<JournalFile>>) {
        if !j_file.read().await.is_need_flush() {
            return;
        }

        for i in
            j_file.read().await.header.checkpoint_index..j_file.read().await.header.committed_index
        {
            self.flush_one_journal_entry(j_file.clone(), i);
        }
    }

    pub async fn flush_all(&self) {
        loop {
            let mut cur: Option<Arc<RwLock<JournalFile>>> = None;

            let j_files = self.journal_files.read().await;
            for i in j_files.iter() {
                let f = i.read().await;
                if f.is_need_flush() {
                    cur = Some(i.clone());
                    break;
                }
            }
            std::mem::drop(j_files);

            if cur.is_none() {
                return;
            }

            let cur = cur.unwrap();
            self.flush_one_journal_file(cur.clone()).await;
            if cur.read().await.is_full_and_all_flushed() {
                self.journal_files.write().await.pop_front().unwrap();

                let path = cur.read().await.path.clone();
                std::mem::drop(cur);

                // remove file
                remove_file(path).await.unwrap();
            }
        }
    }

    pub fn start_flush_back(&mut self) -> Result<()> {
        self.flush_thread = Mutex::new(tokio::spawn(async {
            while !G_JOURNAL
                .read()
                .await
                .stop_flush_back_flag
                .load(Ordering::Relaxed)
            {
                G_JOURNAL.write().await.flush_all().await;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }));

        Ok(())
    }
}
