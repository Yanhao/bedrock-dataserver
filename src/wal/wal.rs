//! WAL (Write-Ahead Logging) main module
//! Responsible for core logic such as log append, recovery, compaction, and group commit.
//! Supports sharding, file segmentation, group commit, etc., ensuring high performance and data consistency.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, ensure, Result};
use thiserror::Error;
use tokio::fs::{self, create_dir_all, remove_dir_all};
use tracing::{debug, error, info};

use idl_gen::replog_pb::Entry;

use super::group_commiter::GroupCommitter;
use super::wal_file;
use crate::config::CONFIG;
use crate::shard::ShardError;

const MAX_ENTRY_COUNT: u64 = 10;

#[derive(Error, Debug, Clone)]
pub enum WalError {
    #[error("file already exists")]
    FileExists,

    #[error("failed to open file")]
    FailedToOpen,
    #[error("failed to read file")]
    FailedToRead,
    #[error("failed to write file")]
    FailedToWrite,
    #[error("failed to seek file")]
    FailedToSeek,
    #[error("failed to create new file")]
    FailedToCreateFile,

    #[error("invalid parameter")]
    InvalidParameter,

    #[error("empty wal files")]
    EmptyWalFiles,
    #[error("wal file full")]
    WalFileFull,

    #[error("to many entries")]
    TooManyEntries,
}

/// Internal WAL structure, manages all WAL files and directory for a shard
struct WalInner {
    shard_id: u64,
    wal_files: Vec<wal_file::WalFile>,
    dir: PathBuf,
}

impl WalInner {
    /// Get WAL file name suffix (sequence number)
    fn wal_file_suffix(path: impl AsRef<Path>) -> u64 {
        let path = path.as_ref().as_os_str().to_str().expect("Invalid path string");
        let suffix_str = path.split('.').last().expect("No suffix found in WAL file name");
        suffix_str.parse().expect("Failed to parse WAL file suffix as u64")
    }

    /// Generate WAL directory path for a shard
    fn wal_dir_path(shard_id: u64) -> PathBuf {
        let wal_dir: PathBuf = CONFIG.read().wal_dir.as_ref().unwrap().into();

        wal_dir
            .join::<String>(format!(
                "{:08x}",
                ((shard_id & 0xFFFFFFFF_00000000) >> 32) as u32
            ))
            .join::<String>(format!("{:08x}", (shard_id & 0x00000000_FFFFFFFF) as u32))
    }

    /// Load all WAL files in the specified directory
    async fn load(dir: impl AsRef<Path>, shard_id: u64) -> Result<WalInner> {
        ensure!(dir.as_ref().exists(), "path not exists");
        ensure!(dir.as_ref().is_dir(), "path is not directory");

        info!("start load wal at: {:?} ...", dir.as_ref());

        let mut wal_file_paths = dir
            .as_ref()
            .read_dir()?
            .map(|ent| ent.map(|ent| ent.path()))
            .collect::<Result<Vec<_>, _>>()?;

        if wal_file_paths.is_empty() {
            return Ok(WalInner {
                shard_id,
                wal_files: vec![],
                dir: dir.as_ref().to_owned(),
            });
        }

        wal_file_paths.sort_by(|a: &PathBuf, b: &PathBuf| {
            let a_suffix = Self::wal_file_suffix(a.as_path());
            let b_suffix = Self::wal_file_suffix(b.as_path());
            u64::cmp(&a_suffix, &b_suffix)
        });

        debug!("wal_file_path length: {}", wal_file_paths.len());

        let mut wal_files = vec![];
        for (i, p) in wal_file_paths.iter().enumerate() {
            wal_files.push(
                wal_file::WalFile::load_wal_file(p.as_path(), i != wal_file_paths.len() - 1)
                    .await?,
            );
        }

        Ok(WalInner {
            shard_id,
            wal_files,
            dir: dir.as_ref().to_owned(),
        })
    }

    /// Create WAL directory for a shard
    pub async fn create(shard_id: u64) -> Result<()> {
        info!("create wal directory for 0x{:016x}", shard_id);
        let wal_manager_dir = Self::wal_dir_path(shard_id);
        create_dir_all(wal_manager_dir).await?;

        Ok(())
    }

    /// Load WAL by shard id
    pub async fn load_wal_by_shard_id(shard_id: u64) -> Result<WalInner> {
        let wal_manager_dir = Self::wal_dir_path(shard_id);
        WalInner::load(wal_manager_dir, shard_id).await
    }

    /// Generate new WAL file path
    fn generate_new_wal_file_path(&self) -> PathBuf {
        if self.wal_files.is_empty() {
            return self.dir.clone().join(String::from_str("wal.0").expect("Failed to create wal.0"));
        }

        let suffix = self.wal_files.last().map(|f| f.suffix()).unwrap_or(0);
        self.dir.clone().join(format!("wal.{}", suffix + 1))
    }

    /// Remove WAL directory for a shard
    pub async fn remove_wal(shard_id: u64) -> Result<()> {
        remove_dir_all(Self::wal_dir_path(shard_id)).await?;
        Ok(())
    }

    /// Discard WAL after next_index
    async fn discard(&mut self, next_index: u64) -> Result<()> {
        if self.wal_files.is_empty() {
            return Ok(());
        }

        while self.wal_files.last().map(|f| f.next_version()).unwrap_or(0) > next_index {
            if self.wal_files.last().map(|f| f.first_version()).unwrap_or(0) > next_index {
                if let Some(wal_file) = self.wal_files.pop() {
                    if let Err(e) = std::fs::remove_file(&wal_file.path) {
                        error!("remove wal file failed: {:?}", e);
                    }
                }
                continue;
            }
            if let Some(last) = self.wal_files.last_mut() {
                if let Err(e) = last.discard(next_index).await {
                    error!("discard wal file failed: {:?}", e);
                }
            }
        }

        Ok(())
    }
}

impl WalInner {
    /// Get log entries in range [lo, hi]
    async fn entries(
        &mut self,
        mut lo: u64,
        hi: u64, /* lo..=hi */
        _max_size: u64,
    ) -> Result<Vec<Entry>> {
        info!(
            "wal entries: lo: {lo}, hi: {hi}, shard_id: {}",
            self.shard_id
        );

        if hi - lo >= MAX_ENTRY_COUNT {
            bail!(WalError::TooManyEntries);
        }

        if self.wal_files.is_empty() {
            bail!(WalError::EmptyWalFiles);
        }

        if self.wal_files.len() == 1 && self.wal_files.first().map(|f| f.entry_len()).unwrap_or(0) <= 1 {
            bail!(WalError::InvalidParameter);
        }

        if lo < self.first_index() {
            bail!(WalError::InvalidParameter);
        }

        if hi >= self.next_index() {
            bail!(WalError::InvalidParameter);
        }

        let mut ret = vec![];
        for file in self.wal_files.iter_mut() {
            info!(
                "wal files: first_version: {}, next_version: {}",
                file.first_version(),
                file.next_version()
            );
            if file.next_version() <= lo {
                continue;
            }

            if lo > hi {
                break;
            }

            if hi < file.first_version() {
                break;
            }

            info!(
                "call wal_file entries, lo: {}, hi: {}",
                lo,
                if hi < file.next_version() {
                    hi
                } else {
                    file.next_version() - 1
                },
            );

            let mut ents = file
                .entries(
                    lo,
                    if hi < file.next_version() {
                        hi
                    } else {
                        file.next_version() - 1
                    },
                )
                .await?;
            ret.extend(ents);

            lo = if hi < file.next_version() {
                hi + 1
            } else {
                file.next_version()
            };
        }

        Ok(ret)
    }

    /// Get the index of the first log entry
    fn first_index(&self) -> u64 {
        if self.wal_files.is_empty() {
            return 0;
        }

        self.wal_files.first().unwrap().first_version()
    }

    /// Get the index of the next available log entry
    fn next_index(&self) -> u64 {
        if self.wal_files.is_empty() {
            return 0;
        }

        self.wal_files.last().unwrap().next_version()
    }

    /// Append log entries, supports discard
    async fn append(&mut self, ents: Vec<Entry>, discard: bool) -> Result<u64> {
        // TODO: use group commit to improve performance
        if discard {
            if let Some(first) = ents.first() {
                self.discard(first.index).await?;
            } else {
                bail!(WalError::InvalidParameter);
            }
        }

        let mut iter = ents.into_iter().peekable();
        loop {
            let ent = match iter.peek() {
                Some(e) => e,
                None => break,
            };

            let need_new_file = self.wal_files.is_empty() || self.wal_files.last().map(|f| f.is_sealed()).unwrap_or(true);
            if need_new_file {
                let path = self.generate_new_wal_file_path();
                let new_wal_file = wal_file::WalFile::create_new_wal_file(path, self.next_index())
                    .await?;
                self.wal_files.push(new_wal_file);
            }

            debug!("append wal entry, index: {}", ent.index);

            let last_mut = self.wal_files.last_mut().expect("WAL files should not be empty after creation");
            if let Err(e) = last_mut.append_entry(ent.clone()).await {
                if let Some(WalError::WalFileFull) = e.downcast_ref() {
                    self.wal_files.last_mut().expect("WAL files should not be empty").seal().await.expect("Seal should not fail");
                    continue;
                }
                return Err(e);
            }

            iter.next();
        }

        Ok(self.next_index() - 1)
    }

    /// Log compaction, clears all logs before compact_index
    async fn compact(&mut self, compact_index: u64) -> Result<()> {
        let next_index = self.next_index();
        info!("compact log to {compact_index}, next_index: {next_index}");
        if next_index == compact_index {
            return Ok(());
        }

        if next_index < compact_index {
            self.wal_files.clear();
            remove_files_in_dir(self.dir.clone()).await?;

            let path = self.generate_new_wal_file_path();
            let new_wal_file = wal_file::WalFile::create_new_wal_file(path, compact_index)
                .await
                .inspect_err(|e| error!("create new wal file failed, err: {e}"))?;

            self.wal_files.push(new_wal_file);

            return Ok(());
        }

        if next_index > compact_index {
            if self.first_index() >= compact_index {
                return Ok(());
            }

            while self.wal_files.len() > 1 {
                if self.wal_files.first().unwrap().next_version() <= compact_index {
                    let path = self.wal_files.first().unwrap().path.clone();
                    self.wal_files.remove(0);

                    if let Err(e) = tokio::fs::remove_file(&path).await {
                        error!("remove wal file failed: {:?}", e);
                    }
                    info!("remove wal file {:?}", &path);
                }
            }

            return Ok(());
        }

        Ok(())
    }
}

/// Remove all files in a directory
async fn remove_files_in_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    let mut dir = fs::read_dir(path).await?;
    while let Some(entry) = dir.next_entry().await? {
        if entry.file_type().await?.is_file() {
            fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

/// Main WAL struct, thread-safe, supports group commit
pub struct Wal {
    inner: Arc<tokio::sync::RwLock<WalInner>>,
    merger: GroupCommitter<
        Entry,
        std::result::Result<MergerAppendRet, WalError>,
        Arc<tokio::sync::RwLock<WalInner>>,
    >,
}

impl Wal {
    /// Remove WAL for a shard
    pub async fn remove_wal(shard_id: u64) -> Result<()> {
        WalInner::remove_wal(shard_id).await
    }

    /// Load WAL for a shard
    pub async fn load_wal_by_shard_id(shard_id: u64) -> Result<Wal> {
        let inner = WalInner::load_wal_by_shard_id(shard_id).await?;

        let wal_inner = Arc::new(tokio::sync::RwLock::new(inner));
        let mut wal = Wal {
            inner: wal_inner.clone(),
            merger: GroupCommitter::builder()
                .extra(Some(wal_inner.clone()))
                .build(),
        };

        wal.merger.start(
            async move |wal_inner: Option<Arc<tokio::sync::RwLock<WalInner>>>,
                        ents: Vec<Entry>|
                        -> std::result::Result<MergerAppendRet, WalError> {
                let index = wal_inner
                    .unwrap()
                    .write()
                    .await
                    .append(ents.clone(), false)
                    .await
                    .map_err(|_e| WalError::FailedToWrite)?;

                let (tx, _) = tokio::sync::broadcast::channel(1);

                Ok(MergerAppendRet {
                    entry: Entry {
                        index,
                        items: ents
                            .into_iter()
                            .map(|ent| ent.items)
                            .collect::<Vec<_>>()
                            .concat(),
                    }, // FIXME: remove duplicates
                    broadcaster: tx,
                })
            },
        );

        Ok(wal)
    }

    /// Create WAL for a shard
    pub async fn create(shard_id: u64) -> Result<()> {
        WalInner::create(shard_id).await
    }
}

impl Wal {
    /// Get log entries in range [lo, hi]
    pub async fn entries(
        &self,
        lo: u64,
        hi: u64, /* lo..=hi */
        max_size: u64,
    ) -> Result<Vec<Entry>> {
        self.inner.write().await.entries(lo, hi, max_size).await
    }

    /// Append log as follower
    pub async fn append_by_follower(&self, ent: Entry) -> Result<()> {
        self.inner.write().await.append(vec![ent], true).await?;
        Ok(())
    }

    /// Append log as leader, supports group commit
    pub async fn append_by_leader(&self, ent: Entry) -> Result<(bool, MergerAppendRet)> {
        let (is_group_leader, merger_append_ret) = self.merger.r#do(ent).await?;

        Ok((is_group_leader, merger_append_ret?))
    }

    /// Log compaction
    pub async fn compact(&self, compact_index: u64) -> Result<()> {
        self.inner.write().await.compact(compact_index).await
    }

    /// Get the index of the first log entry
    pub async fn first_index(&self) -> u64 {
        self.inner.read().await.first_index()
    }

    /// Get the index of the next available log entry
    pub async fn next_index(&self) -> u64 {
        self.inner.read().await.next_index()
    }
}

/// Group commit return struct
#[derive(Clone)]
pub struct MergerAppendRet {
    pub entry: Entry,
    pub broadcaster: tokio::sync::broadcast::Sender<std::result::Result<(), ShardError>>,
}
