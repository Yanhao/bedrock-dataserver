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

struct WalInner {
    shard_id: u64,
    wal_files: Vec<wal_file::WalFile>,
    dir: PathBuf,
}

impl WalInner {
    fn wal_file_suffix(path: impl AsRef<Path>) -> u64 {
        let path = path.as_ref().as_os_str().to_str().unwrap();
        let suffix_str = path.split('.').last().unwrap();

        suffix_str.parse().unwrap()
    }

    fn wal_dir_path(shard_id: u64) -> PathBuf {
        let wal_dir: PathBuf = CONFIG.read().wal_dir.as_ref().unwrap().into();

        wal_dir
            .join::<String>(format!(
                "{:08x}",
                ((shard_id & 0xFFFFFFFF_00000000) >> 32) as u32
            ))
            .join::<String>(format!("{:08x}", (shard_id & 0x00000000_FFFFFFFF) as u32))
    }

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

    pub async fn create(shard_id: u64) -> Result<()> {
        info!("create wal directory for 0x{:016x}", shard_id);
        let wal_manager_dir = Self::wal_dir_path(shard_id);
        create_dir_all(wal_manager_dir).await.unwrap();

        Ok(())
    }

    pub async fn load_wal_by_shard_id(shard_id: u64) -> Result<WalInner> {
        let wal_manager_dir = Self::wal_dir_path(shard_id);
        WalInner::load(wal_manager_dir, shard_id).await
    }

    fn generate_new_wal_file_path(&self) -> PathBuf {
        if self.wal_files.is_empty() {
            return self.dir.clone().join(String::from_str("wal.0").unwrap());
        }

        let suffix = self.wal_files.last().unwrap().suffix();
        self.dir.clone().join(format!("wal.{}", suffix + 1))
    }

    pub async fn remove_wal(shard_id: u64) -> Result<()> {
        remove_dir_all(Self::wal_dir_path(shard_id)).await.unwrap();
        Ok(())
    }

    async fn discard(&mut self, next_index: u64) -> Result<()> {
        if self.wal_files.is_empty() {
            return Ok(());
        }

        while self.wal_files.last().unwrap().next_version() > next_index {
            if self.wal_files.last().unwrap().first_version() > next_index {
                let wal_file = self.wal_files.pop().unwrap();

                std::fs::remove_file(&wal_file.path).unwrap();

                continue;
            }

            self.wal_files
                .last_mut()
                .unwrap()
                .discard(next_index)
                .await
                .unwrap();
        }

        Ok(())
    }
}

impl WalInner {
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

        if self.wal_files.len() == 1 && self.wal_files.first().unwrap().entry_len() <= 1 {
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
            ret.append(&mut ents);

            lo = if hi < file.next_version() {
                hi + 1
            } else {
                file.next_version()
            };
        }

        Ok(ret)
    }

    fn first_index(&self) -> u64 {
        if self.wal_files.is_empty() {
            return 0;
        }

        self.wal_files.first().unwrap().first_version()
    }

    fn next_index(&self) -> u64 {
        if self.wal_files.is_empty() {
            return 0;
        }

        self.wal_files.last().unwrap().next_version()
    }

    async fn append(&mut self, ents: Vec<Entry>, discard: bool) -> Result<u64> {
        // TODO: use group commit to improve performance
        if discard {
            self.discard(ents.first().unwrap().index).await?;
        }

        let mut iter = ents.into_iter().peekable();
        loop {
            let Some(ent) = iter.peek() else {
                break;
            };

            if self.wal_files.is_empty() || self.wal_files.last().unwrap().is_sealed() {
                let path = self.generate_new_wal_file_path();
                let new_wal_file = wal_file::WalFile::create_new_wal_file(path, self.next_index())
                    .await
                    .unwrap();

                self.wal_files.push(new_wal_file);
            }

            debug!("append wal entry, index: {}", ent.index);

            if let Err(e) = self
                .wal_files
                .last_mut()
                .unwrap()
                .append_entry(ent.clone())
                .await
            {
                if let Some(WalError::WalFileFull) = e.downcast_ref() {
                    self.wal_files.last_mut().unwrap().seal().await.unwrap();
                    continue;
                }
                return Err(e);
            }

            iter.next();
        }

        Ok(self.next_index() - 1)
    }

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

                    tokio::fs::remove_file(&path).await.unwrap();
                    info!("remove wal file {:?}", &path);
                }
            }

            return Ok(());
        }

        Ok(())
    }
}

async fn remove_files_in_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    let mut dir = fs::read_dir(path).await?;
    while let Some(entry) = dir.next_entry().await? {
        if entry.file_type().await?.is_file() {
            fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

pub struct Wal {
    inner: Arc<tokio::sync::RwLock<WalInner>>,
    merger: GroupCommitter<
        Entry,
        std::result::Result<MergerAppendRet, WalError>,
        Arc<tokio::sync::RwLock<WalInner>>,
    >,
}

impl Wal {
    pub async fn remove_wal(shard_id: u64) -> Result<()> {
        WalInner::remove_wal(shard_id).await
    }

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

    pub async fn create(shard_id: u64) -> Result<()> {
        WalInner::create(shard_id).await
    }
}

impl Wal {
    pub async fn entries(
        &self,
        lo: u64,
        hi: u64, /* lo..=hi */
        max_size: u64,
    ) -> Result<Vec<Entry>> {
        self.inner.write().await.entries(lo, hi, max_size).await
    }

    pub async fn append_by_follower(&self, ent: Entry) -> Result<()> {
        self.inner.write().await.append(vec![ent], true).await?;
        Ok(())
    }

    pub async fn append_by_leader(&self, ent: Entry) -> Result<(bool, MergerAppendRet)> {
        let (is_group_leader, merger_append_ret) = self.merger.r#do(ent).await?;

        Ok((is_group_leader, merger_append_ret?))
    }

    pub async fn compact(&self, compact_index: u64) -> Result<()> {
        self.inner.write().await.compact(compact_index).await
    }

    pub async fn first_index(&self) -> u64 {
        self.inner.read().await.first_index()
    }

    pub async fn next_index(&self) -> u64 {
        self.inner.read().await.next_index()
    }
}

#[derive(Clone)]
pub struct MergerAppendRet {
    pub entry: Entry,
    pub broadcaster: tokio::sync::broadcast::Sender<std::result::Result<(), ShardError>>,
}
