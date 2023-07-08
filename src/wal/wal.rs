use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::fs::{self, create_dir_all, remove_dir_all};
use tracing::{debug, error, info};

use idl_gen::replog_pb::Entry;

use super::{wal_file, WalError, WalTrait};
use crate::config::CONFIG;

const MAX_ENTRY_COUNT: u64 = 10;

pub struct Wal {
    shard_id: u64,
    wal_files: Vec<wal_file::WalFile>,
    dir: PathBuf,
}

impl Wal {
    fn wal_file_suffix(path: impl AsRef<Path>) -> u64 {
        let path = path.as_ref().as_os_str().to_str().unwrap();
        let suffix_str = path.split(".").last().unwrap();
        let suffix = suffix_str.parse().unwrap();
        suffix
    }

    fn generage_path(shard_id: u64) -> PathBuf {
        let wal_dir: PathBuf = CONFIG.read().wal_directory.as_ref().unwrap().into();

        let storage_id: u32 = ((shard_id & 0xFFFFFFFF_00000000) >> 32) as u32;
        let shard_isn: u32 = (shard_id & 0x00000000_FFFFFFFF) as u32;
        info!(
            "shard_id: 0x{:016x}, {}; storage_id: 0x{:08x}, shard_isn: 0x{:08x}",
            shard_id, shard_id, storage_id, shard_isn
        );

        wal_dir
            .join::<String>(format!("{:08x}", storage_id))
            .join::<String>(format!("{:08x}", shard_isn))
    }

    async fn load(dir: impl AsRef<Path>, shard_id: u64) -> Result<Wal> {
        if !dir.as_ref().exists() {
            error!("file not exists: path: {}", dir.as_ref().display());
            bail!(WalError::FileNotExists);
        }

        if !dir.as_ref().is_dir() {
            bail!(WalError::WrongFilePath);
        }

        info!("start load wal at: {} ...", dir.as_ref().display());
        let mut wal_file_path = vec![];
        for entry in dir.as_ref().read_dir().unwrap() {
            if let Ok(entry) = entry {
                info!("load wal file: {}", entry.path().display());
                wal_file_path.push(entry.path());
            }
        }
        if wal_file_path.is_empty() {
            return Ok(Wal {
                shard_id,
                wal_files: vec![],
                dir: dir.as_ref().to_owned(),
            });
        }

        wal_file_path.sort_by(|a: &PathBuf, b: &PathBuf| {
            let a_suffix = Self::wal_file_suffix(a.as_path());
            let b_suffix = Self::wal_file_suffix(b.as_path());
            u64::cmp(&a_suffix, &b_suffix)
        });

        debug!("wal_file_path length: {}", wal_file_path.len());

        let mut wal_files = vec![];

        let mut i = 0;
        let last_i = wal_file_path.len() - 1;
        for p in wal_file_path.iter() {
            info!("load wal, path: {}", p.as_path().display());

            assert!(i <= last_i, "invalid vector index");
            let wal_file = wal_file::WalFile::load_wal_file(p.as_path(), i == last_i && i != 0)
                .await
                .unwrap();

            debug!("wal next index: {}", wal_file.next_version());

            wal_files.push(wal_file);
            i += 1;
        }

        Ok(Wal {
            shard_id,
            wal_files,
            dir: dir.as_ref().to_owned(),
        })
    }

    pub async fn create_wal_dir(shard_id: u64) -> Result<()> {
        let wal_manager_dir = Self::generage_path(shard_id);
        create_dir_all(wal_manager_dir).await.unwrap();

        Ok(())
    }

    pub async fn load_wal_by_shard_id(shard_id: u64) -> Result<Wal> {
        let wal_manager_dir = Self::generage_path(shard_id);
        return Wal::load(wal_manager_dir, shard_id).await;
    }

    fn generate_new_wal_file_path(&self) -> PathBuf {
        if self.wal_files.len() == 0 {
            return self.dir.clone().join(String::from_str("wal.0").unwrap());
        }

        let suffix = self.wal_files.last().unwrap().suffix();
        return self.dir.clone().join(format!("wal.{}", suffix + 1));
    }

    pub async fn remove_wal(shard_id: u64) -> Result<()> {
        remove_dir_all(Self::generage_path(shard_id)).await.unwrap();
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

#[async_trait]
impl WalTrait for Wal {
    async fn entries(
        &mut self,
        mut lo: u64,
        hi: u64, /* lo..=hi */
        max_size: u64,
    ) -> Result<Vec<Entry>> {
        info!("wal entries: lo: {lo}, hi: {hi}");

        if hi - lo >= MAX_ENTRY_COUNT {
            bail!(WalError::TooManyEntries);
        }

        if self.wal_files.len() == 0 {
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
            if file.next_version() < lo {
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
        if self.next_index() == compact_index {
            return Ok(());
        }

        if self.next_index() < compact_index {
            self.wal_files = vec![];
            remove_files_in_dir(self.dir.clone()).await?;

            let path = self.generate_new_wal_file_path();
            let new_wal_file = wal_file::WalFile::create_new_wal_file(path, compact_index)
                .await
                .unwrap();

            self.wal_files.push(new_wal_file);

            return Ok(());
        }

        info!("compact log to {compact_index}");
        return Ok(());

        if self.wal_files.first().unwrap().first_version() >= compact_index {
            return Ok(());
        }

        // while !self.wal_files.is_empty() {
        //     if self.wal_files.first().unwrap().last_version() < compact_index {
        //         let path = self.wal_files.first().unwrap().path.clone();

        //         self.wal_files.drain(0..0); // FIXME: another way?

        //         info!("remove wal file: {}", path.display());
        //         std::fs::remove_file(path);
        //     }
        // }

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
