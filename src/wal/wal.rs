use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::fs::{create_dir_all, remove_dir_all};
use tracing::{debug, error, info};

use idl_gen::replog_pb::Entry;

use super::{wal_file, WalError, WalTrait};
use crate::config::CONFIG;

const MAX_ENTRY_COUNT: u64 = 100;

pub struct Wal {
    wal_files: Vec<wal_file::WalFile>,
    dir: PathBuf,
}

impl Wal {
    fn suffix(path: impl AsRef<Path>) -> u64 {
        let path = path.as_ref().as_os_str().to_str().unwrap();
        let suffix_str = path.split(".").last().unwrap();
        let suffix = suffix_str.parse().unwrap();
        suffix
    }

    fn generage_path(shard_id: u64) -> PathBuf {
        let wal_dir: PathBuf = CONFIG.read().wal_directory.as_ref().unwrap().into();

        info!("shard_id: 0x{:016x}", shard_id);
        let storage_id: u32 = ((shard_id & 0xFFFFFFFF_00000000) >> 32) as u32;
        let shard_isn: u32 = (shard_id & 0x00000000_FFFFFFFF) as u32;
        info!(
            "storage_id: 0x{:08x}, shard_isn: 0x{:08x}",
            storage_id, shard_isn
        );

        wal_dir
            .join::<String>(format!("{:08x}", storage_id))
            .join::<String>(format!("{:08x}", shard_isn))
    }

    async fn load(dir: impl AsRef<Path>) -> Result<Wal> {
        if !dir.as_ref().exists() {
            error!("file not exists: path: {}", dir.as_ref().display());
            bail!(WalError::FileNotExists);
        }

        if !dir.as_ref().is_dir() {
            bail!(WalError::WrongFilePath);
        }

        let mut wal_file_path = vec![];
        for entry in dir.as_ref().read_dir().unwrap() {
            if let Ok(entry) = entry {
                wal_file_path.push(entry.path());
            }
        }
        if wal_file_path.is_empty() {
            return Ok(Wal {
                wal_files: vec![],
                dir: dir.as_ref().to_owned(),
            });
        }

        wal_file_path.sort_by(|a: &PathBuf, b: &PathBuf| {
            let a_suffix = Self::suffix(a.as_path());
            let b_suffix = Self::suffix(b.as_path());
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

            debug!("wal last index: {}", wal_file.last_version());

            wal_files.push(wal_file);
            i += 1;
        }

        Ok(Wal {
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
        return Wal::load(wal_manager_dir).await;
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

    async fn discard(&mut self, index: u64) -> Result<()> {
        if self.wal_files.is_empty() {
            return Ok(());
        }
        loop {
            if self.wal_files.last().unwrap().last_version() < index {
                break;
            }

            if self.wal_files.last().unwrap().first_version() > index {
                let wal_file = self.wal_files.pop().unwrap();

                std::fs::remove_file(wal_file.path.clone()).unwrap();

                continue;
            }

            self.wal_files
                .last_mut()
                .unwrap()
                .discard(index)
                .await
                .unwrap();
            break;
        }

        Ok(())
    }
}

#[async_trait]
impl WalTrait for Wal {
    async fn entries(&mut self, mut lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>> {
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

        if hi >= self.last_index() + 1 {
            bail!(WalError::InvalidParameter);
        }

        let mut ret = vec![];

        let first_index = self.first_index();
        let last_index = self.last_index();

        for file in self.wal_files.iter_mut() {
            if lo >= hi {
                break;
            }

            if first_index < lo {
                continue;
            }
            if hi < last_index {
                break;
            }

            match file
                .entries(
                    lo,
                    if hi < file.last_version() {
                        hi
                    } else {
                        file.last_version()
                    },
                )
                .await
            {
                Err(e) => {
                    bail!(WalError::InvalidParameter);
                }
                Ok(mut v) => {
                    ret.append(&mut v);
                }
            }

            if hi < file.last_version() {
                lo = hi;
            } else {
                lo = file.last_version();
            }
        }

        Ok(ret)
    }

    fn first_index(&self) -> u64 {
        if self.wal_files.is_empty() {
            return 0;
        }

        self.wal_files.first().unwrap().first_version()
    }

    fn last_index(&self) -> u64 {
        if self.wal_files.is_empty() {
            return 0;
        }

        self.wal_files.last().unwrap().last_version()
    }

    async fn append(&mut self, ents: Vec<Entry>) -> Result<()> {
        self.discard(ents.first().unwrap().index).await?;

        let mut iter = ents.into_iter().peekable();
        loop {
            let Some(ent)= iter.peek() else {
                break;
            };

            if self.wal_files.is_empty() || self.wal_files.last().unwrap().is_sealed() {
                let path = self.generate_new_wal_file_path();
                let new_wal_file = wal_file::WalFile::create_new_wal_file(path).await.unwrap();

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

        Ok(())
    }

    async fn compact(&mut self, compact_index: u64) -> Result<()> {
        if self.wal_files.first().unwrap().first_version() >= compact_index {
            return Ok(());
        }

        while self.wal_files.len() > 0 {
            if self.wal_files.first().unwrap().last_version() < compact_index {
                let path = self.wal_files.first().unwrap().path.clone();
                self.wal_files.drain(0..0);

                std::fs::remove_file(path).unwrap();
            }
        }

        Ok(())
    }
}
