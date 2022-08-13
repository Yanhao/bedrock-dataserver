use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use log::error;
use tokio::fs::{create_dir_all, remove_dir};

use crate::config::CONFIG;

use super::error::WalError;
use super::{wal_file, WalTrait};

use dataserver::replog_pb::Entry;

const MAX_ENTRY_COUNT: u64 = 100;

pub struct Wal {
    wal_files: Vec<wal_file::WalFile>,
    dir: PathBuf,
}

impl Wal {
    pub async fn create_wal_dir(shard_id: u64) -> Result<()> {
        let wal_dir: PathBuf = CONFIG
            .read()
            .unwrap()
            .wal_directory
            .as_ref()
            .unwrap()
            .into();

        let storage_id: u32 = ((shard_id & 0xFFFF0000) >> 32) as u32;
        let shard_id: u32 = (shard_id & 0x0000FFFF) as u32;

        let wal_manager_dir = wal_dir
            .join::<String>(format!("{:#04x}", storage_id))
            .join::<String>(format!("{:#04x}", shard_id));

        create_dir_all(wal_manager_dir).await.unwrap();

        Ok(())
    }

    pub async fn load_wal_by_shard_id(shard_id: u64) -> Result<Wal> {
        let wal_dir: PathBuf = CONFIG
            .read()
            .unwrap()
            .wal_directory
            .as_ref()
            .unwrap()
            .into();

        let storage_id: u32 = ((shard_id & 0xFFFF0000) >> 32) as u32;
        let shard_id: u32 = (shard_id & 0x0000FFFF) as u32;

        let wal_manager_dir = wal_dir
            .join::<String>(format!("{:#04x}", storage_id))
            .join::<String>(format!("{:#04x}", shard_id));

        return Wal::load(wal_manager_dir).await;
    }

    async fn load(dir: impl AsRef<Path>) -> Result<Wal> {
        if !dir.as_ref().exists() {
            error!("file not exists: path: {}", dir.as_ref().display());
            bail!(WalError::FileNotExists);
        }

        if !dir.as_ref().is_dir() {
            bail!(WalError::WrongFilePath);
        }

        let mut wal_files = Vec::new();
        for entry in dir.as_ref().read_dir().expect("") {
            if let Ok(entry) = entry {
                let wal_file = wal_file::WalFile::load_wal_file(entry.path())
                    .await
                    .unwrap();
                wal_files.push(wal_file);
            }
        }

        Ok(Wal {
            wal_files,
            dir: dir.as_ref().to_owned(),
        })
    }

    fn generate_new_wal_file_path(&self) -> PathBuf {
        if self.wal_files.len() == 0 {
            return self.dir.clone().join(String::from_str("wal.0").unwrap());
        }

        let suffix = self.wal_files.last().unwrap().suffix();
        return self.dir.clone().join(format!("wal.{}", suffix));
    }
    
    pub async fn remove_wal(&self) -> Result<()> {
        remove_dir(self.dir.as_path()).await;
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
        self.wal_files.first().unwrap().first_version()
    }

    fn last_index(&self) -> u64 {
        self.wal_files.last().unwrap().last_version()
    }

    async fn append(&mut self, ents: Vec<Entry>) -> Result<()> {
        self.discard(ents.first().unwrap().index).await?;

        if self.wal_files.is_empty() || self.wal_files.last().unwrap().is_sealed() {
            let path = self.generate_new_wal_file_path();
            let new_wal_file = wal_file::WalFile::create_new_wal_file(path).await.unwrap();

            self.wal_files.push(new_wal_file);
        }

        for ent in ents.iter() {
            self.wal_files
                .last_mut()
                .unwrap()
                .append_entry(ent.clone())
                .await
                .unwrap();
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
