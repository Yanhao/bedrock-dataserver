use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};

use super::error::WalError;
use super::wal_file;

pub struct WalManager {
    wal_files: Vec<wal_file::WalFile>,
    dir: PathBuf,
}

impl WalManager {
    pub async fn load(dir: impl AsRef<Path>) -> Result<WalManager> {
        if !dir.as_ref().exists() {
            bail!(WalError::FileExists);
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

        Ok(WalManager {
            wal_files,
            dir: dir.as_ref().to_owned(),
        })
    }

    pub async fn entries(
        &mut self,
        mut lo: u64,
        hi: u64,
        max_size: u64,
    ) -> Result<Vec<Vec<Vec<u8>>>> {
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

        let mut ret = Vec::new();

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
                Ok(v) => {
                    ret.push(v);
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

    pub fn first_index(&self) -> u64 {
        self.wal_files.first().unwrap().first_version()
    }
    pub fn last_index(&self) -> u64 {
        self.wal_files.last().unwrap().last_version()
    }

    pub async fn append(&mut self, ents: Vec<Vec<u8>>) -> Result<()> {
        if self.wal_files.last().unwrap().is_sealed() {
            let path = self.generate_new_wal_file_path();
            let new_wal_file = wal_file::WalFile::create_new_wal_file(path).await.unwrap();

            self.wal_files.push(new_wal_file);
        }

        for ent in ents.iter() {
            self.wal_files
                .last_mut()
                .unwrap()
                .append_entry(&vec![], &vec![], 0)
                .await;
        }

        Ok(())
    }

    fn generate_new_wal_file_path(&self) -> PathBuf {
        if self.wal_files.len() == 0 {
            return "wal.0".to_owned().into();
        }

        let suffix = self.wal_files.last().unwrap().suffix();

        format!("wal.{}", suffix).to_owned().into()
    }

    pub async fn compact(&mut self, compact_index: u64) -> Result<()> {
        if self.wal_files.first().unwrap().first_version() >= compact_index {
            return Ok(());
        }

        while self.wal_files.len() > 0 {
            if self.wal_files.first().unwrap().last_version() < compact_index {
                let path = self.wal_files.first().unwrap().path.clone();
                self.wal_files.drain(0..0);

                std::fs::remove_file(path);
            }
        }

        Ok(())
    }
}
