use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Result};
use tracing::info;

use crate::error::DataServerError;

pub struct Formatter {
    directory: PathBuf,
}

impl Formatter {
    pub fn new(p: impl AsRef<Path>) -> Result<Self> {
        ensure!(p.as_ref().exists(), DataServerError::PathNotExists);
        ensure!(p.as_ref().is_dir(), DataServerError::IsDir);

        Ok(Self {
            directory: p.as_ref().to_path_buf(),
        })
    }

    pub fn format(&self) -> Result<()> {
        info!("formatting...");

        fs::create_dir(self.directory.join("log"))?;
        fs::create_dir(self.directory.join("meta"))?;
        fs::create_dir(self.directory.join("journal"))?;
        fs::create_dir(self.directory.join("chunks"))?;

        for i in 0..=0xfff {
            fs::create_dir(self.directory.join(format!("chunks/{:#05x}", i)))?;
        }

        info!("successfully formatted {:?}!", self.directory.as_os_str());

        Ok(())
    }
}
