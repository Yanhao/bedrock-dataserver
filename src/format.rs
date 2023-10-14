use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Result};
use tracing::info;
// use rocksdb::{Options, DB};

// use crate::meta::MetaData;
// use crate::BlockServerError;
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

    pub fn format(&self) -> std::io::Result<()> {
        info!("formatting...");

        fs::create_dir(self.directory.join("log"))?;
        fs::create_dir(self.directory.join("meta"))?;
        fs::create_dir(self.directory.join("journal"))?;
        fs::create_dir(self.directory.join("chunks"))?;

        for i in 0..=0xfff {
            fs::create_dir(self.directory.join(format!("chunks/{:#05x}", i)))?;
        }

        // let meta_path = self.directory.join("meta");
        // let _m = match MetaData::new(&meta_path) {
        //     Err(e) => {
        //         eprintln!("failed to create rocks db");
        //         return Err(std::io::Error::new(std::io::ErrorKind::Other, ""));
        //     }
        //     Ok(v) => v,
        // };

        // let _ = DB::destroy(&Options::default(), &meta_path);
        // .unwrap();

        info!("successfully formatted {:?}!", self.directory.as_os_str());

        Ok(())
    }
}
