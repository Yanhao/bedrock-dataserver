use std::fs::{read_to_string, OpenOptions};
use std::io::Write;
use std::sync::RwLock;

use anyhow::Result;
use log::debug;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

const METADATA_PATH: &str = "metadata.json";

pub static METADATA: Lazy<RwLock<MetaData>> = Lazy::new(|| {
    let json_raw = read_to_string(METADATA_PATH).unwrap();
    debug!("parse json metadata");
    let m: JsonMeta = serde_json::from_str(&json_raw).unwrap();

    RwLock::new(MetaData::new(m))
});

#[derive(Deserialize, Serialize, Clone)]
pub struct JsonMeta {
    pub cluster_name: String,

    pub metaserver_addrs: Vec<String>,
    pub metaserver_addrs_update_ts: String,

    pub metaserver_leader: String,
    pub metaserver_leader_update_ts: String,
}

pub struct MetaData {
    data: JsonMeta,
}

impl MetaData {
    pub fn new(data: JsonMeta) -> Self {
        Self { data }
    }

    pub fn get_meta(&self) -> JsonMeta {
        self.data.clone()
    }

    pub fn save_meta(&mut self, json_meta: JsonMeta) -> Result<()> {
        let json_raw = serde_json::to_string(&json_meta).unwrap();

        let mut mfile = OpenOptions::new()
            .create(true)
            .append(true)
            .open(METADATA_PATH)?;

        mfile.write(json_raw.as_bytes()).unwrap();

        self.data = json_meta;

        Ok(())
    }
}
