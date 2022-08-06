use std::fs::read_to_string;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::sync::RwLock;

use anyhow::{anyhow, Result};
use get_if_addrs::get_if_addrs;
use log::{debug, error, info};
use once_cell::sync::Lazy;
use serde::{self, Deserialize};

use crate::error::DataServerError;

// the location of aproject configuration directory.
pub const CONFIG_DIR: &str = "/etc/bedrock-dataserver";

pub static CONFIG: Lazy<RwLock<Configuration>> = Lazy::new(|| Default::default());
pub static SELF_ADDR: Lazy<RwLock<SocketAddr>> =
    Lazy::new(|| RwLock::new(unsafe { MaybeUninit::uninit().assume_init() }));

pub fn get_self_socket_addr() -> SocketAddr {
    *SELF_ADDR.read().unwrap()
}

#[derive(Deserialize, Debug, Clone)]
pub enum DiskType {
    SSD,
    HDD,
}

impl Default for DiskType {
    fn default() -> Self {
        DiskType::HDD
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Configuration {
    pub raft_server_addr: Option<String>,
    pub rpc_server_addr: Option<String>,

    pub daemon: Option<bool>,
    pub work_directory: Option<String>,
    pub disk_type: Option<DiskType>,
    pub managers: Option<Vec<String>>,
    pub wal_directory: Option<String>,
}

impl Configuration {
    // parsing the configuration file, default configuration file location is: /etc/aproject/config.toml
    pub fn parse_config_file(file: &str) -> Result<Configuration> {
        info!("parsing configuration file: {}", file);

        debug!("read string from {}", file);
        let file_contents = read_to_string(file).map_err(|_| {
            error!("failed to read configuration file {}", file);
            anyhow!(DataServerError::FailedToRead)
        })?;

        debug!("parse toml");

        let ret: Configuration = toml::from_str(&file_contents).map_err(|e| {
            println!("failed to parse configuration file: {}", e);
            anyhow!(DataServerError::InvalidToml)
        })?;

        debug!("successfully parsed configuration file");
        debug!("configuration: {:?}", ret);
        Ok(ret)
    }
}

fn validate_configuration(config: &Configuration) -> Result<()> {
    Ok(())
}

pub fn config_mod_init(config_file: &str) -> Result<()> {
    let conf = Configuration::parse_config_file(config_file).map_err(|e| {
        error!("failed to initialize config module");
        e
    })?;

    validate_configuration(&conf)?;

    let addr: SocketAddr = conf.raft_server_addr.clone().unwrap().parse().unwrap();
    let ip = get_if_addrs().unwrap()[0].addr.ip();

    *SELF_ADDR.write().unwrap() = SocketAddr::new(ip, addr.port());
    *CONFIG.write().unwrap() = conf;

    info!("successfully initialized config module");
    debug!("configuration: {:?}", *CONFIG.read().unwrap());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_config_parse() {
        let config_file1 = "tests/test_config/config1.toml";
        let config1_res = Configuration::parse_config_file(config_file1);
        assert!(config1_res.is_ok());

        let config1 = config1_res.unwrap();

        assert_eq!(config1.daemon.unwrap(), true);
        assert_eq!(config1.work_directory.unwrap(), "/");
        // assert_eq!(config1.disk_type.unwrap(), DiskType::SSD);
        assert_eq!(config1.rpc_server_addr.unwrap(), "0.0.0.0:8888");

        println!("{:?}", config1.disk_type);
        println!("{:?}", config1.managers);
    }
}
