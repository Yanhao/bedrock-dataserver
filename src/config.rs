use std::net::SocketAddr;
use std::{fs::read_to_string, net::IpAddr};

use anyhow::Result;
use get_if_addrs::get_if_addrs;
use once_cell::sync::Lazy;
use serde::{self, Deserialize};
use tracing::{debug, error, info};

// the location of dataserver configuration directory.
pub const DEFAULT_CONFIG_FILE: &str = "/etc/bedrock/dataserver.toml";

static HOST_IP: Lazy<IpAddr> = Lazy::new(|| get_if_addrs().unwrap()[0].addr.ip());
pub static CONFIG: Lazy<parking_lot::RwLock<Configuration>> = Lazy::new(Default::default);

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Configuration {
    pub rpc_server_addr: Option<String>,
    pub metaserver_url: String,

    pub work_dir: Option<String>,
    pub wal_dir: Option<String>,
    pub data_dir: Option<String>,
}

impl Configuration {
    // parsing the configuration file, default configuration file location is: /etc/bedrock/config.toml
    pub fn parse_config_file(file: &str) -> Result<Configuration> {
        info!("parsing configuration file: {}", file);

        let file_contents = read_to_string(file)?;

        Ok(toml::from_str(&file_contents)?)
    }

    pub fn get_self_socket_addr(&self) -> SocketAddr {
        let addr: SocketAddr = self.rpc_server_addr.clone().unwrap().parse().unwrap();

        SocketAddr::new(*HOST_IP, addr.port())
    }

    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

pub fn init_config(config_file: &str) -> Result<()> {
    let conf = Configuration::parse_config_file(config_file)
        .inspect_err(|e| error!("failed to initialize config module, err: {e}"))?;

    conf.validate()?;

    *CONFIG.write() = conf;

    info!("successfully initialized config module");
    debug!("configuration: {:?}", *CONFIG.read());

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

        assert_eq!(config1.work_dir.unwrap(), "/");
        // assert_eq!(config1.disk_type.unwrap(), DiskType::SSD);
        assert_eq!(config1.rpc_server_addr.unwrap(), "0.0.0.0:8888");
    }
}
