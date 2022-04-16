use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use dataserver::service_pb::data_service_client;
use tonic::transport::Channel;

pub static CONNECTIONS: Lazy<RwLock<Connection>> = Lazy::new(|| RwLock::new(Connection::new()));

pub struct Connection {
    conns: RwLock<HashMap<String, Arc<RwLock<data_service_client::DataServiceClient<Channel>>>>>,
}

impl Connection {
    pub fn new() -> Self {
        Connection {
            conns: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_conn(
        &mut self,
        addr: String,
    ) -> Result<Arc<RwLock<data_service_client::DataServiceClient<Channel>>>> {
        // let sock_addr: SocketAddr = addr.parse().unwrap();

        if let Some(c) = self.conns.read().await.get(&addr) {
            return Ok(c.to_owned());
        }

        let client = data_service_client::DataServiceClient::connect(addr)
            .await
            .unwrap();

        Ok(Arc::new(RwLock::new(client)))
    }
}
