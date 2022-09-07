use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use log::info;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use dataserver::service_pb::data_service_client;

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
            info!("get connection to http://{} from connection pool", addr);
            return Ok(c.to_owned());
        }

        info!("create connection to http://{}", addr);

        let client = data_service_client::DataServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let item = Arc::new(RwLock::new(client));
        self.conns.write().await.insert(addr, item.clone());

        Ok(item)
    }
}
