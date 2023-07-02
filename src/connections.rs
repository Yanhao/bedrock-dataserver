use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::info;

use crate::service_pb::data_service_client;

pub static CONNECTIONS: Lazy<Connections> = Lazy::new(|| Connections::new());

pub struct Connections {
    conns: RwLock<HashMap<String, data_service_client::DataServiceClient<Channel>>>,
}

impl Connections {
    pub fn new() -> Self {
        Connections {
            conns: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_client(
        &self,
        addr: String,
    ) -> Result<data_service_client::DataServiceClient<Channel>> {
        // let sock_addr: SocketAddr = addr.parse().unwrap();

        if let Some(c) = self.conns.read().await.get(&addr) {
            info!("get connection to http://{} from connection pool", addr);
            return Ok(c.clone());
        }

        info!("create connection to http://{}", addr);

        let client = data_service_client::DataServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        self.conns.write().await.insert(addr, client.clone());

        Ok(client)
    }
}
