use std::collections::HashMap;

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, info};

use idl_gen::service_pb::data_service_client;

pub static CONNECTIONS: Lazy<Connections> = Lazy::new(Connections::new);

pub struct Connections {
    conns: RwLock<HashMap<String, data_service_client::DataServiceClient<Channel>>>,
}

impl Default for Connections {
    fn default() -> Self {
        Self::new()
    }
}

impl Connections {
    pub fn new() -> Self {
        Connections {
            conns: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_client(
        &self,
        addr: &str,
    ) -> Result<data_service_client::DataServiceClient<Channel>> {
        if let Some(c) = self.conns.read().await.get(addr) {
            debug!("get connection to http://{} from connection pool", addr);
            return Ok(c.clone());
        }

        info!("create connection to http://{}", addr);

        let client =
            data_service_client::DataServiceClient::connect(format!("http://{}", addr)).await?;

        self.conns
            .write()
            .await
            .insert(addr.to_owned(), client.clone());

        Ok(client)
    }
}
