use std::sync::{Mutex, RwLock};

use anyhow::Result;
use log::{info, warn};
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc};

use dataserver::metaserver_pb::{meta_service_client, HeartBeatRequest};

use crate::{config::get_self_socket_addr, metadata::METADATA};

pub static HEART_BEATER: Lazy<RwLock<HeartBeater>> = Lazy::new(|| Default::default());

#[derive(Default)]
pub struct HeartBeater {
    stop_ch: Mutex<Option<mpsc::Sender<()>>>,
}

impl HeartBeater {
    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        *self.stop_ch.lock().unwrap() = Some(tx);

        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));

        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let meta = METADATA.read().unwrap().get_meta();
                        let addr = meta.metaserver_leader;

                        info!("heartbeat to metaserver ... metaserver addr: {}", addr);
                        let mut client = meta_service_client::MetaServiceClient::connect(addr.clone()).await.unwrap();

                        let req = tonic::Request::new(HeartBeatRequest{
                            addr: get_self_socket_addr().to_string(),
                        });

                        match client.heart_beat(req).await {
                            Err(e) => {
                                warn!("failed to heart beat to {}, err: {:?}", addr, e);
                            },
                            Ok(resp) => {
                                info!("heartbeat response: {:?}", resp);
                            }
                        }
                    }
                }
            }

            info!("heartbeat stopped ...");
        });

        Ok(())
    }

    pub async fn stop(&mut self) {
        self.stop_ch
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
    }
}
