use anyhow::Result;
use log::{info, warn};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::{select, sync::mpsc};

use crate::config::get_self_socket_addr;
use crate::metadata::METADATA;
use crate::metaserver_pb::{meta_service_client, HeartBeatRequest};

pub static HEART_BEATER: Lazy<RwLock<HeartBeater>> = Lazy::new(|| Default::default());

#[derive(Default)]
pub struct HeartBeater {
    stop_ch: Option<mpsc::Sender<()>>,
}

impl HeartBeater {
    pub async fn start(&mut self) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let meta = METADATA.read().unwrap().get_meta();
                        let addr = meta.metaserver_leader;

                        info!("heartbeat to metaserver ... metaserver addr: {}", addr);
                        let mut client = match  meta_service_client::MetaServiceClient::connect(addr.clone()).await {
                            Err(e) => {
                                warn!("failed to connect to {}, err: {:?}", addr, e);
                                continue;
                            },
                            Ok(v) => v
                        };

                        let req = tonic::Request::new(HeartBeatRequest{
                            addr: get_self_socket_addr().to_string(),
                            restarting: false,
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
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
