use anyhow::Result;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::{select, sync::mpsc};
use tracing::{error, info};

use crate::ms_client::MS_CLIENT;

pub static HEART_BEATER: Lazy<RwLock<HeartBeater>> = Lazy::new(Default::default);

#[derive(Default)]
pub struct HeartBeater {
    stop_ch: Option<mpsc::Sender<()>>,
}

impl HeartBeater {
    pub fn start(&mut self) {
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
                        if let Err(e) = MS_CLIENT.heartbeat(false).await {
                            error!("heartbeat to metaserver failed, error: {e}");
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
