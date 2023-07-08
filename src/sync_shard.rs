use anyhow::Result;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::{select, sync::mpsc};
use tracing::{error, info};

use crate::metadata::{Meta, METADATA};
use crate::ms_client::MS_CLIENT;

pub static SHARD_SYNCER: Lazy<RwLock<ShardSyncer>> = Lazy::new(Default::default);

#[derive(Default)]
pub struct ShardSyncer {
    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardSyncer {
    pub fn start(&mut self) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let shards = METADATA.read().shard_iter();

                        if let Err(e) = MS_CLIENT.sync_shards_to_ms(shards).await {
                            error!("sync shard to metaserver failed, error: {e}");
                        }
                    }
                }
            }

            info!("sync shard stopped ...");
        });
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
