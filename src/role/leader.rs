use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc::{self, Receiver};
use tracing::{error, info};

use crate::shard::{EntryWithNotifierSender, Shard, ShardError};

pub struct Leader {
    stop_ch: Option<mpsc::Sender<()>>,
}

impl Leader {
    pub fn new() -> Self {
        Self { stop_ch: None }
    }

    pub async fn start(
        &mut self,
        shard: Arc<Shard>,
        mut input_rx: Receiver<EntryWithNotifierSender>,
    ) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        tokio::spawn(async move {
            let shard_id = shard.get_shard_id();
            info!("start append entry task for shard: {}", shard_id);

            'outer: loop {
                tokio::select! {
                    _ = rx.recv() => {
                        break;
                    }
                    e = input_rx.recv() => {
                        let mut success_replicate_count = 0;
                        let en = e.unwrap();

                        info!("fsm worker receive entry, entry: {:?}", en);

                        let replicates = shard.get_replicates();
                        'inner: for addr in replicates.iter() {
                            match shard.clone().append_log_entry_to(&addr.to_string(), en.entry.clone()).await {
                                Err(ShardError::NotLeader) => {
                                    shard.set_is_leader(false);
                                    shard.switch_role_to_follower().await.unwrap();
                                    en.sender.send(Err(ShardError::NotLeader)).await.unwrap();
                                    continue 'outer;
                                },
                                Err(ShardError::FailedToAppendLog) => {
                                    continue 'inner;
                                },
                                Ok(_) => {
                                    success_replicate_count +=1;
                                    continue 'inner;
                                }
                                Err(_) =>{
                                    panic!("should not happen");
                                }
                            }
                        }

                        info!("success_replicate_count: {}", success_replicate_count);
                        if success_replicate_count >= 0 {
                            if let Err(e) = en.sender.send(Ok(())).await {
                                error!("failed: {}", e);
                            }
                        } else {
                            en.sender.send(Err(ShardError::FailedToAppendLog)).await.unwrap();
                        }
                    }
                }
            }

            info!("stop append entry task for shard: {}", shard_id);
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
