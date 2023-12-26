use std::sync::Arc;

use anyhow::Result;
use tokio::{select, sync::broadcast, sync::mpsc::Receiver, time::MissedTickBehavior};
use tracing::{error, info};

use crate::{
    mvcc::GarbageCollector,
    shard::{EntryWithNotifierSender, Shard, ShardError},
};

pub struct Leader {
    stop_ch: Option<broadcast::Sender<()>>,
}

impl Default for Leader {
    fn default() -> Self {
        Self::new()
    }
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
        let (tx, mut rx1) = broadcast::channel(1);
        self.stop_ch.replace(tx);

        let mut rx2 = rx1.resubscribe();
        let shard_cpy = shard.clone();

        tokio::spawn(async move {
            let shard_id = shard.shard_id();
            info!("start append entry task for shard: 0x{:016x}", shard_id);

            'outer: loop {
                tokio::select! {
                    _ = rx1.recv() => {
                        break;
                    }
                    e = input_rx.recv() => {
                        let mut success_replicate_count = 0;
                        let en = e.unwrap();

                        info!("fsm worker receive entry, entry: {:?}", en);

                        let replicates = shard.replicates();
                        'inner: for addr in replicates.iter() {
                            match shard.clone().append_log_entry_to(&addr.to_string(), en.entry.clone()).await {
                                Err(ShardError::NotLeaderWithTs(last_leader_chagne_ts)) => {
                                    shard.update_membership(false, last_leader_chagne_ts, None); // FIXME: error handling
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

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10 * 60));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut rx3 = rx2.resubscribe();

            loop {
                select! {
                    _ = rx2.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        GarbageCollector::do_gc(shard_cpy.clone(), &mut rx3).await;
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(())?;
        }

        Ok(())
    }
}
