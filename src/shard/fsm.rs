use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::Result;
use log::{info, warn};
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::Request;
use futures_util::stream;

use dataserver::replog_pb::Entry;
use dataserver::service_pb::{shard_append_log_request, ShardAppendLogRequest, ShardInstallSnapshotRequest};

use crate::connections::CONNECTIONS;
use crate::shard::order_keeper::OrderKeeper;
use crate::shard::replicate_log::ReplicateLog;
use crate::shard::snapshoter::SnapShoter;
use crate::shard::Shard;

const INPUT_CHANNEL_LEN: usize = 10240;

pub struct Fsm {
    pub shard: Arc<RwLock<Shard>>,
    rep_log: Arc<RwLock<ReplicateLog>>,
    next_index: AtomicU64,
    order_keeper: OrderKeeper,
    stop_ch: Mutex<Option<mpsc::Sender<()>>>,
    input: Mutex<Option<mpsc::Sender<EntryWithNotifierSender>>>,
}

pub struct EntryWithNotifierSender {
    pub entry: Entry,
    sender: mpsc::Sender<()>,
}

pub struct EntryWithNotifierReceiver {
    pub entry: Entry,
    receiver: mpsc::Receiver<()>,
}

impl EntryWithNotifierReceiver {
    pub async fn wait(&mut self) {
        self.receiver.recv().await;
    }
}

impl Fsm {
    pub fn new(shard: Shard, rep_log: Arc<RwLock<ReplicateLog>>) -> Self {
        Self {
            shard: Arc::new(RwLock::new(shard)),
            rep_log,
            next_index: 0.into(),
            order_keeper: OrderKeeper::new(),
            stop_ch: Mutex::new(None),
            input: Mutex::new(None),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        *self.stop_ch.lock().await = Some(tx);

        let (input_tx, mut input_rx) = mpsc::channel(INPUT_CHANNEL_LEN);
        *self.input.lock().await = Some(input_tx);

        let replicates = self.shard.read().await.get_replicates();
        let shard_id = self.shard.read().await.shard_id;
        let rep_log = self.rep_log.clone();
        let shard = self.shard.clone();

        tokio::spawn(async move {
            info!("start append entry task for shard: {}", shard_id);

            loop {
                tokio::select! {
                    _ = rx.recv() => {
                        break;
                    }
                    e = input_rx.recv() => {
                        let en = e.unwrap();

                        for addr in replicates.iter() {
                            let addr_str = addr.to_string();

                            let client = CONNECTIONS.write().await.get_conn(addr_str).await;

                            let request = Request::new(ShardAppendLogRequest {
                                shard_id,
                                entries: vec![
                                    shard_append_log_request::Entry {
                                        op: en.entry.op.clone(),
                                        index: en.entry.index,
                                        key: en.entry.key.clone(),
                                        value: en.entry.value.clone(),
                                    }
                                ],
                            });

                            let resp =  client.as_ref().unwrap().write().await.shard_append_log(request).await;
                            if let Err(e) = resp {
                                warn!("failed to  append_log, err {}", e);
                                continue
                            }

                            let last_applied_index = resp.unwrap().get_ref().last_applied_index;
                            if last_applied_index >= en.entry.index {
                                continue
                            }

                            let entries_res =  rep_log.read().await.entries(last_applied_index, en.entry.index, 1024) ;
                            if let Err(_) = entries_res {
                                let snap = shard.read().await.create_snapshot().await.unwrap();

                                let mut req_stream = vec![];
                                for i in snap.iter() {
                                    req_stream.push(ShardInstallSnapshotRequest{shard_id, data_piece: i.to_vec()})
                                }

                                client.unwrap().write().await.shard_install_snapshot(Request::new(stream::iter(req_stream))).await.unwrap();
                                // ref: https://github.com/hyperium/tonic/blob/master/examples/routeguide-tutorial.md

                                continue
                            }

                            let ents = entries_res.unwrap();

                            let request = Request::new(ShardAppendLogRequest {
                                shard_id,
                                entries: ents.iter().map(|ent| {
                                    shard_append_log_request::Entry {
                                        op: ent.op.clone(),
                                        index: ent.index,
                                        key: ent.key.clone(),
                                        value: ent.value.clone(),
                                    }
                                }).collect(),
                            }) ;

                            let resp = client.unwrap().write().await.shard_append_log(request).await;
                            if let Err(e) = resp {
                                warn!("failed to append_log, err: {}", e)
                            }
                        }

                        en.sender.send(()).await.unwrap();
                    }
                }
            }

            info!("stop append entry task for shard: {}", shard_id);
        });

        Ok(())
    }

    pub async fn stop(&mut self) {
        self.stop_ch
            .lock()
            .await
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
    }

    pub async fn apply(
        &mut self,
        entry: Entry,
        generate_index: bool,
    ) -> Result<EntryWithNotifierReceiver> {
        let mut entry = entry.clone();
        if generate_index {
            entry.index = self
                .next_index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let (tx, rx) = mpsc::channel(3);

        self.order_keeper.ensure_order(entry.index).await.unwrap();

        // TODO: use group commit to improve performance
        self.rep_log
            .write()
            .await
            .append(vec![entry.clone()])
            .unwrap();

        self.input
            .lock()
            .await
            .as_ref()
            .unwrap()
            .send(EntryWithNotifierSender {
                entry: entry.clone(),
                sender: tx,
            })
            .await;

        self.order_keeper.pass_order(entry.index).await;

        if entry.op == "put" {
            self.shard
                .write()
                .await
                .put(entry.key.as_slice(), entry.value.as_slice())
                .await
                .unwrap();
        }

        Ok(EntryWithNotifierReceiver {
            entry,
            receiver: rx,
        })
    }

    pub async fn last_index(&self) -> u64 {
        self.rep_log.read().await.last_index()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.shard.read().await.get(key).await
    }
}
