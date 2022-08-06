use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::Result;
use futures_util::stream;
use log::{info, warn};
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::Request;

use dataserver::replog_pb::Entry;
use dataserver::service_pb::{
    shard_append_log_request, ShardAppendLogRequest, ShardInstallSnapshotRequest,
};

use crate::connections::CONNECTIONS;
use crate::shard::error::ShardError;
use crate::shard::order_keeper::OrderKeeper;
use crate::shard::replicate_log::ReplicateLog;
use crate::shard::snapshoter::SnapShoter;
use crate::shard::Shard;
use crate::wal::WalManager;
use crate::wal::WalTrait;

const INPUT_CHANNEL_LEN: usize = 10240;

pub struct Fsm {
    pub shard: Arc<RwLock<Shard>>,
    // rep_log: Arc<RwLock<ReplicateLog>>,
    rep_log: Arc<RwLock<WalManager>>,
    next_index: AtomicU64,
    order_keeper: OrderKeeper,
    stop_ch: Mutex<Option<mpsc::Sender<()>>>,
    input: Mutex<Option<mpsc::Sender<EntryWithNotifierSender>>>,
}

pub struct EntryWithNotifierSender {
    pub entry: Entry,
    sender: mpsc::Sender<Result<(), ShardError>>,
}

pub struct EntryWithNotifierReceiver {
    pub entry: Entry,
    receiver: mpsc::Receiver<Result<(), ShardError>>,
}

impl EntryWithNotifierReceiver {
    pub async fn wait_result(&mut self) -> Result<(), ShardError> {
        self.receiver.recv().await.unwrap()
    }
}

impl Fsm {
    // pub fn new(shard: Shard, rep_log: Arc<RwLock<ReplicateLog>>) -> Self {
    pub fn new(shard: Shard, rep_log: Arc<RwLock<WalManager>>) -> Self {
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
        let shard_id = self.shard.read().await.get_shard_id();
        let rep_log = self.rep_log.clone();
        let shard = self.shard.clone();

        tokio::spawn(async move {
            info!("start append entry task for shard: {}", shard_id);

            'outer: loop {
                tokio::select! {
                    _ = rx.recv() => {
                        break;
                    }
                    e = input_rx.recv() => {
                        let en = e.unwrap();

                        'inner: for addr in replicates.iter() {
                            let addr_str = addr.to_string();

                            let client = CONNECTIONS.write().await.get_conn(addr_str).await;

                            let request = Request::new(ShardAppendLogRequest {
                                shard_id,
                                leader_change_ts: Some(shard.read().await.get_leader_change_ts().into()),
                                entries: vec![
                                    shard_append_log_request::Entry {
                                        op: en.entry.op.clone(),
                                        index: en.entry.index,
                                        key: en.entry.key.clone(),
                                        value: en.entry.value.clone(),
                                    }
                                ],
                            });

                            let resp = match client.as_ref().unwrap().write().await.shard_append_log(request).await {
                                Err(e) => {
                                    warn!("failed to  append_log, err {}", e);
                                    continue 'inner;

                                },
                                Ok(v) => v,
                            };
                            if resp.get_ref().is_old_leader {
                                en.sender.send(Err(ShardError::NotLeader)).await.unwrap();
                                continue 'outer;
                            }


                            let last_applied_index = resp.get_ref().last_applied_index;
                            if last_applied_index >= en.entry.index {
                                continue 'inner;
                            }

                            let entries_res =  rep_log.write().await.entries(last_applied_index, en.entry.index, 1024).await;
                            if let Err(_) = entries_res {
                                // let snap = shard.read().await.create_snapshot().await.unwrap();
                                let snap = shard.read().await.kv_store.read().await.create_snapshot_iter().await.unwrap();

                                let mut req_stream = vec![];
                                for kv in snap.into_iter() {
                                    let mut key = kv.0.to_owned();
                                    let mut value = kv.1.to_owned();

                                    let mut item = vec![];
                                    item.append(&mut key);
                                    item.append(&mut vec!['\n' as u8]);
                                    item.append(&mut value);

                                    req_stream.push(ShardInstallSnapshotRequest{shard_id, data_piece: item})
                                }

                                client.unwrap().write().await.shard_install_snapshot(Request::new(stream::iter(req_stream))).await.unwrap();
                                // ref: https://github.com/hyperium/tonic/blob/master/examples/routeguide-tutorial.md

                                continue 'inner;
                            }

                            let ents = entries_res.unwrap();

                            let request = Request::new(ShardAppendLogRequest {
                                shard_id,
                                leader_change_ts: Some(shard.read().await.get_leader_change_ts().into()),
                                entries: ents.iter().map(|ent| {
                                    shard_append_log_request::Entry {
                                        op: ent.op.clone(),
                                        index: ent.index,
                                        key: ent.key.clone(),
                                        value: ent.value.clone(),
                                    }
                                }).collect(),
                            }) ;

                            let resp = match client.unwrap().write().await.shard_append_log(request).await{
                                Err(e) => {
                                    warn!("failed to append_log, err: {}", e);
                                    continue 'inner;
                                },
                                Ok(v) => v,
                            };
                            if resp.get_ref().is_old_leader {
                                en.sender.send(Err(ShardError::NotLeader)).await.unwrap();
                                continue 'outer;
                            }
                        }

                        en.sender.send(Ok(())).await.unwrap();
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
            .await
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
