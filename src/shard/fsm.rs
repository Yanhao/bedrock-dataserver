use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use futures_util::stream;
use log::{debug, error, info, warn};
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::Request;

use crate::replog_pb::Entry;
use crate::service_pb::{
    data_service_client, shard_append_log_request, ShardAppendLogRequest,
    ShardInstallSnapshotRequest,
};

use crate::connections::CONNECTIONS;
use crate::shard::error::ShardError;
use crate::shard::order_keeper::OrderKeeper;
use crate::shard::Shard;
use crate::wal::Wal;
use crate::wal::WalTrait;

const INPUT_CHANNEL_LEN: usize = 10240;

pub struct Fsm {
    shard: Arc<RwLock<Shard>>,
    rep_log: Arc<RwLock<Wal>>,

    next_index: AtomicU64,
    order_keeper: OrderKeeper,
    input: Mutex<Option<mpsc::Sender<EntryWithNotifierSender>>>,

    deleting: AtomicBool,
    installing_snapshot: AtomicBool,

    stop_ch: Mutex<Option<mpsc::Sender<()>>>,
}

#[derive(Debug)]
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
    pub fn new(shard: Shard, wal: Wal) -> Self {
        let next_index = wal.last_index() + 1;
        debug!("next_index: {}", next_index);

        let mut order_keeper = OrderKeeper::new();
        order_keeper.clear_waiters(next_index);

        Self {
            shard: Arc::new(RwLock::new(shard)),
            rep_log: Arc::new(RwLock::new(wal)),
            next_index: next_index.into(),
            order_keeper,
            input: Mutex::new(None),

            deleting: false.into(),
            installing_snapshot: false.into(),

            stop_ch: Mutex::new(None),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        *self.stop_ch.lock().await = Some(tx);

        let (input_tx, mut input_rx) = mpsc::channel(INPUT_CHANNEL_LEN);
        *self.input.lock().await = Some(input_tx);

        let rep_log = self.rep_log.clone();
        let shard = self.shard.clone();

        tokio::spawn(async move {
            let shard_id = shard.read().await.get_shard_id();

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

                        let replicates = shard.read().await.get_replicates();
                        'inner: for addr in replicates.iter() {
                            match Self::append_log_entry_to(shard.clone(), addr.to_string(), en.entry.clone(), rep_log.clone()).await {
                                Err(ShardError::NotLeader) => {
                                    shard.write().await.set_is_leader(false).await;
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

    pub async fn remove_wal(&mut self) -> Result<()> {
        self.rep_log.read().await.remove_wal().await;
        Ok(())
    }

    pub async fn process_write(&mut self, entry: Entry) -> Result<EntryWithNotifierReceiver> {
        let mut entry = entry.clone();
        entry.index = self.next_index.fetch_add(1, Ordering::Relaxed);

        let (tx, rx) = mpsc::channel(3);

        info!("ensure order, index: {}", entry.index);
        self.order_keeper.ensure_order(entry.index).await?;

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
            .await
            .unwrap();

        self.order_keeper.pass_order(entry.index).await;
        info!("pass order, index: {}", entry.index);

        Ok(EntryWithNotifierReceiver {
            entry,
            receiver: rx,
        })
    }

    pub async fn apply_entry(&mut self, entry: Entry) -> Result<()> {
        self.order_keeper.ensure_order(entry.index).await?;

        // TODO: use group commit to improve performance
        self.rep_log
            .write()
            .await
            .append(vec![entry.clone()])
            .await
            .unwrap();

        self.order_keeper.pass_order(entry.index).await;

        if entry.op == "put" {
            self.shard
                .write()
                .await
                .put(&entry.key, &entry.value)
                .await
                .unwrap();
        }

        Ok(())
    }

    pub fn get_shard(&self) -> Arc<RwLock<Shard>> {
        self.shard.clone()
    }

    pub async fn get_shard_id(&self) -> u64 {
        self.shard.read().await.get_shard_id()
    }

    pub async fn last_index(&self) -> u64 {
        self.rep_log.read().await.last_index()
    }

    pub fn get_next_index(&self) -> u64 {
        self.next_index.load(Ordering::Relaxed)
    }

    pub fn mark_deleting(&self) {
        self.deleting.store(true, Ordering::Relaxed);
    }

    pub fn is_deleting(&self) -> bool {
        self.deleting.load(Ordering::Relaxed)
    }

    pub fn set_installing_snapshot(&self, t: bool) {
        self.installing_snapshot.store(t, Ordering::Relaxed);
    }

    pub fn is_installing_snapshot(&self) -> bool {
        self.installing_snapshot.load(Ordering::Relaxed)
    }

    pub async fn is_leader(&self) -> bool {
        self.shard.read().await.is_leader()
    }

    pub fn reset_replog(&mut self, last_index: u64) -> Result<()> {
        todo!()
    }

    async fn append_log_entry_to(
        shard: Arc<RwLock<Shard>>,
        addr: String,
        en: Entry,
        replog: Arc<RwLock<Wal>>,
    ) -> std::result::Result<(), ShardError> {
        let shard_id = shard.read().await.get_shard_id();
        let client = CONNECTIONS
            .write()
            .await
            .get_conn(addr.clone())
            .await
            .unwrap();

        let request = Request::new(ShardAppendLogRequest {
            shard_id,
            leader_change_ts: Some(shard.read().await.get_leader_change_ts().into()),
            entries: vec![shard_append_log_request::Entry {
                op: en.op.clone(),
                index: en.index,
                key: en.key.clone(),
                value: en.value.clone(),
            }],
        });

        let resp = match client.write().await.shard_append_log(request).await {
            Err(e) => {
                warn!("failed to  append_log, err {}", e);
                return Err(ShardError::FailedToAppendLog);
            }
            Ok(v) => v,
        };

        if resp.get_ref().is_old_leader {
            warn!("failed to append log, not leader");
            return Err(ShardError::NotLeader);
        }

        let mut last_applied_index = resp.get_ref().last_applied_index;
        info!("last_applied_index from {}, {}", addr, last_applied_index);

        for i in 1..=3 {
            if last_applied_index >= en.index {
                return Ok(());
            }

            let entries_res = replog
                .write()
                .await
                .entries(last_applied_index, en.index, 1024)
                .await;

            if let Err(_) = entries_res {
                last_applied_index =
                    match Self::install_snapshot_to(shard.clone(), client.clone()).await {
                        Err(_) => {
                            return Err(ShardError::FailedToAppendLog);
                        }
                        Ok(v) => v,
                    };

                continue;
            }

            let ents = entries_res.unwrap();

            let request = Request::new(ShardAppendLogRequest {
                shard_id,
                leader_change_ts: Some(shard.read().await.get_leader_change_ts().into()),
                entries: ents
                    .iter()
                    .map(|ent| shard_append_log_request::Entry {
                        op: ent.op.clone(),
                        index: ent.index,
                        key: ent.key.clone(),
                        value: ent.value.clone(),
                    })
                    .collect(),
            });

            let resp = match client.write().await.shard_append_log(request).await {
                Err(e) => {
                    warn!("failed to append_log, err: {}", e);
                    return Err(ShardError::FailedToAppendLog);
                }
                Ok(v) => v,
            };

            if resp.get_ref().is_old_leader {
                return Err(ShardError::NotLeader);
            }
        }

        Err(ShardError::FailedToAppendLog)
    }

    async fn install_snapshot_to(
        shard: Arc<RwLock<Shard>>,
        client: Arc<RwLock<data_service_client::DataServiceClient<tonic::transport::Channel>>>,
    ) -> Result<u64 /*last_wal_index */> {
        let shard_id = shard.read().await.get_shard_id();

        let snap = shard.read().await.create_snapshot_iter().await.unwrap();

        let last_wal_index = shard.read().await.get_last_wal_index(); // FIXME: keep atomic with above

        let mut req_stream = vec![];
        for kv in snap.into_iter() {
            let mut key = kv.0.to_owned();
            let mut value = kv.1.to_owned();

            let mut item = vec![];
            item.append(&mut key);
            item.append(&mut vec!['\n' as u8]);
            item.append(&mut value);

            req_stream.push(ShardInstallSnapshotRequest {
                shard_id,
                data_piece: item,
                last_wal_index,
            })
        }

        client
            .write()
            .await
            .shard_install_snapshot(Request::new(stream::iter(req_stream)))
            .await
            .unwrap();
        // ref: https://github.com/hyperium/tonic/blob/master/examples/routeguide-tutorial.md

        return Ok(last_wal_index);
    }
}
