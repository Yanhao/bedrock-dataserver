use std::net::SocketAddr;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time;
use std::vec::Vec;

use anyhow::{bail, Result};
use futures_util::stream;
use num_bigint::{BigUint, ToBigUint};
use parking_lot;
use prost::Message;
use tokio::sync::{mpsc, RwLock};
use tonic::Request;
use tracing::{error, info, warn};

use crate::connections::CONNECTIONS;
use crate::kv_store::{self, SledStore};
use crate::replog_pb::Entry;
use crate::service_pb::{
    data_service_client, shard_append_log_request, CreateShardRequest, ShardAppendLogRequest,
    ShardInstallSnapshotRequest, ShardMeta,
};
use crate::shard::error::{self, ShardError};
use crate::shard::order_keeper::OrderKeeper;
use crate::wal::{Wal, WalTrait};

const INPUT_CHANNEL_LEN: usize = 10240;
const SHARD_META_KEY: &'static str = "shard_meta";
const KV_RANGE_LIMIT: i32 = 256;

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

pub struct Shard {
    shard_meta: Arc<parking_lot::RwLock<ShardMeta>>,

    kv_store: kv_store::SledStore,

    input: Option<mpsc::Sender<EntryWithNotifierSender>>,
    rep_log: Arc<RwLock<Wal>>,

    next_index: AtomicU64,
    order_keeper: OrderKeeper,

    deleting: AtomicBool,
    installing_snapshot: AtomicBool,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl Shard {
    fn new(kv_store: kv_store::SledStore, meta: ShardMeta, wal: Wal) -> Self {
        let next_index = wal.last_index() + 1;

        return Shard {
            shard_meta: Arc::new(parking_lot::RwLock::new(meta)),
            kv_store,
            rep_log: Arc::new(RwLock::new(wal)),
            deleting: false.into(),
            installing_snapshot: false.into(),
            next_index: next_index.into(),
            order_keeper: OrderKeeper::new(),
            input: None,
            stop_ch: None,
        };
    }

    pub async fn load_shard(shard_id: u64) -> Result<Self> {
        let sled_db = kv_store::SledStore::load(shard_id).await.unwrap();

        let meta_key = SHARD_META_KEY.as_bytes();
        let raw_meta = sled_db.kv_get(meta_key).await.unwrap();
        let meta = ShardMeta::decode(raw_meta.as_slice()).unwrap();
        let wal = Wal::load_wal_by_shard_id(shard_id).await.unwrap();

        Ok(Self::new(sled_db, meta, wal))
    }

    pub async fn create_shard(req: &Request<CreateShardRequest>) -> Result<Self> {
        let sled_db = kv_store::SledStore::create(req.get_ref().shard_id)
            .await
            .unwrap();

        let shard_id = req.get_ref().shard_id;
        let meta = ShardMeta {
            shard_id,
            is_leader: false,
            create_ts: req.get_ref().create_ts.to_owned().unwrap().into(),
            leader: req.get_ref().leader.clone(),
            leader_change_ts: req.get_ref().leader_change_ts.to_owned().unwrap().into(),
            replicates_update_ts: req.get_ref().replica_update_ts.to_owned().unwrap().into(),

            replicates: req.get_ref().replicates.clone(),
            last_wal_index: 0,

            min_key: req.get_ref().min_key.clone(),
            max_key: req.get_ref().max_key.clone(),
        };

        let mut meta_buf = Vec::new();
        meta.encode(&mut meta_buf).unwrap();
        sled_db
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        let wal = Wal::load_wal_by_shard_id(shard_id).await.unwrap();

        Ok(Self::new(sled_db, meta, wal))
    }

    pub async fn create_shard_for_split(
        shard_id: u64,
        leader: String,
        replicates: Vec<SocketAddr>,
        last_wal_index: u64,
        key_range: Range<Vec<u8>>,
    ) -> Result<Self> {
        let sled_db = kv_store::SledStore::create(shard_id).await.unwrap();

        let meta = ShardMeta {
            shard_id,
            is_leader: false,
            create_ts: Some(time::SystemTime::now().into()),
            replicates_update_ts: Some(time::SystemTime::now().into()),
            leader_change_ts: Some(time::SystemTime::now().into()),

            leader,
            replicates: replicates.into_iter().map(|r| r.to_string()).collect(),

            last_wal_index,

            min_key: key_range.start,
            max_key: key_range.end,
        };

        let mut meta_buf = Vec::new();
        meta.encode(&mut meta_buf).unwrap();
        sled_db
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        let wal = Wal::load_wal_by_shard_id(shard_id).await.unwrap();

        Ok(Self::new(sled_db, meta, wal))
    }
}

impl Shard {
    pub fn get_shard_id(&self) -> u64 {
        self.shard_meta.read().shard_id
    }

    pub fn is_deleting(&self) -> bool {
        self.deleting.load(Ordering::Relaxed)
    }

    pub fn mark_deleting(&self) {
        self.deleting.store(true, Ordering::Relaxed)
    }

    pub fn is_installing_snapshot(&self) -> bool {
        self.installing_snapshot.load(Ordering::Relaxed)
    }

    pub fn set_installing_snapshot(&self, i: bool) {
        self.installing_snapshot.store(i, Ordering::Relaxed);
    }

    pub fn is_leader(&self) -> bool {
        self.shard_meta.read().is_leader
    }

    pub async fn set_is_leader(&self, l: bool) {
        self.shard_meta.write().is_leader = l;
        self.save_meta().await.unwrap();
    }

    pub fn get_leader(&self) -> String {
        self.shard_meta.read().leader.clone().into()
    }

    pub async fn update_leader_change_ts(&self, t: time::SystemTime) {
        self.shard_meta.write().leader_change_ts = Some(t.into());
        self.save_meta().await.unwrap();
    }

    pub fn get_leader_change_ts(&self) -> time::SystemTime {
        self.shard_meta
            .read()
            .leader_change_ts
            .clone()
            .unwrap()
            .into()
    }

    pub fn get_replicates(&self) -> Vec<SocketAddr> {
        self.shard_meta
            .read()
            .replicates
            .iter()
            .map(|a| a.parse().unwrap())
            .collect()
    }

    pub fn get_replicates_strings(&self) -> Vec<String> {
        self.shard_meta.read().replicates.clone()
    }

    pub async fn set_replicates(&self, replicates: &[SocketAddr]) -> Result<()> {
        self.shard_meta.write().replicates = replicates.iter().map(|s| s.to_string()).collect();
        self.save_meta().await.unwrap();

        Ok(())
    }

    pub async fn save_meta(&self) -> Result<()> {
        let mut meta_buf = Vec::new();
        self.shard_meta.read().encode(&mut meta_buf).unwrap();
        self.kv_store
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn remove_shard(&self) -> Result<()> {
        SledStore::remove(self.get_shard_id()).await.unwrap();

        Ok(())
    }

    pub async fn remove_wal(&self) -> Result<()> {
        self.rep_log.read().await.remove_wal().await;
        Ok(())
    }
}

impl Shard {
    pub async fn get_last_index(&self) -> u64 {
        self.rep_log.read().await.last_index()
    }

    pub fn get_next_index(&self) -> u64 {
        self.next_index.load(Ordering::Relaxed)
    }

    pub fn reset_replog(&self, last_index: u64) -> Result<()> {
        todo!()
    }
}

impl Shard {
    pub fn middle_key(&self) -> Vec<u8> {
        let min = BigUint::from_bytes_be(&self.shard_meta.read().min_key);
        let max = BigUint::from_bytes_be(&self.shard_meta.read().max_key);

        let middle = min.clone() + (max - min) / 2.to_biguint().unwrap();

        middle.to_bytes_be()
    }

    pub fn min_key(&self) -> Vec<u8> {
        self.shard_meta.read().min_key.clone()
    }

    pub fn max_key(&self) -> Vec<u8> {
        self.shard_meta.read().max_key.clone()
    }

    pub fn is_key_within_shard(&self, key: &[u8]) -> bool {
        let min = BigUint::from_bytes_be(&self.shard_meta.read().min_key);
        let max = BigUint::from_bytes_be(&self.shard_meta.read().max_key);

        let key = BigUint::from_bytes_be(key);

        return min <= key && key < max;
    }

    pub async fn create_split_iter(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        Ok(kv_store::StoreIter {
            iter: self.kv_store.db.range(start_key..end_key),
        })
    }
}

impl Shard {
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        info!("shard put: key={:?}, value={:?}", key, value,);

        self.kv_store.kv_set(key, value).await.unwrap();

        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let value = match self.kv_store.kv_get(key.as_ref()).await {
            Err(_) => {
                bail!(error::ShardError::NoSuchKey)
            }
            Ok(v) => v,
        };

        Ok(value.to_vec())
    }

    pub async fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<kv_store::KeyValue>> {
        self.kv_store
            .kv_scan(start_key, end_key, KV_RANGE_LIMIT)
            .await
    }

    pub async fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.kv_store
            .kv_delete_range(start_key, end_key)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn clear_data(&self) {
        todo!()
    }

    pub async fn create_snapshot_iter(&self) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        Ok(kv_store::StoreIter {
            iter: self.kv_store.db.iter(),
        })
    }

    pub async fn kv_install_snapshot(&self, piece: &[u8]) -> Result<()> {
        let piece = piece.to_owned();
        let mut ps = piece.split(|b| *b == '\n' as u8);

        let key = ps.next().unwrap().to_owned();
        let value = ps.next().unwrap().to_owned();
        assert!(ps.next().is_none());

        self.kv_store.kv_set(&key, &value).await.unwrap();

        Ok(())
    }
}

impl Shard {
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        // self.stop_ch.replace(tx);

        let (input_tx, mut input_rx) = mpsc::channel::<EntryWithNotifierSender>(INPUT_CHANNEL_LEN);
        // self.input.replace(input_tx);

        let rep_log = self.rep_log.clone();
        let shard = self.clone();

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
                            match Self::append_log_entry_to(shard.clone(), addr.to_string(), en.entry.clone(), rep_log.clone()).await {
                                Err(ShardError::NotLeader) => {
                                    shard.set_is_leader(false).await;
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

    pub async fn process_write(&self, entry: Entry) -> Result<EntryWithNotifierReceiver> {
        let mut entry = entry.clone();
        entry.index = self.next_index.fetch_add(1, Ordering::Relaxed);

        let (tx, rx) = mpsc::channel(3);

        info!("ensure order, index: {}", entry.index);
        // self.order_keeper.ensure_order(entry.index).await?;

        // TODO: use group commit to improve performance
        self.rep_log
            .write()
            .await
            .append(vec![entry.clone()])
            .await
            .unwrap();

        if let Some(i) = self.input.as_ref() {
            i.send(EntryWithNotifierSender {
                entry: entry.clone(),
                sender: tx,
            })
            .await?;
        }

        // self.order_keeper.pass_order(entry.index).await;
        info!("pass order, index: {}", entry.index);

        Ok(EntryWithNotifierReceiver {
            entry,
            receiver: rx,
        })
    }

    pub async fn apply_entry(&self, entry: Entry) -> Result<()> {
        // self.order_keeper.ensure_order(entry.index).await?;

        // TODO: use group commit to improve performance
        self.rep_log
            .write()
            .await
            .append(vec![entry.clone()])
            .await
            .unwrap();

        // self.order_keeper.pass_order(entry.index).await;

        if entry.op == "put" {
            self.put(&entry.key, &entry.value).await.unwrap();
        }

        Ok(())
    }

    async fn append_log_entry_to(
        shard: Arc<Shard>,
        addr: String,
        en: Entry,
        replog: Arc<RwLock<Wal>>,
    ) -> std::result::Result<(), ShardError> {
        let shard_id = shard.get_shard_id();
        let client = CONNECTIONS
            .write()
            .await
            .get_conn(addr.clone())
            .await
            .unwrap();

        let request = Request::new(ShardAppendLogRequest {
            shard_id,
            leader_change_ts: Some(shard.get_leader_change_ts().into()),
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
                leader_change_ts: Some(shard.get_leader_change_ts().into()),
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
        shard: Arc<Shard>,
        client: Arc<RwLock<data_service_client::DataServiceClient<tonic::transport::Channel>>>,
    ) -> Result<u64 /*last_wal_index */> {
        let shard_id = shard.get_shard_id();

        let snap = shard.create_snapshot_iter().await.unwrap();

        // let last_wal_index = shard.read().await.get_last_index().await; // FIXME: keep atomic with above
        let last_wal_index = 0;

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

// #[async_trait]
// impl SnapShoter for Shard {
//     async fn create_snapshot(&self) -> Result<Vec<Vec<u8>>> {
//         let mut ret: Vec<Vec<u8>> = Default::default();

//         for kv in self.kv_data.read().await.iter() {
//             let mut key = kv.0.to_owned();
//             let mut value = kv.1.to_owned();

//             let mut item = vec![];
//             item.append(&mut key);
//             item.append(&mut vec!['\n' as u8]);
//             item.append(&mut value);

//             ret.push(item);
//         }

//         Ok(ret)
//     }

//     async fn install_snapshot(&mut self, piece: &[u8]) -> Result<()> {
//         let piece = piece.to_owned();
//         let mut ps = piece.split(|b| *b == '\n' as u8);

//         let key = ps.next().unwrap().to_owned();
//         let value = ps.next().unwrap().to_owned();
//         assert!(ps.next().is_none());

//         self.kv_store.write().await.insert(key, value);

//         Ok(())
//     }
// }
