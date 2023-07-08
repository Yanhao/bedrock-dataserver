use std::net::SocketAddr;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time;
use std::vec::Vec;

use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwapOption;
use futures_util::stream;
use num_bigint::{BigUint, ToBigUint};
use parking_lot;
use prost::Message;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::Request;
use tracing::{info, warn};

use idl_gen::replog_pb::Entry;
use idl_gen::service_pb::{
    data_service_client, shard_append_log_request, shard_install_snapshot_request,
    CreateShardRequest, ShardAppendLogRequest, ShardInstallSnapshotRequest, ShardMeta,
};

use crate::config::get_self_socket_addr;
use crate::ds_client::CONNECTIONS;
use crate::kv_store::{self, SledStore};
use crate::role::{Follower, Leader, Role};
use crate::shard::ShardError;
use crate::wal::{Wal, WalTrait};

const INPUT_CHANNEL_LEN: usize = 10240;
const SHARD_META_KEY: &'static str = "shard_meta";

#[derive(Debug)]
pub struct EntryWithNotifierSender {
    pub entry: Entry,
    pub sender: mpsc::Sender<Result<(), ShardError>>,
}

pub struct EntryWithNotifierReceiver {
    pub entry: Entry,
    receiver: mpsc::Receiver<Result<(), ShardError>>,
}

impl EntryWithNotifierReceiver {
    pub async fn wait_result(&mut self) -> Result<()> {
        let res = self
            .receiver
            .recv()
            .await
            .ok_or(anyhow!("wait result failed"))?;

        let _a = res?;

        Ok(())
    }
}

pub struct Shard {
    shard_meta: Arc<parking_lot::RwLock<ShardMeta>>,

    input: ArcSwapOption<mpsc::Sender<EntryWithNotifierSender>>,

    pub kv_store: kv_store::SledStore,
    pub replog: Arc<RwLock<Wal>>,

    write_lock: Mutex<()>,

    deleting: AtomicBool,
    installing_snapshot: AtomicBool,

    role: RwLock<Role>,
}

impl Shard {
    fn new(kv_store: kv_store::SledStore, meta: ShardMeta, wal: Wal) -> Self {
        let next_index = wal.next_index();
        info!("next_index: {next_index}");

        return Shard {
            shard_meta: Arc::new(parking_lot::RwLock::new(meta)),
            kv_store,
            replog: Arc::new(RwLock::new(wal)),
            deleting: false.into(),
            installing_snapshot: false.into(),
            input: None.into(),
            write_lock: Mutex::new(()),

            role: RwLock::new(Role::NotReady),
        };
    }

    pub async fn create_shard(req: &Request<CreateShardRequest>) -> Result<()> {
        let shard_id = req.get_ref().shard_id;

        let sled_db = kv_store::SledStore::create(shard_id).await.unwrap();
        let meta = ShardMeta {
            shard_id,
            is_leader: get_self_socket_addr().to_string() == req.get_ref().leader,
            create_ts: req.get_ref().create_ts.to_owned().unwrap().into(),
            leader: req.get_ref().leader.clone(),
            leader_change_ts: req.get_ref().leader_change_ts.to_owned().unwrap().into(),
            replicates_update_ts: req.get_ref().replica_update_ts.to_owned().unwrap().into(),

            replicates: req.get_ref().replicates.clone(),
            next_index: 0,

            min_key: req.get_ref().min_key.clone(),
            max_key: req.get_ref().max_key.clone(),
        };

        let meta_buf = {
            let mut buf = Vec::new();
            meta.encode(&mut buf).unwrap();
            buf
        };

        sled_db
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        Wal::create_wal_dir(shard_id).await?;

        Ok(())
    }

    pub async fn create_shard_for_split(
        shard_id: u64,
        leader: String,
        replicates: Vec<SocketAddr>,
        next_index: u64,
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

            next_index,

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

    pub async fn load_shard(shard_id: u64) -> Result<Self> {
        let sled_db = kv_store::SledStore::load(shard_id).await.unwrap();

        let meta_key = SHARD_META_KEY.as_bytes();
        let raw_meta = sled_db.kv_get(meta_key).await.unwrap();
        let meta = ShardMeta::decode(raw_meta.as_slice()).unwrap();
        let wal = Wal::load_wal_by_shard_id(shard_id).await.unwrap();
        info!("shard meta: {:?}", meta);

        Ok(Self::new(sled_db, meta, wal))
    }

    pub async fn remove_shard(shard_id: u64) -> Result<()> {
        SledStore::remove(shard_id).await?;
        Wal::remove_wal(shard_id).await?;

        Ok(())
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

    pub fn set_is_leader(&self, l: bool) {
        self.shard_meta.write().is_leader = l;
    }

    pub fn get_leader(&self) -> String {
        self.shard_meta.read().leader.clone().into()
    }

    pub fn update_leader_change_ts(&self, t: time::SystemTime) {
        self.shard_meta.write().leader_change_ts = Some(t.into());
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

    pub fn set_replicates(&self, replicates: &[SocketAddr]) -> Result<()> {
        self.shard_meta.write().replicates = replicates.iter().map(|s| s.to_string()).collect();

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
}

impl Shard {
    pub async fn get_next_index(&self) -> u64 {
        self.replog.read().await.next_index()
    }

    pub async fn reset_replog(&self, last_index: u64) -> Result<()> {
        // self.next_index.store(last_index, Ordering::Relaxed);

        self.replog.write().await.compact(last_index).await
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
    pub async fn process_write(
        &self,
        op: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<EntryWithNotifierReceiver> {
        let mut entry = Entry {
            op: op.into(),
            key: key.to_owned(),
            value: value.to_owned(),
            ..Default::default()
        };

        let lg = self.write_lock.lock().await;
        // TODO: use group commit to improve performance
        let index = self
            .replog
            .write()
            .await
            .append(vec![entry.clone()], false)
            .await
            .unwrap();
        info!("write at index: {index}");
        entry.index = index;

        let (tx, rx) = mpsc::channel(3);
        if let Some(i) = self.input.load().as_ref() {
            i.send(EntryWithNotifierSender {
                entry: entry.clone(),
                sender: tx,
            })
            .await?;
        }
        drop(lg);

        Ok(EntryWithNotifierReceiver {
            entry,
            receiver: rx,
        })
    }

    pub async fn append_log_entry(&self, entry: &Entry) -> Result<()> {
        let next_index = self.replog.read().await.next_index();
        info!("last_index: {next_index}, entry index: {}", entry.index);
        if next_index != entry.index {
            bail!(ShardError::LogIndexLag(next_index));
        }

        // TODO: use group commit to improve performance
        self.replog
            .write()
            .await
            .append(vec![entry.clone()], true)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn apply_entry(&self, entry: &Entry) -> Result<()> {
        if entry.op == "put" {
            self.kv_store
                .kv_set(&entry.key, &entry.value)
                .await
                .unwrap();
        }

        Ok(())
    }

    pub async fn append_log_entry_to(
        self: Arc<Shard>,
        addr: &str,
        en: Entry,
    ) -> std::result::Result<(), ShardError> {
        let shard_id = self.get_shard_id();
        let mut client = CONNECTIONS.get_client(addr).await.unwrap();

        let request = Request::new(ShardAppendLogRequest {
            shard_id,
            leader_change_ts: Some(self.get_leader_change_ts().into()),
            entries: vec![shard_append_log_request::Entry {
                op: en.op.clone(),
                index: en.index,
                key: en.key.clone(),
                value: en.value.clone(),
            }],
        });

        let resp = match client.shard_append_log(request).await {
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

        let mut next_index = resp.get_ref().next_index;
        info!("next_index from {}, {}", addr, next_index);

        for _i in 1..=3 {
            if next_index >= en.index {
                return Ok(());
            }

            let entries_res = self
                .replog
                .write()
                .await
                .entries(next_index, en.index, 1024)
                .await;

            if let Err(_) = entries_res {
                next_index = match Self::install_snapshot_to(self.clone(), client.clone()).await {
                    Err(_) => {
                        return Err(ShardError::FailedToAppendLog);
                    }
                    Ok(v) => v,
                };

                continue;
            }

            let ents = entries_res.unwrap();
            info!("entries length: {}", ents.len());

            let request = Request::new(ShardAppendLogRequest {
                shard_id,
                leader_change_ts: Some(self.get_leader_change_ts().into()),
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

            let resp = match client.shard_append_log(request).await {
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
        self: Arc<Shard>,
        mut client: data_service_client::DataServiceClient<tonic::transport::Channel>,
    ) -> Result<u64 /*last_wal_index */> {
        let snap = self.kv_store.create_snapshot_iter().await?;

        let next_wal_index = self.get_next_index().await; // FIXME: keep atomic with above

        let mut req_stream = vec![];
        for kv in snap.into_iter() {
            let (key, value) = (kv.0.to_owned(), kv.1.to_owned());

            info!("install_snapshot_to, key: {:?}, value: {:?}", key, value);

            req_stream.push(ShardInstallSnapshotRequest {
                shard_id: self.get_shard_id(),
                next_index: next_wal_index,
                entries: vec![shard_install_snapshot_request::Entry { key, value }],
            })
        }

        client
            .shard_install_snapshot(Request::new(stream::iter(req_stream)))
            .await?;
        // ref: https://github.com/hyperium/tonic/blob/master/examples/routeguide-tutorial.md

        return Ok(next_wal_index);
    }
}

impl Shard {
    pub async fn stop_role(&self) -> Result<()> {
        match &*self.role.read().await {
            Role::Leader(r) => r.stop().await?,
            Role::Follower(r) => r.stop().await?,
            _ => {}
        };

        *self.role.write().await = Role::NotReady;

        Ok(())
    }

    pub async fn switch_role_to_leader(self: Arc<Self>) -> Result<()> {
        if let Role::Leader(_) = &*self.role.read().await {
            return Ok(());
        }

        if let Role::Follower(r) = &*self.role.read().await {
            r.stop().await?;
        }

        let (input_tx, input_rx) = mpsc::channel::<EntryWithNotifierSender>(INPUT_CHANNEL_LEN);
        self.input.store(Some(Arc::new(input_tx)));

        let mut role = Leader::new();
        role.start(self.clone(), input_rx).await?;

        *self.role.write().await = Role::Leader(role);

        Ok(())
    }

    pub async fn switch_role_to_follower(&self) -> Result<()> {
        if let Role::Follower(_) = &*self.role.read().await {
            return Ok(());
        }

        if let Role::Leader(r) = &*self.role.read().await {
            r.stop().await?;
        }

        self.input.store(None);

        let mut role = Follower::new();
        role.start().await?;
        *self.role.write().await = Role::Follower(role);

        Ok(())
    }
}
