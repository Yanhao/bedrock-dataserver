use std::net::SocketAddr;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::vec::Vec;

use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures_util::stream;
use num_bigint::{BigUint, ToBigUint};
use prost::Message;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::Request;
use tracing::{error, info, warn};

use idl_gen::replog_pb::Entry;
use idl_gen::service_pb::{
    data_service_client, shard_append_log_request, shard_install_snapshot_request,
    CreateShardRequest, ShardAppendLogRequest, ShardInstallSnapshotRequest, ShardMeta,
};

use crate::config::CONFIG;
use crate::ds_client::CONNECTIONS;
use crate::kv_store::{self, KvStore, SledStore};
use crate::metadata::{Meta, METADATA};
use crate::role::{Follower, Leader, Role};
use crate::shard::ShardError;
use crate::wal::{Wal, WalTrait};

use super::Operation;

const INPUT_CHANNEL_LEN: usize = 10240;
const SHARD_META_KEY: &str = "shard_meta";

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

        res?;

        Ok(())
    }
}

pub struct Shard {
    shard_meta: Arc<parking_lot::RwLock<ShardMeta>>,

    input: ArcSwapOption<mpsc::Sender<EntryWithNotifierSender>>,

    pub kv_store: SledStore,
    pub replog: Arc<RwLock<Wal>>,

    write_lock: Mutex<()>,

    deleting: AtomicBool,
    installing_snapshot: AtomicBool,

    role: RwLock<Role>,
}

impl Shard {
    fn new(kv_store: SledStore, meta: ShardMeta, wal: Wal) -> Self {
        let next_index = wal.next_index();
        info!("shard 0x{:016x}, next_index: {next_index}", meta.shard_id);

        Shard {
            shard_meta: Arc::new(parking_lot::RwLock::new(meta)),
            kv_store: kv_store.clone(),
            replog: Arc::new(RwLock::new(wal)),
            deleting: false.into(),
            installing_snapshot: false.into(),
            input: None.into(),
            write_lock: Mutex::new(()),

            role: RwLock::new(Role::NotReady),
        }
    }

    pub async fn create_shard(req: &Request<CreateShardRequest>) -> Result<()> {
        let shard_id = req.get_ref().shard_id;

        SledStore::create(shard_id).await?;
        let sled_db = SledStore::load(shard_id).await?;

        let meta = ShardMeta {
            shard_id,
            is_leader: CONFIG.read().get_self_socket_addr().to_string() == req.get_ref().leader,
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

        sled_db.kv_set(SHARD_META_KEY.into(), meta_buf.into())?;

        Wal::create_wal_dir(shard_id).await?;

        METADATA.write().put_shard(shard_id, meta)?;

        Ok(())
    }

    pub async fn create_shard_for_split(
        shard_id: u64,
        leader: String,
        replicates: Vec<SocketAddr>,
        next_index: u64,
        key_range: Range<Vec<u8>>,
    ) -> Result<Self> {
        SledStore::create(shard_id).await?;
        let sled_db = SledStore::load(shard_id).await?;

        let meta = ShardMeta {
            shard_id,
            is_leader: false,
            create_ts: Some(std::time::SystemTime::now().into()),
            replicates_update_ts: Some(std::time::SystemTime::now().into()),
            leader_change_ts: Some(std::time::SystemTime::now().into()),

            leader,
            replicates: replicates.into_iter().map(|r| r.to_string()).collect(),

            next_index,

            min_key: key_range.start,
            max_key: key_range.end,
        };

        let mut meta_buf = Vec::new();
        meta.encode(&mut meta_buf).unwrap();
        sled_db.kv_set(SHARD_META_KEY.into(), meta_buf.into())?;

        METADATA.write().put_shard(shard_id, meta.clone())?;

        Wal::create_wal_dir(shard_id).await?;
        let wal = Wal::load_wal_by_shard_id(shard_id).await?;

        Ok(Self::new(sled_db, meta, wal))
    }

    pub async fn load_shard(shard_id: u64) -> Result<Self> {
        let sled_db = SledStore::load(shard_id).await.unwrap();

        let raw_meta = sled_db.kv_get(SHARD_META_KEY.into())?.unwrap();
        let meta = ShardMeta::decode(raw_meta).unwrap();
        let wal = Wal::load_wal_by_shard_id(shard_id).await?;
        info!("shard meta: {:?}", meta);

        Ok(Self::new(sled_db, meta, wal))
    }

    pub async fn remove_shard(shard_id: u64) -> Result<()> {
        SledStore::remove(shard_id).await?;
        Wal::remove_wal(shard_id).await?;

        METADATA.write().remove_shard(shard_id)?;

        Ok(())
    }
}

impl Shard {
    pub fn shard_id(&self) -> u64 {
        self.shard_meta.read().shard_id
    }
    pub fn leader(&self) -> String {
        self.shard_meta.read().leader.clone()
    }
    pub fn leader_change_ts(&self) -> std::time::SystemTime {
        self.shard_meta
            .read()
            .leader_change_ts
            .clone()
            .unwrap()
            .into()
    }
    pub fn replicates(&self) -> Vec<SocketAddr> {
        self.shard_meta
            .read()
            .replicates
            .iter()
            .map(|a| a.parse().unwrap())
            .collect()
    }
    pub fn is_deleting(&self) -> bool {
        self.deleting.load(Ordering::Relaxed)
    }
    pub fn is_installing_snapshot(&self) -> bool {
        self.installing_snapshot.load(Ordering::Relaxed)
    }
    pub fn is_leader(&self) -> bool {
        self.shard_meta.read().is_leader
    }

    pub fn mark_deleting(&self) {
        self.deleting.store(true, Ordering::Relaxed)
    }
    pub fn mark_installing_snapshot(&self, i: bool) {
        self.installing_snapshot.store(i, Ordering::Relaxed);
    }
    pub fn set_is_leader(&self, l: bool) {
        self.shard_meta.write().is_leader = l;
    }
    pub fn update_leader_change_ts(&self, t: std::time::SystemTime) {
        self.shard_meta.write().leader_change_ts = Some(t.into());
    }
    pub fn set_replicates(&self, replicates: &[SocketAddr]) {
        self.shard_meta.write().replicates = replicates.iter().map(|s| s.to_string()).collect();
    }

    pub async fn next_index(&self) -> u64 {
        self.replog.read().await.next_index()
    }
    pub async fn reset_replog(&self, next_index: u64) -> Result<()> {
        self.replog.write().await.compact(next_index).await
    }

    pub async fn save_meta(&self) -> Result<()> {
        let mut meta_buf = Vec::new();
        self.shard_meta.read().encode(&mut meta_buf).unwrap();
        self.kv_store
            .kv_set(SHARD_META_KEY.into(), meta_buf.to_vec().into())?;

        METADATA
            .write()
            .put_shard(self.shard_id(), self.shard_meta.read().clone())?;

        Ok(())
    }
}

impl Shard {
    pub fn middle_key(&self) -> Vec<u8> {
        let min = BigUint::from_bytes_be(&self.shard_meta.read().min_key);
        let max = BigUint::from_bytes_be(&self.shard_meta.read().max_key);

        let middle = (max + min) / 2.to_biguint().unwrap();

        middle.to_bytes_be()
    }

    pub fn max_key(&self) -> Vec<u8> {
        self.shard_meta.read().max_key.clone()
    }

    pub fn is_key_within_shard(&self, key: &[u8]) -> bool {
        let min = BigUint::from_bytes_be(&self.shard_meta.read().min_key);
        let max = BigUint::from_bytes_be(&self.shard_meta.read().max_key);

        let key = BigUint::from_bytes_be(key);

        min <= key && key < max
    }

    pub async fn create_split_iter(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)>> {
        Ok(kv_store::StoreIter {
            iter: self.kv_store.db.range(start_key..end_key),
        })
    }
}

impl Shard {
    pub async fn process_write(
        &self,
        op: Operation,
        key: &[u8],
        value: &[u8],
    ) -> Result<EntryWithNotifierReceiver> {
        let mut entry = Entry {
            op: op.to_string(),
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

    pub async fn apply_entry(&self, entry: &Entry) -> Result<Option<Bytes>> {
        let op: Operation = entry.op.clone().into();

        match op {
            Operation::Noop => unreachable!(),
            Operation::Set => {
                self.kv_store
                    .kv_set(entry.key.clone().into(), entry.value.clone().into())?;
            }
            Operation::SetNs => {
                if self.kv_store.kv_get(entry.key.clone().into())?.is_none() {
                    self.kv_store
                        .kv_set(entry.key.clone().into(), entry.value.clone().into())?;
                }
            }
            Operation::Del => {
                return self.kv_store.kv_delete(entry.key.clone().into());
            }
        }

        Ok(None)
    }

    pub async fn append_log_entry_to(
        self: Arc<Shard>,
        addr: &str,
        en: Entry,
    ) -> std::result::Result<(), ShardError> {
        let shard_id = self.shard_id();
        let mut client = CONNECTIONS
            .get_client(addr)
            .await
            .map_err(|_| ShardError::FailedToAppendLog)?;

        let request = Request::new(ShardAppendLogRequest {
            shard_id,
            leader_change_ts: Some(self.leader_change_ts().into()),
            entries: vec![shard_append_log_request::Entry {
                op: en.op.clone(),
                index: en.index,
                key: en.key.clone(),
                value: en.value.clone(),
            }],
        });

        let resp = client
            .shard_append_log(request)
            .await
            .inspect_err(|e| warn!("failed to  append_log, err {}", e))
            .map_err(|_| ShardError::FailedToAppendLog)?;

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

            if let Err(e) = entries_res {
                error!("call replog.entries failed, err: {e}");
                next_index = Self::install_snapshot_to(self.clone(), client.clone(), addr)
                    .await
                    .map_err(|_| ShardError::FailedToAppendLog)?;

                continue;
            }

            let ents = entries_res.unwrap();
            info!("entries length: {}", ents.len());

            let request = Request::new(ShardAppendLogRequest {
                shard_id,
                leader_change_ts: Some(self.leader_change_ts().into()),
                entries: ents
                    .iter()
                    .map(|ent| {
                        info!("append_log_to ent index: {}", ent.index);

                        shard_append_log_request::Entry {
                            op: ent.op.clone(),
                            index: ent.index,
                            key: ent.key.clone(),
                            value: ent.value.clone(),
                        }
                    })
                    .collect(),
            });

            let resp = client
                .shard_append_log(request)
                .await
                .inspect_err(|e| warn!("failed to append_log, err: {}", e))
                .map_err(|_| ShardError::FailedToAppendLog)?;

            if resp.get_ref().is_old_leader {
                return Err(ShardError::NotLeader);
            }
        }

        Err(ShardError::FailedToAppendLog)
    }

    async fn install_snapshot_to(
        self: Arc<Shard>,
        mut client: data_service_client::DataServiceClient<tonic::transport::Channel>,
        addr: &str,
    ) -> Result<u64 /*last_wal_index */> {
        let snap = self.kv_store.take_snapshot()?;

        let next_wal_index = self.next_index().await; // FIXME: keep atomic with above

        let mut req_stream = vec![];
        for kv in snap.into_iter() {
            let (key, value) = (kv.0.to_owned(), kv.1.to_owned());

            info!("install_snapshot_to, key: {:?}, value: {:?}", key, value);

            req_stream.push(ShardInstallSnapshotRequest {
                shard_id: self.shard_id(),
                next_index: next_wal_index,
                entries: vec![shard_install_snapshot_request::Entry {
                    key: key.into(),
                    value: value.to_vec(),
                }],
            })
        }

        client
            .shard_install_snapshot(Request::new(stream::iter(req_stream)))
            .await
            .inspect_err(|e| error!("install snapshot to {addr} failed, err: {e}",))?;
        // ref: https://github.com/hyperium/tonic/blob/master/examples/routeguide-tutorial.md

        Ok(next_wal_index)
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

    pub async fn start_role(self: Arc<Self>) -> Result<()> {
        if self.is_leader() {
            self.switch_role_to_leader().await?;
        } else {
            self.switch_role_to_follower().await?;
        }

        Ok(())
    }
}
