use std::net::SocketAddr;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::vec::Vec;

use anyhow::{bail, Result};
use arc_swap::ArcSwapOption;
use bytes::{Bytes, BytesMut};
use futures::executor::block_on;
use futures_util::stream;
use num_bigint::{BigUint, ToBigUint};
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::Request;
use tracing::{debug, error, info, warn};

use idl_gen::replog_pb::{Entry, EntryItem};
use idl_gen::service_pb::{
    data_service_client, shard_append_log_request, shard_install_snapshot_request,
    ShardAppendLogRequest, ShardInstallSnapshotRequest, ShardMeta,
};

use crate::config::CONFIG;
use crate::ds_client::CONNECTIONS;
use crate::kv_store::{KvStore, SledStore};
use crate::metadata::{Meta, METADATA};
use crate::role::{Follower, Leader, Role};
use crate::shard::ShardError;
use crate::utils::R;
use crate::wal::Wal;

use super::Operation;

const INPUT_CHANNEL_LEN: usize = 10240;
const SHARD_DATA_KEY_PREFIX: &str = "/data/";

#[derive(Debug)]
pub struct EntryWithNotifierSender {
    pub entry: Entry,
    pub sender: tokio::sync::broadcast::Sender<Result<(), ShardError>>,
}

pub struct Shard {
    shard_meta: Arc<parking_lot::RwLock<ShardMeta>>,

    input: ArcSwapOption<mpsc::Sender<EntryWithNotifierSender>>,

    kv_store: Option<SledStore>,
    replog: Option<Wal>,

    write_lock: Mutex<()>,
    membership_update_lock: parking_lot::Mutex<()>,

    deleting: AtomicBool,
    installing_snapshot: AtomicBool,

    role: RwLock<Role>,
}

impl Drop for Shard {
    fn drop(&mut self) {
        let _ = self.kv_store.take();
        let _ = self.replog.take();

        if !self.is_deleting() {
            return;
        }

        let shard_id = self.shard_id();
        block_on(async move {
            let _ = Shard::remove_shard(shard_id).await;
        });
    }
}

impl Shard {
    async fn new(kv_store: SledStore, meta: ShardMeta, wal: Wal) -> Self {
        let next_index = wal.next_index().await;
        info!("shard 0x{:016x}, next_index: {next_index}", meta.shard_id);

        Shard {
            shard_meta: Arc::new(parking_lot::RwLock::new(meta)),
            kv_store: Some(kv_store.clone()),
            replog: Some(wal),
            deleting: false.into(),
            installing_snapshot: false.into(),
            input: None.into(),
            write_lock: Mutex::new(()),
            membership_update_lock: parking_lot::Mutex::new(()),

            role: RwLock::new(Role::NotReady),
        }
    }

    pub async fn create(
        shard_id: u64,
        leader: SocketAddr,
        leader_change_ts: Option<prost_types::Timestamp>,
        replicates: Vec<String>,
        replicates_update_ts: Option<prost_types::Timestamp>,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
    ) -> Result<()> {
        SledStore::create(shard_id).await?;
        Wal::create(shard_id).await?;

        METADATA.write().put_shard(
            shard_id,
            ShardMeta {
                shard_id,
                create_ts: Some(std::time::SystemTime::now().into()),
                leader: leader.to_string(),
                is_leader: CONFIG.read().get_self_socket_addr() == leader,
                leader_change_ts,

                replicates,
                replicates_update_ts,
                next_index: 0,

                min_key,
                max_key,
            },
        )?;

        Ok(())
    }

    pub async fn create_for_split(
        shard_id: u64,
        leader: String,
        replicates: Vec<SocketAddr>,
        next_index: u64,
        key_range: Range<Vec<u8>>,
    ) -> Result<Self> {
        SledStore::create(shard_id).await?;
        Wal::create(shard_id).await?;

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

        METADATA.write().put_shard(shard_id, meta.clone())?;

        Ok(Self::new(
            SledStore::load(shard_id).await?,
            meta,
            Wal::load_wal_by_shard_id(shard_id).await?,
        )
        .await)
    }

    pub async fn load_shard(shard_id: u64) -> Result<Self> {
        let sled_db = SledStore::load(shard_id).await.unwrap();
        let wal = Wal::load_wal_by_shard_id(shard_id).await?;

        let meta = METADATA.read().get_shard(shard_id)?;
        info!("shard meta: {:?}", meta);

        Ok(Self::new(sled_db, meta, wal).await)
    }

    async fn remove_shard(shard_id: u64) -> Result<()> {
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
    pub fn is_leader(&self) -> bool {
        self.shard_meta.read().is_leader
    }
    pub fn leader_change_ts(&self) -> std::time::SystemTime {
        self.shard_meta
            .read()
            .leader_change_ts
            .clone()
            .unwrap()
            .try_into()
            .unwrap()
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
    pub fn mark_deleting(&self) {
        self.deleting.store(true, Ordering::Relaxed);
    }
    pub fn mark_installing_snapshot(&self, i: bool) {
        self.installing_snapshot.store(i, Ordering::Relaxed);
    }

    pub fn update_membership(
        &self,
        is_leader: bool,
        leader_update_ts: std::time::SystemTime,
        replicates: Option<&[SocketAddr]>,
    ) -> Result<()> {
        let _lg = self.membership_update_lock.lock();
        let mut meta = self.shard_meta.read().clone();

        meta.is_leader = is_leader;
        meta.leader_change_ts = Some(leader_update_ts.into());
        if let Some(replicates) = replicates {
            meta.replicates = replicates.iter().map(|s| s.to_string()).collect();
        }
        self.save_meta(&meta)?;

        *self.shard_meta.write() = meta;

        Ok(())
    }

    fn save_meta(&self, meta: &ShardMeta) -> Result<()> {
        METADATA.write().put_shard(self.shard_id(), meta.clone())?;

        Ok(())
    }

    pub fn get_kv_store(&self) -> &(impl KvStore + Send) {
        self.kv_store.as_ref().unwrap()
    }

    pub async fn next_index(&self) -> u64 {
        self.replog.r().next_index().await
    }
    pub async fn reset_replog(&self, next_index: u64) -> Result<()> {
        self.replog.r().compact(next_index).await
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

    pub fn data_key(key: &[u8]) -> Vec<u8> {
        let mut b = BytesMut::new();
        b.extend_from_slice(SHARD_DATA_KEY_PREFIX.as_bytes());
        b.extend_from_slice(&key);
        b.freeze().into()
    }

    pub fn is_key_within_shard(&self, key: &[u8]) -> bool {
        let min = BigUint::from_bytes_be(&self.shard_meta.read().min_key);
        let max = BigUint::from_bytes_be(&self.shard_meta.read().max_key);

        let key = BigUint::from_bytes_be(key);

        min <= key && key < max
    }

    pub async fn create_split_iter<'a, 'b>(
        &'a self,
        start_key: &'b [u8],
        end_key: &'b [u8],
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + 'a> {
        Ok(self.get_kv_store().kv_range(
            Bytes::from(start_key.to_vec()),
            Bytes::from(end_key.to_vec()),
        ))
    }
}

impl Shard {
    pub async fn process_write(
        &self,
        op: Operation,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Bytes>> {
        let entry = Entry {
            index: 0,
            items: vec![EntryItem {
                op: op.to_string(),
                key: key.to_owned(),
                value: value.to_owned(),
            }],
        };

        let _lg = self.write_lock.lock().await;

        let (is_group_leader, merger_append_ret) =
            self.replog.r().append_by_leader(entry.clone()).await?;
        // info!("write at index: {index}");
        // entry.index = index;

        let tx = merger_append_ret.broadcaster;
        let mut rx = tx.subscribe();

        if is_group_leader {
            if let Some(i) = self.input.load().as_ref() {
                i.send(EntryWithNotifierSender {
                    entry: merger_append_ret.entry.clone(),
                    sender: tx,
                })
                .await?;
            }
        }

        debug!("start wait result");
        rx.recv().await?.inspect_err(|e| {
            error!(
                msg = "write wait result failed.",
                err = ?e,
                op = "kv_set",
                key = unsafe { String::from_utf8_unchecked(key.to_vec()) },
            )
        })?;
        debug!(" wait result successed");

        if is_group_leader {
            return self.apply_entry(&merger_append_ret.entry).await;
        }

        Ok(None)
    }

    pub async fn append_log_entry(&self, entry: &Entry) -> Result<()> {
        let next_index = self.replog.r().next_index().await;
        info!("last_index: {next_index}, entry index: {}", entry.index);
        if next_index != entry.index {
            bail!(ShardError::LogIndexLag(next_index));
        }

        self.replog.r().append_by_follower(entry.clone()).await?;

        Ok(())
    }

    pub async fn apply_entry(&self, entry: &Entry) -> Result<Option<Bytes>> {
        for ei in entry.items.iter() {
            let op: Operation = ei.clone().op.into();

            match op {
                Operation::Noop => unreachable!(),
                Operation::Set => {
                    self.get_kv_store()
                        .kv_set(Shard::data_key(&ei.key).into(), ei.value.clone().into())?;
                }
                Operation::SetNs => {
                    if self
                        .get_kv_store()
                        .kv_get(Shard::data_key(&ei.key).into())?
                        .is_none()
                    {
                        self.get_kv_store()
                            .kv_set(Shard::data_key(&ei.key).into(), ei.value.clone().into())?;
                    }
                }
                Operation::Del => {
                    return self
                        .get_kv_store()
                        .kv_delete(Shard::data_key(&ei.key).into());
                }
            }
        }

        Ok(None)
    }

    pub async fn append_log_entry_to_addr(
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
                index: en.index,
                items: en
                    .items
                    .into_iter()
                    .map(|ei| shard_append_log_request::EntryItem {
                        op: ei.op.clone(),
                        key: ei.key.clone(),
                        value: ei.value.clone(),
                    })
                    .collect(),
            }],
        });

        let resp = client
            .shard_append_log(request)
            .await
            .inspect_err(|e| warn!("failed to  append_log, err {}", e))
            .map_err(|_| ShardError::FailedToAppendLog)?;

        if resp.get_ref().is_old_leader {
            warn!("failed to append log, not leader");
            return Err(ShardError::NotLeaderWithTs(
                resp.get_ref()
                    .last_leader_change_ts
                    .r()
                    .clone()
                    .try_into()
                    .unwrap(),
            ));
        }

        let mut next_index = resp.get_ref().next_index;
        info!("next_index from {}, {}", addr, next_index);

        for _i in 1..=3 {
            if next_index >= en.index {
                return Ok(());
            }

            let entries_res = self.replog.r().entries(next_index, en.index, 1024).await;

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
                            index: ent.index,
                            items: ent
                                .items
                                .iter()
                                .map(|ei| shard_append_log_request::EntryItem {
                                    op: ei.op.clone(),
                                    key: ei.key.clone(),
                                    value: ei.value.clone(),
                                })
                                .collect(),
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
        let snap = self.get_kv_store().take_snapshot()?;
        let next_wal_index = self.next_index().await; // FIXME: keep atomic with above line

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
