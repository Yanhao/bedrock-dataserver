use std::net::SocketAddr;
use std::ops::Range;
use std::time;
use std::vec::Vec;

use anyhow::{bail, Result};
use log::info;
use num_bigint::{BigUint, ToBigUint};
use prost::Message;
use tokio::sync::RwLock;
use tonic::Request;

use crate::kv_store::{self, SledStore};
use crate::shard::error;

use dataserver::service_pb::CreateShardRequest;
use dataserver::service_pb::ShardMeta;

const SHARD_META_KEY: &'static str = "shard_meta";

pub struct Shard {
    shard_id: u64,
    kv_store: Option<RwLock<kv_store::SledStore>>,
    shard_meta: ShardMeta,
}

impl Shard {
    pub async fn load_shard(shard_id: u64) -> Result<Self> {
        let sled_kv = kv_store::SledStore::load(shard_id).await.unwrap();

        let meta_key: &[u8] = SHARD_META_KEY.as_bytes();
        let raw_meta = sled_kv.kv_get(meta_key).await.unwrap();
        let meta = ShardMeta::decode(&*raw_meta).unwrap();

        Ok(Self {
            shard_id,
            kv_store: Some(RwLock::new(sled_kv)),
            shard_meta: meta,
        })
    }

    pub async fn create_shard(req: &Request<CreateShardRequest>) -> Self {
        let mut sled_kv = kv_store::SledStore::create(req.get_ref().shard_id)
            .await
            .unwrap();

        let meta = ShardMeta {
            shard_id: req.get_ref().shard_id,
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
        sled_kv
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        Self::new(req.get_ref().shard_id, sled_kv, meta)
    }

    pub async fn create_shard_for_split(
        shard_id: u64,
        leader: String,
        replicates: Vec<SocketAddr>,
        last_wal_index: u64,
        key_range: Range<Vec<u8>>,
    ) -> Self {
        let mut sled_kv = kv_store::SledStore::create(shard_id).await.unwrap();

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
        sled_kv
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        Self::new(shard_id, sled_kv, meta)
    }

    pub async fn remove_shard(&mut self) -> Result<()> {
        self.kv_store = None;
        SledStore::remove(self.shard_id).await.unwrap();

        Ok(())
    }

    fn new(shard_id: u64, kv_store: kv_store::SledStore, meta: ShardMeta) -> Self {
        return Shard {
            shard_id,
            kv_store: Some(RwLock::new(kv_store)),
            shard_meta: meta,
        };
    }

    pub async fn clear_data(&self) {
        todo!()
    }

    pub fn get_shard_id(&self) -> u64 {
        self.shard_id
    }

    pub fn get_replicates(&self) -> Vec<SocketAddr> {
        self.shard_meta
            .replicates
            .iter()
            .map(|a| a.parse().unwrap())
            .collect()
    }

    pub fn get_replicates_strings(&self) -> Vec<String> {
        self.shard_meta.replicates.clone()
    }

    pub async fn set_replicates(&mut self, replicates: &[SocketAddr]) -> Result<()> {
        self.shard_meta.replicates = replicates.iter().map(|s| s.to_string()).collect();
        self.save_meta().await.unwrap();

        Ok(())
    }

    pub async fn set_is_leader(&mut self, l: bool) {
        self.shard_meta.is_leader = l;
        self.save_meta().await.unwrap();
    }

    pub fn is_leader(&self) -> bool {
        self.shard_meta.is_leader
    }

    pub fn get_leader(&self) -> String {
        self.shard_meta.leader.clone().into()
    }

    pub async fn update_leader_change_ts(&mut self, t: time::SystemTime) {
        self.shard_meta.leader_change_ts = Some(t.into());
        self.save_meta().await.unwrap();
    }

    pub fn get_leader_change_ts(&self) -> time::SystemTime {
        self.shard_meta.leader_change_ts.clone().unwrap().into()
    }

    pub fn get_last_wal_index(&self) -> u64 {
        self.shard_meta.last_wal_index
    }

    pub fn middle_key(&self) -> Vec<u8> {
        let min = BigUint::from_bytes_be(&self.shard_meta.min_key);
        let max = BigUint::from_bytes_be(&self.shard_meta.max_key);

        let middle = min.clone() + (max - min) / 2.to_biguint().unwrap();

        middle.to_bytes_be()
    }

    pub fn min_key(&self) -> Vec<u8> {
        self.shard_meta.min_key.clone()
    }

    pub fn max_key(&self) -> Vec<u8> {
        self.shard_meta.max_key.clone()
    }

    pub async fn save_meta(&mut self) -> Result<()> {
        let mut meta_buf = Vec::new();
        self.shard_meta.encode(&mut meta_buf).unwrap();
        self.kv_store
            .as_ref()
            .unwrap()
            .write()
            .await
            .kv_set(SHARD_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn create_split_iter(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        Ok(kv_store::StoreIter {
            iter: self
                .kv_store
                .as_ref()
                .unwrap()
                .read()
                .await
                .db
                .range(start_key..end_key),
        })
    }

    pub async fn create_snapshot_iter(&self) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        Ok(kv_store::StoreIter {
            iter: self.kv_store.as_ref().unwrap().read().await.db.iter(),
        })
    }

    pub async fn kv_install_snapshot(&mut self, piece: &[u8]) -> Result<()> {
        let piece = piece.to_owned();
        let mut ps = piece.split(|b| *b == '\n' as u8);

        let key = ps.next().unwrap().to_owned();
        let value = ps.next().unwrap().to_owned();
        assert!(ps.next().is_none());

        self.kv_store
            .as_ref()
            .unwrap()
            .write()
            .await
            .kv_set(&key, &value)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        info!("shard put: key={:?}, value={:?}", key, value,);

        self.kv_store
            .as_ref()
            .unwrap()
            .write()
            .await
            .kv_set(key, value)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let kv = self.kv_store.as_ref().unwrap().read().await;

        let value = match kv.kv_get(key.as_ref()).await {
            Err(_) => {
                bail!(error::ShardError::NoSuchKey)
            }
            Ok(v) => v,
        };

        // info!(
        //     "shard get: key={}, value={}",
        //     from_utf8(key).unwrap(),
        //     from_utf8(value.as_slice()).unwrap()
        // );

        Ok(value.to_vec())
    }

    pub async fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.kv_store
            .as_ref()
            .unwrap()
            .write()
            .await
            .kv_delete_range(start_key, end_key)
            .await
            .unwrap();

        Ok(())
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
