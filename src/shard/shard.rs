use std::str::from_utf8;
use std::time;
use std::vec::Vec;
use std::{collections::HashMap, net::SocketAddr};

use anyhow::{bail, Result};
use async_trait::async_trait;
use log::info;
use prost::Message;
use tokio::sync::RwLock;
use tonic::Request;

use crate::kv_store;
use crate::shard::error;
use crate::shard::snapshoter::SnapShoter;

use dataserver::service_pb::CreateShardRequest;
use dataserver::service_pb::ShardMeta;

const SHART_META_KEY: &'static str = "shard_meta";

pub struct Shard {
    pub kv_store: RwLock<kv_store::SledStore>,
    pub shard_meta: ShardMeta,
}

impl Shard {
    pub async fn load_shard(shard_id: u64) -> Self {
        let sled_kv = kv_store::SledStore::load(shard_id).await.unwrap();

        let meta_key: &[u8] = SHART_META_KEY.as_bytes();
        let raw_meta = sled_kv.kv_get(meta_key).await.unwrap();
        let meta = ShardMeta::decode(&*raw_meta).unwrap();

        Self {
            kv_store: RwLock::new(sled_kv),
            shard_meta: meta,
        }
    }

    pub async fn create_shard(req: &Request<CreateShardRequest>) -> Self {
        let mut sled_kv = kv_store::SledStore::create(req.get_ref().shard_id)
            .await
            .unwrap();
        let meta = ShardMeta {
            shard_id: req.get_ref().shard_id,
            is_leader: false,
            storage_id: req.get_ref().storage_id,
            create_ts: req.get_ref().create_ts.to_owned().unwrap().into(),
            leader: req.get_ref().leader.clone(),
            leader_change_ts: req.get_ref().leader_change_ts.to_owned().unwrap().into(),
            replicates_update_ts: req.get_ref().replica_update_ts.to_owned().unwrap().into(),

            replicates: req.get_ref().replicates.clone(),
            last_wal_index: 0,
        };

        let mut meta_buf = Vec::new();
        meta.encode(&mut meta_buf).unwrap();
        sled_kv
            .kv_set(SHART_META_KEY.as_bytes(), &meta_buf)
            .await
            .unwrap();

        let new_shard = Self {
            kv_store: RwLock::new(sled_kv),
            shard_meta: meta,
        };

        new_shard
    }

    fn new(shard_id: u64, storage_id: u64, kv_store: kv_store::SledStore, meta: ShardMeta) -> Self {
        return Shard {
            kv_store: RwLock::new(kv_store),

            shard_meta: meta,
        };
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // let key = key.to_vec();
        // let value = value.to_vec();

        info!(
            "shard put: key={:?}, value={:?}",
            key,
            value,
            // from_utf8(&key).unwrap(),
            // from_utf8(&value).unwrap()
        );

        // self.kv_data.write().await.insert(key, value);

        self.kv_store
            .write()
            .await
            .kv_set(key, value)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        // let kv = self.kv_data.read().await;
        let kv = self.kv_store.read().await;

        let value = match kv.kv_get(key.as_ref()).await {
            Err(_) => {
                bail!(error::ShardError::NoSuchKey)
            }
            Ok(v) => v,
        };

        info!(
            "shard get: key={}, value={}",
            from_utf8(key).unwrap(),
            from_utf8(value.as_slice()).unwrap()
        );

        Ok(value.to_vec())
    }

    pub async fn clear_data(&self) {
        todo!()
    }

    pub fn get_shard_id(&self) -> u64 {
        todo!()
    }

    pub fn get_replicates(&self) -> Vec<SocketAddr> {
        todo!()
        // self.replicates.clone()
    }

    pub fn set_replicates(&mut self, replicats: &[SocketAddr]) -> Result<()> {
        todo!()
        // self.replicates = replicats.to_owned();
        // Ok(())
    }

    pub fn set_is_leader(&mut self, l: bool) {
        todo!()
        // self.is_leader = l
    }

    pub fn is_leader(&self) -> bool {
        todo!()
    }

    pub fn update_leader_change_ts(&mut self, t: time::SystemTime) {
        todo!()
        // self.leader_change_ts = t;
    }

    pub fn get_leader_change_ts(&self) -> time::SystemTime {
        todo!()
    }

    pub async fn kv_install_snapshot(&mut self, piece: &[u8]) -> Result<()> {
        let piece = piece.to_owned();
        let mut ps = piece.split(|b| *b == '\n' as u8);

        let key = ps.next().unwrap().to_owned();
        let value = ps.next().unwrap().to_owned();
        assert!(ps.next().is_none());

        self.kv_store
            .write()
            .await
            .kv_set(&key, &value)
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
