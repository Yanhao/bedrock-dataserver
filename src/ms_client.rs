use std::collections::HashMap;
use std::str::pattern::Pattern;
use std::sync::Arc;

use anyhow::{ensure, Result};
use arc_swap::access::Access;
use arc_swap::ArcSwapOption;
use futures_util::stream;
use idl_gen::metaserver_pb::meta_service_client::MetaServiceClient;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use idl_gen::metaserver_pb::sync_shard_in_data_server_request::SyncShardInfo;
use idl_gen::metaserver_pb::{HeartBeatRequest, InfoRequest, SyncShardInDataServerRequest};
use idl_gen::service_pb::ShardMeta;

use crate::config::CONFIG;
use crate::load_status::LOAD_STATUS;
use crate::metadata::METADATA;
use crate::metadata::{Meta, ShardMetaIter};
use crate::utils::{A, R};

pub static MS_CLIENT: Lazy<ArcSwapOption<MsClient>> = Lazy::new(|| None.into());

struct SyncShardIter<T>
where
    T: Iterator<Item = ShardMeta>,
{
    last: bool,
    sync_ts: prost_types::Timestamp,
    shards: T,
}

impl<T> SyncShardIter<T>
where
    T: Iterator<Item = ShardMeta>,
{
    fn new(t: T) -> Self {
        Self {
            last: false,
            sync_ts: std::time::SystemTime::now().into(),
            shards: t,
        }
    }
}

impl Iterator for SyncShardIter<ShardMetaIter> {
    type Item = SyncShardInDataServerRequest;

    fn next(&mut self) -> Option<Self::Item> {
        if self.last {
            return None;
        }

        let shard = self.shards.next();

        let mut ret = SyncShardInDataServerRequest {
            dataserver_addr: CONFIG.read().get_self_socket_addr().to_string(),
            sync_ts: Some(self.sync_ts.clone()),
            shards: vec![],
            is_last_piece: if shard.is_none() {
                self.last = true;
                true
            } else {
                false
            },
        };

        if let Some(s) = shard {
            ret.shards.push(SyncShardInfo {
                shard_id: s.shard_id,
                replica_update_ts: s.replicates_update_ts,
                create_ts: s.create_ts,
                leader: s.leader,
                leader_change_ts: s.leader_change_ts,
            });
        }

        Some(ret)
    }
}

pub struct MsClient {
    leader_conn: Arc<ArcSwapOption<(String, MetaServiceClient<Channel>)>>,
    follower_conns: Arc<parking_lot::RwLock<HashMap<String, MetaServiceClient<Channel>>>>,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl MsClient {
    pub fn new() -> Self {
        Self {
            leader_conn: Arc::new(None.into()),
            follower_conns: Arc::new(parking_lot::RwLock::new(HashMap::new())),

            stop_ch: None,
        }
    }

    async fn update_ms_conns(
        leader_conn: Arc<ArcSwapOption<(String, MetaServiceClient<Channel>)>>,
        follower_conns: Arc<parking_lot::RwLock<HashMap<String, MetaServiceClient<Channel>>>>,
    ) -> Result<()> {
        if leader_conn.load().is_none() {
            let ms_addr = CONFIG.read().metaserver_url.clone();
            let conn = MetaServiceClient::connect(ms_addr.clone()).await?;
            leader_conn.s((ms_addr, conn));

            return Ok(());
        }

        let (url, mut conn) = (*leader_conn.load().r().clone()).clone();
        let resp = conn.info(InfoRequest {}).await?.into_inner();

        let leader_url = if "http://".is_prefix_of(&resp.leader_addr) {
            resp.leader_addr.clone()
        } else {
            format!("http://{}", resp.leader_addr)
        };

        if leader_url != url {
            let conn = MetaServiceClient::connect(resp.leader_addr).await?;
            leader_conn.s((leader_url, conn));
        }

        let mut new_follower_conns = follower_conns
            .read()
            .clone()
            .into_iter()
            .filter(|(addr, _)| resp.follower_addrs.contains(&addr.to_string()))
            .collect::<HashMap<_, _>>();

        for url in resp.follower_addrs.into_iter() {
            let follower_url = if "http://".is_prefix_of(&url) {
                url.clone()
            } else {
                format!("http://{}", url)
            };
            if !new_follower_conns.contains_key(&follower_url) {
                new_follower_conns.insert(follower_url, MetaServiceClient::connect(url).await?);
            }
        }

        *follower_conns.write() = new_follower_conns;

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let ms_addr = CONFIG.read().metaserver_url.clone();
        if let Ok(conn) = MetaServiceClient::connect(ms_addr.clone()).await {
            self.leader_conn.s((ms_addr, conn));
        }

        let leader_conn = self.leader_conn.clone();
        let follower_conns = self.follower_conns.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let _ = Self::update_ms_conns(leader_conn.clone(), follower_conns.clone()).await;
                    }
                }
            }
        });

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}

impl MsClient {
    pub async fn heartbeat(&self, _restarting: bool) -> Result<()> {
        ensure!(
            self.leader_conn.load().is_some(),
            "metaserver connection not ready!"
        );

        let (_, mut client) = (*self.leader_conn.load().r().clone()).clone();

        let req = tonic::Request::new(HeartBeatRequest {
            addr: CONFIG.read().get_self_socket_addr().to_string(),
            restarting: false,
            free_capacity: 1024000, // FIXME:
            cpu_usage: LOAD_STATUS.get_cpuload(),
            qps: 0,
            hot_shards: vec![],
            big_shards: vec![],
            timestamp: Some(std::time::SystemTime::now().into()),
        });

        let resp = client.heart_beat(req).await?;
        debug!("heartbeat response: {:?}", resp.into_inner());

        Ok(())
    }

    pub async fn sync_shards_to_ms(&self) -> Result<()> {
        ensure!(
            self.leader_conn.load().is_some(),
            "metaserver connection not ready!"
        );

        let shards = METADATA.read().shard_iter();
        let (_, mut client) = (*self.leader_conn.load().r().clone()).clone();

        info!("sync shards to metaserver ...");
        let req = tonic::Request::new(stream::iter(SyncShardIter::new(shards.into_iter())));

        let resp = client
            .sync_shard_in_data_server(req)
            .await
            .inspect_err(|e| warn!("failed to sync shard, err: {:?}", e.message()))?;

        debug!("sync shard response: {:?}", resp);
        info!("sync shards to metaserver finished");

        Ok(())
    }
}
