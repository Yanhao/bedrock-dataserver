use std::collections::HashMap;
use std::str::pattern::Pattern;
use std::sync::Arc;

use anyhow::Result;
use arc_swap::access::Access;
use arc_swap::ArcSwapOption;
use futures_util::stream;
use idl_gen::metaserver_pb::meta_service_client::MetaServiceClient;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

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

        // 构建包含端口的dataserver_addr字符串，格式为"name:port"
        let config = CONFIG.read();
        let name = config.name.clone();
        let port = config
            .rpc_server_addr
            .as_ref()
            .and_then(|addr| addr.split(':').nth(1))
            .unwrap_or("8888");
        let dataserver_addr = format!("{}:{}", name, port);

        let mut ret = SyncShardInDataServerRequest {
            dataserver_addr,
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
            debug!(
                "No existing metaserver connection, trying to connect to: {}",
                ms_addr
            );
            match MetaServiceClient::connect(ms_addr.clone()).await {
                Ok(conn) => {
                    leader_conn.s((ms_addr.clone(), conn));
                    info!("Established new metaserver connection to: {}", ms_addr);
                }
                Err(e) => {
                    error!("Failed to connect to metaserver at {}: {:?}", ms_addr, e);
                    // Add more specific diagnostic info
                    if e.to_string().contains("Connection refused") {
                        error!("Metaserver at {} is not reachable. Check if the service is running and network configuration is correct.", ms_addr);
                    } else if e.to_string().contains("Name or service not known") {
                        error!("Could not resolve metaserver hostname: {}. Check DNS configuration or Docker service name.", ms_addr);
                    }
                    return Err(e.into());
                }
            }
            return Ok(());
        }

        let (url, mut conn) = (*leader_conn.load().r().clone()).clone();
        debug!("Using existing metaserver connection: {}", url);
        let resp = match conn.info(InfoRequest {}).await {
            Ok(resp) => {
                info!("response: {:#?}", resp);
                resp.into_inner()
            }
            Err(e) => {
                error!(
                    "Failed to send info request to metaserver at {}: {:?}",
                    url, e
                );
                // Try to reconnect if existing connection fails
                debug!("Attempting to reconnect to metaserver at: {}", url);
                let ms_addr = CONFIG.read().metaserver_url.clone();
                let new_conn = MetaServiceClient::connect(ms_addr.clone()).await?;
                leader_conn.s((ms_addr, new_conn));
                return Ok(());
            }
        };

        let leader_url = if "http://".is_prefix_of(&resp.leader_addr) {
            resp.leader_addr.clone()
        } else {
            format!("http://{}", resp.leader_addr)
        };

        if leader_url != url {
            let conn = MetaServiceClient::connect(leader_url.clone()).await?;
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
        info!("Attempting to connect to metaserver at: {}", ms_addr);
        match MetaServiceClient::connect(ms_addr.clone()).await {
            Ok(conn) => {
                self.leader_conn.s((ms_addr.clone(), conn));
                info!("Successfully connected to metaserver at: {}", ms_addr);
            }
            Err(e) => {
                error!(
                    "Failed to connect to metaserver at {} during initialization: {:?}",
                    ms_addr, e
                );
                // We continue even if initial connection fails, as the background task will retry
            }
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
                        if let Err(e) = Self::update_ms_conns(leader_conn.clone(), follower_conns.clone()).await {
                            warn!("Failed to update metaserver connections: {:?}", e);
                        }
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
        if self.leader_conn.load().is_none() {
            let ms_addr = CONFIG.read().metaserver_url.clone();
            warn!(
                "Heartbeat skipped: metaserver connection not ready for {}",
                ms_addr
            );
            return Err(anyhow::anyhow!("metaserver connection not ready! Check if metaserver is running and network is configured correctly."));
        }

        let (_url, mut client) = (*self.leader_conn.load().r().clone()).clone();

        let req = tonic::Request::new({
            // 构建包含端口的addr字符串，格式为"name:port"
            let config = CONFIG.read();
            let name = config.name.clone();
            let port = config
                .rpc_server_addr
                .as_ref()
                .and_then(|addr| addr.split(':').nth(1))
                .unwrap_or("8888");
            let addr_with_port = format!("{}:{}", name, port);

            let req = HeartBeatRequest {
                addr: addr_with_port,
                restarting: false,
                free_capacity: 1024000, // FIXME:
                cpu_usage: LOAD_STATUS.get_cpuload(),
                qps: 0,
                hot_shards: vec![],
                big_shards: vec![],
                timestamp: Some(std::time::SystemTime::now().into()),
            };
            info!("heartbeat: req: {:#?}", req);
            req
        });

        let resp = client.heart_beat(req).await?;
        debug!("heartbeat response: {:?}", resp.into_inner());

        Ok(())
    }

    pub async fn sync_shards_to_ms(&self) -> Result<()> {
        if self.leader_conn.load().is_none() {
            let ms_addr = CONFIG.read().metaserver_url.clone();
            warn!(
                "Shard sync skipped: metaserver connection not ready for {}",
                ms_addr
            );
            return Err(anyhow::anyhow!("metaserver connection not ready! Check if metaserver is running and network is configured correctly."));
        };

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
