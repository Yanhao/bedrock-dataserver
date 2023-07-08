use anyhow::Result;
use futures_util::stream;
use once_cell::sync::Lazy;
use tracing::{debug, info, warn};

use idl_gen::metaserver_pb::sync_shard_in_data_server_request::SyncShardInfo;
use idl_gen::metaserver_pb::{meta_service_client, HeartBeatRequest, SyncShardInDataServerRequest};
use idl_gen::service_pb::ShardMeta;

use crate::config::CONFIG;
use crate::metadata::ShardMetaIter;
use crate::metadata::METADATA;

pub static MS_CLIENT: Lazy<MsClient> = Lazy::new(MsClient::new);

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
        let shard = self.shards.next();
        if self.last {
            return None;
        }

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

pub struct MsClient {}

impl MsClient {
    pub fn new() -> Self {
        Self {}
    }

    fn get_ms_addr(&self) -> String {
        let meta = METADATA.read().get_meta();
        
        meta.metaserver_leader
    }

    pub async fn heartbeat(&self, _restarting: bool) -> Result<()> {
        let addr = self.get_ms_addr();

        info!("heartbeat to metaserver ... metaserver_addr: {}", addr);
        let mut client = meta_service_client::MetaServiceClient::connect(addr.clone()).await?;

        let req = tonic::Request::new(HeartBeatRequest {
            addr: CONFIG.read().get_self_socket_addr().to_string(),
            restarting: false,
        });

        let resp = client.heart_beat(req).await?;
        debug!("heartbeat response: {:?}", resp);

        Ok(())
    }

    pub async fn sync_shards_to_ms(&self, shards: ShardMetaIter) -> Result<()> {
        let addr = self.get_ms_addr();

        info!("sync shards to metaserver ...");
        let mut client = meta_service_client::MetaServiceClient::connect(addr.clone()).await?;

        let req = tonic::Request::new(stream::iter(SyncShardIter::new(shards.into_iter())));

        let resp = client
            .sync_shard_in_data_server(req)
            .await
            .inspect_err(|e| warn!("failed to sync shard to {}, err: {:?}", addr, e))?;

        debug!("sync shards to metaserver finished");
        debug!("sync shard response: {:?}", resp);

        Ok(())
    }
}
