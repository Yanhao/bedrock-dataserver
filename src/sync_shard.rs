use anyhow::Result;
use futures_util::stream;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::{select, sync::mpsc};
use tracing::{info, warn};

use crate::config::get_self_socket_addr;
use crate::metadata::{Meta, ShardMetaIter, METADATA};
use crate::metaserver_pb::sync_shard_in_data_server_request::SyncShardInfo;
use crate::metaserver_pb::{meta_service_client, SyncShardInDataServerRequest};
use crate::service_pb::ShardMeta;

pub static SHARD_SYNCER: Lazy<RwLock<ShardSyncer>> = Lazy::new(|| Default::default());

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
            dataserver_addr: get_self_socket_addr().to_string(),
            sync_ts: Some(self.sync_ts.clone()),
            shards: vec![],
            is_last_piece: if shard == None {
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

#[derive(Default)]
pub struct ShardSyncer {
    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardSyncer {
    pub async fn start(&mut self) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let meta = METADATA.read().get_meta();
                        let addr = meta.metaserver_leader;

                        info!("sync shard to metaserver ... metaserver addr: {}", addr);
                        let mut client = match  meta_service_client::MetaServiceClient::connect(addr.clone()).await {
                            Err(e) => {
                                warn!("failed to connect to {}, err: {:?}", addr, e);
                                continue;
                            },
                            Ok(v) => v
                        };

                        let shards = METADATA.read().shard_iter();

                        let req = tonic::Request::new(
                            stream::iter(SyncShardIter::new(shards.into_iter()))
                        );

                        match client.sync_shard_in_data_server(req).await {
                            Err(e) => {
                                warn!("failed to sync shard to {}, err: {:?}", addr, e);
                            },
                            Ok(resp) => {
                                info!("sync shard response: {:?}", resp);
                            }
                        }
                    }
                }
            }

            info!("sync shard stopped ...");
        });
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
