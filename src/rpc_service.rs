use std::collections::HashMap;
use std::net::SocketAddr;

use anyhow::{bail, Result};
use dataserver::replog_pb::{self, Entry};
use dataserver::service_pb::shard_append_log_request;
use log::{info, warn};
use tokio::sync::RwLock;
use tonic::{client, Request, Response, Status};

use dataserver::service_pb::data_service_server::DataService;
use dataserver::service_pb::{
    CreateShardRequest, CreateShardResponse, DeleteShardRequest, ShardAppendLogRequest,
    ShardAppendLogResponse, ShardReadRequest, ShardReadResponse, ShardWriteRequest,
    ShardWriteResponse,
};

use crate::connections::CONNECTIONS;
use crate::shard::{self, Fsm, ReplicateLog, Shard, SHARD_MANAGER};

#[derive(Debug, Default)]
pub struct RealDataServer {}

#[tonic::async_trait]
impl DataService for RealDataServer {
    async fn shard_read(
        &self,
        req: Request<ShardReadRequest>,
    ) -> Result<Response<ShardReadResponse>, Status> {
        info!("shard read");

        let shard_id = req.get_ref().shard_id;

        let fsm = match SHARD_MANAGER.read().await.get_shard_fsm(shard_id).await {
            Err(e) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(s) => s,
        };

        let a = fsm.read().await;
        let value = match a.get(req.get_ref().key.as_slice()).await {
            Err(_) => return Err(Status::not_found("no such key")),
            Ok(v) => v,
        };

        let resp = ShardReadResponse { value };

        Ok(Response::new(resp))
    }

    async fn shard_write(
        &self,
        req: Request<ShardWriteRequest>,
    ) -> Result<Response<ShardWriteResponse>, Status> {
        info!("shard write");

        let shard_id = req.get_ref().shard_id;
        let fsm = match SHARD_MANAGER.write().await.get_shard_fsm(shard_id).await {
            Err(_) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(s) => s,
        };

        let entry = fsm
            .write()
            .await
            .apply(Entry {
                index: 0,
                op: "put".to_string(),
                key: req.get_ref().value.clone(),
                value: req.get_ref().value.clone(),
            })
            .await
            .unwrap();

        let replicates = fsm.read().await.shard.get_replicates();

        for addr in replicates {
            let client = CONNECTIONS
                .write()
                .await
                .get_conn(addr.to_string())
                .await
                .unwrap();
            client
                .write()
                .await
                .shard_append_log(ShardAppendLogRequest {
                    shard_id,
                    entries: vec![shard_append_log_request::Entry {
                        index: entry.index,
                        op: "put".to_string(),
                        key: entry.key.clone(),
                        value: entry.value.clone(),
                    }],
                })
                .await;
        }

        let resp = ShardWriteResponse::default();

        Ok(Response::new(resp))
    }

    async fn create_shard(
        &self,
        req: Request<CreateShardRequest>,
    ) -> Result<Response<CreateShardResponse>, Status> {
        info!("create shard, req: {:?}", req);

        let resp = CreateShardResponse::default();

        let mut replicates: Vec<SocketAddr> = Vec::new();
        for rep in req.get_ref().replicates.iter() {
            replicates.push(rep.parse().unwrap());
        }

        let new_shard = shard::Shard {
            shard_id: req.get_ref().shard_id,
            storage_id: req.get_ref().storage_id,
            create_ts: req.get_ref().create_ts.to_owned().unwrap().into(),
            leader: req.get_ref().leader.parse().unwrap(),
            leader_change_ts: req.get_ref().leader_change_ts.to_owned().unwrap().into(),
            replicates_update_ts: req.get_ref().replica_update_ts.to_owned().unwrap().into(),
            replicates,

            kv_data: RwLock::new(HashMap::new()),
        };

        if let Err(e) = SHARD_MANAGER
            .write()
            .await
            .create_shard_fsm(Fsm::new(new_shard, ReplicateLog::new()))
            .await
        {
            warn!("failed to create shard, {:?}", e);
            return Err(Status::invalid_argument(""));
        }

        Ok(Response::new(resp))
    }

    async fn delete_shard(&self, req: Request<DeleteShardRequest>) -> Result<Response<()>, Status> {
        info!("delete shard, req: {:?}", req);

        SHARD_MANAGER
            .write()
            .await
            .remove_shard_fsm(req.get_ref().shard_id)
            .await
            .unwrap();

        Ok(Response::new(()))
    }

    async fn shard_append_log(
        &self,
        req: Request<ShardAppendLogRequest>,
    ) -> Result<Response<ShardAppendLogResponse>, Status> {
        info!("shard append log entries");

        let fsm = SHARD_MANAGER
            .read()
            .await
            .get_shard_fsm(req.get_ref().shard_id)
            .await
            .unwrap();

        for e in req.get_ref().entries.iter() {
            fsm.write().await.apply(replog_pb::Entry {
                op: e.op.clone(),
                index: e.index,
                key: e.key.clone(),
                value: e.value.clone(),
            });
        }

        let last_index = fsm.read().await.last_index();

        let resp = Response::new(ShardAppendLogResponse {
            last_applied_index: last_index,
        });

        Ok(resp)
    }
}
