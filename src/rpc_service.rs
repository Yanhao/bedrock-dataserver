use std::collections::HashMap;
use std::net::SocketAddr;

use anyhow::{bail, Result};
use dataserver::replog_pb::Entry;
use log::{info, warn};
use tokio::sync::RwLock;
use tonic::{client, Request, Response, Status};

use dataserver::service_pb::data_service_server::DataService;
use dataserver::service_pb::{
    CreateShardRequest, CreateShardResponse, DeleteShardRequest, ShardReadRequest,
    ShardReadResponse, ShardWriteRequest, ShardWriteResponse,
};

use crate::connections::CONNECTIONS;
use crate::shard::{self, Fsm, ReplicateLog, SHARD_MANAGER};

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

        fsm.write()
            .await
            .apply(Entry {
                index: 0,
                op: "put".to_string(),
                key: req.get_ref().value.clone(),
                value: req.get_ref().value.clone(),
            })
            .await;

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
                .shard_write(ShardWriteRequest {
                    shard_id,
                    key: req.get_ref().value.clone(),
                    value: req.get_ref().value.clone(),
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
}
