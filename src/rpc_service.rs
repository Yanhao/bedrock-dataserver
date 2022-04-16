use std::net::SocketAddr;

use anyhow::{bail, Result};
use log::{info, warn};
use tonic::{Request, Response, Status};

use dataserver::service_pb::data_service_server::DataService;
use dataserver::service_pb::{
    CreateShardRequest, CreateShardResponse, DeleteShardRequest, ShardReadRequest,
    ShardReadResponse, ShardWriteRequest, ShardWriteResponse,
};

use crate::shard::{self, SHARD_MANAGER};

#[derive(Debug, Default)]
pub struct RealDataServer {}

#[tonic::async_trait]
impl DataService for RealDataServer {
    async fn shard_read(
        &self,
        req: Request<ShardReadRequest>,
    ) -> Result<Response<ShardReadResponse>, Status> {
        info!("shard read");

        let reply = ShardReadResponse::default();

        Ok(Response::new(reply))
    }

    async fn shard_write(
        &self,
        req: Request<ShardWriteRequest>,
    ) -> Result<Response<ShardWriteResponse>, Status> {
        info!("shard write");

        let reply = ShardWriteResponse::default();

        Ok(Response::new(reply))
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
        };

        if let Err(e) = SHARD_MANAGER.write().unwrap().create_shard(new_shard) {
            warn!("failed to create shard, {:?}", e);
            return Err(Status::invalid_argument(""));
        }

        Ok(Response::new(resp))
    }

    async fn delete_shard(&self, req: Request<DeleteShardRequest>) -> Result<Response<()>, Status> {
        info!("delete shard, req: {:?}", req);

        SHARD_MANAGER
            .write()
            .unwrap()
            .remove_shard(req.get_ref().shard_id)
            .unwrap();

        Ok(Response::new(()))
    }
}
