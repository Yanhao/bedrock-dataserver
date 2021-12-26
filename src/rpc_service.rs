use std::net::SocketAddr;

use anyhow::{bail, Result};
use log::info;
use tonic;
use tonic::{Request, Response, Status};

use dataserver::service_pb::data_service_server::DataService;
use dataserver::service_pb::{
    ShardReadRequest, ShardReadResponse, ShardWriteRequest, ShardWriteResponse,
};

#[derive(Debug, Default)]
pub struct RealDataServer {}

#[tonic::async_trait]
impl DataService for RealDataServer {
    async fn shard_read(
        &self,
        request: Request<ShardReadRequest>,
    ) -> Result<Response<ShardReadResponse>, Status> {
        info!("shard read");

        let reply = ShardReadResponse::default();

        Ok(Response::new(reply))
    }

    async fn shard_write(
        &self,
        request: Request<ShardWriteRequest>,
    ) -> Result<Response<ShardWriteResponse>, Status> {
        info!("shard write");

        let reply = ShardWriteResponse::default();

        Ok(Response::new(reply))
    }
}
