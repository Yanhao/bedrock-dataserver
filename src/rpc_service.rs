use std::net::SocketAddr;

use anyhow::{bail, Result};
use log::info;
use tonic;
use tonic::{Request, Response, Status};

use dataserver::service_pb::chunk_rpc_server::ChunkRpc;
use dataserver::service_pb::{ChunkWriteReply, ChunkWriteRequest};

#[derive(Debug, Default)]
pub struct RealDataServer {}

#[tonic::async_trait]
impl ChunkRpc for RealDataServer {
    async fn chunk_write(
        &self,
        request: Request<ChunkWriteRequest>,
    ) -> Result<Response<ChunkWriteReply>, Status> {
        info!("chunk write");

        let reply = ChunkWriteReply::default();

        Ok(Response::new(reply))
    }
}
