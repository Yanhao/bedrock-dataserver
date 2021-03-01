use std::net::SocketAddr;

use anyhow::{bail, Result};
use tonic::{transport::Server, Reqeust, Response, Status};

use service_pb::DataServer;

#[derive(Debug, Default)]
struct RealDataServer {}

#[tonic::async_trait]
impl DataServer for RealDataServer {
    async fn chunk_write(&self, request: Request<()>) -> Result<Response<()>, Status> {}
}

async fn start_grpc_server(addr: SocketAddr) -> Result<()> {
    Server::builder()
        .add_service(DataServer::new(RealDataServer::default()))
        .server(addr)
        .await?;
}
