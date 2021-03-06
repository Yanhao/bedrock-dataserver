use std::future::Future;

use log::{debug, info};
use prost::Message as PbMessage;
use anyhow::Result;

use raft::prelude::Message;

use crate::chunk::ChunkID;
use crate::raftnode_manager::RAFT_MANAGER;
use crate::tcp_server::{Responser, TcpServer};

// define_handler!(raft_message, PbMessage, chunk_id, req, res, {
//     info!("test")
//     // write data to wal

//     //  rebuild a new message and step to raft
//     // RAFT_MANAGER
//     //     .write()
//     //     .unwrap()
//     //     .get(chunk_id)
//     //     .unwrap()
//     //     .step(req);
//     // RAFT_MANAGER.write().unwrap().notify(chunk_id).await;
// });

pub async fn register_raft_handlers(server: &mut TcpServer) -> Result<()> {
    // server.register_handler(0, raft_message);
    Ok(())
}
