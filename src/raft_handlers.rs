use std::future::Future;

use anyhow::Result;
use log::{debug, error, info};
use prost::Message as PbMessage;
use raft::prelude::Message;

use crate::chunk::ChunkID;
use crate::raftnode_manager::RAFT_MANAGER;
use crate::tcp_server::{Responser, TcpServer};

define_handler!(raft_message_handler, Message, chunk_id, req, res, {
    info!("test")
    // write data to wal

    //  rebuild a new message and step to raft
    // RAFT_MANAGER
    //     .write()
    //     .unwrap()
    //     .get(chunk_id)
    //     .unwrap()
    //     .step(req);
    // RAFT_MANAGER.write().unwrap().notify(chunk_id).await;
});

// pub fn raft_message(
//     _chunk_id: ChunkID,
//     req_buf: Vec<u8>,
//     mut res: Responser,
// ) -> Box<dyn Future<Output = ()> + Send> {
//     Box::new(async move {
//         debug!("start ping_handler...");

//         let req = Message::decode(&req_buf[..]).unwrap();

//         if let Err(e) = res.reply_ok().await {
//             error!("failed to send reply ok to peer {}, err: {}", res.addr, e);
//         }
//         debug!("end ping_handler");
//     })
// }

pub fn ping_handler(
    _chunk_id: ChunkID,
    _: Vec<u8>,
    mut res: Responser,
) -> Box<dyn Future<Output = ()> + Send> {
    Box::new(async move {
        debug!("start ping_handler...");
        if let Err(e) = res.reply_ok().await {
            error!("failed to send reply ok to peer {}, err: {}", res.addr, e);
        }
        debug!("end ping_handler");
    })
}

pub async fn register_raft_handlers(server: &mut TcpServer) -> Result<()> {
    server.register_handler(0, ping_handler);
    server.register_handler(1, raft_message_handler);
    Ok(())
}
