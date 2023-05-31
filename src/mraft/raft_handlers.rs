use std::future::Future;
use std::slice;

use anyhow::Result;
use tracing::{debug, error, info};
use prost::Message as PbMessage;

use raft::eraftpb::MessageType;
use raft::prelude::Message;

use crate::chunk::ChunkID;
use crate::chunk_manager::ChunkManager;
use crate::journal::G_JOURNAL;
use crate::raft_log::ReplicateItem;
use crate::raftnode_manager::{RAFT_MANAGER, RAFT_NOTIFIER};
use crate::tcp_server::{Responser, TcpServer};


use crate::message_pb::InstallSnapshot;
use crate::message_pb::RaftContext;

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

define_handler!(raft_message_handler, Message, chunk_id, req, res, {
    info!("raft_message_handler start...");

    let mut new_req = req.clone();
    let chunk = ChunkManager::get_chunk(chunk_id).await.unwrap();

    if let MessageType::MsgAppend = req.msg_type() {
        // write data to wal

        let mut new_entries = vec![];
        for e in req.take_entries().into_iter() {
            let ctx = RaftContext::decode(&e.context[..]).unwrap();

            let pos = G_JOURNAL
                .write()
                .await
                .write_at(chunk_id, &e.data[..], ctx.offset as u64, 0, 0)
                .await
                .unwrap();

            let rep_item = ReplicateItem::default();
            // let rep_item: ReplicateItem = pos.into(); // FIXME: return the position of journal
            let data = unsafe {
                slice::from_raw_parts(
                    &rep_item as *const _ as *const u8,
                    std::mem::size_of::<ReplicateItem>(),
                )
            };
            e.data = data.into();
            new_entries.push(e);
        }
        //  rebuild a new message and step to raft
        new_req.set_entries(new_entries);
    }

    let rnode = RAFT_MANAGER.write().await.get(chunk.id).unwrap();

    rnode.receive(new_req);
    let send_future = RAFT_NOTIFIER.write().unwrap().send(chunk.id);
    send_future.await;
});

define_handler!(snapshot_handler, InstallSnapshot, chunk_id, req, res, {
    info!("install snapshot");
});

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
    server.register_handler(2, snapshot_handler);
    Ok(())
}
