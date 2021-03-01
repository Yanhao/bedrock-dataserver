use std::future::Future;

use log::{debug, info};
use prost::Message as PbMessage;

use raft::prelude::Message;

use crate::chunk::ChunkID;
use crate::tcp_server::Responser;
use crate::raftnode_manager::RAFT_MANAGER;

define_handler!(raft_message, Message, chunk_id, req, res, {

    // write data to wal

    //  rebuild a new message and step to raft
    RAFT_MANAGER.write().unwrap().get(chunk_id).unwrap().step(req);
    RAFT_MANAGER.write().unwrap().notify(chunk_id).await;
});
