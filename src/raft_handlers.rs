use std::future::Future;

use dataserver::message_pb::RequestAppendEntries;

use log::{debug, info};
use prost::Message;

use crate::tcp_server::Responser;

define_handler!(raft_append_entry, RequestAppendEntries, req, res, {
    todo!();
});
