pub mod message_pb {
    include!(concat!(env!("OUT_DIR"), "/message_pb.rs"));
}

pub mod wal_pb {
    include!(concat!(env!("OUT_DIR"), "/wal_pb.rs"));
}

pub mod service_pb {
    // tonic::include_proto!("service_pb");
    include!(concat!(env!("OUT_DIR"), "/bedrock.dataserver.rs"));
}

pub mod metaserver_pb {
    include!(concat!(env!("OUT_DIR"), "/bedrock.metaserver.rs"));
}

pub mod replog_pb {
    include!(concat!(env!("OUT_DIR"), "/replog_pb.rs"));
}

// mod chunk;
pub mod config;
pub mod error;
pub mod format;
pub mod heartbeat;
// mod journal;
pub mod connections;
pub mod kv_store;
pub mod metadata;
pub mod param_check;
pub mod rpc_service;
pub mod shard;
pub mod sync_shard;
pub mod wal;
