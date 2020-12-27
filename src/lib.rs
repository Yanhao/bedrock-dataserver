pub mod message_pb {
    include!(concat!(env!("OUT_DIR"), "/message_pb.rs"));
}

pub mod wal_pb {
    include!(concat!(env!("OUT_DIR"), "/wal_pb.rs"));
}
