pub mod message_pb {
    include!(concat!(env!("OUT_DIR"), "/message_pb.rs"));
}

pub mod wal_pb {
    include!(concat!(env!("OUT_DIR"), "/wal_pb.rs"));
}

pub mod service_pb {
    // tonic::include_proto!("service_pb");
    include!(concat!(env!("OUT_DIR"), "/service_pb.rs"));
}
