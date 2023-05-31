use tracing::info;

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
pub mod service_handler;
pub mod shard;
pub mod sync_shard;
pub mod wal;

pub async fn start_background_tasks() {
    info!("start background tasks ...");

    // if let Err(e) = setup_pid_file(&work_dir) {
    //     error!("failed to setup pid file, err: {}", e);
    //     return;
    // }

    // if let Err(e) = init_raftnode_manager().await {
    //     error!("failed to initialize raftnode manager, err: {}", e);
    //     return;
    // }
    // let raft_manager = RAFT_MANAGER.clone();

    // tokio::spawn(async move {
    //     info!("starting raftnode manager...");
    //     raft_manager.write().await.start_workers().await;
    //     info!("stop raftnode manager");
    // });

    // let mut raft_peer_server = TcpServer::new(
    //     CONFIG
    //         .read()
    //         .unwrap()
    //         .raft_server_addr
    //         .as_ref()
    //         .unwrap()
    //         .parse()
    //         .unwrap(),
    // );
    // register_raft_handlers(&mut raft_peer_server).await;

    // // let r1 = raft_peer_server.clone();
    // tokio::spawn(async move {
    //     info!("starting raft peer server...");
    //     raft_peer_server.run().await;
    //     info!("stop raft peer server");
    // });

    heartbeat::HEART_BEATER.write().start().await;
    sync_shard::SHARD_SYNCER.write().start().await;

    info!("background tasks start finish ...");
}
