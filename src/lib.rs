#![feature(result_option_inspect)]
#![feature(impl_trait_in_assoc_type)]
#![feature(async_closure)]
#![feature(pattern)]
#![feature(slice_pattern)]

use tracing::info;

pub mod config;
pub mod ds_client;
pub mod error;
pub mod format;
pub mod handler;
pub mod heartbeat;
pub mod kv_store;
pub mod load_status;
pub mod metadata;
pub mod migrate_cache;
mod ms_client;
pub mod mvcc;
pub mod param_check;
pub mod role;
pub mod shard;
mod shutdown;
pub mod sync_shard;
pub mod utils;
pub mod wal;

pub async fn start_background_tasks() {
    info!("startting background tasks ...");

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

    heartbeat::HEART_BEATER.write().start();
    sync_shard::SHARD_SYNCER.write().start();

    let mut mc = migrate_cache::MigrateCacher::new();
    mc.start().expect("start migrate cacher failed");
    migrate_cache::MIGRATE_CACHE.store(Some(std::sync::Arc::new(mc)));

    let _ = load_status::LOAD_STATUS;

    info!("background tasks start finished");
}
