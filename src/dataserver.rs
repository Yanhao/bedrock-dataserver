use std::env::set_current_dir;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use chrono;
use clap::{App, Arg};
use dataserver::service_pb::data_service_server::{DataService, DataServiceServer};
use fern;
use fern::colors::{Color, ColoredLevelConfig};
use log::{debug, error, info};
use tokio::signal;
use tokio::sync::RwLock;
use tonic::transport::Server as GrpcServer;

use crate::config::{config_mod_init, CONFIG, CONFIG_DIR};
use crate::format::Formatter;
use crate::rpc_service::RealDataServer;

// mod chunk;
mod config;
mod error;
mod format;
mod heartbeat;
// mod journal;
mod rpc_service;
mod shard;
mod metadata;
mod connections;
mod param_check;

fn setup_logger() -> Result<()> {
    let color = ColoredLevelConfig::new()
        .info(Color::Green)
        .warn(Color::Yellow)
        .debug(Color::Cyan);

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                record.target(),
                color.color(record.level()),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        // .chain(fern::log_file("dataserver.log"))
        .apply()?;
    Ok(())
}

fn create_lock_file(path: impl AsRef<Path>) -> Result<()> {
    let lock_path = path.as_ref().to_path_buf().join("LOCK");
    debug!("lock_path: {:?}", lock_path);
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)?;
    Ok(())
}

fn setup_pid_file(path: impl AsRef<Path>) -> Result<()> {
    todo!()
}

#[tokio::main]
async fn main() {
    setup_logger().unwrap();
    let default_config_file = format!("{}/{}", CONFIG_DIR, "dataserver.toml");

    let app = App::new("DataServer")
        .version("0.0.1")
        .author("Yanhao Mo <yanhaocs@gmail.com>")
        .about("A practice project for distributed storage system")
        .arg(
            Arg::with_name("config file")
                .short("c")
                .long("config")
                .default_value(&default_config_file)
                .help("Specify configuration file location"),
        )
        .arg(
            Arg::with_name("format directory")
                .short("f")
                .long("format")
                .takes_value(true)
                .help("Format data directory"),
        );

    let matches = app.get_matches();
    if matches.is_present("format directory") {
        println!("formatting...");

        let f = match Formatter::new(matches.value_of("format directory").unwrap()) {
            Err(e) => {
                eprintln!("failed to create new formatter, err: {}", e);
                return;
            }
            Ok(v) => v,
        };

        if let Err(e) = f.format() {
            eprintln!(
                "failed to format {}, err: {}",
                matches.value_of("format directory").unwrap(),
                e
            );
        } else {
            println!(
                "successfully formatted {}!",
                matches.value_of("format directory").unwrap(),
            );
        }
        return;
    }

    info!("starting dataserver...");
    let config_file = matches.value_of("config file").unwrap();
    debug!("config file: {}", config_file);

    if let Err(e) = config_mod_init(config_file) {
        error!("failed to initialize configuration, err: {}", e);
        return;
    }

    let work_dir = CONFIG
        .read()
        .unwrap()
        .work_directory
        .as_ref()
        .unwrap()
        .to_owned();
    set_current_dir(&work_dir).unwrap();
    info!("change working directory to {}", &work_dir);

    if let Err(e) = create_lock_file(&work_dir) {
        error!("failed to create lock file, err: {}", e);
        info!("maybe there is another server already running");
        return;
    }

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

    heartbeat::HEART_BEATER.write().unwrap().start().await.unwrap();

    let grpc_server_addr = CONFIG
        .read()
        .unwrap()
        .rpc_server_addr
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let grpc_server =
        GrpcServer::builder().add_service(DataServiceServer::new(RealDataServer::default()));

    tokio::spawn(async move {
        info!("starting data rpc server...");
        grpc_server.serve(grpc_server_addr).await.unwrap();
        info!("stop grpc server");
    });

    info!("starting heartbeat loop...");

    signal::ctrl_c().await.unwrap();
    // r1.read().await.stop().await;

    heartbeat::HEART_BEATER.write().unwrap().stop().await;

    info!("ctrl-c pressed");
    if let Err(e) = std::fs::remove_file(work_dir + "/LOCK") {
        error!("failed to remove lock file, err {}", e);
    }
}
