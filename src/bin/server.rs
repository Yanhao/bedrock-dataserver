#![feature(impl_trait_in_assoc_type)]
use std::env::set_current_dir;
use std::path::Path;

use anyhow::Result;
use clap::Parser;
use tokio::{select, signal};
use tonic::transport::Server as GrpcServer;
use tracing::{error, info};

use idl_gen::service_pb::data_service_server::DataServiceServer;

use dataserver::config::{init_config, CONFIG, DEFAULT_CONFIG_FILE};
use dataserver::format::Formatter;
use dataserver::handler::RealDataServer;

fn create_lock_file(path: impl AsRef<Path>) -> Result<()> {
    let lock_path = path.as_ref().to_path_buf().join("LOCK");
    info!("create lock file: {:?}", lock_path);

    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)?;

    Ok(())
}

fn format_dir(dir: &str) -> Result<()> {
    let f =
        Formatter::new(dir).inspect_err(|e| error!("failed to create new formatter, err: {e}"))?;
    f.format()
        .inspect_err(|e| error!("failed to format {dir}, err: {e}"))?;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = DEFAULT_CONFIG_FILE.to_string())]
    config: String,
    #[arg(short, long, default_value_t = false)]
    format: bool,
    #[arg(long)]
    format_dir: Option<String>,
    #[arg(long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    if args.format {
        format_dir(&args.format_dir.unwrap())?;
        std::process::exit(0);
    }

    info!("starting dataserver...");
    init_config(&args.config)
        .inspect_err(|e| error!("failed to initialize configuration, err: {e}"))?;

    if let Some(name) = args.name {
        info!("overriding name from command line argument: {}", name);
        CONFIG.write().name = name;
    }

    let work_dir = CONFIG.read().work_dir.as_ref().unwrap().to_owned();
    set_current_dir(&work_dir)?;
    info!("change working directory to {work_dir}/");

    create_lock_file(&work_dir).inspect_err(|e| error!("failed to create lock file, err: {e}"))?;

    dataserver::start_background_tasks().await;

    let grpc_server_addr = CONFIG.read().rpc_server_addr.as_ref().unwrap().parse()?;
    tokio::spawn(async move {
        let grpc_server =
            GrpcServer::builder().add_service(DataServiceServer::new(RealDataServer::default()));
        info!("starting data rpc server...");
        grpc_server.serve(grpc_server_addr).await.unwrap();
        info!("stop grpc server");
    });

    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    select! {
        _ = signal::ctrl_c() => {
            info!("ctrl-c pressed");
        },
        _ = sigterm.recv() => {
            info!("SIGTERM received");
        },
    };

    std::fs::remove_file(format!("{work_dir}/LOCK"))
        .inspect_err(|e| error!("failed to remove lock file, err {e}"))?;

    Ok(())
}
