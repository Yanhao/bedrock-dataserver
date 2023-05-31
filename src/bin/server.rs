use std::env::set_current_dir;
use std::path::Path;

use anyhow::Result;
use clap::{App, Arg};
use tokio::signal;
use tonic::transport::Server as GrpcServer;
use tracing::{debug, error, info};
use tracing_subscriber;

use dataserver::config::{config_mod_init, CONFIG, CONFIG_DIR};
use dataserver::format::Formatter;
use dataserver::rpc_service::RealDataServer;
use dataserver::service_pb::data_service_server::DataServiceServer;

fn create_lock_file(path: impl AsRef<Path>) -> Result<()> {
    let lock_path = path.as_ref().to_path_buf().join("LOCK");
    debug!("lock_path: {:?}", lock_path);

    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // console_subscriber::init();
    tracing_subscriber::fmt::init();

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
                return Ok(());
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
        return Ok(());
    }

    info!("starting dataserver...");
    let config_file = matches.value_of("config file").unwrap();
    debug!("config file: {}", config_file);

    if let Err(e) = config_mod_init(config_file) {
        error!("failed to initialize configuration, err: {}", e);
        return Ok(());
    }

    let work_dir = CONFIG.read().work_directory.as_ref().unwrap().to_owned();
    set_current_dir(&work_dir)?;
    info!("change working directory to {}", &work_dir);

    if let Err(e) = create_lock_file(&work_dir) {
        error!("failed to create lock file, err: {}", e);
        info!("maybe there is another server already running");
        return Ok(());
    }

    dataserver::start_background_tasks().await;

    let grpc_server_addr = CONFIG.read().rpc_server_addr.as_ref().unwrap().parse()?;
    let grpc_server =
        GrpcServer::builder().add_service(DataServiceServer::new(RealDataServer::default()));

    tokio::spawn(async move {
        info!("starting data rpc server...");
        grpc_server.serve(grpc_server_addr).await.unwrap();
        info!("stop grpc server");
    });

    signal::ctrl_c().await?;

    info!("ctrl-c pressed");
    if let Err(e) = std::fs::remove_file(format!("{work_dir}/LOCK")) {
        error!("failed to remove lock file, err {}", e);
    }

    Ok(())
}
