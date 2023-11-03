use std::sync::Arc;

use anyhow::Result;
use chrono::prelude::*;

use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tracing::warn;

pub static LOAD_STATUS: Lazy<CpuLoad> = Lazy::new(|| {
    let mut cpu_load = CpuLoad::new();
    cpu_load.start();
    cpu_load
});

#[derive(Debug)]
pub struct LoadAndTs {
    load: f32,
    _ts: chrono::DateTime<Utc>,
}
pub struct CpuLoad {
    stat: Arc<parking_lot::RwLock<LoadAndTs>>,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl CpuLoad {
    fn new() -> Self {
        Self {
            stat: Arc::new(parking_lot::RwLock::new(LoadAndTs {
                load: 0.0,
                _ts: Utc::now(),
            })),

            stop_ch: None,
        }
    }

    pub fn get_cpuload(&self) -> f32 {
        self.stat.read().load
    }

    fn start(&mut self) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let stat = self.stat.clone();

        tokio::spawn(async move {
            let mut collecter = psutil::cpu::CpuPercentCollector::new().unwrap();

            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(60));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let Ok(load) = collecter
                            .cpu_percent()
                            .inspect_err(|e| warn!("calc pod cpu load failed, err: {e}"))
                        else {
                            continue;
                        };

                        *(stat.write()) = LoadAndTs {
                            load,
                            _ts: Utc::now(),
                        };
                    }
                }
            }
        });
    }

    #[allow(unused)]
    pub async fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
