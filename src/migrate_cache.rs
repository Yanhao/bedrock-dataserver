use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use arc_swap::ArcSwapOption;

pub static MIGRATE_CACHE: once_cell::sync::Lazy<ArcSwapOption<MigrateCacher>> =
    once_cell::sync::Lazy::new(|| None.into());

pub struct ShardMigrateInfo {
    pub shard_id: u64,

    pub to_addr: SocketAddr,

    pub start_time: std::time::Instant,
    pub finish_time: std::time::Instant,

    pub expire_at: std::time::Instant,
}

impl ShardMigrateInfo {
    fn is_outdated(&self) -> bool {
        self.expire_at < std::time::Instant::now()
    }
}

pub struct MigrateCacher {
    cache: Arc<parking_lot::RwLock<im::HashMap<u64 /* shard_id */, Arc<ShardMigrateInfo>>>>,

    expire_time: std::time::Duration,

    stop_notifier: Option<Arc<tokio::sync::Notify>>,
}

impl MigrateCacher {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(parking_lot::RwLock::new(im::HashMap::new())),

            expire_time: tokio::time::Duration::from_secs(60),

            stop_notifier: None,
        }
    }

    pub fn get(&self, shard_id: u64) -> Option<Arc<ShardMigrateInfo>> {
        self.cache.read().get(&shard_id).cloned()
    }

    pub fn put(&self, shard_id: u64, mut migration: ShardMigrateInfo) {
        migration.expire_at = (tokio::time::Instant::now() + self.expire_time).into();
        self.cache.write().insert(shard_id, Arc::new(migration));
    }

    #[allow(dead_code)]
    pub fn remove(&self, shard_id: u64) {
        self.cache.write().remove(&shard_id);
    }
}

impl MigrateCacher {
    fn cleanup_expiration(
        cache: Arc<parking_lot::RwLock<im::HashMap<u64, Arc<ShardMigrateInfo>>>>,
    ) {
        let cache_snap = cache.read().clone();

        let keys_to_remove = cache_snap
            .iter()
            .filter_map(|(key, migration)| {
                if migration.is_outdated() {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .take(100)
            .collect::<Vec<_>>();

        for key in keys_to_remove.iter() {
            cache.write().remove(key);
        }
    }

    pub fn start(&mut self) -> Result<()> {
        if self.stop_notifier.is_some() {
            return Ok(());
        }

        let notifier = Arc::new(tokio::sync::Notify::new());
        self.stop_notifier.replace(notifier.clone());

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let cache = self.cache.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = ticker.tick()=> {
                        Self::cleanup_expiration(cache.clone());
                    }

                    _ = notifier.notified() => {
                        return;
                    }
                }
            }
        });

        Ok(())
    }

    #[allow(dead_code)]
    pub fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_notifier.as_ref() {
            s.notify_one();
        }

        Ok(())
    }
}
