use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::{bail, Result};
use async_channel;
use lazy_static::lazy_static;
use log::{info, warn};
use tokio::sync::watch;

use crate::chunk::ChunkID;
use crate::error::DataServerError;
use crate::raft_node::RaftNode;

lazy_static! {
    pub static ref RAFT_MANAGER: RwLock<RaftManager> =
        unsafe { MaybeUninit::uninit().assume_init() };
}

pub struct RaftManager {
    nodes: RwLock<HashMap<ChunkID, Arc<RaftNode>>>,
    notifier: async_channel::Receiver<ChunkID>,

    stop: Option<watch::Sender<()>>,
}

impl RaftManager {
    pub fn new(notifier: async_channel::Receiver<ChunkID>) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            notifier,
            stop: None,
        }
    }

    pub fn put(&self, node: Arc<RaftNode>) -> Result<()> {
        self.nodes
            .write()
            .unwrap()
            .insert(node.get_chunkid()?, node);
        Ok(())
    }

    pub fn get(&self, chunkid: ChunkID) -> Result<Arc<RaftNode>> {
        let ret = self.nodes.read().unwrap().get(&chunkid).unwrap().clone();
        Ok(ret)
    }

    pub fn remove(&self, chunkid: ChunkID) -> Result<()> {
        self.nodes.write().unwrap().remove(&chunkid).unwrap();
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.stop.as_ref().unwrap().send(());
        Ok(())
    }

    pub async fn start_workers(&mut self) -> Result<()> {
        let (tx, rx) = watch::channel(());
        self.stop = Some(tx);

        for i in 0..10 {
            let notifier = self.notifier.clone();
            let mut rx1 = rx.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx1.changed() => {
                            info!("exit from raft process loop");
                            break;
                        }

                        res = notifier.recv() => {
                            let chunkid = match res {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!("");
                                    continue;
                                }
                            };

                            let raftnode = RAFT_MANAGER.read().unwrap().get(chunkid).unwrap();
                            raftnode.process();
                        }
                    }
                }
            });
        }

        Ok(())
    }
}
