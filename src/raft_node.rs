use std::mem;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

use anyhow::{bail, Result};
use log::{debug, info};
use protobuf::Message as PbMessage;
use raft::prelude::*;
use tokio::sync::mpsc;

use crate::chunk::{Chunk, ChunkID};
use crate::connection::CONNECTIONS;
use crate::wal::LogStorage;

pub struct RaftNode {
    notifier: mpsc::Sender<ChunkID>,

    propose_v: Mutex<Vec<Vec<u8>>>,
    receive_v: Mutex<Vec<Message>>,
    tick: Mutex<bool>,
    config_v: Mutex<Vec<ConfChangeV2>>,

    log: LogStorage,
    raw_node: RwLock<RawNode<LogStorage>>,
    chunk: Arc<Chunk>,

    process_lock: Mutex<()>,
}

impl RaftNode {
    pub fn new(
        rnode: RawNode<LogStorage>,
        notifier: mpsc::Sender<ChunkID>,
        log: LogStorage,
        chunk: Arc<Chunk>,
    ) -> Self {
        Self {
            notifier,
            propose_v: Mutex::new(Vec::new()),
            receive_v: Mutex::new(Vec::new()),
            tick: Mutex::new(false),
            config_v: Mutex::new(Vec::new()),

            log,
            raw_node: RwLock::new(rnode),
            chunk,

            process_lock: Mutex::new(()),
        }
    }

    pub fn get_chunkid(&self) -> Result<ChunkID> {
        Ok(self.chunk.id)
    }

    pub async fn process(&self) {
        let process_lg = match self.process_lock.try_lock() {
            Ok(v) => v,
            Err(e) => {
                debug!("there is already a process function is running");
                return;
            }
        };

        loop {
            // process
            let mut p = Vec::new();
            mem::swap(self.propose_v.lock().unwrap().as_mut(), &mut p);

            let mut r = Vec::new();
            mem::swap(self.receive_v.lock().unwrap().as_mut(), &mut r);

            let t = *self.tick.lock().unwrap();

            let mut c = Vec::new();
            mem::swap(self.config_v.lock().unwrap().as_mut(), &mut c);

            if p.len() != 0 {
                for i in p.into_iter() {
                    self.raw_node.write().unwrap().propose(Vec::new(), i);
                }
            }

            if r.len() != 0 {
                for i in r.into_iter() {
                    self.raw_node.write().unwrap().step(i);
                }
            }

            if t {
                self.raw_node.write().unwrap().tick();
            }

            if c.len() != 0 {
                for i in c.into_iter() {
                    self.raw_node
                        .write()
                        .unwrap()
                        .propose_conf_change(Vec::new(), i);
                }
            }

            // check and process ready
            while self.raw_node.write().unwrap().has_ready() {
                let ready = self.raw_node.write().unwrap().ready();
                self.process_ready(&ready).await;
                self.raw_node.write().unwrap().advance(ready);
            }

            // check again
            let propose_lg = self.propose_v.lock().unwrap();
            if propose_lg.len() != 0 {
                continue;
            }
            let receive_lg = self.receive_v.lock().unwrap();
            if receive_lg.len() != 0 {
                continue;
            }
            let tick_lg = self.tick.lock().unwrap();
            if *tick_lg {
                continue;
            }
            let config_lg = self.config_v.lock().unwrap();
            if config_lg.len() != 0 {
                continue;
            }
            drop(process_lg);
            return;
        }
    }

    async fn save_to_storage(
        &self,
        hard_state: Option<&HardState>,
        entries: &Vec<Entry>,
        snap: &Snapshot,
    ) -> Result<()> {
        if let Some(h) = hard_state {
            self.log.save_hardstate(h).await?;
        }

        self.log.append_entries(entries).await?;
        self.log.save_snapshot(snap).await?;
        Ok(())
    }

    async fn send_messages(&self, messages: &Vec<Vec<Message>>) -> Result<()> {
        for i in messages.iter() {
            for m in i.iter() {
                CONNECTIONS.read().await.send(m).await;
            }
        }
        Ok(())
    }

    async fn apply_snapshot(&self, snap: &Snapshot) -> Result<()> {
        if !snap.is_empty() {
            self.chunk.apply_snapshot(snap).await?
        }
        Ok(())
    }

    async fn handle_normal(&self, entry: &Entry) -> Result<()> {
        todo!()
    }

    async fn handle_conf_change(&self, entry: &Entry) -> Result<()> {
        let mut cc = ConfChange::default();
        cc.merge_from_bytes(&entry.data).unwrap();

        let cs = self
            .raw_node
            .write()
            .unwrap()
            .apply_conf_change(&cc)
            .unwrap();

        self.log.save_configration(&cs).await?;

        Ok(())
    }

    async fn commit_entries(&self, entries: &Vec<Entry>) {
        for i in entries.iter() {
            // process i
            if i.data.is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            match i.get_entry_type() {
                EntryType::EntryNormal => self.handle_normal(i).await,
                EntryType::EntryConfChange => self.handle_conf_change(i).await,
                EntryType::EntryConfChangeV2 => unimplemented!(),
            };
        }
    }

    pub async fn process_ready(&self, ready: &Ready) {
        // step 1:
        self.save_to_storage(ready.hs(), ready.entries(), ready.snapshot())
            .await;

        // step 2:
        self.send_messages(ready.messages()).await;

        // step 3:
        self.apply_snapshot(ready.snapshot()).await;

        self.commit_entries(ready.committed_entries()).await;
    }
}
