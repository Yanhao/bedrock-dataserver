use std::mem::MaybeUninit;

use raft::prelude::*;
use raft::eraftpb;
use lazy_static::lazy_static;
use tokio::sync::RwLock;
use anyhow::{bail, Result};

lazy_static! {
    pub static ref CONNECTIONS: RwLock<Connection> = unsafe { MaybeUninit::uninit().assume_init() };
}

pub struct Connection {}

impl Connection {
    pub async fn send(&self, msg: &eraftpb::Message) -> Result<()> {
        todo!()
    }
}
