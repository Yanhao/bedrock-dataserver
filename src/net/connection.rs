use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{bail, Result};
use lazy_static::lazy_static;
use log::{error, info};
use prost::Message as PbMessage;
use protobuf::Message as PPbMessage;
use raft::eraftpb;
use raft::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::error::DataServerError;

use dataserver::message_pb;

lazy_static! {
    pub static ref CONNECTIONS: RwLock<Connection> = unsafe { MaybeUninit::uninit().assume_init() };
}

pub struct Connection {
    capacity: u32,
    conns: HashMap<SocketAddrV4, Arc<TcpStream>>,
}

impl Connection {
    pub fn new(capacity: u32) -> Self {
        Self {
            conns: HashMap::new(),
            capacity,
        }
    }

    pub async fn reborn_connection(&mut self, addr: SocketAddrV4) -> Result<Arc<TcpStream>> {
        assert_eq!(self.conns.contains_key(&addr), true);
        let stream = self.conns.remove(&addr).unwrap();
        // stream.shutdown();
        // drop(stream);

        // self.get_connection(addr).await
    }

    pub async fn get_connection(&mut self, addr: SocketAddrV4) -> Result<Arc<TcpStream>> {
        if let Some(v) = self.conns.get(&addr) {
            return Ok(v.to_owned());
        }

        if self.conns.len() >= self.capacity as usize {
            bail!(DataServerError::TooManyConnections);
        }

        let stream = match TcpStream::connect(addr).await {
            Ok(v) => Arc::new(v),
            Err(e) => {
                error!("failed to connect to {}, err {}", addr, e);
                bail!(DataServerError::FailedToConnect);
            }
        };

        self.capacity += 1;
        self.conns.insert(addr, stream.clone());

        Ok(stream)
    }

    pub async fn send(&mut self, msg: &eraftpb::Message) -> Result<()> {
        let addr = id_to_addr(msg.get_to());

        let mut data = Vec::new();
        msg.write_to_vec(&mut data).unwrap();

        let header = message_pb::RequestHeader {
            magic: 0,
            serial: 0,
            op: 0,
            length: data.len() as u32,
        };

        let mut header_data = Vec::new();
        header.encode(&mut header_data);

        let len_prefix = [header_data.len() as u8];

        let d = [&len_prefix[..], &header_data[..], &data[..]].concat();

        let stream = self.get_connection(addr).await.unwrap();
        stream.write(&d[..]).await?;

        Ok(())
    }
}

pub fn id_to_addr(id: u64) -> SocketAddrV4 {
    let ip = (id >> 32) as u32;
    let port = (id << 32) as u16;

    SocketAddrV4::new(ip.into(), port)
}
