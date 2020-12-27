use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use log::{debug, error, info};
use prost::Message;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use dataserver::message_pb::{RequestHeader, ResponseHeader};

use crate::error::DataServerError;

const REQUEST_MAGIC: u32 = 0x0;
const RESPONSE_MAGIC: u32 = 0x0;
const ERROR_OK: u32 = 0;

struct Messager {
    receiver: OwnedReadHalf,
    sender: Arc<Mutex<OwnedWriteHalf>>,
}

impl Messager {
    fn new(stream: TcpStream) -> Self {
        let (rx, tx) = stream.into_split();
        Self {
            receiver: rx,
            sender: Arc::new(Mutex::new(tx)),
        }
    }

    async fn receive_header(&mut self) -> Result<RequestHeader> {
        let mut len_prefix = [0];
        match self.receiver.read(&mut len_prefix).await {
            Ok(n) if n == 0 => {
                bail!(DataServerError::ConnectionClosed);
            }
            Ok(n) if n != 1 => {
                error!("failed to receive request header, read length: {}", n);
                bail!(DataServerError::FailedToReceive);
            }
            Err(e) => {
                error!("failed to receive request header, err: {}", e);
                bail!(DataServerError::FailedToReceive);
            }
            Ok(_) => (),
        }

        let mut buf = Vec::<u8>::new();
        buf.reserve_exact(len_prefix[0] as usize);
        unsafe { buf.set_len(len_prefix[0] as usize) }

        match self.receiver.read(&mut buf[..]).await {
            Ok(n) if n == 0 => {
                bail!(DataServerError::ConnectionClosed);
            }
            Ok(n) if n != len_prefix[0] as usize => {
                info!("failed to receive request header, read length: {}", n);
                bail!(DataServerError::FailedToReceive);
            }
            Err(e) => {
                error!("failed to receive request header, err: {}", e);
                bail!(DataServerError::FailedToReceive);
            }
            Ok(_) => (),
        }

        match RequestHeader::decode(&buf[..]) {
            Ok(v) => Ok(v),
            Err(e) => {
                bail!("failed to decode request header, err: {}", e);
            }
        }
    }

    async fn receive_body(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.reserve_exact(length);
        unsafe {
            buf.set_len(length);
        }

        debug!("buf length {}", buf.len());
        self.receiver.read(&mut buf).await.unwrap();

        Ok(buf)
    }

    fn get_responser(&self, addr: SocketAddr, op: u8, serial: u64) -> Result<Responser> {
        Ok(Responser {
            addr,
            op,
            serial,
            sender: self.sender.clone(),
        })
    }
}

pub struct Responser {
    addr: SocketAddr,
    op: u8,
    serial: u64,
    sender: Arc<Mutex<OwnedWriteHalf>>,
}

impl Responser {
    pub async fn reply_error_with_data(&mut self, error_code: u32, data: &[u8]) -> Result<()> {
        let res = ResponseHeader {
            magic: RESPONSE_MAGIC,
            serial: self.serial,
            op: self.op as u32,
            error_code,
            length: data.len() as u32,
        };

        let mut buf = Vec::new();
        res.encode(&mut buf).unwrap();

        let len_prefix = [res.encoded_len() as u8];
        let msg = if res.length == 0 {
            [&len_prefix, &buf[..]].concat()
        } else {
            [&len_prefix, &buf[..], data].concat()
        };

        if let Err(e) = self.sender.lock().await.write_all(&msg).await {
            error!("failed to send response to {}, err: {}", self.addr, e);
            bail!(DataServerError::FailedToSend)
        }

        Ok(())
    }

    pub async fn reply_ok(&mut self) -> Result<()> {
        self.reply_error_with_data(ERROR_OK, &[]).await
    }

    pub async fn reply_error(&mut self, error: impl Into<u32>) -> Result<()> {
        self.reply_error_with_data(error.into(), &[]).await
    }

    pub async fn reply_error_with_message(
        &mut self,
        error: impl Into<u32>,
        message: impl Message,
    ) -> Result<()> {
        let mut buf = Vec::new();
        message.encode(&mut buf).unwrap();
        self.reply_error_with_data(error.into(), &buf).await
    }

    pub async fn reply_ok_with_message(&mut self, message: impl Message) -> Result<()> {
        let mut buf = Vec::new();
        message.encode(&mut buf).unwrap();
        self.reply_error_with_data(ERROR_OK, &buf).await
    }
}

struct TcpServer {
    addr: SocketAddr,
    stop_ch: std::sync::Mutex<Option<mpsc::Sender<()>>>,

    handlers: HashMap<
        u8,
        fn(
            serial: u64,
            request: Vec<u8>,
            responser: Responser,
        ) -> Box<dyn Future<Output = ()> + Send>,
    >,
}

impl TcpServer {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stop_ch: std::sync::Mutex::new(None),
            handlers: HashMap::new(),
        }
    }

    async fn start(&self) -> Result<()> {
        let listener = match TcpListener::bind(&self.addr).await {
            Ok(v) => v,
            Err(e) => {
                error!("failed to bind address {}, err: {}", self.addr, e);
                bail!(DataServerError::FailedToStopTcpServer);
            }
        };

        let (tx, mut rx) = mpsc::channel(1);
        *self.stop_ch.lock().unwrap() = Some(tx);

        loop {
            tokio::select! {
            _ = rx.recv() => {
                info!("receive stop message, break from tcp server loop");
                // FIXME: wait all server coroutine exit then break
                break;
            }
            val = listener.accept() => {
                let (socket, addr) = match val {
                    Ok(v) => v,
                    Err(e) => {
                        error!("failed to accept connection, err: {}", e);
                        continue;
                    }
                };
                info!("accept connection from {}", addr);

                let handlers = self.handlers.clone();
                tokio::spawn(async move {
                    let mut msger = Messager::new(socket);
                    loop {
                        info!("serving requests from {}", addr);
                        let header = match msger.receive_header().await {
                            Ok(h) => h,
                            Err(e) => {
                                match e.downcast_ref::<DataServerError>() {
                                    Some(DataServerError::FailedToStopTcpServer) => break,
                                    Some(DataServerError::FailedToStopTcpServer) => break,
                                    _ => {
                                        error!("failed to receive message header, err: {}", e);
                                        break;
                                    }
                                }
                            }
                        };

                        debug!("request op: {:?}", header.op);
                        debug!("request header: {:#?}", header);

                        let request_buf = msger.receive_body(header.length as usize).await.unwrap();
                        let handler = handlers.get(&(header.op as u8)).unwrap().clone();
                        let serial = header.serial;
                        let responser = msger.get_responser(addr, header.op as u8, serial).unwrap();

                        // create new coroutine to handle this request
                        tokio::spawn(async move {
                            Pin::from(handler(serial, request_buf, responser)).await;
                        });

                    }

                });
            }
            }
        }

        info!("exit from tcp server loop");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.stop_ch
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(())
            .await
            .map_err(|_| DataServerError::FailedToStopTcpServer)?;
        Ok(())
    }

    fn register_handler(
        &mut self,
        op: u8,
        fun: fn(
            serial: u64,
            request: Vec<u8>,
            responser: Responser,
        ) -> Box<dyn Future<Output = ()> + Send>,
    ) {
        self.handlers.insert(op, fun);
    }
}


#[macro_export]
macro_rules! define_handler {
    ($fn_name:ident, $typ:ty, $req:tt, $res:ident, $($t:tt)*) => {
        pub fn $fn_name(
            req_buf: Vec<u8>,
            mut $res: Responser,
        ) -> Box<dyn Future<Output = ()> + Send> {
            Box::new(async move {
                debug!("start {}...", &stringify!($fn_name));
                let $req = <$typ>::decode(&req_buf[..]).unwrap();

                $($t)*

                debug!("end {}", &stringify!($fn_name));
            })
        }
    }
}

pub fn ping_handler(_: Vec<u8>, mut res: Responser) -> Box<dyn Future<Output = ()> + Send> {
    Box::new(async move {
        debug!("start ping_handler...");
        if let Err(e) = res.reply_ok().await {
            error!("failed to send reply ok to peer {}, err: {}", res.addr, e);
        }
        debug!("end ping_handler");
    })
}
