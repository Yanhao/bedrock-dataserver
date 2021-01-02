#![feature(type_alias_impl_trait)]

mod chunk;
mod chunk_manager;
mod config;
mod error;
mod heartbeat;
mod raft_node;
mod raftnode_manager;
#[macro_use]
mod tcp_server;
mod raft_handlers;
mod wal;
mod connection;

fn main() {
    println!("Hello, world!");
}
