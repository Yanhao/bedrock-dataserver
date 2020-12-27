#![feature(type_alias_impl_trait)]

mod wal;
mod raft_node;
mod raft_server;
mod raft_apply;
mod raft_handlers;
mod error;

fn main() {
    println!("Hello, world!");
}
