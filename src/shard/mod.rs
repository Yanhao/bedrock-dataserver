use thiserror::Error;

mod order_keeper;
mod shard;
mod shard_manager;

pub use shard::Shard;
pub use shard_manager::{ShardManager, SHARD_MANAGER};

#[derive(Error, Debug)]
pub enum ShardError {
    #[error("no such shard")]
    NoSuchShard,
    #[error("shard already exists")]
    ShardExists,

    #[error("no such key")]
    NoSuchKey,

    #[error("order keeper timeout")]
    Timeout,
    #[error("ignore this order")]
    IgnoreOrder,

    #[error("not leader")]
    NotLeader,

    #[error("append log failed")]
    FailedToAppendLog,
}
