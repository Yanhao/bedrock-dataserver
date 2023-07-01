use thiserror::Error;

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
