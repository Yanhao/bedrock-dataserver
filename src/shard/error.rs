use thiserror::Error;

#[derive(Error, Debug)]
pub enum ShardError {
    #[error("no such shard")]
    NoSuchShard,
    #[error("shard already exists")]
    ShardExists,

    #[error("no such key")]
    NoSuchKey,

    #[error("entry has been compacted")]
    EntryCompacted,
    #[error("index of out bound")]
    IndexOutOfBound,
    #[error("empty replicate log")]
    EmptyRepLog,
    #[error("compact failed")]
    FailedToCompact,

    #[error("order keeper timeout")]
    TimeOut,
    #[error("ignore this order")]
    IgnoreOrder,

    #[error("not leader")]
    NotLeader,

    #[error("append log failed")]
    FailedToAppendLog,
}
