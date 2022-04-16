use thiserror::Error;

#[derive(Error, Debug)]
pub enum ShardError {
    #[error("no such shard")]
    NoSuchShard,
    #[error("shard already exists")]
    ShardExists,
}
