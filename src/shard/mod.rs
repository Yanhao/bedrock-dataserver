use thiserror::Error;

mod operation;
#[allow(clippy::module_inception)]
mod shard;
mod shard_manager;

pub use operation::Operation;
pub use shard::{EntryWithNotifierSender, Shard};
pub use shard_manager::{ShardManager, SHARD_MANAGER};

pub const KV_RANGE_LIMIT: i32 = 256;

#[derive(Error, Debug, Clone)]
pub enum ShardError {
    #[error("not leader")]
    NotLeader,

    #[error("not leader with timestamp")]
    NotLeaderWithTs(std::time::SystemTime),

    #[error("append log failed")]
    FailedToAppendLog,

    #[error("log index lag")]
    LogIndexLag(u64),
}
