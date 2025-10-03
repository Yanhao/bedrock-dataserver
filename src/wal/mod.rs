// WAL (Write-Ahead Logging) module entry
// Unified export for submodules responsible for log persistence, group commit, memory cache, etc.

use anyhow::Result;
use idl_gen::replog_pb::Entry;

mod group_commiter; // Group commit mechanism, improves write throughput
mod mem_log;
mod wal; // WAL main logic, log append, recovery, etc.
mod wal_file; // WAL file low-level read/write // In-memory log cache

pub use wal::Wal;

/// WAL trait, defines common log operation interfaces
#[allow(async_fn_in_trait)]
pub trait WalTrait {
    /// Get log entries in range [lo, hi], up to max_size bytes
    async fn entries(
        &mut self,
        lo: u64,
        hi: u64, /* lo..=hi */
        max_size: u64,
    ) -> Result<Vec<Entry>>;

    /// Append log entries, discard=true to drop old logs, returns last log index
    async fn append(&mut self, ents: Vec<Entry>, discard: bool) -> Result<u64 /* last_index */>;

    /// Compact logs before compact_index
    async fn compact(&mut self, compact_index: u64) -> Result<()>;

    /// Get the index of the first log entry
    async fn first_index(&self) -> u64;
    /// Get the index of the next available log entry
    async fn next_index(&self) -> u64;
}
