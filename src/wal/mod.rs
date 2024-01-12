use anyhow::Result;

use idl_gen::replog_pb::Entry;

// mod mem_log;
mod group_commiter;
#[allow(clippy::module_inception)]
mod wal;
mod wal_file;

pub use wal::Wal;

#[allow(async_fn_in_trait)]
pub trait WalTrait {
    async fn entries(
        &self,
        lo: u64,
        hi: u64, /* lo..=hi */
        max_size: u64,
    ) -> Result<Vec<Entry>>;
    async fn append(&self, ents: Entry, discard: bool) -> Result<u64 /* last_index */>;
    async fn compact(&self, compact_index: u64) -> Result<()>;

    async fn first_index(&self) -> u64;
    async fn next_index(&self) -> u64;
}
