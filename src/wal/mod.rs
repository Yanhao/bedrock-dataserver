use anyhow::Result;

use idl_gen::replog_pb::Entry;

mod mem_log;
#[allow(clippy::module_inception)]
mod wal;
mod wal_file;

pub use wal::Wal;

#[allow(async_fn_in_trait)]
pub trait WalTrait {
    async fn entries(
        &mut self,
        lo: u64,
        hi: u64, /* lo..=hi */
        max_size: u64,
    ) -> Result<Vec<Entry>>;
    async fn append(&mut self, ents: Vec<Entry>, discard: bool) -> Result<u64 /* last_index */>;
    async fn compact(&mut self, compact_index: u64) -> Result<()>;

    fn first_index(&self) -> u64;
    fn next_index(&self) -> u64;
}
