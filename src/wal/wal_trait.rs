use anyhow::Result;
use async_trait::async_trait;

use crate::replog_pb::Entry;

#[async_trait]
pub trait WalTrait {
    async fn entries(&mut self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>>;
    async fn append(&mut self, ents: Vec<Entry>) -> Result<()>;
    async fn compact(&mut self, compact_index: u64) -> Result<()>;

    fn last_index(&self) -> u64;
    fn first_index(&self) -> u64;
}
