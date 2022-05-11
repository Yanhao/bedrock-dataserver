use anyhow::Result;
use tonic::async_trait;

#[async_trait]
pub trait SnapShoter {
    async fn create_snapshot(&self) -> Result<Vec<Vec<u8>>>;
    async fn install_snapshot(&mut self, piece: &[u8]) -> Result<()>;
}
