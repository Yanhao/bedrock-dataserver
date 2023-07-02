use anyhow::Result;
use tokio::{select, sync::mpsc};

pub struct Follower {
    stop_ch: Option<mpsc::Sender<()>>,
}

impl Follower {
    pub fn new() -> Self {
        Self { stop_ch: None }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
