use std::sync::Arc;
use std::{cmp::Ordering, collections::BinaryHeap};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;

use super::ShardError;

type OrderType = u64;

const INVALID_ORDER: OrderType = 0x0;

pub struct Notifier {
    sender: Sender<()>,
    receiver: Receiver<()>,

    err: Result<(), ShardError>,
}

impl Notifier {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);

        Self {
            sender: tx,
            receiver: rx,

            err: Ok(()),
        }
    }

    pub async fn notify(&self) {
        self.sender.send(()).await.unwrap();
    }

    pub async fn wait(&mut self, time_out: tokio::time::Duration) -> Result<(), ShardError> {
        if let Err(_) = tokio::time::timeout(time_out, self.receiver.recv()).await {
            self.err = Err(ShardError::TimeOut.into());

            return Err(ShardError::TimeOut);
        }

        Ok(())
    }

    pub fn get_error(&self) -> Result<(), ShardError> {
        match self.err {
            Err(ShardError::TimeOut) => Err(ShardError::TimeOut),
            _ => Ok(()),
        }
    }

    pub fn set_error(&mut self, err: Result<(), ShardError>) {
        self.err = err;
    }
}

struct Oitem {
    order: OrderType,
    notifier: Arc<Mutex<Notifier>>,
}

impl PartialEq for Oitem {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}

impl PartialOrd for Oitem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.order.partial_cmp(&other.order)
    }
}

impl Eq for Oitem {}

impl Ord for Oitem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order)
    }
}

pub struct OrderKeeper {
    netxt_order: OrderType,
    queue: BinaryHeap<Oitem>,
}

impl OrderKeeper {
    pub fn new() -> Self {
        Self {
            netxt_order: INVALID_ORDER,
            queue: BinaryHeap::new(),
        }
    }

    pub async fn ensure_order(&mut self, order: OrderType) -> Result<(), ShardError> {
        if self.netxt_order < order {
            return Err(ShardError::IgnoreOrder);
        }

        if self.netxt_order == order {
            return Ok(());
        }

        let notifier = Arc::new(Mutex::new(Notifier::new()));
        self.queue.push(Oitem {
            order,
            notifier: notifier.clone(),
        });

        notifier
            .lock()
            .await
            .wait(tokio::time::Duration::from_secs(1))
            .await
            .unwrap();

        return notifier.lock().await.get_error();
    }

    pub async fn pass_order(&mut self, order: OrderType) {
        self.netxt_order = order + 1;

        if self.queue.len() != 0 && self.queue.peek().unwrap().order == self.netxt_order {
            let item = self.queue.peek().unwrap().notifier.clone();

            self.queue.pop();

            item.lock().await.notify().await;
        }
    }

    pub fn clear_waiters(&mut self, order: OrderType) {
        while !self.queue.is_empty() {
            let notifier = self.queue.peek().to_owned();
            self.queue.pop();
        }

        self.set_next_order(order);
    }

    pub fn set_next_order(&mut self, next_order: OrderType) {
        self.netxt_order = next_order;
    }

    pub fn get_next_order(&self) -> OrderType {
        self.netxt_order
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_order_keeper() {}
}
