use std::collections::btree_map::BTreeMap;
use std::vec::Vec;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{LockItem, RangeLock, SledStore};

#[async_trait]
pub trait MvKv: MvKvLite {
    fn write_lock_range(&mut self, tx_id: u64, start_key: &[u8], end_key: &[u8]);
    fn write_lock_record(&mut self, tx_id: u64, key: &[u8]);
    fn read_lock_range(&mut self, tx_id: u64, start_key: &[u8], end_key: &[u8]);
    fn read_lock_record(&mut self, tx_id: u64, key: &[u8]);

    fn write_unlock(&mut self, tx_id: u64);
    fn read_unlock(&mut self, tx_id: u64);

    // fn kv_get_by_version(&mut self, version: u64, key: &[u8]);
    // fn kv_range_by_version(&mut self, version: u64, start_key: &[u8], end_key: &[u8]);

    // fn kv_put_prepare(&mut self, key: &[u8], value: &[u8]);
    // fn kv_commit_tx(&mut self, key: &[u8]);
    // fn kv_abort_tx(&mut self, key: &[u8]);
}

#[async_trait]
pub trait MvKvLite {
    async fn kv_get_by_version(&mut self, version: u64, key: &[u8]) -> Result<Vec<u8>>;
    fn kv_range_by_version(&mut self, version: u64, start_key: &[u8], end_key: &[u8]);

    async fn kv_put_prepare(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn kv_commit_tx(&mut self, key: &[u8], tx_id: u64) -> Result<()>;
    async fn kv_abort_tx(&mut self, key: &[u8]) -> Result<()>;
}

#[allow(unused)]
pub struct MvKvImpl {
    kv_store: SledStore,
    write_locks: RangeLock,
    ongoing_txs: BTreeMap<u64, Vec<LockItem>>,

    write_lock_waiters: RangeLock,

    read_locks: RangeLock,
}

impl MvKvImpl {}

#[async_trait]
impl MvKvLite for MvKvImpl {
    async fn kv_get_by_version(&mut self, _version: u64, _key: &[u8]) -> Result<Vec<u8>> {
        // let key = key + '@' + BigUint::from(version).to_bytes_be();
        // let kvs = self.kv_store.kv_scan(start_key, end_key, 1).await?;
        // Ok(kvs[0].value)
        Err(anyhow!(""))
    }

    fn kv_range_by_version(&mut self, _version: u64, _start_key: &[u8], _end_key: &[u8]) {
        todo!()
    }

    async fn kv_put_prepare(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.kv_store.kv_set(key, value).await?;
        Ok(())
    }

    async fn kv_commit_tx(&mut self, _key: &[u8], _version: u64) -> Result<()> {
        // let key = key + '@' + BigUint::from(version).to_bytes_be();
        // self.kv_store.kv_set(key, value).await?;

        Ok(())
    }

    async fn kv_abort_tx(&mut self, _key: &[u8]) -> Result<()> {
        // self.kv_store.kv_delete(key).await?;

        Ok(())
    }
}

#[async_trait]
impl MvKv for MvKvImpl {
    fn write_lock_range(&mut self, _tx_id: u64, _start_key: &[u8], _end_key: &[u8]) {
        todo!()
        // let o = self.write_locks.lock_overlap(start_key, end_key);
        // if o.len() == 0 {
        //     self.write_locks.lock_range(start_key, end_key);
        //     if let Some(locks) = self.ongoing_txs.get(&tx_id) {
        //         locks.push(LockItem {
        //             start: start_key,
        //             end: end_key,
        //         });
        //     } else {
        //         self.ongoing_txs.insert(
        //             tx_id,
        //             vec![LockItem {
        //                 start: start_key,
        //                 end: end_key,
        //             }],
        //         );
        //     }

        //     return;
        // }

        // self.write_lock_waiters.lock_range(start_key, end_key);
    }

    fn write_lock_record(&mut self, _tx_id: u64, _key: &[u8]) {
        todo!()
        // let key_int = BigUint::from_bytes_be(key);
        // let next_key_int = key_int + 1;

        // let o = self.write_locks.lock_overlap(key, next_key_int);
        // if o.len() == 0 {
        //     self.write_locks.lock_record(key);
        //     if let Some(locks) = self.ongoing_txs.get(&tx_id) {
        //         // locks.push(LockItem {
        //         //     start: start_key,
        //         //     end: end_key,
        //         // });
        //     } else {
        //         // self.ongoing_txs.insert(
        //         //     tx_id,
        //         //     vec![LockItem {
        //         //         start: start_key,
        //         //         end: end_key,
        //         //     }],
        //         // );
        //     }

        //     return;
        // }

        // self.write_lock_waiters.lock_record(key);
    }

    fn read_unlock(&mut self, _tx_id: u64) {}

    fn write_unlock(&mut self, tx_id: u64) {
        let txs = self.ongoing_txs.get(&tx_id).unwrap();
        for lock in txs.iter() {
            self.write_locks.unlock_range(lock.start, lock.end);
        }
    }

    fn read_lock_range(&mut self, _tx_id: u64, _start_key: &[u8], _end_key: &[u8]) {
        todo!()
    }

    fn read_lock_record(&mut self, _tx_id: u64, _key: &[u8]) {
        todo!()
    }
}
