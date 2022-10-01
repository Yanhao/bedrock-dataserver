use super::SledStore;

pub trait MvKv {
    fn WriteLockRange(&mut self, tx_id: u64, start_key: &[u8], end_key: &[u8]);
    fn WriteLockRecord(&mut self, tx_id: u64, key: &[u8]);
    fn ReadLockRange(&mut self, tx_id: u64, start_key: &[u8], end_key: &[u8]);
    fn ReadLockRecord(&mut self, tx_id: u64, key: &[u8]);

    // fn WriteUnLock(&mut self, tx_id: u64);
    // fn ReadUnLock(&mut self, tx_id: u64);

    fn KvGetByVersion(&mut self, version: u64, key: &[u8]);
    fn KvRangeByVersion(&mut self, version: u64, start_key: &[u8], end_key: &[u8]);

    fn KvPutPrepare(&mut self, key: &[u8], value: &[u8]);
    fn KvCommitTx(&mut self, key: &[u8]);
    fn KvAbortTx(&mut self, key: &[u8]);
}

pub trait MvKvLite {
    fn KvGetByVersion(&mut self, version: u64, key: &[u8]);
    fn KvRangeByVersion(&mut self, version: u64, start_key: &[u8], end_key: &[u8]);

    fn KvPutPrepare(&mut self, key: &[u8], value: &[u8]);
    fn KvCommitTx(&mut self, key: &[u8], tx_id: u64);
    fn KvAbortTx(&mut self, key: &[u8]);
}

pub struct MvKvImpl {
    kv_store: SledStore,
// write_locks: RangeLock
}

impl MvKvImpl {}

impl MvKv for MvKvImpl {
    fn WriteLockRange(&mut self, tx_id: u64, start_key: &[u8], end_key: &[u8]) {
        todo!()
    }

    fn WriteLockRecord(&mut self, tx_id: u64, key: &[u8]) {
        todo!()
    }

    fn ReadLockRange(&mut self, tx_id: u64, start_key: &[u8], end_key: &[u8]) {
        todo!()
    }

    fn ReadLockRecord(&mut self, tx_id: u64, key: &[u8]) {
        todo!()
    }

    fn KvGetByVersion(&mut self, version: u64, key: &[u8]) {
        todo!()
    }

    fn KvRangeByVersion(&mut self, version: u64, start_key: &[u8], end_key: &[u8]) {
        todo!()
    }

    fn KvPutPrepare(&mut self, key: &[u8], value: &[u8]) {
        todo!()
    }

    fn KvCommitTx(&mut self, key: &[u8]) {
        todo!()
    }

    fn KvAbortTx(&mut self, key: &[u8]) {
        todo!()
    }
}
