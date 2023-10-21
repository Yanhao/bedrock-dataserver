use core::slice::SlicePattern;

use bytes::Bytes;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvItem {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) timestamp: i64,
    pub(crate) version: u64,
    pub(crate) tombstone: bool,
    pub(crate) commited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxRecord {
    pub(crate) txid: u64,
    pub(crate) timestamp: i64,
    pub(crate) keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxTableItem {
    pub(crate) timestamp: i64,
    pub(crate) key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockItem {
    pub(crate) txid: u64,
    pub(crate) ts: i64,
    pub(crate) r#type: u8,
    pub(crate) range_end: Vec<u8>,
}

impl LockItem {
    pub(crate) fn range_lock_data(txid: u64, range_end: Bytes) -> Bytes {
        let item = LockItem {
            txid,
            ts: Utc::now().timestamp(),
            r#type: 1,
            range_end: range_end.to_vec(),
        };

        serde_json::to_vec(&item).unwrap().into()
    }

    pub(crate) fn record_lock_data(txid: u64) -> Bytes {
        let item = LockItem {
            txid,
            ts: Utc::now().timestamp(),
            r#type: 0,
            range_end: vec![],
        };

        serde_json::to_vec(&item).unwrap().into()
    }

    pub(crate) fn from_data(data: Bytes) -> Self {
        serde_json::from_slice(data.as_slice()).unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn is_range_lock(&self) -> bool {
        self.r#type == 1
    }

    pub(crate) fn is_record_lock(&self) -> bool {
        self.r#type == 0
    }

    pub(crate) fn is_outdated(&self) -> bool {
        Utc.timestamp_opt(self.ts, 0).unwrap()
            + chrono::Duration::from_std(std::time::Duration::from_secs(10)).unwrap()
            < Utc::now()
    }
}
