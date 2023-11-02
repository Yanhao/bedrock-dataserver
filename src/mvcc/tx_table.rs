use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use itertools::Itertools;

use crate::mvcc::model::TxTableItem;

use super::{dstore::Dstore, model::TxRecord};

const TX_TABLE_KEY_PREFIX: &'static str = "/txids/";

#[derive(Clone)]
pub struct TxTable<'a> {
    dstore: Dstore<'a>,
}

impl<'a> TxTable<'a> {
    pub(crate) fn new(dstore: Dstore<'a>) -> Self {
        Self { dstore }
    }
    fn make_tx_table_key_prefix(txid: u64) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(TX_TABLE_KEY_PREFIX.as_bytes());
        b.extend_from_slice(format!("{txid}/").as_bytes());
        b.freeze()
    }

    fn make_tx_table_key(txid: u64) -> Bytes {
        let mut b = BytesMut::new();
        b.extend_from_slice(&Self::make_tx_table_key_prefix(txid));
        b.extend_from_slice(&Utc::now().timestamp_nanos_opt().unwrap().to_be_bytes());
        b.freeze()
    }

    pub(crate) fn get_tx_record(&self, txid: u64) -> Result<TxRecord> {
        let (keys, tss): (Vec<Vec<u8>>, Vec<i64>) = self
            .dstore
            .kv_scan(Self::make_tx_table_key_prefix(txid))?
            .collect_vec()
            .into_iter()
            .map(|(_key, value)| {
                let item: TxTableItem = serde_json::from_slice(&value).unwrap();
                (item.key, item.timestamp)
            })
            .unzip();

        if keys.is_empty() {
            bail!("no such txid");
        }

        Ok(TxRecord {
            txid,
            timestamp: tss.into_iter().min().unwrap(),
            keys,
        })
    }

    pub(crate) async fn remove_tx_record(&self, txid: u64) -> Result<()> {
        let keys = self
            .dstore
            .kv_scan(Self::make_tx_table_key_prefix(txid))?
            .collect_vec()
            .into_iter()
            .map(|(key, _value)| key)
            .collect_vec();

        for key in keys.into_iter() {
            self.dstore.kv_delete(key).await?;
        }

        Ok(())
    }

    pub(crate) async fn put_tx_item(&self, txid: u64, key: Bytes) -> Result<()> {
        self.dstore
            .kv_set(
                Self::make_tx_table_key(txid),
                serde_json::to_vec(&TxTableItem {
                    timestamp: Utc::now().timestamp(),
                    key: key.into(),
                })
                .unwrap()
                .into(),
            )
            .await
    }
}
