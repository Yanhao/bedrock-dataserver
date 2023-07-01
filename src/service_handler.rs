use std::net::SocketAddr;
use std::time;

use anyhow::Result;
use futures::StreamExt;
use futures_util::stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::connections::CONNECTIONS;
use crate::param_check;
use crate::replog_pb::{self, Entry};
use crate::service_pb::data_service_server::DataService;
use crate::service_pb::{
    migrate_shard_request, CancelTxRequest, CancelTxResponse, CommitTxRequest, CommitTxResponse,
    CreateShardRequest, CreateShardResponse, DeleteShardRequest, KeyValue, LockRangeRequest,
    LockRangeResponse, LockRecordRequest, LockRecordResponse, MergeShardRequest,
    MergeShardResponse, MigrateShardRequest, MigrateShardResponse, PrepareTxRequest,
    PrepareTxResponse, ShardAppendLogRequest, ShardAppendLogResponse, ShardInfoRequest,
    ShardInfoResponse, ShardInstallSnapshotRequest, ShardInstallSnapshotResponse, ShardReadRequest,
    ShardReadResponse, ShardScanRequest, ShardScanResponse, ShardWriteRequest, ShardWriteResponse,
    SplitShardRequest, SplitShardResponse, StartTxRequest, StartTxResponse,
    TransferShardLeaderRequest, TransferShardLeaderResponse,
};
use crate::shard::{self, SHARD_MANAGER};
use crate::wal::Wal;

#[derive(Debug, Default)]
pub struct RealDataServer {}

#[tonic::async_trait]
impl DataService for RealDataServer {
    async fn shard_read(
        &self,
        req: Request<ShardReadRequest>,
    ) -> Result<Response<ShardReadResponse>, Status> {
        if !param_check::shard_read_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("shard read, req: {:?}", req);

        let shard_id = req.get_ref().shard_id;
        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        if shard.is_deleting() {
            return Err(Status::not_found("shard is deleted"));
        }

        if shard.is_installing_snapshot() {
            return Err(Status::not_found("shard is repairing"));
        }

        let value = shard
            .get(&req.get_ref().key)
            .await
            .map_err(|_| Status::not_found("no such key"))?;

        let resp = ShardReadResponse { value };

        Ok(Response::new(resp))
    }

    async fn shard_write(
        &self,
        req: Request<ShardWriteRequest>,
    ) -> Result<Response<ShardWriteResponse>, Status> {
        if !param_check::shard_write_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("shard write, req: {:?}", req);
        debug!("shard write: key len: {}", req.get_ref().key.len());
        debug!("shard write: value len: {}", req.get_ref().value.len());

        let shard_id = req.get_ref().shard_id;
        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        if shard.is_deleting() {
            return Err(Status::not_found("shard is deleted"));
        }

        if !shard.is_leader() {
            return Err(Status::unavailable("not leader"));
        }

        let mut entry_notifier = shard
            .process_write(Entry {
                index: 0,
                op: "put".into(),
                key: req.get_ref().key.clone(),
                value: req.get_ref().value.clone(),
            })
            .await
            .map_err(|_| Status::internal(""))?;

        info!("start wait result");
        match entry_notifier.wait_result().await {
            Err(shard::ShardError::NotLeader) => {
                error!("wait failed: not leader");
                return Ok(Response::new(ShardWriteResponse { not_leader: true }));
            }
            Err(e) => {
                error!("wait failed: {}", e);

                return Err(Status::internal("internal error"));
            }
            Ok(_) => {}
        };
        info!("end wait result");

        shard
            .put(&req.get_ref().key, &req.get_ref().value)
            .await
            .map_err(|_| Status::internal("internal error"))?;

        return Ok(Response::new(ShardWriteResponse { not_leader: false }));
    }

    async fn shard_scan(
        &self,
        req: Request<ShardScanRequest>,
    ) -> Result<Response<ShardScanResponse>, Status> {
        if !param_check::shard_scan_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        let shard_id = req.get_ref().shard_id;
        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        if shard.is_deleting() {
            return Err(Status::not_found("shard is deleted"));
        }

        if shard.is_installing_snapshot() {
            return Err(Status::not_found("shard is repairing"));
        }

        if shard.is_key_within_shard(&req.get_ref().start_key) {
            return Err(Status::invalid_argument(""));
        }

        let kvs = shard
            .scan(&req.get_ref().start_key, &req.get_ref().end_key)
            .await
            .unwrap();

        return Ok(Response::new(ShardScanResponse {
            kvs: kvs
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key.clone(),
                    value: kv.value.clone(),
                })
                .collect(),
            no_left: shard.is_key_within_shard(&req.get_ref().start_key)
                && shard.is_key_within_shard(&req.get_ref().end_key)
                && kvs.len() == 0,
        }));
    }

    async fn create_shard(
        &self,
        req: Request<CreateShardRequest>,
    ) -> Result<Response<CreateShardResponse>, Status> {
        if !param_check::create_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("create shard, req: {:?}", req);

        let resp = CreateShardResponse::default();

        let mut replicates: Vec<SocketAddr> = Vec::new();
        for rep in req.get_ref().replicates.iter() {
            replicates.push(rep.parse().unwrap());
        }

        let shard_id = req.get_ref().shard_id;
        Wal::create_wal_dir(shard_id)
            .await
            .map_err(|_| Status::internal("failed to creaete wal directory"))?;

        SHARD_MANAGER
            .read()
            .await
            .create_shard(&req)
            .await
            .map_err(|e| Status::invalid_argument(format!("failed to create shard, {:?}", e)))?;

        let shard = SHARD_MANAGER
            .read()
            .await
            .get_shard(shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        info!("create shard successfully, shard_id: {}", shard_id);

        Ok(Response::new(resp))
    }

    async fn delete_shard(&self, req: Request<DeleteShardRequest>) -> Result<Response<()>, Status> {
        if !param_check::delete_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("delete shard, req: {:?}", req);

        let shard_id = req.get_ref().shard_id;

        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(shard_id)
            .await
            .map_err(|_| {
                warn!("no such shard, shard_id: {}", shard_id);
                Status::not_found("no such shard")
            })?;

        shard.mark_deleting();
        shard.stop().await;
        shard.remove_wal().await.map_err(|_| Status::internal(""))?;

        shard
            .remove_shard()
            .await
            .map_err(|_| Status::internal(""))?;

        SHARD_MANAGER
            .write()
            .await
            .remove_shard(shard_id)
            .await
            .map_err(|_| Status::internal(""))?;

        Ok(Response::new(()))
    }

    async fn shard_info(
        &self,
        req: Request<ShardInfoRequest>,
    ) -> Result<Response<ShardInfoResponse>, Status> {
        if !param_check::shard_info_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(req.get_ref().shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        Ok(Response::new(ShardInfoResponse {
            shard_id: req.get_ref().shard_id,
            create_ts: None,
            replicates: shard.get_replicates_strings(),
            is_leader: shard.is_leader(),
            last_wal_index: shard.get_next_index(),
            leader_change_ts: Some(shard.get_leader_change_ts().into()),
            replicates_update_ts: None,
            leader: String::new(),
        }))
    }

    async fn shard_append_log(
        &self,
        req: Request<ShardAppendLogRequest>,
    ) -> Result<Response<ShardAppendLogResponse>, Status> {
        if !param_check::shard_append_log_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("shard append log entries, req: {:?}", req);

        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(req.get_ref().shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        let leader_change_leader_ts: time::SystemTime =
            req.get_ref().leader_change_ts.to_owned().unwrap().into();

        if shard.get_leader_change_ts() > leader_change_leader_ts {
            let resp = Response::new(ShardAppendLogResponse {
                is_old_leader: true,
                last_applied_index: 0,
            });
            return Ok(resp);
        }

        shard.update_leader_change_ts(leader_change_leader_ts).await;

        for e in req.get_ref().entries.iter() {
            shard
                .apply_entry(replog_pb::Entry {
                    op: e.op.clone(),
                    index: e.index,
                    key: e.key.clone(),
                    value: e.value.clone(),
                })
                .await
                .unwrap();
        }

        let last_index = shard.get_last_index().await;

        let resp = Response::new(ShardAppendLogResponse {
            is_old_leader: false,
            last_applied_index: last_index,
        });

        Ok(resp)
    }

    async fn shard_install_snapshot(
        &self,
        req: Request<Streaming<ShardInstallSnapshotRequest>>,
    ) -> Result<Response<ShardInstallSnapshotResponse>, Status> {
        info!("shard install snapshot");

        let mut in_stream = req.into_inner();
        let first_piece = in_stream.next().await.unwrap().unwrap();
        let last_wal_index = first_piece.last_wal_index;

        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(first_piece.shard_id)
            .await
            .unwrap();

        shard.set_installing_snapshot(true);

        while let Some(result) = in_stream.next().await {
            if let Err(_) = result {
                break;
            }
            let piece = result.unwrap();

            if !param_check::shard_install_snapshot_param_check(&piece) {
                shard.set_installing_snapshot(false);
                return Err(Status::invalid_argument(""));
            }

            shard.clear_data().await;
            shard.kv_install_snapshot(&piece.data_piece).await.unwrap();
        }

        shard.reset_replog(last_wal_index).unwrap();
        shard.set_installing_snapshot(false);

        Ok(Response::new(ShardInstallSnapshotResponse {}))
    }

    async fn transfer_shard_leader(
        &self,
        req: Request<TransferShardLeaderRequest>,
    ) -> Result<Response<TransferShardLeaderResponse>, Status> {
        if !param_check::transfer_shard_leader_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("transfer shard leader, req: {:?}", req);

        let shard_id = req.get_ref().shard_id;
        let shard = match SHARD_MANAGER.write().await.load_shard(shard_id).await {
            Err(_) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(v) => v,
        };

        let mut socks = Vec::<SocketAddr>::new();
        for rep in req.get_ref().replicates.iter() {
            let addr: SocketAddr = rep.parse().unwrap();
            socks.push(addr);
        }

        shard.set_replicates(&socks).await.unwrap();
        shard.set_is_leader(true).await;
        shard
            .update_leader_change_ts(req.get_ref().leader_change_ts.to_owned().unwrap().into())
            .await;

        info!("successfully transfer shard leader");

        Ok(Response::new(TransferShardLeaderResponse {}))
    }

    async fn split_shard(
        &self,
        req: Request<SplitShardRequest>,
    ) -> Result<Response<SplitShardResponse>, Status> {
        if !param_check::split_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        SHARD_MANAGER
            .write()
            .await
            .split_shard(req.get_ref().shard_id, req.get_ref().new_shard_id)
            .await;

        info!(
            "split shard successfully, shard_id: {}, new_shard_id: {}",
            req.get_ref().shard_id,
            req.get_ref().new_shard_id
        );

        Ok(Response::new(SplitShardResponse {}))
    }

    async fn merge_shard(
        &self,
        req: Request<MergeShardRequest>,
    ) -> Result<Response<MergeShardResponse>, Status> {
        if !param_check::merge_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        let shard_id_a = req.get_ref().shard_id_a;
        let shard_id_b = req.get_ref().shard_id_b;

        SHARD_MANAGER
            .write()
            .await
            .merge_shard(shard_id_a, shard_id_b)
            .await;

        Ok(Response::new(MergeShardResponse {}))
    }

    async fn migrate_shard(
        &self,
        req: Request<Streaming<MigrateShardRequest>>,
    ) -> Result<Response<MigrateShardResponse>, Status> {
        let mut in_stream = req.into_inner();
        let first = in_stream.next().await.unwrap().unwrap();

        if first.direction == migrate_shard_request::Direction::From as i32 {
            let shard = match SHARD_MANAGER
                .write()
                .await
                .load_shard(first.shard_id_from)
                .await
            {
                Err(_) => {
                    return Err(Status::not_found("no such shard"));
                }
                Ok(s) => s,
            };
            let snap = shard.create_snapshot_iter().await.unwrap();

            let client = CONNECTIONS
                .write()
                .await
                .get_conn(first.target_address)
                .await
                .unwrap();

            let mut req_stream = vec![MigrateShardRequest {
                shard_id_from: first.shard_id_from,
                shard_id_to: first.shard_id_to,
                direction: migrate_shard_request::Direction::To as i32,
                target_address: "".to_string(),
                entries: vec![],
            }];
            for kv in snap.into_iter() {
                req_stream.push(MigrateShardRequest {
                    shard_id_from: first.shard_id_from,
                    shard_id_to: first.shard_id_to,
                    direction: migrate_shard_request::Direction::To as i32,
                    target_address: "".to_string(),
                    entries: vec![migrate_shard_request::Entry {
                        key: kv.0.to_owned(),
                        value: kv.1.to_owned(),
                    }],
                })
            }

            client
                .write()
                .await
                .migrate_shard(Request::new(stream::iter(req_stream)))
                .await
                .unwrap();

            shard.stop().await;
            shard.remove_shard().await.unwrap();

            return Ok(Response::new(MigrateShardResponse {}));
        }

        if first.direction == migrate_shard_request::Direction::To as i32 {
            while let Some(result) = in_stream.next().await {
                if let Err(_) = result {
                    break;
                }

                let piece = result.unwrap();
                if !param_check::migrate_shard_param_check(&piece) {
                    return Err(Status::invalid_argument(""));
                }

                let shard = match SHARD_MANAGER
                    .write()
                    .await
                    .load_shard(first.shard_id_to)
                    .await
                {
                    Err(_) => {
                        return Err(Status::not_found("no such shard"));
                    }
                    Ok(s) => s,
                };

                for kv in piece.entries.into_iter() {
                    shard.put(&kv.key, &kv.value).await.unwrap();
                }
            }

            return Ok(Response::new(MigrateShardResponse {}));
        }

        return Err(Status::invalid_argument(""));
    }

    async fn start_tx(
        &self,
        req: Request<StartTxRequest>,
    ) -> Result<Response<StartTxResponse>, Status> {
        todo!()
    }

    async fn lock_record(
        &self,
        req: Request<LockRecordRequest>,
    ) -> Result<Response<LockRecordResponse>, Status> {
        if !param_check::lock_record_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        let shard_id = req.get_ref().shard_id;
        let shard = SHARD_MANAGER
            .write()
            .await
            .load_shard(shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        // let res = shard.read().await.get(req.get_ref().key.as_ref());
        // if let Ok(_) = res {

        // }

        return Ok(Response::new(LockRecordResponse {}));
    }

    async fn lock_range(
        &self,
        req: Request<LockRangeRequest>,
    ) -> Result<Response<LockRangeResponse>, Status> {
        todo!()
    }

    async fn prepare_tx(
        &self,
        req: Request<PrepareTxRequest>,
    ) -> Result<Response<PrepareTxResponse>, Status> {
        todo!()
    }

    async fn commit_tx(
        &self,
        req: Request<CommitTxRequest>,
    ) -> Result<Response<CommitTxResponse>, Status> {
        todo!()
    }

    async fn cancel_tx(
        &self,
        req: Request<CancelTxRequest>,
    ) -> Result<Response<CancelTxResponse>, Status> {
        todo!()
    }
}