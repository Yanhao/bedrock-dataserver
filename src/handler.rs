use std::sync::Arc;
use std::time;

use anyhow::Result;
use futures::StreamExt;
use futures_util::stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use idl_gen::replog_pb;
use idl_gen::service_pb::data_service_server::DataService;
use idl_gen::service_pb::{
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

use crate::ds_client::CONNECTIONS;
use crate::migrate_cache::ShardMigrateInfo;
use crate::shard::{Shard, ShardError, KV_RANGE_LIMIT, SHARD_MANAGER};
use crate::{migrate_cache, param_check};

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
        let shard = self.get_shard(shard_id).await?;

        let value = shard
            .kv_store
            .kv_get(&req.get_ref().key)
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
        let shard = self.get_shard(shard_id).await?;

        if !shard.is_leader() {
            return Err(Status::unavailable("not leader"));
        }

        let mut entry_with_notifier = shard
            .process_write("put", &req.get_ref().key, &req.get_ref().value)
            .await
            .map_err(|_| Status::internal("process write failed"))?;

        info!("start wait result");
        if let Err(e) = entry_with_notifier.wait_result().await {
            if let Some(ShardError::NotLeader) = e.downcast_ref() {
                error!("wait failed: not leader");
                return Ok(Response::new(ShardWriteResponse { not_leader: true }));
            }

            error!("wait failed: {}", e);
            return Err(Status::internal("internal error"));
        }
        info!("end wait result");

        shard
            .apply_entry(&entry_with_notifier.entry)
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
        let shard = self.get_shard(shard_id).await?;

        if shard.is_key_within_shard(&req.get_ref().start_key) {
            return Err(Status::invalid_argument(""));
        }

        let kvs = shard
            .kv_store
            .kv_scan(
                &req.get_ref().start_key,
                &req.get_ref().end_key,
                KV_RANGE_LIMIT,
            )
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
                && kvs.is_empty(),
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

        let shard_id = req.get_ref().shard_id;

        Shard::create_shard(&req)
            .await
            .map_err(|e| Status::invalid_argument(format!("failed to create shard, {:?}", e)))?;

        let _shard = self.get_shard(shard_id).await?;

        info!("create shard successfully, shard_id: 0x{:016x}", shard_id);

        Ok(Response::new(resp))
    }

    async fn delete_shard(&self, req: Request<DeleteShardRequest>) -> Result<Response<()>, Status> {
        if !param_check::delete_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("delete shard, req: {:?}", req);

        let shard_id = req.get_ref().shard_id;
        let shard = self.get_shard(shard_id).await?;

        shard.mark_deleting();
        shard
            .stop_role()
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;

        SHARD_MANAGER
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

        let shard_id = req.get_ref().shard_id;
        let shard = self.get_shard(shard_id).await?;

        Ok(Response::new(ShardInfoResponse {
            shard_id: req.get_ref().shard_id,
            create_ts: None,
            replicates: shard.get_replicates_strings(),
            is_leader: shard.is_leader(),
            next_index: shard.get_next_index().await,
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

        let shard_id = req.get_ref().shard_id;
        let shard = self.get_shard(shard_id).await?;

        let leader_change_leader_ts: time::SystemTime =
            req.get_ref().leader_change_ts.to_owned().unwrap().into();

        if shard.get_leader_change_ts() > leader_change_leader_ts {
            let resp = Response::new(ShardAppendLogResponse {
                is_old_leader: true,
                next_index: 0,
            });

            info!("shard_append_log resp: {:?}", resp);
            return Ok(resp);
        }

        shard.update_leader_change_ts(leader_change_leader_ts);

        for e in req.get_ref().entries.iter() {
            let e = replog_pb::Entry {
                op: e.op.clone(),
                index: e.index,
                key: e.key.clone(),
                value: e.value.clone(),
            };
            if let Err(e) = shard.append_log_entry(&e).await {
                if let Some(ShardError::LogIndexLag(next_index)) = e.downcast_ref() {
                    warn!("log index lag, next_index: {next_index}");
                    let resp = Response::new(ShardAppendLogResponse {
                        is_old_leader: false,
                        next_index: *next_index,
                    });

                    info!("shard_append_log 1 resp: {:?}", resp);
                    return Ok(resp);
                }
            }

            shard.apply_entry(&e).await.unwrap();
        }

        let resp = Response::new(ShardAppendLogResponse {
            is_old_leader: false,
            next_index: shard.get_next_index().await,
        });

        info!("shard_append_log 2 resp: {:?}", resp);
        Ok(resp)
    }

    async fn shard_install_snapshot(
        &self,
        req: Request<Streaming<ShardInstallSnapshotRequest>>,
    ) -> Result<Response<ShardInstallSnapshotResponse>, Status> {
        info!("shard install snapshot");

        let mut in_stream = req.into_inner();
        let first_piece = in_stream.next().await.unwrap().unwrap();
        let next_index = first_piece.next_index;
        info!("shard_install_snapshot: next_index: {next_index}");

        let shard = self.get_shard(first_piece.shard_id).await?;

        shard.set_installing_snapshot(true);

        while let Some(result) = in_stream.next().await {
            if result.is_err() {
                break;
            }
            let piece = result.unwrap();

            if !param_check::shard_install_snapshot_param_check(&piece) {
                shard.set_installing_snapshot(false);
                return Err(Status::invalid_argument(""));
            }

            // shard.kv_store.clear_data().await;
            shard
                .kv_store
                .install_snapshot(piece.entries)
                .await
                .unwrap();
        }

        shard.reset_replog(next_index).await.unwrap();
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
        let shard = self.get_shard(shard_id).await?;

        let socket_addrs = req
            .get_ref()
            .replicates
            .iter()
            .map(|x| x.parse().unwrap())
            .collect::<Vec<_>>();

        shard.set_replicates(&socket_addrs);
        shard.set_is_leader(true);
        shard.update_leader_change_ts(req.get_ref().leader_change_ts.to_owned().unwrap().into());
        shard
            .save_meta()
            .await
            .map_err(|_| Status::internal("save shard meta failed"))?;
        shard.switch_role_to_leader().await.unwrap();

        info!("successfully transfer shard leader");

        Ok(Response::new(TransferShardLeaderResponse {}))
    }

    async fn migrate_shard(
        &self,
        req: Request<Streaming<MigrateShardRequest>>,
    ) -> Result<Response<MigrateShardResponse>, Status> {
        let mut in_stream = req.into_inner();
        let first = in_stream.next().await.unwrap().unwrap();

        if first.direction == migrate_shard_request::Direction::From as i32 {
            let start_time = std::time::Instant::now();

            let shard = self.get_shard(first.shard_id_from).await?;
            let snap = shard.kv_store.create_snapshot_iter().await.unwrap();

            let mut client = CONNECTIONS.get_client(&first.target_address).await.unwrap();

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
                .migrate_shard(Request::new(stream::iter(req_stream)))
                .await
                .unwrap();

            shard.mark_deleting();
            shard.stop_role().await.unwrap();
            Shard::remove_shard(shard.get_shard_id()).await.unwrap();

            migrate_cache::MIGRATE_CACHE.load().as_ref().unwrap().put(
                first.shard_id_from,
                ShardMigrateInfo {
                    shard_id: first.shard_id_from,
                    to_addr: first.target_address.parse().unwrap(),
                    start_time,
                    finish_time: std::time::Instant::now(),
                    expire_at: std::time::Instant::now(),
                },
            );

            return Ok(Response::new(MigrateShardResponse {}));
        }

        if first.direction == migrate_shard_request::Direction::To as i32 {
            let shard = self.get_shard(first.shard_id_to).await?;

            while let Some(result) = in_stream.next().await {
                if result.is_err() {
                    break;
                }

                let piece = result.unwrap();
                if !param_check::migrate_shard_param_check(&piece) {
                    return Err(Status::invalid_argument(""));
                }

                for kv in piece.entries.into_iter() {
                    shard.kv_store.kv_set(&kv.key, &kv.value).await.unwrap();
                }
            }

            return Ok(Response::new(MigrateShardResponse {}));
        }

        return Err(Status::invalid_argument(""));
    }

    async fn split_shard(
        &self,
        req: Request<SplitShardRequest>,
    ) -> Result<Response<SplitShardResponse>, Status> {
        if !param_check::split_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        SHARD_MANAGER
            .split_shard(req.get_ref().shard_id, req.get_ref().new_shard_id)
            .await
            .unwrap();

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

        let _ = SHARD_MANAGER.merge_shard(shard_id_a, shard_id_b).await;

        Ok(Response::new(MergeShardResponse {}))
    }

    async fn start_tx(
        &self,
        _req: Request<StartTxRequest>,
    ) -> Result<Response<StartTxResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn lock_record(
        &self,
        req: Request<LockRecordRequest>,
    ) -> Result<Response<LockRecordResponse>, Status> {
        if !param_check::lock_record_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        let shard_id = req.get_ref().shard_id;
        let _shard = self.get_shard(shard_id).await?;

        // let res = shard.read().await.get(req.get_ref().key.as_ref());
        // if let Ok(_) = res {

        // }

        return Ok(Response::new(LockRecordResponse {}));
    }

    async fn lock_range(
        &self,
        _req: Request<LockRangeRequest>,
    ) -> Result<Response<LockRangeResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn prepare_tx(
        &self,
        _req: Request<PrepareTxRequest>,
    ) -> Result<Response<PrepareTxResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn commit_tx(
        &self,
        _req: Request<CommitTxRequest>,
    ) -> Result<Response<CommitTxResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn cancel_tx(
        &self,
        _req: Request<CancelTxRequest>,
    ) -> Result<Response<CancelTxResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

impl RealDataServer {
    async fn get_shard(&self, shard_id: u64) -> Result<Arc<Shard>, Status> {
        let shard = SHARD_MANAGER
            .get_shard(shard_id)
            .await
            .map_err(|_| Status::not_found("no such shard"))?;

        if shard.is_deleting() {
            return Err(Status::not_found("shard is deleted"));
        }

        if shard.is_installing_snapshot() {
            return Err(Status::not_found("shard is repairing"));
        }

        Ok(shard)
    }
}
