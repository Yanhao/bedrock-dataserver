use std::net::SocketAddr;
use std::sync::Arc;
use std::time;

use anyhow::Result;
use dataserver::replog_pb::{self, Entry};
use dataserver::service_pb::{
    ShardInstallSnapshotRequest, ShardInstallSnapshotResponse, TransferShardLeaderRequest,
    TransferShardLeaderResponse,
};
use futures::StreamExt;
use log::{debug, info, warn};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use dataserver::service_pb::data_service_server::DataService;
use dataserver::service_pb::{
    CreateShardRequest, CreateShardResponse, DeleteShardRequest, ShardAppendLogRequest,
    ShardAppendLogResponse, ShardReadRequest, ShardReadResponse, ShardWriteRequest,
    ShardWriteResponse,
};

use crate::param_check;
use crate::shard::{self, Fsm, SHARD_MANAGER};
use crate::wal::WalManager;

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
        let fsm = match SHARD_MANAGER.write().await.load_shard_fsm(shard_id).await {
            Err(_) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(s) => s,
        };

        if fsm.read().await.is_deleting() {
            return Err(Status::not_found("shard is deleted"));
        }

        if fsm.read().await.is_instaling_snapshot() {
            return Err(Status::not_found("shard is repairing"));
        }

        let shard = fsm.read().await.get_shard();

        let value = match shard.read().await.get(req.get_ref().key.as_slice()).await {
            Err(_) => return Err(Status::not_found("no such key")),
            Ok(v) => v,
        };

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
        let fsm = match SHARD_MANAGER.write().await.load_shard_fsm(shard_id).await {
            Err(_) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(s) => s,
        };

        if fsm.read().await.is_deleting() {
            return Err(Status::not_found("shard is deleted"));
        }

        if fsm.read().await.is_leader().await {
            return Err(Status::unavailable("not leader"));
        }

        let mut entry_notifier = fsm
            .write()
            .await
            .process_wirte(Entry {
                index: 0,
                op: "put".into(),
                key: req.get_ref().key.clone(),
                value: req.get_ref().value.clone(),
            })
            .await
            .unwrap();

        if let Err(shard::ShardError::NotLeader) = entry_notifier.wait_result().await {
            return Ok(Response::new(ShardWriteResponse { not_leader: true }));
        }

        return Ok(Response::new(ShardWriteResponse::default()));
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

        let new_shard = shard::Shard::create_shard(&req).await;

        if let Err(e) = SHARD_MANAGER
            .write()
            .await
            .create_shard_fsm(Fsm::new(
                new_shard,
                Arc::new(RwLock::new(
                    WalManager::load_wal_by_shard_id(shard_id).await.unwrap(),
                )),
            ))
            .await
        {
            warn!("failed to create shard, {:?}", e);
            return Err(Status::invalid_argument(""));
        }

        let fsm = SHARD_MANAGER
            .read()
            .await
            .get_shard_fsm(shard_id)
            .await
            .unwrap();

        fsm.write().await.start().await.unwrap();

        info!("create shard successfully, req: {:?}", req);

        Ok(Response::new(resp))
    }

    async fn delete_shard(&self, req: Request<DeleteShardRequest>) -> Result<Response<()>, Status> {
        if !param_check::delete_shard_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("delete shard, req: {:?}", req);

        let shard_id = req.get_ref().shard_id;

        let fsm = SHARD_MANAGER
            .read()
            .await
            .get_shard_fsm(shard_id)
            .await
            .unwrap();

        fsm.read().await.mark_deleting();

        fsm.write().await.stop().await;

        SHARD_MANAGER
            .write()
            .await
            .remove_shard_fsm(shard_id)
            .await
            .unwrap();

        Ok(Response::new(()))
    }

    async fn shard_append_log(
        &self,
        req: Request<ShardAppendLogRequest>,
    ) -> Result<Response<ShardAppendLogResponse>, Status> {
        if !param_check::shard_append_log_param_check(req.get_ref()) {
            return Err(Status::invalid_argument(""));
        }

        info!("shard append log entries, req: {:?}", req);

        let fsm = SHARD_MANAGER
            .read()
            .await
            .get_shard_fsm(req.get_ref().shard_id)
            .await
            .unwrap();

        let leader_change_leader_ts: time::SystemTime =
            req.get_ref().leader_change_ts.to_owned().unwrap().into();

        let shard = fsm.read().await.get_shard();
        if shard.read().await.get_leader_change_ts() > leader_change_leader_ts {
            let resp = Response::new(ShardAppendLogResponse {
                is_old_leader: true,
                last_applied_index: 0,
            });
            return Ok(resp);
        }

        shard
            .write()
            .await
            .update_leader_change_ts(leader_change_leader_ts);

        for e in req.get_ref().entries.iter() {
            fsm.write()
                .await
                .apply_entry(replog_pb::Entry {
                    op: e.op.clone(),
                    index: e.index,
                    key: e.key.clone(),
                    value: e.value.clone(),
                })
                .await
                .unwrap();
        }

        let last_index = fsm.read().await.last_index().await;

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

        let fsm = SHARD_MANAGER
            .read()
            .await
            .get_shard_fsm(first_piece.shard_id)
            .await
            .unwrap();
        let shard = fsm.read().await.get_shard();

        fsm.write().await.set_instaling_snapshot(true);

        while let Some(result) = in_stream.next().await {
            if let Err(_) = result {
                break;
            }
            let piece = result.unwrap();

            if !param_check::shard_install_snapshot_param_check(&piece) {
                fsm.write().await.set_instaling_snapshot(false);
                return Err(Status::invalid_argument(""));
            }

            shard.write().await.clear_data().await;
            shard
                .write()
                .await
                .kv_install_snapshot(&piece.data_piece)
                .await
                .unwrap();
        }

        fsm.write().await.reset_replog(last_wal_index).unwrap();
        fsm.write().await.set_instaling_snapshot(false);

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
        let fsm = match SHARD_MANAGER.read().await.get_shard_fsm(shard_id).await {
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

        let shard = fsm.read().await.get_shard();
        shard.write().await.set_replicates(&socks).unwrap();
        shard.write().await.set_is_leader(true);
        shard
            .write()
            .await
            .update_leader_change_ts(req.get_ref().leader_change_ts.to_owned().unwrap().into());

        Ok(Response::new(TransferShardLeaderResponse {}))
    }
}
