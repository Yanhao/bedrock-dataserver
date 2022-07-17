use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use dataserver::replog_pb::{self, Entry};
use dataserver::service_pb::{
    shard_append_log_request, ShardInstallSnapshotRequest, ShardInstallSnapshotResponse,
    TransferShardLeaderRequest, TransferShardLeaderResponse,
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

use crate::connections::CONNECTIONS;
use crate::param_check;
use crate::shard::SnapShoter;
use crate::shard::{self, Fsm, ReplicateLog, SHARD_MANAGER};

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

        let fsm = match SHARD_MANAGER.read().await.get_shard_fsm(shard_id).await {
            Err(_) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(s) => s,
        };

        let a = fsm.read().await;
        let value = match a.get(req.get_ref().key.as_slice()).await {
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
        let fsm = match SHARD_MANAGER.write().await.get_shard_fsm(shard_id).await {
            Err(_) => {
                return Err(Status::not_found("no such shard"));
            }
            Ok(s) => s,
        };

        let mut entry = fsm
            .write()
            .await
            .apply(
                Entry {
                    index: 0,
                    op: "put".into(),
                    key: req.get_ref().key.clone(),
                    value: req.get_ref().value.clone(),
                },
                true,
            )
            .await
            .unwrap();

        entry.wait().await;

        // let replicates = fsm.read().await.shard.read().await.get_replicates();

        // for addr in replicates {
        //     let client = CONNECTIONS
        //         .write()
        //         .await
        //         .get_conn(addr.to_string())
        //         .await
        //         .unwrap();

        //     client
        //         .write()
        //         .await
        //         .shard_append_log(ShardAppendLogRequest {
        //             shard_id,
        //             entries: vec![shard_append_log_request::Entry {
        //                 index: entry.entry.index,
        //                 op: "put".into(),
        //                 key: entry.entry.key.clone(),
        //                 value: entry.entry.value.clone(),
        //             }],
        //         })
        //         .await
        //         .unwrap();
        // }

        Ok(Response::new(ShardWriteResponse::default()))
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
        let new_shard = shard::Shard {
            shard_id,
            storage_id: req.get_ref().storage_id,
            create_ts: req.get_ref().create_ts.to_owned().unwrap().into(),
            leader: if req.get_ref().leader == "" {
                None
            } else {
                Some(req.get_ref().leader.parse().unwrap())
            },
            leader_change_ts: req.get_ref().leader_change_ts.to_owned().unwrap().into(),
            replicates_update_ts: req.get_ref().replica_update_ts.to_owned().unwrap().into(),
            replicates,

            kv_data: RwLock::new(HashMap::new()),
        };

        if let Err(e) = SHARD_MANAGER
            .write()
            .await
            .create_shard_fsm(Fsm::new(
                new_shard,
                Arc::new(RwLock::new(ReplicateLog::new())),
            ))
            .await
        {
            warn!("failed to create shard, {:?}", e);
            return Err(Status::invalid_argument(""));
        }

        let fsm = SHARD_MANAGER
            .write()
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
            .write()
            .await
            .get_shard_fsm(shard_id)
            .await
            .unwrap();

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

        for e in req.get_ref().entries.iter() {
            fsm.write()
                .await
                .apply(
                    replog_pb::Entry {
                        op: e.op.clone(),
                        index: e.index,
                        key: e.key.clone(),
                        value: e.value.clone(),
                    },
                    false,
                )
                .await
                .unwrap();
        }

        let last_index = fsm.read().await.last_index().await;

        let resp = Response::new(ShardAppendLogResponse {
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
        let mut first_piece = true;

        while let Some(result) = in_stream.next().await {
            if let Err(_) = result {
                break;
            }
            let piece = result.unwrap();

            if !param_check::shard_install_snapshot_param_check(&piece) {
                return Err(Status::invalid_argument(""));
            }

            let shard_id = piece.shard_id;

            let fsm = SHARD_MANAGER
                .read()
                .await
                .get_shard_fsm(shard_id)
                .await
                .unwrap();

            if first_piece {
                fsm.as_ref().write().await.shard.write().await.clear().await;
                first_piece = false;
            }

            fsm.as_ref()
                .write()
                .await
                .shard
                .write()
                .await
                .install_snapshot(&piece.data_piece)
                .await
                .unwrap();
        }

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
            Ok(s) => s,
        };

        let mut socks = Vec::<SocketAddr>::new();
        for rep in req.get_ref().replicates.iter() {
            let addr: SocketAddr = rep.parse().unwrap();
            socks.push(addr);
        }

        fsm.write()
            .await
            .shard
            .write()
            .await
            .set_replicates(&socks)
            .unwrap();

        Ok(Response::new(TransferShardLeaderResponse {}))
    }
}
