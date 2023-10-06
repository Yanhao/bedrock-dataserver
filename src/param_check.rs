use std::net::{AddrParseError, SocketAddr};

use tracing::warn;

use idl_gen::service_pb::{
    AbortTxRequest, CommitTxRequest, CreateShardRequest, DeleteShardRequest, KvGetRequest,
    KvScanRequest, KvSetRequest, MergeShardRequest, MigrateShardRequest, ShardAppendLogRequest,
    ShardInfoRequest, ShardInstallSnapshotRequest, ShardLockRequest, SplitShardRequest,
    TransferShardLeaderRequest,
};

pub fn create_shard_param_check(req: &CreateShardRequest) -> bool {
    if !req.leader.is_empty() {
        let res: Result<SocketAddr, AddrParseError> = req.leader.parse();
        if let Err(e) = res {
            warn!("create_shard_param_check failed, err: {:?}", e);
            return false;
        }
    }

    for r in req.replicates.iter() {
        let res: Result<SocketAddr, AddrParseError> = r.parse();
        if let Err(e) = res {
            warn!("create_shard_param_check failed, err: {:?}", e);
            return false;
        }
    }

    true
}

pub fn delete_shard_param_check(_req: &DeleteShardRequest) -> bool {
    true
}

pub fn shard_info_param_check(_req: &ShardInfoRequest) -> bool {
    true
}

pub fn shard_append_log_param_check(_req: &ShardAppendLogRequest) -> bool {
    true
}

pub fn shard_install_snapshot_param_check(_req: &ShardInstallSnapshotRequest) -> bool {
    true
}

pub fn transfer_shard_leader_param_check(req: &TransferShardLeaderRequest) -> bool {
    for addr in req.replicates.iter() {
        if addr.parse::<SocketAddr>().is_err() {
            return false;
        }
    }

    true
}

pub fn split_shard_param_check(_req: &SplitShardRequest) -> bool {
    true
}

pub fn merge_shard_param_check(_req: &MergeShardRequest) -> bool {
    true
}

pub fn migrate_shard_param_check(_req: &MigrateShardRequest) -> bool {
    true
}

pub fn kv_get_param_check(_req: &KvGetRequest) -> bool {
    true
}

pub fn kv_scan_param_check(_req: &KvScanRequest) -> bool {
    true
}

pub fn kv_set_param_check(_req: &KvSetRequest) -> bool {
    true
}

pub fn shard_lock_param_check(_req: &ShardLockRequest) -> bool {
    true
}

pub fn commit_tx_param_check(_req: &CommitTxRequest) -> bool {
    true
}

pub fn abort_tx_param_check(_req: &AbortTxRequest) -> bool {
    true
}
