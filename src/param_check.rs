use std::net::{AddrParseError, SocketAddr};

use tracing::warn;

use idl_gen::service_pb::{
    CreateShardRequest, DeleteShardRequest, LockRangeRequest, LockRecordRequest, MergeShardRequest,
    MigrateShardRequest, ShardAppendLogRequest, ShardInfoRequest, ShardInstallSnapshotRequest,
    ShardReadRequest, ShardScanRequest, ShardWriteRequest, SplitShardRequest,
    TransferShardLeaderRequest,
};

pub fn shard_read_param_check(req: &ShardReadRequest) -> bool {
    return true;
}

pub fn shard_write_param_check(req: &ShardWriteRequest) -> bool {
    return true;
}

pub fn shard_scan_param_check(req: &ShardScanRequest) -> bool {
    return true;
}

pub fn create_shard_param_check(req: &CreateShardRequest) -> bool {
    if req.leader != "" {
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

    return true;
}

pub fn delete_shard_param_check(req: &DeleteShardRequest) -> bool {
    return true;
}

pub fn shard_info_param_check(req: &ShardInfoRequest) -> bool {
    return true;
}

pub fn shard_append_log_param_check(req: &ShardAppendLogRequest) -> bool {
    return true;
}

pub fn shard_install_snapshot_param_check(req: &ShardInstallSnapshotRequest) -> bool {
    return true;
}

pub fn transfer_shard_leader_param_check(req: &TransferShardLeaderRequest) -> bool {
    for addr in req.replicates.iter() {
        if let Err(e) = addr.parse::<SocketAddr>() {
            return false;
        }
    }

    return true;
}

pub fn split_shard_param_check(req: &SplitShardRequest) -> bool {
    return true;
}

pub fn merge_shard_param_check(req: &MergeShardRequest) -> bool {
    return true;
}

pub fn migrate_shard_param_check(req: &MigrateShardRequest) -> bool {
    return true;
}

pub fn lock_record_param_check(req: &LockRecordRequest) -> bool {
    return true;
}

pub fn lock_range_param_check(req: &LockRangeRequest) -> bool {
    return true;
}
