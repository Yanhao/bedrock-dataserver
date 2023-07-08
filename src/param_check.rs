use std::net::{AddrParseError, SocketAddr};

use tracing::warn;

use idl_gen::service_pb::{
    CreateShardRequest, DeleteShardRequest, LockRangeRequest, LockRecordRequest, MergeShardRequest,
    MigrateShardRequest, ShardAppendLogRequest, ShardInfoRequest, ShardInstallSnapshotRequest,
    ShardReadRequest, ShardScanRequest, ShardWriteRequest, SplitShardRequest,
    TransferShardLeaderRequest,
};

pub fn shard_read_param_check(_req: &ShardReadRequest) -> bool {
    true
}

pub fn shard_write_param_check(_req: &ShardWriteRequest) -> bool {
    true
}

pub fn shard_scan_param_check(_req: &ShardScanRequest) -> bool {
    true
}

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

pub fn lock_record_param_check(_req: &LockRecordRequest) -> bool {
    true
}

pub fn lock_range_param_check(_req: &LockRangeRequest) -> bool {
    true
}
