//! Durable merge_lock must not stick forever after crash recovery.

use std::sync::Arc;

use cis_core::{
    acquire_merge_lock, merge_lock_holder, MergeSagaOrchestrator, OptimisticPatcher,
    PathLeaseManager, SessionId, SpeculativePathTracker,
};
use cis_wal::{BranchId, MergeId};

#[test]
fn compensate_orphans_releases_stuck_merge_lock() {
    let kv = Arc::new(cis_core::MemoryKv::new());
    let branch = BranchId([1u8; 16]);
    let merge_id = MergeId([2u8; 16]);
    acquire_merge_lock(kv.as_ref(), branch, merge_id).unwrap();
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    saga.persist(merge_id, cis_core::SagaPhase::Classifying);
    assert_eq!(merge_lock_holder(kv.as_ref(), branch), Some(merge_id));

    let n = saga.compensate_orphans();
    assert!(n >= 1);
    assert!(
        merge_lock_holder(kv.as_ref(), branch).is_none(),
        "merge_lock must be released when saga is compensated"
    );

    // Speculative write must no longer be blocked by a stuck lock.
    let leases = Arc::new(PathLeaseManager::new());
    let spec = Arc::new(SpeculativePathTracker::new());
    let patcher = OptimisticPatcher::new(leases, spec);
    patcher
        .apply_speculative(
            SessionId(1),
            vec!["src/a.rs".into()],
            Some((kv.as_ref(), branch)),
        )
        .expect("speculative write must succeed after lock release");
}

#[test]
fn compensate_orphans_releases_lock_without_saga() {
    let kv = Arc::new(cis_core::MemoryKv::new());
    let branch = BranchId([3u8; 16]);
    let merge_id = MergeId([4u8; 16]);
    acquire_merge_lock(kv.as_ref(), branch, merge_id).unwrap();
    // No saga_state row — orphan lock only.
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    let n = saga.compensate_orphans();
    assert!(n >= 1);
    assert!(merge_lock_holder(kv.as_ref(), branch).is_none());
}
