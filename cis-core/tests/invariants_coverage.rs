//! Invariant checks should not emit duplicate signals for one condition.

use std::sync::Arc;

use cis_core::{
    check_merge_invariants, check_write_path_invariants, MergeViolation, OptimisticPatcher,
    PathLeaseManager, SessionId, SpeculativePathTracker, WriteCoordinator, WritePathViolation,
};
use cis_wal::{BranchId, MergeId, MutationLog, MutationLogStore};

#[test]
fn lease_without_patch_emits_single_violation() {
    let leases = Arc::new(PathLeaseManager::new());
    let spec = Arc::new(SpeculativePathTracker::new());
    let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
    leases.acquire("orphan.rs", SessionId(1)).unwrap();

    let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(wal);
    let graph = coord.graph();
    let violations = check_write_path_invariants(graph, &leases, &patcher, &spec);
    let lease_violations: Vec<_> = violations
        .iter()
        .filter(|v| {
            matches!(
                v,
                WritePathViolation::LeaseWithoutPatch { .. } | WritePathViolation::OrphanLease { .. }
            )
        })
        .collect();
    assert_eq!(
        lease_violations.len(),
        1,
        "duplicate lease signals: {:?}",
        violations
    );
}

#[test]
fn merge_invariant_uses_coordinator_not_ready() {
    let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(wal);
    // Do not call reconcile_on_startup — coordinator stays not-ready.
    assert!(!coord.is_ready());
    let kv = Arc::new(cis_core::MemoryKv::new());
    let branch = BranchId([1u8; 16]);
    let mid = MergeId([2u8; 16]);
    cis_core::acquire_merge_lock(kv.as_ref(), branch, mid).unwrap();
    let violations = check_merge_invariants(coord.graph(), kv.as_ref(), Some(&coord));
    assert!(
        violations
            .iter()
            .any(|v| matches!(v, MergeViolation::CoordinatorNotReadyWhileMergeLocked { .. })),
        "expected CoordinatorNotReadyWhileMergeLocked, got {:?}",
        violations
    );
}
