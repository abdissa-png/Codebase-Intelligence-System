//! Resume must run before orphan compensation on the periodic path.

use std::sync::Arc;
use std::time::Duration;

use cis_core::{
    acquire_merge_lock, merge_lock_holder, BodyStore, MergeControl, MergeRecoveryGate,
    MergeSagaOrchestrator, PeriodicReconciler, ProductionAuditSink, WriteCoordinator,
};
use cis_wal::{BranchId, MergeId, MutationLog, MutationLogStore};

#[test]
fn reconcile_now_does_not_purge_sagas() {
    let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(cis_core::MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    let mid = MergeId([9u8; 16]);
    saga.persist(mid, cis_core::SagaPhase::Intent);
    let _ = coord.reconcile_now(&saga);
    assert!(
        saga.load(mid).is_some(),
        "reconcile_now must not purge sagas (resume happens first elsewhere)"
    );
}

#[test]
fn periodic_reconciler_compensates_orphans_after_recover() {
    let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(cis_core::MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    let branch = BranchId([5u8; 16]);
    let mid = MergeId([6u8; 16]);
    acquire_merge_lock(kv.as_ref(), branch, mid).unwrap();
    saga.persist(mid, cis_core::SagaPhase::Classifying);

    let merge_control = MergeControl::new(Arc::clone(&kv));
    let body = BodyStore::new(Arc::clone(&kv));
    let gate = MergeRecoveryGate::new(Arc::clone(&kv));
    let dir = tempfile::tempdir().unwrap();
    let audit = ProductionAuditSink::new(dir.path().join("audit.jsonl"));
    let periodic = PeriodicReconciler::new(Duration::from_secs(1));
    let _report = periodic.run_once(
        &coord,
        &saga,
        kv.as_ref(),
        &merge_control,
        &body,
        &gate,
        &audit,
        &[branch],
        None,
    );
    assert!(
        merge_lock_holder(kv.as_ref(), branch).is_none(),
        "lock must be cleared after periodic recover+compensate"
    );
    assert!(
        saga.load(mid).is_none(),
        "orphan saga must be gone after periodic path"
    );
}
