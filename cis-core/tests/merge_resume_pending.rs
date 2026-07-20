//! Unresolved merges without a strategy must remain pending after recovery.

use std::sync::Arc;

use cis_core::{
    acquire_merge_lock, merge_lock_holder, phase_a_classify, recover_inflight_merges, resume_merge,
    revision_binding_kv_key, BodyStore, InMemoryGraph, InMemoryVectorStore, Language, MergeContext,
    MergeControl, MergeRecoveryGate, MergeSagaOrchestrator, MergeWorkflowStatus, NodeIdentity,
    NodeKind, NodeRevision, ResumePendingResolution, RevisionStatus, SagaPhase, SourceSpan,
};
use cis_wal::{BranchId, IdentityId, MergeId, NodeRevisionId};

fn rid(x: u8) -> NodeRevisionId {
    let mut b = [0u8; 16];
    b[15] = x;
    NodeRevisionId(b)
}
fn iid(x: u8) -> IdentityId {
    let mut b = [0u8; 16];
    b[14] = x;
    IdentityId(b)
}
fn make_rev(
    revision_id: NodeRevisionId,
    identity_id: IdentityId,
    branch_id: BranchId,
    name: &str,
    body: [u8; 32],
) -> NodeRevision {
    NodeRevision {
        revision_id,
        identity_id,
        branch_id,
        status: RevisionStatus::Active,
        qualified_name: name.into(),
        file_path: "a.py".into(),
        body_hash: body,
        signature_hash: body,
        language: Language::Python,
        parent_revision_id: None,
        rename_source_id: None,
        tombstoned_at_ms: None,
        span: SourceSpan::UNKNOWN,
    }
}
fn bind(kv: &cis_core::MemoryKv, branch: BranchId, id: IdentityId, rev: NodeRevisionId) {
    kv.set(&revision_binding_kv_key(branch, id), rev.0.to_vec());
}

#[test]
fn resume_pending_resolution_does_not_cancel() {
    let kv = Arc::new(cis_core::MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);
    let i1 = iid(1);
    let r_base = rid(1);
    let r_ours = rid(2);
    let r_theirs = rid(3);
    g.put_identity(NodeIdentity {
        identity_id: i1,
        kind: NodeKind::Function,
    });
    g.put_revision(make_rev(r_base, i1, base, "f", [10u8; 32]));
    g.put_revision(make_rev(r_ours, i1, ours, "f", [20u8; 32]));
    g.put_revision(make_rev(r_theirs, i1, theirs, "f", [30u8; 32]));
    bind(&kv, base, i1, r_base);
    bind(&kv, ours, i1, r_ours);
    bind(&kv, theirs, i1, r_theirs);

    let phase_a = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(phase_a.report.status, MergeWorkflowStatus::RequiresResolution);

    let merge_id = MergeId([9u8; 16]);
    acquire_merge_lock(kv.as_ref(), ours, merge_id).unwrap();
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    saga.persist(merge_id, SagaPhase::Classifying);
    MergeContext {
        merge_id,
        ours_branch: ours,
        theirs_branch: theirs,
        base_branch: base,
        target_branch: ours,
        strategy: None,
    }
    .persist(kv.as_ref());

    let err = resume_merge(&mut g, kv.as_ref(), None, merge_id, &saga, None).unwrap_err();
    assert_eq!(err, ResumePendingResolution);
    assert_eq!(saga.load(merge_id), Some(SagaPhase::Classifying));
    assert_eq!(merge_lock_holder(kv.as_ref(), ours), Some(merge_id));

    let merge_control = MergeControl::new(Arc::clone(&kv));
    let body = BodyStore::new(Arc::clone(&kv));
    let gate = MergeRecoveryGate::new(Arc::clone(&kv));
    let vector = InMemoryVectorStore::new();
    let report = recover_inflight_merges(
        &mut g,
        kv.as_ref(),
        &body,
        &saga,
        &merge_control,
        &vector,
        &gate,
        None,
    );
    assert_eq!(report.pending_resolution, 1);
    assert_eq!(report.compensated, 0);
    assert_eq!(saga.load(merge_id), Some(SagaPhase::Classifying));
    assert_eq!(merge_lock_holder(kv.as_ref(), ours), Some(merge_id));
}
