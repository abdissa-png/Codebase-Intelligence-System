//! **Phase 2.4** — reconciliation tracker integration with merge and query meta.

use cis_core::{
    merge_reconciliation_job_id, phase_c_reconcile_edges, BranchReconciliationTracker, CisMcpRuntime,
    InMemoryGraph, MergeStrategy, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
    Language, SourceSpan,
};
use cis_wal::{BranchId, IdentityId, MergeId, NodeRevisionId};
use std::sync::Arc;

fn branch_hex(b: BranchId) -> String {
    b.0.iter().map(|x| format!("{:02x}", x)).collect()
}

fn bind(kv: &cis_core::MemoryKv, branch: BranchId, identity: IdentityId, rev: NodeRevisionId) {
    let key = cis_core::revision_binding_kv_key(branch, identity);
    kv.set(&key, rev.0.to_vec());
}

#[test]
fn merge_reconciliation_pending_reflects_edge_regen_backlog() {
    let target = BranchId([0u8; 16]);
    let source = BranchId([2u8; 16]);
    let i1 = IdentityId([11u8; 16]);
    let r_base = NodeRevisionId([21u8; 16]);
    let r_theirs = NodeRevisionId([23u8; 16]);

    let rt = CisMcpRuntime::new_dev("/tmp");
    {
        let mut g = rt.graph_mutex().write();
        g.put_identity(NodeIdentity {
            identity_id: i1,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id: r_base,
            identity_id: i1,
            branch_id: target,
            status: RevisionStatus::Active,
            qualified_name: "f".into(),
            file_path: "f.py".into(),
            body_hash: [10u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            tombstoned_at_ms: None,
            span: SourceSpan::UNKNOWN,
        });
        g.put_revision(NodeRevision {
            revision_id: r_theirs,
            identity_id: i1,
            branch_id: source,
            status: RevisionStatus::Active,
            qualified_name: "f".into(),
            file_path: "f.py".into(),
            body_hash: [30u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            tombstoned_at_ms: None,
            span: SourceSpan::UNKNOWN,
        });
    }
    bind(rt.kv().as_ref(), target, i1, r_base);
    bind(rt.kv().as_ref(), source, i1, r_theirs);

    let resp = rt
        .merge_branch(
            0,
            &branch_hex(source),
            &branch_hex(target),
            Some(MergeStrategy::Theirs),
            None,
            None,
        )
        .unwrap();

    assert!(
        resp.needs_edge_regen_count > 0,
        "foreign-branch promoted revision should leave edge-regen backlog without on-disk body"
    );
    assert!(
        resp.meta.background_reconciliation_pending,
        "pending flag should track non-empty edge-regen backlog"
    );
}

#[test]
fn phase_c_leaves_reconciliation_pending_when_edge_regen_backlog() {
    let kv = Arc::new(cis_core::MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let target = BranchId([9u8; 16]);
    let source = BranchId([8u8; 16]);
    let merge_id = MergeId([5u8; 16]);

    let f1 = IdentityId([1u8; 16]);
    let f2 = IdentityId([2u8; 16]);
    let r1 = NodeRevisionId([1u8; 16]);
    let r2 = NodeRevisionId([2u8; 16]);

    g.put_identity(NodeIdentity {
        identity_id: f1,
        kind: NodeKind::Function,
    });
    g.put_identity(NodeIdentity {
        identity_id: f2,
        kind: NodeKind::Function,
    });
    g.put_revision(NodeRevision {
        revision_id: r1,
        identity_id: f1,
        branch_id: target,
        status: RevisionStatus::Active,
        qualified_name: "local_fn".into(),
        file_path: "local.py".into(),
        body_hash: [10u8; 32],
        signature_hash: [0u8; 32],
        language: Language::Python,
        parent_revision_id: None,
        rename_source_id: None,
        tombstoned_at_ms: None,
        span: SourceSpan::UNKNOWN,
    });
    g.put_revision(NodeRevision {
        revision_id: r2,
        identity_id: f2,
        branch_id: source,
        status: RevisionStatus::Active,
        qualified_name: "foreign_fn".into(),
        file_path: "foreign.py".into(),
        body_hash: [20u8; 32],
        signature_hash: [0u8; 32],
        language: Language::Python,
        parent_revision_id: None,
        rename_source_id: None,
        tombstoned_at_ms: None,
        span: SourceSpan::UNKNOWN,
    });
    bind(&kv, target, f1, r1);
    bind(&kv, target, f2, r2);

    let tracker = BranchReconciliationTracker::new();
    let job = merge_reconciliation_job_id(merge_id);
    tracker.register(target, job);

    let promoted = vec![(f1, r1), (f2, r2)];
    let pc = phase_c_reconcile_edges(&mut g, &kv, target, &promoted);
    assert!(
        !pc.needs_edge_regen.is_empty(),
        "foreign-branch promoted revision should need edge regen"
    );
    // Do not call on_job_complete — mirrors merge_branch when backlog remains.

    assert!(
        tracker.is_pending(target),
        "background_reconciliation_pending should stay true until regen worker clears backlog"
    );

    tracker.on_job_complete(target, job);
    assert!(!tracker.is_pending(target));
}
