//! Integration tests for Phase 4: Merge Engine (Phase A/B/C end-to-end).

use std::sync::Arc;

use cis_core::{
    phase_a_classify, phase_b_promote, phase_c_reconcile_edges, phase_c_reconcile_edges_full,
    recover_inflight_merges, resume_merge,
    BodyStore, InMemoryGraph, MergeContext, MergeIdentityClass, MergePreflight, MergeStrategy,
    MergeSagaOrchestrator, MergeWorkflowStatus, MemoryKv, SagaPhase,
};
use cis_core::{
    EdgeResolution, EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision,
    RevisionStatus, SourceSpan, SourceType, file_body_hash_key, stable_id_bytes, stable_rev_id_bytes,
};
use cis_core::revision_binding_kv_key;
use cis_wal::{BranchId, IdentityId, MergeId, NodeRevisionId};

fn iid(b: u8) -> IdentityId {
    let mut x = [0u8; 16];
    x[15] = b;
    IdentityId(x)
}

fn rid(b: u8) -> NodeRevisionId {
    let mut x = [0u8; 16];
    x[14] = b;
    NodeRevisionId(x)
}

fn make_rev(
    revision_id: NodeRevisionId,
    identity_id: IdentityId,
    branch_id: BranchId,
    qn: &str,
    body_hash: [u8; 32],
    sig_hash: [u8; 32],
) -> NodeRevision {
    NodeRevision {
        revision_id,
        identity_id,
        branch_id,
        status: RevisionStatus::Active,
        qualified_name: qn.into(),
        file_path: "test.py".into(),
        body_hash,
        signature_hash: sig_hash,
        language: Language::Python,
        parent_revision_id: None,
        rename_source_id: None,
        span: SourceSpan::UNKNOWN,
        tombstoned_at_ms: None,
    }
}

fn bind(kv: &MemoryKv, branch: BranchId, identity: IdentityId, rev: NodeRevisionId) {
    let key = revision_binding_kv_key(branch, identity);
    kv.set(&key, rev.0.to_vec());
}

fn make_edge(
    eid: [u8; 16],
    src: NodeRevisionId,
    tgt: IdentityId,
    sig_hash: [u8; 32],
) -> GraphEdge {
    GraphEdge {
        edge_id: eid,
        ty: EdgeType::Calls,
        source_revision_id: src,
        target_identity_id: tgt,
        resolution: EdgeResolution {
            target_signature_hash: sig_hash,
            resolver: SourceType::Ast,
            last_validation_ms: 0,
        },
        anchor: SourceSpan::UNKNOWN,
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: Non-overlapping changes merge cleanly
// ---------------------------------------------------------------------------
#[test]
fn merge_non_overlapping_branches() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    // Base: two functions
    let f1 = iid(1);
    let f2 = iid(2);
    let r1 = rid(1);
    let r2 = rid(2);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_identity(NodeIdentity { identity_id: f2, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "alpha", [1u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r2, f2, base, "beta", [2u8; 32], [0u8; 32]));
    bind(&kv, base, f1, r1);
    bind(&kv, base, f2, r2);
    bind(&kv, ours, f1, r1);
    bind(&kv, ours, f2, r2);
    bind(&kv, theirs, f1, r1);
    bind(&kv, theirs, f2, r2);

    // Ours modifies alpha, theirs modifies beta
    let r1_ours = rid(11);
    let r2_theirs = rid(22);
    g.put_revision(make_rev(r1_ours, f1, ours, "alpha", [11u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r2_theirs, f2, theirs, "beta", [22u8; 32], [0u8; 32]));
    bind(&kv, ours, f1, r1_ours);
    bind(&kv, theirs, f2, r2_theirs);

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(pa.report.status, MergeWorkflowStatus::Ready);
    assert_eq!(pa.report.conflicts.len(), 0);

    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    assert_eq!(pb.promoted.len(), 2);
    assert!(pb.unresolved_conflicts.is_empty());

    let pc = phase_c_reconcile_edges(&mut g, &kv, ours, &pb.promoted);
    assert_eq!(pc.dangling_edges_removed, 0);

    // Verify target bindings
    let key_f1 = revision_binding_kv_key(ours, f1);
    let key_f2 = revision_binding_kv_key(ours, f2);
    assert_eq!(kv.get(&key_f1), Some(r1_ours.0.to_vec()));
    assert_eq!(kv.get(&key_f2), Some(r2_theirs.0.to_vec()));
}

// ---------------------------------------------------------------------------
// Scenario 2: BOTH_MODIFIED surfaces RequiresResolution, then resolved
// ---------------------------------------------------------------------------
#[test]
fn both_modified_conflict_surfaces_and_resolves() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    let f1 = iid(1);
    let r_base = rid(1);
    let r_ours = rid(2);
    let r_theirs = rid(3);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r_base, f1, base, "conflict_fn", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_ours, f1, ours, "conflict_fn", [20u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_theirs, f1, theirs, "conflict_fn", [30u8; 32], [0u8; 32]));
    bind(&kv, base, f1, r_base);
    bind(&kv, ours, f1, r_ours);
    bind(&kv, theirs, f1, r_theirs);

    // Phase A without strategy → RequiresResolution
    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(pa.report.status, MergeWorkflowStatus::RequiresResolution);
    assert_eq!(pa.report.conflicts.len(), 1);

    // Phase B without strategy → unresolved
    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    assert_eq!(pb.unresolved_conflicts.len(), 1);
    assert!(pb.promoted.is_empty());

    // Phase B with Ours → resolved
    let pb2 = phase_b_promote(&kv, &mut g, ours, &pa.classified, Some(MergeStrategy::Ours));
    assert_eq!(pb2.promoted.len(), 1);
    assert_eq!(pb2.promoted[0], (f1, r_ours));
    assert_eq!(pb2.orphaned_revisions, vec![r_theirs]);
    assert_eq!(g.get_revision(r_theirs).unwrap().status, RevisionStatus::Orphaned);
}

// ---------------------------------------------------------------------------
// Scenario 3: Theirs adds new functions
// ---------------------------------------------------------------------------
#[test]
fn theirs_new_symbols_promoted_to_target() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    // Base: one function
    let f1 = iid(1);
    let r1 = rid(1);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "existing_fn", [1u8; 32], [0u8; 32]));
    bind(&kv, base, f1, r1);
    bind(&kv, ours, f1, r1);
    bind(&kv, theirs, f1, r1);

    // Theirs adds two new functions
    let f2 = iid(2);
    let f3 = iid(3);
    let r2 = rid(2);
    let r3 = rid(3);
    g.put_identity(NodeIdentity { identity_id: f2, kind: NodeKind::Function });
    g.put_identity(NodeIdentity { identity_id: f3, kind: NodeKind::Class });
    g.put_revision(make_rev(r2, f2, theirs, "new_helper", [2u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r3, f3, theirs, "NewClass", [3u8; 32], [0u8; 32]));
    bind(&kv, theirs, f2, r2);
    bind(&kv, theirs, f3, r3);

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(pa.report.status, MergeWorkflowStatus::Ready);

    // Count classifications
    let new_count = pa.classified.iter().filter(|c| c.class == MergeIdentityClass::TheirsNew).count();
    assert_eq!(new_count, 2);

    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    assert_eq!(pb.promoted.len(), 3);

    // Verify new symbols are bound on target
    let key_f2 = revision_binding_kv_key(ours, f2);
    let key_f3 = revision_binding_kv_key(ours, f3);
    assert_eq!(kv.get(&key_f2), Some(r2.0.to_vec()));
    assert_eq!(kv.get(&key_f3), Some(r3.0.to_vec()));
}

// ---------------------------------------------------------------------------
// Scenario 4: Convergent evolution (both modified to same content)
// ---------------------------------------------------------------------------
#[test]
fn convergent_evolution_auto_resolves() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    let f1 = iid(1);
    let r_base = rid(1);
    let r_ours = rid(2);
    let r_theirs = rid(3);
    let same_body = [99u8; 32];
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r_base, f1, base, "f", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_ours, f1, ours, "f", same_body, [0u8; 32]));
    g.put_revision(make_rev(r_theirs, f1, theirs, "f", same_body, [0u8; 32]));
    bind(&kv, base, f1, r_base);
    bind(&kv, ours, f1, r_ours);
    bind(&kv, theirs, f1, r_theirs);

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(pa.report.status, MergeWorkflowStatus::Ready);
    assert_eq!(pa.classified[0].class, MergeIdentityClass::BothModifiedSame);

    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    assert_eq!(pb.promoted.len(), 1);
    assert!(pb.unresolved_conflicts.is_empty());
}

// ---------------------------------------------------------------------------
// Scenario 5: Delete in theirs, unchanged in ours → accept deletion
// ---------------------------------------------------------------------------
#[test]
fn theirs_delete_ours_unchanged_accepts_deletion() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    let f1 = iid(1);
    let r1 = rid(1);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "deleted_fn", [10u8; 32], [0u8; 32]));
    bind(&kv, base, f1, r1);
    bind(&kv, ours, f1, r1);
    // theirs does NOT bind f1 → deleted

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(pa.report.status, MergeWorkflowStatus::Ready);
    assert_eq!(pa.classified[0].class, MergeIdentityClass::TheirsOnly);

    let _pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    // f1 should be deleted from target
    let key = revision_binding_kv_key(ours, f1);
    assert!(kv.get(&key).is_none());
}

// ---------------------------------------------------------------------------
// Scenario 6: Edge dangling after merge (target deleted)
// ---------------------------------------------------------------------------
#[test]
fn phase_c_cleans_dangling_edges_after_target_deletion() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    // f1: caller, f2: callee (deleted in theirs)
    let f1 = iid(1);
    let f2 = iid(2);
    let r1 = rid(1);
    let r2 = rid(2);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_identity(NodeIdentity { identity_id: f2, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "caller", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r2, f2, base, "callee", [20u8; 32], [0u8; 32]));

    // Edge: caller → callee
    let edge = make_edge([9u8; 16], r1, f2, [0u8; 32]);
    g.replace_edges_for_revision(r1, vec![edge]).unwrap();

    bind(&kv, base, f1, r1);
    bind(&kv, base, f2, r2);
    bind(&kv, ours, f1, r1);
    bind(&kv, ours, f2, r2);
    bind(&kv, theirs, f1, r1);
    // f2 NOT bound in theirs → deleted

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);

    // f2 should be deleted from target
    let key_f2 = revision_binding_kv_key(ours, f2);
    assert!(kv.get(&key_f2).is_none());

    // Phase C should remove the dangling edge
    let pc = phase_c_reconcile_edges(&mut g, &kv, ours, &pb.promoted);
    assert_eq!(pc.dangling_edges_removed, 1);
    assert!(g.outbound_edges(r1).is_empty());
}

// ---------------------------------------------------------------------------
// Scenario 7: Signature drift detection
// ---------------------------------------------------------------------------
#[test]
fn phase_c_flags_signature_drift() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let target = BranchId([9u8; 16]);

    let f1 = iid(1);
    let f2 = iid(2);
    let r1 = rid(1);
    let r2 = rid(2);

    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_identity(NodeIdentity { identity_id: f2, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, target, "caller", [10u8; 32], [0u8; 32]));

    // callee has a new signature hash
    let mut callee_rev = make_rev(r2, f2, target, "callee", [20u8; 32], [0u8; 32]);
    callee_rev.signature_hash = [55u8; 32];
    g.put_revision(callee_rev);

    // Edge references OLD signature hash
    let edge = make_edge([9u8; 16], r1, f2, [44u8; 32]);
    g.replace_edges_for_revision(r1, vec![edge]).unwrap();

    bind(&kv, target, f1, r1);
    bind(&kv, target, f2, r2);

    let promoted = vec![(f1, r1)];
    let pc = phase_c_reconcile_edges(&mut g, &kv, target, &promoted);
    assert_eq!(pc.signature_drifts.len(), 1);
    assert!(pc.signature_drifts[0].contains("caller"));
    assert!(pc.signature_drifts[0].contains("callee"));
}

// ---------------------------------------------------------------------------
// Scenario 8: Saga phase progression
// ---------------------------------------------------------------------------
#[test]
fn saga_tracks_merge_phases() {
    let kv = Arc::new(MemoryKv::new());
    let saga = cis_core::MergeSagaOrchestrator::new(Arc::clone(&kv));
    let mid = cis_wal::MergeId([42u8; 16]);

    saga.persist(mid, cis_core::SagaPhase::Intent);
    assert_eq!(saga.load(mid), Some(cis_core::SagaPhase::Intent));

    saga.persist(mid, cis_core::SagaPhase::Classifying);
    assert_eq!(saga.load(mid), Some(cis_core::SagaPhase::Classifying));

    saga.persist(mid, cis_core::SagaPhase::Promoting);
    assert_eq!(saga.load(mid), Some(cis_core::SagaPhase::Promoting));

    saga.persist(mid, cis_core::SagaPhase::EdgeBatch { seq: 0 });
    assert_eq!(saga.load(mid), Some(cis_core::SagaPhase::EdgeBatch { seq: 0 }));

    saga.persist(mid, cis_core::SagaPhase::Committed);
    assert_eq!(saga.load(mid), Some(cis_core::SagaPhase::Committed));
}

// ---------------------------------------------------------------------------
// Scenario 9: Cancel merge restores pre-merge state
// ---------------------------------------------------------------------------
#[test]
fn cancel_merge_restores_bindings_and_releases_lock() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let branch = BranchId([1u8; 16]);
    let merge_id = cis_wal::MergeId([2u8; 16]);

    let f1 = iid(1);
    let r_old = rid(1);
    let r_new = rid(2);

    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r_old, f1, branch, "f", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_new, f1, branch, "f", [20u8; 32], [0u8; 32]));
    bind(&kv, branch, f1, r_old);

    // Lock and record pre-merge state
    cis_core::acquire_merge_lock(&kv, branch, merge_id).unwrap();
    let mc = cis_core::MergeControl::new(Arc::clone(&kv));
    mc.record_premerge_bindings(merge_id, branch, &[(f1, r_old)]);

    // Simulate merge: rebind to new revision
    bind(&kv, branch, f1, r_new);
    let key = revision_binding_kv_key(branch, f1);
    assert_eq!(kv.get(&key), Some(r_new.0.to_vec()));

    // Cancel merge
    let vs = cis_core::InMemoryVectorStore::new();
    let rep = mc.cancel_merge(merge_id, branch, &mut g, &vs, None, &[]).unwrap();
    assert_eq!(rep.restored_bindings, 1);

    // Verify old binding restored
    assert_eq!(kv.get(&key), Some(r_old.0.to_vec()));

    // Lock released
    assert!(cis_core::merge_lock_holder(&kv, branch).is_none());
}

// ---------------------------------------------------------------------------
// Scenario 10: Large merge (many identities)
// ---------------------------------------------------------------------------
#[test]
fn large_merge_with_50_identities() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    for i in 0..50u8 {
        let id = iid(i);
        let r_base = {
            let mut x = [0u8; 16]; x[14] = i; x[13] = 1; NodeRevisionId(x)
        };
        g.put_identity(NodeIdentity { identity_id: id, kind: NodeKind::Function });
        let mut body = [0u8; 32];
        body[0] = i;
        g.put_revision(make_rev(r_base, id, base, &format!("fn_{i}"), body, [0u8; 32]));
        bind(&kv, base, id, r_base);
        bind(&kv, ours, id, r_base);
        bind(&kv, theirs, id, r_base);

        // Every 5th identity modified in ours
        if i % 5 == 0 {
            let r_ours = {
                let mut x = [0u8; 16]; x[14] = i; x[13] = 2; NodeRevisionId(x)
            };
            let mut body_o = [0u8; 32];
            body_o[0] = i;
            body_o[1] = 1;
            g.put_revision(make_rev(r_ours, id, ours, &format!("fn_{i}"), body_o, [0u8; 32]));
            bind(&kv, ours, id, r_ours);
        }

        // Every 7th identity modified in theirs
        if i % 7 == 0 {
            let r_theirs = {
                let mut x = [0u8; 16]; x[14] = i; x[13] = 3; NodeRevisionId(x)
            };
            let mut body_t = [0u8; 32];
            body_t[0] = i;
            body_t[2] = 1;
            g.put_revision(make_rev(r_theirs, id, theirs, &format!("fn_{i}"), body_t, [0u8; 32]));
            bind(&kv, theirs, id, r_theirs);
        }
    }

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    assert_eq!(pa.classified.len(), 50);

    let conflict_count = pa.classified.iter()
        .filter(|c| c.class == MergeIdentityClass::BothModifiedUnresolved)
        .count();
    // Identity 0 and 35 are modified in both (0%5==0 && 0%7==0, 35%5==0 && 35%7==0)
    assert_eq!(conflict_count, 2);

    let ours_only = pa.classified.iter()
        .filter(|c| c.class == MergeIdentityClass::OursOnly)
        .count();
    // i%5==0 && i%7!=0: {5, 10, 15, 20, 25, 30, 40, 45}
    assert_eq!(ours_only, 8);

    let theirs_only = pa.classified.iter()
        .filter(|c| c.class == MergeIdentityClass::TheirsOnly)
        .count();
    // i%7==0 && i%5!=0: {7, 14, 21, 28, 42, 49}
    assert_eq!(theirs_only, 6);

    let clean = pa.classified.iter()
        .filter(|c| c.class == MergeIdentityClass::Clean)
        .count();
    assert_eq!(clean, 50 - 2 - 8 - 6);

    // With Ours strategy, all should resolve
    let pb = phase_b_promote(
        &kv, &mut g, ours, &pa.classified, Some(MergeStrategy::Ours),
    );
    assert_eq!(pb.promoted.len(), 50);
    assert!(pb.unresolved_conflicts.is_empty());
    // Only conflict losers are orphaned, not base revisions from non-conflict changes
    assert_eq!(pb.orphaned_revisions.len(), 2);
}

// ---------------------------------------------------------------------------
// Scenario 11: Rename detection — theirs renames a function
// ---------------------------------------------------------------------------
#[test]
fn rename_detected_and_unified() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    // Base: old_fn exists
    let old_id = iid(1);
    let new_id = iid(2);
    let r_old = rid(1);
    g.put_identity(NodeIdentity { identity_id: old_id, kind: NodeKind::Function });
    g.put_revision(make_rev(r_old, old_id, base, "old_fn", [10u8; 32], [0u8; 32]));
    bind(&kv, base, old_id, r_old);
    bind(&kv, ours, old_id, r_old);

    // Theirs: old_fn deleted, new_fn created with rename_source_id = old_id
    let r_new = rid(2);
    g.put_identity(NodeIdentity { identity_id: new_id, kind: NodeKind::Function });
    let mut new_rev = make_rev(r_new, new_id, theirs, "new_fn", [20u8; 32], [0u8; 32]);
    new_rev.rename_source_id = Some(old_id);
    g.put_revision(new_rev);
    bind(&kv, theirs, new_id, r_new);
    // old_id NOT bound in theirs → deleted

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);

    // old_fn should be BothDeleted (suppressed by rename detection)
    let old_class = pa.classified.iter().find(|c| c.identity_id == old_id).unwrap();
    assert_eq!(old_class.class, MergeIdentityClass::BothDeleted);

    // new_fn should be RenamedCandidate
    let new_class = pa.classified.iter().find(|c| c.identity_id == new_id).unwrap();
    assert_eq!(new_class.class, MergeIdentityClass::RenamedCandidate);

    // Report should show rename detection
    assert_eq!(pa.report.rename_detections.len(), 1);
    assert!(pa.report.rename_detections[0].contains("new_fn"));

    // Phase B promotes the renamed identity
    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    let new_bound = pb.promoted.iter().find(|(id, _)| *id == new_id);
    assert!(new_bound.is_some());

    // Verify new_fn is bound on target
    let key = revision_binding_kv_key(ours, new_id);
    assert_eq!(kv.get(&key), Some(r_new.0.to_vec()));

    // old_fn should be unbound (deleted)
    let key_old = revision_binding_kv_key(ours, old_id);
    assert!(kv.get(&key_old).is_none());
}

#[test]
fn phase_c_writes_eto_for_rename_and_keeps_inbound_edges() {
    use cis_core::{collect_rename_pairs, EdgeTargetOverrideStore};

    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    let old_id = iid(1);
    let new_id = iid(2);
    let caller_id = iid(3);
    let r_old = rid(1);
    let r_caller = rid(3);
    g.put_identity(NodeIdentity {
        identity_id: old_id,
        kind: NodeKind::Function,
    });
    g.put_identity(NodeIdentity {
        identity_id: new_id,
        kind: NodeKind::Function,
    });
    g.put_identity(NodeIdentity {
        identity_id: caller_id,
        kind: NodeKind::Function,
    });
    g.put_revision(make_rev(
        r_old,
        old_id,
        base,
        "old_fn",
        [10u8; 32],
        [0u8; 32],
    ));
    g.put_revision(make_rev(
        r_caller,
        caller_id,
        base,
        "caller",
        [30u8; 32],
        [0u8; 32],
    ));
    let edge_id = [9u8; 16];
    g.replace_edges_for_revision(r_caller, vec![make_edge(edge_id, r_caller, old_id, [0u8; 32])])
        .unwrap();
    bind(&kv, base, old_id, r_old);
    bind(&kv, base, caller_id, r_caller);
    bind(&kv, ours, old_id, r_old);
    bind(&kv, ours, caller_id, r_caller);

    let r_new = rid(2);
    let mut new_rev = make_rev(r_new, new_id, theirs, "new_fn", [20u8; 32], [0u8; 32]);
    new_rev.rename_source_id = Some(old_id);
    g.put_revision(new_rev);
    bind(&kv, theirs, new_id, r_new);
    bind(&kv, theirs, caller_id, r_caller);

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);
    let pairs = collect_rename_pairs(&g, &pa.classified);
    assert_eq!(pairs, vec![(old_id, new_id)]);

    let pb = phase_b_promote(&kv, &mut g, ours, &pa.classified, None);
    let pc = phase_c_reconcile_edges_full(
        &mut g,
        &kv,
        None,
        ours,
        &pb.promoted,
        Some(&pa.classified),
    );
    assert!(
        pc.eto_retargets >= 1,
        "expected ETO retarget for caller→old_id, got {}",
        pc.eto_retargets
    );
    assert_eq!(
        pc.dangling_edges_removed, 0,
        "ETO-covered edge must not be dropped as dangling"
    );
    assert_eq!(g.outbound_edges(r_caller).len(), 1);

    let eto = EdgeTargetOverrideStore::from_kv(&kv);
    assert_eq!(
        eto.get_override(ours, r_caller, edge_id),
        Some(new_id),
        "caller edge should be overridden to new identity"
    );
}

/// Theirs renamed; ours independently created the same new identity without rename_source_id.
/// Must still detect rename via theirs revision (not `ours.or(theirs)` alone).
#[test]
fn rename_detected_when_ours_lacks_rename_source() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);

    let old_id = iid(1);
    let new_id = iid(2);
    let r_old = rid(1);
    g.put_identity(NodeIdentity {
        identity_id: old_id,
        kind: NodeKind::Function,
    });
    g.put_revision(make_rev(
        r_old,
        old_id,
        base,
        "old_fn",
        [10u8; 32],
        [0u8; 32],
    ));
    bind(&kv, base, old_id, r_old);

    // Ours: deleted old, created new without rename link
    let r_ours_new = rid(2);
    g.put_identity(NodeIdentity {
        identity_id: new_id,
        kind: NodeKind::Function,
    });
    g.put_revision(make_rev(
        r_ours_new,
        new_id,
        ours,
        "new_fn",
        [21u8; 32],
        [0u8; 32],
    ));
    bind(&kv, ours, new_id, r_ours_new);

    // Theirs: deleted old, created new WITH rename_source_id
    let r_theirs_new = rid(3);
    let mut theirs_new = make_rev(
        r_theirs_new,
        new_id,
        theirs,
        "new_fn",
        [22u8; 32],
        [0u8; 32],
    );
    theirs_new.rename_source_id = Some(old_id);
    g.put_revision(theirs_new);
    bind(&kv, theirs, new_id, r_theirs_new);

    let pa = phase_a_classify(&g, &kv, ours, theirs, base);

    let old_class = pa
        .classified
        .iter()
        .find(|c| c.identity_id == old_id)
        .unwrap();
    assert_eq!(old_class.class, MergeIdentityClass::BothDeleted);

    let new_class = pa
        .classified
        .iter()
        .find(|c| c.identity_id == new_id)
        .unwrap();
    assert_eq!(
        new_class.class,
        MergeIdentityClass::RenamedCandidate,
        "theirs rename_source_id must win even when ours revision exists without it"
    );
}

// ---------------------------------------------------------------------------
// Scenario 12: Saga crash-resume from Classifying phase
// ---------------------------------------------------------------------------
#[test]
fn saga_resume_from_classifying() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);
    let merge_id = MergeId([42u8; 16]);

    // Setup: ours modifies f1, theirs modifies f2
    let f1 = iid(1);
    let f2 = iid(2);
    let r1 = rid(1);
    let r2 = rid(2);
    let r1_ours = rid(11);
    let r2_theirs = rid(22);

    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_identity(NodeIdentity { identity_id: f2, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "alpha", [1u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r2, f2, base, "beta", [2u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r1_ours, f1, ours, "alpha", [11u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r2_theirs, f2, theirs, "beta", [22u8; 32], [0u8; 32]));

    bind(&kv, base, f1, r1);
    bind(&kv, base, f2, r2);
    bind(&kv, ours, f1, r1_ours);
    bind(&kv, ours, f2, r2);
    bind(&kv, theirs, f1, r1);
    bind(&kv, theirs, f2, r2_theirs);

    // Simulate: saga was persisted at Classifying but process crashed
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    saga.persist(merge_id, SagaPhase::Classifying);

    let ctx = MergeContext {
        merge_id,
        ours_branch: ours,
        theirs_branch: theirs,
        base_branch: base,
        target_branch: ours,
        strategy: None,
    };
    ctx.persist(&kv);

    // Resume from crash
    let result = resume_merge(&mut g, &kv, None, merge_id, &saga, None)
        .expect("resume should not be pending");
    assert!(result.is_some());

    let out = result.unwrap();
    assert_eq!(out.phase_a.classified.len(), 2);
    assert_eq!(out.phase_b.promoted.len(), 2);

    // Saga should be committed
    assert_eq!(saga.load(merge_id), Some(SagaPhase::Committed));

    // Context should be cleaned up
    assert!(MergeContext::load(&kv, merge_id).is_none());

    // Bindings should be correct
    let key_f1 = revision_binding_kv_key(ours, f1);
    let key_f2 = revision_binding_kv_key(ours, f2);
    assert_eq!(kv.get(&key_f1), Some(r1_ours.0.to_vec()));
    assert_eq!(kv.get(&key_f2), Some(r2_theirs.0.to_vec()));
}

// ---------------------------------------------------------------------------
// Scenario 13: Saga resume from EdgeBatch phase
// ---------------------------------------------------------------------------
#[test]
fn saga_resume_from_edge_batch() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);
    let merge_id = MergeId([99u8; 16]);

    let f1 = iid(1);
    let r1 = rid(1);
    let r1_ours = rid(11);

    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "func", [1u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r1_ours, f1, ours, "func", [11u8; 32], [0u8; 32]));

    bind(&kv, base, f1, r1);
    bind(&kv, ours, f1, r1_ours);
    bind(&kv, theirs, f1, r1);

    // Simulate: Phase B already completed, saga at EdgeBatch
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    saga.persist(merge_id, SagaPhase::EdgeBatch { seq: 0 });

    let ctx = MergeContext {
        merge_id,
        ours_branch: ours,
        theirs_branch: theirs,
        base_branch: base,
        target_branch: ours,
        strategy: None,
    };
    ctx.persist(&kv);

    let result = resume_merge(&mut g, &kv, None, merge_id, &saga, None)
        .expect("resume should not be pending");
    assert!(result.is_some());

    let out = result.unwrap();
    assert!(!out.phase_b.promoted.is_empty());
    assert_eq!(saga.load(merge_id), Some(SagaPhase::Committed));
}

// ---------------------------------------------------------------------------
// Scenario 14: Phase C flags foreign-branch revisions for edge regen
// ---------------------------------------------------------------------------
#[test]
fn phase_c_flags_foreign_revisions_for_edge_regen() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let target = BranchId([9u8; 16]);
    let source = BranchId([8u8; 16]);

    let f1 = iid(1);
    let f2 = iid(2);
    let r1 = rid(1);
    let r2 = rid(2);

    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_identity(NodeIdentity { identity_id: f2, kind: NodeKind::Function });
    // r1 is from target branch, r2 is from source branch
    g.put_revision(make_rev(r1, f1, target, "local_fn", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r2, f2, source, "foreign_fn", [20u8; 32], [0u8; 32]));

    bind(&kv, target, f1, r1);
    bind(&kv, target, f2, r2);

    let promoted = vec![(f1, r1), (f2, r2)];
    let pc = phase_c_reconcile_edges(&mut g, &kv, target, &promoted);

    // r2 is from source branch → should be flagged for edge regen
    assert_eq!(pc.needs_edge_regen.len(), 1);
    assert_eq!(pc.needs_edge_regen[0], r2);
}

// ---------------------------------------------------------------------------
// Scenario 15: MergeContext persist/load roundtrip
// ---------------------------------------------------------------------------
#[test]
fn merge_context_roundtrip() {
    let kv = Arc::new(MemoryKv::new());
    let merge_id = MergeId([77u8; 16]);

    let ctx = MergeContext {
        merge_id,
        ours_branch: BranchId([2u8; 16]),
        theirs_branch: BranchId([3u8; 16]),
        base_branch: BranchId([1u8; 16]),
        target_branch: BranchId([2u8; 16]),
        strategy: Some(MergeStrategy::Theirs),
    };
    ctx.persist(&kv);

    let loaded = MergeContext::load(&kv, merge_id).unwrap();
    assert_eq!(loaded.merge_id.0, merge_id.0);
    assert_eq!(loaded.ours_branch.0, [2u8; 16]);
    assert_eq!(loaded.theirs_branch.0, [3u8; 16]);
    assert_eq!(loaded.strategy, Some(MergeStrategy::Theirs));

    MergeContext::remove(&kv, merge_id);
    assert!(MergeContext::load(&kv, merge_id).is_none());
}

// ---------------------------------------------------------------------------
// Scenario 16: Preflight snapshot enables cancel_merge rollback
// ---------------------------------------------------------------------------
#[test]
fn preflight_snapshot_enables_cancel_restore() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let branch = BranchId([1u8; 16]);
    let merge_id = MergeId([2u8; 16]);
    let spec = cis_core::SpeculativePathTracker::new();

    let f1 = iid(1);
    let r_old = rid(1);
    let r_new = rid(2);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r_old, f1, branch, "f", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_new, f1, branch, "f", [20u8; 32], [0u8; 32]));
    bind(&kv, branch, f1, r_old);

    MergePreflight::begin_with_snapshot(
        Arc::clone(&kv),
        branch,
        merge_id,
        &["f.py".into()],
        &spec,
        &[(f1, r_old)],
    )
    .unwrap();

    bind(&kv, branch, f1, r_new);
    let mc = cis_core::MergeControl::new(Arc::clone(&kv));
    let vs = cis_core::InMemoryVectorStore::new();
    let rep = mc
        .cancel_merge(merge_id, branch, &mut g, &vs, None, &[])
        .unwrap();
    assert_eq!(rep.restored_bindings, 1);
    let key = revision_binding_kv_key(branch, f1);
    assert_eq!(kv.get(&key), Some(r_old.0.to_vec()));
}

// ---------------------------------------------------------------------------
// Scenario 17: Phase C regenerates call edges from body store
// ---------------------------------------------------------------------------
#[test]
fn phase_c_regenerates_call_edges_from_body_store() {
    let kv = Arc::new(MemoryKv::new());
    let body_store = BodyStore::new(Arc::clone(&kv));
    let mut g = InMemoryGraph::default();
    let target = BranchId([9u8; 16]);
    let source = BranchId([8u8; 16]);
    let path = "mod.py";

    let caller_iid = IdentityId(stable_id_bytes("id", path, "caller"));
    let caller_rid = NodeRevisionId(stable_rev_id_bytes(target, path, "caller"));
    let file_hub_iid = IdentityId(stable_id_bytes("file", path, "$hub"));
    let file_hub_rid = NodeRevisionId(stable_rev_id_bytes(target, path, "$file"));

    let content = "import json\n\ndef caller():\n    pass\n";
    body_store.put(file_body_hash_key(path), content.as_bytes().to_vec());

    let json_path = "json.py";
    let json_iid = IdentityId(stable_id_bytes("file", json_path, "$hub"));
    g.put_identity(NodeIdentity { identity_id: json_iid, kind: NodeKind::File });
    let json_rid = NodeRevisionId(stable_rev_id_bytes(target, json_path, "$file"));
    let mut json_rev = make_rev(json_rid, json_iid, target, json_path, [3u8; 32], [0u8; 32]);
    json_rev.file_path = json_path.into();
    g.put_revision(json_rev);
    bind(&kv, target, json_iid, json_rid);

    g.put_identity(NodeIdentity { identity_id: file_hub_iid, kind: NodeKind::File });
    let mut file_hub_rev = make_rev(file_hub_rid, file_hub_iid, target, path, [1u8; 32], [0u8; 32]);
    file_hub_rev.file_path = path.into();
    g.put_revision(file_hub_rev);
    bind(&kv, target, file_hub_iid, file_hub_rid);

    g.put_identity(NodeIdentity { identity_id: caller_iid, kind: NodeKind::Function });
    let mut caller_rev = make_rev(caller_rid, caller_iid, target, "caller", [2u8; 32], [0u8; 32]);
    caller_rev.file_path = path.into();
    g.put_revision(caller_rev);

    bind(&kv, target, caller_iid, caller_rid);

    let classified = vec![cis_core::ClassifiedMergeIdentity {
        identity_id: caller_iid,
        class: MergeIdentityClass::TheirsOnly,
        base_revision: None,
        ours_revision: None,
        theirs_revision: Some(caller_rid),
        qualified_name: "caller".into(),
    }];
    let promoted = vec![(caller_iid, caller_rid)];

    let pc = phase_c_reconcile_edges_full(
        &mut g,
        &kv,
        Some(&body_store),
        target,
        &promoted,
        Some(&classified),
    );
    assert!(pc.edges_regenerated >= 1);
    let edges = g.outbound_edges(file_hub_rid);
    assert!(edges.iter().any(|e| {
        e.ty == EdgeType::Imports && e.target_identity_id == json_iid
    }));
}

// ---------------------------------------------------------------------------
// Scenario 18: Startup recovery completes in-flight merge
// ---------------------------------------------------------------------------
#[test]
fn recover_inflight_merge_completes_after_crash() {
    let kv = Arc::new(MemoryKv::new());
    let body_store = BodyStore::new(Arc::clone(&kv));
    let mut g = InMemoryGraph::default();
    let base = BranchId([1u8; 16]);
    let ours = BranchId([2u8; 16]);
    let theirs = BranchId([3u8; 16]);
    let merge_id = MergeId([42u8; 16]);

    let f1 = iid(1);
    let r1 = rid(1);
    let r1_ours = rid(11);
    g.put_identity(NodeIdentity { identity_id: f1, kind: NodeKind::Function });
    g.put_revision(make_rev(r1, f1, base, "f", [1u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r1_ours, f1, ours, "f", [11u8; 32], [0u8; 32]));
    bind(&kv, base, f1, r1);
    bind(&kv, ours, f1, r1_ours);
    bind(&kv, theirs, f1, r1);

    cis_core::acquire_merge_lock(&kv, ours, merge_id).unwrap();
    let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
    saga.persist(merge_id, SagaPhase::Promoting);
    MergeContext {
        merge_id,
        ours_branch: ours,
        theirs_branch: theirs,
        base_branch: base,
        target_branch: ours,
        strategy: None,
    }
    .persist(&kv);

    let gate = cis_core::MergeRecoveryGate::new(Arc::clone(&kv));
    let mc = cis_core::MergeControl::new(Arc::clone(&kv));
    let vs = cis_core::InMemoryVectorStore::new();
    let rep = recover_inflight_merges(&mut g, &kv, &body_store, &saga, &mc, &vs, &gate, None);
    assert_eq!(rep.resumed, 1);
    assert_eq!(saga.load(merge_id), Some(SagaPhase::Committed));
    assert!(cis_core::merge_lock_holder(&kv, ours).is_none());
}

// ---------------------------------------------------------------------------
// Scenario 19: Saga batch compensate on cancel restores prior edges
// ---------------------------------------------------------------------------
#[test]
fn cancel_restores_saga_edge_batches() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let branch = BranchId([1u8; 16]);
    let merge_id = MergeId([4u8; 16]);
    let target = BranchId([9u8; 16]);

    let i_src = iid(1);
    let i_tgt = iid(2);
    let r_src = rid(1);
    g.put_identity(NodeIdentity {
        identity_id: i_src,
        kind: NodeKind::Function,
    });
    g.put_identity(NodeIdentity {
        identity_id: i_tgt,
        kind: NodeKind::Function,
    });
    g.put_revision(make_rev(r_src, i_src, target, "caller", [10u8; 32], [0u8; 32]));
    let prior = vec![GraphEdge {
        edge_id: [8u8; 16],
        ty: EdgeType::Uses,
        source_revision_id: r_src,
        target_identity_id: i_tgt,
        resolution: EdgeResolution {
            target_signature_hash: [0u8; 32],
            resolver: SourceType::Ast,
            last_validation_ms: 0,
        },
        anchor: SourceSpan::UNKNOWN,
    }];
    g.replace_edges_for_revision(r_src, prior.clone()).unwrap();
    bind(&kv, target, i_src, r_src);

    let batch = cis_core::SagaEdgeBatch {
        seq: 1,
        target_revision_id: r_src,
        prior_edges: prior,
        edges: vec![GraphEdge {
            edge_id: [9u8; 16],
            ty: EdgeType::Calls,
            source_revision_id: r_src,
            target_identity_id: i_tgt,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        }],
    };
    cis_core::apply_saga_edge_batch(&mut g, &batch).unwrap();
    cis_core::persist_saga_edge_batch(&kv, merge_id, &batch);
    cis_core::acquire_merge_lock(&kv, branch, merge_id).unwrap();

    let mc = cis_core::MergeControl::new(Arc::clone(&kv));
    let vs = cis_core::InMemoryVectorStore::new();
    mc.cancel_merge(merge_id, branch, &mut g, &vs, None, &[])
        .unwrap();

    assert_eq!(g.outbound_edges(r_src).len(), 1);
    assert_eq!(g.outbound_edges(r_src)[0].ty, EdgeType::Uses);
}

// ---------------------------------------------------------------------------
// Scenario 20: msnap merge-base classifies target+source divergence
// ---------------------------------------------------------------------------
#[test]
fn msnap_merge_base_classifies_theirs_only() {
    let kv = Arc::new(MemoryKv::new());
    let mut g = InMemoryGraph::default();
    let target = BranchId([1u8; 16]);
    let source = BranchId([2u8; 16]);
    let merge_id = MergeId([3u8; 16]);

    let i1 = iid(1);
    let r_base = rid(1);
    let r_ours = rid(2);
    let r_theirs = rid(3);
    g.put_identity(NodeIdentity {
        identity_id: i1,
        kind: NodeKind::Function,
    });
    g.put_revision(make_rev(r_base, i1, target, "f", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_ours, i1, target, "f", [10u8; 32], [0u8; 32]));
    g.put_revision(make_rev(r_theirs, i1, source, "f", [30u8; 32], [0u8; 32]));
    bind(&kv, target, i1, r_ours);
    bind(&kv, source, i1, r_theirs);
    cis_core::MergeControl::new(Arc::clone(&kv))
        .record_premerge_bindings(merge_id, target, &[(i1, r_base)]);

    let pa = cis_core::phase_a_for_merge(&g, &kv, merge_id, target, source, target, target);
    assert_eq!(pa.classified[0].class, MergeIdentityClass::TheirsOnly);
}
