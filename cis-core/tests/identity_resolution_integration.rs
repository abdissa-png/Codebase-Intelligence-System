//! **Phase 3** — same-file rename preserves identity; conservative split on dissimilar bodies;
//! cross-file move, multi-tombstone tie-break, CAS dedup.

use std::sync::Arc;
use std::thread;

use cis_core::{
    apply_index_events_with_config, content_checksum_32, stable_id_bytes, stable_rev_id_bytes,
    BodyStore, EdgeType,
    FsChangeKind,
    IndexEvent, IndexEventQueue, MemoryKv, MergeSagaOrchestrator, RenameConfig, RevisionStatus,
    WriteCoordinator,
};
use cis_wal::{BranchId, IdentityId, MutationLog, NodeRevisionId};

const BRANCH: BranchId = BranchId([0u8; 16]);

fn make_coord(_kv: &Arc<MemoryKv>) -> Arc<WriteCoordinator> {
    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = Arc::new(WriteCoordinator::new(Arc::clone(&wal)));
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));
    coord
}

fn ingest_file(
    coord: &Arc<WriteCoordinator>,
    kv: Arc<MemoryKv>,
    path: &str,
    src: &str,
    rename_config: Option<RenameConfig>,
) {
    let q = IndexEventQueue::new();
    let events = vec![IndexEvent {
        branch_id: BRANCH,
        path: path.into(),
        kind: FsChangeKind::Modified,
    }];
    let src = src.to_string();
    apply_index_events_with_config(
        &q,
        coord.as_ref(),
        kv,
        events,
        move |_p| Ok(src.clone()),
        None,
        None,
        rename_config,
    )
    .expect("ingest");
}

#[test]
fn same_file_rename_preserves_identity_and_emits_renamed_from() {
    let kv = Arc::new(MemoryKv::new());
    let coord = make_coord(&kv);

    let path = "m.py";
    ingest_file(
        &coord,
        Arc::clone(&kv),
        path,
        "def foo():\n    return 1\n",
        None,
    );
    let iid_foo = IdentityId(stable_id_bytes("id", path, "foo"));
    let foo_body_hash = content_checksum_32("def foo():\n    return 1\n");
    assert!(
        BodyStore::new(Arc::clone(&kv)).get(&foo_body_hash).is_some(),
        "first ingest should persist foo body"
    );

    ingest_file(
        &coord,
        kv,
        path,
        "def bar():\n    return 1\n",
        None,
    );

    let g = coord.as_ref().graph().read();
    let foo_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, path, "foo"));
    let foo = g.get_revision(foo_rev).expect("foo revision");
    assert!(
        matches!(foo.status, RevisionStatus::Tombstone),
        "foo should be tombstoned, status {:?}",
        foo.status
    );
    let bar_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, path, "bar"));
    let bar = g.get_revision(bar_rev).expect("bar revision");
    assert_eq!(
        bar.identity_id, iid_foo,
        "rename should reuse foo identity, got {:?} vs {:?}",
        bar.identity_id, iid_foo
    );
    assert_eq!(bar.rename_source_id, Some(iid_foo));

    let edges = g.outbound_edges(foo_rev);
    assert!(
        edges.iter().any(|e| e.ty == EdgeType::RenamedFrom),
        "expected RENAMED_FROM from tombstone revision, edges: {:?}",
        edges
    );
}

#[test]
fn dissimilar_body_after_rename_splits_identity() {
    let kv = Arc::new(MemoryKv::new());
    let coord = make_coord(&kv);

    let path = "m.py";
    ingest_file(
        &coord,
        Arc::clone(&kv),
        path,
        "def foo():\n    pass\n",
        None,
    );
    ingest_file(
        &coord,
        kv,
        path,
        "def totally_different(self, a, b, c):\n    return self.n(a, b, c)\n",
        None,
    );

    let g = coord.as_ref().graph().read();
    let sym = g
        .revisions()
        .find(|r| r.qualified_name.contains("totally_different"))
        .expect("new symbol");
    let proposed = IdentityId(stable_id_bytes("id", path, "totally_different"));
    assert_eq!(
        sym.identity_id, proposed,
        "dissimilar body should allocate a fresh identity"
    );
    assert!(sym.rename_source_id.is_none());
    let foo_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, path, "foo"));
    assert!(
        !g.outbound_edges(foo_rev)
            .iter()
            .any(|e| e.ty == EdgeType::RenamedFrom)
    );
}

// ── cross-file rename ────────────────────────────────────────────────────────

#[test]
fn cross_file_move_preserves_identity() {
    let kv = Arc::new(MemoryKv::new());
    let coord = make_coord(&kv);
    // Use loose body threshold for this test but keep name proximity high to avoid
    // false positives from path-prefix proximity between same-file qualified names.
    let low_thresh = RenameConfig {
        rename_min_confidence: 0.3,
        body_similarity_threshold: 0.3,
        name_proximity_threshold: 0.85,
        window_days: 30,
    };

    let src = "def big_helper():\n    x = 1\n    y = 2\n    return x + y\n";
    ingest_file(&coord, Arc::clone(&kv), "a.py", src, Some(low_thresh));
    let iid_orig = IdentityId(stable_id_bytes("id", "a.py", "big_helper"));

    ingest_file(
        &coord,
        Arc::clone(&kv),
        "a.py",
        "",
        Some(low_thresh),
    );

    ingest_file(&coord, kv, "b.py", src, Some(low_thresh));

    let g = coord.graph().read();
    let b_helper = g
        .revisions()
        .find(|r| r.file_path == "b.py" && r.qualified_name.contains("big_helper"))
        .expect("b.py::big_helper");
    assert_eq!(
        b_helper.identity_id, iid_orig,
        "cross-file move should reuse original identity; got {:?} vs {:?}",
        b_helper.identity_id, iid_orig
    );
    let tomb = g
        .revisions()
        .find(|r| r.file_path == "a.py" && r.qualified_name.contains("big_helper"))
        .expect("tombstone in a.py");
    assert!(
        matches!(tomb.status, RevisionStatus::Tombstone),
        "original should be tombstoned"
    );
    assert!(
        g.outbound_edges(tomb.revision_id)
            .iter()
            .any(|e| e.ty == EdgeType::RenamedFrom),
        "RENAMED_FROM edge expected on tombstone revision"
    );
}

// ── multi-tombstone tie-break ─────────────────────────────────────────────────

#[test]
fn multi_tombstone_best_score_wins() {
    use cis_core::identity_resolution::{best_tombstone_rename, RenameConfig as RCfg};
    use cis_core::{BodyStore, InMemoryGraph, NodeIdentity, NodeKind, NodeRevision, RevisionStatus, Language, SourceSpan};
    use cis_wal::{NodeRevisionId, IdentityId, BranchId};

    let kv = Arc::new(MemoryKv::new());
    let bs = BodyStore::new(Arc::clone(&kv));
    let mut g = InMemoryGraph::default();
    let branch = BranchId([0u8; 16]);
    let path = "m.py";

    let body_low = "def alpha(): pass";
    let body_high = "def compute():\n    x = 1\n    y = 2\n    return x + y\n";
    let bh_low = {
        let mut h = [1u8; 32]; h[0] = 10; h
    };
    let bh_high = {
        let mut h = [2u8; 32]; h[0] = 20; h
    };
    bs.put(bh_low, body_low.as_bytes().to_vec());
    bs.put(bh_high, body_high.as_bytes().to_vec());

    let iid_low = IdentityId([10u8; 16]);
    let iid_high = IdentityId([20u8; 16]);

    for (iid, rid_byte, qn, bh) in [
        (iid_low, 1u8, "m.alpha", bh_low),
        (iid_high, 2u8, "m.compute", bh_high),
    ] {
        g.put_identity(NodeIdentity { identity_id: iid, kind: NodeKind::Function });
        g.put_revision(NodeRevision {
            revision_id: NodeRevisionId([rid_byte; 16]),
            identity_id: iid,
            branch_id: branch,
            status: RevisionStatus::Tombstone,
            qualified_name: qn.into(),
            file_path: path.into(),
            body_hash: bh,
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
    }

    let cfg = RCfg { rename_min_confidence: 0.3, body_similarity_threshold: 0.3, name_proximity_threshold: 0.85, window_days: 30 };
    let resolver = cis_core::IdentityResolver::from_policy(0.3);

    let new_body = "def compute_v2():\n    x = 1\n    y = 2\n    return x + y\n";
    let result = best_tombstone_rename(&g, branch, path, "m.compute_v2", new_body, &resolver, &cfg, Some(&bs));
    assert!(result.is_some(), "should find a rename candidate");
    let (matched_iid, _, _) = result.unwrap();
    assert_eq!(matched_iid, iid_high, "higher-scoring tombstone should win");
}

// ── CAS dedup ─────────────────────────────────────────────────────────────────

#[test]
fn concurrent_ingest_same_file_produces_one_identity() {
    let kv = Arc::new(MemoryKv::new());
    let coord = make_coord(&kv);
    let branch = BranchId([0u8; 16]);
    let path = "m.py";
    let src = "def shared():\n    return 42\n";

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let coord_c = Arc::clone(&coord);
            let kv_c = Arc::clone(&kv);
            let src_c = src.to_string();
            thread::spawn(move || {
                let q = IndexEventQueue::new();
                let events = vec![IndexEvent { branch_id: branch, path: path.into(), kind: FsChangeKind::Modified }];
                let _ = apply_index_events_with_config(
                    &q,
                    coord_c.as_ref(),
                    kv_c,
                    events,
                    move |_| Ok(src_c.clone()),
                    None,
                    None,
                    None,
                );
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let g = coord.graph().read();
    let active_identities: std::collections::HashSet<IdentityId> = g
        .revisions()
        .filter(|r| r.file_path == path && r.qualified_name.contains("shared"))
        .filter(|r| matches!(r.status, RevisionStatus::Active))
        .map(|r| r.identity_id)
        .collect();
    assert_eq!(
        active_identities.len(),
        1,
        "all concurrent ingests should converge on one active identity"
    );
}
