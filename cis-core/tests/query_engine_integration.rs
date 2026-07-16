//! **Phase 5** — query engine wired to ingested graph (confidence, ETO, tombstone bridging).

use std::sync::Arc;

use cis_core::{
    apply_index_events_with_config, expand_context_bfs, resolve_definition_target,
    resolve_identity_revision, stable_rev_id_bytes, EdgeTargetOverrideStore, EdgeType,
    FsChangeKind, IndexEvent, IndexEventQueue, MemoryKv, RankingPolicy, RevisionStatus,
    WriteCoordinator,
};
use cis_wal::{BranchId, MutationLog, NodeRevisionId};

const BRANCH: BranchId = BranchId([0u8; 16]);

fn coord() -> Arc<WriteCoordinator> {
    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    Arc::new(WriteCoordinator::new(wal))
}

fn ingest(path: &str, src: &str, coord: &WriteCoordinator, kv: Arc<MemoryKv>) {
    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        coord,
        kv,
        vec![IndexEvent {
            branch_id: BRANCH,
            path: path.into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }],
        move |_p| Ok(src.to_string()),
        None,
        None,
        None,
    )
    .expect("ingest");
}

#[test]
fn expand_context_assigns_per_hit_confidence() {
    let kv = Arc::new(MemoryKv::new());
    let coord = coord();
    ingest("a.py", "def f():\n    pass\n", &coord, Arc::clone(&kv));
    let g = coord.graph().read();
    let rid = NodeRevisionId(stable_rev_id_bytes(BRANCH, "a.py", "f"));
    let eto = EdgeTargetOverrideStore::new(kv);
    let policy = RankingPolicy::default();
    let chain = [BRANCH];
    let out = expand_context_bfs(&g, &eto, &policy, &chain, rid, 1, 1_000_000);
    assert!(!out.hits.is_empty());
    for (_, conf) in &out.hits {
        assert!(*conf > 0.0 && *conf <= 1.0);
    }
}

#[test]
fn rename_preserves_identity_and_bridges_tombstone_query() {
    let kv = Arc::new(MemoryKv::new());
    let coord = coord();
    let path = "m.py";
    ingest(path, "def foo():\n    return 1\n", &coord, Arc::clone(&kv));
    ingest(path, "def bar():\n    return 1\n", &coord, kv);
    let g = coord.graph().read();
    let foo_rev = NodeRevisionId(stable_rev_id_bytes(BRANCH, path, "foo"));
    let foo = g.get_revision(foo_rev).unwrap();
    assert!(matches!(foo.status, RevisionStatus::Tombstone));
    assert!(
        g.outbound_edges(foo_rev)
            .iter()
            .any(|e| e.ty == EdgeType::RenamedFrom),
        "RENAMED_FROM on tombstone"
    );
    let chain = [BRANCH];
    let bridged = resolve_identity_revision(&g, &chain, foo.identity_id).unwrap();
    assert!(
        bridged.qualified_name.ends_with("bar"),
        "bridged to active successor, got {}",
        bridged.qualified_name
    );
}

#[test]
fn eto_overrides_definition_resolution() {
    let kv = Arc::new(MemoryKv::new());
    let coord = coord();
    ingest("a.py", "def caller():\n    pass\n", &coord, Arc::clone(&kv));
    ingest("b.py", "def wrong():\n    pass\n", &coord, Arc::clone(&kv));
    ingest("c.py", "def right():\n    pass\n", &coord, Arc::clone(&kv));

    let g = coord.graph().read();
    let caller = g
        .revisions()
        .find(|r| r.qualified_name.ends_with("caller"))
        .expect("caller");
    let wrong = g
        .revisions()
        .find(|r| r.qualified_name.ends_with("wrong"))
        .expect("wrong");
    let right = g
        .revisions()
        .find(|r| r.qualified_name.ends_with("right"))
        .expect("right");
    let caller_rid = caller.revision_id;
    let wrong_i = wrong.identity_id;
    let right_i = right.identity_id;
    let edges: Vec<_> = g.outbound_edges(caller_rid).to_vec();
    drop(g);

    let eto = EdgeTargetOverrideStore::new(Arc::clone(&kv));
    let edge_id = [0xABu8; 16];
    let mut g = coord.graph().write();
    let edges = if edges.is_empty() {
        vec![cis_core::GraphEdge {
            edge_id,
            ty: EdgeType::Calls,
            source_revision_id: caller_rid,
            target_identity_id: wrong_i,
            resolution: cis_core::EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: cis_core::SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: cis_core::SourceSpan::UNKNOWN,
        }]
    } else {
        edges
    };
    if let Some(e) = edges.iter().find(|e| e.ty == EdgeType::Calls) {
        eto.set_override(BRANCH, caller_rid, e.edge_id, right_i);
    } else {
        eto.set_override(BRANCH, caller_rid, edge_id, right_i);
    }
    g.replace_edges_for_revision(caller_rid, edges).unwrap();
    drop(g);

    let g = coord.graph().read();
    let chain = [BRANCH];
    let resolved = resolve_definition_target(&g, &eto, &chain, caller_rid).unwrap();
    assert_eq!(resolved.identity_id, right_i);
}
