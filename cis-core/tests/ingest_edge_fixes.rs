//! Wave-1 ingest edge cases: time-travel binds resolved identity; Renamed tombstones old path.

use std::sync::Arc;

use cis_core::{
    apply_index_events_with_config, chunk_id, record_committed_snapshot, stable_id_bytes,
    FsChangeKind, IndexEvent, IndexEventQueue, InMemoryGraph, InMemoryVectorStore, Language,
    MemoryKv, MergeSagaOrchestrator, NodeRevision, RenameConfig, RevisionIndexCow, RevisionStatus,
    SharedInMemoryGraph, SourceSpan, VectorCleanupQueue, VectorCleanupWorker, WriteCoordinator,
};
use cis_wal::{BranchId, IdentityId, MutationLog, NodeRevisionId};

const BRANCH: BranchId = BranchId([0u8; 16]);

fn make_coord() -> (Arc<WriteCoordinator>, Arc<MemoryKv>) {
    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = Arc::new(WriteCoordinator::new(Arc::clone(&wal)));
    let kv = Arc::new(MemoryKv::new());
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));
    (coord, kv)
}

fn rename_cfg() -> RenameConfig {
    RenameConfig {
        rename_min_confidence: 0.3,
        body_similarity_threshold: 0.3,
        name_proximity_threshold: 0.85,
        window_days: 30,
    }
}

#[test]
fn time_travel_binds_resolved_identity_after_rename() {
    let (coord, kv) = make_coord();
    let ri = RevisionIndexCow::root(BRANCH, Arc::clone(&kv));
    let path = "m.py";
    let body = "def compute():\n    x = 1\n    y = 2\n    return x + y\n";
    let q = IndexEventQueue::new();
    apply_index_events_with_config(
        &q,
        coord.as_ref(),
        Arc::clone(&kv),
        vec![IndexEvent::new(BRANCH, path, FsChangeKind::Modified)],
        {
            let s = body.to_string();
            move |_| Ok(s.clone())
        },
        Some(Arc::clone(&ri)),
        None,
        Some(rename_cfg()),
    )
    .expect("ingest v1");

    let foo_iid = {
        let g = coord.graph().read();
        let found = g
            .revisions()
            .find(|r| {
                r.qualified_name.contains("compute") && matches!(r.status, RevisionStatus::Active)
            })
            .map(|r| r.identity_id);
        found.expect("compute")
    };

    let renamed = "def compute_v2():\n    x = 1\n    y = 2\n    return x + y\n";
    apply_index_events_with_config(
        &q,
        coord.as_ref(),
        Arc::clone(&kv),
        vec![IndexEvent::new(BRANCH, path, FsChangeKind::Modified)],
        {
            let s = renamed.to_string();
            move |_| Ok(s.clone())
        },
        Some(Arc::clone(&ri)),
        None,
        Some(rename_cfg()),
    )
    .expect("ingest rename");

    let (actual_iid, bar_rid) = {
        let g = coord.graph().read();
        let rev = g
            .revisions()
            .find(|r| {
                r.qualified_name.contains("compute_v2")
                    && matches!(r.status, RevisionStatus::Active)
            })
            .map(|r| (r.identity_id, r.revision_id));
        rev.expect("compute_v2")
    };
    assert_eq!(actual_iid, foo_iid, "rename should preserve identity");

    let proposed_bar = IdentityId(stable_id_bytes("id", path, "compute_v2"));
    assert_ne!(
        proposed_bar, actual_iid,
        "precondition: proposed id differs from inherited"
    );

    assert_eq!(
        ri.lookup(actual_iid),
        Some(bar_rid),
        "RevisionIndexCow must bind resolved identity"
    );
    assert_ne!(
        ri.lookup(proposed_bar),
        Some(bar_rid),
        "must not bind proposed identity to renamed revision"
    );

    let wal_log_id = 99u64;
    record_committed_snapshot(ri.as_ref(), kv.as_ref(), BRANCH, wal_log_id, None);
}

#[test]
fn renamed_event_tombstones_old_path() {
    let (coord, kv) = make_coord();
    let q = IndexEventQueue::new();
    let old_path = "old.py";
    let new_path = "new.py";
    let body = "def moved_fn():\n    x = 1\n    y = 2\n    return x + y\n";

    apply_index_events_with_config(
        &q,
        coord.as_ref(),
        Arc::clone(&kv),
        vec![IndexEvent::new(BRANCH, old_path, FsChangeKind::Modified)],
        {
            let s = body.to_string();
            move |_| Ok(s.clone())
        },
        None,
        None,
        Some(rename_cfg()),
    )
    .expect("ingest old");

    let id_before = IdentityId(stable_id_bytes("id", old_path, "moved_fn"));

    apply_index_events_with_config(
        &q,
        coord.as_ref(),
        Arc::clone(&kv),
        vec![IndexEvent::renamed(BRANCH, old_path, new_path)],
        {
            let s = body.to_string();
            let np = new_path.to_string();
            move |p| {
                if p == np {
                    Ok(s.clone())
                } else {
                    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"))
                }
            }
        },
        None,
        None,
        Some(rename_cfg()),
    )
    .expect("rename ingest");

    let g = coord.graph().read();
    let old_tomb = g
        .revisions()
        .find(|r| r.file_path == old_path && r.qualified_name.contains("moved_fn"))
        .expect("old path revision");
    assert!(
        matches!(old_tomb.status, RevisionStatus::Tombstone),
        "Renamed must tombstone the old path, got {:?}",
        old_tomb.status
    );

    let new_rev = g
        .revisions()
        .find(|r| {
            r.file_path == new_path
                && r.qualified_name.contains("moved_fn")
                && matches!(r.status, RevisionStatus::Active)
        })
        .expect("new path active");
    // Primary contract for Renamed/Moved: old path is gone. Identity continuity is covered by
    // cross_file_move_preserves_identity (Deleted/orphan + new path) when bodies match.
    assert_ne!(
        new_rev.revision_id, old_tomb.revision_id,
        "new path must get its own revision id"
    );
    let _ = id_before;
}

#[test]
fn vector_gc_uses_chunk_id_refcount() {
    let mut g = InMemoryGraph::default();
    let body = [9u8; 32];
    let rid1 = NodeRevisionId([1u8; 16]);
    let rid2 = NodeRevisionId([2u8; 16]);
    let iid1 = IdentityId([1u8; 16]);
    let iid2 = IdentityId([2u8; 16]);
    for (rid, iid) in [(rid1, iid1), (rid2, iid2)] {
        g.put_revision(NodeRevision {
            revision_id: rid,
            identity_id: iid,
            branch_id: BRANCH,
            status: RevisionStatus::Tombstone,
            qualified_name: format!("sym_{}", rid.0[0]),
            file_path: "a.py".into(),
            body_hash: body,
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: Some(0),
        });
    }
    let graph = SharedInMemoryGraph::new(g);
    let vector = InMemoryVectorStore::new();
    let c1 = chunk_id(iid1, rid1, 0);
    let c2 = chunk_id(iid2, rid2, 0);
    vector.register(c1, body);
    vector.register(c2, body);
    vector.set_embedding(body, vec![0.1, 0.2], "m");
    assert_eq!(vector.refcount_for_body(&body), 2);

    {
        let mut gg = graph.write();
        gg.remove_revision(rid1);
    }
    let vq = VectorCleanupQueue::new();
    vq.enqueue_delete(c1);
    let worker = VectorCleanupWorker::new(Arc::new(vq));
    let rep = worker.drain_batch(&vector, 8);
    assert_eq!(rep.deleted_ok, 1);
    assert!(
        vector.vector_for_body(&body).is_some(),
        "shared body embedding must survive first chunk GC"
    );
    assert_eq!(vector.refcount_for_body(&body), 1);
}
