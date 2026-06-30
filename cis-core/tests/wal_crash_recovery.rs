//! **Phase 1 / Epic 2.4** — simulated crash at each WAL phase, then `reconcile_on_startup`.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use cis_core::{
    CoordinatorPersistence, GraphMutationSet, MergeSagaOrchestrator, WriteCoordinator,
};
use cis_wal::{
    BranchId, DurableMutationLog, IdentityId, MutationLogStore, MutationPhase, NodeRevisionId,
};

use cis_core::{
    Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
};

fn temp_cis(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-wal-crash-{}-{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn iid(x: u8) -> IdentityId {
    let mut b = [0u8; 16];
    b[14] = x;
    IdentityId(b)
}

fn rid(x: u8) -> NodeRevisionId {
    let mut b = [0u8; 16];
    b[15] = x;
    NodeRevisionId(b)
}

fn reopen(cis: &PathBuf) -> Arc<WriteCoordinator> {
    let wal: Arc<dyn MutationLogStore> =
        Arc::new(DurableMutationLog::open(cis.join("wal.json")).unwrap());
    Arc::new(WriteCoordinator::open(
        wal,
        Some(CoordinatorPersistence {
            cis_dir: cis.clone(),
        }),
    ))
}

fn reconcile(coord: &WriteCoordinator) -> cis_core::RecoveryReport {
    let saga = MergeSagaOrchestrator::new(Arc::new(cis_core::MemoryKv::new()));
    coord.reconcile_on_startup(&saga)
}

#[test]
fn crash_at_pending_marks_failed_and_clears_mutation_index() {
    let cis = temp_cis("pending");
    let r = rid(1);
    {
        let coord = reopen(&cis);
        let _ = reconcile(coord.as_ref());
        let _ = coord.begin_mutation(&GraphMutationSet::new(vec![r], [9u8; 32]));
        // crash — no commit_graph
    }
    let coord2 = reopen(&cis);
    let rep = reconcile(coord2.as_ref());
    assert!(rep.wal_replayed >= 1);
    assert_eq!(coord2.wal().get(1).unwrap().phase, MutationPhase::Failed);
    assert_eq!(coord2.mutation_index().lock().unwrap().len(), 0);
}

#[test]
fn crash_at_graph_done_replays_to_committed() {
    let cis = temp_cis("graph-done");
    let r = rid(2);
    let i = iid(2);
    let mut log_id = 0u64;
    {
        let coord = reopen(&cis);
        let _ = reconcile(coord.as_ref());
        log_id = coord
            .begin_mutation(&GraphMutationSet::new(vec![r], [4u8; 32]))
            .unwrap();
        coord.commit_graph(log_id, |g| {
            g.put_identity(NodeIdentity {
                identity_id: i,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: r,
                identity_id: i,
                branch_id: BranchId([0u8; 16]),
                status: RevisionStatus::Active,
                qualified_name: "gd_fn".into(),
                file_path: "t.py".into(),
                body_hash: [5u8; 32],
                signature_hash: [6u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: cis_core::SourceSpan::UNKNOWN,
            });
            Ok(())
        })
        .unwrap();
        assert_eq!(
            coord.wal().get(log_id).unwrap().phase,
            MutationPhase::GraphDone
        );
    }
    let coord2 = reopen(&cis);
    let rep = reconcile(coord2.as_ref());
    assert!(rep.wal_replayed >= 1);
    assert_eq!(
        coord2.wal().get(log_id).unwrap().phase,
        MutationPhase::Committed
    );
    assert_eq!(coord2.mutation_index().lock().unwrap().len(), 0);
}

#[test]
fn crash_at_vector_done_finalizes_committed() {
    let cis = temp_cis("vector-done");
    let r = rid(3);
    let i = iid(3);
    {
        let coord = reopen(&cis);
        let _ = reconcile(coord.as_ref());
        let id = coord
            .begin_mutation(&GraphMutationSet::new(vec![r], [6u8; 32]))
            .unwrap();
        coord.commit_graph(id, |g| {
            g.put_identity(NodeIdentity {
                identity_id: i,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: r,
                identity_id: i,
                branch_id: BranchId([0u8; 16]),
                status: RevisionStatus::Active,
                qualified_name: "vd_fn".into(),
                file_path: "v.py".into(),
                body_hash: [7u8; 32],
                signature_hash: [8u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: cis_core::SourceSpan::UNKNOWN,
            });
            Ok(())
        })
        .unwrap();
        // Partial vector commit: upsert + VectorDone, stop before Committed
        {
            let g = coord.graph().read();
            let nr = g.get_revision(r).unwrap();
            let cid = cis_core::chunk_id(nr.identity_id, nr.revision_id, 0);
            coord.vector().upsert(cid, vec![1.0, 0.0, 0.0]);
        }
        let rec = coord.wal().get(id).unwrap();
        coord
            .wal()
            .update_phase(id, MutationPhase::VectorDone)
            .unwrap();
        coord
            .mutation_index()
            .lock()
            .unwrap()
            .apply_transition(&rec, MutationPhase::VectorDone);
        coord.persist_snapshots().unwrap();
    }
    let coord2 = reopen(&cis);
    let rep = reconcile(coord2.as_ref());
    assert!(rep.wal_replayed >= 1);
    assert_eq!(coord2.wal().get(1).unwrap().phase, MutationPhase::Committed);
    assert_eq!(coord2.mutation_index().lock().unwrap().len(), 0);
    let g = coord2.graph().read();
    assert!(
        g.revisions()
            .any(|x| x.qualified_name == "vd_fn")
    );
}

#[test]
fn committed_mutation_has_empty_mutation_index() {
    let cis = temp_cis("committed");
    let r = rid(4);
    let i = iid(4);
    {
        let coord = reopen(&cis);
        let _ = reconcile(coord.as_ref());
        let id = coord
            .begin_mutation(&GraphMutationSet::new(vec![r], [1u8; 32]))
            .unwrap();
        coord.commit_graph(id, |g| {
            g.put_identity(NodeIdentity {
                identity_id: i,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: r,
                identity_id: i,
                branch_id: BranchId([0u8; 16]),
                status: RevisionStatus::Active,
                qualified_name: "ok".into(),
                file_path: "o.py".into(),
                body_hash: [2u8; 32],
                signature_hash: [3u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: cis_core::SourceSpan::UNKNOWN,
            });
            Ok(())
        })
        .unwrap();
        coord.commit_vector(id).unwrap();
    }
    let coord2 = reopen(&cis);
    let rep = reconcile(coord2.as_ref());
    assert_eq!(rep.in_flight_wal_records, 0);
    assert_eq!(coord2.mutation_index().lock().unwrap().len(), 0);
    assert_eq!(coord2.wal().get(1).unwrap().phase, MutationPhase::Committed);
}
