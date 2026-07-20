//! Concurrent commit_graph/commit_vector on the same log id must not duplicate side effects.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use cis_core::{
    FaultAction, FaultInjector, GraphMutationSet, Language, MergeSagaOrchestrator, NodeIdentity,
    NodeKind, NodeRevision, RevisionStatus, SourceSpan, WriteCoordinator,
};
use cis_wal::{
    BranchId, IdentityId, MutationLog, MutationLogStore, MutationPhase, NodeRevisionId,
};

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

struct DelayGraphCommit {
    delay_ms: u64,
}

impl FaultInjector for DelayGraphCommit {
    fn before_graph_commit(&self) -> FaultAction {
        FaultAction::DelayMs(self.delay_ms)
    }
}

#[test]
fn concurrent_commit_graph_applies_once() {
    let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
    let coord = Arc::new(WriteCoordinator::new(Arc::clone(&wal)));
    coord.set_fault_injector(Arc::new(DelayGraphCommit { delay_ms: 50 }));
    let kv = Arc::new(cis_core::MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(kv);
    let _ = coord.reconcile_on_startup(&saga);

    let r = rid(1);
    let set = GraphMutationSet::new(vec![r], [2u8; 32]);
    let id = coord.begin_mutation(&set).unwrap();

    let apply_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = Arc::clone(&coord);
        let n = Arc::clone(&apply_count);
        handles.push(thread::spawn(move || {
            c.commit_graph(id, |g| {
                n.fetch_add(1, Ordering::SeqCst);
                let i = iid(1);
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "a".into(),
                    file_path: "a.py".into(),
                    body_hash: [3u8; 32],
                    signature_hash: [4u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: SourceSpan::UNKNOWN,
                });
                Ok(())
            })
        }));
    }
    let mut ok = 0usize;
    let mut mismatch = 0usize;
    for h in handles {
        match h.join().unwrap() {
            Ok(()) => ok += 1,
            Err(cis_core::CoordinatorError::PhaseMismatch { .. }) => mismatch += 1,
            Err(e) => panic!("unexpected: {:?}", e),
        }
    }
    assert_eq!(ok, 1, "exactly one commit_graph must succeed");
    assert_eq!(mismatch, 3);
    assert_eq!(
        apply_count.load(Ordering::SeqCst),
        1,
        "graph apply must run exactly once"
    );
    assert_eq!(wal.get(id).unwrap().phase, MutationPhase::GraphDone);
}
