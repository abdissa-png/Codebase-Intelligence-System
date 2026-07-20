//! **WriteCoordinator** — WAL phases + graph + vector (**§01.4**, class diagram).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use cis_wal::{
    LogId, MutationIndex, MutationKind, MutationLogError, MutationLogStore, MutationPhase,
    MutationRecord,
};

use crate::chunk_id::chunk_id;
use crate::embedding_queue::{EmbedJob, EmbeddingQueue, EmbeddingQueueState};
use crate::fault_injection::{apply_fault, FaultInjector, NoOpFaultInjector};
use crate::graph::{InMemoryGraph, RevisionStatus};
use crate::shared_graph::SharedInMemoryGraph;
use crate::graph_mutation::GraphMutationSet;
use crate::persistence::{
    graph_snapshot_path, load_state_from_cis_dir, save_vector_snapshot,
    vector_snapshot_path, PersistenceLoadReport,
};
use crate::reconciliation::RecoveryReport;
use crate::saga::MergeSagaOrchestrator;
use crate::vector_store::{InMemoryVectorStore, VectorChunkStore};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoordinatorError {
    #[error(transparent)]
    Wal(#[from] MutationLogError),
    #[error("unknown mutation log id {0}")]
    UnknownMutation(LogId),
    #[error("phase mismatch: want {expected:?} have {got:?}")]
    PhaseMismatch {
        expected: MutationPhase,
        got: MutationPhase,
    },
    #[error("graph apply failed: {0}")]
    Graph(&'static str),
    #[error("persistence: {0}")]
    Persist(String),
    #[error("fault injection: deliberate error")]
    Injected,
}

/// When set, graph/vector JSON snapshots are written under this directory (typically `repo/.cis`).
#[derive(Debug, Clone)]
pub struct CoordinatorPersistence {
    pub cis_dir: PathBuf,
}

pub struct WriteCoordinator {
    wal: Arc<dyn MutationLogStore>,
    graph: SharedInMemoryGraph,
    mutation_index: Mutex<MutationIndex>,
    queue: Mutex<EmbeddingQueue>,
    vector: InMemoryVectorStore,
    persistence: Option<CoordinatorPersistence>,
    /// When true, auto graph/vector flushes during commit are skipped (batch ingest).
    defer_snapshot_flush: AtomicBool,
    /// Optional hook after embedding worker writes vectors (e.g. rebuild ANN index).
    post_embed_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// **NFR-R2:** false until **`reconcile_on_startup`** completes successfully.
    ready: AtomicBool,
    /// **Phase 4** — optional fault injection for hardening tests.
    fault_injector: Mutex<Arc<dyn FaultInjector>>,
    /// Serializes phase check + side effects + `update_phase` for commit_graph/commit_vector.
    commit_lock: Mutex<()>,
}

impl std::fmt::Debug for WriteCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteCoordinator")
            .field("ready", &self.ready.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl WriteCoordinator {
    pub fn new(wal: Arc<dyn MutationLogStore>) -> Self {
        Self::new_inner(wal, InMemoryGraph::default(), InMemoryVectorStore::new(), None)
    }

    /// Open coordinator with optional `.cis/` snapshots + durable WAL already attached.
    pub fn open(
        wal: Arc<dyn MutationLogStore>,
        persistence: Option<CoordinatorPersistence>,
    ) -> Self {
        let mut graph = InMemoryGraph::default();
        let vector = InMemoryVectorStore::new();
        if let Some(ref p) = persistence {
            let _ = load_state_from_cis_dir(&p.cis_dir, &mut graph, &vector);
        }
        Self::new_inner(wal, graph, vector, persistence)
    }

    fn new_inner(
        wal: Arc<dyn MutationLogStore>,
        graph: InMemoryGraph,
        vector: InMemoryVectorStore,
        persistence: Option<CoordinatorPersistence>,
    ) -> Self {
        Self {
            wal,
            graph: SharedInMemoryGraph::new(graph),
            mutation_index: Mutex::new(MutationIndex::new()),
            queue: Mutex::new(EmbeddingQueue::new()),
            vector,
            persistence,
            defer_snapshot_flush: AtomicBool::new(false),
            post_embed_hook: Mutex::new(None),
            ready: AtomicBool::new(false),
            fault_injector: Mutex::new(Arc::new(NoOpFaultInjector)),
            commit_lock: Mutex::new(()),
        }
    }

    /// Replace the fault injector (hardening tests).
    pub fn set_fault_injector(&self, injector: Arc<dyn FaultInjector>) {
        *self.fault_injector.lock().unwrap() = injector;
    }

    pub fn fault_injector(&self) -> Arc<dyn FaultInjector> {
        Arc::clone(&*self.fault_injector.lock().unwrap())
    }

    fn apply_hook(&self, action: crate::fault_injection::FaultAction) -> Result<(), CoordinatorError> {
        apply_fault(action).map_err(|_| CoordinatorError::Injected)
    }

    /// Defer per-commit graph/vector JSON writes until [`Self::flush_committed_snapshots`].
    pub fn set_defer_snapshot_flush(&self, defer: bool) {
        self.defer_snapshot_flush.store(defer, Ordering::SeqCst);
    }

    /// Register a callback invoked after the embedding worker stores new vectors.
    pub fn set_post_embed_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self.post_embed_hook.lock().unwrap() = hook;
    }

    pub fn run_post_embed_hook(&self) {
        if let Some(h) = self.post_embed_hook.lock().unwrap().as_ref() {
            h();
        }
    }

    /// Flush graph + vector snapshots once (e.g. end of batch ingest).
    pub fn flush_committed_snapshots(&self) -> Result<(), CoordinatorError> {
        self.flush_graph_snapshot_inner(true)?;
        self.flush_vector_snapshot_inner(true)?;
        Ok(())
    }

    fn should_skip_auto_snapshot_flush(&self) -> bool {
        self.defer_snapshot_flush.load(Ordering::SeqCst)
            || !crate::persistence::snapshot_persist_enabled()
    }

    pub fn persistence_dir(&self) -> Option<&Path> {
        self.persistence.as_ref().map(|p| p.cis_dir.as_path())
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Load `.cis/graph.json` + `.cis/vector.json` into this coordinator (e.g. after in-memory construction).
    pub fn load_snapshots(&self) -> PersistenceLoadReport {
        let mut report = PersistenceLoadReport::default();
        let Some(ref p) = self.persistence else {
            return report;
        };
        let mut g = self.graph.write();
        report = load_state_from_cis_dir(&p.cis_dir, &mut *g, &self.vector);
        report
    }

    fn flush_graph_snapshot(&self) -> Result<(), CoordinatorError> {
        self.flush_graph_snapshot_inner(false)
    }

    fn flush_graph_delta(&self, affected: &[cis_wal::NodeRevisionId]) -> Result<(), CoordinatorError> {
        let Some(ref p) = self.persistence else {
            return Ok(());
        };
        if self.should_skip_auto_snapshot_flush() {
            return Ok(());
        }
        let g = self.graph.read();
        crate::graph_store::save_graph_delta(&p.cis_dir, &g, affected)
            .map_err(|e| CoordinatorError::Persist(e.to_string()))
    }

    fn flush_vector_snapshot(&self) -> Result<(), CoordinatorError> {
        self.flush_vector_snapshot_inner(false)
    }

    fn flush_graph_snapshot_inner(&self, force: bool) -> Result<(), CoordinatorError> {
        let Some(ref p) = self.persistence else {
            return Ok(());
        };
        if !force && self.should_skip_auto_snapshot_flush() {
            return Ok(());
        }
        let g = self.graph.read();
        crate::graph_store::save_graph_with_backend(&p.cis_dir, &g)
            .map_err(|e| CoordinatorError::Persist(e.to_string()))
    }

    fn flush_vector_snapshot_inner(&self, force: bool) -> Result<(), CoordinatorError> {
        let Some(ref p) = self.persistence else {
            return Ok(());
        };
        if !force && self.should_skip_auto_snapshot_flush() {
            return Ok(());
        }
        save_vector_snapshot(&vector_snapshot_path(&p.cis_dir), &self.vector)
            .map_err(|e| CoordinatorError::Persist(e.to_string()))
    }

    pub fn mutation_index(&self) -> &Mutex<MutationIndex> {
        &self.mutation_index
    }

    /// Flush graph + vector JSON snapshots when persistence is enabled.
    pub fn persist_snapshots(&self) -> Result<(), CoordinatorError> {
        self.flush_graph_snapshot_inner(true)?;
        self.flush_vector_snapshot_inner(true)?;
        Ok(())
    }

    /// `VectorDone` → `Committed` without re-upserting chunks (crash after vector flush).
    pub fn finalize_committed(&self, id: LogId) -> Result<(), CoordinatorError> {
        let rec = self.wal.get(id).ok_or(CoordinatorError::UnknownMutation(id))?;
        if rec.phase != MutationPhase::VectorDone {
            return Err(CoordinatorError::PhaseMismatch {
                expected: MutationPhase::VectorDone,
                got: rec.phase,
            });
        }
        let before = rec;
        self.wal.update_phase(id, MutationPhase::Committed)?;
        self.mutation_index
            .lock()
            .unwrap()
            .apply_transition(&before, MutationPhase::Committed);
        self.flush_graph_snapshot_inner(true)?;
        self.flush_vector_snapshot_inner(true)?;
        Ok(())
    }

    /// Finish in-flight WAL rows after loading graph snapshot (**Phase 1** replay).
    fn replay_inflight_wal(&self) -> Result<usize, CoordinatorError> {
        let mut rows = self.wal.iter_all();
        rows.sort_by_key(|r| r.log_id);
        let mut replayed = 0usize;
        for rec in rows {
            match rec.phase {
                MutationPhase::Pending => {
                    self.wal.update_phase(rec.log_id, MutationPhase::Failed)?;
                    replayed += 1;
                }
                MutationPhase::GraphDone => {
                    if Self::is_graph_only_kind(&rec.kind) {
                        self.replay_status_mutation_at_graph_done(&rec)?;
                    } else {
                        self.commit_vector(rec.log_id)?;
                    }
                    replayed += 1;
                }
                MutationPhase::VectorDone => {
                    if Self::is_graph_only_kind(&rec.kind) {
                        self.finalize_graph_only(rec.log_id)?;
                    } else {
                        self.finalize_committed(rec.log_id)?;
                    }
                    replayed += 1;
                }
                _ => {}
            }
        }
        self.sync_rebuild_index_from_wal();
        Ok(replayed)
    }

    /// Single startup pipeline: WAL replay + **`MutationIndex`** rebuild.
    /// Saga orphan compensation is deferred until after merge resume
    /// ([`crate::merge_engine::recover_inflight_merges`]) so resumable merges are not purged first.
    pub fn reconcile_on_startup(&self, saga: &MergeSagaOrchestrator) -> RecoveryReport {
        let report = self.reconcile_now(saga);
        if report.wal_replay_failed {
            eprintln!("cis: refusing ready — WAL replay failed");
            self.ready.store(false, Ordering::SeqCst);
        } else {
            self.ready.store(true, Ordering::SeqCst);
        }
        report
    }

    /// Reusable reconciliation body (**Phase 3.3**): WAL replay + index rebuild (no saga purge).
    pub fn reconcile_now(&self, saga: &MergeSagaOrchestrator) -> RecoveryReport {
        let _ = saga; // reserved for future WAL↔saga cross-checks
        let mut wal_replay_failed = false;
        let wal_replayed = match self.replay_inflight_wal() {
            Ok(n) => n,
            Err(e) => {
                eprintln!("cis: WAL replay error: {:?}", e);
                wal_replay_failed = true;
                0
            }
        };
        self.sync_rebuild_index_from_wal();
        let mut rows = self.wal.iter_all();
        rows.sort_by_key(|r| r.log_id);
        let in_flight_wal_records = rows.iter().filter(|r| r.phase.is_in_flight()).count();
        let mutation_index_entries = self.mutation_index.lock().unwrap().len();
        RecoveryReport {
            mutation_index_entries,
            in_flight_wal_records,
            sagas_compensated: 0,
            wal_replayed,
            wal_replay_failed,
            consistency: None,
        }
    }

    pub fn set_embedding_queue_thresholds(&self, hwm: u32, lwm: u32) {
        let mut q = self.queue.lock().unwrap();
        q.hwm = hwm as usize;
        q.lwm = lwm as usize;
    }

    pub fn wal(&self) -> Arc<dyn MutationLogStore> {
        Arc::clone(&self.wal)
    }

    pub fn graph(&self) -> &SharedInMemoryGraph {
        &self.graph
    }

    pub fn vector(&self) -> &InMemoryVectorStore {
        &self.vector
    }

    /// Same **`InMemoryVectorStore`** as **`vector()`**, as the trait object used by merge rollback + DLQ worker.
    pub fn vector_chunk_store(&self) -> &dyn VectorChunkStore {
        &self.vector
    }

    pub fn drain_embed_jobs(&self, max: usize) -> Vec<EmbedJob> {
        let mut q = self.queue.lock().unwrap();
        let mut out = Vec::with_capacity(max.min(q.depth()));
        for _ in 0..max {
            let Some(job) = q.drain_one() else {
                break;
            };
            out.push(job);
        }
        out
    }

    pub fn reenqueue_embed_front(&self, job: EmbedJob) {
        self.queue.lock().unwrap().reenqueue_front(job);
    }

    pub fn embedding_queue_depth(&self) -> usize {
        self.queue.lock().unwrap().depth()
    }

    pub fn embedding_queue_state(&self) -> EmbeddingQueueState {
        self.queue.lock().unwrap().state()
    }

    pub fn embedding_queue_hwm(&self) -> u32 {
        self.queue.lock().unwrap().hwm as u32
    }

    pub fn embedding_queue_lwm(&self) -> u32 {
        self.queue.lock().unwrap().lwm as u32
    }

    pub fn enqueue_embed_job(&self, job: EmbedJob) {
        self.queue.lock().unwrap().enqueue(job);
    }

    pub fn begin_mutation(&self, set: &GraphMutationSet) -> Result<LogId, CoordinatorError> {
        let inj = self.fault_injector.lock().unwrap();
        self.apply_hook(inj.before_wal_append())?;
        drop(inj);
        let rec = MutationRecord {
            log_id: 0,
            kind: MutationKind::Single,
            phase: MutationPhase::Pending,
            affected_revisions: set.affected_revisions.clone(),
            payload_checksum: set.payload_checksum,
            created_at_ms: 0,
        };
        let id = self.wal.append(rec)?;
        let r = self.wal.get(id).unwrap();
        self.mutation_index.lock().unwrap().register_new_record(&r);
        Ok(id)
    }

    pub fn commit_graph(
        &self,
        id: LogId,
        apply: impl FnOnce(&mut InMemoryGraph) -> Result<(), &'static str>,
    ) -> Result<(), CoordinatorError> {
        let _commit = self.commit_lock.lock().unwrap();
        let rec = self.wal.get(id).ok_or(CoordinatorError::UnknownMutation(id))?;
        if rec.phase != MutationPhase::Pending {
            return Err(CoordinatorError::PhaseMismatch {
                expected: MutationPhase::Pending,
                got: rec.phase,
            });
        }
        {
            let inj = self.fault_injector.lock().unwrap();
            self.apply_hook(inj.before_graph_commit())?;
            drop(inj);
            let mut g = self.graph.write();
            apply(&mut g).map_err(CoordinatorError::Graph)?;
        }
        let before = rec;
        self.wal.update_phase(id, MutationPhase::GraphDone)?;
        self.mutation_index
            .lock()
            .unwrap()
            .apply_transition(&before, MutationPhase::GraphDone);
        self.flush_graph_delta(&before.affected_revisions)?;
        Ok(())
    }

    pub fn commit_vector(&self, id: LogId) -> Result<(), CoordinatorError> {
        let _commit = self.commit_lock.lock().unwrap();
        let rec = self.wal.get(id).ok_or(CoordinatorError::UnknownMutation(id))?;
        if rec.phase != MutationPhase::GraphDone {
            return Err(CoordinatorError::PhaseMismatch {
                expected: MutationPhase::GraphDone,
                got: rec.phase,
            });
        }
        {
            let inj = self.fault_injector.lock().unwrap();
            self.apply_hook(inj.before_vector_commit())?;
            drop(inj);
            let g = self.graph.read();
            for rid in &rec.affected_revisions {
                let Some(nr) = g.get_revision(*rid) else {
                    continue;
                };
                let cid = chunk_id(nr.identity_id, nr.revision_id, 0);
                self.queue.lock().unwrap().enqueue(EmbedJob {
                    wal_log_id: id,
                    chunk_id: cid,
                    text_digest: nr.body_hash,
                });
                self.vector.register(cid, nr.body_hash);
            }
        }
        let rec_gd = self.wal.get(id).unwrap();
        self.wal.update_phase(id, MutationPhase::VectorDone)?;
        self.mutation_index
            .lock()
            .unwrap()
            .apply_transition(&rec_gd, MutationPhase::VectorDone);
        let rec_vd = self.wal.get(id).unwrap();
        self.wal.update_phase(id, MutationPhase::Committed)?;
        self.mutation_index
            .lock()
            .unwrap()
            .apply_transition(&rec_vd, MutationPhase::Committed);
        self.flush_graph_snapshot()?;
        self.flush_vector_snapshot()?;
        Ok(())
    }

    pub fn sync_rebuild_index_from_wal(&self) {
        let mut rows = self.wal.iter_all();
        rows.sort_by_key(|r| r.log_id);
        self.mutation_index
            .lock()
            .unwrap()
            .rebuild_from_wal(&rows);
    }

    pub fn mark_failed(&self, id: LogId) -> Result<(), CoordinatorError> {
        let _ = self.wal.get(id).ok_or(CoordinatorError::UnknownMutation(id))?;
        self.wal.update_phase(id, MutationPhase::Failed)?;
        self.sync_rebuild_index_from_wal();
        Ok(())
    }

    fn is_graph_only_kind(kind: &MutationKind) -> bool {
        matches!(
            kind,
            MutationKind::PromoteSpeculative { .. } | MutationKind::RevertSpeculative { .. }
        )
    }

    /// Begin a WAL-backed graph-only status mutation (promote/revert speculative).
    pub fn begin_status_mutation(
        &self,
        kind: MutationKind,
        set: &GraphMutationSet,
    ) -> Result<LogId, CoordinatorError> {
        let inj = self.fault_injector.lock().unwrap();
        self.apply_hook(inj.before_wal_append())?;
        drop(inj);
        let rec = MutationRecord {
            log_id: 0,
            kind,
            phase: MutationPhase::Pending,
            affected_revisions: set.affected_revisions.clone(),
            payload_checksum: set.payload_checksum,
            created_at_ms: 0,
        };
        let id = self.wal.append(rec)?;
        let r = self.wal.get(id).unwrap();
        self.mutation_index.lock().unwrap().register_new_record(&r);
        Ok(id)
    }

    /// `GraphDone` or `VectorDone` → `Committed` for status-only mutations (no vector enqueue).
    pub fn finalize_graph_only(&self, id: LogId) -> Result<(), CoordinatorError> {
        let rec = self.wal.get(id).ok_or(CoordinatorError::UnknownMutation(id))?;
        if !Self::is_graph_only_kind(&rec.kind) {
            return Err(CoordinatorError::PhaseMismatch {
                expected: MutationPhase::GraphDone,
                got: rec.phase,
            });
        }
        if rec.phase != MutationPhase::GraphDone && rec.phase != MutationPhase::VectorDone {
            return Err(CoordinatorError::PhaseMismatch {
                expected: MutationPhase::GraphDone,
                got: rec.phase,
            });
        }
        let before = rec;
        self.wal.update_phase(id, MutationPhase::Committed)?;
        self.mutation_index
            .lock()
            .unwrap()
            .apply_transition(&before, MutationPhase::Committed);
        self.flush_graph_snapshot_inner(true)?;
        Ok(())
    }

    fn reapply_status_mutation(
        rec: &MutationRecord,
        g: &mut InMemoryGraph,
    ) -> Result<(), &'static str> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        match &rec.kind {
            MutationKind::PromoteSpeculative { .. } => {
                for rid in &rec.affected_revisions {
                    if let Some(rev) = g.get_revision(*rid).cloned() {
                        let mut updated = rev;
                        updated.status = RevisionStatus::Active;
                        g.put_revision(updated);
                    }
                }
            }
            MutationKind::RevertSpeculative { .. } => {
                for rid in &rec.affected_revisions {
                    if let Some(rev) = g.get_revision(*rid).cloned() {
                        let mut updated = rev;
                        updated.status = RevisionStatus::Tombstone;
                        updated.tombstoned_at_ms = Some(ts);
                        g.put_revision(updated);
                    }
                }
            }
            _ => return Err("not a status mutation"),
        }
        Ok(())
    }

    fn replay_status_mutation_at_graph_done(&self, rec: &MutationRecord) -> Result<(), CoordinatorError> {
        {
            let mut g = self.graph.write();
            Self::reapply_status_mutation(rec, &mut g).map_err(CoordinatorError::Graph)?;
        }
        self.finalize_graph_only(rec.log_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_id::chunk_id;
    use cis_wal::{DurableMutationLog, MutationKind, MutationLog, MutationLogStore, NodeRevisionId};

    use crate::graph::{
        Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
    };
    use crate::saga::MergeSagaOrchestrator;
    use cis_wal::{BranchId, IdentityId};

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

    #[test]
    fn happy_path_mutation_pipeline() {
        let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
        let c = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(kv);
        assert!(!c.is_ready());
        let _ = c.reconcile_on_startup(&saga);
        assert!(c.is_ready());
        let r = rid(1);
        let set = GraphMutationSet::new(vec![r], [2u8; 32]);
        let id = c.begin_mutation(&set).unwrap();
        c.commit_graph(id, |g| {
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
                qualified_name: "f".into(),
                file_path: "a.py".into(),
                body_hash: [3u8; 32],
                signature_hash: [4u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                    tombstoned_at_ms: None,
                span: crate::graph::SourceSpan::UNKNOWN,
            });
            Ok(())
        })
        .unwrap();
        c.commit_vector(id).unwrap();
        assert_eq!(wal.get(id).unwrap().phase, MutationPhase::Committed);
        let cid = chunk_id(iid(1), r, 0);
        assert!(c.vector().has(&cid));
    }

    #[test]
    fn durable_wal_survives_coordinator_reopen() {
        use std::fs;

        let dir = std::env::temp_dir().join(format!("cis-coord-wal-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wal.json");

        let r = rid(9);
        {
            let d = DurableMutationLog::create_new(&path).unwrap();
            let wal: Arc<dyn MutationLogStore> = Arc::new(d);
            let c = WriteCoordinator::new(Arc::clone(&wal));
            let kv = Arc::new(crate::kv::MemoryKv::new());
            let saga = MergeSagaOrchestrator::new(kv);
            let _ = c.reconcile_on_startup(&saga);
            let set = GraphMutationSet::new(vec![r], [2u8; 32]);
            let id = c.begin_mutation(&set).unwrap();
            c.commit_graph(id, |g| {
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
                    qualified_name: "g".into(),
                    file_path: "b.py".into(),
                    body_hash: [3u8; 32],
                    signature_hash: [4u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
            c.commit_vector(id).unwrap();
        }

        let wal2: Arc<dyn MutationLogStore> = Arc::new(DurableMutationLog::open(&path).unwrap());
        let c2 = WriteCoordinator::new(Arc::clone(&wal2));
        let kv2 = Arc::new(crate::kv::MemoryKv::new());
        let saga2 = MergeSagaOrchestrator::new(kv2);
        let rep = c2.reconcile_on_startup(&saga2);
        assert_eq!(rep.mutation_index_entries, 0);
        assert_eq!(rep.in_flight_wal_records, 0);
        let mut rows = wal2.iter_all();
        rows.sort_by_key(|x| x.log_id);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].phase, MutationPhase::Committed);
        assert_eq!(rows[0].affected_revisions, vec![r]);
    }

    #[test]
    fn graph_snapshot_survives_coordinator_reopen() {
        use std::fs;

        let dir = std::env::temp_dir().join(format!("cis-coord-snap-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cis = dir.join(".cis");
        fs::create_dir_all(&cis).unwrap();

        let r = rid(3);
        let i = iid(2);
        {
            let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
            let c = WriteCoordinator::open(
                Arc::clone(&wal),
                Some(CoordinatorPersistence {
                    cis_dir: cis.clone(),
                }),
            );
            let kv = Arc::new(crate::kv::MemoryKv::new());
            let saga = MergeSagaOrchestrator::new(kv);
            let _ = c.reconcile_on_startup(&saga);
            let set = GraphMutationSet::new(vec![r], [2u8; 32]);
            let id = c.begin_mutation(&set).unwrap();
            c.commit_graph(id, |g| {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "persisted_fn".into(),
                    file_path: "p.py".into(),
                    body_hash: [3u8; 32],
                    signature_hash: [4u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
            c.commit_vector(id).unwrap();
            assert!(graph_snapshot_path(&cis).exists());
        }

        let wal2: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
        let c2 = WriteCoordinator::open(
            wal2,
            Some(CoordinatorPersistence {
                cis_dir: cis.clone(),
            }),
        );
        let kv2 = Arc::new(crate::kv::MemoryKv::new());
        let saga2 = MergeSagaOrchestrator::new(kv2);
        let _ = c2.reconcile_on_startup(&saga2);
        let g = c2.graph().read();
        let rev = g
            .revisions()
            .find(|x| x.qualified_name == "persisted_fn")
            .expect("symbol restored from snapshot");
        assert_eq!(rev.revision_id, r);
        assert_eq!(rev.identity_id, i);
    }

    #[test]
    fn replays_vector_done_wal_after_restart() {
        use std::fs;

        let dir = std::env::temp_dir().join(format!("cis-coord-vd-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cis = dir.join(".cis");
        fs::create_dir_all(&cis).unwrap();
        let wal_path = cis.join("wal.json");

        let r = rid(6);
        let i = iid(5);
        {
            let wal = DurableMutationLog::create_new(&wal_path).unwrap();
            let wal: Arc<dyn MutationLogStore> = Arc::new(wal);
            let c = WriteCoordinator::open(
                Arc::clone(&wal),
                Some(CoordinatorPersistence {
                    cis_dir: cis.clone(),
                }),
            );
            let kv = Arc::new(crate::kv::MemoryKv::new());
            let saga = MergeSagaOrchestrator::new(kv);
            let _ = c.reconcile_on_startup(&saga);
            let id = c.begin_mutation(&GraphMutationSet::new(vec![r], [2u8; 32])).unwrap();
            c.commit_graph(id, |g| {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "vd".into(),
                    file_path: "v.py".into(),
                    body_hash: [3u8; 32],
                    signature_hash: [4u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
            let rec = wal.get(id).unwrap();
            wal.update_phase(id, MutationPhase::VectorDone).unwrap();
            c.mutation_index()
                .lock()
                .unwrap()
                .apply_transition(&rec, MutationPhase::VectorDone);
            c.persist_snapshots().unwrap();
        }

        let wal2: Arc<dyn MutationLogStore> =
            Arc::new(DurableMutationLog::open(&wal_path).unwrap());
        let c2 = WriteCoordinator::open(
            Arc::clone(&wal2),
            Some(CoordinatorPersistence {
                cis_dir: cis.clone(),
            }),
        );
        let kv2 = Arc::new(crate::kv::MemoryKv::new());
        let saga2 = MergeSagaOrchestrator::new(kv2);
        let rep = c2.reconcile_on_startup(&saga2);
        assert!(rep.wal_replayed >= 1);
        assert_eq!(wal2.get(1).unwrap().phase, MutationPhase::Committed);
    }

    #[test]
    fn replays_graph_done_wal_after_restart() {
        use std::fs;

        let dir = std::env::temp_dir().join(format!("cis-coord-gd-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cis = dir.join(".cis");
        fs::create_dir_all(&cis).unwrap();
        let wal_path = cis.join("wal.json");

        let r = rid(5);
        let i = iid(4);
        {
            let wal = DurableMutationLog::create_new(&wal_path).unwrap();
            let wal: Arc<dyn MutationLogStore> = Arc::new(wal);
            let c = WriteCoordinator::open(
                Arc::clone(&wal),
                Some(CoordinatorPersistence {
                    cis_dir: cis.clone(),
                }),
            );
            let kv = Arc::new(crate::kv::MemoryKv::new());
            let saga = MergeSagaOrchestrator::new(kv);
            let _ = c.reconcile_on_startup(&saga);
            let set = GraphMutationSet::new(vec![r], [2u8; 32]);
            let id = c.begin_mutation(&set).unwrap();
            c.commit_graph(id, |g| {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "inflight".into(),
                    file_path: "i.py".into(),
                    body_hash: [3u8; 32],
                    signature_hash: [4u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
            // Simulate crash after graph commit: WAL at GraphDone, snapshot on disk.
        }

        let wal2: Arc<dyn MutationLogStore> =
            Arc::new(DurableMutationLog::open(&wal_path).unwrap());
        let c2 = WriteCoordinator::open(
            Arc::clone(&wal2),
            Some(CoordinatorPersistence {
                cis_dir: cis.clone(),
            }),
        );
        let kv2 = Arc::new(crate::kv::MemoryKv::new());
        let saga2 = MergeSagaOrchestrator::new(kv2);
        let rep = c2.reconcile_on_startup(&saga2);
        assert!(rep.wal_replayed >= 1);
        assert_eq!(wal2.get(1).unwrap().phase, MutationPhase::Committed);
        let g = c2.graph().read();
        assert!(
            g.revisions()
                .any(|x| x.qualified_name == "inflight")
        );
    }

    #[test]
    fn replays_promote_speculative_at_graph_done() {
        use std::fs;

        let dir = std::env::temp_dir().join(format!("cis-promote-gd-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cis = dir.join(".cis");
        fs::create_dir_all(&cis).unwrap();
        let wal_path = cis.join("wal.json");

        let r = rid(11);
        let i = iid(11);
        {
            let wal = DurableMutationLog::create_new(&wal_path).unwrap();
            let wal: Arc<dyn MutationLogStore> = Arc::new(wal);
            let c = WriteCoordinator::open(
                Arc::clone(&wal),
                Some(CoordinatorPersistence {
                    cis_dir: cis.clone(),
                }),
            );
            let kv = Arc::new(crate::kv::MemoryKv::new());
            let saga = MergeSagaOrchestrator::new(kv);
            let _ = c.reconcile_on_startup(&saga);

            c.commit_graph(
                c.begin_status_mutation(
                    MutationKind::PromoteSpeculative { patch_id: 42 },
                    &GraphMutationSet::new(vec![r], [2u8; 32]),
                )
                .unwrap(),
                |g| {
                    g.put_identity(NodeIdentity {
                        identity_id: i,
                        kind: NodeKind::Function,
                    });
                    g.put_revision(NodeRevision {
                        revision_id: r,
                        identity_id: i,
                        branch_id: BranchId([0u8; 16]),
                        status: RevisionStatus::Active,
                        qualified_name: "promoted".into(),
                        file_path: "p.py".into(),
                        body_hash: [3u8; 32],
                        signature_hash: [4u8; 32],
                        language: Language::Python,
                        parent_revision_id: None,
                        rename_source_id: None,
                        tombstoned_at_ms: None,
                        span: crate::graph::SourceSpan::UNKNOWN,
                    });
                    Ok(())
                },
            )
            .unwrap();
            c.persist_snapshots().unwrap();
        }

        let wal2: Arc<dyn MutationLogStore> =
            Arc::new(DurableMutationLog::open(&wal_path).unwrap());
        assert_eq!(wal2.get(1).unwrap().phase, MutationPhase::GraphDone);

        let c2 = WriteCoordinator::open(
            Arc::clone(&wal2),
            Some(CoordinatorPersistence {
                cis_dir: cis.clone(),
            }),
        );
        let kv2 = Arc::new(crate::kv::MemoryKv::new());
        let saga2 = MergeSagaOrchestrator::new(kv2);
        let rep = c2.reconcile_on_startup(&saga2);
        assert!(rep.wal_replayed >= 1);
        assert_eq!(wal2.get(1).unwrap().phase, MutationPhase::Committed);
        let g = c2.graph().read();
        let rev = g.get_revision(r).expect("revision present");
        assert_eq!(rev.status, RevisionStatus::Active);
    }

    #[test]
    fn replays_promote_speculative_at_vector_done() {
        use std::fs;

        let dir = std::env::temp_dir().join(format!("cis-promote-vd-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cis = dir.join(".cis");
        fs::create_dir_all(&cis).unwrap();
        let wal_path = cis.join("wal.json");

        let r = rid(12);
        let i = iid(12);
        {
            let wal = DurableMutationLog::create_new(&wal_path).unwrap();
            let wal: Arc<dyn MutationLogStore> = Arc::new(wal);
            let c = WriteCoordinator::open(
                Arc::clone(&wal),
                Some(CoordinatorPersistence {
                    cis_dir: cis.clone(),
                }),
            );
            let kv = Arc::new(crate::kv::MemoryKv::new());
            let saga = MergeSagaOrchestrator::new(kv);
            let _ = c.reconcile_on_startup(&saga);

            let log_id = c
                .begin_status_mutation(
                    MutationKind::PromoteSpeculative { patch_id: 99 },
                    &GraphMutationSet::new(vec![r], [2u8; 32]),
                )
                .unwrap();
            c.commit_graph(log_id, |g| {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "vd_promote".into(),
                    file_path: "v.ts".into(),
                    body_hash: [3u8; 32],
                    signature_hash: [4u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
            let rec = wal.get(log_id).unwrap();
            wal.update_phase(log_id, MutationPhase::VectorDone).unwrap();
            c.mutation_index()
                .lock()
                .unwrap()
                .apply_transition(&rec, MutationPhase::VectorDone);
            c.persist_snapshots().unwrap();
        }

        let wal2: Arc<dyn MutationLogStore> =
            Arc::new(DurableMutationLog::open(&wal_path).unwrap());
        assert_eq!(wal2.get(1).unwrap().phase, MutationPhase::VectorDone);

        let c2 = WriteCoordinator::open(
            Arc::clone(&wal2),
            Some(CoordinatorPersistence {
                cis_dir: cis.clone(),
            }),
        );
        let kv2 = Arc::new(crate::kv::MemoryKv::new());
        let saga2 = MergeSagaOrchestrator::new(kv2);
        let rep = c2.reconcile_on_startup(&saga2);
        assert!(rep.wal_replayed >= 1);
        assert_eq!(wal2.get(1).unwrap().phase, MutationPhase::Committed);
    }
}
