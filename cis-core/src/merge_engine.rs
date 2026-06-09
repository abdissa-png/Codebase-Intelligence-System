//! **Merge engine** — Phase A classifier, Phase B promotion, Phase C edge reconciliation (**FR-1.14**, **FR-1.16**).
//!
//! Three-phase merge following v2.6 spec:
//! - **Phase A**: Diff-based identity classification using revision lineage + body/signature hashes
//! - **Phase B**: Promotion loop — winning revisions bound to target branch, losers orphaned
//! - **Phase C**: Edge reconciliation — dangling edge cleanup, signature drift detection, cardinality enforcement

use std::collections::{HashMap, HashSet};

use cis_wal::{
    merge_cancelled_record, merge_record, BranchId, IdentityId, LogId, MergeId, MutationLogError,
    MutationLogStore, NodeRevisionId,
};

use crate::merge_saga_batch::SagaEdgeBatch;

use crate::body_store::BodyStore;
use crate::branch_reconciliation_tracker::BranchReconciliationTracker;
use crate::graph::{
    validate_edge_cardinality, EdgeType, InMemoryGraph, RevisionStatus,
};
use crate::kv::MemoryKv;
use crate::merge_control::MergeControl;
use crate::merge_lock::{merge_lock_holder, release_merge_lock};
use crate::revision_index::revision_binding_kv_key;
use crate::vector_store::VectorChunkStore;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MergeWorkflowStatus {
    #[default]
    Ready,
    RequiresResolution,
    InProgress,
}

#[derive(Debug, Default, Clone)]
pub struct MergeReport {
    pub status: MergeWorkflowStatus,
    pub resolved_count: usize,
    pub conflicts: Vec<String>,
    pub rename_detections: Vec<String>,
    pub signature_drift: Vec<String>,
    pub dangling_edges: Vec<String>,
    pub cardinality_violations: Vec<String>,
}

/// **Phase A** identity-level classes (v2.6 — feeds `MergeReport`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeIdentityClass {
    Clean,
    OursOnly,
    TheirsOnly,
    BothModifiedSame,
    BothModifiedUnresolved,
    RenamedCandidate,
    BothDeleted,
    BaseDeletedFeatureNew,
    OursDeletedTheirsModified,
    TheirsDeletedOursModified,
    OursNew,
    TheirsNew,
    BothNewDivergent,
}

/// Per-identity classification produced by Phase A.
#[derive(Debug, Clone)]
pub struct ClassifiedMergeIdentity {
    pub identity_id: IdentityId,
    pub class: MergeIdentityClass,
    pub base_revision: Option<NodeRevisionId>,
    pub ours_revision: Option<NodeRevisionId>,
    pub theirs_revision: Option<NodeRevisionId>,
    pub qualified_name: String,
}

/// Conflict resolution strategy for `BothModifiedUnresolved` identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStrategy {
    Ours,
    Theirs,
}

/// Phase A output: report + per-identity classifications.
#[derive(Debug, Clone)]
pub struct PhaseAResult {
    pub report: MergeReport,
    pub classified: Vec<ClassifiedMergeIdentity>,
}

/// Phase B output: which revisions were promoted and which were orphaned.
#[derive(Debug, Clone, Default)]
pub struct PhaseBResult {
    pub promoted: Vec<(IdentityId, NodeRevisionId)>,
    pub orphaned_revisions: Vec<NodeRevisionId>,
    pub unresolved_conflicts: Vec<ClassifiedMergeIdentity>,
}

/// Phase C output: edge reconciliation results.
#[derive(Debug, Clone, Default)]
pub struct PhaseCResult {
    pub dangling_edges_removed: usize,
    pub signature_drifts: Vec<String>,
    pub signature_reresolved: usize,
    pub cardinality_violations: Vec<String>,
    pub edges_checked: usize,
    /// Revisions whose outbound edges were rebuilt from source bodies.
    pub edges_regenerated: usize,
    /// Revisions flagged when body store was unavailable for regen.
    pub needs_edge_regen: Vec<NodeRevisionId>,
    /// Edge batches applied during Phase C (for saga compensate on cancel).
    pub saga_batches: Vec<SagaEdgeBatch>,
}

#[derive(Debug, Clone, Default)]
pub struct MergeRecoveryReport {
    pub resumed: usize,
    pub compensated: usize,
    pub entries: Vec<MergeRecoveryEntry>,
}

#[derive(Debug, Clone)]
pub struct MergeRecoveryEntry {
    pub merge_id: MergeId,
    pub resumed_from_phase: Option<String>,
    pub compensated: bool,
    pub edges_regenerated: usize,
    pub dangling_edges_removed: usize,
    pub signature_reresolved: usize,
    pub cardinality_violations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResumeMergeOutcome {
    pub resumed_from_phase: String,
    pub phase_a: PhaseAResult,
    pub phase_b: PhaseBResult,
    pub phase_c: PhaseCResult,
}

pub fn merge_reconciliation_job_id(merge_id: MergeId) -> u64 {
    u64::from_le_bytes(merge_id.0[0..8].try_into().expect("merge id is 16 bytes"))
}

fn saga_phase_label(phase: &crate::saga::SagaPhase) -> String {
    match phase {
        crate::saga::SagaPhase::Intent => "Intent".into(),
        crate::saga::SagaPhase::Classifying => "Classifying".into(),
        crate::saga::SagaPhase::Promoting => "Promoting".into(),
        crate::saga::SagaPhase::EdgeBatch { seq } => format!("EdgeBatch:{seq}"),
        crate::saga::SagaPhase::PointerSwap => "PointerSwap".into(),
        crate::saga::SagaPhase::Committed => "Committed".into(),
    }
}

fn phase_c_reconcile_tracked(
    graph: &mut InMemoryGraph,
    kv: &MemoryKv,
    body_store: Option<&BodyStore>,
    target: BranchId,
    merge_id: MergeId,
    promoted: &[(IdentityId, NodeRevisionId)],
    classified: Option<&[ClassifiedMergeIdentity]>,
    tracker: Option<&BranchReconciliationTracker>,
) -> PhaseCResult {
    let job_id = merge_reconciliation_job_id(merge_id);
    if let Some(t) = tracker {
        t.register(target, job_id);
    }
    let pc = phase_c_reconcile_edges_full(graph, kv, body_store, target, promoted, classified);
    if pc.needs_edge_regen.is_empty() {
        if let Some(t) = tracker {
            t.on_job_complete(target, job_id);
        }
    }
    pc
}

/// Persisted merge context for crash-resume.
///
/// Stored in KV under `merge_ctx:{merge_id_hex}` so a process restart
/// can detect an in-flight merge and resume from the last completed phase.
#[derive(Debug, Clone)]
pub struct MergeContext {
    pub merge_id: MergeId,
    pub ours_branch: BranchId,
    pub theirs_branch: BranchId,
    pub base_branch: BranchId,
    pub target_branch: BranchId,
    pub strategy: Option<MergeStrategy>,
}

impl MergeContext {
    fn kv_key(merge_id: MergeId) -> String {
        format!(
            "merge_ctx:{}",
            merge_id.0.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        )
    }

    pub fn persist(&self, kv: &MemoryKv) {
        let mut buf = Vec::with_capacity(16 * 5 + 1);
        buf.extend_from_slice(&self.merge_id.0);
        buf.extend_from_slice(&self.ours_branch.0);
        buf.extend_from_slice(&self.theirs_branch.0);
        buf.extend_from_slice(&self.base_branch.0);
        buf.extend_from_slice(&self.target_branch.0);
        buf.push(match self.strategy {
            None => 0,
            Some(MergeStrategy::Ours) => 1,
            Some(MergeStrategy::Theirs) => 2,
        });
        kv.set(&Self::kv_key(self.merge_id), buf);
    }

    pub fn load(kv: &MemoryKv, merge_id: MergeId) -> Option<Self> {
        let buf = kv.get(&Self::kv_key(merge_id))?;
        if buf.len() < 16 * 5 + 1 {
            return None;
        }
        fn read16(buf: &[u8], off: usize) -> [u8; 16] {
            let mut a = [0u8; 16];
            a.copy_from_slice(&buf[off..off + 16]);
            a
        }
        let strategy = match buf[80] {
            1 => Some(MergeStrategy::Ours),
            2 => Some(MergeStrategy::Theirs),
            _ => None,
        };
        Some(MergeContext {
            merge_id: MergeId(read16(&buf, 0)),
            ours_branch: BranchId(read16(&buf, 16)),
            theirs_branch: BranchId(read16(&buf, 32)),
            base_branch: BranchId(read16(&buf, 48)),
            target_branch: BranchId(read16(&buf, 64)),
            strategy,
        })
    }

    pub fn remove(kv: &MemoryKv, merge_id: MergeId) {
        kv.delete(&Self::kv_key(merge_id));
    }
}

/// Resume a merge from the last persisted saga phase.
pub fn resume_merge(
    graph: &mut InMemoryGraph,
    kv: &MemoryKv,
    body_store: Option<&BodyStore>,
    merge_id: MergeId,
    saga: &crate::saga::MergeSagaOrchestrator,
    tracker: Option<&BranchReconciliationTracker>,
) -> Option<ResumeMergeOutcome> {
    let ctx = MergeContext::load(kv, merge_id)?;
    let phase = saga.load(merge_id)?;
    let resumed_from_phase = saga_phase_label(&phase);

    match phase {
        crate::saga::SagaPhase::Committed => None,

        crate::saga::SagaPhase::Intent | crate::saga::SagaPhase::Classifying => {
            saga.persist(merge_id, crate::saga::SagaPhase::Classifying);
            let pa = phase_a_for_merge(
                graph,
                kv,
                merge_id,
                ctx.ours_branch,
                ctx.theirs_branch,
                ctx.target_branch,
                ctx.base_branch,
            );
            if pa.report.status == MergeWorkflowStatus::RequiresResolution && ctx.strategy.is_none()
            {
                return None;
            }
            saga.persist(merge_id, crate::saga::SagaPhase::Promoting);
            let pb = phase_b_promote(kv, graph, ctx.target_branch, &pa.classified, ctx.strategy);
            saga.persist(merge_id, crate::saga::SagaPhase::EdgeBatch { seq: 0 });
            let pc = phase_c_reconcile_tracked(
                graph,
                kv,
                body_store,
                ctx.target_branch,
                merge_id,
                &pb.promoted,
                Some(&pa.classified),
                tracker,
            );
            saga.persist(merge_id, crate::saga::SagaPhase::Committed);
            MergeContext::remove(kv, merge_id);
            Some(ResumeMergeOutcome {
                resumed_from_phase,
                phase_a: pa,
                phase_b: pb,
                phase_c: pc,
            })
        }

        crate::saga::SagaPhase::Promoting => {
            let pa = phase_a_for_merge(
                graph,
                kv,
                merge_id,
                ctx.ours_branch,
                ctx.theirs_branch,
                ctx.target_branch,
                ctx.base_branch,
            );
            saga.persist(merge_id, crate::saga::SagaPhase::Promoting);
            let pb = phase_b_promote(kv, graph, ctx.target_branch, &pa.classified, ctx.strategy);
            saga.persist(merge_id, crate::saga::SagaPhase::EdgeBatch { seq: 0 });
            let pc = phase_c_reconcile_tracked(
                graph,
                kv,
                body_store,
                ctx.target_branch,
                merge_id,
                &pb.promoted,
                Some(&pa.classified),
                tracker,
            );
            saga.persist(merge_id, crate::saga::SagaPhase::Committed);
            MergeContext::remove(kv, merge_id);
            Some(ResumeMergeOutcome {
                resumed_from_phase,
                phase_a: pa,
                phase_b: pb,
                phase_c: pc,
            })
        }

        crate::saga::SagaPhase::EdgeBatch { .. } | crate::saga::SagaPhase::PointerSwap => {
            let pa = phase_a_for_merge(
                graph,
                kv,
                merge_id,
                ctx.ours_branch,
                ctx.theirs_branch,
                ctx.target_branch,
                ctx.base_branch,
            );
            let promoted: Vec<_> = pa
                .classified
                .iter()
                .filter_map(|ci| {
                    let key = revision_binding_kv_key(ctx.target_branch, ci.identity_id);
                    kv.get(&key).and_then(|v| {
                        if v.len() == 16 {
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(&v);
                            Some((ci.identity_id, NodeRevisionId(arr)))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            saga.persist(merge_id, crate::saga::SagaPhase::EdgeBatch { seq: 0 });
            let pc = phase_c_reconcile_tracked(
                graph,
                kv,
                body_store,
                ctx.target_branch,
                merge_id,
                &promoted,
                Some(&pa.classified),
                tracker,
            );
            saga.persist(merge_id, crate::saga::SagaPhase::Committed);
            MergeContext::remove(kv, merge_id);
            let pb = PhaseBResult {
                promoted,
                ..Default::default()
            };
            Some(ResumeMergeOutcome {
                resumed_from_phase,
                phase_a: pa,
                phase_b: pb,
                phase_c: pc,
            })
        }
    }
}

fn merge_id_from_saga_key(k: &str) -> Option<MergeId> {
    let hex = k.strip_prefix("saga_state:")?;
    if hex.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(MergeId(b))
}

/// On startup: resume in-flight merges that still hold the lock, else compensate.
pub fn recover_inflight_merges(
    graph: &mut InMemoryGraph,
    kv: &MemoryKv,
    body_store: &BodyStore,
    saga: &crate::saga::MergeSagaOrchestrator,
    merge_control: &MergeControl,
    vector: &dyn VectorChunkStore,
    gate: &crate::merge_gate::MergeRecoveryGate,
    tracker: Option<&BranchReconciliationTracker>,
) -> MergeRecoveryReport {
    let mut report = MergeRecoveryReport::default();
    let rows = kv.scan_prefix("saga_state:");
    for (k, v) in rows {
        if v.first().copied() == Some(6) {
            continue;
        }
        let Some(merge_id) = merge_id_from_saga_key(&k) else {
            kv.delete(&k);
            report.compensated += 1;
            continue;
        };
        let Some(ctx) = MergeContext::load(kv, merge_id) else {
            saga.purge_merge_saga_state(merge_id);
            report.compensated += 1;
            continue;
        };
        if merge_lock_holder(kv, ctx.target_branch) == Some(merge_id) {
            if let Some(out) = resume_merge(graph, kv, Some(body_store), merge_id, saga, tracker) {
                let _ = release_merge_lock(kv, ctx.target_branch, merge_id);
                report.resumed += 1;
                report.entries.push(MergeRecoveryEntry {
                    merge_id,
                    resumed_from_phase: Some(out.resumed_from_phase),
                    compensated: false,
                    edges_regenerated: out.phase_c.edges_regenerated,
                    dangling_edges_removed: out.phase_c.dangling_edges_removed,
                    signature_reresolved: out.phase_c.signature_reresolved,
                    cardinality_violations: out.phase_c.cardinality_violations,
                });
                continue;
            }
        }
        gate.begin_rollback(ctx.target_branch);
        let _ = merge_control.cancel_merge(
            merge_id,
            ctx.target_branch,
            graph,
            vector,
            None,
            &[],
        );
        saga.purge_merge_saga_state(merge_id);
        MergeContext::remove(kv, merge_id);
        gate.end_rollback(ctx.target_branch);
        report.compensated += 1;
        report.entries.push(MergeRecoveryEntry {
            merge_id,
            resumed_from_phase: None,
            compensated: true,
            edges_regenerated: 0,
            dangling_edges_removed: 0,
            signature_reresolved: 0,
            cardinality_violations: vec![],
        });
    }
    report
}

// ---------------------------------------------------------------------------
// WAL helpers (unchanged)
// ---------------------------------------------------------------------------

pub fn append_merge_cancelled_marker(
    wal: &dyn MutationLogStore,
    merge_id: MergeId,
) -> Result<LogId, MutationLogError> {
    wal.append(merge_cancelled_record(merge_id))
}

/// Append marker **after** successful cross-store rollback (ordering contract).
pub fn append_merge_cancelled_after_control(
    wal: &dyn MutationLogStore,
    merge_id: MergeId,
    rollback_ok: bool,
) -> Result<Option<LogId>, MutationLogError> {
    if !rollback_ok {
        return Ok(None);
    }
    let id = append_merge_cancelled_marker(wal, merge_id)?;
    Ok(Some(id))
}

/// Append **`MutationKind::Merge`** after a successful merge commit.
pub fn append_merge_committed(
    wal: &dyn MutationLogStore,
    merge_id: MergeId,
    promoted: &[(IdentityId, NodeRevisionId)],
) -> Result<LogId, MutationLogError> {
    let affected: Vec<NodeRevisionId> = promoted.iter().map(|(_, r)| *r).collect();
    wal.append(merge_record(merge_id, affected))
}

// ---------------------------------------------------------------------------
// Stub classifier (retained for wiring tests)
// ---------------------------------------------------------------------------

/// Deterministic stub classifier for wiring tests; production uses [`phase_a_classify`].
pub fn classify_identity_stub(
    ours_changed: bool,
    theirs_changed: bool,
    rename_signal: bool,
) -> MergeIdentityClass {
    if rename_signal {
        MergeIdentityClass::RenamedCandidate
    } else if ours_changed && theirs_changed {
        MergeIdentityClass::BothModifiedUnresolved
    } else {
        MergeIdentityClass::Clean
    }
}

// ---------------------------------------------------------------------------
// Report builder
// ---------------------------------------------------------------------------

pub fn merge_phase_a_report<'a>(
    items: impl Iterator<Item = (&'a str, MergeIdentityClass)>,
) -> MergeReport {
    let mut rep = MergeReport::default();
    for (label, class) in items {
        match class {
            MergeIdentityClass::Clean
            | MergeIdentityClass::OursOnly
            | MergeIdentityClass::TheirsOnly
            | MergeIdentityClass::BothModifiedSame
            | MergeIdentityClass::BothDeleted
            | MergeIdentityClass::OursNew
            | MergeIdentityClass::TheirsNew => {
                rep.resolved_count += 1;
            }
            MergeIdentityClass::BothModifiedUnresolved
            | MergeIdentityClass::BothNewDivergent => {
                rep.status = MergeWorkflowStatus::RequiresResolution;
                rep.conflicts.push(label.to_string());
            }
            MergeIdentityClass::RenamedCandidate => {
                rep.rename_detections.push(label.to_string());
                rep.resolved_count += 1;
            }
            MergeIdentityClass::BaseDeletedFeatureNew => {
                rep.status = MergeWorkflowStatus::RequiresResolution;
                rep.conflicts.push(format!("{label}:base_deleted_feature"));
            }
            MergeIdentityClass::OursDeletedTheirsModified => {
                rep.status = MergeWorkflowStatus::RequiresResolution;
                rep.conflicts.push(format!("{label}:ours_deleted_theirs_modified"));
            }
            MergeIdentityClass::TheirsDeletedOursModified => {
                rep.status = MergeWorkflowStatus::RequiresResolution;
                rep.conflicts.push(format!("{label}:theirs_deleted_ours_modified"));
            }
        }
    }
    rep
}

pub fn run_phase_a_classify(items: &[(String, MergeIdentityClass)]) -> MergeReport {
    merge_phase_a_report(items.iter().map(|(s, c)| (s.as_str(), *c)))
}

// ---------------------------------------------------------------------------
// KV helpers
// ---------------------------------------------------------------------------

fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

/// Scan all `ri:{branch}:{identity}` entries in KV for a given branch.
fn scan_branch_bindings(
    kv: &MemoryKv,
    branch_id: BranchId,
) -> HashMap<IdentityId, NodeRevisionId> {
    let prefix = format!("ri:{}:", hex16(&branch_id.0));
    let mut map = HashMap::new();
    for (k, v) in kv.scan_prefix(&prefix) {
        let parts: Vec<&str> = k.split(':').collect();
        if parts.len() != 3 || v.len() != 16 {
            continue;
        }
        if let Some(identity_bytes) = parse_hex16(parts[2]) {
            let mut rev = [0u8; 16];
            rev.copy_from_slice(&v);
            map.insert(IdentityId(identity_bytes), NodeRevisionId(rev));
        }
    }
    map
}

/// Snapshot current `ri:` bindings on a branch for pre-merge rollback.
pub fn premerge_bindings_for_branch(
    kv: &MemoryKv,
    branch_id: BranchId,
) -> Vec<(IdentityId, NodeRevisionId)> {
    scan_branch_bindings(kv, branch_id)
        .into_iter()
        .collect()
}

/// Distinct file paths touched by classified identities (for speculative quiescence).
pub fn affected_paths_for_classified(
    graph: &InMemoryGraph,
    classified: &[ClassifiedMergeIdentity],
) -> Vec<String> {
    let mut paths = HashSet::new();
    for ci in classified {
        for rid in [ci.base_revision, ci.ours_revision, ci.theirs_revision]
            .into_iter()
            .flatten()
        {
            if let Some(rev) = graph.get_revision(rid) {
                if !rev.file_path.is_empty() {
                    paths.insert(rev.file_path.clone());
                }
            }
        }
    }
    paths.into_iter().collect()
}

fn identity_body_changed(class: MergeIdentityClass) -> bool {
    !matches!(
        class,
        MergeIdentityClass::Clean | MergeIdentityClass::BothDeleted
    )
}

fn files_for_edge_regen(
    graph: &InMemoryGraph,
    classified: Option<&[ClassifiedMergeIdentity]>,
    target_branch: BranchId,
    promoted: &[(IdentityId, NodeRevisionId)],
) -> HashSet<String> {
    let indexers = crate::language_indexer::default_indexers();
    let class_map: HashMap<IdentityId, MergeIdentityClass> = classified
        .map(|c| c.iter().map(|ci| (ci.identity_id, ci.class)).collect())
        .unwrap_or_default();
    let mut files = HashSet::new();
    for &(iid, rid) in promoted {
        let needs = class_map
            .get(&iid)
            .map(|c| identity_body_changed(*c))
            .unwrap_or(true)
            || graph
                .get_revision(rid)
                .map(|r| r.branch_id != target_branch)
                .unwrap_or(false);
        if needs {
            if let Some(rev) = graph.get_revision(rid) {
                if !rev.file_path.is_empty()
                    && crate::language_indexer::indexer_for_path(&rev.file_path, &indexers).is_some()
                {
                    files.insert(rev.file_path.clone());
                }
            }
        }
    }
    files
}

// ---------------------------------------------------------------------------
// Phase A — Real diff-based identity classifier
// ---------------------------------------------------------------------------

/// Classify a single identity by comparing body hashes across base/ours/theirs.
fn classify_single_identity(
    graph: &InMemoryGraph,
    base_rid: Option<NodeRevisionId>,
    ours_rid: Option<NodeRevisionId>,
    theirs_rid: Option<NodeRevisionId>,
) -> MergeIdentityClass {
    let base_hash = base_rid.and_then(|r| graph.get_revision(r)).map(|r| r.body_hash);
    let ours_hash = ours_rid.and_then(|r| graph.get_revision(r)).map(|r| r.body_hash);
    let theirs_hash = theirs_rid.and_then(|r| graph.get_revision(r)).map(|r| r.body_hash);

    match (base_hash, ours_hash, theirs_hash) {
        (None, None, None) => MergeIdentityClass::Clean,

        // New in ours only
        (None, Some(_), None) => MergeIdentityClass::OursNew,
        // New in theirs only
        (None, None, Some(_)) => MergeIdentityClass::TheirsNew,
        // New in both — check if same content
        (None, Some(o), Some(t)) if o == t => MergeIdentityClass::BothModifiedSame,
        (None, Some(_), Some(_)) => MergeIdentityClass::BothNewDivergent,

        // All three present — compare hashes
        (Some(b), Some(o), Some(t)) => {
            if o == b && t == b {
                MergeIdentityClass::Clean
            } else if o == b && t != b {
                MergeIdentityClass::TheirsOnly
            } else if o != b && t == b {
                MergeIdentityClass::OursOnly
            } else if o == t {
                MergeIdentityClass::BothModifiedSame
            } else {
                MergeIdentityClass::BothModifiedUnresolved
            }
        }

        // Theirs deleted (base present, ours present, theirs absent)
        (Some(b), Some(o), None) => {
            if o == b {
                // Ours unchanged, theirs deleted → auto-resolve to "accept deletion"
                MergeIdentityClass::TheirsOnly
            } else {
                MergeIdentityClass::TheirsDeletedOursModified
            }
        }

        // Ours deleted (base present, theirs present, ours absent)
        (Some(b), None, Some(t)) => {
            if t == b {
                // Theirs unchanged, ours deleted → auto-resolve to "accept deletion"
                MergeIdentityClass::OursOnly
            } else {
                MergeIdentityClass::OursDeletedTheirsModified
            }
        }

        // Deleted in both
        (Some(_), None, None) => MergeIdentityClass::BothDeleted,

        // Base absent but identity exists only in one side — treat as new
        // (These are edge cases where the base revision was evicted)
    }
}

/// **Phase A**: Classify all identities across ours/theirs/base branches.
///
/// Scans `ri:` bindings for all three branches, compares body hashes,
/// and produces a classification for each identity.
pub fn phase_a_classify(
    graph: &InMemoryGraph,
    kv: &MemoryKv,
    ours_branch: BranchId,
    theirs_branch: BranchId,
    base_branch: BranchId,
) -> PhaseAResult {
    let base_bindings = scan_branch_bindings(kv, base_branch);
    let ours_bindings = scan_branch_bindings(kv, ours_branch);
    let theirs_bindings = scan_branch_bindings(kv, theirs_branch);

    let mut all_identities = HashSet::new();
    all_identities.extend(base_bindings.keys());
    all_identities.extend(ours_bindings.keys());
    all_identities.extend(theirs_bindings.keys());

    let mut classified = Vec::with_capacity(all_identities.len());

    for &identity_id in &all_identities {
        let base_rev = base_bindings.get(&identity_id).copied();
        let ours_rev = ours_bindings.get(&identity_id).copied();
        let theirs_rev = theirs_bindings.get(&identity_id).copied();

        let class = classify_single_identity(graph, base_rev, ours_rev, theirs_rev);

        let qualified_name = ours_rev
            .or(theirs_rev)
            .or(base_rev)
            .and_then(|r| graph.get_revision(r))
            .map(|r| r.qualified_name.clone())
            .unwrap_or_else(|| hex16(&identity_id.0));

        classified.push(ClassifiedMergeIdentity {
            identity_id,
            class,
            base_revision: base_rev,
            ours_revision: ours_rev,
            theirs_revision: theirs_rev,
            qualified_name,
        });
    }

    detect_renames(graph, &mut classified);

    let report = merge_phase_a_report(
        classified.iter().map(|c| (c.qualified_name.as_str(), c.class)),
    );

    PhaseAResult { report, classified }
}

/// **Phase A** using explicit base bindings (e.g. preflight **`msnap`** = merge-base).
pub fn phase_a_classify_with_base(
    graph: &InMemoryGraph,
    kv: &MemoryKv,
    ours_branch: BranchId,
    theirs_branch: BranchId,
    base_bindings: &HashMap<IdentityId, NodeRevisionId>,
) -> PhaseAResult {
    let ours_bindings = scan_branch_bindings(kv, ours_branch);
    let theirs_bindings = scan_branch_bindings(kv, theirs_branch);

    let mut all_identities = HashSet::new();
    all_identities.extend(base_bindings.keys());
    all_identities.extend(ours_bindings.keys());
    all_identities.extend(theirs_bindings.keys());

    let mut classified = Vec::with_capacity(all_identities.len());

    for &identity_id in &all_identities {
        let base_rev = base_bindings.get(&identity_id).copied();
        let ours_rev = ours_bindings.get(&identity_id).copied();
        let theirs_rev = theirs_bindings.get(&identity_id).copied();

        let class = classify_single_identity(graph, base_rev, ours_rev, theirs_rev);

        let qualified_name = ours_rev
            .or(theirs_rev)
            .or(base_rev)
            .and_then(|r| graph.get_revision(r))
            .map(|r| r.qualified_name.clone())
            .unwrap_or_else(|| hex16(&identity_id.0));

        classified.push(ClassifiedMergeIdentity {
            identity_id,
            class,
            base_revision: base_rev,
            ours_revision: ours_rev,
            theirs_revision: theirs_rev,
            qualified_name,
        });
    }

    detect_renames(graph, &mut classified);

    let report = merge_phase_a_report(
        classified.iter().map(|c| (c.qualified_name.as_str(), c.class)),
    );

    PhaseAResult { report, classified }
}

/// Phase A with msnap merge-base when available; otherwise `fallback_base_branch` bindings.
pub fn phase_a_for_merge(
    graph: &InMemoryGraph,
    kv: &MemoryKv,
    merge_id: MergeId,
    ours_branch: BranchId,
    theirs_branch: BranchId,
    target_branch: BranchId,
    fallback_base_branch: BranchId,
) -> PhaseAResult {
    let base_bindings = crate::merge_control::load_msnap_bindings(kv, merge_id, target_branch);
    if base_bindings.is_empty() {
        phase_a_classify(
            graph,
            kv,
            ours_branch,
            theirs_branch,
            fallback_base_branch,
        )
    } else {
        phase_a_classify_with_base(graph, kv, ours_branch, theirs_branch, &base_bindings)
    }
}

/// Post-classification rename detection pass.
///
/// Scans for identity pairs where one was deleted and another was created
/// with `rename_source_id` pointing to the deleted identity. When found,
/// the new identity is reclassified as `RenamedCandidate` and the old
/// (deleted) identity is reclassified as `BothDeleted` to suppress
/// duplicate promotion.
fn detect_renames(graph: &InMemoryGraph, classified: &mut [ClassifiedMergeIdentity]) {
    let deleted_set: HashSet<IdentityId> = classified
        .iter()
        .filter(|c| {
            matches!(
                c.class,
                MergeIdentityClass::TheirsOnly
                    | MergeIdentityClass::OursOnly
                    | MergeIdentityClass::BothDeleted
            )
        })
        .map(|c| c.identity_id)
        .collect();

    if deleted_set.is_empty() {
        return;
    }

    let mut rename_pairs: Vec<(IdentityId, IdentityId)> = Vec::new();
    for ci in classified.iter() {
        if !matches!(
            ci.class,
            MergeIdentityClass::OursNew
                | MergeIdentityClass::TheirsNew
                | MergeIdentityClass::OursOnly
                | MergeIdentityClass::TheirsOnly
        ) {
            continue;
        }
        let rev_id = ci.ours_revision.or(ci.theirs_revision);
        let rename_src = rev_id
            .and_then(|r| graph.get_revision(r))
            .and_then(|r| r.rename_source_id);
        if let Some(src_identity) = rename_src {
            if deleted_set.contains(&src_identity) {
                rename_pairs.push((src_identity, ci.identity_id));
            }
        }
    }

    for (old_id, new_id) in &rename_pairs {
        for ci in classified.iter_mut() {
            if ci.identity_id == *new_id {
                ci.class = MergeIdentityClass::RenamedCandidate;
            } else if ci.identity_id == *old_id {
                ci.class = MergeIdentityClass::BothDeleted;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase B — Revision promotion
// ---------------------------------------------------------------------------

/// **Phase B**: Promote winning revisions to the target branch.
///
/// For each classified identity, binds the winning revision on the target branch
/// and orphans the losing revision. When `strategy` is `None`, `BothModifiedUnresolved`
/// identities are left unresolved (returned in `unresolved_conflicts`).
pub fn phase_b_promote(
    kv: &MemoryKv,
    graph: &mut InMemoryGraph,
    target_branch: BranchId,
    classified: &[ClassifiedMergeIdentity],
    strategy: Option<MergeStrategy>,
) -> PhaseBResult {
    let mut result = PhaseBResult::default();

    for ci in classified {
        let winner: Option<NodeRevisionId> = match ci.class {
            MergeIdentityClass::Clean => ci.ours_revision.or(ci.base_revision),
            MergeIdentityClass::OursOnly | MergeIdentityClass::OursNew => ci.ours_revision,
            MergeIdentityClass::TheirsOnly | MergeIdentityClass::TheirsNew => ci.theirs_revision,
            MergeIdentityClass::BothModifiedSame => ci.ours_revision,
            MergeIdentityClass::RenamedCandidate => ci.theirs_revision.or(ci.ours_revision),
            MergeIdentityClass::BothDeleted => None,

            MergeIdentityClass::BothModifiedUnresolved
            | MergeIdentityClass::BothNewDivergent => {
                match strategy {
                    Some(MergeStrategy::Ours) => ci.ours_revision,
                    Some(MergeStrategy::Theirs) => ci.theirs_revision,
                    None => {
                        result.unresolved_conflicts.push(ci.clone());
                        continue;
                    }
                }
            }

            MergeIdentityClass::OursDeletedTheirsModified => {
                match strategy {
                    Some(MergeStrategy::Ours) => None,
                    Some(MergeStrategy::Theirs) => ci.theirs_revision,
                    None => {
                        result.unresolved_conflicts.push(ci.clone());
                        continue;
                    }
                }
            }

            MergeIdentityClass::TheirsDeletedOursModified => {
                match strategy {
                    Some(MergeStrategy::Ours) => ci.ours_revision,
                    Some(MergeStrategy::Theirs) => None,
                    None => {
                        result.unresolved_conflicts.push(ci.clone());
                        continue;
                    }
                }
            }

            MergeIdentityClass::BaseDeletedFeatureNew => {
                match strategy {
                    Some(MergeStrategy::Ours) => ci.ours_revision,
                    Some(MergeStrategy::Theirs) => ci.theirs_revision,
                    None => {
                        result.unresolved_conflicts.push(ci.clone());
                        continue;
                    }
                }
            }
        };

        if let Some(win_rid) = winner {
            let key = revision_binding_kv_key(target_branch, ci.identity_id);
            kv.set(&key, win_rid.0.to_vec());
            result.promoted.push((ci.identity_id, win_rid));

            let is_conflict_class = matches!(
                ci.class,
                MergeIdentityClass::BothModifiedUnresolved
                    | MergeIdentityClass::BothNewDivergent
                    | MergeIdentityClass::OursDeletedTheirsModified
                    | MergeIdentityClass::TheirsDeletedOursModified
                    | MergeIdentityClass::BaseDeletedFeatureNew
            );
            if is_conflict_class {
                let loser = match () {
                    _ if ci.ours_revision == Some(win_rid) => ci.theirs_revision,
                    _ if ci.theirs_revision == Some(win_rid) => ci.ours_revision,
                    _ => None,
                };
                if let Some(lose_rid) = loser {
                    if lose_rid != win_rid {
                        if let Some(rev) = graph.get_revision(lose_rid) {
                            if rev.status == RevisionStatus::Active {
                                graph.set_revision_status(lose_rid, RevisionStatus::Orphaned);
                                result.orphaned_revisions.push(lose_rid);
                            }
                        }
                    }
                }
            }
        } else {
            // Winner is None → identity was deleted; remove RI binding on target
            let key = revision_binding_kv_key(target_branch, ci.identity_id);
            kv.delete(&key);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Phase C — Edge reconciliation
// ---------------------------------------------------------------------------

fn record_edge_batch(
    graph: &mut InMemoryGraph,
    result: &mut PhaseCResult,
    revision_id: NodeRevisionId,
    new_edges: Vec<crate::graph::GraphEdge>,
) -> Result<(), &'static str> {
    let prior = graph.outbound_edges(revision_id).to_vec();
    if prior == new_edges {
        return Ok(());
    }
    let seq = result.saga_batches.len() as u32 + 1;
    graph.replace_edges_for_revision(revision_id, new_edges.clone())?;
    result.saga_batches.push(SagaEdgeBatch {
        seq,
        target_revision_id: revision_id,
        prior_edges: prior,
        edges: new_edges,
    });
    Ok(())
}

/// **Phase C**: Reconcile edges for promoted revisions (dangling cleanup, drift, cardinality).
pub fn phase_c_reconcile_edges(
    graph: &mut InMemoryGraph,
    kv: &MemoryKv,
    target_branch: BranchId,
    promoted: &[(IdentityId, NodeRevisionId)],
) -> PhaseCResult {
    phase_c_reconcile_edges_full(graph, kv, None, target_branch, promoted, None)
}

/// Full Phase C with optional AST edge regeneration from `BodyStore`.
pub fn phase_c_reconcile_edges_full(
    graph: &mut InMemoryGraph,
    kv: &MemoryKv,
    body_store: Option<&BodyStore>,
    target_branch: BranchId,
    promoted: &[(IdentityId, NodeRevisionId)],
    classified: Option<&[ClassifiedMergeIdentity]>,
) -> PhaseCResult {
    let mut result = PhaseCResult::default();

    if let Some(bs) = body_store {
        let indexers = crate::language_indexer::default_indexers();
        let files = files_for_edge_regen(graph, classified, target_branch, promoted);
        let mod_map = crate::call_resolve::module_map_for_paths(
            crate::call_resolve::paths_on_branch(graph, target_branch),
            &indexers,
        );
        for path in files {
            match crate::ingest::load_file_body(bs, &path) {
                Some(content) => {
                    match crate::call_resolve::regen_edges_for_file_with_graph(
                        &path,
                        &content,
                        target_branch,
                        &mod_map,
                        Some(graph),
                        &indexers,
                    ) {
                        Ok(edge_map) => {
                            for (rid, mut edges) in edge_map {
                                for e in graph.outbound_edges(rid) {
                                    if e.ty == EdgeType::RenamedFrom {
                                        edges.push(e.clone());
                                    }
                                }
                                if record_edge_batch(graph, &mut result, rid, edges).is_ok() {
                                    result.edges_regenerated += 1;
                                }
                            }
                        }
                        Err(_) => {
                            if let Some(rid) = graph
                                .revision_ids_for_file(target_branch, &path)
                                .first()
                                .copied()
                            {
                                result.needs_edge_regen.push(rid);
                            }
                        }
                    }
                }
                None => {
                    for &(iid, rid) in promoted {
                        if graph
                            .get_revision(rid)
                            .map(|r| r.file_path == path)
                            .unwrap_or(false)
                        {
                            result.needs_edge_regen.push(rid);
                            let _ = iid;
                        }
                    }
                }
            }
        }
    } else {
        for &(_identity_id, revision_id) in promoted {
            if let Some(rev) = graph.get_revision(revision_id) {
                if rev.branch_id != target_branch {
                    result.needs_edge_regen.push(revision_id);
                }
            }
        }
    }

    let target_bindings = scan_branch_bindings(kv, target_branch);
    let active_identities: HashSet<IdentityId> = target_bindings.keys().copied().collect();

    for &(_identity_id, revision_id) in promoted {
        let edges = graph.outbound_edges(revision_id).to_vec();
        let mut kept_edges = Vec::new();
        let mut changed = false;

        for edge in &edges {
            result.edges_checked += 1;

            if !active_identities.contains(&edge.target_identity_id) {
                result.dangling_edges_removed += 1;
                changed = true;
                continue;
            }

            if let Some(&target_rev_id) = target_bindings.get(&edge.target_identity_id) {
                if let Some(target_rev) = graph.get_revision(target_rev_id) {
                    if edge.resolution.target_signature_hash != [0u8; 32]
                        && edge.resolution.target_signature_hash != target_rev.signature_hash
                    {
                        let source_qn = graph
                            .get_revision(revision_id)
                            .map(|r| r.qualified_name.clone())
                            .unwrap_or_default();
                        let target_qn = target_rev.qualified_name.clone();
                        result.signature_drifts.push(format!(
                            "{source_qn} -> {target_qn}: signature changed"
                        ));
                        let mut fixed = edge.clone();
                        fixed.resolution.target_signature_hash = target_rev.signature_hash;
                        kept_edges.push(fixed);
                        result.signature_reresolved += 1;
                        changed = true;
                        continue;
                    }
                }
            }

            kept_edges.push(edge.clone());
        }

        if changed {
            let _ = record_edge_batch(graph, &mut result, revision_id, kept_edges);
        }

        let final_edges = if changed {
            graph.outbound_edges(revision_id).to_vec()
        } else {
            edges
        };
        if let Err(v) = validate_edge_cardinality(&final_edges) {
            let qn = graph
                .get_revision(revision_id)
                .map(|r| r.qualified_name.as_str())
                .unwrap_or("?");
            result.cardinality_violations.push(format!(
                "{qn}: {:?} count={} (min={}, max={})",
                v.ty, v.count, v.min, v.max
            ));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cis_wal::{MutationIndex, MutationLog, MutationKind};

    use crate::graph::{
        EdgeResolution, EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision,
        SourceSpan, SourceType,
    };
    use crate::kv::MemoryKv;
    use crate::merge_control::MergeControl;
    use crate::merge_gate::MergeRecoveryGate;
    use crate::merge_lock::acquire_merge_lock;
    use crate::saga::{MergeSagaOrchestrator, SagaPhase};
    use crate::vector_store::InMemoryVectorStore;
    use cis_wal::MergeId;

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
    ) -> NodeRevision {
        NodeRevision {
            revision_id,
            identity_id,
            branch_id,
            status: RevisionStatus::Active,
            qualified_name: qn.into(),
            file_path: "test.py".into(),
            body_hash,
            signature_hash: [0u8; 32],
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

    // -----------------------------------------------------------------------
    // Existing tests (preserved)
    // -----------------------------------------------------------------------

    #[test]
    fn merge_cancelled_wal_appends() {
        let wal = MutationLog::new();
        let mid = MergeId([3u8; 16]);
        let id = append_merge_cancelled_marker(&wal, mid).unwrap();
        let rec = wal.get(id).unwrap();
        assert!(matches!(
            rec.kind,
            MutationKind::MergeCancelled { merge_id: m } if m == mid
        ));
        let mut idx = MutationIndex::new();
        idx.register_new_record(&rec);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn phase_a_marks_conflicts_and_renames() {
        let items = vec![
            ("a".into(), MergeIdentityClass::Clean),
            (
                "b".into(),
                classify_identity_stub(true, true, false),
            ),
            ("c".into(), classify_identity_stub(false, false, true)),
        ];
        let rep = run_phase_a_classify(&items);
        assert_eq!(rep.resolved_count, 2);
        assert_eq!(rep.conflicts, vec!["b"]);
        assert_eq!(rep.rename_detections, vec!["c"]);
        assert_eq!(rep.status, MergeWorkflowStatus::RequiresResolution);
    }

    #[test]
    fn cancel_merge_recovery_appends_merge_cancelled_after_gate_and_saga_purge() {
        let wal = MutationLog::new();
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([1u8; 16]);
        let merge_id = MergeId([2u8; 16]);
        acquire_merge_lock(&kv, branch, merge_id).unwrap();
        let mc = MergeControl::new(Arc::clone(&kv));
        mc.record_premerge_bindings(merge_id, branch, &[]);

        let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
        saga.persist(merge_id, SagaPhase::EdgeBatch { seq: 1 });
        saga.persist_batch_marker(merge_id, 1);

        let gate = MergeRecoveryGate::new(Arc::clone(&kv));
        gate.begin_rollback(branch);
        assert!(gate.is_query_blocked(branch));

        let mut g = InMemoryGraph::default();
        mc.cancel_merge(
            merge_id,
            branch,
            &mut g,
            &InMemoryVectorStore::new(),
            None,
            &[],
        )
        .unwrap();

        assert_eq!(saga.compensate_orphans(), 1);
        assert!(saga.load(merge_id).is_none());
        assert!(kv
            .get(&MergeSagaOrchestrator::batch_key(merge_id, 1))
            .is_none());

        append_merge_cancelled_after_control(&wal, merge_id, true).unwrap();

        gate.end_rollback(branch);
        assert!(!gate.is_query_blocked(branch));

        assert!(wal.iter_all().iter().any(|r| {
            matches!(
                r.kind,
                MutationKind::MergeCancelled { merge_id: m } if m == merge_id
            )
        }));
    }

    // -----------------------------------------------------------------------
    // Phase A — real classifier tests
    // -----------------------------------------------------------------------

    #[test]
    fn phase_a_clean_when_unchanged() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let r1 = rid(1);
        let body = [10u8; 32];
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r1, i1, base, "f", body));

        bind(&kv, base, i1, r1);
        bind(&kv, ours, i1, r1);
        bind(&kv, theirs, i1, r1);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified.len(), 1);
        assert_eq!(result.classified[0].class, MergeIdentityClass::Clean);
        assert_eq!(result.report.resolved_count, 1);
        assert_eq!(result.report.status, MergeWorkflowStatus::Ready);
    }

    #[test]
    fn phase_a_ours_only() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let r_base = rid(1);
        let r_ours = rid(2);
        let base_body = [10u8; 32];
        let ours_body = [20u8; 32];

        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_base, i1, base, "f", base_body));
        g.put_revision(make_rev(r_ours, i1, ours, "f", ours_body));

        bind(&kv, base, i1, r_base);
        bind(&kv, ours, i1, r_ours);
        bind(&kv, theirs, i1, r_base);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified[0].class, MergeIdentityClass::OursOnly);
        assert_eq!(result.report.resolved_count, 1);
    }

    #[test]
    fn phase_a_theirs_only() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let r_base = rid(1);
        let r_theirs = rid(3);
        let base_body = [10u8; 32];
        let theirs_body = [30u8; 32];

        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_base, i1, base, "f", base_body));
        g.put_revision(make_rev(r_theirs, i1, theirs, "f", theirs_body));

        bind(&kv, base, i1, r_base);
        bind(&kv, ours, i1, r_base);
        bind(&kv, theirs, i1, r_theirs);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified[0].class, MergeIdentityClass::TheirsOnly);
    }

    #[test]
    fn affected_paths_for_classified_collects_revision_paths() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let i2 = iid(2);
        let r1_base = rid(1);
        let r1_ours_id = rid(2);
        let mut r1_ours = make_rev(r1_ours_id, i1, ours, "f1", [20u8; 32]);
        r1_ours.file_path = "a.py".into();
        let r2_base = rid(3);
        let r2_theirs_id = rid(4);
        let mut r2_theirs = make_rev(r2_theirs_id, i2, theirs, "f2", [30u8; 32]);
        r2_theirs.file_path = "b.py".into();

        g.put_identity(NodeIdentity {
            identity_id: i1,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: i2,
            kind: NodeKind::Function,
        });
        g.put_revision(make_rev(r1_base, i1, base, "f1", [10u8; 32]));
        g.put_revision(r1_ours);
        g.put_revision(make_rev(r2_base, i2, base, "f2", [10u8; 32]));
        g.put_revision(r2_theirs);

        bind(&kv, base, i1, r1_base);
        bind(&kv, ours, i1, r1_ours_id);
        bind(&kv, base, i2, r2_base);
        bind(&kv, ours, i2, r2_base);
        bind(&kv, theirs, i2, r2_theirs_id);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        let paths = affected_paths_for_classified(&g, &result.classified);
        assert!(paths.contains(&"a.py".to_string()));
        assert!(paths.contains(&"b.py".to_string()));
    }

    #[test]
    fn phase_a_both_modified_unresolved() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let r_base = rid(1);
        let r_ours = rid(2);
        let r_theirs = rid(3);

        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_base, i1, base, "f", [10u8; 32]));
        g.put_revision(make_rev(r_ours, i1, ours, "f", [20u8; 32]));
        g.put_revision(make_rev(r_theirs, i1, theirs, "f", [30u8; 32]));

        bind(&kv, base, i1, r_base);
        bind(&kv, ours, i1, r_ours);
        bind(&kv, theirs, i1, r_theirs);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified[0].class, MergeIdentityClass::BothModifiedUnresolved);
        assert_eq!(result.report.status, MergeWorkflowStatus::RequiresResolution);
        assert_eq!(result.report.conflicts.len(), 1);
    }

    #[test]
    fn phase_a_both_modified_same_content() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let r_base = rid(1);
        let r_ours = rid(2);
        let r_theirs = rid(3);
        let same_body = [20u8; 32];

        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_base, i1, base, "f", [10u8; 32]));
        g.put_revision(make_rev(r_ours, i1, ours, "f", same_body));
        g.put_revision(make_rev(r_theirs, i1, theirs, "f", same_body));

        bind(&kv, base, i1, r_base);
        bind(&kv, ours, i1, r_ours);
        bind(&kv, theirs, i1, r_theirs);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified[0].class, MergeIdentityClass::BothModifiedSame);
        assert_eq!(result.report.resolved_count, 1);
    }

    #[test]
    fn phase_a_new_in_theirs_only() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        let i1 = iid(1);
        let r_theirs = rid(3);

        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_theirs, i1, theirs, "new_fn", [30u8; 32]));

        bind(&kv, theirs, i1, r_theirs);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified[0].class, MergeIdentityClass::TheirsNew);
        assert_eq!(result.report.resolved_count, 1);
    }

    #[test]
    fn phase_a_multi_identity_mixed() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);

        // i1: clean (unchanged)
        let i1 = iid(1);
        let r1 = rid(1);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r1, i1, base, "clean_fn", [10u8; 32]));
        bind(&kv, base, i1, r1);
        bind(&kv, ours, i1, r1);
        bind(&kv, theirs, i1, r1);

        // i2: ours only modified
        let i2 = iid(2);
        let r2b = rid(2);
        let r2o = rid(12);
        g.put_identity(NodeIdentity { identity_id: i2, kind: NodeKind::Function });
        g.put_revision(make_rev(r2b, i2, base, "ours_mod", [20u8; 32]));
        g.put_revision(make_rev(r2o, i2, ours, "ours_mod", [21u8; 32]));
        bind(&kv, base, i2, r2b);
        bind(&kv, ours, i2, r2o);
        bind(&kv, theirs, i2, r2b);

        // i3: conflict
        let i3 = iid(3);
        let r3b = rid(3);
        let r3o = rid(13);
        let r3t = rid(23);
        g.put_identity(NodeIdentity { identity_id: i3, kind: NodeKind::Function });
        g.put_revision(make_rev(r3b, i3, base, "conflict_fn", [30u8; 32]));
        g.put_revision(make_rev(r3o, i3, ours, "conflict_fn", [31u8; 32]));
        g.put_revision(make_rev(r3t, i3, theirs, "conflict_fn", [32u8; 32]));
        bind(&kv, base, i3, r3b);
        bind(&kv, ours, i3, r3o);
        bind(&kv, theirs, i3, r3t);

        // i4: new in theirs
        let i4 = iid(4);
        let r4t = rid(24);
        g.put_identity(NodeIdentity { identity_id: i4, kind: NodeKind::Function });
        g.put_revision(make_rev(r4t, i4, theirs, "new_theirs", [40u8; 32]));
        bind(&kv, theirs, i4, r4t);

        let result = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(result.classified.len(), 4);
        assert_eq!(result.report.resolved_count, 3);
        assert_eq!(result.report.conflicts.len(), 1);
        assert_eq!(result.report.status, MergeWorkflowStatus::RequiresResolution);
    }

    // -----------------------------------------------------------------------
    // Phase B — promotion tests
    // -----------------------------------------------------------------------

    #[test]
    fn phase_b_promotes_ours_only() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i1 = iid(1);
        let r_ours = rid(2);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_ours, i1, target, "f", [20u8; 32]));

        let classified = vec![ClassifiedMergeIdentity {
            identity_id: i1,
            class: MergeIdentityClass::OursOnly,
            base_revision: Some(rid(1)),
            ours_revision: Some(r_ours),
            theirs_revision: Some(rid(1)),
            qualified_name: "f".into(),
        }];

        let result = phase_b_promote(&kv, &mut g, target, &classified, None);
        assert_eq!(result.promoted.len(), 1);
        assert_eq!(result.promoted[0], (i1, r_ours));
        assert!(result.unresolved_conflicts.is_empty());

        let key = revision_binding_kv_key(target, i1);
        assert_eq!(kv.get(&key), Some(r_ours.0.to_vec()));
    }

    #[test]
    fn phase_b_conflict_without_strategy_is_unresolved() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i1 = iid(1);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(rid(2), i1, target, "f", [20u8; 32]));
        g.put_revision(make_rev(rid(3), i1, target, "f", [30u8; 32]));

        let classified = vec![ClassifiedMergeIdentity {
            identity_id: i1,
            class: MergeIdentityClass::BothModifiedUnresolved,
            base_revision: Some(rid(1)),
            ours_revision: Some(rid(2)),
            theirs_revision: Some(rid(3)),
            qualified_name: "f".into(),
        }];

        let result = phase_b_promote(&kv, &mut g, target, &classified, None);
        assert!(result.promoted.is_empty());
        assert_eq!(result.unresolved_conflicts.len(), 1);
    }

    #[test]
    fn phase_b_conflict_with_ours_strategy() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i1 = iid(1);
        let r_ours = rid(2);
        let r_theirs = rid(3);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_ours, i1, target, "f", [20u8; 32]));
        g.put_revision(make_rev(r_theirs, i1, target, "f", [30u8; 32]));

        let classified = vec![ClassifiedMergeIdentity {
            identity_id: i1,
            class: MergeIdentityClass::BothModifiedUnresolved,
            base_revision: Some(rid(1)),
            ours_revision: Some(r_ours),
            theirs_revision: Some(r_theirs),
            qualified_name: "f".into(),
        }];

        let result = phase_b_promote(&kv, &mut g, target, &classified, Some(MergeStrategy::Ours));
        assert_eq!(result.promoted.len(), 1);
        assert_eq!(result.promoted[0], (i1, r_ours));
        assert!(result.unresolved_conflicts.is_empty());

        // Theirs revision should be orphaned
        assert_eq!(result.orphaned_revisions.len(), 1);
        assert_eq!(result.orphaned_revisions[0], r_theirs);
        assert_eq!(g.get_revision(r_theirs).unwrap().status, RevisionStatus::Orphaned);
    }

    #[test]
    fn phase_b_conflict_with_theirs_strategy() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i1 = iid(1);
        let r_ours = rid(2);
        let r_theirs = rid(3);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_ours, i1, target, "f", [20u8; 32]));
        g.put_revision(make_rev(r_theirs, i1, target, "f", [30u8; 32]));

        let classified = vec![ClassifiedMergeIdentity {
            identity_id: i1,
            class: MergeIdentityClass::BothModifiedUnresolved,
            base_revision: Some(rid(1)),
            ours_revision: Some(r_ours),
            theirs_revision: Some(r_theirs),
            qualified_name: "f".into(),
        }];

        let result = phase_b_promote(
            &kv, &mut g, target, &classified, Some(MergeStrategy::Theirs),
        );
        assert_eq!(result.promoted[0], (i1, r_theirs));
        assert_eq!(result.orphaned_revisions, vec![r_ours]);
        assert_eq!(g.get_revision(r_ours).unwrap().status, RevisionStatus::Orphaned);
    }

    // -----------------------------------------------------------------------
    // Phase C — edge reconciliation tests
    // -----------------------------------------------------------------------

    #[test]
    fn phase_c_removes_dangling_edges() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i_src = iid(1);
        let i_tgt = iid(2);
        let r_src = rid(1);
        g.put_identity(NodeIdentity { identity_id: i_src, kind: NodeKind::Function });
        g.put_identity(NodeIdentity { identity_id: i_tgt, kind: NodeKind::Function });
        g.put_revision(make_rev(r_src, i_src, target, "caller", [10u8; 32]));

        let edge = GraphEdge {
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
        };
        g.replace_edges_for_revision(r_src, vec![edge]).unwrap();

        // Only bind source on target branch, NOT the target identity → edge is dangling
        bind(&kv, target, i_src, r_src);

        let promoted = vec![(i_src, r_src)];
        let result = phase_c_reconcile_edges(&mut g, &kv, target, &promoted);
        assert_eq!(result.dangling_edges_removed, 1);
        assert!(g.outbound_edges(r_src).is_empty());
    }

    #[test]
    fn phase_c_detects_signature_drift() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i_src = iid(1);
        let i_tgt = iid(2);
        let r_src = rid(1);
        let r_tgt = rid(2);
        g.put_identity(NodeIdentity { identity_id: i_src, kind: NodeKind::Function });
        g.put_identity(NodeIdentity { identity_id: i_tgt, kind: NodeKind::Function });
        g.put_revision(make_rev(r_src, i_src, target, "caller", [10u8; 32]));
        let mut tgt_rev = make_rev(r_tgt, i_tgt, target, "callee", [20u8; 32]);
        tgt_rev.signature_hash = [99u8; 32];
        g.put_revision(tgt_rev);

        let edge = GraphEdge {
            edge_id: [9u8; 16],
            ty: EdgeType::Calls,
            source_revision_id: r_src,
            target_identity_id: i_tgt,
            resolution: EdgeResolution {
                target_signature_hash: [55u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(r_src, vec![edge]).unwrap();

        bind(&kv, target, i_src, r_src);
        bind(&kv, target, i_tgt, r_tgt);

        let promoted = vec![(i_src, r_src)];
        let result = phase_c_reconcile_edges(&mut g, &kv, target, &promoted);
        assert_eq!(result.signature_drifts.len(), 1);
        assert!(result.signature_drifts[0].contains("signature changed"));
        assert_eq!(result.signature_reresolved, 1);
        let kept = g.outbound_edges(r_src);
        assert_eq!(kept[0].resolution.target_signature_hash, [99u8; 32]);
    }

    #[test]
    fn append_merge_committed_writes_wal_marker() {
        let wal = MutationLog::new();
        let mid = MergeId([5u8; 16]);
        let rid = NodeRevisionId([6u8; 16]);
        append_merge_committed(&wal, mid, &[(IdentityId([1u8; 16]), rid)]).unwrap();
        assert!(wal.iter_all().iter().any(|r| {
            matches!(r.kind, MutationKind::Merge { merge_id: m } if m == mid)
        }));
    }

    #[test]
    fn phase_a_msnap_base_detects_theirs_only_change() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([1u8; 16]);
        let source = BranchId([2u8; 16]);
        let merge_id = MergeId([3u8; 16]);

        let i1 = iid(1);
        let r_base = rid(1);
        let r_ours = rid(2);
        let r_theirs = rid(3);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_base, i1, target, "f", [10u8; 32]));
        g.put_revision(make_rev(r_ours, i1, target, "f", [10u8; 32]));
        g.put_revision(make_rev(r_theirs, i1, source, "f", [30u8; 32]));

        bind(&kv, target, i1, r_ours);
        bind(&kv, source, i1, r_theirs);
        MergeControl::new(Arc::clone(&kv)).record_premerge_bindings(merge_id, target, &[(i1, r_base)]);

        let result =
            phase_a_for_merge(&g, &kv, merge_id, target, source, target, target);
        assert_eq!(result.classified[0].class, MergeIdentityClass::TheirsOnly);
    }

    #[test]
    fn phase_c_keeps_valid_edges() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let target = BranchId([9u8; 16]);

        let i_src = iid(1);
        let i_tgt = iid(2);
        let r_src = rid(1);
        let r_tgt = rid(2);
        g.put_identity(NodeIdentity { identity_id: i_src, kind: NodeKind::Function });
        g.put_identity(NodeIdentity { identity_id: i_tgt, kind: NodeKind::Function });
        g.put_revision(make_rev(r_src, i_src, target, "caller", [10u8; 32]));
        g.put_revision(make_rev(r_tgt, i_tgt, target, "callee", [20u8; 32]));

        let edge = GraphEdge {
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
        };
        g.replace_edges_for_revision(r_src, vec![edge]).unwrap();

        bind(&kv, target, i_src, r_src);
        bind(&kv, target, i_tgt, r_tgt);

        let promoted = vec![(i_src, r_src)];
        let result = phase_c_reconcile_edges(&mut g, &kv, target, &promoted);
        assert_eq!(result.dangling_edges_removed, 0);
        assert_eq!(result.edges_checked, 1);
        assert_eq!(g.outbound_edges(r_src).len(), 1);
    }

    // -----------------------------------------------------------------------
    // End-to-end: Phase A → B → C
    // -----------------------------------------------------------------------

    #[test]
    fn full_merge_non_overlapping_changes() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);
        let target = ours;

        // i1: modified only in ours
        let i1 = iid(1);
        let r1_base = rid(1);
        let r1_ours = rid(11);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r1_base, i1, base, "fn_a", [10u8; 32]));
        g.put_revision(make_rev(r1_ours, i1, ours, "fn_a", [11u8; 32]));
        bind(&kv, base, i1, r1_base);
        bind(&kv, ours, i1, r1_ours);
        bind(&kv, theirs, i1, r1_base);

        // i2: modified only in theirs
        let i2 = iid(2);
        let r2_base = rid(2);
        let r2_theirs = rid(22);
        g.put_identity(NodeIdentity { identity_id: i2, kind: NodeKind::Function });
        g.put_revision(make_rev(r2_base, i2, base, "fn_b", [20u8; 32]));
        g.put_revision(make_rev(r2_theirs, i2, theirs, "fn_b", [21u8; 32]));
        bind(&kv, base, i2, r2_base);
        bind(&kv, ours, i2, r2_base);
        bind(&kv, theirs, i2, r2_theirs);

        // i3: unchanged
        let i3 = iid(3);
        let r3 = rid(3);
        g.put_identity(NodeIdentity { identity_id: i3, kind: NodeKind::Function });
        g.put_revision(make_rev(r3, i3, base, "fn_c", [30u8; 32]));
        bind(&kv, base, i3, r3);
        bind(&kv, ours, i3, r3);
        bind(&kv, theirs, i3, r3);

        // Phase A
        let phase_a = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(phase_a.report.status, MergeWorkflowStatus::Ready);
        assert_eq!(phase_a.report.resolved_count, 3);
        assert!(phase_a.report.conflicts.is_empty());

        // Phase B
        let phase_b = phase_b_promote(&kv, &mut g, target, &phase_a.classified, None);
        assert_eq!(phase_b.promoted.len(), 3);
        assert!(phase_b.unresolved_conflicts.is_empty());

        // Verify target branch bindings
        let target_bindings = scan_branch_bindings(&kv, target);
        assert_eq!(target_bindings.get(&i1), Some(&r1_ours));
        assert_eq!(target_bindings.get(&i2), Some(&r2_theirs));
        assert_eq!(target_bindings.get(&i3), Some(&r3));
    }

    #[test]
    fn full_merge_conflict_resolved_with_strategy() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);
        let target = ours;

        let i1 = iid(1);
        let r_base = rid(1);
        let r_ours = rid(2);
        let r_theirs = rid(3);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r_base, i1, base, "f", [10u8; 32]));
        g.put_revision(make_rev(r_ours, i1, ours, "f", [20u8; 32]));
        g.put_revision(make_rev(r_theirs, i1, theirs, "f", [30u8; 32]));
        bind(&kv, base, i1, r_base);
        bind(&kv, ours, i1, r_ours);
        bind(&kv, theirs, i1, r_theirs);

        // Phase A — should detect conflict
        let phase_a = phase_a_classify(&g, &kv, ours, theirs, base);
        assert_eq!(phase_a.report.status, MergeWorkflowStatus::RequiresResolution);

        // Phase B with Theirs strategy
        let phase_b = phase_b_promote(
            &kv, &mut g, target, &phase_a.classified, Some(MergeStrategy::Theirs),
        );
        assert_eq!(phase_b.promoted.len(), 1);
        assert_eq!(phase_b.promoted[0], (i1, r_theirs));
        assert_eq!(phase_b.orphaned_revisions, vec![r_ours]);

        let target_bindings = scan_branch_bindings(&kv, target);
        assert_eq!(target_bindings.get(&i1), Some(&r_theirs));
    }

    #[test]
    fn full_merge_with_edge_reconciliation() {
        let kv = Arc::new(MemoryKv::new());
        let mut g = InMemoryGraph::default();
        let base = BranchId([1u8; 16]);
        let ours = BranchId([2u8; 16]);
        let theirs = BranchId([3u8; 16]);
        let target = ours;

        // i1: caller (modified in theirs)
        let i1 = iid(1);
        let r1_base = rid(1);
        let r1_theirs = rid(11);
        g.put_identity(NodeIdentity { identity_id: i1, kind: NodeKind::Function });
        g.put_revision(make_rev(r1_base, i1, base, "caller", [10u8; 32]));
        g.put_revision(make_rev(r1_theirs, i1, theirs, "caller", [11u8; 32]));
        bind(&kv, base, i1, r1_base);
        bind(&kv, ours, i1, r1_base);
        bind(&kv, theirs, i1, r1_theirs);

        // i2: callee (deleted in theirs — will make edge dangling)
        let i2 = iid(2);
        let r2_base = rid(2);
        g.put_identity(NodeIdentity { identity_id: i2, kind: NodeKind::Function });
        g.put_revision(make_rev(r2_base, i2, base, "callee", [20u8; 32]));
        bind(&kv, base, i2, r2_base);
        bind(&kv, ours, i2, r2_base);
        // Not bound in theirs → deleted

        // Edge: caller → callee
        let edge = GraphEdge {
            edge_id: [9u8; 16],
            ty: EdgeType::Calls,
            source_revision_id: r1_theirs,
            target_identity_id: i2,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(r1_theirs, vec![edge]).unwrap();

        // Phase A
        let phase_a = phase_a_classify(&g, &kv, ours, theirs, base);

        // Phase B
        let phase_b = phase_b_promote(&kv, &mut g, target, &phase_a.classified, None);
        assert!(!phase_b.promoted.is_empty());

        // Phase C — should detect dangling edge to deleted callee
        let result = phase_c_reconcile_edges(&mut g, &kv, target, &phase_b.promoted);
        assert_eq!(result.dangling_edges_removed, 1);
    }
}
