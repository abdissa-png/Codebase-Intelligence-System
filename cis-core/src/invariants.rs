//! Invariant checks for hardening tests (**Phase 4.3**).

use std::collections::{HashMap, HashSet};

use cis_wal::{BranchId, MergeId, NodeRevisionId};

use crate::graph::{InMemoryGraph, RevisionStatus};
use crate::graph_consistency::{check_consistency, ConsistencyReport};
use crate::merge_saga_batch::load_saga_edge_batches;
use crate::optimistic_patcher::OptimisticPatcher;
use crate::path_lease::{PathLeaseManager, SessionId, SpeculativePathTracker};
use crate::shared_graph::SharedInMemoryGraph;
use crate::{BodyStore, MemoryKv, WriteCoordinator};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InvariantReport {
    pub consistency: ConsistencyReport,
    pub write_path: Vec<WritePathViolation>,
    pub merge: Vec<MergeViolation>,
}

impl InvariantReport {
    pub fn is_clean(&self) -> bool {
        self.consistency.is_clean() && self.write_path.is_empty() && self.merge.is_empty()
    }

    pub fn summary(&self) -> String {
        if self.is_clean() {
            return "invariants: clean".into();
        }
        format!(
            "{}; write_path={}; merge={}",
            self.consistency.summary(),
            self.write_path.len(),
            self.merge.len(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WritePathViolation {
    LeaseWithoutPatch { path: String, session: u64 },
    PatchWithoutLease { patch_id: u64, path: String },
    SpecTrackerMismatch {
        path: String,
        tracker_count: u32,
        patch_refs: u32,
    },
    SpeculativeWithoutPatch { revision_id: NodeRevisionId },
    OrphanLease { path: String, session: u64 },
}

/// Selects which mid-race states are legal during stress quiescence checks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InvariantCheckMode {
    #[default]
    Strict,
    /// Merge preflight may hold `merge_lock:` before saga/msnap is persisted.
    MergePreflightWindow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeViolation {
    SagaPendingWithMissingRevision {
        merge_id: MergeId,
        revision_id: NodeRevisionId,
    },
    MergeLockWithoutSaga { branch: BranchId },
    SagaWithoutMergeLock { merge_id: MergeId, branch: BranchId },
    /// Merge lock held while the write coordinator is not ready (failed WAL replay).
    CoordinatorNotReadyWhileMergeLocked { branch: BranchId },
}

/// Context for running all invariant checks.
pub struct InvariantContext<'a> {
    pub graph: &'a SharedInMemoryGraph,
    pub kv: &'a MemoryKv,
    pub body_store: &'a BodyStore,
    pub branches: &'a [BranchId],
    pub leases: &'a PathLeaseManager,
    pub patcher: &'a OptimisticPatcher,
    pub spec_tracker: &'a SpeculativePathTracker,
    pub coordinator: Option<&'a WriteCoordinator>,
}

pub fn check_write_path_invariants(
    graph: &SharedInMemoryGraph,
    leases: &PathLeaseManager,
    patcher: &OptimisticPatcher,
    spec_tracker: &SpeculativePathTracker,
) -> Vec<WritePathViolation> {
    let mut violations = Vec::new();
    let active_leases = leases.active_paths();
    let patch_paths = patcher.all_open_paths();
    let patch_count_by_path = patcher.path_refcounts();

    let leased_paths: HashSet<String> = active_leases.iter().map(|(p, _)| p.clone()).collect();
    let patch_path_set: HashSet<String> = patch_paths.iter().cloned().collect();

    for (path, session) in &active_leases {
        if !patch_path_set.contains(path) {
            violations.push(WritePathViolation::LeaseWithoutPatch {
                path: path.clone(),
                session: session.0,
            });
        }
    }

    for (patch_id, path) in patcher.open_patch_paths() {
        if !leased_paths.contains(&path) {
            violations.push(WritePathViolation::PatchWithoutLease { patch_id, path });
        }
    }

    for (path, tracker_count) in spec_tracker.path_counts() {
        let patch_refs = patch_count_by_path.get(&path).copied().unwrap_or(0);
        if tracker_count != patch_refs {
            violations.push(WritePathViolation::SpecTrackerMismatch {
                path,
                tracker_count,
                patch_refs,
            });
        }
    }

    let g = graph.read();
    for rev in g.revisions() {
        if !matches!(rev.status, RevisionStatus::Speculative) {
            continue;
        }
        let has_patch = patch_path_set.contains(&rev.file_path);
        if !has_patch {
            violations.push(WritePathViolation::SpeculativeWithoutPatch {
                revision_id: rev.revision_id,
            });
        }
    }

    // Note: OrphanLease was previously emitted for the same condition as LeaseWithoutPatch;
    // keep a single signal to avoid duplicate invariant noise.
    let _ = active_leases;

    violations
}

pub fn check_merge_invariants_with_mode(
    graph: &SharedInMemoryGraph,
    kv: &MemoryKv,
    coordinator: Option<&WriteCoordinator>,
    mode: InvariantCheckMode,
) -> Vec<MergeViolation> {
    let violations = check_merge_invariants_inner(graph, kv, coordinator);
    if mode == InvariantCheckMode::Strict {
        return violations;
    }
    violations
        .into_iter()
        .filter(|v| !matches!(v, MergeViolation::MergeLockWithoutSaga { .. }))
        .collect()
}

pub fn check_merge_invariants(
    graph: &SharedInMemoryGraph,
    kv: &MemoryKv,
    coordinator: Option<&WriteCoordinator>,
) -> Vec<MergeViolation> {
    check_merge_invariants_with_mode(graph, kv, coordinator, InvariantCheckMode::Strict)
}

fn check_merge_invariants_inner(
    graph: &SharedInMemoryGraph,
    kv: &MemoryKv,
    coordinator: Option<&WriteCoordinator>,
) -> Vec<MergeViolation> {
    let mut violations = Vec::new();
    let g = graph.read();

    let mut lock_branch_by_merge: HashMap<MergeId, BranchId> = HashMap::new();
    for (key, val) in kv.scan_prefix("merge_lock:") {
        if val.len() != 16 {
            continue;
        }
        let branch_hex = key.strip_prefix("merge_lock:");
        let Some(branch_hex) = branch_hex else { continue };
        let Some(branch) = parse_branch_hex(branch_hex) else { continue };
        let mut merge_bytes = [0u8; 16];
        merge_bytes.copy_from_slice(&val);
        lock_branch_by_merge.insert(MergeId(merge_bytes), branch);
    }

    // When a coordinator is supplied, refuse merge locks while WAL replay left it not-ready.
    if let Some(coord) = coordinator {
        if !coord.is_ready() && !lock_branch_by_merge.is_empty() {
            for (_, branch) in &lock_branch_by_merge {
                violations.push(MergeViolation::CoordinatorNotReadyWhileMergeLocked {
                    branch: *branch,
                });
            }
        }
    }

    for (key, _) in kv.scan_prefix("saga_batch:") {
        let merge_hex = key
            .strip_prefix("saga_batch:")
            .and_then(|rest| rest.split(':').next());
        let Some(merge_hex) = merge_hex else { continue };
        let merge_id = parse_merge_hex(merge_hex);
        let Some(merge_id) = merge_id else { continue };

        for rev in g.revisions() {
            if matches!(rev.status, RevisionStatus::Tombstone) {
                if saga_references_revision(kv, merge_id, rev.revision_id) {
                    violations.push(MergeViolation::SagaPendingWithMissingRevision {
                        merge_id,
                        revision_id: rev.revision_id,
                    });
                }
            }
        }
    }

    for (key, val) in kv.scan_prefix("merge_lock:") {
        if val.len() != 16 {
            continue;
        }
        let branch_hex = key.strip_prefix("merge_lock:");
        let Some(branch_hex) = branch_hex else { continue };
        let Some(branch) = parse_branch_hex(branch_hex) else { continue };
        let mut merge_bytes = [0u8; 16];
        merge_bytes.copy_from_slice(&val);
        let merge_id = MergeId(merge_bytes);
        let merge_hex = hex16(&merge_id.0);
        let saga_key = format!("saga_state:{merge_hex}");
        let has_saga = kv.get(&saga_key).map(|v| !v.is_empty()).unwrap_or(false);
        let msnap_prefix = format!("msnap:{merge_hex}:");
        let has_snapshot = !kv.scan_prefix(&msnap_prefix).is_empty();
        if !has_saga && !has_snapshot {
            violations.push(MergeViolation::MergeLockWithoutSaga { branch });
        }
    }

    for (key, val) in kv.scan_prefix("saga_state:") {
        if val.first() == Some(&6) {
            continue;
        }
        let merge_hex = key.strip_prefix("saga_state:");
        let Some(merge_hex) = merge_hex else { continue };
        let Some(merge_id) = parse_merge_hex(merge_hex) else { continue };
        if lock_branch_by_merge.contains_key(&merge_id) {
            continue;
        }
        let branch = branch_for_merge(kv, merge_id).unwrap_or(BranchId([0u8; 16]));
        violations.push(MergeViolation::SagaWithoutMergeLock { merge_id, branch });
    }

    violations
}

fn saga_references_revision(kv: &MemoryKv, merge_id: MergeId, revision_id: NodeRevisionId) -> bool {
    load_saga_edge_batches(kv, merge_id)
        .iter()
        .any(|batch| batch.target_revision_id == revision_id)
}

fn branch_for_merge(kv: &MemoryKv, merge_id: MergeId) -> Option<BranchId> {
    let prefix = format!("msnap:{}:ri:", hex16(&merge_id.0));
    for (key, _) in kv.scan_prefix(&prefix) {
        let rest = key.strip_prefix(&prefix)?;
        let branch_hex = rest.split(':').next()?;
        return parse_branch_hex(branch_hex);
    }
    None
}

pub fn check_all_invariants(ctx: &InvariantContext<'_>) -> InvariantReport {
    check_all_invariants_with_mode(ctx, InvariantCheckMode::Strict)
}

pub fn check_all_invariants_with_mode(
    ctx: &InvariantContext<'_>,
    mode: InvariantCheckMode,
) -> InvariantReport {
    InvariantReport {
        consistency: check_consistency(ctx.graph, ctx.kv, ctx.body_store, ctx.branches),
        write_path: check_write_path_invariants(
            ctx.graph,
            ctx.leases,
            ctx.patcher,
            ctx.spec_tracker,
        ),
        merge: check_merge_invariants_with_mode(ctx.graph, ctx.kv, ctx.coordinator, mode),
    }
}

pub fn assert_invariants(ctx: &InvariantContext<'_>) {
    let report = check_all_invariants(ctx);
    assert!(
        report.is_clean(),
        "invariant violation: {}",
        report.summary()
    );
}

fn parse_branch_hex(s: &str) -> Option<BranchId> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(BranchId(b))
}

fn parse_merge_hex(s: &str) -> Option<MergeId> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(MergeId(b))
}

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|byte| format!("{:02x}", byte)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn clean_empty_runtime() {
        let graph = SharedInMemoryGraph::new(InMemoryGraph::default());
        let kv = Arc::new(MemoryKv::new());
        let body = BodyStore::new(Arc::clone(&kv));
        let leases = Arc::new(PathLeaseManager::new());
        let spec = Arc::new(SpeculativePathTracker::new());
        let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
        let ctx = InvariantContext {
            graph: &graph,
            kv: kv.as_ref(),
            body_store: &body,
            branches: &[],
            leases: leases.as_ref(),
            patcher: &patcher,
            spec_tracker: spec.as_ref(),
            coordinator: None,
        };
        assert_invariants(&ctx);
    }
}
