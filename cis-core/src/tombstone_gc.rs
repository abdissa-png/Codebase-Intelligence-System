//! Tombstone garbage collection worker (**Phase 3.4**).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cis_wal::{BranchId, NodeRevisionId};

use crate::graph::{InMemoryGraph, RevisionStatus};
use crate::graph_delete_queue::{DeleteJob, DeleteReason, GraphDeleteQueue};
use crate::ranking_policy::RankingPolicy;
use crate::revision_index::revision_binding_kv_key;
use crate::vector_cleanup_queue::VectorCleanupQueue;
use crate::{MemoryKv, SharedInMemoryGraph, WriteCoordinator};

#[derive(Debug, Default, Clone)]
pub struct GcDrainReport {
    pub attempted: usize,
    pub deleted_ok: usize,
    pub requeued: usize,
}

#[derive(Debug, Clone)]
pub struct TombstoneGcWorker {
    pub retention: Duration,
}

impl TombstoneGcWorker {
    pub fn from_policy(policy: &RankingPolicy) -> Self {
        Self {
            retention: Duration::from_secs(policy.tombstone_retention_days as u64 * 86_400),
        }
    }

    pub fn scan_eligible(
        &self,
        graph: &SharedInMemoryGraph,
        kv: &MemoryKv,
        coordinator: &WriteCoordinator,
        policy: &RankingPolicy,
        now_ms: u64,
    ) -> Vec<NodeRevisionId> {
        let g = graph.read();
        let bound = bound_revision_ids(kv);
        let protected_snapshots = snapshot_protected_revisions(kv, policy);
        let protected_wal = wal_protected_revisions(coordinator);
        let retention_ms = self.retention.as_millis() as u64;

        g.revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Tombstone))
            .filter(|r| {
                r.tombstoned_at_ms
                    .map(|t| now_ms.saturating_sub(t) >= retention_ms)
                    .unwrap_or(false)
            })
            .filter(|r| !bound.contains(&r.revision_id))
            .filter(|r| !protected_snapshots.contains(&r.revision_id))
            .filter(|r| !protected_wal.contains(&r.revision_id))
            .map(|r| r.revision_id)
            .collect()
    }

    pub fn enqueue_deletes(&self, ids: &[NodeRevisionId], queue: &GraphDeleteQueue, branch: BranchId) {
        for id in ids {
            queue.enqueue(DeleteJob {
                revision_id: *id,
                branch_id: branch,
                reason: DeleteReason::TombstoneExpired,
            });
        }
    }

    pub fn drain_batch(
        &self,
        graph: &SharedInMemoryGraph,
        kv: &MemoryKv,
        vector_queue: &VectorCleanupQueue,
        queue: &GraphDeleteQueue,
        max_batch: usize,
    ) -> GcDrainReport {
        let mut report = GcDrainReport::default();
        for _ in 0..max_batch {
            let Some(job) = queue.dequeue() else {
                break;
            };
            report.attempted += 1;
            match delete_revision(graph, kv, vector_queue, job.revision_id) {
                Ok(()) => report.deleted_ok += 1,
                Err(_) => {
                    queue.enqueue(job);
                    report.requeued += 1;
                }
            }
        }
        report
    }
}

fn bound_revision_ids(kv: &MemoryKv) -> HashSet<NodeRevisionId> {
    let mut out = HashSet::new();
    for (_, val) in kv.scan_prefix("ri:") {
        if val.len() == 16 {
            let mut b = [0u8; 16];
            b.copy_from_slice(&val);
            out.insert(NodeRevisionId(b));
        }
    }
    out
}

fn snapshot_protected_revisions(kv: &MemoryKv, policy: &RankingPolicy) -> HashSet<NodeRevisionId> {
    let _ = policy.time_travel_retention_days;
    let mut out = HashSet::new();
    for (_, val) in kv.scan_prefix("ris:") {
        if let Ok(ids) = serde_json::from_slice::<Vec<String>>(&val) {
            for s in ids {
                if s.len() == 32 {
                    let mut b = [0u8; 16];
                    for i in 0..16 {
                        if let Ok(x) = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16) {
                            b[i] = x;
                        }
                    }
                    out.insert(NodeRevisionId(b));
                }
            }
        }
    }
    out
}

fn wal_protected_revisions(coordinator: &WriteCoordinator) -> HashSet<NodeRevisionId> {
    let mut out = HashSet::new();
    for rec in coordinator.wal().iter_all() {
        if rec.phase.is_in_flight() {
            for r in rec.affected_revisions {
                out.insert(r);
            }
        }
    }
    out
}

fn delete_revision(
    graph: &SharedInMemoryGraph,
    kv: &MemoryKv,
    vector_queue: &VectorCleanupQueue,
    revision_id: NodeRevisionId,
) -> Result<(), &'static str> {
    let body_hash = {
        let g = graph.read();
        let rev = g.get_revision(revision_id).ok_or("missing revision")?;
        if !matches!(rev.status, RevisionStatus::Tombstone) {
            return Err("not tombstone");
        }
        rev.body_hash
    };
    {
        let mut g = graph.write();
        g.remove_revision(revision_id);
    }
    for (key, val) in kv.scan_prefix("ri:") {
        if val.len() == 16 && val == revision_id.0.to_vec() {
            kv.delete(&key);
        }
    }
    let _ = body_hash;
    vector_queue.enqueue_delete(body_hash);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Language, NodeIdentity, NodeKind, NodeRevision, SourceType};
    use cis_wal::MutationLog;

    fn tombstone_rev(id: u8, tombstoned_ms: u64) -> NodeRevision {
        NodeRevision {
            revision_id: NodeRevisionId([id; 16]),
            identity_id: cis_wal::IdentityId([id; 16]),
            branch_id: BranchId([0u8; 16]),
            status: RevisionStatus::Tombstone,
            qualified_name: format!("sym_{id}"),
            file_path: "a.py".into(),
            body_hash: [id; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: Default::default(),
            tombstoned_at_ms: Some(tombstoned_ms),
        }
    }

    #[test]
    fn scan_respects_retention() {
        let mut g = InMemoryGraph::default();
        g.put_revision(tombstone_rev(1, 0));
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        let wal = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(wal);
        let worker = TombstoneGcWorker {
            retention: Duration::from_secs(1),
        };
        let policy = RankingPolicy::default();
        let now = 10_000u64;
        let ids = worker.scan_eligible(&graph, &kv, &coord, &policy, now);
        assert_eq!(ids.len(), 1);
    }
}
