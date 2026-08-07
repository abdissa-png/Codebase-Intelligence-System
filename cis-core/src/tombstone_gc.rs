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
        // Soft-deleted Actives whose branch no longer has children become tombstones first.
        {
            let mut g = graph.write();
            let _ = crate::identity_resolution::finalize_soft_deletes_without_children(&mut g, kv);
        }
        let g = graph.read();
        let bound = bound_revision_ids(kv);
        let protected_snapshots = snapshot_protected_revisions(kv, policy);
        let protected_wal = wal_protected_revisions(coordinator);
        let retention_ms = self.retention.as_millis() as u64;

        g.revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Tombstone))
            .filter(|r| {
                // Missing timestamp → treat as ancient / eligible for GC (aligned with
                // rename window excluding untimestamped tombs from candidates).
                r.tombstoned_at_ms
                    .map(|t| now_ms.saturating_sub(t) >= retention_ms)
                    .unwrap_or(true)
            })
            .filter(|r| !bound.contains(&r.revision_id))
            .filter(|r| !protected_snapshots.contains(&r.revision_id))
            .filter(|r| !protected_wal.contains(&r.revision_id))
            .filter(|r| {
                !crate::edge_target_override::tombstone_needed_for_inbound_bridge(
                    &g,
                    r.branch_id,
                    r.identity_id,
                )
            })
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
    for (key, val) in kv.scan_prefix("ri:") {
        // Skip provisional CAS rows (`ri:provisional:...`); only real bindings are 16-byte rev ids.
        if key.starts_with("ri:provisional:") {
            continue;
        }
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
    let (identity_id, _body_hash) = {
        let g = graph.read();
        let rev = g.get_revision(revision_id).ok_or("missing revision")?;
        if !matches!(rev.status, RevisionStatus::Tombstone) {
            return Err("not tombstone");
        }
        if crate::edge_target_override::tombstone_needed_for_inbound_bridge(
            &g,
            rev.branch_id,
            rev.identity_id,
        ) {
            return Err("inbound references");
        }
        (rev.identity_id, rev.body_hash)
    };
    {
        let mut g = graph.write();
        g.remove_revision(revision_id);
    }
    for (key, val) in kv.scan_prefix("ri:") {
        if key.starts_with("ri:provisional:") {
            continue;
        }
        if val.len() == 16 && val == revision_id.0.to_vec() {
            kv.delete(&key);
        }
    }
    crate::edge_target_override::delete_eto_for_source_revision(kv, revision_id);
    // Queue the lifecycle chunk_id (not body_hash) so VectorStore refcounting works.
    let cid = crate::chunk_id::chunk_id(identity_id, revision_id, 0);
    vector_queue.enqueue_delete(cid);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Language, NodeKind, NodeRevision};
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

    #[test]
    fn delete_revision_clears_eto_rows() {
        let mut g = InMemoryGraph::default();
        let rid = NodeRevisionId([7u8; 16]);
        let mut rev = tombstone_rev(7, 0);
        rev.revision_id = rid;
        g.put_identity(crate::graph::NodeIdentity {
            identity_id: rev.identity_id,
            kind: NodeKind::Function,
        });
        g.put_revision(rev);
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        let branch = BranchId([0u8; 16]);
        let edge_id = [3u8; 16];
        let key = crate::edge_target_override::eto_key(branch, rid, edge_id);
        kv.set(&key, cis_wal::IdentityId([4u8; 16]).0.to_vec());
        assert!(kv.get(&key).is_some());

        let vq = VectorCleanupQueue::default();
        delete_revision(&graph, &kv, &vq, rid).unwrap();
        assert!(kv.get(&key).is_none());
        assert!(graph.read().get_revision(rid).is_none());
    }

    #[test]
    fn scan_skips_tombstone_with_live_inbound_edges() {
        use crate::graph::{
            EdgeResolution, EdgeType, GraphEdge, NodeIdentity, RevisionStatus, SourceSpan,
            SourceType,
        };
        use cis_wal::IdentityId;

        let mut g = InMemoryGraph::default();
        let branch = BranchId([0u8; 16]);
        let target_i = IdentityId([10u8; 16]);
        let caller_i = IdentityId([20u8; 16]);
        let caller_r = NodeRevisionId([20u8; 16]);
        let tomb_r = NodeRevisionId([10u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: caller_i,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: target_i,
            kind: NodeKind::Function,
        });
        let mut tomb = tombstone_rev(10, 0);
        tomb.revision_id = tomb_r;
        tomb.identity_id = target_i;
        tomb.branch_id = branch;
        g.put_revision(tomb);
        g.put_revision(NodeRevision {
            revision_id: caller_r,
            identity_id: caller_i,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: "caller".into(),
            file_path: "a.py".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: Default::default(),
            tombstoned_at_ms: None,
        });
        g.replace_edges_for_revision(
            caller_r,
            vec![GraphEdge {
                edge_id: [7u8; 16],
                ty: EdgeType::Calls,
                source_revision_id: caller_r,
                target_identity_id: target_i,
                resolution: EdgeResolution {
                    target_signature_hash: [0u8; 32],
                    resolver: SourceType::Ast,
                    last_validation_ms: 0,
                },
                anchor: SourceSpan::UNKNOWN,
            }],
        )
        .unwrap();

        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        let wal = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(wal);
        let worker = TombstoneGcWorker {
            retention: Duration::from_secs(0),
        };
        let policy = RankingPolicy::default();
        let ids = worker.scan_eligible(&graph, &kv, &coord, &policy, 10_000);
        assert!(
            !ids.contains(&tomb_r),
            "tombstone with inbound edges must not be GC-eligible"
        );
    }
}
