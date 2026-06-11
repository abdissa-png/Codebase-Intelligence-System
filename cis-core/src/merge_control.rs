//! **`cancel_merge`** rollback core — **FR-1.16** (subset: `ri:` restore, edge drop, vector delete, lock release).

use std::collections::HashMap;
use std::sync::Arc;

use cis_wal::{BranchId, IdentityId, MergeId, NodeRevisionId};
use thiserror::Error;

use crate::chunk_id::chunk_id;
use crate::graph::InMemoryGraph;
use crate::kv::{CasError, MemoryKv};
use crate::merge_lock;
use crate::revision_index::revision_binding_kv_key;
use crate::vector_cleanup_queue::VectorCleanupQueue;
use crate::vector_store::VectorChunkStore;

#[derive(Debug, Error)]
pub enum MergeCancelError {
    #[error(transparent)]
    Cas(#[from] CasError),
    #[error("parse msnap key")]
    BadSnapshotKey,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MergeCancelReport {
    pub merge_id: MergeId,
    pub restored_bindings: usize,
    pub snapshot_keys_cleared: usize,
    pub edges_cleared_attempts: usize,
    pub vector_chunks_deleted: usize,
    /// **FR-1.16 / v2.6:** chunks routed to DLQ when synchronous delete fails.
    pub vector_chunks_enqueued_dlq: usize,
}

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn msnap_prefix(merge_id: MergeId) -> String {
    format!("msnap:{}:", hex16(&merge_id.0))
}

/// `msnap:{merge}:ri:{branch}:{identity}` → pre-merge `revision_id` bytes
fn msnap_key(merge_id: MergeId, branch_id: BranchId, identity_id: IdentityId) -> String {
    format!(
        "msnap:{}:ri:{}:{}",
        hex16(&merge_id.0),
        hex16(&branch_id.0),
        hex16(&identity_id.0)
    )
}

fn parse_hex_identity32(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Returns `(branch_id, identity_id)` from a full msnap key.
fn parse_msnap_ri_key(key: &str) -> Option<(BranchId, IdentityId)> {
    let p: Vec<&str> = key.split(':').collect();
    if p.len() != 5 || p[0] != "msnap" || p[2] != "ri" {
        return None;
    }
    let branch = BranchId(parse_hex_identity32(p[3])?);
    let ident = IdentityId(parse_hex_identity32(p[4])?);
    Some((branch, ident))
}

#[derive(Debug)]
pub struct MergeControl {
    kv: Arc<MemoryKv>,
}

/// Pre-merge `ri:` bindings captured in **`msnap:{merge}:ri:{branch}:{identity}`** keys.
pub fn load_msnap_bindings(
    kv: &MemoryKv,
    merge_id: MergeId,
    branch_id: BranchId,
) -> HashMap<IdentityId, NodeRevisionId> {
    let pref = msnap_prefix(merge_id);
    let mut out = HashMap::new();
    for (k, v) in kv.scan_prefix(&pref) {
        let Some((b, ident)) = parse_msnap_ri_key(&k) else {
            continue;
        };
        if b != branch_id || v.len() != 16 {
            continue;
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&v);
        out.insert(ident, NodeRevisionId(arr));
    }
    out
}

impl MergeControl {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    /// After **`acquire_merge_lock`**, persist **pre-merge** `ri:` pairs for **`cancel_merge`**.
    pub fn record_premerge_bindings(
        &self,
        merge_id: MergeId,
        branch_id: BranchId,
        bindings: &[(IdentityId, NodeRevisionId)],
    ) {
        for &(id, rev) in bindings {
            self.kv
                .set(&msnap_key(merge_id, branch_id, id), rev.0.to_vec());
        }
    }

    /// 1. Restore `ri:` from snapshot for this `merge_id`.  
    /// 2. For each revision in `clear_edges_for`, drop outbound edges (**FR-1.16** partial graph cleanup).  
    /// 3. Delete vector chunks (or enqueue **`VectorCleanupQueue`** on failure per v2.6).  
    /// 4. Remove `msnap:*` keys.  
    /// 5. Release **`merge_lock`** if still held by **`merge_id`**.
    ///
    /// **`vector_dlq`:** when `delete_chunks` returns **`Err`**, each affected `chunk_id` is appended to the DLQ
    /// so the branch can still unlock. If **`None`**, delete failure is ignored for unlock (observability gap — prefer passing a DLQ in production).
    pub fn cancel_merge(
        &self,
        merge_id: MergeId,
        branch_id: BranchId,
        graph: &mut InMemoryGraph,
        vector: &dyn VectorChunkStore,
        vector_dlq: Option<&VectorCleanupQueue>,
        clear_edges_for: &[NodeRevisionId],
    ) -> Result<MergeCancelReport, MergeCancelError> {
        let saga_batches = crate::merge_saga_batch::load_saga_edge_batches(&self.kv, merge_id);
        crate::merge_saga_batch::compensate_saga_edge_batches(graph, &saga_batches);
        crate::merge_saga_batch::purge_saga_edge_payloads(&self.kv, merge_id);

        let mut report = MergeCancelReport {
            merge_id,
            restored_bindings: 0,
            snapshot_keys_cleared: 0,
            edges_cleared_attempts: 0,
            vector_chunks_deleted: 0,
            vector_chunks_enqueued_dlq: 0,
        };
        let pref = msnap_prefix(merge_id);
        let rows = self.kv.scan_prefix(&pref);
        for (k, v) in &rows {
            if !k.contains(":ri:") {
                continue;
            }
            let (b, ident) = parse_msnap_ri_key(k).ok_or(MergeCancelError::BadSnapshotKey)?;
            if b != branch_id {
                continue;
            }
            if v.len() != 16 {
                continue;
            }
            self.kv.set(&revision_binding_kv_key(branch_id, ident), v.clone());
            report.restored_bindings += 1;
        }
        for rid in clear_edges_for {
            let _ = graph.replace_edges_for_revision(*rid, vec![]);
            report.edges_cleared_attempts += 1;
            if let Some(nr) = graph.get_revision(*rid) {
                let cid = chunk_id(nr.identity_id, *rid, 0);
                if vector.has_chunk(&cid) {
                    match vector.delete_chunks(&[cid]) {
                        Ok(()) => report.vector_chunks_deleted += 1,
                        Err(_) => {
                            if let Some(dlq) = vector_dlq {
                                dlq.enqueue_delete(cid);
                                report.vector_chunks_enqueued_dlq += 1;
                            }
                        }
                    }
                }
            }
        }
        for (k, _) in rows {
            self.kv.delete(&k);
            report.snapshot_keys_cleared += 1;
        }
        if merge_lock::merge_lock_holder(&self.kv, branch_id) == Some(merge_id) {
            merge_lock::release_merge_lock(&self.kv, branch_id, merge_id)?;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_id::chunk_id;
    use crate::graph::{
        EdgeResolution, Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus, SourceType,
    };
    use crate::merge_lock::acquire_merge_lock;
    use crate::revision_index::RevisionIndex;
    use crate::vector_store::InMemoryVectorStore;
    use cis_wal::NodeRevisionId;

    #[test]
    fn cancel_restores_ri_clears_lock() {
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([1u8; 16]);
        let merge_id = MergeId([2u8; 16]);
        let ri = RevisionIndex::new(branch, Arc::clone(&kv));
        let i = IdentityId([3u8; 16]);
        let r_old = NodeRevisionId([4u8; 16]);
        let r_new = NodeRevisionId([5u8; 16]);
        ri.bind(i, r_old);
        acquire_merge_lock(&kv, branch, merge_id).unwrap();
        let mc = MergeControl::new(Arc::clone(&kv));
        mc.record_premerge_bindings(merge_id, branch, &[(i, r_old)]);
        ri.bind(i, r_new);
        assert_eq!(ri.lookup(i), Some(r_new));
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: i,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id: r_new,
            identity_id: i,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: "f".into(),
            file_path: "a.py".into(),
            body_hash: [9u8; 32],
            signature_hash: [8u8; 32],
            language: Language::Python,
            parent_revision_id: Some(r_old),
            rename_source_id: None,
                    tombstoned_at_ms: None,
            span: crate::graph::SourceSpan::UNKNOWN,
        });
        let eid = [7u8; 16];
        let edge = crate::graph::GraphEdge {
            edge_id: eid,
            ty: crate::graph::EdgeType::Calls,
            source_revision_id: r_new,
            target_identity_id: i,
            resolution: EdgeResolution {
                target_signature_hash: [1u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: crate::graph::SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(r_new, vec![edge]).unwrap();
        let v = InMemoryVectorStore::new();
        let cid = chunk_id(i, r_new, 0);
        v.upsert(cid, vec![1.0]);
        let rep = mc
            .cancel_merge(merge_id, branch, &mut g, &v, None, &[r_new])
            .unwrap();
        assert_eq!(ri.lookup(i), Some(r_old));
        assert_eq!(rep.restored_bindings, 1);
        assert!(g.outbound_edges(r_new).is_empty());
        assert!(!v.has(&cid));
        assert!(merge_lock::merge_lock_holder(&kv, branch).is_none());
    }

    #[test]
    fn cancel_enqueues_dlq_when_vector_delete_fails() {
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([1u8; 16]);
        let merge_id = MergeId([2u8; 16]);
        let i = IdentityId([3u8; 16]);
        let r_old = NodeRevisionId([4u8; 16]);
        let r_new = NodeRevisionId([5u8; 16]);
        let ri = RevisionIndex::new(branch, Arc::clone(&kv));
        ri.bind(i, r_old);
        acquire_merge_lock(&kv, branch, merge_id).unwrap();
        let mc = MergeControl::new(Arc::clone(&kv));
        mc.record_premerge_bindings(merge_id, branch, &[(i, r_old)]);
        ri.bind(i, r_new);
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: i,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id: r_new,
            identity_id: i,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: "f".into(),
            file_path: "a.py".into(),
            body_hash: [9u8; 32],
            signature_hash: [8u8; 32],
            language: Language::Python,
            parent_revision_id: Some(r_old),
            rename_source_id: None,
                    tombstoned_at_ms: None,
            span: crate::graph::SourceSpan::UNKNOWN,
        });
        let base = InMemoryVectorStore::new();
        let cid = chunk_id(i, r_new, 0);
        base.upsert(cid, vec![1.0]);
        let flaky = crate::vector_store::FlakyVectorStore::new(base);
        flaky.set_fail_delete(true);
        let dlq = crate::vector_cleanup_queue::VectorCleanupQueue::new();
        let rep = mc
            .cancel_merge(merge_id, branch, &mut g, &flaky, Some(&dlq), &[r_new])
            .unwrap();
        assert_eq!(rep.vector_chunks_enqueued_dlq, 1);
        assert_eq!(dlq.dequeue(), Some(cid));
        assert!(flaky.inner().has(&cid));
    }
}
