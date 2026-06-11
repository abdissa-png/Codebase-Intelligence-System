//! **FR-1.14** saga **`EDGE_BATCH_N`** apply + inverse compensate.

use cis_wal::{MergeId, NodeRevisionId};

use crate::graph::{GraphEdge, InMemoryGraph};
use crate::kv::MemoryKv;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SagaEdgeBatch {
    pub seq: u32,
    pub target_revision_id: NodeRevisionId,
    /// Outbound edges before this batch was applied (restored on compensate).
    pub prior_edges: Vec<GraphEdge>,
    /// Outbound edges after apply.
    pub edges: Vec<GraphEdge>,
}

pub fn apply_saga_edge_batch(
    graph: &mut InMemoryGraph,
    batch: &SagaEdgeBatch,
) -> Result<(), &'static str> {
    graph.replace_edges_for_revision(batch.target_revision_id, batch.edges.clone())
}

/// **Compensate:** restore pre-batch edge sets in **reverse** application order.
pub fn compensate_saga_edge_batches(graph: &mut InMemoryGraph, batches: &[SagaEdgeBatch]) {
    for b in batches.iter().rev() {
        let _ = graph.replace_edges_for_revision(b.target_revision_id, b.prior_edges.clone());
    }
}

fn merge_hex(merge_id: MergeId) -> String {
    merge_id
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn saga_payload_key(merge_id: MergeId, seq: u32) -> String {
    format!("saga_payload:{}:{}", merge_hex(merge_id), seq)
}

/// Persist serialized batch payload for crash/cancel compensation.
pub fn persist_saga_edge_batch(kv: &MemoryKv, merge_id: MergeId, batch: &SagaEdgeBatch) {
    if let Ok(bytes) = serde_json::to_vec(batch) {
        kv.set(&saga_payload_key(merge_id, batch.seq), bytes);
    }
}

/// Load all saga edge batches for a merge (ascending `seq`).
pub fn load_saga_edge_batches(kv: &MemoryKv, merge_id: MergeId) -> Vec<SagaEdgeBatch> {
    let prefix = format!("saga_payload:{}:", merge_hex(merge_id));
    let mut batches: Vec<SagaEdgeBatch> = kv
        .scan_prefix(&prefix)
        .into_iter()
        .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
        .collect();
    batches.sort_by_key(|b| b.seq);
    batches
}

pub fn purge_saga_edge_payloads(kv: &MemoryKv, merge_id: MergeId) {
    let prefix = format!("saga_payload:{}:", merge_hex(merge_id));
    for (k, _) in kv.scan_prefix(&prefix) {
        kv.delete(&k);
    }
}

/// Apply batches to the graph, persisting each payload and saga idempotency marker.
pub fn apply_and_persist_saga_batches(
    graph: &mut InMemoryGraph,
    kv: &MemoryKv,
    merge_id: MergeId,
    saga: &crate::saga::MergeSagaOrchestrator,
    batches: &[SagaEdgeBatch],
) -> Result<(), &'static str> {
    for batch in batches {
        apply_saga_edge_batch(graph, batch)?;
        persist_saga_edge_batch(kv, merge_id, batch);
        saga.persist_batch_marker(merge_id, batch.seq);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cis_wal::{BranchId, IdentityId};

    use crate::graph::{
        EdgeResolution, EdgeType, Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
        SourceSpan, SourceType,
    };

    #[test]
    fn batch_apply_then_compensate_restores_prior() {
        let mut g = InMemoryGraph::default();
        let i_src = IdentityId([1u8; 16]);
        let i_tgt = IdentityId([2u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: i_src,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: i_tgt,
            kind: NodeKind::Function,
        });
        let r = NodeRevisionId([3u8; 16]);
        g.put_revision(NodeRevision {
            revision_id: r,
            identity_id: i_src,
            branch_id: BranchId([0u8; 16]),
            status: RevisionStatus::Active,
            qualified_name: "f".into(),
            file_path: "a.py".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            tombstoned_at_ms: None,
            span: SourceSpan::UNKNOWN,
        });
        let prior = vec![GraphEdge {
            edge_id: [8u8; 16],
            ty: EdgeType::Uses,
            source_revision_id: r,
            target_identity_id: i_tgt,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        }];
        let _ = g.replace_edges_for_revision(r, prior.clone());
        let edge = GraphEdge {
            edge_id: [9u8; 16],
            ty: EdgeType::Calls,
            source_revision_id: r,
            target_identity_id: i_tgt,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        let batch = SagaEdgeBatch {
            seq: 1,
            target_revision_id: r,
            prior_edges: prior,
            edges: vec![edge],
        };
        apply_saga_edge_batch(&mut g, &batch).unwrap();
        assert_eq!(g.outbound_edges(r).len(), 1);
        assert_eq!(g.outbound_edges(r)[0].ty, EdgeType::Calls);
        compensate_saga_edge_batches(&mut g, &[batch]);
        assert_eq!(g.outbound_edges(r).len(), 1);
        assert_eq!(g.outbound_edges(r)[0].ty, EdgeType::Uses);
    }

    #[test]
    fn saga_payload_roundtrip_kv() {
        let kv = crate::kv::MemoryKv::new();
        let mid = MergeId([7u8; 16]);
        let batch = SagaEdgeBatch {
            seq: 2,
            target_revision_id: NodeRevisionId([3u8; 16]),
            prior_edges: vec![],
            edges: vec![],
        };
        persist_saga_edge_batch(&kv, mid, &batch);
        let loaded = load_saga_edge_batches(&kv, mid);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].seq, 2);
        purge_saga_edge_payloads(&kv, mid);
        assert!(load_saga_edge_batches(&kv, mid).is_empty());
    }
}
