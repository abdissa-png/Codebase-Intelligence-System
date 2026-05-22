//! **EdgeTargetOverride** KV (`eto:{branch}:{source_rev}:{edge_id}`) — **§01.7.1a**.

use std::sync::Arc;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::kv::MemoryKv;

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

pub fn eto_key(branch_id: BranchId, source_revision: NodeRevisionId, edge_id: [u8; 16]) -> String {
    format!(
        "eto:{}:{}:{}",
        hex16(&branch_id.0),
        hex16(&source_revision.0),
        hex16(&edge_id)
    )
}

#[derive(Debug)]
pub struct EdgeTargetOverrideStore {
    kv: Arc<MemoryKv>,
}

impl EdgeTargetOverrideStore {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    pub fn set_override(
        &self,
        branch_id: BranchId,
        source_rev: NodeRevisionId,
        edge_id: [u8; 16],
        new_target: IdentityId,
    ) {
        let k = eto_key(branch_id, source_rev, edge_id);
        self.kv.set(&k, new_target.0.to_vec());
    }

    pub fn get_override(
        &self,
        branch_id: BranchId,
        source_rev: NodeRevisionId,
        edge_id: [u8; 16],
    ) -> Option<IdentityId> {
        let v = self.kv.get(&eto_key(branch_id, source_rev, edge_id))?;
        if v.len() != 16 {
            return None;
        }
        let mut a = [0u8; 16];
        a.copy_from_slice(&v);
        Some(IdentityId(a))
    }

    pub fn effective_target_identity(
        &self,
        branch_id: BranchId,
        edge: &crate::graph::GraphEdge,
    ) -> IdentityId {
        self.get_override(branch_id, edge.source_revision_id, edge.edge_id)
            .unwrap_or(edge.target_identity_id)
    }

    pub fn delete_override(
        &self,
        branch_id: BranchId,
        source_rev: NodeRevisionId,
        edge_id: [u8; 16],
    ) {
        self.kv.delete(&eto_key(branch_id, source_rev, edge_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let s = EdgeTargetOverrideStore::new(kv);
        let b = BranchId([1u8; 16]);
        let r = NodeRevisionId([2u8; 16]);
        let e = [3u8; 16];
        let t = IdentityId([4u8; 16]);
        s.set_override(b, r, e, t);
        assert_eq!(s.get_override(b, r, e), Some(t));
    }

    #[test]
    fn effective_target_uses_override_when_present() {
        use crate::graph::{EdgeResolution, EdgeType, GraphEdge, SourceType};
        let kv = Arc::new(MemoryKv::new());
        let s = EdgeTargetOverrideStore::new(kv);
        let b = BranchId([1u8; 16]);
        let r = NodeRevisionId([2u8; 16]);
        let e = [3u8; 16];
        let orig = IdentityId([4u8; 16]);
        let alt = IdentityId([5u8; 16]);
        s.set_override(b, r, e, alt);
        let edge = GraphEdge {
            edge_id: e,
            ty: EdgeType::Calls,
            source_revision_id: r,
            target_identity_id: orig,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: crate::graph::SourceSpan::UNKNOWN,
        };
        assert_eq!(s.effective_target_identity(b, &edge), alt);
    }
}
