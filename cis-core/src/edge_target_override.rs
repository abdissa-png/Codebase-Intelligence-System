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

/// Remove every `eto:*:{source_rev}:*` row (all branches) for a GC'd revision.
pub fn delete_eto_for_source_revision(kv: &MemoryKv, source_rev: NodeRevisionId) -> usize {
    let rev_hex = hex16(&source_rev.0);
    let mut keys = Vec::new();
    for (k, _) in kv.scan_prefix("eto:") {
        let parts: Vec<&str> = k.split(':').collect();
        // eto:{branch}:{source_rev}:{edge_id}
        if parts.len() == 4 && parts[2] == rev_hex {
            keys.push(k);
        }
    }
    let n = keys.len();
    for k in keys {
        kv.delete(&k);
    }
    n
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

    /// Resolve the effective target for `edge` on a single `branch_id`.
    ///
    /// Prefer [`Self::effective_target_identity_in_chain`] for interactive queries so
    /// parent-branch overrides on inherited edges are visible.
    pub fn effective_target_identity(
        &self,
        branch_id: BranchId,
        edge: &crate::graph::GraphEdge,
    ) -> IdentityId {
        self.get_override(branch_id, edge.source_revision_id, edge.edge_id)
            .unwrap_or(edge.target_identity_id)
    }

    /// Nearest-first ETO lookup across a branch ancestry chain (`[child, parent, …]`).
    ///
    /// A child override wins over a parent override for the same `(source_rev, edge_id)`.
    /// If no chain member has an override, falls back to `edge.target_identity_id`.
    pub fn effective_target_identity_in_chain(
        &self,
        chain: &[BranchId],
        edge: &crate::graph::GraphEdge,
    ) -> IdentityId {
        for &branch_id in chain {
            if let Some(id) =
                self.get_override(branch_id, edge.source_revision_id, edge.edge_id)
            {
                return id;
            }
        }
        edge.target_identity_id
    }

    pub fn delete_override(
        &self,
        branch_id: BranchId,
        source_rev: NodeRevisionId,
        edge_id: [u8; 16],
    ) {
        self.kv.delete(&eto_key(branch_id, source_rev, edge_id));
    }

    /// Remove every `eto:*:{source_rev}:*` row (all branches) for a GC'd revision.
    pub fn delete_overrides_for_source_revision(&self, source_rev: NodeRevisionId) -> usize {
        delete_eto_for_source_revision(&self.kv, source_rev)
    }

    /// `(branch, source_rev, edge_id)` for every ETO whose override target is `target`.
    pub fn overrides_targeting(
        &self,
        target: IdentityId,
    ) -> Vec<(BranchId, NodeRevisionId, [u8; 16])> {
        let want = target.0.as_slice();
        let mut out = Vec::new();
        for (k, v) in self.kv.scan_prefix("eto:") {
            if v.as_slice() != want {
                continue;
            }
            let parts: Vec<&str> = k.split(':').collect();
            if parts.len() != 4 {
                continue;
            }
            let Some(branch) = parse_hex16_id(parts[1]).map(BranchId) else {
                continue;
            };
            let Some(source_rev) = parse_hex16_id(parts[2]).map(NodeRevisionId) else {
                continue;
            };
            let Some(edge_id) = parse_hex16_id(parts[3]) else {
                continue;
            };
            out.push((branch, source_rev, edge_id));
        }
        out
    }
}

fn parse_hex16_id(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(b)
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

    #[test]
    fn chain_inherits_parent_override() {
        use crate::graph::{EdgeResolution, EdgeType, GraphEdge, SourceType};
        let kv = Arc::new(MemoryKv::new());
        let s = EdgeTargetOverrideStore::new(kv);
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let r = NodeRevisionId([2u8; 16]);
        let e = [3u8; 16];
        let orig = IdentityId([4u8; 16]);
        let parent_alt = IdentityId([5u8; 16]);
        s.set_override(main, r, e, parent_alt);
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
        assert_eq!(
            s.effective_target_identity_in_chain(&[feature, main], &edge),
            parent_alt
        );
    }

    #[test]
    fn chain_child_override_wins_over_parent() {
        use crate::graph::{EdgeResolution, EdgeType, GraphEdge, SourceType};
        let kv = Arc::new(MemoryKv::new());
        let s = EdgeTargetOverrideStore::new(kv);
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let r = NodeRevisionId([2u8; 16]);
        let e = [3u8; 16];
        let orig = IdentityId([4u8; 16]);
        let parent_alt = IdentityId([5u8; 16]);
        let child_alt = IdentityId([6u8; 16]);
        s.set_override(main, r, e, parent_alt);
        s.set_override(feature, r, e, child_alt);
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
        assert_eq!(
            s.effective_target_identity_in_chain(&[feature, main], &edge),
            child_alt
        );
    }

    #[test]
    fn delete_overrides_for_source_revision_clears_all_branches() {
        let kv = Arc::new(MemoryKv::new());
        let s = EdgeTargetOverrideStore::new(Arc::clone(&kv));
        let b1 = BranchId([1u8; 16]);
        let b2 = BranchId([2u8; 16]);
        let r = NodeRevisionId([9u8; 16]);
        let other = NodeRevisionId([8u8; 16]);
        let e = [3u8; 16];
        let t = IdentityId([4u8; 16]);
        s.set_override(b1, r, e, t);
        s.set_override(b2, r, e, t);
        s.set_override(b1, other, e, t);
        assert_eq!(s.delete_overrides_for_source_revision(r), 2);
        assert!(s.get_override(b1, r, e).is_none());
        assert!(s.get_override(b2, r, e).is_none());
        assert_eq!(s.get_override(b1, other, e), Some(t));
    }

    #[test]
    fn overrides_targeting_finds_eto_rows() {
        let kv = Arc::new(MemoryKv::new());
        let s = EdgeTargetOverrideStore::new(kv);
        let b = BranchId([1u8; 16]);
        let r = NodeRevisionId([2u8; 16]);
        let e = [3u8; 16];
        let t = IdentityId([9u8; 16]);
        s.set_override(b, r, e, t);
        let hits = s.overrides_targeting(t);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], (b, r, e));
    }
}
