//! **EdgeTargetOverride** KV (`eto:{branch}:{source_rev}:{edge_id}`) — **§01.7.1a**.

use std::sync::Arc;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::InMemoryGraph;
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

    /// Share an existing [`MemoryKv`] handle (clone shares the interior map).
    pub fn from_kv(kv: &MemoryKv) -> Self {
        Self {
            kv: Arc::new(kv.clone()),
        }
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

    /// Redirect every live outbound edge on `branch` whose raw target is `old_target`
    /// to `new_target` via ETO. No-op when identities are equal.
    ///
    /// Used after rename detection (ingest / merge) so callers keep resolving correctly
    /// even after the old identity's tombstone is GC'd.
    pub fn retarget_inbound_edges_for_rename(
        &self,
        graph: &InMemoryGraph,
        branch: BranchId,
        old_target: IdentityId,
        new_target: IdentityId,
    ) -> usize {
        retarget_inbound_edges_for_rename(graph, self, branch, old_target, new_target)
    }
}

/// Redirect live inbound edges from `old_target` → `new_target` on `branch`.
///
/// Returns the number of ETO rows written. Skips when `old_target == new_target`.
///
/// Source revisions are resolved preferentially via the target-branch primary, then
/// any live Active/Speculative revision of the source identity (merge graphs often
/// keep edges on base-branch revision rows that are merely rebound on the target).
pub fn retarget_inbound_edges_for_rename(
    graph: &InMemoryGraph,
    eto: &EdgeTargetOverrideStore,
    branch: BranchId,
    old_target: IdentityId,
    new_target: IdentityId,
) -> usize {
    if old_target == new_target {
        return 0;
    }
    let mut n = 0usize;
    for src_iid in graph.source_identities_targeting(old_target) {
        let mut source_revs: Vec<NodeRevisionId> = Vec::new();
        if let Some(src) = graph.primary_revision_for_identity(branch, src_iid) {
            source_revs.push(src.revision_id);
        } else {
            for rev in graph.revisions() {
                if rev.identity_id == src_iid
                    && matches!(
                        rev.status,
                        crate::graph::RevisionStatus::Active
                            | crate::graph::RevisionStatus::Speculative
                    )
                {
                    source_revs.push(rev.revision_id);
                }
            }
        }
        for rid in source_revs {
            for e in graph.outbound_edges(rid) {
                if e.target_identity_id == old_target {
                    eto.set_override(branch, rid, e.edge_id, new_target);
                    n += 1;
                }
            }
        }
    }
    n
}

/// True when a tombstone must be kept because its identity has no live primary and
/// live edges still target it (query resolution still needs tombstone bridging /
/// pending ETO coverage).
pub fn tombstone_needed_for_inbound_bridge(
    graph: &InMemoryGraph,
    branch_id: BranchId,
    identity_id: IdentityId,
) -> bool {
    if graph
        .primary_revision_for_identity(branch_id, identity_id)
        .is_some()
    {
        return false;
    }
    !graph.source_identities_targeting(identity_id).is_empty()
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

    #[test]
    fn retarget_inbound_edges_writes_eto_for_live_callers() {
        use crate::graph::{
            EdgeResolution, EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision,
            RevisionStatus, SourceSpan, SourceType,
        };
        let kv = Arc::new(MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let branch = BranchId([1u8; 16]);
        let old_t = IdentityId([10u8; 16]);
        let new_t = IdentityId([11u8; 16]);
        let caller_i = IdentityId([20u8; 16]);
        let caller_r = NodeRevisionId([20u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: caller_i,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: old_t,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: new_t,
            kind: NodeKind::Function,
        });
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
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let edge_id = [7u8; 16];
        g.replace_edges_for_revision(
            caller_r,
            vec![GraphEdge {
                edge_id,
                ty: EdgeType::Calls,
                source_revision_id: caller_r,
                target_identity_id: old_t,
                resolution: EdgeResolution {
                    target_signature_hash: [0u8; 32],
                    resolver: SourceType::Ast,
                    last_validation_ms: 0,
                },
                anchor: SourceSpan::UNKNOWN,
            }],
        )
        .unwrap();
        assert_eq!(
            retarget_inbound_edges_for_rename(&g, &eto, branch, old_t, new_t),
            1
        );
        assert_eq!(eto.get_override(branch, caller_r, edge_id), Some(new_t));
        assert_eq!(
            retarget_inbound_edges_for_rename(&g, &eto, branch, old_t, old_t),
            0,
            "same-identity rename must be a no-op"
        );
    }

    #[test]
    fn tombstone_needed_when_no_live_primary_and_inbound_edges() {
        use crate::graph::{
            EdgeResolution, EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision,
            RevisionStatus, SourceSpan, SourceType,
        };
        let mut g = InMemoryGraph::default();
        let branch = BranchId([1u8; 16]);
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
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        g.put_revision(NodeRevision {
            revision_id: tomb_r,
            identity_id: target_i,
            branch_id: branch,
            status: RevisionStatus::Tombstone,
            qualified_name: "old".into(),
            file_path: "a.py".into(),
            body_hash: [1u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: Some(1),
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
        assert!(tombstone_needed_for_inbound_bridge(&g, branch, target_i));
    }
}
