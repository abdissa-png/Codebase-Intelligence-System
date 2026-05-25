//! **Phase 5 / FR-2.5** — confidence-aware graph traversal, ETO resolution, tombstone bridging.

use std::collections::{HashSet, VecDeque};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::confidence::{edge_confidence, node_confidence_from_inbound, path_confidence, path_floor};
use crate::edge_target_override::EdgeTargetOverrideStore;
use crate::graph::{
    EdgeType, GraphEdge, InMemoryGraph, NodeKind, NodeRevision, RevisionStatus, SourceType,
};
use crate::index_model::stable_rev_id_bytes;
use crate::ranking_policy::RankingPolicySnapshot;

fn is_context_edge(ty: EdgeType) -> bool {
    matches!(
        ty,
        EdgeType::Calls | EdgeType::Imports | EdgeType::Uses | EdgeType::Extends
    )
}

/// File hub revision (`$file`) for import edges on a path.
pub fn file_hub_revision_for_path(
    g: &InMemoryGraph,
    branch: BranchId,
    file_path: &str,
) -> Option<NodeRevisionId> {
    let rid = NodeRevisionId(stable_rev_id_bytes(branch, file_path, "$file"));
    g.get_revision(rid)
        .filter(|r| r.branch_id == branch)
        .map(|_| rid)
}

fn is_file_hub_revision(g: &InMemoryGraph, rid: NodeRevisionId) -> bool {
    g.get_revision(rid)
        .and_then(|r| g.identity_kind(r.identity_id))
        == Some(NodeKind::File)
}

/// Outbound edges for context traversal, including file-hub import hop (imports not duplicated on symbols).
pub fn outbound_context_edges<'a>(
    g: &'a InMemoryGraph,
    branch: BranchId,
    source_revision: NodeRevisionId,
) -> Vec<&'a GraphEdge> {
    let mut out: Vec<&GraphEdge> = g
        .outbound_edges(source_revision)
        .iter()
        .filter(|e| is_context_edge(e.ty))
        .collect();
    if !is_file_hub_revision(g, source_revision) {
        if let Some(rev) = g.get_revision(source_revision) {
            if let Some(fr) = file_hub_revision_for_path(g, branch, &rev.file_path) {
                for e in g.outbound_edges(fr) {
                    if e.ty == EdgeType::Imports {
                        out.push(e);
                    }
                }
            }
        }
    }
    out
}

/// **FR-2.5 / §01.3** — `Stub` nodes are traversal boundaries (OPAQUE gate).
pub fn is_opaque_traversal_gate(g: &InMemoryGraph, rev: &NodeRevision) -> bool {
    g.identity_kind(rev.identity_id) == Some(NodeKind::Stub)
}

/// Follow **`RENAMED_FROM`** on a tombstone revision to the successor identity.
pub fn rename_successor_identity(g: &InMemoryGraph, tombstone_revision: NodeRevisionId) -> Option<IdentityId> {
    for e in g.outbound_edges(tombstone_revision) {
        if e.ty == EdgeType::RenamedFrom {
            return Some(e.target_identity_id);
        }
    }
    None
}

/// Resolve an identity to the best query target revision, bridging tombstones via **`RENAMED_FROM`**.
pub fn resolve_identity_revision<'a>(
    g: &'a InMemoryGraph,
    branch: BranchId,
    identity_id: IdentityId,
) -> Option<&'a NodeRevision> {
    let primary = g.primary_revision_for_identity(branch, identity_id)?;
    if matches!(primary.status, RevisionStatus::Active) {
        return Some(primary);
    }
    if matches!(primary.status, RevisionStatus::Tombstone) {
        let successor = rename_successor_identity(g, primary.revision_id)?;
        return g
            .primary_revision_for_identity(branch, successor)
            .filter(|r| matches!(r.status, RevisionStatus::Active));
    }
    None
}

/// Effective target identity (ETO) then revision resolution with tombstone bridging.
pub fn resolve_edge_target<'a>(
    g: &'a InMemoryGraph,
    eto: &EdgeTargetOverrideStore,
    branch: BranchId,
    edge: &GraphEdge,
) -> Option<&'a NodeRevision> {
    let target_id = eto.effective_target_identity(branch, edge);
    resolve_identity_revision(g, branch, target_id)
}

fn min_source_on_path(current: SourceType, edge: &GraphEdge) -> SourceType {
    let s = edge.resolution.resolver;
    if path_floor(s) < path_floor(current) {
        s
    } else {
        current
    }
}

fn inbound_edge_confidences(
    g: &InMemoryGraph,
    branch: BranchId,
    identity_id: IdentityId,
    now_ms: u64,
    half_life_ms: u64,
) -> Vec<f64> {
    let mut confs = Vec::new();
    for sid in g.source_identities_targeting(identity_id) {
        let Some(src) = g.primary_revision_for_identity(branch, sid) else {
            continue;
        };
        for e in g.outbound_edges(src.revision_id) {
            if e.target_identity_id == identity_id {
                confs.push(edge_confidence(e, now_ms, half_life_ms));
            }
        }
    }
    confs
}

/// Node confidence for MCP hits (**§01.1**).
pub fn node_hit_confidence(
    g: &InMemoryGraph,
    rev: &NodeRevision,
    branch: BranchId,
    now_ms: u64,
    half_life_ms: u64,
) -> f64 {
    let inbound = inbound_edge_confidences(g, branch, rev.identity_id, now_ms, half_life_ms);
    node_confidence_from_inbound(&inbound, SourceType::Ast)
}

/// Result of confidence-aware BFS for **`expand_context`**.
#[derive(Debug)]
pub struct ExpandContextResult {
    pub hits: Vec<(NodeRevisionId, f64)>,
    pub pruned_low_confidence_count: usize,
}

/// BFS over Calls/Imports/Uses with path-confidence pruning and OPAQUE stub gates.
pub fn expand_context_bfs(
    g: &InMemoryGraph,
    eto: &EdgeTargetOverrideStore,
    policy: &RankingPolicySnapshot,
    branch: BranchId,
    start: NodeRevisionId,
    depth: u32,
    now_ms: u64,
) -> ExpandContextResult {
    let half_life_ms = policy.recency.half_life_days as u64 * 24 * 60 * 60 * 1000;
    let min_path = policy.min_path_confidence;
    let mut seen_rev: HashSet<NodeRevisionId> = HashSet::new();
    let mut q: VecDeque<(NodeRevisionId, u32, Vec<f64>, SourceType)> = VecDeque::new();
    q.push_back((start, 0, vec![], SourceType::Ast));
    let mut hits = Vec::new();
    let mut pruned = 0usize;

    while let Some((rid, d, path_edge_confs, min_src)) = q.pop_front() {
        if !seen_rev.insert(rid) || d > depth {
            continue;
        }
        let Some(r) = g.get_revision(rid) else { continue };
        if r.branch_id != branch {
            continue;
        }
        let node_conf = if rid == start {
            node_hit_confidence(g, r, branch, now_ms, half_life_ms)
        } else {
            path_confidence(&path_edge_confs, min_src)
        };
        if rid != start && node_conf < min_path {
            pruned += 1;
            continue;
        }
        hits.push((rid, node_conf));

        if d == depth || is_opaque_traversal_gate(g, r) {
            continue;
        }

        for e in outbound_context_edges(g, branch, rid) {
            let ec = edge_confidence(e, now_ms, half_life_ms);
            let mut next_confs = path_edge_confs.clone();
            next_confs.push(ec);
            let next_min = min_source_on_path(min_src, e);
            let next_path = path_confidence(&next_confs, next_min);
            if next_path < min_path {
                pruned += 1;
                continue;
            }
            let Some(next_rev) = resolve_edge_target(g, eto, branch, e) else {
                pruned += 1;
                continue;
            };
            q.push_back((next_rev.revision_id, d + 1, next_confs, next_min));
        }
    }

    ExpandContextResult {
        hits,
        pruned_low_confidence_count: pruned,
    }
}

/// First definition edge (Imports/Extends/Calls) respecting ETO + tombstone bridging + file-hub imports.
pub fn resolve_definition_target<'a>(
    g: &'a InMemoryGraph,
    eto: &EdgeTargetOverrideStore,
    branch: BranchId,
    source_revision: NodeRevisionId,
) -> Option<&'a NodeRevision> {
    for e in outbound_context_edges(g, branch, source_revision) {
        if e.ty == EdgeType::Calls {
            if let Some(trev) = resolve_edge_target(g, eto, branch, e) {
                return Some(trev);
            }
        }
    }
    for e in outbound_context_edges(g, branch, source_revision) {
        if matches!(e.ty, EdgeType::Imports | EdgeType::Extends) {
            if let Some(trev) = resolve_edge_target(g, eto, branch, e) {
                return Some(trev);
            }
        }
    }
    None
}

/// Count outbound definition edges that fail resolution (for explain_context).
pub fn count_unresolved_definition_edges(
    g: &InMemoryGraph,
    eto: &EdgeTargetOverrideStore,
    branch: BranchId,
    source_revision: NodeRevisionId,
) -> usize {
    outbound_context_edges(g, branch, source_revision)
        .iter()
        .filter(|e| {
            matches!(
                e.ty,
                EdgeType::Imports | EdgeType::Extends | EdgeType::Calls
            ) && resolve_edge_target(g, eto, branch, e).is_none()
        })
        .count()
}

/// Count traversal neighbors pruned by path-confidence floor (expand_context semantics).
pub fn count_pruned_expand_neighbors(
    g: &InMemoryGraph,
    eto: &EdgeTargetOverrideStore,
    policy: &RankingPolicySnapshot,
    branch: BranchId,
    source_revision: NodeRevisionId,
    depth: u32,
    now_ms: u64,
) -> usize {
    expand_context_bfs(g, eto, policy, branch, source_revision, depth, now_ms).pruned_low_confidence_count
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::edge_target_override::EdgeTargetOverrideStore;
    use crate::graph::{
        EdgeResolution, GraphEdge, InMemoryGraph, Language, NodeIdentity, NodeRevision, SourceSpan,
    };
    use crate::identity_resolver::{IdentityResolver, RenameEvidence, RenameSignalKind};
    use crate::ranking_policy::RankingPolicy;
    use cis_wal::BranchId;

    fn branch() -> BranchId {
        BranchId([9u8; 16])
    }

    fn rev(
        g: &mut InMemoryGraph,
        id: u8,
        identity: u8,
        name: &str,
        status: RevisionStatus,
    ) -> NodeRevisionId {
        let iid = IdentityId([identity; 16]);
        let rid = NodeRevisionId([id; 16]);
        g.put_identity(NodeIdentity {
            identity_id: iid,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id: rid,
            identity_id: iid,
            branch_id: branch(),
            status,
            qualified_name: name.into(),
            file_path: "m.py".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        rid
    }

    fn call_edge(g: &mut InMemoryGraph, src: NodeRevisionId, target: IdentityId, edge_byte: u8) {
        let mut eid = [0u8; 16];
        eid[0] = edge_byte;
        let e = GraphEdge {
            edge_id: eid,
            ty: EdgeType::Calls,
            source_revision_id: src,
            target_identity_id: target,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(src, vec![e]).unwrap();
    }

    #[test]
    fn tombstone_bridges_to_active_successor() {
        let mut g = InMemoryGraph::default();
        let b = branch();
        let old_i = IdentityId([1u8; 16]);
        let new_i = IdentityId([2u8; 16]);
        let tomb = rev(&mut g, 1, 1, "foo", RevisionStatus::Tombstone);
        let _active = rev(&mut g, 2, 2, "bar", RevisionStatus::Active);
        let renamed = IdentityResolver::renamed_from_edge(
            tomb,
            new_i,
            RenameEvidence {
                kind: RenameSignalKind::AstBodySimilarity,
                confidence: 0.9,
            },
            [8u8; 16],
        );
        g.replace_edges_for_revision(tomb, vec![renamed]).unwrap();
        let tomb_rev = g.primary_revision_for_identity(b, old_i).unwrap();
        assert!(matches!(tomb_rev.status, RevisionStatus::Tombstone));
        assert_eq!(rename_successor_identity(&g, tomb_rev.revision_id), Some(new_i));
        let bridged = resolve_identity_revision(&g, b, old_i).unwrap();
        assert_eq!(bridged.qualified_name, "bar");
        assert_eq!(bridged.revision_id, NodeRevisionId([2u8; 16]));
    }

    #[test]
    fn eto_redirects_definition_target() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let caller = rev(&mut g, 10, 10, "caller", RevisionStatus::Active);
        let wrong = IdentityId([20u8; 16]);
        let right = IdentityId([30u8; 16]);
        rev(&mut g, 20, 20, "wrong", RevisionStatus::Active);
        rev(&mut g, 30, 30, "right", RevisionStatus::Active);
        let edge_id = [7u8; 16];
        let e = GraphEdge {
            edge_id,
            ty: EdgeType::Calls,
            source_revision_id: caller,
            target_identity_id: wrong,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(caller, vec![e]).unwrap();
        eto.set_override(b, caller, edge_id, right);
        let target = resolve_definition_target(&g, &eto, b, caller).unwrap();
        assert_eq!(target.qualified_name, "right");
    }

    #[test]
    fn expand_prunes_below_min_path_confidence() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let seed = rev(&mut g, 1, 1, "seed", RevisionStatus::Active);
        let leaf = rev(&mut g, 2, 2, "leaf", RevisionStatus::Active);
        call_edge(&mut g, seed, IdentityId([2u8; 16]), 1);
        let _ = leaf;
        let mut policy = RankingPolicy::default();
        policy.min_path_confidence = 0.99;
        let out = expand_context_bfs(&g, &eto, &policy, b, seed, 1, 1_000_000);
        assert_eq!(out.hits.len(), 1, "only seed should survive strict floor");
        assert!(out.pruned_low_confidence_count >= 1);
    }

    #[test]
    fn opaque_stub_halts_expansion() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let seed = rev(&mut g, 1, 1, "seed", RevisionStatus::Active);
        let stub_i = IdentityId([5u8; 16]);
        let stub_rid = NodeRevisionId([5u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: stub_i,
            kind: NodeKind::Stub,
        });
        g.put_revision(NodeRevision {
            revision_id: stub_rid,
            identity_id: stub_i,
            branch_id: b,
            status: RevisionStatus::Active,
            qualified_name: "pkg.stub".into(),
            file_path: "".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Unknown,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let beyond = rev(&mut g, 6, 6, "beyond", RevisionStatus::Active);
        call_edge(&mut g, seed, stub_i, 2);
        call_edge(&mut g, stub_rid, IdentityId([6u8; 16]), 3);
        let _ = beyond;
        let policy = RankingPolicy::default();
        let out = expand_context_bfs(&g, &eto, &policy, b, seed, 2, 1_000_000);
        let ids: HashSet<_> = out.hits.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&seed));
        assert!(ids.contains(&stub_rid));
        assert!(!ids.contains(&NodeRevisionId([6u8; 16])));
    }
}
