//! **Phase 5 / FR-2.5** — confidence-aware graph traversal, ETO resolution, tombstone bridging.

use std::collections::{HashSet, VecDeque};
use std::rc::Rc;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::confidence::{edge_confidence, node_confidence_from_inbound, path_confidence, path_floor};
use crate::deletion_absence::DeletionAbsenceStore;
use crate::edge_target_override::EdgeTargetOverrideStore;
use crate::graph::{
    EdgeType, GraphEdge, NodeKind, NodeRevision, RevisionStatus, SourceType,
};
use crate::graph_view::GraphView;
use crate::index_model::stable_rev_id_bytes;
use crate::ranking_policy::RankingPolicySnapshot;

fn is_context_edge(ty: EdgeType) -> bool {
    // Context expansion includes Calls and Uses. Go-to-definition does not:
    // those edges leave a symbol that is already the definition.
    matches!(
        ty,
        EdgeType::Calls | EdgeType::Imports | EdgeType::Uses | EdgeType::Extends
    )
}

/// Edges that mean "the definition of this revision is elsewhere."
/// Calls and Uses are dependencies of the defining revision.
pub(crate) fn is_definition_edge(ty: EdgeType) -> bool {
    matches!(ty, EdgeType::Imports | EdgeType::Extends)
}

fn revision_on_chain(chain: &[BranchId], branch_id: BranchId) -> bool {
    chain.iter().any(|b| *b == branch_id)
}

/// File hub revision (`$file`) for import edges on a path.
///
/// Probes each branch in the ancestry chain because hub revision ids are
/// derived from `branch` via [`stable_rev_id_bytes`].
pub fn file_hub_revision_for_path(
    g: &impl GraphView,
    chain: &[BranchId],
    file_path: &str,
) -> Option<NodeRevisionId> {
    for &branch in chain {
        let rid = NodeRevisionId(stable_rev_id_bytes(branch, file_path, "$file"));
        if let Some(r) = g.get_revision(rid) {
            if revision_on_chain(chain, r.branch_id) {
                return Some(rid);
            }
        }
    }
    None
}

/// Outbound Calls/Imports/Uses/Extends on `source` only (no file-hub import hop).
pub fn outbound_local_context_edges(
    g: &impl GraphView,
    source_revision: NodeRevisionId,
) -> Vec<GraphEdge> {
    g.outbound_edges(source_revision)
        .into_iter()
        .filter(|e| is_context_edge(e.ty))
        .collect()
}

/// Outbound edges for definition resolution.
///
/// Imports live on the `$file` hub. They are definitions of that hub only —
/// inheriting them onto every symbol made `go_to_definition` prefer a poisoned
/// file-hub `Imports` edge over the symbol's own `Uses` edge.
/// `file_imports` reads the hub directly.
pub fn outbound_context_edges(
    g: &impl GraphView,
    _chain: &[BranchId],
    source_revision: NodeRevisionId,
) -> Vec<GraphEdge> {
    outbound_local_context_edges(g, source_revision)
}

/// **FR-2.5 / §01.3** — `Stub` nodes are traversal boundaries (OPAQUE gate).
pub fn is_opaque_traversal_gate(g: &impl GraphView, rev: &NodeRevision) -> bool {
    g.identity_kind(rev.identity_id) == Some(NodeKind::Stub)
}

/// Follow **`RENAMED_FROM`** on a tombstone revision to the successor identity.
pub fn rename_successor_identity(g: &impl GraphView, tombstone_revision: NodeRevisionId) -> Option<IdentityId> {
    for e in g.outbound_edges(tombstone_revision) {
        if e.ty == EdgeType::RenamedFrom {
            return Some(e.target_identity_id);
        }
    }
    None
}

/// Resolve an identity to the best query target revision, bridging tombstones via **`RENAMED_FROM`**.
///
/// `chain` is nearest-first (`[feature, parent, …]`). A tombstone on a nearer branch hides
/// parent revisions for that identity until a successor is bridged.
///
/// **Active** and **Speculative** revisions are both queryable (speculative covers in-flight
/// `write_file` / patch edits on the editing branch).
///
/// Prefer [`resolve_identity_revision_with_absence`] for interactive queries so durable
/// `deleted:` markers survive tombstone GC.
pub fn resolve_identity_revision(
    g: &impl GraphView,
    chain: &[BranchId],
    identity_id: IdentityId,
) -> Option<NodeRevision> {
    resolve_identity_revision_with_absence(g, chain, identity_id, None)
}

/// Like [`resolve_identity_revision`], but honors durable deletion absence markers.
///
/// Nearest-first walk over `chain` (`[child, parent, …]`). Soft-deleted Active revisions
/// (absence marker, status still live) and tombstones are skipped when the deletion was
/// planted **after** the querying branch forked (temporal COW). Own-branch / pre-fork
/// deletions stop inheritance (do not fall through to an ancestor Active).
///
/// Live primaries (`Active`/`Speculative`) never include tombstones. Per branch, if only a
/// tombstone remains and it applies to this query: bridge via `RENAMED_FROM` when present,
/// otherwise treat as deleted.
pub fn resolve_identity_revision_with_absence(
    g: &impl GraphView,
    chain: &[BranchId],
    identity_id: IdentityId,
    absence: Option<&DeletionAbsenceStore>,
) -> Option<NodeRevision> {
    for &branch_id in chain {
        let honored_absent = absence
            .map(|store| {
                store.is_deleted_on_branch(branch_id, identity_id)
                    && store.should_honor_deletion_for_query(chain, branch_id, identity_id)
            })
            .unwrap_or(false);

        if let Some(primary) = g.primary_revision_for_identity(branch_id, identity_id) {
            if matches!(
                primary.status,
                RevisionStatus::Active | RevisionStatus::Speculative
            ) {
                if honored_absent {
                    // Soft-deleted for this query — stop; do not inherit parent.
                    return None;
                }
                return Some(primary);
            }
        }

        // Absence without a live primary (e.g. tombstone already GC'd).
        if honored_absent {
            return None;
        }

        if let Some(tomb) = g.tombstone_revision_for_identity(branch_id, identity_id) {
            if let Some(store) = absence {
                // Post-fork ancestor tombstone: skip and keep walking toward older parents.
                if store.is_deleted_on_branch(branch_id, identity_id)
                    && !store.should_honor_deletion_for_query(chain, branch_id, identity_id)
                {
                    continue;
                }
            }
            if let Some(successor) = rename_successor_identity(g, tomb.revision_id) {
                if let Some(store) = absence {
                    if store.is_deleted_for_query(chain, successor) {
                        return None;
                    }
                }
                return g
                    .primary_revision_for_identity_in_chain(chain, successor)
                    .filter(|r| {
                        matches!(
                            r.status,
                            RevisionStatus::Active | RevisionStatus::Speculative
                        )
                    });
            }
            // Pure deletion tombstone on this branch: stop inheritance.
            return None;
        }
    }
    None
}

/// Effective target identity (ETO) then revision resolution with tombstone bridging.
///
/// ETO overrides are resolved nearest-first across `chain` so a parent-branch override
/// on an inherited edge is visible unless the child overrides it.
pub fn resolve_edge_target(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    edge: &GraphEdge,
) -> Option<NodeRevision> {
    resolve_edge_target_with_absence(g, eto, chain, edge, None)
}

/// Like [`resolve_edge_target`] with deletion absence.
pub fn resolve_edge_target_with_absence(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    edge: &GraphEdge,
    absence: Option<&DeletionAbsenceStore>,
) -> Option<NodeRevision> {
    if chain.is_empty() {
        return None;
    }
    let target_id = eto.effective_target_identity_in_chain(chain, edge);
    resolve_identity_revision_with_absence(g, chain, target_id, absence)
}

fn min_source_on_path(current: SourceType, edge: &GraphEdge) -> SourceType {
    let s = edge.resolution.resolver;
    if path_floor(s) < path_floor(current) {
        s
    } else {
        current
    }
}

/// Walk inbound edges whose **effective** target (after ETO) is `identity_id`.
///
/// Two paths:
/// 1. [`GraphView::inbound_edges_to`] — canonical reverse index, then
///    resolve each source and filter by effective target (drops edges retargeted away).
/// 2. [`EdgeTargetOverrideStore::overrides_targeting`] — ETO rows that retarget an edge
///    *to* `identity_id` from a different canonical target (not in `target_reverse`).
///
/// Dedupes by `edge_id`. `edge_filter` selects edge types (e.g. `Calls` only).
/// `visit` returns `false` to stop early (e.g. hit limit).
pub fn for_each_inbound_edge<FFilter, FVisit>(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    identity_id: IdentityId,
    absence: Option<&DeletionAbsenceStore>,
    edge_filter: FFilter,
    mut visit: FVisit,
) where
    FFilter: Fn(&GraphEdge) -> bool,
    FVisit: FnMut(&NodeRevision, &GraphEdge) -> bool,
{
    let mut seen_edges: HashSet<[u8; 16]> = HashSet::new();
    let mut seen_idents: HashSet<IdentityId> = HashSet::new();

    for (src_rev, _) in g.inbound_edges_to(identity_id, None) {
        seen_idents.insert(src_rev.identity_id);
    }
    for sid in seen_idents {
        let Some(src) = resolve_identity_revision_with_absence(g, chain, sid, absence) else {
            continue;
        };
        let edges = g.outbound_edges(src.revision_id);
        for e in &edges {
            if !edge_filter(e) {
                continue;
            }
            if eto.effective_target_identity_in_chain(chain, e) != identity_id {
                continue;
            }
            if !seen_edges.insert(e.edge_id) {
                continue;
            }
            if !visit(&src, e) {
                return;
            }
        }
    }

    // ETO may retarget an edge whose raw target is not `identity_id`.
    for (eto_branch, source_rev, edge_id) in eto.overrides_targeting(identity_id) {
        if !chain.iter().any(|b| *b == eto_branch) {
            continue;
        }
        let Some(src) = g.get_revision(source_rev) else {
            continue;
        };
        if !revision_on_chain(chain, src.branch_id) {
            continue;
        }
        if let Some(store) = absence {
            if store.is_deleted_for_query(chain, src.identity_id) {
                continue;
            }
        }
        let edges = g.outbound_edges(source_rev);
        for e in &edges {
            if e.edge_id != edge_id {
                continue;
            }
            if !edge_filter(e) {
                continue;
            }
            if eto.effective_target_identity_in_chain(chain, e) != identity_id {
                continue;
            }
            if !seen_edges.insert(e.edge_id) {
                continue;
            }
            if !visit(&src, e) {
                return;
            }
        }
    }
}

fn inbound_edge_confidences(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    identity_id: IdentityId,
    now_ms: u64,
    half_life_ms: u64,
    absence: Option<&DeletionAbsenceStore>,
) -> Vec<f64> {
    let mut confs = Vec::new();
    for_each_inbound_edge(g, eto, chain, identity_id, absence, |_| true, |_src, e| {
        confs.push(edge_confidence(e, now_ms, half_life_ms));
        true
    });
    confs
}

/// Node confidence for MCP hits (**§01.1**).
pub fn node_hit_confidence(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    rev: &NodeRevision,
    chain: &[BranchId],
    now_ms: u64,
    half_life_ms: u64,
) -> f64 {
    node_hit_confidence_with_absence(g, eto, rev, chain, now_ms, half_life_ms, None)
}

/// Like [`node_hit_confidence`] with deletion absence for inbound source resolution.
pub fn node_hit_confidence_with_absence(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    rev: &NodeRevision,
    chain: &[BranchId],
    now_ms: u64,
    half_life_ms: u64,
    absence: Option<&DeletionAbsenceStore>,
) -> f64 {
    let inbound =
        inbound_edge_confidences(g, eto, chain, rev.identity_id, now_ms, half_life_ms, absence);
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
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    policy: &RankingPolicySnapshot,
    chain: &[BranchId],
    start: NodeRevisionId,
    depth: u32,
    now_ms: u64,
) -> ExpandContextResult {
    expand_context_bfs_with_absence(g, eto, policy, chain, start, depth, now_ms, None)
}

/// Like [`expand_context_bfs`] with deletion absence.
pub fn expand_context_bfs_with_absence(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    policy: &RankingPolicySnapshot,
    chain: &[BranchId],
    start: NodeRevisionId,
    depth: u32,
    now_ms: u64,
    absence: Option<&DeletionAbsenceStore>,
) -> ExpandContextResult {
    let half_life_ms = policy.recency.half_life_days as u64 * 24 * 60 * 60 * 1000;
    let min_path = policy.min_path_confidence;
    let mut seen_rev: HashSet<NodeRevisionId> = HashSet::new();
    // Path confidence stored as Rc<[f64]> so siblings share the parent prefix cheaply
    // until an edge is accepted (then a new Rc is allocated).
    let mut q: VecDeque<(NodeRevisionId, u32, Rc<[f64]>, SourceType)> = VecDeque::new();
    q.push_back((start, 0, Rc::from([]), SourceType::Ast));
    let mut hits = Vec::new();
    let mut pruned = 0usize;

    while let Some((rid, d, path_edge_confs, min_src)) = q.pop_front() {
        if !seen_rev.insert(rid) || d > depth {
            continue;
        }
        let Some(r) = g.get_revision(rid) else { continue };
        if !revision_on_chain(chain, r.branch_id) {
            continue;
        }
        let node_conf = if rid == start {
            node_hit_confidence_with_absence(g, eto, &r, chain, now_ms, half_life_ms, absence)
        } else {
            path_confidence(&path_edge_confs, min_src)
        };
        if rid != start && node_conf < min_path {
            pruned += 1;
            continue;
        }
        hits.push((rid, node_conf));

        if d == depth || is_opaque_traversal_gate(g, &r) {
            continue;
        }

        for e in outbound_local_context_edges(g, rid) {
            let ec = edge_confidence(&e, now_ms, half_life_ms);
            let next_min = min_source_on_path(min_src, &e);
            // Build next path only after prune check would need the scores —
            // allocate once into a buffer, score, then Rc-wrap if accepted.
            let mut next_buf = Vec::with_capacity(path_edge_confs.len() + 1);
            next_buf.extend_from_slice(&path_edge_confs);
            next_buf.push(ec);
            let next_path = path_confidence(&next_buf, next_min);
            if next_path < min_path {
                pruned += 1;
                continue;
            }
            let Some(next_rev) = resolve_edge_target_with_absence(g, eto, chain, &e, absence) else {
                pruned += 1;
                continue;
            };
            q.push_back((next_rev.revision_id, d + 1, Rc::from(next_buf), next_min));
        }
    }

    ExpandContextResult {
        hits,
        pruned_low_confidence_count: pruned,
    }
}

/// First Extends or Imports edge on this revision, respecting ETO and tombstone bridging.
pub fn resolve_definition_target(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    source_revision: NodeRevisionId,
) -> Option<NodeRevision> {
    resolve_definition_target_with_absence(g, eto, chain, source_revision, None)
}

/// Like [`resolve_definition_target`] with deletion absence.
///
/// Priority: Extends, then Imports, on this revision only.
/// Calls and Uses stay on `get_dependencies` / `get_callers`. Following them
/// from `find_symbol` opens a callee or a field type instead of the symbol
/// the caller already resolved. A `File` target is used only when no symbol
/// target resolves.
pub fn resolve_definition_target_with_absence(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    source_revision: NodeRevisionId,
    absence: Option<&DeletionAbsenceStore>,
) -> Option<NodeRevision> {
    let edges = outbound_context_edges(g, chain, source_revision);
    let mut file_fallback = None;
    let tiers: [fn(&GraphEdge) -> bool; 2] = [
        |e| e.ty == EdgeType::Extends,
        |e| e.ty == EdgeType::Imports,
    ];
    for pred in tiers {
        for e in &edges {
            if !pred(e) {
                continue;
            }
            let Some(trev) = resolve_edge_target_with_absence(g, eto, chain, e, absence) else {
                continue;
            };
            if g.identity_kind(trev.identity_id) == Some(NodeKind::File) {
                if file_fallback.is_none() {
                    file_fallback = Some(trev);
                }
            } else {
                return Some(trev);
            }
        }
    }
    file_fallback
}

/// Count outbound definition edges that fail resolution (for explain_context).
pub fn count_unresolved_definition_edges(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    chain: &[BranchId],
    source_revision: NodeRevisionId,
) -> usize {
    outbound_context_edges(g, chain, source_revision)
        .iter()
        .filter(|e| {
            is_definition_edge(e.ty) && resolve_edge_target(g, eto, chain, e).is_none()
        })
        .count()
}

/// Count traversal neighbors pruned by path-confidence floor (expand_context semantics).
pub fn count_pruned_expand_neighbors(
    g: &impl GraphView,
    eto: &EdgeTargetOverrideStore,
    policy: &RankingPolicySnapshot,
    chain: &[BranchId],
    source_revision: NodeRevisionId,
    depth: u32,
    now_ms: u64,
) -> usize {
    expand_context_bfs(g, eto, policy, chain, source_revision, depth, now_ms)
        .pruned_low_confidence_count
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
        assert!(
            g.primary_revision_for_identity(b, old_i).is_none(),
            "tombstones are never bound as primary"
        );
        let tomb_rev = g.tombstone_revision_for_identity(b, old_i).unwrap();
        assert!(matches!(tomb_rev.status, RevisionStatus::Tombstone));
        assert_eq!(rename_successor_identity(&g, tomb_rev.revision_id), Some(new_i));
        let chain = [b];
        let bridged = resolve_identity_revision(&g, &chain, old_i).unwrap();
        assert_eq!(bridged.qualified_name, "bar");
        assert_eq!(bridged.revision_id, NodeRevisionId([2u8; 16]));
    }

    #[test]
    fn eto_redirects_definition_target() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let caller = rev(&mut g, 10, 10, "caller", RevisionStatus::Active);
        let wrong = IdentityId([20u8; 16]);
        let right = IdentityId([30u8; 16]);
        rev(&mut g, 20, 20, "wrong", RevisionStatus::Active);
        rev(&mut g, 30, 30, "right", RevisionStatus::Active);
        let edge_id = [7u8; 16];
        let e = GraphEdge {
            edge_id,
            ty: EdgeType::Extends,
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
        let target = resolve_definition_target(&g, &eto, &chain, caller).unwrap();
        assert_eq!(target.qualified_name, "right");
    }

    #[test]
    fn eto_parent_override_visible_on_feature_chain() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let caller_i = IdentityId([10u8; 16]);
        let wrong = IdentityId([20u8; 16]);
        let right = IdentityId([30u8; 16]);
        let caller = NodeRevisionId([10u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: caller_i,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id: caller,
            identity_id: caller_i,
            branch_id: main,
            status: RevisionStatus::Active,
            qualified_name: "caller".into(),
            file_path: "m.py".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        for (id, name) in [(20u8, "wrong"), (30u8, "right")] {
            let iid = IdentityId([id; 16]);
            g.put_identity(NodeIdentity {
                identity_id: iid,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: NodeRevisionId([id; 16]),
                identity_id: iid,
                branch_id: main,
                status: RevisionStatus::Active,
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
        }
        let edge_id = [7u8; 16];
        let e = GraphEdge {
            edge_id,
            ty: EdgeType::Extends,
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
        eto.set_override(main, caller, edge_id, right);
        let chain = [feature, main];
        let target = resolve_definition_target(&g, &eto, &chain, caller).unwrap();
        assert_eq!(target.qualified_name, "right");
    }

    #[test]
    fn expand_prunes_below_min_path_confidence() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let seed = rev(&mut g, 1, 1, "seed", RevisionStatus::Active);
        let leaf = rev(&mut g, 2, 2, "leaf", RevisionStatus::Active);
        call_edge(&mut g, seed, IdentityId([2u8; 16]), 1);
        let _ = leaf;
        let mut policy = RankingPolicy::default();
        policy.min_path_confidence = 0.99;
        let out = expand_context_bfs(&g, &eto, &policy, &chain, seed, 1, 1_000_000);
        assert_eq!(out.hits.len(), 1, "only seed should survive strict floor");
        assert!(out.pruned_low_confidence_count >= 1);
    }

    #[test]
    fn opaque_stub_halts_expansion() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
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
        let out = expand_context_bfs(&g, &eto, &policy, &chain, seed, 2, 1_000_000);
        let ids: HashSet<_> = out.hits.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&seed));
        assert!(ids.contains(&stub_rid));
        assert!(!ids.contains(&NodeRevisionId([6u8; 16])));
    }

    #[test]
    fn expand_skips_parent_file_hub_imports() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let file_path = "m.py";
        let hub_iid = IdentityId([10u8; 16]);
        let hub_rid = NodeRevisionId(crate::index_model::stable_rev_id_bytes(
            b, file_path, "$file",
        ));
        g.put_identity(NodeIdentity {
            identity_id: hub_iid,
            kind: NodeKind::File,
        });
        g.put_revision(NodeRevision {
            revision_id: hub_rid,
            identity_id: hub_iid,
            branch_id: b,
            status: RevisionStatus::Active,
            qualified_name: file_path.into(),
            file_path: file_path.into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let dep = rev(&mut g, 8, 8, "pkg.dep", RevisionStatus::Active);
        let import = GraphEdge {
            edge_id: [40u8; 16],
            ty: EdgeType::Imports,
            source_revision_id: hub_rid,
            target_identity_id: IdentityId([8u8; 16]),
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan {
                start_line: 1,
                start_col: 1,
                end_line: 1,
                end_col: 20,
            },
        };
        g.replace_edges_for_revision(hub_rid, vec![import]).unwrap();
        let seed = rev(&mut g, 1, 1, "foo", RevisionStatus::Active);
        let leaf = rev(&mut g, 2, 2, "leaf", RevisionStatus::Active);
        call_edge(&mut g, seed, IdentityId([2u8; 16]), 1);
        let _ = (dep, leaf);

        let hopped = outbound_context_edges(&g, &chain, seed);
        assert!(
            hopped.iter().all(|e| e.ty != EdgeType::Imports),
            "definition resolution must not inherit parent-file Imports: {:?}",
            hopped.iter().map(|e| e.ty).collect::<Vec<_>>()
        );
        let local = outbound_local_context_edges(&g, seed);
        assert!(
            local.iter().all(|e| e.ty != EdgeType::Imports),
            "local edges must not include parent-file Imports: {:?}",
            local.iter().map(|e| e.ty).collect::<Vec<_>>()
        );

        let policy = RankingPolicy::default();
        let out = expand_context_bfs(&g, &eto, &policy, &chain, seed, 1, 1_000_000);
        let ids: HashSet<_> = out.hits.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&seed));
        assert!(ids.contains(&NodeRevisionId([2u8; 16])));
        assert!(
            !ids.contains(&NodeRevisionId([8u8; 16])),
            "expand_context must not dump parent-file import targets"
        );
    }

    #[test]
    fn definition_uses_local_type_not_poisoned_file_hub_import() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let file_path = "cis-core/src/coordinator.rs";
        let symbol = rev(&mut g, 1, 1, "CoordinatorError", RevisionStatus::Active);
        // `rev` stamps m.py; retarget the symbol onto the file that owns the hub.
        let mut symbol_rev = g.get_revision(symbol).unwrap().clone();
        symbol_rev.file_path = file_path.into();
        g.put_revision(symbol_rev);
        let type_iid = IdentityId([2u8; 16]);
        let _ty = rev(&mut g, 2, 2, "graph.rs::RevisionStatus", RevisionStatus::Active);
        let hub_iid = IdentityId([10u8; 16]);
        let hub_rid = NodeRevisionId(crate::index_model::stable_rev_id_bytes(
            b, file_path, "$file",
        ));
        g.put_identity(NodeIdentity {
            identity_id: hub_iid,
            kind: NodeKind::File,
        });
        g.put_revision(NodeRevision {
            revision_id: hub_rid,
            identity_id: hub_iid,
            branch_id: b,
            status: RevisionStatus::Active,
            qualified_name: file_path.into(),
            file_path: file_path.into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Rust,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let poisoned = IdentityId([8u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: poisoned,
            kind: NodeKind::File,
        });
        g.put_revision(NodeRevision {
            revision_id: NodeRevisionId([8u8; 16]),
            identity_id: poisoned,
            branch_id: b,
            status: RevisionStatus::Active,
            qualified_name: "cis-core/src/indexer_eval/graph.rs".into(),
            file_path: "cis-core/src/indexer_eval/graph.rs".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Rust,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let resolution = EdgeResolution {
            target_signature_hash: [0u8; 32],
            resolver: SourceType::Ast,
            last_validation_ms: 0,
        };
        g.replace_edges_for_revision(
            hub_rid,
            vec![GraphEdge {
                edge_id: [40u8; 16],
                ty: EdgeType::Imports,
                source_revision_id: hub_rid,
                target_identity_id: poisoned,
                resolution: resolution.clone(),
                anchor: SourceSpan::UNKNOWN,
            }],
        )
        .unwrap();
        g.replace_edges_for_revision(
            symbol,
            vec![GraphEdge {
                edge_id: [41u8; 16],
                ty: EdgeType::Uses,
                source_revision_id: symbol,
                target_identity_id: type_iid,
                resolution,
                anchor: SourceSpan::UNKNOWN,
            }],
        )
        .unwrap();
        assert!(
            resolve_definition_target(&g, &eto, &chain, symbol).is_none(),
            "Uses on the defining revision is a dependency, not a definition hop"
        );

        let bare = rev(&mut g, 3, 3, "Bare", RevisionStatus::Active);
        let mut bare_rev = g.get_revision(bare).unwrap().clone();
        bare_rev.file_path = file_path.into();
        g.put_revision(bare_rev);
        assert!(
            resolve_definition_target(&g, &eto, &chain, bare).is_none(),
            "a symbol with no local edges must not inherit the file hub's import"
        );
    }

    #[test]
    fn definition_file_import_does_not_follow_uses() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let symbol = rev(&mut g, 1, 1, "CoordinatorError", RevisionStatus::Active);
        let type_iid = IdentityId([2u8; 16]);
        let _ty = rev(&mut g, 2, 2, "graph.rs::RevisionStatus", RevisionStatus::Active);
        let file_iid = IdentityId([8u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: file_iid,
            kind: NodeKind::File,
        });
        g.put_revision(NodeRevision {
            revision_id: NodeRevisionId([8u8; 16]),
            identity_id: file_iid,
            branch_id: b,
            status: RevisionStatus::Active,
            qualified_name: "wrong.rs".into(),
            file_path: "wrong.rs".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Rust,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let resolution = EdgeResolution {
            target_signature_hash: [0u8; 32],
            resolver: SourceType::Ast,
            last_validation_ms: 0,
        };
        g.replace_edges_for_revision(
            symbol,
            vec![
                GraphEdge {
                    edge_id: [1u8; 16],
                    ty: EdgeType::Imports,
                    source_revision_id: symbol,
                    target_identity_id: file_iid,
                    resolution: resolution.clone(),
                    anchor: SourceSpan::UNKNOWN,
                },
                GraphEdge {
                    edge_id: [2u8; 16],
                    ty: EdgeType::Uses,
                    source_revision_id: symbol,
                    target_identity_id: type_iid,
                    resolution,
                    anchor: SourceSpan::UNKNOWN,
                },
            ],
        )
        .unwrap();
        let target = resolve_definition_target(&g, &eto, &chain, symbol).unwrap();
        assert_eq!(
            target.qualified_name, "wrong.rs",
            "Uses does not outrank an Import; a File import stays the fallback"
        );
        assert_ne!(target.identity_id, type_iid);
    }

    #[test]
    fn definition_follows_extends_not_calls() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let symbol = rev(&mut g, 1, 1, "Flask", RevisionStatus::Active);
        let base = IdentityId([2u8; 16]);
        let _base_rev = rev(&mut g, 2, 2, "sansio/app.py::App", RevisionStatus::Active);
        let callee = IdentityId([3u8; 16]);
        let _callee_rev = rev(&mut g, 3, 3, "ctx.py::_AppCtxGlobals.get", RevisionStatus::Active);
        let resolution = EdgeResolution {
            target_signature_hash: [0u8; 32],
            resolver: SourceType::Ast,
            last_validation_ms: 0,
        };
        g.replace_edges_for_revision(
            symbol,
            vec![
                GraphEdge {
                    edge_id: [1u8; 16],
                    ty: EdgeType::Calls,
                    source_revision_id: symbol,
                    target_identity_id: callee,
                    resolution: resolution.clone(),
                    anchor: SourceSpan::UNKNOWN,
                },
                GraphEdge {
                    edge_id: [2u8; 16],
                    ty: EdgeType::Extends,
                    source_revision_id: symbol,
                    target_identity_id: base,
                    resolution,
                    anchor: SourceSpan::UNKNOWN,
                },
            ],
        )
        .unwrap();
        let target = resolve_definition_target(&g, &eto, &chain, symbol).unwrap();
        assert_eq!(target.qualified_name, "sansio/app.py::App");
    }

    #[test]
    fn primary_in_chain_nearest_wins() {
        let mut g = InMemoryGraph::default();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([7u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: iid,
            kind: NodeKind::Function,
        });
        let main_rid = NodeRevisionId([71u8; 16]);
        g.put_revision(NodeRevision {
            revision_id: main_rid,
            identity_id: iid,
            branch_id: main,
            status: RevisionStatus::Active,
            qualified_name: "alpha".into(),
            file_path: "a.py".into(),
            body_hash: [1u8; 32],
            signature_hash: [1u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let feat_rid = NodeRevisionId([72u8; 16]);
        g.put_revision(NodeRevision {
            revision_id: feat_rid,
            identity_id: iid,
            branch_id: feature,
            status: RevisionStatus::Active,
            qualified_name: "alpha".into(),
            file_path: "a.py".into(),
            body_hash: [2u8; 32],
            signature_hash: [2u8; 32],
            language: Language::Python,
            parent_revision_id: Some(main_rid),
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let chain = [feature, main];
        let resolved = resolve_identity_revision(&g, &chain, iid).unwrap();
        assert_eq!(resolved.revision_id, feat_rid);
        assert_eq!(
            g.primary_revision_for_identity_in_chain(&chain, iid)
                .unwrap()
                .revision_id,
            feat_rid
        );
    }

    #[test]
    fn chain_inherits_parent_when_child_has_no_primary() {
        let mut g = InMemoryGraph::default();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([8u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: iid,
            kind: NodeKind::Function,
        });
        let main_rid = NodeRevisionId([81u8; 16]);
        g.put_revision(NodeRevision {
            revision_id: main_rid,
            identity_id: iid,
            branch_id: main,
            status: RevisionStatus::Active,
            qualified_name: "beta".into(),
            file_path: "b.py".into(),
            body_hash: [3u8; 32],
            signature_hash: [3u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let chain = [feature, main];
        let resolved = resolve_identity_revision(&g, &chain, iid).unwrap();
        assert_eq!(resolved.revision_id, main_rid);
        assert_eq!(resolved.branch_id, main);
    }

    #[test]
    fn absence_hides_parent_even_without_local_tombstone() {
        use crate::deletion_absence::DeletionAbsenceStore;
        use crate::MemoryKv;

        let mut g = InMemoryGraph::default();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([42u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: iid,
            kind: NodeKind::Function,
        });
        let main_rid = NodeRevisionId([81u8; 16]);
        g.put_revision(NodeRevision {
            revision_id: main_rid,
            identity_id: iid,
            branch_id: main,
            status: RevisionStatus::Active,
            qualified_name: "gone".into(),
            file_path: "t.py".into(),
            body_hash: [3u8; 32],
            signature_hash: [3u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        // No feature revision at all (tombstone already GC'd) — only the absence marker.
        let kv = Arc::new(MemoryKv::new());
        let absence = DeletionAbsenceStore::new(kv);
        absence.mark_deleted(feature, iid);
        let chain = [feature, main];
        assert!(
            resolve_identity_revision_with_absence(&g, &chain, iid, Some(&absence)).is_none(),
            "absence must hide parent Active after overlay GC"
        );
        // Without absence, parent would be visible:
        assert_eq!(
            resolve_identity_revision(&g, &chain, iid)
                .unwrap()
                .revision_id,
            main_rid
        );
        // Parent branch alone still sees it:
        assert!(
            resolve_identity_revision_with_absence(&g, &[main], iid, Some(&absence)).is_some()
        );
    }

    #[test]
    fn clearing_absence_restores_inheritance() {
        use crate::deletion_absence::DeletionAbsenceStore;
        use crate::MemoryKv;

        let mut g = InMemoryGraph::default();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([43u8; 16]);
        g.put_identity(NodeIdentity {
            identity_id: iid,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id: NodeRevisionId([83u8; 16]),
            identity_id: iid,
            branch_id: main,
            status: RevisionStatus::Active,
            qualified_name: "restored".into(),
            file_path: "r.py".into(),
            body_hash: [4u8; 32],
            signature_hash: [4u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let kv = Arc::new(MemoryKv::new());
        let absence = DeletionAbsenceStore::new(kv);
        absence.mark_deleted(feature, iid);
        let chain = [feature, main];
        assert!(resolve_identity_revision_with_absence(&g, &chain, iid, Some(&absence)).is_none());
        absence.clear_deleted(feature, iid);
        assert!(resolve_identity_revision_with_absence(&g, &chain, iid, Some(&absence)).is_some());
    }

    #[test]
    fn inbound_walk_finds_canonical_and_eto_retarget() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(Arc::clone(&kv));
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let target = IdentityId([50u8; 16]);
        let wrong = IdentityId([51u8; 16]);
        rev(&mut g, 50, 50, "target", RevisionStatus::Active);
        rev(&mut g, 51, 51, "wrong", RevisionStatus::Active);
        let canonical_caller = rev(&mut g, 60, 60, "canonical_caller", RevisionStatus::Active);
        let eto_caller = rev(&mut g, 61, 61, "eto_caller", RevisionStatus::Active);
        call_edge(&mut g, canonical_caller, target, 1);
        let edge_id = {
            let mut eid = [0u8; 16];
            eid[0] = 2;
            eid
        };
        let e = GraphEdge {
            edge_id,
            ty: EdgeType::Calls,
            source_revision_id: eto_caller,
            target_identity_id: wrong,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(eto_caller, vec![e]).unwrap();
        eto.set_override(b, eto_caller, edge_id, target);

        let mut names = Vec::new();
        for_each_inbound_edge(
            &g,
            &eto,
            &chain,
            target,
            None,
            |e| e.ty == EdgeType::Calls,
            |src, _e| {
                names.push(src.qualified_name.clone());
                true
            },
        );
        names.sort();
        assert_eq!(
            names,
            vec!["canonical_caller".to_string(), "eto_caller".to_string()]
        );
    }

    #[test]
    fn inbound_walk_skips_eto_retargeted_away() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let target = IdentityId([70u8; 16]);
        let other = IdentityId([71u8; 16]);
        rev(&mut g, 70, 70, "target", RevisionStatus::Active);
        rev(&mut g, 71, 71, "other", RevisionStatus::Active);
        let caller = rev(&mut g, 72, 72, "caller", RevisionStatus::Active);
        let edge_id = {
            let mut eid = [0u8; 16];
            eid[0] = 9;
            eid
        };
        let e = GraphEdge {
            edge_id,
            ty: EdgeType::Calls,
            source_revision_id: caller,
            target_identity_id: target,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(caller, vec![e]).unwrap();
        eto.set_override(b, caller, edge_id, other);

        let mut count = 0usize;
        for_each_inbound_edge(
            &g,
            &eto,
            &chain,
            target,
            None,
            |e| e.ty == EdgeType::Calls,
            |_src, _e| {
                count += 1;
                true
            },
        );
        assert_eq!(count, 0, "ETO retarget away must hide caller from target");
    }

    #[test]
    fn inbound_walk_calls_filter_excludes_imports() {
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let eto = EdgeTargetOverrideStore::new(kv);
        let mut g = InMemoryGraph::default();
        let b = branch();
        let chain = [b];
        let target = IdentityId([80u8; 16]);
        rev(&mut g, 80, 80, "target", RevisionStatus::Active);
        let importer = rev(&mut g, 81, 81, "importer", RevisionStatus::Active);
        let e = GraphEdge {
            edge_id: [3u8; 16],
            ty: EdgeType::Imports,
            source_revision_id: importer,
            target_identity_id: target,
            resolution: EdgeResolution {
                target_signature_hash: [0u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(importer, vec![e]).unwrap();

        let mut calls_only = 0usize;
        for_each_inbound_edge(
            &g,
            &eto,
            &chain,
            target,
            None,
            |e| e.ty == EdgeType::Calls,
            |_src, _e| {
                calls_only += 1;
                true
            },
        );
        assert_eq!(calls_only, 0);

        let mut any = 0usize;
        for_each_inbound_edge(
            &g,
            &eto,
            &chain,
            target,
            None,
            |_| true,
            |_src, _e| {
                any += 1;
                true
            },
        );
        assert_eq!(any, 1);
    }
}
