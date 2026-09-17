//! Owned graph query/write surface (**Phase 3–4**).
//!
//! [`InMemoryGraph`] still returns borrowed slices internally. SQL cannot. This trait is the
//! API ingest and MCP queries use when the coordinator is SQLite-backed (no RAM hydrate).

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::{
    EdgeType, GraphEdge, InMemoryGraph, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
};

/// Active-index counters for `index_status` without walking every revision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphIndexCounts {
    pub active_revisions: usize,
    pub active_outbound_edges: usize,
    pub indexable_files: usize,
    pub identities: usize,
    pub revisions: usize,
    pub edges: usize,
}

/// Read-only graph queries with **owned** returns.
pub trait GraphView {
    fn get_revision(&self, id: NodeRevisionId) -> Option<NodeRevision>;
    fn outbound_edges(&self, id: NodeRevisionId) -> Vec<GraphEdge>;
    fn primary_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision>;
    fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> Vec<NodeRevisionId>;
    fn identity_kind(&self, id: IdentityId) -> Option<NodeKind>;

    /// First tombstone row for `(branch, identity)`, if any.
    fn tombstone_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision>;

    /// Revisions whose `body_hash` matches (ANN / semantic search).
    fn revision_ids_for_body_hash(&self, body_hash: &[u8; 32]) -> Vec<NodeRevisionId>;

    /// Nearest-first primary across `[child, parent, …]`.
    fn primary_revision_for_identity_in_chain(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        for &branch_id in chain {
            if let Some(rev) = self.primary_revision_for_identity(branch_id, identity_id) {
                return Some(rev);
            }
        }
        None
    }

    /// Revisions on `chain` whose `qualified_name` contains `needle` (case-sensitive).
    fn find_revisions_qn_contains(
        &self,
        chain: &[BranchId],
        needle: &str,
        limit: usize,
    ) -> Vec<NodeRevision>;

    /// Edges targeting `identity`, optionally filtered by type, with their source revision.
    fn inbound_edges_to(
        &self,
        target: IdentityId,
        ty: Option<EdgeType>,
    ) -> Vec<(NodeRevision, GraphEdge)>;

    fn index_counts(&self) -> GraphIndexCounts;

    /// Distinct identities that have at least one revision on `branch`.
    fn identity_ids_on_branch(&self, branch: BranchId) -> Vec<IdentityId>;

    /// Count revisions for `identity_id` on `chain` with `status`.
    fn count_revisions_with_status(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
        status: RevisionStatus,
    ) -> usize;

    /// Revisions whose `branch_id` is in `branches`. Empty `branches` means every revision.
    fn revisions_on_branches(&self, branches: &[BranchId]) -> Vec<NodeRevision>;
}

impl<T: GraphView + ?Sized> GraphView for &T {
    fn get_revision(&self, id: NodeRevisionId) -> Option<NodeRevision> {
        (**self).get_revision(id)
    }
    fn outbound_edges(&self, id: NodeRevisionId) -> Vec<GraphEdge> {
        (**self).outbound_edges(id)
    }
    fn primary_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        (**self).primary_revision_for_identity(branch_id, identity_id)
    }
    fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> Vec<NodeRevisionId> {
        (**self).revision_ids_for_file(branch_id, file_path)
    }
    fn identity_kind(&self, id: IdentityId) -> Option<NodeKind> {
        (**self).identity_kind(id)
    }
    fn tombstone_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        (**self).tombstone_revision_for_identity(branch_id, identity_id)
    }
    fn revision_ids_for_body_hash(&self, body_hash: &[u8; 32]) -> Vec<NodeRevisionId> {
        (**self).revision_ids_for_body_hash(body_hash)
    }
    fn primary_revision_for_identity_in_chain(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        (**self).primary_revision_for_identity_in_chain(chain, identity_id)
    }
    fn find_revisions_qn_contains(
        &self,
        chain: &[BranchId],
        needle: &str,
        limit: usize,
    ) -> Vec<NodeRevision> {
        (**self).find_revisions_qn_contains(chain, needle, limit)
    }
    fn inbound_edges_to(
        &self,
        target: IdentityId,
        ty: Option<EdgeType>,
    ) -> Vec<(NodeRevision, GraphEdge)> {
        (**self).inbound_edges_to(target, ty)
    }
    fn index_counts(&self) -> GraphIndexCounts {
        (**self).index_counts()
    }
    fn identity_ids_on_branch(&self, branch: BranchId) -> Vec<IdentityId> {
        (**self).identity_ids_on_branch(branch)
    }
    fn count_revisions_with_status(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
        status: RevisionStatus,
    ) -> usize {
        (**self).count_revisions_with_status(chain, identity_id, status)
    }
    fn revisions_on_branches(&self, branches: &[BranchId]) -> Vec<NodeRevision> {
        (**self).revisions_on_branches(branches)
    }
}

/// Write surface used by ingest/merge. RAM implements it directly; SQLite ingest
/// mutates a working-set overlay then flushes via `apply_delta`.
pub trait GraphWrite: GraphView {
    fn put_identity(&mut self, id: NodeIdentity);
    fn put_revision(&mut self, rev: NodeRevision);
    fn set_revision_status(&mut self, id: NodeRevisionId, status: RevisionStatus);
    fn replace_edges_for_revision(
        &mut self,
        revision_id: NodeRevisionId,
        edges: Vec<GraphEdge>,
    ) -> Result<(), &'static str>;
}

impl GraphWrite for InMemoryGraph {
    fn put_identity(&mut self, id: NodeIdentity) {
        InMemoryGraph::put_identity(self, id);
    }
    fn put_revision(&mut self, rev: NodeRevision) {
        InMemoryGraph::put_revision(self, rev);
    }
    fn set_revision_status(&mut self, id: NodeRevisionId, status: RevisionStatus) {
        InMemoryGraph::set_revision_status(self, id, status);
    }
    fn replace_edges_for_revision(
        &mut self,
        revision_id: NodeRevisionId,
        edges: Vec<GraphEdge>,
    ) -> Result<(), &'static str> {
        InMemoryGraph::replace_edges_for_revision(self, revision_id, edges)
    }
}

/// Overlay-first view: ingest working set in RAM, committed files in SQL.
pub struct OverlayGraphView<'a> {
    pub overlay: &'a InMemoryGraph,
    pub committed: Option<&'a dyn GraphView>,
}

impl GraphView for OverlayGraphView<'_> {
    fn get_revision(&self, id: NodeRevisionId) -> Option<NodeRevision> {
        GraphView::get_revision(self.overlay, id)
            .or_else(|| self.committed.and_then(|c| c.get_revision(id)))
    }

    fn outbound_edges(&self, id: NodeRevisionId) -> Vec<GraphEdge> {
        if GraphView::get_revision(self.overlay, id).is_some() {
            return GraphView::outbound_edges(self.overlay, id);
        }
        self.committed
            .map(|c| c.outbound_edges(id))
            .unwrap_or_default()
    }

    fn primary_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        GraphView::primary_revision_for_identity(self.overlay, branch_id, identity_id).or_else(
            || {
                self.committed
                    .and_then(|c| c.primary_revision_for_identity(branch_id, identity_id))
            },
        )
    }

    fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> Vec<NodeRevisionId> {
        let ids = GraphView::revision_ids_for_file(self.overlay, branch_id, file_path);
        if !ids.is_empty() {
            return ids;
        }
        self.committed
            .map(|c| c.revision_ids_for_file(branch_id, file_path))
            .unwrap_or_default()
    }

    fn identity_kind(&self, id: IdentityId) -> Option<NodeKind> {
        GraphView::identity_kind(self.overlay, id)
            .or_else(|| self.committed.and_then(|c| c.identity_kind(id)))
    }

    fn tombstone_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        GraphView::tombstone_revision_for_identity(self.overlay, branch_id, identity_id).or_else(
            || {
                self.committed
                    .and_then(|c| c.tombstone_revision_for_identity(branch_id, identity_id))
            },
        )
    }

    fn revision_ids_for_body_hash(&self, body_hash: &[u8; 32]) -> Vec<NodeRevisionId> {
        let mut ids = GraphView::revision_ids_for_body_hash(self.overlay, body_hash);
        if let Some(c) = self.committed {
            for id in c.revision_ids_for_body_hash(body_hash) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids
    }

    fn find_revisions_qn_contains(
        &self,
        chain: &[BranchId],
        needle: &str,
        limit: usize,
    ) -> Vec<NodeRevision> {
        let mut out = GraphView::find_revisions_qn_contains(self.overlay, chain, needle, 0);
        if let Some(c) = self.committed {
            for r in c.find_revisions_qn_contains(chain, needle, 0) {
                if !out.iter().any(|x| x.revision_id == r.revision_id) {
                    out.push(r);
                }
            }
        }
        sort_revisions_qn(&mut out);
        if limit > 0 {
            out.truncate(limit);
        }
        out
    }

    fn inbound_edges_to(
        &self,
        target: IdentityId,
        ty: Option<EdgeType>,
    ) -> Vec<(NodeRevision, GraphEdge)> {
        let mut out = GraphView::inbound_edges_to(self.overlay, target, ty);
        if let Some(c) = self.committed {
            for row in c.inbound_edges_to(target, ty) {
                if !out.iter().any(|(_, e)| e.edge_id == row.1.edge_id) {
                    out.push(row);
                }
            }
        }
        sort_inbound(&mut out);
        out
    }

    fn index_counts(&self) -> GraphIndexCounts {
        let overlay = GraphView::index_counts(self.overlay);
        match self.committed {
            Some(c) => {
                let committed = c.index_counts();
                GraphIndexCounts {
                    active_revisions: overlay.active_revisions.max(committed.active_revisions),
                    active_outbound_edges: overlay
                        .active_outbound_edges
                        .max(committed.active_outbound_edges),
                    indexable_files: overlay.indexable_files.max(committed.indexable_files),
                    identities: overlay.identities.max(committed.identities),
                    revisions: overlay.revisions.max(committed.revisions),
                    edges: overlay.edges.max(committed.edges),
                }
            }
            None => overlay,
        }
    }

    fn identity_ids_on_branch(&self, branch: BranchId) -> Vec<IdentityId> {
        let mut ids = GraphView::identity_ids_on_branch(self.overlay, branch);
        if let Some(c) = self.committed {
            for id in c.identity_ids_on_branch(branch) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids
    }

    fn count_revisions_with_status(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
        status: RevisionStatus,
    ) -> usize {
        let overlay = GraphView::count_revisions_with_status(self.overlay, chain, identity_id, status);
        match self.committed {
            Some(c) => overlay.max(c.count_revisions_with_status(chain, identity_id, status)),
            None => overlay,
        }
    }

    fn revisions_on_branches(&self, branches: &[BranchId]) -> Vec<NodeRevision> {
        let mut out = GraphView::revisions_on_branches(self.overlay, branches);
        if let Some(c) = self.committed {
            for r in c.revisions_on_branches(branches) {
                if !out.iter().any(|x| x.revision_id == r.revision_id) {
                    out.push(r);
                }
            }
        }
        out.sort_by(|a, b| a.revision_id.0.cmp(&b.revision_id.0));
        out
    }
}

/// Mutable overlay over a committed [`GraphView`] (sqlite-primary merge).
///
/// Reads overlay first, then SQL. Writes copy-on-write into the overlay so merge
/// does not hydrate the full graph via [`crate::graph_store::GraphStore::load_into`].
pub struct OverlayGraphMut<'a> {
    pub overlay: &'a mut InMemoryGraph,
    pub committed: Option<&'a dyn GraphView>,
}

impl OverlayGraphMut<'_> {
    fn view(&self) -> OverlayGraphView<'_> {
        OverlayGraphView {
            overlay: self.overlay,
            committed: self.committed,
        }
    }

    fn cow_revision(&mut self, id: NodeRevisionId) {
        if GraphView::get_revision(self.overlay, id).is_some() {
            return;
        }
        let Some(committed) = self.committed else {
            return;
        };
        let Some(rev) = committed.get_revision(id) else {
            return;
        };
        if let Some(kind) = committed.identity_kind(rev.identity_id) {
            self.overlay.put_identity(NodeIdentity {
                identity_id: rev.identity_id,
                kind,
            });
        }
        let edges = committed.outbound_edges(id);
        self.overlay.put_revision(rev);
        let _ = self.overlay.replace_edges_for_revision(id, edges);
    }
}

impl GraphView for OverlayGraphMut<'_> {
    fn get_revision(&self, id: NodeRevisionId) -> Option<NodeRevision> {
        self.view().get_revision(id)
    }
    fn outbound_edges(&self, id: NodeRevisionId) -> Vec<GraphEdge> {
        self.view().outbound_edges(id)
    }
    fn primary_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        self.view()
            .primary_revision_for_identity(branch_id, identity_id)
    }
    fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> Vec<NodeRevisionId> {
        self.view().revision_ids_for_file(branch_id, file_path)
    }
    fn identity_kind(&self, id: IdentityId) -> Option<NodeKind> {
        self.view().identity_kind(id)
    }
    fn tombstone_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        self.view()
            .tombstone_revision_for_identity(branch_id, identity_id)
    }
    fn revision_ids_for_body_hash(&self, body_hash: &[u8; 32]) -> Vec<NodeRevisionId> {
        self.view().revision_ids_for_body_hash(body_hash)
    }
    fn find_revisions_qn_contains(
        &self,
        chain: &[BranchId],
        needle: &str,
        limit: usize,
    ) -> Vec<NodeRevision> {
        self.view()
            .find_revisions_qn_contains(chain, needle, limit)
    }
    fn inbound_edges_to(
        &self,
        target: IdentityId,
        ty: Option<EdgeType>,
    ) -> Vec<(NodeRevision, GraphEdge)> {
        self.view().inbound_edges_to(target, ty)
    }
    fn index_counts(&self) -> GraphIndexCounts {
        self.view().index_counts()
    }
    fn identity_ids_on_branch(&self, branch: BranchId) -> Vec<IdentityId> {
        self.view().identity_ids_on_branch(branch)
    }
    fn count_revisions_with_status(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
        status: RevisionStatus,
    ) -> usize {
        self.view()
            .count_revisions_with_status(chain, identity_id, status)
    }
    fn revisions_on_branches(&self, branches: &[BranchId]) -> Vec<NodeRevision> {
        self.view().revisions_on_branches(branches)
    }
}

impl GraphWrite for OverlayGraphMut<'_> {
    fn put_identity(&mut self, id: NodeIdentity) {
        self.overlay.put_identity(id);
    }
    fn put_revision(&mut self, rev: NodeRevision) {
        self.overlay.put_revision(rev);
    }
    fn set_revision_status(&mut self, id: NodeRevisionId, status: RevisionStatus) {
        self.cow_revision(id);
        self.overlay.set_revision_status(id, status);
    }
    fn replace_edges_for_revision(
        &mut self,
        revision_id: NodeRevisionId,
        edges: Vec<GraphEdge>,
    ) -> Result<(), &'static str> {
        self.cow_revision(revision_id);
        self.overlay.replace_edges_for_revision(revision_id, edges)
    }
}

impl GraphView for InMemoryGraph {
    fn get_revision(&self, id: NodeRevisionId) -> Option<NodeRevision> {
        InMemoryGraph::get_revision(self, id).cloned()
    }

    fn outbound_edges(&self, id: NodeRevisionId) -> Vec<GraphEdge> {
        let mut edges = InMemoryGraph::outbound_edges(self, id).to_vec();
        edges.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
        edges
    }

    fn primary_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        InMemoryGraph::primary_revision_for_identity(self, branch_id, identity_id).cloned()
    }

    fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> Vec<NodeRevisionId> {
        let mut ids = InMemoryGraph::revision_ids_for_file(self, branch_id, file_path).to_vec();
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids
    }

    fn identity_kind(&self, id: IdentityId) -> Option<NodeKind> {
        InMemoryGraph::identity_kind(self, id)
    }

    fn tombstone_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<NodeRevision> {
        InMemoryGraph::tombstone_revision_for_identity(self, branch_id, identity_id).cloned()
    }

    fn revision_ids_for_body_hash(&self, body_hash: &[u8; 32]) -> Vec<NodeRevisionId> {
        InMemoryGraph::revision_ids_for_body_hash(self, body_hash).to_vec()
    }

    fn find_revisions_qn_contains(
        &self,
        chain: &[BranchId],
        needle: &str,
        limit: usize,
    ) -> Vec<NodeRevision> {
        let mut out: Vec<NodeRevision> = self
            .revisions()
            .filter(|r| chain.iter().any(|b| *b == r.branch_id) && r.qualified_name.contains(needle))
            .cloned()
            .collect();
        sort_revisions_qn(&mut out);
        if limit > 0 {
            out.truncate(limit);
        }
        out
    }

    fn inbound_edges_to(
        &self,
        target: IdentityId,
        ty: Option<EdgeType>,
    ) -> Vec<(NodeRevision, GraphEdge)> {
        let mut out = Vec::new();
        for rev in self.revisions() {
            for e in InMemoryGraph::outbound_edges(self, rev.revision_id) {
                if e.target_identity_id != target {
                    continue;
                }
                if ty.is_some_and(|t| e.ty != t) {
                    continue;
                }
                out.push((rev.clone(), e.clone()));
            }
        }
        sort_inbound(&mut out);
        out
    }

    fn index_counts(&self) -> GraphIndexCounts {
        let mut files = std::collections::HashSet::new();
        let mut active_revisions = 0usize;
        let mut active_outbound_edges = 0usize;
        for r in self.revisions() {
            if matches!(r.status, RevisionStatus::Active) {
                active_revisions += 1;
                active_outbound_edges += InMemoryGraph::outbound_edges(self, r.revision_id).len();
                if crate::language_indexer::path_is_indexable(&r.file_path) {
                    files.insert(r.file_path.clone());
                }
            }
        }
        GraphIndexCounts {
            active_revisions,
            active_outbound_edges,
            indexable_files: files.len(),
            identities: self.identity_count(),
            revisions: self.revision_count(),
            edges: self.edge_count(),
        }
    }

    fn identity_ids_on_branch(&self, branch: BranchId) -> Vec<IdentityId> {
        let mut ids: Vec<IdentityId> = self
            .revisions()
            .filter(|r| r.branch_id == branch)
            .map(|r| r.identity_id)
            .collect();
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids.dedup();
        ids
    }

    fn count_revisions_with_status(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
        status: RevisionStatus,
    ) -> usize {
        self.revisions()
            .filter(|r| {
                r.identity_id == identity_id
                    && r.status == status
                    && chain.iter().any(|b| *b == r.branch_id)
            })
            .count()
    }

    fn revisions_on_branches(&self, branches: &[BranchId]) -> Vec<NodeRevision> {
        let mut out: Vec<NodeRevision> = self
            .revisions()
            .filter(|r| branches.is_empty() || branches.iter().any(|b| *b == r.branch_id))
            .cloned()
            .collect();
        out.sort_by(|a, b| a.revision_id.0.cmp(&b.revision_id.0));
        out
    }
}

pub(crate) fn sort_revisions_qn(rows: &mut [NodeRevision]) {
    rows.sort_by(|a, b| {
        a.qualified_name
            .cmp(&b.qualified_name)
            .then_with(|| a.revision_id.0.cmp(&b.revision_id.0))
    });
}

pub(crate) fn sort_inbound(rows: &mut [(NodeRevision, GraphEdge)]) {
    rows.sort_by(|a, b| a.1.edge_id.cmp(&b.1.edge_id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Language, NodeIdentity, RevisionStatus, SourceSpan};
    use cis_wal::BranchId;

    fn rid(b: u8) -> NodeRevisionId {
        NodeRevisionId([b; 16])
    }
    fn iid(b: u8) -> IdentityId {
        IdentityId([b; 16])
    }
    fn branch(b: u8) -> BranchId {
        BranchId([b; 16])
    }

    fn rev(id: u8, ident: u8, br: u8, status: RevisionStatus, qn: &str) -> NodeRevision {
        NodeRevision {
            revision_id: rid(id),
            identity_id: iid(ident),
            branch_id: branch(br),
            status,
            qualified_name: qn.into(),
            file_path: "a.py".into(),
            body_hash: [id; 32],
            signature_hash: [id; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        }
    }

    #[test]
    fn ram_primary_prefers_active_over_speculative_and_tombstone() {
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: iid(1),
            kind: NodeKind::Function,
        });
        g.put_revision(rev(1, 1, 1, RevisionStatus::Tombstone, "pkg.old"));
        g.put_revision(rev(2, 1, 1, RevisionStatus::Speculative, "pkg.spec"));
        g.put_revision(rev(3, 1, 1, RevisionStatus::Active, "pkg.live"));
        let p = GraphView::primary_revision_for_identity(&g, branch(1), iid(1)).unwrap();
        assert_eq!(p.revision_id, rid(3));
        assert_eq!(p.status, RevisionStatus::Active);
    }

    #[test]
    fn ram_qn_contains_respects_chain_and_limit() {
        let mut g = InMemoryGraph::default();
        g.put_revision(rev(1, 1, 1, RevisionStatus::Active, "alpha.foo"));
        g.put_revision(rev(2, 2, 2, RevisionStatus::Active, "alpha.bar"));
        g.put_revision(rev(3, 3, 1, RevisionStatus::Active, "other"));
        let chain = [branch(1)];
        let hits = GraphView::find_revisions_qn_contains(&g, &chain, "alpha", 8);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].revision_id, rid(1));
    }

    #[test]
    fn revisions_on_branches_filters_and_empty_means_all() {
        let mut g = InMemoryGraph::default();
        g.put_revision(rev(1, 1, 1, RevisionStatus::Active, "a"));
        g.put_revision(rev(2, 2, 2, RevisionStatus::Active, "b"));
        let one = GraphView::revisions_on_branches(&g, &[branch(1)]);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].revision_id, rid(1));
        let all = GraphView::revisions_on_branches(&g, &[]);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn overlay_mut_cows_status_without_copying_unrelated_rows() {
        let mut committed = InMemoryGraph::default();
        committed.put_identity(NodeIdentity {
            identity_id: iid(1),
            kind: NodeKind::Function,
        });
        committed.put_revision(rev(1, 1, 1, RevisionStatus::Active, "keep"));
        committed.put_identity(NodeIdentity {
            identity_id: iid(2),
            kind: NodeKind::Function,
        });
        committed.put_revision(rev(2, 2, 1, RevisionStatus::Active, "other"));
        let mut overlay = InMemoryGraph::default();
        {
            let mut g = OverlayGraphMut {
                overlay: &mut overlay,
                committed: Some(&committed),
            };
            GraphWrite::set_revision_status(&mut g, rid(1), RevisionStatus::Orphaned);
            assert_eq!(
                GraphView::get_revision(&g, rid(1)).unwrap().status,
                RevisionStatus::Orphaned
            );
            assert_eq!(
                GraphView::get_revision(&g, rid(2)).unwrap().qualified_name,
                "other"
            );
        }
        assert_eq!(overlay.revision_count(), 1);
        assert_eq!(
            GraphView::get_revision(&overlay, rid(1)).unwrap().status,
            RevisionStatus::Orphaned
        );
        assert!(GraphView::get_revision(&overlay, rid(2)).is_none());
    }
}
