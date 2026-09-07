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
}
