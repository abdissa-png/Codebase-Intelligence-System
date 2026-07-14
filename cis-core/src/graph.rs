//! In-memory asymmetric graph (**EI-1, EI-2**) + **TargetReverseIndex** (v2.6: identity-only).

use std::collections::{HashMap, HashSet};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeKind {
    Function,
    Class,
    File,
    Config,
    Stub,
    Test,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RevisionStatus {
    Active,
    Tombstone,
    Orphaned,
    /// Written by an agent `write_file`/`apply_patch` but not yet confirmed via FS confirm token.
    /// Queries prefer Active over Speculative (architecture SP-6).
    /// Old snapshots that pre-date this variant deserialize missing values as `Active` via serde default.
    #[serde(other)]
    Speculative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Language {
    Unknown,
    Python,
    TypeScript,
    Go,
    Rust,
    Java,
    Cpp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub identity_id: IdentityId,
    pub kind: NodeKind,
}

/// 1-based source range for a symbol definition or edge anchor (`0` = unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SourceSpan {
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
}

impl SourceSpan {
    pub const UNKNOWN: Self = Self {
        start_line: 0,
        start_col: 0,
        end_line: 0,
        end_col: 0,
    };

    pub fn is_unknown(self) -> bool {
        self.start_line == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRevision {
    pub revision_id: NodeRevisionId,
    pub identity_id: IdentityId,
    pub branch_id: BranchId,
    pub status: RevisionStatus,
    pub qualified_name: String,
    pub file_path: String,
    pub body_hash: [u8; 32],
    pub signature_hash: [u8; 32],
    pub language: Language,
    /// Prior revision for the same identity (time-travel / merge classification).
    #[serde(default)]
    pub parent_revision_id: Option<NodeRevisionId>,
    /// Prior identity when this revision is the target of a rename.
    #[serde(default)]
    pub rename_source_id: Option<IdentityId>,
    /// Definition site in `file_path` (ingest / tree-sitter).
    #[serde(default)]
    pub span: SourceSpan,
    /// Wall-clock ms when this revision was tombstoned (set by tombstone helpers).
    /// `None` for Active/Speculative revisions and old snapshots that predate this field.
    #[serde(default)]
    pub tombstoned_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeType {
    Calls,
    Imports,
    Uses,
    Extends,
    Configures,
    CoLocated,
    TestOf,
    RenamedFrom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceType {
    Compiler,
    Lsp,
    Ast,
    Textual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeResolution {
    pub target_signature_hash: [u8; 32],
    pub resolver: SourceType,
    /// Wall clock ms when this resolution was validated (**FR-2.10** / **H-6**).
    pub last_validation_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub edge_id: [u8; 16],
    pub ty: EdgeType,
    pub source_revision_id: NodeRevisionId,
    pub target_identity_id: IdentityId,
    pub resolution: EdgeResolution,
    /// Source span in the owning revision body (0 = unknown until ingest provides spans).
    #[serde(default)]
    pub anchor: SourceSpan,
}

impl GraphEdge {
    pub fn with_anchor(mut self, anchor: SourceSpan) -> Self {
        self.anchor = anchor;
        self
    }
}

/// On-disk `graph.json` format version (**Phase 1**).
pub const GRAPH_SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Default)]
pub struct InMemoryGraph {
    identities: HashMap<IdentityId, NodeIdentity>,
    revisions: HashMap<NodeRevisionId, NodeRevision>,
    edges_by_revision: HashMap<NodeRevisionId, Vec<GraphEdge>>,
    target_reverse: HashMap<IdentityId, HashSet<IdentityId>>,
    /// **Phase 1.1** — `(branch, identity) → primary revision id`.
    primary_by_identity: HashMap<(BranchId, IdentityId), NodeRevisionId>,
    /// **Phase 1.2** — `(branch, file_path) → all revision ids on that path`.
    revisions_by_file: HashMap<(BranchId, String), Vec<NodeRevisionId>>,
    /// `(branch, identity) → revision ids` for O(k) primary recompute.
    revisions_by_identity: HashMap<(BranchId, IdentityId), Vec<NodeRevisionId>>,
}

impl InMemoryGraph {
    pub fn put_identity(&mut self, id: NodeIdentity) {
        self.identities.insert(id.identity_id, id);
    }

    pub fn put_revision(&mut self, rev: NodeRevision) {
        let old = self.revisions.get(&rev.revision_id).cloned();
        if let Some(ref o) = old {
            if o.branch_id != rev.branch_id || o.file_path != rev.file_path {
                Self::remove_from_file_index_map(&mut self.revisions_by_file, o);
            }
            if o.branch_id != rev.branch_id || o.identity_id != rev.identity_id {
                Self::remove_from_identity_index_map(&mut self.revisions_by_identity, o);
            }
        }
        self.revisions.insert(rev.revision_id, rev.clone());
        if old.is_none()
            || old
                .as_ref()
                .map(|o| o.branch_id != rev.branch_id || o.file_path != rev.file_path)
                .unwrap_or(true)
        {
            Self::add_to_file_index_map(&mut self.revisions_by_file, &rev);
        }
        if old.is_none()
            || old
                .as_ref()
                .map(|o| o.branch_id != rev.branch_id || o.identity_id != rev.identity_id)
                .unwrap_or(true)
        {
            Self::add_to_identity_index_map(&mut self.revisions_by_identity, &rev);
        }
        self.recompute_primary(rev.branch_id, rev.identity_id);
        if let Some(o) = old {
            if o.branch_id != rev.branch_id || o.identity_id != rev.identity_id {
                self.recompute_primary(o.branch_id, o.identity_id);
            }
        }
    }

    fn add_to_file_index_map(
        map: &mut HashMap<(BranchId, String), Vec<NodeRevisionId>>,
        rev: &NodeRevision,
    ) {
        if rev.file_path.is_empty() {
            return;
        }
        let key = (rev.branch_id, rev.file_path.clone());
        let entry = map.entry(key).or_default();
        if !entry.contains(&rev.revision_id) {
            entry.push(rev.revision_id);
        }
    }

    fn remove_from_file_index_map(
        map: &mut HashMap<(BranchId, String), Vec<NodeRevisionId>>,
        rev: &NodeRevision,
    ) {
        if rev.file_path.is_empty() {
            return;
        }
        let key = (rev.branch_id, rev.file_path.clone());
        if let Some(v) = map.get_mut(&key) {
            v.retain(|id| *id != rev.revision_id);
            if v.is_empty() {
                map.remove(&key);
            }
        }
    }

    fn add_to_identity_index_map(
        map: &mut HashMap<(BranchId, IdentityId), Vec<NodeRevisionId>>,
        rev: &NodeRevision,
    ) {
        let key = (rev.branch_id, rev.identity_id);
        let entry = map.entry(key).or_default();
        if !entry.contains(&rev.revision_id) {
            entry.push(rev.revision_id);
        }
    }

    fn remove_from_identity_index_map(
        map: &mut HashMap<(BranchId, IdentityId), Vec<NodeRevisionId>>,
        rev: &NodeRevision,
    ) {
        let key = (rev.branch_id, rev.identity_id);
        if let Some(v) = map.get_mut(&key) {
            v.retain(|id| *id != rev.revision_id);
            if v.is_empty() {
                map.remove(&key);
            }
        }
    }

    /// Rebuild **Phase 1** secondary indices from all revisions (snapshot load / recovery).
    pub fn rebuild_secondary_indices(&mut self) {
        self.primary_by_identity.clear();
        self.revisions_by_file.clear();
        self.revisions_by_identity.clear();
        let revs: Vec<NodeRevision> = self.revisions.values().cloned().collect();
        for rev in &revs {
            Self::add_to_file_index_map(&mut self.revisions_by_file, rev);
            Self::add_to_identity_index_map(&mut self.revisions_by_identity, rev);
        }
        let mut identities: HashSet<(BranchId, IdentityId)> = HashSet::new();
        for rev in &revs {
            identities.insert((rev.branch_id, rev.identity_id));
        }
        for (branch, identity) in identities {
            self.recompute_primary(branch, identity);
        }
    }

    /// Permanently remove a revision and its edges (**Phase 3.4** tombstone GC).
    pub fn remove_revision(&mut self, revision_id: NodeRevisionId) -> bool {
        let Some(rev) = self.revisions.remove(&revision_id) else {
            return false;
        };
        Self::remove_from_file_index_map(&mut self.revisions_by_file, &rev);
        Self::remove_from_identity_index_map(&mut self.revisions_by_identity, &rev);
        if let Some(old) = self.edges_by_revision.remove(&revision_id) {
            Self::unlink_source_from_target_reverse(
                &mut self.target_reverse,
                rev.identity_id,
                &old,
            );
        }
        self.recompute_primary(rev.branch_id, rev.identity_id);
        true
    }

    fn unlink_source_from_target_reverse(
        target_reverse: &mut HashMap<IdentityId, HashSet<IdentityId>>,
        source_identity: IdentityId,
        edges: &[GraphEdge],
    ) {
        for e in edges {
            if let Some(set) = target_reverse.get_mut(&e.target_identity_id) {
                set.remove(&source_identity);
                if set.is_empty() {
                    target_reverse.remove(&e.target_identity_id);
                }
            }
        }
    }

    fn recompute_primary(&mut self, branch_id: BranchId, identity_id: IdentityId) {
        let key = (branch_id, identity_id);
        let mut fallback: Option<NodeRevisionId> = None;
        let candidates = self
            .revisions_by_identity
            .get(&key)
            .cloned()
            .unwrap_or_default();
        for rid in candidates {
            let Some(r) = self.revisions.get(&rid) else {
                continue;
            };
            if matches!(r.status, RevisionStatus::Active) {
                self.primary_by_identity.insert(key, r.revision_id);
                return;
            }
            if fallback.is_none() || r.revision_id.0 < fallback.unwrap().0 {
                fallback = Some(r.revision_id);
            }
        }
        if let Some(rid) = fallback {
            self.primary_by_identity.insert(key, rid);
        } else {
            self.primary_by_identity.remove(&key);
        }
    }

    pub fn get_revision(&self, id: NodeRevisionId) -> Option<&NodeRevision> {
        self.revisions.get(&id)
    }

    pub fn identity_kind(&self, id: IdentityId) -> Option<NodeKind> {
        self.identities.get(&id).map(|n| n.kind)
    }

    /// Iterate all revisions (MCP / diagnostics).
    pub fn revisions(&self) -> impl Iterator<Item = &NodeRevision> {
        self.revisions.values()
    }

    pub fn primary_revision_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<&NodeRevision> {
        let rid = self
            .primary_by_identity
            .get(&(branch_id, identity_id))
            .copied()?;
        self.revisions.get(&rid)
    }

    /// Nearest-first primary across a branch ancestry chain (`[child, parent, …]`).
    pub fn primary_revision_for_identity_in_chain(
        &self,
        chain: &[BranchId],
        identity_id: IdentityId,
    ) -> Option<&NodeRevision> {
        for &branch_id in chain {
            if let Some(rev) = self.primary_revision_for_identity(branch_id, identity_id) {
                return Some(rev);
            }
        }
        None
    }

    /// All revision ids on `(branch, file_path)` (any status).
    pub fn revision_ids_for_file(&self, branch_id: BranchId, file_path: &str) -> &[NodeRevisionId] {
        self.revisions_by_file
            .get(&(branch_id, file_path.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// All revision ids for `(branch, identity)` (any status).
    pub fn revision_ids_for_identity(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> &[NodeRevisionId] {
        self.revisions_by_identity
            .get(&(branch_id, identity_id))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Speculative revisions on a file path (confirm/revert hot path).
    pub fn speculative_revision_ids_for_file(
        &self,
        branch_id: BranchId,
        file_path: &str,
    ) -> Vec<NodeRevisionId> {
        self.revision_ids_for_file(branch_id, file_path)
            .iter()
            .filter(|rid| {
                self.revisions
                    .get(rid)
                    .map(|r| matches!(r.status, RevisionStatus::Speculative))
                    .unwrap_or(false)
            })
            .copied()
            .collect()
    }

    /// Tombstone revisions on a file path (rename detection hot path).
    pub fn tombstone_revisions_for_file<'a>(
        &'a self,
        branch_id: BranchId,
        file_path: &str,
    ) -> Vec<&'a NodeRevision> {
        self.revision_ids_for_file(branch_id, file_path)
            .iter()
            .filter_map(|rid| self.revisions.get(rid))
            .filter(|r| matches!(r.status, RevisionStatus::Tombstone))
            .collect()
    }

    /// Tombstone revisions on a branch (optional file filter).
    pub fn tombstone_revisions_on_branch<'a>(
        &'a self,
        branch_id: BranchId,
        file_path: Option<&str>,
    ) -> Vec<&'a NodeRevision> {
        match file_path {
            Some(path) => self.tombstone_revisions_for_file(branch_id, path),
            None => self
                .revisions
                .values()
                .filter(|r| r.branch_id == branch_id && matches!(r.status, RevisionStatus::Tombstone))
                .collect(),
        }
    }

    /// Distinct file paths with at least one revision on `branch`.
    pub fn file_paths_on_branch(&self, branch_id: BranchId) -> impl Iterator<Item = &String> {
        self.revisions_by_file
            .keys()
            .filter(move |(b, _)| *b == branch_id)
            .map(|(_, p)| p)
    }

    /// Active revision file paths on a branch (Phase C edge regen).
    pub fn active_file_paths_on_branch(&self, branch_id: BranchId) -> HashSet<String> {
        let mut paths = HashSet::new();
        for path in self.file_paths_on_branch(branch_id) {
            let has_active = self
                .revision_ids_for_file(branch_id, path)
                .iter()
                .any(|rid| {
                    self.revisions
                        .get(rid)
                        .map(|r| matches!(r.status, RevisionStatus::Active))
                        .unwrap_or(false)
                });
            if has_active {
                paths.insert(path.clone());
            }
        }
        paths
    }

    /// **EI-1:** atomically replace the full outbound edge list for `revision_id`.
    pub fn replace_edges_for_revision(
        &mut self,
        revision_id: NodeRevisionId,
        new_edges: Vec<GraphEdge>,
    ) -> Result<(), &'static str> {
        let source_identity = self
            .revisions
            .get(&revision_id)
            .map(|r| r.identity_id)
            .ok_or("unknown revision")?;
        validate_edge_cardinality(&new_edges).map_err(|_| "cardinality_violation")?;
        if let Some(old) = self.edges_by_revision.get(&revision_id) {
            Self::unlink_source_from_target_reverse(
                &mut self.target_reverse,
                source_identity,
                old,
            );
        }
        for e in &new_edges {
            if e.source_revision_id != revision_id {
                return Err("edge source_revision_id must match replace_edges_for_revision");
            }
            self
                .target_reverse
                .entry(e.target_identity_id)
                .or_default()
                .insert(source_identity);
        }
        self.edges_by_revision.insert(revision_id, new_edges);
        Ok(())
    }

    pub fn outbound_edges(&self, revision_id: NodeRevisionId) -> &[GraphEdge] {
        self.edges_by_revision
            .get(&revision_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// **TargetReverseIndex** query: source identities that have an edge **to** `target` (any tier).
    pub fn source_identities_targeting(&self, target: IdentityId) -> HashSet<IdentityId> {
        self.target_reverse
            .get(&target)
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_revision_status(&mut self, id: NodeRevisionId, status: RevisionStatus) {
        let (branch, identity) = if let Some(rev) = self.revisions.get(&id) {
            (rev.branch_id, rev.identity_id)
        } else {
            return;
        };
        if let Some(rev) = self.revisions.get_mut(&id) {
            rev.status = status;
        }
        self.recompute_primary(branch, identity);
    }

    pub fn identity_count(&self) -> usize {
        self.identities.len()
    }

    pub fn revision_count(&self) -> usize {
        self.revisions.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges_by_revision.values().map(|v| v.len()).sum()
    }

    /// Serializable view for **Phase 1** graph persistence (`graph.json`).
    pub fn to_snapshot(&self) -> GraphSnapshot {
        let mut edges: Vec<GraphEdge> = Vec::new();
        for list in self.edges_by_revision.values() {
            edges.extend(list.iter().cloned());
        }
        GraphSnapshot {
            version: GRAPH_SNAPSHOT_VERSION,
            identities: self.identities.values().cloned().collect(),
            revisions: self.revisions.values().cloned().collect(),
            edges,
        }
    }

    /// Deep clone including edges and **TargetReverseIndex**.
    pub fn clone_full(&self) -> Result<Self, &'static str> {
        Self::from_snapshot(self.to_snapshot())
    }

    /// Rebuild graph + **TargetReverseIndex** from a snapshot.
    pub fn from_snapshot(snap: GraphSnapshot) -> Result<Self, &'static str> {
        let mut g = InMemoryGraph::default();
        for id in snap.identities {
            g.put_identity(id);
        }
        for rev in snap.revisions {
            g.put_revision(rev);
        }
        let mut by_rev: HashMap<NodeRevisionId, Vec<GraphEdge>> = HashMap::new();
        for e in snap.edges {
            by_rev.entry(e.source_revision_id).or_default().push(e);
        }
        for (rev, edges) in by_rev {
            g.replace_edges_for_revision(rev, edges)?;
        }
        g.rebuild_secondary_indices();
        Ok(g)
    }
}

/// On-disk graph snapshot (**Epic 2** / Phase 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub version: u32,
    pub identities: Vec<NodeIdentity>,
    pub revisions: Vec<NodeRevision>,
    pub edges: Vec<GraphEdge>,
}

// --- **FR-2.11 / FR-2.12** edge metadata + cardinality ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationTier {
    Sync,
    Background,
}

#[derive(Debug, Clone, Copy)]
pub struct EdgeTypeMetadata {
    pub tier: ReconciliationTier,
    pub min_outbound: u32,
    pub max_outbound: u32,
}

pub fn metadata_for(ty: EdgeType) -> EdgeTypeMetadata {
    match ty {
        EdgeType::Calls => EdgeTypeMetadata {
            tier: ReconciliationTier::Sync,
            min_outbound: 0,
            max_outbound: 10_000,
        },
        EdgeType::Imports | EdgeType::Uses | EdgeType::Extends => EdgeTypeMetadata {
            tier: ReconciliationTier::Sync,
            min_outbound: 0,
            max_outbound: 10_000,
        },
        EdgeType::Configures | EdgeType::CoLocated | EdgeType::TestOf => EdgeTypeMetadata {
            tier: ReconciliationTier::Background,
            min_outbound: 0,
            max_outbound: 10_000,
        },
        EdgeType::RenamedFrom => EdgeTypeMetadata {
            tier: ReconciliationTier::Sync,
            min_outbound: 0,
            max_outbound: 1,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardinalityViolation {
    pub source_revision_id: NodeRevisionId,
    pub ty: EdgeType,
    pub count: u32,
    pub min: u32,
    pub max: u32,
}

pub fn validate_edge_cardinality(edges: &[GraphEdge]) -> Result<(), CardinalityViolation> {
    let mut counts: HashMap<(NodeRevisionId, EdgeType), u32> = HashMap::new();
    for e in edges {
        *counts.entry((e.source_revision_id, e.ty)).or_insert(0) += 1;
    }
    for (&(rev, ty), &n) in &counts {
        let m = metadata_for(ty);
        if n < m.min_outbound || n > m.max_outbound {
            return Err(CardinalityViolation {
                source_revision_id: rev,
                ty,
                count: n,
                min: m.min_outbound,
                max: m.max_outbound,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> IdentityId {
        let mut x = [0u8; 16];
        x[15] = b;
        IdentityId(x)
    }

    fn rid(b: u8) -> NodeRevisionId {
        let mut x = [0u8; 16];
        x[14] = b;
        NodeRevisionId(x)
    }

    fn rid2(b: u8) -> NodeRevisionId {
        let mut x = [0u8; 16];
        x[13] = b;
        NodeRevisionId(x)
    }

    #[test]
    fn cardinality_rejects_two_renamed_from() {
        let mut g = InMemoryGraph::default();
        let i_src = id(1);
        let i_tgt = id(2);
        g.put_identity(NodeIdentity {
            identity_id: i_src,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: i_tgt,
            kind: NodeKind::Function,
        });
        let r = rid2(1);
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
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let res = EdgeResolution {
            target_signature_hash: [0u8; 32],
            resolver: SourceType::Ast,
            last_validation_ms: 0,
        };
        let edges = vec![
            GraphEdge {
                edge_id: [1u8; 16],
                ty: EdgeType::RenamedFrom,
                source_revision_id: r,
                target_identity_id: i_tgt,
                resolution: res.clone(),
                anchor: SourceSpan::UNKNOWN,
            },
            GraphEdge {
                edge_id: [2u8; 16],
                ty: EdgeType::RenamedFrom,
                source_revision_id: r,
                target_identity_id: i_tgt,
                resolution: res,
                anchor: SourceSpan::UNKNOWN,
            },
        ];
        assert!(g.replace_edges_for_revision(r, edges).is_err());
    }

    #[test]
    fn replace_edges_maintains_reverse_index() {
        let mut g = InMemoryGraph::default();
        let i_src = id(1);
        let i_tgt = id(2);
        g.put_identity(NodeIdentity {
            identity_id: i_src,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: i_tgt,
            kind: NodeKind::Function,
        });
        let r = rid(1);
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
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let eid = [7u8; 16];
        let edge = GraphEdge {
            edge_id: eid,
            ty: EdgeType::Calls,
            source_revision_id: r,
            target_identity_id: i_tgt,
            resolution: EdgeResolution {
                target_signature_hash: [1u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(r, vec![edge]).unwrap();
        assert!(g.source_identities_targeting(i_tgt).contains(&i_src));
        g.replace_edges_for_revision(r, vec![]).unwrap();
        assert!(!g.source_identities_targeting(i_tgt).contains(&i_src));
    }

    #[test]
    fn remove_revision_clears_target_reverse() {
        let mut g = InMemoryGraph::default();
        let i_src = id(1);
        let i_tgt = id(2);
        g.put_identity(NodeIdentity {
            identity_id: i_src,
            kind: NodeKind::Function,
        });
        g.put_identity(NodeIdentity {
            identity_id: i_tgt,
            kind: NodeKind::Function,
        });
        let r = rid(1);
        g.put_revision(NodeRevision {
            revision_id: r,
            identity_id: i_src,
            branch_id: BranchId([0u8; 16]),
            status: RevisionStatus::Tombstone,
            qualified_name: "f".into(),
            file_path: "a.py".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: Some(1),
        });
        let edge = GraphEdge {
            edge_id: [7u8; 16],
            ty: EdgeType::RenamedFrom,
            source_revision_id: r,
            target_identity_id: i_tgt,
            resolution: EdgeResolution {
                target_signature_hash: [1u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 0,
            },
            anchor: SourceSpan::UNKNOWN,
        };
        g.replace_edges_for_revision(r, vec![edge]).unwrap();
        assert!(g.source_identities_targeting(i_tgt).contains(&i_src));
        assert!(g.remove_revision(r));
        assert!(
            !g.source_identities_targeting(i_tgt).contains(&i_src),
            "GC must unlink source from target_reverse"
        );
        assert!(g.source_identities_targeting(i_tgt).is_empty());
    }

    #[test]
    fn node_revision_and_graph_edge_serde_roundtrip() {
        let rev = NodeRevision {
            revision_id: rid(3),
            identity_id: id(4),
            branch_id: BranchId([0u8; 16]),
            status: RevisionStatus::Active,
            qualified_name: "m::f".into(),
            file_path: "m.py".into(),
            body_hash: [1u8; 32],
            signature_hash: [2u8; 32],
            language: Language::Python,
            parent_revision_id: Some(rid(2)),
            rename_source_id: Some(id(5)),
            span: SourceSpan {
                start_line: 10,
                start_col: 4,
                end_line: 12,
                end_col: 20,
            },
            tombstoned_at_ms: None,
        };
        let edge = GraphEdge {
            edge_id: [6u8; 16],
            ty: EdgeType::Calls,
            source_revision_id: rev.revision_id,
            target_identity_id: id(6),
            resolution: EdgeResolution {
                target_signature_hash: [3u8; 32],
                resolver: SourceType::Ast,
                last_validation_ms: 42,
            },
            anchor: SourceSpan {
                start_line: 11,
                start_col: 8,
                end_line: 11,
                end_col: 15,
            },
        };
        let rev_json = serde_json::to_string(&rev).unwrap();
        let edge_json = serde_json::to_string(&edge).unwrap();
        let rev2: NodeRevision = serde_json::from_str(&rev_json).unwrap();
        let edge2: GraphEdge = serde_json::from_str(&edge_json).unwrap();
        assert_eq!(rev, rev2);
        assert_eq!(edge, edge2);
        let mut v = serde_json::to_value(&edge).unwrap();
        v.as_object_mut().unwrap().remove("anchor");
        let edge_legacy: GraphEdge = serde_json::from_value(v).unwrap();
        assert!(edge_legacy.anchor.is_unknown());
    }

    #[test]
    fn primary_index_prefers_active_over_speculative() {
        let mut g = InMemoryGraph::default();
        let branch = BranchId([0u8; 16]);
        let identity = id(10);
        let spec = rid(10);
        let active = rid(11);
        for (rid, status) in [(spec, RevisionStatus::Speculative), (active, RevisionStatus::Active)] {
            g.put_revision(NodeRevision {
                revision_id: rid,
                identity_id: identity,
                branch_id: branch,
                status,
                qualified_name: "f".into(),
                file_path: "a.py".into(),
                body_hash: [0u8; 32],
                signature_hash: [0u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                span: SourceSpan::UNKNOWN,
                tombstoned_at_ms: None,
            });
        }
        assert_eq!(
            g.primary_revision_for_identity(branch, identity)
                .map(|r| r.revision_id),
            Some(active)
        );
    }

    #[test]
    fn primary_index_recomputes_on_tombstone() {
        let mut g = InMemoryGraph::default();
        let branch = BranchId([0u8; 16]);
        let identity = id(11);
        let r1 = rid(20);
        let r2 = rid(21);
        for (rid, status) in [(r1, RevisionStatus::Active), (r2, RevisionStatus::Active)] {
            g.put_revision(NodeRevision {
                revision_id: rid,
                identity_id: identity,
                branch_id: branch,
                status,
                qualified_name: "f".into(),
                file_path: "a.py".into(),
                body_hash: [0u8; 32],
                signature_hash: [0u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                span: SourceSpan::UNKNOWN,
                tombstoned_at_ms: None,
            });
        }
        g.set_revision_status(r1, RevisionStatus::Tombstone);
        assert_eq!(
            g.primary_revision_for_identity(branch, identity)
                .map(|r| r.revision_id),
            Some(r2)
        );
    }

    #[test]
    fn file_index_speculative_subset() {
        let mut g = InMemoryGraph::default();
        let branch = BranchId([0u8; 16]);
        let path = "src/foo.py";
        let spec = rid(30);
        let active = rid(31);
        g.put_revision(NodeRevision {
            revision_id: spec,
            identity_id: id(1),
            branch_id: branch,
            status: RevisionStatus::Speculative,
            qualified_name: "f".into(),
            file_path: path.into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        g.put_revision(NodeRevision {
            revision_id: active,
            identity_id: id(2),
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: "g".into(),
            file_path: path.into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        assert_eq!(
            g.speculative_revision_ids_for_file(branch, path),
            vec![spec]
        );
    }

    #[test]
    fn from_snapshot_rebuilds_indices() {
        let mut g = InMemoryGraph::default();
        let branch = BranchId([0u8; 16]);
        let identity = id(12);
        let r = rid(40);
        g.put_revision(NodeRevision {
            revision_id: r,
            identity_id: identity,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: "h".into(),
            file_path: "b.py".into(),
            body_hash: [0u8; 32],
            signature_hash: [0u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: None,
        });
        let snap = g.to_snapshot();
        let g2 = InMemoryGraph::from_snapshot(snap).unwrap();
        assert_eq!(
            g2.primary_revision_for_identity(branch, identity)
                .map(|rev| rev.revision_id),
            Some(r)
        );
        assert_eq!(g2.revision_ids_for_file(branch, "b.py"), &[r]);
    }
}
