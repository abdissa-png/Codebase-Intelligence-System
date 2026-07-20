//! Per-identity revision lineage via `parent_revision_id` and rename back-links.

use std::collections::HashSet;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::deletion_absence::DeletionAbsenceStore;
use crate::graph::{InMemoryGraph, NodeRevision, RevisionStatus};
use crate::query_engine::resolve_identity_revision_with_absence;

const DEFAULT_MAX_DEPTH: usize = 32;

fn revision_on_chain(chain: &[BranchId], branch_id: BranchId) -> bool {
    chain.iter().any(|b| *b == branch_id)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Retire a live revision to tombstone without a deletion-absence marker (in-place history).
pub fn retire_revision_to_tombstone(graph: &mut InMemoryGraph, revision_id: NodeRevisionId) {
    let Some(mut rev) = graph.get_revision(revision_id).cloned() else {
        return;
    };
    if matches!(rev.status, RevisionStatus::Tombstone) {
        return;
    }
    rev.status = RevisionStatus::Tombstone;
    rev.tombstoned_at_ms = Some(now_ms());
    graph.put_revision(rev);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageLink {
    Start,
    ParentRevision,
    RenameSource,
}

#[derive(Debug, Clone)]
pub struct LineageStep {
    pub revision_id: NodeRevisionId,
    pub identity_id: IdentityId,
    pub qualified_name: String,
    pub body_hash: [u8; 32],
    pub status: RevisionStatus,
    pub link: LineageLink,
}

#[derive(Debug, Clone)]
pub struct LineageOptions {
    pub max_depth: usize,
    pub include_tombstones: bool,
    pub follow_renames: bool,
}

impl Default for LineageOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            include_tombstones: true,
            follow_renames: true,
        }
    }
}

fn push_step(out: &mut Vec<LineageStep>, rev: &NodeRevision, link: LineageLink) {
    out.push(LineageStep {
        revision_id: rev.revision_id,
        identity_id: rev.identity_id,
        qualified_name: rev.qualified_name.clone(),
        body_hash: rev.body_hash,
        status: rev.status,
        link,
    });
}

/// Walk backward from `head` following `parent_revision_id` and `rename_source_id`.
pub fn lineage_from_revision(
    g: &InMemoryGraph,
    chain: &[BranchId],
    head: &NodeRevision,
    opts: &LineageOptions,
) -> Vec<LineageStep> {
    let mut out = Vec::new();
    let mut seen: HashSet<(IdentityId, NodeRevisionId)> = HashSet::new();
    let mut cur_rid = head.revision_id;
    let mut next_link = LineageLink::Start;

    for _ in 0..opts.max_depth {
        let Some(rev) = g.get_revision(cur_rid) else {
            break;
        };
        if !revision_on_chain(chain, rev.branch_id) {
            break;
        }
        if !out.is_empty()
            && !opts.include_tombstones
            && matches!(rev.status, RevisionStatus::Tombstone)
        {
            break;
        }
        if !seen.insert((rev.identity_id, cur_rid)) {
            break;
        }
        push_step(&mut out, rev, next_link);

        if let Some(parent) = rev.parent_revision_id {
            if g.get_revision(parent).is_some() {
                cur_rid = parent;
                next_link = LineageLink::ParentRevision;
                continue;
            }
        }

        if opts.follow_renames {
            if let Some(src_iid) = rev.rename_source_id {
                if let Some(tomb) = g.tombstone_revision_for_identity_in_chain(chain, src_iid) {
                    cur_rid = tomb.revision_id;
                    next_link = LineageLink::RenameSource;
                    continue;
                }
            }
        }

        break;
    }
    out
}

/// Resolve `identity_id` on `chain`, then walk its lineage backward.
pub fn lineage_for_identity(
    g: &InMemoryGraph,
    chain: &[BranchId],
    identity_id: IdentityId,
    absence: Option<&DeletionAbsenceStore>,
    opts: &LineageOptions,
) -> Option<Vec<LineageStep>> {
    let head = resolve_identity_revision_with_absence(g, chain, identity_id, absence)?;
    Some(lineage_from_revision(g, chain, head, opts))
}

/// Lowest common ancestor revision id between two heads (shared revision in both lineages).
pub fn merge_base_revision(
    g: &InMemoryGraph,
    chain: &[BranchId],
    ours: NodeRevisionId,
    theirs: NodeRevisionId,
    opts: &LineageOptions,
) -> Option<NodeRevisionId> {
    let ours_head = g.get_revision(ours)?;
    let theirs_head = g.get_revision(theirs)?;
    let ours_set: HashSet<_> = lineage_from_revision(g, chain, ours_head, opts)
        .into_iter()
        .map(|s| s.revision_id)
        .collect();
    for step in lineage_from_revision(g, chain, theirs_head, opts) {
        if ours_set.contains(&step.revision_id) {
            return Some(step.revision_id);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{
        Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus, SourceSpan,
    };
    use crate::identity_resolver::{IdentityResolver, RenameEvidence, RenameSignalKind};
    use crate::index_model::stable_rev_id_bytes;

    fn branch() -> BranchId {
        BranchId([1u8; 16])
    }

    fn id(n: u8) -> IdentityId {
        IdentityId([n; 16])
    }

    fn rid(n: u8) -> NodeRevisionId {
        NodeRevisionId([n; 16])
    }

    fn rev(
        g: &mut InMemoryGraph,
        revision_id: NodeRevisionId,
        identity_id: IdentityId,
        qn: &str,
        status: RevisionStatus,
        parent: Option<NodeRevisionId>,
        rename_source: Option<IdentityId>,
    ) {
        g.put_identity(NodeIdentity {
            identity_id,
            kind: NodeKind::Function,
        });
        g.put_revision(NodeRevision {
            revision_id,
            identity_id,
            branch_id: branch(),
            status,
            qualified_name: qn.into(),
            file_path: "m.py".into(),
            body_hash: [revision_id.0[0]; 32],
            signature_hash: [revision_id.0[0]; 32],
            language: Language::Python,
            parent_revision_id: parent,
            rename_source_id: rename_source,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: if matches!(status, RevisionStatus::Tombstone) {
                Some(1)
            } else {
                None
            },
        });
    }

    #[test]
    fn walks_parent_revision_chain() {
        let mut g = InMemoryGraph::default();
        let iid = id(1);
        rev(&mut g, rid(1), iid, "v1", RevisionStatus::Tombstone, None, None);
        rev(&mut g, rid(2), iid, "v2", RevisionStatus::Active, Some(rid(1)), None);
        let chain = [branch()];
        let head = g.get_revision(rid(2)).unwrap();
        let steps = lineage_from_revision(&g, &chain, head, &LineageOptions::default());
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].revision_id, rid(2));
        assert_eq!(steps[1].revision_id, rid(1));
        assert!(matches!(steps[1].status, RevisionStatus::Tombstone));
    }

    #[test]
    fn follows_rename_source_backward() {
        let mut g = InMemoryGraph::default();
        let old_i = id(1);
        let new_i = id(2);
        let tomb = rid(1);
        let active = rid(2);
        rev(&mut g, tomb, old_i, "foo", RevisionStatus::Tombstone, None, None);
        rev(
            &mut g,
            active,
            new_i,
            "bar",
            RevisionStatus::Active,
            Some(tomb),
            Some(old_i),
        );
        let renamed = IdentityResolver::renamed_from_edge(
            tomb,
            new_i,
            RenameEvidence {
                kind: RenameSignalKind::AstBodySimilarity,
                confidence: 0.9,
            },
            [9u8; 16],
        );
        g.replace_edges_for_revision(tomb, vec![renamed]).unwrap();
        let chain = [branch()];
        let head = g.get_revision(active).unwrap();
        let steps = lineage_from_revision(&g, &chain, head, &LineageOptions::default());
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].qualified_name, "bar");
        assert_eq!(steps[1].qualified_name, "foo");
    }

    #[test]
    fn merge_base_finds_shared_ancestor() {
        let mut g = InMemoryGraph::default();
        let iid = id(3);
        rev(&mut g, rid(10), iid, "base", RevisionStatus::Tombstone, None, None);
        rev(
            &mut g,
            rid(11),
            iid,
            "mid",
            RevisionStatus::Tombstone,
            Some(rid(10)),
            None,
        );
        rev(
            &mut g,
            rid(12),
            iid,
            "tip",
            RevisionStatus::Active,
            Some(rid(11)),
            None,
        );
        let chain = [branch()];
        let base = merge_base_revision(&g, &chain, rid(12), rid(11), &LineageOptions::default());
        assert_eq!(base, Some(rid(11)));
        let base2 = merge_base_revision(&g, &chain, rid(12), rid(10), &LineageOptions::default());
        assert_eq!(base2, Some(rid(10)));
    }

    #[test]
    fn retire_revision_to_tombstone_preserves_row() {
        let mut g = InMemoryGraph::default();
        let iid = id(4);
        let path = "a.py";
        let b = branch();
        let stable = NodeRevisionId(stable_rev_id_bytes(b, path, "fn"));
        rev(&mut g, stable, iid, "fn", RevisionStatus::Active, None, None);
        retire_revision_to_tombstone(&mut g, stable);
        let r = g.get_revision(stable).unwrap();
        assert!(matches!(r.status, RevisionStatus::Tombstone));
        // Row is preserved for lineage/bridging, but never bound as primary.
        assert!(g.primary_revision_for_identity(b, iid).is_none());
        assert_eq!(
            g.tombstone_revision_for_identity(b, iid)
                .map(|t| t.revision_id),
            Some(stable)
        );
    }
}
