//! Read-only graph + KV consistency checker (**Phase 3.7**).

use std::collections::{HashMap, HashSet};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::{InMemoryGraph, RevisionStatus};
use crate::revision_index::revision_binding_kv_key;
use crate::{MemoryKv, SharedInMemoryGraph};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConsistencyReport {
    pub dangling_bindings: Vec<(BranchId, IdentityId)>,
    pub orphaned_active_without_binding: Vec<NodeRevisionId>,
    pub tombstones_still_bound: Vec<(BranchId, IdentityId)>,
    pub duplicate_active_per_identity: Vec<(BranchId, IdentityId)>,
    pub body_hash_missing_from_store: Vec<[u8; 32]>,
    pub secondary_index_desync: Vec<String>,
}

impl ConsistencyReport {
    pub fn is_clean(&self) -> bool {
        self.dangling_bindings.is_empty()
            && self.orphaned_active_without_binding.is_empty()
            && self.tombstones_still_bound.is_empty()
            && self.duplicate_active_per_identity.is_empty()
            && self.body_hash_missing_from_store.is_empty()
            && self.secondary_index_desync.is_empty()
    }

    pub fn summary(&self) -> String {
        if self.is_clean() {
            return "graph consistency: clean".into();
        }
        format!(
            "dangling_bindings={} orphaned_active={} tombstones_bound={} duplicate_active={} missing_body={} index_desync={}",
            self.dangling_bindings.len(),
            self.orphaned_active_without_binding.len(),
            self.tombstones_still_bound.len(),
            self.duplicate_active_per_identity.len(),
            self.body_hash_missing_from_store.len(),
            self.secondary_index_desync.len(),
        )
    }
}

pub fn check_consistency(
    graph: &SharedInMemoryGraph,
    kv: &MemoryKv,
    body_store: &crate::BodyStore,
    branches: &[BranchId],
) -> ConsistencyReport {
    let g = graph.read();
    let mut report = ConsistencyReport::default();

    let mut bound_revisions: HashSet<NodeRevisionId> = HashSet::new();
    for (key, val) in kv.scan_prefix("ri:") {
        if val.len() != 16 {
            continue;
        }
        let mut rev_bytes = [0u8; 16];
        rev_bytes.copy_from_slice(&val);
        let rev_id = NodeRevisionId(rev_bytes);
        bound_revisions.insert(rev_id);

        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 3 {
            continue;
        }
        let branch = parse_branch_hex(parts[1]);
        let identity = parse_identity_hex(parts[2]);
        let (branch, identity) = match (branch, identity) {
            (Some(b), Some(i)) => (b, i),
            _ => continue,
        };

        let Some(rev) = g.get_revision(rev_id) else {
            report.dangling_bindings.push((branch, identity));
            continue;
        };
        if matches!(rev.status, RevisionStatus::Tombstone) {
            report.tombstones_still_bound.push((branch, identity));
        }
    }

    let branch_set: HashSet<BranchId> = branches.iter().copied().collect();
    let mut active_per_identity: HashMap<(BranchId, IdentityId), usize> = HashMap::new();
    for rev in g.revisions() {
        if !branch_set.is_empty() && !branch_set.contains(&rev.branch_id) {
            continue;
        }
        if matches!(rev.status, RevisionStatus::Active) {
            *active_per_identity
                .entry((rev.branch_id, rev.identity_id))
                .or_insert(0) += 1;
            if !bound_revisions.contains(&rev.revision_id) {
                report.orphaned_active_without_binding.push(rev.revision_id);
            }
        }
        if body_store.get(&rev.body_hash).is_none() && rev.body_hash != [0u8; 32] {
            report.body_hash_missing_from_store.push(rev.body_hash);
        }
    }

    for ((branch, identity), count) in active_per_identity {
        if count > 1 {
            report.duplicate_active_per_identity.push((branch, identity));
        }
        if let Some(primary) = g.primary_revision_for_identity(branch, identity) {
            let key = revision_binding_kv_key(branch, identity);
            if let Some(val) = kv.get(&key) {
                if val.len() == 16 {
                    let mut b = [0u8; 16];
                    b.copy_from_slice(&val);
                    let bound = NodeRevisionId(b);
                    if bound != primary.revision_id {
                        report.secondary_index_desync.push(format!(
                            "primary_by_identity mismatch branch={} identity={}",
                            hex16(&branch.0),
                            hex16(&identity.0),
                        ));
                    }
                }
            }
        }
    }

    report
}

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn parse_branch_hex(s: &str) -> Option<BranchId> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(BranchId(b))
}

fn parse_identity_hex(s: &str) -> Option<IdentityId> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(IdentityId(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{
        InMemoryGraph, Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus, SourceSpan,
    };

    fn mk_rev(
        rev_byte: u8,
        identity_byte: u8,
        branch_byte: u8,
        status: RevisionStatus,
    ) -> NodeRevision {
        NodeRevision {
            revision_id: NodeRevisionId([rev_byte; 16]),
            identity_id: IdentityId([identity_byte; 16]),
            branch_id: BranchId([branch_byte; 16]),
            status,
            qualified_name: format!("sym{rev_byte}"),
            file_path: format!("f{rev_byte}.rs"),
            body_hash: [rev_byte; 32],
            signature_hash: [0u8; 32],
            language: Language::Rust,
            parent_revision_id: None,
            rename_source_id: None,
            span: SourceSpan::UNKNOWN,
            tombstoned_at_ms: if matches!(status, RevisionStatus::Tombstone) {
                Some(1)
            } else {
                None
            },
        }
    }

    #[test]
    fn detects_dangling_binding() {
        let graph = SharedInMemoryGraph::new(InMemoryGraph::default());
        let kv = MemoryKv::new();
        let body = crate::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let branch = BranchId([1u8; 16]);
        let identity = IdentityId([2u8; 16]);
        let missing = NodeRevisionId([9u8; 16]);
        kv.set(
            &revision_binding_kv_key(branch, identity),
            missing.0.to_vec(),
        );
        let rep = check_consistency(&graph, &kv, &body, &[branch]);
        assert_eq!(rep.dangling_bindings.len(), 1);
    }

    #[test]
    fn clean_empty_graph() {
        let graph = SharedInMemoryGraph::new(InMemoryGraph::default());
        let kv = MemoryKv::new();
        let body = crate::BodyStore::new(std::sync::Arc::new(MemoryKv::new()));
        let rep = check_consistency(&graph, &kv, &body, &[]);
        assert!(rep.is_clean());
    }

    #[test]
    fn detects_orphaned_active_without_binding() {
        let branch = BranchId([1u8; 16]);
        let mut g = InMemoryGraph::default();
        let rev = mk_rev(3, 2, 1, RevisionStatus::Active);
        g.put_identity(NodeIdentity {
            identity_id: rev.identity_id,
            kind: NodeKind::Function,
        });
        g.put_revision(rev);
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        let body = crate::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let rep = check_consistency(&graph, &kv, &body, &[branch]);
        assert_eq!(rep.orphaned_active_without_binding.len(), 1);
    }

    #[test]
    fn detects_tombstone_still_bound() {
        let branch = BranchId([1u8; 16]);
        let identity = IdentityId([2u8; 16]);
        let mut g = InMemoryGraph::default();
        let rev = mk_rev(4, 2, 1, RevisionStatus::Tombstone);
        g.put_identity(NodeIdentity {
            identity_id: identity,
            kind: NodeKind::Function,
        });
        g.put_revision(rev.clone());
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        kv.set(
            &revision_binding_kv_key(branch, identity),
            rev.revision_id.0.to_vec(),
        );
        let body = crate::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let rep = check_consistency(&graph, &kv, &body, &[branch]);
        assert_eq!(rep.tombstones_still_bound.len(), 1);
    }

    #[test]
    fn detects_duplicate_active_per_identity() {
        let branch = BranchId([1u8; 16]);
        let identity = IdentityId([2u8; 16]);
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: identity,
            kind: NodeKind::Function,
        });
        g.put_revision(mk_rev(5, 2, 1, RevisionStatus::Active));
        g.put_revision(mk_rev(6, 2, 1, RevisionStatus::Active));
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        let body = crate::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let rep = check_consistency(&graph, &kv, &body, &[branch]);
        assert_eq!(rep.duplicate_active_per_identity.len(), 1);
    }

    #[test]
    fn detects_missing_body_hash() {
        let branch = BranchId([1u8; 16]);
        let mut g = InMemoryGraph::default();
        let rev = mk_rev(8, 2, 1, RevisionStatus::Active);
        g.put_identity(NodeIdentity {
            identity_id: rev.identity_id,
            kind: NodeKind::Function,
        });
        g.put_revision(rev);
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        let body = crate::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let rep = check_consistency(&graph, &kv, &body, &[branch]);
        assert_eq!(rep.body_hash_missing_from_store.len(), 1);
    }

    #[test]
    fn detects_secondary_index_desync() {
        let branch = BranchId([1u8; 16]);
        let identity = IdentityId([2u8; 16]);
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: identity,
            kind: NodeKind::Function,
        });
        let rev1 = mk_rev(10, 2, 1, RevisionStatus::Active);
        let rev2 = mk_rev(11, 2, 1, RevisionStatus::Tombstone);
        g.put_revision(rev1.clone());
        g.put_revision(rev2.clone());
        let graph = SharedInMemoryGraph::new(g);
        let kv = MemoryKv::new();
        kv.set(
            &revision_binding_kv_key(branch, identity),
            rev2.revision_id.0.to_vec(),
        );
        let body = crate::BodyStore::new(std::sync::Arc::new(kv.clone()));
        let rep = check_consistency(&graph, &kv, &body, &[branch]);
        assert!(
            !rep.secondary_index_desync.is_empty(),
            "expected primary/binding mismatch"
        );
    }
}
