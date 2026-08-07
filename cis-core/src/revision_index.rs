//! **`RevisionIndex`** — thin branch-facing API over [`RevisionIndexCow`] (**C-2**, **FR-2.6**).

use std::collections::HashSet;
use std::sync::Arc;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::kv::MemoryKv;
use crate::revision_cow::RevisionIndexCow;

/// Matches [`RevisionIndexCow`] compaction depth — ancestry walks stop here.
const BRANCH_ANCESTRY_MAX_DEPTH: usize = 10;

pub fn revision_binding_kv_key(branch_id: BranchId, identity_id: IdentityId) -> String {
    let hex_branch = branch_id
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let hex_id = identity_id
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    format!("ri:{}:{}", hex_branch, hex_id)
}

fn hex_branch_id(branch_id: BranchId) -> String {
    branch_id
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

/// Copy every `ri:{parent}:{identity}` row to `ri:{child}:{identity}` at branch creation.
///
/// ETO rows (`eto:{branch}:…`) are **not** copied: interactive queries inherit parent
/// overrides via [`crate::edge_target_override::EdgeTargetOverrideStore::effective_target_identity_in_chain`].
pub fn fork_branch_bindings(kv: &MemoryKv, parent: BranchId, child: BranchId) -> usize {
    let parent_hex = hex_branch_id(parent);
    let child_hex = hex_branch_id(child);
    let prefix = format!("ri:{parent_hex}:");
    let mut count = 0usize;
    for (k, v) in kv.scan_prefix(&prefix) {
        let parts: Vec<&str> = k.split(':').collect();
        if parts.len() != 3 {
            continue;
        }
        let child_key = format!("ri:{child_hex}:{}", parts[2]);
        if kv.get(&child_key).is_none() {
            kv.set(&child_key, v);
            count += 1;
        }
    }
    let parent_meta = format!("branch_parent:{child_hex}");
    if kv.get(&parent_meta).is_none() {
        kv.set(&parent_meta, parent.0.to_vec());
    }
    // Temporal COW: record when this child forked so post-fork parent deletions
    // do not hide inherited symbols for the child.
    crate::deletion_absence::record_fork_timestamp(kv, child);
    count
}

/// Walk `branch_parent:{child}` from `branch` toward the root.
///
/// Returns `[branch, parent, grandparent, …]` (nearest first). Cycles and depth beyond
/// [`BRANCH_ANCESTRY_MAX_DEPTH`] are truncated.
pub fn branch_ancestry(kv: &MemoryKv, branch: BranchId) -> Vec<BranchId> {
    let mut chain = Vec::with_capacity(BRANCH_ANCESTRY_MAX_DEPTH + 1);
    let mut seen = HashSet::new();
    let mut cur = branch;
    loop {
        if !seen.insert(cur) {
            break;
        }
        chain.push(cur);
        if chain.len() > BRANCH_ANCESTRY_MAX_DEPTH {
            break;
        }
        let key = format!("branch_parent:{}", hex_branch_id(cur));
        let Some(bytes) = kv.get(&key) else {
            break;
        };
        if bytes.len() != 16 {
            break;
        }
        let mut parent = [0u8; 16];
        parent.copy_from_slice(&bytes);
        cur = BranchId(parent);
    }
    chain
}

#[derive(Debug)]
pub struct RevisionIndex {
    inner: Arc<RevisionIndexCow>,
}

impl RevisionIndex {
    /// Hydrates from existing `ri:{branch}:*` KV entries (cold start parity).
    pub fn new(branch_id: BranchId, kv: Arc<MemoryKv>) -> Self {
        Self {
            inner: RevisionIndexCow::root_hydrated(branch_id, kv),
        }
    }

    pub fn branch_id(&self) -> BranchId {
        self.inner.branch_id()
    }

    pub fn as_cow(&self) -> Arc<RevisionIndexCow> {
        Arc::clone(&self.inner)
    }

    pub fn bind(&self, identity_id: IdentityId, revision_id: NodeRevisionId) {
        self.inner.bind(identity_id, revision_id);
    }

    pub fn lookup(&self, identity_id: IdentityId) -> Option<NodeRevisionId> {
        self.inner.lookup(identity_id)
    }

    pub fn scan_branch(&self) -> Vec<(IdentityId, NodeRevisionId)> {
        self.inner.resolved_bindings()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let b = BranchId([9u8; 16]);
        let ri = RevisionIndex::new(b, Arc::clone(&kv));
        let id = IdentityId([1u8; 16]);
        let rev = NodeRevisionId([2u8; 16]);
        ri.bind(id, rev);
        assert_eq!(ri.lookup(id), Some(rev));
    }

    #[test]
    fn fork_branch_bindings_copies_rows() {
        let kv = Arc::new(MemoryKv::new());
        let parent = BranchId([1u8; 16]);
        let child = BranchId([2u8; 16]);
        let phex = parent.0.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        let idhex = "ab".repeat(16);
        kv.set(
            &format!("ri:{phex}:{idhex}"),
            NodeRevisionId([3u8; 16]).0.to_vec(),
        );
        assert_eq!(fork_branch_bindings(kv.as_ref(), parent, child), 1);
        let chex = child.0.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        assert!(kv.get(&format!("ri:{chex}:{idhex}")).is_some());
        assert!(crate::deletion_absence::fork_timestamp(kv.as_ref(), child).is_some());
    }

    #[test]
    fn branch_ancestry_single_root() {
        let kv = MemoryKv::new();
        let main = BranchId([1u8; 16]);
        assert_eq!(branch_ancestry(&kv, main), vec![main]);
    }

    #[test]
    fn branch_ancestry_multi_level() {
        let kv = MemoryKv::new();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let sub = BranchId([3u8; 16]);
        fork_branch_bindings(&kv, main, feature);
        fork_branch_bindings(&kv, feature, sub);
        assert_eq!(branch_ancestry(&kv, sub), vec![sub, feature, main]);
    }

    #[test]
    fn branch_ancestry_cycle_guard() {
        let kv = MemoryKv::new();
        let a = BranchId([0x0a; 16]);
        let b = BranchId([0x0b; 16]);
        kv.set(&format!("branch_parent:{}", hex_branch_id(a)), b.0.to_vec());
        kv.set(&format!("branch_parent:{}", hex_branch_id(b)), a.0.to_vec());
        let chain = branch_ancestry(&kv, a);
        assert_eq!(chain, vec![a, b]);
    }
}
