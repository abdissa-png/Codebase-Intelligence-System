//! Branch-local **deletion absence** markers (`deleted:{branch}:{identity}`).
//!
//! When a symbol is deleted on a feature branch, CIS plants a tombstone overlay so
//! inherited parent revisions stay hidden. Those overlays are eligible for tombstone
//! GC after retention — without a durable absence signal, chain resolution would
//! fall through to the parent and **resurrect** the deleted symbol.
//!
//! Absence keys are inherited by **ancestry walk** (like ETO), not copied on fork.

use std::sync::Arc;

use cis_wal::{BranchId, IdentityId};

use crate::kv::MemoryKv;

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

/// KV key for a branch-local deletion absence marker.
pub fn deleted_key(branch_id: BranchId, identity_id: IdentityId) -> String {
    format!(
        "deleted:{}:{}",
        hex16(&branch_id.0),
        hex16(&identity_id.0)
    )
}

/// Durable marker value (single byte). Presence of the key is the signal.
const MARKER: &[u8] = &[1u8];

/// Read/write API for deletion absence markers.
#[derive(Debug, Clone)]
pub struct DeletionAbsenceStore {
    kv: Arc<MemoryKv>,
}

impl DeletionAbsenceStore {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    pub fn kv(&self) -> &Arc<MemoryKv> {
        &self.kv
    }

    /// Mark `identity_id` as deleted on `branch_id` (hides inherited parents).
    pub fn mark_deleted(&self, branch_id: BranchId, identity_id: IdentityId) {
        self.kv.set(&deleted_key(branch_id, identity_id), MARKER.to_vec());
    }

    /// Clear the absence marker when the identity is recreated on this branch.
    pub fn clear_deleted(&self, branch_id: BranchId, identity_id: IdentityId) {
        self.kv.delete(&deleted_key(branch_id, identity_id));
    }

    /// Whether this branch itself has a deletion marker (no ancestry walk).
    pub fn is_deleted_on_branch(&self, branch_id: BranchId, identity_id: IdentityId) -> bool {
        self.kv
            .get(&deleted_key(branch_id, identity_id))
            .is_some()
    }

    /// Nearest-first: if any branch in `chain` (`[child, parent, …]`) has marked
    /// the identity deleted, the identity is absent for this query.
    pub fn is_deleted_in_chain(&self, chain: &[BranchId], identity_id: IdentityId) -> bool {
        chain
            .iter()
            .any(|&b| self.is_deleted_on_branch(b, identity_id))
    }

    /// Remove every `deleted:{branch}:*` row for a purged branch.
    pub fn purge_branch(&self, branch_id: BranchId) -> usize {
        let prefix = format!("deleted:{}:", hex16(&branch_id.0));
        let keys: Vec<String> = self
            .kv
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let n = keys.len();
        for k in keys {
            self.kv.delete(&k);
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_clear_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(Arc::clone(&kv));
        let branch = BranchId([1u8; 16]);
        let iid = IdentityId([2u8; 16]);
        assert!(!store.is_deleted_on_branch(branch, iid));
        store.mark_deleted(branch, iid);
        assert!(store.is_deleted_on_branch(branch, iid));
        assert!(kv.get(&deleted_key(branch, iid)).is_some());
        store.clear_deleted(branch, iid);
        assert!(!store.is_deleted_on_branch(branch, iid));
    }

    #[test]
    fn chain_nearest_deletion_hides() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(kv);
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([9u8; 16]);
        store.mark_deleted(feature, iid);
        assert!(store.is_deleted_in_chain(&[feature, main], iid));
        assert!(!store.is_deleted_in_chain(&[main], iid));
    }

    #[test]
    fn purge_branch_removes_only_that_branch() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(kv);
        let a = BranchId([1u8; 16]);
        let b = BranchId([2u8; 16]);
        let iid = IdentityId([3u8; 16]);
        store.mark_deleted(a, iid);
        store.mark_deleted(b, iid);
        assert_eq!(store.purge_branch(a), 1);
        assert!(!store.is_deleted_on_branch(a, iid));
        assert!(store.is_deleted_on_branch(b, iid));
    }
}
