//! Branch-local **deletion absence** markers (`deleted:{branch}:{identity}`).
//!
//! When a symbol is deleted on a feature branch, CIS plants a tombstone overlay so
//! inherited parent revisions stay hidden. Those overlays are eligible for tombstone
//! GC after retention — without a durable absence signal, chain resolution would
//! fall through to the parent and **resurrect** the deleted symbol.
//!
//! Absence keys are inherited by **ancestry walk** (like ETO), not copied on fork.
//!
//! **Temporal COW:** ancestor deletions planted *after* a child forked are ignored for
//! that child's queries (`fork_ts:{child}` vs deletion timestamp). Own-branch deletions
//! are always honored.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

/// KV key for when `child` forked from its parent (`u64` LE millis).
pub fn fork_ts_key(child: BranchId) -> String {
    format!("fork_ts:{}", hex16(&child.0))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn read_u64_le(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(bytes);
    Some(u64::from_le_bytes(a))
}

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
    ///
    /// Value is wall-clock ms (`u64` LE). Legacy single-byte markers are still recognized
    /// as present (treated as timestamp `0` — always honored for temporal checks).
    pub fn mark_deleted(&self, branch_id: BranchId, identity_id: IdentityId) {
        self.kv.set(
            &deleted_key(branch_id, identity_id),
            now_ms().to_le_bytes().to_vec(),
        );
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

    /// Wall-clock ms when the deletion was planted, if any.
    ///
    /// Legacy `[1u8]` markers return `Some(0)`.
    pub fn deletion_timestamp(
        &self,
        branch_id: BranchId,
        identity_id: IdentityId,
    ) -> Option<u64> {
        let v = self.kv.get(&deleted_key(branch_id, identity_id))?;
        if let Some(ts) = read_u64_le(&v) {
            return Some(ts);
        }
        if !v.is_empty() {
            return Some(0);
        }
        None
    }

    /// Nearest-first: if any branch in `chain` (`[child, parent, …]`) has marked
    /// the identity deleted, the identity is absent for this query.
    ///
    /// Prefer [`Self::should_honor_deletion_for_query`] for COW-aware resolution.
    pub fn is_deleted_in_chain(&self, chain: &[BranchId], identity_id: IdentityId) -> bool {
        chain
            .iter()
            .any(|&b| self.is_deleted_on_branch(b, identity_id))
    }

    /// Whether a deletion planted on `deleted_on` should hide `identity_id` for the
    /// querying branch (`chain[0]`).
    ///
    /// - Own-branch deletions are always honored.
    /// - Ancestor deletions are honored only when `deletion_ts <= fork_ts(query)`
    ///   (deleted before / at fork). Post-fork ancestor deletes are ignored so the
    ///   child keeps COW-inherited visibility.
    /// - Missing `fork_ts` → conservative honor (legacy forks).
    pub fn should_honor_deletion_for_query(
        &self,
        chain: &[BranchId],
        deleted_on: BranchId,
        identity_id: IdentityId,
    ) -> bool {
        let Some(del_ts) = self.deletion_timestamp(deleted_on, identity_id) else {
            return false;
        };
        let Some(&query) = chain.first() else {
            return true;
        };
        if deleted_on == query {
            return true;
        }
        match fork_timestamp(self.kv.as_ref(), query) {
            Some(fork_ts) => del_ts <= fork_ts,
            None => true,
        }
    }

    /// Temporal COW chain check: true when any honored deletion hides the identity.
    pub fn is_deleted_for_query(&self, chain: &[BranchId], identity_id: IdentityId) -> bool {
        chain.iter().any(|&b| {
            self.is_deleted_on_branch(b, identity_id)
                && self.should_honor_deletion_for_query(chain, b, identity_id)
        })
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

/// Record wall-clock fork time for `child` (idempotent — first write wins).
pub fn record_fork_timestamp(kv: &MemoryKv, child: BranchId) {
    let key = fork_ts_key(child);
    if kv.get(&key).is_none() {
        kv.set(&key, now_ms().to_le_bytes().to_vec());
    }
}

/// When `child` forked from its parent, if recorded.
pub fn fork_timestamp(kv: &MemoryKv, child: BranchId) -> Option<u64> {
    let v = kv.get(&fork_ts_key(child))?;
    read_u64_le(&v)
}

/// Direct children of `parent` via `branch_parent:{child} → parent` rows.
pub fn child_branches(kv: &MemoryKv, parent: BranchId) -> Vec<BranchId> {
    let mut out = Vec::new();
    for (key, val) in kv.scan_prefix("branch_parent:") {
        if val.as_slice() != parent.0.as_slice() {
            continue;
        }
        let Some(hex) = key.strip_prefix("branch_parent:") else {
            continue;
        };
        if hex.len() != 32 {
            continue;
        }
        let mut b = [0u8; 16];
        let mut ok = true;
        for i in 0..16 {
            match u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                Ok(x) => b[i] = x,
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            out.push(BranchId(b));
        }
    }
    out
}

/// Whether `parent` has at least one forked child branch.
pub fn has_child_branches(kv: &MemoryKv, parent: BranchId) -> bool {
    !child_branches(kv, parent).is_empty()
}

/// All `(branch, identity)` pairs with a `deleted:` marker.
pub fn list_deleted_markers(kv: &MemoryKv) -> Vec<(BranchId, IdentityId)> {
    let mut out = Vec::new();
    for (key, _) in kv.scan_prefix("deleted:") {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 3 {
            continue;
        }
        let (Some(branch), Some(identity)) = (parse_hex16_id(parts[1]), parse_hex16_id(parts[2]))
        else {
            continue;
        };
        out.push((BranchId(branch), IdentityId(identity)));
    }
    out
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
    fn mark_clear_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(Arc::clone(&kv));
        let branch = BranchId([1u8; 16]);
        let iid = IdentityId([2u8; 16]);
        assert!(!store.is_deleted_on_branch(branch, iid));
        store.mark_deleted(branch, iid);
        assert!(store.is_deleted_on_branch(branch, iid));
        assert!(store.deletion_timestamp(branch, iid).is_some());
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
    fn post_fork_parent_deletion_ignored_for_child() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(Arc::clone(&kv));
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([9u8; 16]);
        // Child forked first…
        kv.set(&fork_ts_key(feature), 100u64.to_le_bytes().to_vec());
        // …then parent deleted.
        kv.set(&deleted_key(main, iid), 200u64.to_le_bytes().to_vec());
        let chain = [feature, main];
        assert!(!store.should_honor_deletion_for_query(&chain, main, iid));
        assert!(!store.is_deleted_for_query(&chain, iid));
        // Parent query still honors its own deletion.
        assert!(store.should_honor_deletion_for_query(&[main], main, iid));
        assert!(store.is_deleted_for_query(&[main], iid));
    }

    #[test]
    fn pre_fork_parent_deletion_honored() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(Arc::clone(&kv));
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([9u8; 16]);
        kv.set(&deleted_key(main, iid), 50u64.to_le_bytes().to_vec());
        kv.set(&fork_ts_key(feature), 100u64.to_le_bytes().to_vec());
        assert!(store.is_deleted_for_query(&[feature, main], iid));
    }

    #[test]
    fn legacy_marker_treated_as_ancient() {
        let kv = Arc::new(MemoryKv::new());
        let store = DeletionAbsenceStore::new(Arc::clone(&kv));
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let iid = IdentityId([9u8; 16]);
        kv.set(&deleted_key(main, iid), vec![1u8]);
        kv.set(&fork_ts_key(feature), 100u64.to_le_bytes().to_vec());
        assert_eq!(store.deletion_timestamp(main, iid), Some(0));
        assert!(store.is_deleted_for_query(&[feature, main], iid));
    }

    #[test]
    fn child_branches_reverse_lookup() {
        let kv = MemoryKv::new();
        let main = BranchId([1u8; 16]);
        let feature = BranchId([2u8; 16]);
        let other = BranchId([3u8; 16]);
        kv.set(
            &format!("branch_parent:{}", hex16(&feature.0)),
            main.0.to_vec(),
        );
        kv.set(
            &format!("branch_parent:{}", hex16(&other.0)),
            main.0.to_vec(),
        );
        let kids = child_branches(&kv, main);
        assert_eq!(kids.len(), 2);
        assert!(has_child_branches(&kv, main));
        assert!(!has_child_branches(&kv, feature));
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
