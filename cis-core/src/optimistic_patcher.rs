//! **`OptimisticPatcher`** shell — **§01.5**, **US-04** (leases + speculative path registration).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::path_lease::{LeaseError, PathLeaseManager, SessionId, SpeculativePathTracker};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OptimisticPatchError {
    #[error(transparent)]
    Lease(#[from] LeaseError),
    #[error("unknown patch {0}")]
    UnknownPatch(u64),
    #[error("session mismatch for patch {0}")]
    SessionMismatch(u64),
}

#[derive(Debug)]
struct PatchRecord {
    session: SessionId,
    paths: Vec<String>,
    created_ms: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug)]
pub struct OptimisticPatcher {
    leases: Arc<PathLeaseManager>,
    spec_paths: Arc<SpeculativePathTracker>,
    patches: Mutex<HashMap<u64, PatchRecord>>,
    next_id: AtomicU64,
}

impl OptimisticPatcher {
    pub fn new(leases: Arc<PathLeaseManager>, spec_paths: Arc<SpeculativePathTracker>) -> Self {
        Self {
            leases,
            spec_paths,
            patches: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn spec_tracker(&self) -> Arc<SpeculativePathTracker> {
        Arc::clone(&self.spec_paths)
    }

    /// **FR-4.2:** acquire leases in deterministic sorted order.
    pub fn apply_speculative(
        &self,
        session: SessionId,
        mut paths: Vec<String>,
    ) -> Result<u64, OptimisticPatchError> {
        paths.sort();
        for p in &paths {
            self.leases.acquire(p, session)?;
        }
        for p in &paths {
            self.spec_paths.register(p.clone());
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.patches
            .lock()
            .unwrap()
            .insert(id, PatchRecord { session, paths, created_ms: now_ms() });
        Ok(id)
    }

    pub fn revert(&self, patch_id: u64, session: SessionId) -> Result<(), OptimisticPatchError> {
        let rec = self
            .patches
            .lock()
            .unwrap()
            .remove(&patch_id)
            .ok_or(OptimisticPatchError::UnknownPatch(patch_id))?;
        if rec.session != session {
            self.patches.lock().unwrap().insert(patch_id, rec);
            return Err(OptimisticPatchError::SessionMismatch(patch_id));
        }
        for p in &rec.paths {
            self.spec_paths.unregister(p);
            self.leases.release(p, session);
        }
        Ok(())
    }

    /// **Epic 3.3 promote:** release leases + unregister speculative paths, returning the affected paths.
    /// Callers (MCP `confirm_patch`, FS sync) then transition graph revisions to `Active`.
    pub fn promote(&self, patch_id: u64, session: SessionId) -> Result<Vec<String>, OptimisticPatchError> {
        let rec = self
            .patches
            .lock()
            .unwrap()
            .remove(&patch_id)
            .ok_or(OptimisticPatchError::UnknownPatch(patch_id))?;
        if rec.session != session {
            self.patches.lock().unwrap().insert(patch_id, rec);
            return Err(OptimisticPatchError::SessionMismatch(patch_id));
        }
        for p in &rec.paths {
            self.spec_paths.unregister(p);
            self.leases.release(p, session);
        }
        Ok(rec.paths)
    }

    /// Return the paths registered for `patch_id` without removing the record.
    pub fn peek_patch_paths(&self, patch_id: u64) -> Vec<String> {
        self.patches
            .lock()
            .unwrap()
            .get(&patch_id)
            .map(|r| r.paths.clone())
            .unwrap_or_default()
    }

    /// Return the session_id for a patch, if it exists.
    pub fn patch_session(&self, patch_id: u64) -> Option<SessionId> {
        self.patches
            .lock()
            .unwrap()
            .get(&patch_id)
            .map(|r| r.session)
    }

    /// Find a pending patch_id whose registered paths contain `rel_path` (checked against abs and rel forms).
    pub fn pending_patch_for_path(&self, rel_path: &str, repo_root: &str) -> Option<u64> {
        let abs_path = format!("{}/{}", repo_root.trim_end_matches('/'), rel_path);
        let patches = self.patches.lock().unwrap();
        for (&id, rec) in patches.iter() {
            if rec.paths.iter().any(|p| p == rel_path || p == &abs_path) {
                return Some(id);
            }
        }
        None
    }

    /// Return patch ids that were created more than `ttl_secs` ago.
    pub fn expired_patch_ids(&self, ttl_secs: u64) -> Vec<u64> {
        let now = now_ms();
        let cutoff = now.saturating_sub(ttl_secs * 1000);
        self.patches
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.created_ms < cutoff)
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn revert_session(&self, session: SessionId) -> usize {
        let ids: Vec<u64> = self
            .patches
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.session == session)
            .map(|(&k, _)| k)
            .collect();
        let mut n = 0usize;
        for id in ids {
            if self.revert(id, session).is_ok() {
                n += 1;
            }
        }
        n
    }

    /// Number of open patches (hardening / invariant checks).
    #[doc(hidden)]
    pub fn open_patch_count(&self) -> usize {
        self.patches.lock().unwrap().len()
    }

    /// Union of paths referenced by all open patches.
    #[doc(hidden)]
    pub fn all_open_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        for rec in self.patches.lock().unwrap().values() {
            for p in &rec.paths {
                if !paths.contains(p) {
                    paths.push(p.clone());
                }
            }
        }
        paths
    }

    /// Per-path open-patch reference counts.
    #[doc(hidden)]
    pub fn path_refcounts(&self) -> HashMap<String, u32> {
        let mut counts: HashMap<String, u32> = HashMap::new();
        for rec in self.patches.lock().unwrap().values() {
            for p in &rec.paths {
                *counts.entry(p.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// `(patch_id, path)` pairs for every open patch path.
    #[doc(hidden)]
    pub fn open_patch_paths(&self) -> Vec<(u64, String)> {
        let mut out = Vec::new();
        for (&id, rec) in self.patches.lock().unwrap().iter() {
            for p in &rec.paths {
                out.push((id, p.clone()));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_lock::merge_lock_holder;
    use crate::merge_preflight::MergePreflight;
    use cis_wal::{BranchId, MergeId};

    #[test]
    fn merge_blocked_while_spec_active() {
        let leases = Arc::new(PathLeaseManager::new());
        let spec = Arc::new(SpeculativePathTracker::new());
        let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let branch = BranchId([9u8; 16]);
        let mid = MergeId([10u8; 16]);
        patcher
            .apply_speculative(SessionId(1), vec!["src/m.rs".into()])
            .unwrap();
        let err = MergePreflight::begin_with_snapshot(
            Arc::clone(&kv),
            branch,
            mid,
            &["src/m.rs".into()],
            &spec,
            &[],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            crate::merge_preflight::MergePreflightError::SpeculativeConflict { .. }
        ));
        assert!(merge_lock_holder(&kv, branch).is_none());
        patcher.revert(1, SessionId(1)).unwrap();
        MergePreflight::begin_with_snapshot(
            Arc::clone(&kv),
            branch,
            mid,
            &["src/m.rs".into()],
            &spec,
            &[],
        )
        .unwrap();
        assert_eq!(merge_lock_holder(&kv, branch), Some(mid));
    }
}
