//! **`OptimisticPatcher`** shell — **§01.5**, **US-04** (leases + speculative path registration).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::merge_lock::merge_lock_holder;
use crate::kv::MemoryKv;
use crate::path_lease::{LeaseError, PathLeaseManager, SessionId, SpeculativePathTracker};
use cis_wal::BranchId;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OptimisticPatchError {
    #[error(transparent)]
    Lease(#[from] LeaseError),
    #[error("423 Locked — merge preflight in progress")]
    MergeLocked,
    #[error("unknown patch {0}")]
    UnknownPatch(u64),
    #[error("session mismatch for patch {0}")]
    SessionMismatch(u64),
    /// Reverting an older patch while a newer open patch still touches the same path.
    #[error("409 conflict — patch {patch_id} superseded by newer open patch {newer_patch_id}")]
    Superseded {
        patch_id: u64,
        newer_patch_id: u64,
    },
}

fn path_matches_patch(rec_paths: &[String], rel_path: &str, abs_path: &str) -> bool {
    rec_paths
        .iter()
        .any(|p| p == rel_path || p == abs_path)
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
    ///
    /// When `merge_ctx` is `Some`, refuses new speculative paths while a merge lock is held
    /// on that branch (EI-5 — exactly one of preflight or speculative may win per path).
    pub fn apply_speculative(
        &self,
        session: SessionId,
        mut paths: Vec<String>,
        merge_ctx: Option<(&MemoryKv, BranchId)>,
    ) -> Result<u64, OptimisticPatchError> {
        paths.sort();
        self.spec_paths.with_merge_spec_gate(|| {
            if let Some((kv, branch)) = merge_ctx {
                if merge_lock_holder(kv, branch).is_some() {
                    return Err(OptimisticPatchError::MergeLocked);
                }
            }
            let mut acquired: Vec<&str> = Vec::with_capacity(paths.len());
            for p in &paths {
                if let Err(e) = self.leases.acquire(p, session) {
                    for prev in &acquired {
                        self.leases.release(prev, session);
                    }
                    return Err(OptimisticPatchError::Lease(e));
                }
                acquired.push(p.as_str());
            }
            if let Some((kv, branch)) = merge_ctx {
                if merge_lock_holder(kv, branch).is_some() {
                    for p in &acquired {
                        self.leases.release(p, session);
                    }
                    return Err(OptimisticPatchError::MergeLocked);
                }
            }
            for p in &paths {
                self.spec_paths.register(p.clone());
            }
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            self.patches.lock().unwrap().insert(
                id,
                PatchRecord {
                    session,
                    paths,
                    created_ms: now_ms(),
                },
            );
            Ok(id)
        })
    }

    pub fn revert(&self, patch_id: u64, session: SessionId) -> Result<(), OptimisticPatchError> {
        let rec = {
            let mut patches = self.patches.lock().unwrap();
            let Some(rec) = patches.get(&patch_id) else {
                return Err(OptimisticPatchError::UnknownPatch(patch_id));
            };
            if rec.session != session {
                return Err(OptimisticPatchError::SessionMismatch(patch_id));
            }
            patches.remove(&patch_id).expect("patch present after get")
        };
        for p in &rec.paths {
            self.spec_paths.unregister(p);
            self.leases.release(p, session);
        }
        Ok(())
    }

    /// **Epic 3.3 promote:** release leases + unregister speculative paths, returning the affected paths.
    /// Callers (MCP `confirm_patch`, FS sync) then transition graph revisions to `Active`.
    pub fn promote(&self, patch_id: u64, session: SessionId) -> Result<Vec<String>, OptimisticPatchError> {
        let rec = {
            let mut patches = self.patches.lock().unwrap();
            let Some(rec) = patches.get(&patch_id) else {
                return Err(OptimisticPatchError::UnknownPatch(patch_id));
            };
            if rec.session != session {
                return Err(OptimisticPatchError::SessionMismatch(patch_id));
            }
            patches.remove(&patch_id).expect("patch present after get")
        };
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

    /// Open patch ids whose registered paths contain `rel_path` (abs or rel), sorted ascending.
    pub fn open_patches_for_path(&self, rel_path: &str, repo_root: &str) -> Vec<u64> {
        let abs_path = format!("{}/{}", repo_root.trim_end_matches('/'), rel_path);
        let patches = self.patches.lock().unwrap();
        let mut ids: Vec<u64> = patches
            .iter()
            .filter(|(_, rec)| path_matches_patch(&rec.paths, rel_path, &abs_path))
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Newest (max id) pending patch whose registered paths contain `rel_path`.
    ///
    /// Prefer this over HashMap iteration order so FS sync confirms the patch matching
    /// current disk content when multiple open patches briefly overlap.
    pub fn pending_patch_for_path(&self, rel_path: &str, repo_root: &str) -> Option<u64> {
        self.open_patches_for_path(rel_path, repo_root).into_iter().max()
    }

    /// If any open patch with a higher id shares a path with `patch_id`, return that newer id.
    pub fn newer_open_patch_on_paths(&self, patch_id: u64) -> Option<u64> {
        let patches = self.patches.lock().unwrap();
        let Some(rec) = patches.get(&patch_id) else {
            return None;
        };
        let my_paths: &Vec<String> = &rec.paths;
        patches
            .iter()
            .filter(|(&id, other)| {
                id > patch_id && other.paths.iter().any(|p| my_paths.iter().any(|mp| mp == p))
            })
            .map(|(&id, _)| id)
            .max()
    }

    /// Same-session open patches on `rel_path`, sorted ascending (oldest first).
    pub fn same_session_open_patches_for_path(
        &self,
        session: SessionId,
        rel_path: &str,
        repo_root: &str,
    ) -> Vec<u64> {
        let abs_path = format!("{}/{}", repo_root.trim_end_matches('/'), rel_path);
        let patches = self.patches.lock().unwrap();
        let mut ids: Vec<u64> = patches
            .iter()
            .filter(|(_, rec)| {
                rec.session == session && path_matches_patch(&rec.paths, rel_path, &abs_path)
            })
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
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
            .apply_speculative(SessionId(1), vec!["src/m.rs".into()], None)
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

    #[test]
    fn speculative_blocked_while_merge_lock_held() {
        let leases = Arc::new(PathLeaseManager::new());
        let spec = Arc::new(SpeculativePathTracker::new());
        let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let branch = BranchId([11u8; 16]);
        let mid = MergeId([12u8; 16]);
        crate::merge_lock::acquire_merge_lock(&kv, branch, mid).unwrap();
        let err = patcher
            .apply_speculative(
                SessionId(1),
                vec!["src/m.rs".into()],
                Some((&kv, branch)),
            )
            .unwrap_err();
        assert_eq!(err, OptimisticPatchError::MergeLocked);
        assert!(!spec.intersects(&["src/m.rs".into()]));
        crate::merge_lock::release_merge_lock(&kv, branch, mid).unwrap();
        patcher
            .apply_speculative(
                SessionId(1),
                vec!["src/m.rs".into()],
                Some((&kv, branch)),
            )
            .unwrap();
    }

    #[test]
    fn partial_acquire_conflict_releases_earlier_leases() {
        let leases = Arc::new(PathLeaseManager::new());
        let spec = Arc::new(SpeculativePathTracker::new());
        let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
        // Hold "b.rs" as another session so multi-path acquire fails mid-loop
        // after "a.rs" is acquired (paths are sorted).
        leases.acquire("b.rs", SessionId(99)).unwrap();
        let err = patcher
            .apply_speculative(
                SessionId(1),
                vec!["b.rs".into(), "a.rs".into()],
                None,
            )
            .unwrap_err();
        assert!(matches!(err, OptimisticPatchError::Lease(_)));
        assert!(
            leases.holder("a.rs").is_none(),
            "a.rs lease must be released after mid-acquire conflict"
        );
        assert_eq!(leases.holder("b.rs"), Some(SessionId(99)));
        assert_eq!(patcher.open_patch_count(), 0);
        assert!(!spec.intersects(&["a.rs".into(), "b.rs".into()]));
    }

    #[test]
    fn session_mismatch_leaves_patch_intact() {
        let leases = Arc::new(PathLeaseManager::new());
        let spec = Arc::new(SpeculativePathTracker::new());
        let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
        let id = patcher
            .apply_speculative(SessionId(1), vec!["a.rs".into()], None)
            .unwrap();
        assert_eq!(
            patcher.revert(id, SessionId(99)),
            Err(OptimisticPatchError::SessionMismatch(id))
        );
        assert_eq!(patcher.open_patch_count(), 1);
        assert_eq!(
            patcher.promote(id, SessionId(99)),
            Err(OptimisticPatchError::SessionMismatch(id))
        );
        assert_eq!(patcher.open_patch_count(), 1);
        patcher.revert(id, SessionId(1)).unwrap();
        assert_eq!(patcher.open_patch_count(), 0);
    }

    #[test]
    fn pending_patch_for_path_returns_newest() {
        let leases = Arc::new(PathLeaseManager::new());
        let spec = Arc::new(SpeculativePathTracker::new());
        let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
        let root = "/repo";
        let abs = "/repo/shared.py";
        let a = patcher
            .apply_speculative(SessionId(1), vec![abs.into()], None)
            .unwrap();
        let b = patcher
            .apply_speculative(SessionId(1), vec![abs.into()], None)
            .unwrap();
        assert!(b > a);
        assert_eq!(
            patcher.pending_patch_for_path("shared.py", root),
            Some(b)
        );
        assert_eq!(
            patcher.open_patches_for_path("shared.py", root),
            vec![a, b]
        );
        assert_eq!(patcher.newer_open_patch_on_paths(a), Some(b));
        assert_eq!(patcher.newer_open_patch_on_paths(b), None);
    }
}
