//! **FR-1.15** + **EI-5:** merge lock **before** speculative quiescence; release lock on speculative hit.

use std::sync::Arc;

use cis_wal::{BranchId, IdentityId, MergeId, NodeRevisionId};
use thiserror::Error;

use crate::kv::{CasError, MemoryKv};
use crate::merge_control::MergeControl;
use crate::merge_lock::{acquire_merge_lock, merge_lock_holder, release_merge_lock};
use crate::path_lease::SpeculativePathTracker;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MergePreflightError {
    #[error(transparent)]
    Cas(#[from] CasError),
    #[error("423 Locked — merge in progress (blocking merge_id present)")]
    Locked { holder: MergeId },
    #[error("409 Conflict — speculative patches on merge paths (EI-5)")]
    SpeculativeConflict { paths: Vec<String> },
}

pub struct MergePreflight;

impl MergePreflight {
    /// 1. `merge_lock:{branch}` CAS (**held before** speculative check, per spec).  
    /// 2. If any `affected_paths` ∈ active speculative set → **release lock** and return **`409`**.  
    /// 3. Record pre-merge `ri:` snapshot via **`MergeControl`**.
    pub fn begin_with_snapshot(
        kv: Arc<MemoryKv>,
        branch_id: BranchId,
        merge_id: MergeId,
        affected_paths: &[String],
        spec: &SpeculativePathTracker,
        pre_bindings: &[(IdentityId, NodeRevisionId)],
    ) -> Result<(), MergePreflightError> {
        Self::acquire_lock_then_quiesce_speculative(&kv, branch_id, merge_id, affected_paths, spec)?;
        let mc = MergeControl::new(Arc::clone(&kv));
        mc.record_premerge_bindings(merge_id, branch_id, pre_bindings);
        Ok(())
    }

    pub fn acquire_lock_then_quiesce_speculative(
        kv: &MemoryKv,
        branch_id: BranchId,
        merge_id: MergeId,
        affected_paths: &[String],
        spec: &SpeculativePathTracker,
    ) -> Result<(), MergePreflightError> {
        match acquire_merge_lock(kv, branch_id, merge_id) {
            Ok(()) => {}
            Err(CasError::Mismatch(_)) => {
                let holder = merge_lock_holder(kv, branch_id)
                    .ok_or_else(|| CasError::Mismatch("merge_lock".into()))?;
                return Err(MergePreflightError::Locked { holder });
            }
        }
        let conflicts = spec.conflicting_paths(affected_paths);
        if !conflicts.is_empty() {
            release_merge_lock(kv, branch_id, merge_id)?;
            return Err(MergePreflightError::SpeculativeConflict { paths: conflicts });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_lock::acquire_merge_lock;
    use cis_wal::NodeRevisionId;

    #[test]
    fn speculative_forces_release() {
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([1u8; 16]);
        let m = MergeId([2u8; 16]);
        let spec = SpeculativePathTracker::new();
        spec.register("p/x.ts");
        let err = MergePreflight::acquire_lock_then_quiesce_speculative(
            &kv,
            branch,
            m,
            &["p/x.ts".into()],
            &spec,
        )
        .unwrap_err();
        assert!(matches!(err, MergePreflightError::SpeculativeConflict { .. }));
        assert!(merge_lock_holder(&kv, branch).is_none());
    }

    #[test]
    fn speculative_refcount_blocks_until_all_patches_gone() {
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([1u8; 16]);
        let m = MergeId([2u8; 16]);
        let spec = SpeculativePathTracker::new();
        spec.register("p/x.ts");
        spec.register("p/x.ts");
        spec.unregister("p/x.ts");
        let err = MergePreflight::acquire_lock_then_quiesce_speculative(
            &kv,
            branch,
            m,
            &["p/x.ts".into()],
            &spec,
        )
        .unwrap_err();
        assert!(matches!(err, MergePreflightError::SpeculativeConflict { .. }));
        spec.unregister("p/x.ts");
        MergePreflight::acquire_lock_then_quiesce_speculative(
            &kv,
            branch,
            m,
            &["p/x.ts".into()],
            &spec,
        )
        .expect("merge allowed after all speculative refs cleared");
    }

    #[test]
    fn locked_when_merge_holder_differs() {
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([1u8; 16]);
        let m1 = MergeId([2u8; 16]);
        let m2 = MergeId([3u8; 16]);
        acquire_merge_lock(&kv, branch, m1).unwrap();
        let err = MergePreflight::acquire_lock_then_quiesce_speculative(
            &kv,
            branch,
            m2,
            &[],
            &SpeculativePathTracker::new(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            MergePreflightError::Locked {
                holder: m1
            }
        );
    }

    #[test]
    fn begin_snapshot_happy_path() {
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([5u8; 16]);
        let mid = MergeId([6u8; 16]);
        let i = IdentityId([7u8; 16]);
        let r = NodeRevisionId([8u8; 16]);
        MergePreflight::begin_with_snapshot(
            Arc::clone(&kv),
            branch,
            mid,
            &["a.py".into()],
            &SpeculativePathTracker::new(),
            &[(i, r)],
        )
        .unwrap();
        assert_eq!(merge_lock_holder(&kv, branch), Some(mid));
        let rows = kv.scan_prefix("msnap:");
        assert_eq!(rows.len(), 1);
    }
}
