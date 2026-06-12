//! **`BranchReconciliationTracker`** — **RC-4** / **H-9** branch-level pending background jobs.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use cis_wal::BranchId;

#[derive(Debug, Default)]
pub struct BranchReconciliationTracker {
    jobs: Mutex<HashMap<BranchId, HashSet<u64>>>,
}

impl BranchReconciliationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, branch_id: BranchId, job_id: u64) {
        self.jobs
            .lock()
            .unwrap()
            .entry(branch_id)
            .or_default()
            .insert(job_id);
    }

    pub fn on_job_complete(&self, branch_id: BranchId, job_id: u64) {
        let mut g = self.jobs.lock().unwrap();
        if let Some(set) = g.get_mut(&branch_id) {
            set.remove(&job_id);
            if set.is_empty() {
                g.remove(&branch_id);
            }
        }
    }

    /// **`meta.background_reconciliation_pending`** at branch granularity (v2.6).
    pub fn is_pending(&self, branch_id: BranchId) -> bool {
        self.jobs
            .lock()
            .unwrap()
            .get(&branch_id)
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clears_when_last_job_finishes() {
        let t = BranchReconciliationTracker::new();
        let b = BranchId([1u8; 16]);
        t.register(b, 10);
        assert!(t.is_pending(b));
        t.on_job_complete(b, 10);
        assert!(!t.is_pending(b));
    }
}
