//! Cached last consistency-check summary for **`system_status`** (**Phase 5.4**).

use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::graph_consistency::ConsistencyReport;

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct ConsistencyStatusSummary {
    pub checked_at_ms: Option<u64>,
    pub clean: bool,
    pub summary: String,
    pub dangling_bindings: usize,
    pub orphaned_active: usize,
    pub tombstones_still_bound: usize,
    pub duplicate_active: usize,
    pub missing_body: usize,
    pub index_desync: usize,
}

impl ConsistencyStatusSummary {
    pub fn from_report(report: &ConsistencyReport, checked_at_ms: u64) -> Self {
        Self {
            checked_at_ms: Some(checked_at_ms),
            clean: report.is_clean(),
            summary: report.summary(),
            dangling_bindings: report.dangling_bindings.len(),
            orphaned_active: report.orphaned_active_without_binding.len(),
            tombstones_still_bound: report.tombstones_still_bound.len(),
            duplicate_active: report.duplicate_active_per_identity.len(),
            missing_body: report.body_hash_missing_from_store.len(),
            index_desync: report.secondary_index_desync.len(),
        }
    }

    pub fn never_checked() -> Self {
        Self {
            summary: "consistency: never checked".into(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LastConsistencySnapshot {
    inner: Arc<Mutex<ConsistencyStatusSummary>>,
}

impl LastConsistencySnapshot {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ConsistencyStatusSummary::never_checked())),
        }
    }

    pub fn update(&self, report: &ConsistencyReport, checked_at_ms: u64) {
        *self.inner.lock().unwrap() =
            ConsistencyStatusSummary::from_report(report, checked_at_ms);
    }

    pub fn read(&self) -> ConsistencyStatusSummary {
        self.inner.lock().unwrap().clone()
    }

    pub fn arc(&self) -> Arc<Mutex<ConsistencyStatusSummary>> {
        Arc::clone(&self.inner)
    }
}
