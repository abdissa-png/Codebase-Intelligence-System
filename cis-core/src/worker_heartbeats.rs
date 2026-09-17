//! Background worker liveness ticks for **`system_status`** (**Phase 5.4**).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Expected loop interval (seconds) per named worker for stale detection.
pub const WORKER_EXPECTED_INTERVAL_SECS: &[(&str, u64)] = &[
    ("cis-audit-epoch", 300),
    ("cis-wal-compaction", 600),
    ("cis-periodic-reconciler", 3600),
    ("cis-tombstone-gc", 3600),
    ("cis-disk-monitor", 30),
    ("cis-embedding-worker", 30),
    ("cis-vector-cleanup", 30),
    ("cis-merge-ttl-sweep", 60),
    ("cis-fs-sync", 30),
    ("cis-policy-watch", 60),
];

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WorkerHeartbeat {
    pub name: String,
    pub last_tick_ms: Option<u64>,
    pub seconds_since_last_tick: Option<u64>,
    pub expected_interval_secs: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct WorkerHeartbeatSummary {
    pub workers: Vec<WorkerHeartbeat>,
    pub any_stale: bool,
}

#[derive(Debug, Default)]
pub struct WorkerHeartbeats {
    ticks: BTreeMap<String, Arc<AtomicU64>>,
}

impl WorkerHeartbeats {
    pub fn new() -> Self {
        let mut ticks = BTreeMap::new();
        for (name, _) in WORKER_EXPECTED_INTERVAL_SECS {
            ticks.insert((*name).into(), Arc::new(AtomicU64::new(0)));
        }
        Self { ticks }
    }

    pub fn handle(&self, name: &str) -> Arc<AtomicU64> {
        self.ticks
            .get(name)
            .cloned()
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)))
    }

    pub fn tick(&self, name: &str) {
        let h = self.handle(name);
        h.store(now_ms(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> WorkerHeartbeatSummary {
        let now = now_ms();
        let mut workers = Vec::new();
        let mut any_stale = false;
        for (name, expected) in WORKER_EXPECTED_INTERVAL_SECS {
            let last = self
                .ticks
                .get(*name)
                .map(|a| a.load(Ordering::Relaxed))
                .unwrap_or(0);
            let (last_tick_ms, secs_since) = if last == 0 {
                (None, None)
            } else {
                let secs = now.saturating_sub(last) / 1000;
                (Some(last), Some(secs))
            };
            // Never-started workers are optional (e.g. `cis-policy-watch` when no
            // policy file exists). Do not treat last_tick=0 as stale.
            let stale = secs_since
                .map(|s| s > expected.saturating_mul(2))
                .unwrap_or(false);
            if stale {
                any_stale = true;
            }
            workers.push(WorkerHeartbeat {
                name: (*name).into(),
                last_tick_ms,
                seconds_since_last_tick: secs_since,
                expected_interval_secs: *expected,
                stale,
            });
        }
        WorkerHeartbeatSummary {
            workers,
            any_stale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn never_started_workers_are_not_stale() {
        let hb = WorkerHeartbeats::new();
        let snap = hb.snapshot();
        let policy = snap
            .workers
            .iter()
            .find(|w| w.name == "cis-policy-watch")
            .expect("policy-watch is listed");
        assert!(!policy.stale);
        assert!(!snap.any_stale);
        assert!(policy.last_tick_ms.is_none());
    }

    #[test]
    fn missed_ticks_are_stale() {
        let hb = WorkerHeartbeats::new();
        let h = hb.handle("cis-policy-watch");
        let old = now_ms().saturating_sub(10 * 60 * 1000);
        h.store(old, Ordering::Relaxed);
        let snap = hb.snapshot();
        let policy = snap
            .workers
            .iter()
            .find(|w| w.name == "cis-policy-watch")
            .unwrap();
        assert!(policy.stale);
        assert!(snap.any_stale);
    }
}
