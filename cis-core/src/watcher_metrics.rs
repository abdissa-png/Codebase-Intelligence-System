//! FS watcher observability: event counts, debounce delays, missed-event sampling (**Phase 5.1**).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::Serialize;

const DELAY_RING_CAP: usize = 512;

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct WatcherStatusSnapshot {
    pub raw_events: u64,
    pub scheduled_events: u64,
    pub coalesced_events: u64,
    pub reindex_batches: u64,
    pub reindexed_files: u64,
    pub missed_event_samples: u64,
    pub pending_count: usize,
    pub pending_high_water: u64,
    pub debounce_p50_ms: Option<u64>,
    pub debounce_p99_ms: Option<u64>,
    pub last_sample_at_ms: Option<u64>,
}

fn percentile(sorted: &[u64], p: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    Some(sorted[idx.min(sorted.len() - 1)])
}

/// Thread-safe watcher counters and debounce delay ring buffer.
#[derive(Debug, Default)]
pub struct WatcherMetrics {
    raw_events: AtomicU64,
    scheduled_events: AtomicU64,
    coalesced_events: AtomicU64,
    reindex_batches: AtomicU64,
    reindexed_files: AtomicU64,
    missed_event_samples: AtomicU64,
    pending_high_water: AtomicU64,
    last_sample_at_ms: AtomicU64,
    debounce_delays_ms: Mutex<VecDeque<u64>>,
}

impl WatcherMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_raw_events(&self, n: u64) {
        self.raw_events.fetch_add(n, Ordering::Relaxed);
    }

    /// Returns true if the path already had a pending entry (coalesced).
    pub fn record_schedule(&self, had_existing: bool) {
        self.scheduled_events.fetch_add(1, Ordering::Relaxed);
        if had_existing {
            self.coalesced_events.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_debounce_delays(&self, delays_ms: impl IntoIterator<Item = u64>) {
        let mut ring = self.debounce_delays_ms.lock().unwrap();
        for d in delays_ms {
            if ring.len() >= DELAY_RING_CAP {
                ring.pop_front();
            }
            ring.push_back(d);
        }
    }

    pub fn record_reindex_batch(&self, file_count: usize) {
        self.reindex_batches.fetch_add(1, Ordering::Relaxed);
        self.reindexed_files
            .fetch_add(file_count as u64, Ordering::Relaxed);
    }

    pub fn record_missed_sample(&self) {
        self.missed_event_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_last_sample_at_ms(&self, ms: u64) {
        self.last_sample_at_ms.store(ms, Ordering::Relaxed);
    }

    pub fn update_pending_count(&self, count: usize) {
        let c = count as u64;
        let mut cur = self.pending_high_water.load(Ordering::Relaxed);
        while c > cur {
            match self.pending_high_water.compare_exchange_weak(
                cur,
                c,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    pub fn snapshot(&self, pending_count: usize) -> WatcherStatusSnapshot {
        self.update_pending_count(pending_count);
        let mut delays: Vec<u64> = self
            .debounce_delays_ms
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect();
        delays.sort_unstable();
        let last_ms = self.last_sample_at_ms.load(Ordering::Relaxed);
        WatcherStatusSnapshot {
            raw_events: self.raw_events.load(Ordering::Relaxed),
            scheduled_events: self.scheduled_events.load(Ordering::Relaxed),
            coalesced_events: self.coalesced_events.load(Ordering::Relaxed),
            reindex_batches: self.reindex_batches.load(Ordering::Relaxed),
            reindexed_files: self.reindexed_files.load(Ordering::Relaxed),
            missed_event_samples: self.missed_event_samples.load(Ordering::Relaxed),
            pending_count,
            pending_high_water: self.pending_high_water.load(Ordering::Relaxed),
            debounce_p50_ms: percentile(&delays, 0.50),
            debounce_p99_ms: percentile(&delays, 0.99),
            last_sample_at_ms: if last_ms == 0 {
                None
            } else {
                Some(last_ms)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescing_and_percentiles() {
        let m = WatcherMetrics::new();
        m.record_schedule(false);
        m.record_schedule(true);
        m.record_debounce_delays([10, 20, 30, 40, 100]);
        let snap = m.snapshot(2);
        assert_eq!(snap.scheduled_events, 2);
        assert_eq!(snap.coalesced_events, 1);
        assert_eq!(snap.debounce_p50_ms, Some(30));
        assert_eq!(snap.pending_count, 2);
    }
}
