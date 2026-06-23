//! Embedding queue drain-rate observability (**Phase 5.3**).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::embedding_queue::EmbeddingQueueState;
use crate::embedding_worker::EmbeddingDrainReport;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EmbeddingStatusSnapshot {
    pub queue_depth: usize,
    pub queue_state: String,
    pub hwm: u32,
    pub lwm: u32,
    pub drains_per_minute: f64,
    pub embedded_per_minute: f64,
    pub last_drain_at_ms: Option<u64>,
    pub last_drain_attempted: usize,
    pub last_drain_embedded_ok: usize,
}

impl Default for EmbeddingStatusSnapshot {
    fn default() -> Self {
        Self {
            queue_depth: 0,
            queue_state: "NORMAL".into(),
            hwm: 0,
            lwm: 0,
            drains_per_minute: 0.0,
            embedded_per_minute: 0.0,
            last_drain_at_ms: None,
            last_drain_attempted: 0,
            last_drain_embedded_ok: 0,
        }
    }
}

fn queue_state_label(state: EmbeddingQueueState) -> String {
    match state {
        EmbeddingQueueState::Normal => "NORMAL".into(),
        EmbeddingQueueState::BackpressureHigh => "BACKPRESSURE_HIGH".into(),
        EmbeddingQueueState::Outage => "OUTAGE".into(),
    }
}

/// Rolling drain metrics updated by the embedding worker thread.
#[derive(Debug, Default)]
pub struct EmbeddingMetrics {
    last_drain_at_ms: AtomicU64,
    last_drain_report: Mutex<EmbeddingDrainReport>,
    drains_last_minute: Mutex<VecDeque<(u64, EmbeddingDrainReport)>>,
}

impl EmbeddingMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_drain(&self, report: EmbeddingDrainReport) {
        let ts = now_ms();
        self.last_drain_at_ms.store(ts, Ordering::Relaxed);
        *self.last_drain_report.lock().unwrap() = report;
        let mut q = self.drains_last_minute.lock().unwrap();
        q.push_back((ts, report));
        let cutoff = ts.saturating_sub(60_000);
        while q.front().is_some_and(|(t, _)| *t < cutoff) {
            q.pop_front();
        }
    }

    pub fn snapshot(
        &self,
        depth: usize,
        state: EmbeddingQueueState,
        hwm: u32,
        lwm: u32,
    ) -> EmbeddingStatusSnapshot {
        let now = now_ms();
        let cutoff = now.saturating_sub(60_000);
        let q = self.drains_last_minute.lock().unwrap();
        let mut drain_count = 0u64;
        let mut embedded_total = 0u64;
        for (ts, rep) in q.iter() {
            if *ts >= cutoff && rep.attempted > 0 {
                drain_count += 1;
                embedded_total += rep.embedded_ok as u64;
            }
        }
        let last = self.last_drain_report.lock().unwrap();
        let last_ts = self.last_drain_at_ms.load(Ordering::Relaxed);
        EmbeddingStatusSnapshot {
            queue_depth: depth,
            queue_state: queue_state_label(state),
            hwm,
            lwm,
            drains_per_minute: drain_count as f64,
            embedded_per_minute: embedded_total as f64,
            last_drain_at_ms: if last_ts == 0 { None } else { Some(last_ts) },
            last_drain_attempted: last.attempted,
            last_drain_embedded_ok: last.embedded_ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_rate_counts_non_empty_drains_in_window() {
        let m = EmbeddingMetrics::new();
        m.record_drain(EmbeddingDrainReport {
            attempted: 3,
            embedded_ok: 2,
            ..Default::default()
        });
        m.record_drain(EmbeddingDrainReport::default());
        let snap = m.snapshot(5, EmbeddingQueueState::Normal, 5000, 1000);
        assert_eq!(snap.drains_per_minute, 1.0);
        assert_eq!(snap.embedded_per_minute, 2.0);
        assert_eq!(snap.queue_depth, 5);
        assert_eq!(snap.queue_state, "NORMAL");
    }
}
