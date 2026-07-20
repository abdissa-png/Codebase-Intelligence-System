//! Bounded embedding queue with HWM/LWM (**§01.4**).

use std::collections::VecDeque;

use cis_wal::LogId;
use serde::{Deserialize, Serialize};

/// Default **DERIVED** §01.4: HWM 5000, LWM 1000.
pub const DEFAULT_HWM: usize = 5000;
pub const DEFAULT_LWM: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EmbeddingQueueState {
    Normal,
    BackpressureHigh,
    Outage,
}

#[derive(Debug, Clone)]
pub struct EmbedJob {
    pub wal_log_id: LogId,
    pub chunk_id: [u8; 32],
    pub text_digest: [u8; 32],
}

#[derive(Debug, Default)]
pub struct EmbeddingQueue {
    depth: usize,
    pub hwm: usize,
    pub lwm: usize,
    pending: VecDeque<EmbedJob>,
}

impl EmbeddingQueue {
    pub fn new() -> Self {
        Self::with_thresholds(DEFAULT_HWM, DEFAULT_LWM)
    }

    pub fn with_thresholds(hwm: usize, lwm: usize) -> Self {
        Self {
            depth: 0,
            hwm,
            lwm,
            pending: VecDeque::new(),
        }
    }

    pub fn enqueue(&mut self, job: EmbedJob) {
        self.pending.push_back(job);
        self.depth = self.pending.len();
    }

    pub fn drain_one(&mut self) -> Option<EmbedJob> {
        let j = self.pending.pop_front();
        self.depth = self.pending.len();
        j
    }

    pub fn depth(&self) -> usize {
        self.pending.len()
    }

    pub fn state(&self) -> EmbeddingQueueState {
        if self.depth() > self.hwm {
            EmbeddingQueueState::BackpressureHigh
        } else {
            EmbeddingQueueState::Normal
        }
    }

    /// Recovery: enqueue vector retry jobs at the front (O(1)).
    pub fn reenqueue_front(&mut self, job: EmbedJob) {
        self.pending.push_front(job);
        self.depth = self.pending.len();
    }
}
