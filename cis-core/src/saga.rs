//! Merge **saga** KV schema `saga_state:{merge_id}` (v2.6). Full batch compensate lives in merge engine.

use std::sync::Arc;

use cis_wal::MergeId;

use crate::kv::MemoryKv;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaPhase {
    Intent,
    Classifying,
    Promoting,
    EdgeBatch { seq: u32 },
    PointerSwap,
    Committed,
}

#[derive(Debug)]
pub struct MergeSagaOrchestrator {
    kv: Arc<MemoryKv>,
}

impl MergeSagaOrchestrator {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    fn apply_saga_fault(&self, merge_id: MergeId) -> bool {
        let inj = self.kv.fault_injector();
        crate::fault_injection::apply_fault(inj.before_saga_persist(merge_id)).is_ok()
    }

    pub fn persist_batch_marker(&self, merge_id: MergeId, batch_seq: u32) {
        if !self.apply_saga_fault(merge_id) {
            return;
        }
        let k = Self::batch_key(merge_id, batch_seq);
        self.kv.set(&k, vec![1u8]);
    }

    pub fn batch_key(merge_id: MergeId, batch_seq: u32) -> String {
        format!(
            "saga_batch:{}:{}",
            merge_id
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            batch_seq
        )
    }

    fn key(merge_id: MergeId) -> String {
        format!(
            "saga_state:{}",
            merge_id
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        )
    }

    pub fn persist(&self, merge_id: MergeId, phase: SagaPhase) {
        if !self.apply_saga_fault(merge_id) {
            return;
        }
        let (tag, seq) = match phase {
            SagaPhase::Intent => (1u8, 0u32),
            SagaPhase::Classifying => (2, 0),
            SagaPhase::Promoting => (3, 0),
            SagaPhase::EdgeBatch { seq } => (4, seq),
            SagaPhase::PointerSwap => (5, 0),
            SagaPhase::Committed => (6, 0),
        };
        let mut v = vec![tag];
        v.extend_from_slice(&seq.to_le_bytes());
        self.kv.set(&Self::key(merge_id), v);
    }

    pub fn load(&self, merge_id: MergeId) -> Option<SagaPhase> {
        let v = self.kv.get(&Self::key(merge_id))?;
        if v.is_empty() {
            return None;
        }
        let seq = if v.len() >= 5 {
            u32::from_le_bytes(v[1..5].try_into().ok()?)
        } else {
            0
        };
        Some(match v[0] {
            1 => SagaPhase::Intent,
            2 => SagaPhase::Classifying,
            3 => SagaPhase::Promoting,
            4 => SagaPhase::EdgeBatch { seq },
            5 => SagaPhase::PointerSwap,
            6 => SagaPhase::Committed,
            _ => return None,
        })
    }

    fn merge_id_hex(merge_id: MergeId) -> String {
        merge_id
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }

    fn merge_id_from_saga_key(k: &str) -> Option<MergeId> {
        let hex = k.strip_prefix("saga_state:")?;
        if hex.len() != 32 {
            return None;
        }
        let mut b = [0u8; 16];
        for i in 0..16 {
            b[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(MergeId(b))
    }

    /// Remove **`saga_state`**, **`saga_batch`**, and **`saga_payload`** keys for one merge.
    pub fn purge_merge_saga_state(&self, merge_id: MergeId) {
        let hex = Self::merge_id_hex(merge_id);
        let batch_prefix = format!("saga_batch:{}:", hex);
        for (k, _) in self.kv.scan_prefix(&batch_prefix) {
            self.kv.delete(&k);
        }
        crate::merge_saga_batch::purge_saga_edge_payloads(&self.kv, merge_id);
        self.kv.delete(&Self::key(merge_id));
    }

    /// Release any durable `merge_lock:` held by `merge_id` (and its `merge_started` stamp).
    fn release_locks_for_merge(&self, merge_id: MergeId) {
        for (branch, holder) in crate::merge_lock::scan_merge_lock_holders(&self.kv) {
            if holder == merge_id {
                let _ = crate::merge_lock::release_merge_lock(&self.kv, branch, merge_id);
            }
        }
        // Defensive: clear start stamp even if lock key was already gone.
        self.kv
            .delete(&crate::merge_lock::merge_started_key(merge_id));
    }

    /// Drop every saga not in **`Committed`**, releasing any associated merge locks.
    /// Prefer calling [`crate::merge_engine::recover_inflight_merges`] first so resumable
    /// merges are attempted before compensation.
    pub fn compensate_orphans(&self) -> usize {
        let rows = self.kv.scan_prefix("saga_state:");
        let mut n = 0usize;
        for (k, v) in rows {
            if v.first().copied() == Some(6) {
                continue;
            }
            if let Some(mid) = Self::merge_id_from_saga_key(&k) {
                self.release_locks_for_merge(mid);
                self.purge_merge_saga_state(mid);
            } else {
                self.kv.delete(&k);
            }
            n += 1;
        }
        // Also clear merge locks that have no saga at all (stuck after crash).
        for (branch, merge_id) in crate::merge_lock::scan_merge_lock_holders(&self.kv) {
            if self.load(merge_id).is_none() {
                let _ = crate::merge_lock::release_merge_lock(&self.kv, branch, merge_id);
                n += 1;
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saga_fault_injector_skips_persist() {
        use crate::fault_injection::{AlwaysFail, FailHooks};
        let kv = Arc::new(MemoryKv::new());
        kv.set_fault_injector(Arc::new(AlwaysFail {
            hooks: FailHooks(FailHooks::SAGA_PERSIST),
        }));
        let o = MergeSagaOrchestrator::new(kv.clone());
        let mid = MergeId([13u8; 16]);
        o.persist(mid, SagaPhase::Intent);
        assert!(o.load(mid).is_none());
    }

    #[test]
    fn roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let o = MergeSagaOrchestrator::new(kv);
        let mid = MergeId([11u8; 16]);
        o.persist(mid, SagaPhase::EdgeBatch { seq: 3 });
        assert_eq!(o.load(mid), Some(SagaPhase::EdgeBatch { seq: 3 }));
    }

    #[test]
    fn saga_batch_idempotency_key() {
        let kv = Arc::new(MemoryKv::new());
        let o = MergeSagaOrchestrator::new(Arc::clone(&kv));
        let mid = MergeId([12u8; 16]);
        o.persist_batch_marker(mid, 7);
        assert!(kv
            .get(&MergeSagaOrchestrator::batch_key(mid, 7))
            .is_some());
    }
}
