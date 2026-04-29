//! Append-only **`MutationLog`** (`wal:` keys) with phase updates and compaction (**FR-1.7**).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use thiserror::Error;

use crate::phase::MutationPhase;
use crate::record::MutationRecord;

pub type LogId = u64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MutationLogError {
    #[error("unknown log id {0}")]
    UnknownLogId(LogId),
    #[error("illegal phase transition {0:?} -> {1:?}")]
    IllegalTransition(MutationPhase, MutationPhase),
    #[error("invariant violation: {0}")]
    Invariant(&'static str),
    #[error("wal persistence: {0}")]
    Persist(String),
}

#[derive(Debug, Default, Clone)]
pub struct WalCompactionReport {
    pub records_dropped: usize,
    pub bytes_estimated_freed: u64,
}

/// In-process implementation backed by an ordered map (production uses `KVStore` + `wal:{id}`).
#[derive(Debug)]
pub struct MutationLog {
    next_id: AtomicU64,
    records: RwLock<BTreeMap<LogId, MutationRecord>>,
    /// **DESIGNED:** approximate serialized JSON size per record for `wal_max_bytes` enforcement.
    approx_record_bytes: u64,
}

impl MutationLog {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            records: RwLock::new(BTreeMap::new()),
            approx_record_bytes: 256,
        }
    }

    pub fn with_params(first_log_id: LogId, approx_record_bytes: u64) -> Self {
        Self {
            next_id: AtomicU64::new(first_log_id),
            records: RwLock::new(BTreeMap::new()),
            approx_record_bytes: approx_record_bytes.max(64),
        }
    }

    /// Rebuild from a persisted snapshot (**FR-1.7** / NFR-R2).  
    /// `next_allocate_id` must be strictly greater than every `record.log_id`.
    pub fn restore(next_allocate_id: LogId, records: Vec<MutationRecord>) -> Result<Self, MutationLogError> {
        let mut map = BTreeMap::new();
        let mut max_seen: LogId = 0;
        for r in records {
            max_seen = max_seen.max(r.log_id);
            if map.insert(r.log_id, r).is_some() {
                return Err(MutationLogError::Invariant("duplicate log_id in restore snapshot"));
            }
        }
        if next_allocate_id <= max_seen {
            return Err(MutationLogError::Invariant(
                "restore next_allocate_id must exceed max log_id",
            ));
        }
        Ok(Self {
            next_id: AtomicU64::new(next_allocate_id),
            records: RwLock::new(map),
            approx_record_bytes: 256,
        })
    }

    /// Next `log_id` that **`append`** will allocate (for durable snapshots).
    pub fn next_allocate_id(&self) -> LogId {
        self.next_id.load(Ordering::SeqCst)
    }

    pub fn approx_record_bytes(&self) -> u64 {
        self.approx_record_bytes
    }

    /// Append a new row at the next `log_id`. `record.log_id` is overwritten to match allocation.
    pub fn append(&self, mut record: MutationRecord) -> Result<LogId, MutationLogError> {
        record.validate_invariants().map_err(MutationLogError::Invariant)?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        record.log_id = id;
        let mut g = self.records.write().unwrap();
        g.insert(id, record);
        Ok(id)
    }

    pub fn get(&self, id: LogId) -> Option<MutationRecord> {
        self.records.read().unwrap().get(&id).cloned()
    }

    pub fn update_phase(&self, id: LogId, phase: MutationPhase) -> Result<(), MutationLogError> {
        let mut g = self.records.write().unwrap();
        let rec = g
            .get_mut(&id)
            .ok_or(MutationLogError::UnknownLogId(id))?;
        if !rec.phase.can_transition_to(phase) {
            return Err(MutationLogError::IllegalTransition(rec.phase, phase));
        }
        rec.phase = phase;
        Ok(())
    }

    /// All records with `log_id >= first_pending` such that phase is in-flight, **plus** any tail
    /// the `ReconciliationEngine` must inspect (same ordering as map iteration).
    pub fn get_pending_tail(&self) -> Vec<MutationRecord> {
        let g = self.records.read().unwrap();
        g.values()
            .filter(|r| r.phase.is_in_flight())
            .cloned()
            .collect()
    }

    pub fn iter_all(&self) -> Vec<MutationRecord> {
        self.records.read().unwrap().values().cloned().collect()
    }

    /// **FR-1.7:** drop the lowest-`log_id` **compactable** rows while estimated size > `wal_max_bytes`.
    /// **DESIGNED:** `Committed` and `Failed` are both compactable terminals (release WAL retention and FR-1.13(d)
    /// "no PENDING mutation" — neither is in-flight).
    pub fn truncate_committed(&self, wal_max_bytes: u64) -> WalCompactionReport {
        let estimate = || -> u64 {
            let n = self.records.read().unwrap().len() as u64;
            n.saturating_mul(self.approx_record_bytes)
        };
        let mut dropped = 0usize;
        let mut freed: u64 = 0;
        while estimate() > wal_max_bytes {
            let oldest_compactable = {
                let g = self.records.read().unwrap();
                g.iter()
                    .filter(|(_, r)| {
                        r.phase == MutationPhase::Committed || r.phase == MutationPhase::Failed
                    })
                    .map(|(k, _)| *k)
                    .min()
            };
            let Some(key) = oldest_compactable else {
                break;
            };
            if let Some(r) = self.records.write().unwrap().remove(&key) {
                dropped += 1;
                freed = freed.saturating_add(self.approx_record_bytes);
                let _ = r;
            }
        }
        WalCompactionReport {
            records_dropped: dropped,
            bytes_estimated_freed: freed,
        }
    }
}

impl Default for MutationLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeRevisionId;
    use crate::record::MutationKind;

    fn rid(n: u8) -> NodeRevisionId {
        let mut b = [0u8; 16];
        b[15] = n;
        NodeRevisionId(b)
    }

    fn mk(phase: MutationPhase, revs: Vec<NodeRevisionId>) -> MutationRecord {
        MutationRecord {
            log_id: 0,
            kind: MutationKind::Single,
            phase,
            affected_revisions: revs,
            payload_checksum: [1u8; 32],
            created_at_ms: 42,
        }
    }

    #[test]
    fn append_update_phase_happy_path() {
        let wal = MutationLog::new();
        let r = rid(9);
        let id = wal.append(mk(MutationPhase::Pending, vec![r])).unwrap();
        wal.update_phase(id, MutationPhase::GraphDone).unwrap();
        wal.update_phase(id, MutationPhase::VectorDone).unwrap();
        wal.update_phase(id, MutationPhase::Committed).unwrap();
        assert!(wal.get_pending_tail().is_empty());
    }

    #[test]
    fn illegal_transition_rejected() {
        let wal = MutationLog::new();
        let r = rid(2);
        let id = wal.append(mk(MutationPhase::Pending, vec![r])).unwrap();
        let err = wal.update_phase(id, MutationPhase::Committed).unwrap_err();
        assert!(matches!(err, MutationLogError::IllegalTransition(_, _)));
    }

    #[test]
    fn compaction_drops_failed_like_committed() {
        let wal = MutationLog::with_params(1, 64);
        let _ = wal.append(mk(MutationPhase::Failed, vec![rid(1)])).unwrap();
        let _ = wal
            .append(mk(MutationPhase::Pending, vec![rid(2)]))
            .unwrap();
        let rep = wal.truncate_committed(50);
        assert!(rep.records_dropped >= 1);
        assert!(!wal.get_pending_tail().is_empty());
    }

    #[test]
    fn compaction_keeps_inflight() {
        let wal = MutationLog::with_params(1, 64);
        let r = rid(3);
        let id1 = wal.append(mk(MutationPhase::Committed, vec![r])).unwrap();
        let _id2 = wal
            .append(mk(MutationPhase::Pending, vec![rid(4)]))
            .unwrap();
        assert!(wal.get(id1).is_some());
        let rep = wal.truncate_committed(50);
        assert!(rep.records_dropped >= 1);
        assert!(!wal.get_pending_tail().is_empty());
    }
}
