//! Scheduled WAL compaction (**Phase 3.2**).

use std::sync::Arc;
use std::time::Duration;

use cis_wal::{MutationLogStore, WalCompactionReport};

use crate::merge_gate::MergeRecoveryGate;
use crate::security::ProductionAuditSink;
use crate::{MemoryKv, MergeSagaOrchestrator, SagaPhase};

/// Background WAL compaction scheduler.
#[derive(Debug, Clone)]
pub struct WalCompactionScheduler {
    pub interval: Duration,
    pub min_bytes_before_compact: u64,
}

impl Default for WalCompactionScheduler {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(600),
            min_bytes_before_compact: 1024 * 1024,
        }
    }
}

impl WalCompactionScheduler {
    pub fn new(interval: Duration, min_bytes_before_compact: u64) -> Self {
        Self {
            interval,
            min_bytes_before_compact,
        }
    }

    /// Returns `None` when preconditions block compaction or WAL is below threshold.
    pub fn run_once(
        &self,
        wal: &dyn MutationLogStore,
        wal_max_bytes: u64,
        kv: &MemoryKv,
        saga: &MergeSagaOrchestrator,
        gate: &MergeRecoveryGate,
        audit: &ProductionAuditSink,
    ) -> Option<WalCompactionReport> {
        if !Self::preconditions_ok(kv, saga, gate) {
            return None;
        }
        let approx = wal.record_count() as u64 * 256;
        if approx < self.min_bytes_before_compact.max(wal_max_bytes / 4) {
            return None;
        }
        let report = wal.truncate_committed(wal_max_bytes).ok()?;
        if report.records_dropped > 0 {
            audit.record_sync(
                0,
                format!(
                    "wal_compaction removed={} freed_est={}",
                    report.records_dropped, report.bytes_estimated_freed
                ),
            );
        }
        Some(report)
    }

    fn preconditions_ok(kv: &MemoryKv, saga: &MergeSagaOrchestrator, gate: &MergeRecoveryGate) -> bool {
        if gate.any_blocked() {
            return false;
        }
        for (key, val) in kv.scan_prefix("saga_state:") {
            if val.is_empty() {
                continue;
            }
            let merge_hex = key.strip_prefix("saga_state:").unwrap_or("");
            if merge_hex.len() != 32 {
                continue;
            }
            let mut mid = [0u8; 16];
            for i in 0..16 {
                if let Ok(b) = u8::from_str_radix(&merge_hex[i * 2..i * 2 + 2], 16) {
                    mid[i] = b;
                }
            }
            if let Some(phase) = saga.load(cis_wal::MergeId(mid)) {
                if phase != SagaPhase::Committed {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cis_wal::{
        MergeId, MutationKind, MutationLog, MutationPhase, MutationRecord, MutationLogStore,
        NodeRevisionId,
    };

    use crate::merge_gate::MergeRecoveryGate;
    use crate::saga::{MergeSagaOrchestrator, SagaPhase};
    use crate::security::ProductionAuditSink;
    use crate::MemoryKv;

    fn mk(phase: MutationPhase, n: u8) -> MutationRecord {
        MutationRecord {
            log_id: 0,
            kind: MutationKind::Single,
            phase,
            affected_revisions: vec![NodeRevisionId([n; 16])],
            payload_checksum: [0u8; 32],
            created_at_ms: 0,
        }
    }

    fn audit_sink() -> Arc<ProductionAuditSink> {
        let dir = std::env::temp_dir().join(format!("cis-wal-audit-{}", std::process::id()));
        ProductionAuditSink::new(dir.join("audit.jsonl"))
    }

    #[test]
    fn compaction_drops_committed_only() {
        let wal = MutationLog::with_params(1, 64);
        for i in 1..=5u8 {
            wal.append(mk(MutationPhase::Committed, i)).unwrap();
        }
        let rep = wal.truncate_committed(50);
        assert!(rep.records_dropped >= 1);
        wal.append(mk(MutationPhase::Pending, 99)).unwrap();
        assert!(!wal.get_pending_tail().is_empty());
    }

    #[test]
    fn scheduler_blocks_during_merge_recovery() {
        let wal = MutationLog::with_params(1, 64);
        for i in 1..=20u8 {
            wal.append(mk(MutationPhase::Committed, i)).unwrap();
        }
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
        let gate = MergeRecoveryGate::new(Arc::clone(&kv));
        gate.begin_rollback(cis_wal::BranchId([1u8; 16]));
        let sched = WalCompactionScheduler::new(Duration::from_secs(600), 0);
        let rep = sched.run_once(
            &wal,
            1024,
            &kv,
            &saga,
            &gate,
            &audit_sink(),
        );
        assert!(rep.is_none(), "compaction must not run during merge recovery");
    }

    #[test]
    fn scheduler_blocks_during_inflight_saga() {
        let wal = MutationLog::with_params(1, 64);
        for i in 1..=20u8 {
            wal.append(mk(MutationPhase::Committed, i)).unwrap();
        }
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
        let gate = MergeRecoveryGate::new(Arc::clone(&kv));
        let mid = MergeId([7u8; 16]);
        saga.persist(mid, SagaPhase::Promoting);
        let sched = WalCompactionScheduler::new(Duration::from_secs(600), 0);
        let rep = sched.run_once(
            &wal,
            1024,
            &kv,
            &saga,
            &gate,
            &audit_sink(),
        );
        assert!(rep.is_none(), "compaction must not run during in-flight saga");
    }

    #[test]
    fn scheduler_runs_when_preconditions_clear() {
        let wal = MutationLog::with_params(1, 64);
        for i in 1..=20u8 {
            wal.append(mk(MutationPhase::Committed, i)).unwrap();
        }
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
        let gate = MergeRecoveryGate::new(Arc::clone(&kv));
        let mid = MergeId([7u8; 16]);
        saga.persist(mid, SagaPhase::Committed);
        let sched = WalCompactionScheduler::new(Duration::from_secs(600), 0);
        let rep = sched
            .run_once(&wal, 1024, &kv, &saga, &gate, &audit_sink())
            .expect("compaction should run");
        assert!(rep.records_dropped >= 1);
    }
}
