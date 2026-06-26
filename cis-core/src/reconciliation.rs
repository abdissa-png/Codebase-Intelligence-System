//! Startup recovery: rebuild `MutationIndex`, saga hygiene (**ReconciliationEngine** sketch).

use std::sync::Arc;
use std::time::Duration;

use cis_wal::{MutationIndex, MutationLogStore};

use crate::consistency_snapshot::LastConsistencySnapshot;
use crate::graph_consistency::{check_consistency, ConsistencyReport};
use crate::merge_control::MergeControl;
use crate::merge_engine::recover_inflight_merges;
use crate::merge_gate::MergeRecoveryGate;
use crate::security::ProductionAuditSink;
use crate::saga::MergeSagaOrchestrator;
use crate::WriteCoordinator;

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub mutation_index_entries: usize,
    pub in_flight_wal_records: usize,
    pub sagas_compensated: usize,
    /// In-flight WAL rows finished or marked failed during startup replay.
    pub wal_replayed: usize,
    /// Optional consistency report from the last periodic pass.
    pub consistency: Option<ConsistencyReport>,
}

#[derive(Debug)]
pub struct ReconciliationEngine {
    wal: Arc<dyn MutationLogStore>,
    saga: MergeSagaOrchestrator,
}

impl ReconciliationEngine {
    pub fn new(wal: Arc<dyn MutationLogStore>, saga: MergeSagaOrchestrator) -> Self {
        Self { wal, saga }
    }

    /// **DERIVED** ordering: (1) compensate orphaned sagas, (2) rebuild `MutationIndex` from full WAL.
    /// Graph/vector replay hooks attach here in the full merge implementation.
    pub fn reconcile_on_startup(&self, mutation_index: &mut MutationIndex) -> RecoveryReport {
        let compensated = self.saga.compensate_orphans();
        let mut rows = self.wal.iter_all();
        rows.sort_by_key(|r| r.log_id);
        mutation_index.rebuild_from_wal(&rows);
        let inflight = rows.iter().filter(|r| r.phase.is_in_flight()).count();
        RecoveryReport {
            mutation_index_entries: mutation_index.len(),
            in_flight_wal_records: inflight,
            sagas_compensated: compensated,
            wal_replayed: 0,
            consistency: None,
        }
    }
}

/// Periodic reconciliation beyond startup (**Phase 3.3**).
#[derive(Debug, Clone)]
pub struct PeriodicReconciler {
    pub interval: Duration,
}

impl Default for PeriodicReconciler {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600),
        }
    }
}

impl PeriodicReconciler {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    pub fn run_once(
        &self,
        coordinator: &WriteCoordinator,
        saga: &MergeSagaOrchestrator,
        kv: &crate::MemoryKv,
        merge_control: &MergeControl,
        body_store: &crate::BodyStore,
        gate: &MergeRecoveryGate,
        audit: &ProductionAuditSink,
        branches: &[cis_wal::BranchId],
        last_consistency: Option<&LastConsistencySnapshot>,
    ) -> RecoveryReport {
        let mut report = coordinator.reconcile_now(saga);
        let merge_rep = {
            let mut g = coordinator.graph().write();
            recover_inflight_merges(
                &mut *g,
                kv,
                body_store,
                saga,
                merge_control,
                coordinator.vector_chunk_store(),
                gate,
                None,
            )
        };
        if merge_rep.resumed > 0 || merge_rep.compensated > 0 {
            audit.record_sync(
                0,
                format!(
                    "periodic_merge_recovery resumed={} compensated={}",
                    merge_rep.resumed, merge_rep.compensated
                ),
            );
        }
        let consistency = check_consistency(coordinator.graph(), kv, body_store, branches);
        if let Some(cache) = last_consistency {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            cache.update(&consistency, now_ms);
        }
        if !consistency.is_clean() {
            audit.record_sync(0, format!("consistency_check {}", consistency.summary()));
        }
        report.consistency = Some(consistency);
        if report.sagas_compensated > 0 || report.wal_replayed > 0 {
            audit.record_sync(
                0,
                format!(
                    "periodic_reconcile sagas={} wal_replayed={} inflight={}",
                    report.sagas_compensated, report.wal_replayed, report.in_flight_wal_records
                ),
            );
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cis_wal::{MutationIndex, MutationKind, MutationLog, MutationPhase, MutationRecord};
    use cis_wal::{MergeId, MutationLogStore, NodeRevisionId};

    use crate::saga::SagaPhase;

    fn mk_row(phase: MutationPhase) -> MutationRecord {
        let r = NodeRevisionId([5u8; 16]);
        MutationRecord {
            log_id: 0,
            kind: MutationKind::Single,
            phase,
            affected_revisions: vec![r],
            payload_checksum: [0u8; 32],
            created_at_ms: 0,
        }
    }

    #[test]
    fn saga_cleaned_then_index_rebuilt() {
        let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::clone(&kv));
        let mid = MergeId([8u8; 16]);
        saga.persist(mid, SagaPhase::Intent);
        let engine = ReconciliationEngine::new(Arc::clone(&wal), saga);
        wal.append(mk_row(MutationPhase::Pending)).unwrap();
        let mut idx = MutationIndex::new();
        let rep = engine.reconcile_on_startup(&mut idx);
        assert_eq!(rep.sagas_compensated, 1);
        assert!(rep.mutation_index_entries >= 1);
        assert_eq!(rep.in_flight_wal_records, 1);
    }
}
