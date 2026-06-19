//! Shared handles wired between **`cisd`** and [`CisMcpRuntime`] (**Phase 3**).

use std::path::Path;
use std::sync::Arc;

use crate::consistency_snapshot::LastConsistencySnapshot;
use crate::degraded::{DiskPressureFlag, VectorDegradedController};
use crate::embedding_metrics::EmbeddingMetrics;
use crate::graph_delete_queue::GraphDeleteQueue;
use crate::persistence::cis_dir;
use crate::ranking_policy::RankingPolicy;
use crate::security::{ProductionAuditSink, QuotaTracker};
use crate::reconciliation::PeriodicReconciler;
use crate::tombstone_gc::TombstoneGcWorker;
use crate::wal_compaction::WalCompactionScheduler;
use crate::worker_heartbeats::WorkerHeartbeats;
use std::time::Duration;

/// Background subsystem handles created once in **`cisd`** and shared with MCP runtime + worker threads.
#[derive(Clone)]
pub struct CisDaemonHandles {
    pub audit: Arc<ProductionAuditSink>,
    pub disk_pressure: Arc<DiskPressureFlag>,
    pub vector_degraded: Arc<VectorDegradedController>,
    pub quota: Arc<QuotaTracker>,
    pub graph_delete_queue: Arc<GraphDeleteQueue>,
    pub wal_compaction: WalCompactionScheduler,
    pub tombstone_gc: TombstoneGcWorker,
    pub periodic_reconciler: PeriodicReconciler,
    pub embedding_metrics: Arc<EmbeddingMetrics>,
    pub last_consistency: LastConsistencySnapshot,
    pub worker_heartbeats: Arc<WorkerHeartbeats>,
}

impl CisDaemonHandles {
    pub fn open(repo_root: &Path, policy: &RankingPolicy) -> Self {
        let cis = cis_dir(repo_root);
        let _ = std::fs::create_dir_all(&cis);
        let audit = ProductionAuditSink::new(cis.join("audit.jsonl"));
        let graph_delete_queue = Arc::new(
            GraphDeleteQueue::open_persistent(cis.join("graph_delete_queue.json"))
                .unwrap_or_else(|_| GraphDeleteQueue::new()),
        );
        Self {
            audit,
            disk_pressure: Arc::new(DiskPressureFlag::default()),
            vector_degraded: Arc::new(VectorDegradedController::new(
                policy.embedding_queue_hwm,
                policy.embedding_queue_lwm,
                policy.vector_recovery_debounce_s,
            )),
            quota: Arc::new(QuotaTracker::new(32)),
            graph_delete_queue,
            wal_compaction: WalCompactionScheduler::new(Duration::from_secs(600), 1024 * 1024),
            tombstone_gc: TombstoneGcWorker::from_policy(policy),
            periodic_reconciler: PeriodicReconciler::new(Duration::from_secs(3600)),
            embedding_metrics: Arc::new(EmbeddingMetrics::new()),
            last_consistency: LastConsistencySnapshot::new(),
            worker_heartbeats: Arc::new(WorkerHeartbeats::new()),
        }
    }

    #[doc(hidden)]
    pub fn for_tests(repo_root: &Path) -> Self {
        Self::open(repo_root, &RankingPolicy::default())
    }
}
