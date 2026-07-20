//! Shared [`MutationLogStore`] for in-memory and durable WAL implementations (**FR-1.7**, **NFR-R2**).

use crate::log::{LogId, MutationLog, MutationLogError, WalCompactionReport};
use crate::phase::MutationPhase;
use crate::record::MutationRecord;

/// Operations required by `WriteCoordinator`, merge markers, and compaction.
pub trait MutationLogStore: Send + Sync + std::fmt::Debug {
    fn append(&self, record: MutationRecord) -> Result<LogId, MutationLogError>;
    fn get(&self, id: LogId) -> Option<MutationRecord>;
    fn update_phase(&self, id: LogId, phase: MutationPhase) -> Result<(), MutationLogError>;
    fn iter_all(&self) -> Vec<MutationRecord>;
    /// Cheap record count without materializing a full clone of every record.
    fn record_count(&self) -> usize {
        self.iter_all().len()
    }
    fn next_allocate_id(&self) -> LogId;
    fn truncate_committed(&self, wal_max_bytes: u64) -> Result<WalCompactionReport, MutationLogError>;
}

impl MutationLogStore for MutationLog {
    fn append(&self, record: MutationRecord) -> Result<LogId, MutationLogError> {
        MutationLog::append(self, record)
    }

    fn get(&self, id: LogId) -> Option<MutationRecord> {
        MutationLog::get(self, id)
    }

    fn update_phase(&self, id: LogId, phase: MutationPhase) -> Result<(), MutationLogError> {
        MutationLog::update_phase(self, id, phase)
    }

    fn iter_all(&self) -> Vec<MutationRecord> {
        MutationLog::iter_all(self)
    }

    fn record_count(&self) -> usize {
        MutationLog::len(self)
    }

    fn next_allocate_id(&self) -> LogId {
        MutationLog::next_allocate_id(self)
    }

    fn truncate_committed(&self, wal_max_bytes: u64) -> Result<WalCompactionReport, MutationLogError> {
        Ok(MutationLog::truncate_committed(self, wal_max_bytes))
    }
}
