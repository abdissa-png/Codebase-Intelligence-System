//! CIS **MutationLog** (WAL) and **MutationIndex** — derived from:
//! - §01.4 Cross-Store Consistency (phase machine)
//! - FR-1.7 (`WriteCoordinator` / WAL compaction rule)
//! - Class diagram: `MutationLog`, `MutationIndex`, `WriteCoordinator`
//! - `ReconciliationEngine` recovery steps 2b–2d (phase-driven replay hooks)
//!
//! **Authoritative normative source:** `CIS-Architecture-v2.md` in the repo root.

mod file_log;
mod ids;
mod index;
mod log;
mod phase;
mod record;
mod wal_backend;

pub use file_log::DurableMutationLog;
pub use ids::{BranchId, IdentityId, MergeId, NodeRevisionId};
pub use index::MutationIndex;
pub use log::{LogId, MutationLog, MutationLogError, WalCompactionReport};
pub use wal_backend::MutationLogStore;
pub use phase::MutationPhase;
pub use record::{merge_cancelled_record, merge_record, MutationKind, MutationRecord};
