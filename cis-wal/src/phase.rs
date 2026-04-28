//! Per-mutation lifecycle (§01.4).

use serde::{Deserialize, Serialize};

/// **DERIVED** from §01.4 state machine:
/// `PENDING → GRAPH_DONE → VECTOR_DONE → COMMITTED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MutationPhase {
    Pending,
    GraphDone,
    VectorDone,
    /// Terminal: durable cross-store completion; WAL compaction may elide prefix up to here.
    Committed,
    /// Terminal: reconciliation gave up after bounded retries (`ReconciliationEngine` step 2b).
    /// **ASSUMED** (doc names the state but not exact index visibility): failed mutations do not
    /// contribute to `stale` via an incomplete phase — coordinator MUST NOT leave partial graph
    /// without rollback before marking `Failed` (operational contract; see Risk Register in design doc).
    Failed,
}

impl MutationPhase {
    #[inline]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Failed)
    }

    /// Phases that keep a record in the coordinator “pending tail” for replay / `MutationIndex`.
    #[inline]
    pub const fn is_in_flight(self) -> bool {
        !self.is_terminal()
    }

    /// Valid forward transitions (single steps). Merge/saga may insert extra bookkeeping;
    /// this captures the **graph/vector commit** spine from §01.4.
    #[inline]
    pub fn can_transition_to(self, next: MutationPhase) -> bool {
        match (self, next) {
            (Self::Pending, Self::GraphDone) => true,
            (Self::GraphDone, Self::VectorDone) => true,
            // Graph-only status mutations (promote/revert speculative) skip vector phase.
            (Self::GraphDone, Self::Committed) => true,
            (Self::VectorDone, Self::Committed) => true,
            // Failure is allowed from any non-terminal state (DESIGNED: fail-fast escape hatch).
            (s, Self::Failed) if !s.is_terminal() => true,
            // Idempotent no-op or terminal self (for replay deduplication).
            (a, b) if a == b => true,
            _ => false,
        }
    }
}
