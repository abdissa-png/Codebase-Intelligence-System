//! `MutationRecord` — stored under KV prefix `wal:` as `wal:{log_id}` (**DERIVED** namespace table §01.6.2).

use serde::{Deserialize, Serialize};

use crate::ids::{MergeId, NodeRevisionId};
use crate::phase::MutationPhase;

/// **DERIVED** from class diagram `MutationLog.append`: `SINGLE | BULK | MERGE`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MutationKind {
    Single,
    Bulk {
        /// **DESIGNED**: bounded count for metrics / sanity; not spelled in architecture.
        #[serde(default)]
        item_count: u32,
    },
    Merge {
        merge_id: MergeId,
    },
    /// **`cancel_merge`** completed (**FR-1.16** marker for auditors / replay).
    MergeCancelled {
        merge_id: MergeId,
    },
    /// Speculative patch confirmed — promote revisions to `Active` (**Phase 0**).
    PromoteSpeculative {
        patch_id: u64,
    },
    /// Speculative patch reverted — tombstone revisions (**Phase 0**).
    RevertSpeculative {
        patch_id: u64,
    },
}

/// One append-only WAL row. `log_id` is monotonic per storage namespace (tenant/repo shard).
///
/// **DESIGNED** fields not explicit in prose: `payload_checksum` fingerprints the serialized
/// `GraphMutationSet` / saga batch for tamper detection during replay (aligns with NFR-SEC4 spirit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationRecord {
    pub log_id: u64,
    pub kind: MutationKind,
    pub phase: MutationPhase,
    /// Every revision whose staleness can be affected by this mutation’s phase (§01.4 table).
    pub affected_revisions: Vec<NodeRevisionId>,
    /// Blake3 or SHA-256 over canonical mutation payload; **ASSUMED** algorithm chosen at storage init.
    pub payload_checksum: [u8; 32],
    /// Wall clock ms at append; GC / merge-TTL uses policy on top.
    pub created_at_ms: i64,
}

impl MutationRecord {
    /// **Invariant W-1:** in-flight records must name at least one revision (no ghost mutations).
    pub fn validate_invariants(&self) -> Result<(), &'static str> {
        if matches!(
            self.kind,
            MutationKind::MergeCancelled { .. }
                | MutationKind::Merge { .. }
                | MutationKind::PromoteSpeculative { .. }
                | MutationKind::RevertSpeculative { .. }
        ) {
            return Ok(());
        }
        if self.phase.is_in_flight() && self.affected_revisions.is_empty() {
            return Err("W-1: affected_revisions empty while phase in-flight");
        }
        Ok(())
    }
}

/// WAL marker appended after a successful **`merge_branch`** commit.
pub fn merge_record(merge_id: MergeId, affected_revisions: Vec<NodeRevisionId>) -> MutationRecord {
    MutationRecord {
        log_id: 0,
        kind: MutationKind::Merge { merge_id },
        phase: MutationPhase::Committed,
        affected_revisions,
        payload_checksum: [0u8; 32],
        created_at_ms: 0,
    }
}

/// WAL marker appended after **`cancel_merge`** succeeds (**FR-1.16** step 5).
pub fn merge_cancelled_record(merge_id: MergeId) -> MutationRecord {
    MutationRecord {
        log_id: 0,
        kind: MutationKind::MergeCancelled { merge_id },
        phase: MutationPhase::Committed,
        affected_revisions: vec![],
        payload_checksum: [0u8; 32],
        created_at_ms: 0,
    }
}
