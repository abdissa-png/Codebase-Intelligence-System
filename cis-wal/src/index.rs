//! In-memory **`MutationIndex`** — O(1) staleness lookup (§01.4, class diagram).

use std::collections::HashMap;

use crate::ids::NodeRevisionId;
use crate::phase::MutationPhase;
use crate::record::MutationRecord;

/// Maps `NodeRevisionId` → newest in-flight `MutationPhase` for that revision.
///
/// **DERIVED** intent: “Reads consult it in O(1) — no WAL scan.”
#[derive(Debug, Default, Clone)]
pub struct MutationIndex {
    by_revision: HashMap<NodeRevisionId, MutationPhase>,
}

impl MutationIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Latest phase for this revision if any in-flight mutation touches it.
    #[inline]
    pub fn get_phase(&self, revision: NodeRevisionId) -> Option<MutationPhase> {
        self.by_revision.get(&revision).copied()
    }

    /// **DERIVED** from §01.4 staleness table + US-03: graph-only queries care about `Pending` only;
    /// hybrid cares about `Pending | GraphDone`. Callers pass `vector_sensitive`.
    #[inline]
    pub fn is_stale_for_query(&self, revision: NodeRevisionId, vector_sensitive: bool) -> bool {
        match self.get_phase(revision) {
            None => false,
            Some(MutationPhase::Pending) => true,
            Some(MutationPhase::GraphDone) => vector_sensitive,
            Some(MutationPhase::VectorDone) => false,
            Some(MutationPhase::Committed | MutationPhase::Failed) => false,
        }
    }

    pub fn rebuild_from_wal(&mut self, records: &[MutationRecord]) {
        self.by_revision.clear();
        let mut pending: Vec<&MutationRecord> =
            records.iter().filter(|r| r.phase.is_in_flight()).collect();
        pending.sort_by_key(|r| r.log_id);
        for r in pending {
            for rev in &r.affected_revisions {
                self.by_revision.insert(*rev, r.phase);
            }
        }
    }

    pub fn apply_transition(&mut self, record: &MutationRecord, new_phase: MutationPhase) {
        debug_assert!(
            record.phase.can_transition_to(new_phase),
            "illegal phase transition {:?} -> {:?}",
            record.phase,
            new_phase
        );
        if new_phase.is_terminal() {
            for rev in &record.affected_revisions {
                self.by_revision.remove(rev);
            }
        } else {
            for rev in &record.affected_revisions {
                self.by_revision.insert(*rev, new_phase);
            }
        }
    }

    pub fn register_new_record(&mut self, record: &MutationRecord) {
        if record.phase.is_terminal() {
            return;
        }
        for rev in &record.affected_revisions {
            self.by_revision.insert(*rev, record.phase);
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.by_revision.len()
    }

    pub fn detect_overlapping_inflight(
        records: &[MutationRecord],
    ) -> Vec<(NodeRevisionId, u64, u64)> {
        let mut inflight: Vec<&MutationRecord> =
            records.iter().filter(|r| r.phase.is_in_flight()).collect();
        inflight.sort_by_key(|r| r.log_id);
        let mut owner: HashMap<NodeRevisionId, u64> = HashMap::new();
        let mut conflicts = Vec::new();
        for r in inflight {
            for rev in &r.affected_revisions {
                match owner.get(rev) {
                    Some(prev_log) if *prev_log != r.log_id => {
                        conflicts.push((*rev, *prev_log, r.log_id));
                    }
                    Some(_) => {}
                    None => {
                        owner.insert(*rev, r.log_id);
                    }
                }
            }
        }
        conflicts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::MutationKind;

    fn rid(n: u8) -> NodeRevisionId {
        let mut b = [0u8; 16];
        b[15] = n;
        NodeRevisionId(b)
    }

    fn sample_record(
        log_id: u64,
        phase: MutationPhase,
        revs: Vec<NodeRevisionId>,
    ) -> MutationRecord {
        MutationRecord {
            log_id,
            kind: MutationKind::Single,
            phase,
            affected_revisions: revs,
            payload_checksum: [0u8; 32],
            created_at_ms: 0,
        }
    }

    #[test]
    fn rebuild_monotonic_last_wins() {
        let r1 = rid(1);
        let mut idx = MutationIndex::new();
        let rows = vec![
            sample_record(1, MutationPhase::Pending, vec![r1]),
            sample_record(2, MutationPhase::GraphDone, vec![r1]),
        ];
        idx.rebuild_from_wal(&rows);
        assert_eq!(idx.get_phase(r1), Some(MutationPhase::GraphDone));
    }

    #[test]
    fn staleness_semantics_match_01_04_table() {
        let r = rid(7);
        let rec_p = sample_record(1, MutationPhase::Pending, vec![r]);
        let mut idx = MutationIndex::new();
        idx.register_new_record(&rec_p);
        assert!(idx.is_stale_for_query(r, false));
        assert!(idx.is_stale_for_query(r, true));

        let rec_g = sample_record(1, MutationPhase::GraphDone, vec![r]);
        idx.apply_transition(&rec_p, MutationPhase::GraphDone);
        assert_eq!(idx.get_phase(r), Some(MutationPhase::GraphDone));
        assert!(!idx.is_stale_for_query(r, false));
        assert!(idx.is_stale_for_query(r, true));

        idx.apply_transition(&rec_g, MutationPhase::VectorDone);
        assert_eq!(idx.get_phase(r), Some(MutationPhase::VectorDone));
        assert!(!idx.is_stale_for_query(r, true));
    }

    #[test]
    fn terminal_clears_index() {
        let r = rid(3);
        let mut idx = MutationIndex::new();
        let rec = sample_record(7, MutationPhase::VectorDone, vec![r]);
        idx.register_new_record(&rec);
        idx.apply_transition(&rec, MutationPhase::Committed);
        assert_eq!(idx.get_phase(r), None);
    }
}
