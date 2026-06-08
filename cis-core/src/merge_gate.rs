//! **MergeRecoveryGate** — **409 merge_in_progress** / torn-read barrier during **`cancel_merge`** (**FR-1.16**, v2.4).

use std::sync::Arc;

use cis_wal::BranchId;

use crate::kv::MemoryKv;

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

#[derive(Debug)]
pub struct MergeRecoveryGate {
    kv: Arc<MemoryKv>,
}

impl MergeRecoveryGate {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    pub fn begin_rollback(&self, branch_id: BranchId) {
        let k = format!("merge_recovery_block:{}", hex16(&branch_id.0));
        self.kv.set(&k, vec![1u8]);
    }

    pub fn end_rollback(&self, branch_id: BranchId) {
        let k = format!("merge_recovery_block:{}", hex16(&branch_id.0));
        self.kv.delete(&k);
    }

    /// **AC-09.4** / query layer: return 409 while set.
    pub fn is_query_blocked(&self, branch_id: BranchId) -> bool {
        let k = format!("merge_recovery_block:{}", hex16(&branch_id.0));
        self.kv.get(&k).is_some()
    }

    /// True when any branch is under merge-recovery rollback.
    pub fn any_blocked(&self) -> bool {
        self.kv
            .scan_prefix("merge_recovery_block:")
            .into_iter()
            .any(|(k, _)| self.kv.get(&k).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let g = MergeRecoveryGate::new(kv);
        let b = BranchId([9u8; 16]);
        assert!(!g.is_query_blocked(b));
        g.begin_rollback(b);
        assert!(g.is_query_blocked(b));
        assert!(g.any_blocked());
        g.end_rollback(b);
        assert!(!g.is_query_blocked(b));
        assert!(!g.any_blocked());
    }
}
