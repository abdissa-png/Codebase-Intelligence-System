//! **IdentityResolver** provisional CAS — **§01.6.1** (`ri:provisional:{branch}:{semantic_hash}`).

use std::sync::Arc;
use std::time::Duration;

use cis_wal::{BranchId, IdentityId};

use crate::kv::{CasError, MemoryKv};

const STATE_ALLOCATING: u8 = 1;
const STATE_READY: u8 = 2;

fn prov_key(branch_id: BranchId, semantic_hash: &[u8; 32]) -> String {
    let h = semantic_hash.iter().map(|b| format!("{:02x}", b)).collect::<String>();
    format!(
        "ri:provisional:{}:{}",
        branch_id
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>(),
        h
    )
}

fn encode(state: u8, id: &IdentityId) -> Vec<u8> {
    let mut v = vec![state];
    v.extend_from_slice(&id.0);
    v
}

fn decode(v: &[u8]) -> Option<(u8, IdentityId)> {
    if v.len() != 17 {
        return None;
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&v[1..]);
    Some((v[0], IdentityId(id)))
}

#[derive(Debug, Clone)]
pub struct IdentityProvisionalCas {
    kv: Arc<MemoryKv>,
}

impl IdentityProvisionalCas {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    /// Winner: `Ok(Some(identity_id))` if this caller created `ALLOCATING` and then transitions `READY`.
    /// Loser: `Ok(None)` — poll `poll_ready`.
    /// Crash before READY: entry remains ALLOCATING; **DESIGNED** TTL sweep would delete (5s) — here optional `sweep_expired` below.
    pub fn try_allocate(
        &self,
        branch_id: BranchId,
        semantic_hash: [u8; 32],
        proposed_identity: IdentityId,
    ) -> Result<Option<IdentityId>, CasError> {
        let k = prov_key(branch_id, &semantic_hash);
        self.apply_identity_cas_fault("allocate")?;
        match self.kv.compare_and_swap(
            &k,
            None,
            encode(STATE_ALLOCATING, &proposed_identity),
        ) {
            Ok(()) => {
                self.apply_identity_cas_fault("ready")?;
                self.kv
                    .compare_and_swap(
                        &k,
                        Some(&encode(STATE_ALLOCATING, &proposed_identity)),
                        encode(STATE_READY, &proposed_identity),
                    )
                    .expect("we hold allocating");
                Ok(Some(proposed_identity))
            }
            Err(CasError::Mismatch(_)) => Ok(None),
        }
    }

    fn apply_identity_cas_fault(&self, phase: &str) -> Result<(), CasError> {
        let inj = self.kv.fault_injector();
        if crate::fault_injection::apply_fault(inj.before_identity_cas(phase)).is_err() {
            return Err(CasError::Mismatch(format!(
                "fault injection at identity cas {phase}"
            )));
        }
        Ok(())
    }

    pub fn poll_ready(
        &self,
        branch_id: BranchId,
        semantic_hash: [u8; 32],
    ) -> Option<IdentityId> {
        let v = self.kv.get(&prov_key(branch_id, &semantic_hash))?;
        let (st, id) = decode(&v)?;
        (st == STATE_READY).then_some(id)
    }

    /// Test/dev: simulate TTL expiry by deleting stuck ALLOCATING rows.
    pub fn sweep_allocating_for_test(&self, branch_id: BranchId, semantic_hash: [u8; 32]) {
        let k = prov_key(branch_id, &semantic_hash);
        if let Some(v) = self.kv.get(&k) {
            if let Some((STATE_ALLOCATING, _)) = decode(&v) {
                self.kv.delete(&k);
            }
        }
    }

    pub fn backoff_ladder_ms(attempt: u32) -> u64 {
        (50u64 << attempt.min(8)).min(5000)
    }

    /// Convenience: spin with ladder up to **DESIGNED** max wait 5s (spec).
    pub fn wait_ready_or_clear(
        &self,
        branch_id: BranchId,
        semantic_hash: [u8; 32],
        ttl_expired_clear: bool,
    ) -> Option<IdentityId> {
        for attempt in 0u32..10 {
            if let Some(id) = self.poll_ready(branch_id, semantic_hash) {
                return Some(id);
            }
            std::thread::sleep(Duration::from_millis(Self::backoff_ladder_ms(attempt)));
        }
        if ttl_expired_clear {
            self.sweep_allocating_for_test(branch_id, semantic_hash);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_cas_fault_injection() {
        use crate::fault_injection::{AlwaysFail, FailHooks};
        let kv = Arc::new(MemoryKv::new());
        kv.set_fault_injector(Arc::new(AlwaysFail {
            hooks: FailHooks(FailHooks::IDENTITY_CAS),
        }));
        let cas = IdentityProvisionalCas::new(kv);
        let b = BranchId([8u8; 16]);
        let sem = [3u8; 32];
        let id = IdentityId([4u8; 16]);
        assert!(cas.try_allocate(b, sem, id).is_err());
    }

    #[test]
    fn cas_winner_loser() {
        let kv = Arc::new(MemoryKv::new());
        let cas = IdentityProvisionalCas::new(Arc::clone(&kv));
        let b = BranchId([7u8; 16]);
        let sem = [9u8; 32];
        let id_a = IdentityId([1u8; 16]);
        let id_b = IdentityId([2u8; 16]);
        let w = cas.try_allocate(b, sem, id_a).unwrap();
        assert_eq!(w, Some(id_a));
        let l = cas.try_allocate(b, sem, id_b).unwrap();
        assert_eq!(l, None);
        assert_eq!(cas.poll_ready(b, sem), Some(id_a));
    }
}
