//! **IdentityResolver** provisional CAS — **§01.6.1** (`ri:provisional:{branch}:{semantic_hash}`).

use std::sync::Arc;
use std::time::Duration;

use cis_wal::{BranchId, IdentityId};

use crate::kv::{CasError, MemoryKv};

const STATE_ALLOCATING: u8 = 1;
const STATE_READY: u8 = 2;

/// Designed TTL for stuck ALLOCATING rows (crash between allocate and READY).
pub const ALLOCATING_TTL_MS: u64 = 5_000;

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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Wire format: `[state:1][id:16][created_ms:8]` (25 bytes). Legacy 17-byte rows
/// (state+id only) are treated as expired when ALLOCATING.
fn encode(state: u8, id: &IdentityId, created_ms: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(25);
    v.push(state);
    v.extend_from_slice(&id.0);
    v.extend_from_slice(&created_ms.to_le_bytes());
    v
}

fn decode(v: &[u8]) -> Option<(u8, IdentityId, Option<u64>)> {
    if v.len() != 17 && v.len() != 25 {
        return None;
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&v[1..17]);
    let created = if v.len() == 25 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&v[17..25]);
        Some(u64::from_le_bytes(b))
    } else {
        None
    };
    Some((v[0], IdentityId(id), created))
}

fn allocating_expired(created: Option<u64>, now: u64) -> bool {
    match created {
        None => true, // legacy ALLOCATING without timestamp
        Some(t) => now.saturating_sub(t) >= ALLOCATING_TTL_MS,
    }
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
    /// Stuck ALLOCATING past [`ALLOCATING_TTL_MS`] is cleared and allocation is retried once.
    pub fn try_allocate(
        &self,
        branch_id: BranchId,
        semantic_hash: [u8; 32],
        proposed_identity: IdentityId,
    ) -> Result<Option<IdentityId>, CasError> {
        self.try_allocate_inner(branch_id, semantic_hash, proposed_identity, true)
    }

    fn try_allocate_inner(
        &self,
        branch_id: BranchId,
        semantic_hash: [u8; 32],
        proposed_identity: IdentityId,
        allow_ttl_retry: bool,
    ) -> Result<Option<IdentityId>, CasError> {
        let k = prov_key(branch_id, &semantic_hash);
        self.apply_identity_cas_fault("allocate")?;
        let created = now_ms();
        match self.kv.compare_and_swap(
            &k,
            None,
            encode(STATE_ALLOCATING, &proposed_identity, created),
        ) {
            Ok(()) => {
                self.apply_identity_cas_fault("ready")?;
                match self.kv.compare_and_swap(
                    &k,
                    Some(&encode(STATE_ALLOCATING, &proposed_identity, created)),
                    encode(STATE_READY, &proposed_identity, created),
                ) {
                    Ok(()) => Ok(Some(proposed_identity)),
                    Err(CasError::Mismatch(_)) => {
                        // Lost race (e.g. concurrent TTL cleanup deleted ALLOCATING).
                        Ok(None)
                    }
                    Err(e) => Err(e),
                }
            }
            Err(CasError::Mismatch(_)) => {
                if allow_ttl_retry && self.clear_expired_allocating(branch_id, semantic_hash) {
                    return self.try_allocate_inner(
                        branch_id,
                        semantic_hash,
                        proposed_identity,
                        false,
                    );
                }
                Ok(None)
            }
        }
    }

    /// Delete stuck ALLOCATING entries past TTL via conditional CAS delete.
    /// Returns true if a key was cleared. Never deletes a READY winner.
    pub fn clear_expired_allocating(
        &self,
        branch_id: BranchId,
        semantic_hash: [u8; 32],
    ) -> bool {
        let k = prov_key(branch_id, &semantic_hash);
        let Some(v) = self.kv.get(&k) else {
            return false;
        };
        let Some((STATE_ALLOCATING, _, created)) = decode(&v) else {
            return false;
        };
        if !allocating_expired(created, now_ms()) {
            return false;
        }
        // Only delete if the exact ALLOCATING value is still present.
        self.kv.compare_and_delete(&k, &v).is_ok()
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
        let (st, id, _) = decode(&v)?;
        (st == STATE_READY).then_some(id)
    }

    /// Test/dev: simulate TTL expiry by deleting stuck ALLOCATING rows.
    pub fn sweep_allocating_for_test(&self, branch_id: BranchId, semantic_hash: [u8; 32]) {
        let k = prov_key(branch_id, &semantic_hash);
        if let Some(v) = self.kv.get(&k) {
            if let Some((STATE_ALLOCATING, _, _)) = decode(&v) {
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
            let _ = self.clear_expired_allocating(branch_id, semantic_hash);
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

    #[test]
    fn stuck_allocating_cleared_on_retry() {
        let kv = Arc::new(MemoryKv::new());
        let cas = IdentityProvisionalCas::new(Arc::clone(&kv));
        let b = BranchId([1u8; 16]);
        let sem = [2u8; 32];
        let id_a = IdentityId([3u8; 16]);
        let id_b = IdentityId([4u8; 16]);
        // Plant a legacy ALLOCATING row (no timestamp → treated as expired).
        let k = prov_key(b, &sem);
        let mut legacy = vec![STATE_ALLOCATING];
        legacy.extend_from_slice(&id_a.0);
        kv.set(&k, legacy);
        let w = cas.try_allocate(b, sem, id_b).unwrap();
        assert_eq!(w, Some(id_b), "expired ALLOCATING must be reclaimable");
        assert_eq!(cas.poll_ready(b, sem), Some(id_b));
    }
}
