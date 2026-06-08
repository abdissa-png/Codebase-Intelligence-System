//! `merge_lock:{branch_id}` — **FR-1.15** (CAS acquire / verified release).
//! **`merge_started:{merge_id}`** — wall-clock start for **FR-1.14** TTL sweep.

use std::time::{SystemTime, UNIX_EPOCH};

use cis_wal::{BranchId, MergeId};

use crate::kv::{CasError, MemoryKv};

fn merge_id_hex(merge_id: MergeId) -> String {
    merge_id
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

pub fn merge_started_key(merge_id: MergeId) -> String {
    format!("merge_started:{}", merge_id_hex(merge_id))
}

pub fn record_merge_started_ms(kv: &MemoryKv, merge_id: MergeId, started_ms: u64) {
    kv.set(
        &merge_started_key(merge_id),
        started_ms.to_le_bytes().to_vec(),
    );
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// **`true`** if **`merge_started`** exists and age exceeds **`ttl_hours`**.
pub fn merge_ttl_expired_ms(kv: &MemoryKv, merge_id: MergeId, now_ms: u64, ttl_hours: u32) -> bool {
    let Some(v) = kv.get(&merge_started_key(merge_id)) else {
        return false;
    };
    if v.len() != 8 {
        return false;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&v[..8]);
    let started = u64::from_le_bytes(b);
    now_ms.saturating_sub(started) > u64::from(ttl_hours).saturating_mul(3_600_000)
}

/// Release lock + delete **`merge_started`** when TTL expired (**policy.merge_ttl_hours**).
pub fn sweep_expired_merge_intents(
    kv: &MemoryKv,
    branch_id: BranchId,
    merge_id: MergeId,
    now_ms: u64,
    ttl_hours: u32,
) -> Result<bool, CasError> {
    if !merge_ttl_expired_ms(kv, merge_id, now_ms, ttl_hours) {
        return Ok(false);
    }
    release_merge_lock(kv, branch_id, merge_id)?;
    Ok(true)
}

pub fn merge_lock_key(branch_id: BranchId) -> String {
    format!(
        "merge_lock:{}",
        branch_id
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    )
}

pub fn acquire_merge_lock(
    kv: &MemoryKv,
    branch_id: BranchId,
    merge_id: MergeId,
) -> Result<(), CasError> {
    kv.compare_and_swap(&merge_lock_key(branch_id), None, merge_id.0.to_vec())?;
    record_merge_started_ms(kv, merge_id, now_epoch_ms());
    Ok(())
}

pub fn release_merge_lock(
    kv: &MemoryKv,
    branch_id: BranchId,
    merge_id: MergeId,
) -> Result<(), CasError> {
    kv.compare_and_delete(&merge_lock_key(branch_id), &merge_id.0)?;
    kv.delete(&merge_started_key(merge_id));
    Ok(())
}

pub fn merge_lock_holder(kv: &MemoryKv, branch_id: BranchId) -> Option<MergeId> {
    let v = kv.get(&merge_lock_key(branch_id))?;
    if v.len() != 16 {
        return None;
    }
    let mut a = [0u8; 16];
    a.copy_from_slice(&v);
    Some(MergeId(a))
}

pub fn scan_merge_lock_holders(kv: &MemoryKv) -> Vec<(BranchId, MergeId)> {
    let mut out = Vec::new();
    for (k, v) in kv.scan_prefix("merge_lock:") {
        let Some(hex) = k.strip_prefix("merge_lock:") else {
            continue;
        };
        if hex.len() != 32 {
            continue;
        }
        let mut b = [0u8; 16];
        let mut ok = true;
        for i in 0..16 {
            if let Ok(x) = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                b[i] = x;
            } else {
                ok = false;
                break;
            }
        }
        if !ok || v.len() != 16 {
            continue;
        }
        let mut m = [0u8; 16];
        m.copy_from_slice(&v);
        out.push((BranchId(b), MergeId(m)));
    }
    out
}

/// Run [`sweep_expired_merge_intents`] for every held merge lock (policy TTL).
pub fn sweep_all_expired_merge_intents(kv: &MemoryKv, now_ms: u64, ttl_hours: u32) -> usize {
    let locks = scan_merge_lock_holders(kv);
    let mut cleared = 0usize;
    for (branch, mid) in locks {
        if sweep_expired_merge_intents(kv, branch, mid, now_ms, ttl_hours).unwrap_or(false) {
            cleared += 1;
        }
    }
    cleared
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_roundtrip() {
        let kv = MemoryKv::new();
        let b = BranchId([3u8; 16]);
        let m = MergeId([4u8; 16]);
        acquire_merge_lock(&kv, b, m).unwrap();
        assert_eq!(merge_lock_holder(&kv, b), Some(m));
        assert!(kv.get(&merge_started_key(m)).is_some());
        release_merge_lock(&kv, b, m).unwrap();
        assert_eq!(merge_lock_holder(&kv, b), None);
        assert!(kv.get(&merge_started_key(m)).is_none());
    }

    #[test]
    fn ttl_sweep_releases_lock() {
        let kv = MemoryKv::new();
        let branch = BranchId([4u8; 16]);
        let mid = MergeId([5u8; 16]);
        acquire_merge_lock(&kv, branch, mid).unwrap();
        record_merge_started_ms(&kv, mid, 0);
        let ok = sweep_expired_merge_intents(&kv, branch, mid, 10_000_000, 2).unwrap();
        assert!(ok);
        assert!(merge_lock_holder(&kv, branch).is_none());
    }

    #[test]
    fn sweep_all_clears_expired() {
        let kv = MemoryKv::new();
        let b = BranchId([1u8; 16]);
        let m = MergeId([2u8; 16]);
        acquire_merge_lock(&kv, b, m).unwrap();
        record_merge_started_ms(&kv, m, 0);
        let n = sweep_all_expired_merge_intents(&kv, 10_000_000, 2);
        assert_eq!(n, 1);
        assert!(merge_lock_holder(&kv, b).is_none());
    }
}
