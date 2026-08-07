//! `branch_name → branch_id` (**BranchRegistry**, SPF-2).

use std::sync::Arc;

use cis_wal::BranchId;

use crate::kv::MemoryKv;

#[derive(Debug)]
pub struct BranchRegistry {
    kv: Arc<MemoryKv>,
}

impl BranchRegistry {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv }
    }

    /// Lookup-only: returns the registered id if `branch_reg:{name}` exists with a valid 16-byte value.
    pub fn get_id(&self, branch_name: &str) -> Option<BranchId> {
        let key = format!("branch_reg:{}", branch_name);
        let v = self.kv.get(&key)?;
        if v.len() != 16 {
            return None;
        }
        let mut b = [0u8; 16];
        b.copy_from_slice(&v);
        Some(BranchId(b))
    }

    /// Whether `branch_name` is already registered in this KV.
    pub fn is_registered(&self, branch_name: &str) -> bool {
        self.get_id(branch_name).is_some()
    }

    /// New UUID per **first** registration of `name` in this KV; subsequent calls return the same id
    /// until the `branch_reg:` key is deleted (simulates branch delete in tests).
    ///
    /// **`main`** is seeded to `BranchId([0u8; 16])` on first registration for legacy test/fixture compat.
    pub fn get_or_create_id(&self, branch_name: &str) -> BranchId {
        if let Some(id) = self.get_id(branch_name) {
            return id;
        }
        let key = format!("branch_reg:{}", branch_name);
        if branch_name == "main" {
            let id = BranchId([0u8; 16]);
            self.kv.set(&key, id.0.to_vec());
            return id;
        }
        let seq = self
            .kv
            .get("meta:branch_seq")
            .and_then(|v| {
                if v.len() == 8 {
                    Some(u64::from_le_bytes(v.try_into().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or(0)
            + 1;
        self.kv.set("meta:branch_seq", seq.to_le_bytes().to_vec());
        let mut raw = [0u8; 16];
        raw[0..8].copy_from_slice(&seq.to_le_bytes());
        let h = fnv_branch_name(branch_name);
        raw[8..16].copy_from_slice(&h);
        let id = BranchId(raw);
        self.kv.set(&key, id.0.to_vec());
        id
    }

    /// All registered `(name, branch_id)` pairs.
    pub fn list_branches(&self) -> Vec<(String, BranchId)> {
        self.kv
            .scan_prefix("branch_reg:")
            .into_iter()
            .filter_map(|(k, v)| {
                if v.len() != 16 {
                    return None;
                }
                let name = k.strip_prefix("branch_reg:")?.to_string();
                let mut b = [0u8; 16];
                b.copy_from_slice(&v);
                Some((name, BranchId(b)))
            })
            .collect()
    }
}

#[inline]
fn fnv_branch_name(name: &str) -> [u8; 8] {
    let mut h: u64 = 0xcbf29ce484222325;
    const P: u64 = 0x100000001b3;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(P);
    }
    h.to_le_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_for_same_name() {
        let kv = Arc::new(MemoryKv::new());
        let reg = BranchRegistry::new(kv);
        let a = reg.get_or_create_id("main");
        let b = reg.get_or_create_id("main");
        assert_eq!(a.0, b.0);
    }

    #[test]
    fn different_names_differ() {
        let kv = Arc::new(MemoryKv::new());
        let reg = BranchRegistry::new(kv);
        let a = reg.get_or_create_id("main");
        let c = reg.get_or_create_id("feature");
        assert_ne!(a.0, c.0);
    }

    #[test]
    fn main_seeded_to_zero_id() {
        let kv = Arc::new(MemoryKv::new());
        let reg = BranchRegistry::new(kv);
        assert_eq!(reg.get_or_create_id("main").0, [0u8; 16]);
    }

    #[test]
    fn get_id_none_until_registered() {
        let kv = Arc::new(MemoryKv::new());
        let reg = BranchRegistry::new(kv);
        assert!(reg.get_id("feature").is_none());
        assert!(!reg.is_registered("feature"));
        let id = reg.get_or_create_id("feature");
        assert_eq!(reg.get_id("feature"), Some(id));
        assert!(reg.is_registered("feature"));
    }
}
