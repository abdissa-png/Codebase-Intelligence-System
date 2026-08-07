//! In-memory KV with CAS (merge locks, identity provisional keys, `wal:`, `ri:`, `saga_state:`).

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CasError {
    #[error("compare-and-swap mismatch for key {0:?}")]
    Mismatch(String),
}

/// Upper bound for [`MemoryKv::scan_prefix`] range scans (exclusive end key).
pub(crate) fn next_lexical_prefix(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(b) = bytes.pop() {
        if b != 0xff {
            bytes.push(b + 1);
            return String::from_utf8(bytes).expect("prefix is valid utf-8");
        }
    }
    format!("{prefix}\0")
}

#[derive(Clone)]
pub struct MemoryKv {
    inner: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    fault_injector: Arc<RwLock<Arc<dyn crate::fault_injection::FaultInjector>>>,
}

impl std::fmt::Debug for MemoryKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryKv").finish_non_exhaustive()
    }
}

impl Default for MemoryKv {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryKv {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            fault_injector: Arc::new(RwLock::new(Arc::new(
                crate::fault_injection::NoOpFaultInjector,
            ))),
        }
    }

    /// Replace the fault injector (hardening tests).
    pub fn set_fault_injector(&self, injector: Arc<dyn crate::fault_injection::FaultInjector>) {
        *self.fault_injector.write().unwrap() = injector;
    }

    pub fn fault_injector(&self) -> Arc<dyn crate::fault_injection::FaultInjector> {
        Arc::clone(&*self.fault_injector.read().unwrap())
    }

    fn apply_kv_fault(&self, key: &str) -> bool {
        let inj = self.fault_injector.read().unwrap();
        crate::fault_injection::apply_fault(inj.before_kv_write(key)).is_ok()
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(key).cloned()
    }

    pub fn set(&self, key: &str, value: Vec<u8>) {
        if !self.apply_kv_fault(key) {
            return;
        }
        self.inner.write().unwrap().insert(key.to_string(), value);
    }

    pub fn delete(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.write().unwrap().remove(key)
    }

    pub fn compare_and_delete(&self, key: &str, expected: &[u8]) -> Result<(), CasError> {
        let mut g = self.inner.write().unwrap();
        let cur = g.get(key).map(|v| v.as_slice());
        if cur != Some(expected) {
            return Err(CasError::Mismatch(key.to_string()));
        }
        g.remove(key);
        Ok(())
    }

    /// `expected == None` → key must be absent. `expected == Some(bytes)` → current value must match.
    pub fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: Vec<u8>,
    ) -> Result<(), CasError> {
        let mut g = self.inner.write().unwrap();
        let cur = g.get(key).map(|v| v.as_slice());
        match expected {
            None => {
                if cur.is_some() {
                    return Err(CasError::Mismatch(key.to_string()));
                }
                g.insert(key.to_string(), value);
                Ok(())
            }
            Some(exp) => {
                if cur != Some(exp) {
                    return Err(CasError::Mismatch(key.to_string()));
                }
                g.insert(key.to_string(), value);
                Ok(())
            }
        }
    }

    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        let g = self.inner.read().unwrap();
        let end = next_lexical_prefix(prefix);
        g.range(prefix.to_string()..end)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn snapshot(&self) -> KvSnapshot {
        KvSnapshot {
            entries: self.inner.read().unwrap().clone(),
        }
    }

    /// Replace all entries (Phase 1 workspace restore).
    pub fn restore_snapshot(&self, snap: &KvSnapshot) {
        let mut g = self.inner.write().unwrap();
        g.clear();
        for (k, v) in &snap.entries {
            g.insert(k.clone(), v.clone());
        }
    }

    /// Merge durable keys into existing KV (does not clear ephemeral merge/saga keys).
    pub fn merge_snapshot(&self, snap: &KvSnapshot) {
        let mut g = self.inner.write().unwrap();
        for (k, v) in &snap.entries {
            g.insert(k.clone(), v.clone());
        }
    }
}

/// Prefixes persisted across restart (**ADR 0001**).
pub const DURABLE_KV_PREFIXES: &[&str] = &[
    "ri:",
    "ris:",
    "tt:",
    "deleted:",
    "fork_ts:",
    "branch_parent:",
    "merge_ctx:",
    "saga_state:",
    "saga_batch:",
    "saga_payload:",
    "msnap:",
    "merge_lock:",
    "audit:epoch:",
];

/// Filter snapshot to durable revision-index / time-travel keys only.
pub fn durable_kv_subset(full: &KvSnapshot) -> KvSnapshot {
    KvSnapshot {
        entries: full
            .entries
            .iter()
            .filter(|(k, _)| DURABLE_KV_PREFIXES.iter().any(|p| k.starts_with(p)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

/// Durable subset for `kv.json` persistence (excludes `ris:` when SQLite metadata is active).
pub fn durable_kv_subset_for_persist(full: &KvSnapshot) -> KvSnapshot {
    let subset = durable_kv_subset(full);
    if crate::metadata_store::metadata_backend_from_env()
        != crate::metadata_store::MetadataBackendKind::Sqlite
    {
        return subset;
    }
    #[cfg(feature = "body-sqlite")]
    {
        KvSnapshot {
            entries: subset
                .entries
                .into_iter()
                .filter(|(k, _)| !k.starts_with("ris:"))
                .collect(),
        }
    }
    #[cfg(not(feature = "body-sqlite"))]
    subset
}

/// Owned clone for recovery assertions.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct KvSnapshot {
    pub entries: BTreeMap<String, Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_insert_when_empty() {
        let kv = MemoryKv::new();
        kv.compare_and_swap("k", None, vec![1]).unwrap();
        assert_eq!(kv.get("k"), Some(vec![1]));
    }

    #[test]
    fn cas_rejects_wrong_expected() {
        let kv = MemoryKv::new();
        kv.set("k", vec![1]);
        let err = kv.compare_and_swap("k", Some(&[2]), vec![3]).unwrap_err();
        assert_eq!(err, CasError::Mismatch("k".into()));
    }

    #[test]
    fn compare_and_delete_exact() {
        let kv = MemoryKv::new();
        kv.set("k", vec![1, 2]);
        kv.compare_and_delete("k", &[1, 2]).unwrap();
        assert!(kv.get("k").is_none());
    }

    #[test]
    fn scan_prefix_range_only() {
        let kv = MemoryKv::new();
        kv.set("ri:aa:01", vec![1]);
        kv.set("ri:aa:02", vec![2]);
        kv.set("ri:ab:01", vec![3]);
        kv.set("ri:ba:01", vec![4]);
        let rows = kv.scan_prefix("ri:aa:");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "ri:aa:01");
        assert_eq!(rows[1].0, "ri:aa:02");
    }

    #[test]
    fn next_lexical_prefix_increments_last_byte() {
        assert_eq!(next_lexical_prefix("ri:aa:"), "ri:aa;");
        assert_eq!(next_lexical_prefix("abc"), "abd");
    }

    #[test]
    fn durable_subset_includes_audit_epochs() {
        let mut snap = KvSnapshot::default();
        snap.entries.insert("ri:01:02".into(), vec![1]);
        snap.entries.insert("audit:epoch:00000000000000000001".into(), vec![2]);
        snap.entries.insert("ephemeral:tmp".into(), vec![3]);
        let subset = durable_kv_subset(&snap);
        assert!(subset.entries.contains_key("ri:01:02"));
        assert!(subset
            .entries
            .contains_key("audit:epoch:00000000000000000001"));
        assert!(!subset.entries.contains_key("ephemeral:tmp"));
    }
}
