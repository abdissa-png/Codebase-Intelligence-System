//! KV with CAS (merge locks, identity provisional keys, `wal:`, `ri:`, `saga_state:`).
//!
//! Default is an in-memory [`BTreeMap`] persisted as `kv.json`. `CIS_KV_BACKEND=sqlite`
//! stores durable prefixes in `.cis/store.db`.

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

/// Prefixes persisted across restart (**ADR 0001**).
///
/// **`eto:` is intentionally absent.** Edge-target overrides are process-local
/// (Phase 6 product decision). Making them durable would change query results
/// after restart; [`crate::edge_target_override`] documents this.
pub const DURABLE_KV_PREFIXES: &[&str] = &[
    "ri:",
    "ris:",
    "tt:",
    "deleted:",
    "fork_ts:",
    "branch_parent:",
    "branch_reg:",
    "meta:branch_seq",
    "merge_ctx:",
    "saga_state:",
    "saga_batch:",
    "saga_payload:",
    "msnap:",
    "merge_lock:",
    "audit:epoch:",
];

pub fn is_durable_kv_key(key: &str) -> bool {
    DURABLE_KV_PREFIXES.iter().any(|p| key.starts_with(p))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvBackendKind {
    Json,
    Sqlite,
}

/// Read `CIS_KV_BACKEND=json|sqlite` (default `json`). Independent of `CIS_METADATA_BACKEND`.
pub fn kv_backend_from_env() -> KvBackendKind {
    match std::env::var_os("CIS_KV_BACKEND") {
        Some(v) if v == "sqlite" || v == "sqlite3" => KvBackendKind::Sqlite,
        _ => KvBackendKind::Json,
    }
}

/// When `CIS_KV_BACKEND=sqlite`, also write `kv.json` if `CIS_KV_JSON_EXPORT=1`.
pub fn kv_json_export_enabled() -> bool {
    std::env::var_os("CIS_KV_JSON_EXPORT").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    })
}

/// Shared method surface for the RAM and SQLite backends (tests run on both).
pub trait KvStore: Send + Sync {
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn set(&self, key: &str, value: Vec<u8>);
    fn delete(&self, key: &str) -> Option<Vec<u8>>;
    fn compare_and_delete(&self, key: &str, expected: &[u8]) -> Result<(), CasError>;
    fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: Vec<u8>,
    ) -> Result<(), CasError>;
    fn scan_prefix(&self, prefix: &str) -> Vec<(String, Vec<u8>)>;
    fn snapshot(&self) -> KvSnapshot;
    fn restore_snapshot(&self, snap: &KvSnapshot);
    fn merge_snapshot(&self, snap: &KvSnapshot);

    fn copy_prefix_remap(&self, src_prefix: &str, dst_prefix: &str) -> usize {
        copy_prefix_remap_scan(self, src_prefix, dst_prefix)
    }
}

fn copy_prefix_remap_scan<K: KvStore + ?Sized>(
    kv: &K,
    src_prefix: &str,
    dst_prefix: &str,
) -> usize {
    let mut n = 0usize;
    for (k, v) in kv.scan_prefix(src_prefix) {
        let Some(suffix) = k.strip_prefix(src_prefix) else {
            continue;
        };
        let dest = format!("{dst_prefix}{suffix}");
        if kv.get(&dest).is_none() {
            kv.set(&dest, v);
            n += 1;
        }
    }
    n
}

#[derive(Clone)]
enum KvInner {
    Memory(Arc<RwLock<BTreeMap<String, Vec<u8>>>>),
    #[cfg(feature = "body-sqlite")]
    Sqlite(Arc<crate::sqlite_kv::SqliteKv>),
}

#[derive(Clone)]
pub struct MemoryKv {
    inner: KvInner,
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
            inner: KvInner::Memory(Arc::new(RwLock::new(BTreeMap::new()))),
            fault_injector: Arc::new(RwLock::new(Arc::new(
                crate::fault_injection::NoOpFaultInjector,
            ))),
        }
    }

    #[cfg(feature = "body-sqlite")]
    pub fn open_sqlite(cis_dir: &std::path::Path) -> std::io::Result<Self> {
        Ok(Self {
            inner: KvInner::Sqlite(Arc::new(crate::sqlite_kv::SqliteKv::open(cis_dir)?)),
            fault_injector: Arc::new(RwLock::new(Arc::new(
                crate::fault_injection::NoOpFaultInjector,
            ))),
        })
    }

    #[cfg(feature = "body-sqlite")]
    pub fn open_sqlite_in_memory() -> Self {
        Self {
            inner: KvInner::Sqlite(Arc::new(crate::sqlite_kv::SqliteKv::open_in_memory())),
            fault_injector: Arc::new(RwLock::new(Arc::new(
                crate::fault_injection::NoOpFaultInjector,
            ))),
        }
    }

    pub fn is_sqlite_backed(&self) -> bool {
        match self.inner {
            KvInner::Memory(_) => false,
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(_) => true,
        }
    }

    pub fn durable_row_count(&self) -> usize {
        match &self.inner {
            KvInner::Memory(map) => map
                .read()
                .unwrap()
                .keys()
                .filter(|k| is_durable_kv_key(k))
                .count(),
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.durable_row_count(),
        }
    }

    pub fn sql_write_count(&self) -> u64 {
        match &self.inner {
            KvInner::Memory(_) => 0,
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.sql_write_count(),
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
        match &self.inner {
            KvInner::Memory(map) => map.read().unwrap().get(key).cloned(),
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.get(key),
        }
    }

    pub fn set(&self, key: &str, value: Vec<u8>) {
        if !self.apply_kv_fault(key) {
            return;
        }
        match &self.inner {
            KvInner::Memory(map) => {
                map.write().unwrap().insert(key.to_string(), value);
            }
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.set(key, value),
        }
    }

    pub fn delete(&self, key: &str) -> Option<Vec<u8>> {
        match &self.inner {
            KvInner::Memory(map) => map.write().unwrap().remove(key),
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.delete(key),
        }
    }

    pub fn compare_and_delete(&self, key: &str, expected: &[u8]) -> Result<(), CasError> {
        match &self.inner {
            KvInner::Memory(map) => {
                let mut g = map.write().unwrap();
                let cur = g.get(key).map(|v| v.as_slice());
                if cur != Some(expected) {
                    return Err(CasError::Mismatch(key.to_string()));
                }
                g.remove(key);
                Ok(())
            }
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.compare_and_delete(key, expected),
        }
    }

    /// `expected == None` → key must be absent. `expected == Some(bytes)` → current value must match.
    pub fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: Vec<u8>,
    ) -> Result<(), CasError> {
        match &self.inner {
            KvInner::Memory(map) => {
                let mut g = map.write().unwrap();
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
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.compare_and_swap(key, expected, value),
        }
    }

    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        match &self.inner {
            KvInner::Memory(map) => {
                let g = map.read().unwrap();
                let end = next_lexical_prefix(prefix);
                g.range(prefix.to_string()..end)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.scan_prefix(prefix),
        }
    }

    pub fn snapshot(&self) -> KvSnapshot {
        match &self.inner {
            KvInner::Memory(map) => KvSnapshot {
                entries: map.read().unwrap().clone(),
            },
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.snapshot(),
        }
    }

    /// Replace all entries (Phase 1 workspace restore).
    pub fn restore_snapshot(&self, snap: &KvSnapshot) {
        match &self.inner {
            KvInner::Memory(map) => {
                let mut g = map.write().unwrap();
                g.clear();
                for (k, v) in &snap.entries {
                    g.insert(k.clone(), v.clone());
                }
            }
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.restore_snapshot(snap),
        }
    }

    /// Merge durable keys into existing KV (does not clear ephemeral merge/saga keys).
    pub fn merge_snapshot(&self, snap: &KvSnapshot) {
        match &self.inner {
            KvInner::Memory(map) => {
                let mut g = map.write().unwrap();
                for (k, v) in &snap.entries {
                    g.insert(k.clone(), v.clone());
                }
            }
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.merge_snapshot(snap),
        }
    }

    /// Copy every key under `src_prefix` to `dst_prefix`, skipping destinations that already exist.
    ///
    /// SQLite uses one `INSERT…SELECT` for durable rows instead of a Rust scan/set loop.
    pub fn copy_prefix_remap(&self, src_prefix: &str, dst_prefix: &str) -> usize {
        match &self.inner {
            KvInner::Memory(_) => copy_prefix_remap_scan(self, src_prefix, dst_prefix),
            #[cfg(feature = "body-sqlite")]
            KvInner::Sqlite(s) => s.copy_prefix_remap(src_prefix, dst_prefix),
        }
    }
}

impl KvStore for MemoryKv {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        MemoryKv::get(self, key)
    }
    fn set(&self, key: &str, value: Vec<u8>) {
        MemoryKv::set(self, key, value)
    }
    fn delete(&self, key: &str) -> Option<Vec<u8>> {
        MemoryKv::delete(self, key)
    }
    fn compare_and_delete(&self, key: &str, expected: &[u8]) -> Result<(), CasError> {
        MemoryKv::compare_and_delete(self, key, expected)
    }
    fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: Vec<u8>,
    ) -> Result<(), CasError> {
        MemoryKv::compare_and_swap(self, key, expected, value)
    }
    fn scan_prefix(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        MemoryKv::scan_prefix(self, prefix)
    }
    fn snapshot(&self) -> KvSnapshot {
        MemoryKv::snapshot(self)
    }
    fn restore_snapshot(&self, snap: &KvSnapshot) {
        MemoryKv::restore_snapshot(self, snap)
    }
    fn merge_snapshot(&self, snap: &KvSnapshot) {
        MemoryKv::merge_snapshot(self, snap)
    }
    fn copy_prefix_remap(&self, src_prefix: &str, dst_prefix: &str) -> usize {
        MemoryKv::copy_prefix_remap(self, src_prefix, dst_prefix)
    }
}

/// Filter snapshot to durable revision-index / time-travel keys only.
pub fn durable_kv_subset(full: &KvSnapshot) -> KvSnapshot {
    KvSnapshot {
        entries: full
            .entries
            .iter()
            .filter(|(k, _)| is_durable_kv_key(k))
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

    fn backends() -> Vec<(&'static str, MemoryKv)> {
        let mut out = vec![("memory", MemoryKv::new())];
        #[cfg(feature = "body-sqlite")]
        out.push(("sqlite", MemoryKv::open_sqlite_in_memory()));
        out
    }

    fn run_on_backends(test: impl Fn(&str, &MemoryKv)) {
        for (name, kv) in backends() {
            test(name, &kv);
        }
    }

    #[test]
    fn cas_insert_when_empty() {
        run_on_backends(|name, kv| {
            kv.compare_and_swap("k", None, vec![1]).unwrap();
            assert_eq!(kv.get("k"), Some(vec![1]), "{name}");
        });
    }

    #[test]
    fn cas_rejects_wrong_expected() {
        run_on_backends(|name, kv| {
            kv.set("k", vec![1]);
            let err = kv.compare_and_swap("k", Some(&[2]), vec![3]).unwrap_err();
            assert_eq!(err, CasError::Mismatch("k".into()), "{name}");
        });
    }

    #[test]
    fn cas_durable_ri_key() {
        run_on_backends(|name, kv| {
            let key = "ri:aa:01";
            kv.compare_and_swap(key, None, vec![1]).unwrap();
            assert_eq!(kv.get(key), Some(vec![1]), "{name}");
            kv.compare_and_swap(key, Some(&[1]), vec![2]).unwrap();
            assert_eq!(kv.get(key), Some(vec![2]), "{name}");
            kv.compare_and_swap(key, Some(&[1]), vec![3]).unwrap_err();
        });
    }

    #[test]
    fn compare_and_delete_ephemeral() {
        run_on_backends(|name, kv| {
            kv.set("eto:x", vec![1, 2]);
            kv.compare_and_delete("eto:x", &[1, 2]).unwrap();
            assert!(kv.get("eto:x").is_none(), "{name}");
        });
    }

    #[test]
    fn compare_and_delete_durable() {
        run_on_backends(|name, kv| {
            kv.set("ri:del:01", vec![1, 2]);
            kv.compare_and_delete("ri:del:01", &[1, 2]).unwrap();
            assert!(kv.get("ri:del:01").is_none(), "{name}");
        });
    }

    #[test]
    fn scan_prefix_range_only() {
        run_on_backends(|name, kv| {
            kv.set("ri:aa:01", vec![1]);
            kv.set("ri:aa:02", vec![2]);
            kv.set("ri:ab:01", vec![3]);
            kv.set("ri:ba:01", vec![4]);
            let rows = kv.scan_prefix("ri:aa:");
            assert_eq!(rows.len(), 2, "{name}");
            assert_eq!(rows[0].0, "ri:aa:01", "{name}");
            assert_eq!(rows[1].0, "ri:aa:02", "{name}");
        });
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

    #[test]
    fn durable_subset_includes_branch_registry() {
        let mut snap = KvSnapshot::default();
        snap.entries.insert("branch_reg:feature".into(), vec![0; 16]);
        snap.entries
            .insert("meta:branch_seq".into(), 1u64.to_le_bytes().to_vec());
        snap.entries.insert("ephemeral:tmp".into(), vec![9]);
        let subset = durable_kv_subset(&snap);
        assert!(subset.entries.contains_key("branch_reg:feature"));
        assert!(subset.entries.contains_key("meta:branch_seq"));
        assert!(!subset.entries.contains_key("ephemeral:tmp"));
    }

    #[test]
    fn copy_prefix_remap_skips_existing() {
        run_on_backends(|name, kv| {
            kv.set("ri:aa:01", vec![1]);
            kv.set("ri:bb:01", vec![9]);
            let n = kv.copy_prefix_remap("ri:aa:", "ri:bb:");
            assert_eq!(n, 0, "{name}: destination already occupied");
            assert_eq!(kv.get("ri:bb:01"), Some(vec![9]), "{name}");
        });
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn sqlite_ephemeral_keys_are_not_durable() {
        let dir = tempfile::tempdir().unwrap();
        let kv = MemoryKv::open_sqlite(dir.path()).unwrap();
        kv.set("eto:branch:edge", vec![1]);
        kv.set("body:abcd", vec![2]);
        kv.set("ri:aa:01", vec![3]);
        drop(kv);
        let kv2 = MemoryKv::open_sqlite(dir.path()).unwrap();
        assert!(kv2.get("eto:branch:edge").is_none());
        assert!(kv2.get("body:abcd").is_none());
        assert_eq!(kv2.get("ri:aa:01"), Some(vec![3]));
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn sqlite_cas_blob_exact_match() {
        let kv = MemoryKv::open_sqlite_in_memory();
        let key = "merge_lock:job";
        kv.compare_and_swap(key, None, vec![1, 2, 3]).unwrap();
        kv.compare_and_swap(key, Some(&[1, 2]), vec![9]).unwrap_err();
        kv.compare_and_swap(key, Some(&[1, 2, 3]), vec![4]).unwrap();
        assert_eq!(kv.get(key), Some(vec![4]));
    }
}
