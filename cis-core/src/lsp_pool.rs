//! **LSPPool** — `(Language, BranchId)` virtual workspace + cache keys (**FR-1.3**, **RC-9**, v2.5 cache poisoning fix).

use std::collections::HashMap;
use std::sync::Mutex;

use cis_wal::BranchId;

use crate::graph::Language;
use crate::MemoryKv;

/// v2.5 / v2.6: **`hash(content_hash ‖ dependency_content_hashes)`** for LSP structural cache.
pub fn lsp_cache_key(
    virtual_uri: &str,
    content_hash: &[u8; 32],
    dependency_hashes: &[[u8; 32]],
) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    virtual_uri.hash(&mut h);
    content_hash.hash(&mut h);
    for d in dependency_hashes {
        d.hash(&mut h);
    }
    let x = h.finish();
    format!("lsp:{virtual_uri}:{x:016x}")
}

#[derive(Debug, Default)]
pub struct LspPoolState {
    virtual_workspace_uris: Mutex<HashMap<(Language, BranchId), String>>,
}

impl LspPoolState {
    pub fn new() -> Self {
        Self::default()
    }

    /// **`cis-virtual://`** workspace URI per (**language**, **branch**).
    pub fn virtual_uri_for(&self, language: Language, branch: BranchId) -> String {
        let key = (language, branch);
        let mut g = self.virtual_workspace_uris.lock().unwrap();
        g.entry(key)
            .or_insert_with(|| {
                format!(
                    "cis-virtual://{:?}/{}",
                    language,
                    branch
                        .0
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                )
            })
            .clone()
    }
}

#[derive(Debug, Default)]
pub struct LspSessionFlags {
    pub lsp_unavailable: bool,
}

impl LspSessionFlags {
    pub fn set_degraded(&mut self, unavailable: bool) {
        self.lsp_unavailable = unavailable;
    }
}

/// **v2.5** — 24h GC for **`lsp:`** KV entries (`stored_at_ms` in value prefix).
#[derive(Debug)]
pub struct LspCacheSweeper {
    ttl_ms: u64,
}

impl LspCacheSweeper {
    pub const DEFAULT_TTL_MS: u64 = 86_400_000;

    pub fn new(ttl_ms: u64) -> Self {
        Self { ttl_ms }
    }

    /// Remove expired **`lsp:`** keys; returns removed count.
    pub fn sweep(&self, kv: &MemoryKv, now_ms: u64) -> usize {
        let prefix = "lsp:";
        let mut n = 0usize;
        for (k, v) in kv.scan_prefix(prefix) {
            if v.len() < 8 {
                continue;
            }
            let mut ts = [0u8; 8];
            ts.copy_from_slice(&v[..8]);
            let stored = u64::from_le_bytes(ts);
            if now_ms.saturating_sub(stored) > self.ttl_ms {
                kv.delete(&k);
                n += 1;
            }
        }
        n
    }
}

/// Encode **value** = `stored_at_ms (le u8×8) ‖ payload` for sweeper.
pub fn lsp_cache_value_with_timestamp(stored_at_ms: u64, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + payload.len());
    v.extend_from_slice(&stored_at_ms.to_le_bytes());
    v.extend_from_slice(payload);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_uri_stable() {
        let p = LspPoolState::new();
        let u1 = p.virtual_uri_for(Language::Python, BranchId([1u8; 16]));
        let u2 = p.virtual_uri_for(Language::Python, BranchId([1u8; 16]));
        assert_eq!(u1, u2);
        assert!(u1.starts_with("cis-virtual://"));
    }

    #[test]
    fn cache_key_changes_with_dependency() {
        let u = "cis-virtual://Python/01";
        let c = [3u8; 32];
        let k1 = lsp_cache_key(u, &c, &[]);
        let k2 = lsp_cache_key(u, &c, &[[1u8; 32]]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn sweeper_removes_stale() {
        let kv = MemoryKv::new();
        let k = "lsp:test:k";
        let old = lsp_cache_value_with_timestamp(0, b"x");
        kv.set(k, old);
        let s = LspCacheSweeper::new(1000);
        assert_eq!(s.sweep(&kv, 5000), 1);
        assert!(kv.get(k).is_none());
    }
}
