//! Content-addressed **`body:`** prefix (**C-4**, **FR-1.13** alignment).

use std::sync::Arc;

use crate::kv::MemoryKv;

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

/// Optional disk/blob lookup used when the in-memory `body:` KV is cold.
pub type BodyFallback = Arc<dyn Fn(&[u8; 32]) -> Option<Vec<u8>> + Send + Sync>;

pub struct BodyStore {
    kv: Arc<MemoryKv>,
    fallback: Option<BodyFallback>,
}

impl std::fmt::Debug for BodyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyStore").finish_non_exhaustive()
    }
}

impl Clone for BodyStore {
    fn clone(&self) -> Self {
        Self {
            kv: Arc::clone(&self.kv),
            fallback: self.fallback.clone(),
        }
    }
}

impl BodyStore {
    pub fn new(kv: Arc<MemoryKv>) -> Self {
        Self { kv, fallback: None }
    }

    /// Query path: KV first, then blob/file store. Does not copy the blob back into KV
    /// (avoid silent full-corpus rehydrate via embed/sync loops).
    pub fn with_fallback(kv: Arc<MemoryKv>, fallback: BodyFallback) -> Self {
        Self {
            kv,
            fallback: Some(fallback),
        }
    }

    pub fn put(&self, body_hash: [u8; 32], content: Vec<u8>) {
        let key = format!("body:{}", hex32(&body_hash));
        self.kv.set(&key, content);
    }

    /// Memory-only lookup used by blob sync/GC so a disk fallback cannot pull the
    /// whole corpus back into a checkpoint write.
    pub fn get_cached(&self, body_hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.kv.get(&format!("body:{}", hex32(body_hash)))
    }

    pub fn get(&self, body_hash: &[u8; 32]) -> Option<Vec<u8>> {
        if let Some(bytes) = self.get_cached(body_hash) {
            return Some(bytes);
        }
        self.fallback.as_ref().and_then(|f| f(body_hash))
    }

    pub fn has(&self, body_hash: &[u8; 32]) -> bool {
        self.get(body_hash).is_some()
    }

    pub fn delete(&self, body_hash: &[u8; 32]) {
        self.kv.delete(&format!("body:{}", hex32(body_hash)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let kv = Arc::new(MemoryKv::new());
        let bs = BodyStore::new(kv);
        let h = [7u8; 32];
        bs.put(h, b"hello".to_vec());
        assert_eq!(bs.get(&h), Some(b"hello".to_vec()));
    }

    #[test]
    fn get_uses_fallback_without_caching() {
        let kv = Arc::new(MemoryKv::new());
        let h = [9u8; 32];
        let bs = BodyStore::with_fallback(
            Arc::clone(&kv),
            Arc::new(|hash| {
                if *hash == [9u8; 32] {
                    Some(b"from-disk".to_vec())
                } else {
                    None
                }
            }),
        );
        assert_eq!(bs.get(&h), Some(b"from-disk".to_vec()));
        assert!(bs.get_cached(&h).is_none(), "fallback must not fill KV");
    }
}
