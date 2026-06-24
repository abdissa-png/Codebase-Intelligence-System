//! Vector store — content-addressed by **`body_hash`**, chunk lifecycle by **`chunk_id`** (v2.6 + Phase 6).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::embedder::cosine_similarity;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("vector chunk delete failed (degraded / timeout)")]
pub struct VectorDeleteError;

/// Stored embedding payload keyed by content hash.
#[derive(Debug, Clone)]
pub struct VectorEntry {
    pub vec: Vec<f32>,
    pub model_id: String,
}

/// Minimal store surface for merge rollback + cleanup queue.
pub trait VectorChunkStore: Send + Sync {
    fn has_chunk(&self, id: &[u8; 32]) -> bool;
    fn delete_chunks(&self, ids: &[[u8; 32]]) -> Result<(), VectorDeleteError>;
}

#[derive(Debug, Default)]
struct VectorStoreInner {
    /// Content-addressed embeddings: `body_hash` → vector + model.
    vectors: HashMap<[u8; 32], VectorEntry>,
    /// Chunk lifecycle unit: `chunk_id` → `body_hash`.
    chunk_to_body: HashMap<[u8; 32], [u8; 32]>,
    /// Reference count per `body_hash` (number of live chunks referencing it).
    refcount: HashMap<[u8; 32], usize>,
}

#[derive(Clone, Default, Debug)]
pub struct InMemoryVectorStore {
    inner: Arc<Mutex<VectorStoreInner>>,
}

impl InMemoryVectorStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a chunk referencing a body; embedding may be pending until worker fills it.
    pub fn register(&self, chunk_id: [u8; 32], body_hash: [u8; 32]) {
        let mut g = self.inner.lock().unwrap();
        if let Some(prev) = g.chunk_to_body.insert(chunk_id, body_hash) {
            if let Some(rc) = g.refcount.get_mut(&prev) {
                *rc = rc.saturating_sub(1);
                if *rc == 0 {
                    g.refcount.remove(&prev);
                    g.vectors.remove(&prev);
                }
            }
        }
        *g.refcount.entry(body_hash).or_insert(0) += 1;
    }

    /// Store (or replace) the embedding for a content hash.
    pub fn set_embedding(&self, body_hash: [u8; 32], embedding: Vec<f32>, model_id: impl Into<String>) {
        self.inner.lock().unwrap().vectors.insert(
            body_hash,
            VectorEntry {
                vec: embedding,
                model_id: model_id.into(),
            },
        );
    }

    /// Lookup embedding by content hash.
    pub fn vector_for_body(&self, body_hash: &[u8; 32]) -> Option<VectorEntry> {
        self.inner.lock().unwrap().vectors.get(body_hash).cloned()
    }

    /// Whether a chunk id is registered (embedding may still be pending).
    pub fn has_chunk(&self, chunk_id: &[u8; 32]) -> bool {
        self.inner.lock().unwrap().chunk_to_body.contains_key(chunk_id)
    }

    pub fn has(&self, chunk_id: &[u8; 32]) -> bool {
        self.has_chunk(chunk_id)
    }

    /// Legacy/test path: treat `chunk_id` as its own `body_hash`.
    pub fn upsert(&self, chunk_id: [u8; 32], embedding: Vec<f32>) {
        self.register(chunk_id, chunk_id);
        self.set_embedding(chunk_id, embedding, "legacy");
    }

    pub fn delete(&self, chunk_ids: &[[u8; 32]]) {
        let _ = self.delete_chunks(chunk_ids);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().chunk_to_body.len()
    }

    /// Number of distinct content hashes with stored embeddings.
    pub fn embedded_body_count(&self) -> usize {
        self.inner.lock().unwrap().vectors.len()
    }

    pub fn refcount_for_body(&self, body_hash: &[u8; 32]) -> usize {
        self.inner
            .lock()
            .unwrap()
            .refcount
            .get(body_hash)
            .copied()
            .unwrap_or(0)
    }

    pub fn body_for_chunk(&self, chunk_id: &[u8; 32]) -> Option<[u8; 32]> {
        self.inner.lock().unwrap().chunk_to_body.get(chunk_id).copied()
    }

    /// Export full store state for persistence (v2 snapshot).
    pub fn export_snapshot(&self) -> VectorStoreSnapshot {
        let g = self.inner.lock().unwrap();
        VectorStoreSnapshot {
            version: VECTOR_STORE_SNAPSHOT_VERSION,
            chunks: g
                .chunk_to_body
                .iter()
                .map(|(cid, bh)| VectorChunkRecord {
                    chunk_id: *cid,
                    body_hash: *bh,
                })
                .collect(),
            vectors: g
                .vectors
                .iter()
                .map(|(bh, e)| VectorBodyRecord {
                    body_hash: *bh,
                    embedding: e.vec.clone(),
                    model_id: e.model_id.clone(),
                })
                .collect(),
        }
    }

    pub fn restore_snapshot(&self, snap: &VectorStoreSnapshot) {
        let mut g = VectorStoreInner::default();
        for rec in &snap.chunks {
            g.chunk_to_body.insert(rec.chunk_id, rec.body_hash);
            *g.refcount.entry(rec.body_hash).or_insert(0) += 1;
        }
        for rec in &snap.vectors {
            g.vectors.insert(
                rec.body_hash,
                VectorEntry {
                    vec: rec.embedding.clone(),
                    model_id: rec.model_id.clone(),
                },
            );
        }
        *self.inner.lock().unwrap() = g;
    }

    /// Legacy export: chunk_id → embedding (only when chunk_id == body_hash or vector exists).
    pub fn export_chunks(&self) -> Vec<([u8; 32], Vec<f32>)> {
        let g = self.inner.lock().unwrap();
        g.chunk_to_body
            .keys()
            .filter_map(|cid| {
                let bh = g.chunk_to_body.get(cid)?;
                let entry = g.vectors.get(bh)?;
                Some((*cid, entry.vec.clone()))
            })
            .collect()
    }

    pub fn replace_all(&self, chunks: Vec<([u8; 32], Vec<f32>)>) {
        let mut g = VectorStoreInner::default();
        for (cid, emb) in chunks {
            g.chunk_to_body.insert(cid, cid);
            *g.refcount.entry(cid).or_insert(0) += 1;
            g.vectors.insert(
                cid,
                VectorEntry {
                    vec: emb,
                    model_id: "legacy".into(),
                },
            );
        }
        *self.inner.lock().unwrap() = g;
    }

    /// Cosine top-K over candidate `(body_hash, opaque_id)` pairs; skips missing embeddings.
    pub fn cosine_topk(
        &self,
        query: &[f32],
        candidates: &[([u8; 32], String)],
        k: usize,
    ) -> Vec<(String, f64)> {
        let g = self.inner.lock().unwrap();
        let mut scored: Vec<(String, f64)> = Vec::new();
        for (body_hash, id) in candidates {
            let Some(entry) = g.vectors.get(body_hash) else {
                continue;
            };
            let score = cosine_similarity(query, &entry.vec);
            scored.push((id.clone(), score));
        }
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        scored
    }
}

pub const VECTOR_STORE_SNAPSHOT_VERSION: u32 = 2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorChunkRecord {
    pub chunk_id: [u8; 32],
    pub body_hash: [u8; 32],
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorBodyRecord {
    pub body_hash: [u8; 32],
    pub embedding: Vec<f32>,
    pub model_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorStoreSnapshot {
    pub version: u32,
    pub chunks: Vec<VectorChunkRecord>,
    pub vectors: Vec<VectorBodyRecord>,
}

impl VectorChunkStore for InMemoryVectorStore {
    fn has_chunk(&self, id: &[u8; 32]) -> bool {
        self.has_chunk(id)
    }

    fn delete_chunks(&self, ids: &[[u8; 32]]) -> Result<(), VectorDeleteError> {
        let mut g = self.inner.lock().unwrap();
        for cid in ids {
            let Some(body_hash) = g.chunk_to_body.remove(cid) else {
                continue;
            };
            if let Some(rc) = g.refcount.get_mut(&body_hash) {
                *rc = rc.saturating_sub(1);
                if *rc == 0 {
                    g.refcount.remove(&body_hash);
                    g.vectors.remove(&body_hash);
                }
            }
        }
        Ok(())
    }
}

/// Test / simulation: fail deletes when `fail_next` is toggled.
#[derive(Debug)]
pub struct FlakyVectorStore {
    inner: InMemoryVectorStore,
    fail_delete: AtomicBool,
}

impl FlakyVectorStore {
    pub fn new(inner: InMemoryVectorStore) -> Self {
        Self {
            inner,
            fail_delete: AtomicBool::new(false),
        }
    }

    pub fn set_fail_delete(&self, v: bool) {
        self.fail_delete.store(v, Ordering::SeqCst);
    }

    pub fn inner(&self) -> &InMemoryVectorStore {
        &self.inner
    }
}

impl VectorChunkStore for FlakyVectorStore {
    fn has_chunk(&self, id: &[u8; 32]) -> bool {
        self.inner.has_chunk(id)
    }

    fn delete_chunks(&self, ids: &[[u8; 32]]) -> Result<(), VectorDeleteError> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(VectorDeleteError);
        }
        self.inner.delete_chunks(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_set_embedding() {
        let s = InMemoryVectorStore::new();
        let chunk = [1u8; 32];
        let body = [2u8; 32];
        s.register(chunk, body);
        assert!(s.has_chunk(&chunk));
        assert!(s.vector_for_body(&body).is_none());
        s.set_embedding(body, vec![1.0, 0.0], "test");
        assert_eq!(s.vector_for_body(&body).unwrap().vec, vec![1.0, 0.0]);
    }

    #[test]
    fn refcounted_delete_preserves_shared_body() {
        let s = InMemoryVectorStore::new();
        let c1 = [1u8; 32];
        let c2 = [2u8; 32];
        let body = [9u8; 32];
        s.register(c1, body);
        s.register(c2, body);
        s.set_embedding(body, vec![0.5, 0.5], "m");
        assert_eq!(s.refcount_for_body(&body), 2);
        s.delete_chunks(&[c1]).unwrap();
        assert!(!s.has_chunk(&c1));
        assert!(s.has_chunk(&c2));
        assert!(s.vector_for_body(&body).is_some());
        s.delete_chunks(&[c2]).unwrap();
        assert!(s.vector_for_body(&body).is_none());
    }

    #[test]
    fn multi_branch_shares_body_hash_vector() {
        let s = InMemoryVectorStore::new();
        let body = [77u8; 32];
        let c_main = [1u8; 32];
        let c_feat = [2u8; 32];
        s.register(c_main, body);
        s.register(c_feat, body);
        s.set_embedding(body, vec![0.6, 0.8], "stub/hashed-bow");
        assert_eq!(s.refcount_for_body(&body), 2);
        assert_eq!(s.embedded_body_count(), 1);
        s.delete_chunks(&[c_main]).unwrap();
        assert!(s.vector_for_body(&body).is_some());
        assert_eq!(s.refcount_for_body(&body), 1);
    }

    #[test]
    fn cosine_topk_orders_by_similarity() {
        let s = InMemoryVectorStore::new();
        let h1 = [10u8; 32];
        let h2 = [11u8; 32];
        s.set_embedding(h1, vec![1.0, 0.0], "m");
        s.set_embedding(h2, vec![0.0, 1.0], "m");
        let q = vec![1.0, 0.0];
        let hits = s.cosine_topk(
            &q,
            &[
                (h1, "a".into()),
                (h2, "b".into()),
            ],
            2,
        );
        assert_eq!(hits[0].0, "a");
        assert!(hits[0].1 > hits[1].1);
    }
}
