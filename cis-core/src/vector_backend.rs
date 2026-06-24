//! Vector store + ANN index backend boundary (**ADR 0006**).

use std::sync::Mutex;

use crate::semantic_ann::{AnnIndex, FlatAnnIndex};
use crate::vector_store::InMemoryVectorStore;

/// Wraps content-addressed vector storage and an ANN index (Qdrant/pgvector can implement this trait).
pub trait VectorBackend: Send + Sync {
    fn store(&self) -> &InMemoryVectorStore;
    fn rebuild_ann(&self);
    fn ann_top_k(&self, query: &[f32], k: usize) -> Vec<([u8; 32], f64)>;
    fn ann_len(&self) -> usize;
}

/// Default in-process backend: [`InMemoryVectorStore`] + [`FlatAnnIndex`].
#[derive(Debug)]
pub struct InMemoryVectorBackend {
    store: InMemoryVectorStore,
    ann: Mutex<FlatAnnIndex>,
}

impl InMemoryVectorBackend {
    pub fn new(store: InMemoryVectorStore) -> Self {
        Self {
            store,
            ann: Mutex::new(FlatAnnIndex::new()),
        }
    }

    pub fn from_parts(store: InMemoryVectorStore, ann: FlatAnnIndex) -> Self {
        Self {
            store,
            ann: Mutex::new(ann),
        }
    }

    pub fn ann_index(&self) -> &Mutex<FlatAnnIndex> {
        &self.ann
    }
}

impl VectorBackend for InMemoryVectorBackend {
    fn store(&self) -> &InMemoryVectorStore {
        &self.store
    }

    fn rebuild_ann(&self) {
        let snap = self.store.export_snapshot();
        let mut ann = self.ann.lock().unwrap();
        ann.clear();
        for v in snap.vectors {
            ann.upsert(v.body_hash, v.embedding);
        }
    }

    fn ann_top_k(&self, query: &[f32], k: usize) -> Vec<([u8; 32], f64)> {
        self.ann.lock().unwrap().top_k(query, k)
    }

    fn ann_len(&self) -> usize {
        self.ann.lock().unwrap().len()
    }
}
