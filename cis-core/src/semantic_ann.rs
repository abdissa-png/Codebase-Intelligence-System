//! Flat cosine ANN index over content-addressed embeddings (**Phase 7C** prototype).

use std::collections::HashMap;

use crate::embedder::cosine_similarity;

/// Pluggable approximate-nearest-neighbor index (**ADR 0006** extension point).
pub trait AnnIndex: Send {
    fn upsert(&mut self, body_hash: [u8; 32], vec: Vec<f32>);
    fn top_k(&self, query: &[f32], k: usize) -> Vec<([u8; 32], f64)>;
    fn clear(&mut self);
    fn len(&self) -> usize;
}

/// In-process flat index: body_hash → normalized vector. Rebuilt on ingest commit.
#[derive(Debug, Default)]
pub struct FlatAnnIndex {
    dim: usize,
    entries: Vec<([u8; 32], Vec<f32>)>,
    by_hash: HashMap<[u8; 32], usize>,
}

impl FlatAnnIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.dim = 0;
        self.entries.clear();
        self.by_hash.clear();
    }

    pub fn upsert(&mut self, body_hash: [u8; 32], vec: Vec<f32>) {
        if vec.is_empty() {
            return;
        }
        if self.dim == 0 {
            self.dim = vec.len();
        } else if vec.len() != self.dim {
            return;
        }
        if let Some(&i) = self.by_hash.get(&body_hash) {
            self.entries[i].1 = vec;
            return;
        }
        let i = self.entries.len();
        self.entries.push((body_hash, vec));
        self.by_hash.insert(body_hash, i);
    }

    pub fn remove(&mut self, body_hash: &[u8; 32]) {
        if let Some(i) = self.by_hash.remove(body_hash) {
            self.entries.swap_remove(i);
            if i < self.entries.len() {
                self.by_hash.insert(self.entries[i].0, i);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Top-K by cosine similarity against query vector.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<([u8; 32], f64)> {
        if self.entries.is_empty() || query.len() != self.dim || k == 0 {
            return Vec::new();
        }
        let mut scored: Vec<([u8; 32], f64)> = self
            .entries
            .iter()
            .map(|(h, v)| (*h, cosine_similarity(query, v)))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }
}

impl AnnIndex for FlatAnnIndex {
    fn upsert(&mut self, body_hash: [u8; 32], vec: Vec<f32>) {
        FlatAnnIndex::upsert(self, body_hash, vec);
    }

    fn top_k(&self, query: &[f32], k: usize) -> Vec<([u8; 32], f64)> {
        self.search(query, k)
    }

    fn clear(&mut self) {
        FlatAnnIndex::clear(self);
    }

    fn len(&self) -> usize {
        FlatAnnIndex::len(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_k_ordering() {
        let mut idx = FlatAnnIndex::new();
        let h1 = [1u8; 32];
        let h2 = [2u8; 32];
        idx.upsert(h1, vec![1.0, 0.0]);
        idx.upsert(h2, vec![0.0, 1.0]);
        let hits = idx.search(&[0.9, 0.1], 2);
        assert_eq!(hits[0].0, h1);
    }
}
