//! Background drain of **`EmbeddingQueue`** — async embedding computation (Phase 6).

use std::collections::HashSet;

use crate::body_store::BodyStore;
use crate::coordinator::WriteCoordinator;
use crate::embedder::Embedder;
use crate::embedding_queue::EmbedJob;
use crate::vector_store::InMemoryVectorStore;

/// One logical pump of the embedding queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmbeddingDrainReport {
    pub attempted: usize,
    pub embedded_ok: usize,
    pub cache_hits: usize,
    pub requeued: usize,
    pub skipped_no_body: usize,
}

#[derive(Debug)]
pub struct EmbeddingWorker;

impl EmbeddingWorker {
    /// Process up to **`max_batch`** jobs from the coordinator queue.
    pub fn drain_batch(
        coord: &WriteCoordinator,
        embedder: &dyn Embedder,
        body_store: &BodyStore,
        vector: &InMemoryVectorStore,
        max_batch: usize,
    ) -> EmbeddingDrainReport {
        let jobs = coord.drain_embed_jobs(max_batch);
        if jobs.is_empty() {
            return EmbeddingDrainReport::default();
        }

        let model_id = embedder.model_id();
        let mut report = EmbeddingDrainReport {
            attempted: jobs.len(),
            ..Default::default()
        };

        let mut seen_bodies: HashSet<[u8; 32]> = HashSet::new();
        let mut pending: Vec<EmbedJob> = Vec::new();
        let mut texts: Vec<String> = Vec::new();
        let mut body_hashes: Vec<[u8; 32]> = Vec::new();
        let graph = coord.graph().read();

        for job in jobs {
            let bh = job.text_digest;
            if !seen_bodies.insert(bh) {
                continue;
            }
            if let Some(existing) = vector.vector_for_body(&bh) {
                if existing.model_id == model_id {
                    report.cache_hits += 1;
                    continue;
                }
            }
            let Some(bytes) = body_store.get(&bh) else {
                report.skipped_no_body += 1;
                pending.push(job);
                continue;
            };
            let snippet = String::from_utf8_lossy(&bytes);
            let enrich = graph
                .revisions()
                .find(|r| r.body_hash == bh)
                .map(|r| {
                    format!(
                        "{}\n{}\n{}",
                        r.qualified_name, r.file_path, snippet
                    )
                })
                .unwrap_or_else(|| snippet.into_owned());
            texts.push(enrich);
            body_hashes.push(bh);
            pending.push(job);
        }
        drop(graph);

        if texts.is_empty() {
            for job in pending {
                coord.reenqueue_embed_front(job);
                report.requeued += 1;
            }
            return report;
        }

        match embedder.embed_batch(&texts) {
            Ok(vectors) => {
                let mut ann_batch: Vec<([u8; 32], Vec<f32>)> =
                    Vec::with_capacity(body_hashes.len());
                for (bh, vec) in body_hashes.into_iter().zip(vectors) {
                    // Clone for the vector store; move the original into the ANN batch.
                    vector.set_embedding(bh, vec.clone(), model_id);
                    ann_batch.push((bh, vec));
                    report.embedded_ok += 1;
                }
                if !ann_batch.is_empty() {
                    coord.run_post_embed_hook(ann_batch);
                }
            }
            Err(_) => {
                for job in pending {
                    coord.reenqueue_embed_front(job);
                    report.requeued += 1;
                }
            }
        }

        report
    }

    /// **DESIGNED** backoff ladder (ms): 250, 500, … capped at **`cap_ms`** (default cap 30_000).
    pub fn next_backoff_ms(fail_streak: u32, cap_ms: u64) -> u64 {
        let ms = 250u64 << fail_streak.min(10);
        ms.min(cap_ms)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cis_wal::{BranchId, IdentityId, MutationLog, MutationLogStore, NodeRevisionId};

    use crate::chunk_id::chunk_id;
    use crate::coordinator::WriteCoordinator;
    use crate::embedder::{EmbedError, Embedder, StubEmbedder};
    use crate::graph::{
        Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
    };
    use crate::graph_mutation::GraphMutationSet;
    use crate::kv::MemoryKv;

    use super::*;

    struct FlakyEmbedder {
        inner: StubEmbedder,
        fail: std::sync::atomic::AtomicBool,
    }

    impl FlakyEmbedder {
        fn new() -> Self {
            Self {
                inner: StubEmbedder::new(),
                fail: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl Embedder for FlakyEmbedder {
        fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(EmbedError::Api("simulated".into()));
            }
            self.inner.embed_batch(texts)
        }

        fn dim(&self) -> usize {
            self.inner.dim()
        }

        fn model_id(&self) -> &str {
            self.inner.model_id()
        }
    }

    fn seed_revision(coord: &WriteCoordinator, body: &[u8], body_hash: [u8; 32]) -> [u8; 32] {
        let kv = Arc::new(MemoryKv::new());
        let bs = BodyStore::new(kv);
        bs.put(body_hash, body.to_vec());

        let r = NodeRevisionId([7u8; 16]);
        let i = IdentityId([8u8; 16]);
        let set = GraphMutationSet::new(vec![r], [1u8; 32]);
        let id = coord.begin_mutation(&set).unwrap();
        coord
            .commit_graph(id, |g| {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "auth.login".into(),
                    file_path: "auth.py".into(),
                    body_hash,
                    signature_hash: [2u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
        coord.commit_vector(id).unwrap();
        chunk_id(i, r, 0)
    }

    #[test]
    fn worker_drains_queue_and_populates_vector() {
        let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let bs = BodyStore::new(Arc::clone(&kv));
        let body_hash = [42u8; 32];
        bs.put(body_hash, b"def authenticate(): pass".to_vec());

        let r = NodeRevisionId([7u8; 16]);
        let i = IdentityId([8u8; 16]);
        let set = GraphMutationSet::new(vec![r], [1u8; 32]);
        let id = coord.begin_mutation(&set).unwrap();
        coord
            .commit_graph(id, |g| {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: BranchId([0u8; 16]),
                    status: RevisionStatus::Active,
                    qualified_name: "auth.login".into(),
                    file_path: "auth.py".into(),
                    body_hash,
                    signature_hash: [2u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: crate::graph::SourceSpan::UNKNOWN,
                });
                Ok(())
            })
            .unwrap();
        coord.commit_vector(id).unwrap();
        assert!(coord.embedding_queue_depth() >= 1);

        let embedder = StubEmbedder::new();
        let rep = EmbeddingWorker::drain_batch(
            &coord,
            &embedder,
            &bs,
            coord.vector(),
            8,
        );
        assert!(rep.embedded_ok >= 1);
        assert!(coord.vector().vector_for_body(&body_hash).is_some());
    }

    #[test]
    fn worker_requeues_on_embed_failure() {
        let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let bs = BodyStore::new(Arc::clone(&kv));
        let body_hash = [55u8; 32];
        bs.put(body_hash, b"def authenticate(): pass".to_vec());
        let _ = seed_revision(&coord, b"def authenticate(): pass", body_hash);

        let flaky = FlakyEmbedder::new();
        flaky.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let depth_before = coord.embedding_queue_depth();
        let rep = EmbeddingWorker::drain_batch(&coord, &flaky, &bs, coord.vector(), 8);
        assert!(rep.requeued >= 1);
        assert!(coord.embedding_queue_depth() >= depth_before);
    }

    #[test]
    fn worker_invokes_post_embed_hook_with_batch() {
        let wal: Arc<dyn MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let bs = BodyStore::new(Arc::clone(&kv));
        let body_hash = [77u8; 32];
        bs.put(body_hash, b"def hook_probe(): pass".to_vec());
        let _ = seed_revision(&coord, b"def hook_probe(): pass", body_hash);

        let seen: Arc<std::sync::Mutex<Vec<([u8; 32], Vec<f32>)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_hook = Arc::clone(&seen);
        coord.set_post_embed_hook(Some(Arc::new(move |entries| {
            *seen_hook.lock().unwrap() = entries;
        })));

        let embedder = StubEmbedder::new();
        let rep = EmbeddingWorker::drain_batch(&coord, &embedder, &bs, coord.vector(), 8);
        assert!(rep.embedded_ok >= 1);
        let batch = seen.lock().unwrap();
        assert_eq!(batch.len(), rep.embedded_ok);
        assert_eq!(batch[0].0, body_hash);
        assert!(!batch[0].1.is_empty());
    }
}
