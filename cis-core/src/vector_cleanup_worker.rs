//! Background drain of **`VectorCleanupQueue`** (**v2.6** `VectorCleanupWorker`) — exponential backoff between steps when deletes fail.

use std::sync::Arc;
use std::time::Duration;

use crate::vector_cleanup_queue::VectorCleanupQueue;
use crate::vector_store::VectorChunkStore;

/// One logical pump of the DLQ (suitable for a timer or reconciler tick).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorCleanupDrainReport {
    pub attempted: usize,
    pub deleted_ok: usize,
    pub requeued: usize,
}

#[derive(Debug)]
pub struct VectorCleanupWorker {
    queue: Arc<VectorCleanupQueue>,
}

impl VectorCleanupWorker {
    pub fn new(queue: Arc<VectorCleanupQueue>) -> Self {
        Self { queue }
    }

    pub fn queue(&self) -> Arc<VectorCleanupQueue> {
        Arc::clone(&self.queue)
    }

    /// Process up to **`max_batch`** head entries. Failed deletes are **re-enqueued** at the tail (at-least-once).
    pub fn drain_batch(&self, vector: &dyn VectorChunkStore, max_batch: usize) -> VectorCleanupDrainReport {
        let mut report = VectorCleanupDrainReport::default();
        for _ in 0..max_batch {
            let Some(cid) = self.queue.dequeue() else {
                break;
            };
            report.attempted += 1;
            match vector.delete_chunks(&[cid]) {
                Ok(()) => report.deleted_ok += 1,
                Err(_) => {
                    self.queue.enqueue_delete(cid);
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

    /// Sleep helper for embedded loops (production often uses a timer wheel instead).
    pub fn sleep_backoff(fail_streak: u32) {
        let d = Duration::from_millis(Self::next_backoff_ms(fail_streak, 30_000));
        std::thread::sleep(d);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_store::{FlakyVectorStore, InMemoryVectorStore};

    #[test]
    fn requeues_on_delete_fail_then_drains() {
        let q = Arc::new(VectorCleanupQueue::new());
        q.enqueue_delete([9u8; 32]);
        let inner = InMemoryVectorStore::new();
        inner.upsert([9u8; 32], vec![1.0]);
        let flaky = FlakyVectorStore::new(inner);
        let w = VectorCleanupWorker::new(Arc::clone(&q));
        flaky.set_fail_delete(true);
        // One attempt per batch: otherwise a failing delete would be retried `max_batch` times in one pump.
        let r1 = w.drain_batch(&flaky, 1);
        assert_eq!(r1.requeued, 1);
        assert_eq!(q.depth(), 1);
        flaky.set_fail_delete(false);
        let r2 = w.drain_batch(&flaky, 8);
        assert_eq!(r2.deleted_ok, 1);
        assert_eq!(q.depth(), 0);
        assert!(!flaky.inner().has(&[9u8; 32]));
    }

    #[test]
    fn backoff_caps() {
        assert_eq!(VectorCleanupWorker::next_backoff_ms(0, 30_000), 250);
        assert_eq!(VectorCleanupWorker::next_backoff_ms(20, 500), 500);
    }
}
