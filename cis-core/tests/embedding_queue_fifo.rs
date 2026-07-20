//! EmbeddingQueue is FIFO with O(1) reenqueue_front.

use cis_core::{EmbedJob, EmbeddingQueue};

fn job(n: u8) -> EmbedJob {
    EmbedJob {
        wal_log_id: n as u64,
        chunk_id: [n; 32],
        text_digest: [n; 32],
    }
}

#[test]
fn enqueue_drain_is_fifo() {
    let mut q = EmbeddingQueue::new();
    q.enqueue(job(1));
    q.enqueue(job(2));
    q.enqueue(job(3));
    assert_eq!(q.drain_one().unwrap().wal_log_id, 1);
    assert_eq!(q.drain_one().unwrap().wal_log_id, 2);
    assert_eq!(q.drain_one().unwrap().wal_log_id, 3);
    assert!(q.drain_one().is_none());
}

#[test]
fn reenqueue_front_is_next_drained() {
    let mut q = EmbeddingQueue::new();
    q.enqueue(job(1));
    q.enqueue(job(2));
    let first = q.drain_one().unwrap();
    assert_eq!(first.wal_log_id, 1);
    q.reenqueue_front(first);
    assert_eq!(q.drain_one().unwrap().wal_log_id, 1);
    assert_eq!(q.drain_one().unwrap().wal_log_id, 2);
}

#[test]
fn retries_do_not_starve_older_jobs() {
    let mut q = EmbeddingQueue::new();
    for i in 1..=5u8 {
        q.enqueue(job(i));
    }
    // Simulate retry of job 1 while 2..5 wait — reenqueue_front then FIFO drain.
    let j1 = q.drain_one().unwrap();
    q.reenqueue_front(j1);
    let order: Vec<u64> = (0..5).map(|_| q.drain_one().unwrap().wal_log_id).collect();
    assert_eq!(order, vec![1, 2, 3, 4, 5]);
}
