//! Concurrent DurableMutationLog flush must not lose records or tear snapshots.

use std::sync::Arc;
use std::thread;

use cis_wal::{
    DurableMutationLog, MutationKind, MutationLogStore, MutationPhase, MutationRecord,
    NodeRevisionId,
};

fn mk(n: u8) -> MutationRecord {
    let mut b = [0u8; 16];
    b[15] = n;
    MutationRecord {
        log_id: 0,
        kind: MutationKind::Single,
        phase: MutationPhase::Pending,
        affected_revisions: vec![NodeRevisionId(b)],
        payload_checksum: [n; 32],
        created_at_ms: 1,
    }
}

#[test]
fn concurrent_append_survives_reload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.json");
    let wal = Arc::new(DurableMutationLog::create_new(&path).unwrap());

    let mut handles = Vec::new();
    for i in 0..8u8 {
        let w = Arc::clone(&wal);
        handles.push(thread::spawn(move || {
            for j in 0..20u8 {
                let id = w.append(mk(i.wrapping_mul(20).wrapping_add(j))).unwrap();
                let _ = w.update_phase(id, MutationPhase::GraphDone);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let n = wal.iter_all().len();
    assert_eq!(n, 160, "in-memory must retain all concurrent appends");

    drop(wal);
    let reloaded = DurableMutationLog::open(&path).unwrap();
    assert_eq!(
        reloaded.iter_all().len(),
        160,
        "reloaded snapshot must retain all records"
    );
    // Snapshot must parse as valid JSON (open already validated).
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(serde_json::from_str::<serde_json::Value>(&raw).is_ok());
}
