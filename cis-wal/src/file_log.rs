//! JSON snapshot persistence for **`MutationLog`** (**FR-1.7**, **NFR-R2**).
//!
//! Writes atomically: unique temp → `fsync` → `rename` → **`wal.json`**.
//! A process-wide flush mutex serializes snapshot+rename so concurrent writers
//! cannot tear or lose records.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::log::{LogId, MutationLog, MutationLogError, WalCompactionReport};
use crate::record::MutationRecord;
use crate::wal_backend::MutationLogStore;

#[derive(Debug, Serialize, Deserialize)]
struct WalSnapshot {
    next_allocate_id: LogId,
    records: Vec<MutationRecord>,
}

/// File-backed WAL wrapping in-memory [`MutationLog`]; every mutation **flushes** full snapshot.
#[derive(Debug)]
pub struct DurableMutationLog {
    path: PathBuf,
    log: MutationLog,
    /// Serializes snapshot materialization + rename.
    flush_lock: Mutex<()>,
    flush_seq: AtomicU64,
}

impl DurableMutationLog {
    pub fn create_new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let log = MutationLog::new();
        let d = Self {
            path,
            log,
            flush_lock: Mutex::new(()),
            flush_seq: AtomicU64::new(0),
        };
        d.flush()?;
        Ok(d)
    }

    /// Load existing snapshot or create empty if missing.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if !path.exists() {
            return Self::create_new(path);
        }
        let f = File::open(&path)?;
        let snap: WalSnapshot = serde_json::from_reader(f)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let log = MutationLog::restore(snap.next_allocate_id, snap.records).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
        })?;
        Ok(Self {
            path,
            log,
            flush_lock: Mutex::new(()),
            flush_seq: AtomicU64::new(0),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn inner(&self) -> &MutationLog {
        &self.log
    }

    pub fn append(&self, record: MutationRecord) -> Result<LogId, MutationLogError> {
        let id = self.log.append(record)?;
        self.flush()
            .map_err(|e| MutationLogError::Persist(e.to_string()))?;
        Ok(id)
    }

    pub fn update_phase(
        &self,
        id: LogId,
        phase: crate::phase::MutationPhase,
    ) -> Result<(), MutationLogError> {
        self.log.update_phase(id, phase)?;
        self.flush()
            .map_err(|e| MutationLogError::Persist(e.to_string()))?;
        Ok(())
    }

    pub fn truncate_committed(
        &self,
        wal_max_bytes: u64,
    ) -> Result<WalCompactionReport, MutationLogError> {
        let rep = self.log.truncate_committed(wal_max_bytes);
        self.flush()
            .map_err(|e| MutationLogError::Persist(e.to_string()))?;
        Ok(rep)
    }

    fn flush(&self) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        let snap = WalSnapshot {
            next_allocate_id: self.log.next_allocate_id(),
            records: self.log.iter_all(),
        };
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let seq = self.flush_seq.fetch_add(1, Ordering::SeqCst);
        let tmp = self.path.with_extension(format!(
            "json.tmp.{}.{}",
            std::process::id(),
            seq
        ));
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            serde_json::to_writer_pretty(&mut f, &snap)?;
            f.flush()?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl MutationLogStore for DurableMutationLog {
    fn append(&self, record: MutationRecord) -> Result<LogId, MutationLogError> {
        DurableMutationLog::append(self, record)
    }

    fn get(&self, id: LogId) -> Option<MutationRecord> {
        self.log.get(id)
    }

    fn update_phase(
        &self,
        id: LogId,
        phase: crate::phase::MutationPhase,
    ) -> Result<(), MutationLogError> {
        DurableMutationLog::update_phase(self, id, phase)
    }

    fn iter_all(&self) -> Vec<MutationRecord> {
        self.log.iter_all()
    }

    fn record_count(&self) -> usize {
        self.log.len()
    }

    fn next_allocate_id(&self) -> LogId {
        self.log.next_allocate_id()
    }

    fn truncate_committed(
        &self,
        wal_max_bytes: u64,
    ) -> Result<WalCompactionReport, MutationLogError> {
        DurableMutationLog::truncate_committed(self, wal_max_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{MutationKind, MutationRecord};
    use crate::{MutationPhase, NodeRevisionId};

    fn rid(n: u8) -> NodeRevisionId {
        let mut b = [0u8; 16];
        b[15] = n;
        NodeRevisionId(b)
    }

    fn mk(phase: MutationPhase, revs: Vec<NodeRevisionId>) -> MutationRecord {
        MutationRecord {
            log_id: 0,
            kind: MutationKind::Single,
            phase,
            affected_revisions: revs,
            payload_checksum: [1u8; 32],
            created_at_ms: 42,
        }
    }

    /// NFR-R2 / FR-1.7 — reload after process boundary.
    #[test]
    fn durable_wal_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("cis-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wal.json");

        let r = rid(7);
        {
            let wal = DurableMutationLog::create_new(&path).unwrap();
            let id = wal
                .append(mk(MutationPhase::Pending, vec![r]))
                .unwrap();
            wal.update_phase(id, MutationPhase::GraphDone).unwrap();
        }

        let wal2 = DurableMutationLog::open(&path).unwrap();
        let rec = wal2
            .inner()
            .get(1)
            .expect("record 1");
        assert_eq!(rec.phase, MutationPhase::GraphDone);
        assert_eq!(rec.affected_revisions, vec![r]);
    }
}
