//! JSON snapshot persistence for **`MutationLog`** (**FR-1.7**, **NFR-R2**).
//!
//! Writes atomically: unique temp → optional `fsync` → `rename` → **`wal.json`**.
//! A process-wide flush mutex serializes snapshot+rename so concurrent writers
//! cannot tear or lose records.
//!
//! ## Performance knobs (env)
//! - `CIS_WAL_FLUSH_EVERY=N` — persist every N mutations (default `1`). Use higher
//!   values (e.g. `64`) during cold bootstrap; dirty state is flushed on Drop.
//! - `CIS_WAL_FSYNC=0` — skip `sync_all` (still atomic rename). Faster on slow disks.

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

fn flush_every() -> u64 {
    std::env::var("CIS_WAL_FLUSH_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(1)
}

fn fsync_enabled() -> bool {
    !std::env::var_os("CIS_WAL_FSYNC").is_some_and(|v| {
        v == "0" || v.eq_ignore_ascii_case("false")
    })
}

/// File-backed WAL wrapping in-memory [`MutationLog`]; mutations flush per policy.
#[derive(Debug)]
pub struct DurableMutationLog {
    path: PathBuf,
    log: MutationLog,
    /// Serializes snapshot materialization + rename.
    flush_lock: Mutex<()>,
    flush_seq: AtomicU64,
    /// Mutations since the last successful durable flush.
    dirty_ops: AtomicU64,
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
            dirty_ops: AtomicU64::new(0),
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
            dirty_ops: AtomicU64::new(0),
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
        self.note_dirty_and_maybe_flush()?;
        Ok(id)
    }

    pub fn update_phase(
        &self,
        id: LogId,
        phase: crate::phase::MutationPhase,
    ) -> Result<(), MutationLogError> {
        self.log.update_phase(id, phase)?;
        self.note_dirty_and_maybe_flush()?;
        Ok(())
    }

    pub fn truncate_committed(
        &self,
        wal_max_bytes: u64,
    ) -> Result<WalCompactionReport, MutationLogError> {
        let rep = self.log.truncate_committed(wal_max_bytes);
        // Compaction always persists immediately.
        self.flush()
            .map_err(|e| MutationLogError::Persist(e.to_string()))?;
        self.dirty_ops.store(0, Ordering::SeqCst);
        Ok(rep)
    }

    /// Force a durable snapshot regardless of `CIS_WAL_FLUSH_EVERY`.
    pub fn flush_now(&self) -> io::Result<()> {
        self.flush()?;
        self.dirty_ops.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn note_dirty_and_maybe_flush(&self) -> Result<(), MutationLogError> {
        let n = self.dirty_ops.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= flush_every() {
            self.flush()
                .map_err(|e| MutationLogError::Persist(e.to_string()))?;
            self.dirty_ops.store(0, Ordering::SeqCst);
        }
        Ok(())
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
            // Compact JSON — pretty-printing made cold bootstrap rewrite multi‑MB wal
            // snapshots on every mutation phase.
            serde_json::to_writer(&mut f, &snap)?;
            f.flush()?;
            if fsync_enabled() {
                f.sync_all()?;
            }
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl Drop for DurableMutationLog {
    fn drop(&mut self) {
        if self.dirty_ops.load(Ordering::SeqCst) > 0 {
            let _ = self.flush();
        }
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

    fn flush_persistent(&self) -> Result<(), MutationLogError> {
        self.flush_now()
            .map_err(|e| MutationLogError::Persist(e.to_string()))
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
            wal.flush_now().unwrap();
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
