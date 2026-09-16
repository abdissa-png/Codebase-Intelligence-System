//! SQLite-backed application WAL (`CIS_WAL_BACKEND=sqlite`) in `.cis/wal.db`.
//!
//! Append / phase update / compaction are single-row DML. The in-memory
//! [`MutationLog`] remains the query cache so reconcile stays O(records) in RAM
//! without rewriting `wal.json` on every mutation.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use cis_wal::{
    DurableMutationLog, LogId, MutationLog, MutationLogError, MutationLogStore, MutationPhase,
    MutationRecord, WalCompactionReport,
};
use rusqlite::{params, Connection, OptionalExtension};

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS wal (
        log_id INTEGER PRIMARY KEY,
        phase INTEGER NOT NULL,
        payload BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wal_meta (
        k TEXT PRIMARY KEY,
        v INTEGER NOT NULL
    );
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalBackendKind {
    Json,
    Sqlite,
}

/// Read `CIS_WAL_BACKEND=json|sqlite` (default `json`).
pub fn wal_backend_from_env() -> WalBackendKind {
    match std::env::var_os("CIS_WAL_BACKEND") {
        Some(v) if v == "sqlite" || v == "sqlite3" => WalBackendKind::Sqlite,
        _ => WalBackendKind::Json,
    }
}

/// When `CIS_WAL_BACKEND=sqlite`, also write `wal.json` if `CIS_WAL_JSON_EXPORT=1`.
pub fn wal_json_export_enabled() -> bool {
    std::env::var_os("CIS_WAL_JSON_EXPORT").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    })
}

pub fn wal_db_path(cis_dir: impl AsRef<Path>) -> PathBuf {
    cis_dir.as_ref().join("wal.db")
}

fn phase_to_i64(p: MutationPhase) -> i64 {
    match p {
        MutationPhase::Pending => 0,
        MutationPhase::GraphDone => 1,
        MutationPhase::VectorDone => 2,
        MutationPhase::Committed => 3,
        MutationPhase::Failed => 4,
    }
}

fn configure_conn(conn: &Connection) -> io::Result<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(|e| io::Error::other(e.to_string()))?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    let sync = if std::env::var_os("CIS_WAL_FSYNC").is_some_and(|v| {
        v == "0" || v.eq_ignore_ascii_case("false")
    }) {
        "NORMAL"
    } else {
        "FULL"
    };
    conn.pragma_update(None, "synchronous", sync)
        .map_err(|e| io::Error::other(e.to_string()))?;
    conn.execute_batch(SCHEMA)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(())
}

/// Durable mutation log: RAM cache + incremental SQLite rows.
pub struct SqliteMutationLog {
    cis_dir: PathBuf,
    log: MutationLog,
    conn: Mutex<Connection>,
    sql_writes: AtomicU64,
}

impl std::fmt::Debug for SqliteMutationLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteMutationLog")
            .field("records", &self.log.len())
            .finish_non_exhaustive()
    }
}

impl SqliteMutationLog {
    pub fn open(cis_dir: impl AsRef<Path>) -> io::Result<Self> {
        let cis_dir = cis_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&cis_dir)?;
        let path = wal_db_path(&cis_dir);
        let conn = Connection::open(&path).map_err(|e| io::Error::other(e.to_string()))?;
        configure_conn(&conn)?;
        let sql_writes = AtomicU64::new(0);
        let n = count_rows(&conn)?;
        if n == 0 {
            let json_path = crate::persistence::wal_path(&cis_dir);
            if json_path.is_file() {
                import_json_file(&conn, &json_path, &sql_writes)?;
            }
        }
        let (next_id, records) = load_records(&conn)?;
        let log = if records.is_empty() {
            MutationLog::new()
        } else {
            MutationLog::restore(next_id, records)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
        };
        Ok(Self {
            cis_dir,
            log,
            conn: Mutex::new(conn),
            sql_writes,
        })
    }

    pub fn sql_write_count(&self) -> u64 {
        self.sql_writes.load(Ordering::Relaxed)
    }

    pub fn durable_row_count(&self) -> usize {
        let conn = self.conn.lock().expect("sqlite wal lock");
        count_rows(&conn).unwrap_or(0)
    }

    /// `PRAGMA wal_checkpoint(PASSIVE)` on `.cis/wal.db` without loading records.
    pub fn checkpoint_file(cis_dir: impl AsRef<Path>) -> io::Result<()> {
        let path = wal_db_path(cis_dir);
        if !path.is_file() {
            return Ok(());
        }
        let conn = Connection::open(&path).map_err(|e| io::Error::other(e.to_string()))?;
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .map_err(|e| io::Error::other(e.to_string()))
    }

    fn persist_record(&self, rec: &MutationRecord) -> Result<(), MutationLogError> {
        let conn = self.conn.lock().expect("sqlite wal lock");
        upsert_record(&conn, rec).map_err(|e| MutationLogError::Persist(e.to_string()))?;
        conn.execute(
            "INSERT INTO wal_meta (k, v) VALUES ('next_id', ?1)
             ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![self.log.next_allocate_id() as i64],
        )
        .map_err(|e| MutationLogError::Persist(e.to_string()))?;
        self.sql_writes.fetch_add(2, Ordering::Relaxed);
        Ok(())
    }

    fn persist_phase(&self, rec: &MutationRecord) -> Result<(), MutationLogError> {
        let conn = self.conn.lock().expect("sqlite wal lock");
        upsert_record(&conn, rec).map_err(|e| MutationLogError::Persist(e.to_string()))?;
        self.sql_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn maybe_export_json(&self) {
        if !wal_json_export_enabled() {
            return;
        }
        let path = crate::persistence::wal_path(&self.cis_dir);
        let Ok(d) = DurableMutationLog::create_new(&path) else {
            return;
        };
        for rec in self.log.iter_all() {
            let _ = d.append(rec);
        }
        let _ = d.flush_now();
    }
}

fn count_rows(conn: &Connection) -> io::Result<usize> {
    conn.query_row("SELECT COUNT(*) FROM wal", [], |r| r.get::<_, i64>(0))
        .map(|n| n as usize)
        .map_err(|e| io::Error::other(e.to_string()))
}

fn upsert_record(conn: &Connection, rec: &MutationRecord) -> rusqlite::Result<()> {
    let payload = serde_json::to_vec(rec).map_err(|e| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(io::Error::other(e.to_string())))
    })?;
    conn.execute(
        "INSERT INTO wal (log_id, phase, payload) VALUES (?1, ?2, ?3)
         ON CONFLICT(log_id) DO UPDATE SET phase = excluded.phase, payload = excluded.payload",
        params![rec.log_id as i64, phase_to_i64(rec.phase), payload],
    )?;
    Ok(())
}

fn import_json_file(
    conn: &Connection,
    path: &Path,
    sql_writes: &AtomicU64,
) -> io::Result<()> {
    let imported = DurableMutationLog::open(path)?;
    let records = imported.iter_all();
    if records.is_empty() {
        return Ok(());
    }
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| io::Error::other(e.to_string()))?;
    for rec in &records {
        upsert_record(&tx, rec).map_err(|e| io::Error::other(e.to_string()))?;
    }
    tx.execute(
        "INSERT INTO wal_meta (k, v) VALUES ('next_id', ?1)
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        params![imported.next_allocate_id() as i64],
    )
    .map_err(|e| io::Error::other(e.to_string()))?;
    tx.commit().map_err(|e| io::Error::other(e.to_string()))?;
    sql_writes.fetch_add(records.len() as u64 + 1, Ordering::Relaxed);
    Ok(())
}

fn load_records(conn: &Connection) -> io::Result<(LogId, Vec<MutationRecord>)> {
    let next_id: i64 = conn
        .query_row("SELECT v FROM wal_meta WHERE k = 'next_id'", [], |r| r.get(0))
        .optional()
        .map_err(|e| io::Error::other(e.to_string()))?
        .unwrap_or(1);
    let mut stmt = conn
        .prepare("SELECT payload FROM wal ORDER BY log_id")
        .map_err(|e| io::Error::other(e.to_string()))?;
    let iter = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(|e| io::Error::other(e.to_string()))?;
    let mut records = Vec::new();
    let mut max_id: LogId = 0;
    for r in iter {
        let bytes = r.map_err(|e| io::Error::other(e.to_string()))?;
        let rec: MutationRecord = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        max_id = max_id.max(rec.log_id);
        records.push(rec);
    }
    let next = (next_id as LogId).max(max_id.saturating_add(1)).max(1);
    Ok((next, records))
}

impl MutationLogStore for SqliteMutationLog {
    fn append(&self, record: MutationRecord) -> Result<LogId, MutationLogError> {
        let id = self.log.append(record)?;
        if let Some(rec) = self.log.get(id) {
            self.persist_record(&rec)?;
        }
        Ok(id)
    }

    fn get(&self, id: LogId) -> Option<MutationRecord> {
        self.log.get(id)
    }

    fn update_phase(&self, id: LogId, phase: MutationPhase) -> Result<(), MutationLogError> {
        self.log.update_phase(id, phase)?;
        if let Some(rec) = self.log.get(id) {
            self.persist_phase(&rec)?;
        }
        Ok(())
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
        let before: std::collections::HashSet<LogId> =
            self.log.iter_all().into_iter().map(|r| r.log_id).collect();
        let rep = self.log.truncate_committed(wal_max_bytes);
        let after: std::collections::HashSet<LogId> =
            self.log.iter_all().into_iter().map(|r| r.log_id).collect();
        let conn = self.conn.lock().expect("sqlite wal lock");
        for id in before.difference(&after) {
            conn.execute("DELETE FROM wal WHERE log_id = ?1", params![*id as i64])
                .map_err(|e| MutationLogError::Persist(e.to_string()))?;
            self.sql_writes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(rep)
    }

    fn flush_persistent(&self) -> Result<(), MutationLogError> {
        let conn = self.conn.lock().expect("sqlite wal lock");
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .map_err(|e| MutationLogError::Persist(e.to_string()))?;
        drop(conn);
        self.maybe_export_json();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cis_wal::{MutationKind, NodeRevisionId};

    fn rid(n: u8) -> NodeRevisionId {
        let mut b = [0u8; 16];
        b[15] = n;
        NodeRevisionId(b)
    }

    fn mk(phase: MutationPhase) -> MutationRecord {
        MutationRecord {
            log_id: 0,
            kind: MutationKind::Single,
            phase,
            affected_revisions: vec![rid(1)],
            payload_checksum: [1u8; 32],
            created_at_ms: 42,
        }
    }

    fn temp_cis() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cis-wal-sql-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn sqlite_wal_survives_reopen() {
        let cis = temp_cis();
        let id;
        {
            let wal = SqliteMutationLog::open(&cis).unwrap();
            id = wal.append(mk(MutationPhase::Pending)).unwrap();
            wal.update_phase(id, MutationPhase::GraphDone).unwrap();
            wal.update_phase(id, MutationPhase::Committed).unwrap();
            assert!(wal.sql_write_count() >= 3, "each mutation is a row write");
        }
        let wal2 = SqliteMutationLog::open(&cis).unwrap();
        let rec = wal2.get(id).expect("record");
        assert_eq!(rec.phase, MutationPhase::Committed);
        assert!(
            !crate::persistence::wal_path(&cis).exists(),
            "sqlite WAL must not write wal.json by default"
        );
    }

    #[test]
    fn sqlite_wal_corrupt_json_import_fails_closed() {
        let cis = temp_cis();
        std::fs::write(crate::persistence::wal_path(&cis), b"CORRUPT").unwrap();
        let err = SqliteMutationLog::open(&cis).unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "corrupt wal.json must not become an empty sqlite WAL"
        );
    }

    #[test]
    fn sqlite_wal_phase_is_not_full_rewrite() {
        let cis = temp_cis();
        let wal = SqliteMutationLog::open(&cis).unwrap();
        let mut ids = Vec::new();
        for _ in 0..8 {
            ids.push(wal.append(mk(MutationPhase::Pending)).unwrap());
        }
        let after_append = wal.sql_write_count();
        wal.update_phase(ids[3], MutationPhase::GraphDone).unwrap();
        assert_eq!(
            wal.sql_write_count(),
            after_append + 1,
            "phase update must be one SQL statement, not a snapshot rewrite"
        );
    }
}
