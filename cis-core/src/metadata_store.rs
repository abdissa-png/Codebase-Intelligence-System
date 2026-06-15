//! SQLite metadata store for RIS snapshots (**Phase 8C**).
//!
//! See `docs/adr/0003-metadata-storage.md`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cis_wal::BranchId;

/// Metadata backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataBackendKind {
    Json,
    Sqlite,
}

pub fn metadata_backend_from_env() -> MetadataBackendKind {
    match std::env::var_os("CIS_METADATA_BACKEND") {
        Some(v) if v == "sqlite" || v == "sqlite3" => MetadataBackendKind::Sqlite,
        _ => MetadataBackendKind::Json,
    }
}

pub fn store_db_path(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("store.db")
}

/// `.cis/store.db` — RIS snapshots + optional durable KV offload.
#[cfg(feature = "body-sqlite")]
#[derive(Debug)]
pub struct MetadataStore {
    conn: std::sync::Mutex<rusqlite::Connection>,
}

#[cfg(feature = "body-sqlite")]
impl MetadataStore {
    pub fn open(cis_dir: impl AsRef<Path>) -> io::Result<Self> {
        let cis = cis_dir.as_ref();
        fs::create_dir_all(cis)?;
        let db_path = store_db_path(cis);
        let conn = rusqlite::Connection::open(db_path).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, e.to_string())
        })?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ris_snapshots (
                branch BLOB NOT NULL,
                epoch INTEGER NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (branch, epoch)
            );
            CREATE TABLE IF NOT EXISTS kv_durable (
                key BLOB PRIMARY KEY,
                value BLOB NOT NULL
            );",
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(Self {
            conn: std::sync::Mutex::new(conn),
        })
    }

    pub fn put_ris_snapshot(
        &self,
        branch: BranchId,
        epoch: u64,
        payload: &[u8],
    ) -> io::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO ris_snapshots (branch, epoch, payload) VALUES (?1, ?2, ?3)
             ON CONFLICT(branch, epoch) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![branch.0.as_slice(), epoch as i64, payload],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    pub fn get_ris_snapshot(
        &self,
        branch: BranchId,
        epoch: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT payload FROM ris_snapshots WHERE branch = ?1 AND epoch = ?2")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut rows = stmt
            .query(rusqlite::params![branch.0.as_slice(), epoch as i64])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if let Some(row) = rows
            .next()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
        {
            let bytes: Vec<u8> = row
                .get(0)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    pub fn list_ris_epochs(&self, branch: BranchId) -> io::Result<Vec<u64>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT epoch FROM ris_snapshots WHERE branch = ?1 ORDER BY epoch")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let rows = stmt
            .query_map(rusqlite::params![branch.0.as_slice()], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))? as u64);
        }
        Ok(out)
    }

    pub fn list_ris_snapshots(&self) -> io::Result<Vec<([u8; 16], u64, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT branch, epoch, payload FROM ris_snapshots")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let branch: Vec<u8> = row.get(0)?;
                let epoch: i64 = row.get(1)?;
                let payload: Vec<u8> = row.get(2)?;
                Ok((branch, epoch, payload))
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            let (branch, epoch, payload) =
                r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            if branch.len() != 16 {
                continue;
            }
            let mut b = [0u8; 16];
            b.copy_from_slice(&branch);
            out.push((b, epoch as u64, payload));
        }
        Ok(out)
    }
}

#[cfg(feature = "body-sqlite")]
pub fn open_metadata_store_if_enabled(cis_dir: &Path) -> io::Result<Option<MetadataStore>> {
    if metadata_backend_from_env() != MetadataBackendKind::Sqlite {
        return Ok(None);
    }
    Ok(Some(MetadataStore::open(cis_dir)?))
}

#[cfg(not(feature = "body-sqlite"))]
pub fn open_metadata_store_if_enabled(_cis_dir: &Path) -> io::Result<Option<()>> {
    Ok(None)
}
