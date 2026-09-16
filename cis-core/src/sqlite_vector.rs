//! SQLite-backed vector persistence (`CIS_VECTOR_BACKEND=sqlite`) in `.cis/vectors.db`.
//!
//! Chunks and embeddings are upserted incrementally. [`InMemoryVectorStore`] stays
//! the query/ANN working set; `vector.json` is not rewritten unless JSON export is on.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};

use crate::vector_store::{
    InMemoryVectorStore, VectorBodyRecord, VectorChunkRecord, VectorPersistHook,
    VectorStoreSnapshot, VECTOR_STORE_SNAPSHOT_VERSION,
};

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS vectors (
        body_hash BLOB PRIMARY KEY,
        model_id TEXT NOT NULL,
        dim INTEGER NOT NULL,
        embedding BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS chunks (
        chunk_id BLOB PRIMARY KEY,
        body_hash BLOB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_chunks_body ON chunks(body_hash);
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorBackendKind {
    Json,
    Sqlite,
}

/// Read `CIS_VECTOR_BACKEND=json|sqlite` (default `json`).
pub fn vector_backend_from_env() -> VectorBackendKind {
    match std::env::var_os("CIS_VECTOR_BACKEND") {
        Some(v) if v == "sqlite" || v == "sqlite3" => VectorBackendKind::Sqlite,
        _ => VectorBackendKind::Json,
    }
}

/// When `CIS_VECTOR_BACKEND=sqlite`, also write `vector.json` if `CIS_VECTOR_JSON_EXPORT=1`.
pub fn vector_json_export_enabled() -> bool {
    std::env::var_os("CIS_VECTOR_JSON_EXPORT").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    })
}

pub fn vectors_db_path(cis_dir: impl AsRef<Path>) -> PathBuf {
    cis_dir.as_ref().join("vectors.db")
}

fn configure_conn(conn: &Connection) -> io::Result<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(|e| io::Error::other(e.to_string()))?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| io::Error::other(e.to_string()))?;
    conn.execute_batch(SCHEMA)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(())
}

fn f32s_to_le(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn le_to_f32s(bytes: &[u8]) -> io::Result<Vec<f32>> {
    if bytes.len() % 4 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "embedding blob length is not a multiple of 4",
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4);
    for chunk in bytes.chunks_exact(4) {
        out.push(f32::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(out)
}

fn blob32(v: &[u8]) -> io::Result<[u8; 32]> {
    v.try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "expected 32-byte blob"))
}

/// Write-through SQLite sidecar for [`InMemoryVectorStore`].
#[derive(Clone)]
pub struct SqliteVectorStore {
    conn: Arc<Mutex<Connection>>,
    sql_writes: Arc<AtomicU64>,
}

impl std::fmt::Debug for SqliteVectorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteVectorStore").finish_non_exhaustive()
    }
}

impl SqliteVectorStore {
    pub fn open(cis_dir: impl AsRef<Path>) -> io::Result<Self> {
        let cis = cis_dir.as_ref();
        std::fs::create_dir_all(cis)?;
        let conn = Connection::open(vectors_db_path(cis))
            .map_err(|e| io::Error::other(e.to_string()))?;
        configure_conn(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            sql_writes: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn sql_write_count(&self) -> u64 {
        self.sql_writes.load(Ordering::Relaxed)
    }

    pub fn chunk_count(&self) -> usize {
        let conn = self.conn.lock().expect("sqlite vector lock");
        conn.query_row("SELECT COUNT(*) FROM chunks", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
            .unwrap_or(0)
    }

    pub fn vector_count(&self) -> usize {
        let conn = self.conn.lock().expect("sqlite vector lock");
        conn.query_row("SELECT COUNT(*) FROM vectors", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.chunk_count() == 0 && self.vector_count() == 0
    }

    pub fn import_snapshot(&self, snap: &VectorStoreSnapshot) -> io::Result<()> {
        let conn = self.conn.lock().expect("sqlite vector lock");
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| io::Error::other(e.to_string()))?;
        for rec in &snap.chunks {
            upsert_chunk(&tx, &rec.chunk_id, &rec.body_hash)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        for rec in &snap.vectors {
            upsert_vector(&tx, &rec.body_hash, &rec.embedding, &rec.model_id)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        tx.commit().map_err(|e| io::Error::other(e.to_string()))?;
        self.sql_writes
            .fetch_add((snap.chunks.len() + snap.vectors.len()) as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn load_into(&self, store: &InMemoryVectorStore) -> io::Result<usize> {
        let snap = self.export_snapshot()?;
        let n = snap.chunks.len();
        store.restore_snapshot(&snap);
        Ok(n)
    }

    pub fn export_snapshot(&self) -> io::Result<VectorStoreSnapshot> {
        let conn = self.conn.lock().expect("sqlite vector lock");
        let mut chunks = Vec::new();
        {
            let mut stmt = conn
                .prepare("SELECT chunk_id, body_hash FROM chunks")
                .map_err(|e| io::Error::other(e.to_string()))?;
            let iter = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|e| io::Error::other(e.to_string()))?;
            for r in iter {
                let (cid, bh) = r.map_err(|e| io::Error::other(e.to_string()))?;
                chunks.push(VectorChunkRecord {
                    chunk_id: blob32(&cid)?,
                    body_hash: blob32(&bh)?,
                });
            }
        }
        let mut vectors = Vec::new();
        {
            let mut stmt = conn
                .prepare("SELECT body_hash, model_id, embedding FROM vectors")
                .map_err(|e| io::Error::other(e.to_string()))?;
            let iter = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(|e| io::Error::other(e.to_string()))?;
            for r in iter {
                let (bh, model_id, raw) = r.map_err(|e| io::Error::other(e.to_string()))?;
                vectors.push(VectorBodyRecord {
                    body_hash: blob32(&bh)?,
                    embedding: le_to_f32s(&raw)?,
                    model_id,
                });
            }
        }
        Ok(VectorStoreSnapshot {
            version: VECTOR_STORE_SNAPSHOT_VERSION,
            chunks,
            vectors,
        })
    }

    pub fn checkpoint(&self) -> io::Result<()> {
        let conn = self.conn.lock().expect("sqlite vector lock");
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

fn upsert_chunk(conn: &Connection, chunk_id: &[u8; 32], body_hash: &[u8; 32]) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO chunks (chunk_id, body_hash) VALUES (?1, ?2)
         ON CONFLICT(chunk_id) DO UPDATE SET body_hash = excluded.body_hash",
        params![chunk_id.as_slice(), body_hash.as_slice()],
    )?;
    Ok(())
}

fn upsert_vector(
    conn: &Connection,
    body_hash: &[u8; 32],
    embedding: &[f32],
    model_id: &str,
) -> rusqlite::Result<()> {
    let blob = f32s_to_le(embedding);
    conn.execute(
        "INSERT INTO vectors (body_hash, model_id, dim, embedding) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(body_hash) DO UPDATE SET
            model_id = excluded.model_id,
            dim = excluded.dim,
            embedding = excluded.embedding",
        params![
            body_hash.as_slice(),
            model_id,
            embedding.len() as i64,
            blob
        ],
    )?;
    Ok(())
}

impl VectorPersistHook for SqliteVectorStore {
    fn checkpoint(&self) {
        if let Err(e) = SqliteVectorStore::checkpoint(self) {
            eprintln!("cis: sqlite vector checkpoint failed: {e}");
        }
    }

    fn on_register(&self, chunk_id: [u8; 32], body_hash: [u8; 32]) {
        let conn = self.conn.lock().expect("sqlite vector lock");
        if let Err(e) = upsert_chunk(&conn, &chunk_id, &body_hash) {
            eprintln!("cis: sqlite vector register failed: {e}");
            return;
        }
        self.sql_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn on_set_embedding(&self, body_hash: [u8; 32], embedding: &[f32], model_id: &str) {
        let conn = self.conn.lock().expect("sqlite vector lock");
        if let Err(e) = upsert_vector(&conn, &body_hash, embedding, model_id) {
            eprintln!("cis: sqlite vector embed failed: {e}");
            return;
        }
        self.sql_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn on_delete_chunks(&self, ids: &[[u8; 32]]) {
        let conn = self.conn.lock().expect("sqlite vector lock");
        for cid in ids {
            let body: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT body_hash FROM chunks WHERE chunk_id = ?1",
                    params![cid.as_slice()],
                    |r| r.get(0),
                )
                .optional()
                .unwrap_or(None);
            if conn
                .execute("DELETE FROM chunks WHERE chunk_id = ?1", params![cid.as_slice()])
                .is_err()
            {
                continue;
            }
            self.sql_writes.fetch_add(1, Ordering::Relaxed);
            if let Some(bh) = body {
                let n: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM chunks WHERE body_hash = ?1",
                        params![bh.as_slice()],
                        |r| r.get(0),
                    )
                    .unwrap_or(1);
                if n == 0 {
                    let _ = conn.execute(
                        "DELETE FROM vectors WHERE body_hash = ?1",
                        params![bh.as_slice()],
                    );
                    self.sql_writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_cis() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cis-vec-sql-{}-{}",
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
    fn sqlite_vectors_survive_reopen() {
        let cis = temp_cis();
        let chunk = [1u8; 32];
        let body = [2u8; 32];
        {
            let sql = SqliteVectorStore::open(&cis).unwrap();
            let ram = InMemoryVectorStore::new();
            ram.set_persist(Arc::new(sql.clone()));
            ram.register(chunk, body);
            ram.set_embedding(body, vec![0.25, 0.75], "m");
            assert_eq!(sql.chunk_count(), 1);
            assert_eq!(sql.vector_count(), 1);
        }
        let sql = SqliteVectorStore::open(&cis).unwrap();
        let ram = InMemoryVectorStore::new();
        sql.load_into(&ram).unwrap();
        assert!(ram.has_chunk(&chunk));
        assert_eq!(ram.vector_for_body(&body).unwrap().vec, vec![0.25, 0.75]);
        assert!(!crate::persistence::vector_snapshot_path(&cis).exists());
    }

    #[test]
    fn sqlite_vector_embed_is_incremental() {
        let cis = temp_cis();
        let sql = SqliteVectorStore::open(&cis).unwrap();
        let ram = InMemoryVectorStore::new();
        ram.set_persist(Arc::new(sql.clone()));
        for i in 0..8u8 {
            let mut chunk = [0u8; 32];
            let mut body = [0u8; 32];
            chunk[0] = i;
            body[0] = i + 10;
            ram.register(chunk, body);
            ram.set_embedding(body, vec![i as f32], "m");
        }
        let writes = sql.sql_write_count();
        assert!(
            writes >= 16 && writes < 100,
            "8 register + 8 embed should be O(n) row writes, got {writes}"
        );
    }
}
