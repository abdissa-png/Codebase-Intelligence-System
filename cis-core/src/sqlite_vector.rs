//! SQLite-backed vector persistence (`CIS_VECTOR_BACKEND=sqlite`) in `.cis/vectors.db`
//! or unified `.cis/cis.db`.
//!
//! Chunks and embeddings are upserted incrementally. [`InMemoryVectorStore`] stays
//! the query working set when loaded; ANN search can run on the durable `vec0` index
//! (sqlite-vec) so `CIS_DEFER_VECTOR_LOAD` still has nearest-neighbor hits.
//! `vector.json` is not rewritten unless JSON export is on.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};

use rusqlite::{params, Connection, OptionalExtension};

use crate::embedder::cosine_similarity;
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
    CREATE INDEX IF NOT EXISTS idx_vectors_model_dim ON vectors(model_id, dim);
    CREATE TABLE IF NOT EXISTS vec_rowids (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        body_hash BLOB NOT NULL UNIQUE
    );
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
    crate::sqlite_paths::sqlite_file(cis_dir.as_ref(), "vectors.db")
}

fn register_sqlite_vec() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| unsafe {
        rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute(
            sqlite_vec::sqlite3_vec_init as *const (),
        )));
    });
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

fn vec0_sql(dim: usize) -> String {
    format!("CREATE VIRTUAL TABLE IF NOT EXISTS vec_ann USING vec0(embedding float[{dim}])")
}

fn ensure_vec0(conn: &Connection, dim: usize) -> rusqlite::Result<bool> {
    if dim == 0 {
        return Ok(false);
    }
    let existing: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'vec_ann'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let want = format!("float[{dim}]");
    if let Some(sql) = &existing {
        if sql.contains(&want) {
            return Ok(true);
        }
        conn.execute_batch("DROP TABLE IF EXISTS vec_ann; DELETE FROM vec_rowids;")?;
    }
    conn.execute_batch(&vec0_sql(dim))?;
    Ok(true)
}

fn majority_vector_dim(conn: &Connection) -> Option<usize> {
    conn.query_row(
        "SELECT dim FROM vectors GROUP BY dim ORDER BY COUNT(*) DESC LIMIT 1",
        [],
        |r| r.get::<_, i64>(0),
    )
    .ok()
    .and_then(|d| (d > 0).then_some(d as usize))
}

fn backfill_vec_ann(conn: &Connection) -> rusqlite::Result<Option<usize>> {
    let Some(dim) = majority_vector_dim(conn) else {
        return Ok(None);
    };
    if !ensure_vec0(conn, dim)? {
        return Ok(None);
    }
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM vec_ann", [], |r| r.get(0))
        .unwrap_or(0);
    if n > 0 {
        return Ok(Some(dim));
    }
    let mut stmt = conn.prepare("SELECT body_hash, embedding FROM vectors WHERE dim = ?1")?;
    let rows = stmt.query_map(params![dim as i64], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    for row in rows {
        let (hash, blob) = row?;
        upsert_ann_row(conn, &hash, &blob)?;
    }
    Ok(Some(dim))
}

fn upsert_ann_row(conn: &Connection, body_hash: &[u8], embedding: &[u8]) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO vec_rowids (body_hash) VALUES (?1)
         ON CONFLICT(body_hash) DO NOTHING",
        params![body_hash],
    )?;
    let id: i64 = conn.query_row(
        "SELECT id FROM vec_rowids WHERE body_hash = ?1",
        params![body_hash],
        |r| r.get(0),
    )?;
    let _ = conn.execute("DELETE FROM vec_ann WHERE rowid = ?1", params![id]);
    conn.execute(
        "INSERT INTO vec_ann(rowid, embedding) VALUES (?1, ?2)",
        params![id, embedding],
    )?;
    Ok(())
}

fn delete_ann_row(conn: &Connection, body_hash: &[u8]) {
    let id: Option<i64> = conn
        .query_row(
            "SELECT id FROM vec_rowids WHERE body_hash = ?1",
            params![body_hash],
            |r| r.get(0),
        )
        .optional()
        .unwrap_or(None);
    if let Some(id) = id {
        let _ = conn.execute("DELETE FROM vec_ann WHERE rowid = ?1", params![id]);
        let _ = conn.execute("DELETE FROM vec_rowids WHERE id = ?1", params![id]);
    }
}

fn search_vec0(
    conn: &Connection,
    query: &[f32],
    k: usize,
) -> rusqlite::Result<Vec<([u8; 32], f64)>> {
    let blob = f32s_to_le(query);
    let mut stmt = conn.prepare(
        "SELECT r.body_hash, a.distance
         FROM vec_ann a
         JOIN vec_rowids r ON r.id = a.rowid
         WHERE a.embedding MATCH ?1 AND k = ?2",
    )?;
    let iter = stmt.query_map(params![blob, k as i64], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, f64>(1)?))
    })?;
    let mut out = Vec::new();
    for row in iter {
        let (h, dist) = row?;
        let Some(hash) = blob32_opt(&h) else {
            continue;
        };
        // sqlite-vec reports L2 distance; invert so callers get a higher-is-better score
        // compatible with FlatAnnIndex cosine.
        let score = if dist.is_finite() {
            1.0 / (1.0 + dist)
        } else {
            0.0
        };
        out.push((hash, score));
    }
    Ok(out)
}

fn search_brute(
    conn: &Connection,
    query: &[f32],
    k: usize,
) -> rusqlite::Result<Vec<([u8; 32], f64)>> {
    let dim = query.len() as i64;
    let mut stmt =
        conn.prepare("SELECT body_hash, embedding FROM vectors WHERE dim = ?1")?;
    let iter = stmt.query_map(params![dim], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut scored = Vec::new();
    for row in iter {
        let (h, raw) = row?;
        let Some(hash) = blob32_opt(&h) else {
            continue;
        };
        let Ok(vec) = le_to_f32s(&raw) else {
            continue;
        };
        scored.push((hash, cosine_similarity(query, &vec)));
    }
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);
    Ok(scored)
}

fn blob32_opt(v: &[u8]) -> Option<[u8; 32]> {
    blob32(v).ok()
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
    /// Dimension of the `vec0` table, if created.
    ann_dim: Arc<Mutex<Option<usize>>>,
}

impl std::fmt::Debug for SqliteVectorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteVectorStore").finish_non_exhaustive()
    }
}

impl SqliteVectorStore {
    pub fn open(cis_dir: impl AsRef<Path>) -> io::Result<Self> {
        register_sqlite_vec();
        let cis = cis_dir.as_ref();
        std::fs::create_dir_all(cis)?;
        let conn = Connection::open(vectors_db_path(cis))
            .map_err(|e| io::Error::other(e.to_string()))?;
        configure_conn(&conn)?;
        let ann_dim = backfill_vec_ann(&conn).unwrap_or(None);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            sql_writes: Arc::new(AtomicU64::new(0)),
            ann_dim: Arc::new(Mutex::new(ann_dim)),
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
        let dim = backfill_vec_ann(&conn).unwrap_or(None);
        drop(conn);
        if let Some(dim) = dim {
            *self.ann_dim.lock().unwrap() = Some(dim);
        }
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

    /// Nearest neighbors from sqlite-vec (`vec0`), falling back to a dim-filtered scan.
    pub fn search_topk(&self, query: &[f32], k: usize) -> Vec<([u8; 32], f64)> {
        if query.is_empty() || k == 0 {
            return Vec::new();
        }
        let conn = self.conn.lock().expect("sqlite vector lock");
        let dim = *self.ann_dim.lock().unwrap();
        if dim == Some(query.len()) {
            if let Ok(hits) = search_vec0(&conn, query, k) {
                if !hits.is_empty() {
                    return hits;
                }
            }
        }
        search_brute(&conn, query, k).unwrap_or_default()
    }

    fn sync_ann(&self, conn: &Connection, body_hash: &[u8; 32], embedding: &[f32]) {
        let dim = embedding.len();
        let mut guard = self.ann_dim.lock().unwrap();
        if guard.is_none() {
            if ensure_vec0(conn, dim).unwrap_or(false) {
                *guard = Some(dim);
            }
        }
        if *guard == Some(dim) {
            let blob = f32s_to_le(embedding);
            let _ = upsert_ann_row(conn, body_hash.as_slice(), &blob);
        }
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
        self.sync_ann(&conn, &body_hash, embedding);
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
                    delete_ann_row(&conn, &bh);
                    self.sql_writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn search_topk(&self, query: &[f32], k: usize) -> Option<Vec<([u8; 32], f64)>> {
        Some(SqliteVectorStore::search_topk(self, query, k))
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

    #[test]
    fn sqlite_vec_extension_reports_version() {
        register_sqlite_vec();
        let conn = Connection::open_in_memory().unwrap();
        let v: String = conn.query_row("SELECT vec_version()", [], |r| r.get(0)).unwrap();
        assert!(v.starts_with('v'), "vec_version={v}");
    }

    #[test]
    fn sqlite_vec_search_ranks_nearest_without_ram_load() {
        let cis = temp_cis();
        let near = [3u8; 32];
        let far = [4u8; 32];
        {
            let sql = SqliteVectorStore::open(&cis).unwrap();
            let ram = InMemoryVectorStore::new();
            ram.set_persist(Arc::new(sql.clone()));
            ram.register([1u8; 32], near);
            ram.register([2u8; 32], far);
            ram.set_embedding(near, vec![1.0, 0.0], "m");
            ram.set_embedding(far, vec![0.0, 1.0], "m");
            let hits = sql.search_topk(&[0.9, 0.1], 2);
            assert!(!hits.is_empty(), "expected ANN or brute hits");
            assert_eq!(hits[0].0, near);
        }
        let sql = SqliteVectorStore::open(&cis).unwrap();
        let hits = sql.search_topk(&[0.9, 0.1], 1);
        assert_eq!(hits[0].0, near, "reopen must search durable index without load_into");
    }
}
