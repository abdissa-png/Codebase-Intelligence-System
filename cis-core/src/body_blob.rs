//! Content-addressed body blobs under `.cis/bodies/` or `.cis/bodies.db` (**Phase 7–8**).
//!
//! See `docs/adr/0002-body-storage.md`.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cis_wal::BranchId;

use crate::body_store::BodyStore;
use crate::graph::{InMemoryGraph, RevisionStatus};
use crate::ingest::file_body_hash_key;

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn parse_hex32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut h = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        if i >= 32 {
            break;
        }
        h[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(h)
}

/// Shard path: `.cis/bodies/{aa}/{bb}{rest32}`.
pub fn body_blob_path(cis: impl AsRef<Path>, body_hash: &[u8; 32]) -> PathBuf {
    let h = hex32(body_hash);
    cis.as_ref().join("bodies").join(&h[..2]).join(&h[2..])
}

pub fn bodies_dir(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("bodies")
}

pub fn bodies_db_path(cis: impl AsRef<Path>) -> PathBuf {
    cis.as_ref().join("bodies.db")
}

/// Body blob backend selection (**Phase 8A**).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyBackendKind {
    File,
    Sqlite,
}

/// Read `CIS_BODY_BACKEND=file|sqlite` (default `file`).
pub fn body_backend_from_env() -> BodyBackendKind {
    match std::env::var_os("CIS_BODY_BACKEND") {
        Some(v) if v == "sqlite" || v == "sqlite3" => BodyBackendKind::Sqlite,
        _ => BodyBackendKind::File,
    }
}

/// Collect `body_hash` values referenced by live graph revisions on `branch`.
pub fn referenced_body_hashes(
    graph: &InMemoryGraph,
    branch: BranchId,
    include_speculative: bool,
) -> HashSet<[u8; 32]> {
    let mut out = HashSet::new();
    for r in graph.revisions() {
        if r.branch_id != branch {
            continue;
        }
        let live = matches!(r.status, RevisionStatus::Active)
            || (include_speculative && matches!(r.status, RevisionStatus::Speculative));
        if live {
            out.insert(r.body_hash);
            if r.qualified_name == r.file_path {
                out.insert(file_body_hash_key(&r.file_path));
            }
        }
    }
    out
}

/// Port for body blob persistence (files default; SQLite optional).
pub trait BodyBlobStore: Send + Sync {
    fn get(&self, body_hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>>;
    fn put(&self, body_hash: &[u8; 32], content: &[u8]) -> io::Result<()>;
    fn delete(&self, body_hash: &[u8; 32]) -> io::Result<()>;
    /// Remove blobs whose hash is not in `keep`.
    fn gc_except(&self, keep: &HashSet<[u8; 32]>) -> io::Result<usize>;
    /// Enumerate stored hashes (for migration verify).
    fn list_hashes(&self) -> io::Result<Vec<[u8; 32]>>;
}

/// Open the configured body blob store for `cis_dir`.
pub fn open_body_blob_store(cis_dir: impl AsRef<Path>) -> Arc<dyn BodyBlobStore> {
    let cis = cis_dir.as_ref();
    match body_backend_from_env() {
        BodyBackendKind::File => Arc::new(FileBodyBlobStore::new(cis)),
        BodyBackendKind::Sqlite => open_sqlite_or_fallback(cis),
    }
}

fn open_sqlite_or_fallback(cis: &Path) -> Arc<dyn BodyBlobStore> {
    #[cfg(feature = "body-sqlite")]
    {
        match SqliteBodyBlobStore::open(cis) {
            Ok(s) => return Arc::new(s),
            Err(e) => eprintln!("cis: CIS_BODY_BACKEND=sqlite open failed ({e}); using files"),
        }
    }
    #[cfg(not(feature = "body-sqlite"))]
    {
        eprintln!(
            "cis: CIS_BODY_BACKEND=sqlite requires --features body-sqlite; using files"
        );
    }
    Arc::new(FileBodyBlobStore::new(cis))
}

/// File-backed store under `.cis/bodies/`.
#[derive(Debug, Clone)]
pub struct FileBodyBlobStore {
    cis_dir: PathBuf,
}

impl FileBodyBlobStore {
    pub fn new(cis_dir: impl AsRef<Path>) -> Self {
        Self {
            cis_dir: cis_dir.as_ref().to_path_buf(),
        }
    }
}

impl BodyBlobStore for FileBodyBlobStore {
    fn get(&self, body_hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>> {
        load_body_blob_file(&self.cis_dir, body_hash)
    }

    fn put(&self, body_hash: &[u8; 32], content: &[u8]) -> io::Result<()> {
        save_body_blob_file(&self.cis_dir, body_hash, content)
    }

    fn delete(&self, body_hash: &[u8; 32]) -> io::Result<()> {
        delete_body_blob_file(&self.cis_dir, body_hash)
    }

    fn gc_except(&self, keep: &HashSet<[u8; 32]>) -> io::Result<usize> {
        gc_body_blob_files(&self.cis_dir, keep)
    }

    fn list_hashes(&self) -> io::Result<Vec<[u8; 32]>> {
        list_body_blob_file_hashes(&self.cis_dir)
    }
}

/// SQLite-backed body store (optional; see `body-sqlite` feature).
#[cfg(feature = "body-sqlite")]
#[derive(Debug)]
pub struct SqliteBodyBlobStore {
    conn: std::sync::Mutex<rusqlite::Connection>,
}

#[cfg(feature = "body-sqlite")]
impl SqliteBodyBlobStore {
    pub fn open(cis_dir: impl AsRef<Path>) -> io::Result<Self> {
        let cis = cis_dir.as_ref();
        fs::create_dir_all(cis)?;
        let db_path = bodies_db_path(cis);
        let conn = rusqlite::Connection::open(db_path).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, e.to_string())
        })?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS bodies (
                hash BLOB PRIMARY KEY,
                bytes BLOB NOT NULL,
                size INTEGER NOT NULL
            );",
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(Self {
            conn: std::sync::Mutex::new(conn),
        })
    }
}

#[cfg(feature = "body-sqlite")]
impl BodyBlobStore for SqliteBodyBlobStore {
    fn get(&self, body_hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT bytes FROM bodies WHERE hash = ?1")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut rows = stmt
            .query(rusqlite::params![body_hash.as_slice()])
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

    fn put(&self, body_hash: &[u8; 32], content: &[u8]) -> io::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO bodies (hash, bytes, size) VALUES (?1, ?2, ?3)
             ON CONFLICT(hash) DO UPDATE SET bytes = excluded.bytes, size = excluded.size",
            rusqlite::params![body_hash.as_slice(), content, content.len() as i64],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    fn delete(&self, body_hash: &[u8; 32]) -> io::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM bodies WHERE hash = ?1",
            rusqlite::params![body_hash.as_slice()],
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    fn gc_except(&self, keep: &HashSet<[u8; 32]>) -> io::Result<usize> {
        let all = self.list_hashes()?;
        let mut removed = 0usize;
        for h in all {
            if !keep.contains(&h) {
                self.delete(&h)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn list_hashes(&self) -> io::Result<Vec<[u8; 32]>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT hash FROM bodies")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let b: Vec<u8> = row.get(0)?;
                Ok(b)
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            let b = r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            if b.len() == 32 {
                let mut h = [0u8; 32];
                h.copy_from_slice(&b);
                out.push(h);
            }
        }
        Ok(out)
    }
}

pub fn load_body_blob_file(cis: impl AsRef<Path>, body_hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>> {
    let path = body_blob_path(&cis, body_hash);
    if !path.is_file() {
        return Ok(None);
    }
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(Some(buf))
}

pub fn save_body_blob_file(cis: impl AsRef<Path>, body_hash: &[u8; 32], content: &[u8]) -> io::Result<()> {
    let path = body_blob_path(&cis, body_hash);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(content)?;
        f.flush()?;
        f.sync_all()?;
    }
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn delete_body_blob_file(cis: impl AsRef<Path>, body_hash: &[u8; 32]) -> io::Result<()> {
    let path = body_blob_path(&cis, body_hash);
    if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// Legacy helper — file backend only.
pub fn load_body_blob(cis: impl AsRef<Path>, body_hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>> {
    load_body_blob_file(cis, body_hash)
}

/// Legacy helper — file backend only.
pub fn save_body_blob(cis: impl AsRef<Path>, body_hash: &[u8; 32], content: &[u8]) -> io::Result<()> {
    save_body_blob_file(cis, body_hash, content)
}

/// Load with store first, then file fallback (migration dual-read).
pub fn load_body_blob_with_fallback(
    store: &dyn BodyBlobStore,
    cis_dir: &Path,
    body_hash: &[u8; 32],
) -> io::Result<Option<Vec<u8>>> {
    if let Some(bytes) = store.get(body_hash)? {
        return Ok(Some(bytes));
    }
    load_body_blob_file(cis_dir, body_hash)
}

/// Persist referenced blobs from `BodyStore` through the blob store.
pub fn sync_bodies_to_store(
    store: &dyn BodyBlobStore,
    body_store: &BodyStore,
    referenced: &HashSet<[u8; 32]>,
) -> io::Result<usize> {
    let mut n = 0usize;
    for h in referenced {
        if let Some(bytes) = body_store.get(h) {
            store.put(h, &bytes)?;
            n += 1;
        }
    }
    Ok(n)
}

/// Legacy name — opens file store internally.
pub fn sync_bodies_to_disk(
    cis: impl AsRef<Path>,
    body_store: &BodyStore,
    referenced: &HashSet<[u8; 32]>,
) -> io::Result<usize> {
    let store = open_body_blob_store(cis.as_ref());
    sync_bodies_to_store(store.as_ref(), body_store, referenced)
}

/// Hydrate `BodyStore` from blob store (+ file fallback).
pub fn hydrate_bodies_from_store(
    store: &dyn BodyBlobStore,
    cis_dir: &Path,
    body_store: &BodyStore,
    referenced: &HashSet<[u8; 32]>,
) -> io::Result<usize> {
    let mut n = 0usize;
    for h in referenced {
        if body_store.has(h) {
            continue;
        }
        if let Some(bytes) = load_body_blob_with_fallback(store, cis_dir, h)? {
            body_store.put(*h, bytes);
            n += 1;
        }
    }
    Ok(n)
}

/// Legacy name.
pub fn hydrate_bodies_from_disk(
    cis: impl AsRef<Path>,
    body_store: &BodyStore,
    referenced: &HashSet<[u8; 32]>,
) -> io::Result<usize> {
    let cis = cis.as_ref();
    let store = open_body_blob_store(cis);
    hydrate_bodies_from_store(store.as_ref(), cis, body_store, referenced)
}

pub fn gc_body_blob_files(cis: impl AsRef<Path>, keep: &HashSet<[u8; 32]>) -> io::Result<usize> {
    let root = bodies_dir(&cis);
    if !root.is_dir() {
        return Ok(0);
    }
    let mut removed = 0usize;
    let keep_hex: HashSet<String> = keep.iter().map(hex32).collect();
    for entry in walk_body_files(&root)? {
        let path = entry?;
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.ends_with(".tmp") {
            let _ = fs::remove_file(&path);
            continue;
        }
        let shard = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if shard.len() != 2 {
            continue;
        }
        let full_hex = format!("{shard}{name}");
        if full_hex.len() == 64 && !keep_hex.contains(&full_hex) {
            fs::remove_file(&path)?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Legacy alias.
pub fn gc_body_blobs(cis: impl AsRef<Path>, keep: &HashSet<[u8; 32]>) -> io::Result<usize> {
    gc_body_blob_files(cis, keep)
}

pub fn gc_bodies_with_store(
    store: &dyn BodyBlobStore,
    cis_dir: &Path,
    body_store: &BodyStore,
    kv: &crate::kv::MemoryKv,
    keep: &HashSet<[u8; 32]>,
) -> io::Result<usize> {
    let mut removed = store.gc_except(keep)?;
    if body_backend_from_env() == BodyBackendKind::File {
        removed += gc_body_blob_files(cis_dir, keep)?;
    }
    for (k, _) in kv.scan_prefix("body:") {
        let Some(hex) = k.strip_prefix("body:") else {
            continue;
        };
        let Some(h) = parse_hex32(hex) else {
            continue;
        };
        if !keep.contains(&h) {
            body_store.delete(&h);
            removed += 1;
        }
    }
    Ok(removed)
}

/// GC in-memory `body:` keys and blob store.
pub fn gc_bodies(
    cis: impl AsRef<Path>,
    body_store: &BodyStore,
    kv: &crate::kv::MemoryKv,
    keep: &HashSet<[u8; 32]>,
) -> io::Result<usize> {
    let cis = cis.as_ref();
    let store = open_body_blob_store(cis);
    gc_bodies_with_store(store.as_ref(), cis, body_store, kv, keep)
}

fn list_body_blob_file_hashes(cis: &Path) -> io::Result<Vec<[u8; 32]>> {
    let root = bodies_dir(cis);
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in walk_body_files(&root)? {
        let path = entry?;
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.ends_with(".tmp") {
            continue;
        }
        let shard = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if shard.len() != 2 {
            continue;
        }
        let full_hex = format!("{shard}{name}");
        if full_hex.len() == 64 {
            if let Some(h) = parse_hex32(&full_hex) {
                out.push(h);
            }
        }
    }
    Ok(out)
}

pub fn walk_body_files_pub(root: &Path) -> io::Result<impl Iterator<Item = io::Result<PathBuf>> + '_> {
    walk_body_files(root)
}

fn walk_body_files(root: &Path) -> io::Result<impl Iterator<Item = io::Result<PathBuf>> + '_> {
    fn walk(dir: &Path, out: &mut Vec<io::Result<PathBuf>>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) => {
                out.push(Err(e));
                return;
            }
        };
        for ent in entries {
            match ent {
                Ok(e) => {
                    let p = e.path();
                    if p.is_dir() {
                        walk(&p, out);
                    } else if p.is_file() {
                        out.push(Ok(p));
                    }
                }
                Err(e) => out.push(Err(e)),
            }
        }
    }
    let mut v = Vec::new();
    walk(root, &mut v);
    Ok(v.into_iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body_store::BodyStore;
    use std::sync::Arc;

    #[test]
    fn blob_roundtrip_and_gc() {
        let tmp = tempfile::tempdir().unwrap();
        let h = content_hash("fn foo(): pass");
        save_body_blob_file(tmp.path(), &h, b"fn foo(): pass").unwrap();
        assert_eq!(
            load_body_blob_file(tmp.path(), &h).unwrap(),
            Some(b"fn foo(): pass".to_vec())
        );
        let keep = HashSet::new();
        assert_eq!(gc_body_blob_files(tmp.path(), &keep).unwrap(), 1);
        assert!(load_body_blob_file(tmp.path(), &h).unwrap().is_none());
    }

    fn content_hash(s: &str) -> [u8; 32] {
        crate::ingest::content_checksum_32(s)
    }

    #[test]
    fn gc_kv_bodies() {
        let tmp = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::kv::MemoryKv::new());
        let bs = BodyStore::new(Arc::clone(&kv));
        let h1 = content_hash("a");
        let h2 = content_hash("b");
        bs.put(h1, b"a".to_vec());
        bs.put(h2, b"b".to_vec());
        let mut keep = HashSet::new();
        keep.insert(h1);
        let n = gc_bodies(tmp.path(), &bs, &kv, &keep).unwrap();
        assert!(n >= 1);
        assert!(bs.get(&h1).is_some());
        assert!(bs.get(&h2).is_none());
    }

    #[test]
    fn file_store_trait_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileBodyBlobStore::new(tmp.path());
        let h = content_hash("x");
        store.put(&h, b"payload").unwrap();
        assert_eq!(store.get(&h).unwrap(), Some(b"payload".to_vec()));
        let mut keep = HashSet::new();
        keep.insert(h);
        assert_eq!(store.gc_except(&keep).unwrap(), 0);
        assert!(store.get(&h).unwrap().is_some());
    }

    #[cfg(feature = "body-sqlite")]
    #[test]
    fn sqlite_store_roundtrip_and_gc() {
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("CIS_BODY_BACKEND", "sqlite");
        let store = SqliteBodyBlobStore::open(tmp.path()).unwrap();
        let h1 = content_hash("one");
        let h2 = content_hash("two");
        store.put(&h1, b"1").unwrap();
        store.put(&h2, b"2").unwrap();
        let mut keep = HashSet::new();
        keep.insert(h1);
        assert_eq!(store.gc_except(&keep).unwrap(), 1);
        assert!(store.get(&h1).unwrap().is_some());
        assert!(store.get(&h2).unwrap().is_none());
        std::env::remove_var("CIS_BODY_BACKEND");
    }
}
