//! SQLite-backed KV (`CIS_KV_BACKEND=sqlite`) in `.cis/store.db`.
//!
//! Durable prefixes live in the `kv` table. Ephemeral keys (`eto:`, `body:`, …)
//! stay in a RAM overlay so enabling SQLite does not change product durability.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::kv::{
    is_durable_kv_key, next_lexical_prefix, CasError, KvSnapshot, DURABLE_KV_PREFIXES,
};

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS kv (
        k TEXT PRIMARY KEY COLLATE BINARY,
        v BLOB NOT NULL
    ) WITHOUT ROWID;
";

pub struct SqliteKv {
    conn: Mutex<Connection>,
    ephemeral: RwLock<BTreeMap<String, Vec<u8>>>,
    sql_writes: AtomicU64,
}

impl std::fmt::Debug for SqliteKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteKv").finish_non_exhaustive()
    }
}

impl SqliteKv {
    pub fn open(cis_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(cis_dir)?;
        let path = crate::metadata_store::store_db_path(cis_dir);
        let conn = Connection::open(path).map_err(|e| io::Error::other(e.to_string()))?;
        configure_conn(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            ephemeral: RwLock::new(BTreeMap::new()),
            sql_writes: AtomicU64::new(0),
        })
    }

    pub fn open_in_memory() -> Self {
        let conn = Connection::open_in_memory().expect("sqlite kv :memory:");
        configure_conn(&conn).expect("sqlite kv pragma");
        Self {
            conn: Mutex::new(conn),
            ephemeral: RwLock::new(BTreeMap::new()),
            sql_writes: AtomicU64::new(0),
        }
    }

    pub fn sql_write_count(&self) -> u64 {
        self.sql_writes.load(Ordering::Relaxed)
    }

    pub fn durable_row_count(&self) -> usize {
        let conn = self.conn.lock().expect("sqlite kv lock");
        conn.query_row("SELECT COUNT(*) FROM kv", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
            .unwrap_or(0)
    }

    fn bump_write(&self) {
        self.sql_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        if is_durable_kv_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            conn.query_row("SELECT v FROM kv WHERE k = ?1", params![key], |r| r.get(0))
                .optional()
                .unwrap_or_else(|e| panic!("cis sqlite kv get: {e}"))
        } else {
            self.ephemeral.read().unwrap().get(key).cloned()
        }
    }

    pub fn set(&self, key: &str, value: Vec<u8>) {
        if is_durable_kv_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            conn.execute(
                "INSERT INTO kv (k, v) VALUES (?1, ?2)
                 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                params![key, value],
            )
            .unwrap_or_else(|e| panic!("cis sqlite kv set: {e}"));
            self.bump_write();
        } else {
            self.ephemeral
                .write()
                .unwrap()
                .insert(key.to_string(), value);
        }
    }

    pub fn delete(&self, key: &str) -> Option<Vec<u8>> {
        if is_durable_kv_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            let prev: Option<Vec<u8>> = conn
                .query_row("SELECT v FROM kv WHERE k = ?1", params![key], |r| r.get(0))
                .optional()
                .unwrap_or_else(|e| panic!("cis sqlite kv delete: {e}"));
            if prev.is_some() {
                conn.execute("DELETE FROM kv WHERE k = ?1", params![key])
                    .unwrap_or_else(|e| panic!("cis sqlite kv delete: {e}"));
                self.bump_write();
            }
            prev
        } else {
            self.ephemeral.write().unwrap().remove(key)
        }
    }

    pub fn compare_and_delete(&self, key: &str, expected: &[u8]) -> Result<(), CasError> {
        if !is_durable_kv_key(key) {
            let mut g = self.ephemeral.write().unwrap();
            let cur = g.get(key).map(|v| v.as_slice());
            if cur != Some(expected) {
                return Err(CasError::Mismatch(key.to_string()));
            }
            g.remove(key);
            return Ok(());
        }
        let mut conn = self.conn.lock().expect("sqlite kv lock");
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap_or_else(|e| panic!("cis sqlite kv cas begin: {e}"));
        let n = tx
            .execute(
                "DELETE FROM kv WHERE k = ?1 AND v = ?2",
                params![key, expected],
            )
            .unwrap_or_else(|e| panic!("cis sqlite kv compare_and_delete: {e}"));
        if n == 0 {
            let _ = tx.rollback();
            return Err(CasError::Mismatch(key.to_string()));
        }
        tx.commit()
            .unwrap_or_else(|e| panic!("cis sqlite kv cas commit: {e}"));
        self.bump_write();
        Ok(())
    }

    pub fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: Vec<u8>,
    ) -> Result<(), CasError> {
        if !is_durable_kv_key(key) {
            let mut g = self.ephemeral.write().unwrap();
            let cur = g.get(key).map(|v| v.as_slice());
            match expected {
                None => {
                    if cur.is_some() {
                        return Err(CasError::Mismatch(key.to_string()));
                    }
                    g.insert(key.to_string(), value);
                    Ok(())
                }
                Some(exp) => {
                    if cur != Some(exp) {
                        return Err(CasError::Mismatch(key.to_string()));
                    }
                    g.insert(key.to_string(), value);
                    Ok(())
                }
            }
        } else {
            let mut conn = self.conn.lock().expect("sqlite kv lock");
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap_or_else(|e| panic!("cis sqlite kv cas begin: {e}"));
            let current: Option<Vec<u8>> = tx
                .query_row("SELECT v FROM kv WHERE k = ?1", params![key], |r| r.get(0))
                .optional()
                .unwrap_or_else(|e| panic!("cis sqlite kv cas get: {e}"));
            match expected {
                None => {
                    if current.is_some() {
                        let _ = tx.rollback();
                        return Err(CasError::Mismatch(key.to_string()));
                    }
                    tx.execute(
                        "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                        params![key, value],
                    )
                    .unwrap_or_else(|e| panic!("cis sqlite kv cas insert: {e}"));
                }
                Some(exp) => {
                    if current.as_deref() != Some(exp) {
                        let _ = tx.rollback();
                        return Err(CasError::Mismatch(key.to_string()));
                    }
                    tx.execute(
                        "INSERT INTO kv (k, v) VALUES (?1, ?2)
                         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                        params![key, value],
                    )
                    .unwrap_or_else(|e| panic!("cis sqlite kv cas update: {e}"));
                }
            }
            tx.commit()
                .unwrap_or_else(|e| panic!("cis sqlite kv cas commit: {e}"));
            self.bump_write();
            Ok(())
        }
    }

    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        let end = next_lexical_prefix(prefix);
        let mut rows = Vec::new();
        if prefix_may_hit_durable(prefix) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            let mut stmt = conn
                .prepare("SELECT k, v FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k")
                .unwrap_or_else(|e| panic!("cis sqlite kv scan: {e}"));
            let iter = stmt
                .query_map(params![prefix, end], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .unwrap_or_else(|e| panic!("cis sqlite kv scan: {e}"));
            for r in iter {
                rows.push(r.unwrap_or_else(|e| panic!("cis sqlite kv scan: {e}")));
            }
        }
        {
            let g = self.ephemeral.read().unwrap();
            rows.extend(
                g.range(prefix.to_string()..end)
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
        }
        if rows.len() > 1 {
            rows.sort_by(|a, b| a.0.cmp(&b.0));
        }
        rows
    }

    pub fn snapshot(&self) -> KvSnapshot {
        let mut entries = BTreeMap::new();
        {
            let conn = self.conn.lock().expect("sqlite kv lock");
            let mut stmt = conn
                .prepare("SELECT k, v FROM kv")
                .unwrap_or_else(|e| panic!("cis sqlite kv snapshot: {e}"));
            let iter = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .unwrap_or_else(|e| panic!("cis sqlite kv snapshot: {e}"));
            for r in iter {
                let (k, v) = r.unwrap_or_else(|e| panic!("cis sqlite kv snapshot: {e}"));
                entries.insert(k, v);
            }
        }
        entries.extend(self.ephemeral.read().unwrap().clone());
        KvSnapshot { entries }
    }

    pub fn restore_snapshot(&self, snap: &KvSnapshot) {
        {
            let conn = self.conn.lock().expect("sqlite kv lock");
            conn.execute("DELETE FROM kv", [])
                .unwrap_or_else(|e| panic!("cis sqlite kv restore: {e}"));
            self.bump_write();
        }
        self.ephemeral.write().unwrap().clear();
        self.merge_snapshot(snap);
    }

    pub fn merge_snapshot(&self, snap: &KvSnapshot) {
        let mut conn = self.conn.lock().expect("sqlite kv lock");
        let tx = conn
            .transaction()
            .unwrap_or_else(|e| panic!("cis sqlite kv merge begin: {e}"));
        let mut eph = self.ephemeral.write().unwrap();
        for (k, v) in &snap.entries {
            if is_durable_kv_key(k) {
                tx.execute(
                    "INSERT INTO kv (k, v) VALUES (?1, ?2)
                     ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                    params![k, v],
                )
                .unwrap_or_else(|e| panic!("cis sqlite kv merge: {e}"));
            } else {
                eph.insert(k.clone(), v.clone());
            }
        }
        drop(eph);
        tx.commit()
            .unwrap_or_else(|e| panic!("cis sqlite kv merge commit: {e}"));
        self.bump_write();
    }

    /// Copy `src_prefix*` → `dst_prefix*` with one `INSERT…SELECT` for durable rows.
    pub fn copy_prefix_remap(&self, src_prefix: &str, dst_prefix: &str) -> usize {
        let end = next_lexical_prefix(src_prefix);
        let start = (src_prefix.len() as i64) + 1;
        let mut n = 0usize;
        if prefix_may_hit_durable(src_prefix) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            n += conn
                .execute(
                    "INSERT OR IGNORE INTO kv (k, v)
                     SELECT ?1 || substr(k, ?2), v FROM kv
                     WHERE k >= ?3 AND k < ?4",
                    params![dst_prefix, start, src_prefix, end],
                )
                .unwrap_or_else(|e| panic!("cis sqlite kv copy_prefix: {e}"));
            self.bump_write();
        }
        let mut eph = self.ephemeral.write().unwrap();
        let pending: Vec<(String, Vec<u8>)> = eph
            .range(src_prefix.to_string()..end)
            .filter_map(|(k, v)| {
                let suffix = k.strip_prefix(src_prefix)?;
                Some((format!("{dst_prefix}{suffix}"), v.clone()))
            })
            .collect();
        for (dest, v) in pending {
            if !eph.contains_key(&dest) {
                eph.insert(dest, v);
                n += 1;
            }
        }
        n
    }
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

fn prefix_may_hit_durable(prefix: &str) -> bool {
    DURABLE_KV_PREFIXES
        .iter()
        .any(|p| prefix.starts_with(p) || p.starts_with(prefix))
}
