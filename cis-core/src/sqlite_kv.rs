//! SQLite-backed KV (`CIS_KV_BACKEND=sqlite`) in `.cis/store.db`.
//!
//! Full-width `ri:{branch}:{identity}` and `deleted:{branch}:{identity}` keys live in
//! typed tables (no hex-in-string rows). Remaining durable prefixes stay in `kv`.
//! Ephemeral keys (`eto:`, `body:`, …) stay in a RAM overlay so enabling SQLite
//! does not change product durability — **ETO does not survive restart**.

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
    CREATE TABLE IF NOT EXISTS ri (
        branch_id BLOB NOT NULL,
        identity_id BLOB NOT NULL,
        revision_id BLOB NOT NULL,
        PRIMARY KEY (branch_id, identity_id)
    ) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS deleted (
        branch_id BLOB NOT NULL,
        identity_id BLOB NOT NULL,
        payload BLOB NOT NULL,
        PRIMARY KEY (branch_id, identity_id)
    ) WITHOUT ROWID;
";

#[derive(Clone, Copy)]
enum TypedKey {
    Ri {
        branch: [u8; 16],
        identity: [u8; 16],
    },
    Deleted {
        branch: [u8; 16],
        identity: [u8; 16],
    },
}

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
        conn.query_row(
            "SELECT (SELECT COUNT(*) FROM kv)
                  + (SELECT COUNT(*) FROM ri)
                  + (SELECT COUNT(*) FROM deleted)",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map(|n| n as usize)
        .unwrap_or(0)
    }

    /// Rows still stored as hex strings in the generic `kv` table (tests / leftovers).
    pub fn hex_kv_table_count(&self) -> usize {
        let conn = self.conn.lock().expect("sqlite kv lock");
        conn.query_row("SELECT COUNT(*) FROM kv", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
            .unwrap_or(0)
    }

    pub fn typed_ri_count(&self) -> usize {
        let conn = self.conn.lock().expect("sqlite kv lock");
        conn.query_row("SELECT COUNT(*) FROM ri", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
            .unwrap_or(0)
    }

    fn bump_write(&self) {
        self.sql_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(tk) = parse_typed_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            return typed_get(&conn, tk);
        }
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
        if let Some(tk) = parse_typed_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            typed_upsert(&conn, tk, &value);
            self.bump_write();
            return;
        }
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
        if let Some(tk) = parse_typed_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            let prev = typed_get(&conn, tk);
            if prev.is_some() {
                typed_delete(&conn, tk);
                self.bump_write();
            }
            return prev;
        }
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
        if let Some(tk) = parse_typed_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            let cur = typed_get(&conn, tk);
            if cur.as_deref() != Some(expected) {
                return Err(CasError::Mismatch(key.to_string()));
            }
            typed_delete(&conn, tk);
            self.bump_write();
            return Ok(());
        }
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
        if let Some(tk) = parse_typed_key(key) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            let current = typed_get(&conn, tk);
            match expected {
                None => {
                    if current.is_some() {
                        return Err(CasError::Mismatch(key.to_string()));
                    }
                    typed_upsert(&conn, tk, &value);
                }
                Some(exp) => {
                    if current.as_deref() != Some(exp) {
                        return Err(CasError::Mismatch(key.to_string()));
                    }
                    typed_upsert(&conn, tk, &value);
                }
            }
            self.bump_write();
            return Ok(());
        }
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
            rows.extend(typed_scan(&conn, prefix, &end));
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
            for (k, v) in typed_scan(&conn, "ri:", &next_lexical_prefix("ri:")) {
                entries.insert(k, v);
            }
            for (k, v) in typed_scan(&conn, "deleted:", &next_lexical_prefix("deleted:")) {
                entries.insert(k, v);
            }
        }
        entries.extend(self.ephemeral.read().unwrap().clone());
        KvSnapshot { entries }
    }

    pub fn restore_snapshot(&self, snap: &KvSnapshot) {
        {
            let conn = self.conn.lock().expect("sqlite kv lock");
            conn.execute_batch("DELETE FROM kv; DELETE FROM ri; DELETE FROM deleted;")
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
            if let Some(tk) = parse_typed_key(k) {
                typed_upsert(&tx, tk, v);
            } else if is_durable_kv_key(k) {
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
        if let (Some(src_b), Some(dst_b)) = (
            parse_ri_branch_copy_prefix(src_prefix),
            parse_ri_branch_copy_prefix(dst_prefix),
        ) {
            let conn = self.conn.lock().expect("sqlite kv lock");
            n += conn
                .execute(
                    "INSERT OR IGNORE INTO ri (branch_id, identity_id, revision_id)
                     SELECT ?1, identity_id, revision_id FROM ri WHERE branch_id = ?2",
                    params![dst_b.as_slice(), src_b.as_slice()],
                )
                .unwrap_or_else(|e| panic!("cis sqlite kv copy_prefix ri: {e}"));
            self.bump_write();
        } else if prefix_may_hit_durable(src_prefix) {
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
            let typed_n = typed_copy_filter(&conn, src_prefix, dst_prefix, &end);
            if typed_n > 0 {
                self.bump_write();
            }
            n += typed_n;
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
    migrate_hex_kv_to_typed(conn)?;
    Ok(())
}

fn migrate_hex_kv_to_typed(conn: &Connection) -> io::Result<()> {
    let mut stmt = conn
        .prepare("SELECT k, v FROM kv WHERE k LIKE 'ri:%' OR k LIKE 'deleted:%'")
        .map_err(|e| io::Error::other(e.to_string()))?;
    let iter = stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)))
        .map_err(|e| io::Error::other(e.to_string()))?;
    let mut move_me = Vec::new();
    for r in iter {
        let (k, v) = r.map_err(|e| io::Error::other(e.to_string()))?;
        if parse_typed_key(&k).is_some() {
            move_me.push((k, v));
        }
    }
    drop(stmt);
    if move_me.is_empty() {
        return Ok(());
    }
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| io::Error::other(e.to_string()))?;
    for (k, v) in &move_me {
        let tk = parse_typed_key(k).expect("filtered");
        typed_upsert(&tx, tk, v);
        tx.execute("DELETE FROM kv WHERE k = ?1", params![k])
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    tx.commit().map_err(|e| io::Error::other(e.to_string()))?;
    Ok(())
}

fn prefix_may_hit_durable(prefix: &str) -> bool {
    DURABLE_KV_PREFIXES
        .iter()
        .any(|p| prefix.starts_with(p) || p.starts_with(prefix))
}

fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn parse_typed_key(key: &str) -> Option<TypedKey> {
    if let Some(rest) = key.strip_prefix("ri:") {
        let (bhex, ihex) = rest.split_once(':')?;
        return Some(TypedKey::Ri {
            branch: parse_hex16(bhex)?,
            identity: parse_hex16(ihex)?,
        });
    }
    if let Some(rest) = key.strip_prefix("deleted:") {
        let (bhex, ihex) = rest.split_once(':')?;
        return Some(TypedKey::Deleted {
            branch: parse_hex16(bhex)?,
            identity: parse_hex16(ihex)?,
        });
    }
    None
}

fn format_ri_key(branch: &[u8; 16], identity: &[u8; 16]) -> String {
    format!("ri:{}:{}", hex16(branch), hex16(identity))
}

fn format_deleted_key(branch: &[u8; 16], identity: &[u8; 16]) -> String {
    format!("deleted:{}:{}", hex16(branch), hex16(identity))
}

/// `ri:{32-hex}:` used by [`crate::revision_index::fork_branch_bindings`].
fn parse_ri_branch_copy_prefix(prefix: &str) -> Option<[u8; 16]> {
    let rest = prefix.strip_prefix("ri:")?;
    let rest = rest.strip_suffix(':').unwrap_or(rest);
    if rest.contains(':') {
        return None;
    }
    parse_hex16(rest)
}

fn typed_get(conn: &Connection, tk: TypedKey) -> Option<Vec<u8>> {
    match tk {
        TypedKey::Ri { branch, identity } => conn
            .query_row(
                "SELECT revision_id FROM ri WHERE branch_id = ?1 AND identity_id = ?2",
                params![branch.as_slice(), identity.as_slice()],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or_else(|e| panic!("cis sqlite kv ri get: {e}")),
        TypedKey::Deleted { branch, identity } => conn
            .query_row(
                "SELECT payload FROM deleted WHERE branch_id = ?1 AND identity_id = ?2",
                params![branch.as_slice(), identity.as_slice()],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or_else(|e| panic!("cis sqlite kv deleted get: {e}")),
    }
}

fn typed_upsert(conn: &Connection, tk: TypedKey, value: &[u8]) {
    match tk {
        TypedKey::Ri { branch, identity } => {
            conn.execute(
                "INSERT INTO ri (branch_id, identity_id, revision_id) VALUES (?1, ?2, ?3)
                 ON CONFLICT(branch_id, identity_id) DO UPDATE SET revision_id = excluded.revision_id",
                params![branch.as_slice(), identity.as_slice(), value],
            )
            .unwrap_or_else(|e| panic!("cis sqlite kv ri set: {e}"));
        }
        TypedKey::Deleted { branch, identity } => {
            conn.execute(
                "INSERT INTO deleted (branch_id, identity_id, payload) VALUES (?1, ?2, ?3)
                 ON CONFLICT(branch_id, identity_id) DO UPDATE SET payload = excluded.payload",
                params![branch.as_slice(), identity.as_slice(), value],
            )
            .unwrap_or_else(|e| panic!("cis sqlite kv deleted set: {e}"));
        }
    }
}

fn typed_delete(conn: &Connection, tk: TypedKey) {
    match tk {
        TypedKey::Ri { branch, identity } => {
            conn.execute(
                "DELETE FROM ri WHERE branch_id = ?1 AND identity_id = ?2",
                params![branch.as_slice(), identity.as_slice()],
            )
            .unwrap_or_else(|e| panic!("cis sqlite kv ri delete: {e}"));
        }
        TypedKey::Deleted { branch, identity } => {
            conn.execute(
                "DELETE FROM deleted WHERE branch_id = ?1 AND identity_id = ?2",
                params![branch.as_slice(), identity.as_slice()],
            )
            .unwrap_or_else(|e| panic!("cis sqlite kv deleted delete: {e}"));
        }
    }
}

fn typed_scan(conn: &Connection, prefix: &str, end: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    if range_may_include(prefix, end, "ri:") {
        if let Some(branch) = parse_ri_branch_copy_prefix(prefix) {
            let mut stmt = conn
                .prepare("SELECT identity_id, revision_id FROM ri WHERE branch_id = ?1")
                .unwrap_or_else(|e| panic!("cis sqlite kv ri scan: {e}"));
            let iter = stmt
                .query_map(params![branch.as_slice()], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .unwrap_or_else(|e| panic!("cis sqlite kv ri scan: {e}"));
            for r in iter {
                let (ident, rev) = r.unwrap_or_else(|e| panic!("cis sqlite kv ri scan: {e}"));
                let Some(ib) = blob16(&ident) else {
                    continue;
                };
                let k = format_ri_key(&branch, &ib);
                if k.as_str() >= prefix && k.as_str() < end {
                    out.push((k, rev));
                }
            }
        } else {
            let mut stmt = conn
                .prepare("SELECT branch_id, identity_id, revision_id FROM ri")
                .unwrap_or_else(|e| panic!("cis sqlite kv ri scan all: {e}"));
            let iter = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap_or_else(|e| panic!("cis sqlite kv ri scan all: {e}"));
            for r in iter {
                let (b, i, v) = r.unwrap_or_else(|e| panic!("cis sqlite kv ri scan all: {e}"));
                let (Some(bb), Some(ib)) = (blob16(&b), blob16(&i)) else {
                    continue;
                };
                let k = format_ri_key(&bb, &ib);
                if k.as_str() >= prefix && k.as_str() < end {
                    out.push((k, v));
                }
            }
        }
    }
    if range_may_include(prefix, end, "deleted:") {
        let mut stmt = conn
            .prepare("SELECT branch_id, identity_id, payload FROM deleted")
            .unwrap_or_else(|e| panic!("cis sqlite kv deleted scan: {e}"));
        let iter = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .unwrap_or_else(|e| panic!("cis sqlite kv deleted scan: {e}"));
        for r in iter {
            let (b, i, v) = r.unwrap_or_else(|e| panic!("cis sqlite kv deleted scan: {e}"));
            let (Some(bb), Some(ib)) = (blob16(&b), blob16(&i)) else {
                continue;
            };
            let k = format_deleted_key(&bb, &ib);
            if k.as_str() >= prefix && k.as_str() < end {
                out.push((k, v));
            }
        }
    }
    out
}

fn typed_copy_filter(conn: &Connection, src_prefix: &str, dst_prefix: &str, end: &str) -> usize {
    let mut n = 0usize;
    for (k, v) in typed_scan(conn, src_prefix, end) {
        let Some(suffix) = k.strip_prefix(src_prefix) else {
            continue;
        };
        let dest = format!("{dst_prefix}{suffix}");
        let Some(tk) = parse_typed_key(&dest) else {
            continue;
        };
        if typed_get(conn, tk).is_some() {
            continue;
        }
        typed_upsert(conn, tk, &v);
        n += 1;
    }
    n
}

fn range_may_include(prefix: &str, end: &str, token: &str) -> bool {
    token >= prefix && token < end || token.starts_with(prefix) || prefix.starts_with(token)
}

fn blob16(v: &[u8]) -> Option<[u8; 16]> {
    (v.len() == 16).then(|| {
        let mut a = [0u8; 16];
        a.copy_from_slice(v);
        a
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::revision_index::revision_binding_kv_key;
    use cis_wal::{BranchId, IdentityId, NodeRevisionId};

    #[test]
    fn full_hex_ri_uses_typed_table_not_kv() {
        let kv = SqliteKv::open_in_memory();
        let branch = BranchId([1u8; 16]);
        let ident = IdentityId([2u8; 16]);
        let rev = NodeRevisionId([3u8; 16]);
        let key = revision_binding_kv_key(branch, ident);
        kv.set(&key, rev.0.to_vec());
        assert_eq!(kv.get(&key), Some(rev.0.to_vec()));
        assert_eq!(kv.typed_ri_count(), 1);
        assert_eq!(kv.hex_kv_table_count(), 0, "full-width ri keys must not stay as hex KV");
        assert_eq!(kv.scan_prefix(&format!("ri:{}:", hex16(&branch.0))).len(), 1);
    }

    #[test]
    fn short_ri_test_keys_remain_in_kv_table() {
        let kv = SqliteKv::open_in_memory();
        kv.set("ri:aa:01", vec![1]);
        assert_eq!(kv.get("ri:aa:01"), Some(vec![1]));
        assert_eq!(kv.typed_ri_count(), 0);
        assert_eq!(kv.hex_kv_table_count(), 1);
    }

    #[test]
    fn migrate_hex_ri_rows_into_typed_table() {
        let kv = SqliteKv::open_in_memory();
        let branch = BranchId([9u8; 16]);
        let ident = IdentityId([8u8; 16]);
        let key = revision_binding_kv_key(branch, ident);
        {
            let conn = kv.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                params![key, [7u8; 16].as_slice()],
            )
            .unwrap();
        }
        assert_eq!(kv.typed_ri_count(), 0);
        migrate_hex_kv_to_typed(&kv.conn.lock().unwrap()).unwrap();
        assert_eq!(kv.typed_ri_count(), 1);
        assert_eq!(kv.hex_kv_table_count(), 0);
        assert_eq!(kv.get(&key), Some(vec![7u8; 16]));
    }

    #[test]
    fn deleted_full_hex_uses_typed_table() {
        use crate::deletion_absence::deleted_key;
        let kv = SqliteKv::open_in_memory();
        let key = deleted_key(BranchId([4u8; 16]), IdentityId([5u8; 16]));
        kv.set(&key, 42u64.to_le_bytes().to_vec());
        assert_eq!(kv.get(&key), Some(42u64.to_le_bytes().to_vec()));
        assert_eq!(kv.hex_kv_table_count(), 0);
        assert_eq!(kv.scan_prefix("deleted:").len(), 1);
        kv.delete(&key);
        assert!(kv.get(&key).is_none());
        assert_eq!(kv.scan_prefix("deleted:").len(), 0);
    }
}
