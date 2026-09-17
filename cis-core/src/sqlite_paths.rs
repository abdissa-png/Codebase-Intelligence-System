//! Shared SQLite file layout: split `graph.db` / `store.db` / … or one `.cis/cis.db`.
//!
//! Existing split files stay put. A new workspace uses `cis.db` when every store backend
//! is sqlite (or `CIS_SQLITE_UNIFIED=1`). `CIS_SQLITE_UNIFIED=0` forces split names.

use std::path::{Path, PathBuf};

const UNIFIED_NAME: &str = "cis.db";
const SPLIT_NAMES: &[&str] = &[
    "graph.db",
    "store.db",
    "wal.db",
    "vectors.db",
    "bodies.db",
];

/// Resolve the SQLite file for one store (`graph.db`, `store.db`, …).
pub fn sqlite_file(cis: &Path, split_name: &str) -> PathBuf {
    let unified = cis.join(UNIFIED_NAME);
    let split = cis.join(split_name);
    if unified.is_file() {
        return unified;
    }
    if split.is_file() {
        return split;
    }
    if sqlite_unified_from_env() && !any_split_exists(cis) {
        return unified;
    }
    split
}

/// `CIS_SQLITE_UNIFIED=1` / `0` overrides. Unset follows [`all_sqlite_backends_from_env`].
pub fn sqlite_unified_from_env() -> bool {
    match std::env::var_os("CIS_SQLITE_UNIFIED") {
        Some(v) if v == "0" || v.eq_ignore_ascii_case("false") => false,
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true") => true,
        _ => all_sqlite_backends_from_env(),
    }
}

pub fn all_sqlite_backends_from_env() -> bool {
    env_is_sqlite("CIS_GRAPH_BACKEND")
        && env_is_sqlite("CIS_KV_BACKEND")
        && env_is_sqlite("CIS_WAL_BACKEND")
        && env_is_sqlite("CIS_VECTOR_BACKEND")
        && env_is_sqlite("CIS_BODY_BACKEND")
        && env_is_sqlite("CIS_METADATA_BACKEND")
}

fn env_is_sqlite(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|v| v == "sqlite" || v == "sqlite3")
}

fn any_split_exists(cis: &Path) -> bool {
    SPLIT_NAMES.iter().any(|n| cis.join(n).is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_existing_cis_db() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cis.db"), []).unwrap();
        assert_eq!(
            sqlite_file(dir.path(), "store.db"),
            dir.path().join("cis.db")
        );
        assert_eq!(
            sqlite_file(dir.path(), "graph.db"),
            dir.path().join("cis.db")
        );
    }

    #[test]
    fn prefers_existing_split_when_no_cis_db() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("store.db"), []).unwrap();
        assert_eq!(
            sqlite_file(dir.path(), "store.db"),
            dir.path().join("store.db")
        );
        assert_eq!(
            sqlite_file(dir.path(), "graph.db"),
            dir.path().join("graph.db"),
            "missing split names must not jump to cis.db while another split file exists"
        );
    }
}
