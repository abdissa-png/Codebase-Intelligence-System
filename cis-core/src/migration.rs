//! One-shot migration helpers (**Phase 8B/8C**).

use std::fs;
use std::io;
use std::path::Path;

use crate::body_blob::{
    open_body_blob_store, walk_body_files_pub, BodyBlobStore, FileBodyBlobStore,
};
#[cfg(feature = "body-sqlite")]
use crate::body_blob::SqliteBodyBlobStore;
use crate::kv::MemoryKv;
use crate::persistence::{cis_dir, kv_snapshot_path, load_kv_snapshot};
use cis_wal::BranchId;

#[derive(Debug, Default)]
pub struct MigrateBodiesReport {
    pub files_scanned: usize,
    pub inserted: usize,
    pub skipped_existing: usize,
    pub verified: usize,
    pub files_deleted: usize,
}

/// Copy `.cis/bodies/` shard files into `bodies.db` (or active blob store).
pub fn migrate_bodies_from_files(
    repo_root: impl AsRef<Path>,
    delete_files: bool,
) -> io::Result<MigrateBodiesReport> {
    if std::env::var_os("CIS_MIGRATION_STRICT").is_some_and(|v| v == "1") {
        #[cfg(not(feature = "body-sqlite"))]
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "CIS_MIGRATION_STRICT=1 requires body-sqlite feature",
            ));
        }
        if std::env::var_os("CIS_BODY_BACKEND").is_some_and(|v| v == "sqlite") {
            #[cfg(not(feature = "body-sqlite"))]
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "CIS_BODY_BACKEND=sqlite requires body-sqlite feature",
                ));
            }
        }
    }
    let cis = cis_dir(&repo_root);
    let root = cis.join("bodies");
    if !root.is_dir() {
        return Ok(MigrateBodiesReport::default());
    }
    let store = open_body_blob_store(&cis);
    let mut rep = MigrateBodiesReport::default();
    for entry in walk_body_files_pub(&root)? {
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
        if full_hex.len() != 64 {
            continue;
        }
        rep.files_scanned += 1;
        let bytes = fs::read(&path)?;
        let mut h = [0u8; 32];
        for (i, chunk) in full_hex.as_bytes().chunks(2).enumerate() {
            if i >= 32 {
                break;
            }
            h[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap_or("00"), 16).unwrap_or(0);
        }
        if store.get(&h)?.is_some() {
            rep.skipped_existing += 1;
        } else {
            store.put(&h, &bytes)?;
            rep.inserted += 1;
        }
        if let Ok(Some(round)) = store.get(&h) {
            if round == bytes {
                rep.verified += 1;
            }
        }
        if delete_files {
            fs::remove_file(&path)?;
            rep.files_deleted += 1;
        }
    }
    Ok(rep)
}

#[derive(Debug, Default)]
pub struct MigrateKvReport {
    pub ris_keys_migrated: usize,
    pub kv_keys_migrated: usize,
    pub kv_json_stripped: usize,
}

fn parse_ris_key(key: &str) -> Option<(BranchId, u64)> {
    let rest = key.strip_prefix("ris:")?;
    let (branch_hex, epoch_str) = rest.split_once(':')?;
    if branch_hex.len() != 32 || epoch_str.len() != 20 {
        return None;
    }
    let mut branch = [0u8; 16];
    for (i, chunk) in branch_hex.as_bytes().chunks(2).enumerate() {
        if i >= 16 {
            break;
        }
        branch[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    let epoch = epoch_str.parse().ok()?;
    Some((BranchId(branch), epoch))
}

/// Copy `ris:` entries from `kv.json` into SQLite metadata store.
#[cfg(feature = "body-sqlite")]
pub fn migrate_kv_ris_to_sqlite(
    repo_root: impl AsRef<Path>,
    compact_kv_json: bool,
) -> io::Result<MigrateKvReport> {
    use crate::metadata_store::MetadataStore;

    let root = repo_root.as_ref();
    let cis = cis_dir(root);
    let kpath = kv_snapshot_path(&cis);
    if !kpath.exists() {
        return Ok(MigrateKvReport::default());
    }
    let kv = MemoryKv::new();
    load_kv_snapshot(&kpath, &kv)?;
    let meta = MetadataStore::open(&cis)?;
    let mut rep = MigrateKvReport::default();
    let snap = kv.snapshot();
    for (key, value) in &snap.entries {
        if let Some((branch, epoch)) = parse_ris_key(key) {
            meta.put_ris_snapshot(branch, epoch, value)?;
            rep.ris_keys_migrated += 1;
        }
    }
    if compact_kv_json {
        let ris_keys: Vec<String> = snap
            .entries
            .keys()
            .filter(|k| k.starts_with("ris:"))
            .cloned()
            .collect();
        rep.kv_json_stripped = ris_keys.len();
        for key in ris_keys {
            kv.delete(&key);
        }
        crate::persistence::save_kv_snapshot(&kpath, &kv)?;
    }
    Ok(rep)
}

#[cfg(not(feature = "body-sqlite"))]
pub fn migrate_kv_ris_to_sqlite(
    _repo_root: impl AsRef<Path>,
    _compact_kv_json: bool,
) -> io::Result<MigrateKvReport> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "migrate-kv requires --features body-sqlite",
    ))
}

/// Verify file vs sqlite body counts (when both exist).
pub fn verify_body_migration(repo_root: impl AsRef<Path>) -> io::Result<(usize, usize)> {
    let cis = cis_dir(&repo_root);
    let file_store = FileBodyBlobStore::new(&cis);
    let file_n = file_store.list_hashes()?.len();
    #[cfg(feature = "body-sqlite")]
    {
        if cis.join("bodies.db").is_file() {
            let sqlite = SqliteBodyBlobStore::open(&cis)?;
            return Ok((file_n, sqlite.list_hashes()?.len()));
        }
    }
    Ok((file_n, 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body_blob::save_body_blob_file;
    use crate::ingest::content_checksum_32;

    #[test]
    fn migrate_bodies_inserts_files() {
        let tmp = tempfile::tempdir().unwrap();
        let cis = cis_dir(tmp.path());
        let h = content_checksum_32("migrate-me");
        save_body_blob_file(&cis, &h, b"migrate-me").unwrap();
        std::env::set_var("CIS_BODY_BACKEND", "file");
        let rep = migrate_bodies_from_files(tmp.path(), false).unwrap();
        assert!(rep.files_scanned >= 1);
        assert!(rep.inserted >= 1 || rep.skipped_existing >= 1);
    }
}
