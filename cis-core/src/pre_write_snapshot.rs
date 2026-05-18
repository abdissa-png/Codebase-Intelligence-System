//! Pre-write byte snapshots for speculative patches — restore disk on revert.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Captures file bytes before the first overwrite per path in a patch.
#[derive(Debug)]
pub struct PreWriteSnapshotStore {
    by_patch: Mutex<HashMap<u64, HashMap<String, Vec<u8>>>>,
    disk_dir: PathBuf,
}

impl PreWriteSnapshotStore {
    pub fn new(cis_dir: PathBuf) -> io::Result<Self> {
        let disk_dir = cis_dir.join("pre_write_snapshots");
        fs::create_dir_all(&disk_dir)?;
        Ok(Self {
            by_patch: Mutex::new(HashMap::new()),
            disk_dir,
        })
    }

    /// Record pre-write bytes for `rel_path` under `patch_id` (first write only per path).
    pub fn capture_if_absent(
        &self,
        patch_id: u64,
        rel_path: &str,
        pre_write_bytes: Vec<u8>,
    ) -> io::Result<()> {
        let mut g = self.by_patch.lock().unwrap();
        let entry = g.entry(patch_id).or_default();
        if entry.contains_key(rel_path) {
            return Ok(());
        }
        self.persist_patch_file(patch_id, rel_path, &pre_write_bytes)?;
        entry.insert(rel_path.to_string(), pre_write_bytes);
        Ok(())
    }

    /// Restore all snapshotted paths for `patch_id` under `repo_root`.
    pub fn restore_patch(&self, patch_id: u64, repo_root: &Path) -> io::Result<()> {
        let paths: HashMap<String, Vec<u8>> = {
            let g = self.by_patch.lock().unwrap();
            g.get(&patch_id).cloned().unwrap_or_default()
        };
        for (rel, bytes) in paths {
            let abs = repo_root.join(&rel);
            if bytes.is_empty() {
                let _ = fs::remove_file(&abs);
            } else if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent)?;
                fs::write(&abs, &bytes)?;
            } else {
                fs::write(&abs, &bytes)?;
            }
        }
        Ok(())
    }

    /// Restore a single path from an inline snapshot (used when token write fails before store capture).
    pub fn restore_inline(repo_root: &Path, rel_path: &str, pre_write_bytes: &[u8]) -> io::Result<()> {
        let abs = repo_root.join(rel_path);
        if pre_write_bytes.is_empty() {
            let _ = fs::remove_file(&abs);
        } else if let Some(parent) = abs.parent() {
            fs::create_dir_all(parent)?;
            fs::write(&abs, pre_write_bytes)?;
        } else {
            fs::write(&abs, pre_write_bytes)?;
        }
        Ok(())
    }

    /// Drop in-memory and on-disk snapshots for `patch_id`.
    pub fn clear_patch(&self, patch_id: u64) {
        self.by_patch.lock().unwrap().remove(&patch_id);
        let patch_dir = self.disk_dir.join(patch_id.to_string());
        let _ = fs::remove_dir_all(patch_dir);
    }

    fn persist_patch_file(&self, patch_id: u64, rel_path: &str, bytes: &[u8]) -> io::Result<()> {
        let patch_dir = self.disk_dir.join(patch_id.to_string());
        fs::create_dir_all(&patch_dir)?;
        let safe_name = rel_path.replace('/', "__");
        let final_path = patch_dir.join(format!("{}.bin", safe_name));
        let tmp_path = patch_dir.join(format!("{}.bin.tmp", safe_name));
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            f.write_all(bytes)?;
            f.flush()?;
            f.sync_all()?;
        }
        fs::rename(tmp_path, final_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_restore_and_clear() {
        let dir = std::env::temp_dir().join(format!("cis-prewrite-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let repo = dir.join("repo");
        fs::create_dir_all(&repo).unwrap();
        let cis = dir.join(".cis");
        let store = PreWriteSnapshotStore::new(cis).unwrap();

        store.capture_if_absent(1, "a.py", b"original".to_vec()).unwrap();
        store.capture_if_absent(1, "a.py", b"ignored".to_vec()).unwrap();

        fs::write(repo.join("a.py"), b"modified").unwrap();
        store.restore_patch(1, &repo).unwrap();
        assert_eq!(fs::read(repo.join("a.py")).unwrap(), b"original");

        store.clear_patch(1);
        store.restore_patch(1, &repo).unwrap();
        assert_eq!(fs::read(repo.join("a.py")).unwrap(), b"original");
    }

    #[test]
    fn restore_removes_new_file_when_pre_write_empty() {
        let dir = std::env::temp_dir().join(format!("cis-prewrite-new-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let repo = dir.join("repo");
        fs::create_dir_all(&repo).unwrap();
        let cis = dir.join(".cis");
        let store = PreWriteSnapshotStore::new(cis).unwrap();

        store.capture_if_absent(2, "new.py", Vec::new()).unwrap();
        fs::write(repo.join("new.py"), b"created").unwrap();
        store.restore_patch(2, &repo).unwrap();
        assert!(!repo.join("new.py").exists());
    }
}
