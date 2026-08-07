//! Opt-in git branch name → CIS active branch sync (**Phase 2.2**, ADR 0007).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::branch_registry::BranchRegistry;
use crate::fork_branch_bindings;
use crate::revision_cow::RevisionIndexCow;
use crate::MemoryKv;
use cis_wal::BranchId;

static GIT_SYNC_JOB_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GitBranchSyncConfig {
    pub enabled: bool,
    pub auto_fork: bool,
}

impl GitBranchSyncConfig {
    pub fn from_env() -> Self {
        let enabled = std::env::var_os("CIS_GIT_BRANCH_SYNC").is_some_and(|v| v == "1");
        let auto_fork = if enabled {
            !std::env::var_os("CIS_GIT_BRANCH_AUTO_FORK").is_some_and(|v| v == "0")
        } else {
            false
        };
        Self { enabled, auto_fork }
    }
}

pub fn git_head_path(repo_root: &Path) -> PathBuf {
    repo_root.join(".git").join("HEAD")
}

pub fn git_merge_head_path(repo_root: &Path) -> PathBuf {
    repo_root.join(".git").join("MERGE_HEAD")
}

pub fn git_merge_in_progress(repo_root: &Path) -> bool {
    git_merge_head_path(repo_root).exists()
}

/// Resolve current git branch name; `None` for detached HEAD or git errors.
pub fn git_abbrev_ref(repo_root: &Path) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let name = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if name.is_empty() || name == "HEAD" {
        return None;
    }
    Some(name)
}

/// Apply git branch name to CIS runtime state (registry + optional fork + active branch).
pub fn apply_git_branch_name(
    repo_root: &Path,
    kv: Arc<MemoryKv>,
    registry: &BranchRegistry,
    active_branch_name: &std::sync::RwLock<String>,
    active_branch_id: &std::sync::RwLock<BranchId>,
    revision_index: &std::sync::RwLock<Arc<RevisionIndexCow>>,
) -> Option<String> {
    if git_merge_in_progress(repo_root) {
        return None;
    }
    let git_name = git_abbrev_ref(repo_root)?;
    let cfg = GitBranchSyncConfig::from_env();
    if !cfg.enabled {
        return Some(git_name);
    }

    let prev_name = active_branch_name.read().unwrap().clone();
    let prev_id = *active_branch_id.read().unwrap();
    let is_new = !registry.is_registered(&git_name);
    let child_id = registry.get_or_create_id(&git_name);

    if cfg.auto_fork && is_new && prev_name != git_name {
        let _ = fork_branch_bindings(kv.as_ref(), prev_id, child_id);
    }

    if prev_name != git_name {
        *active_branch_name.write().unwrap() = git_name.clone();
        *active_branch_id.write().unwrap() = child_id;
        *revision_index.write().unwrap() =
            RevisionIndexCow::root_hydrated(child_id, Arc::clone(&kv));
    }

    Some(git_name)
}

/// Poll `.git/HEAD` mtime and sync when changed (used from fs-sync loop).
pub fn poll_git_head_and_sync(
    repo_root: &Path,
    kv: Arc<MemoryKv>,
    registry: &BranchRegistry,
    active_branch_name: &std::sync::RwLock<String>,
    active_branch_id: &std::sync::RwLock<BranchId>,
    revision_index: &std::sync::RwLock<Arc<RevisionIndexCow>>,
    last_mtime: &mut Option<std::time::SystemTime>,
) {
    if !GitBranchSyncConfig::from_env().enabled {
        return;
    }
    let head = git_head_path(repo_root);
    let Ok(meta) = std::fs::metadata(&head) else {
        return;
    };
    let Ok(mtime) = meta.modified() else {
        return;
    };
    if last_mtime.map(|t| t >= mtime).unwrap_or(false) {
        return;
    }
    *last_mtime = Some(mtime);
    if let Some(name) = apply_git_branch_name(
        repo_root,
        kv,
        registry,
        active_branch_name,
        active_branch_id,
        revision_index,
    ) {
        eprintln!("cis-git-sync: active branch → {name}");
    }
}

pub fn reconciliation_job_id_for_git_sync() -> u64 {
    GIT_SYNC_JOB_SEQ.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, RwLock};

    use crate::branch_registry::BranchRegistry;

    static GIT_SYNC_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn init_git_repo(dir: &Path) {
        Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(dir)
            .output()
            .expect("git init");
        std::fs::write(dir.join("README"), "x").unwrap();
        Command::new("git")
            .args(["add", "README"])
            .current_dir(dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap();
    }

    #[test]
    fn git_abbrev_ref_main() {
        let _lock = GIT_SYNC_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        assert_eq!(git_abbrev_ref(dir.path()).as_deref(), Some("main"));
    }

    #[test]
    fn apply_git_branch_updates_active_and_forks_bindings() {
        use cis_wal::{IdentityId, NodeRevisionId};

        use crate::premerge_bindings_for_branch;
        use crate::revision_index::revision_binding_kv_key;

        let _lock = GIT_SYNC_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::env::set_var("CIS_GIT_BRANCH_SYNC", "1");
        std::env::remove_var("CIS_GIT_BRANCH_AUTO_FORK");

        let kv = Arc::new(MemoryKv::new());
        let reg = BranchRegistry::new(Arc::clone(&kv));
        let main_id = reg.get_or_create_id("main");
        for i in 0..6u8 {
            let identity = IdentityId([i; 16]);
            let revision = NodeRevisionId([i + 50; 16]);
            kv.set(
                &revision_binding_kv_key(main_id, identity),
                revision.0.to_vec(),
            );
        }
        assert_eq!(premerge_bindings_for_branch(kv.as_ref(), main_id).len(), 6);

        let active_name = RwLock::new("main".to_string());
        let active_id = RwLock::new(main_id);
        let ri = RwLock::new(RevisionIndexCow::root_hydrated(main_id, Arc::clone(&kv)));

        let out = Command::new("git")
            .args(["checkout", "-b", "feature"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git checkout -b feature: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(git_abbrev_ref(dir.path()).as_deref(), Some("feature"));

        let name = apply_git_branch_name(
            dir.path(),
            Arc::clone(&kv),
            &reg,
            &active_name,
            &active_id,
            &ri,
        )
        .unwrap();
        assert_eq!(name, "feature");
        assert_eq!(*active_name.read().unwrap(), "feature");

        let feature_id = *active_id.read().unwrap();
        let child_bindings = premerge_bindings_for_branch(kv.as_ref(), feature_id);
        assert!(
            child_bindings.len() >= 6,
            "new git branch should fork parent ri: bindings, got {}",
            child_bindings.len()
        );

        std::env::remove_var("CIS_GIT_BRANCH_SYNC");
    }

    #[test]
    fn apply_git_branch_skips_fork_when_auto_fork_disabled() {
        use cis_wal::{IdentityId, NodeRevisionId};

        use crate::premerge_bindings_for_branch;
        use crate::revision_index::revision_binding_kv_key;

        let _lock = GIT_SYNC_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::env::set_var("CIS_GIT_BRANCH_SYNC", "1");
        std::env::set_var("CIS_GIT_BRANCH_AUTO_FORK", "0");

        let kv = Arc::new(MemoryKv::new());
        let reg = BranchRegistry::new(Arc::clone(&kv));
        let main_id = reg.get_or_create_id("main");
        let identity = IdentityId([9u8; 16]);
        let revision = NodeRevisionId([19u8; 16]);
        kv.set(
            &revision_binding_kv_key(main_id, identity),
            revision.0.to_vec(),
        );

        let active_name = RwLock::new("main".to_string());
        let active_id = RwLock::new(main_id);
        let ri = RwLock::new(RevisionIndexCow::root_hydrated(main_id, Arc::clone(&kv)));

        Command::new("git")
            .args(["checkout", "-b", "feature"])
            .current_dir(dir.path())
            .output()
            .unwrap();

        apply_git_branch_name(
            dir.path(),
            Arc::clone(&kv),
            &reg,
            &active_name,
            &active_id,
            &ri,
        )
        .unwrap();

        let feature_id = *active_id.read().unwrap();
        assert_eq!(
            premerge_bindings_for_branch(kv.as_ref(), feature_id).len(),
            0,
            "auto_fork=0 must not copy parent bindings"
        );

        std::env::remove_var("CIS_GIT_BRANCH_SYNC");
        std::env::remove_var("CIS_GIT_BRANCH_AUTO_FORK");
    }
}
