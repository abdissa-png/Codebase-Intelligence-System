//! Lease / speculative-patch lifecycle regression tests.
//! Covers partial-acquire release and write/apply_patch rollback on failure.

use std::fs;
use std::sync::Arc;

use cis_core::{AuthError, CisMcpRuntime, OptimisticPatcher, PathLeaseManager, SessionId, SpeculativePathTracker};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

#[test]
fn partial_acquire_conflict_releases_earlier_leases() {
    let leases = Arc::new(PathLeaseManager::new());
    let spec = Arc::new(SpeculativePathTracker::new());
    let patcher = OptimisticPatcher::new(Arc::clone(&leases), Arc::clone(&spec));
    leases.acquire("b.rs", SessionId(99)).unwrap();
    let err = patcher
        .apply_speculative(SessionId(1), vec!["b.rs".into(), "a.rs".into()], None)
        .unwrap_err();
    assert!(matches!(err, cis_core::OptimisticPatchError::Lease(_)));
    assert!(leases.holder("a.rs").is_none());
    assert_eq!(leases.holder("b.rs"), Some(SessionId(99)));
    assert_eq!(patcher.open_patch_count(), 0);
    assert!(leases.active_paths().iter().all(|(p, _)| p == "b.rs"));
}

#[test]
fn apply_patch_bad_diff_releases_leases() {
    let (dir, rt) = make_runtime();
    let rel = "bad_diff.py";
    fs::write(dir.path().join(rel), "def a():\n    return 1\n").unwrap();

    // Looks like a unified diff but does not apply cleanly.
    let bad = "--- a/bad_diff.py\n+++ b/bad_diff.py\n@@ -1,2 +1,2 @@\n-def totally_different():\n-    return 0\n+def a():\n+    return 2\n";
    let err = rt.apply_patch(0, rel, bad, false).unwrap_err();
    assert!(matches!(err, AuthError::InvalidInput));
    assert!(
        rt.leases().is_empty(),
        "leases must be released after bad-diff apply_patch failure"
    );
    assert_eq!(rt.patcher().open_patch_count(), 0);
    assert_eq!(
        fs::read_to_string(dir.path().join(rel)).unwrap(),
        "def a():\n    return 1\n",
        "disk content must be unchanged"
    );
}

#[test]
fn write_file_fs_failure_releases_leases() {
    let (dir, rt) = make_runtime();
    // Target path is an existing directory — fs::write must fail.
    let rel = "not_a_file";
    fs::create_dir_all(dir.path().join(rel)).unwrap();

    let err = rt.write_file(0, rel, "x = 1\n", false).unwrap_err();
    assert!(
        matches!(err, AuthError::InvalidInput | AuthError::Forbidden),
        "expected write failure, got {:?}",
        err
    );
    assert!(
        rt.leases().is_empty(),
        "leases must be empty after write_file failure"
    );
    assert_eq!(rt.patcher().open_patch_count(), 0);
}
