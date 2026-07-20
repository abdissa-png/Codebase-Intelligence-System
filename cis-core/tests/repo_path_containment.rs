//! resolve_repo_path must not create directories outside the repo before rejecting.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use cis_core::{AuthError, CisMcpRuntime};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

#[test]
fn absolute_outside_repo_is_forbidden_without_creating_dirs() {
    let (_dir, rt) = make_runtime();
    let outside = std::env::temp_dir().join(format!(
        "cis-path-escape-{}-{}",
        std::process::id(),
        "nested_should_not_exist"
    ));
    let marker = outside.join("deep").join("file.py");
    // Ensure parent does not exist yet.
    let _ = fs::remove_dir_all(&outside);

    let err = rt
        .write_file(0, marker.to_str().unwrap(), "x = 1\n", false)
        .unwrap_err();
    assert!(
        matches!(err, AuthError::Forbidden | AuthError::InvalidInput),
        "expected Forbidden, got {:?}",
        err
    );
    assert!(
        !outside.exists(),
        "must not create directories outside the repo before rejecting"
    );
}

#[test]
fn dotdot_path_is_forbidden() {
    let (_dir, rt) = make_runtime();
    let err = rt
        .write_file(0, "../outside.py", "x = 1\n", false)
        .unwrap_err();
    assert!(matches!(err, AuthError::Forbidden));
}

#[test]
fn relative_nested_path_inside_repo_is_ok() {
    let (dir, rt) = make_runtime();
    let rel = "sub/dir/ok.py";
    rt.write_file(0, rel, "x = 1\n", false).unwrap();
    assert!(dir.path().join(rel).exists());
}
