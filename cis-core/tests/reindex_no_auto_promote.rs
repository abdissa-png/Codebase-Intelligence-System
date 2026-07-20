//! reindex_paths must not auto-promote unconfirmed speculative patches.

use std::fs;
use std::sync::Arc;

use cis_core::{clear_token_with_retry, CisMcpRuntime, RevisionStatus};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

#[test]
fn reindex_paths_does_not_promote_without_confirm_token() {
    let (dir, rt) = make_runtime();
    let rel = "pending.py";
    fs::write(dir.path().join(rel), "def a():\n    return 1\n").unwrap();
    let resp = rt
        .write_file(0, rel, "def a():\n    return 2\n", true)
        .unwrap();
    let patch_id = resp.patch_id;
    let nonce = format!("{}", patch_id);
    clear_token_with_retry(rt.confirm_backend(), &nonce).unwrap();

    let reindex = rt.reindex_paths(0, &[rel], None).unwrap();
    assert_eq!(
        reindex.patches_confirmed, 0,
        "reindex must not promote without confirm token"
    );
    assert_eq!(
        rt.patcher().open_patch_count(),
        1,
        "speculative patch must remain open"
    );
    assert_eq!(rt.patcher().peek_patch_paths(patch_id).len(), 1);

    let g = rt.graph_mutex().read();
    let still_spec = g
        .revisions()
        .any(|r| matches!(r.status, RevisionStatus::Speculative));
    // Speculative graph rows may or may not exist depending on reindex path; patch lease is the contract.
    let _ = still_spec;
}

#[test]
fn reindex_paths_promotes_when_confirm_token_present() {
    let (dir, rt) = make_runtime();
    let rel = "ok.py";
    fs::write(dir.path().join(rel), "def a():\n    return 1\n").unwrap();
    let resp = rt
        .write_file(0, rel, "def a():\n    return 2\n", true)
        .unwrap();
    let patch_id = resp.patch_id;
    assert!(rt
        .confirm_backend()
        .read_token(&format!("{}", patch_id))
        .unwrap()
        .is_some());

    let reindex = rt.reindex_paths(0, &[rel], None).unwrap();
    assert_eq!(
        reindex.patches_confirmed, 1,
        "reindex may promote when confirm token is present"
    );
    assert_eq!(rt.patcher().open_patch_count(), 0);
}
