//! FS-sync vs confirm-token race: agent writes must not auto-revert when token is present.

use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cis_core::{
    clear_token_with_retry, flush_debounced_reindex, CisMcpRuntime, FsChangeKind, IndexEvent,
};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

#[test]
fn write_file_emits_token_before_disk_content_is_visible_to_sync() {
    let (dir, rt) = make_runtime();
    let rel = "agent.py";
    fs::write(dir.path().join(rel), "def old():\n    pass\n").unwrap();
    let resp = rt.write_file(0, rel, "def new():\n    pass\n", false).unwrap();
    let nonce = format!("{}", resp.patch_id);
    assert!(
        rt.confirm_backend()
            .read_token(&nonce)
            .unwrap()
            .is_some(),
        "confirm token must exist after successful write_file"
    );
    // Simulate FS event after write: with token present, should promote not revert.
    rt.index_debouncer().schedule(IndexEvent::new(
        rt.active_branch(),
        rel,
        FsChangeKind::Modified,
    ));
    thread::sleep(Duration::from_millis(350));
    flush_debounced_reindex(rt.as_ref());
    assert_eq!(
        rt.patcher().open_patch_count(),
        0,
        "patch should be promoted by FS sync when token is present"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join(rel)).unwrap(),
        "def new():\n    pass\n",
        "disk content must remain agent write after promote"
    );
}

#[test]
fn external_edit_without_token_reverts_pending_patch() {
    let (dir, rt) = make_runtime();
    let rel = "ext.py";
    let original = "def orig():\n    pass\n";
    fs::write(dir.path().join(rel), original).unwrap();
    let resp = rt
        .write_file(0, rel, "def agent():\n    pass\n", false)
        .unwrap();
    let nonce = format!("{}", resp.patch_id);
    clear_token_with_retry(rt.confirm_backend(), &nonce).unwrap();
    assert!(rt
        .confirm_backend()
        .read_token(&nonce)
        .unwrap()
        .is_none());

    fs::write(dir.path().join(rel), "def external():\n    pass\n").unwrap();
    rt.index_debouncer().schedule(IndexEvent::new(
        rt.active_branch(),
        rel,
        FsChangeKind::Modified,
    ));
    thread::sleep(Duration::from_millis(350));
    flush_debounced_reindex(rt.as_ref());
    assert_eq!(
        rt.patcher().open_patch_count(),
        0,
        "pending patch without token must be reverted as external edit"
    );
}
