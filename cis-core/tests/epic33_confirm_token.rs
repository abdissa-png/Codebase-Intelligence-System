//! **Epic 3.3** — confirm_token promote/revert integration tests.
//!
//! Tests the speculative → Active promotion flow and the external-edit revert path.

use std::sync::Arc;

use cis_core::{CisMcpRuntime, RevisionStatus};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

// ── Confirm token sidecar roundtrip ─────────────────────────────────────────

#[test]
fn sidecar_token_written_on_write_file() {
    let (dir, rt) = make_runtime();
    let session_id = 0u64;
    let resp = rt.write_file(session_id, "test.py", "def foo(): pass\n", true).unwrap();
    let nonce = format!("{}", resp.patch_id);
    let sidecar = dir.path().join(format!(".cis_confirm_{}", nonce));
    assert!(
        sidecar.exists(),
        "sidecar confirm token should be written after write_file; nonce={nonce}"
    );
}

#[test]
fn write_file_produces_speculative_revisions() {
    let (dir, rt) = make_runtime();
    let src = "def my_func():\n    return 42\n";
    let rel = "work.py";
    let abs = dir.path().join(rel);
    // Pre-create the file so write_file doesn't fail
    std::fs::write(&abs, src).unwrap();
    let _resp = rt.write_file(0, rel, src, true).unwrap();
    let g = rt.graph_mutex().read();
    let branch = rt.default_branch();
    let speculative: Vec<_> = g
        .revisions()
        .filter(|r| r.branch_id == branch && matches!(r.status, RevisionStatus::Speculative))
        .collect();
    assert!(
        !speculative.is_empty(),
        "write_file should produce Speculative revisions, got none"
    );
}

// ── confirm_patch promotes to Active ─────────────────────────────────────────

#[test]
fn confirm_patch_activates_speculative_revisions() {
    let (dir, rt) = make_runtime();
    let rel = "a.py";
    let src = "def alpha():\n    return 1\n";
    std::fs::write(dir.path().join(rel), src).unwrap();
    let write_resp = rt.write_file(0, rel, src, true).unwrap();
    let patch_id = write_resp.patch_id;

    // Verify speculative
    {
        let g = rt.graph_mutex().read();
        let spec: Vec<_> = g
            .revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Speculative))
            .collect();
        assert!(!spec.is_empty(), "expected speculative revisions before confirm");
    }

    let confirm_resp = rt.confirm_patch(0, patch_id).unwrap();
    assert_eq!(confirm_resp.patch_id, patch_id);
    assert!(
        confirm_resp.revisions_activated > 0,
        "confirm_patch should activate at least one revision"
    );

    // Sidecar should be cleaned up
    let sidecar = dir.path().join(format!(".cis_confirm_{}", patch_id));
    assert!(
        !sidecar.exists(),
        "confirm_patch should clear the sidecar token"
    );

    // All revisions should now be Active
    {
        let g = rt.graph_mutex().read();
        let still_spec: Vec<_> = g
            .revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Speculative))
            .collect();
        assert!(
            still_spec.is_empty(),
            "no Speculative revisions should remain after confirm; found {:?}",
            still_spec.iter().map(|r| &r.qualified_name).collect::<Vec<_>>()
        );
    }
}

// ── revert_patch tombstones speculative revisions ────────────────────────────

#[test]
fn revert_patch_tombstones_speculative_revisions() {
    let (dir, rt) = make_runtime();
    let rel = "b.py";
    let src = "def beta():\n    return 2\n";
    std::fs::write(dir.path().join(rel), src).unwrap();
    let write_resp = rt.write_file(0, rel, src, true).unwrap();
    let patch_id = write_resp.patch_id;

    let revert_resp = rt.revert_patch(0, patch_id).unwrap();
    assert_eq!(revert_resp.patch_id, patch_id);

    // Sidecar token cleared
    let sidecar = dir.path().join(format!(".cis_confirm_{}", patch_id));
    assert!(
        !sidecar.exists(),
        "revert_patch should clear the sidecar token"
    );

    // Speculative revisions tombstoned; graph reindexed from restored disk (Phase 0).
    let g = rt.graph_mutex().read();
    let branch = rt.default_branch();
    let speculative: Vec<_> = g
        .revisions()
        .filter(|r| {
            r.branch_id == branch
                && r.file_path == rel
                && matches!(r.status, RevisionStatus::Speculative)
        })
        .collect();
    assert!(
        speculative.is_empty(),
        "no Speculative revisions should remain after revert+reindex"
    );
    let active: Vec<_> = g
        .revisions()
        .filter(|r| {
            r.branch_id == branch
                && r.file_path == rel
                && matches!(r.status, RevisionStatus::Active)
                && !r.qualified_name.ends_with(".py")
        })
        .collect();
    assert!(
        !active.is_empty(),
        "revert with reindex=true should rebuild Active revisions from restored file"
    );
}

// ── TTL orphan sweep ─────────────────────────────────────────────────────────

#[test]
fn sweep_speculative_orphans_reverts_expired() {
    let (dir, rt) = make_runtime();
    let rel = "c.py";
    let src = "def gamma():\n    return 3\n";
    std::fs::write(dir.path().join(rel), src).unwrap();
    rt.write_file(0, rel, src, true).unwrap();

    // Sweep with TTL=0 (everything expired)
    let swept = rt.sweep_speculative_orphans(0);
    assert!(swept > 0, "sweep with ttl=0 should expire all pending patches");

    let g = rt.graph_mutex().read();
    let branch = rt.default_branch();
    let spec_remaining: Vec<_> = g
        .revisions()
        .filter(|r| r.branch_id == branch && matches!(r.status, RevisionStatus::Speculative))
        .collect();
    assert!(
        spec_remaining.is_empty(),
        "no Speculative revisions should remain after TTL sweep"
    );
}
