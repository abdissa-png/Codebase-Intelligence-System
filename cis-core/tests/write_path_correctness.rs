//! **Phase 0** — write path correctness acceptance tests.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use cis_core::{
    write_token_with_retry, AuthError, CisMcpRuntime, ConfirmTokenBackend, CoordinatorPersistence,
    GraphMutationSet, MergeSagaOrchestrator, RevisionStatus, WriteCoordinator,
};
use cis_wal::{
    BranchId, DurableMutationLog, MutationKind, MutationLogStore, MutationPhase, NodeRevisionId,
};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

// ── (a) write → revert round-trips disk content ─────────────────────────────

#[test]
fn revert_restores_disk_content() {
    let (dir, rt) = make_runtime();
    let rel = "foo.py";
    let original = "def foo():\n    return 1\n";
    let modified = "def foo():\n    return 99\n";
    fs::write(dir.path().join(rel), original).unwrap();

    let write_resp = rt.write_file(0, rel, modified, false).unwrap();
    assert_eq!(fs::read_to_string(dir.path().join(rel)).unwrap(), modified);

    rt.revert_patch(0, write_resp.patch_id).unwrap();
    assert_eq!(
        fs::read_to_string(dir.path().join(rel)).unwrap(),
        original,
        "revert must restore pre-write bytes on disk"
    );
}

#[test]
fn revert_removes_newly_created_file() {
    let (dir, rt) = make_runtime();
    let rel = "brand_new.py";
    let content = "x = 1\n";
    assert!(!dir.path().join(rel).exists());

    let write_resp = rt.write_file(0, rel, content, false).unwrap();
    assert!(dir.path().join(rel).exists());

    rt.revert_patch(0, write_resp.patch_id).unwrap();
    assert!(
        !dir.path().join(rel).exists(),
        "revert of a newly created file should remove it"
    );
}

// ── (b) two open patches on one path — revert only one ──────────────────────

#[test]
fn revert_one_of_two_patches_keeps_speculative_path() {
    let (dir, rt) = make_runtime();
    let rel = "shared.py";
    let v1 = "def a():\n    pass\n";
    let v2 = "def b():\n    pass\n";
    fs::write(dir.path().join(rel), v1).unwrap();

    let patch_a = rt.write_file(0, rel, v1, false).unwrap().patch_id;
    let patch_b = rt.write_file(0, rel, v2, false).unwrap().patch_id;
    assert_ne!(patch_a, patch_b);

    rt.revert_patch(0, patch_a).unwrap();
    assert!(
        rt.has_speculative_path(rel),
        "path must remain speculative while patch_b is still open"
    );
}

// ── (c) crash replay preserves confirmed Active state ─────────────────────

fn temp_cis(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-write-correctness-{}-{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn crash_after_confirm_replay_preserves_active() {
    let repo = temp_cis("confirm-replay");
    let rel = "replay.py";
    let src = "def replay_fn():\n    return 0\n";
    fs::write(repo.join(rel), src).unwrap();

    let patch_id;
    {
        std::env::remove_var("CIS_WAL_MEMORY");
        std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
        let rt = CisMcpRuntime::new_dev(&repo.to_string_lossy());
        let write_resp = rt.write_file(0, rel, src, true).unwrap();
        patch_id = write_resp.patch_id;
        rt.confirm_patch(0, patch_id).unwrap();

        let g = rt.graph_mutex().read();
        let active_before: Vec<_> = g
            .revisions()
            .filter(|r| {
                r.file_path == rel
                    && matches!(r.status, RevisionStatus::Active)
                    && !r.qualified_name.ends_with(".py")
            })
            .collect();
        assert!(!active_before.is_empty(), "expected Active revisions after confirm");
    }

    {
        std::env::remove_var("CIS_WAL_MEMORY");
        std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
        let rt2 = Arc::new(CisMcpRuntime::new_dev(&repo.to_string_lossy()));
        let g = rt2.graph_mutex().read();
        let active_after: Vec<_> = g
            .revisions()
            .filter(|r| {
                r.file_path == rel
                    && matches!(r.status, RevisionStatus::Active)
                    && !r.qualified_name.ends_with(".py")
            })
            .collect();
        assert!(
            !active_after.is_empty(),
            "Active revisions must survive coordinator reopen + reconcile"
        );
    }
}

#[test]
fn crash_at_graph_done_promote_replay_preserves_active() {
    let cis = temp_cis("gd-promote");
    let repo = cis.clone();
    let rel = "gd.py";
    let src = "def gd():\n    return 1\n";
    fs::write(repo.join(rel), src).unwrap();
    fs::create_dir_all(cis.join(".cis")).unwrap();
    let wal_path = cis.join(".cis/wal.json");
    DurableMutationLog::create_new(&wal_path).unwrap();

    let branch = BranchId([0u8; 16]);
    let captured_rids: Vec<NodeRevisionId>;

    {
        std::env::remove_var("CIS_WAL_MEMORY");
        std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
        let rt = CisMcpRuntime::new_dev(&repo.to_string_lossy());
        let patch_id = rt.write_file(0, rel, src, true).unwrap().patch_id;
        captured_rids = rt
            .graph_mutex()
            .read()
            .revisions()
            .filter(|r| r.file_path == rel && matches!(r.status, RevisionStatus::Speculative))
            .map(|r| r.revision_id)
            .collect();
        assert!(!captured_rids.is_empty());

        let set = GraphMutationSet::new(captured_rids.clone(), [7u8; 32]);
        let coord = rt.coordinator();
        let log_id = coord
            .begin_status_mutation(MutationKind::PromoteSpeculative { patch_id }, &set)
            .unwrap();
        coord
            .commit_graph(log_id, |g| {
                for rid in &captured_rids {
                    if let Some(rev) = g.get_revision(*rid).cloned() {
                        let mut updated = rev;
                        updated.status = RevisionStatus::Active;
                        g.put_revision(updated);
                    }
                }
                Ok(())
            })
            .unwrap();
        // Simulate crash before finalize_graph_only.
    }

    let wal: Arc<dyn MutationLogStore> = Arc::new(DurableMutationLog::open(&wal_path).unwrap());
    let promote_row = wal
        .iter_all()
        .into_iter()
        .find(|r| matches!(r.kind, MutationKind::PromoteSpeculative { .. }))
        .expect("PromoteSpeculative WAL row");
    assert_eq!(promote_row.phase, MutationPhase::GraphDone);

    let coord = Arc::new(WriteCoordinator::open(
        Arc::clone(&wal),
        Some(CoordinatorPersistence {
            cis_dir: cis.join(".cis"),
        }),
    ));
    let saga = MergeSagaOrchestrator::new(Arc::new(cis_core::MemoryKv::new()));
    let rep = coord.reconcile_on_startup(&saga);
    assert!(rep.wal_replayed >= 1);
    let after = wal.get(promote_row.log_id).unwrap();
    assert_eq!(after.phase, MutationPhase::Committed);

    let g = coord.graph().read();
    for rid in &captured_rids {
        let rev = g.get_revision(*rid).expect("revision restored");
        assert_eq!(rev.status, RevisionStatus::Active);
        assert_eq!(rev.branch_id, branch);
    }
}

// ── (d) confirm-token write failure surfaces as error ───────────────────────

#[derive(Debug)]
struct FailingConfirmBackend {
    fail_writes: std::sync::atomic::AtomicU32,
}

impl ConfirmTokenBackend for FailingConfirmBackend {
    fn write_token(&self, _nonce: &str, _payload: &[u8]) -> std::io::Result<()> {
        self.fail_writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "injected token write failure",
        ))
    }

    fn read_token(&self, _nonce: &str) -> std::io::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn clear_token(&self, _nonce: &str) -> std::io::Result<()> {
        Ok(())
    }

    fn mode_label(&self) -> &'static str {
        "failing-test"
    }
}

#[test]
fn confirm_token_write_failure_surfaces_error() {
    let backend = FailingConfirmBackend {
        fail_writes: std::sync::atomic::AtomicU32::new(0),
    };
    let err = write_token_with_retry(&backend, "1", b"1").unwrap_err();
    assert!(
        err.to_string().contains("injected")
            || backend
                .fail_writes
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 3
    );
}

#[test]
fn write_file_fails_when_confirm_token_unwritable() {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rel = "token_fail.ts";
    let original = "export const x = 1;\n";
    fs::write(dir.path().join(rel), original).unwrap();

    let backend = Arc::new(Box::new(FailingConfirmBackend {
        fail_writes: std::sync::atomic::AtomicU32::new(0),
    }) as Box<dyn ConfirmTokenBackend>);
    let rt = CisMcpRuntime::new_dev_with_confirm_backend(
        &dir.path().to_string_lossy(),
        backend,
    );

    let err = rt
        .write_file(0, rel, "export const x = 2;\n", false)
        .unwrap_err();
    assert!(
        matches!(err, AuthError::InvalidInput),
        "write_file should fail when confirm token cannot be written"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join(rel)).unwrap(),
        original,
        "disk must be rolled back to pre-write content"
    );
    assert!(
        rt.pending_patch_for_path(rel).is_none(),
        "no orphan patch record should remain"
    );
    let sidecar_count = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".cis_confirm_")
        })
        .count();
    assert_eq!(sidecar_count, 0, "no orphan confirm sidecar should remain");
}

// ── (0.5) TypeScript revert reindexes from restored disk ────────────────────

#[test]
fn revert_ts_file_reindexes_after_write() {
    let (dir, rt) = make_runtime();
    let rel = "app.ts";
    let original = "export function foo(): number {\n  return 1;\n}\n";
    let modified = "export function foo(): number {\n  return 99;\n}\n";
    fs::write(dir.path().join(rel), original).unwrap();

    let write_resp = rt.write_file(0, rel, modified, true).unwrap();
    {
        let g = rt.graph_mutex().read();
        let spec: Vec<_> = g
            .revisions()
            .filter(|r| r.file_path == rel && matches!(r.status, RevisionStatus::Speculative))
            .collect();
        assert!(!spec.is_empty(), "write with reindex should produce speculative revisions");
    }

    rt.revert_patch(0, write_resp.patch_id).unwrap();
    assert_eq!(
        fs::read_to_string(dir.path().join(rel)).unwrap(),
        original,
        "revert must restore disk before reindex"
    );

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
        "no speculative revisions should remain after revert+reindex"
    );
    let active: Vec<_> = g
        .revisions()
        .filter(|r| {
            r.branch_id == branch
                && r.file_path == rel
                && matches!(r.status, RevisionStatus::Active)
                && !r.qualified_name.ends_with(".ts")
        })
        .collect();
    assert!(
        !active.is_empty(),
        "reindex after revert should rebuild Active revisions from restored file"
    );
}
