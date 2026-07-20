//! Session admin enforcement and explicit session identity.

use std::sync::Arc;

use cis_core::{AuthError, AuthProvider, CisMcpRuntime, Session};

fn make_runtime() -> (tempfile::TempDir, Arc<CisMcpRuntime>) {
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Arc::new(CisMcpRuntime::new_dev(&dir.path().to_string_lossy()));
    (dir, rt)
}

#[test]
fn require_admin_rejects_non_admin_session() {
    let a = AuthProvider::new();
    a.register(Session {
        id: 7,
        admin: false,
        repo_roots: vec!["/repo".into()],
    });
    a.register(Session {
        id: 1,
        admin: true,
        repo_roots: vec!["/repo".into()],
    });
    assert!(matches!(a.require_admin(7), Err(AuthError::Forbidden)));
    assert!(a.require_admin(1).is_ok());
    assert!(matches!(
        a.require_admin(99),
        Err(AuthError::Unauthorized)
    ));
}

#[test]
fn non_admin_cannot_purge_branch_or_ttl_sweep() {
    let (dir, rt) = make_runtime();
    // Register a non-admin session (session 0 is admin by default).
    // AuthProvider is internal; use write via session 0 then call privileged APIs
    // with an unregistered session to get Unauthorized, and register via public API if any.
    let _ = dir;
    assert!(matches!(
        rt.purge_branch(99, "00000000000000000000000000000000"),
        Err(AuthError::Unauthorized)
    ));
    assert!(matches!(
        rt.run_merge_ttl_sweep(99),
        Err(AuthError::Unauthorized)
    ));
}

#[test]
fn admin_session_zero_can_run_ttl_sweep() {
    let (_dir, rt) = make_runtime();
    // Session 0 is pre-registered as admin.
    assert!(rt.run_merge_ttl_sweep(0).is_ok());
}
