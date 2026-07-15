//! **Phase 1** — ingest → persist → simulated restart → query same symbol.

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use cis_core::{
    apply_index_events, cis_dir, open_persisted_coordinator, save_workspace_snapshots,
    FsChangeKind, IndexEvent, IndexEventQueue, MergeSagaOrchestrator, MemoryKv, NodeKind,
    WriteCoordinator,
};
use cis_wal::{BranchId, IdentityId, NodeRevisionId};

/// `CIS_*` backend env vars are process-global; serialize tests that read/write them.
static CIS_ENV_LOCK: Mutex<()> = Mutex::new(());

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-restart-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Restart tests assert Phase-1 disk persistence. Clear integration-test env vars that may
/// be set on the parent `cargo test` command line (e.g. `CIS_SKIP_WORKSPACE_LOAD=1`).
fn clear_cis_integration_test_env() {
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
    std::env::remove_var("CIS_FORCE_REINDEX");
    std::env::remove_var("CIS_SKIP_MERGE_RECOVER");
    std::env::remove_var("CIS_BODY_BACKEND");
    std::env::remove_var("CIS_METADATA_BACKEND");
    std::env::remove_var("CIS_GRAPH_BACKEND");
}

fn find_qualified_name(coord: &WriteCoordinator, needle: &str) -> bool {
    coord
        .graph()
        .read()
        .revisions()
        .any(|r| r.qualified_name.contains(needle))
}

#[test]
fn ingest_survives_simulated_process_restart() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_cis_integration_test_env();
    std::env::remove_var("CIS_WAL_MEMORY");
    let root = temp_repo("survival");
    let py = root.join("hello.py");
    fs::write(
        &py,
        "def greet():\n    return 'hi'\n\ndef other():\n    pass\n",
    )
    .unwrap();

    let branch = BranchId([0u8; 16]);
    let kv = Arc::new(MemoryKv::new());
    let mut bound_identity: Option<IdentityId> = None;

    {
        let coord = open_persisted_coordinator(&root).expect("open coordinator");
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let q = IndexEventQueue::new();
        let rep = apply_index_events(
            &q,
            coord.as_ref(),
            Arc::clone(&kv),
            vec![IndexEvent {
                branch_id: branch,
                path: "hello.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            }],
            |rel| fs::read_to_string(root.join(rel)),
            None,
            None,
        )
        .expect("ingest");
        assert!(rep.applied >= 1, "expected ingest to apply: {:?}", rep);
        assert!(find_qualified_name(coord.as_ref(), "greet"));
        let g = coord.graph().read();
        for r in g.revisions() {
            if r.branch_id == branch {
                let key = cis_core::revision_binding_kv_key(branch, r.identity_id);
                kv.set(&key, r.revision_id.0.to_vec());
                if r.qualified_name.contains("greet") {
                    bound_identity = Some(r.identity_id);
                }
            }
        }
        save_workspace_snapshots(&cis_dir(&root), &g, coord.vector(), kv.as_ref()).unwrap();
    }

    {
        let coord = open_persisted_coordinator(&root).expect("reopen coordinator");
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let rep = coord.reconcile_on_startup(&saga);
        assert_eq!(
            rep.in_flight_wal_records, 0,
            "no dangling WAL after committed ingest"
        );
        assert_eq!(
            coord.mutation_index().lock().unwrap().len(),
            0,
            "MutationIndex empty when WAL fully committed"
        );
        assert!(
            find_qualified_name(coord.as_ref(), "greet"),
            "symbol greet must survive restart"
        );
        let g = coord.graph().read();
        let rev = g
            .revisions()
            .find(|r| r.qualified_name.contains("greet"))
            .expect("greet revision");
        assert_eq!(rev.file_path, "hello.py");
        assert!(
            matches!(
                g.identity_kind(rev.identity_id),
                Some(NodeKind::Function)
            )
        );
        if let Some(i) = bound_identity {
            let key = cis_core::revision_binding_kv_key(branch, i);
            assert!(
                kv.get(&key).is_some(),
                "revision index binding persisted in kv.json"
            );
            let bound = kv.get(&key).unwrap();
            assert_eq!(bound.len(), 16);
            let mut rid_bytes = [0u8; 16];
            rid_bytes.copy_from_slice(&bound);
            assert_eq!(NodeRevisionId(rid_bytes), rev.revision_id);
        }
    }
}

#[test]
fn mcp_bootstrap_skips_reindex_when_snapshot_warm() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_cis_integration_test_env();
    let root = temp_repo("mcp-skip");
    fs::write(root.join("m.py"), "def warm():\n    pass\n").unwrap();

    let branch = BranchId([0u8; 16]);
    let kv = Arc::new(MemoryKv::new());

    {
        let coord = open_persisted_coordinator(&root).unwrap();
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let q = IndexEventQueue::new();
        apply_index_events(
            &q,
            coord.as_ref(),
            Arc::clone(&kv),
            vec![IndexEvent {
                branch_id: branch,
                path: "m.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            }],
            |rel| fs::read_to_string(root.join(rel)),
            None,
            None,
        )
        .unwrap();
        let g = coord.graph().read();
        save_workspace_snapshots(&cis_dir(&root), &g, coord.vector(), kv.as_ref()).unwrap();
    }

    std::env::set_var("CIS_WAL_MEMORY", "1");
    let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rep = rt
        .bootstrap_python_index_from_repo()
        .expect("bootstrap from warm snapshot");
    assert_eq!(rep.applied, 0, "should not re-ingest when snapshot exists");
    let hits = rt
        .find_symbol(0, "warm", None, 32, false)
        .expect("find_symbol");
    assert!(
        !hits.matches.is_empty(),
        "warm symbol queryable after restart bootstrap"
    );
}

#[test]
fn mcp_skip_index_loads_snapshot_only() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_cis_integration_test_env();
    let root = temp_repo("skip-index");
    fs::write(root.join("snap.py"), "def snap_only():\n    pass\n").unwrap();

    let branch = BranchId([0u8; 16]);
    let kv = Arc::new(MemoryKv::new());
    {
        let coord = open_persisted_coordinator(&root).unwrap();
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        apply_index_events(
            &IndexEventQueue::new(),
            coord.as_ref(),
            Arc::clone(&kv),
            vec![IndexEvent {
                branch_id: branch,
                path: "snap.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            }],
            |rel| fs::read_to_string(root.join(rel)),
            None,
            None,
        )
        .unwrap();
        let g = coord.graph().read();
        save_workspace_snapshots(&cis_dir(&root), &g, coord.vector(), kv.as_ref()).unwrap();
    }

    let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
    assert!(
        rt.graph_mutex()
            .read()
            .revisions()
            .any(|r| r.qualified_name.contains("snap_only")),
        "new_dev loads graph.json before bootstrap"
    );
}

#[test]
fn symbol_body_survives_restart_via_cis_bodies() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_cis_integration_test_env();
    std::env::remove_var("CIS_WAL_MEMORY");
    let root = temp_repo("body-survival");
    let py_src = "def executeMove():\n    '''Move a chess piece.'''\n    pass\n";
    fs::write(root.join("game.py"), py_src).unwrap();

    let branch = BranchId([0u8; 16]);

    {
        let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
        rt.reindex_python_paths(&["game.py"]).expect("ingest");
        rt.sync_bodies_after_commit(branch);
        rt.save_workspace(0).expect("persist");
    }

    {
        std::env::set_var("CIS_WAL_MEMORY", "1");
        let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
        let rep = rt.load_persisted_workspace();
        assert!(rep.graph_loaded, "graph must load after restart");
        let body = rt
            .get_symbol_body(0, None, Some("executeMove"), None, 8192)
            .expect("get_symbol_body");
        assert!(
            body.text.contains("executeMove"),
            "body text should contain symbol name: {:?}",
            body.text
        );
        assert!(
            !body.text.contains("(body not available)"),
            "expected non-stub body after restart, source={:?}",
            body.source
        );
        assert!(
            body.text.len() >= 20,
            "expected substantive body, got {} chars",
            body.text.len()
        );
    }
}

#[test]
#[cfg(feature = "body-sqlite")]
fn symbol_body_survives_restart_via_sqlite_bodies() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_cis_integration_test_env();
    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::set_var("CIS_BODY_BACKEND", "sqlite");
    let root = temp_repo("body-sqlite-survival");
    let py_src = "def executeMove():\n    '''Move a chess piece.'''\n    pass\n";
    fs::write(root.join("game.py"), py_src).unwrap();

    let branch = BranchId([0u8; 16]);

    {
        let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
        rt.reindex_python_paths(&["game.py"]).expect("ingest");
        rt.sync_bodies_after_commit(branch);
        rt.save_workspace(0).expect("persist");
    }

    {
        std::env::set_var("CIS_WAL_MEMORY", "1");
        std::env::set_var("CIS_BODY_BACKEND", "sqlite");
        let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
        let rep = rt.load_persisted_workspace();
        assert!(rep.graph_loaded, "graph must load after restart");
        let body = rt
            .get_symbol_body(0, None, Some("executeMove"), None, 8192)
            .expect("get_symbol_body");
        assert!(
            body.text.contains("executeMove"),
            "body text should contain symbol name: {:?}",
            body.text
        );
        assert!(
            !body.text.contains("(body not available)"),
            "expected non-stub body after sqlite restart, source={:?}",
            body.source
        );
    }
    std::env::remove_var("CIS_BODY_BACKEND");
}

#[test]
#[cfg(feature = "body-sqlite")]
fn sqlite_backends_survive_restart() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_cis_integration_test_env();
    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::set_var("CIS_BODY_BACKEND", "sqlite");
    std::env::set_var("CIS_METADATA_BACKEND", "sqlite");
    let root = temp_repo("sqlite-backends");
    let py_src = "def persistMe():\n    '''sqlite survival.'''\n    return 1\n";
    fs::write(root.join("persist.py"), py_src).unwrap();

    {
        let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
        rt.reindex_python_paths(&["persist.py"]).expect("ingest");
        rt.save_workspace(0).expect("persist");
        let cis = cis_core::cis_dir(&root);
        assert!(
            cis_core::store_db_path(&cis).is_file(),
            "store.db should exist after save_workspace with metadata sqlite backend"
        );
        assert!(
            cis_core::bodies_db_path(&cis).is_file(),
            "bodies.db should exist with body sqlite backend"
        );
    }

    {
        std::env::set_var("CIS_WAL_MEMORY", "1");
        std::env::set_var("CIS_BODY_BACKEND", "sqlite");
        std::env::set_var("CIS_METADATA_BACKEND", "sqlite");
        let rt = cis_core::CisMcpRuntime::new_dev(&root.to_string_lossy());
        let rep = rt.load_persisted_workspace();
        assert!(rep.graph_loaded, "graph must load after sqlite restart");
        let hits = rt
            .find_symbol(0, "persistMe", None, 8, false)
            .expect("find_symbol");
        assert!(
            hits.matches.iter().any(|m| m.qualified_name.contains("persistMe")),
            "symbol must survive sqlite body+metadata restart"
        );
        let body = rt
            .get_symbol_body(0, None, Some("persistMe"), None, 8192)
            .expect("get_symbol_body");
        assert!(
            body.text.contains("sqlite survival"),
            "body text should load from sqlite backend: {:?}",
            body.text
        );
    }
    std::env::remove_var("CIS_BODY_BACKEND");
    std::env::remove_var("CIS_METADATA_BACKEND");
}

#[test]
fn body_gc_drops_orphan_hashes() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    use cis_core::{
        gc_bodies_with_store, referenced_body_hashes, BodyStore, FileBodyBlobStore,
    };
    clear_cis_integration_test_env();
    let root = temp_repo("body-gc");
    let kv = Arc::new(MemoryKv::new());
    let bs = BodyStore::new(Arc::clone(&kv));
    let h_keep = cis_core::content_checksum_32("keep");
    let h_drop = cis_core::content_checksum_32("drop");
    bs.put(h_keep, b"keep".to_vec());
    bs.put(h_drop, b"drop".to_vec());
    cis_core::save_body_blob(cis_core::cis_dir(&root), &h_drop, b"drop").unwrap();

    let coord = open_persisted_coordinator(&root).unwrap();
    let branch = BranchId([0u8; 16]);
    fs::write(root.join("x.py"), "def keep():\n    pass\n").unwrap();
    apply_index_events(
        &IndexEventQueue::new(),
        coord.as_ref(),
        Arc::clone(&kv),
        vec![IndexEvent {
            branch_id: branch,
            path: "x.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }],
        |rel| fs::read_to_string(root.join(rel)),
        None,
        None,
    )
    .unwrap();
    let keep = {
        let g = coord.graph().read();
        referenced_body_hashes(&g, branch, true, false)
    };
    let cis = cis_core::cis_dir(&root);
    let store = FileBodyBlobStore::new(&cis);
    let n = gc_bodies_with_store(&store, &cis, &bs, kv.as_ref(), &keep).unwrap();
    assert!(n >= 1);
    assert!(bs.get(&h_drop).is_none());
    assert!(
        cis_core::load_body_blob(&cis, &h_drop)
            .unwrap()
            .is_none()
    );
}
