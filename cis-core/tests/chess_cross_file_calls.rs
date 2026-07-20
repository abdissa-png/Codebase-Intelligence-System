//! Verify chess repo ingest emits cross-file `Calls` edges (Board → BoardUtils).
//!
//! Requires **`tree-sitter`** for class/loop call resolution on integration tests marked below.
//! Run: `cargo test -p cis-core --features tree-sitter,body-sqlite --test chess_cross_file_calls`

mod support;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use cis_core::{
    apply_index_events, collect_py_files, CisMcpRuntime, EdgeType, FsChangeKind, IndexEvent,
    IndexEventQueue, MemoryKv, WriteCoordinator,
};
use cis_core::MergeSagaOrchestrator;
use cis_wal::{BranchId, MutationLog};

use support::{chess_fixture_root, ensure_chess_fixture};

/// Process-global `CIS_*` env + shared fixture `.cis/` — serialize MCP bootstrap tests.
static CHESS_MCP_LOCK: Mutex<()> = Mutex::new(());

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

#[test]
fn chess_ingest_has_cross_file_calls() {
    let root = chess_root();
    if !ensure_chess_fixture(&root) {
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));

    let mut paths = Vec::new();
    collect_py_files(&root, &mut paths);
    let branch = BranchId([0u8; 16]);
    let events: Vec<IndexEvent> = paths
        .iter()
        .map(|p| {
            let rel = p.strip_prefix(&root).unwrap();
            IndexEvent {
                branch_id: branch,
                path: rel.to_string_lossy().replace('\\', "/"),
                kind: FsChangeKind::Modified,
                old_path: None,
            }
        })
        .collect();
    let root2 = root.clone();
    let rep = apply_index_events(
        &IndexEventQueue::new(),
        &coord,
        kv,
        events,
        move |rel| std::fs::read_to_string(root2.join(rel)),
        None,
        None,
    )
    .expect("ingest");
    assert!(rep.applied > 0);

    let g = coord.graph().read();
    let mut calls = 0usize;
    let mut board_calls_to_utils = 0usize;
    for r in g.revisions() {
        if !r.file_path.contains("Board.py") {
            continue;
        }
        for e in g.outbound_edges(r.revision_id) {
            if e.ty != EdgeType::Calls {
                continue;
            }
            calls += 1;
            let tgt = g
                .primary_revision_for_identity(branch, e.target_identity_id)
                .map(|t| t.qualified_name.clone())
                .unwrap_or_default();
            if tgt.contains("BoardUtils") && tgt.contains("getStr") {
                board_calls_to_utils += 1;
            }
        }
    }
    eprintln!("Board.py Calls edges: {calls}, to BoardUtils helpers: {board_calls_to_utils}");
    assert!(
        calls > 0,
        "expected Calls edges from Board.py after cross-file ingest fix"
    );
    assert!(
        board_calls_to_utils > 0,
        "expected Board.py to call getStrPosition/getPosition in BoardUtils"
    );
}

#[test]
#[cfg(feature = "tree-sitter")]
fn chess_screen_calls_board_initialize() {
    let root = chess_root();
    if !ensure_chess_fixture(&root) {
        return;
    }

    let _guard = CHESS_MCP_LOCK.lock().unwrap();
    std::env::set_var("CIS_FORCE_REINDEX", "1");
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    std::env::remove_var("CIS_GRAPH_BACKEND");

    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let boot = rt
        .bootstrap_python_index_from_repo()
        .expect("full reindex");
    assert!(boot.applied > 0, "ingest: {:?}", boot);
    rt.sync_bodies_after_commit(BranchId([0u8; 16]));
    rt.save_workspace(0).expect("save_workspace");

    let idx = rt.index_status(0).expect("index_status");
    eprintln!(
        "  Indexed: symbols={} edges={} files={} mode={}",
        idx.symbols_indexed, idx.edges_indexed, idx.files_scanned, idx.ingest_mode
    );

    let init = rt
        .find_symbol(0, "Board.initialize", None, 10, false)
        .expect("find_symbol");
    let init_rev = init
        .matches
        .iter()
        .find(|m| m.file_path.contains("Board.py"))
        .map(|m| m.identity_id_hex.clone())
        .expect("Board.initialize in Board.py");

    let callers = rt
        .get_callers(0, &init_rev, None, 20)
        .expect("get_callers");
    eprintln!(
        "Board.initialize callers: {}",
        callers
            .hits
            .iter()
            .map(|h| format!("{} ({})", h.qualified_name, h.file_path))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let from_screen = callers
        .hits
        .iter()
        .any(|h| h.file_path.contains("Screen.py"));
    assert!(
        from_screen,
        "expected Screen.py callers of Board.initialize, got {} hits",
        callers.hits.len()
    );

    let builder = rt
        .find_symbol(0, "Board.builder", None, 10, false)
        .expect("find_symbol");
    let builder_id = builder
        .matches
        .iter()
        .find(|m| m.file_path.contains("Board.py"))
        .map(|m| m.identity_id_hex.clone())
        .expect("Board.builder in Board.py");

    let builder_callers = rt
        .get_callers(0, &builder_id, None, 30)
        .expect("get_callers");
    eprintln!(
        "Board.builder callers: {}",
        builder_callers
            .hits
            .iter()
            .map(|h| format!("{} ({})", h.qualified_name, h.file_path))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let screen_calls_builder = builder_callers
        .hits
        .iter()
        .any(|h| h.file_path.contains("Screen.py") && h.qualified_name.contains("Screen.run"));
    assert!(
        screen_calls_builder,
        "expected Screen.run to call Board.builder via self.board, got {} hits",
        builder_callers.hits.len()
    );

    let on_click = rt
        .find_symbol(0, "on_click", None, 20, false)
        .expect("find_symbol");
    let on_click_id = on_click
        .matches
        .iter()
        .find(|m| m.file_path.contains("Tile.py") && m.qualified_name.contains("ChessTile"))
        .map(|m| m.identity_id_hex.clone())
        .expect("ChessTile.on_click in Tile.py");

    let on_click_callers = rt
        .get_callers(0, &on_click_id, None, 30)
        .expect("get_callers");
    eprintln!(
        "ChessTile.on_click callers: {}",
        on_click_callers
            .hits
            .iter()
            .map(|h| format!("{} ({})", h.qualified_name, h.file_path))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let screen_calls_on_click = on_click_callers
        .hits
        .iter()
        .any(|h| h.file_path.contains("Screen.py") && h.qualified_name.contains("Screen.run"));
    assert!(
        screen_calls_on_click,
        "expected Screen.run to call ChessTile.on_click via loop var, got {} hits",
        on_click_callers.hits.len()
    );

    let g = rt.coordinator().graph().read();
    let mut call_edges = 0usize;
    for r in g.revisions() {
        for e in g.outbound_edges(r.revision_id) {
            if e.ty == EdgeType::Calls {
                call_edges += 1;
            }
        }
    }
    eprintln!("Total Calls edges after full reindex: {call_edges}");
    let min_edges = support::chess_min_edges();
    assert!(
        call_edges >= min_edges,
        "expected at least {min_edges} Calls edges, got {call_edges}"
    );
}

#[test]
fn chess_mcp_queries_after_graph_sync() {
    let root = chess_root();
    if !root.is_dir() {
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));

    let mut paths = Vec::new();
    collect_py_files(&root, &mut paths);
    let branch = BranchId([0u8; 16]);
    let events: Vec<IndexEvent> = paths
        .iter()
        .map(|p| {
            let rel = p.strip_prefix(&root).unwrap();
            IndexEvent {
                branch_id: branch,
                path: rel.to_string_lossy().replace('\\', "/"),
                kind: FsChangeKind::Modified,
                old_path: None,
            }
        })
        .collect();
    let root2 = root.clone();
    apply_index_events(
        &IndexEventQueue::new(),
        &coord,
        kv,
        events,
        move |rel| std::fs::read_to_string(root2.join(rel)),
        None,
        None,
    )
    .expect("ingest");

    // Empty temp repo root — avoid loading large `.cis/kv.json` from the chess clone.
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = CisMcpRuntime::new_dev(&tmp.path().to_string_lossy());
    sync_graph(&rt, &coord);
    rt.sync_revision_index_from_graph();
    rt.sync_index_status_from_graph();

    let idx = rt.index_status(0).unwrap();
    assert!(idx.symbols_indexed > 50, "index_status should reflect graph");

    let find = rt.find_symbol(0, "getStrPosition", None, 5, false).unwrap();
    let rev = find
        .matches
        .iter()
        .find(|m| m.file_path.contains("BoardUtils"))
        .map(|m| m.revision_id_hex.clone())
        .expect("getStrPosition");

    let refs = rt.find_references(0, &rev, None, 30).unwrap();
    eprintln!("find_references(getStrPosition): {} hits", refs.hits.len());
    assert!(
        !refs.hits.is_empty(),
        "Board.py should reference getStrPosition via Calls edges"
    );

    let board_caller = rt
        .find_symbol(0, "calculateMoves", None, 10, false)
        .unwrap()
        .matches
        .into_iter()
        .find(|m| m.file_path.contains("Board.py"))
        .expect("Board.calculateMoves");
    let exp = rt
        .expand_context(0, &board_caller.revision_id_hex, 1, None, usize::MAX)
        .unwrap();
    let names: Vec<_> = exp.hits.iter().map(|h| h.qualified_name.as_str()).collect();
    assert!(
        names.iter().any(|n| n.contains("BoardUtils")),
        "expand_context from Board method should reach BoardUtils (call/import), got {names:?}"
    );
}

fn sync_graph(rt: &CisMcpRuntime, coord: &WriteCoordinator) {
    let src = coord.graph().read();
    let cloned = src.clone_full().expect("clone graph");
    drop(src);
    *rt.graph_mutex().write() = cloned;
}

#[test]
#[cfg(all(feature = "tree-sitter", feature = "body-sqlite"))]
fn chess_graph_sqlite_restart_query_parity() {
    let root = chess_root();
    if !ensure_chess_fixture(&root) {
        return;
    }

    let _guard = CHESS_MCP_LOCK.lock().unwrap();
    // Isolated workspace so parallel chess tests do not fight over fixture `.cis/`.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work = tmp.path().join("chess");
    copy_dir_recursive(&root, &work).expect("copy chess fixture");
    let _ = std::fs::remove_dir_all(work.join(".cis"));

    std::env::set_var("CIS_GRAPH_BACKEND", "sqlite");
    std::env::set_var("CIS_FORCE_REINDEX", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    std::env::remove_var("CIS_WAL_MEMORY");

    let edge_counts = |rt: &CisMcpRuntime| -> (usize, usize) {
        let g = rt.coordinator().graph().read();
        let mut calls = 0usize;
        let mut total = 0usize;
        for r in g.revisions() {
            for e in g.outbound_edges(r.revision_id) {
                total += 1;
                if e.ty == EdgeType::Calls {
                    calls += 1;
                }
            }
        }
        (calls, total)
    };

    let (calls_before, edges_before);
    {
        let rt = CisMcpRuntime::new_dev(&work.to_string_lossy());
        rt.bootstrap_python_index_from_repo()
            .expect("full reindex");
        (calls_before, edges_before) = edge_counts(&rt);
        assert!(calls_before >= 100, "expected Calls edges, got {calls_before}");
        rt.save_workspace(0).expect("save_workspace");
        let minimax = rt.find_symbol(0, "minimax", None, 5, false).expect("find_symbol");
        assert!(
            minimax.matches.iter().any(|m| m.qualified_name.contains("minimax")),
            "minimax should be findable before restart"
        );
    }

    {
        std::env::set_var("CIS_WAL_MEMORY", "1");
        std::env::set_var("CIS_GRAPH_BACKEND", "sqlite");
        std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
        let rt = CisMcpRuntime::new_dev(&work.to_string_lossy());
        let (calls_after, edges_after) = edge_counts(&rt);
        assert_eq!(
            calls_before, calls_after,
            "Calls edge count must match after sqlite graph restart"
        );
        assert_eq!(
            edges_before, edges_after,
            "total edge count must match after sqlite graph restart (no collapse)"
        );
        let minimax = rt.find_symbol(0, "minimax", None, 5, false).expect("find_symbol");
        assert!(
            minimax.matches.iter().any(|m| m.qualified_name.contains("minimax")),
            "minimax query parity after sqlite restart"
        );
        let ctx = rt
            .expand_context(
                0,
                &minimax.matches[0].revision_id_hex,
                1,
                None,
                usize::MAX,
            )
            .expect("expand_context");
        assert!(
            !ctx.hits.is_empty(),
            "expand_context should return neighbors after sqlite restart"
        );
    }

    std::env::remove_var("CIS_GRAPH_BACKEND");
    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::remove_var("CIS_FORCE_REINDEX");
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
}

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            if entry.file_name() == ".git" || entry.file_name() == ".cis" {
                continue;
            }
            copy_dir_recursive(&entry.path(), &to)?;
        } else if ty.is_file() {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}
