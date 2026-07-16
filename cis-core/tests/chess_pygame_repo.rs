//! Smoke test: ingest the cloned [Pygame chess](https://github.com/abdissa-png/A-chess-game-using-Pygame) repo
//! and exercise MCP-equivalent queries on `CisMcpRuntime`.
//!
//! Expects the repo at **`cis/fixtures/chess_pygame`**. If missing, tests skip.
//!
//! **Ingest:** without **`tree-sitter`**, only module-level `def` / `async def` are indexed from regex. With **`tree-sitter`**,
//! classes/methods and lightweight call edges are included (**FR-1.1** / §01.7.4).

mod support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cis_core::{
    apply_index_events, stable_id_bytes, stable_rev_id_bytes, CisMcpRuntime, EdgeType, FsChangeKind,
    IndexEvent,
    IndexEventQueue, MemoryKv, MergeSagaOrchestrator, WriteCoordinator,
};
use cis_wal::{BranchId, IdentityId, MutationLog, NodeRevisionId};

use support::chess_fixture_root;

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

fn collect_py_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            collect_py_files(&p, out);
        } else if p.extension().and_then(|x| x.to_str()) == Some("py") {
            out.push(p);
        }
    }
}

fn count_renamed_from_edges(coord: &WriteCoordinator) -> usize {
    let g = coord.graph().read();
    g.revisions()
        .map(|r| {
            g.outbound_edges(r.revision_id)
                .iter()
                .filter(|e| e.ty == EdgeType::RenamedFrom)
                .count()
        })
        .sum()
}

fn ingest_chess_paths(
    coord: &WriteCoordinator,
    kv: Arc<MemoryKv>,
    root: &Path,
    py_paths: &[PathBuf],
) -> cis_core::IngestApplyReport {
    let branch = BranchId([0u8; 16]);
    let q = IndexEventQueue::new();
    let events: Vec<IndexEvent> = py_paths
        .iter()
        .map(|p| {
            let rel = p.strip_prefix(root).unwrap();
            IndexEvent {
                branch_id: branch,
                path: rel.to_string_lossy().replace('\\', "/"),
                kind: FsChangeKind::Modified,
                old_path: None,
            }
        })
        .collect();
    let chess = root.to_path_buf();
    apply_index_events(
        &q,
        coord,
        kv,
        events,
        move |rel| std::fs::read_to_string(chess.join(rel)),
        None,
        None,
    )
    .expect("apply_index_events")
}

/// Copy coordinator graph into MCP dev runtime (separate in-memory graphs today).
fn sync_mcp_graph_from_coordinator(rt: &CisMcpRuntime, coord: &WriteCoordinator) {
    let src = coord.graph().read();
    let cloned = src.clone_full().expect("clone graph");
    drop(src);
    *rt.graph_mutex().write() = cloned;
}

#[test]
fn chess_repo_ingest_creates_top_level_function_nodes() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!(
            "chess_pygame_repo: skip — clone to {}",
            root.display()
        );
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let saga_kv = Arc::new(MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(saga_kv);
    let _ = coord.reconcile_on_startup(&saga);

    let mut py_paths = Vec::new();
    collect_py_files(&root, &mut py_paths);
    py_paths.sort();

    let branch = BranchId([0u8; 16]);
    let q = IndexEventQueue::new();
    let events: Vec<IndexEvent> = py_paths
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

    let chess = root.clone();
    let rep = apply_index_events(
        &q,
        &coord,
        kv,
        events,
        move |rel| std::fs::read_to_string(chess.join(rel)),
        None,
        None,
    )
    .expect("apply_index_events");

    assert!(
        rep.applied > 0,
        "expected some .py files ingested; parse_errors={} skipped_non_py={}",
        rep.parse_errors,
        rep.skipped_non_py
    );

    let g = coord.graph().read();
    let n_rev = g.revisions().count();
    assert!(n_rev > 0, "expected revision nodes");

    let board_py_non_file: usize = g
        .revisions()
        .filter(|r| r.file_path.ends_with("Board.py") && r.qualified_name != "Board.py")
        .count();
    #[cfg(not(feature = "tree-sitter"))]
    assert_eq!(
        board_py_non_file, 0,
        "regex ingest should not add column-0 defs from Board.py (only File hub)"
    );
    #[cfg(feature = "tree-sitter")]
    assert!(
        board_py_non_file > 0,
        "tree-sitter ingest should index Board.py class/method symbols"
    );

    let qnames: HashSet<_> = g.revisions().map(|r| r.qualified_name.clone()).collect();
    assert!(
        qnames.iter().any(|q| q.contains("getStrPosition") || q.contains("getPosition")),
        "expected some BoardUtils-style helpers as qualified_name entries: sample {:?}",
        qnames.iter().take(5).collect::<Vec<_>>()
    );
}

#[test]
fn chess_repo_mcp_find_symbol_after_graph_sync() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!("chess_pygame_repo: skip — clone to {}", root.display());
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let saga_kv = Arc::new(MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(saga_kv);
    let _ = coord.reconcile_on_startup(&saga);

    let mut py_paths = Vec::new();
    collect_py_files(&root, &mut py_paths);

    let branch = BranchId([0u8; 16]);
    let q = IndexEventQueue::new();
    let events: Vec<IndexEvent> = py_paths
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

    let chess = root.clone();
    apply_index_events(
        &q,
        &coord,
        Arc::clone(&kv),
        events,
        move |rel| std::fs::read_to_string(chess.join(rel)),
        None,
        None,
    )
    .unwrap();

    // Empty temp repo — do not load chess `.cis/graph.json` (large, and can stall `new_dev`).
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = CisMcpRuntime::new_dev(&tmp.path().to_string_lossy());
    sync_mcp_graph_from_coordinator(&rt, &coord);
    rt.sync_revision_index_from_graph();
    rt.sync_index_status_from_graph();

    let resp = rt.find_symbol(0, "getStrPosition", None, 20, false).expect("find_symbol");
    assert!(
        !resp.matches.is_empty(),
        "expected hit for getStrPosition in BoardUtils-style code"
    );

    let sem = rt
        .semantic_search(0, "BoardUtils", None, 10)
        .expect("semantic_search");
    assert!(
        !sem.hits.is_empty(),
        "semantic_search should match BoardUtils in qualified_name"
    );

    let piece_rev = rt
        .find_symbol(0, "getPosition", None, 5, false)
        .unwrap()
        .matches
        .first()
        .map(|m| m.revision_id_hex.clone())
        .expect("getPosition revision");
    #[cfg(not(feature = "tree-sitter"))]
    {
        let gtd = rt
            .go_to_definition(0, &piece_rev, None)
            .expect("go_to_definition");
        assert!(
            gtd.target.is_none(),
            "regex ingest has no call edges, so go_to_definition should not resolve"
        );
    }
    #[cfg(feature = "tree-sitter")]
    {
        let _ = rt.go_to_definition(0, &piece_rev, None);
    }
    let expand = rt
        .expand_context(0, &piece_rev, 1, None, usize::MAX)
        .expect("expand_context");
    #[cfg(not(feature = "tree-sitter"))]
    assert_eq!(
        expand.hits.len(),
        1,
        "expand_context depth 1 should return only the seed revision when there are no edges"
    );
    #[cfg(feature = "tree-sitter")]
    {
        let g = coord.graph().read();
        let has_method_symbols = g
            .revisions()
            .filter(|r| r.qualified_name.contains('.'))
            .any(|r| r.file_path.ends_with(".py") && r.qualified_name != r.file_path);
        assert!(
            has_method_symbols,
            "tree-sitter ingest should include class/method symbols in chess repo"
        );
        assert!(
            expand.hits.len() >= 1,
            "expand_context should return at least the seed in tree-sitter mode"
        );
    }
}

/// **Phase 3** — chess repo as a real multi-file corpus: no spurious renames on clean ingest / re-ingest.
#[test]
fn chess_full_ingest_has_no_renamed_from_edges() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!("chess_pygame_repo: skip — clone to {}", root.display());
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
    let _ = coord.reconcile_on_startup(&saga);

    let mut py_paths = Vec::new();
    collect_py_files(&root, &mut py_paths);
    py_paths.sort();

    let rep = ingest_chess_paths(&coord, kv, &root, &py_paths);
    assert!(rep.applied > 0);

    assert_eq!(
        count_renamed_from_edges(&coord),
        0,
        "initial ingest of unchanged chess repo should not emit RENAMED_FROM"
    );
}

#[test]
fn chess_reingest_unchanged_preserves_identities_and_no_renamed_from() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!("chess_pygame_repo: skip — clone to {}", root.display());
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
    let _ = coord.reconcile_on_startup(&saga);

    let mut py_paths = Vec::new();
    collect_py_files(&root, &mut py_paths);
    py_paths.sort();

    ingest_chess_paths(&coord, Arc::clone(&kv), &root, &py_paths);

    let before: HashSet<(String, IdentityId)> = {
        let g = coord.graph().read();
        g.revisions()
            .filter(|r| matches!(r.status, cis_core::RevisionStatus::Active))
            .map(|r| (r.qualified_name.clone(), r.identity_id))
            .collect()
    };

    ingest_chess_paths(&coord, kv, &root, &py_paths);

    let after: HashSet<(String, IdentityId)> = {
        let g = coord.graph().read();
        g.revisions()
            .filter(|r| matches!(r.status, cis_core::RevisionStatus::Active))
            .map(|r| (r.qualified_name.clone(), r.identity_id))
            .collect()
    };

    assert_eq!(
        before, after,
        "unchanged re-ingest should not reassign identities"
    );
    assert_eq!(
        count_renamed_from_edges(&coord),
        0,
        "unchanged re-ingest should not create RENAMED_FROM edges"
    );
}

/// Replace one helper with a clearly unrelated function; sibling symbols must keep identity.
#[test]
fn chess_boardutils_dissimilar_replace_avoids_false_rename() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!("chess_pygame_repo: skip — clone to {}", root.display());
        return;
    }

    let path = "BoardUtils.py";
    let original = std::fs::read_to_string(root.join(path)).expect("BoardUtils.py");
    if !original.contains("def getStrPosition") {
        eprintln!("chess: skip BoardUtils shape changed");
        return;
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
    let _ = coord.reconcile_on_startup(&saga);
    let branch = BranchId([0u8; 16]);
    let q = IndexEventQueue::new();

    let ingest_one = |content: &str| {
        let events = vec![IndexEvent {
            branch_id: branch,
            path: path.into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
                let c = content.to_string();
        apply_index_events(
            &q,
            &coord,
            Arc::clone(&kv),
            events,
            move |_| Ok(c.clone()),
            None,
            None,
        )
        .unwrap();
    };

    ingest_one(&original);

    let iid_get_position = IdentityId(stable_id_bytes("id", path, "getPosition"));
    let get_position_rev = NodeRevisionId(stable_rev_id_bytes(branch, path, "getPosition"));
    assert_eq!(
        coord
            .graph()
            .read()
            .get_revision(get_position_rev)
            .map(|r| r.identity_id),
        Some(iid_get_position)
    );

    let mut edited = original.clone();
    edited = edited.replace(
        "def getStrPosition(position):\n    x,y=position[0],position[1]\n    return str(x)+COLUMNS[y]",
        "def getStrPosition(self, a, b, c):\n    return self.n(a, b, c)",
    );
    assert_ne!(edited, original);

    ingest_one(&edited);

    let g = coord.graph().read();
    assert_eq!(
        g.get_revision(get_position_rev).map(|r| r.identity_id),
        Some(iid_get_position),
        "getPosition identity must not be stolen by unrelated getStrPosition body change"
    );

    let iid_get_str = IdentityId(stable_id_bytes("id", path, "getStrPosition"));
    let str_rev = NodeRevisionId(stable_rev_id_bytes(branch, path, "getStrPosition"));
    let str = g.get_revision(str_rev).expect("getStrPosition revision");
    assert_eq!(
        str.identity_id, iid_get_str,
        "dissimilar replacement should mint a fresh identity for getStrPosition"
    );
    assert!(
        str.rename_source_id.is_none(),
        "should not falsely link getStrPosition to a tombstone"
    );

    let false_renames = g
        .revisions()
        .flat_map(|r| g.outbound_edges(r.revision_id).iter())
        .filter(|e| e.ty == EdgeType::RenamedFrom)
        .filter(|e| e.target_identity_id == iid_get_position)
        .count();
    assert_eq!(
        false_renames, 0,
        "no RENAMED_FROM edge should target getPosition after unrelated edit"
    );
}
