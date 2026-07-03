//! End-to-end production-style eval on **`_tmp_chess_pygame`**:
//! cold start → query battery → feature branch edit → merge to main → cleanup.
//!
//! Run: `cargo test -p cis-core --test chess_production_workflow --features tree-sitter -- --nocapture`

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cis_core::{
    phase_a_classify, premerge_bindings_for_branch, reindex_python_paths_on_coordinator,
    save_workspace_snapshots, stable_id_bytes, CisMcpRuntime, MergeStrategy, RevisionIndexCow,
};
use cis_core::cis_dir;
use cis_wal::{BranchId, IdentityId};

use support::chess_fixture_root;

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

fn branch_hex(b: BranchId) -> String {
    b.0.iter().map(|x| format!("{:02x}", x)).collect()
}

fn materialize_overlay_bindings(overlay: &RevisionIndexCow) {
    for (identity_id, revision_id) in overlay.resolved_bindings() {
        overlay.bind(identity_id, revision_id);
    }
}

fn purge_branch_kv(kv: &cis_core::MemoryKv, branch: BranchId) {
    let prefix = format!("ri:{}:", branch_hex(branch));
    for (k, _) in kv.scan_prefix(&prefix) {
        kv.delete(&k);
    }
}

fn sep(title: &str) {
    println!("\n{}", "=".repeat(72));
    println!("  {title}");
    println!("{}", "=".repeat(72));
}

fn timed<F, T>(label: &str, f: F) -> T
where
    F: FnOnce() -> T,
{
    let t0 = Instant::now();
    let out = f();
    println!("  [{:.1}ms] {label}", t0.elapsed().as_secs_f64() * 1000.0);
    out
}

#[test]
fn chess_production_instance_query_and_branch_merge() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!("skip: no chess repo at {}", root.display());
        return;
    }

    let main_branch = BranchId([0u8; 16]);
    let feature_a = BranchId([0x02; 16]);
    let feature_b = BranchId([0x03; 16]);
    let rel_board_utils = "BoardUtils.py";

    sep("CIS production workflow — chess fixture");
    println!("Repo: {}", root.display());
    println!(
        "Branches: main={} feature_a={} feature_b={}",
        branch_hex(main_branch),
        branch_hex(feature_a),
        branch_hex(feature_b)
    );

    // Production-like cold start: load `.cis/` if present (no test-only skip flags).
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
    std::env::remove_var("CIS_FORCE_REINDEX");
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_REINDEX_PERSIST", "0");

    let rt = timed("CisMcpRuntime::new_dev (load .cis + recover)", || {
        CisMcpRuntime::new_dev(&root.to_string_lossy())
    });

    let g = rt.graph_mutex().read();
    let rev_count = g.revisions().count();
    let edge_count: usize = g.revisions().map(|r| g.outbound_edges(r.revision_id).len()).sum();
    drop(g);

    if rev_count < 50 {
        sep("Bootstrap (graph empty or stale — full reindex)");
        std::env::set_var("CIS_FORCE_REINDEX", "1");
        let boot = timed("bootstrap_python_index_from_repo", || {
            rt.bootstrap_python_index_from_repo()
                .expect("bootstrap")
        });
        println!("  applied={} parse_errors={}", boot.applied, boot.parse_errors);
        std::env::remove_var("CIS_FORCE_REINDEX");
    } else {
        println!("  Loaded graph: {rev_count} revisions, {edge_count} outbound edges");
    }

    let idx = timed("index_status", || rt.index_status(0).expect("index_status"));
    println!(
        "  symbols={} edges_indexed={} files={} mode={}",
        idx.symbols_indexed, idx.edges_indexed, idx.files_scanned, idx.ingest_mode
    );

    // --- Query quality / usefulness ---
    sep("Query battery (agent-style)");

    let find = timed("find_symbol(getStrPosition)", || {
        rt.find_symbol(0, "getStrPosition", None, 10, false).unwrap()
    });
    println!("  hits={} retrieval_conf={:.3}", find.matches.len(), find.meta.retrieval_confidence);
    for m in find.matches.iter().take(3) {
        println!(
            "    {} @ {}:{} conf={:.3}",
            m.qualified_name, m.file_path, m.start_line, m.confidence
        );
    }
    assert!(!find.matches.is_empty(), "need getStrPosition hit");

    let sem = timed("semantic_search(BoardUtils)", || {
        rt.semantic_search(0, "BoardUtils", None, 8).unwrap()
    });
    println!("  semantic hits={}", sem.hits.len());
    for h in sem.hits.iter().take(3) {
        println!("    score={:.3} {}", h.score, h.qualified_name);
    }

    let board_rev = find
        .matches
        .iter()
        .find(|m| m.file_path.contains("BoardUtils"))
        .map(|m| m.revision_id_hex.clone())
        .expect("BoardUtils hit");

    let refs = timed("find_references(getStrPosition)", || {
        rt.find_references(0, &board_rev, None, 20).unwrap()
    });
    println!("  references={} (expect Board.py callers with Calls edges)", refs.hits.len());
    for h in refs.hits.iter().take(3) {
        println!("    {} @ {}", h.qualified_name, h.file_path);
    }

    if let Some(caller) = rt
        .find_symbol(0, "calculateMoves", None, 5, false)
        .unwrap()
        .matches
        .into_iter()
        .find(|m| m.file_path.contains("Board.py"))
    {
        let exp = timed("expand_context(Board.calculateMoves, depth=1)", || {
            rt.expand_context(0, &caller.revision_id_hex, 1, None, usize::MAX).unwrap()
        });
        println!("  expand_context hits={} pruned={}", exp.hits.len(), exp.meta.pruned_low_confidence_count);
        let names: Vec<_> = exp.hits.iter().map(|h| h.qualified_name.as_str()).collect();
        println!("    neighbors: {names:?}");
    }

    let gtd = timed("go_to_definition(from BoardUtils hit)", || {
        rt.go_to_definition(0, &board_rev, None).unwrap()
    });
    println!(
        "  go_to_definition target={}",
        gtd.target
            .as_ref()
            .map(|t| t.qualified_name.as_str())
            .unwrap_or("(none)")
    );

    // --- Feature branch A: edit BoardUtils ---
    sep("Feature branch A — fork, edit, reindex");

    let original = std::fs::read_to_string(root.join(rel_board_utils)).expect("BoardUtils.py");
    let needle = "    x,y=position[0],position[1]\n    return str(x)+COLUMNS[y]";
    if !original.contains(needle) {
        eprintln!("skip merge section: BoardUtils.py layout changed");
        return;
    }

    let edit_a = original.replace(
        needle,
        "    x,y=position[0],position[1]\n    # cis-feature-a\n    return str(x)+COLUMNS[y]",
    );
    let edit_b = original.replace(
        needle,
        "    x,y=position[0],position[1]\n    # cis-feature-b\n    return str(x)+COLUMNS[y]",
    );

    let idx_a = RevisionIndexCow::fork(rt.revision_index(), feature_a);
    materialize_overlay_bindings(&idx_a);

    std::fs::write(root.join(rel_board_utils), &edit_a).expect("write feature-a");
    let rep_a = timed("reindex feature_a (BoardUtils)", || {
        reindex_python_paths_on_coordinator(
            rt.coordinator().as_ref(),
            rt.repo_root(),
            rt.kv(),
            &idx_a,
            feature_a,
            vec![rel_board_utils.into()],
            None,
        )
        .expect("reindex a")
    });
    println!("  feature_a reindex applied={}", rep_a.applied);

    // Feature branch B: alternate edit (same file, different body) on overlay only
    let idx_b = RevisionIndexCow::fork(rt.revision_index(), feature_b);
    materialize_overlay_bindings(&idx_b);
    std::fs::write(root.join(rel_board_utils), &edit_b).expect("write feature-b");
    let rep_b = timed("reindex feature_b (BoardUtils)", || {
        reindex_python_paths_on_coordinator(
            rt.coordinator().as_ref(),
            rt.repo_root(),
            rt.kv(),
            &idx_b,
            feature_b,
            vec![rel_board_utils.into()],
            None,
        )
        .expect("reindex b")
    });
    println!("  feature_b reindex applied={}", rep_b.applied);

    let get_str_iid = IdentityId(stable_id_bytes("id", rel_board_utils, "getStrPosition"));
    let main_rev = premerge_bindings_for_branch(rt.kv().as_ref(), main_branch)
        .iter()
        .find(|(i, _)| *i == get_str_iid)
        .map(|(_, r)| *r);
    let a_rev = premerge_bindings_for_branch(rt.kv().as_ref(), feature_a)
        .iter()
        .find(|(i, _)| *i == get_str_iid)
        .map(|(_, r)| *r);
    let b_rev = premerge_bindings_for_branch(rt.kv().as_ref(), feature_b)
        .iter()
        .find(|(i, _)| *i == get_str_iid)
        .map(|(_, r)| *r);
    println!(
        "  getStrPosition revisions: main={:?} feature_a={:?} feature_b={:?}",
        main_rev, a_rev, b_rev
    );
    assert_ne!(main_rev, a_rev, "feature_a should differ from main");

    let g = rt.graph_mutex().read();
    let pa = phase_a_classify(&g, rt.kv().as_ref(), main_branch, feature_a, main_branch);
    drop(g);
    println!(
        "  Phase A (main ← feature_a): conflicts={} resolved={} theirs_only={}",
        pa.report.conflicts.len(),
        pa.report.resolved_count,
        pa.classified
            .iter()
            .filter(|c| matches!(c.class, cis_core::MergeIdentityClass::TheirsOnly))
            .count()
    );

    sep("Merge feature_a → main (strategy=theirs)");
    let merge_resp = timed("merge_branch(feature_a → main)", || {
        rt.merge_branch(
            0,
            &branch_hex(feature_a),
            &branch_hex(main_branch),
            Some(MergeStrategy::Theirs),
            None,
            None,
        )
        .expect("merge")
    });
    println!(
        "  saga={} promoted={} edges_regen={} dangling_removed={}",
        merge_resp.saga_phase,
        merge_resp.promoted_count,
        merge_resp.edges_regenerated,
        merge_resp.dangling_edges_removed
    );
    assert_eq!(merge_resp.saga_phase, "Committed");
    assert!(merge_resp.promoted_count >= 1);

    // Post-merge query: file on disk still has feature-b text until we restore; graph should reflect merge
    let post = timed("find_symbol after merge", || {
        rt.find_symbol(0, "getStrPosition", None, 5, false).unwrap()
    });
    println!("  post-merge hits={}", post.matches.len());

    // --- Cleanup: restore disk, purge feature branches, reindex main ---
    sep("Cleanup — restore master/main, drop feature branches");

    std::fs::write(root.join(rel_board_utils), &original).expect("restore BoardUtils.py");
    purge_branch_kv(rt.kv().as_ref(), feature_a);
    purge_branch_kv(rt.kv().as_ref(), feature_b);
    println!("  purged ri:* for feature_a and feature_b");

    let root_idx = rt.revision_index();
    let rep_main = timed("reindex main (BoardUtils restore)", || {
        reindex_python_paths_on_coordinator(
            rt.coordinator().as_ref(),
            rt.repo_root(),
            rt.kv(),
            &root_idx,
            main_branch,
            vec![rel_board_utils.into()],
            None,
        )
        .expect("reindex main")
    });
    println!("  main reindex applied={}", rep_main.applied);

    let g = rt.graph_mutex().read();
    let calls_from_board: usize = g
        .revisions()
        .filter(|r| r.file_path.contains("Board.py"))
        .map(|r| {
            g.outbound_edges(r.revision_id)
                .iter()
                .filter(|e| e.ty == cis_core::EdgeType::Calls)
                .count()
        })
        .sum();
    drop(g);
    println!("  Board.py Calls edges (cross-file quality): {calls_from_board}");

    let cis = cis_dir(&root);
    if cis.is_dir() {
        let g = rt.graph_mutex().read();
        let _ = save_workspace_snapshots(&cis, &g, rt.vector_store(), rt.kv().as_ref());
        drop(g);
        println!("  persisted snapshots to {}", cis.display());
    }

    sep("Summary");
    println!("  Cold start + queries: OK");
    println!("  Cross-file references: {} (tree-sitter Calls)", refs.hits.len());
    println!("  Branch fork + dual feature overlays + merge to main: OK");
    println!("  Cleanup: disk restored, feature branch KV purged, main reindexed");
    println!("  Note: CIS \"main\" is BranchId zeros (master equivalent), not git branch metadata.");
}
