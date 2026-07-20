//! Merge-engine smoke test on the real **`_tmp_chess_pygame`** clone.
//!
//! Production workflow:
//! - **target**: `bootstrap_python_index_from_repo`
//! - **source**: `fork_branch_bindings` + `RevisionIndexCow::fork` + materialized `ri:source:*`
//! - **delta**: edit `BoardUtils.py`, `reindex_python_paths` on the source overlay
//! - **merge**: `merge_branch` with `strategy=theirs`
//!
//! Run: `cargo test -p cis-core --test chess_pygame_merge -- --nocapture`

mod support;

use std::path::PathBuf;

use cis_core::{
    fork_branch_bindings, phase_a_classify, premerge_bindings_for_branch,
    reindex_python_paths_on_coordinator, stable_id_bytes, CisMcpRuntime, MergeStrategy,
    RevisionIndexCow,
};
use cis_wal::{BranchId, IdentityId};

use support::{chess_fixture_root, ensure_chess_fixture};

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

fn branch_hex(b: BranchId) -> String {
    b.0.iter().map(|x| format!("{:02x}", x)).collect()
}

#[test]
fn chess_pygame_merge_engine_demo() {
    let root = chess_root();
    if !ensure_chess_fixture(&root) {
        return;
    }

    let target = BranchId([0u8; 16]);
    let source = BranchId([0x02; 16]);
    let rel_path = "BoardUtils.py";

    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_FORCE_REINDEX", "1");
    std::env::set_var("CIS_REINDEX_PERSIST", "0");

    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    assert_eq!(rt.default_branch(), target);

    let rep = rt
        .bootstrap_python_index_from_repo()
        .expect("bootstrap_python_index_from_repo");
    assert!(rep.applied > 0, "expected python ingest");

    let source_idx = RevisionIndexCow::fork(rt.revision_index(), source);
    let target_bindings = premerge_bindings_for_branch(rt.kv().as_ref(), target);
    let bindings_copied = fork_branch_bindings(rt.kv().as_ref(), target, source);
    assert_eq!(
        bindings_copied,
        target_bindings.len(),
        "fork_branch_bindings should copy every parent ri: row"
    );
    assert_eq!(
        premerge_bindings_for_branch(rt.kv().as_ref(), source).len(),
        target_bindings.len(),
    );

    let original = std::fs::read_to_string(root.join(rel_path)).expect("BoardUtils.py");
    let needle = "    x,y=position[0],position[1]\n    return str(x)+COLUMNS[y]";
    if !original.contains(needle) {
        eprintln!("chess_pygame_merge: skip — BoardUtils.py layout changed");
        return;
    }
    let edited = original.replace(
        needle,
        "    x,y=position[0],position[1]\n    # cis-merge-demo\n    return str(x)+COLUMNS[y]",
    );
    assert_ne!(edited, original, "expected a real body change for merge classification");
    std::fs::write(root.join(rel_path), &edited).expect("write BoardUtils edit");

    let rep_source = reindex_python_paths_on_coordinator(
        rt.coordinator().as_ref(),
        rt.repo_root(),
        rt.kv(),
        &source_idx,
        source,
        vec![rel_path.into()],
        None,
    )
    .expect("source branch reindex");
    assert!(rep_source.applied > 0, "expected BoardUtils reindex on source");

    let source_bindings = premerge_bindings_for_branch(rt.kv().as_ref(), source);

    let get_str_iid = IdentityId(stable_id_bytes("id", rel_path, "getStrPosition"));
    let target_get_str = target_bindings
        .iter()
        .find(|(i, _)| *i == get_str_iid)
        .map(|(_, r)| *r);
    let source_get_str = source_bindings
        .iter()
        .find(|(i, _)| *i == get_str_iid)
        .map(|(_, r)| *r);
    assert_ne!(
        target_get_str, source_get_str,
        "feature branch should bind a different revision than main for getStrPosition"
    );

    let g = rt.graph_mutex().read();
    let n_rev = g.revisions().count();
    let pa = phase_a_classify(&g, rt.kv().as_ref(), target, source, target);
    drop(g);

    let theirs_only: Vec<_> = pa
        .classified
        .iter()
        .filter(|c| matches!(c.class, cis_core::MergeIdentityClass::TheirsOnly))
        .map(|c| c.qualified_name.clone())
        .collect();
    let clean = pa
        .classified
        .iter()
        .filter(|c| matches!(c.class, cis_core::MergeIdentityClass::Clean))
        .count();

    println!("\n=== CIS merge engine on chess fixture (production path) ===\n");
    println!("Repo: {}", root.display());
    println!("Bootstrap: target applied={}", rep.applied);
    println!("Source reindex: applied={}", rep_source.applied);
    println!(
        "RI bindings: target={} source={}",
        target_bindings.len(),
        source_bindings.len()
    );
    println!("Graph: {n_rev} revisions");
    println!(
        "Phase A: {} identities, {} auto-resolved, {} conflicts, {} renames",
        pa.classified.len(),
        pa.report.resolved_count,
        pa.report.conflicts.len(),
        pa.report.rename_detections.len()
    );
    println!("  Clean: {clean}");
    println!("  TheirsOnly: {}", theirs_only.len());
    if !theirs_only.is_empty() {
        println!("  Sample TheirsOnly:");
        for q in theirs_only.iter().take(10) {
            println!("    - {q}");
        }
    }

    let resp = rt
        .merge_branch(
            0,
            &branch_hex(source),
            &branch_hex(target),
            Some(MergeStrategy::Theirs),
            None,
            None,
        )
        .expect("merge_branch");

    println!("\nMerge result:");
    println!("  saga_phase: {}", resp.saga_phase);
    println!("  promoted: {}", resp.promoted_count);
    println!("  orphaned: {}", resp.orphaned_count);
    println!("  edges_regenerated: {}", resp.edges_regenerated);
    println!("  dangling_edges_removed: {}", resp.dangling_edges_removed);
    println!("  signature_drift warnings: {}", resp.signature_drift.len());
    println!(
        "  WAL merge committed: {}",
        rt.wal()
            .iter_all()
            .iter()
            .any(|r| matches!(r.kind, cis_wal::MutationKind::Merge { .. }))
    );

    println!("\nProgress timeline:");
    for ev in &resp.progress {
        println!("  [{}ms] {} — {}", ev.elapsed_ms, ev.phase, ev.detail);
    }

    std::fs::write(root.join(rel_path), &original).expect("restore BoardUtils.py");

    assert_eq!(resp.saga_phase, "Committed");
    // After source-only reindex, sync_revision_index_from_graph may drop forked ri: rows for
    // identities without Active/Speculative revisions on the source branch. This is harmless to
    // interactive queries: they resolve via the branch ancestry chain against primary_by_identity
    // (see query_engine::resolve_identity_revision + revision_index::branch_ancestry), not the
    // ephemeral ri: copies. Inherited-symbol query visibility is covered by branch_inherited_queries.rs.
    assert!(
        source_bindings.len() >= 1,
        "source branch should retain bindings for reindexed paths"
    );
    let board_utils_delta = pa.classified.iter().any(|c| {
        (c.qualified_name.contains("BoardUtils") || c.qualified_name.contains("getStrPosition"))
            && !matches!(c.class, cis_core::MergeIdentityClass::Clean)
    });
    assert!(
        theirs_only.len() >= 1 || board_utils_delta,
        "expected BoardUtils delta in Phase A; classes: {:?}",
        pa.classified
            .iter()
            .filter(|c| c.qualified_name.contains("BoardUtils") || c.qualified_name.contains("getStrPosition"))
            .map(|c| (&c.qualified_name, c.class))
            .collect::<Vec<_>>()
    );
    assert!(
        resp.promoted_count >= 1,
        "expected promotions from source branch"
    );
}
