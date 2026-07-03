//! **Phase 2.8** — fork → edit → merge without manual `ri:` binding.

use std::fs;
use std::path::PathBuf;

use cis_core::{phase_a_for_merge, CisMcpRuntime, MergeIdentityClass, MergeStrategy};
use cis_wal::{BranchId, MergeId};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-merge-fork-e2e-{}-{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn branch_hex(b: BranchId) -> String {
    b.0.iter().map(|x| format!("{:02x}", x)).collect()
}

fn parse_branch(hex: &str) -> BranchId {
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    BranchId(b)
}

#[test]
fn fork_edit_merge_without_manual_ri_bindings() {
    let root = temp_repo("fork-merge");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());

    let rel = "app.py";
    let initial = "def alpha():\n    return 1\n\ndef beta():\n    return 2\n";
    fs::write(root.join(rel), initial).unwrap();

    rt.reindex_python_paths(&[rel]).expect("ingest main");

    let created = rt.create_branch(0, "feature", Some("main")).unwrap();
    assert!(
        created.bindings_copied > 0,
        "fork should copy parent ri: bindings, got {}",
        created.bindings_copied
    );
    let feature_hex = created.branch_id_hex.clone();
    let feature_id = parse_branch(&feature_hex);

    rt.switch_branch(0, "feature").unwrap();
    let edited = "def alpha():\n    return 99\n\ndef beta():\n    return 2\n";
    rt.write_file(0, rel, edited, true).unwrap();

    rt.switch_branch(0, "main").unwrap();
    let main_id = rt.active_branch();
    let main_hex = branch_hex(main_id);

    let phase_a = {
        let g = rt.graph_mutex().read();
        phase_a_for_merge(
            &g,
            rt.kv().as_ref(),
            MergeId([0u8; 16]),
            main_id,
            feature_id,
            main_id,
            main_id,
        )
    };
    let alpha_class = phase_a
        .classified
        .iter()
        .find(|c| c.qualified_name.contains("alpha"))
        .map(|c| c.class)
        .expect("alpha identity should be classified");
    assert!(
        matches!(
            alpha_class,
            MergeIdentityClass::BothModifiedUnresolved
                | MergeIdentityClass::BothModifiedSame
                | MergeIdentityClass::TheirsOnly
                | MergeIdentityClass::OursDeletedTheirsModified
        ),
        "edited alpha should be classified as modified on feature branch, got {:?}",
        alpha_class
    );

    let resp = rt
        .merge_branch(
            0,
            &feature_hex,
            &main_hex,
            Some(MergeStrategy::Theirs),
            None,
            None,
        )
        .unwrap();

    assert_eq!(resp.saga_phase, "Committed");
    assert!(
        resp.promoted_count >= 1,
        "expected at least one promoted identity, got {}",
        resp.promoted_count
    );
    assert_eq!(resp.needs_edge_regen_count, 0);
    assert!(
        !resp.meta.background_reconciliation_pending,
        "reconciliation should be complete when edge regen backlog is empty"
    );

    let on_disk = fs::read_to_string(root.join(rel)).unwrap();
    assert!(
        on_disk.contains("return 99"),
        "main working tree should reflect merged feature edit"
    );

    let metrics = cis_core::read_merge_metrics(&root.join(".cis"), 5).unwrap();
    assert!(
        metrics.iter().any(|m| m.merge_id_hex == resp.merge_id_hex),
        "merge metrics jsonl should contain merge record"
    );

    let listed = rt.list_merge_metrics(0, 10, None).unwrap();
    assert!(
        listed.records.iter().any(|m| m.merge_id_hex == resp.merge_id_hex),
        "list_merge_metrics MCP path should return the merge record"
    );
    assert!(
        listed.rollup.window_records >= 1,
        "rollup should cover returned merge records"
    );

    let _ = fs::remove_dir_all(&root);
}
