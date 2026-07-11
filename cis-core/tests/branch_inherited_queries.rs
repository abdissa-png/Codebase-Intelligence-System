//! Inherited symbols visible to interactive queries on feature branches.
//!
//! Covers ancestry-chain resolution (not ephemeral copied `ri:` rows): find_symbol,
//! go_to_definition, find_references, get_dependencies, get_symbol_body, durability
//! after `sync_revision_index_from_graph`, override/nearest-wins, multi-level fork,
//! deletion hiding, and find_symbol_at against a pre-sync RIS snapshot.

use std::fs;
use std::path::PathBuf;

use cis_core::{record_committed_snapshot, CisMcpRuntime, SymbolHit};
use cis_wal::{BranchId, NodeRevisionId};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-branch-inherited-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn parse_branch(hex: &str) -> BranchId {
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    BranchId(b)
}

fn parse_rev(hex: &str) -> NodeRevisionId {
    let mut a = [0u8; 16];
    for i in 0..16 {
        a[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    NodeRevisionId(a)
}

fn names(matches: &[SymbolHit]) -> Vec<String> {
    matches.iter().map(|m| m.qualified_name.clone()).collect()
}

#[test]
fn feature_branch_sees_inherited_and_edited_symbols() {
    let root = temp_repo("vis");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());

    // Two files: edit only a.py on feature so b.py::beta stays a parent revision.
    fs::write(root.join("a.py"), "def alpha():\n    return 1\n").unwrap();
    fs::write(
        root.join("b.py"),
        "from a import alpha\n\ndef beta():\n    return alpha()\n",
    )
    .unwrap();
    rt.reindex_python_paths(&["a.py", "b.py"])
        .expect("ingest main");

    let created = rt.create_branch(0, "feature", Some("main")).unwrap();
    assert!(created.bindings_copied > 0);
    let feature_id = parse_branch(&created.branch_id_hex);
    rt.switch_branch(0, "feature").unwrap();

    rt.write_file(0, "a.py", "def alpha():\n    return 99\n", true)
        .unwrap();

    let found_alpha = rt.find_symbol(0, "alpha", None, 20, false).unwrap();
    assert!(
        names(&found_alpha.matches)
            .iter()
            .any(|n| n.contains("alpha")),
        "edited alpha should be visible: {:?}",
        names(&found_alpha.matches)
    );
    let alpha_hit = found_alpha
        .matches
        .iter()
        .find(|m| m.qualified_name.contains("alpha"))
        .unwrap();

    let found_beta = rt.find_symbol(0, "beta", None, 20, false).unwrap();
    assert!(
        names(&found_beta.matches)
            .iter()
            .any(|n| n.contains("beta")),
        "inherited beta should be visible on feature: {:?}",
        names(&found_beta.matches)
    );
    let beta_hit = found_beta
        .matches
        .iter()
        .find(|m| m.qualified_name.contains("beta"))
        .unwrap();

    let g = rt.graph_mutex().read();
    let alpha_rev = g
        .get_revision(parse_rev(&alpha_hit.revision_id_hex))
        .expect("alpha rev");
    assert_eq!(
        alpha_rev.branch_id, feature_id,
        "edited alpha should live on feature branch"
    );
    let beta_rev = g
        .get_revision(parse_rev(&beta_hit.revision_id_hex))
        .expect("beta rev");
    assert_ne!(
        beta_rev.branch_id, feature_id,
        "untouched beta in b.py should still be the parent revision"
    );
    drop(g);

    let body = rt
        .get_symbol_body(0, None, Some("beta"), None, 512)
        .expect("get_symbol_body inherited beta");
    assert!(
        body.text.contains("alpha"),
        "inherited body should load, got {:?}",
        body.text
    );

    let deps = rt
        .get_dependencies(0, &beta_hit.revision_id_hex, None, 20)
        .expect("get_dependencies");
    assert!(
        deps.hits.iter().any(|h| h.qualified_name.contains("alpha")),
        "beta should depend on alpha: {:?}",
        names(&deps.hits)
    );

    let gtd = rt
        .go_to_definition(0, &beta_hit.revision_id_hex, None)
        .expect("go_to_definition");
    assert!(
        gtd.target
            .as_ref()
            .map(|t| t.qualified_name.contains("alpha"))
            .unwrap_or(false),
        "go_to_definition from beta should reach alpha: {:?}",
        gtd.target
    );

    let refs = rt
        .find_references(0, &alpha_hit.revision_id_hex, None, 20)
        .expect("find_references");
    assert!(
        refs.hits.iter().any(|h| h.qualified_name.contains("beta")),
        "alpha should be referenced by beta: {:?}",
        names(&refs.hits)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn inherited_queries_survive_ri_reconciliation() {
    let root = temp_repo("durable");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    fs::write(root.join("keep.py"), "def keep():\n    return 1\n").unwrap();
    fs::write(root.join("other.py"), "def other():\n    return 2\n").unwrap();
    rt.reindex_python_paths(&["keep.py", "other.py"])
        .expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    // Reindex only keep.py so sync drops inherited ri: rows for other.py.
    rt.write_file(0, "keep.py", "def keep():\n    return 9\n", true)
        .unwrap();
    rt.sync_revision_index_from_graph();

    let feature = rt.active_branch();
    let prefix = format!(
        "ri:{}:",
        feature
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    );
    let ri_count = rt.kv().scan_prefix(&prefix).len();

    let found = rt.find_symbol(0, "other", None, 20, false).unwrap();
    assert!(
        names(&found.matches).iter().any(|n| n.contains("other")),
        "inherited 'other' must remain queryable after ri: GC: {:?} (ri_count={})",
        names(&found.matches),
        ri_count
    );
    let body = rt
        .get_symbol_body(0, None, Some("other"), None, 256)
        .expect("body after sync");
    assert!(body.text.contains("return 2"));

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn multi_level_fork_inherits_through_parent() {
    let root = temp_repo("multilevel");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "m.py";
    fs::write(root.join(rel), "def deep():\n    return 42\n").unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.create_branch(0, "subfeature", Some("feature")).unwrap();
    rt.switch_branch(0, "subfeature").unwrap();

    let found = rt.find_symbol(0, "deep", None, 10, false).unwrap();
    assert!(
        names(&found.matches).iter().any(|n| n.contains("deep")),
        "subfeature should see main symbol through feature: {:?}",
        names(&found.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn tombstoned_symbol_hidden_on_feature() {
    let root = temp_repo("tombstone");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "t.py";
    fs::write(
        root.join(rel),
        "def stay():\n    return 1\n\ndef gone():\n    return 2\n",
    )
    .unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    // Remove `gone` on feature — ingest plants a local tombstone that hides the parent.
    rt.write_file(0, rel, "def stay():\n    return 1\n", true)
        .unwrap();

    let stay = rt.find_symbol(0, "stay", None, 10, false).unwrap();
    assert!(
        names(&stay.matches).iter().any(|n| n.contains("stay")),
        "stay should remain: {:?}",
        names(&stay.matches)
    );

    let gone = rt.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        !names(&gone.matches).iter().any(|n| n.contains("gone")),
        "deleted inherited 'gone' must not appear on feature: {:?}",
        names(&gone.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn find_symbol_at_sees_inherited_overlay_bindings() {
    let root = temp_repo("at");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "a.py";
    fs::write(
        root.join(rel),
        "def one():\n    return 1\n\ndef two():\n    return 2\n",
    )
    .unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    // Snapshot without sync_revision_index_from_graph so copied ri: rows remain.
    let wal_log_id = 42u64;
    let branch = rt.active_branch();
    record_committed_snapshot(
        rt.revision_index().as_ref(),
        rt.kv().as_ref(),
        branch,
        wal_log_id,
        None,
    );

    let at = rt
        .find_symbol_at(0, "two", wal_log_id, None, 20)
        .expect("find_symbol_at");
    assert!(
        names(&at.matches).iter().any(|n| n.contains("two")),
        "find_symbol_at should return inherited overlay binding: {:?}",
        names(&at.matches)
    );

    let _ = fs::remove_dir_all(&root);
}
