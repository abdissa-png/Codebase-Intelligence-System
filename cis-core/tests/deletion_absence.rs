//! Deletion absence markers (`deleted:{branch}:{identity}`) survive tombstone GC.
//!
//! Covers: mark on delete, hide after overlay removal, clear on recreate, find_references,
//! chain inheritance of absence (no fork copy), and main-branch visibility.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use cis_core::{
    cis_dir, deleted_key, load_kv_snapshot, record_committed_snapshot, DeletionAbsenceStore,
    GcDrainReport, GraphDeleteQueue, RevisionStatus, TombstoneGcWorker, VectorCleanupQueue,
    CisMcpRuntime,
};
use cis_wal::{IdentityId, NodeRevisionId};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-deletion-absence-{}-{}-{}",
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

fn names(hits: &[cis_core::SymbolHit]) -> Vec<String> {
    hits.iter().map(|h| h.qualified_name.clone()).collect()
}

fn parse_identity(hex: &str) -> IdentityId {
    let mut a = [0u8; 16];
    for i in 0..16 {
        a[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    IdentityId(a)
}

/// Run the real tombstone GC worker (`scan_eligible` → `enqueue_deletes` → `drain_batch`).
fn drain_real_tombstone_gc(rt: &CisMcpRuntime) -> GcDrainReport {
    let worker = TombstoneGcWorker::from_policy(&rt.policy_snapshot());
    let policy = rt.policy_snapshot();
    let eligible = worker.scan_eligible(
        rt.coordinator().graph(),
        rt.kv().as_ref(),
        rt.coordinator(),
        &policy,
        u64::MAX,
    );
    let queue = GraphDeleteQueue::new();
    worker.enqueue_deletes(&eligible, &queue, rt.active_branch());
    let vq = VectorCleanupQueue::default();
    worker.drain_batch(
        rt.coordinator().graph(),
        rt.kv().as_ref(),
        &vq,
        &queue,
        eligible.len().max(1),
    )
}

/// Force-delete every Tombstone revision (simulates GC after retention).
fn force_gc_all_tombstones(coord: &cis_core::WriteCoordinator) {
    let ids: Vec<NodeRevisionId> = {
        let g = coord.graph().read();
        g.revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Tombstone))
            .map(|r| r.revision_id)
            .collect()
    };
    let mut g = coord.graph().write();
    for rid in ids {
        g.remove_revision(rid);
    }
}

#[test]
fn deleted_inherited_symbol_stays_hidden_after_tombstone_gc() {
    let root = temp_repo("gc-hide");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "t.py";
    fs::write(
        root.join(rel),
        "def stay():\n    return 1\n\ndef gone():\n    return 2\n",
    )
    .unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest main");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    // Delete `gone` on feature.
    rt.write_file(0, rel, "def stay():\n    return 1\n", true)
        .unwrap();

    let before = rt.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        !names(&before.matches).iter().any(|n| n.contains("gone")),
        "gone must be hidden while tombstone exists: {:?}",
        names(&before.matches)
    );

    // Absence marker must have been written.
    let feature = rt.active_branch();
    let gone_id = {
        let g = rt.coordinator().graph().read();
        let id = g
            .revisions()
            .find(|r| r.qualified_name.contains("gone"))
            .map(|r| r.identity_id)
            .expect("gone identity still in graph (main or tomb)");
        id
    };
    let absence = DeletionAbsenceStore::new(Arc::clone(rt.kv()));
    assert!(
        absence.is_deleted_on_branch(feature, gone_id),
        "expected deleted:{} key",
        deleted_key(feature, gone_id)
    );

    // Simulate tombstone GC — overlays gone, absence remains.
    force_gc_all_tombstones(rt.coordinator());

    let after = rt.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        !names(&after.matches).iter().any(|n| n.contains("gone")),
        "gone must stay hidden after GC via absence KV: {:?}",
        names(&after.matches)
    );

    let stay = rt.find_symbol(0, "stay", None, 10, false).unwrap();
    assert!(
        names(&stay.matches).iter().any(|n| n.contains("stay")),
        "stay must remain visible: {:?}",
        names(&stay.matches)
    );

    // Parent branch still sees gone.
    rt.switch_branch(0, "main").unwrap();
    let on_main = rt.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        names(&on_main.matches).iter().any(|n| n.contains("gone")),
        "main must still see gone: {:?}",
        names(&on_main.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn recreate_clears_absence_and_symbol_returns() {
    let root = temp_repo("recreate");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "r.py";
    fs::write(root.join(rel), "def gone():\n    return 1\n").unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    rt.write_file(0, rel, "x = 0\n", true).unwrap();
    force_gc_all_tombstones(rt.coordinator());
    assert!(
        !names(&rt.find_symbol(0, "gone", None, 10, false).unwrap().matches)
            .iter()
            .any(|n| n.contains("gone"))
    );

    // Recreate on feature — clears absence.
    rt.write_file(0, rel, "def gone():\n    return 9\n", true)
        .unwrap();
    let found = rt.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        names(&found.matches).iter().any(|n| n.contains("gone")),
        "recreated gone must be visible: {:?}",
        names(&found.matches)
    );

    let feature = rt.active_branch();
    let iid = parse_identity(&found.matches[0].identity_id_hex);
    assert!(
        !DeletionAbsenceStore::new(Arc::clone(rt.kv())).is_deleted_on_branch(feature, iid),
        "absence must be cleared on recreate"
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn find_references_skips_caller_deleted_on_feature_after_gc() {
    let root = temp_repo("refs-gc");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    fs::write(root.join("a.py"), "def alpha():\n    return 1\n").unwrap();
    fs::write(
        root.join("b.py"),
        "from a import alpha\n\ndef beta():\n    return alpha()\n",
    )
    .unwrap();
    rt.reindex_python_paths(&["a.py", "b.py"])
        .expect("ingest");

    let alpha_hit = rt
        .find_symbol(0, "alpha", None, 20, false)
        .unwrap()
        .matches
        .into_iter()
        .find(|m| m.qualified_name.contains("alpha"))
        .expect("alpha");

    let refs_main = rt
        .find_references(0, &alpha_hit.revision_id_hex, None, 20)
        .unwrap();
    assert!(
        names(&refs_main.hits).iter().any(|n| n.contains("beta")),
        "main: beta should reference alpha: {:?}",
        names(&refs_main.hits)
    );

    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    // Delete beta (caller) on feature.
    rt.write_file(0, "b.py", "x = 1\n", true).unwrap();
    force_gc_all_tombstones(rt.coordinator());

    let alpha_feat = rt
        .find_symbol(0, "alpha", None, 20, false)
        .unwrap()
        .matches
        .into_iter()
        .find(|m| m.qualified_name.contains("alpha"))
        .expect("alpha on feature");

    let refs_feat = rt
        .find_references(0, &alpha_feat.revision_id_hex, None, 20)
        .unwrap();
    assert!(
        !names(&refs_feat.hits).iter().any(|n| n.contains("beta")),
        "feature after GC must not list deleted beta as a reference: {:?}",
        names(&refs_feat.hits)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn child_branch_inherits_parent_absence_via_chain_walk() {
    let root = temp_repo("chain-abs");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "c.py";
    fs::write(root.join(rel), "def deep():\n    return 1\n").unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();
    rt.write_file(0, rel, "# deleted deep\n", true).unwrap();
    force_gc_all_tombstones(rt.coordinator());

    // Fork from feature — child should also hide deep (chain walk, no copy needed).
    rt.create_branch(0, "sub", Some("feature")).unwrap();
    rt.switch_branch(0, "sub").unwrap();
    let found = rt.find_symbol(0, "deep", None, 10, false).unwrap();
    assert!(
        !names(&found.matches).iter().any(|n| n.contains("deep")),
        "sub must inherit feature deletion via chain: {:?}",
        names(&found.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn durable_prefix_includes_deleted() {
    assert!(
        cis_core::DURABLE_KV_PREFIXES.iter().any(|p| *p == "deleted:"),
        "deleted: must be durable across restart"
    );
}

#[test]
fn real_tombstone_gc_worker_hides_deleted_after_drain() {
    let root = temp_repo("real-gc");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "gc.py";
    fs::write(
        root.join(rel),
        "def stay():\n    return 1\n\ndef gone():\n    return 2\n",
    )
    .unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest main");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    rt.write_file(0, rel, "def stay():\n    return 1\n", true)
        .unwrap();

    let tomb_count_before = {
        let g = rt.coordinator().graph().read();
        g.revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Tombstone))
            .count()
    };
    assert!(
        tomb_count_before > 0,
        "delete should create at least one tombstone overlay"
    );

    let report = drain_real_tombstone_gc(&rt);
    assert!(
        report.deleted_ok > 0,
        "real GC worker should delete tombstones: {:?}",
        report
    );

    let tomb_count_after = {
        let g = rt.coordinator().graph().read();
        g.revisions()
            .filter(|r| matches!(r.status, RevisionStatus::Tombstone))
            .count()
    };
    assert_eq!(
        tomb_count_after, 0,
        "eligible tombstones should be removed from graph"
    );

    let after = rt.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        !names(&after.matches).iter().any(|n| n.contains("gone")),
        "gone must stay hidden after real GC: {:?}",
        names(&after.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn absence_markers_survive_kv_snapshot_reload() {
    let root = temp_repo("kv-roundtrip");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "p.py";
    fs::write(root.join(rel), "def gone():\n    return 1\n").unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();
    rt.write_file(0, rel, "x = 0\n", true).unwrap();
    force_gc_all_tombstones(rt.coordinator());

    let feature = rt.active_branch();
    let gone_id = {
        let g = rt.coordinator().graph().read();
        let rev = g
            .revisions()
            .find(|r| r.qualified_name.contains("gone"))
            .expect("gone identity on main");
        rev.identity_id
    };
    assert!(
        DeletionAbsenceStore::new(Arc::clone(rt.kv())).is_deleted_on_branch(feature, gone_id),
        "marker must exist before persist"
    );

    rt.save_workspace(0).expect("save_workspace");
    let kpath = cis_core::kv_snapshot_path(&cis_dir(&root));
    assert!(kpath.exists(), "kv.json should be written");

    let kv2 = Arc::new(cis_core::MemoryKv::new());
    let loaded = load_kv_snapshot(&kpath, kv2.as_ref()).expect("load kv");
    assert!(loaded > 0, "kv snapshot should contain entries");
    assert!(
        DeletionAbsenceStore::new(Arc::clone(&kv2)).is_deleted_on_branch(feature, gone_id),
        "deleted: marker must survive kv.json round-trip"
    );

    let rt2 = CisMcpRuntime::new_dev(&root.to_string_lossy());
    rt2.switch_branch(0, "feature").unwrap();
    let found = rt2.find_symbol(0, "gone", None, 10, false).unwrap();
    assert!(
        !names(&found.matches).iter().any(|n| n.contains("gone")),
        "reopened runtime must honor persisted absence: {:?}",
        names(&found.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn get_callers_skips_deleted_caller_after_gc() {
    let root = temp_repo("callers-gc");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    fs::write(root.join("a.py"), "def alpha():\n    return 1\n").unwrap();
    fs::write(
        root.join("b.py"),
        "from a import alpha\n\ndef beta():\n    return alpha()\n",
    )
    .unwrap();
    rt.reindex_python_paths(&["a.py", "b.py"])
        .expect("ingest");

    let alpha_hit = rt
        .find_symbol(0, "alpha", None, 20, false)
        .unwrap()
        .matches
        .into_iter()
        .find(|m| m.qualified_name.contains("alpha"))
        .expect("alpha");
    let alpha_id = alpha_hit.identity_id_hex.clone();

    let callers_main = rt.get_callers(0, &alpha_id, None, 20).unwrap();
    assert!(
        names(&callers_main.hits).iter().any(|n| n.contains("beta")),
        "main: beta should call alpha: {:?}",
        names(&callers_main.hits)
    );

    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();
    rt.write_file(0, "b.py", "x = 1\n", true).unwrap();
    drain_real_tombstone_gc(&rt);

    let callers_feat = rt.get_callers(0, &alpha_id, None, 20).unwrap();
    assert!(
        !names(&callers_feat.hits).iter().any(|n| n.contains("beta")),
        "feature after GC must not list deleted beta as caller: {:?}",
        names(&callers_feat.hits)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn same_file_rename_clears_absence_and_stays_visible_after_gc() {
    let root = temp_repo("rename");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "rn.py";
    fs::write(root.join(rel), "def foo():\n    return 1\n").unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest");

    let foo_hit = rt
        .find_symbol(0, "foo", None, 10, false)
        .unwrap()
        .matches
        .into_iter()
        .find(|m| m.qualified_name.contains("foo"))
        .expect("foo before rename");
    let iid = parse_identity(&foo_hit.identity_id_hex);

    rt.write_file(0, rel, "def bar():\n    return 1\n", true)
        .unwrap();

    let bar = rt.find_symbol(0, "bar", None, 10, false).unwrap();
    assert!(
        names(&bar.matches).iter().any(|n| n.contains("bar")),
        "renamed symbol must be visible as bar: {:?}",
        names(&bar.matches)
    );
    let bar_iid = parse_identity(
        &bar
            .matches
            .iter()
            .find(|m| m.qualified_name.contains("bar"))
            .expect("bar hit")
            .identity_id_hex,
    );
    assert_eq!(
        bar_iid, iid,
        "same-file rename should preserve identity id"
    );

    let branch = rt.active_branch();
    assert!(
        !DeletionAbsenceStore::new(Arc::clone(rt.kv())).is_deleted_on_branch(branch, iid),
        "rename must clear stale deleted: marker on recreate"
    );

    drain_real_tombstone_gc(&rt);

    let bar_after = rt.find_symbol(0, "bar", None, 10, false).unwrap();
    assert!(
        names(&bar_after.matches).iter().any(|n| n.contains("bar")),
        "bar must remain visible after tombstone GC: {:?}",
        names(&bar_after.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn find_symbol_at_hides_deleted_inherited_after_gc() {
    let root = temp_repo("at-abs");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rel = "at.py";
    fs::write(
        root.join(rel),
        "def stay():\n    return 1\n\ndef gone():\n    return 2\n",
    )
    .unwrap();
    rt.reindex_python_paths(&[rel]).expect("ingest main");
    rt.create_branch(0, "feature", Some("main")).unwrap();
    rt.switch_branch(0, "feature").unwrap();

    let wal_log_id = 77u64;
    record_committed_snapshot(
        rt.revision_index().as_ref(),
        rt.kv().as_ref(),
        rt.active_branch(),
        wal_log_id,
        None,
    );

    rt.write_file(0, rel, "def stay():\n    return 1\n", true)
        .unwrap();

    let before_gc = rt
        .find_symbol_at(0, "gone", wal_log_id, None, 10)
        .expect("find_symbol_at before gc");
    assert!(
        !names(&before_gc.matches).iter().any(|n| n.contains("gone")),
        "find_symbol_at must honor absence even when overlay still binds parent rev: {:?}",
        names(&before_gc.matches)
    );

    drain_real_tombstone_gc(&rt);

    let after_gc = rt
        .find_symbol_at(0, "gone", wal_log_id, None, 10)
        .expect("find_symbol_at after gc");
    assert!(
        !names(&after_gc.matches).iter().any(|n| n.contains("gone")),
        "find_symbol_at must stay hidden after tombstone GC: {:?}",
        names(&after_gc.matches)
    );

    let stay = rt
        .find_symbol_at(0, "stay", wal_log_id, None, 10)
        .expect("find_symbol_at stay");
    assert!(
        names(&stay.matches).iter().any(|n| n.contains("stay")),
        "non-deleted symbols must still resolve: {:?}",
        names(&stay.matches)
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn tombstone_gc_worker_scan_still_runs_with_absence() {
    // Smoke: worker construction from policy still works; absence does not block GC scan API.
    let root = temp_repo("gc-scan");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let worker = TombstoneGcWorker::from_policy(&rt.policy_snapshot());
    let eligible = worker.scan_eligible(
        rt.coordinator().graph(),
        rt.kv().as_ref(),
        rt.coordinator(),
        &rt.policy_snapshot(),
        u64::MAX,
    );
    let _ = eligible;
    let _ = fs::remove_dir_all(&root);
}
