//! Tombstone body hydration across restart + rename detection / consistency.

use std::fs;
use std::path::PathBuf;

use cis_core::{CisMcpRuntime, RevisionStatus};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-tomb-hydrate-{}-{}-{}",
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

#[test]
fn rename_after_restart_uses_hydrated_tombstone_bodies() {
    let root = temp_repo("rename");
    let rel = "m.py";
    let body_v1 = "def compute():\n    x = 1\n    y = 2\n    return x + y\n";
    let body_v2 = "def compute_v2():\n    x = 1\n    y = 2\n    return x + y\n";

    let (identity_before, tomb_hash) = {
        let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
        fs::write(root.join(rel), body_v1).unwrap();
        rt.reindex_python_paths(&[rel]).expect("ingest v1");
        let id = {
            let g = rt.graph_mutex().read();
            let found = g
                .revisions()
                .find(|r| {
                    r.qualified_name.contains("compute")
                        && matches!(r.status, RevisionStatus::Active)
                })
                .map(|r| (r.identity_id, r.body_hash));
            found.expect("compute identity")
        };
        // Tombstone compute by removing it.
        fs::write(root.join(rel), "def other():\n    return 0\n").unwrap();
        rt.reindex_python_paths(&[rel]).expect("tombstone");
        let tomb_hash = {
            let g = rt.graph_mutex().read();
            let found = g
                .revisions()
                .find(|r| r.identity_id == id.0 && matches!(r.status, RevisionStatus::Tombstone))
                .map(|r| r.body_hash);
            found.unwrap_or(id.1)
        };
        assert!(
            rt.body_store().get(&tomb_hash).is_some(),
            "pre-restart BodyStore should hold tombstone body"
        );
        rt.sync_bodies_after_commit(rt.active_branch());
        rt.save_workspace(0).expect("persist");
        (id.0, tomb_hash)
    };

    // Simulated restart: fresh runtime hydrates live + tombstone bodies.
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let rep = rt.load_persisted_workspace();
    assert!(rep.graph_loaded, "graph must load");
    assert!(
        rt.body_store().get(&tomb_hash).is_some(),
        "tombstone body must be hydrated into BodyStore after restart"
    );

    let cons = rt.check_graph_consistency(0).expect("consistency");
    assert!(
        cons.missing_body == 0,
        "tombstone bodies must not appear missing: {}",
        cons.summary
    );

    fs::write(root.join(rel), body_v2).unwrap();
    rt.reindex_python_paths(&[rel]).expect("rename ingest");
    let identity_after = {
        let g = rt.graph_mutex().read();
        let found = g
            .revisions()
            .find(|r| {
                r.qualified_name.contains("compute_v2")
                    && matches!(r.status, RevisionStatus::Active)
            })
            .map(|r| r.identity_id);
        found.expect("compute_v2")
    };
    assert_eq!(
        identity_before, identity_after,
        "rename after restart must reuse tombstone identity via hydrated body"
    );

    let _ = fs::remove_dir_all(&root);
}
