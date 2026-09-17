//! Dual JSON + SQLite MCP flows (not a clone of every RAM unit test).
//!
//! ```text
//! cargo test -p cis-core --features tree-sitter,body-sqlite --test store_matrix -- --test-threads=1
//! ```

#![cfg(feature = "body-sqlite")]

#[path = "support/store_env.rs"]
mod store_env;

use std::fs;
use std::path::PathBuf;

use cis_core::{cis_dir, CisMcpRuntime, MergeStrategy};
use store_env::{apply_profile, clear_store_env, StoreProfile, CIS_ENV_LOCK};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-store-matrix-{}-{}-{}",
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

fn with_profile(profile: StoreProfile, body: impl FnOnce(StoreProfile, &PathBuf)) {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_store_env();
    apply_profile(profile);
    let root = temp_repo(profile.name());
    body(profile, &root);
    let _ = fs::remove_dir_all(&root);
    clear_store_env();
}

#[test]
fn store_matrix_find_symbol_after_ingest() {
    for profile in StoreProfile::all() {
        with_profile(profile, |profile, root| {
            fs::write(root.join("app.py"), "def persistMe():\n    return 1\n").unwrap();
            let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
            if profile == StoreProfile::Sqlite {
                assert!(rt.kv().is_sqlite_backed());
                assert_eq!(
                    rt.coordinator().graph().read().revision_count(),
                    0,
                    "sqlite overlay empty at boot"
                );
            }
            rt.reindex_python_paths(&["app.py"]).expect("ingest");
            let hits = rt
                .find_symbol(0, "persistMe", None, 8, false)
                .expect("find_symbol");
            assert!(
                hits.matches
                    .iter()
                    .any(|m| m.qualified_name.contains("persistMe")),
                "{} find_symbol missed persistMe: {:?}",
                profile.name(),
                hits.matches
            );
        });
    }
}

#[test]
fn store_matrix_write_revert() {
    for profile in StoreProfile::all() {
        with_profile(profile, |profile, root| {
            let rel = "foo.py";
            let original = "def foo():\n    return 1\n";
            let modified = "def foo():\n    return 99\n";
            fs::write(root.join(rel), original).unwrap();
            let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
            let write_resp = rt.write_file(0, rel, modified, false).unwrap();
            assert_eq!(fs::read_to_string(root.join(rel)).unwrap(), modified);
            rt.revert_patch(0, write_resp.patch_id).unwrap();
            assert_eq!(
                fs::read_to_string(root.join(rel)).unwrap(),
                original,
                "{} revert must restore disk bytes",
                profile.name()
            );
        });
    }
}

#[test]
fn store_matrix_fork_edit_merge() {
    for profile in StoreProfile::all() {
        with_profile(profile, |profile, root| {
            fs::write(root.join("app.py"), "def alpha():\n    return 1\n").unwrap();
            let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
            let report = rt.reindex_python_paths(&["app.py"]).expect("ingest");
            assert!(
                report.applied > 0,
                "{} ingest applied=0 parse_errors={}",
                profile.name(),
                report.parse_errors
            );
            let created = rt.create_branch(0, "feature", Some("main")).unwrap();
            assert!(
                created.bindings_copied > 0,
                "{} fork copied 0 ri bindings",
                profile.name()
            );
            rt.switch_branch(0, "feature").unwrap();
            rt.write_file(0, "app.py", "def alpha():\n    return 99\n", true)
                .unwrap();
            rt.switch_branch(0, "main").unwrap();
            let main_hex: String = rt
                .active_branch()
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let resp = rt
                .merge_branch(
                    0,
                    &created.branch_id_hex,
                    &main_hex,
                    Some(MergeStrategy::Theirs),
                    None,
                    None,
                )
                .expect("merge_branch");
            assert_eq!(resp.saga_phase, "Committed", "{}", profile.name());
            assert!(
                resp.promoted_count >= 1,
                "{} promoted_count={}",
                profile.name(),
                resp.promoted_count
            );
            let on_disk = fs::read_to_string(root.join("app.py")).unwrap();
            assert!(
                on_disk.contains("return 99"),
                "{} working tree after merge: {on_disk}",
                profile.name()
            );
        });
    }
}

#[test]
fn store_matrix_sqlite_unified_file_and_no_json_snapshots() {
    with_profile(StoreProfile::Sqlite, |_, root| {
        fs::write(root.join("app.py"), "def persistMe():\n    return 1\n").unwrap();
        let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
        rt.reindex_python_paths(&["app.py"]).expect("ingest");
        rt.save_workspace(0).expect("save");
        let cis = cis_dir(root);
        assert!(
            cis.join("cis.db").is_file(),
            "full sqlite profile should create .cis/cis.db"
        );
        assert!(
            !cis.join("store.db").exists(),
            "unified layout must not also create store.db"
        );
        assert!(
            !cis.join("graph.db").exists(),
            "unified layout must not also create graph.db"
        );
        assert!(
            !cis.join("kv.json").exists(),
            "sqlite KV must not rewrite kv.json"
        );
        assert!(
            !cis.join("graph.json").exists(),
            "sqlite graph must not rewrite graph.json"
        );
        assert!(
            !cis.join("wal.json").exists(),
            "sqlite WAL must not rewrite wal.json"
        );
        assert!(
            !cis.join("vector.json").exists(),
            "sqlite vectors must not rewrite vector.json"
        );
        let durable = rt.kv().durable_row_count();
        drop(rt);
        let rt2 = CisMcpRuntime::new_dev(&root.to_string_lossy());
        assert!(rt2.kv().is_sqlite_backed());
        assert_eq!(
            rt2.kv().durable_row_count(),
            durable,
            "durable KV must survive in cis.db"
        );
        let hits = rt2
            .find_symbol(0, "persistMe", None, 8, false)
            .expect("find after restart");
        assert!(
            hits.matches
                .iter()
                .any(|m| m.qualified_name.contains("persistMe")),
            "symbol must survive unified sqlite restart"
        );
    });
}
