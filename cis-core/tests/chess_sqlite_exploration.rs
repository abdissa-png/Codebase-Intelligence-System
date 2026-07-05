//! End-to-end SQLite migration + chess repo exploration.
//!
//! Run:
//! ```text
//! cargo test -p cis-core --test chess_sqlite_exploration \
//!   --features tree-sitter,body-sqlite -- --nocapture
//! ```

mod support;

use std::path::PathBuf;

use cis_core::CisMcpRuntime;
use cis_wal::BranchId;

use support::chess_fixture_root;

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

fn sep(title: &str) {
    println!("\n{}", "=".repeat(72));
    println!("  {title}");
    println!("{}", "=".repeat(72));
}

fn human_bytes(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{:.2} MB", n as f64 / 1_048_576.0)
    } else if n >= 1024 {
        format!("{:.1} KB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn file_size(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

#[test]
#[cfg(all(feature = "tree-sitter", feature = "body-sqlite"))]
fn chess_sqlite_migration_and_exploration() {
    let root = chess_root();
    if !root.is_dir() {
        eprintln!("skip: clone chess repo to {}", root.display());
        return;
    }

    // Clean slate for this exploration run.
    let cis = cis_core::cis_dir(&root);
    let _ = std::fs::remove_dir_all(&cis);

    sep("Phase 1 — Bootstrap + persist (file body backend)");
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::remove_var("CIS_FORCE_REINDEX");
    std::env::set_var("CIS_BODY_BACKEND", "file");
    std::env::set_var("CIS_METADATA_BACKEND", "json");

    {
        let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
        let boot = rt
            .bootstrap_python_index_from_repo()
            .expect("bootstrap");
        assert!(boot.applied > 0, "ingest: {:?}", boot);
        rt.sync_bodies_after_commit(BranchId([0u8; 16]));
        rt.save_workspace(0).expect("save_workspace");

        let idx = rt.index_status(0).expect("index_status");
        println!(
            "  Indexed: symbols={} edges={} files={} mode={}",
            idx.symbols_indexed, idx.edges_indexed, idx.files_scanned, idx.ingest_mode
        );

        // Record a time-travel checkpoint so RIS keys exist for migrate-kv.
        if let Some(log_id) = rt.coordinator().wal().iter_all().last().map(|r| r.log_id) {
            rt.record_time_travel_checkpoint(log_id);
        }
        rt.save_workspace(0).expect("save after checkpoint");
    }

    let kv_path = cis_core::kv_snapshot_path(&cis);
    let kv_before = file_size(&kv_path);
    let bodies_dir = cis.join("bodies");
    let body_files = cis_core::walk_body_files_pub(&bodies_dir)
        .map(|it| it.count())
        .unwrap_or(0);
    println!(
        "  Persisted: kv.json={} body_shards={}",
        human_bytes(kv_before),
        body_files
    );

    sep("Phase 2 — migrate-bodies → bodies.db");
    std::env::set_var("CIS_BODY_BACKEND", "sqlite");
    let body_rep = cis_core::migrate_bodies_from_files(&root, false).expect("migrate-bodies");
    let (file_n, sqlite_n) = cis_core::verify_body_migration(&root).expect("verify bodies");
    println!(
        "  migrate-bodies: scanned={} inserted={} verified={}",
        body_rep.files_scanned, body_rep.inserted, body_rep.verified
    );
    println!("  verify: file_shards={file_n} sqlite_rows={sqlite_n}");
    assert!(sqlite_n >= file_n || body_rep.inserted + body_rep.skipped_existing >= file_n);

    let bodies_db = cis_core::bodies_db_path(&cis);
    assert!(bodies_db.is_file(), "bodies.db should exist");
    println!("  bodies.db size: {}", human_bytes(file_size(&bodies_db)));

    sep("Phase 3 — migrate-kv → store.db");
    std::env::set_var("CIS_METADATA_BACKEND", "sqlite");
    let kv_rep = cis_core::migrate_kv_ris_to_sqlite(&root, true).expect("migrate-kv");
    println!(
        "  migrate-kv: ris_migrated={} kv_stripped={}",
        kv_rep.ris_keys_migrated, kv_rep.kv_json_stripped
    );

    let store_db = cis_core::store_db_path(&cis);
    if store_db.is_file() {
        println!("  store.db size: {}", human_bytes(file_size(&store_db)));
    }
    let kv_after = file_size(&kv_path);
    println!(
        "  kv.json: {} → {} ({:.0}% reduction)",
        human_bytes(kv_before),
        human_bytes(kv_after),
        if kv_before > 0 {
            (1.0 - kv_after as f64 / kv_before as f64) * 100.0
        } else {
            0.0
        }
    );

    sep("Phase 4 — Restart with SQLite backends + explore");
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_BODY_BACKEND", "sqlite");
    std::env::set_var("CIS_METADATA_BACKEND", "sqlite");

    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let load = rt.load_persisted_workspace();
    assert!(load.graph_loaded, "graph must load: {:?}", load);
    println!(
        "  Loaded: identities={} revisions={} edges={} kv_entries={}",
        load.graph_identities, load.graph_revisions, load.graph_edges, load.kv_entries
    );

    let idx = rt.index_status(0).expect("index_status");
    println!(
        "  Live index: symbols={} edges={} mode={}",
        idx.symbols_indexed, idx.edges_indexed, idx.ingest_mode
    );

    // --- Exploration scenarios (same workflow as chess_query_engine_eval) ---

    sep("find_symbol(\"getStrPosition\")");
    let find = rt.find_symbol(0, "getStrPosition", None, 10, false).expect("find_symbol");
    assert!(!find.matches.is_empty(), "expected getStrPosition hit");
    for m in &find.matches {
        println!(
            "  {} @ {}:{} conf={:.3}",
            m.qualified_name, m.file_path, m.start_line, m.confidence
        );
    }

    sep("get_symbol_body(\"executeMove\") — SQLite body store");
    let body = rt
        .get_symbol_body(0, None, Some("executeMove"), None, 4096)
        .expect("get_symbol_body");
    println!("  source={:?} len={} truncated={}", body.source, body.text.len(), body.truncated);
    println!("  preview:\n{}", body.text.lines().take(8).collect::<Vec<_>>().join("\n"));
    assert!(
        body.text.contains("executeMove") || body.text.contains("Move"),
        "expected substantive body, got stub?"
    );
    assert!(
        !body.text.contains("(body not available)"),
        "body should resolve via sqlite store after restart"
    );

    sep("semantic_search(\"board position\")");
    let sem = rt
        .semantic_search(0, "position", None, 5)
        .expect("semantic_search");
    for h in sem.hits.iter().take(5) {
        println!("  score={:.4} {}", h.score, h.qualified_name);
    }

    sep("expand_context(getStrPosition, depth=2)");
    if let Some(rev) = find.matches.first() {
        let exp = rt
            .expand_context(0, &rev.revision_id_hex, 2, None, usize::MAX)
            .expect("expand_context");
        println!("  neighborhood hits: {}", exp.hits.len());
        for h in exp.hits.iter().take(6) {
            println!("    {} @ {}", h.qualified_name, h.file_path);
        }
    }

    sep("find_references(\"getPosition\")");
    if let Some(pos) = rt
        .find_symbol(0, "getPosition", None, 5, false)
        .ok()
        .and_then(|r| {
            r.matches
                .into_iter()
                .find(|m| m.file_path.contains("BoardUtils"))
        })
    {
        let refs = rt
            .find_references(0, &pos.revision_id_hex, None, 10)
            .expect("find_references");
        println!("  references: {}", refs.hits.len());
        for h in refs.hits.iter().take(5) {
            println!("    {} @ {}:{}", h.qualified_name, h.file_path, h.start_line);
        }
    }

    sep("Graph edge census");
    {
        let g = rt.graph_mutex().read();
        let mut calls = 0usize;
        let mut imports = 0usize;
        for r in g.revisions() {
            for e in g.outbound_edges(r.revision_id) {
                match e.ty {
                    cis_core::EdgeType::Calls => calls += 1,
                    cis_core::EdgeType::Imports => imports += 1,
                    _ => {}
                }
            }
        }
        println!(
            "  revisions={} Calls={} Imports={}",
            g.revisions().count(),
            calls,
            imports
        );
        assert!(calls > 0, "tree-sitter should produce Calls edges in chess repo");
    }

    sep("Summary — SQLite artifacts");
    println!("  bodies.db:  {} ({sqlite_n} rows)", human_bytes(file_size(&bodies_db)));
    if store_db.is_file() {
        println!("  store.db:   {}", human_bytes(file_size(&store_db)));
    }
    println!("  kv.json:    {}", human_bytes(file_size(&kv_path)));
    println!("  graph.json: {}", human_bytes(file_size(&cis_core::graph_snapshot_path(&cis))));

    std::env::remove_var("CIS_BODY_BACKEND");
    std::env::remove_var("CIS_METADATA_BACKEND");
    std::env::remove_var("CIS_WAL_MEMORY");
}
