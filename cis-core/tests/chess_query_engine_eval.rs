//! Real-world query-engine evaluation on the chess fixture repo.
//!
//! Simulates an agent workflow: find symbol → go to definition → expand context → callers/deps.
//!
//! Run: `cargo test -p cis-core --test chess_query_engine_eval -- --nocapture`

mod support;

use std::path::PathBuf;

use cis_core::CisMcpRuntime;

use support::{chess_fixture_root, ensure_chess_fixture};

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

fn sep(title: &str) {
    println!("\n{}", "=".repeat(72));
    println!("  {title}");
    println!("{}", "=".repeat(72));
}

fn print_hit(label: &str, h: &cis_core::SymbolHit) {
    println!(
        "  {label}: {} @ {}:{} (conf={:.3}, rev={}…)",
        h.qualified_name,
        h.file_path,
        h.start_line,
        h.confidence,
        &h.revision_id_hex[..8]
    );
}

fn print_meta(tool: &str, meta: &cis_core::QueryMeta) {
    println!(
        "  meta[{tool}]: nodes={} pruned={} retrieval_conf={:.3} policy={} mode={}",
        meta.node_count,
        meta.pruned_low_confidence_count,
        meta.retrieval_confidence,
        meta.policy_version,
        meta.ingest_mode
    );
    if !meta.degraded_modes.is_empty() {
        println!("  degraded: {:?}", meta.degraded_modes);
    }
}

#[test]
fn chess_query_engine_real_world_eval() {
    let root = chess_root();
    if !ensure_chess_fixture(&root) {
        return;
    }

    let features = if cfg!(feature = "tree-sitter") {
        "tree-sitter"
    } else {
        "regex (default)"
    };

    sep(&format!("CIS Query Engine eval — {} / ingest={features}", root.display()));

    // Skip loading chess `.cis/*` snapshots (can stall under parallel `cargo test`); bootstrap in-process.
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_FORCE_REINDEX", "1");
    std::env::set_var("CIS_REINDEX_PERSIST", "0");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let boot = rt
        .bootstrap_python_index_from_repo()
        .expect("bootstrap_python_index_from_repo");
    assert!(boot.applied > 0, "expected python ingest: {:?}", boot);
    let idx = rt.index_status(0).expect("index_status");
    println!(
        "Index: symbols={} edges={} files={} mode={}",
        idx.symbols_indexed,
        idx.edges_indexed,
        idx.files_scanned,
        idx.ingest_mode
    );

    // --- Scenario 1: "Where is getStrPosition?" (navigation start) ---
    sep("1. find_symbol(\"getStrPosition\") — locate helper");
    let find = rt.find_symbol(0, "getStrPosition", None, 10, false).expect("find_symbol");
    print_meta("find_symbol", &find.meta);
    if find.matches.is_empty() {
        println!("  FAIL: no matches (expected BoardUtils.getStrPosition)");
    } else {
        for (i, m) in find.matches.iter().enumerate() {
            print_hit(&format!("#{i}"), m);
        }
    }

    let seed_rev = find
        .matches
        .iter()
        .find(|m| m.file_path.contains("BoardUtils"))
        .or_else(|| find.matches.first())
        .map(|m| m.revision_id_hex.clone());

    // --- Scenario 2: semantic_search (agent discovery) ---
    sep("2. semantic_search(\"board position\") — discovery");
    let sem = rt
        .semantic_search(0, "position", None, 8)
        .expect("semantic_search");
    print_meta("semantic_search", &sem.meta);
    for (i, h) in sem.hits.iter().take(5).enumerate() {
        println!(
            "  #{}: score={:.4} {} rev={}…",
            i,
            h.score,
            h.qualified_name,
            &h.revision_id_hex[..8]
        );
    }
    if sem.hits.is_empty() {
        println!("  WARN: no semantic hits for 'position'");
    }

    // --- Scenario 3: go_to_definition from a call site ---
    sep("3. go_to_definition — jump to callee");
    if let Some(caller) = rt
        .find_symbol(0, "Board", None, 5, false)
        .ok()
        .and_then(|r| r.matches.into_iter().find(|m| m.qualified_name.contains("Board")))
    {
        println!("  seed caller: {}", caller.qualified_name);
        let gtd = rt
            .go_to_definition(0, &caller.revision_id_hex, None)
            .expect("go_to_definition");
        print_meta("go_to_definition", &gtd.meta);
        if let Some(t) = &gtd.target {
            print_hit("target", t);
        } else {
            let why = rt
                .why_no_definition(0, &caller.revision_id_hex, None)
                .expect("why_no_definition");
            println!("  no target — reasons: {:?}", why.reasons);
        }
    } else {
        println!("  skip: Board class revision not found");
    }

    // --- Scenario 4: expand_context from BoardUtils helper ---
    sep("4. expand_context(depth=2) — local neighborhood");
    if let Some(rev_hex) = seed_rev.clone() {
        let exp = rt
            .expand_context(0, &rev_hex, 2, None, usize::MAX)
            .expect("expand_context");
        print_meta("expand_context", &exp.meta);
        println!("  hits ({}):", exp.hits.len());
        for h in &exp.hits {
            print_hit("  ", h);
        }
        if exp.hits.len() <= 1 {
            println!(
                "  NOTE: only seed node — graph may lack Calls/Uses edges (Imports-only ingest)"
            );
        }
    }

    // --- Scenario 5: get_dependencies on BoardUtils file hub ---
    sep("5. get_dependencies — what BoardUtils imports");
    if let Some(rev_hex) = seed_rev.clone() {
        let deps = rt
            .get_dependencies(0, &rev_hex, None, usize::MAX)
            .expect("get_dependencies");
        print_meta("get_dependencies", &deps.meta);
        for h in deps.hits.iter().take(8) {
            print_hit("dep", h);
        }
        if deps.hits.is_empty() {
            println!("  WARN: no import dependencies resolved");
        }
    }

    // --- Scenario 6: find_references on getPosition ---
    sep("6. find_references(\"getPosition\") — who uses it?");
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
            .find_references(0, &pos.revision_id_hex, None, 15)
            .expect("find_references");
        print_meta("find_references", &refs.meta);
        println!("  references ({}):", refs.hits.len());
        for h in refs.hits.iter().take(10) {
            print_hit("ref", h);
        }
    }

    // --- Scenario 7: explain_context observability ---
    sep("7. explain_context — policy / prune diagnostics");
    if let Some(rev_hex) = seed_rev {
        let ex = rt
            .explain_context(0, &rev_hex, 4000, None)
            .expect("explain_context");
        print_meta("explain_context", &ex.meta);
        println!("  counts: {:?}", ex.counts);
        println!("  notes: {}", ex.notes);
    }

    // --- Graph edge census (ground truth for expectations) ---
    sep("8. Graph edge census (runtime)");
    {
        let g = rt.graph_mutex().read();
        let mut calls = 0usize;
        let mut imports = 0usize;
        let mut uses = 0usize;
        let n_rev = g.revisions().count();
        for r in g.revisions() {
            for e in g.outbound_edges(r.revision_id) {
                match e.ty {
                    cis_core::EdgeType::Calls => calls += 1,
                    cis_core::EdgeType::Imports => imports += 1,
                    cis_core::EdgeType::Uses => uses += 1,
                    _ => {}
                }
            }
        }
        println!("  revisions={n_rev} Calls={calls} Imports={imports} Uses={uses}");
        if calls == 0 {
            println!("  → expand_context / go_to_definition limited without call edges");
        }
    }

    // --- Fresh bootstrap (production re-index path) ---
    sep("9. Fresh bootstrap_python_index_from_repo — in-memory graph");
    std::env::set_var("CIS_FORCE_REINDEX", "1");
    let rep = rt
        .bootstrap_python_index_from_repo()
        .expect("bootstrap");
    println!("  bootstrap applied={}", rep.applied);
    rt.sync_revision_index_from_graph();
    let idx2 = rt.index_status(0).expect("index_status");
    println!(
        "  post-bootstrap index: symbols={} edges={}",
        idx2.symbols_indexed, idx2.edges_indexed
    );
    {
        let g = rt.graph_mutex().read();
        let n_rev = g.revisions().count();
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
        println!("  in-memory: revisions={n_rev} Calls={calls} Imports={imports}");
    }
    if let Some(rev_hex) = rt
        .find_symbol(0, "getStrPosition", None, 1, false)
        .ok()
        .and_then(|r| r.matches.first().map(|m| m.revision_id_hex.clone()))
    {
        let exp = rt.expand_context(0, &rev_hex, 2, None, usize::MAX).expect("expand after bootstrap");
        println!(
            "  expand_context after bootstrap: {} hits, pruned={}",
            exp.hits.len(),
            exp.meta.pruned_low_confidence_count
        );
        for h in exp.hits.iter().take(6) {
            print_hit("  ", h);
        }
        let refs = rt
            .find_symbol(0, "getPosition", None, 1, false)
            .ok()
            .and_then(|r| r.matches.into_iter().find(|m| m.file_path.contains("BoardUtils")))
            .map(|m| m.revision_id_hex);
        if let Some(pos_hex) = refs {
            let fr = rt
                .find_references(0, &pos_hex, None, 20)
                .expect("find_references after bootstrap");
            println!("  find_references(getPosition): {} hits", fr.hits.len());
            for h in fr.hits.iter().take(5) {
                print_hit("ref", h);
            }
        }
    }

    sep("Eval complete");
}
