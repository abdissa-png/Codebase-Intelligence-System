//! Real-codebase evaluation of tree-sitter language indexers.
//!
//! This is **not** a snippet unit test. It walks whole trees (CIS itself, the chess
//! pygame fixture, optional fetched OSS corpora) and measures:
//!
//! 1. **Cross-file graph resolution** after ingest — imports / calls / extends that
//!    should land in another corpus file, and whether they did (non-stub).
//! 2. In-file CST recall against an independent census (symbols, calls, imports, …).
//!
//! ```text
//! cargo test -p cis-core --features tree-sitter-all --test indexer_corpus_eval -- --nocapture
//!
//! # Extra language corpora (Go/JS/TS/C/C++/Java/C#):
//! ./scripts/fetch_indexer_eval_corpora.sh
//! ```
//!
//! Floors are env-overridable (`CIS_INDEXER_EVAL_MIN_XFILE_CALL`,
//! `CIS_INDEXER_EVAL_MIN_IMPORT_RESOLUTION`, `CIS_INDEXER_EVAL_MIN_SYMBOL_RECALL`, …).
//! Set `CIS_INDEXER_EVAL_SKIP_GRAPH=1` to skip the ingest/resolution pass.
//! Set `CIS_INDEXER_EVAL_JSON=path` to dump the full report.

use cis_core::indexer_eval::{run_builtin_suite, EvalConfig};

#[test]
fn indexer_corpus_recall_on_real_codebases() {
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");
    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_REINDEX_PERSIST", "0");

    let cfg = EvalConfig::default();
    let report = run_builtin_suite(&cfg);
    let text = report.format_human();
    println!("{text}");

    if let Ok(path) = std::env::var("CIS_INDEXER_EVAL_JSON") {
        if let Ok(json) = serde_json::to_string_pretty(&report) {
            let _ = std::fs::write(&path, json);
            println!("wrote JSON report to {path}");
        }
    }

    let scored_files: usize = report
        .corpora
        .iter()
        .flat_map(|c| c.languages.iter().map(|l| l.files))
        .sum();
    assert!(
        scored_files > 0,
        "no indexable files found — enable language features (tree-sitter-all) and/or clone fixtures"
    );

    let failures = report.failures();
    assert!(
        failures.is_empty(),
        "indexer corpus eval failed:\n{}",
        failures.join("\n")
    );
}
