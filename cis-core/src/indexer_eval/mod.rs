//! Real-corpus evaluation of tree-sitter language indexers.
//!
//! Two questions, in order of importance:
//!
//! 1. **Cross-file graph** — after `apply_index_events`, did imports / calls / extends
//!    that *should* land in another corpus file actually land on a non-stub node there?
//!    Navigation quality depends on this far more than on in-file CST extraction.
//! 2. **In-file CST recall** — independent Tree-sitter census vs indexer `FileIndex`
//!    (symbols, calls, imports, extends, type-uses) on the same file.
//!
//! Toy snippets belong in `*_indexer.rs` unit tests.
//!
//! Run: `cargo test -p cis-core --features tree-sitter-all --test indexer_corpus_eval -- --nocapture`

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::graph::{Language, NodeKind, SourceSpan};
use crate::index_model::FileIndex;
use crate::index_walk::collect_source_files;
use crate::language_indexer::{default_indexers, indexer_for_path, LanguageIndexer};

mod graph;
mod oracle;

pub use graph::{CrossFileReport, GraphLandingReport};
pub use oracle::{census, grammar_available, OracleFile};

const DEFAULT_MAX_FILE_BYTES: u64 = 2_000_000;
const MISS_SAMPLES: usize = 12;

#[derive(Debug, Clone)]
pub struct EvalConfig {
    pub max_file_bytes: u64,
    pub graph_ingest: bool,
    pub max_graph_files: usize,
    pub min_files_to_score: usize,
    pub min_files_for_graph: usize,
    pub max_files_per_language: usize,
    pub floors: RecallFloors,
}

impl Default for EvalConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: env_u64("CIS_INDEXER_EVAL_MAX_FILE_BYTES", DEFAULT_MAX_FILE_BYTES),
            graph_ingest: !env_flag("CIS_INDEXER_EVAL_SKIP_GRAPH"),
            max_graph_files: env_usize("CIS_INDEXER_EVAL_MAX_GRAPH_FILES", 200),
            min_files_to_score: env_usize("CIS_INDEXER_EVAL_MIN_FILES", 3),
            min_files_for_graph: env_usize("CIS_INDEXER_EVAL_MIN_GRAPH_FILES", 5),
            max_files_per_language: env_usize("CIS_INDEXER_EVAL_MAX_FILES", 120),
            floors: RecallFloors::from_env(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RecallFloors {
    pub symbol_recall: f64,
    pub call_recall: f64,
    pub import_recall: f64,
    pub extends_recall: f64,
    pub use_recall: f64,
    pub parse_success: f64,
    pub graph_symbol_landing: f64,
    pub graph_call_landing: f64,
    pub import_resolution: f64,
    pub cross_file_call_resolution: f64,
    pub intra_file_call_resolution: f64,
    pub min_cross_file_calls: usize,
}

impl RecallFloors {
    pub fn from_env() -> Self {
        Self {
            symbol_recall: env_f64("CIS_INDEXER_EVAL_MIN_SYMBOL_RECALL", 0.88),
            call_recall: env_f64("CIS_INDEXER_EVAL_MIN_CALL_RECALL", 0.80),
            import_recall: env_f64("CIS_INDEXER_EVAL_MIN_IMPORT_RECALL", 0.85),
            extends_recall: env_f64("CIS_INDEXER_EVAL_MIN_EXTENDS_RECALL", 0.70),
            use_recall: env_f64("CIS_INDEXER_EVAL_MIN_USE_RECALL", 0.45),
            parse_success: env_f64("CIS_INDEXER_EVAL_MIN_PARSE_SUCCESS", 0.95),
            graph_symbol_landing: env_f64("CIS_INDEXER_EVAL_MIN_GRAPH_SYMBOL", 0.90),
            graph_call_landing: env_f64("CIS_INDEXER_EVAL_MIN_GRAPH_CALL", 0.15),
            import_resolution: env_f64("CIS_INDEXER_EVAL_MIN_IMPORT_RESOLUTION", 0.55),
            cross_file_call_resolution: env_f64("CIS_INDEXER_EVAL_MIN_XFILE_CALL", 0.35),
            intra_file_call_resolution: env_f64("CIS_INDEXER_EVAL_MIN_INTRA_CALL", 0.45),
            min_cross_file_calls: env_usize("CIS_INDEXER_EVAL_MIN_XFILE_CALLS", 8),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MissSample {
    pub path: String,
    pub name: String,
    pub line: u32,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileScore {
    pub path: String,
    pub language: String,
    pub oracle_symbols: usize,
    pub indexed_symbols: usize,
    pub symbol_recall: f64,
    pub call_recall: f64,
    pub import_recall: f64,
    pub extends_recall: f64,
    pub use_recall: f64,
    pub invariant_violations: Vec<String>,
    pub index_error: Option<String>,
    pub parse_error_in_tree: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LanguageReport {
    pub language: String,
    pub corpus: String,
    pub files: usize,
    pub skipped_large: usize,
    pub index_failures: usize,
    pub trees_with_error: usize,
    pub parse_success: f64,
    pub oracle_indexable_symbols: usize,
    pub matched_symbols: usize,
    pub symbol_recall: f64,
    pub nested_oracle_symbols: usize,
    pub nested_matched: usize,
    pub oracle_calls: usize,
    pub matched_calls: usize,
    pub call_recall: f64,
    pub oracle_imports: usize,
    pub matched_imports: usize,
    pub import_recall: f64,
    pub oracle_extends: usize,
    pub matched_extends: usize,
    pub extends_recall: f64,
    pub oracle_uses: usize,
    pub matched_uses: usize,
    pub use_recall: f64,
    pub invariant_violations: usize,
    pub missed_symbols: Vec<MissSample>,
    pub missed_calls: Vec<MissSample>,
    pub missed_imports: Vec<MissSample>,
    pub missed_extends: Vec<MissSample>,
    pub worst_files: Vec<FileScore>,
    pub invariant_samples: Vec<String>,
    pub graph: Option<GraphLandingReport>,
    /// Why graph ingest was skipped (closed-world incomplete, too few files, …).
    pub graph_skip: Option<String>,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CorpusReport {
    pub name: String,
    pub root: String,
    pub languages: Vec<LanguageReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalSuiteReport {
    pub corpora: Vec<CorpusReport>,
}

impl EvalSuiteReport {
    pub fn failures(&self) -> Vec<String> {
        self.corpora
            .iter()
            .flat_map(|c| {
                c.languages
                    .iter()
                    .flat_map(|l| l.failures.iter().cloned())
            })
            .collect()
    }

    pub fn format_human(&self) -> String {
        let mut s = String::new();
        s.push_str("CIS indexer corpus evaluation\n");
        s.push_str("================================\n");
        for corpus in &self.corpora {
            s.push_str(&format!("\nCorpus: {} ({})\n", corpus.name, corpus.root));
            for lang in &corpus.languages {
                s.push_str(&format_language(lang));
            }
        }
        let fails = self.failures();
        if fails.is_empty() {
            s.push_str("\nRESULT: PASS\n");
        } else {
            s.push_str(&format!("\nRESULT: FAIL ({} checks)\n", fails.len()));
            for f in &fails {
                s.push_str(&format!("  - {f}\n"));
            }
        }
        s
    }
}

fn format_language(lang: &LanguageReport) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "  [{lang}] files={files} index_fail={fail} tree_error={err} parse_ok={parse:.1}%\n",
        lang = lang.language,
        files = lang.files,
        fail = lang.index_failures,
        err = lang.trees_with_error,
        parse = lang.parse_success * 100.0,
    ));
    if let Some(g) = &lang.graph {
        let xf = &g.cross_file;
        s.push_str(&format!(
            "    CROSS-FILE  imports {ri}/{ei} ({irp:.0}%)  calls {rc}/{ec} ({crp:.0}%)  extends {re}/{ee} ({erp:.0}%)\n",
            ri = xf.resolved_imports,
            ei = xf.expected_imports,
            irp = xf.import_resolution * 100.0,
            rc = xf.resolved_cross_file_calls,
            ec = xf.expected_cross_file_calls,
            crp = xf.cross_file_call_resolution * 100.0,
            re = xf.resolved_cross_file_extends,
            ee = xf.expected_cross_file_extends,
            erp = xf.cross_file_extends_resolution * 100.0,
        ));
        s.push_str(&format!(
            "               intra-calls {ria}/{eia} ({iap:.0}%)  graph xfile_calls={xc} intra={ic} stubs={st} dangling={dn} xfile_imports={xi}\n",
            ria = xf.resolved_intra_file_calls,
            eia = xf.expected_intra_file_calls,
            iap = xf.intra_file_call_resolution * 100.0,
            xc = xf.graph_cross_file_calls,
            ic = xf.graph_intra_file_calls,
            st = xf.graph_stub_calls,
            dn = xf.graph_dangling_calls,
            xi = xf.graph_cross_file_imports,
        ));
        s.push_str(&format!(
            "    graph       symbol_land={:.1}% ingested={}/{}  (raw call_land={:.1}% is not cross-file)\n",
            g.symbol_landing * 100.0,
            g.files_ingested,
            lang.files,
            g.call_landing * 100.0,
        ));
        if !xf.sample_resolved_cross_calls.is_empty() {
            s.push_str("    resolved xfile samples:\n");
            for m in xf.sample_resolved_cross_calls.iter().take(6) {
                s.push_str(&format!("      {m}\n"));
            }
        }
        if !xf.missed_cross_calls.is_empty() {
            s.push_str("    missed xfile calls:\n");
            for m in xf.missed_cross_calls.iter().take(8) {
                s.push_str(&format!("      {m}\n"));
            }
        }
        if !xf.missed_imports.is_empty() {
            s.push_str("    missed xfile imports:\n");
            for m in xf.missed_imports.iter().take(6) {
                s.push_str(&format!("      {m}\n"));
            }
        }
        if !xf.missed_extends.is_empty() {
            s.push_str("    missed xfile extends:\n");
            for m in xf.missed_extends.iter().take(4) {
                s.push_str(&format!("      {m}\n"));
            }
        }
    } else if let Some(why) = &lang.graph_skip {
        s.push_str(&format!("    CROSS-FILE  skipped ({why})\n"));
    }
    s.push_str(&format!(
        "    symbols  recall={:.1}%  ({}/{} indexable; nested {}/{})\n",
        lang.symbol_recall * 100.0,
        lang.matched_symbols,
        lang.oracle_indexable_symbols,
        lang.nested_matched,
        lang.nested_oracle_symbols,
    ));
    s.push_str(&format!(
        "    calls    recall={:.1}%  ({}/{})\n",
        lang.call_recall * 100.0,
        lang.matched_calls,
        lang.oracle_calls,
    ));
    s.push_str(&format!(
        "    imports  recall={:.1}%  ({}/{})\n",
        lang.import_recall * 100.0,
        lang.matched_imports,
        lang.oracle_imports,
    ));
    s.push_str(&format!(
        "    extends  recall={:.1}%  ({}/{})\n",
        lang.extends_recall * 100.0,
        lang.matched_extends,
        lang.oracle_extends,
    ));
    s.push_str(&format!(
        "    uses     recall={:.1}%  ({}/{})  invariants={}\n",
        lang.use_recall * 100.0,
        lang.matched_uses,
        lang.oracle_uses,
        lang.invariant_violations,
    ));
    if !lang.invariant_samples.is_empty() {
        s.push_str("    invariant samples:\n");
        for inv in lang.invariant_samples.iter().take(8) {
            s.push_str(&format!("      {inv}\n"));
        }
    }
    if lang.files > 0 && lang.trees_with_error * 2 > lang.files {
        s.push_str("    note: majority of trees have ERROR nodes — recall floors skipped (grammar mismatch)\n");
    }
    if !lang.missed_symbols.is_empty() {
        s.push_str("    missed symbols:\n");
        for m in lang.missed_symbols.iter().take(MISS_SAMPLES) {
            s.push_str(&format!(
                "      {}:{} {} ({})\n",
                m.path, m.line, m.name, m.detail
            ));
        }
    }
    if !lang.missed_calls.is_empty() {
        s.push_str("    missed calls:\n");
        for m in lang.missed_calls.iter().take(8) {
            s.push_str(&format!(
                "      {}:{} {} ({})\n",
                m.path, m.line, m.name, m.detail
            ));
        }
    }
    if !lang.worst_files.is_empty() {
        s.push_str("    weakest files:\n");
        for f in lang.worst_files.iter().take(5) {
            s.push_str(&format!(
                "      {}  sym={:.0}% call={:.0}%\n",
                f.path,
                f.symbol_recall * 100.0,
                f.call_recall * 100.0
            ));
        }
    }
    s
}

/// Evaluate one repository root. `rel_prefix` is prepended to repo-relative paths in reports.
pub fn evaluate_corpus(
    name: &str,
    root: &Path,
    cfg: &EvalConfig,
) -> CorpusReport {
    let indexers = default_indexers();
    let exts: Vec<&str> = indexers.iter().map(|i| i.file_extension()).collect();
    let mut files = Vec::new();
    collect_source_files(root, &exts, &mut files);

    let mut by_lang: BTreeMap<String, LangAcc> = BTreeMap::new();
    let mut indexed_for_graph: Vec<(String, PathBuf, FileIndex)> = Vec::new();

    for abs in &files {
        let rel = abs
            .strip_prefix(root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| abs.to_string_lossy().replace('\\', "/"));
        if skip_eval_rel_path(&rel) {
            continue;
        }
        let Some(indexer) = indexer_for_path(&rel, &indexers) else {
            continue;
        };
        let lang = language_name(indexer.language());
        let acc = by_lang.entry(lang.clone()).or_insert_with(|| LangAcc {
            language: lang.clone(),
            ..LangAcc::default()
        });
        if acc.files >= cfg.max_files_per_language {
            continue;
        }

        let meta = match std::fs::metadata(abs) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.len() > cfg.max_file_bytes {
            acc.skipped_large += 1;
            continue;
        }
        let Ok(content) = std::fs::read_to_string(abs) else {
            acc.read_failures += 1;
            continue;
        };

        acc.files += 1;
        let indexed = indexer.index_file(&rel, &content);
        let oracle = if grammar_available(rel.rsplit('.').next().unwrap_or("")) {
            census(&rel, &content)
        } else {
            None
        };

        match (&indexed, &oracle) {
            (Ok(idx), Some(or)) => {
                if or.has_error {
                    acc.trees_with_error += 1;
                }
                let score = score_file(&rel, &lang, idx, or);
                acc.absorb(&score);
                indexed_for_graph.push((rel, abs.clone(), idx.clone()));
            }
            (Ok(idx), None) => {
                let inv = file_invariants(idx);
                acc.invariant_violations += inv.len();
                indexed_for_graph.push((rel, abs.clone(), idx.clone()));
            }
            (Err(e), _) => {
                acc.index_failures += 1;
                acc.file_scores.push(FileScore {
                    path: rel,
                    language: lang,
                    oracle_symbols: 0,
                    indexed_symbols: 0,
                    symbol_recall: 0.0,
                    call_recall: 0.0,
                    import_recall: 0.0,
                    extends_recall: 0.0,
                    use_recall: 0.0,
                    invariant_violations: vec![],
                    index_error: Some(format!("{e:?}")),
                    parse_error_in_tree: oracle.as_ref().map(|o| o.has_error).unwrap_or(false),
                });
            }
        }
    }

    let mut languages = Vec::new();
    for (lang_name, acc) in by_lang {
        let mut report = acc.finish(name, cfg);
        attach_graph_eval(&mut report, name, root, cfg, &indexers, &indexed_for_graph, &lang_name);
        languages.push(report);
    }

    CorpusReport {
        name: name.to_string(),
        root: root.display().to_string(),
        languages,
    }
}

/// Built-in corpora: CIS Rust sources, chess pygame, helper scripts, optional fetched trees.
pub fn builtin_corpora() -> Vec<(String, PathBuf, bool)> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cis = manifest.join("..");
    let mut out = vec![
        ("cis-rust".into(), manifest.join("src"), true),
        ("cis-mcp".into(), cis.join("cis-mcp/src"), false),
        ("cis-wal".into(), cis.join("cis-wal/src"), false),
        ("cis-scripts".into(), cis.join("scripts"), false),
        ("chess-pygame".into(), cis.join("fixtures/chess_pygame"), false),
    ];
    let extra_root = cis.join("fixtures/indexer_eval");
    if extra_root.is_dir() {
        if let Ok(rd) = std::fs::read_dir(&extra_root) {
            for ent in rd.flatten() {
                let p = ent.path();
                if p.is_dir() {
                    let name = format!("eval-{}", ent.file_name().to_string_lossy());
                    out.push((name, p, false));
                }
            }
        }
    }
    if let Ok(extra) = std::env::var("CIS_INDEXER_EVAL_ROOTS") {
        for part in extra.split(':') {
            let p = PathBuf::from(part.trim());
            if p.is_dir() {
                let name = p
                    .file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "extra".into());
                out.push((name, p, false));
            }
        }
    }
    out
}

pub fn run_builtin_suite(cfg: &EvalConfig) -> EvalSuiteReport {
    let mut corpora = Vec::new();
    for (name, root, required) in builtin_corpora() {
        if !root.is_dir() {
            let require_fixtures = std::env::var_os("CIS_REQUIRE_FIXTURES")
                .is_some_and(|v| v == "1");
            if required || (require_fixtures && name == "chess-pygame") {
                let dummy = CorpusReport {
                    name: name.clone(),
                    root: root.display().to_string(),
                    languages: vec![LanguageReport {
                        language: "missing".into(),
                        corpus: name.clone(),
                        failures: vec![format!("required corpus missing: {}", root.display())],
                        graph_skip: None,
                        ..LanguageReport::default()
                    }],
                };
                corpora.push(dummy);
            }
            continue;
        }
        corpora.push(evaluate_corpus(&name, &root, cfg));
    }
    EvalSuiteReport { corpora }
}

#[derive(Default)]
struct LangAcc {
    language: String,
    files: usize,
    skipped_large: usize,
    read_failures: usize,
    index_failures: usize,
    trees_with_error: usize,
    oracle_indexable_symbols: usize,
    matched_symbols: usize,
    nested_oracle_symbols: usize,
    nested_matched: usize,
    oracle_calls: usize,
    matched_calls: usize,
    oracle_imports: usize,
    matched_imports: usize,
    oracle_extends: usize,
    matched_extends: usize,
    oracle_uses: usize,
    matched_uses: usize,
    invariant_violations: usize,
    missed_symbols: Vec<MissSample>,
    missed_calls: Vec<MissSample>,
    missed_imports: Vec<MissSample>,
    missed_extends: Vec<MissSample>,
    file_scores: Vec<FileScore>,
    invariant_samples: Vec<String>,
}

impl LangAcc {
    fn absorb(&mut self, score: &FileScorePlus) {
        self.oracle_indexable_symbols += score.oracle_indexable;
        self.matched_symbols += score.matched_symbols;
        self.nested_oracle_symbols += score.nested_oracle;
        self.nested_matched += score.nested_matched;
        self.oracle_calls += score.oracle_calls;
        self.matched_calls += score.matched_calls;
        self.oracle_imports += score.oracle_imports;
        self.matched_imports += score.matched_imports;
        self.oracle_extends += score.oracle_extends;
        self.matched_extends += score.matched_extends;
        self.oracle_uses += score.oracle_uses;
        self.matched_uses += score.matched_uses;
        self.invariant_violations += score.file.invariant_violations.len();
        self.invariant_samples.extend(score.file.invariant_violations.iter().cloned());
        self.missed_symbols.extend(score.missed_symbols.clone());
        self.missed_calls.extend(score.missed_calls.clone());
        self.missed_imports.extend(score.missed_imports.clone());
        self.missed_extends.extend(score.missed_extends.clone());
        self.file_scores.push(score.file.clone());
    }

    fn finish(mut self, corpus: &str, cfg: &EvalConfig) -> LanguageReport {
        self.missed_symbols.truncate(MISS_SAMPLES);
        self.missed_calls.truncate(MISS_SAMPLES);
        self.missed_imports.truncate(MISS_SAMPLES);
        self.missed_extends.truncate(MISS_SAMPLES);
        self.invariant_samples.truncate(MISS_SAMPLES);
        self.file_scores.sort_by(|a, b| {
            a.symbol_recall
                .partial_cmp(&b.symbol_recall)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let worst: Vec<_> = self.file_scores.iter().take(8).cloned().collect();
        let parse_success = {
            let attempts = self.files;
            if attempts == 0 {
                1.0
            } else {
                1.0 - (self.index_failures as f64 / attempts as f64)
            }
        };
        let grammar_mismatch = self.files > 0
            && self.trees_with_error * 2 > self.files;
        let mut report = LanguageReport {
            language: self.language.clone(),
            corpus: corpus.to_string(),
            files: self.files,
            skipped_large: self.skipped_large,
            index_failures: self.index_failures,
            trees_with_error: self.trees_with_error,
            parse_success,
            oracle_indexable_symbols: self.oracle_indexable_symbols,
            matched_symbols: self.matched_symbols,
            symbol_recall: ratio(self.matched_symbols, self.oracle_indexable_symbols),
            nested_oracle_symbols: self.nested_oracle_symbols,
            nested_matched: self.nested_matched,
            oracle_calls: self.oracle_calls,
            matched_calls: self.matched_calls,
            call_recall: ratio(self.matched_calls, self.oracle_calls),
            oracle_imports: self.oracle_imports,
            matched_imports: self.matched_imports,
            import_recall: ratio(self.matched_imports, self.oracle_imports),
            oracle_extends: self.oracle_extends,
            matched_extends: self.matched_extends,
            extends_recall: ratio(self.matched_extends, self.oracle_extends),
            oracle_uses: self.oracle_uses,
            matched_uses: self.matched_uses,
            use_recall: ratio(self.matched_uses, self.oracle_uses),
            invariant_violations: self.invariant_violations,
            missed_symbols: self.missed_symbols,
            missed_calls: self.missed_calls,
            missed_imports: self.missed_imports,
            missed_extends: self.missed_extends,
            worst_files: worst,
            invariant_samples: self.invariant_samples,
            graph: None,
            graph_skip: None,
            failures: vec![],
        };
        if self.files >= cfg.min_files_to_score && !grammar_mismatch {
            push_floor(
                &mut report.failures,
                corpus,
                &self.language,
                "symbol recall",
                report.symbol_recall,
                cfg.floors.symbol_recall,
                report.oracle_indexable_symbols,
                20,
            );
            push_floor(
                &mut report.failures,
                corpus,
                &self.language,
                "call recall",
                report.call_recall,
                cfg.floors.call_recall,
                report.oracle_calls,
                40,
            );
            push_floor(
                &mut report.failures,
                corpus,
                &self.language,
                "import recall",
                report.import_recall,
                cfg.floors.import_recall,
                report.oracle_imports,
                8,
            );
            push_floor(
                &mut report.failures,
                corpus,
                &self.language,
                "extends recall",
                report.extends_recall,
                cfg.floors.extends_recall,
                report.oracle_extends,
                8,
            );
            push_floor(
                &mut report.failures,
                corpus,
                &self.language,
                "use recall",
                report.use_recall,
                cfg.floors.use_recall,
                report.oracle_uses,
                40,
            );
            if parse_success + 1e-9 < cfg.floors.parse_success {
                report.failures.push(format!(
                    "{corpus}/{} parse success {:.1}% < {:.0}%",
                    self.language,
                    parse_success * 100.0,
                    cfg.floors.parse_success * 100.0
                ));
            }
            if report.invariant_violations > 0 {
                report.failures.push(format!(
                    "{corpus}/{} {} FileIndex invariant violations",
                    self.language, report.invariant_violations
                ));
            }
        }
        report
    }
}

fn attach_graph_eval(
    report: &mut LanguageReport,
    corpus: &str,
    root: &Path,
    cfg: &EvalConfig,
    indexers: &[Box<dyn LanguageIndexer>],
    indexed_for_graph: &[(String, PathBuf, FileIndex)],
    lang_name: &str,
) {
    if !cfg.graph_ingest {
        report.graph_skip = Some("CIS_INDEXER_EVAL_SKIP_GRAPH=1".into());
        return;
    }
    let subset: Vec<_> = indexed_for_graph
        .iter()
        .filter(|(rel, _, _)| {
            indexer_for_path(rel, indexers)
                .map(|i| language_name(i.language()) == lang_name)
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    if subset.len() < cfg.min_files_for_graph {
        report.graph_skip = Some(format!(
            "{} files < min_graph_files {}",
            subset.len(),
            cfg.min_files_for_graph
        ));
        return;
    }
    if subset.len() > cfg.max_graph_files {
        // A truncated subset would miss callees outside the cap and lie about resolution.
        report.graph_skip = Some(format!(
            "{} files > max_graph_files {} (closed-world ingest would be incomplete)",
            subset.len(),
            cfg.max_graph_files
        ));
        return;
    }
    // Optional OSS trees: ingest small ones closed-world; skip large (gson/dapper/underscore).
    if corpus.starts_with("eval-") && subset.len() > 40 {
        report.graph_skip = Some(format!(
            "eval corpus {} files > 40 (CST recall still scored; set CIS_INDEXER_EVAL_MAX_GRAPH_FILES and raise this cap to ingest)",
            subset.len()
        ));
        return;
    }
    match graph::ingest_and_score(root, &subset, indexers, lang_name) {
        Ok(g) => {
            let grammar_mismatch = report.files > 0 && report.trees_with_error * 2 > report.files;
            if !grammar_mismatch {
                apply_graph_floors(report, corpus, lang_name, &g, cfg);
            }
            report.graph = Some(g);
        }
        Err(e) => {
            report.failures.push(format!("{corpus}/{lang_name} graph ingest: {e}"));
        }
    }
}

fn apply_graph_floors(
    report: &mut LanguageReport,
    corpus: &str,
    lang: &str,
    g: &graph::GraphLandingReport,
    cfg: &EvalConfig,
) {
    if g.symbol_landing + 1e-9 < cfg.floors.graph_symbol_landing && g.indexed_symbols > 0 {
        report.failures.push(format!(
            "{corpus}/{lang} graph symbol landing {:.1}% < {:.0}%",
            g.symbol_landing * 100.0,
            cfg.floors.graph_symbol_landing * 100.0
        ));
    }
    let xf = &g.cross_file;
    let apply_resolution = matches!(
        lang,
        "python" | "rust" | "typescript" | "javascript" | "go" | "java" | "c"
    );
    if apply_resolution {
        push_floor(
            &mut report.failures,
            corpus,
            lang,
            "import resolution",
            xf.import_resolution,
            cfg.floors.import_resolution,
            xf.expected_imports,
            5,
        );
        push_floor(
            &mut report.failures,
            corpus,
            lang,
            "cross-file call resolution",
            xf.cross_file_call_resolution,
            cfg.floors.cross_file_call_resolution,
            xf.expected_cross_file_calls,
            8,
        );
        push_floor(
            &mut report.failures,
            corpus,
            lang,
            "intra-file call resolution",
            xf.intra_file_call_resolution,
            cfg.floors.intra_file_call_resolution,
            xf.expected_intra_file_calls,
            15,
        );
    }
    if corpus == "chess-pygame" {
        if xf.expected_cross_file_calls < 8 {
            report.failures.push(format!(
                "{corpus}/{lang} closed-world expected only {} cross-file calls (need ≥8; BoardUtils star-import should produce many)",
                xf.expected_cross_file_calls
            ));
        }
        if xf.graph_cross_file_calls < cfg.floors.min_cross_file_calls {
            report.failures.push(format!(
                "{corpus}/{lang} graph cross-file Calls edges {} < {}",
                xf.graph_cross_file_calls, cfg.floors.min_cross_file_calls
            ));
        }
    }
}

fn push_floor(
    failures: &mut Vec<String>,
    corpus: &str,
    lang: &str,
    label: &str,
    got: f64,
    floor: f64,
    denom: usize,
    min_denom: usize,
) {
    if denom < min_denom {
        return;
    }
    if got + 1e-9 < floor {
        failures.push(format!(
            "{corpus}/{lang} {label} {:.1}% < {:.0}% (n={denom})",
            got * 100.0,
            floor * 100.0
        ));
    }
}

struct FileScorePlus {
    file: FileScore,
    oracle_indexable: usize,
    matched_symbols: usize,
    nested_oracle: usize,
    nested_matched: usize,
    oracle_calls: usize,
    matched_calls: usize,
    oracle_imports: usize,
    matched_imports: usize,
    oracle_extends: usize,
    matched_extends: usize,
    oracle_uses: usize,
    matched_uses: usize,
    missed_symbols: Vec<MissSample>,
    missed_calls: Vec<MissSample>,
    missed_imports: Vec<MissSample>,
    missed_extends: Vec<MissSample>,
}

fn score_file(path: &str, lang: &str, idx: &FileIndex, oracle: &OracleFile) -> FileScorePlus {
    let (matched_s, miss_s, nest_m) = match_symbols(idx, oracle);
    let (matched_c, miss_c, n_calls) = match_calls(idx, oracle, lang);
    let (matched_i, miss_i, n_imp) = match_imports(idx, oracle);
    let (matched_e, miss_e, n_ext) = match_extends(idx, oracle);
    let (matched_u, _miss_u, n_use) = match_uses(idx, oracle);

    let indexable: Vec<_> = oracle.symbols.iter().filter(|s| s.indexable).collect();
    let nested: Vec<_> = oracle.symbols.iter().filter(|s| !s.indexable).collect();
    let miss = |items: Vec<(String, u32, String)>, n: usize| {
        items
            .into_iter()
            .take(n)
            .map(|(name, line, detail)| MissSample {
                path: path.to_string(),
                name,
                line,
                detail,
            })
            .collect()
    };

    FileScorePlus {
        file: FileScore {
            path: path.to_string(),
            language: lang.to_string(),
            oracle_symbols: indexable.len(),
            indexed_symbols: idx
                .symbols
                .iter()
                .filter(|s| s.stable_key != "$file")
                .count(),
            symbol_recall: ratio(matched_s, indexable.len()),
            call_recall: ratio(matched_c, n_calls),
            import_recall: ratio(matched_i, n_imp),
            extends_recall: ratio(matched_e, n_ext),
            use_recall: ratio(matched_u, n_use),
            invariant_violations: file_invariants(idx),
            index_error: None,
            parse_error_in_tree: oracle.has_error,
        },
        oracle_indexable: indexable.len(),
        matched_symbols: matched_s,
        nested_oracle: nested.len(),
        nested_matched: nest_m,
        oracle_calls: n_calls,
        matched_calls: matched_c,
        oracle_imports: n_imp,
        matched_imports: matched_i,
        oracle_extends: n_ext,
        matched_extends: matched_e,
        oracle_uses: n_use,
        matched_uses: matched_u,
        missed_symbols: miss(miss_s, 4),
        missed_calls: miss(miss_c, 3),
        missed_imports: miss(miss_i, 3),
        missed_extends: miss(miss_e, 3),
    }
}

fn match_symbols(
    idx: &FileIndex,
    oracle: &OracleFile,
) -> (usize, Vec<(String, u32, String)>, usize) {
    let indexed: Vec<_> = idx
        .symbols
        .iter()
        .filter(|s| s.stable_key != "$file" && s.kind != NodeKind::File)
        .collect();
    let mut used = vec![false; indexed.len()];
    let mut matched = 0usize;
    let mut nested_matched = 0usize;
    let mut misses = Vec::new();

    for sym in &oracle.symbols {
        let found = find_symbol(&indexed, &mut used, sym.name.as_str(), sym.kind, sym.line);
        if found {
            if sym.indexable {
                matched += 1;
            } else {
                nested_matched += 1;
            }
        } else if sym.indexable {
            misses.push((
                sym.name.clone(),
                sym.line,
                format!("{} {:?}", sym.node_kind, sym.kind),
            ));
        }
    }
    (matched, misses, nested_matched)
}

fn find_symbol(
    indexed: &[&crate::index_model::ParsedSymbol],
    used: &mut [bool],
    name: &str,
    kind: NodeKind,
    line: u32,
) -> bool {
    let mut best: Option<usize> = None;
    let mut best_dist = u32::MAX;
    for (i, s) in indexed.iter().enumerate() {
        if used[i] {
            continue;
        }
        if s.kind != kind {
            continue;
        }
        if !names_match(&s.stable_key, name) {
            continue;
        }
        let dist = line_distance(s.span, line);
        if dist <= 2 && dist < best_dist {
            best_dist = dist;
            best = Some(i);
        }
    }
    if let Some(i) = best {
        used[i] = true;
        return true;
    }
    // Name-only fallback: some indexers use type-qualified keys whose span is the impl block.
    for (i, s) in indexed.iter().enumerate() {
        if used[i] {
            continue;
        }
        if s.kind != kind {
            continue;
        }
        if names_match(&s.stable_key, name) && span_nearby(s.span, line, 40) {
            used[i] = true;
            return true;
        }
    }
    false
}

fn match_calls(
    idx: &FileIndex,
    oracle: &OracleFile,
    lang: &str,
) -> (usize, Vec<(String, u32, String)>, usize) {
    let mut used = vec![false; idx.calls.len()];
    let mut matched = 0usize;
    let mut misses = Vec::new();
    let mut required = 0usize;
    for c in &oracle.calls {
        if !required_call(lang, &c.leaf) {
            continue;
        }
        required += 1;
        let mut found = false;
        let mut best: Option<usize> = None;
        let mut best_dist = u32::MAX;
        for (i, ic) in idx.calls.iter().enumerate() {
            if used[i] {
                continue;
            }
            let leaf = ic.callee.leaf_name().trim_end_matches('!');
            let want = c.leaf.trim_end_matches('!');
            if leaf != want && !names_match(leaf, want) {
                continue;
            }
            let dist = line_distance(ic.span, c.line);
            if dist <= 2 && dist < best_dist {
                best_dist = dist;
                best = Some(i);
            }
        }
        if let Some(i) = best {
            used[i] = true;
            found = true;
        }
        if found {
            matched += 1;
        } else {
            misses.push((c.leaf.clone(), c.line, "call".into()));
        }
    }
    (matched, misses, required)
}

fn required_call(lang: &str, leaf: &str) -> bool {
    !is_eval_trivial_call(lang, leaf)
}

pub(crate) fn is_eval_trivial_call(lang: &str, leaf: &str) -> bool {
    let leaf = leaf.trim_end_matches('!');
    if TRIVIAL_CALLS.contains(&leaf) {
        return true;
    }
    match lang {
        "python" => PY_TRIVIAL_CALLS.contains(&leaf),
        "rust" => RS_TRIVIAL_CALLS.contains(&leaf),
        "javascript" | "typescript" => JS_TRIVIAL_CALLS.contains(&leaf),
        "go" => GO_TRIVIAL_CALLS.contains(&leaf),
        "c" | "cpp" => C_TRIVIAL_CALLS.contains(&leaf),
        _ => false,
    }
}

/// Methods the indexers are not expected to graph (std/prelude noise).
const TRIVIAL_CALLS: &[&str] = &[
    "clone", "unwrap", "expect", "into", "from", "default", "drop",
    "as_str", "as_ref", "as_mut", "as_slice", "as_bytes", "to_string", "to_owned",
    "to_vec", "iter", "into_iter", "collect", "map", "filter", "and_then", "or_else",
    "ok", "err", "ok_or", "map_err", "flatten", "copied", "cloned", "take", "get",
    "set", "push", "pop", "insert", "remove", "contains", "len", "is_empty",
    "is_some", "is_none", "is_ok", "is_err", "is_some_and", "is_none_or",
    "unwrap_err", "inspect_err", "inspect", "trim_matches", "trim_start_matches",
    "trim_end_matches", "split_once", "rsplit_once", "strip_prefix", "strip_suffix", "next", "peek", "find", "any", "all",
    "fold", "sum", "min", "max", "rev", "zip", "enumerate", "skip", "chain",
    "then", "then_some", "lock", "read", "write", "borrow", "borrow_mut",
    "deref", "deref_mut", "clone_from", "fmt", "from_str", "parse",
    "to_path_buf", "display", "file_name", "parent", "is_dir", "is_file",
    "exists", "metadata", "to_string_lossy", "as_os_str", "strip_prefix",
    "to_ascii_lowercase", "replace", "split", "join", "trim", "trim_start",
    "trim_end", "starts_with", "ends_with", "is_empty", "chars", "bytes",
    "encode_utf8", "unwrap_or", "unwrap_or_else", "unwrap_or_default",
    "unwrap_unchecked", "ok_or_else", "transpose", "copied", "filter_map",
    "flat_map", "inspect", "cloned", "by_ref", "position", "rposition",
    "count", "last", "nth", "step_by", "cycle", "scan", "take_while",
    "skip_while", "peekable", "fuse", "unzip", "partition", "try_fold",
    "for_each", "eq", "ne", "lt", "le", "gt", "ge", "cmp", "partial_cmp",
    "hash", "type_id", "size_of", "align_of", "drop_in_place",
    "into_inner", "get_mut", "get_or_insert", "entry", "or_insert",
    "or_default", "or_insert_with", "and_modify", "keys", "values",
    "retain", "clear", "drain", "extend", "append", "split_off",
    "sort", "sort_by", "sort_unstable", "binary_search", "contains_key",
    "load", "store", "fetch_add", "fetch_sub", "notify_all", "notify_one",
    "wait", "send", "recv", "try_send", "try_recv", "subscribe",
    "as_mut_slice", "with_capacity", "reserve", "shrink_to_fit",
    "first", "last", "split_at", "chunks", "windows", "repeat",
    "to_lowercase", "to_uppercase", "repeat", "escape_default",
    "as_ptr", "as_mut_ptr", "len", "capacity", "ptr",
];

const PY_TRIVIAL_CALLS: &[&str] = &[
    "strip", "split", "splitlines", "join", "format", "replace", "resolve",
    "read_text", "write_text", "read", "write", "exists", "mkdir", "open",
    "close", "append", "extend", "keys", "values", "items", "get", "pop",
    "update", "copy", "clear", "sort", "reverse", "add", "discard",
    "__init__", "__str__", "__repr__", "__enter__", "__exit__",
    "startswith", "endswith", "lower", "upper", "encode", "decode",
    "fetchone", "fetchall", "execute", "cursor", "connect", "commit",
    "Path", "print", "exit", "getattr", "setattr", "hasattr",
];

const RS_TRIVIAL_CALLS: &[&str] = &[
    "to_le_bytes", "from_le_bytes", "to_be_bytes", "from_be_bytes",
    "saturating_sub", "saturating_add", "checked_add", "wrapping_add",
    "is_ascii_alphabetic", "is_ascii_alphanumeric", "is_empty",
    "utf8_text", "named_child", "named_child_count", "child_by_field_name",
    "kind", "parent", "start_position", "end_position", "walk",
    "set_language", "parse", "root_node", "has_error",
];

const GO_TRIVIAL_CALLS: &[&str] = &[
    "make", "new", "panic", "len", "cap", "append", "copy", "delete", "close",
    "println", "print", "panicf", "recover", "complex", "real", "imag",
];

const C_TRIVIAL_CALLS: &[&str] = &[
    "malloc", "calloc", "realloc", "free", "memcpy", "memmove", "memset", "memcmp",
    "strlen", "strcpy", "strncpy", "strcmp", "strncmp", "sprintf", "snprintf",
    "fprintf", "printf", "scanf", "fopen", "fclose", "fread", "fwrite",
    "assert", "abort", "exit", "defined", "decltype", "sizeof", "offsetof",
    "begin", "end", "c_str", "flush", "rdbuf", "sync", "strcat", "strncat",
];

const JS_TRIVIAL_CALLS: &[&str] = &[
    "push", "pop", "shift", "unshift", "slice", "splice", "map", "filter",
    "reduce", "forEach", "find", "includes", "indexOf", "join", "split",
    "trim", "toString", "valueOf", "hasOwnProperty", "keys", "values",
    "assign", "freeze", "isArray", "then", "catch", "finally",
    "toLowerCase", "toUpperCase", "substring", "substr", "concat",
    "charAt", "charCodeAt", "padStart", "padEnd", "repeat", "match",
    "replace", "search", "startsWith", "endsWith", "propertyIsEnumerable",
];

fn match_imports(
    idx: &FileIndex,
    oracle: &OracleFile,
) -> (usize, Vec<(String, u32, String)>, usize) {
    let mut used = vec![false; idx.imports.len()];
    let mut matched = 0usize;
    let mut misses = Vec::new();
    for imp in &oracle.imports {
        let mut found = false;
        for (i, ii) in idx.imports.iter().enumerate() {
            if used[i] {
                continue;
            }
            if import_names_match(&ii.module, &imp.module) {
                used[i] = true;
                found = true;
                break;
            }
        }
        if found {
            matched += 1;
        } else {
            misses.push((imp.module.clone(), imp.line, "import".into()));
        }
    }
    (matched, misses, oracle.imports.len())
}

fn match_extends(
    idx: &FileIndex,
    oracle: &OracleFile,
) -> (usize, Vec<(String, u32, String)>, usize) {
    let mut used = vec![false; idx.extends.len()];
    let mut matched = 0usize;
    let mut misses = Vec::new();
    for ex in &oracle.extends {
        let mut found = false;
        for (i, ie) in idx.extends.iter().enumerate() {
            if used[i] {
                continue;
            }
            if names_match(&ie.class_stable_key, &ex.class_name)
                && names_match(&ie.base_name, &ex.base_name)
            {
                used[i] = true;
                found = true;
                break;
            }
        }
        if found {
            matched += 1;
        } else {
            misses.push((
                format!("{}:{}", ex.class_name, ex.base_name),
                ex.line,
                "extends".into(),
            ));
        }
    }
    (matched, misses, oracle.extends.len())
}

fn match_uses(
    idx: &FileIndex,
    oracle: &OracleFile,
) -> (usize, Vec<(String, u32, String)>, usize) {
    let mut bag: HashSet<String> = idx
        .uses
        .iter()
        .map(|u| last_ident(&u.type_name))
        .collect();
    // Instance-field inferred types also count as uses.
    for fields in idx.instance_fields.values() {
        for ty in fields.values() {
            bag.insert(last_ident(ty));
        }
    }
    let mut matched = 0usize;
    let mut misses = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for u in &oracle.uses {
        let key = last_ident(&u.type_name);
        if !seen.insert(key.clone()) {
            continue;
        }
        if bag.contains(&key) {
            matched += 1;
        } else {
            misses.push((key, u.line, "use".into()));
        }
    }
    (matched, misses, seen.len())
}

fn file_invariants(idx: &FileIndex) -> Vec<String> {
    let mut v = Vec::new();
    let files: Vec<_> = idx
        .symbols
        .iter()
        .filter(|s| s.stable_key == "$file" || s.kind == NodeKind::File)
        .collect();
    if files.len() != 1 {
        v.push(format!("expected one $file hub, found {}", files.len()));
    }
    let keys: HashSet<&str> = idx.symbols.iter().map(|s| s.stable_key.as_str()).collect();
    let id_keys: HashSet<String> = idx.symbols.iter().map(|s| s.identity_key()).collect();
    if id_keys.len() != idx.symbols.len() {
        v.push("duplicate identity keys".into());
    }
    for s in &idx.symbols {
        if s.stable_key.is_empty() {
            v.push("empty stable_key".into());
        }
        if !span_ok(s.span) {
            v.push(format!("bad span on {}", s.stable_key));
        }
    }
    for c in &idx.calls {
        if c.caller_stable_key != "$file" && !keys.contains(c.caller_stable_key.as_str()) {
            v.push(format!("call caller missing: {}", c.caller_stable_key));
        }
        if c.callee.leaf_name().is_empty() {
            v.push("empty call leaf".into());
        }
        if !span_ok(c.span) {
            v.push("bad call span".into());
        }
    }
    for e in &idx.extends {
        if e.base_name.is_empty() {
            v.push("empty extends base".into());
        }
        if e.class_stable_key.is_empty() {
            v.push("empty extends class".into());
        }
    }
    for u in &idx.uses {
        if u.owner_stable_key != "$file" && !keys.contains(u.owner_stable_key.as_str()) {
            v.push(format!("use owner missing: {}", u.owner_stable_key));
        }
    }
    v.truncate(20);
    v
}

fn span_ok(span: SourceSpan) -> bool {
    span.start_line >= 1 && span.end_line >= span.start_line
}

fn line_distance(span: SourceSpan, line: u32) -> u32 {
    if span.start_line == 0 {
        return 999;
    }
    if line >= span.start_line && line <= span.end_line.max(span.start_line) {
        0
    } else if line < span.start_line {
        span.start_line - line
    } else {
        line - span.end_line.max(span.start_line)
    }
}

fn span_nearby(span: SourceSpan, line: u32, slack: u32) -> bool {
    line_distance(span, line) <= slack
}

fn names_match(indexed: &str, oracle: &str) -> bool {
    if indexed == oracle {
        return true;
    }
    let a = last_ident(indexed);
    let b = last_ident(oracle);
    if a == b {
        return true;
    }
    let an = indexed.replace("::", ".");
    let bn = oracle.replace("::", ".");
    an == bn || last_ident(&an) == last_ident(&bn)
}

fn last_ident(name: &str) -> String {
    name.replace("::", ".")
        .rsplit(['.', '/', '\\'])
        .next()
        .unwrap_or(name)
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '<' || c == '>')
        .to_string()
}

fn import_names_match(indexed: &str, oracle: &str) -> bool {
    if names_match(indexed, oracle) {
        return true;
    }
    let a = indexed.replace("::", ".").replace('\\', "/");
    let b = oracle.replace("::", ".").replace('\\', "/");
    if a.contains(&b) || b.contains(&a) {
        return true;
    }
    last_ident(&a) == last_ident(&b)
}

fn skip_eval_rel_path(rel: &str) -> bool {
    let file = rel.rsplit(['/', '\\']).next().unwrap_or(rel);
    if crate::index_walk::is_skipped_source_filename(file) {
        return true;
    }
    let lower = file.to_ascii_lowercase();
    if lower.ends_with("_test.go")
        || lower.ends_with("_test.rs")
        || lower.ends_with("_test.py")
        || lower.contains(".test.")
        || lower.contains(".spec.")
    {
        return true;
    }
    rel.split(['/', '\\']).any(|c| {
        matches!(
            c,
            "test"
                | "tests"
                | "testdata"
                | "__tests__"
                | "spec"
                | "specs"
                | "examples"
                | "example"
                | "docs"
                | "doc"
                | "benchmark"
                | "benchmarks"
                | "contrib"
                | "support"
                | "fuzzing"
                | "third_party"
                | "vendor"
                | "graal-native-image-test"
                | "node_modules"
                | ".git"
        )
    })
}

fn language_name(lang: Language) -> String {
    match lang {
        Language::Python => "python",
        Language::Rust => "rust",
        Language::Go => "go",
        Language::JavaScript => "javascript",
        Language::TypeScript => "typescript",
        Language::Java => "java",
        Language::C => "c",
        Language::Cpp => "cpp",
        Language::CSharp => "csharp",
        Language::Unknown => "unknown",
    }
    .to_string()
}

fn ratio(n: usize, d: usize) -> f64 {
    if d == 0 {
        1.0
    } else {
        n as f64 / d as f64
    }
}

fn env_flag(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
