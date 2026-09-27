//! Closed-world graph evaluation: did ingest resolve **cross-file** imports, calls, and extends?
//!
//! Intra-file CST recall (oracle.rs) answers "did we extract the node?". This module answers
//! the question that actually matters for navigation: given the indexer's own FileIndex plus a
//! module map, which relations *should* point at another file in the corpus, and did the graph
//! land a non-stub edge there after `apply_index_events`?

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cis_wal::{BranchId, IdentityId, MutationLog};

use crate::graph::{EdgeType, InMemoryGraph, NodeKind, NodeRevision, RevisionStatus};
use crate::index_model::{CallReceiver, FileIndex, ImportStyle};
use crate::ingest::{
    apply_index_events, module_map_for_paths, FsChangeKind, IndexEvent, IndexEventQueue,
};
use crate::kv::MemoryKv;
use crate::language_indexer::{indexer_for_path, LanguageIndexer};
use crate::saga::MergeSagaOrchestrator;
use crate::WriteCoordinator;

use super::is_eval_trivial_call;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct GraphLandingReport {
    pub files_ingested: usize,
    pub apply_errors: usize,
    pub indexed_symbols: usize,
    pub graph_symbols: usize,
    pub symbol_landing: f64,
    pub missed_symbols: usize,
    pub indexed_calls: usize,
    pub graph_calls: usize,
    pub call_landing: f64,
    pub indexed_imports: usize,
    pub graph_imports: usize,
    pub import_landing: f64,
    pub indexed_extends: usize,
    pub graph_extends: usize,
    pub extends_landing: f64,
    pub indexed_uses: usize,
    pub graph_uses: usize,
    pub use_landing: f64,
    /// Fraction of *created* Calls edges whose target is not a Stub.
    pub resolved_call_rate: f64,
    pub cross_file: CrossFileReport,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CrossFileReport {
    pub expected_imports: usize,
    pub resolved_imports: usize,
    pub import_resolution: f64,
    pub expected_cross_file_calls: usize,
    pub resolved_cross_file_calls: usize,
    pub cross_file_call_resolution: f64,
    pub expected_intra_file_calls: usize,
    pub resolved_intra_file_calls: usize,
    pub intra_file_call_resolution: f64,
    pub expected_cross_file_extends: usize,
    pub resolved_cross_file_extends: usize,
    pub cross_file_extends_resolution: f64,
    pub graph_cross_file_calls: usize,
    pub graph_intra_file_calls: usize,
    pub graph_stub_calls: usize,
    pub graph_dangling_calls: usize,
    pub graph_cross_file_imports: usize,
    pub graph_cross_file_extends: usize,
    pub graph_cross_file_uses: usize,
    pub missed_imports: Vec<String>,
    pub missed_cross_calls: Vec<String>,
    pub missed_intra_calls: Vec<String>,
    pub missed_extends: Vec<String>,
    pub sample_resolved_cross_calls: Vec<String>,
}

struct ExpectedRel {
    src_path: String,
    src_key: String,
    tgt_path: String,
    tgt_leaf: String,
    cross_file: bool,
}

pub fn ingest_and_score(
    root: &Path,
    files: &[(String, PathBuf, FileIndex)],
    indexers: &[Box<dyn LanguageIndexer>],
    lang: &str,
) -> Result<GraphLandingReport, String> {
    if files.is_empty() {
        return Ok(GraphLandingReport::default());
    }

    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));
    let branch = BranchId([0u8; 16]);

    let events: Vec<IndexEvent> = files
        .iter()
        .filter(|(rel, _, _)| indexer_for_path(rel, indexers).is_some())
        .map(|(rel, _, _)| IndexEvent {
            branch_id: branch,
            path: rel.clone(),
            kind: FsChangeKind::Modified,
            old_path: None,
        })
        .collect();

    let root_buf = root.to_path_buf();
    let applied = apply_index_events(
        &IndexEventQueue::new(),
        &coord,
        kv,
        events,
        move |rel| std::fs::read_to_string(root_buf.join(rel)),
        None,
        None,
    )
    .map_err(|e| format!("apply_index_events: {e}"))?;

    let batch: HashMap<String, FileIndex> = files
        .iter()
        .map(|(rel, _, idx)| (rel.clone(), idx.clone()))
        .collect();
    let paths: Vec<String> = files.iter().map(|(rel, _, _)| rel.clone()).collect();
    let mod_map = module_map_for_paths(paths, indexers);

    let g = coord.graph().read();
    let mut by_qn: HashMap<(String, String), NodeKind> = HashMap::new();
    let mut graph_calls = 0usize;
    let mut graph_imports = 0usize;
    let mut graph_extends = 0usize;
    let mut graph_uses = 0usize;
    let mut resolved_calls = 0usize;
    let mut call_edges = 0usize;

    let mut xf = CrossFileReport::default();

    for rev in g.revisions() {
        if !matches!(
            rev.status,
            RevisionStatus::Active | RevisionStatus::Speculative
        ) {
            continue;
        }
        if let Some(kind) = g.identity_kind(rev.identity_id) {
            if matches!(kind, NodeKind::Function | NodeKind::Class) {
                by_qn.insert((rev.file_path.clone(), rev.qualified_name.clone()), kind);
            }
        }
        for e in g.outbound_edges(rev.revision_id) {
            let tgt_rev = g.primary_revision_for_identity(branch, e.target_identity_id);
            let tgt_kind = g.identity_kind(e.target_identity_id);
            let cross = match tgt_rev {
                Some(t) => t.file_path != rev.file_path,
                None => false,
            };
            match e.ty {
                EdgeType::Calls => {
                    graph_calls += 1;
                    call_edges += 1;
                    if tgt_kind == Some(NodeKind::Stub) {
                        xf.graph_stub_calls += 1;
                    } else if tgt_rev.is_none() {
                        xf.graph_dangling_calls += 1;
                    } else {
                        resolved_calls += 1;
                        if cross {
                            xf.graph_cross_file_calls += 1;
                        } else {
                            xf.graph_intra_file_calls += 1;
                        }
                    }
                }
                EdgeType::Imports => {
                    graph_imports += 1;
                    if cross {
                        xf.graph_cross_file_imports += 1;
                    }
                }
                EdgeType::Extends => {
                    graph_extends += 1;
                    if cross {
                        xf.graph_cross_file_extends += 1;
                    }
                }
                EdgeType::Uses => {
                    graph_uses += 1;
                    if cross {
                        xf.graph_cross_file_uses += 1;
                    }
                }
                _ => {}
            }
        }
    }

    let mut indexed_symbols = 0usize;
    let mut missed_symbols = 0usize;
    let mut indexed_calls = 0usize;
    let mut indexed_imports = 0usize;
    let mut indexed_extends = 0usize;
    let mut indexed_uses = 0usize;

    let mut exp_imports = Vec::new();
    let mut exp_calls = Vec::new();
    let mut exp_extends = Vec::new();

    for (rel, _, idx) in files {
        for s in &idx.symbols {
            if s.stable_key == "$file" || s.kind == NodeKind::File {
                continue;
            }
            indexed_symbols += 1;
            if !by_qn.contains_key(&(rel.clone(), s.qualified_name.clone())) {
                missed_symbols += 1;
            }
        }
        indexed_calls += idx.calls.len();
        indexed_imports += idx.imports.len();
        indexed_extends += idx.extends.len();
        indexed_uses += idx.uses.len();

        exp_imports.extend(expected_imports(rel, idx, &mod_map));
        exp_calls.extend(expected_calls(rel, idx, &mod_map, &batch, lang));
        exp_extends.extend(expected_extends(rel, idx, &mod_map, &batch));
    }

    score_expected_imports(&g, branch, &exp_imports, &mut xf);
    score_expected_calls(&g, branch, &exp_calls, &mut xf);
    score_expected_extends(&g, branch, &exp_extends, &mut xf);

    xf.import_resolution = ratio(xf.resolved_imports, xf.expected_imports);
    xf.cross_file_call_resolution =
        ratio(xf.resolved_cross_file_calls, xf.expected_cross_file_calls);
    xf.intra_file_call_resolution =
        ratio(xf.resolved_intra_file_calls, xf.expected_intra_file_calls);
    xf.cross_file_extends_resolution = ratio(
        xf.resolved_cross_file_extends,
        xf.expected_cross_file_extends,
    );
    xf.missed_imports.truncate(8);
    xf.missed_cross_calls.truncate(10);
    xf.missed_intra_calls.truncate(6);
    xf.missed_extends.truncate(6);
    xf.sample_resolved_cross_calls.truncate(8);

    let graph_symbols = by_qn.len();
    Ok(GraphLandingReport {
        files_ingested: applied.applied,
        apply_errors: applied.parse_errors,
        indexed_symbols,
        graph_symbols,
        symbol_landing: ratio(indexed_symbols.saturating_sub(missed_symbols), indexed_symbols),
        missed_symbols,
        indexed_calls,
        graph_calls,
        call_landing: ratio(graph_calls.min(indexed_calls), indexed_calls),
        indexed_imports,
        graph_imports,
        import_landing: ratio(graph_imports.min(indexed_imports), indexed_imports),
        indexed_extends,
        graph_extends,
        extends_landing: ratio(graph_extends.min(indexed_extends), indexed_extends),
        indexed_uses,
        graph_uses,
        use_landing: ratio(graph_uses.min(indexed_uses), indexed_uses),
        resolved_call_rate: ratio(resolved_calls, call_edges),
        cross_file: xf,
    })
}

fn expected_imports(
    path: &str,
    idx: &FileIndex,
    mod_map: &HashMap<String, String>,
) -> Vec<ExpectedRel> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for imp in &idx.imports {
        let Some(tp) = simple_resolve_module(&imp.module, path, mod_map) else {
            continue;
        };
        if tp == path {
            continue;
        }
        if !seen.insert(tp.clone()) {
            continue;
        }
        out.push(ExpectedRel {
            src_path: path.to_string(),
            src_key: "$file".into(),
            tgt_path: tp,
            tgt_leaf: "$file".into(),
            cross_file: true,
        });
    }
    out
}

fn expected_extends(
    path: &str,
    idx: &FileIndex,
    mod_map: &HashMap<String, String>,
    batch: &HashMap<String, FileIndex>,
) -> Vec<ExpectedRel> {
    let bindings = import_name_bindings(idx, path, mod_map);
    let mut out = Vec::new();
    for ext in &idx.extends {
        let (tgt_path, leaf) = if let Some((fp, remote)) = bindings.get(&ext.base_name) {
            (fp.clone(), remote.clone())
        } else if file_has_symbol(idx, &ext.base_name) {
            (path.to_string(), ext.base_name.clone())
        } else if let Some(fp) = find_unique_class(batch, &ext.base_name) {
            (fp, ext.base_name.clone())
        } else {
            continue;
        };
        if !file_has_symbol(batch.get(&tgt_path).unwrap_or(idx), &leaf)
            && tgt_path != path
        {
            // Imported name may be the module; skip if the class is not there.
            if !file_has_symbol(batch.get(&tgt_path).unwrap_or(idx), &ext.base_name) {
                continue;
            }
        }
        out.push(ExpectedRel {
            src_path: path.to_string(),
            src_key: ext.class_stable_key.clone(),
            tgt_path: tgt_path.clone(),
            tgt_leaf: ext.base_name.clone(),
            cross_file: tgt_path != path,
        });
    }
    out
}

fn expected_calls(
    path: &str,
    idx: &FileIndex,
    mod_map: &HashMap<String, String>,
    batch: &HashMap<String, FileIndex>,
    lang: &str,
) -> Vec<ExpectedRel> {
    let bindings = import_name_bindings(idx, path, mod_map);
    let star_files: Vec<String> = idx
        .imports
        .iter()
        .filter(|i| i.style == ImportStyle::Star)
        .filter_map(|i| simple_resolve_module(&i.module, path, mod_map))
        .filter(|p| p != path)
        .collect();

    let mut out = Vec::new();
    for call in &idx.calls {
        let leaf = call.callee.leaf_name().to_string();
        if is_eval_trivial_call(lang, &leaf) {
            continue;
        }
        let caller = if call.caller_stable_key.is_empty() {
            "$file".to_string()
        } else {
            call.caller_stable_key.clone()
        };

        let resolved = match &call.callee {
            CallReceiver::Bare(name) => {
                if let Some((fp, remote)) = bindings.get(name) {
                    if file_has_callable(batch.get(fp).unwrap_or(idx), remote) {
                        Some((fp.clone(), remote.clone()))
                    } else {
                        None
                    }
                } else if file_has_callable(idx, name) {
                    Some((path.to_string(), name.clone()))
                } else {
                    let via_star = star_files.iter().find_map(|fp| {
                        batch.get(fp).and_then(|fi| {
                            if file_has_callable(fi, name) {
                                Some((fp.clone(), name.clone()))
                            } else {
                                None
                            }
                        })
                    });
                    via_star.or_else(|| {
                        // Go/Python same-package (same directory) calls have no import.
                        // Rust files in one folder are separate modules — don't guess.
                        if lang == "rust" {
                            None
                        } else {
                            unique_callable_in_dir(batch, path, name)
                                .map(|fp| (fp, name.clone()))
                        }
                    })
                }
            }
            CallReceiver::Attr { object, name } => resolve_attr_call(
                object,
                name,
                path,
                idx,
                &bindings,
                batch,
            ),
        };

        let Some((tgt_path, tgt_leaf)) = resolved else {
            continue;
        };
        if tgt_path == path && !file_has_callable(idx, &tgt_leaf) {
            continue;
        }
        if tgt_path != path {
            let Some(fi) = batch.get(&tgt_path) else {
                continue;
            };
            if !file_has_callable(fi, &tgt_leaf) {
                continue;
            }
        }
        out.push(ExpectedRel {
            src_path: path.to_string(),
            src_key: caller,
            tgt_path: tgt_path.clone(),
            tgt_leaf,
            cross_file: tgt_path != path,
        });
    }
    out
}

fn resolve_attr_call(
    object: &CallReceiver,
    name: &str,
    path: &str,
    idx: &FileIndex,
    bindings: &HashMap<String, (String, String)>,
    batch: &HashMap<String, FileIndex>,
) -> Option<(String, String)> {
    match object {
        CallReceiver::Bare(root) if root == "self" || root == "this" || root == "Self" => {
            let class = idx
                .symbols
                .iter()
                .filter(|s| s.kind == NodeKind::Class)
                .map(|s| s.stable_key.as_str())
                .find(|c| file_has_callable(idx, &format!("{c}.{name}")))
                .map(|c| format!("{c}.{name}"))
                .unwrap_or_else(|| name.to_string());
            Some((path.to_string(), class))
        }
        CallReceiver::Bare(root) => {
            if let Some((fp, remote)) = bindings.get(root) {
                let fi = batch.get(fp)?;
                let member = format!("{remote}.{name}");
                if file_has_callable(fi, &member) {
                    return Some((fp.clone(), member));
                }
                if file_has_callable(fi, name) {
                    return Some((fp.clone(), name.to_string()));
                }
                return None;
            }
            if file_has_callable(idx, &format!("{root}.{name}")) {
                return Some((path.to_string(), format!("{root}.{name}")));
            }
            if let Some(ty) = idx
                .instance_fields
                .values()
                .find_map(|fields| fields.get(root))
            {
                if let Some((fp, remote)) = bindings.get(ty) {
                    let fi = batch.get(fp)?;
                    let member = format!("{remote}.{name}");
                    if file_has_callable(fi, &member) || file_has_callable(fi, name) {
                        return Some((
                            fp.clone(),
                            if file_has_callable(fi, &member) {
                                member
                            } else {
                                name.to_string()
                            },
                        ));
                    }
                }
            }
            None
        }
        CallReceiver::Attr { object: inner, name: mid } => {
            let (fp, owner) = resolve_attr_call(inner, mid, path, idx, bindings, batch)?;
            let fi = batch.get(&fp)?;
            let member = format!("{owner}.{name}");
            if file_has_callable(fi, &member) {
                Some((fp, member))
            } else if file_has_callable(fi, name) {
                Some((fp, name.to_string()))
            } else {
                None
            }
        }
    }
}

fn import_name_bindings(
    idx: &FileIndex,
    path: &str,
    mod_map: &HashMap<String, String>,
) -> HashMap<String, (String, String)> {
    let mut out = HashMap::new();
    for imp in &idx.imports {
        let Some(tp) = simple_resolve_module(&imp.module, path, mod_map) else {
            continue;
        };
        match imp.style {
            ImportStyle::Names => {
                for (remote, local) in &imp.names {
                    out.insert(local.clone(), (tp.clone(), remote.clone()));
                }
            }
            ImportStyle::ModuleOnly => {
                let local = crate::call_resolve::module_only_local_name(imp);
                out.insert(local.clone(), (tp, local));
            }
            ImportStyle::Star => {}
        }
    }
    out
}

fn simple_resolve_module(
    module: &str,
    from_path: &str,
    mod_map: &HashMap<String, String>,
) -> Option<String> {
    // Same resolver as ingest. A second suffix/same-dir guess here used to
    // expect `indexer_eval/graph.rs` for `crate::graph` from a nested file.
    crate::call_resolve::resolve_module_path_at(module, Some(from_path), mod_map)
}

fn unique_callable_in_dir(
    batch: &HashMap<String, FileIndex>,
    from_path: &str,
    name: &str,
) -> Option<String> {
    let dir = from_path.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    let mut hits: Vec<String> = batch
        .iter()
        .filter(|(p, idx)| {
            let pdir = p.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
            pdir == dir && file_has_callable(idx, name)
        })
        .map(|(p, _)| p.clone())
        .collect();
    hits.sort();
    hits.dedup();
    if hits.len() == 1 {
        hits.pop()
    } else {
        None
    }
}

fn file_has_symbol(idx: &FileIndex, name: &str) -> bool {
    let leaf = name.rsplit('.').next().unwrap_or(name);
    idx.symbols.iter().any(|s| {
        s.stable_key != "$file"
            && (s.stable_key == name
                || s.stable_key == leaf
                || s.stable_key.ends_with(&format!(".{leaf}"))
                || s.stable_key.ends_with(&format!(".{name}")))
    })
}

fn file_has_callable(idx: &FileIndex, name: &str) -> bool {
    if name == "$file" {
        return true;
    }
    let leaf = name.rsplit('.').next().unwrap_or(name);
    idx.symbols.iter().any(|s| {
        matches!(s.kind, NodeKind::Function | NodeKind::Class)
            && (s.stable_key == name
                || s.stable_key == leaf
                || s.stable_key.ends_with(&format!(".{leaf}"))
                || s.stable_key.ends_with(&format!(".{name}")))
    })
}

fn find_unique_class(batch: &HashMap<String, FileIndex>, name: &str) -> Option<String> {
    let mut hits = Vec::new();
    for (path, idx) in batch {
        if idx.symbols.iter().any(|s| s.kind == NodeKind::Class && (s.stable_key == name || s.stable_key.ends_with(&format!(".{name}")))) {
            hits.push(path.clone());
        }
    }
    if hits.len() == 1 {
        Some(hits.pop().unwrap())
    } else {
        None
    }
}

fn score_expected_imports(
    g: &InMemoryGraph,
    branch: BranchId,
    expected: &[ExpectedRel],
    xf: &mut CrossFileReport,
) {
    xf.expected_imports = expected.len();
    for exp in expected {
        xf.resolved_imports += usize::from(import_edge_exists(g, branch, exp));
        if !import_edge_exists(g, branch, exp) {
            xf.missed_imports
                .push(format!("{} → {}", exp.src_path, exp.tgt_path));
        }
    }
}

fn score_expected_calls(
    g: &InMemoryGraph,
    branch: BranchId,
    expected: &[ExpectedRel],
    xf: &mut CrossFileReport,
) {
    for exp in expected {
        let ok = call_edge_exists(g, branch, exp);
        if exp.cross_file {
            xf.expected_cross_file_calls += 1;
            if ok {
                xf.resolved_cross_file_calls += 1;
                xf.sample_resolved_cross_calls.push(format!(
                    "{}::{} → {}::{}",
                    exp.src_path, exp.src_key, exp.tgt_path, exp.tgt_leaf
                ));
            } else {
                xf.missed_cross_calls.push(format!(
                    "{}::{} → {}::{}",
                    exp.src_path, exp.src_key, exp.tgt_path, exp.tgt_leaf
                ));
            }
        } else {
            xf.expected_intra_file_calls += 1;
            if ok {
                xf.resolved_intra_file_calls += 1;
            } else {
                xf.missed_intra_calls.push(format!(
                    "{}::{} → {}",
                    exp.src_path, exp.src_key, exp.tgt_leaf
                ));
            }
        }
    }
}

fn score_expected_extends(
    g: &InMemoryGraph,
    branch: BranchId,
    expected: &[ExpectedRel],
    xf: &mut CrossFileReport,
) {
    for exp in expected {
        if !exp.cross_file {
            continue;
        }
        xf.expected_cross_file_extends += 1;
        if extends_edge_exists(g, branch, exp) {
            xf.resolved_cross_file_extends += 1;
        } else {
            xf.missed_extends.push(format!(
                "{}::{} extends {}::{}",
                exp.src_path, exp.src_key, exp.tgt_path, exp.tgt_leaf
            ));
        }
    }
}

fn import_edge_exists(g: &InMemoryGraph, branch: BranchId, exp: &ExpectedRel) -> bool {
    let Some(src) = find_revision(g, &exp.src_path, "$file") else {
        return file_has_import_from_any_hub(g, branch, &exp.src_path, &exp.tgt_path);
    };
    for e in g.outbound_edges(src.revision_id) {
        if e.ty != EdgeType::Imports {
            continue;
        }
        if target_in_file(g, branch, e.target_identity_id, &exp.tgt_path) {
            return true;
        }
    }
    file_has_import_from_any_hub(g, branch, &exp.src_path, &exp.tgt_path)
}

fn file_has_import_from_any_hub(
    g: &InMemoryGraph,
    branch: BranchId,
    src_path: &str,
    tgt_path: &str,
) -> bool {
    for rev in g.revisions() {
        if rev.file_path != src_path {
            continue;
        }
        for e in g.outbound_edges(rev.revision_id) {
            if e.ty == EdgeType::Imports && target_in_file(g, branch, e.target_identity_id, tgt_path)
            {
                return true;
            }
        }
    }
    false
}

fn call_edge_exists(g: &InMemoryGraph, branch: BranchId, exp: &ExpectedRel) -> bool {
    for rev in g.revisions() {
        if rev.file_path != exp.src_path {
            continue;
        }
        if !revision_matches_key(rev, &exp.src_key) {
            continue;
        }
        for e in g.outbound_edges(rev.revision_id) {
            if e.ty != EdgeType::Calls {
                continue;
            }
            if g.identity_kind(e.target_identity_id) == Some(NodeKind::Stub) {
                continue;
            }
            if !target_in_file(g, branch, e.target_identity_id, &exp.tgt_path) {
                continue;
            }
            if exp.tgt_leaf == "$file" {
                return true;
            }
            if let Some(t) = g.primary_revision_for_identity(branch, e.target_identity_id) {
                if name_leaf_match(&t.qualified_name, &exp.tgt_leaf) {
                    return true;
                }
            }
        }
    }
    false
}

fn extends_edge_exists(g: &InMemoryGraph, branch: BranchId, exp: &ExpectedRel) -> bool {
    for rev in g.revisions() {
        if rev.file_path != exp.src_path {
            continue;
        }
        if !revision_matches_key(rev, &exp.src_key) {
            continue;
        }
        for e in g.outbound_edges(rev.revision_id) {
            if e.ty != EdgeType::Extends {
                continue;
            }
            if target_in_file(g, branch, e.target_identity_id, &exp.tgt_path) {
                if let Some(t) = g.primary_revision_for_identity(branch, e.target_identity_id) {
                    if name_leaf_match(&t.qualified_name, &exp.tgt_leaf) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn target_in_file(
    g: &InMemoryGraph,
    branch: BranchId,
    iid: IdentityId,
    path: &str,
) -> bool {
    g.primary_revision_for_identity(branch, iid)
        .is_some_and(|t| t.file_path == path)
}

fn find_revision<'a>(g: &'a InMemoryGraph, path: &str, key: &str) -> Option<&'a NodeRevision> {
    g.revisions().find(|r| r.file_path == path && revision_matches_key(r, key))
}

fn revision_matches_key(rev: &NodeRevision, key: &str) -> bool {
    if key == "$file" {
        return rev.qualified_name == rev.file_path
            || rev.qualified_name.ends_with("::$file")
            || rev.qualified_name == format!("{}::$file", rev.file_path);
    }
    let q = &rev.qualified_name;
    q.ends_with(&format!("::{key}"))
        || q.ends_with(&format!(".{key}"))
        || q.rsplit("::").next() == Some(key)
        || q.rsplit('.').next() == Some(key.rsplit('.').next().unwrap_or(key))
}

fn name_leaf_match(qualified: &str, leaf: &str) -> bool {
    if leaf == "$file" {
        return true;
    }
    let qleaf = qualified
        .rsplit("::")
        .next()
        .unwrap_or(qualified)
        .rsplit('.')
        .next()
        .unwrap_or(qualified);
    let want = leaf.rsplit('.').next().unwrap_or(leaf);
    qleaf == want || qualified.contains(leaf) || qualified.ends_with(&format!(".{leaf}"))
}

fn ratio(n: usize, d: usize) -> f64 {
    if d == 0 {
        1.0
    } else {
        n as f64 / d as f64
    }
}
