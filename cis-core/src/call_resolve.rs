//! Cross-file call and import edge resolution (B1–B4).

use std::collections::{HashMap, HashSet};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::{
    EdgeResolution, EdgeType, GraphEdge, NodeKind, RevisionStatus, SourceSpan, SourceType,
};

use crate::index_model::{
    edge_id_bytes, stable_id_bytes, stable_rev_id_bytes, CallReceiver, FileIndex, ImportBinding,
    ImportStyle, ParsedCall, ParsedImport,
};

fn path_matches_stripped_module(path: &str, stripped: &str) -> bool {
    const EXTS: &[&str] = &[".ts", ".tsx", ".py", ".rs", ".go", ".js", ".jsx"];
    for ext in EXTS {
        let file = format!("{stripped}{ext}");
        if path == file || path.ends_with(&format!("/{file}")) {
            return true;
        }
    }
    false
}

/// Resolve an import module string to a repo-relative file path via `mod_map`.
fn resolve_module_path(module: &str, mod_map: &HashMap<String, String>) -> Option<String> {
    if let Some(p) = mod_map.get(module) {
        return Some(p.clone());
    }
    let mut stripped = module.trim();
    while stripped.starts_with("./") || stripped.starts_with("../") {
        stripped = stripped
            .trim_start_matches("./")
            .trim_start_matches("../");
    }
    if let Some(p) = mod_map.get(stripped) {
        return Some(p.clone());
    }
    // Path-segment / file-suffix match only — never bare `ends_with("utils")`
    // (that would incorrectly match `my_utils`, `test_utils`, …).
    mod_map
        .iter()
        .find(|(k, v)| {
            *k == stripped
                || k.ends_with(&format!("/{stripped}"))
                || k.ends_with(&format!(".{stripped}"))
                || path_matches_stripped_module(v, stripped)
        })
        .map(|(_, v)| v.clone())
}

pub(crate) fn build_import_bindings(
    imports: &[ParsedImport],
    mod_map: &HashMap<String, String>,
) -> HashMap<String, ImportBinding> {
    let mut out = HashMap::new();
    for imp in imports {
        let Some(tp) = resolve_module_path(&imp.module, mod_map) else {
            continue;
        };
        match imp.style {
            ImportStyle::Names => {
                for name in &imp.names {
                    out.insert(
                        name.clone(),
                        ImportBinding {
                            file_path: tp.clone(),
                            remote_name: name.clone(),
                        },
                    );
                }
            }
            ImportStyle::ModuleOnly => {
                let local = imp.module.rsplit('.').next().unwrap_or(&imp.module).to_string();
                out.insert(
                    local.clone(),
                    ImportBinding {
                        file_path: tp.clone(),
                        remote_name: local,
                    },
                );
            }
            ImportStyle::Star => {}
        }
    }
    out
}

pub(crate) fn caller_enclosing_class(caller_stable_key: &str) -> Option<&str> {
    caller_stable_key
        .find('.')
        .map(|i| &caller_stable_key[..i])
        .filter(|c| *c != "$file")
}

pub(crate) fn class_symbol_in_file(index: &FileIndex, type_name: &str) -> bool {
    index.symbols.iter().any(|s| {
        s.kind == NodeKind::Class && (s.stable_key == type_name || s.stable_key.ends_with(&format!(".{type_name}")))
    })
}

pub(crate) fn resolve_type_location(
    type_name: &str,
    path: &str,
    index: &FileIndex,
    import_bindings: &HashMap<String, ImportBinding>,
) -> (String, String) {
    if let Some(b) = import_bindings.get(type_name) {
        return (b.file_path.clone(), b.remote_name.clone());
    }
    if class_symbol_in_file(index, type_name) {
        return (path.to_string(), type_name.to_string());
    }
    (path.to_string(), type_name.to_string())
}

/// Infer the class/type `(file_path, owner_symbol)` for a receiver expression.
pub(crate) fn infer_type_of_receiver(
    receiver: &CallReceiver,
    caller_stable_key: &str,
    caller_class: &str,
    path: &str,
    index: &FileIndex,
    import_bindings: &HashMap<String, ImportBinding>,
) -> Option<(String, String)> {
    match receiver {
        CallReceiver::Bare(name) if name == "self" => Some((
            path.to_string(),
            caller_class.to_string(),
        )),
        CallReceiver::Bare(name) => {
            if let Some(type_name) = index
                .function_locals
                .get(caller_stable_key)
                .and_then(|locals| locals.get(name))
            {
                return Some(resolve_type_location(
                    type_name,
                    path,
                    index,
                    import_bindings,
                ));
            }
            if let Some(b) = import_bindings.get(name) {
                Some((b.file_path.clone(), b.remote_name.clone()))
            } else if class_symbol_in_file(index, name) {
                Some((path.to_string(), name.clone()))
            } else {
                None
            }
        }
        CallReceiver::Attr { object, name } => {
            let (_parent_file, parent_type) = infer_type_of_receiver(
                object,
                caller_stable_key,
                caller_class,
                path,
                index,
                import_bindings,
            )?;
            let field_type = index
                .instance_fields
                .get(&parent_type)
                .and_then(|fields| fields.get(name))?;
            Some(resolve_type_location(
                field_type,
                path,
                index,
                import_bindings,
            ))
        }
    }
}

pub(crate) fn resolve_call_same_file(path: &str, index: &FileIndex, call: &ParsedCall) -> Option<IdentityId> {
    match &call.callee {
        CallReceiver::Bare(name) => resolve_symbol_in_file_index(path, name, index),
        CallReceiver::Attr { object, name } => {
            if let CallReceiver::Bare(owner) = object.as_ref() {
                if let Some(id) = resolve_member_in_file_index(path, owner, name, index) {
                    return Some(id);
                }
            }
            resolve_symbol_in_file_index(path, name, index)
        }
    }
}

pub(crate) fn resolve_member_in_file_index(
    path: &str,
    owner: &str,
    member: &str,
    index: &FileIndex,
) -> Option<IdentityId> {
    let member_key = format!("{owner}.{member}");
    resolve_symbol_in_file_index(path, &member_key, index)
}

pub(crate) fn resolve_member_in_module(
    file_path: &str,
    owner: &str,
    member: &str,
    branch: BranchId,
    graph: Option<&crate::graph::InMemoryGraph>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let member_key = format!("{owner}.{member}");
    resolve_symbol_in_module(file_path, &member_key, branch, graph, batch_indexes)
}

pub(crate) fn resolve_symbol_in_file_index(path: &str, simple_name: &str, index: &FileIndex) -> Option<IdentityId> {
    let exact = format!("{path}::{simple_name}");
    // Prefer canonical (empty disambiguator) when multiple symbols share a name.
    let mut fallback: Option<IdentityId> = None;
    for sym in &index.symbols {
        if sym.qualified_name != exact {
            continue;
        }
        let id = IdentityId(stable_id_bytes("id", path, &sym.identity_key()));
        if sym.disambiguator.is_empty() {
            return Some(id);
        }
        if fallback.is_none() {
            fallback = Some(id);
        }
    }
    if let Some(id) = fallback {
        return Some(id);
    }
    for sym in &index.symbols {
        if sym.kind != NodeKind::Function && sym.kind != NodeKind::Class {
            continue;
        }
        if sym.stable_key == simple_name || sym.stable_key.ends_with(&format!(".{simple_name}")) {
            let id = IdentityId(stable_id_bytes("id", path, &sym.identity_key()));
            if sym.disambiguator.is_empty() {
                return Some(id);
            }
            if fallback.is_none() {
                fallback = Some(id);
            }
        }
    }
    fallback
}

/// Revision / identity hash key for a symbol looked up by display `stable_key`.
/// Prefers the canonical (empty disambiguator) entry.
fn identity_key_for_owner(index: &FileIndex, stable_key: &str) -> String {
    index
        .symbols
        .iter()
        .find(|s| s.stable_key == stable_key && s.disambiguator.is_empty())
        .or_else(|| index.symbols.iter().find(|s| s.stable_key == stable_key))
        .map(|s| s.identity_key())
        .unwrap_or_else(|| stable_key.to_string())
}

/// Prefer a Class symbol when resolving extends / class owners.
fn identity_key_for_class(index: &FileIndex, class_key: &str) -> String {
    index
        .symbols
        .iter()
        .find(|s| s.kind == NodeKind::Class && s.stable_key == class_key)
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.stable_key == class_key && s.disambiguator.is_empty())
        })
        .or_else(|| index.symbols.iter().find(|s| s.stable_key == class_key))
        .map(|s| s.identity_key())
        .unwrap_or_else(|| class_key.to_string())
}

pub(crate) fn resolve_symbol_in_graph(
    graph: &crate::graph::InMemoryGraph,
    branch: BranchId,
    file_path: &str,
    simple_name: &str,
) -> Option<IdentityId> {
    let exact = format!("{file_path}::{simple_name}");
    let canonical = IdentityId(stable_id_bytes("id", file_path, simple_name));
    let mut fallback: Option<IdentityId> = None;
    for r in graph.revisions() {
        if r.branch_id != branch || r.file_path != file_path {
            continue;
        }
        if !matches!(r.status, RevisionStatus::Active) {
            continue;
        }
        if r.qualified_name == exact {
            if r.identity_id == canonical {
                return Some(r.identity_id);
            }
            if fallback.is_none() {
                fallback = Some(r.identity_id);
            }
        }
    }
    if let Some(id) = fallback {
        return Some(id);
    }
    for r in graph.revisions() {
        if r.branch_id != branch || r.file_path != file_path {
            continue;
        }
        if !matches!(r.status, RevisionStatus::Active) {
            continue;
        }
        if r.qualified_name.ends_with(&format!(".{simple_name}")) {
            if r.identity_id == canonical {
                return Some(r.identity_id);
            }
            if fallback.is_none() {
                fallback = Some(r.identity_id);
            }
        }
    }
    fallback
}

pub(crate) fn resolve_symbol_in_module(
    file_path: &str,
    simple_name: &str,
    branch: BranchId,
    graph: Option<&crate::graph::InMemoryGraph>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    if let Some(g) = graph {
        if let Some(id) = resolve_symbol_in_graph(g, branch, file_path, simple_name) {
            return Some(id);
        }
    }
    batch_indexes
        .get(file_path)
        .and_then(|idx| resolve_symbol_in_file_index(file_path, simple_name, idx))
}

pub(crate) fn callee_in_import_scope(imp: &ParsedImport, callee_simple: &str) -> bool {
    match imp.style {
        ImportStyle::Star => true,
        ImportStyle::Names => imp.names.iter().any(|n| n == callee_simple),
        ImportStyle::ModuleOnly => false,
    }
}

pub(crate) fn resolve_call_target(
    path: &str,
    branch: BranchId,
    index: &FileIndex,
    call: &ParsedCall,
    mod_map: &HashMap<String, String>,
    graph: Option<&crate::graph::InMemoryGraph>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let import_bindings = build_import_bindings(&index.imports, mod_map);

    if let Some(id) = resolve_call_same_file(path, index, call) {
        return Some(id);
    }

    match &call.callee {
        CallReceiver::Bare(name) => {
            if let Some(binding) = import_bindings.get(name) {
                if let Some(id) = resolve_symbol_in_module(
                    &binding.file_path,
                    &binding.remote_name,
                    branch,
                    graph,
                    batch_indexes,
                ) {
                    return Some(id);
                }
            }
            for imp in &index.imports {
                if !callee_in_import_scope(imp, name) {
                    continue;
                }
                let Some(target_path) = resolve_module_path(&imp.module, mod_map) else {
                    continue;
                };
                if target_path == path {
                    continue;
                }
                if let Some(id) = resolve_symbol_in_module(
                    &target_path,
                    name,
                    branch,
                    graph,
                    batch_indexes,
                ) {
                    return Some(id);
                }
            }
        }
        CallReceiver::Attr { object, name } => {
            if let Some(caller_class) = caller_enclosing_class(&call.caller_stable_key) {
                if let Some((owner_file, owner_sym)) = infer_type_of_receiver(
                    object,
                    &call.caller_stable_key,
                    caller_class,
                    path,
                    index,
                    &import_bindings,
                ) {
                    if let Some(id) = resolve_member_in_module(
                        &owner_file,
                        &owner_sym,
                        name,
                        branch,
                        graph,
                        batch_indexes,
                    ) {
                        return Some(id);
                    }
                }
            }
            if let Some(base) = object.root_bare_name() {
                if let Some(binding) = import_bindings.get(base) {
                    if let Some(id) = resolve_member_in_module(
                        &binding.file_path,
                        &binding.remote_name,
                        name,
                        branch,
                        graph,
                        batch_indexes,
                    ) {
                        return Some(id);
                    }
                }
                if let Some(id) = resolve_member_in_module(
                    path,
                    base,
                    name,
                    branch,
                    graph,
                    batch_indexes,
                ) {
                    return Some(id);
                }
            }
            let leaf = name.as_str();
            for imp in &index.imports {
                if !callee_in_import_scope(imp, leaf) {
                    continue;
                }
                let Some(target_path) = resolve_module_path(&imp.module, mod_map) else {
                    continue;
                };
                if target_path == path {
                    continue;
                }
                if let Some(id) = resolve_symbol_in_module(
                    &target_path,
                    leaf,
                    branch,
                    graph,
                    batch_indexes,
                ) {
                    return Some(id);
                }
            }
        }
    }
    None
}

pub(crate) fn ast_edge_resolution() -> EdgeResolution {
    EdgeResolution {
        target_signature_hash: [0u8; 32],
        resolver: SourceType::Ast,
        last_validation_ms: 0,
    }
}

pub(crate) fn import_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    label: &str,
    anchor: SourceSpan,
) -> GraphEdge {
    GraphEdge {
        edge_id: edge_id_bytes("imp", path, src_rid, label, anchor),
        ty: EdgeType::Imports,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor,
    }
}

pub(crate) fn call_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    label: &str,
    anchor: SourceSpan,
) -> GraphEdge {
    GraphEdge {
        edge_id: edge_id_bytes("cal", path, src_rid, label, anchor),
        ty: EdgeType::Calls,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor,
    }
}

pub(crate) fn extends_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    class_key: &str,
    base_name: &str,
    anchor: SourceSpan,
) -> GraphEdge {
    let label = format!("{class_key}:extends:{base_name}");
    GraphEdge {
        edge_id: edge_id_bytes("ext", path, src_rid, &label, anchor),
        ty: EdgeType::Extends,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor,
    }
}

pub(crate) fn use_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    owner_key: &str,
    type_name: &str,
    anchor: SourceSpan,
) -> GraphEdge {
    let label = format!("{owner_key}:uses:{type_name}");
    GraphEdge {
        edge_id: edge_id_bytes("use", path, src_rid, &label, anchor),
        ty: EdgeType::Uses,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor,
    }
}

pub(crate) fn resolve_type_name_to_identity(
    path: &str,
    branch: BranchId,
    type_name: &str,
    index: &FileIndex,
    mod_map: &HashMap<String, String>,
    graph: Option<&crate::graph::InMemoryGraph>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let import_bindings = build_import_bindings(&index.imports, mod_map);
    let (owner_file, simple) = resolve_type_location(type_name, path, index, &import_bindings);
    resolve_symbol_in_module(&owner_file, &simple, branch, graph, batch_indexes)
}

pub(crate) fn attach_import_and_call_edges(
    path: &str,
    branch: BranchId,
    index: &FileIndex,
    mod_map: &HashMap<String, String>,
    graph: Option<&crate::graph::InMemoryGraph>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> HashMap<NodeRevisionId, Vec<GraphEdge>> {
    let mut edge_map: HashMap<NodeRevisionId, Vec<GraphEdge>> = HashMap::new();
    let file_hub_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
    for imp in &index.imports {
        let Some(tp) = resolve_module_path(&imp.module, mod_map) else {
            continue;
        };
        if tp == path {
            continue;
        }
        let hub = IdentityId(stable_id_bytes("file", &tp, "$hub"));
        match imp.style {
            ImportStyle::ModuleOnly | ImportStyle::Star => {
                edge_map.entry(file_hub_rid).or_default().push(import_edge(
                    file_hub_rid,
                    hub,
                    path,
                    &imp.module,
                    imp.span,
                ));
            }
            ImportStyle::Names => {
                for name in &imp.names {
                    let tiid = resolve_symbol_in_module(&tp, name, branch, graph, batch_indexes)
                        .unwrap_or(hub);
                    let label = format!("{}:{}", imp.module, name);
                    edge_map.entry(file_hub_rid).or_default().push(import_edge(
                        file_hub_rid,
                        tiid,
                        path,
                        &label,
                        imp.span,
                    ));
                }
            }
        }
    }
    for ext in &index.extends {
        let class_ikey = identity_key_for_class(index, &ext.class_stable_key);
        let class_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, &class_ikey));
        if let Some(tiid) = resolve_type_name_to_identity(
            path,
            branch,
            &ext.base_name,
            index,
            mod_map,
            graph,
            batch_indexes,
        ) {
            edge_map.entry(class_rid).or_default().push(extends_edge(
                class_rid,
                tiid,
                path,
                &ext.class_stable_key,
                &ext.base_name,
                ext.span,
            ));
        }
    }
    for u in &index.uses {
        let owner_ikey = identity_key_for_owner(index, &u.owner_stable_key);
        let owner_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, &owner_ikey));
        if let Some(tiid) = resolve_type_name_to_identity(
            path,
            branch,
            &u.type_name,
            index,
            mod_map,
            graph,
            batch_indexes,
        ) {
            edge_map.entry(owner_rid).or_default().push(use_edge(
                owner_rid,
                tiid,
                path,
                &u.owner_stable_key,
                &u.type_name,
                u.span,
            ));
        }
    }
    for call in &index.calls {
        let caller_ikey = identity_key_for_owner(index, &call.caller_stable_key);
        let caller_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, &caller_ikey));
        if let Some(tiid) = resolve_call_target(
            path,
            branch,
            index,
            call,
            mod_map,
            graph,
            batch_indexes,
        ) {
            let label = format!("{}->{}", call.caller_stable_key, call.callee.label());
            edge_map.entry(caller_rid).or_default().push(call_edge(
                caller_rid,
                tiid,
                path,
                &label,
                call.span,
            ));
        }
    }
    edge_map
}

/// Build `module → file path` map for import edge resolution during merge regen.
pub fn module_map_from_paths(paths: impl IntoIterator<Item = impl AsRef<str>>) -> HashMap<String, String> {
    module_map_for_paths(paths, &crate::language_indexer::default_indexers())
}

/// Language-dispatched module map for edge regeneration.
pub fn module_map_for_paths(
    paths: impl IntoIterator<Item = impl AsRef<str>>,
    indexers: &[Box<dyn crate::language_indexer::LanguageIndexer>],
) -> HashMap<String, String> {
    paths
        .into_iter()
        .filter_map(|p| {
            let s = p.as_ref().to_string();
            crate::language_indexer::indexer_for_path(&s, indexers)
                .map(|idx| (idx.module_key(&s), s))
        })
        .collect()
}

/// Active revision file paths on a branch (language-agnostic).
pub fn paths_on_branch(
    graph: &crate::graph::InMemoryGraph,
    branch: BranchId,
) -> HashSet<String> {
    graph.active_file_paths_on_branch(branch)
}

/// Collect distinct `.py` file paths with active revisions on a branch.
pub fn python_paths_on_branch(graph: &crate::graph::InMemoryGraph, branch: BranchId) -> HashSet<String> {
    paths_on_branch(graph, branch)
        .into_iter()
        .filter(|p| p.ends_with(".py"))
        .collect()
}

/// Re-parse a Python file and build outbound edges per revision (ingest-compatible IDs).
pub fn regen_edges_for_python_file(
    path: &str,
    content: &str,
    branch: BranchId,
    mod_map: &HashMap<String, String>,
) -> Result<HashMap<NodeRevisionId, Vec<GraphEdge>>, &'static str> {
    regen_edges_for_python_file_with_graph(path, content, branch, mod_map, None)
}

/// Like [`regen_edges_for_python_file`] but resolves cross-file **`Calls`** via the live graph.
pub fn regen_edges_for_python_file_with_graph(
    path: &str,
    content: &str,
    branch: BranchId,
    mod_map: &HashMap<String, String>,
    graph: Option<&crate::graph::InMemoryGraph>,
) -> Result<HashMap<NodeRevisionId, Vec<GraphEdge>>, &'static str> {
    regen_edges_for_file_with_graph(
        path,
        content,
        branch,
        mod_map,
        graph,
        &crate::language_indexer::default_indexers(),
    )
}

/// Language-dispatched edge regeneration for merge Phase C and batch ingest.
pub fn regen_edges_for_file_with_graph(
    path: &str,
    content: &str,
    branch: BranchId,
    mod_map: &HashMap<String, String>,
    graph: Option<&crate::graph::InMemoryGraph>,
    indexers: &[Box<dyn crate::language_indexer::LanguageIndexer>],
) -> Result<HashMap<NodeRevisionId, Vec<GraphEdge>>, &'static str> {
    let indexer = crate::language_indexer::indexer_for_path(path, indexers)
        .ok_or("unsupported language")?;
    let index = indexer
        .index_file(path, content)
        .map_err(|_| "parse failed")?;
    let mut batch_indexes = HashMap::new();
    batch_indexes.insert(path.to_string(), index.clone());
    Ok(attach_import_and_call_edges(
        path,
        branch,
        &index,
        mod_map,
        graph,
        &batch_indexes,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cis_wal::BranchId;

    use crate::index_model::{CallReceiver, FileIndex, ImportStyle, ParsedCall, ParsedImport, ParsedSymbol};

    use super::*;

    fn span() -> SourceSpan {
        SourceSpan {
            start_line: 1,
            start_col: 1,
            end_line: 1,
            end_col: 1,
        }
    }

    #[test]
    fn resolve_module_path_strips_relative_prefix() {
        let mut mod_map = HashMap::new();
        mod_map.insert("util".to_string(), "src/util.ts".to_string());
        assert_eq!(
            resolve_module_path("./util", &mod_map),
            Some("src/util.ts".to_string())
        );
    }

    #[test]
    fn resolve_module_path_does_not_match_suffix_sibling() {
        let mut mod_map = HashMap::new();
        mod_map.insert("my_utils".to_string(), "lib/my_utils.py".to_string());
        mod_map.insert("test_utils".to_string(), "lib/test_utils.py".to_string());
        assert_eq!(
            resolve_module_path("utils", &mod_map),
            None,
            "bare ends_with must not pick my_utils/test_utils"
        );
    }

    #[test]
    fn resolve_module_path_matches_path_suffix_file() {
        let mut mod_map = HashMap::new();
        mod_map.insert("pkg.helpers".to_string(), "pkg/helpers.py".to_string());
        assert_eq!(
            resolve_module_path("helpers", &mod_map),
            Some("pkg/helpers.py".to_string())
        );
    }

    #[test]
    fn build_import_bindings_maps_named_imports() {
        let mut mod_map = HashMap::new();
        mod_map.insert("helpers".to_string(), "lib/helpers.py".to_string());
        let imports = vec![ParsedImport {
            module: "helpers".to_string(),
            style: ImportStyle::Names,
            names: vec!["run".to_string()],
            span: span(),
        }];
        let bindings = build_import_bindings(&imports, &mod_map);
        assert_eq!(bindings.get("run").unwrap().file_path, "lib/helpers.py");
        assert_eq!(bindings.get("run").unwrap().remote_name, "run");
    }

    #[test]
    fn resolve_call_target_cross_file_import() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "main.py";
        let callee_path = "helpers.py";
        let mut mod_map = HashMap::new();
        mod_map.insert("helpers".to_string(), callee_path.to_string());

        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "run".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::run"),
            kind: NodeKind::Function,
            span: span(),
        });

        let mut caller_idx = FileIndex::default();
        caller_idx.imports.push(ParsedImport {
            module: "helpers".to_string(),
            style: ImportStyle::Names,
            names: vec!["run".to_string()],
            span: span(),
        });
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "main".to_string(),
            callee: CallReceiver::bare("run"),
            span: span(),
        });

        let mut batch_indexes = HashMap::new();
        batch_indexes.insert(callee_path.to_string(), callee_idx);

        let call = &caller_idx.calls[0];
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            call,
            &mod_map,
            None,
            &batch_indexes,
        );
        assert!(target.is_some());
    }

    #[test]
    fn resolve_call_same_file_prefers_exact_qualified_name() {
        let path = "mod.py";
        let mut index = FileIndex::default();
        index.symbols.push(ParsedSymbol {
            stable_key: "foo".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::foo"),
            kind: NodeKind::Function,
            span: span(),
        });
        let call = ParsedCall {
            caller_stable_key: "bar".to_string(),
            callee: CallReceiver::bare("foo"),
            span: span(),
        };
        assert!(resolve_call_same_file(path, &index, &call).is_some());
    }

    #[test]
    fn distinct_edge_ids_for_duplicate_call_sites() {
        let branch = BranchId([0u8; 16]);
        let path = "main.py";
        let caller_sk = "foo";
        let span_a = SourceSpan {
            start_line: 10,
            start_col: 4,
            end_line: 10,
            end_col: 12,
        };
        let span_b = SourceSpan {
            start_line: 20,
            start_col: 4,
            end_line: 20,
            end_col: 12,
        };
        let caller_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, caller_sk));
        let target = IdentityId(stable_id_bytes("id", path, "bar"));
        let e1 = call_edge(caller_rid, target, path, "foo->bar", span_a);
        let e2 = call_edge(caller_rid, target, path, "foo->bar", span_b);
        assert_ne!(e1.edge_id, e2.edge_id);
    }

    #[test]
    fn file_hub_only_import_edges() {
        let branch = BranchId([0u8; 16]);
        let path = "main.py";
        let mut mod_map = HashMap::new();
        mod_map.insert("helpers".to_string(), "helpers.py".to_string());
        let mut index = FileIndex::default();
        index.symbols.push(ParsedSymbol {
            stable_key: "run".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::run"),
            kind: NodeKind::Function,
            span: span(),
        });
        index.imports.push(ParsedImport {
            module: "helpers".to_string(),
            style: ImportStyle::Names,
            names: vec!["helper".to_string()],
            span: span(),
        });
        let edges = attach_import_and_call_edges(path, branch, &index, &mod_map, None, &HashMap::new());
        let file_hub = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
        assert!(edges.contains_key(&file_hub));
        assert_eq!(edges.get(&file_hub).map(|v| v.len()), Some(1));
        let run_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "run"));
        assert!(!edges.contains_key(&run_rid));
    }

    #[test]
    fn regen_edges_for_typescript_file() {
        let branch = BranchId([0u8; 16]);
        let path = "app.ts";
        let content = "import { helper } from './util';\nexport function run() { helper(); }\n";
        let indexers = crate::language_indexer::default_indexers();
        let mod_map = module_map_for_paths(["app.ts", "util.ts"], &indexers);
        let edges = regen_edges_for_file_with_graph(
            path,
            content,
            branch,
            &mod_map,
            None,
            &indexers,
        )
        .expect("ts regen");
        let file_hub = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
        assert!(
            edges.contains_key(&file_hub),
            "typescript file should produce import edges via language dispatch"
        );
    }
}
