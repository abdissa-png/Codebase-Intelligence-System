//! Cross-file call and import edge resolution (B1–B4).

use std::collections::{HashMap, HashSet};

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::graph::{
    EdgeResolution, EdgeType, GraphEdge, NodeKind, RevisionStatus, SourceSpan, SourceType,
};

use crate::index_model::{
    edge_id_bytes, scope_relative_edge_pos, stable_id_bytes, stable_rev_id_bytes, CallReceiver,
    FileIndex, ImportBinding, ImportStyle, ParsedCall, ParsedImport,
};

/// Map `dir/foo.rs` → `dir.foo` so Rust imports align with module-map keys.
///
/// Lives here, not in `rust_indexer`, because CI's `tree-sitter` job does not
/// enable `ts-rust` and still resolves `crate::` paths.
pub(crate) fn path_to_rust_module_key(rel_path: &str) -> String {
    let base = rel_path.trim_end_matches(".rs").replace('/', ".");
    if base.ends_with(".mod") {
        base.trim_end_matches(".mod").to_string()
    } else if base.ends_with(".lib") || base.ends_with(".main") {
        base.rsplit_once('.').map(|(parent, _)| parent.to_string()).unwrap_or(base)
    } else {
        base
    }
}

const MODULE_EXTS: &[&str] = &[
    ".ts", ".tsx", ".py", ".rs", ".go", ".js", ".jsx", ".java", ".cs", ".c",
    ".h", ".cpp", ".cxx", ".cc", ".hpp", ".hxx", ".hh",
];

fn path_matches_stripped_module(path: &str, stripped: &str) -> bool {
    let stripped = stripped.trim();
    if stripped.is_empty() {
        return false;
    }
    if let Some(stem) = stem_if_filename(stripped) {
        if path == stripped || path.ends_with(&format!("/{stripped}")) {
            return true;
        }
        for ext in MODULE_EXTS {
            let file = format!("{stem}{ext}");
            if path == file || path.ends_with(&format!("/{file}")) {
                return true;
            }
        }
        return false;
    }
    for ext in MODULE_EXTS {
        let file = format!("{stripped}{ext}");
        if path == file || path.ends_with(&format!("/{file}")) {
            return true;
        }
    }
    false
}

fn stem_if_filename(name: &str) -> Option<&str> {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    MODULE_EXTS
        .iter()
        .find(|ext| base.ends_with(*ext))
        .map(|ext| &base[..base.len() - ext.len()])
}

fn path_dir(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

/// Go / Python / Java / C# / C(++) treat same-directory files as one package (or link set).
/// Rust modules in one folder are still distinct crates/modules — do not guess.
fn allows_same_dir_resolution(path: &str) -> bool {
    matches!(
        path.rsplit('.').next().unwrap_or(""),
        "go" | "py" | "java" | "cs" | "c" | "h" | "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "hh"
    )
}

fn is_stdlib_type_name(name: &str) -> bool {
    matches!(
        name,
        "Object"
            | "Exception"
            | "RuntimeException"
            | "Throwable"
            | "Error"
            | "String"
            | "Integer"
            | "Boolean"
            | "Long"
            | "Double"
            | "Float"
            | "List"
            | "Map"
            | "Set"
            | "Optional"
            | "Iterable"
            | "Iterator"
            | "Collection"
            | "Comparable"
            | "Serializable"
            | "Cloneable"
            | "Runnable"
            | "Override"
            | "Deprecated"
            | "IDisposable"
            | "IEnumerable"
            | "IEnumerator"
            | "IList"
            | "IDictionary"
            | "Attribute"
            | "Enum"
            | "Delegate"
            | "EventArgs"
            | "Task"
            | "Action"
            | "Func"
            | "object"
            | "string"
            | "error"
            | "any"
            | "interface"
            | "type"
    )
}

fn symbol_name_matches(stable_key: &str, name: &str) -> bool {
    let leaf = name.rsplit('.').next().unwrap_or(name);
    stable_key == name
        || stable_key == leaf
        || stable_key.ends_with(&format!(".{leaf}"))
        || stable_key.ends_with(&format!(".{name}"))
}

fn file_has_callable(index: &FileIndex, name: &str) -> bool {
    index.symbols.iter().any(|s| {
        matches!(s.kind, NodeKind::Function | NodeKind::Class) && symbol_name_matches(&s.stable_key, name)
    })
}

fn unique_paths_in_dir(
    batch: &HashMap<String, FileIndex>,
    from_path: &str,
    pred: impl Fn(&FileIndex) -> bool,
) -> Option<String> {
    let dir = path_dir(from_path);
    let mut hits: Vec<&String> = batch
        .iter()
        .filter(|(p, idx)| path_dir(p) == dir && pred(idx))
        .map(|(p, _)| p)
        .collect();
    hits.sort();
    hits.dedup();
    if hits.len() == 1 {
        Some(hits[0].clone())
    } else {
        None
    }
}

fn unique_callable_in_dir(
    batch: &HashMap<String, FileIndex>,
    from_path: &str,
    name: &str,
) -> Option<String> {
    unique_paths_in_dir(batch, from_path, |idx| file_has_callable(idx, name))
}

fn unique_class_in_scope(
    batch: &HashMap<String, FileIndex>,
    from_path: &str,
    name: &str,
) -> Option<String> {
    if is_stdlib_type_name(name) {
        return None;
    }
    let is_class = |idx: &FileIndex| {
        idx.symbols.iter().any(|s| {
            s.kind == NodeKind::Class && symbol_name_matches(&s.stable_key, name)
        })
    };
    if let Some(p) = unique_paths_in_dir(batch, from_path, is_class) {
        return Some(p);
    }
    let mut all: Vec<&String> = batch
        .iter()
        .filter(|(_, idx)| is_class(idx))
        .map(|(p, _)| p)
        .collect();
    all.sort();
    all.dedup();
    if all.len() == 1 {
        Some(all[0].clone())
    } else {
        None
    }
}

fn resolve_relative_or_include(
    module: &str,
    from_path: &str,
    mod_map: &HashMap<String, String>,
) -> Option<String> {
    let raw = module
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '<' || c == '>');
    if raw.is_empty() {
        return None;
    }
    let dir = path_dir(from_path);
    let joined = if dir.is_empty() {
        raw.to_string()
    } else {
        format!("{dir}/{raw}")
    };
    for v in mod_map.values() {
        if v == &joined || v.as_str() == raw {
            return Some(v.clone());
        }
    }
    let base = raw.rsplit('/').next().unwrap_or(raw);
    let mut hits: Vec<&String> = mod_map
        .values()
        .filter(|v| path_matches_stripped_module(v, base) || v.ends_with(&format!("/{base}")))
        .collect();
    hits.sort();
    hits.dedup();
    if hits.len() == 1 {
        return Some(hits[0].clone());
    }
    if hits.len() > 1 {
        let same_dir: Vec<&String> = hits
            .iter()
            .copied()
            .filter(|v| path_dir(v) == dir)
            .collect();
        if same_dir.len() == 1 {
            return Some(same_dir[0].clone());
        }
    }
    None
}

/// Strip Rust path prefixes (`crate.` / `self.` / leading `super.`) so imports like
/// `crate.coordinator` match mod-map keys such as `cis-core.src.coordinator`.
///
/// This is only a fallback when the import is not anchored to a source file.
/// Anchored `crate` / `self` / `super` paths are resolved from the importing file
/// so a bare suffix cannot collide (`graph` vs `indexer_eval/graph`).
fn strip_rust_path_prefixes(module: &str) -> &str {
    let mut s = module;
    loop {
        if let Some(rest) = s.strip_prefix("crate.") {
            s = rest;
            continue;
        }
        if let Some(rest) = s.strip_prefix("self.") {
            s = rest;
            continue;
        }
        if let Some(rest) = s.strip_prefix("super.") {
            s = rest;
            continue;
        }
        break;
    }
    s
}

fn is_rust_prelude_module(module: &str) -> bool {
    module == "std"
        || module == "core"
        || module == "alloc"
        || module.starts_with("std.")
        || module.starts_with("core.")
        || module.starts_with("alloc.")
}

fn rust_path_is_anchored(module: &str) -> bool {
    module == "crate"
        || module == "self"
        || module == "super"
        || module.starts_with("crate.")
        || module.starts_with("self.")
        || module.starts_with("super.")
}

fn python_import_root_prefix(prefix: &str) -> bool {
    if prefix.is_empty() || prefix.contains('.') {
        return false;
    }
    !matches!(prefix, "test" | "tests" | "testing" | "__pycache__")
}

/// Absolute `import flask` matches `flask` or `src.flask`, not
/// `tests...inner2.flask` and not `src.flask.typing` for `import typing`.
fn python_absolute_module(module: &str, mod_map: &HashMap<String, String>) -> Option<String> {
    let module = module.trim();
    if module.is_empty() || module.starts_with('.') {
        return None;
    }
    let suffix = format!(".{module}");
    let mut hits: Vec<String> = mod_map
        .iter()
        .filter(|(k, _)| {
            k.strip_suffix(suffix.as_str())
                .is_some_and(python_import_root_prefix)
        })
        .map(|(_, v)| v.clone())
        .collect();
    hits.sort();
    hits.dedup();
    if hits.len() == 1 {
        hits.pop()
    } else {
        None
    }
}

fn python_package_of_file(from_path: &str) -> String {
    let key = crate::python_indexer::path_to_python_module_key(from_path);
    if from_path.ends_with("/__init__.py") || from_path == "__init__.py" {
        return key;
    }
    match key.rfind('.') {
        Some(i) => key[..i].to_string(),
        None => String::new(),
    }
}

/// `from .cli import AppGroup` and `from ..helpers import x`, counted in dots.
fn resolve_python_relative(
    module: &str,
    from_path: &str,
    mod_map: &HashMap<String, String>,
) -> Option<String> {
    let module = module.trim();
    let mut dots = 0usize;
    let rest = module.trim_start_matches(|c| {
        if c == '.' {
            dots += 1;
            true
        } else {
            false
        }
    });
    if dots == 0 {
        return None;
    }
    let mut base = python_package_of_file(from_path);
    for _ in 1..dots {
        if let Some(i) = base.rfind('.') {
            base.truncate(i);
        } else {
            base.clear();
            break;
        }
    }
    let target = if rest.is_empty() {
        base
    } else if base.is_empty() {
        rest.to_string()
    } else {
        format!("{base}.{rest}")
    };
    if target.is_empty() {
        None
    } else {
        mod_map.get(&target).cloned()
    }
}

fn is_script_module_path(path: &str) -> bool {
    path.ends_with(".js")
        || path.ends_with(".jsx")
        || path.ends_with(".mjs")
        || path.ends_with(".cjs")
        || path.ends_with(".ts")
        || path.ends_with(".tsx")
}

fn normalize_rel_path(path: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            parts.pop();
            continue;
        }
        parts.push(part);
    }
    parts.join("/")
}

/// `require("./application")` and `from './util'` omit or include the extension.
fn resolve_script_relative(
    module: &str,
    from_path: &str,
    mod_map: &HashMap<String, String>,
) -> Option<String> {
    let raw = module.trim().trim_matches(|c| c == '"' || c == '\'' || c == '`');
    if !raw.starts_with('.') {
        return None;
    }
    let dir = path_dir(from_path);
    let joined = if dir.is_empty() {
        raw.to_string()
    } else {
        format!("{dir}/{raw}")
    };
    let joined = normalize_rel_path(&joined);
    let candidates = [
        joined.clone(),
        format!("{joined}.js"),
        format!("{joined}.jsx"),
        format!("{joined}.mjs"),
        format!("{joined}.cjs"),
        format!("{joined}.ts"),
        format!("{joined}.tsx"),
        format!("{joined}/index.js"),
        format!("{joined}/index.jsx"),
        format!("{joined}/index.ts"),
        format!("{joined}/index.tsx"),
    ];
    for candidate in candidates {
        if let Some(p) = mod_map.values().find(|v| v.as_str() == candidate) {
            return Some(p.clone());
        }
    }
    None
}

/// Every module-map path that suffix-matches `candidate`. Exact key hits return
/// that single path. Multiple hits stay unresolved — `HashMap` iteration order
/// must not pick `indexer_eval/graph.rs` over `graph.rs`.
fn module_candidate_paths(candidate: &str, mod_map: &HashMap<String, String>) -> Vec<String> {
    if candidate.is_empty() {
        return Vec::new();
    }
    if let Some(p) = mod_map.get(candidate) {
        return vec![p.clone()];
    }
    // Path-segment / file-suffix match only — never bare `ends_with("utils")`
    // (that would incorrectly match `my_utils`, `test_utils`, …).
    let mut hits: Vec<String> = mod_map
        .iter()
        .filter(|(k, v)| {
            *k == candidate
                || k.ends_with(&format!("/{candidate}"))
                || k.ends_with(&format!(".{candidate}"))
                || path_matches_stripped_module(v, candidate)
        })
        .map(|(_, v)| v.clone())
        .collect();
    hits.sort();
    hits.dedup();
    hits
}

fn unique_module_candidate(candidate: &str, mod_map: &HashMap<String, String>) -> Option<String> {
    let mut hits = module_candidate_paths(candidate, mod_map);
    if hits.len() == 1 {
        hits.pop()
    } else {
        None
    }
}

fn same_dir_candidate(hits: &[String], from_path: &str) -> Option<String> {
    let dir = path_dir(from_path);
    let mut same: Vec<&String> = hits.iter().filter(|v| path_dir(v) == dir).collect();
    same.sort();
    same.dedup();
    if same.len() == 1 {
        Some(same[0].clone())
    } else {
        None
    }
}

/// Nearest ancestor `lib.rs` (preferred) or `main.rs` module key for `from_path`.
fn rust_crate_root_key(from_path: &str, mod_map: &HashMap<String, String>) -> Option<String> {
    let mut dir = path_dir(from_path).to_string();
    loop {
        for name in ["lib.rs", "main.rs"] {
            let candidate = if dir.is_empty() {
                name.to_string()
            } else {
                format!("{dir}/{name}")
            };
            if mod_map.values().any(|v| v == &candidate) {
                return Some(path_to_rust_module_key(&candidate));
            }
        }
        if dir.is_empty() {
            return None;
        }
        match dir.rsplit_once('/') {
            Some((parent, _)) => dir = parent.to_string(),
            None => dir.clear(),
        }
    }
}

/// Resolve `crate` / `self` / `super` against the importing file's crate, not a
/// basename suffix. `crate::graph` from `cis-core/src/coordinator.rs` is
/// `cis-core.src.graph`, never `cis-core.src.indexer_eval.graph`.
fn resolve_rust_path(
    normalized: &str,
    from_path: &str,
    mod_map: &HashMap<String, String>,
) -> Option<String> {
    if !rust_path_is_anchored(normalized) {
        return None;
    }
    let mut parts: Vec<&str> = normalized.split('.').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return None;
    }
    let base = match parts[0] {
        "crate" => {
            parts.remove(0);
            rust_crate_root_key(from_path, mod_map)?
        }
        "self" => {
            parts.remove(0);
            path_to_rust_module_key(from_path)
        }
        "super" => {
            let mut current = path_to_rust_module_key(from_path);
            while parts.first().copied() == Some("super") {
                parts.remove(0);
                current = current
                    .rsplit_once('.')
                    .map(|(parent, _)| parent.to_string())
                    .unwrap_or_default();
            }
            current
        }
        _ => return None,
    };
    if base.is_empty() && parts.is_empty() {
        return None;
    }
    let key = if parts.is_empty() {
        base
    } else if base.is_empty() {
        parts.join(".")
    } else {
        format!("{base}.{}", parts.join("."))
    };
    mod_map.get(&key).cloned()
}

/// Resolve an import module string to a repo-relative file path via `mod_map`.
fn resolve_module_path(module: &str, mod_map: &HashMap<String, String>) -> Option<String> {
    resolve_module_path_at(module, None, mod_map)
}

pub(crate) fn resolve_module_path_at(
    module: &str,
    from_path: Option<&str>,
    mod_map: &HashMap<String, String>,
) -> Option<String> {
    let raw = module
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '<' || c == '>');
    if raw.is_empty() {
        return None;
    }
    // Go import paths and other URL-like modules: exact keys only.
    // `github.com/gorilla/mux` is not whichever file happens to be named `mux`.
    if raw.contains('/') && !raw.starts_with('.') {
        if let Some(p) = mod_map.get(raw) {
            return Some(p.clone());
        }
        return mod_map.get(&raw.replace('/', ".")).cloned();
    }
    // Normalize Rust `::` separators to `.` (indexers may emit either).
    let normalized = raw.replace("::", ".");
    if is_rust_prelude_module(&normalized) {
        return None;
    }
    if let Some(p) = mod_map.get(&normalized) {
        return Some(p.clone());
    }
    if let Some(from) = from_path {
        if from.ends_with(".py") {
            if raw.starts_with('.') {
                return resolve_python_relative(raw, from, mod_map);
            }
            return python_absolute_module(&normalized, mod_map);
        }
        if is_script_module_path(from) && raw.starts_with('.') {
            if let Some(p) = resolve_script_relative(raw, from, mod_map) {
                return Some(p);
            }
        }
        if is_script_module_path(from) && !raw.starts_with('.') && !raw.contains('/') {
            // `require("fs")` / `require("debug")` is a package, not a basename in the repo.
            return None;
        }
        if from.ends_with(".rs") {
            if let Some(p) = resolve_rust_path(&normalized, from, mod_map) {
                return Some(p);
            }
            // Crate root unknown (no lib.rs/main.rs in the map): accept a unique
            // suffix of the path after `crate`/`self`/`super`, and refuse when
            // several files share that basename.
            if rust_path_is_anchored(&normalized) {
                let stripped = strip_rust_path_prefixes(&normalized);
                return unique_module_candidate(stripped, mod_map);
            }
        }
        if raw.starts_with('.') || raw.contains('/') {
            if let Some(p) = resolve_relative_or_include(module, from, mod_map) {
                return Some(p);
            }
        }
    }
    let mut stripped = normalized.as_str();
    while stripped.starts_with("./") || stripped.starts_with("../") {
        stripped = stripped
            .trim_start_matches("./")
            .trim_start_matches("../");
    }
    let rust_stripped = strip_rust_path_prefixes(stripped);
    if is_rust_prelude_module(rust_stripped) {
        return None;
    }
    if let Some(p) = unique_module_candidate(rust_stripped, mod_map) {
        return Some(p);
    }
    // Ambiguous basename: a sibling file is a safe guess for unanchored imports.
    // Anchored `crate::` paths already returned above.
    if let Some(from) = from_path {
        let hits = module_candidate_paths(rust_stripped, mod_map);
        if hits.len() > 1 {
            if let Some(p) = same_dir_candidate(&hits, from) {
                return Some(p);
            }
        }
    }
    None
}

pub(crate) fn build_import_bindings(
    imports: &[ParsedImport],
    mod_map: &HashMap<String, String>,
    from_path: &str,
) -> HashMap<String, ImportBinding> {
    let mut out = HashMap::new();
    for imp in imports {
        let tp = resolve_module_path_at(&imp.module, Some(from_path), mod_map).or_else(|| {
            if imp.style == ImportStyle::Names && imp.names.len() == 1 {
                let fq = format!("{}.{}", imp.module, imp.names[0]);
                unique_module_candidate(&fq, mod_map)
            } else {
                None
            }
        });
        let Some(tp) = tp else {
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
        s.kind == NodeKind::Class
            && (s.stable_key == type_name || s.stable_key.ends_with(&format!(".{type_name}")))
    })
}

fn identity_key_for_class(index: &FileIndex, class_key: &str) -> Option<String> {
    index
        .symbols
        .iter()
        .find(|s| s.kind == NodeKind::Class && s.stable_key == class_key)
        .or_else(|| {
            index.symbols.iter().find(|s| {
                s.kind == NodeKind::Class
                    && s.stable_key.ends_with(&format!(".{class_key}"))
            })
        })
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.stable_key == class_key && s.disambiguator.is_empty())
        })
        .or_else(|| index.symbols.iter().find(|s| s.stable_key == class_key))
        .map(|s| s.identity_key())
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
        CallReceiver::Bare(name) if name == "self" || name == "this" => {
            if caller_class.is_empty() {
                None
            } else {
                Some((path.to_string(), caller_class.to_string()))
            }
        }
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
        CallReceiver::Bare(name) => {
            if let Some(id) = resolve_symbol_in_file_index_exact(path, name, index) {
                return Some(id);
            }
            // Rust: `foo()` is never `Self::foo` — methods need `self.` / `Type::`.
            // Java/Python/C#: unqualified `foo()` inside a class can be a same-type method.
            if !path.ends_with(".rs") {
                if let Some(cls) = caller_enclosing_class(&call.caller_stable_key) {
                    if let Some(id) = resolve_member_in_file_index(path, cls, name, index) {
                        return Some(id);
                    }
                }
            }
            None
        }
        CallReceiver::Attr { object, name } => {
            if let CallReceiver::Bare(owner) = object.as_ref() {
                resolve_member_in_file_index(path, owner, name, index)
            } else {
                None
            }
        }
    }
}

fn resolve_symbol_in_file_index_exact(path: &str, simple_name: &str, index: &FileIndex) -> Option<IdentityId> {
    let exact = format!("{path}::{simple_name}");
    let mut best_fn_empty: Option<IdentityId> = None;
    let mut best_fn: Option<IdentityId> = None;
    let mut best_empty: Option<IdentityId> = None;
    let mut best_any: Option<IdentityId> = None;
    for sym in &index.symbols {
        if !(sym.qualified_name == exact || sym.stable_key == simple_name) {
            continue;
        }
        let id = IdentityId(stable_id_bytes("id", path, &sym.identity_key()));
        if sym.kind == NodeKind::Function && sym.disambiguator.is_empty() {
            best_fn_empty.get_or_insert(id);
        } else if sym.kind == NodeKind::Function {
            best_fn.get_or_insert(id);
        } else if sym.disambiguator.is_empty() {
            best_empty.get_or_insert(id);
        } else {
            best_any.get_or_insert(id);
        }
    }
    best_fn_empty.or(best_fn).or(best_empty).or(best_any)
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

/// Unique Function whose name leaf is `leaf` (`Type.leaf` or bare `leaf`).
/// Ambiguous leaves (`new`/`get` in a file with several impls) return None.
fn unique_function_leaf<'a>(index: &'a FileIndex, leaf: &str) -> Option<&'a str> {
    if leaf.is_empty() {
        return None;
    }
    let suffix = format!(".{leaf}");
    let mut hit: Option<&str> = None;
    for s in &index.symbols {
        if s.kind != NodeKind::Function {
            continue;
        }
        if s.stable_key == leaf || s.stable_key.ends_with(&suffix) {
            match hit {
                Some(prev) if prev != s.stable_key => return None,
                Some(_) => {}
                None => hit = Some(s.stable_key.as_str()),
            }
        }
    }
    hit
}

fn unique_function_leaf_in_graph(
    graph: &dyn crate::graph_view::GraphView,
    branch: BranchId,
    file_path: &str,
    leaf: &str,
) -> Option<IdentityId> {
    if leaf.is_empty() {
        return None;
    }
    let suffix = format!(".{leaf}");
    let mut hit: Option<IdentityId> = None;
    for rid in graph.revision_ids_for_file(branch, file_path) {
        let Some(r) = graph.get_revision(rid) else {
            continue;
        };
        if !matches!(r.status, RevisionStatus::Active) {
            continue;
        }
        if graph.identity_kind(r.identity_id) != Some(NodeKind::Function) {
            continue;
        }
        let q = r.qualified_name.as_str();
        let key = q.rsplit("::").next().unwrap_or(q);
        if key != leaf && !key.ends_with(&suffix) {
            continue;
        }
        match hit {
            Some(prev) if prev != r.identity_id => return None,
            Some(_) => {}
            None => hit = Some(r.identity_id),
        }
    }
    hit
}

pub(crate) fn resolve_member_in_module(
    file_path: &str,
    owner: &str,
    member: &str,
    branch: BranchId,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let member_key = format!("{owner}.{member}");
    if let Some(id) = resolve_symbol_in_module(file_path, &member_key, branch, graph, batch_indexes)
    {
        return Some(id);
    }
    if let Some(idx) = batch_indexes.get(file_path) {
        if let Some(key) = unique_function_leaf(idx, member) {
            if let Some(id) = resolve_symbol_in_file_index(file_path, key, idx) {
                return Some(id);
            }
        }
    }
    graph.and_then(|g| unique_function_leaf_in_graph(g, branch, file_path, member))
}

pub(crate) fn resolve_symbol_in_file_index(path: &str, simple_name: &str, index: &FileIndex) -> Option<IdentityId> {
    let exact = format!("{path}::{simple_name}");
    // Prefer Function (call/runtime target), then canonical empty disambiguator, then any.
    let mut best_fn_empty: Option<IdentityId> = None;
    let mut best_fn: Option<IdentityId> = None;
    let mut best_empty: Option<IdentityId> = None;
    let mut best_any: Option<IdentityId> = None;
    for sym in &index.symbols {
        let name_match = sym.qualified_name == exact
            || ((sym.kind == NodeKind::Function || sym.kind == NodeKind::Class)
                && (sym.stable_key == simple_name
                    || sym.stable_key.ends_with(&format!(".{simple_name}"))));
        if !name_match {
            continue;
        }
        let id = IdentityId(stable_id_bytes("id", path, &sym.identity_key()));
        if sym.kind == NodeKind::Function && sym.disambiguator.is_empty() {
            best_fn_empty.get_or_insert(id);
        } else if sym.kind == NodeKind::Function {
            best_fn.get_or_insert(id);
        } else if sym.disambiguator.is_empty() {
            best_empty.get_or_insert(id);
        } else {
            best_any.get_or_insert(id);
        }
    }
    best_fn_empty.or(best_fn).or(best_empty).or(best_any)
}

/// Revision / identity hash key for a function (calls / uses owners).
/// Prefers Function symbols so class+function collisions attach edges to the def.
/// Returns `None` when `stable_key` is not a symbol in this file.
fn identity_key_for_owner(index: &FileIndex, stable_key: &str) -> Option<String> {
    index
        .symbols
        .iter()
        .find(|s| {
            s.kind == NodeKind::Function
                && s.stable_key == stable_key
                && s.disambiguator.is_empty()
        })
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.kind == NodeKind::Function && s.stable_key == stable_key)
        })
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.stable_key == stable_key && s.disambiguator.is_empty())
        })
        .or_else(|| index.symbols.iter().find(|s| s.stable_key == stable_key))
        .map(|s| s.identity_key())
}

/// Anchor edges to a real symbol revision; fall back to `$file` for typed locals.
fn owner_revision_ikey(index: &FileIndex, stable_key: &str) -> String {
    identity_key_for_owner(index, stable_key)
        .or_else(|| identity_key_for_owner(index, "$file"))
        .unwrap_or_else(|| "$file".to_string())
}

pub(crate) fn resolve_symbol_in_graph(
    graph: &dyn crate::graph_view::GraphView,
    branch: BranchId,
    file_path: &str,
    simple_name: &str,
) -> Option<IdentityId> {
    let exact = format!("{file_path}::{simple_name}");
    let canonical = IdentityId(stable_id_bytes("id", file_path, simple_name));
    let mut best_fn_canon: Option<IdentityId> = None;
    let mut best_fn: Option<IdentityId> = None;
    let mut best_canon: Option<IdentityId> = None;
    let mut best_any: Option<IdentityId> = None;
    for rid in graph.revision_ids_for_file(branch, file_path) {
        let Some(r) = graph.get_revision(rid) else {
            continue;
        };
        if !matches!(r.status, RevisionStatus::Active) {
            continue;
        }
        let name_match = r.qualified_name == exact
            || r.qualified_name.ends_with(&format!(".{simple_name}"));
        if !name_match {
            continue;
        }
        let is_fn = graph.identity_kind(r.identity_id) == Some(NodeKind::Function);
        if is_fn && r.identity_id == canonical {
            best_fn_canon.get_or_insert(r.identity_id);
        } else if is_fn {
            best_fn.get_or_insert(r.identity_id);
        } else if r.identity_id == canonical {
            best_canon.get_or_insert(r.identity_id);
        } else {
            best_any.get_or_insert(r.identity_id);
        }
    }
    best_fn_canon.or(best_fn).or(best_canon).or(best_any)
}

pub(crate) fn resolve_symbol_in_module(
    file_path: &str,
    simple_name: &str,
    branch: BranchId,
    graph: Option<&dyn crate::graph_view::GraphView>,
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

fn looks_like_type_owner(name: &str) -> bool {
    !matches!(name, "self" | "this" | "Self" | "crate" | "super")
        && name.chars().next().is_some_and(|c| c.is_uppercase())
}

/// `Type::method` forwarding (UFCS): prefer the imported type's method over a
/// same-file wrapper that happens to share `Type.method` (trait impl → inherent).
fn resolve_imported_ufcs_target(
    path: &str,
    branch: BranchId,
    call: &ParsedCall,
    import_bindings: &HashMap<String, ImportBinding>,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let CallReceiver::Attr { object, name } = &call.callee else {
        return None;
    };
    let CallReceiver::Bare(owner) = object.as_ref() else {
        return None;
    };
    if !looks_like_type_owner(owner) {
        return None;
    }
    let binding = import_bindings.get(owner)?;
    if binding.file_path == path {
        return None;
    }
    resolve_member_in_module(
        &binding.file_path,
        &binding.remote_name,
        name,
        branch,
        graph,
        batch_indexes,
    )
}

fn flatten_receiver<'a>(recv: &'a CallReceiver) -> Vec<&'a str> {
    match recv {
        CallReceiver::Bare(n) => vec![n.as_str()],
        CallReceiver::Attr { object, name } => {
            let mut segs = flatten_receiver(object);
            segs.push(name.as_str());
            segs
        }
    }
}

/// `crate::mod::Type::method` / `mod::func` — not `Board::new` (that's a type UFCS).
fn resolve_rust_path_call(
    path: &str,
    branch: BranchId,
    call: &ParsedCall,
    mod_map: &HashMap<String, String>,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    if !path.ends_with(".rs") {
        return None;
    }
    if !matches!(call.callee, CallReceiver::Attr { .. }) {
        return None;
    }
    let mut rest = flatten_receiver(&call.callee);
    while matches!(rest.first().copied(), Some("crate" | "self" | "super")) {
        rest.remove(0);
    }
    if rest.len() < 2 {
        return None;
    }
    let method = *rest.last()?;
    if rest.len() >= 3 {
        let owner = rest[rest.len() - 2];
        let module = rest[..rest.len() - 2].join(".");
        if let Some(fp) = resolve_module_path_at(&module, Some(path), mod_map) {
            if let Some(id) = resolve_member_in_module(
                &fp,
                owner,
                method,
                branch,
                graph,
                batch_indexes,
            ) {
                return Some(id);
            }
        }
    }
    let module_owner = rest[rest.len() - 2];
    if rest.len() == 2 && looks_like_type_owner(module_owner) {
        return None;
    }
    let module = rest[..rest.len() - 1].join(".");
    let fp = resolve_module_path_at(&module, Some(path), mod_map)?;
    if let Some(id) = resolve_symbol_in_module(&fp, method, branch, graph, batch_indexes) {
        return Some(id);
    }
    batch_indexes.get(&fp).and_then(|idx| {
        unique_function_leaf(idx, method)
            .and_then(|key| resolve_symbol_in_file_index(&fp, key, idx))
    })
}

pub(crate) fn resolve_call_target(
    path: &str,
    branch: BranchId,
    index: &FileIndex,
    call: &ParsedCall,
    mod_map: &HashMap<String, String>,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let import_bindings = build_import_bindings(&index.imports, mod_map, path);
    resolve_call_target_with_bindings(
        path,
        branch,
        index,
        call,
        mod_map,
        graph,
        batch_indexes,
        &import_bindings,
    )
}

fn resolve_call_target_with_bindings(
    path: &str,
    branch: BranchId,
    index: &FileIndex,
    call: &ParsedCall,
    mod_map: &HashMap<String, String>,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
    import_bindings: &HashMap<String, ImportBinding>,
) -> Option<IdentityId> {
    if let Some(id) = resolve_imported_ufcs_target(
        path,
        branch,
        call,
        &import_bindings,
        graph,
        batch_indexes,
    ) {
        return Some(id);
    }

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
                let Some(target_path) = resolve_module_path_at(&imp.module, Some(path), mod_map)
                else {
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
            if allows_same_dir_resolution(path) {
                if let Some(fp) = unique_callable_in_dir(batch_indexes, path, name) {
                    if fp != path {
                        if let Some(id) =
                            resolve_symbol_in_module(&fp, name, branch, graph, batch_indexes)
                        {
                            return Some(id);
                        }
                    }
                }
            }
        }
        CallReceiver::Attr { object, name } => {
            let caller_class = caller_enclosing_class(&call.caller_stable_key).unwrap_or("");
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
            if let Some(id) = resolve_rust_path_call(
                path,
                branch,
                call,
                mod_map,
                graph,
                batch_indexes,
            ) {
                return Some(id);
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
                if allows_same_dir_resolution(path) {
                    if let Some(fp) = unique_class_in_scope(batch_indexes, path, base) {
                        if let Some(id) = resolve_member_in_module(
                            &fp,
                            base,
                            name,
                            branch,
                            graph,
                            batch_indexes,
                        ) {
                            return Some(id);
                        }
                    }
                    if let Some(fp) = unique_callable_in_dir(batch_indexes, path, name) {
                        if fp != path {
                            if let Some(id) = resolve_symbol_in_module(
                                &fp,
                                name,
                                branch,
                                graph,
                                batch_indexes,
                            ) {
                                return Some(id);
                            }
                        }
                    }
                }
            }
            let leaf = name.as_str();
            for imp in &index.imports {
                if !callee_in_import_scope(imp, leaf) {
                    continue;
                }
                let Some(target_path) = resolve_module_path_at(&imp.module, Some(path), mod_map)
                else {
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

/// Look up the innermost owning scope's 1-based start line for `stable_key`.
/// File hubs / missing symbols default to line `1` (module top).
fn scope_start_line_for_key(index: &FileIndex, stable_key: &str) -> u32 {
    let start = index
        .symbols
        .iter()
        .find(|s| {
            s.kind == NodeKind::Function
                && s.stable_key == stable_key
                && s.disambiguator.is_empty()
        })
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.kind == NodeKind::Function && s.stable_key == stable_key)
        })
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.kind == NodeKind::Class && s.stable_key == stable_key)
        })
        .or_else(|| {
            index
                .symbols
                .iter()
                .find(|s| s.stable_key == stable_key && s.disambiguator.is_empty())
        })
        .or_else(|| index.symbols.iter().find(|s| s.stable_key == stable_key))
        .map(|s| s.span.start_line)
        .unwrap_or(1);
    if start == 0 {
        1
    } else {
        start
    }
}

fn file_hub_scope_start(index: &FileIndex) -> u32 {
    index
        .symbols
        .iter()
        .find(|s| s.stable_key == "$file" || s.kind == NodeKind::File)
        .map(|s| {
            if s.span.start_line == 0 {
                1
            } else {
                s.span.start_line
            }
        })
        .unwrap_or(1)
}

pub(crate) fn import_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    label: &str,
    absolute_anchor: SourceSpan,
    scope_start_line: u32,
) -> GraphEdge {
    let (rel_line, rel_col) = scope_relative_edge_pos(absolute_anchor, scope_start_line);
    GraphEdge {
        edge_id: edge_id_bytes("imp", path, src_rid, label, rel_line, rel_col),
        ty: EdgeType::Imports,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor: absolute_anchor,
    }
}

pub(crate) fn call_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    label: &str,
    absolute_anchor: SourceSpan,
    scope_start_line: u32,
) -> GraphEdge {
    let (rel_line, rel_col) = scope_relative_edge_pos(absolute_anchor, scope_start_line);
    GraphEdge {
        edge_id: edge_id_bytes("cal", path, src_rid, label, rel_line, rel_col),
        ty: EdgeType::Calls,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor: absolute_anchor,
    }
}

pub(crate) fn extends_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    class_key: &str,
    base_name: &str,
    absolute_anchor: SourceSpan,
    scope_start_line: u32,
) -> GraphEdge {
    let label = format!("{class_key}:extends:{base_name}");
    let (rel_line, rel_col) = scope_relative_edge_pos(absolute_anchor, scope_start_line);
    GraphEdge {
        edge_id: edge_id_bytes("ext", path, src_rid, &label, rel_line, rel_col),
        ty: EdgeType::Extends,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor: absolute_anchor,
    }
}

pub(crate) fn use_edge(
    src_rid: NodeRevisionId,
    tgt: IdentityId,
    path: &str,
    owner_key: &str,
    type_name: &str,
    absolute_anchor: SourceSpan,
    scope_start_line: u32,
) -> GraphEdge {
    let label = format!("{owner_key}:uses:{type_name}");
    let (rel_line, rel_col) = scope_relative_edge_pos(absolute_anchor, scope_start_line);
    GraphEdge {
        edge_id: edge_id_bytes("use", path, src_rid, &label, rel_line, rel_col),
        ty: EdgeType::Uses,
        source_revision_id: src_rid,
        target_identity_id: tgt,
        resolution: ast_edge_resolution(),
        anchor: absolute_anchor,
    }
}

pub(crate) fn resolve_type_name_to_identity(
    path: &str,
    branch: BranchId,
    type_name: &str,
    index: &FileIndex,
    mod_map: &HashMap<String, String>,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> Option<IdentityId> {
    let import_bindings = build_import_bindings(&index.imports, mod_map, path);
    let (owner_file, simple) = resolve_type_location(type_name, path, index, &import_bindings);
    if let Some(id) = resolve_symbol_in_module(&owner_file, &simple, branch, graph, batch_indexes) {
        return Some(id);
    }
    if allows_same_dir_resolution(path) || owner_file == path {
        if let Some(fp) = unique_class_in_scope(batch_indexes, path, type_name) {
            return resolve_symbol_in_module(&fp, type_name, branch, graph, batch_indexes);
        }
    }
    None
}

pub(crate) fn attach_import_and_call_edges(
    path: &str,
    branch: BranchId,
    index: &FileIndex,
    mod_map: &HashMap<String, String>,
    graph: Option<&dyn crate::graph_view::GraphView>,
    batch_indexes: &HashMap<String, FileIndex>,
) -> HashMap<NodeRevisionId, Vec<GraphEdge>> {
    let mut edge_map: HashMap<NodeRevisionId, Vec<GraphEdge>> = HashMap::new();
    let file_hub_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
    let file_scope = file_hub_scope_start(index);
    for imp in &index.imports {
        let Some(tp) = resolve_module_path_at(&imp.module, Some(path), mod_map).or_else(|| {
            if imp.style == ImportStyle::Names && imp.names.len() == 1 {
                let fq = format!("{}.{}", imp.module, imp.names[0]);
                unique_module_candidate(&fq, mod_map)
            } else {
                None
            }
        }) else {
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
                    file_scope,
                ));
            }
            ImportStyle::Names => {
                for name in &imp.names {
                    // A missing symbol is not the module's file hub. Falling back
                    // stored a poisoned Imports edge whenever module resolution
                    // picked the wrong `graph.rs`.
                    let Some(tiid) =
                        resolve_symbol_in_module(&tp, name, branch, graph, batch_indexes)
                    else {
                        continue;
                    };
                    let label = format!("{}:{}", imp.module, name);
                    edge_map.entry(file_hub_rid).or_default().push(import_edge(
                        file_hub_rid,
                        tiid,
                        path,
                        &label,
                        imp.span,
                        file_scope,
                    ));
                }
            }
        }
    }
    for ext in &index.extends {
        let Some(class_ikey) = identity_key_for_class(index, &ext.class_stable_key) else {
            // Extends without a class symbol cannot be anchored — skip.
            continue;
        };
        let class_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, &class_ikey));
        let class_scope = scope_start_line_for_key(index, &ext.class_stable_key);
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
                class_scope,
            ));
        }
    }
    for u in &index.uses {
        // Typed locals (non-symbols) fall back to the file hub so ingest never
        // tries to replace edges for a revision that was never created.
        let owner_ikey = owner_revision_ikey(index, &u.owner_stable_key);
        let owner_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, &owner_ikey));
        let owner_scope = scope_start_line_for_key(index, &u.owner_stable_key);
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
                owner_scope,
            ));
        }
    }
    let import_bindings = build_import_bindings(&index.imports, mod_map, path);
    for call in &index.calls {
        let Some(caller_ikey) = identity_key_for_owner(index, &call.caller_stable_key)
            .or_else(|| identity_key_for_owner(index, "$file"))
        else {
            continue;
        };
        let caller_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, &caller_ikey));
        let caller_scope = scope_start_line_for_key(index, &call.caller_stable_key);
        if let Some(tiid) = resolve_call_target_with_bindings(
            path,
            branch,
            index,
            call,
            mod_map,
            graph,
            batch_indexes,
            &import_bindings,
        ) {
            let label = format!("{}->{}", call.caller_stable_key, call.callee.label());
            edge_map.entry(caller_rid).or_default().push(call_edge(
                caller_rid,
                tiid,
                path,
                &label,
                call.span,
                caller_scope,
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
            crate::language_indexer::indexer_for_path(&s, indexers).and_then(|idx| {
                let k = idx.module_key(&s);
                if k.is_empty() {
                    None
                } else {
                    Some((k, s))
                }
            })
        })
        .collect()
}

use crate::graph_view::GraphView;

/// Active revision file paths on a branch (language-agnostic).
pub fn paths_on_branch(graph: &dyn GraphView, branch: BranchId) -> HashSet<String> {
    graph
        .revisions_on_branches(&[branch])
        .into_iter()
        .filter(|r| matches!(r.status, RevisionStatus::Active) && !r.file_path.is_empty())
        .map(|r| r.file_path)
        .collect()
}

/// Collect distinct indexed source paths with active revisions on a branch.
///
/// Historically Python-only; now keeps any path registered in [`crate::language_indexer::default_indexers`].
pub fn python_paths_on_branch(graph: &dyn GraphView, branch: BranchId) -> HashSet<String> {
    paths_on_branch(graph, branch)
        .into_iter()
        .filter(|p| crate::language_indexer::path_is_indexable(p))
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
    graph: Option<&dyn crate::graph_view::GraphView>,
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
    graph: Option<&dyn crate::graph_view::GraphView>,
    indexers: &[Box<dyn crate::language_indexer::LanguageIndexer>],
) -> Result<HashMap<NodeRevisionId, Vec<GraphEdge>>, &'static str> {
    let indexer = crate::language_indexer::indexer_for_path(path, indexers)
        .ok_or("unsupported language")?;
    let mut index = indexer
        .index_file(path, content)
        .map_err(|_| "parse failed")?;
    if let Some(g) = graph {
        crate::index_model::stabilize_disambiguators(&mut index, path, branch, g);
    }
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
    fn resolve_module_path_strips_crate_prefix_for_rust() {
        let mut mod_map = HashMap::new();
        mod_map.insert(
            "cis-core.src.coordinator".to_string(),
            "cis-core/src/coordinator.rs".to_string(),
        );
        assert_eq!(
            resolve_module_path("crate.coordinator", &mod_map),
            Some("cis-core/src/coordinator.rs".to_string())
        );
        assert_eq!(
            resolve_module_path("crate::coordinator", &mod_map),
            Some("cis-core/src/coordinator.rs".to_string())
        );
    }

    #[test]
    fn crate_graph_resolves_to_crate_root_not_nested_basename() {
        let mut mod_map = HashMap::new();
        let real = "cis-core/src/graph.rs";
        let nested = "cis-core/src/indexer_eval/graph.rs";
        mod_map.insert("cis-core.src".into(), "cis-core/src/lib.rs".into());
        mod_map.insert(
            "cis-core.src.graph".into(),
            real.into(),
        );
        mod_map.insert(
            "cis-core.src.indexer_eval.graph".into(),
            nested.into(),
        );
        mod_map.insert(
            "cis-core.src.indexer_eval".into(),
            "cis-core/src/indexer_eval/mod.rs".into(),
        );
        let from = "cis-core/src/coordinator.rs";
        assert_eq!(
            resolve_module_path_at("crate::graph", Some(from), &mod_map),
            Some(real.to_string()),
            "crate::graph must not suffix-match indexer_eval/graph.rs"
        );
        assert_eq!(
            resolve_module_path_at("crate.graph", Some(from), &mod_map),
            Some(real.to_string())
        );
        assert_eq!(
            resolve_module_path_at(
                "crate::indexer_eval::graph",
                Some(from),
                &mod_map
            ),
            Some(nested.to_string())
        );
        assert_eq!(
            resolve_module_path_at(
                "super::graph",
                Some("cis-core/src/indexer_eval/mod.rs"),
                &mod_map
            ),
            Some(real.to_string()),
            "super::graph from indexer_eval is the parent crate module"
        );
        assert_eq!(
            resolve_module_path_at(
                "super::graph",
                Some("cis-core/src/indexer_eval/other.rs"),
                &mod_map
            ),
            Some(nested.to_string())
        );
        assert_eq!(
            resolve_module_path_at(
                "crate::graph",
                Some("cis-core/src/indexer_eval/mod.rs"),
                &mod_map
            ),
            Some(real.to_string()),
            "crate:: from a nested file is the crate root, not the nested basename"
        );
        assert_eq!(
            resolve_module_path_at("graph", Some(from), &mod_map),
            Some(real.to_string()),
            "an unanchored bare name may use the unique sibling file"
        );
        assert_eq!(
            resolve_module_path_at("graph", Some("other/place.rs"), &mod_map),
            None,
            "ambiguous bare graph must not pick a HashMap victim"
        );
    }

    #[test]
    fn named_import_miss_does_not_target_file_hub() {
        let branch = BranchId([0u8; 16]);
        let path = "cis-core/src/coordinator.rs";
        let wrong = "cis-core/src/indexer_eval/graph.rs";
        let mut mod_map = HashMap::new();
        mod_map.insert("cis-core.src".into(), "cis-core/src/lib.rs".into());
        mod_map.insert(
            "cis-core.src.graph".into(),
            "cis-core/src/graph.rs".into(),
        );
        mod_map.insert("cis-core.src.indexer_eval.graph".into(), wrong.into());
        let mut index = FileIndex::default();
        index.imports.push(ParsedImport {
            module: "crate.graph".into(),
            style: ImportStyle::Names,
            names: vec!["InMemoryGraph".into(), "RevisionStatus".into()],
            span: span(),
        });
        let mut wrong_idx = FileIndex::default();
        wrong_idx.symbols.push(ParsedSymbol {
            stable_key: "$file".into(),
            disambiguator: String::new(),
            qualified_name: wrong.into(),
            kind: NodeKind::File,
            span: span(),
        });
        let mut batch = HashMap::new();
        batch.insert(wrong.into(), wrong_idx);
        let edges =
            attach_import_and_call_edges(path, branch, &index, &mod_map, None, &batch);
        let file_hub = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
        let imports = edges.get(&file_hub).map(Vec::as_slice).unwrap_or(&[]);
        assert!(
            imports.is_empty(),
            "missing symbols must not become Imports to a file hub, got {imports:?}"
        );
        let wrong_hub = IdentityId(stable_id_bytes("file", wrong, "$hub"));
        assert!(
            imports.iter().all(|e| e.target_identity_id != wrong_hub),
            "must not poison the indexer_eval/graph.rs hub"
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
        let bindings = build_import_bindings(&imports, &mod_map, "main.py");
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
    fn resolve_write_coordinator_open_via_crate_import() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "cis-core/src/mcp_runtime.rs";
        let callee_path = "cis-core/src/coordinator.rs";
        let mut mod_map = HashMap::new();
        mod_map.insert(
            "cis-core.src.coordinator".to_string(),
            callee_path.to_string(),
        );

        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "WriteCoordinator".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::WriteCoordinator"),
            kind: NodeKind::Class,
            span: span(),
        });
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "WriteCoordinator.open".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::WriteCoordinator.open"),
            kind: NodeKind::Function,
            span: span(),
        });

        let mut caller_idx = FileIndex::default();
        caller_idx.imports.push(ParsedImport {
            module: "crate.coordinator".to_string(),
            style: ImportStyle::Names,
            names: vec!["WriteCoordinator".to_string()],
            span: span(),
        });
        caller_idx.symbols.push(ParsedSymbol {
            stable_key: "boot".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{caller_path}::boot"),
            kind: NodeKind::Function,
            span: span(),
        });
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "boot".to_string(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::bare("WriteCoordinator")),
                name: "open".to_string(),
            },
            span: span(),
        });

        let mut batch_indexes = HashMap::new();
        batch_indexes.insert(callee_path.to_string(), callee_idx);

        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch_indexes,
        )
        .expect("WriteCoordinator::open should resolve across crate:: import");
        let expected = IdentityId(stable_id_bytes(
            "id",
            callee_path,
            "WriteCoordinator.open",
        ));
        assert_eq!(target, expected);

        let edges = attach_import_and_call_edges(
            caller_path,
            branch,
            &caller_idx,
            &mod_map,
            None,
            &batch_indexes,
        );
        let file_hub = NodeRevisionId(stable_rev_id_bytes(branch, caller_path, "$file"));
        let imports = edges.get(&file_hub).expect("file hub import edges");
        assert!(
            imports.iter().any(|e| e.ty == EdgeType::Imports),
            "crate::coordinator named import should create Imports edges"
        );
        let boot_rid = NodeRevisionId(stable_rev_id_bytes(branch, caller_path, "boot"));
        let calls = edges.get(&boot_rid).expect("boot call edges");
        assert!(
            calls.iter().any(|e| e.ty == EdgeType::Calls && e.target_identity_id == target),
            "boot should Call WriteCoordinator.open"
        );
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
        let scope_start = 5;
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
        let e1 = call_edge(caller_rid, target, path, "foo->bar", span_a, scope_start);
        let e2 = call_edge(caller_rid, target, path, "foo->bar", span_b, scope_start);
        assert_ne!(e1.edge_id, e2.edge_id);
        // Display anchors remain file-absolute.
        assert_eq!(e1.anchor.start_line, 10);
        assert_eq!(e2.anchor.start_line, 20);
    }

    #[test]
    fn edge_id_stable_when_absolute_lines_shift_but_relative_pos_unchanged() {
        let branch = BranchId([0u8; 16]);
        let path = "main.py";
        let caller_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "run"));
        let target = IdentityId(stable_id_bytes("id", path, "helper"));
        // Inserting 3 lines above the function shifts absolute lines, not relative.
        let before = call_edge(
            caller_rid,
            target,
            path,
            "run->helper",
            SourceSpan {
                start_line: 12,
                start_col: 4,
                end_line: 12,
                end_col: 10,
            },
            10, // function starts at line 10 → relative line 2
        );
        let after = call_edge(
            caller_rid,
            target,
            path,
            "run->helper",
            SourceSpan {
                start_line: 15,
                start_col: 4,
                end_line: 15,
                end_col: 10,
            },
            13, // function now starts at line 13 → relative line still 2
        );
        assert_eq!(before.edge_id, after.edge_id);
        assert_eq!(before.anchor.start_line, 12);
        assert_eq!(after.anchor.start_line, 15);
    }

    #[test]
    fn edge_id_changes_when_relative_position_inside_scope_changes() {
        let branch = BranchId([0u8; 16]);
        let path = "main.py";
        let caller_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "run"));
        let target = IdentityId(stable_id_bytes("id", path, "helper"));
        let scope = 10;
        let early = call_edge(
            caller_rid,
            target,
            path,
            "run->helper",
            SourceSpan {
                start_line: 12,
                start_col: 4,
                end_line: 12,
                end_col: 10,
            },
            scope,
        );
        let later = call_edge(
            caller_rid,
            target,
            path,
            "run->helper",
            SourceSpan {
                start_line: 14,
                start_col: 4,
                end_line: 14,
                end_col: 10,
            },
            scope,
        );
        assert_ne!(early.edge_id, later.edge_id);
    }

    #[test]
    fn attach_edges_uses_caller_scope_relative_edge_ids() {
        let branch = BranchId([0u8; 16]);
        let path = "main.py";
        let mut mod_map = HashMap::new();
        mod_map.insert("helpers".to_string(), "helpers.py".to_string());

        let mut helpers = FileIndex::default();
        helpers.symbols.push(ParsedSymbol {
            stable_key: "helper".to_string(),
            disambiguator: String::new(),
            qualified_name: "helpers.py::helper".into(),
            kind: NodeKind::Function,
            span: SourceSpan {
                start_line: 1,
                start_col: 1,
                end_line: 2,
                end_col: 1,
            },
        });

        // Scenario A: helper call at absolute line 12, function starts at 10.
        let mut index_a = FileIndex::default();
        index_a.symbols.push(ParsedSymbol {
            stable_key: "$file".into(),
            disambiguator: String::new(),
            qualified_name: path.into(),
            kind: NodeKind::File,
            span: SourceSpan {
                start_line: 1,
                start_col: 1,
                end_line: 20,
                end_col: 1,
            },
        });
        index_a.symbols.push(ParsedSymbol {
            stable_key: "run".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::run"),
            kind: NodeKind::Function,
            span: SourceSpan {
                start_line: 10,
                start_col: 1,
                end_line: 15,
                end_col: 1,
            },
        });
        index_a.imports.push(ParsedImport {
            module: "helpers".into(),
            style: ImportStyle::Names,
            names: vec!["helper".into()],
            span: SourceSpan {
                start_line: 1,
                start_col: 1,
                end_line: 1,
                end_col: 20,
            },
        });
        index_a.calls.push(ParsedCall {
            caller_stable_key: "run".into(),
            callee: CallReceiver::bare("helper"),
            span: SourceSpan {
                start_line: 12,
                start_col: 4,
                end_line: 12,
                end_col: 12,
            },
        });

        // Scenario B: three blank lines inserted above `run` — absolute shift, same relative.
        let mut index_b = index_a.clone();
        index_b.symbols[1].span.start_line = 13;
        index_b.symbols[1].span.end_line = 18;
        index_b.calls[0].span.start_line = 15;
        index_b.calls[0].span.end_line = 15;

        let mut batch = HashMap::new();
        batch.insert("helpers.py".to_string(), helpers);

        let edges_a =
            attach_import_and_call_edges(path, branch, &index_a, &mod_map, None, &batch);
        let edges_b =
            attach_import_and_call_edges(path, branch, &index_b, &mod_map, None, &batch);

        let run_rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "run"));
        let a = edges_a.get(&run_rid).expect("call edges for run in A");
        let b = edges_b.get(&run_rid).expect("call edges for run in B");
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        assert_eq!(a[0].edge_id, b[0].edge_id, "relative edge id must survive insert above fn");
        assert_eq!(a[0].anchor.start_line, 12);
        assert_eq!(b[0].anchor.start_line, 15);
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
        let mut helpers = FileIndex::default();
        helpers.symbols.push(ParsedSymbol {
            stable_key: "helper".into(),
            disambiguator: String::new(),
            qualified_name: "helpers.py::helper".into(),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut batch = HashMap::new();
        batch.insert("helpers.py".into(), helpers);
        let edges = attach_import_and_call_edges(path, branch, &index, &mod_map, None, &batch);
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
        let util_src = "export function helper() { return 1; }\n";
        let indexers = crate::language_indexer::default_indexers();
        let mod_map = module_map_for_paths(["app.ts", "util.ts"], &indexers);
        let app = crate::language_indexer::indexer_for_path(path, &indexers)
            .expect("ts indexer")
            .index_file(path, content)
            .expect("index app.ts");
        let util = crate::language_indexer::indexer_for_path("util.ts", &indexers)
            .expect("ts indexer")
            .index_file("util.ts", util_src)
            .expect("index util.ts");
        let mut batch = HashMap::new();
        batch.insert(path.to_string(), app.clone());
        batch.insert("util.ts".to_string(), util);
        let edges = attach_import_and_call_edges(path, branch, &app, &mod_map, None, &batch);
        let file_hub = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
        let imports = edges.get(&file_hub).expect("typescript named import edge");
        let helper = IdentityId(stable_id_bytes("id", "util.ts", "helper"));
        assert!(
            imports.iter().any(|e| e.ty == EdgeType::Imports && e.target_identity_id == helper),
            "named import must target helper, not a file hub: {imports:?}"
        );
        let util_hub = IdentityId(stable_id_bytes("file", "util.ts", "$hub"));
        assert!(imports.iter().all(|e| e.target_identity_id != util_hub));

        // Single-file regen cannot see `helper`, so it must not invent a hub edge.
        let regen = regen_edges_for_file_with_graph(
            path,
            content,
            branch,
            &mod_map,
            None,
            &indexers,
        )
        .expect("ts regen");
        let regen_imports = regen.get(&file_hub).map(Vec::as_slice).unwrap_or(&[]);
        assert!(
            regen_imports.is_empty(),
            "missing symbol must not fall back to the util.ts file hub: {regen_imports:?}"
        );
    }

    #[test]
    fn typed_local_uses_anchor_to_file_hub_not_orphan_revision() {
        use crate::graph::NodeKind;
        use crate::index_model::{ParsedSymbol, ParsedUse, whole_file_span};
        let branch = BranchId([0u8; 16]);
        let path = "typed.ts";
        let mut index = FileIndex::default();
        index.symbols.push(ParsedSymbol {
            stable_key: "$file".into(),
            disambiguator: String::new(),
            qualified_name: path.into(),
            kind: NodeKind::File,
            span: whole_file_span("const x: Board = null;\n"),
        });
        index.symbols.push(ParsedSymbol {
            stable_key: "Board".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::Board"),
            kind: NodeKind::Class,
            span: span(),
        });
        // Owner "x" is NOT a symbol — previously created an unknown revision id.
        index.uses.push(ParsedUse {
            owner_stable_key: "x".into(),
            type_name: "Board".into(),
            span: span(),
        });
        let mut batch = HashMap::new();
        batch.insert(path.to_string(), index.clone());
        let edges =
            attach_import_and_call_edges(path, branch, &index, &HashMap::new(), None, &batch);
        let file_hub = NodeRevisionId(stable_rev_id_bytes(branch, path, "$file"));
        let orphan = NodeRevisionId(stable_rev_id_bytes(branch, path, "x"));
        assert!(
            edges.contains_key(&file_hub),
            "typed local uses must anchor to $file hub"
        );
        assert!(
            !edges.contains_key(&orphan),
            "must not emit edges for non-symbol owner revisions"
        );
    }

    #[test]
    fn resolve_call_target_same_package_go() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "mux.go";
        let callee_path = "route.go";
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "NewRouter".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::NewRouter"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut caller_idx = FileIndex::default();
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "Example".to_string(),
            callee: CallReceiver::bare("NewRouter"),
            span: span(),
        });
        let mut batch = HashMap::new();
        batch.insert(caller_path.to_string(), caller_idx.clone());
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &HashMap::new(),
            None,
            &batch,
        )
        .expect("same-package Go call should resolve without an import");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes("id", callee_path, "NewRouter"))
        );
    }

    #[test]
    fn resolve_call_target_does_not_guess_rust_same_dir() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "src/a.rs";
        let sibling = "src/b.rs";
        let mut sibling_idx = FileIndex::default();
        sibling_idx.symbols.push(ParsedSymbol {
            stable_key: "helper".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{sibling}::helper"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut caller_idx = FileIndex::default();
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "run".to_string(),
            callee: CallReceiver::bare("helper"),
            span: span(),
        });
        let mut batch = HashMap::new();
        batch.insert(sibling.to_string(), sibling_idx);
        assert!(
            resolve_call_target(
                caller_path,
                branch,
                &caller_idx,
                &caller_idx.calls[0],
                &HashMap::new(),
                None,
                &batch,
            )
            .is_none(),
            "Rust sibling files are separate modules"
        );
    }

    #[test]
    fn resolve_local_typed_receiver_without_enclosing_class() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "main.rs";
        let callee_path = "graph.rs";
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "InMemoryGraph".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::InMemoryGraph"),
            kind: NodeKind::Class,
            span: span(),
        });
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "InMemoryGraph.to_snapshot".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::InMemoryGraph.to_snapshot"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut caller_idx = FileIndex::default();
        caller_idx.imports.push(ParsedImport {
            module: "crate.graph".to_string(),
            style: ImportStyle::Names,
            names: vec!["InMemoryGraph".to_string()],
            span: span(),
        });
        caller_idx
            .function_locals
            .entry("boot".into())
            .or_default()
            .insert("g".into(), "InMemoryGraph".into());
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "boot".to_string(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::bare("g")),
                name: "to_snapshot".to_string(),
            },
            span: span(),
        });
        let mut mod_map = HashMap::new();
        mod_map.insert("crate.graph".into(), callee_path.to_string());
        mod_map.insert("graph".into(), callee_path.to_string());
        let mut batch = HashMap::new();
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch,
        )
        .expect("g.to_snapshot should follow function_locals + import");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes("id", callee_path, "InMemoryGraph.to_snapshot"))
        );
    }

    #[test]
    fn resolve_extends_unique_class_same_package() {
        let branch = BranchId([0u8; 16]);
        let child_path = "pkg/Dog.java";
        let parent_path = "pkg/Animal.java";
        let mut parent = FileIndex::default();
        parent.symbols.push(ParsedSymbol {
            stable_key: "Animal".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{parent_path}::Animal"),
            kind: NodeKind::Class,
            span: span(),
        });
        let mut child = FileIndex::default();
        child.symbols.push(ParsedSymbol {
            stable_key: "Dog".to_string(),
            disambiguator: String::new(),
            qualified_name: format!("{child_path}::Dog"),
            kind: NodeKind::Class,
            span: span(),
        });
        child.extends.push(crate::index_model::ParsedExtends {
            class_stable_key: "Dog".to_string(),
            base_name: "Animal".to_string(),
            span: span(),
        });
        let mut batch = HashMap::new();
        batch.insert(parent_path.to_string(), parent);
        batch.insert(child_path.to_string(), child.clone());
        let id = resolve_type_name_to_identity(
            child_path,
            branch,
            "Animal",
            &child,
            &HashMap::new(),
            None,
            &batch,
        )
        .expect("same-package extends");
        assert_eq!(id, IdentityId(stable_id_bytes("id", parent_path, "Animal")));
    }

    #[test]
    fn resolve_c_include_header_to_sibling_source() {
        let mut mod_map = HashMap::new();
        mod_map.insert("zutil".into(), "zutil.c".into());
        mod_map.insert("deflate".into(), "deflate.c".into());
        assert_eq!(
            resolve_module_path_at("zutil.h", Some("deflate.c"), &mod_map),
            Some("zutil.c".to_string())
        );
        assert_eq!(
            resolve_module_path_at("\"zutil.h\"", Some("deflate.c"), &mod_map),
            Some("zutil.c".to_string())
        );
    }

    #[test]
    fn java_fqcn_import_binds_to_file() {
        let mut mod_map = HashMap::new();
        mod_map.insert(
            "com.google.gson.Gson".into(),
            "src/com/google/gson/Gson.java".into(),
        );
        let imports = vec![ParsedImport {
            module: "com.google.gson".into(),
            style: ImportStyle::Names,
            names: vec!["Gson".into()],
            span: span(),
        }];
        let bindings = build_import_bindings(&imports, &mod_map, "src/Main.java");
        assert_eq!(
            bindings.get("Gson").unwrap().file_path,
            "src/com/google/gson/Gson.java"
        );
    }

    #[test]
    fn resolve_module_receiver_unique_method_leaf() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "persistence.rs";
        let callee_path = "graph.rs";
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "InMemoryGraph".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::InMemoryGraph"),
            kind: NodeKind::Class,
            span: span(),
        });
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "InMemoryGraph.to_snapshot".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::InMemoryGraph.to_snapshot"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut caller_idx = FileIndex::default();
        caller_idx.imports.push(ParsedImport {
            module: "crate.graph".into(),
            style: ImportStyle::ModuleOnly,
            names: vec![],
            span: span(),
        });
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "save_graph_snapshot".into(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::bare("graph")),
                name: "to_snapshot".into(),
            },
            span: span(),
        });
        let mut mod_map = HashMap::new();
        mod_map.insert("crate.graph".into(), callee_path.to_string());
        mod_map.insert("graph".into(), callee_path.to_string());
        let mut batch = HashMap::new();
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch,
        )
        .expect("unique to_snapshot on imported module should resolve");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes(
                "id",
                callee_path,
                "InMemoryGraph.to_snapshot"
            ))
        );
    }

    #[test]
    fn resolve_ufcs_prefers_imported_type_over_same_file_wrapper() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "wal_backend.rs";
        let callee_path = "log.rs";
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "MutationLog".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::MutationLog"),
            kind: NodeKind::Class,
            span: span(),
        });
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "MutationLog.update_phase".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::MutationLog.update_phase"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut caller_idx = FileIndex::default();
        caller_idx.symbols.push(ParsedSymbol {
            stable_key: "MutationLog.update_phase".into(),
            disambiguator: String::new(),
            qualified_name: format!("{caller_path}::MutationLog.update_phase"),
            kind: NodeKind::Function,
            span: span(),
        });
        caller_idx.imports.push(ParsedImport {
            module: "crate.log".into(),
            style: ImportStyle::Names,
            names: vec!["MutationLog".into()],
            span: span(),
        });
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "MutationLog.update_phase".into(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::bare("MutationLog")),
                name: "update_phase".into(),
            },
            span: span(),
        });
        let mut mod_map = HashMap::new();
        mod_map.insert("crate.log".into(), callee_path.to_string());
        mod_map.insert("log".into(), callee_path.to_string());
        let mut batch = HashMap::new();
        batch.insert(caller_path.to_string(), caller_idx.clone());
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch,
        )
        .expect("UFCS should follow the imported inherent impl");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes(
                "id",
                callee_path,
                "MutationLog.update_phase"
            ))
        );
    }

    #[test]
    fn resolve_bare_does_not_steal_method_of_another_type() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "mcp.rs";
        let callee_path = "security.rs";
        let mut caller_idx = FileIndex::default();
        caller_idx.symbols.push(ParsedSymbol {
            stable_key: "CisMcpRuntime.verify_audit_chain".into(),
            disambiguator: String::new(),
            qualified_name: format!("{caller_path}::CisMcpRuntime.verify_audit_chain"),
            kind: NodeKind::Function,
            span: span(),
        });
        caller_idx.imports.push(ParsedImport {
            module: "crate.security".into(),
            style: ImportStyle::Names,
            names: vec!["verify_audit_chain".into()],
            span: span(),
        });
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "CisMcpRuntime.system_status".into(),
            callee: CallReceiver::bare("verify_audit_chain"),
            span: span(),
        });
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "verify_audit_chain".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::verify_audit_chain"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut mod_map = HashMap::new();
        mod_map.insert("crate.security".into(), callee_path.to_string());
        mod_map.insert("security".into(), callee_path.to_string());
        let mut batch = HashMap::new();
        batch.insert(caller_path.to_string(), caller_idx.clone());
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch,
        )
        .expect("imported free fn, not the method with the same leaf");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes("id", callee_path, "verify_audit_chain"))
        );
    }

    #[test]
    fn resolve_crate_path_associated_function() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "mcp.rs";
        let callee_path = "confirm_token.rs";
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "FaultInjectingConfirmBackend.new".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::FaultInjectingConfirmBackend.new"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut caller_idx = FileIndex::default();
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "CisMcpRuntime.new_dev_with_options".into(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::Attr {
                    object: Box::new(CallReceiver::Attr {
                        object: Box::new(CallReceiver::bare("crate")),
                        name: "confirm_token".into(),
                    }),
                    name: "FaultInjectingConfirmBackend".into(),
                }),
                name: "new".into(),
            },
            span: span(),
        });
        let mut mod_map = HashMap::new();
        mod_map.insert("confirm_token".into(), callee_path.to_string());
        mod_map.insert("crate.confirm_token".into(), callee_path.to_string());
        let mut batch = HashMap::new();
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch,
        )
        .expect("crate::mod::Type::new");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes(
                "id",
                callee_path,
                "FaultInjectingConfirmBackend.new"
            ))
        );
    }

    #[test]
    fn resolve_attr_does_not_steal_same_file_leaf() {
        let branch = BranchId([0u8; 16]);
        let caller_path = "graph_store.rs";
        let callee_path = "graph.rs";
        let mut caller_idx = FileIndex::default();
        caller_idx.symbols.push(ParsedSymbol {
            stable_key: "SqliteGraphStore.revision_count".into(),
            disambiguator: String::new(),
            qualified_name: format!("{caller_path}::SqliteGraphStore.revision_count"),
            kind: NodeKind::Function,
            span: span(),
        });
        caller_idx
            .function_locals
            .entry("migrate_graph_json_to_sqlite".into())
            .or_default()
            .insert("graph".into(), "InMemoryGraph".into());
        caller_idx.imports.push(ParsedImport {
            module: "crate.graph".into(),
            style: ImportStyle::Names,
            names: vec!["InMemoryGraph".into()],
            span: span(),
        });
        caller_idx.calls.push(ParsedCall {
            caller_stable_key: "migrate_graph_json_to_sqlite".into(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::bare("graph")),
                name: "revision_count".into(),
            },
            span: span(),
        });
        let mut callee_idx = FileIndex::default();
        callee_idx.symbols.push(ParsedSymbol {
            stable_key: "InMemoryGraph.revision_count".into(),
            disambiguator: String::new(),
            qualified_name: format!("{callee_path}::InMemoryGraph.revision_count"),
            kind: NodeKind::Function,
            span: span(),
        });
        let mut mod_map = HashMap::new();
        mod_map.insert("crate.graph".into(), callee_path.to_string());
        mod_map.insert("graph".into(), callee_path.to_string());
        let mut batch = HashMap::new();
        batch.insert(callee_path.to_string(), callee_idx);
        let target = resolve_call_target(
            caller_path,
            branch,
            &caller_idx,
            &caller_idx.calls[0],
            &mod_map,
            None,
            &batch,
        )
        .expect("typed graph.revision_count, not the local helper");
        assert_eq!(
            target,
            IdentityId(stable_id_bytes(
                "id",
                callee_path,
                "InMemoryGraph.revision_count"
            ))
        );
    }

    #[test]
    fn python_absolute_import_prefers_package_over_nested_basename() {
        let mut mod_map = HashMap::new();
        mod_map.insert(
            "src.flask".into(),
            "src/flask/__init__.py".into(),
        );
        mod_map.insert(
            "tests.test_apps.cliapp.inner1.inner2.flask".into(),
            "tests/test_apps/cliapp/inner1/inner2/flask.py".into(),
        );
        mod_map.insert("src.flask.typing".into(), "src/flask/typing.py".into());
        mod_map.insert("src.flask.cli".into(), "src/flask/cli.py".into());
        let flask = resolve_module_path_at("flask", Some("tests/test_basic.py"), &mod_map);
        assert_eq!(flask.as_deref(), Some("src/flask/__init__.py"));
        let typing = resolve_module_path_at("typing", Some("src/flask/app.py"), &mod_map);
        assert!(typing.is_none(), "stdlib typing must not bind to flask/typing.py, got {typing:?}");
        let cli = resolve_module_path_at(".cli", Some("src/flask/app.py"), &mod_map);
        assert_eq!(cli.as_deref(), Some("src/flask/cli.py"));
        let nested = resolve_module_path_at(
            "...flask",
            Some("src/flask/app.py"),
            &mod_map,
        );
        assert!(nested.is_none());
    }

    #[test]
    fn script_require_resolves_relative_not_bare_package() {
        let mut mod_map = HashMap::new();
        mod_map.insert("lib.application".into(), "lib/application.js".into());
        mod_map.insert("lib.utils".into(), "lib/utils.js".into());
        mod_map.insert("elsewhere.fs".into(), "elsewhere/fs.js".into());
        let app = resolve_module_path_at("./application", Some("lib/express.js"), &mod_map);
        assert_eq!(app.as_deref(), Some("lib/application.js"));
        let fs = resolve_module_path_at("fs", Some("lib/express.js"), &mod_map);
        assert!(fs.is_none(), "bare require must not suffix-match, got {fs:?}");
    }
}
