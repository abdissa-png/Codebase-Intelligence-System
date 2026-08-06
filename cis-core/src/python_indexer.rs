//! Python-specific file indexing (regex and optional tree-sitter).

use std::collections::HashSet;

use crate::graph::NodeKind;

use crate::index_model::{
    assign_collision_disambiguators, CallReceiver, FileIndex, ImportStyle, ParsedCall,
    ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse, span_from_byte_range, whole_file_span,
};

/// Map `dir/Mod.py` → `dir.Mod` for **`from X import …`** resolution.
pub fn path_to_python_module_key(rel_path: &str) -> String {
    rel_path.trim_end_matches(".py").replace('/', ".")
}

fn parse_import_names(tail: &str) -> Vec<String> {
    tail.split(',')
        .filter_map(|part| {
            let p = part.trim().trim_matches('(').trim_matches(')');
            if p.is_empty() || p == "*" {
                return None;
            }
            let name = p.split(" as ").next()?.trim();
            if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect()
}

fn extract_imports_regex(content: &str) -> Vec<ParsedImport> {
    let mut out = Vec::new();
    let Ok(re_imp) = regex::Regex::new(r"(?m)^import\s+([\w.]+)") else {
        return out;
    };
    let Ok(re_from) = regex::Regex::new(r"(?m)^from\s+([\w.]+)\s+import\s+(.+)") else {
        return out;
    };
    for cap in re_imp.captures_iter(content) {
        if let (Some(full), Some(m)) = (cap.get(0), cap.get(1)) {
            out.push(ParsedImport {
                module: m.as_str().to_string(),
                style: ImportStyle::ModuleOnly,
                names: vec![],
                span: span_from_byte_range(content, full.start(), full.end()),
            });
        }
    }
    for cap in re_from.captures_iter(content) {
        if let (Some(full), Some(m), Some(tail)) = (cap.get(0), cap.get(1), cap.get(2)) {
            let tail = tail.as_str().trim();
            let (style, names) = if tail == "*" {
                (ImportStyle::Star, vec![])
            } else {
                (ImportStyle::Names, parse_import_names(tail))
            };
            out.push(ParsedImport {
                module: m.as_str().to_string(),
                style,
                names,
                span: span_from_byte_range(content, full.start(), full.end()),
            });
        }
    }
    out
}

fn extract_python_file_index_regex(path: &str, content: &str) -> Result<FileIndex, &'static str> {
    let mut idx = FileIndex::default();
    idx.symbols.push(ParsedSymbol {
        stable_key: "$file".into(),
        disambiguator: String::new(),
        qualified_name: path.to_string(),
        kind: NodeKind::File,
        span: whole_file_span(content),
    });
    let re = regex::Regex::new(r"(?m)^(?:async\s+)?def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")
        .map_err(|_| "regex")?;
    for cap in re.captures_iter(content) {
        if let (Some(full), Some(m)) = (cap.get(0), cap.get(1)) {
            let n = m.as_str().to_string();
            idx.symbols.push(ParsedSymbol {
                stable_key: n.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{n}"),
                kind: NodeKind::Function,
                span: span_from_byte_range(content, full.start(), full.end()),
            });
        }
    }
    idx.imports = extract_imports_regex(content);
    idx.calls = extract_calls_regex_top_level(path, content, &idx.symbols);
    idx.calls.extend(extract_calls_regex_module_level(content));
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

/// Lightweight call sites for module-level `def` bodies (regex ingest path).
fn extract_calls_regex_top_level(
    path: &str,
    content: &str,
    symbols: &[ParsedSymbol],
) -> Vec<ParsedCall> {
    let Ok(re_def) = regex::Regex::new(r"(?m)^(?:async\s+)?def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\([^)]*\)\s*:") else {
        return vec![];
    };
    let Ok(re_call) = regex::Regex::new(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(") else {
        return vec![];
    };
    let top_level: HashSet<&str> = symbols
        .iter()
        .filter(|s| s.kind == NodeKind::Function && !s.stable_key.contains('.'))
        .map(|s| s.stable_key.as_str())
        .collect();
    let mut calls = Vec::new();
    let mut spans: Vec<(usize, usize, String)> = Vec::new();
    for cap in re_def.captures_iter(content) {
        let Some(full) = cap.get(0) else { continue };
        let Some(name) = cap.get(1) else { continue };
        if !top_level.contains(name.as_str()) {
            continue;
        }
        spans.push((full.start(), full.end(), name.as_str().to_string()));
    }
    for (i, (body_start, _hdr_end, caller)) in spans.iter().enumerate() {
        let body_end = spans
            .get(i + 1)
            .map(|(next, _, _)| *next)
            .unwrap_or(content.len());
        let body = &content[*body_start..body_end];
        for cap in re_call.captures_iter(body) {
            let Some(full) = cap.get(0) else { continue };
            let Some(callee) = cap.get(1) else { continue };
            let callee_name = callee.as_str();
            if callee_name == caller
                || callee_name == "def"
                || callee_name == "async"
                || callee_name == "print"
                || callee_name == "super"
            {
                continue;
            }
            if full.start() > 0 && body.as_bytes().get(full.start() - 1) == Some(&b'.') {
                continue;
            }
            let byte = body_start + full.start();
            calls.push(ParsedCall {
                caller_stable_key: caller.clone(),
                callee: CallReceiver::bare(callee_name),
                span: span_from_byte_range(content, byte, byte + full.len()),
            });
        }
        let Ok(re_qualified) = regex::Regex::new(
            r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\(",
        ) else {
            continue;
        };
        for cap in re_qualified.captures_iter(body) {
            let Some(full) = cap.get(0) else { continue };
            let Some(base) = cap.get(1) else { continue };
            let Some(member) = cap.get(2) else { continue };
            if member.as_str() == "def" || member.as_str() == "async" {
                continue;
            }
            let byte = body_start + full.start();
            calls.push(ParsedCall {
                caller_stable_key: caller.clone(),
                callee: CallReceiver::Attr {
                    object: Box::new(CallReceiver::bare(base.as_str())),
                    name: member.as_str().to_string(),
                },
                span: span_from_byte_range(content, byte, byte + full.len()),
            });
        }
    }
    let _ = path;
    calls
}

/// Module-level `expr()` and `obj.method()` call sites (regex ingest path).
fn extract_calls_regex_module_level(content: &str) -> Vec<ParsedCall> {
    let Ok(re_qualified) = regex::Regex::new(
        r"(?m)^[^\n#]*?([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\(",
    ) else {
        return vec![];
    };
    let Ok(re_call) = regex::Regex::new(r"(?m)^([a-zA-Z_][a-zA-Z0-9_]*)\s*\(") else {
        return vec![];
    };
    let mut calls = Vec::new();
    for cap in re_qualified.captures_iter(content) {
        let Some(full) = cap.get(0) else { continue };
        let Some(base) = cap.get(1) else { continue };
        let Some(member) = cap.get(2) else { continue };
        if member.as_str() == "def" || member.as_str() == "if" {
            continue;
        }
        calls.push(ParsedCall {
            caller_stable_key: "$file".into(),
            callee: CallReceiver::Attr {
                object: Box::new(CallReceiver::bare(base.as_str())),
                name: member.as_str().to_string(),
            },
            span: span_from_byte_range(content, full.start(), full.end()),
        });
    }
    for cap in re_call.captures_iter(content) {
        let Some(full) = cap.get(0) else { continue };
        let Some(callee) = cap.get(1) else { continue };
        let c = callee.as_str();
        if c == "def" || c == "class" || c == "if" || c == "for" || c == "while" || c == "with" {
            continue;
        }
        if full.as_str().trim_start().starts_with("def ")
            || full.as_str().contains('.')
        {
            continue;
        }
        calls.push(ParsedCall {
            caller_stable_key: "$file".into(),
            callee: CallReceiver::bare(c),
            span: span_from_byte_range(content, full.start(), full.end()),
        });
    }
    calls
}

#[cfg(feature = "tree-sitter")]
fn extract_python_file_index_tree_sitter(path: &str, content: &str) -> Result<FileIndex, &'static str> {
    use tree_sitter::Node;

    use crate::index_model::span_from_tree_sitter_node;

    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .map_err(|_| "tree_sitter_language")?;
    let tree = parser.parse(content, None).ok_or("tree_sitter_parse")?;
    let root = tree.root_node();
    if root.has_error() {
        return Err("tree_sitter_error");
    }

    let mut idx = FileIndex::default();
    idx.symbols.push(ParsedSymbol {
        stable_key: "$file".into(),
        disambiguator: String::new(),
        qualified_name: path.to_string(),
        kind: NodeKind::File,
        span: whole_file_span(content),
    });

    fn parse_call_receiver(node: Node, src: &str) -> Option<CallReceiver> {
        match node.kind() {
            "identifier" => {
                let name = node.utf8_text(src.as_bytes()).ok()?.to_string();
                Some(CallReceiver::Bare(name))
            }
            "attribute" => {
                let obj = node.child_by_field_name("object")?;
                let attr = node.child_by_field_name("attribute")?;
                let obj_r = parse_call_receiver(obj, src)?;
                let name = attr.utf8_text(src.as_bytes()).ok()?.to_string();
                Some(CallReceiver::Attr {
                    object: Box::new(obj_r),
                    name,
                })
            }
            _ => None,
        }
    }

    fn type_name_from_annotation(node: Node, src: &str) -> Option<String> {
        match node.kind() {
            "type" => node
                .named_child(0)
                .and_then(|c| type_name_from_annotation(c, src)),
            "identifier" => node.utf8_text(src.as_bytes()).ok().map(|s| s.to_string()),
            "attribute" => node
                .utf8_text(src.as_bytes())
                .ok()
                .and_then(|s| s.rsplit('.').next().map(|x| x.to_string())),
            _ => None,
        }
    }

    fn infer_type_from_expr(node: Node, src: &str) -> Option<String> {
        match node.kind() {
            "call" => {
                let func = node.child_by_field_name("function")?;
                parse_call_receiver(func, src)?
                    .root_bare_name()
                    .map(|s| s.to_string())
            }
            "attribute" => parse_call_receiver(node, src)?
                .root_bare_name()
                .map(|s| s.to_string()),
            _ => None,
        }
    }

    fn self_field_from_target(node: Node, src: &str) -> Option<String> {
        if node.kind() != "attribute" {
            return None;
        }
        let obj = node.child_by_field_name("object")?;
        let attr = node.child_by_field_name("attribute")?;
        if obj.kind() != "identifier" {
            return None;
        }
        if obj.utf8_text(src.as_bytes()).ok()? != "self" {
            return None;
        }
        attr.utf8_text(src.as_bytes()).ok().map(|s| s.to_string())
    }

    fn field_assignment_from_node(node: Node, src: &str) -> Option<(String, String)> {
        match node.kind() {
            "assignment" => {
                let left = node.child_by_field_name("left")?;
                let right = node.child_by_field_name("right")?;
                let field = self_field_from_target(left, src)?;
                let type_name = infer_type_from_expr(right, src)?;
                Some((field, type_name))
            }
            "annotated_assignment" => {
                let name = node.child_by_field_name("name")?;
                let field = self_field_from_target(name, src)?;
                let type_name = node
                    .child_by_field_name("type")
                    .and_then(|t| type_name_from_annotation(t, src))
                    .or_else(|| {
                        node.child_by_field_name("value")
                            .and_then(|v| infer_type_from_expr(v, src))
                    })?;
                Some((field, type_name))
            }
            _ => None,
        }
    }

    fn record_instance_field_assignment(
        node: Node,
        src: &str,
        class_key: &str,
        idx: &mut FileIndex,
    ) {
        let Some((field, type_name)) = field_assignment_from_node(node, src) else {
            return;
        };
        if type_name == "self" {
            return;
        }
        let fields = idx.instance_fields.entry(class_key.to_string()).or_default();
        if fields
            .get(&field)
            .is_some_and(|existing| existing != "self")
        {
            return;
        }
        fields.insert(field, type_name);
    }

    fn visit_instance_fields(node: Node, src: &str, class_key: &str, idx: &mut FileIndex) {
        if matches!(node.kind(), "assignment" | "annotated_assignment") {
            record_instance_field_assignment(node, src, class_key, idx);
        }
        let mut i = 0usize;
        while let Some(c) = node.named_child(i) {
            visit_instance_fields(c, src, class_key, idx);
            i += 1;
        }
    }

    fn first_call_argument(node: Node) -> Option<Node> {
        if node.kind() != "call" {
            return None;
        }
        let args = node.child_by_field_name("arguments")?;
        let mut i = 0usize;
        while let Some(c) = args.named_child(i) {
            if c.kind() != "comment" {
                return Some(c);
            }
            i += 1;
        }
        None
    }

    fn record_list_append(node: Node, src: &str, idx: &mut FileIndex) {
        if node.kind() != "call" {
            return;
        }
        let Some(func) = node.child_by_field_name("function") else {
            return;
        };
        if func.kind() != "attribute" {
            return;
        }
        let Some(method) = func.child_by_field_name("attribute") else {
            return;
        };
        if method.utf8_text(src.as_bytes()).ok().as_deref() != Some("append") {
            return;
        }
        let Some(list_obj) = func.child_by_field_name("object") else {
            return;
        };
        if list_obj.kind() != "identifier" {
            return;
        }
        let Some(list_name) = list_obj.utf8_text(src.as_bytes()).ok().map(|s| s.to_string()) else {
            return;
        };
        let Some(arg) = first_call_argument(node) else {
            return;
        };
        let Some(type_name) = infer_type_from_expr(arg, src) else {
            return;
        };
        if type_name == "self" {
            return;
        }
        idx.list_element_types.insert(list_name, type_name);
    }

    fn record_for_loop_binding(node: Node, src: &str, fn_key: &str, idx: &mut FileIndex) {
        if node.kind() != "for_statement" {
            return;
        }
        let Some(left) = node.child_by_field_name("left") else {
            return;
        };
        if left.kind() != "identifier" {
            return;
        }
        let Some(loop_var) = left.utf8_text(src.as_bytes()).ok().map(|s| s.to_string()) else {
            return;
        };
        let Some(right) = node.child_by_field_name("right") else {
            return;
        };
        if right.kind() != "identifier" {
            return;
        };
        let Some(iter_name) = right.utf8_text(src.as_bytes()).ok().map(|s| s.to_string()) else {
            return;
        };
        let Some(elem_type) = idx.list_element_types.get(&iter_name).cloned() else {
            return;
        };
        idx.function_locals
            .entry(fn_key.to_string())
            .or_default()
            .insert(loop_var, elem_type);
    }

    fn collect_list_appends(node: Node, src: &str, idx: &mut FileIndex) {
        if node.kind() == "call" {
            record_list_append(node, src, idx);
        }
        let mut i = 0usize;
        while let Some(c) = node.named_child(i) {
            collect_list_appends(c, src, idx);
            i += 1;
        }
    }

    fn bind_for_loop_vars(node: Node, src: &str, fn_key: &str, idx: &mut FileIndex) {
        if node.kind() == "for_statement" {
            record_for_loop_binding(node, src, fn_key, idx);
        }
        let mut i = 0usize;
        while let Some(c) = node.named_child(i) {
            bind_for_loop_vars(c, src, fn_key, idx);
            i += 1;
        }
    }

    fn simple_type_name_from_node(node: Node, src: &str) -> Option<String> {
        match node.kind() {
            "identifier" => node
                .utf8_text(src.as_bytes())
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            "attribute" => node
                .child_by_field_name("attribute")
                .and_then(|n| n.utf8_text(src.as_bytes()).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            _ => None,
        }
    }

    fn record_type_use(
        uses: &mut Vec<ParsedUse>,
        owner_stable_key: &str,
        type_node: Node,
        src: &str,
    ) {
        if let Some(type_name) = simple_type_name_from_node(type_node, src) {
            uses.push(ParsedUse {
                owner_stable_key: owner_stable_key.to_string(),
                type_name,
                span: span_from_tree_sitter_node(type_node),
            });
        }
    }

    fn visit_type_uses(node: Node, src: &str, owner_sk: &str, uses: &mut Vec<ParsedUse>) {
        match node.kind() {
            "typed_parameter" => {
                if let Some(t) = node.child_by_field_name("type") {
                    record_type_use(uses, owner_sk, t, src);
                }
            }
            "type" => record_type_use(uses, owner_sk, node, src),
            _ => {}
        }
        let mut i = 0usize;
        while let Some(c) = node.named_child(i) {
            visit_type_uses(c, src, owner_sk, uses);
            i += 1;
        }
    }

    fn visit_calls(
        node: Node,
        src: &str,
        caller_sk: &str,
        calls: &mut Vec<ParsedCall>,
        uses: &mut Vec<ParsedUse>,
    ) {
        if node.kind() == "call" {
            if let Some(f) = node.child_by_field_name("function") {
                if let Some(callee) = parse_call_receiver(f, src) {
                    let leaf = callee.leaf_name();
                    if leaf == "isinstance" {
                        if let Some(args) = node.child_by_field_name("arguments") {
                            if let Some(type_node) = args.named_child(1) {
                                record_type_use(uses, caller_sk, type_node, src);
                            }
                        }
                    } else if leaf != "print" && leaf != "super" {
                        calls.push(ParsedCall {
                            caller_stable_key: caller_sk.to_string(),
                            callee,
                            span: span_from_tree_sitter_node(node),
                        });
                    }
                }
            }
        }
        let mut i = 0usize;
        while let Some(c) = node.named_child(i) {
            visit_calls(c, src, caller_sk, calls, uses);
            i += 1;
        }
    }

    fn visit_module_level_calls(node: Node, src: &str, calls: &mut Vec<ParsedCall>, uses: &mut Vec<ParsedUse>) {
        if node.kind() == "expression_statement" {
            visit_calls(node, src, "$file", calls, uses);
            return;
        }
        if node.kind() == "module" || node.kind() == "block" {
            let mut i = 0usize;
            while let Some(c) = node.named_child(i) {
                if c.kind() == "expression_statement" {
                    visit_calls(c, src, "$file", calls, uses);
                }
                i += 1;
            }
        }
    }

    fn visit(
        node: Node,
        src: &str,
        path: &str,
        cls: &mut Vec<String>,
        idx: &mut FileIndex,
    ) {
        match node.kind() {
            "module" | "block" => {
                let mut i = 0usize;
                while let Some(c) = node.named_child(i) {
                    visit(c, src, path, cls, idx);
                    i += 1;
                }
            }
            "decorated_definition" => {
                let mut i = 0usize;
                while let Some(c) = node.named_child(i) {
                    visit(c, src, path, cls, idx);
                    i += 1;
                }
            }
            "class_definition" => {
                let name = node
                    .child_by_field_name("name")
                    .and_then(|n| n.utf8_text(src.as_bytes()).ok())
                    .unwrap_or("");
                cls.push(name.to_string());
                let stable = cls.join(".");
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: format!("{path}::{stable}"),
                    kind: NodeKind::Class,
                    span: span_from_tree_sitter_node(node),
                });
                if let Some(superclasses) = node.child_by_field_name("superclasses") {
                    let mut si = 0usize;
                    while let Some(c) = superclasses.named_child(si) {
                        if let Some(base_name) = simple_type_name_from_node(c, src) {
                            if base_name != "object" {
                                idx.extends.push(ParsedExtends {
                                    class_stable_key: stable.clone(),
                                    base_name,
                                    span: span_from_tree_sitter_node(c),
                                });
                            }
                        }
                        si += 1;
                    }
                }
                let mut i = 0usize;
                while let Some(c) = node.named_child(i) {
                    if c.kind() == "block" {
                        visit(c, src, path, cls, idx);
                    }
                    i += 1;
                }
                cls.pop();
            }
            "function_definition" => {
                let name = node
                    .child_by_field_name("name")
                    .and_then(|n| n.utf8_text(src.as_bytes()).ok())
                    .unwrap_or("");
                let stable = if cls.is_empty() {
                    name.to_string()
                } else {
                    format!("{}.{}", cls.join("."), name)
                };
                let qn = format!("{path}::{stable}");
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: qn,
                    kind: NodeKind::Function,
                    span: span_from_tree_sitter_node(node),
                });
                if let Some(params) = node.child_by_field_name("parameters") {
                    visit_type_uses(params, src, &stable, &mut idx.uses);
                }
                if let Some(ret) = node.child_by_field_name("return_type") {
                    record_type_use(&mut idx.uses, &stable, ret, src);
                }
                let class_key = cls.join(".");
                let mut i = 0usize;
                while let Some(c) = node.named_child(i) {
                    if c.kind() == "block" {
                        collect_list_appends(c, src, idx);
                        bind_for_loop_vars(c, src, &stable, idx);
                        visit_calls(c, src, &stable, &mut idx.calls, &mut idx.uses);
                        if !class_key.is_empty() {
                            visit_instance_fields(c, src, &class_key, idx);
                        }
                    }
                    i += 1;
                }
            }
            _ => {}
        }
    }

    visit(root, content, path, &mut Vec::new(), &mut idx);
    visit_module_level_calls(root, content, &mut idx.calls, &mut idx.uses);
    idx.imports = extract_imports_regex(content);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

pub fn index_python_file(path: &str, content: &str) -> Result<FileIndex, &'static str> {
    #[cfg(feature = "tree-sitter")]
    {
        match extract_python_file_index_tree_sitter(path, content) {
            Ok(i) => Ok(i),
            Err(_) => extract_python_file_index_regex(path, content),
        }
    }
    #[cfg(not(feature = "tree-sitter"))]
    {
        extract_python_file_index_regex(path, content)
    }
}

/// **FR-1.1** — top-level `def` names (legacy helper for tests). Full indexing uses [`index_python_file`].
pub fn extract_python_top_level_defs(src: &str) -> Result<Vec<String>, &'static str> {
    let idx = index_python_file("__anon__.py", src)?;
    Ok(idx
        .symbols
        .iter()
        .filter(|s| s.kind == NodeKind::Function && !s.stable_key.contains('.'))
        .map(|s| s.stable_key.clone())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_model::symbol_identity_key;

    #[cfg(feature = "tree-sitter")]
    #[test]
    fn indexer_disambiguates_class_and_function_same_name() {
        let src = "class foo:\n    pass\n\ndef foo():\n    return 1\n";
        let idx = index_python_file("m.py", src).unwrap();
        let classes: Vec<_> = idx
            .symbols
            .iter()
            .filter(|s| s.kind == NodeKind::Class && s.stable_key == "foo")
            .collect();
        let funcs: Vec<_> = idx
            .symbols
            .iter()
            .filter(|s| s.kind == NodeKind::Function && s.stable_key == "foo")
            .collect();
        assert_eq!(classes.len(), 1);
        assert_eq!(funcs.len(), 1);
        assert_eq!(classes[0].disambiguator, "class");
        assert_eq!(funcs[0].disambiguator, "");
        assert_eq!(classes[0].qualified_name, "m.py::foo");
        assert_eq!(funcs[0].qualified_name, "m.py::foo");
        assert_ne!(classes[0].identity_key(), funcs[0].identity_key());
        assert_eq!(funcs[0].identity_key(), symbol_identity_key("foo", ""));
        assert_eq!(classes[0].identity_key(), symbol_identity_key("foo", "class"));
    }

    #[test]
    fn indexer_disambiguates_duplicate_function_defs() {
        let src = "def foo(x: int):\n    ...\n\ndef foo(x: str):\n    ...\n\ndef foo(x):\n    return x\n";
        let idx = index_python_file("m.py", src).unwrap();
        let foos: Vec<_> = idx
            .symbols
            .iter()
            .filter(|s| s.kind == NodeKind::Function && s.stable_key == "foo")
            .collect();
        assert_eq!(foos.len(), 3);
        assert_eq!(foos[0].disambiguator, "fn:0");
        assert_eq!(foos[1].disambiguator, "fn:1");
        assert_eq!(foos[2].disambiguator, "");
        let keys: std::collections::HashSet<_> =
            foos.iter().map(|s| s.identity_key()).collect();
        assert_eq!(keys.len(), 3);
        assert!(foos.iter().all(|s| s.qualified_name == "m.py::foo"));
    }
}
