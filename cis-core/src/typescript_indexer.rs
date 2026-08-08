//! TypeScript/TSX indexer — regex fallback + optional tree-sitter path.
//!
//! When `ts-typescript` feature is enabled, uses tree-sitter for rich extraction of
//! classes, interfaces, enums, type aliases, functions, methods, arrow functions,
//! imports, call sites, extends/implements, decorators, and type annotations.
//! Falls back to regex when the feature is disabled or parsing fails.

use crate::graph::{NodeKind, SourceSpan};
use crate::index_model::{
    assign_collision_disambiguators, whole_file_span, FileIndex, ImportStyle, ParsedCall,
    ParsedImport, ParsedSymbol,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_typescript_module_key(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".ts")
        .trim_end_matches(".tsx")
        .replace('/', ".")
}

// ───────────────────── regex fallback (unchanged) ─────────────────────

fn index_typescript_file_regex(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut idx = FileIndex::default();
    idx.symbols.push(ParsedSymbol {
        stable_key: "$file".into(),
        disambiguator: String::new(),
        qualified_name: path.to_string(),
        kind: NodeKind::File,
        span: whole_file_span(content),
    });

    let re_fn = regex::Regex::new(
        r"(?m)(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    )
    .map_err(|_| IndexError::ParseFailed)?;
    for cap in re_fn.captures_iter(content) {
        let Some(full) = cap.get(0) else { continue };
        let Some(name) = cap.get(1) else { continue };
        let n = name.as_str().to_string();
        idx.symbols.push(ParsedSymbol {
            stable_key: n.clone(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::{n}"),
            kind: NodeKind::Function,
            span: span_range(content, full.start(), full.end()),
        });
    }

    idx.imports = extract_imports_ts_regex(content);
    idx.calls = extract_calls_ts_regex(content, &idx.symbols);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

fn span_range(content: &str, start: usize, end: usize) -> SourceSpan {
    crate::index_model::span_from_byte_range(content, start, end)
}

fn extract_imports_ts_regex(content: &str) -> Vec<ParsedImport> {
    let mut out = Vec::new();
    let Ok(re_from) =
        regex::Regex::new(r#"(?m)^import\s+(?:\{([^}]+)\}|([A-Za-z_][\w]*))\s+from\s+['"]([^'"]+)['"]"#)
    else {
        return out;
    };
    for cap in re_from.captures_iter(content) {
        let Some(full) = cap.get(0) else { continue };
        let module = cap.get(3).map(|m| m.as_str().to_string()).unwrap_or_default();
        let names_part = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let default_name = cap.get(2).map(|m| m.as_str());
        let (style, names) = if !names_part.is_empty() {
            (
                ImportStyle::Names,
                names_part
                    .split(',')
                    .filter_map(|p| {
                        let n = p.trim().split(':').next()?.trim();
                        if n.is_empty() { None } else { Some(n.to_string()) }
                    })
                    .collect(),
            )
        } else if let Some(d) = default_name {
            (ImportStyle::Names, vec![d.to_string()])
        } else {
            (ImportStyle::ModuleOnly, vec![])
        };
        out.push(ParsedImport {
            module,
            style,
            names,
            span: span_range(content, full.start(), full.end()),
        });
    }
    out
}

fn extract_calls_ts_regex(content: &str, symbols: &[ParsedSymbol]) -> Vec<ParsedCall> {
    let Ok(re_call) = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(") else {
        return vec![];
    };
    let Ok(re_qualified) =
        regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\(")
    else {
        return vec![];
    };
    let callers: Vec<String> = symbols
        .iter()
        .filter(|s| s.kind == NodeKind::Function)
        .map(|s| s.stable_key.clone())
        .collect();
    let mut calls = Vec::new();
    for caller in &callers {
        let body = content;
        for cap in re_qualified.captures_iter(body) {
            let Some(full) = cap.get(0) else { continue };
            let Some(base) = cap.get(1) else { continue };
            let Some(member) = cap.get(2) else { continue };
            calls.push(ParsedCall {
                caller_stable_key: caller.clone(),
                callee: crate::index_model::CallReceiver::Attr {
                    object: Box::new(crate::index_model::CallReceiver::Bare(
                        base.as_str().to_string(),
                    )),
                    name: member.as_str().to_string(),
                },
                span: span_range(content, full.start(), full.end()),
            });
        }
        for cap in re_call.captures_iter(body) {
            let Some(full) = cap.get(0) else { continue };
            let Some(callee) = cap.get(1) else { continue };
            if full.start() > 0 && body.as_bytes().get(full.start() - 1) == Some(&b'.') {
                continue;
            }
            let name = callee.as_str();
            if name == "function" || name == caller {
                continue;
            }
            calls.push(ParsedCall {
                caller_stable_key: caller.clone(),
                callee: crate::index_model::CallReceiver::bare(name),
                span: span_range(content, full.start(), full.end()),
            });
        }
    }
    calls
}

// ───────────────────── tree-sitter path ─────────────────────

#[cfg(feature = "ts-typescript")]
fn index_typescript_file_tree_sitter(
    path: &str,
    content: &str,
    tsx: bool,
) -> Result<FileIndex, IndexError> {
    use tree_sitter::Node;
    use crate::index_model::{
        span_from_tree_sitter_node, CallReceiver, ParsedExtends, ParsedUse,
    };

    let mut parser = tree_sitter::Parser::new();
    let lang = if tsx {
        tree_sitter_typescript::LANGUAGE_TSX.into()
    } else {
        tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
    };
    parser.set_language(&lang).map_err(|_| IndexError::ParseFailed)?;
    let tree = parser.parse(content, None).ok_or(IndexError::ParseFailed)?;
    let root = tree.root_node();

    let mut idx = FileIndex::default();
    idx.symbols.push(ParsedSymbol {
        stable_key: "$file".into(),
        disambiguator: String::new(),
        qualified_name: path.to_string(),
        kind: NodeKind::File,
        span: whole_file_span(content),
    });

    fn node_text<'a>(node: Node, src: &'a str) -> &'a str {
        node.utf8_text(src.as_bytes()).unwrap_or("")
    }

    fn parse_call_receiver(node: Node, src: &str) -> Option<CallReceiver> {
        match node.kind() {
            "identifier" => Some(CallReceiver::Bare(node_text(node, src).to_string())),
            "this" => Some(CallReceiver::Bare("this".into())),
            "member_expression" => {
                let obj = node.child_by_field_name("object")?;
                let prop = node.child_by_field_name("property")?;
                let obj_r = parse_call_receiver(obj, src)?;
                Some(CallReceiver::Attr {
                    object: Box::new(obj_r),
                    name: node_text(prop, src).to_string(),
                })
            }
            _ => None,
        }
    }

    fn simple_type_name(node: Node, src: &str) -> Option<String> {
        match node.kind() {
            "type_identifier" | "identifier" => {
                let t = node_text(node, src).trim().to_string();
                if t.is_empty() { None } else { Some(t) }
            }
            "generic_type" | "type_reference" => {
                node.named_child(0).and_then(|c| simple_type_name(c, src))
            }
            "nested_type_identifier" => {
                let c = node.named_child_count();
                if c > 0 {
                    node.named_child(c - 1)
                        .map(|n| node_text(n, src).trim().to_string())
                        .filter(|s| !s.is_empty())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
        if let Some(name) = simple_type_name(node, src) {
            let builtin = matches!(
                name.as_str(),
                "void" | "string" | "number" | "boolean" | "any" | "unknown"
                    | "never" | "undefined" | "null" | "symbol" | "bigint" | "object"
            );
            if !builtin {
                uses.push(ParsedUse {
                    owner_stable_key: owner.to_string(),
                    type_name: name,
                    span: span_from_tree_sitter_node(node),
                });
            }
        }
    }

    fn visit_type_annotations(node: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
        match node.kind() {
            "type_annotation" => {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        record_type_use(uses, owner, c, src);
                    }
                }
            }
            "type_identifier" | "generic_type" | "type_reference" => {
                record_type_use(uses, owner, node, src);
            }
            _ => {}
        }
        let count = node.named_child_count();
        for i in 0..count {
            if let Some(c) = node.named_child(i) {
                visit_type_annotations(c, src, owner, uses);
            }
        }
    }

    fn visit_calls(
        node: Node, src: &str, caller: &str,
        calls: &mut Vec<ParsedCall>, uses: &mut Vec<ParsedUse>,
    ) {
        match node.kind() {
            "call_expression" => {
                if let Some(func) = node.child_by_field_name("function") {
                    if let Some(callee) = parse_call_receiver(func, src) {
                        let leaf = callee.leaf_name();
                        if !matches!(leaf,
                            "console" | "log" | "warn" | "error" | "info" | "debug"
                            | "require" | "parseInt" | "parseFloat" | "isNaN"
                            | "setTimeout" | "setInterval"
                        ) {
                            calls.push(ParsedCall {
                                caller_stable_key: caller.to_string(),
                                callee,
                                span: span_from_tree_sitter_node(node),
                            });
                        }
                    }
                }
            }
            "new_expression" => {
                if let Some(constructor) = node.child_by_field_name("constructor") {
                    if let Some(callee) = parse_call_receiver(constructor, src) {
                        calls.push(ParsedCall {
                            caller_stable_key: caller.to_string(),
                            callee,
                            span: span_from_tree_sitter_node(node),
                        });
                    }
                }
            }
            "jsx_self_closing_element" | "jsx_opening_element" => {
                if let Some(name_node) = node.child_by_field_name("name") {
                    let tag = node_text(name_node, src);
                    if !tag.is_empty() && tag.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                        uses.push(ParsedUse {
                            owner_stable_key: caller.to_string(),
                            type_name: tag.to_string(),
                            span: span_from_tree_sitter_node(name_node),
                        });
                    }
                }
            }
            _ => {}
        }
        let count = node.named_child_count();
        for i in 0..count {
            if let Some(c) = node.named_child(i) {
                visit_calls(c, src, caller, calls, uses);
            }
        }
    }

    fn extract_imports(root: Node, src: &str) -> Vec<ParsedImport> {
        let mut imports = Vec::new();
        let count = root.named_child_count();
        for i in 0..count {
            let Some(child) = root.named_child(i) else { continue };
            if child.kind() == "import_statement" {
                extract_single_import(child, src, &mut imports);
            }
        }
        imports
    }

    fn extract_single_import(node: Node, src: &str, imports: &mut Vec<ParsedImport>) {
        let source = node.child_by_field_name("source")
            .map(|n| {
                let t = node_text(n, src);
                t.trim_matches('\'').trim_matches('"').to_string()
            })
            .unwrap_or_default();

        let mut names = Vec::new();
        let mut style = ImportStyle::ModuleOnly;

        let nc = node.named_child_count();
        for i in 0..nc {
            let Some(child) = node.named_child(i) else { continue };
            match child.kind() {
                "import_clause" => {
                    let ic = child.named_child_count();
                    for j in 0..ic {
                        let Some(inner) = child.named_child(j) else { continue };
                        match inner.kind() {
                            "identifier" => {
                                names.push(node_text(inner, src).to_string());
                                style = ImportStyle::Names;
                            }
                            "named_imports" => {
                                collect_named_imports(inner, src, &mut names);
                                style = ImportStyle::Names;
                            }
                            "namespace_import" => {
                                style = ImportStyle::Star;
                                if let Some(alias) = inner.child_by_field_name("name") {
                                    names.push(node_text(alias, src).to_string());
                                }
                            }
                            _ => {}
                        }
                    }
                }
                "named_imports" => {
                    collect_named_imports(child, src, &mut names);
                    style = ImportStyle::Names;
                }
                "identifier" => {
                    names.push(node_text(child, src).to_string());
                    style = ImportStyle::Names;
                }
                "namespace_import" => {
                    style = ImportStyle::Star;
                }
                _ => {}
            }
        }

        imports.push(ParsedImport {
            module: source,
            style,
            names,
            span: span_from_tree_sitter_node(node),
        });
    }

    fn collect_named_imports(node: Node, src: &str, names: &mut Vec<String>) {
        let count = node.named_child_count();
        for i in 0..count {
            let Some(spec) = node.named_child(i) else { continue };
            if spec.kind() == "import_specifier" {
                let local = spec.child_by_field_name("alias")
                    .or_else(|| spec.child_by_field_name("name"))
                    .map(|n| node_text(n, src).to_string());
                if let Some(n) = local {
                    if !n.is_empty() {
                        names.push(n);
                    }
                }
            }
        }
    }

    fn visit_module_level_calls(
        root: Node, src: &str,
        calls: &mut Vec<ParsedCall>, uses: &mut Vec<ParsedUse>,
    ) {
        let count = root.named_child_count();
        for i in 0..count {
            let Some(child) = root.named_child(i) else { continue };
            if child.kind() == "expression_statement" {
                visit_calls(child, src, "$file", calls, uses);
            }
        }
    }

    fn this_field_from_target(node: Node, src: &str) -> Option<String> {
        if node.kind() != "member_expression" {
            return None;
        }
        let obj = node.child_by_field_name("object")?;
        let prop = node.child_by_field_name("property")?;
        if obj.kind() != "this" {
            return None;
        }
        let name = node_text(prop, src).trim().to_string();
        if name.is_empty() {
            None
        } else {
            Some(name)
        }
    }

    fn infer_type_from_expr(node: Node, src: &str) -> Option<String> {
        match node.kind() {
            "new_expression" => {
                let constructor = node.child_by_field_name("constructor")?;
                parse_call_receiver(constructor, src)?
                    .root_bare_name()
                    .map(|s| s.to_string())
            }
            "call_expression" => {
                let func = node.child_by_field_name("function")?;
                parse_call_receiver(func, src)?
                    .root_bare_name()
                    .map(|s| s.to_string())
            }
            _ => None,
        }
    }

    fn field_assignment_from_node(node: Node, src: &str) -> Option<(String, String)> {
        if node.kind() != "assignment_expression" {
            return None;
        }
        let left = node.child_by_field_name("left")?;
        let right = node.child_by_field_name("right")?;
        let field = this_field_from_target(left, src)?;
        let type_name = infer_type_from_expr(right, src)?;
        Some((field, type_name))
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
        if type_name == "this" || type_name == "self" {
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
        if node.kind() == "assignment_expression" {
            record_instance_field_assignment(node, src, class_key, idx);
        }
        let count = node.named_child_count();
        for i in 0..count {
            if let Some(c) = node.named_child(i) {
                visit_instance_fields(c, src, class_key, idx);
            }
        }
    }

    fn record_heritage_bases(heritage: Node, src: &str, class_key: &str, idx: &mut FileIndex) {
        let hc = heritage.named_child_count();
        for j in 0..hc {
            let Some(h) = heritage.named_child(j) else { continue };
            match h.kind() {
                "extends_clause" | "implements_clause" => {
                    let ec = h.named_child_count();
                    for k in 0..ec {
                        let Some(base) = h.named_child(k) else { continue };
                        if let Some(base_name) = simple_type_name(base, src) {
                            if base_name != "object" {
                                idx.extends.push(ParsedExtends {
                                    class_stable_key: class_key.to_string(),
                                    base_name,
                                    span: span_from_tree_sitter_node(base),
                                });
                            }
                        }
                    }
                }
                _ => {
                    if let Some(base_name) = simple_type_name(h, src) {
                        if base_name != "object" {
                            idx.extends.push(ParsedExtends {
                                class_stable_key: class_key.to_string(),
                                base_name,
                                span: span_from_tree_sitter_node(h),
                            });
                        }
                    }
                }
            }
        }
    }

    fn index_class_like(
        node: Node, src: &str, path: &str,
        cls: &mut Vec<String>, idx: &mut FileIndex,
    ) {
        let name = node.child_by_field_name("name")
            .map(|n| node_text(n, src))
            .unwrap_or("");
        if name.is_empty() { return; }
        cls.push(name.to_string());
        let stable = cls.join(".");
        idx.symbols.push(ParsedSymbol {
            stable_key: stable.clone(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::{stable}"),
            kind: NodeKind::Class,
            span: span_from_tree_sitter_node(node),
        });
        let nc = node.named_child_count();
        for i in 0..nc {
            let Some(child) = node.named_child(i) else { continue };
            if child.kind() == "class_heritage" {
                record_heritage_bases(child, src, &stable, idx);
            }
        }
        if let Some(tp) = node.child_by_field_name("type_parameters") {
            visit_type_annotations(tp, src, &stable, &mut idx.uses);
        }
        if let Some(body) = node.child_by_field_name("body") {
            visit_class_body(body, src, path, cls, idx);
        }
        cls.pop();
    }

    fn visit_class_body(
        body: Node, src: &str, path: &str,
        cls: &mut Vec<String>, idx: &mut FileIndex,
    ) {
        let count = body.named_child_count();
        for i in 0..count {
            let Some(member) = body.named_child(i) else { continue };
            match member.kind() {
                "method_definition" | "public_field_definition" | "method_signature" => {
                    let name = member.child_by_field_name("name")
                        .map(|n| node_text(n, src))
                        .unwrap_or("");
                    if name.is_empty() { continue; }
                    let is_fn = matches!(member.kind(), "method_definition" | "method_signature")
                        || member.child_by_field_name("value")
                            .map(|v| matches!(v.kind(), "arrow_function" | "function_expression" | "function"))
                            .unwrap_or(false);
                    if is_fn {
                        let stable = format!("{}.{}", cls.join("."), name);
                        idx.symbols.push(ParsedSymbol {
                            stable_key: stable.clone(),
                            disambiguator: String::new(),
                            qualified_name: format!("{path}::{stable}"),
                            kind: NodeKind::Function,
                            span: span_from_tree_sitter_node(member),
                        });
                        if let Some(params) = member.child_by_field_name("parameters") {
                            visit_type_annotations(params, src, &stable, &mut idx.uses);
                        }
                        if let Some(ret) = member.child_by_field_name("return_type") {
                            visit_type_annotations(ret, src, &stable, &mut idx.uses);
                        }
                        if let Some(body) = member.child_by_field_name("body") {
                            visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                            let class_key = cls.join(".");
                            if !class_key.is_empty() {
                                visit_instance_fields(body, src, &class_key, idx);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn visit(
        node: Node, src: &str, path: &str,
        cls: &mut Vec<String>, idx: &mut FileIndex,
    ) {
        match node.kind() {
            "program" | "statement_block" => {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        visit(c, src, path, cls, idx);
                    }
                }
            }
            "export_statement" => {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        visit(c, src, path, cls, idx);
                    }
                }
            }
            "decorator" | "decorators" => {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        visit(c, src, path, cls, idx);
                    }
                }
            }
            "function_declaration" | "generator_function_declaration" => {
                let name = node.child_by_field_name("name")
                    .map(|n| node_text(n, src))
                    .unwrap_or("");
                if name.is_empty() { return; }
                let stable = if cls.is_empty() {
                    name.to_string()
                } else {
                    format!("{}.{}", cls.join("."), name)
                };
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: format!("{path}::{stable}"),
                    kind: NodeKind::Function,
                    span: span_from_tree_sitter_node(node),
                });
                if let Some(params) = node.child_by_field_name("parameters") {
                    visit_type_annotations(params, src, &stable, &mut idx.uses);
                }
                if let Some(ret) = node.child_by_field_name("return_type") {
                    visit_type_annotations(ret, src, &stable, &mut idx.uses);
                }
                if let Some(body) = node.child_by_field_name("body") {
                    visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                }
            }
            "class_declaration" | "abstract_class_declaration" => {
                index_class_like(node, src, path, cls, idx);
            }
            "interface_declaration" => {
                let name = node.child_by_field_name("name")
                    .map(|n| node_text(n, src))
                    .unwrap_or("");
                if name.is_empty() { return; }
                let stable = if cls.is_empty() {
                    name.to_string()
                } else {
                    format!("{}.{}", cls.join("."), name)
                };
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: format!("{path}::{stable}"),
                    kind: NodeKind::Class,
                    span: span_from_tree_sitter_node(node),
                });
                // extends interfaces
                let nc = node.named_child_count();
                for i in 0..nc {
                    let Some(child) = node.named_child(i) else { continue };
                    if child.kind() == "extends_type_clause" || child.kind() == "extends_clause" {
                        let ec = child.named_child_count();
                        for j in 0..ec {
                            if let Some(base) = child.named_child(j) {
                                if let Some(base_name) = simple_type_name(base, src) {
                                    idx.extends.push(ParsedExtends {
                                        class_stable_key: stable.clone(),
                                        base_name,
                                        span: span_from_tree_sitter_node(base),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            "type_alias_declaration" => {
                let name = node.child_by_field_name("name")
                    .map(|n| node_text(n, src))
                    .unwrap_or("");
                if !name.is_empty() {
                    let stable = if cls.is_empty() {
                        name.to_string()
                    } else {
                        format!("{}.{}", cls.join("."), name)
                    };
                    idx.symbols.push(ParsedSymbol {
                        stable_key: stable.clone(),
                        disambiguator: String::new(),
                        qualified_name: format!("{path}::{stable}"),
                        kind: NodeKind::Class,
                        span: span_from_tree_sitter_node(node),
                    });
                    if let Some(val) = node.child_by_field_name("value") {
                        visit_type_annotations(val, src, &stable, &mut idx.uses);
                    }
                }
            }
            "enum_declaration" => {
                let name = node.child_by_field_name("name")
                    .map(|n| node_text(n, src))
                    .unwrap_or("");
                if !name.is_empty() {
                    let stable = if cls.is_empty() {
                        name.to_string()
                    } else {
                        format!("{}.{}", cls.join("."), name)
                    };
                    idx.symbols.push(ParsedSymbol {
                        stable_key: stable,
                        disambiguator: String::new(),
                        qualified_name: format!("{path}::{name}"),
                        kind: NodeKind::Class,
                        span: span_from_tree_sitter_node(node),
                    });
                }
            }
            "lexical_declaration" | "variable_declaration" => {
                let count = node.named_child_count();
                for i in 0..count {
                    let Some(decl) = node.named_child(i) else { continue };
                    if decl.kind() == "variable_declarator" {
                        let name_node = decl.child_by_field_name("name");
                        let value_node = decl.child_by_field_name("value");
                        if let (Some(name_n), Some(val)) = (name_node, value_node) {
                            if name_n.kind() == "identifier" {
                                let is_fn = matches!(val.kind(),
                                    "arrow_function" | "function_expression"
                                    | "function" | "generator_function"
                                );
                                if is_fn {
                                    let n = node_text(name_n, src);
                                    let stable = if cls.is_empty() {
                                        n.to_string()
                                    } else {
                                        format!("{}.{}", cls.join("."), n)
                                    };
                                    idx.symbols.push(ParsedSymbol {
                                        stable_key: stable.clone(),
                                        disambiguator: String::new(),
                                        qualified_name: format!("{path}::{stable}"),
                                        kind: NodeKind::Function,
                                        span: span_from_tree_sitter_node(decl),
                                    });
                                    if let Some(params) = val.child_by_field_name("parameters") {
                                        visit_type_annotations(params, src, &stable, &mut idx.uses);
                                    }
                                    if let Some(ret) = val.child_by_field_name("return_type") {
                                        visit_type_annotations(ret, src, &stable, &mut idx.uses);
                                    }
                                    if let Some(body) = val.child_by_field_name("body") {
                                        visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                                    }
                                }
                            }
                        }
                        if let Some(ta) = decl.child_by_field_name("type") {
                            // Locals are not symbols — attach type uses to the file hub
                            // (or the arrow/function being declared if this declarator is one).
                            let owner = if value_node.as_ref().is_some_and(|v| {
                                matches!(
                                    v.kind(),
                                    "arrow_function"
                                        | "function_expression"
                                        | "function"
                                        | "generator_function"
                                )
                            }) {
                                name_node
                                    .map(|n| node_text(n, src))
                                    .unwrap_or("$file")
                            } else {
                                "$file"
                            };
                            visit_type_annotations(ta, src, owner, &mut idx.uses);
                        }
                    }
                }
            }
            _ => {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        visit(c, src, path, cls, idx);
                    }
                }
            }
        }
    }

    visit(root, content, path, &mut Vec::new(), &mut idx);
    visit_module_level_calls(root, content, &mut idx.calls, &mut idx.uses);
    idx.imports = extract_imports(root, content);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

// ───────────────────── public entry point ─────────────────────

pub fn index_typescript_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    #[cfg(feature = "ts-typescript")]
    {
        let tsx = path.ends_with(".tsx");
        match index_typescript_file_tree_sitter(path, content, tsx) {
            Ok(i) => return Ok(i),
            Err(_) => {}
        }
    }
    index_typescript_file_regex(path, content)
}

pub struct TypeScriptIndexer;

impl LanguageIndexer for TypeScriptIndexer {
    fn language(&self) -> Language {
        Language::TypeScript
    }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_typescript_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_typescript_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str {
        "ts"
    }
}

pub struct TypeScriptTsxIndexer;

impl LanguageIndexer for TypeScriptTsxIndexer {
    fn language(&self) -> Language {
        Language::TypeScript
    }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_typescript_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_typescript_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str {
        "tsx"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_ts_function_and_call() {
        let src = "import { foo } from './util';\nexport function main() {\n  foo();\n}\n";
        let idx = index_typescript_file("app.ts", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "main"));
        assert!(!idx.calls.is_empty());
    }

    #[cfg(feature = "ts-typescript")]
    #[test]
    fn ts_tree_sitter_indexes_class_and_interface() {
        let src = r#"
interface Drawable {
    draw(): void;
}

class Board implements Drawable {
    private size: number;

    constructor(size: number) {
        this.size = size;
    }

    draw(): void {
        console.log('drawing');
    }

    getSize(): number {
        return this.size;
    }
}
"#;
        let idx = index_typescript_file("board.ts", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Drawable" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.constructor"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.draw"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.getSize"));
    }

    #[cfg(feature = "ts-typescript")]
    #[test]
    fn ts_tree_sitter_indexes_enum_and_type_alias() {
        let src = r#"
enum Color { Red, Green, Blue }
type Point = { x: number; y: number; };
"#;
        let idx = index_typescript_file("types.ts", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Color"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Point"));
    }

    #[cfg(feature = "ts-typescript")]
    #[test]
    fn ts_tree_sitter_indexes_arrow_function() {
        let src = "const add = (a: number, b: number): number => a + b;\n";
        let idx = index_typescript_file("util.ts", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "add" && s.kind == NodeKind::Function));
    }

    #[cfg(feature = "ts-typescript")]
    #[test]
    fn ts_tree_sitter_indexes_extends_implements_and_instance_fields() {
        let src = r#"
class Dog extends Animal implements Runnable {
    constructor() {
        this.cells = new ArrayList();
        this.board = Board();
    }

    tick(): void {
        this.cells.get(0);
    }
}

@abstractmethod
abstract class Foo extends Bar {
    method(): void {}
}
"#;
        let idx = index_typescript_file("dog.ts", src).unwrap();
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Animal"));
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Runnable"));
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Foo" && e.base_name == "Bar"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Foo" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Foo.method"));
        let fields = idx.instance_fields.get("Dog").expect("Dog instance_fields");
        assert_eq!(fields.get("cells").map(String::as_str), Some("ArrayList"));
        assert_eq!(fields.get("board").map(String::as_str), Some("Board"));
    }
}
