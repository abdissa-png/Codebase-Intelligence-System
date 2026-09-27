//! Rust tree-sitter indexer — extracts functions, structs, enums, traits, impl blocks,
//! use declarations, call sites, trait implementations, and type annotations.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

/// Map `dir/foo.rs` → `dir.foo` so Rust imports align with [`crate::call_resolve`]
/// module lookup (same `.` convention as Python / TS).
pub fn path_to_rust_module_key(rel_path: &str) -> String {
    crate::call_resolve::path_to_rust_module_key(rel_path)
}

pub fn index_rust_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .map_err(|_| IndexError::ParseFailed)?;
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

    visit(root, content, path, &mut Vec::new(), &mut idx);
    idx.imports = extract_use_declarations(root, content);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

fn node_text<'a>(node: Node, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn parse_call_receiver(node: Node, src: &str) -> Option<CallReceiver> {
    match node.kind() {
        "identifier" | "field_identifier" | "type_identifier" => {
            Some(CallReceiver::Bare(node_text(node, src).to_string()))
        }
        "self" => Some(CallReceiver::Bare("self".to_string())),
        "field_expression" => {
            let value = node.child_by_field_name("value")?;
            let field = node.child_by_field_name("field")?;
            let obj = parse_call_receiver(value, src)?;
            Some(CallReceiver::Attr {
                object: Box::new(obj),
                name: node_text(field, src).to_string(),
            })
        }
        // `Type::method` / `module::Type::method` — keep the path as Attr chain so
        // call_resolve can bind associated functions (not bare leaf names).
        "scoped_identifier" => {
            let name = node.child_by_field_name("name")?;
            let name_s = node_text(name, src).to_string();
            if let Some(path) = node.child_by_field_name("path") {
                if let Some(obj) = parse_call_receiver(path, src) {
                    return Some(CallReceiver::Attr {
                        object: Box::new(obj),
                        name: name_s,
                    });
                }
            }
            Some(CallReceiver::Bare(name_s))
        }
        "call_expression" => node
            .child_by_field_name("function")
            .and_then(|f| parse_call_receiver(f, src)),
        "parenthesized_expression" => node.named_child(0).and_then(|c| parse_call_receiver(c, src)),
        _ => None,
    }
}

fn rust_wrapper_type(name: &str) -> bool {
    matches!(
        name,
        "Box"
            | "Arc"
            | "Rc"
            | "Weak"
            | "Option"
            | "Result"
            | "Mutex"
            | "RwLock"
            | "Ref"
            | "RefMut"
            | "Cell"
            | "RefCell"
            | "Pin"
            | "Cow"
            | "ManuallyDrop"
    )
}

fn first_type_argument(args: Node, src: &str) -> Option<String> {
    let count = args.named_child_count();
    for i in 0..count {
        let Some(c) = args.named_child(i) else { continue };
        if matches!(c.kind(), "lifetime" | "type_binding") {
            continue;
        }
        if let Some(n) = simple_type_name(c, src) {
            return Some(n);
        }
    }
    None
}

fn simple_type_name(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "type_identifier" | "identifier" => {
            let t = node_text(node, src).trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        }
        "scoped_type_identifier" | "scoped_identifier" => {
            node.child_by_field_name("name")
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "generic_type" => {
            let outer = node
                .child_by_field_name("type")
                .and_then(|n| simple_type_name(n, src));
            if let Some(ref o) = outer {
                if rust_wrapper_type(o) {
                    if let Some(args) = node.child_by_field_name("type_arguments") {
                        if let Some(inner) = first_type_argument(args, src) {
                            return Some(inner);
                        }
                    }
                }
            }
            outer
        }
        "reference_type" | "pointer_type" => {
            node.child_by_field_name("type")
                .and_then(|n| simple_type_name(n, src))
        }
        "dynamic_type" | "abstract_type" => node
            .child_by_field_name("trait")
            .and_then(|n| simple_type_name(n, src)),
        _ => None,
    }
}

fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
    if let Some(name) = simple_type_name(node, src) {
        let builtin = matches!(
            name.as_str(),
            "bool" | "char" | "u8" | "u16" | "u32" | "u64" | "u128" | "usize"
                | "i8" | "i16" | "i32" | "i64" | "i128" | "isize"
                | "f32" | "f64" | "str" | "String" | "Self" | "self"
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
        "type_identifier" | "scoped_type_identifier" | "generic_type" => {
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

fn bind_rust_locals(node: Node, src: &str, fn_key: &str, class_key: Option<&str>, idx: &mut FileIndex) {
    if node.kind() == "let_declaration" {
        if let Some(ident) = node
            .child_by_field_name("pattern")
            .and_then(|pat| ident_from_pattern(pat, src))
        {
            let ty = node
                .child_by_field_name("type")
                .and_then(|t| simple_type_name(t, src))
                .or_else(|| {
                    node.child_by_field_name("value")
                        .and_then(|v| infer_rust_ctor_type(v, src))
                        .filter(|t| t != "self" && t != "Self" && t != "this")
                })
                .or_else(|| {
                    node.child_by_field_name("value")
                        .and_then(|v| type_from_self_field_chain(v, src, class_key, idx))
                });
            if let Some(ty) = ty {
                idx.function_locals
                    .entry(fn_key.to_string())
                    .or_default()
                    .insert(ident, ty);
            }
        }
    }
    let count = node.named_child_count();
    for i in 0..count {
        if let Some(c) = node.named_child(i) {
            bind_rust_locals(c, src, fn_key, class_key, idx);
        }
    }
}

fn type_from_self_field_chain(
    node: Node,
    src: &str,
    class_key: Option<&str>,
    idx: &FileIndex,
) -> Option<String> {
    let class_key = class_key?;
    let recv = parse_call_receiver(node, src)?;
    field_type_from_self_chain(&recv, class_key, idx)
}

fn field_type_from_self_chain(
    recv: &CallReceiver,
    class_key: &str,
    idx: &FileIndex,
) -> Option<String> {
    match recv {
        CallReceiver::Attr { object, name } => match object.as_ref() {
            CallReceiver::Bare(n) if n == "self" || n == "this" => idx
                .instance_fields
                .get(class_key)
                .and_then(|fields| fields.get(name))
                .cloned(),
            inner @ CallReceiver::Attr { .. } => {
                field_type_from_self_chain(inner, class_key, idx)
            }
            _ => None,
        },
        _ => None,
    }
}

fn infer_rust_ctor_type(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "call_expression" => {
            let recv = node
                .child_by_field_name("function")
                .and_then(|f| parse_call_receiver(f, src))?;
            let root = recv.root_bare_name()?;
            if rust_wrapper_type(root) {
                if let Some(args) = node.child_by_field_name("arguments") {
                    let n = args.named_child_count();
                    for i in 0..n {
                        if let Some(c) = args.named_child(i) {
                            if let Some(inner) = infer_rust_ctor_type(c, src) {
                                return Some(inner);
                            }
                        }
                    }
                }
            }
            if root.chars().next().is_some_and(|c| c.is_uppercase()) {
                Some(root.to_string())
            } else {
                None
            }
        }
        "struct_expression" => node
            .child_by_field_name("name")
            .and_then(|n| parse_call_receiver(n, src))
            .and_then(|r| r.root_bare_name().map(|s| s.to_string()))
            .filter(|s| s.chars().next().is_some_and(|c| c.is_uppercase())),
        "reference_expression" | "unary_expression" | "try_expression" => {
            node.named_child(0).and_then(|c| infer_rust_ctor_type(c, src))
        }
        _ => None,
    }
}

fn visit_calls(node: Node, src: &str, caller: &str, calls: &mut Vec<ParsedCall>, uses: &mut Vec<ParsedUse>) {
    match node.kind() {
        "call_expression" => {
            if let Some(func) = node.child_by_field_name("function") {
                if let Some(callee) = parse_call_receiver(func, src) {
                    let leaf = callee.leaf_name();
                    if leaf != "println" && leaf != "eprintln" && leaf != "format"
                        && leaf != "panic" && leaf != "todo" && leaf != "unimplemented"
                        && leaf != "dbg" && leaf != "assert" && leaf != "assert_eq"
                        && leaf != "assert_ne" && leaf != "vec"
                    {
                        calls.push(ParsedCall {
                            caller_stable_key: caller.to_string(),
                            callee,
                            span: span_from_tree_sitter_node(node),
                        });
                    }
                }
            }
        }
        "macro_invocation" => {
            if let Some(mac) = node.child_by_field_name("macro") {
                let name = node_text(mac, src);
                if !matches!(name, "println" | "eprintln" | "format" | "panic" | "todo"
                    | "unimplemented" | "dbg" | "assert" | "assert_eq" | "assert_ne"
                    | "vec" | "cfg" | "derive" | "include" | "env" | "concat"
                    | "stringify" | "write" | "writeln" | "log" | "info" | "warn"
                    | "error" | "debug" | "trace")
                {
                    calls.push(ParsedCall {
                        caller_stable_key: caller.to_string(),
                        callee: CallReceiver::Bare(format!("{name}!")),
                        span: span_from_tree_sitter_node(node),
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

fn extract_use_declarations(root: Node, src: &str) -> Vec<ParsedImport> {
    let mut imports = Vec::new();
    walk_use_declarations(root, src, &mut imports);
    imports
}

fn walk_use_declarations(node: Node, src: &str, imports: &mut Vec<ParsedImport>) {
    if node.kind() == "use_declaration" {
        extract_use_tree(node, src, imports);
        return;
    }
    let count = node.named_child_count();
    for i in 0..count {
        if let Some(c) = node.named_child(i) {
            walk_use_declarations(c, src, imports);
        }
    }
}

fn extract_use_tree(node: Node, src: &str, imports: &mut Vec<ParsedImport>) {
    let text = node_text(node, src).to_string();

    let module = if let Some(arg) = node.child_by_field_name("argument") {
        extract_use_path_module(arg, src)
    } else {
        text.trim_start_matches("use ").trim_end_matches(';').to_string()
    };

    let has_star = text.contains("::*");
    let (style, names) = if has_star {
        (ImportStyle::Star, vec![])
    } else {
        let mut names = Vec::new();
        collect_use_names(node, src, &mut names);
        if names.is_empty() {
            if let Some(last) = module.rsplit("::").next() {
                names.push((last.to_string(), last.to_string()));
            }
        }
        (ImportStyle::Names, names)
    };

    imports.push(ParsedImport {
        module: module.replace("::", "."),
        style,
        names,
        span: span_from_tree_sitter_node(node),
    });
}

fn extract_use_path_module(node: Node, src: &str) -> String {
    match node.kind() {
        "use_as_clause" | "scoped_identifier" | "identifier" | "scoped_use_list" => {
            let text = node_text(node, src);
            text.split('{').next().unwrap_or(text).trim_end_matches("::").to_string()
        }
        "use_wildcard" => {
            let text = node_text(node, src);
            text.trim_end_matches("::*").to_string()
        }
        "use_list" => {
            if let Some(parent) = node.parent() {
                let text = node_text(parent, src);
                text.split('{').next().unwrap_or("").trim_end_matches("::").to_string()
            } else {
                String::new()
            }
        }
        _ => node_text(node, src).to_string(),
    }
}

fn collect_use_names(node: Node, src: &str, names: &mut Vec<(String, String)>) {
    match node.kind() {
        "use_list" | "scoped_use_list" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    collect_use_names(c, src, names);
                }
            }
        }
        "use_as_clause" => {
            let remote = node
                .child_by_field_name("path")
                .and_then(|path| path.child_by_field_name("name"))
                .map(|n| node_text(n, src).to_string())
                .filter(|s| !s.is_empty());
            let local = node
                .child_by_field_name("alias")
                .map(|n| node_text(n, src).to_string())
                .filter(|s| !s.is_empty());
            match (remote, local) {
                (Some(remote), Some(local)) => names.push((remote, local)),
                (Some(remote), None) => names.push((remote.clone(), remote)),
                (None, Some(local)) => names.push((local.clone(), local)),
                (None, None) => {}
            }
        }
        "identifier" => {
            let n = node_text(node, src).to_string();
            names.push((n.clone(), n));
        }
        "scoped_identifier" => {
            if let Some(name) = node.child_by_field_name("name") {
                let n = node_text(name, src).to_string();
                names.push((n.clone(), n));
            }
        }
        _ => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    collect_use_names(c, src, names);
                }
            }
        }
    }
}

fn ident_from_pattern(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "identifier" => {
            let t = node_text(node, src);
            if t.is_empty() || t == "_" || t == "self" {
                None
            } else {
                Some(t.to_string())
            }
        }
        "mut_pattern" | "ref_pattern" | "reference_pattern" | "pointer_pattern"
        | "captured_pattern" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    if let Some(id) = ident_from_pattern(c, src) {
                        return Some(id);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn bind_rust_params(params: Node, src: &str, fn_key: &str, idx: &mut FileIndex) {
    let count = params.named_child_count();
    for i in 0..count {
        let Some(p) = params.named_child(i) else { continue };
        if p.kind() != "parameter" {
            continue;
        }
        let Some(ty) = p
            .child_by_field_name("type")
            .and_then(|t| simple_type_name(t, src))
        else {
            continue;
        };
        let Some(pat) = p.child_by_field_name("pattern") else {
            continue;
        };
        let Some(ident) = ident_from_pattern(pat, src) else {
            continue;
        };
        idx.function_locals
            .entry(fn_key.to_string())
            .or_default()
            .insert(ident, ty);
    }
}

fn record_struct_fields(body: Node, src: &str, type_name: &str, idx: &mut FileIndex) {
    let count = body.named_child_count();
    for i in 0..count {
        let Some(f) = body.named_child(i) else { continue };
        if f.kind() != "field_declaration" {
            continue;
        }
        let Some(name) = f.child_by_field_name("name") else { continue };
        let Some(ty) = f
            .child_by_field_name("type")
            .and_then(|t| simple_type_name(t, src))
        else {
            continue;
        };
        let field = node_text(name, src);
        if field.is_empty() {
            continue;
        }
        idx.instance_fields
            .entry(type_name.to_string())
            .or_default()
            .insert(field.to_string(), ty);
    }
}

fn visit_rust_instance_fields(node: Node, src: &str, class_key: &str, idx: &mut FileIndex) {
    if node.kind() == "assignment_expression" {
        if let (Some(left), Some(right)) = (
            node.child_by_field_name("left"),
            node.child_by_field_name("right"),
        ) {
            if left.kind() == "field_expression" {
                if let Some(val) = left.child_by_field_name("value") {
                    let val_txt = node_text(val, src);
                    if val.kind() == "self" || val_txt == "self" {
                        if let Some(field) = left.child_by_field_name("field") {
                            if let Some(ty) = infer_rust_ctor_type(right, src) {
                                if ty != "self" && ty != "Self" {
                                    idx.instance_fields
                                        .entry(class_key.to_string())
                                        .or_default()
                                        .insert(node_text(field, src).to_string(), ty);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let count = node.named_child_count();
    for i in 0..count {
        if let Some(c) = node.named_child(i) {
            visit_rust_instance_fields(c, src, class_key, idx);
        }
    }
}

fn visit(node: Node, src: &str, path: &str, scope: &mut Vec<String>, idx: &mut FileIndex) {
    match node.kind() {
        "source_file" | "block" | "declaration_list" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, scope, idx);
                }
            }
        }
        "attribute_item" | "inner_attribute_item" => {}
        "function_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            let stable = if scope.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", scope.join("."), name)
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
                bind_rust_params(params, src, &stable, idx);
            }
            if let Some(ret) = node.child_by_field_name("return_type") {
                record_type_use(&mut idx.uses, &stable, ret, src);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                let class_key = if scope.is_empty() {
                    None
                } else {
                    Some(scope.join("."))
                };
                bind_rust_locals(body, src, &stable, class_key.as_deref(), idx);
                if let Some(ref cls) = class_key {
                    visit_rust_instance_fields(body, src, cls, idx);
                }
            }
        }
        "struct_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            let stable = if scope.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", scope.join("."), name)
            };
            idx.symbols.push(ParsedSymbol {
                stable_key: stable.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{stable}"),
                kind: NodeKind::Class,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(body) = node.child_by_field_name("body") {
                visit_type_annotations(body, src, &stable, &mut idx.uses);
                record_struct_fields(body, src, &stable, idx);
            }
        }
        "enum_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            let stable = if scope.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", scope.join("."), name)
            };
            idx.symbols.push(ParsedSymbol {
                stable_key: stable.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{stable}"),
                kind: NodeKind::Class,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(body) = node.child_by_field_name("body") {
                visit_type_annotations(body, src, &stable, &mut idx.uses);
            }
        }
        "trait_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            let stable = if scope.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", scope.join("."), name)
            };
            idx.symbols.push(ParsedSymbol {
                stable_key: stable.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{stable}"),
                kind: NodeKind::Class,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(bounds) = node.child_by_field_name("bounds") {
                let bc = bounds.named_child_count();
                for bi in 0..bc {
                    if let Some(b) = bounds.named_child(bi) {
                        record_type_use(&mut idx.uses, &stable, b, src);
                    }
                }
            }
            scope.push(name.to_string());
            if let Some(body) = node.child_by_field_name("body") {
                let count = body.named_child_count();
                for i in 0..count {
                    if let Some(c) = body.named_child(i) {
                        visit(c, src, path, scope, idx);
                    }
                }
            }
            scope.pop();
        }
        "impl_item" => {
            let type_name = node
                .child_by_field_name("type")
                .and_then(|n| simple_type_name(n, src))
                .unwrap_or_else(|| {
                    let impl_type = node
                        .child_by_field_name("type")
                        .map(|n| node_text(n, src).to_string())
                        .unwrap_or_default();
                    impl_type
                        .split('<')
                        .next()
                        .unwrap_or(&impl_type)
                        .rsplit("::")
                        .next()
                        .unwrap_or(&impl_type)
                        .trim()
                        .to_string()
                });
            if type_name.is_empty() { return; }
            let class_key = if scope.is_empty() {
                type_name.clone()
            } else {
                format!("{}.{}", scope.join("."), type_name)
            };

            if let Some(trait_node) = node.child_by_field_name("trait") {
                if let Some(trait_name) = simple_type_name(trait_node, src) {
                    idx.extends.push(ParsedExtends {
                        class_stable_key: class_key.clone(),
                        base_name: trait_name,
                        span: span_from_tree_sitter_node(trait_node),
                    });
                }
            }

            scope.push(type_name);
            if let Some(body) = node.child_by_field_name("body") {
                let count = body.named_child_count();
                for i in 0..count {
                    if let Some(c) = body.named_child(i) {
                        visit(c, src, path, scope, idx);
                    }
                }
            }
            scope.pop();
        }
        "mod_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            if let Some(body) = node.child_by_field_name("body") {
                scope.push(name.to_string());
                let count = body.named_child_count();
                for i in 0..count {
                    if let Some(c) = body.named_child(i) {
                        visit(c, src, path, scope, idx);
                    }
                }
                scope.pop();
            }
        }
        "const_item" | "static_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if !name.is_empty() {
                let stable = if scope.is_empty() {
                    name.to_string()
                } else {
                    format!("{}.{}", scope.join("."), name)
                };
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: format!("{path}::{stable}"),
                    kind: NodeKind::Function,
                    span: span_from_tree_sitter_node(node),
                });
                if let Some(ty) = node.child_by_field_name("type") {
                    record_type_use(&mut idx.uses, &stable, ty, src);
                }
            }
        }
        "type_item" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if !name.is_empty() {
                let stable = if scope.is_empty() {
                    name.to_string()
                } else {
                    format!("{}.{}", scope.join("."), name)
                };
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: format!("{path}::{stable}"),
                    kind: NodeKind::Class,
                    span: span_from_tree_sitter_node(node),
                });
                if let Some(val) = node.child_by_field_name("type") {
                    record_type_use(&mut idx.uses, &stable, val, src);
                }
            }
        }
        _ => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, scope, idx);
                }
            }
        }
    }
}

pub struct RustIndexer;

impl LanguageIndexer for RustIndexer {
    fn language(&self) -> Language { Language::Rust }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_rust_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_rust_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "rs" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_struct_and_impl_method() {
        let src = r#"
struct Board {
    cells: Vec<Cell>,
}

impl Board {
    fn new() -> Self {
        Board { cells: vec![] }
    }

    fn get_cell(&self, idx: usize) -> &Cell {
        &self.cells[idx]
    }
}
"#;
        let idx = index_rust_file("board.rs", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.new" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.get_cell" && s.kind == NodeKind::Function));
    }

    #[test]
    fn indexes_trait_impl_extends() {
        let src = r#"
trait Display {
    fn fmt(&self) -> String;
}

struct Foo;

impl Display for Foo {
    fn fmt(&self) -> String { String::new() }
}
"#;
        let idx = index_rust_file("foo.rs", src).unwrap();
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Foo" && e.base_name == "Display"));
    }

    #[test]
    fn indexes_use_declarations() {
        let src = "use std::collections::HashMap;\nuse crate::graph::{NodeKind, Language};\n";
        let idx = index_rust_file("lib.rs", src).unwrap();
        assert!(!idx.imports.is_empty());
        let crate_graph = idx
            .imports
            .iter()
            .find(|i| i.module == "crate.graph")
            .expect("crate::graph import");
        assert!(crate_graph.names.iter().any(|(n, _)| n == "NodeKind"));
        assert!(crate_graph.names.iter().any(|(n, _)| n == "Language"));
    }

    #[test]
    fn module_key_uses_dot_separators() {
        assert_eq!(
            path_to_rust_module_key("cis-core/src/coordinator.rs"),
            "cis-core.src.coordinator"
        );
        assert_eq!(path_to_rust_module_key("cis-core/src/lib.rs"), "cis-core.src");
        assert_eq!(path_to_rust_module_key("cis-core/src/foo/mod.rs"), "cis-core.src.foo");
    }

    #[test]
    fn indexes_enum_and_type_alias() {
        let src = r#"
enum Color { Red, Green, Blue }
type Result<T> = std::result::Result<T, Error>;
"#;
        let idx = index_rust_file("types.rs", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Color" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Result" && s.kind == NodeKind::Class));
    }

    #[test]
    fn indexes_call_sites() {
        let src = r#"
fn main() {
    let board = Board::new();
    board.get_cell(0);
    helper();
}
fn helper() {}
"#;
        let idx = index_rust_file("main.rs", src).unwrap();
        assert!(idx.calls.iter().any(|c| c.caller_stable_key == "main"));
        let associated = idx
            .calls
            .iter()
            .find(|c| c.callee.label() == "Board.new")
            .expect("Board::new should be Attr(Board, new)");
        assert!(matches!(
            &associated.callee,
            CallReceiver::Attr { name, .. } if name == "new"
        ));
        assert!(idx.calls.iter().any(|c| c.callee.label() == "board.get_cell"));
        assert!(idx.calls.iter().any(|c| c.callee.label() == "helper"));
        assert_eq!(
            idx.function_locals.get("main").and_then(|m| m.get("board")).map(String::as_str),
            Some("Board")
        );
    }

    #[test]
    fn indexes_chained_calls() {
        let src = r#"
fn run(g: Graph) {
    g.lock().unwrap();
}
"#;
        let idx = index_rust_file("main.rs", src).unwrap();
        assert!(
            idx.calls.iter().any(|c| c.callee.label() == "g.lock"),
            "inner lock call"
        );
        assert!(
            idx.calls.iter().any(|c| c.callee.label() == "g.lock.unwrap"),
            "chained unwrap should keep the receiver chain, got {:?}",
            idx.calls.iter().map(|c| c.callee.label()).collect::<Vec<_>>()
        );
        assert_eq!(
            idx.function_locals.get("run").and_then(|m| m.get("g")).map(String::as_str),
            Some("Graph")
        );
    }

    #[test]
    fn binds_typed_params_struct_fields_and_self_assigns() {
        let src = r#"
pub struct Store {
    graph: InMemoryGraph,
}
impl Store {
    pub fn save(&self, graph: &InMemoryGraph) {
        graph.to_snapshot();
    }
    pub fn set(&mut self) {
        self.graph = InMemoryGraph::new();
    }
}
fn save_graph_snapshot(graph: &InMemoryGraph) {
    graph.to_snapshot();
}
fn wrap(g: Arc<InMemoryGraph>, board: Option<&Board>) {}
"#;
        let idx = index_rust_file("persist.rs", src).unwrap();
        assert_eq!(
            idx.function_locals
                .get("save_graph_snapshot")
                .and_then(|m| m.get("graph"))
                .map(String::as_str),
            Some("InMemoryGraph"),
            "typed parameters must populate function_locals"
        );
        assert_eq!(
            idx.function_locals
                .get("Store.save")
                .and_then(|m| m.get("graph"))
                .map(String::as_str),
            Some("InMemoryGraph")
        );
        assert_eq!(
            idx.instance_fields
                .get("Store")
                .and_then(|m| m.get("graph"))
                .map(String::as_str),
            Some("InMemoryGraph"),
            "struct fields and/or self.field = Type::new()"
        );
        assert_eq!(
            idx.function_locals
                .get("wrap")
                .and_then(|m| m.get("g"))
                .map(String::as_str),
            Some("InMemoryGraph"),
            "Arc<T> should unwrap to T"
        );
        assert_eq!(
            idx.function_locals
                .get("wrap")
                .and_then(|m| m.get("board"))
                .map(String::as_str),
            Some("Board"),
            "Option<&T> should unwrap to T"
        );
    }

    #[test]
    fn binds_self_field_after_lock_unwrap() {
        let src = r#"
pub struct Backend {
    ann: Mutex<FlatAnnIndex>,
}
impl Backend {
    fn rebuild(&self) {
        let mut ann = self.ann.lock().unwrap();
        ann.upsert();
    }
}
"#;
        let idx = index_rust_file("vec.rs", src).unwrap();
        assert_eq!(
            idx.instance_fields
                .get("Backend")
                .and_then(|m| m.get("ann"))
                .map(String::as_str),
            Some("FlatAnnIndex")
        );
        assert_eq!(
            idx.function_locals
                .get("Backend.rebuild")
                .and_then(|m| m.get("ann"))
                .map(String::as_str),
            Some("FlatAnnIndex"),
            "let x = self.field.lock().unwrap() should keep the field type"
        );
    }

    #[test]
    fn unwraps_arc_new_constructor_arg() {
        let src = r#"
fn t() {
    let leases = Arc::new(PathLeaseManager::new());
    leases.acquire();
}
"#;
        let idx = index_rust_file("p.rs", src).unwrap();
        assert_eq!(
            idx.function_locals
                .get("t")
                .and_then(|m| m.get("leases"))
                .map(String::as_str),
            Some("PathLeaseManager")
        );
    }

    #[test]
    fn indexes_type_uses_and_disambiguates_collisions() {
        let src = r#"
struct Foo {}
fn Foo() {}
fn helper(x: Board) -> Cell { Cell }
"#;
        let idx = index_rust_file("mix.rs", src).unwrap();
        let foos: Vec<_> = idx.symbols.iter().filter(|s| s.stable_key == "Foo").collect();
        assert_eq!(foos.len(), 2);
        assert!(foos.iter().any(|s| s.kind == NodeKind::Class && s.disambiguator == "class"));
        assert!(foos.iter().any(|s| s.kind == NodeKind::Function && s.disambiguator.is_empty()));
        assert!(idx.uses.iter().any(|u| u.owner_stable_key == "helper" && u.type_name == "Board"));
        assert!(idx.uses.iter().any(|u| u.owner_stable_key == "helper" && u.type_name == "Cell"));
    }
}
