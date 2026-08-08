//! JavaScript/JSX tree-sitter indexer — extracts functions, classes, arrow functions,
//! ES6 imports, call sites, class inheritance, and JSX component usage.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_js_module_key(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".js")
        .trim_end_matches(".jsx")
        .replace('/', ".")
}

pub fn index_javascript_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_javascript::LANGUAGE.into())
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
    visit_module_level_calls(root, content, &mut idx.calls, &mut idx.uses);
    idx.imports = extract_imports(root, content);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

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
                        | "setTimeout" | "setInterval" | "clearTimeout" | "clearInterval"
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
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
            }
        }
        "class_declaration" => {
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
            // extends
            let nc = node.named_child_count();
            for i in 0..nc {
                let Some(child) = node.named_child(i) else { continue };
                if child.kind() == "class_heritage" {
                    let hc = child.named_child_count();
                    for j in 0..hc {
                        if let Some(h) = child.named_child(j) {
                            if h.kind() == "identifier" || h.kind() == "member_expression" {
                                let base = node_text(h, src).to_string();
                                if !base.is_empty() && base != "object" {
                                    idx.extends.push(ParsedExtends {
                                        class_stable_key: stable.clone(),
                                        base_name: base,
                                        span: span_from_tree_sitter_node(h),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_class_body(body, src, path, cls, idx);
            }
            cls.pop();
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
                                | "generator_function"
                            );
                            let is_class = val.kind() == "class";
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
                                if let Some(body) = val.child_by_field_name("body") {
                                    visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                                }
                            } else if is_class {
                                let n = node_text(name_n, src);
                                cls.push(n.to_string());
                                let stable = cls.join(".");
                                idx.symbols.push(ParsedSymbol {
                                    stable_key: stable.clone(),
                                    disambiguator: String::new(),
                                    qualified_name: format!("{path}::{stable}"),
                                    kind: NodeKind::Class,
                                    span: span_from_tree_sitter_node(decl),
                                });
                                if let Some(body) = val.child_by_field_name("body") {
                                    visit_class_body(body, src, path, cls, idx);
                                }
                                cls.pop();
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn visit_class_body(
    body: Node, src: &str, path: &str,
    cls: &mut Vec<String>, idx: &mut FileIndex,
) {
    let count = body.named_child_count();
    for i in 0..count {
        let Some(member) = body.named_child(i) else { continue };
        match member.kind() {
            "method_definition" => {
                let name = member.child_by_field_name("name")
                    .map(|n| node_text(n, src))
                    .unwrap_or("");
                if name.is_empty() { continue; }
                let stable = format!("{}.{}", cls.join("."), name);
                idx.symbols.push(ParsedSymbol {
                    stable_key: stable.clone(),
                    disambiguator: String::new(),
                    qualified_name: format!("{path}::{stable}"),
                    kind: NodeKind::Function,
                    span: span_from_tree_sitter_node(member),
                });
                if let Some(body) = member.child_by_field_name("body") {
                    visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                    let class_key = cls.join(".");
                    if !class_key.is_empty() {
                        visit_instance_fields(body, src, &class_key, idx);
                    }
                }
            }
            "field_definition" => {
                let name_node = member.child_by_field_name("property");
                let value_node = member.child_by_field_name("value");
                if let (Some(nn), Some(val)) = (name_node, value_node) {
                    if matches!(val.kind(), "arrow_function" | "function_expression") {
                        let n = node_text(nn, src);
                        let stable = format!("{}.{}", cls.join("."), n);
                        idx.symbols.push(ParsedSymbol {
                            stable_key: stable.clone(),
                            disambiguator: String::new(),
                            qualified_name: format!("{path}::{stable}"),
                            kind: NodeKind::Function,
                            span: span_from_tree_sitter_node(member),
                        });
                        if let Some(body) = val.child_by_field_name("body") {
                            visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                            let class_key = cls.join(".");
                            if !class_key.is_empty() {
                                visit_instance_fields(body, src, &class_key, idx);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

pub struct JavaScriptIndexer;

impl LanguageIndexer for JavaScriptIndexer {
    fn language(&self) -> Language { Language::JavaScript }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_javascript_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_js_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "js" }
}

pub struct JsxIndexer;

impl LanguageIndexer for JsxIndexer {
    fn language(&self) -> Language { Language::JavaScript }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_javascript_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_js_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "jsx" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_function_and_class() {
        let src = r#"
import { helper } from './util';

export function main() {
    helper();
}

class Board {
    constructor(size) {
        this.size = size;
    }

    getCell(idx) {
        return this.cells[idx];
    }
}
"#;
        let idx = index_javascript_file("app.js", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "main" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.constructor"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.getCell"));
    }

    #[test]
    fn indexes_arrow_function() {
        let src = "const add = (a, b) => a + b;\nconst greet = () => { return 'hello'; };\n";
        let idx = index_javascript_file("util.js", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "add"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "greet"));
    }

    #[test]
    fn indexes_imports() {
        let src = "import React from 'react';\nimport { useState, useEffect } from 'react';\n";
        let idx = index_javascript_file("app.js", src).unwrap();
        assert!(idx.imports.len() >= 2);
    }

    #[test]
    fn indexes_class_extends() {
        let src = "class Dog extends Animal {\n  bark() { return 'woof'; }\n}\n";
        let idx = index_javascript_file("dog.js", src).unwrap();
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Animal"));
    }

    #[test]
    fn indexes_instance_fields_from_this_assignment() {
        let src = r#"
class Board {
    constructor(size) {
        this.cells = new ArrayList();
        this.board = Board();
    }

    getCell(idx) {
        return this.cells.get(idx);
    }
}
"#;
        let idx = index_javascript_file("board.js", src).unwrap();
        let fields = idx.instance_fields.get("Board").expect("Board instance_fields");
        assert_eq!(fields.get("cells").map(String::as_str), Some("ArrayList"));
        assert_eq!(fields.get("board").map(String::as_str), Some("Board"));
    }
}
