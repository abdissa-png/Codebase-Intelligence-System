//! Go tree-sitter indexer — extracts functions, methods, types (struct/interface),
//! import declarations, call sites, interface embedding, and type annotations.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_go_module_key(rel_path: &str) -> String {
    rel_path.trim_end_matches(".go").replace('/', ".")
}

pub fn index_go_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_go::LANGUAGE.into())
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

    visit(root, content, path, &mut idx);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

fn node_text<'a>(node: Node, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn parse_call_receiver(node: Node, src: &str) -> Option<CallReceiver> {
    match node.kind() {
        "identifier" => Some(CallReceiver::Bare(node_text(node, src).to_string())),
        "selector_expression" => {
            let operand = node.child_by_field_name("operand")?;
            let field = node.child_by_field_name("field")?;
            let obj = parse_call_receiver(operand, src)?;
            Some(CallReceiver::Attr {
                object: Box::new(obj),
                name: node_text(field, src).to_string(),
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
        "qualified_type" => {
            node.child_by_field_name("name")
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "pointer_type" | "slice_type" | "array_type" | "channel_type" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    let r = simple_type_name(c, src);
                    if r.is_some() { return r; }
                }
            }
            None
        }
        _ => None,
    }
}

fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
    if let Some(name) = simple_type_name(node, src) {
        let builtin = matches!(
            name.as_str(),
            "bool" | "byte" | "rune" | "int" | "int8" | "int16" | "int32" | "int64"
                | "uint" | "uint8" | "uint16" | "uint32" | "uint64" | "uintptr"
                | "float32" | "float64" | "complex64" | "complex128"
                | "string" | "error" | "any"
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

fn visit_calls(node: Node, src: &str, caller: &str, calls: &mut Vec<ParsedCall>, uses: &mut Vec<ParsedUse>) {
    if node.kind() == "call_expression" {
        if let Some(func) = node.child_by_field_name("function") {
            if let Some(callee) = parse_call_receiver(func, src) {
                let leaf = callee.leaf_name();
                if !matches!(leaf, "println" | "printf" | "print" | "fmt"
                    | "Println" | "Printf" | "Print" | "Sprintf" | "Fprintf"
                    | "make" | "new" | "len" | "cap" | "append" | "copy"
                    | "delete" | "close" | "panic" | "recover")
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
        if child.kind() == "import_declaration" {
            let ic = child.named_child_count();
            for j in 0..ic {
                let Some(spec) = child.named_child(j) else { continue };
                match spec.kind() {
                    "import_spec" => {
                        extract_single_import(spec, src, &mut imports);
                    }
                    "import_spec_list" => {
                        let sc = spec.named_child_count();
                        for k in 0..sc {
                            if let Some(s) = spec.named_child(k) {
                                if s.kind() == "import_spec" {
                                    extract_single_import(s, src, &mut imports);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    imports
}

fn extract_single_import(spec: Node, src: &str, imports: &mut Vec<ParsedImport>) {
    let path_node = spec.child_by_field_name("path");
    let name_node = spec.child_by_field_name("name");
    let module = path_node
        .map(|n| node_text(n, src).trim_matches('"').to_string())
        .unwrap_or_default();

    let alias = name_node.map(|n| node_text(n, src).to_string());
    let (style, names) = if alias.as_deref() == Some(".") {
        (ImportStyle::Star, vec![])
    } else {
        let pkg = alias.unwrap_or_else(|| {
            module.rsplit('/').next().unwrap_or(&module).to_string()
        });
        (ImportStyle::Names, vec![pkg])
    };

    imports.push(ParsedImport {
        module,
        style,
        names,
        span: span_from_tree_sitter_node(spec),
    });
}

fn visit(node: Node, src: &str, path: &str, idx: &mut FileIndex) {
    match node.kind() {
        "source_file" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, idx);
                }
            }
            idx.imports = extract_imports(node, src);
        }
        "function_declaration" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            idx.symbols.push(ParsedSymbol {
                stable_key: name.to_string(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{name}"),
                kind: NodeKind::Function,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(params) = node.child_by_field_name("parameters") {
                visit_param_types(params, src, name, &mut idx.uses);
            }
            if let Some(result) = node.child_by_field_name("result") {
                record_type_use(&mut idx.uses, name, result, src);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, name, &mut idx.calls, &mut idx.uses);
            }
        }
        "method_declaration" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }

            let receiver_type = node.child_by_field_name("receiver")
                .and_then(|r| extract_receiver_type(r, src))
                .unwrap_or_default();

            let stable = if receiver_type.is_empty() {
                name.to_string()
            } else {
                format!("{receiver_type}.{name}")
            };
            idx.symbols.push(ParsedSymbol {
                stable_key: stable.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{stable}"),
                kind: NodeKind::Function,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(params) = node.child_by_field_name("parameters") {
                visit_param_types(params, src, &stable, &mut idx.uses);
            }
            if let Some(result) = node.child_by_field_name("result") {
                record_type_use(&mut idx.uses, &stable, result, src);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
            }
        }
        "type_declaration" => {
            let count = node.named_child_count();
            for i in 0..count {
                let Some(spec) = node.named_child(i) else { continue };
                if spec.kind() == "type_spec" {
                    let type_name = spec.child_by_field_name("name")
                        .map(|n| node_text(n, src))
                        .unwrap_or("");
                    if type_name.is_empty() { continue; }

                    let type_body = spec.child_by_field_name("type");
                    let is_interface = type_body
                        .map(|t| t.kind() == "interface_type")
                        .unwrap_or(false);

                    idx.symbols.push(ParsedSymbol {
                        stable_key: type_name.to_string(),
                        disambiguator: String::new(),
                        qualified_name: format!("{path}::{type_name}"),
                        kind: NodeKind::Class,
                        span: span_from_tree_sitter_node(spec),
                    });

                    if let Some(body) = type_body {
                        if is_interface {
                            visit_interface_embeds(body, src, type_name, idx);
                        }
                        visit_struct_field_types(body, src, type_name, &mut idx.uses);
                    }
                }
            }
        }
        _ => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, idx);
                }
            }
        }
    }
}

fn extract_receiver_type(receiver: Node, src: &str) -> Option<String> {
    let count = receiver.named_child_count();
    for i in 0..count {
        let Some(param) = receiver.named_child(i) else { continue };
        if param.kind() == "parameter_declaration" {
            if let Some(ty) = param.child_by_field_name("type") {
                return simple_type_name(ty, src);
            }
        }
    }
    None
}

fn visit_param_types(node: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    let count = node.named_child_count();
    for i in 0..count {
        let Some(param) = node.named_child(i) else { continue };
        if param.kind() == "parameter_declaration" {
            if let Some(ty) = param.child_by_field_name("type") {
                record_type_use(uses, owner, ty, src);
            }
        }
    }
}

fn visit_struct_field_types(node: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    if node.kind() == "struct_type" {
        if let Some(fields) = node.child_by_field_name("fields") {
            let count = fields.named_child_count();
            for i in 0..count {
                let Some(field) = fields.named_child(i) else { continue };
                if field.kind() == "field_declaration" {
                    if let Some(ty) = field.child_by_field_name("type") {
                        record_type_use(uses, owner, ty, src);
                    }
                }
            }
        }
    }
}

fn visit_interface_embeds(node: Node, src: &str, iface_name: &str, idx: &mut FileIndex) {
    // Go interface bodies contain `method_elem` and `type_elem` (embedded interfaces).
    let count = node.named_child_count();
    for i in 0..count {
        let Some(child) = node.named_child(i) else { continue };
        match child.kind() {
            "type_elem" | "type_identifier" | "qualified_type" => {
                record_interface_embed(child, src, iface_name, idx);
            }
            "constraint_elem" | "interface_type_name" => {
                record_interface_embed(child, src, iface_name, idx);
            }
            _ => {
                // Nested wrappers (e.g. constraint terms) — look for type identifiers.
                let inner = child.named_child_count();
                for j in 0..inner {
                    if let Some(c) = child.named_child(j) {
                        if matches!(
                            c.kind(),
                            "type_identifier" | "qualified_type" | "type_elem"
                        ) {
                            record_interface_embed(c, src, iface_name, idx);
                        }
                    }
                }
            }
        }
    }
}

fn record_interface_embed(node: Node, src: &str, iface_name: &str, idx: &mut FileIndex) {
    match node.kind() {
        "type_identifier" | "qualified_type" => {
            if let Some(name) = simple_type_name(node, src) {
                idx.extends.push(ParsedExtends {
                    class_stable_key: iface_name.to_string(),
                    base_name: name,
                    span: span_from_tree_sitter_node(node),
                });
            }
        }
        _ => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    if let Some(name) = simple_type_name(c, src) {
                        idx.extends.push(ParsedExtends {
                            class_stable_key: iface_name.to_string(),
                            base_name: name,
                            span: span_from_tree_sitter_node(c),
                        });
                    } else {
                        record_interface_embed(c, src, iface_name, idx);
                    }
                }
            }
        }
    }
}

pub struct GoIndexer;

impl LanguageIndexer for GoIndexer {
    fn language(&self) -> Language { Language::Go }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_go_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_go_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "go" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_function_and_type() {
        let src = r#"
package main

type Board struct {
    cells []Cell
}

func NewBoard() *Board {
    return &Board{}
}

func (b *Board) GetCell(idx int) Cell {
    return b.cells[idx]
}
"#;
        let idx = index_go_file("board.go", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "NewBoard" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.GetCell" && s.kind == NodeKind::Function));
    }

    #[test]
    fn indexes_imports() {
        let src = r#"
package main

import (
    "fmt"
    "os"
)
"#;
        let idx = index_go_file("main.go", src).unwrap();
        assert!(idx.imports.len() >= 2);
    }

    #[test]
    fn indexes_interface_embedding() {
        let src = r#"
package io

type Reader interface {
    Read(p []byte) (n int, err error)
}

type ReadCloser interface {
    Reader
    Close() error
}
"#;
        let idx = index_go_file("io.go", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Reader"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "ReadCloser"));
        assert!(idx.extends.iter().any(|e| {
            e.class_stable_key == "ReadCloser" && e.base_name == "Reader"
        }));
    }

    #[test]
    fn indexes_call_sites_and_type_uses() {
        let src = r#"
package main

func helper() {}

func main() {
    helper()
    b := NewBoard()
    b.GetCell(0)
}
"#;
        let idx = index_go_file("main.go", src).unwrap();
        assert!(idx.calls.iter().any(|c| {
            c.caller_stable_key == "main" && c.callee.leaf_name() == "helper"
        }));
    }
}
