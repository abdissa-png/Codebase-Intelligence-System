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
    let base = rel_path.trim_end_matches(".rs").replace('/', ".");
    if base.ends_with(".mod") {
        base.trim_end_matches(".mod").to_string()
    } else if base.ends_with(".lib") || base.ends_with(".main") {
        base.rsplit_once('.').map(|(p, _)| p.to_string()).unwrap_or(base)
    } else {
        base
    }
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
        _ => None,
    }
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
            node.child_by_field_name("type")
                .and_then(|n| simple_type_name(n, src))
        }
        "reference_type" => {
            node.child_by_field_name("type")
                .and_then(|n| simple_type_name(n, src))
        }
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
    let count = root.named_child_count();
    for i in 0..count {
        let Some(child) = root.named_child(i) else { continue };
        if child.kind() == "use_declaration" {
            extract_use_tree(child, src, &mut imports);
        }
    }
    imports
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
                names.push(last.to_string());
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

fn collect_use_names(node: Node, src: &str, names: &mut Vec<String>) {
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
            if let Some(alias) = node.child_by_field_name("alias") {
                names.push(node_text(alias, src).to_string());
            } else if let Some(path) = node.child_by_field_name("path") {
                if let Some(name) = path.child_by_field_name("name") {
                    names.push(node_text(name, src).to_string());
                }
            }
        }
        "identifier" => {
            names.push(node_text(node, src).to_string());
        }
        "scoped_identifier" => {
            if let Some(name) = node.child_by_field_name("name") {
                names.push(node_text(name, src).to_string());
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
            }
            if let Some(ret) = node.child_by_field_name("return_type") {
                record_type_use(&mut idx.uses, &stable, ret, src);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
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
            let impl_type = node.child_by_field_name("type")
                .map(|n| node_text(n, src).to_string())
                .unwrap_or_default();
            let type_name = impl_type.split('<').next().unwrap_or(&impl_type).trim().to_string();
            if type_name.is_empty() { return; }

            if let Some(trait_node) = node.child_by_field_name("trait") {
                if let Some(trait_name) = simple_type_name(trait_node, src) {
                    idx.extends.push(ParsedExtends {
                        class_stable_key: type_name.clone(),
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
        assert!(crate_graph.names.iter().any(|n| n == "NodeKind"));
        assert!(crate_graph.names.iter().any(|n| n == "Language"));
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
