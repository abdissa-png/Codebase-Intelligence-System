//! Java tree-sitter indexer — extracts classes, interfaces, enums, methods,
//! constructors, import declarations, call sites, inheritance, and type annotations.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_java_module_key(rel_path: &str) -> String {
    rel_path.trim_end_matches(".java").replace('/', ".")
}

pub fn index_java_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_java::LANGUAGE.into())
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
        "field_access" => {
            let obj = node.child_by_field_name("object")?;
            let field = node.child_by_field_name("field")?;
            let obj_r = parse_call_receiver(obj, src)?;
            Some(CallReceiver::Attr {
                object: Box::new(obj_r),
                name: node_text(field, src).to_string(),
            })
        }
        "scoped_identifier" => {
            let name = node.child_by_field_name("name")?;
            Some(CallReceiver::Bare(node_text(name, src).to_string()))
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
        "scoped_type_identifier" => {
            node.child_by_field_name("name")
                .or_else(|| {
                    let c = node.named_child_count();
                    if c > 0 { node.named_child(c - 1) } else { None }
                })
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "generic_type" => {
            node.named_child(0).and_then(|n| simple_type_name(n, src))
        }
        "array_type" => {
            node.child_by_field_name("element")
                .and_then(|n| simple_type_name(n, src))
        }
        _ => None,
    }
}

fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
    if let Some(name) = simple_type_name(node, src) {
        let builtin = matches!(
            name.as_str(),
            "void" | "int" | "long" | "short" | "byte" | "float" | "double"
                | "boolean" | "char" | "String" | "Object"
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

fn visit_calls(
    node: Node, src: &str, caller: &str,
    calls: &mut Vec<ParsedCall>, uses: &mut Vec<ParsedUse>,
) {
    match node.kind() {
        "method_invocation" => {
            let name_node = node.child_by_field_name("name");
            let obj_node = node.child_by_field_name("object");
            if let Some(name_n) = name_node {
                let name = node_text(name_n, src);
                if matches!(name, "println" | "print" | "printf" | "format") {
                    // skip common I/O
                } else {
                    let callee = if let Some(obj) = obj_node {
                        if let Some(obj_r) = parse_call_receiver(obj, src) {
                            CallReceiver::Attr {
                                object: Box::new(obj_r),
                                name: name.to_string(),
                            }
                        } else {
                            CallReceiver::Bare(name.to_string())
                        }
                    } else {
                        CallReceiver::Bare(name.to_string())
                    };
                    calls.push(ParsedCall {
                        caller_stable_key: caller.to_string(),
                        callee,
                        span: span_from_tree_sitter_node(node),
                    });
                }
            }
        }
        "object_creation_expression" => {
            if let Some(type_node) = node.child_by_field_name("type") {
                if let Some(type_name) = simple_type_name(type_node, src) {
                    calls.push(ParsedCall {
                        caller_stable_key: caller.to_string(),
                        callee: CallReceiver::Bare(type_name.clone()),
                        span: span_from_tree_sitter_node(node),
                    });
                    uses.push(ParsedUse {
                        owner_stable_key: caller.to_string(),
                        type_name,
                        span: span_from_tree_sitter_node(type_node),
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
        if child.kind() == "import_declaration" {
            let text = node_text(child, src).to_string();
            let module_text = text
                .trim_start_matches("import ")
                .trim_start_matches("static ")
                .trim_end_matches(';')
                .trim()
                .to_string();
            let (style, names, module) = if module_text.ends_with(".*") {
                let m = module_text.trim_end_matches(".*");
                (ImportStyle::Star, vec![], m.to_string())
            } else {
                let parts: Vec<&str> = module_text.rsplitn(2, '.').collect();
                let (name, module) = if parts.len() == 2 {
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    (module_text.clone(), module_text.clone())
                };
                (ImportStyle::Names, vec![name], module)
            };
            imports.push(ParsedImport {
                module,
                style,
                names,
                span: span_from_tree_sitter_node(child),
            });
        }
    }
    imports
}

fn visit_param_types(node: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    let count = node.named_child_count();
    for i in 0..count {
        let Some(param) = node.named_child(i) else { continue };
        if param.kind() == "formal_parameter" || param.kind() == "spread_parameter" {
            if let Some(ty) = param.child_by_field_name("type") {
                record_type_use(uses, owner, ty, src);
            }
        }
    }
}

fn this_field_from_target(node: Node, src: &str) -> Option<String> {
    if node.kind() != "field_access" {
        return None;
    }
    let obj = node.child_by_field_name("object")?;
    let field = node.child_by_field_name("field")?;
    if obj.kind() != "this" {
        return None;
    }
    let name = node_text(field, src).trim().to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn infer_type_from_expr(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "object_creation_expression" => {
            node.child_by_field_name("type")
                .and_then(|t| simple_type_name(t, src))
        }
        "method_invocation" => {
            let name = node.child_by_field_name("name").map(|n| node_text(n, src))?;
            if node.child_by_field_name("object").is_some() {
                return None;
            }
            let t = name.trim().to_string();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
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

fn visit(
    node: Node, src: &str, path: &str,
    cls: &mut Vec<String>, idx: &mut FileIndex,
) {
    match node.kind() {
        "program" | "block" | "class_body" | "interface_body" | "enum_body" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, cls, idx);
                }
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
            // superclass — wrapped as `superclass` → type node
            if let Some(sc) = node.child_by_field_name("superclass") {
                let type_node = sc.named_child(0).unwrap_or(sc);
                if let Some(base) = simple_type_name(type_node, src) {
                    if base != "Object" {
                        idx.extends.push(ParsedExtends {
                            class_stable_key: stable.clone(),
                            base_name: base,
                            span: span_from_tree_sitter_node(type_node),
                        });
                    }
                }
            }
            // interfaces — wrapped as `super_interfaces` → `type_list` → types
            if let Some(interfaces) = node.child_by_field_name("interfaces") {
                let type_list = interfaces.named_child(0).unwrap_or(interfaces);
                let ic = type_list.named_child_count();
                for i in 0..ic {
                    if let Some(iface) = type_list.named_child(i) {
                        if let Some(base) = simple_type_name(iface, src) {
                            idx.extends.push(ParsedExtends {
                                class_stable_key: stable.clone(),
                                base_name: base,
                                span: span_from_tree_sitter_node(iface),
                            });
                        }
                    }
                }
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit(body, src, path, cls, idx);
            }
            cls.pop();
        }
        "interface_declaration" => {
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
            // extends_interfaces is a child (not a named field) wrapping `type_list`
            let nc = node.named_child_count();
            for i in 0..nc {
                let Some(child) = node.named_child(i) else { continue };
                if child.kind() != "extends_interfaces" {
                    continue;
                }
                let type_list = child.named_child(0).unwrap_or(child);
                let ec = type_list.named_child_count();
                for j in 0..ec {
                    if let Some(base_node) = type_list.named_child(j) {
                        if let Some(base) = simple_type_name(base_node, src) {
                            idx.extends.push(ParsedExtends {
                                class_stable_key: stable.clone(),
                                base_name: base,
                                span: span_from_tree_sitter_node(base_node),
                            });
                        }
                    }
                }
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit(body, src, path, cls, idx);
            }
            cls.pop();
        }
        "enum_declaration" => {
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
            if let Some(body) = node.child_by_field_name("body") {
                visit(body, src, path, cls, idx);
            }
            cls.pop();
        }
        "method_declaration" => {
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
            if let Some(ret) = node.child_by_field_name("type") {
                record_type_use(&mut idx.uses, &stable, ret, src);
            }
            if let Some(params) = node.child_by_field_name("parameters") {
                visit_param_types(params, src, &stable, &mut idx.uses);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                let class_key = cls.join(".");
                if !class_key.is_empty() {
                    visit_instance_fields(body, src, &class_key, idx);
                }
            }
        }
        "constructor_declaration" => {
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
                visit_param_types(params, src, &stable, &mut idx.uses);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                let class_key = cls.join(".");
                if !class_key.is_empty() {
                    visit_instance_fields(body, src, &class_key, idx);
                }
            }
        }
        "field_declaration" => {
            let owner = if cls.is_empty() {
                "$file".to_string()
            } else {
                cls.join(".")
            };
            let count = node.named_child_count();
            for i in 0..count {
                let Some(decl) = node.named_child(i) else { continue };
                if decl.kind() != "variable_declarator" {
                    continue;
                }
                if let Some(value) = decl.child_by_field_name("value") {
                    visit_calls(value, src, &owner, &mut idx.calls, &mut idx.uses);
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

pub struct JavaIndexer;

impl LanguageIndexer for JavaIndexer {
    fn language(&self) -> Language { Language::Java }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_java_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_java_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "java" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_class_methods_and_constructor() {
        let src = r#"
package com.example;

import java.util.List;

public class Board {
    private List<Cell> cells;

    public Board(int size) {
        this.cells = new ArrayList<>();
    }

    public Cell getCell(int idx) {
        return cells.get(idx);
    }
}
"#;
        let idx = index_java_file("Board.java", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.Board" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.getCell" && s.kind == NodeKind::Function));
    }

    #[test]
    fn indexes_extends_and_implements() {
        let src = r#"
public class Dog extends Animal implements Runnable, Serializable {
    public void run() {}
}
"#;
        let idx = index_java_file("Dog.java", src).unwrap();
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Animal"));
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Runnable"));
    }

    #[test]
    fn indexes_interface_declaration() {
        let src = r#"
public interface Drawable {
    void draw();
    int getArea();
}
"#;
        let idx = index_java_file("Drawable.java", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Drawable" && s.kind == NodeKind::Class));
    }

    #[test]
    fn indexes_imports() {
        let src = "import java.util.List;\nimport java.io.*;\n";
        let idx = index_java_file("App.java", src).unwrap();
        assert!(idx.imports.len() >= 2);
        assert!(idx.imports.iter().any(|i| i.style == ImportStyle::Star));
    }

    #[test]
    fn indexes_instance_fields_from_this_assignment() {
        let src = r#"
public class Board {
    private Cell cell = new Cell();

    public Board(int size) {
        this.cells = new ArrayList<>();
    }

    public Cell getCell(int idx) {
        return this.cells.get(idx);
    }
}
"#;
        let idx = index_java_file("Board.java", src).unwrap();
        let fields = idx.instance_fields.get("Board").expect("Board instance_fields");
        assert_eq!(fields.get("cells").map(String::as_str), Some("ArrayList"));
        assert!(
            idx.calls.iter().any(|c| {
                c.caller_stable_key == "Board"
                    && matches!(&c.callee, CallReceiver::Bare(n) if n == "Cell")
            }),
            "field initializer new Cell() should be attributed to class"
        );
        assert!(
            idx.calls.iter().any(|c| {
                c.caller_stable_key == "Board.getCell"
                    && matches!(
                        &c.callee,
                        CallReceiver::Attr { object, name }
                            if name == "get"
                                && matches!(
                                    object.as_ref(),
                                    CallReceiver::Attr { object, name: field }
                                        if field == "cells"
                                            && matches!(
                                                object.as_ref(),
                                                CallReceiver::Bare(b) if b == "this"
                                            )
                                )
                    )
            }),
            "expected this.cells.get call site in getCell"
        );
    }
}
