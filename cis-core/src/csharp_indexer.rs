//! C# tree-sitter indexer — extracts classes, structs, interfaces, enums, methods,
//! constructors, properties, using directives, call sites, inheritance, and type annotations.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_csharp_module_key(rel_path: &str) -> String {
    rel_path.trim_end_matches(".cs").replace('/', ".")
}

pub fn index_csharp_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    let language: tree_sitter::Language = tree_sitter_c_sharp::LANGUAGE.into();
    parser
        .set_language(&language)
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
    idx.imports = extract_usings(root, content);
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
        "member_access_expression" => {
            let expr = node.child_by_field_name("expression")?;
            let name = node.child_by_field_name("name")?;
            let obj = parse_call_receiver(expr, src)?;
            Some(CallReceiver::Attr {
                object: Box::new(obj),
                name: node_text(name, src).to_string(),
            })
        }
        "generic_name" => {
            node.child_by_field_name("name")
                .map(|n| CallReceiver::Bare(node_text(n, src).to_string()))
                .or_else(|| {
                    node.named_child(0)
                        .map(|n| CallReceiver::Bare(node_text(n, src).to_string()))
                })
        }
        _ => None,
    }
}

fn simple_type_name(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "identifier" | "predefined_type" => {
            let t = node_text(node, src).trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        }
        "generic_name" => {
            node.child_by_field_name("name")
                .or_else(|| node.named_child(0))
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "qualified_name" => {
            node.child_by_field_name("name")
                .or_else(|| {
                    let c = node.named_child_count();
                    if c > 0 { node.named_child(c - 1) } else { None }
                })
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "nullable_type" | "array_type" => {
            node.named_child(0).and_then(|c| simple_type_name(c, src))
        }
        _ => None,
    }
}

fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
    if let Some(name) = simple_type_name(node, src) {
        let builtin = matches!(
            name.as_str(),
            "void" | "int" | "long" | "short" | "byte" | "sbyte" | "float" | "double"
                | "decimal" | "bool" | "char" | "string" | "object" | "dynamic"
                | "var" | "nint" | "nuint"
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
        "invocation_expression" => {
            if let Some(func) = node.child_by_field_name("function") {
                if let Some(callee) = parse_call_receiver(func, src) {
                    let leaf = callee.leaf_name();
                    if !matches!(leaf, "WriteLine" | "Write" | "ToString"
                        | "Format" | "Equals" | "GetHashCode" | "GetType")
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

fn extract_usings(root: Node, src: &str) -> Vec<ParsedImport> {
    let mut imports = Vec::new();
    let count = root.named_child_count();
    for i in 0..count {
        let Some(child) = root.named_child(i) else { continue };
        if child.kind() == "using_directive" {
            let text = node_text(child, src).to_string();
            let module = text
                .trim_start_matches("using ")
                .trim_start_matches("static ")
                .trim_end_matches(';')
                .trim()
                .to_string();
            let short_name = module
                .rsplit('.')
                .next()
                .unwrap_or(&module)
                .to_string();
            imports.push(ParsedImport {
                module,
                style: ImportStyle::Star,
                names: vec![short_name],
                span: span_from_tree_sitter_node(child),
            });
        }
    }
    imports
}

fn extract_base_types(node: Node, src: &str, class_key: &str, idx: &mut FileIndex) {
    let count = node.named_child_count();
    for i in 0..count {
        let Some(child) = node.named_child(i) else { continue };
        if child.kind() == "base_list" {
            let bc = child.named_child_count();
            for j in 0..bc {
                if let Some(base) = child.named_child(j) {
                    if let Some(base_name) = simple_type_name(base, src) {
                        if base_name != "object" && base_name != "Object" {
                            idx.extends.push(ParsedExtends {
                                class_stable_key: class_key.to_string(),
                                base_name,
                                span: span_from_tree_sitter_node(base),
                            });
                        }
                    }
                }
            }
        }
    }
}

fn visit_param_types(node: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    let count = node.named_child_count();
    for i in 0..count {
        let Some(param) = node.named_child(i) else { continue };
        if param.kind() == "parameter" {
            if let Some(ty) = param.child_by_field_name("type") {
                record_type_use(uses, owner, ty, src);
            }
        }
    }
}

fn this_field_from_target(node: Node, src: &str) -> Option<String> {
    if node.kind() != "member_access_expression" {
        return None;
    }
    let expr = node.child_by_field_name("expression")?;
    let name = node.child_by_field_name("name")?;
    if expr.kind() != "this" {
        return None;
    }
    let field = node_text(name, src).trim().to_string();
    if field.is_empty() {
        None
    } else {
        Some(field)
    }
}

fn infer_type_from_expr(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "object_creation_expression" => {
            node.child_by_field_name("type")
                .and_then(|t| simple_type_name(t, src))
        }
        "invocation_expression" => {
            let func = node.child_by_field_name("function")?;
            if func.kind() != "identifier" && func.kind() != "generic_name" {
                return None;
            }
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

fn visit(
    node: Node, src: &str, path: &str,
    cls: &mut Vec<String>, idx: &mut FileIndex,
) {
    match node.kind() {
        "compilation_unit" | "declaration_list" | "block" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, cls, idx);
                }
            }
        }
        "namespace_declaration" | "file_scoped_namespace_declaration" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, cls, idx);
                }
            }
        }
        "class_declaration" | "record_declaration" => {
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
            extract_base_types(node, src, &stable, idx);
            if let Some(body) = node.child_by_field_name("body") {
                visit(body, src, path, cls, idx);
            }
            cls.pop();
        }
        "struct_declaration" => {
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
            extract_base_types(node, src, &stable, idx);
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
            extract_base_types(node, src, &stable, idx);
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
            if let Some(ret) = node.child_by_field_name("returns") {
                record_type_use(&mut idx.uses, &stable, ret, src);
            }
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
        "property_declaration" => {
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
            if let Some(ty) = node.child_by_field_name("type") {
                record_type_use(&mut idx.uses, &stable, ty, src);
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

pub struct CSharpIndexer;

impl LanguageIndexer for CSharpIndexer {
    fn language(&self) -> Language { Language::CSharp }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_csharp_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_csharp_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "cs" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_class_methods_and_properties() {
        let src = r#"
using System;
using System.Collections.Generic;

namespace Example {
    public class Board {
        public int Size { get; set; }
        private List<Cell> cells;

        public Board(int size) {
            Size = size;
            cells = new List<Cell>();
        }

        public Cell GetCell(int idx) {
            return cells[idx];
        }
    }
}
"#;
        let idx = index_csharp_file("Board.cs", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.Board" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.GetCell" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board.Size"));
    }

    #[test]
    fn indexes_inheritance() {
        let src = r#"
public class Dog : Animal, IRunnable {
    public void Run() {}
}
"#;
        let idx = index_csharp_file("Dog.cs", src).unwrap();
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Animal"));
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "IRunnable"));
    }

    #[test]
    fn indexes_interface() {
        let src = r#"
public interface IDrawable {
    void Draw();
    int GetArea();
}
"#;
        let idx = index_csharp_file("IDrawable.cs", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "IDrawable" && s.kind == NodeKind::Class));
    }

    #[test]
    fn indexes_usings() {
        let src = "using System;\nusing System.Collections.Generic;\n";
        let idx = index_csharp_file("App.cs", src).unwrap();
        assert!(idx.imports.len() >= 2);
    }

    #[test]
    fn indexes_instance_fields_from_this_assignment() {
        let src = r#"
public class Board {
    public Board(int size) {
        this.cells = new List<Cell>();
    }

    public Cell GetCell(int idx) {
        return this.cells[idx];
    }
}
"#;
        let idx = index_csharp_file("Board.cs", src).unwrap();
        let fields = idx.instance_fields.get("Board").expect("Board instance_fields");
        assert_eq!(fields.get("cells").map(String::as_str), Some("List"));
    }
}
