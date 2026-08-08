//! C++ tree-sitter indexer — extracts functions, classes, structs, enums, namespaces,
//! templates, `#include` directives, call sites, inheritance, and type annotations.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedExtends, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_cpp_module_key(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".cpp")
        .trim_end_matches(".cxx")
        .trim_end_matches(".cc")
        .trim_end_matches(".hpp")
        .trim_end_matches(".hxx")
        .trim_end_matches(".hh")
        .trim_end_matches(".h")
        .replace('/', ".")
}

pub fn index_cpp_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_cpp::LANGUAGE.into())
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
    idx.imports = extract_includes(root, content);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

fn node_text<'a>(node: Node, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn parse_call_receiver(node: Node, src: &str) -> Option<CallReceiver> {
    match node.kind() {
        "identifier" | "field_identifier" | "destructor_name" => {
            Some(CallReceiver::Bare(node_text(node, src).to_string()))
        }
        "field_expression" => {
            let arg = node.child_by_field_name("argument")?;
            let field = node.child_by_field_name("field")?;
            let obj = parse_call_receiver(arg, src)?;
            Some(CallReceiver::Attr {
                object: Box::new(obj),
                name: node_text(field, src).to_string(),
            })
        }
        "qualified_identifier" => {
            let name = node.child_by_field_name("name")?;
            Some(CallReceiver::Bare(node_text(name, src).to_string()))
        }
        "template_function" => {
            node.child_by_field_name("name")
                .and_then(|n| parse_call_receiver(n, src))
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
        "qualified_identifier" => {
            node.child_by_field_name("name")
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "template_type" => {
            node.child_by_field_name("name")
                .and_then(|n| simple_type_name(n, src))
        }
        "reference_declarator" | "pointer_declarator" | "abstract_reference_declarator"
        | "abstract_pointer_declarator" => {
            node.named_child(0).and_then(|c| simple_type_name(c, src))
        }
        _ => None,
    }
}

fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
    if let Some(name) = simple_type_name(node, src) {
        let builtin = matches!(
            name.as_str(),
            "void" | "int" | "char" | "short" | "long" | "float" | "double"
                | "unsigned" | "signed" | "bool" | "auto" | "size_t" | "ssize_t"
                | "ptrdiff_t" | "nullptr_t" | "wchar_t" | "char8_t" | "char16_t" | "char32_t"
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

fn this_field_from_target(node: Node, src: &str) -> Option<String> {
    if node.kind() != "field_expression" {
        return None;
    }
    let arg = node.child_by_field_name("argument")?;
    let field = node.child_by_field_name("field")?;
    let arg_text = node_text(arg, src);
    if arg.kind() != "this" && arg_text != "this" {
        return None;
    }
    let name = node_text(field, src).to_string();
    if name.is_empty() { None } else { Some(name) }
}

fn infer_type_from_expr(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "new_expression" => node
            .child_by_field_name("type")
            .and_then(|t| simple_type_name(t, src))
            .or_else(|| {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        if let Some(name) = simple_type_name(c, src) {
                            return Some(name);
                        }
                    }
                }
                None
            }),
        "call_expression" => {
            let func = node.child_by_field_name("function")?;
            parse_call_receiver(func, src)?
                .root_bare_name()
                .map(|s| s.to_string())
        }
        _ => None,
    }
}

fn record_instance_field_assignment(node: Node, src: &str, class_key: &str, idx: &mut FileIndex) {
    if node.kind() != "assignment_expression" {
        return;
    }
    let Some(left) = node.child_by_field_name("left") else { return };
    let Some(right) = node.child_by_field_name("right") else { return };
    let Some(field) = this_field_from_target(left, src) else { return };
    let Some(type_name) = infer_type_from_expr(right, src) else { return };
    if type_name == "this" || type_name == "self" {
        return;
    }
    let fields = idx.instance_fields.entry(class_key.to_string()).or_default();
    if fields
        .get(&field)
        .is_some_and(|existing| existing != "this" && existing != "self")
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
                        "printf" | "fprintf" | "sprintf" | "snprintf"
                        | "cout" | "cerr" | "endl"
                        | "malloc" | "calloc" | "realloc" | "free"
                        | "sizeof" | "assert" | "static_assert"
                        | "std" | "move" | "forward"
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

fn extract_includes(root: Node, src: &str) -> Vec<ParsedImport> {
    let mut imports = Vec::new();
    let count = root.named_child_count();
    for i in 0..count {
        let Some(child) = root.named_child(i) else { continue };
        if child.kind() == "preproc_include" {
            if let Some(path_node) = child.child_by_field_name("path") {
                let raw = node_text(path_node, src);
                let module = raw
                    .trim_matches('"')
                    .trim_matches('<')
                    .trim_matches('>')
                    .to_string();
                let header_name = module
                    .rsplit('/')
                    .next()
                    .unwrap_or(&module)
                    .trim_end_matches(".h")
                    .trim_end_matches(".hpp")
                    .trim_end_matches(".hxx")
                    .to_string();
                imports.push(ParsedImport {
                    module,
                    style: ImportStyle::Star,
                    names: vec![header_name],
                    span: span_from_tree_sitter_node(child),
                });
            }
        }
    }
    imports
}

fn extract_declarator_name(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "identifier" | "field_identifier" => Some(node_text(node, src).to_string()),
        "function_declarator" => {
            node.child_by_field_name("declarator")
                .and_then(|d| extract_declarator_name(d, src))
        }
        "pointer_declarator" | "reference_declarator" => {
            node.child_by_field_name("declarator")
                .and_then(|d| extract_declarator_name(d, src))
        }
        "qualified_identifier" => {
            node.child_by_field_name("name")
                .map(|n| node_text(n, src).to_string())
        }
        "destructor_name" => Some(node_text(node, src).to_string()),
        "operator_name" => Some(node_text(node, src).to_string()),
        "template_function" => {
            node.child_by_field_name("name")
                .and_then(|n| extract_declarator_name(n, src))
        }
        _ => None,
    }
}

fn extract_scoped_type_name(node: Node, src: &str) -> String {
    if node.kind() == "qualified_identifier" {
        if let Some(scope) = node.child_by_field_name("scope") {
            let scope_name = node_text(scope, src);
            if let Some(name) = node.child_by_field_name("name") {
                return format!("{}.{}", scope_name.trim_end_matches("::"), node_text(name, src));
            }
        }
    }
    extract_declarator_name(node, src).unwrap_or_default()
}

fn visit(node: Node, src: &str, path: &str, scope: &mut Vec<String>, idx: &mut FileIndex) {
    match node.kind() {
        "translation_unit" | "compound_statement" | "declaration_list" | "field_declaration_list" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, scope, idx);
                }
            }
        }
        "namespace_definition" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if !name.is_empty() {
                scope.push(name.to_string());
            }
            if let Some(body) = node.child_by_field_name("body") {
                let count = body.named_child_count();
                for i in 0..count {
                    if let Some(c) = body.named_child(i) {
                        visit(c, src, path, scope, idx);
                    }
                }
            }
            if !name.is_empty() {
                scope.pop();
            }
        }
        "template_declaration" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    if c.kind() != "template_parameter_list" {
                        visit(c, src, path, scope, idx);
                    }
                }
            }
        }
        "function_definition" => {
            let declarator = node.child_by_field_name("declarator");
            let raw_name = declarator.and_then(|d| extract_declarator_name(d, src));
            let Some(raw) = raw_name else { return };
            if raw.is_empty() { return; }

            let scoped_name = declarator
                .map(|d| extract_scoped_type_name(d, src))
                .unwrap_or_else(|| raw.clone());

            let stable = if scope.is_empty() {
                scoped_name.clone()
            } else {
                format!("{}.{}", scope.join("."), scoped_name)
            };

            idx.symbols.push(ParsedSymbol {
                stable_key: stable.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{stable}"),
                kind: NodeKind::Function,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(ret_type) = node.child_by_field_name("type") {
                record_type_use(&mut idx.uses, &stable, ret_type, src);
            }
            if let Some(decl) = declarator {
                visit_param_types_cpp(decl, src, &stable, &mut idx.uses);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &stable, &mut idx.calls, &mut idx.uses);
                // Methods defined inside a class/struct body have the type on the scope stack.
                if let Some(class_key) = scope.last() {
                    visit_instance_fields(body, src, class_key, idx);
                } else if let Some((type_name, _)) = scoped_name.rsplit_once("::") {
                    // Out-of-line `Type::method` — use the left-hand type as class key.
                    let class_key = type_name.rsplit("::").next().unwrap_or(type_name);
                    visit_instance_fields(body, src, class_key, idx);
                } else if scoped_name.contains('.') {
                    if let Some((class_key, _)) = scoped_name.rsplit_once('.') {
                        visit_instance_fields(body, src, class_key, idx);
                    }
                }
            }
        }
        "class_specifier" | "struct_specifier" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            let has_body = node.child_by_field_name("body").is_some();
            if !has_body { return; }

            scope.push(name.to_string());
            let stable = scope.join(".");
            idx.symbols.push(ParsedSymbol {
                stable_key: stable.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{stable}"),
                kind: NodeKind::Class,
                span: span_from_tree_sitter_node(node),
            });

            // base class clause
            let nc = node.named_child_count();
            for i in 0..nc {
                let Some(child) = node.named_child(i) else { continue };
                if child.kind() == "base_class_clause" {
                    let bc = child.named_child_count();
                    for j in 0..bc {
                        if let Some(base) = child.named_child(j) {
                            if let Some(base_name) = simple_type_name(base, src) {
                                if base_name != "public" && base_name != "private"
                                    && base_name != "protected" && base_name != "virtual"
                                {
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

            if let Some(body) = node.child_by_field_name("body") {
                visit(body, src, path, scope, idx);
            }
            scope.pop();
        }
        "enum_specifier" => {
            let name = node.child_by_field_name("name")
                .map(|n| node_text(n, src))
                .unwrap_or("");
            if name.is_empty() { return; }
            let has_body = node.child_by_field_name("body").is_some();
            if !has_body { return; }

            let stable = if scope.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", scope.join("."), name)
            };
            idx.symbols.push(ParsedSymbol {
                stable_key: stable,
                disambiguator: String::new(),
                qualified_name: format!("{path}::{name}"),
                kind: NodeKind::Class,
                span: span_from_tree_sitter_node(node),
            });
        }
        "type_definition" => {
            if let Some(decl) = node.child_by_field_name("declarator") {
                if let Some(name) = extract_declarator_name(decl, src) {
                    if !name.is_empty() {
                        let stable = if scope.is_empty() {
                            name.clone()
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
                        if let Some(ty) = node.child_by_field_name("type") {
                            record_type_use(&mut idx.uses, &stable, ty, src);
                        }
                    }
                }
            }
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, scope, idx);
                }
            }
        }
        "declaration" => {
            if let Some(decl) = node.child_by_field_name("declarator") {
                if decl.kind() == "function_declarator" {
                    if let Some(name) = extract_declarator_name(decl, src) {
                        if !name.is_empty() {
                            let already = idx.symbols.iter().any(|s| s.stable_key == name);
                            if !already {
                                let stable = if scope.is_empty() {
                                    name.clone()
                                } else {
                                    format!("{}.{}", scope.join("."), name)
                                };
                                idx.symbols.push(ParsedSymbol {
                                    stable_key: stable,
                                    disambiguator: String::new(),
                                    qualified_name: format!("{path}::{name}"),
                                    kind: NodeKind::Function,
                                    span: span_from_tree_sitter_node(node),
                                });
                            }
                        }
                    }
                }
            }
        }
        "access_specifier" => {}
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

fn visit_param_types_cpp(declarator: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    match declarator.kind() {
        "function_declarator" => {
            if let Some(params) = declarator.child_by_field_name("parameters") {
                let count = params.named_child_count();
                for i in 0..count {
                    let Some(param) = params.named_child(i) else { continue };
                    if param.kind() == "parameter_declaration" || param.kind() == "optional_parameter_declaration" {
                        if let Some(ty) = param.child_by_field_name("type") {
                            record_type_use(uses, owner, ty, src);
                        }
                    }
                }
            }
        }
        "pointer_declarator" | "reference_declarator" => {
            if let Some(inner) = declarator.child_by_field_name("declarator") {
                visit_param_types_cpp(inner, src, owner, uses);
            }
        }
        _ => {}
    }
}

pub struct CppIndexer;

impl LanguageIndexer for CppIndexer {
    fn language(&self) -> Language { Language::Cpp }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_cpp_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_cpp_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "cpp" }
}

pub struct CppHeaderIndexer;

impl LanguageIndexer for CppHeaderIndexer {
    fn language(&self) -> Language { Language::Cpp }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_cpp_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_cpp_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "hpp" }
}

macro_rules! cpp_ext_indexer {
    ($name:ident, $ext:expr) => {
        pub struct $name;

        impl LanguageIndexer for $name {
            fn language(&self) -> Language {
                Language::Cpp
            }

            fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
                index_cpp_file(path, content)
            }

            fn module_key(&self, rel_path: &str) -> String {
                path_to_cpp_module_key(rel_path)
            }

            fn file_extension(&self) -> &'static str {
                $ext
            }
        }
    };
}

cpp_ext_indexer!(CppCxxIndexer, "cxx");
cpp_ext_indexer!(CppCcIndexer, "cc");
cpp_ext_indexer!(CppHxxIndexer, "hxx");
cpp_ext_indexer!(CppHhIndexer, "hh");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_class_and_methods() {
        let src = r#"
#include <vector>
#include "board.h"

class Board {
public:
    Board(int size);
    Cell getCell(int idx) const;
private:
    std::vector<Cell> cells_;
};

Board::Board(int size) {
    cells_.resize(size);
}

Cell Board::getCell(int idx) const {
    return cells_[idx];
}
"#;
        let idx = index_cpp_file("board.cpp", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key.contains("Board") && s.kind == NodeKind::Function));
    }

    #[test]
    fn indexes_inheritance() {
        let src = r#"
class Animal {
public:
    virtual void speak() = 0;
};

class Dog : public Animal {
public:
    void speak() override {}
};
"#;
        let idx = index_cpp_file("animals.cpp", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Animal"));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Dog"));
        assert!(idx.extends.iter().any(|e| e.class_stable_key == "Dog" && e.base_name == "Animal"));
    }

    #[test]
    fn indexes_namespace() {
        let src = r#"
namespace game {
    class Engine {
    public:
        void run();
    };
}
"#;
        let idx = index_cpp_file("engine.cpp", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "game.Engine"));
    }

    #[test]
    fn indexes_includes() {
        let src = "#include <iostream>\n#include \"myheader.hpp\"\n";
        let idx = index_cpp_file("main.cpp", src).unwrap();
        assert!(idx.imports.len() >= 2);
    }

    #[test]
    fn indexes_instance_fields_from_this_assignment() {
        let src = r#"
class Board {
public:
    Board() {
        this->cells = new CellGrid();
    }
    CellGrid* cells;
};
"#;
        let idx = index_cpp_file("board.cpp", src).unwrap();
        let fields = idx.instance_fields.get("Board").expect("Board instance_fields");
        assert_eq!(fields.get("cells").map(String::as_str), Some("CellGrid"));
    }
}
