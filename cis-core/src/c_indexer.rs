//! C tree-sitter indexer — extracts functions, structs, unions, enums, typedefs,
//! `#include` directives, call sites, and type annotations.

use tree_sitter::Node;

use crate::graph::NodeKind;
use crate::index_model::{
    assign_collision_disambiguators, span_from_tree_sitter_node, whole_file_span, CallReceiver,
    FileIndex, ImportStyle, ParsedCall, ParsedImport, ParsedSymbol, ParsedUse,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_c_module_key(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".c")
        .trim_end_matches(".h")
        .replace('/', ".")
}

pub fn index_c_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_c::LANGUAGE.into())
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
    idx.imports = extract_includes(root, content);
    assign_collision_disambiguators(&mut idx);
    Ok(idx)
}

fn node_text<'a>(node: Node, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn parse_call_receiver(node: Node, src: &str) -> Option<CallReceiver> {
    match node.kind() {
        "identifier" => Some(CallReceiver::Bare(node_text(node, src).to_string())),
        "field_expression" => {
            let arg = node.child_by_field_name("argument")?;
            let field = node.child_by_field_name("field")?;
            let obj = parse_call_receiver(arg, src)?;
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
        "type_identifier" | "identifier" | "primitive_type" => {
            let t = node_text(node, src).trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        }
        "struct_specifier" | "union_specifier" | "enum_specifier" => {
            node.child_by_field_name("name")
                .map(|n| node_text(n, src).trim().to_string())
                .filter(|s| !s.is_empty())
        }
        "sized_type_specifier" => {
            Some(node_text(node, src).trim().to_string()).filter(|s| !s.is_empty())
        }
        _ => None,
    }
}

fn record_type_use(uses: &mut Vec<ParsedUse>, owner: &str, node: Node, src: &str) {
    if let Some(name) = simple_type_name(node, src) {
        let builtin = matches!(
            name.as_str(),
            "void" | "int" | "char" | "short" | "long" | "float" | "double"
                | "unsigned" | "signed" | "size_t" | "ssize_t" | "ptrdiff_t"
                | "bool" | "_Bool" | "FILE"
        );
        if !builtin && !name.contains(' ') {
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
    if node.kind() == "call_expression" {
        if let Some(func) = node.child_by_field_name("function") {
            if let Some(callee) = parse_call_receiver(func, src) {
                let leaf = callee.leaf_name();
                if !matches!(leaf,
                    "printf" | "fprintf" | "sprintf" | "snprintf"
                    | "scanf" | "fscanf" | "sscanf"
                    | "puts" | "fputs" | "putchar" | "getchar"
                    | "malloc" | "calloc" | "realloc" | "free"
                    | "memcpy" | "memset" | "memmove" | "memcmp"
                    | "strlen" | "strcpy" | "strncpy" | "strcmp" | "strncmp" | "strcat"
                    | "sizeof" | "assert" | "exit" | "abort"
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
        "identifier" | "type_identifier" | "field_identifier" => {
            let name = node_text(node, src).to_string();
            if name.is_empty() { None } else { Some(name) }
        }
        "function_declarator"
        | "pointer_declarator"
        | "array_declarator"
        | "pointer_type_declarator"
        | "function_type_declarator"
        | "array_type_declarator"
        | "parenthesized_declarator"
        | "parenthesized_type_declarator" => {
            if let Some(d) = node.child_by_field_name("declarator") {
                if let Some(name) = extract_declarator_name(d, src) {
                    return Some(name);
                }
            }
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    if let Some(name) = extract_declarator_name(c, src) {
                        return Some(name);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn visit(node: Node, src: &str, path: &str, idx: &mut FileIndex) {
    match node.kind() {
        "translation_unit" => {
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    visit(c, src, path, idx);
                }
            }
        }
        "function_definition" => {
            let declarator = node.child_by_field_name("declarator");
            let name = declarator.and_then(|d| extract_declarator_name(d, src));
            let Some(name) = name else { return };
            if name.is_empty() { return; }
            idx.symbols.push(ParsedSymbol {
                stable_key: name.clone(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::{name}"),
                kind: NodeKind::Function,
                span: span_from_tree_sitter_node(node),
            });
            if let Some(decl) = declarator {
                visit_param_types_c(decl, src, &name, &mut idx.uses);
            }
            if let Some(ret_type) = node.child_by_field_name("type") {
                record_type_use(&mut idx.uses, &name, ret_type, src);
            }
            if let Some(body) = node.child_by_field_name("body") {
                visit_calls(body, src, &name, &mut idx.calls, &mut idx.uses);
            }
        }
        "declaration" => {
            // Forward declarations / function prototypes
            if let Some(decl) = node.child_by_field_name("declarator") {
                if decl.kind() == "function_declarator" {
                    if let Some(name) = extract_declarator_name(decl, src) {
                        if !name.is_empty() {
                            let already_defined = idx.symbols.iter().any(|s| s.stable_key == name);
                            if !already_defined {
                                idx.symbols.push(ParsedSymbol {
                                    stable_key: name.clone(),
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
        "struct_specifier" | "union_specifier" | "enum_specifier" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = node_text(name_node, src);
                if !name.is_empty() {
                    let has_body = node.child_by_field_name("body").is_some();
                    if has_body {
                        idx.symbols.push(ParsedSymbol {
                            stable_key: name.to_string(),
                            disambiguator: String::new(),
                            qualified_name: format!("{path}::{name}"),
                            kind: NodeKind::Class,
                            span: span_from_tree_sitter_node(node),
                        });
                        if let Some(body) = node.child_by_field_name("body") {
                            visit_struct_fields(body, src, name, &mut idx.uses);
                        }
                    }
                }
            }
        }
        "type_definition" => {
            // `typedef` may declare multiple aliases; field "declarator" is multi-valued.
            let mut cursor = node.walk();
            let declarators: Vec<_> = node
                .children_by_field_name("declarator", &mut cursor)
                .collect();
            for decl in declarators {
                if let Some(name) = extract_declarator_name(decl, src) {
                    let already = idx.symbols.iter().any(|s| s.stable_key == name);
                    if !already {
                        idx.symbols.push(ParsedSymbol {
                            stable_key: name.clone(),
                            disambiguator: String::new(),
                            qualified_name: format!("{path}::{name}"),
                            kind: NodeKind::Class,
                            span: span_from_tree_sitter_node(node),
                        });
                        if let Some(ty) = node.child_by_field_name("type") {
                            // Prefer the typedef name over an anonymous struct/union body.
                            if ty.child_by_field_name("name").is_some() {
                                record_type_use(&mut idx.uses, &name, ty, src);
                            } else if let Some(body) = ty.child_by_field_name("body") {
                                visit_struct_fields(body, src, &name, &mut idx.uses);
                            }
                        }
                    }
                }
            }
            // Also index a named struct/union/enum inside the typedef.
            let count = node.named_child_count();
            for i in 0..count {
                if let Some(c) = node.named_child(i) {
                    if matches!(c.kind(), "struct_specifier" | "union_specifier" | "enum_specifier") {
                        visit(c, src, path, idx);
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

fn visit_param_types_c(declarator: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    if declarator.kind() == "function_declarator" {
        if let Some(params) = declarator.child_by_field_name("parameters") {
            let count = params.named_child_count();
            for i in 0..count {
                let Some(param) = params.named_child(i) else { continue };
                if param.kind() == "parameter_declaration" {
                    if let Some(ty) = param.child_by_field_name("type") {
                        record_type_use(uses, owner, ty, src);
                    }
                }
            }
        }
    }
}

fn visit_struct_fields(body: Node, src: &str, owner: &str, uses: &mut Vec<ParsedUse>) {
    let count = body.named_child_count();
    for i in 0..count {
        let Some(field) = body.named_child(i) else { continue };
        if field.kind() == "field_declaration" {
            if let Some(ty) = field.child_by_field_name("type") {
                record_type_use(uses, owner, ty, src);
            }
        }
    }
}

pub struct CIndexer;

impl LanguageIndexer for CIndexer {
    fn language(&self) -> Language { Language::C }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_c_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_c_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "c" }
}

pub struct CHeaderIndexer;

impl LanguageIndexer for CHeaderIndexer {
    fn language(&self) -> Language { Language::C }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        index_c_file(path, content)
    }

    fn module_key(&self, rel_path: &str) -> String {
        path_to_c_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str { "h" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_function_and_struct() {
        let src = r#"
#include <stdio.h>
#include "board.h"

typedef struct {
    int x;
    int y;
} Point;

struct Board {
    Point cells[64];
    int size;
};

int board_init(struct Board* b, int size) {
    b->size = size;
    return 0;
}

Point board_get_cell(struct Board* b, int idx) {
    return b->cells[idx];
}
"#;
        let idx = index_c_file("board.c", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Point" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Board" && s.kind == NodeKind::Class));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "board_init" && s.kind == NodeKind::Function));
        assert!(idx.symbols.iter().any(|s| s.stable_key == "board_get_cell" && s.kind == NodeKind::Function));
    }

    #[test]
    fn indexes_includes() {
        let src = "#include <stdio.h>\n#include \"myheader.h\"\n";
        let idx = index_c_file("main.c", src).unwrap();
        assert!(idx.imports.len() >= 2);
    }

    #[test]
    fn indexes_enum() {
        let src = "enum Color { RED, GREEN, BLUE };\n";
        let idx = index_c_file("colors.c", src).unwrap();
        assert!(idx.symbols.iter().any(|s| s.stable_key == "Color" && s.kind == NodeKind::Class));
    }
}
