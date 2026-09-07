//! Independent tree-sitter CST census used as ground truth for indexer recall.
//!
//! This walker is **not** the language indexer. It enumerates every declaration and
//! relation node the grammar exposes, then classifies a subset as *indexable*
//! (module / type-member scope). Nested functions and anonymous literals are kept
//! as diagnostics but are not required for recall floors.

use crate::graph::NodeKind;
use tree_sitter::Node;

#[derive(Debug, Clone)]
pub struct OracleSymbol {
    pub name: String,
    pub kind: NodeKind,
    pub line: u32,
    pub col: u32,
    pub node_kind: String,
    pub indexable: bool,
}

#[derive(Debug, Clone)]
pub struct OracleCall {
    pub leaf: String,
    pub line: u32,
    pub col: u32,
}

#[derive(Debug, Clone)]
pub struct OracleImport {
    pub module: String,
    pub line: u32,
}

#[derive(Debug, Clone)]
pub struct OracleExtends {
    pub class_name: String,
    pub base_name: String,
    pub line: u32,
}

#[derive(Debug, Clone)]
pub struct OracleUse {
    pub type_name: String,
    pub line: u32,
}

#[derive(Debug, Clone, Default)]
pub struct OracleFile {
    pub has_error: bool,
    pub symbols: Vec<OracleSymbol>,
    pub calls: Vec<OracleCall>,
    pub imports: Vec<OracleImport>,
    pub extends: Vec<OracleExtends>,
    pub uses: Vec<OracleUse>,
}

pub fn census(path: &str, content: &str) -> Option<OracleFile> {
    let ext = path.rsplit('.').next().unwrap_or("");
    let tree = parse_source(ext, content)?;
    let root = tree.root_node();
    let mut out = OracleFile {
        has_error: root.has_error(),
        ..OracleFile::default()
    };
    match ext {
        "py" => census_python(root, content, &mut out),
        "rs" => census_rust(root, content, &mut out),
        "go" => census_go(root, content, &mut out),
        "js" | "jsx" => census_javascript(root, content, &mut out),
        "ts" | "tsx" => census_typescript(root, content, &mut out),
        "java" => census_java(root, content, &mut out),
        "c" | "h" => census_c(root, content, &mut out),
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "hh" => census_cpp(root, content, &mut out),
        "cs" => census_csharp(root, content, &mut out),
        _ => return None,
    }
    Some(out)
}

pub fn grammar_available(ext: &str) -> bool {
    match ext {
        "py" => cfg!(feature = "tree-sitter"),
        "rs" => cfg!(feature = "ts-rust"),
        "go" => cfg!(feature = "ts-go"),
        "js" | "jsx" => cfg!(feature = "ts-javascript"),
        "ts" | "tsx" => cfg!(feature = "ts-typescript"),
        "java" => cfg!(feature = "ts-java"),
        "c" | "h" => cfg!(feature = "ts-c"),
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "hh" => cfg!(feature = "ts-cpp"),
        "cs" => cfg!(feature = "ts-csharp"),
        _ => false,
    }
}

fn parse_source(ext: &str, content: &str) -> Option<tree_sitter::Tree> {
    let mut parser = tree_sitter::Parser::new();
    let ok = match ext {
        "py" => {
            #[cfg(feature = "tree-sitter")]
            {
                parser
                    .set_language(&tree_sitter_python::LANGUAGE.into())
                    .ok()?;
                true
            }
            #[cfg(not(feature = "tree-sitter"))]
            {
                false
            }
        }
        "rs" => {
            #[cfg(feature = "ts-rust")]
            {
                parser.set_language(&tree_sitter_rust::LANGUAGE.into()).ok()?;
                true
            }
            #[cfg(not(feature = "ts-rust"))]
            {
                false
            }
        }
        "go" => {
            #[cfg(feature = "ts-go")]
            {
                parser.set_language(&tree_sitter_go::LANGUAGE.into()).ok()?;
                true
            }
            #[cfg(not(feature = "ts-go"))]
            {
                false
            }
        }
        "js" | "jsx" => {
            #[cfg(feature = "ts-javascript")]
            {
                parser
                    .set_language(&tree_sitter_javascript::LANGUAGE.into())
                    .ok()?;
                true
            }
            #[cfg(not(feature = "ts-javascript"))]
            {
                false
            }
        }
        "ts" => {
            #[cfg(feature = "ts-typescript")]
            {
                parser
                    .set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
                    .ok()?;
                true
            }
            #[cfg(not(feature = "ts-typescript"))]
            {
                false
            }
        }
        "tsx" => {
            #[cfg(feature = "ts-typescript")]
            {
                parser
                    .set_language(&tree_sitter_typescript::LANGUAGE_TSX.into())
                    .ok()?;
                true
            }
            #[cfg(not(feature = "ts-typescript"))]
            {
                false
            }
        }
        "java" => {
            #[cfg(feature = "ts-java")]
            {
                parser.set_language(&tree_sitter_java::LANGUAGE.into()).ok()?;
                true
            }
            #[cfg(not(feature = "ts-java"))]
            {
                false
            }
        }
        "c" | "h" => {
            #[cfg(feature = "ts-c")]
            {
                parser.set_language(&tree_sitter_c::LANGUAGE.into()).ok()?;
                true
            }
            #[cfg(not(feature = "ts-c"))]
            {
                false
            }
        }
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "hh" => {
            #[cfg(feature = "ts-cpp")]
            {
                parser.set_language(&tree_sitter_cpp::LANGUAGE.into()).ok()?;
                true
            }
            #[cfg(not(feature = "ts-cpp"))]
            {
                false
            }
        }
        "cs" => {
            #[cfg(feature = "ts-csharp")]
            {
                parser
                    .set_language(&tree_sitter_c_sharp::LANGUAGE.into())
                    .ok()?;
                true
            }
            #[cfg(not(feature = "ts-csharp"))]
            {
                false
            }
        }
        _ => false,
    };
    if !ok {
        return None;
    }
    parser.parse(content, None)
}

fn text<'a>(node: Node, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn line_col(node: Node) -> (u32, u32) {
    let p = node.start_position();
    (p.row as u32 + 1, p.column as u32 + 1)
}

fn in_type_position(node: Node) -> bool {
    let Some(p) = node.parent() else {
        return false;
    };
    matches!(
        p.kind(),
        "type"
            | "generic_type"
            | "reference_type"
            | "pointer_type"
            | "array_type"
            | "parameter"
            | "parameter_declaration"
            | "typed_parameter"
            | "field_declaration"
            | "type_annotation"
            | "return_type"
            | "constrained_type"
            | "type_arguments"
            | "base_list"
            | "super_interfaces"
            | "superclass"
            | "type_identifier"
            | "scoped_type_identifier"
            | "generic_type_with_turbofish"
            | "let_declaration"
            | "const_item"
            | "static_item"
            | "property_declaration"
            | "variable_declaration"
            | "local_variable_declaration"
            | "type_parameter"
            | "nullable_type"
            | "union_type"
            | "predefined_type"
    )
}

fn ident_ok(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        }
        _ => false,
    }
}

fn last_ident(name: &str) -> String {
    name.replace("::", ".")
        .rsplit(['.', '/'])
        .next()
        .unwrap_or(name)
        .trim()
        .to_string()
}

fn field_name(node: Node, src: &str) -> Option<String> {
    let n = node.child_by_field_name("name")?;
    let raw = text(n, src).trim();
    let leaf = last_ident(raw);
    if ident_ok(&leaf) {
        Some(leaf)
    } else if ident_ok(raw) {
        Some(raw.to_string())
    } else {
        None
    }
}

fn push_symbol(
    out: &mut OracleFile,
    node: Node,
    src: &str,
    name: String,
    kind: NodeKind,
    nest_kinds: &[&str],
) {
    if name.is_empty() || !ident_ok(&last_ident(&name)) && !name.contains("::") {
        return;
    }
    let (line, col) = line_col(node);
    let _ = src;
    out.symbols.push(OracleSymbol {
        name,
        kind,
        line,
        col,
        node_kind: node.kind().to_string(),
        indexable: !inside_function(node, nest_kinds),
    });
}

fn inside_function(node: Node, nest_kinds: &[&str]) -> bool {
    let mut p = node.parent();
    while let Some(n) = p {
        if nest_kinds.iter().any(|k| n.kind() == *k) {
            return true;
        }
        p = n.parent();
    }
    false
}

fn walk(node: Node, visit: &mut impl FnMut(Node)) {
    visit(node);
    let count = node.named_child_count();
    for i in 0..count {
        if let Some(c) = node.named_child(i) {
            if c.kind() != "comment" {
                walk(c, visit);
            }
        }
    }
}

fn leaf_identifier(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "identifier" | "field_identifier" | "type_identifier" | "property_identifier"
        | "constant" | "this" | "super" => {
            let t = text(node, src).trim();
            if t.is_empty() {
                None
            } else {
                Some(last_ident(t))
            }
        }
        "scoped_identifier" | "qualified_identifier" | "qualified_type" | "scoped_type_identifier"
        | "member_expression" | "attribute" | "field_expression" | "member_access_expression" => {
            if let Some(name) = node.child_by_field_name("name") {
                return leaf_identifier(name, src);
            }
            if let Some(name) = node.child_by_field_name("attribute") {
                return leaf_identifier(name, src);
            }
            if let Some(name) = node.child_by_field_name("property") {
                return leaf_identifier(name, src);
            }
            if let Some(name) = node.child_by_field_name("field") {
                return leaf_identifier(name, src);
            }
            let count = node.named_child_count();
            if count > 0 {
                if let Some(c) = node.named_child(count - 1) {
                    return leaf_identifier(c, src);
                }
            }
            None
        }
        "generic_type" | "generic_type_with_turbofish" => node
            .child_by_field_name("type")
            .or_else(|| node.named_child(0))
            .and_then(|n| leaf_identifier(n, src)),
        "parenthesized_expression" => node.named_child(0).and_then(|n| leaf_identifier(n, src)),
        _ => None,
    }
}

fn push_call(out: &mut OracleFile, node: Node, func: Node, src: &str, skip: &[&str]) {
    let Some(leaf) = leaf_identifier(func, src) else {
        return;
    };
    if skip.iter().any(|s| *s == leaf) {
        return;
    }
    if !ident_ok(&leaf) && !leaf.ends_with('!') {
        return;
    }
    let (line, col) = line_col(node);
    out.calls.push(OracleCall { leaf, line, col });
}

fn push_import(out: &mut OracleFile, node: Node, module: String) {
    let module = module
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '<' || c == '>' || c == ';')
        .replace("::", ".")
        .trim_start_matches("use ")
        .trim()
        .to_string();
    if module.is_empty() {
        return;
    }
    let (line, _) = line_col(node);
    out.imports.push(OracleImport { module, line });
}

fn push_extends(out: &mut OracleFile, class_name: String, base_name: String, node: Node) {
    let class_name = last_ident(&class_name);
    let base_name = last_ident(&base_name);
    if class_name.is_empty() || base_name.is_empty() {
        return;
    }
    if matches!(
        base_name.as_str(),
        "object" | "Object" | "public" | "private" | "protected" | "virtual"
    ) {
        return;
    }
    let (line, _) = line_col(node);
    out.extends.push(OracleExtends {
        class_name,
        base_name,
        line,
    });
}

fn push_use(out: &mut OracleFile, node: Node, src: &str, skip: &[&str]) {
    let Some(name) = leaf_identifier(node, src) else {
        return;
    };
    if skip.iter().any(|s| *s == name) {
        return;
    }
    if !ident_ok(&name) {
        return;
    }
    let (line, _) = line_col(node);
    out.uses.push(OracleUse {
        type_name: name,
        line,
    });
}

fn c_declarator_name(node: Node, src: &str) -> Option<String> {
    match node.kind() {
        "identifier" | "field_identifier" | "type_identifier" => {
            let t = text(node, src).trim();
            if ident_ok(t) {
                Some(t.to_string())
            } else {
                None
            }
        }
        "qualified_identifier" | "destructor_name" | "operator_name" => {
            Some(last_ident(text(node, src)))
        }
        "function_declarator"
        | "pointer_declarator"
        | "reference_declarator"
        | "array_declarator"
        | "parenthesized_declarator"
        | "abstract_pointer_declarator" => {
            if let Some(d) = node.child_by_field_name("declarator") {
                c_declarator_name(d, src)
            } else {
                let count = node.named_child_count();
                for i in 0..count {
                    if let Some(c) = node.named_child(i) {
                        if let Some(n) = c_declarator_name(c, src) {
                            return Some(n);
                        }
                    }
                }
                None
            }
        }
        _ => {
            if let Some(d) = node.child_by_field_name("declarator") {
                c_declarator_name(d, src)
            } else {
                None
            }
        }
    }
}

const PY_NEST: &[&str] = &["function_definition", "lambda"];
const RS_NEST: &[&str] = &["function_item", "closure_expression"];
const GO_NEST: &[&str] = &["function_declaration", "method_declaration", "func_literal"];
const JS_NEST: &[&str] = &[
    "function_declaration",
    "generator_function_declaration",
    "function_expression",
    "generator_function",
    "arrow_function",
    "method_definition",
];
const JAVA_NEST: &[&str] = &[
    "method_declaration",
    "constructor_declaration",
    "compact_constructor_declaration",
    "lambda_expression",
];
const C_NEST: &[&str] = &["function_definition"];
const CS_NEST: &[&str] = &[
    "method_declaration",
    "constructor_declaration",
    "destructor_declaration",
    "local_function_statement",
    "anonymous_method_expression",
    "lambda_expression",
    "accessor_declaration",
];

const PY_CALL_SKIP: &[&str] = &["print", "super", "isinstance"];
const RS_CALL_SKIP: &[&str] = &[
    "println", "eprintln", "format", "panic", "todo", "unimplemented", "dbg", "assert",
    "assert_eq", "assert_ne", "vec", "cfg", "derive", "include", "env", "concat",
    "stringify", "write", "writeln", "info", "warn", "error", "debug", "trace",
];
const JS_CALL_SKIP: &[&str] = &[
    "console", "log", "warn", "error", "info", "debug", "require", "parseInt",
    "parseFloat", "isNaN", "setTimeout", "setInterval", "clearTimeout", "clearInterval",
];
const PY_TYPE_SKIP: &[&str] = &[
    "int", "str", "float", "bool", "bytes", "list", "dict", "set", "tuple", "None",
    "Any", "Optional", "Self", "object",
];
const RS_TYPE_SKIP: &[&str] = &[
    "bool", "char", "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32",
    "i64", "i128", "isize", "f32", "f64", "str", "String", "Self", "self",
];
const JAVA_TYPE_SKIP: &[&str] = &[
    "int", "long", "short", "byte", "char", "float", "double", "boolean", "void",
    "String", "Object",
];
const C_TYPE_SKIP: &[&str] = &[
    "int", "char", "void", "short", "long", "float", "double", "unsigned", "signed",
    "bool", "size_t", "ssize_t", "uint8_t", "uint16_t", "uint32_t", "uint64_t",
    "int8_t", "int16_t", "int32_t", "int64_t",
];
const CS_TYPE_SKIP: &[&str] = &[
    "int", "long", "short", "byte", "char", "float", "double", "bool", "void",
    "string", "object", "decimal", "var",
];

fn census_python(root: Node, src: &str, out: &mut OracleFile) {
    walk(root, &mut |n| match n.kind() {
        "function_definition" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, PY_NEST);
            }
        }
        "class_definition" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name.clone(), NodeKind::Class, PY_NEST);
                if let Some(supers) = n.child_by_field_name("superclasses") {
                    let count = supers.named_child_count();
                    for i in 0..count {
                        if let Some(b) = supers.named_child(i) {
                            if let Some(base) = leaf_identifier(b, src) {
                                push_extends(out, name.clone(), base, b);
                            }
                        }
                    }
                }
            }
        }
        "call" => {
            if let Some(f) = n.child_by_field_name("function") {
                push_call(out, n, f, src, PY_CALL_SKIP);
            }
        }
        "import_statement" | "import_from_statement" => {
            if !inside_function(n, PY_NEST) {
                let module = if n.kind() == "import_from_statement" {
                    n.child_by_field_name("module_name")
                        .map(|m| text(m, src).to_string())
                        .unwrap_or_else(|| text(n, src).to_string())
                } else {
                    n.named_child(0)
                        .map(|m| text(m, src).to_string())
                        .unwrap_or_else(|| text(n, src).to_string())
                };
                push_import(out, n, module);
            }
        }
        "type" | "typed_parameter" => {
            let ty = if n.kind() == "typed_parameter" {
                n.child_by_field_name("type")
            } else {
                Some(n)
            };
            if let Some(t) = ty {
                push_use(out, t, src, PY_TYPE_SKIP);
            }
        }
        _ => {}
    });
}

fn census_rust(root: Node, src: &str, out: &mut OracleFile) {
    walk(root, &mut |n| match n.kind() {
        "function_item" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, RS_NEST);
            }
        }
        "struct_item" | "enum_item" | "trait_item" | "type_item" | "union_item" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Class, RS_NEST);
            }
        }
        "const_item" | "static_item" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, RS_NEST);
            }
        }
        "impl_item" => {
            if let Some(trait_node) = n.child_by_field_name("trait") {
                if let Some(ty) = n.child_by_field_name("type") {
                    if let (Some(tr), Some(tn)) = (
                        leaf_identifier(trait_node, src),
                        leaf_identifier(ty, src),
                    ) {
                        push_extends(out, tn, tr, trait_node);
                    }
                }
            }
        }
        "call_expression" => {
            if let Some(f) = n.child_by_field_name("function") {
                push_call(out, n, f, src, RS_CALL_SKIP);
            }
        }
        "macro_invocation" => {
            if let Some(mac) = n.child_by_field_name("macro") {
                let name = last_ident(text(mac, src));
                if !RS_CALL_SKIP.contains(&name.as_str()) && ident_ok(&name) {
                    let (line, col) = line_col(n);
                    out.calls.push(OracleCall {
                        leaf: format!("{name}!"),
                        line,
                        col,
                    });
                }
            }
        }
        "use_declaration" => {
            if n.parent().is_some_and(|p| p.kind() == "source_file") {
                push_import(out, n, text(n, src).to_string());
            }
        }
        "type_identifier" | "scoped_type_identifier" => {
            if in_type_position(n) {
                push_use(out, n, src, RS_TYPE_SKIP);
            }
        }
        _ => {}
    });
}

fn census_go(root: Node, src: &str, out: &mut OracleFile) {
    walk(root, &mut |n| match n.kind() {
        "function_declaration" | "method_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, GO_NEST);
            }
        }
        "type_spec" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name.clone(), NodeKind::Class, GO_NEST);
                if let Some(body) = n.child_by_field_name("type") {
                    if body.kind() == "interface_type" {
                        let count = body.named_child_count();
                        for i in 0..count {
                            if let Some(c) = body.named_child(i) {
                                if let Some(base) = leaf_identifier(c, src) {
                                    if base != name {
                                        push_extends(out, name.clone(), base, c);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "call_expression" => {
            if let Some(f) = n.child_by_field_name("function") {
                push_call(out, n, f, src, &[]);
            }
        }
        "import_spec" => {
            if !inside_function(n, GO_NEST) {
                let path = n
                    .child_by_field_name("path")
                    .map(|p| text(p, src).trim_matches('"').to_string())
                    .unwrap_or_else(|| text(n, src).trim_matches('"').to_string());
                push_import(out, n, path);
            }
        }
        "type_identifier" | "qualified_type" => {
            if in_type_position(n) {
                push_use(out, n, src, &["int", "int64", "string", "bool", "byte", "error", "any"]);
            }
        }
        _ => {}
    });
}

fn census_js_like(root: Node, src: &str, out: &mut OracleFile, ts: bool) {
    walk(root, &mut |n| match n.kind() {
        "function_declaration" | "generator_function_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, JS_NEST);
            }
        }
        "class_declaration" | "abstract_class_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name.clone(), NodeKind::Class, JS_NEST);
                let count = n.named_child_count();
                for i in 0..count {
                    let Some(child) = n.named_child(i) else { continue };
                    if child.kind() == "class_heritage" {
                        let hc = child.named_child_count();
                        for j in 0..hc {
                            if let Some(h) = child.named_child(j) {
                                if let Some(base) = leaf_identifier(h, src) {
                                    push_extends(out, name.clone(), base, h);
                                }
                            }
                        }
                    }
                }
            }
        }
        "interface_declaration" | "enum_declaration" | "type_alias_declaration" if ts => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Class, JS_NEST);
            }
        }
        "method_definition" => {
            if let Some(name) = field_name(n, src).or_else(|| {
                n.child_by_field_name("name")
                    .map(|x| last_ident(text(x, src)))
                    .filter(|s| ident_ok(s) || s == "constructor")
            }) {
                push_symbol(out, n, src, name, NodeKind::Function, JS_NEST);
            }
        }
        "variable_declarator" => {
            let Some(name_n) = n.child_by_field_name("name") else { return };
            let Some(val) = n.child_by_field_name("value") else { return };
            if name_n.kind() != "identifier" {
                return;
            }
            let name = text(name_n, src).trim().to_string();
            if !ident_ok(&name) {
                return;
            }
            match val.kind() {
                "arrow_function" | "function_expression" | "generator_function" => {
                    push_symbol(out, n, src, name, NodeKind::Function, JS_NEST);
                }
                "class" => {
                    push_symbol(out, n, src, name, NodeKind::Class, JS_NEST);
                }
                _ => {}
            }
        }
        "call_expression" => {
            if let Some(f) = n.child_by_field_name("function") {
                push_call(out, n, f, src, JS_CALL_SKIP);
            }
        }
        "new_expression" => {
            if let Some(c) = n.child_by_field_name("constructor") {
                push_call(out, n, c, src, JS_CALL_SKIP);
            }
        }
        "import_statement" => {
            let module = n
                .child_by_field_name("source")
                .map(|s| text(s, src).trim_matches(|c| c == '"' || c == '\'').to_string())
                .unwrap_or_else(|| text(n, src).to_string());
            push_import(out, n, module);
        }
        "type_identifier" if ts => {
            if in_type_position(n) {
                push_use(out, n, src, &["string", "number", "boolean", "void", "any", "never", "unknown"]);
            }
        }
        _ => {}
    });
}

fn census_javascript(root: Node, src: &str, out: &mut OracleFile) {
    census_js_like(root, src, out, false);
}

fn census_typescript(root: Node, src: &str, out: &mut OracleFile) {
    census_js_like(root, src, out, true);
}

fn census_java(root: Node, src: &str, out: &mut OracleFile) {
    walk(root, &mut |n| match n.kind() {
        "class_declaration" | "interface_declaration" | "enum_declaration" | "record_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name.clone(), NodeKind::Class, JAVA_NEST);
                if let Some(sc) = n.child_by_field_name("superclass") {
                    if let Some(base) = leaf_identifier(sc, src) {
                        push_extends(out, name.clone(), base, sc);
                    }
                }
                if let Some(ifaces) = n.child_by_field_name("interfaces") {
                    let count = ifaces.named_child_count();
                    for i in 0..count {
                        if let Some(c) = ifaces.named_child(i) {
                            if let Some(base) = leaf_identifier(c, src) {
                                push_extends(out, name.clone(), base, c);
                            }
                        }
                    }
                }
            }
        }
        "method_declaration" | "constructor_declaration" | "compact_constructor_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, JAVA_NEST);
            }
        }
        "method_invocation" => {
            let name = n
                .child_by_field_name("name")
                .and_then(|x| leaf_identifier(x, src))
                .or_else(|| field_name(n, src));
            if let Some(leaf) = name {
                if ident_ok(&leaf) {
                    let (line, col) = line_col(n);
                    out.calls.push(OracleCall { leaf, line, col });
                }
            }
        }
        "object_creation_expression" => {
            if let Some(ty) = n.child_by_field_name("type") {
                if let Some(leaf) = leaf_identifier(ty, src) {
                    let (line, col) = line_col(n);
                    out.calls.push(OracleCall { leaf, line, col });
                }
            }
        }
        "import_declaration" => {
            push_import(out, n, text(n, src).replace("import ", "").replace(";", ""));
        }
        "type_identifier" => {
            if in_type_position(n) {
                push_use(out, n, src, JAVA_TYPE_SKIP);
            }
        }
        _ => {}
    });
}

fn census_c(root: Node, src: &str, out: &mut OracleFile) {
    census_c_family(root, src, out, false);
}

fn census_cpp(root: Node, src: &str, out: &mut OracleFile) {
    census_c_family(root, src, out, true);
}

fn census_c_family(root: Node, src: &str, out: &mut OracleFile, cpp: bool) {
    walk(root, &mut |n| match n.kind() {
        "function_definition" => {
            if let Some(decl) = n.child_by_field_name("declarator") {
                if let Some(name) = c_declarator_name(decl, src) {
                    push_symbol(out, n, src, name, NodeKind::Function, C_NEST);
                }
            }
        }
        "struct_specifier" | "union_specifier" | "enum_specifier" | "class_specifier" => {
            if n.child_by_field_name("body").is_none() {
                return;
            }
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name.clone(), NodeKind::Class, C_NEST);
                if cpp {
                    let count = n.named_child_count();
                    for i in 0..count {
                        let Some(child) = n.named_child(i) else { continue };
                        if child.kind() == "base_class_clause" {
                            let bc = child.named_child_count();
                            for j in 0..bc {
                                if let Some(b) = child.named_child(j) {
                                    if let Some(base) = leaf_identifier(b, src) {
                                        push_extends(out, name.clone(), base, b);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "type_definition" => {
            if let Some(decl) = n.child_by_field_name("declarator") {
                if let Some(name) = c_declarator_name(decl, src) {
                    push_symbol(out, n, src, name, NodeKind::Class, C_NEST);
                }
            }
        }
        "call_expression" => {
            if let Some(f) = n.child_by_field_name("function") {
                push_call(out, n, f, src, &[]);
            }
        }
        "preproc_include" => {
            if n.parent().is_some_and(|p| p.kind() == "translation_unit") {
                let path = n
                    .child_by_field_name("path")
                    .map(|p| text(p, src).to_string())
                    .unwrap_or_else(|| text(n, src).to_string());
                push_import(out, n, path);
            }
        }
        "type_identifier" => {
            if in_type_position(n) {
                push_use(out, n, src, C_TYPE_SKIP);
            }
        }
        _ => {}
    });
}

fn census_csharp(root: Node, src: &str, out: &mut OracleFile) {
    walk(root, &mut |n| match n.kind() {
        "class_declaration"
        | "struct_declaration"
        | "interface_declaration"
        | "enum_declaration"
        | "record_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name.clone(), NodeKind::Class, CS_NEST);
                if let Some(bases) = n.child_by_field_name("bases") {
                    let count = bases.named_child_count();
                    for i in 0..count {
                        if let Some(b) = bases.named_child(i) {
                            if let Some(base) = leaf_identifier(b, src) {
                                push_extends(out, name.clone(), base, b);
                            }
                        }
                    }
                }
            }
        }
        "method_declaration" | "constructor_declaration" | "destructor_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, CS_NEST);
            }
        }
        "property_declaration" => {
            if let Some(name) = field_name(n, src) {
                push_symbol(out, n, src, name, NodeKind::Function, CS_NEST);
            }
        }
        "invocation_expression" => {
            if let Some(f) = n.child_by_field_name("function") {
                push_call(out, n, f, src, &[]);
            }
        }
        "object_creation_expression" => {
            if let Some(ty) = n.child_by_field_name("type") {
                if let Some(leaf) = leaf_identifier(ty, src) {
                    let (line, col) = line_col(n);
                    out.calls.push(OracleCall { leaf, line, col });
                }
            }
        }
        "using_directive" => {
            push_import(out, n, text(n, src).replace("using ", "").replace(";", ""));
        }
        "identifier" => {
            if in_type_position(n) {
                if let Some(name) = leaf_identifier(n, src) {
                    if name.chars().next().is_some_and(|c| c.is_uppercase()) {
                        push_use(out, n, src, CS_TYPE_SKIP);
                    }
                }
            }
        }
        _ => {}
    });
}
