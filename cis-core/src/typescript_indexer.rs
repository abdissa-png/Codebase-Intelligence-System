//! TypeScript indexer MVP (**Phase 5**) — regex path; reuses [`CallResolver`] unchanged.

use crate::graph::{NodeKind, SourceSpan};
use crate::index_model::{
    whole_file_span, FileIndex, ImportStyle, ParsedCall, ParsedImport, ParsedSymbol,
};
use crate::language_indexer::{IndexError, LanguageIndexer};
use crate::graph::Language;

pub fn path_to_typescript_module_key(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".ts")
        .trim_end_matches(".tsx")
        .replace('/', ".")
}

pub fn index_typescript_file(path: &str, content: &str) -> Result<FileIndex, IndexError> {
    let mut idx = FileIndex::default();
    idx.symbols.push(ParsedSymbol {
        stable_key: "$file".into(),
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
            qualified_name: format!("{path}::{n}"),
            kind: NodeKind::Function,
            span: span_range(content, full.start(), full.end()),
        });
    }

    idx.imports = extract_imports_ts(content);
    idx.calls = extract_calls_ts(content, &idx.symbols);
    Ok(idx)
}

fn span_range(content: &str, start: usize, end: usize) -> SourceSpan {
    crate::index_model::span_from_byte_range(content, start, end)
}

fn extract_imports_ts(content: &str) -> Vec<ParsedImport> {
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

fn extract_calls_ts(content: &str, symbols: &[ParsedSymbol]) -> Vec<ParsedCall> {
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
}
