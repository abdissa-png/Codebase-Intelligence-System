//! Language-neutral ingest plugin boundary (**ADR 0004**).

use crate::graph::Language;
use crate::index_model::FileIndex;

/// Parse a source file into a language-neutral [`FileIndex`].
pub trait LanguageIndexer: Send + Sync {
    fn language(&self) -> Language;
    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError>;
    fn module_key(&self, rel_path: &str) -> String;
    fn file_extension(&self) -> &'static str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexError {
    ParseFailed,
    Unsupported,
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseFailed => write!(f, "index parse failed"),
            Self::Unsupported => write!(f, "unsupported for this indexer"),
        }
    }
}

impl std::error::Error for IndexError {}

/// Default Python indexer (tree-sitter when enabled, else regex).
pub struct PythonIndexer;

impl LanguageIndexer for PythonIndexer {
    fn language(&self) -> Language {
        Language::Python
    }

    fn index_file(&self, path: &str, content: &str) -> Result<FileIndex, IndexError> {
        crate::python_indexer::index_python_file(path, content).map_err(|_| IndexError::ParseFailed)
    }

    fn module_key(&self, rel_path: &str) -> String {
        crate::python_indexer::path_to_python_module_key(rel_path)
    }

    fn file_extension(&self) -> &'static str {
        "py"
    }
}

/// Default indexer set for bootstrap — Python + TypeScript/TSX always; other languages
/// when their `ts-*` Cargo features are enabled (`tree-sitter-all` enables all).
pub fn default_indexers() -> Vec<Box<dyn LanguageIndexer>> {
    let mut v: Vec<Box<dyn LanguageIndexer>> = vec![
        Box::new(PythonIndexer),
        Box::new(crate::typescript_indexer::TypeScriptIndexer),
        Box::new(crate::typescript_indexer::TypeScriptTsxIndexer),
    ];
    #[cfg(feature = "ts-rust")]
    v.push(Box::new(crate::rust_indexer::RustIndexer));
    #[cfg(feature = "ts-go")]
    v.push(Box::new(crate::go_indexer::GoIndexer));
    #[cfg(feature = "ts-javascript")]
    {
        v.push(Box::new(crate::javascript_indexer::JavaScriptIndexer));
        v.push(Box::new(crate::javascript_indexer::JsxIndexer));
    }
    #[cfg(feature = "ts-java")]
    v.push(Box::new(crate::java_indexer::JavaIndexer));
    #[cfg(feature = "ts-c")]
    {
        v.push(Box::new(crate::c_indexer::CIndexer));
        v.push(Box::new(crate::c_indexer::CHeaderIndexer));
    }
    #[cfg(feature = "ts-cpp")]
    {
        v.push(Box::new(crate::cpp_indexer::CppIndexer));
        v.push(Box::new(crate::cpp_indexer::CppCxxIndexer));
        v.push(Box::new(crate::cpp_indexer::CppCcIndexer));
        v.push(Box::new(crate::cpp_indexer::CppHeaderIndexer));
        v.push(Box::new(crate::cpp_indexer::CppHxxIndexer));
        v.push(Box::new(crate::cpp_indexer::CppHhIndexer));
    }
    #[cfg(feature = "ts-csharp")]
    v.push(Box::new(crate::csharp_indexer::CSharpIndexer));
    v
}

/// Pick indexer by file path extension.
pub fn indexer_for_path<'a>(
    path: &str,
    indexers: &'a [Box<dyn LanguageIndexer>],
) -> Option<&'a dyn LanguageIndexer> {
    let ext = path.rsplit('.').next()?;
    indexers
        .iter()
        .find(|i| i.file_extension() == ext)
        .map(|b| b.as_ref())
}

/// True when `path` has an extension registered in [`default_indexers`] and is not a
/// generated/minified artifact under a builtin skip directory.
pub fn path_is_indexable(path: &str) -> bool {
    if crate::index_walk::path_has_builtin_skip_dir(path) {
        return false;
    }
    let file_name = path.rsplit(['/', '\\']).next().unwrap_or(path);
    if crate::index_walk::is_skipped_source_filename(file_name) {
        return false;
    }
    indexer_for_path(path, &default_indexers()).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_is_indexable_accepts_registered_sources() {
        assert!(path_is_indexable("src/main.py"));
        assert!(path_is_indexable("app.ts"));
        assert!(path_is_indexable("App.tsx"));
        #[cfg(feature = "ts-rust")]
        assert!(path_is_indexable("src/lib.rs"));
        #[cfg(feature = "ts-go")]
        assert!(path_is_indexable("pkg/main.go"));
        #[cfg(feature = "ts-javascript")]
        {
            assert!(path_is_indexable("src/index.js"));
            assert!(path_is_indexable("src/Widget.jsx"));
        }
        #[cfg(feature = "ts-java")]
        assert!(path_is_indexable("com/Example.java"));
        #[cfg(feature = "ts-c")]
        {
            assert!(path_is_indexable("src/main.c"));
            assert!(path_is_indexable("include/board.h"));
        }
        #[cfg(feature = "ts-cpp")]
        {
            assert!(path_is_indexable("src/board.cpp"));
            assert!(path_is_indexable("src/board.cc"));
            assert!(path_is_indexable("src/board.cxx"));
            assert!(path_is_indexable("include/board.hpp"));
            assert!(path_is_indexable("include/board.hh"));
        }
        #[cfg(feature = "ts-csharp")]
        assert!(path_is_indexable("Program.cs"));
    }

    #[test]
    fn path_is_indexable_rejects_build_artifacts() {
        assert!(!path_is_indexable("target/debug/lib.rs"));
        assert!(!path_is_indexable("node_modules/pkg/index.js"));
        assert!(!path_is_indexable("vendor/mod/x.go"));
        assert!(!path_is_indexable("src/bundle.min.js"));
        assert!(!path_is_indexable("readme.md"));
        assert!(!path_is_indexable("__pycache__/x.py"));
    }
}
