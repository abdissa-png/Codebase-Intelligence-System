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

/// Default indexer set for bootstrap (Python + TypeScript MVP).
pub fn default_indexers() -> Vec<Box<dyn LanguageIndexer>> {
    vec![
        Box::new(PythonIndexer),
        Box::new(crate::typescript_indexer::TypeScriptIndexer),
        Box::new(crate::typescript_indexer::TypeScriptTsxIndexer),
    ]
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
