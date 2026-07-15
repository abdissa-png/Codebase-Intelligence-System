//! Language-neutral index types, span helpers, and stable id material for ingest.

use std::collections::HashMap;

use cis_wal::{BranchId, NodeRevisionId};

use crate::graph::{NodeKind, SourceSpan};

/// Stable 128-bit id for deterministic ingest identities (**§01.6** provisional-style key material).
pub fn stable_id_bytes(tag: &str, a: &str, b: &str) -> [u8; 16] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    tag.hash(&mut h);
    a.hash(&mut h);
    b.hash(&mut h);
    let x = h.finish();
    let mut h2 = DefaultHasher::new();
    13u8.hash(&mut h2);
    tag.hash(&mut h2);
    a.hash(&mut h2);
    b.hash(&mut h2);
    let y = h2.finish();
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&x.to_le_bytes());
    out[8..16].copy_from_slice(&y.to_le_bytes());
    out
}

pub(crate) fn hash32_key(tag: &str, a: &str, b: &str) -> [u8; 32] {
    let x = stable_id_bytes(tag, a, b);
    let y = stable_id_bytes(&(tag.to_owned() + "2"), a, b);
    let mut o = [0u8; 32];
    o[..16].copy_from_slice(&x);
    o[16..].copy_from_slice(&y);
    o
}

/// Content checksum for merge diffing and content-addressed body storage.
pub fn content_checksum_32(body: &str) -> [u8; 32] {
    hash32_key("bodychk", body, "")
}

/// Hex tag for a [`BranchId`] (used in branch-scoped revision keys).
pub fn branch_id_tag(branch: BranchId) -> String {
    branch.0.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Stable revision id per `(branch, path, symbol)` so branches do not stomp one graph slot.
pub fn stable_rev_id_bytes(branch: BranchId, path: &str, stable_key: &str) -> [u8; 16] {
    let tag = branch_id_tag(branch);
    stable_id_bytes("rev", &tag, &format!("{path}\x1f{stable_key}"))
}

/// Content-addressed revision id for append-only symbol history (distinct body per slot).
pub fn content_rev_id_bytes(
    branch: BranchId,
    path: &str,
    stable_key: &str,
    body_hash: [u8; 32],
) -> [u8; 16] {
    let hex: String = body_hash.iter().map(|b| format!("{:02x}", b)).collect();
    let tag = branch_id_tag(branch);
    stable_id_bytes("revgen", &tag, &format!("{path}\x1f{stable_key}\x1f{hex}"))
}

/// Stable edge id including source revision + anchor (Option B — restart-safe SQLite rows).
pub(crate) fn edge_id_bytes(
    tag: &str,
    path: &str,
    src: NodeRevisionId,
    label: &str,
    anchor: SourceSpan,
) -> [u8; 16] {
    let src_hex: String = src.0.iter().map(|b| format!("{:02x}", b)).collect();
    let key = format!("{src_hex}:{label}:{}:{}", anchor.start_line, anchor.start_col);
    stable_id_bytes(tag, path, &key)
}

/// Structural CAS key for identity provisional allocation — **path + stable_key**, not body text.
/// Body-identical stubs in different files must not share a CAS slot.
pub fn identity_cas_semantic_hash(path: &str, stable_key: &str) -> [u8; 32] {
    hash32_key("cas", path, stable_key)
}

/// Structural BodyStore slot for a symbol (distinct from [`NodeRevision::body_hash`] content checksum).
pub fn body_store_slot_key(path: &str, stable_key: &str) -> [u8; 32] {
    hash32_key("bh", path, stable_key)
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedSymbol {
    pub(crate) stable_key: String,
    pub(crate) qualified_name: String,
    pub(crate) kind: NodeKind,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportStyle {
    /// `import module` — no bare-name re-exports into scope.
    ModuleOnly,
    /// `from module import *`
    Star,
    /// `from module import a, b, …`
    Names,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedImport {
    pub(crate) module: String,
    pub(crate) style: ImportStyle,
    /// Set when `style == Names` (simple names only).
    pub(crate) names: Vec<String>,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallReceiver {
    Bare(String),
    Attr {
        object: Box<CallReceiver>,
        name: String,
    },
}

impl CallReceiver {
    pub(crate) fn bare(name: impl Into<String>) -> Self {
        Self::Bare(name.into())
    }

    pub(crate) fn label(&self) -> String {
        match self {
            Self::Bare(n) => n.clone(),
            Self::Attr { object, name } => format!("{}.{name}", object.label()),
        }
    }

    pub(crate) fn leaf_name(&self) -> &str {
        match self {
            Self::Bare(n) => n.as_str(),
            Self::Attr { name, .. } => name.as_str(),
        }
    }

    pub(crate) fn root_bare_name(&self) -> Option<&str> {
        match self {
            Self::Bare(n) => Some(n.as_str()),
            Self::Attr { object, .. } => object.root_bare_name(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedCall {
    pub(crate) caller_stable_key: String,
    pub(crate) callee: CallReceiver,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedExtends {
    pub(crate) class_stable_key: String,
    pub(crate) base_name: String,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedUse {
    pub(crate) owner_stable_key: String,
    pub(crate) type_name: String,
    pub(crate) span: SourceSpan,
}

/// Local import name → (target file path, symbol name in that module).
#[derive(Debug, Clone)]
pub(crate) struct ImportBinding {
    pub(crate) file_path: String,
    pub(crate) remote_name: String,
}

#[derive(Debug, Default, Clone)]
pub struct FileIndex {
    pub(crate) symbols: Vec<ParsedSymbol>,
    pub(crate) imports: Vec<ParsedImport>,
    pub(crate) calls: Vec<ParsedCall>,
    pub(crate) extends: Vec<ParsedExtends>,
    pub(crate) uses: Vec<ParsedUse>,
    /// `class_stable_key` → `field_name` → inferred type simple name (e.g. `Board`).
    pub(crate) instance_fields: HashMap<String, HashMap<String, String>>,
    /// Module-level list variable → element type (e.g. `TILES` → `ChessTile`).
    pub(crate) list_element_types: HashMap<String, String>,
    /// `function_stable_key` → local/loop variable → type simple name.
    pub(crate) function_locals: HashMap<String, HashMap<String, String>>,
}

pub(crate) fn byte_offset_to_point(content: &str, byte: usize) -> (u32, u32) {
    let mut line = 1u32;
    let mut col = 1u32;
    for (i, ch) in content.char_indices() {
        if i >= byte {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

pub(crate) fn span_from_byte_range(content: &str, start_byte: usize, end_byte: usize) -> SourceSpan {
    let (start_line, start_col) = byte_offset_to_point(content, start_byte);
    let (end_line, end_col) = byte_offset_to_point(content, end_byte.saturating_sub(1));
    SourceSpan {
        start_line,
        start_col,
        end_line,
        end_col,
    }
}

pub(crate) fn whole_file_span(content: &str) -> SourceSpan {
    let line_count = content.lines().count().max(1) as u32;
    let last_col = content
        .lines()
        .last()
        .map(|l| l.chars().count().max(1) as u32)
        .unwrap_or(1);
    SourceSpan {
        start_line: 1,
        start_col: 1,
        end_line: line_count,
        end_col: last_col,
    }
}

#[cfg(feature = "tree-sitter")]
pub(crate) fn span_from_tree_sitter_node(node: tree_sitter::Node) -> SourceSpan {
    let start = node.start_position();
    let end = node.end_position();
    SourceSpan {
        start_line: start.row as u32 + 1,
        start_col: start.column as u32 + 1,
        end_line: end.row as u32 + 1,
        end_col: end.column as u32 + 1,
    }
}
