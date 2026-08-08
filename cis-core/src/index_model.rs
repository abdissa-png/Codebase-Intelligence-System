//! Language-neutral index types, span helpers, and stable id material for ingest.

use std::collections::{HashMap, HashSet};

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

/// Convert a file-absolute edge anchor into coordinates relative to the innermost owning
/// scope (function / class / file hub).
///
/// `scope_start_line` is the 1-based start line of that scope. Relative line `0` is the
/// first line of the scope. Column stays file-absolute (stable when lines are inserted
/// *above* the scope). Unknown/`0` scope falls back to line `1` (module top).
///
/// Absolute display spans on [`crate::graph::GraphEdge::anchor`] are unchanged — only
/// the hash input for [`edge_id_bytes`] uses these relative coordinates so edits in
/// other functions do not reshuffle edge ids.
pub(crate) fn scope_relative_edge_pos(anchor: SourceSpan, scope_start_line: u32) -> (u32, u32) {
    let scope = if scope_start_line == 0 { 1 } else { scope_start_line };
    let rel_line = anchor.start_line.saturating_sub(scope);
    (rel_line, anchor.start_col)
}

/// Stable edge id including source revision + **scope-relative** anchor
/// (Option B — restart-safe SQLite rows).
///
/// `relative_line` / `relative_col` come from [`scope_relative_edge_pos`], not from the
/// file-absolute [`SourceSpan`] stored on the edge for display.
pub(crate) fn edge_id_bytes(
    tag: &str,
    path: &str,
    src: NodeRevisionId,
    label: &str,
    relative_line: u32,
    relative_col: u32,
) -> [u8; 16] {
    let src_hex: String = src.0.iter().map(|b| format!("{:02x}", b)).collect();
    let key = format!("{src_hex}:{label}:{relative_line}:{relative_col}");
    stable_id_bytes(tag, path, &key)
}

/// Structural CAS key for identity provisional allocation — **path + stable_key**, not body text.
/// Body-identical stubs in different files must not share a CAS slot.
pub fn identity_cas_semantic_hash(path: &str, stable_key: &str) -> [u8; 32] {
    hash32_key("cas", path, stable_key)
}

/// Structural BodyStore slot for a symbol (distinct from [`NodeRevision::body_hash`] content checksum).
///
/// Callers must pass [`symbol_identity_key`] (or [`ParsedSymbol::identity_key`]), not bare
/// `stable_key`, so colliding same-name symbols do not share a slot.
pub fn body_store_slot_key(path: &str, identity_key: &str) -> [u8; 32] {
    hash32_key("bh", path, identity_key)
}

/// ASCII Record Separator — never appears in Python/TS identifiers.
const IDENTITY_KEY_SEP: char = '\u{001e}';

/// Identity / revision hash material for a symbol.
///
/// When `disambiguator` is empty (the common case), this equals `stable_key` so existing
/// graphs keep stable ids. Colliding same-name symbols (overloads, class+function) get a
/// non-empty disambiguator so each receives a distinct identity while `qualified_name`
/// stays human-readable for MCP search.
pub fn symbol_identity_key(stable_key: &str, disambiguator: &str) -> String {
    if disambiguator.is_empty() {
        stable_key.to_string()
    } else {
        format!("{stable_key}{IDENTITY_KEY_SEP}{disambiguator}")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedSymbol {
    /// Intra-file display / search name (e.g. `"Board.get_position"`, `"$file"`).
    pub(crate) stable_key: String,
    /// Empty unless [`assign_collision_disambiguators`] found a same-`stable_key` collision.
    pub(crate) disambiguator: String,
    pub(crate) qualified_name: String,
    pub(crate) kind: NodeKind,
    pub(crate) span: SourceSpan,
}

impl ParsedSymbol {
    pub(crate) fn identity_key(&self) -> String {
        symbol_identity_key(&self.stable_key, &self.disambiguator)
    }
}

/// Assign disambiguators only when multiple symbols share a `stable_key` in one file.
///
/// - Last function keeps an empty disambiguator (Python last-definition / overload impl).
/// - Earlier colliding functions get `fn:0`, `fn:1`, …
/// - Colliding classes get `class` / `class:N`.
/// - Non-colliding symbols are left untouched (backward-compatible identity keys).
pub(crate) fn assign_collision_disambiguators(idx: &mut FileIndex) {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, s) in idx.symbols.iter().enumerate() {
        if s.stable_key == "$file" {
            continue;
        }
        groups.entry(s.stable_key.clone()).or_default().push(i);
    }
    for indices in groups.into_values() {
        if indices.len() <= 1 {
            continue;
        }
        let mut classes = Vec::new();
        let mut functions = Vec::new();
        let mut others = Vec::new();
        for &i in &indices {
            match idx.symbols[i].kind {
                NodeKind::Class => classes.push(i),
                NodeKind::Function => functions.push(i),
                _ => others.push(i),
            }
        }

        if !functions.is_empty() {
            for (ci, &i) in classes.iter().enumerate() {
                idx.symbols[i].disambiguator = if ci == 0 {
                    "class".into()
                } else {
                    format!("class:{ci}")
                };
            }
            let last_fn = *functions.last().unwrap();
            for (fi, &i) in functions.iter().enumerate() {
                idx.symbols[i].disambiguator = if i == last_fn {
                    String::new()
                } else {
                    format!("fn:{fi}")
                };
            }
        } else if classes.len() > 1 {
            for (ci, &i) in classes.iter().enumerate() {
                idx.symbols[i].disambiguator = if ci == 0 {
                    String::new()
                } else {
                    format!("class:{ci}")
                };
            }
        } else if classes.len() == 1 {
            idx.symbols[classes[0]].disambiguator = "class".into();
        }

        for (oi, &i) in others.iter().enumerate() {
            idx.symbols[i].disambiguator = format!("other:{oi}");
        }
    }
}

/// After choosing which symbol keeps the empty disambiguator, tag every other symbol in
/// the collision group uniquely (functions `fn:N`, classes `class`/`class:N`, …).
fn reassign_group_around_bare(idx: &mut FileIndex, indices: &[usize], keep: usize) {
    let mut classes = Vec::new();
    let mut functions = Vec::new();
    let mut others = Vec::new();
    for &i in indices {
        match idx.symbols[i].kind {
            NodeKind::Class => classes.push(i),
            NodeKind::Function => functions.push(i),
            _ => others.push(i),
        }
    }
    for &i in indices {
        idx.symbols[i].disambiguator = String::new();
    }
    idx.symbols[keep].disambiguator = String::new();

    let mut fn_i = 0usize;
    for &i in &functions {
        if i == keep {
            continue;
        }
        idx.symbols[i].disambiguator = format!("fn:{fn_i}");
        fn_i += 1;
    }
    let mut class_i = 0usize;
    for &i in &classes {
        if i == keep {
            continue;
        }
        idx.symbols[i].disambiguator = if class_i == 0 {
            "class".into()
        } else {
            format!("class:{class_i}")
        };
        class_i += 1;
    }
    let mut other_i = 0usize;
    for &i in &others {
        if i == keep {
            continue;
        }
        idx.symbols[i].disambiguator = format!("other:{other_i}");
        other_i += 1;
    }
}

/// Reassign collision disambiguators so an existing bare identity slot keeps its kind.
///
/// Default [`assign_collision_disambiguators`] always gives the last function the empty
/// disambiguator. On re-ingest that remaps a pre-existing class at `id(path, "foo")` onto
/// a new key. This pass checks the live graph: if the bare slot is already occupied by a
/// different kind, that occupant keeps `""` and remaining symbols are re-tagged uniquely
/// (so a three-way class+two-fn collision never produces two `fn:0` keys).
pub(crate) fn stabilize_disambiguators(
    idx: &mut FileIndex,
    path: &str,
    branch: BranchId,
    graph: &crate::graph::InMemoryGraph,
) {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, s) in idx.symbols.iter().enumerate() {
        if s.stable_key == "$file" {
            continue;
        }
        groups.entry(s.stable_key.clone()).or_default().push(i);
    }
    for indices in groups.into_values() {
        if indices.len() <= 1 {
            continue;
        }
        let Some(canon) = indices
            .iter()
            .copied()
            .find(|&i| idx.symbols[i].disambiguator.is_empty())
        else {
            continue;
        };
        let bare_key = idx.symbols[canon].stable_key.clone();
        let bare_iid = cis_wal::IdentityId(stable_id_bytes("id", path, &bare_key));
        let Some(prev) = graph.primary_revision_for_identity(branch, bare_iid) else {
            continue;
        };
        if !matches!(
            prev.status,
            crate::graph::RevisionStatus::Active | crate::graph::RevisionStatus::Speculative
        ) {
            continue;
        }
        let Some(old_kind) = graph.identity_kind(bare_iid) else {
            continue;
        };
        if old_kind == idx.symbols[canon].kind {
            continue;
        }
        let Some(keep) = indices
            .iter()
            .copied()
            .find(|&i| i != canon && idx.symbols[i].kind == old_kind)
        else {
            continue;
        };
        reassign_group_around_bare(idx, &indices, keep);
    }
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

#[cfg(feature = "ts-runtime")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::NodeKind;
    use crate::graph::SourceSpan;

    fn span() -> SourceSpan {
        SourceSpan {
            start_line: 1,
            start_col: 1,
            end_line: 1,
            end_col: 1,
        }
    }

    #[test]
    fn scope_relative_edge_pos_is_zero_on_scope_start_line() {
        let anchor = SourceSpan {
            start_line: 10,
            start_col: 4,
            end_line: 10,
            end_col: 8,
        };
        assert_eq!(scope_relative_edge_pos(anchor, 10), (0, 4));
        assert_eq!(scope_relative_edge_pos(anchor, 8), (2, 4));
    }

    #[test]
    fn scope_relative_edge_pos_unknown_scope_defaults_to_line_one() {
        let anchor = SourceSpan {
            start_line: 5,
            start_col: 1,
            end_line: 5,
            end_col: 2,
        };
        assert_eq!(scope_relative_edge_pos(anchor, 0), (4, 1));
    }

    #[test]
    fn edge_id_bytes_uses_relative_coordinates() {
        let src = NodeRevisionId([1u8; 16]);
        let a = edge_id_bytes("cal", "a.py", src, "f->g", 2, 4);
        let b = edge_id_bytes("cal", "a.py", src, "f->g", 2, 4);
        let c = edge_id_bytes("cal", "a.py", src, "f->g", 3, 4);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn symbol_identity_key_empty_disambiguator_is_stable_key() {
        assert_eq!(symbol_identity_key("foo", ""), "foo");
        assert_eq!(symbol_identity_key("Board.get", ""), "Board.get");
    }

    #[test]
    fn symbol_identity_key_joins_with_record_separator() {
        let k = symbol_identity_key("foo", "class");
        assert!(k.starts_with("foo"));
        assert!(k.contains('\u{001e}'));
        assert!(k.ends_with("class"));
        assert_ne!(k, "foo");
    }

    #[test]
    fn assign_disambiguators_leaves_unique_names_alone() {
        let mut idx = FileIndex::default();
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: "m.py::foo".into(),
            kind: NodeKind::Function,
            span: span(),
        });
        idx.symbols.push(ParsedSymbol {
            stable_key: "bar".into(),
            disambiguator: String::new(),
            qualified_name: "m.py::bar".into(),
            kind: NodeKind::Class,
            span: span(),
        });
        assign_collision_disambiguators(&mut idx);
        assert!(idx.symbols.iter().all(|s| s.disambiguator.is_empty()));
    }

    #[test]
    fn assign_disambiguators_class_and_function_same_name() {
        let mut idx = FileIndex::default();
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: "m.py::foo".into(),
            kind: NodeKind::Class,
            span: span(),
        });
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: "m.py::foo".into(),
            kind: NodeKind::Function,
            span: span(),
        });
        assign_collision_disambiguators(&mut idx);
        let class = idx.symbols.iter().find(|s| s.kind == NodeKind::Class).unwrap();
        let func = idx.symbols.iter().find(|s| s.kind == NodeKind::Function).unwrap();
        assert_eq!(class.disambiguator, "class");
        assert_eq!(func.disambiguator, "");
        assert_ne!(class.identity_key(), func.identity_key());
        assert_eq!(class.qualified_name, func.qualified_name);
    }

    #[test]
    fn assign_disambiguators_overload_style_last_fn_canonical() {
        let mut idx = FileIndex::default();
        for _ in 0..3 {
            idx.symbols.push(ParsedSymbol {
                stable_key: "foo".into(),
                disambiguator: String::new(),
                qualified_name: "m.py::foo".into(),
                kind: NodeKind::Function,
                span: span(),
            });
        }
        assign_collision_disambiguators(&mut idx);
        let keys: Vec<_> = idx
            .symbols
            .iter()
            .map(|s| s.disambiguator.as_str())
            .collect();
        assert_eq!(keys, vec!["fn:0", "fn:1", ""]);
        let ids: HashSet<_> = idx.symbols.iter().map(|s| s.identity_key()).collect();
        assert_eq!(ids.len(), 3);
        assert!(idx.symbols.iter().all(|s| s.qualified_name == "m.py::foo"));
    }

    #[test]
    fn stabilize_keeps_existing_class_on_bare_slot() {
        use cis_wal::{BranchId, IdentityId, NodeRevisionId};

        use crate::graph::{
            InMemoryGraph, Language, NodeIdentity, NodeRevision, RevisionStatus,
        };

        let path = "m.py";
        let branch = BranchId([0u8; 16]);
        let bare_iid = IdentityId(stable_id_bytes("id", path, "foo"));
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: bare_iid,
            kind: NodeKind::Class,
        });
        let rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "foo"));
        g.put_revision(NodeRevision {
            revision_id: rid,
            identity_id: bare_iid,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: format!("{path}::foo"),
            file_path: path.into(),
            body_hash: [1u8; 32],
            signature_hash: [1u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: span(),
            tombstoned_at_ms: None,
        });

        let mut idx = FileIndex::default();
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::foo"),
            kind: NodeKind::Class,
            span: span(),
        });
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::foo"),
            kind: NodeKind::Function,
            span: span(),
        });
        assign_collision_disambiguators(&mut idx);
        assert_eq!(
            idx.symbols
                .iter()
                .find(|s| s.kind == NodeKind::Function)
                .unwrap()
                .disambiguator,
            ""
        );
        stabilize_disambiguators(&mut idx, path, branch, &g);
        let class = idx.symbols.iter().find(|s| s.kind == NodeKind::Class).unwrap();
        let func = idx.symbols.iter().find(|s| s.kind == NodeKind::Function).unwrap();
        assert_eq!(class.disambiguator, "");
        assert_eq!(func.disambiguator, "fn:0");
        assert_eq!(class.identity_key(), "foo");
        assert_eq!(func.identity_key(), symbol_identity_key("foo", "fn:0"));
    }

    #[test]
    fn stabilize_noop_when_bare_slot_already_matches_function() {
        use cis_wal::{BranchId, IdentityId, NodeRevisionId};

        use crate::graph::{
            InMemoryGraph, Language, NodeIdentity, NodeRevision, RevisionStatus,
        };

        let path = "m.py";
        let branch = BranchId([0u8; 16]);
        let bare_iid = IdentityId(stable_id_bytes("id", path, "foo"));
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: bare_iid,
            kind: NodeKind::Function,
        });
        let rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "foo"));
        g.put_revision(NodeRevision {
            revision_id: rid,
            identity_id: bare_iid,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: format!("{path}::foo"),
            file_path: path.into(),
            body_hash: [1u8; 32],
            signature_hash: [1u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: span(),
            tombstoned_at_ms: None,
        });

        let mut idx = FileIndex::default();
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::foo"),
            kind: NodeKind::Class,
            span: span(),
        });
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::foo"),
            kind: NodeKind::Function,
            span: span(),
        });
        assign_collision_disambiguators(&mut idx);
        stabilize_disambiguators(&mut idx, path, branch, &g);
        let class = idx.symbols.iter().find(|s| s.kind == NodeKind::Class).unwrap();
        let func = idx.symbols.iter().find(|s| s.kind == NodeKind::Function).unwrap();
        // Function already owns bare slot — leave default assignment alone.
        assert_eq!(func.disambiguator, "");
        assert_eq!(class.disambiguator, "class");
    }

    #[test]
    fn stabilize_three_way_class_plus_two_fns_unique_keys() {
        use cis_wal::{BranchId, IdentityId, NodeRevisionId};

        use crate::graph::{
            InMemoryGraph, Language, NodeIdentity, NodeRevision, RevisionStatus,
        };

        let path = "m.py";
        let branch = BranchId([0u8; 16]);
        let bare_iid = IdentityId(stable_id_bytes("id", path, "foo"));
        let mut g = InMemoryGraph::default();
        g.put_identity(NodeIdentity {
            identity_id: bare_iid,
            kind: NodeKind::Class,
        });
        let rid = NodeRevisionId(stable_rev_id_bytes(branch, path, "foo"));
        g.put_revision(NodeRevision {
            revision_id: rid,
            identity_id: bare_iid,
            branch_id: branch,
            status: RevisionStatus::Active,
            qualified_name: format!("{path}::foo"),
            file_path: path.into(),
            body_hash: [1u8; 32],
            signature_hash: [1u8; 32],
            language: Language::Python,
            parent_revision_id: None,
            rename_source_id: None,
            span: span(),
            tombstoned_at_ms: None,
        });

        let mut idx = FileIndex::default();
        idx.symbols.push(ParsedSymbol {
            stable_key: "foo".into(),
            disambiguator: String::new(),
            qualified_name: format!("{path}::foo"),
            kind: NodeKind::Class,
            span: span(),
        });
        for _ in 0..2 {
            idx.symbols.push(ParsedSymbol {
                stable_key: "foo".into(),
                disambiguator: String::new(),
                qualified_name: format!("{path}::foo"),
                kind: NodeKind::Function,
                span: span(),
            });
        }
        assign_collision_disambiguators(&mut idx);
        // Default: last fn "", earlier fn "fn:0", class "class".
        assert_eq!(
            idx.symbols
                .iter()
                .find(|s| s.kind == NodeKind::Class)
                .unwrap()
                .disambiguator,
            "class"
        );
        stabilize_disambiguators(&mut idx, path, branch, &g);

        let class = idx.symbols.iter().find(|s| s.kind == NodeKind::Class).unwrap();
        assert_eq!(class.disambiguator, "");
        assert_eq!(class.identity_key(), "foo");

        let fn_tags: Vec<_> = idx
            .symbols
            .iter()
            .filter(|s| s.kind == NodeKind::Function)
            .map(|s| s.disambiguator.as_str())
            .collect();
        assert_eq!(fn_tags.len(), 2);
        assert!(fn_tags.iter().all(|t| t.starts_with("fn:")));
        assert_ne!(fn_tags[0], fn_tags[1], "functions must not share a tag");

        let ids: HashSet<_> = idx.symbols.iter().map(|s| s.identity_key()).collect();
        assert_eq!(ids.len(), 3, "three-way collision must yield three identity keys");
    }

    #[test]
    fn body_store_slot_key_distinct_for_disambiguated_symbols() {
        let a = body_store_slot_key("m.py", &symbol_identity_key("foo", ""));
        let b = body_store_slot_key("m.py", &symbol_identity_key("foo", "fn:0"));
        let c = body_store_slot_key("m.py", &symbol_identity_key("foo", "class"));
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }
}
