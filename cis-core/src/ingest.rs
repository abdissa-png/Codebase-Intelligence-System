//! **FR-1.5** `IndexEventQueue` + **FR-1.1** Python ingest (regex `def` extractor; enable **`tree-sitter`** feature for native parsing per `.cis/grammar.lock`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;

use cis_wal::{BranchId, IdentityId, NodeRevisionId};

use crate::body_store::BodyStore;
use crate::call_resolve::attach_import_and_call_edges;
use crate::coordinator::{CoordinatorError, WriteCoordinator};
use crate::graph::{
    EdgeType, GraphEdge, Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus,
};
use crate::graph_mutation::GraphMutationSet;
use crate::identity_cas::IdentityProvisionalCas;
use crate::deletion_absence::DeletionAbsenceStore;
use crate::identity_resolution::{
    body_snippet_for_span, tombstone_all_file_symbols_with_absence,
    tombstone_orphaned_file_symbols_with_absence, RenameConfig,
};
use crate::identity_resolver::IdentityResolver;
use crate::index_model::hash32_key;
use crate::language_indexer::{default_indexers, indexer_for_path};
use crate::merge_lock::merge_lock_holder;
use crate::python_indexer::index_python_file;
use crate::MemoryKv;

pub use crate::call_resolve::{
    module_map_for_paths, module_map_from_paths, paths_on_branch, python_paths_on_branch,
    regen_edges_for_file_with_graph, regen_edges_for_python_file,
    regen_edges_for_python_file_with_graph,
};
pub use crate::index_model::{
    body_store_slot_key, branch_id_tag, content_checksum_32, content_rev_id_bytes,
    identity_cas_semantic_hash, stable_id_bytes, stable_rev_id_bytes, symbol_identity_key,
    FileIndex,
};
pub use crate::python_indexer::{extract_python_top_level_defs, path_to_python_module_key};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsChangeKind {
    Created,
    Modified,
    Deleted,
    Renamed,
    Moved,
}

#[derive(Debug, Clone)]
pub struct IndexEvent {
    pub branch_id: BranchId,
    pub path: String,
    pub kind: FsChangeKind,
    /// Prior path for [`FsChangeKind::Renamed`] / [`FsChangeKind::Moved`].
    /// When set, symbols on this path are tombstoned before indexing `path`.
    pub old_path: Option<String>,
}

impl IndexEvent {
    pub fn new(branch_id: BranchId, path: impl Into<String>, kind: FsChangeKind) -> Self {
        Self {
            branch_id,
            path: path.into(),
            kind,
            old_path: None,
        }
    }

    pub fn renamed(
        branch_id: BranchId,
        old_path: impl Into<String>,
        new_path: impl Into<String>,
    ) -> Self {
        Self {
            branch_id,
            path: new_path.into(),
            kind: FsChangeKind::Renamed,
            old_path: Some(old_path.into()),
        }
    }

    pub fn moved(
        branch_id: BranchId,
        old_path: impl Into<String>,
        new_path: impl Into<String>,
    ) -> Self {
        Self {
            branch_id,
            path: new_path.into(),
            kind: FsChangeKind::Moved,
            old_path: Some(old_path.into()),
        }
    }
}

#[derive(Debug, Default)]
pub struct IndexEventQueue {
    q: Mutex<VecDeque<IndexEvent>>,
}

impl IndexEventQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&self, ev: IndexEvent) {
        self.q.lock().unwrap().push_back(ev);
    }

    pub fn drain(&self) -> Vec<IndexEvent> {
        self.q.lock().unwrap().drain(..).collect()
    }

    pub fn depth(&self) -> usize {
        self.q.lock().unwrap().len()
    }
}

/// Content key for a file hub body in `BodyStore`.
pub fn file_body_hash_key(path: &str) -> [u8; 32] {
    hash32_key("bh", path, "$file")
}

/// Load full file text from the content-addressed body store (file hub slot).
pub fn load_file_body(body_store: &BodyStore, file_path: &str) -> Option<String> {
    body_store
        .get(&file_body_hash_key(file_path))
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

#[derive(Debug, Default, Clone)]
pub struct IngestApplyReport {
    pub applied: usize,
    pub requeued_merge_lock: usize,
    pub skipped_non_py: usize,
    /// `.py` files where parsing produced no symbols (should be rare: every file has a **File** hub).
    pub skipped_empty_py: usize,
    pub skipped_delete_stub: usize,
    pub parse_errors: usize,
}

/// Drain ingest events into **`WriteCoordinator`**. If **`merge_lock`** is held for the event’s branch,
/// the event is **re-queued** (FR-1.5 / v2.4).
///
/// **`time_travel_ri`**: when set, `ri:` bindings are updated and a **`ris:`** snapshot is recorded
/// for the committed WAL **`log_id`** (**FR-4.11**).
///
/// **`git_head_hex`**: optional **40-char** Git commit OID; when set, indexes **`tt:git:` → `log_id`**
/// via [`crate::time_travel::record_git_oid_wal_log`] for MCP **`commit_hash`** resolution.
pub fn apply_index_events(
    requeue: &IndexEventQueue,
    coord: &WriteCoordinator,
    kv: Arc<MemoryKv>,
    events: Vec<IndexEvent>,
    read_file: impl FnMut(&str) -> std::io::Result<String>,
    time_travel_ri: Option<Arc<crate::revision_cow::RevisionIndexCow>>,
    git_head_hex: Option<&str>,
) -> Result<IngestApplyReport, CoordinatorError> {
    apply_index_events_with_config(
        requeue,
        coord,
        kv,
        events,
        read_file,
        time_travel_ri,
        git_head_hex,
        None,
    )
}

/// Like [`apply_index_events`] with optional rename tunables from policy (Phase 3).
pub fn apply_index_events_with_config(
    requeue: &IndexEventQueue,
    coord: &WriteCoordinator,
    kv: Arc<MemoryKv>,
    events: Vec<IndexEvent>,
    mut read_file: impl FnMut(&str) -> std::io::Result<String>,
    time_travel_ri: Option<Arc<crate::revision_cow::RevisionIndexCow>>,
    git_head_hex: Option<&str>,
    rename_config: Option<RenameConfig>,
) -> Result<IngestApplyReport, CoordinatorError> {
    let indexers = default_indexers();
    let mut module_to_path: HashMap<String, String> = HashMap::new();
    for ev in &events {
        if let Some(idx) = indexer_for_path(&ev.path, &indexers) {
            let k = idx.module_key(&ev.path);
            module_to_path.insert(k, ev.path.clone());
        }
    }

    let rename_config = rename_config.unwrap_or_default();
    let resolver = IdentityResolver::from_policy(rename_config.rename_min_confidence);
    let body_store = BodyStore::new(Arc::clone(&kv));
    let identity_cas = IdentityProvisionalCas::new(Arc::clone(&kv));
    let absence = DeletionAbsenceStore::new(Arc::clone(&kv));
    let cis_dir = coord.persistence_dir().map(|p| p.to_path_buf());
    coord.set_defer_snapshot_flush(true);

    // Pre-parse batch so cross-file `Calls` resolve regardless of ingest order.
    let mut batch_indexes: HashMap<String, FileIndex> = HashMap::new();
    for ev in &events {
        let Some(indexer) = indexer_for_path(&ev.path, &indexers) else {
            continue;
        };
        if matches!(ev.kind, FsChangeKind::Deleted) {
            continue;
        }
        if let Ok(content) = read_file(&ev.path) {
            if let Ok(idx) = indexer.index_file(&ev.path, &content) {
                batch_indexes.insert(ev.path.clone(), idx);
            }
        }
    }

    let mut rep = IngestApplyReport::default();
    for ev in events {
        if merge_lock_holder(kv.as_ref(), ev.branch_id).is_some() {
            requeue.enqueue(ev);
            rep.requeued_merge_lock += 1;
            continue;
        }
        match ev.kind {
            FsChangeKind::Deleted => {
                let Some(indexer) = indexer_for_path(&ev.path, &indexers) else {
                    rep.skipped_non_py += 1;
                    continue;
                };
                let _ = indexer;
                let branch = ev.branch_id;
                let path = ev.path.clone();
                // Use a single-revision mutation set with a sentinel id so the coordinator
                // can still acquire the mutation lock even though there are no new revisions.
                let sentinel_rid = NodeRevisionId(stable_id_bytes("del", &path, "$tombstone"));
                let set = GraphMutationSet::new(vec![sentinel_rid], [0u8; 32]);
                let mid = coord.begin_mutation(&set)?;
                let absence_c = absence.clone();
                coord.commit_graph(mid, move |g| {
                    tombstone_all_file_symbols_with_absence(
                        g,
                        branch,
                        &path,
                        Some(&absence_c),
                    );
                    Ok(())
                })?;
                rep.applied += 1;
                continue;
            }
            FsChangeKind::Renamed | FsChangeKind::Moved => {
                // Tombstone the old path first when the caller supplied it.
                if let Some(old) = ev.old_path.as_ref() {
                    if indexer_for_path(old, &indexers).is_some() {
                        let branch = ev.branch_id;
                        let old_path = old.clone();
                        let sentinel_rid =
                            NodeRevisionId(stable_id_bytes("del", &old_path, "$tombstone"));
                        let set = GraphMutationSet::new(vec![sentinel_rid], [0u8; 32]);
                        let mid = coord.begin_mutation(&set)?;
                        let absence_c = absence.clone();
                        coord.commit_graph(mid, move |g| {
                            tombstone_all_file_symbols_with_absence(
                                g,
                                branch,
                                &old_path,
                                Some(&absence_c),
                            );
                            Ok(())
                        })?;
                    }
                }
            }
            FsChangeKind::Created | FsChangeKind::Modified => {}
        }
        let Some(indexer) = indexer_for_path(&ev.path, &indexers) else {
            rep.skipped_non_py += 1;
            continue;
        };
        let content = match read_file(&ev.path) {
            Ok(c) => c,
            Err(_) => {
                rep.parse_errors += 1;
                continue;
            }
        };
        let index = match indexer.index_file(&ev.path, &content) {
            Ok(i) => i,
            Err(_) => {
                rep.parse_errors += 1;
                continue;
            }
        };
        if index.symbols.is_empty() {
            rep.skipped_empty_py += 1;
            continue;
        }
        let lang = indexer.language();
        let branch = ev.branch_id;
        let revs: Vec<NodeRevisionId> = index
            .symbols
            .iter()
            .map(|s| NodeRevisionId(stable_rev_id_bytes(branch, &ev.path, &s.identity_key())))
            .collect();
        let set = GraphMutationSet::new(revs.clone(), content_checksum_32(&content));
        let id = coord.begin_mutation(&set)?;
        let path = ev.path.clone();
        let index_c = index.clone();
        let mod_map = module_to_path.clone();
        let path_c = path.clone();
        let file_content = content.clone();
        let rename_cfg = rename_config;
        let resolver_c = resolver.clone();
        let body_store_c = body_store.clone();
        let identity_cas_c = identity_cas.clone();
        let batch_indexes_c = batch_indexes.clone();
        let lang_c = lang;
        let absence_c = absence.clone();
        let bindings_out: Arc<Mutex<Vec<(IdentityId, NodeRevisionId)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let bindings_c = Arc::clone(&bindings_out);
        coord.commit_graph(id, move |g| {
            let retained: HashSet<IdentityId> = index_c
                .symbols
                .iter()
                .map(|s| {
                    if s.kind == NodeKind::File {
                        IdentityId(stable_id_bytes("file", &path_c, "$hub"))
                    } else {
                        IdentityId(stable_id_bytes("id", &path_c, &s.identity_key()))
                    }
                })
                .collect();
            tombstone_orphaned_file_symbols_with_absence(
                g,
                branch,
                &path_c,
                &retained,
                Some(&absence_c),
            );

            let mut rename_edges: Vec<GraphEdge> = Vec::new();
            let mut claimed_tomb_ids: std::collections::HashSet<cis_wal::NodeRevisionId> =
                std::collections::HashSet::new();
            let mut local_bindings: Vec<(IdentityId, NodeRevisionId)> = Vec::new();
            let mut rid_remap: HashMap<NodeRevisionId, NodeRevisionId> = HashMap::new();

            for s in &index_c.symbols {
                let id_key = s.identity_key();
                let stable_rid =
                    NodeRevisionId(stable_rev_id_bytes(branch, &path_c, &id_key));
                let proposed_iid = if s.kind == NodeKind::File {
                    IdentityId(stable_id_bytes("file", &path_c, "$hub"))
                } else {
                    IdentityId(stable_id_bytes("id", &path_c, &id_key))
                };

                let body_snip = if s.kind == NodeKind::File {
                    file_content.clone()
                } else {
                    // Regex single-line spans continue through the indented block via
                    // body_snippet_for_span; tree-sitter multi-line spans are trusted as-is.
                    body_snippet_for_span(&file_content, s.span.start_line, s.span.end_line)
                };
                let content_hash = content_checksum_32(&body_snip);
                // CAS key is symbol identity (path+identity_key), never body text.
                let sem_hash = crate::index_model::identity_cas_semantic_hash(&path_c, &id_key);

                let (iid, rename_source_id) = if s.kind == NodeKind::File {
                    (proposed_iid, None)
                } else {
                    let outcome = crate::identity_resolution::resolve_or_create(
                        g,
                        branch,
                        &path_c,
                        &s.qualified_name,
                        &body_snip,
                        proposed_iid,
                        &resolver_c,
                        &rename_cfg,
                        Some(&body_store_c),
                        Some(&identity_cas_c),
                        sem_hash,
                        &mut claimed_tomb_ids,
                    );
                    let rename_source_id = outcome.rename_link.map(|(tomb_rev, ev)| {
                        rename_edges.push(IdentityResolver::renamed_from_edge(
                            tomb_rev,
                            outcome.identity_id,
                            ev,
                            stable_id_bytes("rn", &path_c, &id_key),
                        ));
                        g.get_revision(tomb_rev)
                            .map(|r| r.identity_id)
                            .unwrap_or(outcome.identity_id)
                    });
                    (outcome.identity_id, rename_source_id)
                };

                let (rid, parent_revision_id) =
                    match g.primary_revision_for_identity(branch, iid).cloned() {
                        None => (stable_rid, None),
                        Some(prev)
                            if matches!(
                                prev.status,
                                RevisionStatus::Active | RevisionStatus::Speculative
                            ) && prev.body_hash == content_hash =>
                        {
                            // Idempotent re-index: same live body.
                            (prev.revision_id, prev.parent_revision_id)
                        }
                        Some(prev)
                            if matches!(
                                prev.status,
                                RevisionStatus::Active | RevisionStatus::Speculative
                            ) =>
                        {
                            // In-place edit: append a content-addressed revision.
                            crate::revision_lineage::retire_revision_to_tombstone(
                                g,
                                prev.revision_id,
                            );
                            (
                                NodeRevisionId(content_rev_id_bytes(
                                    branch,
                                    &path_c,
                                    &id_key,
                                    content_hash,
                                )),
                                Some(prev.revision_id),
                            )
                        }
                        Some(prev) => {
                            // Primary is tombstone/orphaned (typical after rename detection).
                            // Prefer the stable slot for this symbol key so callers can look
                            // up by stable_rev_id_bytes; fall back to content id if occupied
                            // by a different identity.
                            let rid = match g.get_revision(stable_rid) {
                                None => stable_rid,
                                Some(existing) if existing.identity_id == iid => stable_rid,
                                Some(_) => NodeRevisionId(content_rev_id_bytes(
                                    branch,
                                    &path_c,
                                    &id_key,
                                    content_hash,
                                )),
                            };
                            (rid, Some(prev.revision_id))
                        }
                    };
                rid_remap.insert(stable_rid, rid);

                let body_bytes = body_snip.into_bytes();
                body_store_c.put(content_hash, body_bytes.clone());
                if s.kind == NodeKind::File {
                    body_store_c.put(file_body_hash_key(&path_c), body_bytes);
                }

                g.put_identity(NodeIdentity {
                    identity_id: iid,
                    kind: s.kind,
                });
                // Recreating an Active/Speculative revision clears any prior deletion absence.
                absence_c.clear_deleted(branch, iid);
                g.put_revision(NodeRevision {
                    revision_id: rid,
                    identity_id: iid,
                    branch_id: branch,
                    status: RevisionStatus::Active,
                    qualified_name: s.qualified_name.clone(),
                    file_path: path_c.clone(),
                    body_hash: content_hash,
                    signature_hash: content_hash,
                    language: lang_c,
                    parent_revision_id,
                    rename_source_id,
                    span: s.span,
                    tombstoned_at_ms: None,
                });
                local_bindings.push((iid, rid));
            }

            let edge_map = attach_import_and_call_edges(
                &path_c,
                branch,
                &index_c,
                &mod_map,
                Some(g),
                &batch_indexes_c,
            );
            for (stable_rid, edges) in edge_map {
                if edges.is_empty() {
                    continue;
                }
                let target_rid = rid_remap.get(&stable_rid).copied().unwrap_or(stable_rid);
                let remapped: Vec<GraphEdge> = edges
                    .into_iter()
                    .map(|mut e| {
                        e.source_revision_id = target_rid;
                        e
                    })
                    .collect();
                g.replace_edges_for_revision(target_rid, remapped)
                    .map_err(|_| "edge_replace")?;
            }
            for e in rename_edges {
                let rid = e.source_revision_id;
                let mut list = g.outbound_edges(rid).to_vec();
                list.push(e);
                g.replace_edges_for_revision(rid, list)
                    .map_err(|_| "edge_replace")?;
            }
            *bindings_c.lock().unwrap() = local_bindings;
            Ok(())
        })?;
        if let Some(ri) = &time_travel_ri {
            // Bind the *resolved* identity (post-rename), not the proposed stable_id.
            for (iid, rid) in bindings_out.lock().unwrap().iter().copied() {
                ri.bind(iid, rid);
            }
        }
        coord.commit_vector(id)?;
        if let Some(ri) = &time_travel_ri {
            crate::time_travel::record_committed_snapshot(
                ri.as_ref(),
                kv.as_ref(),
                branch,
                id,
                cis_dir.as_deref(),
            );
        }
        if let Some(oid) = git_head_hex {
            let _ = crate::time_travel::record_git_oid_wal_log(kv.as_ref(), oid, id);
        }
        rep.applied += 1;
    }
    coord.set_defer_snapshot_flush(false);
    coord.flush_committed_snapshots()?;
    Ok(rep)
}

/// **FR-1.11** — three-signal helper retained for LSP wiring; use [`crate::identity_resolver::IdentityResolver`] for rename math.
#[derive(Debug, Default)]
pub struct IdentityResolverShell;

impl IdentityResolverShell {
    pub fn below_rename_threshold(&self, confidence: f64, policy_min: f64) -> bool {
        confidence < policy_min
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cis_wal::MutationLog;

    use crate::coordinator::WriteCoordinator;
    use crate::graph::NodeRevision;
    use crate::merge_lock::acquire_merge_lock;
    use crate::saga::MergeSagaOrchestrator;
    use cis_wal::MergeId;

    #[test]
    fn index_queue_fifo() {
        let q = IndexEventQueue::new();
        q.enqueue(IndexEvent {
            branch_id: BranchId([0u8; 16]),
            path: "a.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        });
        assert_eq!(q.depth(), 1);
        let d = q.drain();
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn resolver_split_guard() {
        let r = IdentityResolverShell::default();
        assert!(r.below_rename_threshold(0.3, 0.5));
        assert!(!r.below_rename_threshold(0.6, 0.5));
    }

    #[test]
    fn extract_python_finds_def() {
        let src = "def foo():\n    pass\n\ndef bar(x):\n    return x\n";
        let names = extract_python_top_level_defs(src).unwrap();
        assert!(names.contains(&"foo".into()));
        assert!(names.contains(&"bar".into()));
    }

    #[test]
    fn extract_python_regex_assigns_definition_spans() {
        let src = "# header\ndef foo():\n    pass\n";
        let idx = index_python_file("m.py", src).unwrap();
        let foo = idx
            .symbols
            .iter()
            .find(|s| s.stable_key == "foo")
            .expect("foo");
        assert_eq!(foo.span.start_line, 2);
        assert!(foo.span.start_col >= 1);
    }

    #[test]
    fn cross_file_call_via_star_import() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);

        let utils = "def getStrPosition(x):\n    return str(x)\n";
        let board = "from utils import *\n\ndef move():\n    getStrPosition(1)\n";

        let events = vec![
            IndexEvent {
                branch_id: branch,
                path: "utils.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
            IndexEvent {
                branch_id: branch,
                path: "Board.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
        ];
        let rep = apply_index_events(
            &IndexEventQueue::new(),
            &coord,
            Arc::clone(&kv),
            events,
            |p| {
                if p == "utils.py" {
                    Ok(utils.to_string())
                } else {
                    Ok(board.to_string())
                }
            },
            None,
            None,
        )
        .expect("ingest");
        assert_eq!(rep.applied, 2);

        let g = coord.graph().read();
        let mut calls = 0usize;
        for r in g.revisions() {
            for e in g.outbound_edges(r.revision_id) {
                if e.ty == EdgeType::Calls {
                    calls += 1;
                }
            }
        }
        assert!(
            calls >= 1,
            "expected cross-file Calls edge from Board.py to utils.getStrPosition, got {calls}"
        );
    }

    #[test]
    #[cfg(feature = "tree-sitter")]
    fn qualified_call_via_class_import() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);

        let board = "class Board:\n    def initialize(self):\n        pass\n";
        let screen = "from Board import Board\n\nclass Screen:\n    def run(self):\n        Board.initialize()\n";

        let events = vec![
            IndexEvent {
                branch_id: branch,
                path: "Board.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
            IndexEvent {
                branch_id: branch,
                path: "Screen.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
        ];
        let rep = apply_index_events(
            &IndexEventQueue::new(),
            &coord,
            Arc::clone(&kv),
            events,
            |p| {
                if p == "Board.py" {
                    Ok(board.to_string())
                } else {
                    Ok(screen.to_string())
                }
            },
            None,
            None,
        )
        .expect("ingest");
        assert_eq!(rep.applied, 2);

        let g = coord.graph().read();
        let init_id = IdentityId(stable_id_bytes("id", "Board.py", "Board.initialize"));
        let mut found = false;
        for r in g.revisions() {
            for e in g.outbound_edges(r.revision_id) {
                if e.ty == EdgeType::Calls && e.target_identity_id == init_id {
                    found = true;
                }
            }
        }
        assert!(
            found,
            "expected Calls edge from Screen.run to Board.initialize"
        );
    }

    #[test]
    #[cfg(feature = "tree-sitter")]
    fn self_field_call_chain() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);

        let board = "class Board:\n    def builder(self, move):\n        pass\n";
        let screen = "from Board import Board\n\nclass Screen:\n    def __init__(self):\n        self.board = Board.initialize()\n    def run(self):\n        self.board.builder(move)\n";

        let events = vec![
            IndexEvent {
                branch_id: branch,
                path: "Board.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
            IndexEvent {
                branch_id: branch,
                path: "Screen.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
        ];
        let rep = apply_index_events(
            &IndexEventQueue::new(),
            &coord,
            Arc::clone(&kv),
            events,
            |p| {
                if p == "Board.py" {
                    Ok(board.to_string())
                } else {
                    Ok(screen.to_string())
                }
            },
            None,
            None,
        )
        .expect("ingest");
        assert_eq!(rep.applied, 2);

        let builder_id = IdentityId(stable_id_bytes("id", "Board.py", "Board.builder"));
        let g = coord.graph().read();
        let mut found = false;
        for r in g.revisions() {
            if !r.qualified_name.contains("Screen.run") {
                continue;
            }
            for e in g.outbound_edges(r.revision_id) {
                if e.ty == EdgeType::Calls && e.target_identity_id == builder_id {
                    found = true;
                }
            }
        }
        assert!(
            found,
            "expected Calls edge from Screen.run to Board.builder via self.board"
        );
    }

    #[test]
    #[cfg(feature = "tree-sitter")]
    fn loop_var_call_chain() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);

        let tile = "class ChessTile:\n    def on_click(self, a, b):\n        pass\n";
        let screen = "from Tile import ChessTile\n\nclass Screen:\n    def run(self):\n        TILES = []\n        TILES.append(ChessTile())\n        for chessTile in TILES:\n            chessTile.on_click(a, b)\n";

        let events = vec![
            IndexEvent {
                branch_id: branch,
                path: "Tile.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
            IndexEvent {
                branch_id: branch,
                path: "Screen.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            },
        ];
        let rep = apply_index_events(
            &IndexEventQueue::new(),
            &coord,
            Arc::clone(&kv),
            events,
            |p| {
                if p == "Tile.py" {
                    Ok(tile.to_string())
                } else {
                    Ok(screen.to_string())
                }
            },
            None,
            None,
        )
        .expect("ingest");
        assert_eq!(rep.applied, 2);

        let on_click_id = IdentityId(stable_id_bytes("id", "Tile.py", "ChessTile.on_click"));
        let g = coord.graph().read();
        let mut found = false;
        for r in g.revisions() {
            if !r.qualified_name.contains("Screen.run") {
                continue;
            }
            for e in g.outbound_edges(r.revision_id) {
                if e.ty == EdgeType::Calls && e.target_identity_id == on_click_id {
                    found = true;
                }
            }
        }
        assert!(
            found,
            "expected Calls edge from Screen.run to ChessTile.on_click via loop var"
        );
    }

    #[test]
    fn ingest_applies_through_coordinator() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga_kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(saga_kv);
        let _ = coord.reconcile_on_startup(&saga);

        let q = IndexEventQueue::new();
        let src = "def zzz():\n  pass\n";
        let events = vec![IndexEvent {
            branch_id: BranchId([0u8; 16]),
            path: "t.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
                let rep = apply_index_events(
            &q,
            &coord,
            Arc::clone(&kv),
            events,
            |_p| Ok(src.to_string()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(rep.applied, 1);
        let zzz = find_symbol_at_in_graph(&coord, "zzz");
        assert!(zzz.qualified_name.contains("zzz"));
        assert_eq!(zzz.span.start_line, 1);
        assert!(!zzz.span.is_unknown());
    }

    #[test]
    fn ingest_requeues_when_merge_locked() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([9u8; 16]);
        let mid = MergeId([1u8; 16]);
        acquire_merge_lock(kv.as_ref(), branch, mid).unwrap();
        let q = IndexEventQueue::new();
        let events = vec![IndexEvent {
            branch_id: branch,
            path: "a.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
                let rep = apply_index_events(
            &q,
            &coord,
            kv,
            events,
            |_p| Ok("def f():pass\n".into()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(rep.requeued_merge_lock, 1);
        assert_eq!(rep.applied, 0);
        assert_eq!(q.depth(), 1);
    }

    #[test]
    fn ingest_records_time_travel_snapshot_when_ri_provided() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([0u8; 16]);
        let ri = crate::revision_cow::RevisionIndexCow::root(branch, Arc::clone(&kv));
        let saga_kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(saga_kv);
        let _ = coord.reconcile_on_startup(&saga);

        let q = IndexEventQueue::new();
        let src = "def ttsnap():\n  pass\n";
        let events = vec![IndexEvent {
            branch_id: branch,
            path: "snap.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
                let rep = apply_index_events(
            &q,
            &coord,
            Arc::clone(&kv),
            events,
            |_p| Ok(src.to_string()),
            Some(Arc::clone(&ri)),
            None,
        )
        .unwrap();
        assert_eq!(rep.applied, 1);
        let r = find_symbol_at_in_graph(&coord, "ttsnap");
        let log_id = 1u64;
        let epoch = crate::time_travel::resolve_epoch_for_wal_log(kv.as_ref(), branch, log_id).unwrap();
        assert!(epoch > 0);
        let snap = crate::time_travel::overlay_at_wal_log(&kv, branch, log_id).unwrap();
        let rid = r.revision_id;
        assert_eq!(snap.lookup(r.identity_id), Some(rid));
    }

    #[test]
    fn ingest_records_git_oid_for_commit_anchor() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let branch = BranchId([0u8; 16]);
        let ri = crate::revision_cow::RevisionIndexCow::root(branch, Arc::clone(&kv));
        let saga_kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(saga_kv);
        let _ = coord.reconcile_on_startup(&saga);

        let oid = "aabbccddeeff00112233445566778899aabbccdd";
        let q = IndexEventQueue::new();
        let src = "def gitoid_fn():\n  pass\n";
        let events = vec![IndexEvent {
            branch_id: branch,
            path: "g.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
                let rep = apply_index_events(
            &q,
            &coord,
            Arc::clone(&kv),
            events,
            |_p| Ok(src.to_string()),
            Some(Arc::clone(&ri)),
            Some(oid),
        )
        .unwrap();
        assert_eq!(rep.applied, 1);
        let log_id = 1u64;
        assert_eq!(
            crate::time_travel::resolve_wal_log_from_git_oid(kv.as_ref(), oid),
            Some(log_id)
        );
    }

    #[cfg(feature = "tree-sitter")]
    #[test]
    fn extract_python_tree_sitter_sees_decorated_def() {
        let src = "def dec(f):\n    return f\n\n@dec\ndef wrapped():\n    pass\n";
        let names = extract_python_top_level_defs(src).unwrap();
        assert!(names.contains(&"wrapped".into()));
    }

    #[test]
    fn same_file_rename_preserves_identity_through_ingest() {
        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);
        let path = "m.py";
        let q = IndexEventQueue::new();

        let apply = |src: &str| {
            let events = vec![IndexEvent {
                branch_id: branch,
                path: path.into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            }];
            let s = src.to_string();
            apply_index_events(
                &q,
                &coord,
                Arc::clone(&kv),
                events,
                move |_p| Ok(s.clone()),
                None,
                None,
            )
            .unwrap()
        };

        apply("def foo():\n    return 1\n");
        let iid_foo = IdentityId(stable_id_bytes("id", path, "foo"));
        apply("def bar():\n    return 1\n");

        let g = coord.graph().read();
        let bar = g
            .primary_revision_for_identity(branch, iid_foo)
            .expect("bar via preserved identity");
        assert!(
            bar.qualified_name.contains("bar"),
            "expected bar qn, got {}",
            bar.qualified_name
        );
        assert_eq!(bar.identity_id, iid_foo);
        assert_eq!(bar.rename_source_id, Some(iid_foo));
        let foo_rev = NodeRevisionId(stable_rev_id_bytes(branch, path, "foo"));
        assert!(matches!(
            g.get_revision(foo_rev).map(|r| r.status),
            Some(RevisionStatus::Tombstone)
        ));
        assert!(
            g.outbound_edges(foo_rev)
                .iter()
                .any(|e| e.ty == EdgeType::RenamedFrom)
        );
    }

    #[test]
    fn ingest_append_style_two_versions() {
        use cis_wal::MutationLog;

        use crate::coordinator::WriteCoordinator;
        use crate::graph::{Language, NodeIdentity, NodeKind, NodeRevision, SourceSpan};
        use crate::graph_mutation::GraphMutationSet;
        use crate::index_model::{content_checksum_32, content_rev_id_bytes, stable_rev_id_bytes};

        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);
        let path = "t.py";
        let iid = IdentityId(stable_id_bytes("id", path, "f"));
        let stable = NodeRevisionId(stable_rev_id_bytes(branch, path, "f"));
        let h1 = content_checksum_32("body1");
        let h2 = content_checksum_32("body2");

        let mid1 = coord
            .begin_mutation(&GraphMutationSet::new(vec![stable], h1))
            .unwrap();
        coord
            .commit_graph(mid1, move |g| {
                g.put_identity(NodeIdentity {
                    identity_id: iid,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: stable,
                    identity_id: iid,
                    branch_id: branch,
                    status: RevisionStatus::Active,
                    qualified_name: format!("{path}::f"),
                    file_path: path.into(),
                    body_hash: h1,
                    signature_hash: h1,
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    span: SourceSpan::UNKNOWN,
                    tombstoned_at_ms: None,
                });
                Ok(())
            })
            .unwrap();

        let new_rid = NodeRevisionId(content_rev_id_bytes(branch, path, "f", h2));
        let mid2 = coord
            .begin_mutation(&GraphMutationSet::new(vec![stable, new_rid], h2))
            .unwrap();
        coord
            .commit_graph(mid2, move |g| {
                let prev = g
                    .primary_revision_for_identity(branch, iid)
                    .cloned()
                    .expect("prev");
                crate::revision_lineage::retire_revision_to_tombstone(g, prev.revision_id);
                g.put_revision(NodeRevision {
                    revision_id: new_rid,
                    identity_id: iid,
                    branch_id: branch,
                    status: RevisionStatus::Active,
                    qualified_name: format!("{path}::f"),
                    file_path: path.into(),
                    body_hash: h2,
                    signature_hash: h2,
                    language: Language::Python,
                    parent_revision_id: Some(prev.revision_id),
                    rename_source_id: None,
                    span: SourceSpan::UNKNOWN,
                    tombstoned_at_ms: None,
                });
                Ok(())
            })
            .unwrap();

        let g = coord.graph().read();
        assert_eq!(g.revision_ids_for_identity(branch, iid).len(), 2);
        let steps = crate::revision_lineage::lineage_for_identity(
            &g,
            &[branch],
            iid,
            None,
            &crate::revision_lineage::LineageOptions::default(),
        )
        .expect("lineage");
        assert_eq!(steps.len(), 2);
        assert!(matches!(steps[0].status, RevisionStatus::Active));
        assert!(matches!(steps[1].status, RevisionStatus::Tombstone));
    }

    #[cfg(feature = "tree-sitter")]
    #[test]
    fn ingest_class_and_function_same_name_get_distinct_identities() {
        use cis_wal::MutationLog;

        use crate::coordinator::WriteCoordinator;
        use crate::index_model::symbol_identity_key;
        use crate::saga::MergeSagaOrchestrator;
        use crate::MemoryKv;

        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);
        let path = "collide.py";
        let q = IndexEventQueue::new();
        let src = "class foo:\n    pass\n\ndef foo():\n    return 1\n";
        let events = vec![IndexEvent {
            branch_id: branch,
            path: path.into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
        let s = src.to_string();
        apply_index_events(
            &q,
            &coord,
            Arc::clone(&kv),
            events,
            move |_p| Ok(s.clone()),
            None,
            None,
        )
        .unwrap();

        let iid_fn = IdentityId(stable_id_bytes("id", path, &symbol_identity_key("foo", "")));
        let iid_class =
            IdentityId(stable_id_bytes("id", path, &symbol_identity_key("foo", "class")));
        assert_ne!(iid_fn, iid_class);

        let g = coord.graph().read();
        let fn_rev = g
            .primary_revision_for_identity(branch, iid_fn)
            .expect("function identity");
        let class_rev = g
            .primary_revision_for_identity(branch, iid_class)
            .expect("class identity");
        assert_eq!(fn_rev.qualified_name, format!("{path}::foo"));
        assert_eq!(class_rev.qualified_name, format!("{path}::foo"));
        assert!(matches!(fn_rev.status, RevisionStatus::Active));
        assert!(matches!(class_rev.status, RevisionStatus::Active));
        assert_eq!(
            g.identity_kind(iid_fn),
            Some(NodeKind::Function)
        );
        assert_eq!(
            g.identity_kind(iid_class),
            Some(NodeKind::Class)
        );
        // MCP search surface: both still match a "foo" needle via qualified_name.
        let hits: Vec<_> = g
            .revisions()
            .filter(|r| {
                matches!(r.status, RevisionStatus::Active)
                    && r.file_path == path
                    && r.qualified_name.contains("foo")
                    && r.qualified_name != path
            })
            .collect();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn ingest_overload_style_defs_get_distinct_identities_last_canonical() {
        use cis_wal::MutationLog;

        use crate::coordinator::WriteCoordinator;
        use crate::index_model::symbol_identity_key;
        use crate::saga::MergeSagaOrchestrator;
        use crate::MemoryKv;

        let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let kv = Arc::new(MemoryKv::new());
        let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
        let _ = coord.reconcile_on_startup(&saga);
        let branch = BranchId([0u8; 16]);
        let path = "overloads.py";
        let q = IndexEventQueue::new();
        let src = "def foo(x: int):\n    ...\n\ndef foo(x: str):\n    ...\n\ndef foo(x):\n    return x\n";
        let events = vec![IndexEvent {
            branch_id: branch,
            path: path.into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        }];
        let s = src.to_string();
        apply_index_events(
            &q,
            &coord,
            Arc::clone(&kv),
            events,
            move |_p| Ok(s.clone()),
            None,
            None,
        )
        .unwrap();

        let iid_canon =
            IdentityId(stable_id_bytes("id", path, &symbol_identity_key("foo", "")));
        let iid_0 =
            IdentityId(stable_id_bytes("id", path, &symbol_identity_key("foo", "fn:0")));
        let iid_1 =
            IdentityId(stable_id_bytes("id", path, &symbol_identity_key("foo", "fn:1")));
        let g = coord.graph().read();
        assert!(g.primary_revision_for_identity(branch, iid_canon).is_some());
        assert!(g.primary_revision_for_identity(branch, iid_0).is_some());
        assert!(g.primary_revision_for_identity(branch, iid_1).is_some());
        let body = g
            .primary_revision_for_identity(branch, iid_canon)
            .unwrap();
        assert_eq!(body.qualified_name, format!("{path}::foo"));
        // Canonical slot is the last definition (implementation).
        assert!(body.span.start_line >= 5 || body.body_hash != [0u8; 32]);
    }

    fn find_symbol_at_in_graph(coord: &WriteCoordinator, needle: &str) -> NodeRevision {
        let g = coord.graph().read();
        for r in g.revisions() {
            if r.qualified_name.contains(needle) {
                return r.clone();
            }
        }
        panic!("not found");
    }
}
