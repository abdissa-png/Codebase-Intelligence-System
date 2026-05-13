//! **FR-1.5** — filesystem walk + coordinator ingest on the runtime’s shared graph (**Phase 3**).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cis_wal::BranchId;

use crate::coordinator::{CoordinatorError, WriteCoordinator};
use crate::fs_sync::reindex_persist_snapshots_enabled;
use crate::graph::InMemoryGraph;
use crate::identity_resolution::RenameConfig;
use crate::ingest::{
    apply_index_events_with_config, FsChangeKind, IndexEvent, IndexEventQueue, IngestApplyReport,
};
use crate::ranking_policy::RankingPolicy;
use crate::persistence::{cis_dir, save_workspace_snapshots};
use crate::revision_cow::RevisionIndexCow;
use crate::index_walk;
use crate::vector_store::InMemoryVectorStore;
use crate::MemoryKv;

/// Load `RenameConfig` from `.cis/ranking_policy.yaml` if present, else use defaults.
fn load_rename_config(root: &Path) -> RenameConfig {
    let policy_path = root.join(".cis").join("ranking_policy.yaml");
    if let Ok(yaml) = std::fs::read_to_string(&policy_path) {
        if let Ok(p) = RankingPolicy::from_yaml_str(&yaml) {
            return RenameConfig::from_policy(&p);
        }
    }
    RenameConfig::default()
}

/// Recursively collect source files with any of the given extensions under `root`.
///
/// Honors `.gitignore` by default (see [`index_walk::index_respect_gitignore`]).
pub fn collect_source_files(root: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    index_walk::collect_source_files(root, extensions, out);
}

/// Collect files for all default indexers (`.py`, `.ts`, `.tsx`).
pub fn collect_indexable_files(root: &Path, out: &mut Vec<PathBuf>) {
    let exts: Vec<&str> = crate::language_indexer::default_indexers()
        .iter()
        .map(|i| i.file_extension())
        .collect();
    collect_source_files(root, &exts, out);
}

/// Recursively collect `*.py` paths under `root`.
pub fn collect_py_files(root: &Path, out: &mut Vec<PathBuf>) {
    collect_source_files(root, &["py"], out);
}

fn sync_revision_index_from_graph(
    graph: &InMemoryGraph,
    revision_index: &RevisionIndexCow,
    kv: &MemoryKv,
    branch: BranchId,
) {
    let branch_hex = branch
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let prefix = format!("ri:{branch_hex}:");
    let keys: Vec<String> = kv
        .scan_prefix(&prefix)
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    for key in keys {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 3 {
            continue;
        }
        let Some(identity) = parse_identity_hex(parts[2]) else {
            continue;
        };
        match graph.primary_revision_for_identity(branch, identity) {
            Some(rev)
                if matches!(
                    rev.status,
                    crate::graph::RevisionStatus::Active | crate::graph::RevisionStatus::Speculative
                ) =>
            {
                revision_index.bind(identity, rev.revision_id);
            }
            _ => revision_index.unbind(identity),
        }
    }
    let identities: HashSet<_> = graph
        .revisions()
        .filter(|r| r.branch_id == branch)
        .map(|r| r.identity_id)
        .collect();
    for identity in identities {
        if revision_index.lookup(identity).is_some() {
            continue;
        }
        if let Some(rev) = graph.primary_revision_for_identity(branch, identity) {
            if matches!(
                rev.status,
                crate::graph::RevisionStatus::Active | crate::graph::RevisionStatus::Speculative
            ) {
                revision_index.bind(identity, rev.revision_id);
            }
        }
    }
}

fn parse_identity_hex(s: &str) -> Option<cis_wal::IdentityId> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(cis_wal::IdentityId(b))
}

fn maybe_persist_workspace(
    root: &Path,
    coord: &WriteCoordinator,
    kv: &MemoryKv,
) -> Result<(), CoordinatorError> {
    if !reindex_persist_snapshots_enabled() {
        return Ok(());
    }
    let g = coord.graph().read();
    save_workspace_snapshots(&cis_dir(root), &g, coord.vector(), kv)
        .map_err(|e| CoordinatorError::Persist(e.to_string()))
}

/// Walk `repo_root` for `*.py` and ingest via the **shared** [`WriteCoordinator`] (no second open, no graph clone).
///
/// Caller must have loaded `.cis` snapshots on this coordinator already (e.g. [`CisMcpRuntime::new_dev`]).
/// Set **`CIS_FORCE_REINDEX=1`** to re-walk even when revisions already exist.
/// Set **`CIS_REINDEX_PERSIST=0`** to skip snapshot write until MCP `save_workspace`.
pub fn bootstrap_python_workspace_on_coordinator(
    coord: &WriteCoordinator,
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
) -> Result<IngestApplyReport, CoordinatorError> {
    let root = PathBuf::from(repo_root);
    let force = std::env::var_os("CIS_FORCE_REINDEX").is_some_and(|v| v == "1");
    let existing_revisions = coord.graph().read().revision_count();
    if existing_revisions > 0 && !force {
        let g = coord.graph().read();
        sync_revision_index_from_graph(&g, revision_index, kv.as_ref(), branch);
        drop(g);
        maybe_persist_workspace(&root, coord, kv.as_ref())?;
        return Ok(IngestApplyReport::default());
    }

    let mut paths = Vec::new();
    collect_indexable_files(&root, &mut paths);

    let q = IndexEventQueue::new();
    let events: Vec<IndexEvent> = paths
        .iter()
        .filter_map(|p| {
            let rel = p.strip_prefix(&root).ok()?;
            Some(IndexEvent {
                branch_id: branch,
                path: rel.to_string_lossy().replace('\\', "/"),
                kind: FsChangeKind::Modified,
            })
        })
        .collect();

    let rename_config = load_rename_config(&root);
    let root_clone = root.clone();
    let rep = apply_index_events_with_config(
        &q,
        coord,
        Arc::clone(kv),
        events,
        move |rel| std::fs::read_to_string(root_clone.join(rel)),
        None,
        None,
        Some(rename_config),
    )?;

    let g = coord.graph().read();
    sync_revision_index_from_graph(&g, revision_index, kv.as_ref(), branch);
    drop(g);
    maybe_persist_workspace(&root, coord, kv.as_ref())?;

    Ok(rep)
}

/// Walk `repo_root` for indexable sources and ingest via the shared coordinator (alias for Python bootstrap today).
pub fn bootstrap_index_from_repo(
    coord: &WriteCoordinator,
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
) -> Result<IngestApplyReport, CoordinatorError> {
    bootstrap_python_workspace_on_coordinator(coord, repo_root, kv, revision_index, branch)
}

/// Legacy API: opens a **separate** coordinator and clones into `graph_mutex` (tests / non-MCP callers).
/// Prefer [`bootstrap_python_workspace_on_coordinator`] when a shared runtime coordinator exists.
pub fn bootstrap_python_workspace_into_graph(
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    graph_mutex: &std::sync::Mutex<InMemoryGraph>,
    vector: &InMemoryVectorStore,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
) -> Result<IngestApplyReport, CoordinatorError> {
    use crate::persistence::open_persisted_coordinator;
    use crate::saga::MergeSagaOrchestrator;

    let root = PathBuf::from(repo_root);
    let coord = open_persisted_coordinator(&root)
        .map_err(|e| CoordinatorError::Persist(e.to_string()))?;
    let saga_kv = Arc::new(MemoryKv::new());
    let saga = MergeSagaOrchestrator::new(saga_kv);
    let _ = coord.reconcile_on_startup(&saga);

    let rep = bootstrap_python_workspace_on_coordinator(
        coord.as_ref(),
        repo_root,
        kv,
        revision_index,
        branch,
    )?;

    *graph_mutex.lock().unwrap() = coord
        .graph()
        .read()
        .clone_full()
        .expect("clone graph");
    vector.replace_all(coord.vector().export_chunks());
    Ok(rep)
}
