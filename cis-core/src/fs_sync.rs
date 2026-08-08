//! **Phase 2** — incremental ingest, debounced FS watch, MCP write → graph sync (**FR-1.5**).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use std::collections::HashSet;

use cis_wal::BranchId;

use crate::coordinator::{CoordinatorError, WriteCoordinator};
use crate::graph::{InMemoryGraph, RevisionStatus};
use crate::shared_graph::SharedInMemoryGraph;
use crate::identity_resolution::RenameConfig;
use crate::ingest::{
    apply_index_events_with_config, FsChangeKind, IndexEvent, IndexEventQueue, IngestApplyReport,
};
use crate::ranking_policy::RankingPolicy;
use crate::persistence::{cis_dir, open_persisted_coordinator, save_workspace_snapshots};
use crate::revision_cow::RevisionIndexCow;
use crate::saga::MergeSagaOrchestrator;
use crate::vector_store::InMemoryVectorStore;
use crate::watcher_metrics::WatcherMetrics;
use crate::MemoryKv;

/// Default debounce for external FS events (ms). Override with `CIS_INDEX_DEBOUNCE_MS`.
pub const DEFAULT_DEBOUNCE_MS: u64 = 300;

/// Default poll interval when `notify` is unavailable (ms). Override with `CIS_FS_POLL_MS`.
pub const DEFAULT_POLL_MS: u64 = 500;

/// When `CIS_REINDEX_PERSIST=0`, incremental [`reindex_python_paths_with_config`] skips
/// [`save_workspace_snapshots`] (call MCP `save_workspace` or set this var to `1` for legacy behavior).
pub fn reindex_persist_snapshots_enabled() -> bool {
    crate::persistence::snapshot_persist_enabled()
}

#[derive(Debug, Clone)]
pub struct FsSyncConfig {
    pub debounce: Duration,
    pub poll_interval: Duration,
}

impl Default for FsSyncConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(DEFAULT_DEBOUNCE_MS),
            poll_interval: Duration::from_millis(DEFAULT_POLL_MS),
        }
    }
}

impl FsSyncConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(s) = std::env::var("CIS_INDEX_DEBOUNCE_MS") {
            if let Ok(ms) = s.parse::<u64>() {
                c.debounce = Duration::from_millis(ms);
            }
        }
        if let Ok(s) = std::env::var("CIS_FS_POLL_MS") {
            if let Ok(ms) = s.parse::<u64>() {
                c.poll_interval = Duration::from_millis(ms);
            }
        } else if let Ok(s) = std::env::var("CIS_FS_WATCH_POLL_SECS") {
            if let Ok(secs) = s.parse::<u64>() {
                c.poll_interval = Duration::from_secs(secs.max(1));
            }
        }
        c
    }
}

/// Coalesce index events per relative path; flush when quiet for `debounce`.
#[derive(Debug)]
pub struct IndexDebouncer {
    debounce: Duration,
    pending: Mutex<HashMap<String, (IndexEvent, Instant)>>,
    metrics: Mutex<Option<Arc<WatcherMetrics>>>,
}

impl IndexDebouncer {
    pub fn new(debounce: Duration) -> Self {
        Self {
            debounce,
            pending: Mutex::new(HashMap::new()),
            metrics: Mutex::new(None),
        }
    }

    pub fn set_metrics(&self, metrics: Arc<WatcherMetrics>) {
        *self.metrics.lock().unwrap() = Some(metrics);
    }

    fn metrics(&self) -> Option<Arc<WatcherMetrics>> {
        self.metrics.lock().unwrap().clone()
    }

    pub fn schedule(&self, ev: IndexEvent) {
        let had = self
            .pending
            .lock()
            .unwrap()
            .contains_key(&ev.path);
        self.pending
            .lock()
            .unwrap()
            .insert(ev.path.clone(), (ev, Instant::now()));
        if let Some(m) = self.metrics() {
            m.record_schedule(had);
            m.update_pending_count(self.pending_count());
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn record_drain_delays(&self, drained: &[(IndexEvent, Instant)]) {
        let Some(m) = self.metrics() else {
            return;
        };
        let now = Instant::now();
        m.record_debounce_delays(
            drained
                .iter()
                .map(|(_, t)| now.duration_since(*t).as_millis() as u64),
        );
        m.update_pending_count(self.pending_count());
    }

    /// Paths whose last schedule time is older than `debounce`.
    pub fn drain_ready(&self) -> Vec<IndexEvent> {
        let now = Instant::now();
        let mut g = self.pending.lock().unwrap();
        let ready_keys: Vec<String> = g
            .iter()
            .filter(|(_, (_, t))| now.duration_since(*t) >= self.debounce)
            .map(|(k, _)| k.clone())
            .collect();
        let drained: Vec<(IndexEvent, Instant)> = ready_keys
            .into_iter()
            .filter_map(|k| g.remove(&k))
            .collect();
        drop(g);
        self.record_drain_delays(&drained);
        drained.into_iter().map(|(ev, _)| ev).collect()
    }

    /// Immediate flush (MCP `write_file` / `apply_patch`).
    pub fn flush_all(&self) -> Vec<IndexEvent> {
        let drained: Vec<(IndexEvent, Instant)> = self
            .pending
            .lock()
            .unwrap()
            .drain()
            .map(|(_, v)| v)
            .collect();
        self.record_drain_delays(&drained);
        drained.into_iter().map(|(ev, _)| ev).collect()
    }
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
                    RevisionStatus::Active | RevisionStatus::Speculative
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
                RevisionStatus::Active | RevisionStatus::Speculative
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

fn sync_coordinator_to_runtime(
    coord: &crate::coordinator::WriteCoordinator,
    graph: &SharedInMemoryGraph,
    vector: &InMemoryVectorStore,
) {
    *graph.write() = coord
        .graph()
        .read()
        .clone_full()
        .expect("clone graph");
    vector.replace_all(coord.vector().export_chunks());
}

/// Load `RenameConfig` from `.cis/ranking_policy.yaml` under `repo_root`, or use defaults.
pub fn load_rename_config_from_root(repo_root: &str) -> RenameConfig {
    let policy_path = PathBuf::from(repo_root)
        .join(".cis")
        .join("ranking_policy.yaml");
    if let Ok(yaml) = std::fs::read_to_string(&policy_path) {
        if let Ok(p) = RankingPolicy::from_yaml_str(&yaml) {
            return RenameConfig::from_policy(&p);
        }
    }
    RenameConfig::default()
}

/// Incrementally re-index repo-relative paths (alias for [`reindex_python_paths`]).
pub fn reindex_paths(
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    graph: &SharedInMemoryGraph,
    vector: &InMemoryVectorStore,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
    paths: Vec<String>,
) -> Result<IngestApplyReport, CoordinatorError> {
    reindex_python_paths(
        repo_root, kv, graph, vector, revision_index, branch, paths,
    )
}

/// Incrementally re-index only the given repo-relative `*.py` paths.
/// Loads rename thresholds from `.cis/ranking_policy.yaml` if present.
pub fn reindex_python_paths(
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    graph: &SharedInMemoryGraph,
    vector: &InMemoryVectorStore,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
    paths: Vec<String>,
) -> Result<IngestApplyReport, CoordinatorError> {
    let cfg = load_rename_config_from_root(repo_root);
    reindex_python_paths_with_config(
        repo_root,
        kv,
        graph,
        vector,
        revision_index,
        branch,
        paths,
        Some(cfg),
    )
}

/// Incremental re-index on an existing **`WriteCoordinator`** (no per-call open, no graph clone).
pub fn reindex_paths_on_coordinator(
    coord: &WriteCoordinator,
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
    paths: Vec<String>,
    rename_config: Option<RenameConfig>,
) -> Result<IngestApplyReport, CoordinatorError> {
    if paths.is_empty() {
        return Ok(IngestApplyReport::default());
    }
    let root = PathBuf::from(repo_root);

    let events: Vec<IndexEvent> = paths
        .iter()
        .map(|path| IndexEvent {
            branch_id: branch,
            path: path.clone(),
            kind: FsChangeKind::Modified,
            old_path: None,
        })
        .collect();

    let root_clone = root.clone();
    let q = IndexEventQueue::new();
    let rep = apply_index_events_with_config(
        &q,
        coord,
        Arc::clone(kv),
        events,
        move |rel| std::fs::read_to_string(root_clone.join(rel)),
        Some(Arc::clone(revision_index)),
        None,
        rename_config,
    )?;

    let g = coord.graph().read();
    sync_revision_index_from_graph(&g, revision_index, kv.as_ref(), branch);
    drop(g);
    if reindex_persist_snapshots_enabled() {
        let g = coord.graph().read();
        save_workspace_snapshots(&cis_dir(&root), &g, coord.vector(), kv.as_ref())
            .map_err(|e| CoordinatorError::Persist(e.to_string()))?;
    }

    Ok(rep)
}

/// Filter `paths` to those registered in [`crate::language_indexer::default_indexers`]
/// and not under builtin skip dirs / generated filenames.
fn filter_indexable_paths(paths: Vec<String>) -> Vec<String> {
    paths
        .into_iter()
        .filter(|p| crate::language_indexer::path_is_indexable(p))
        .collect()
}

/// Re-index registered language sources (Python/TS/… depending on Cargo features).
///
/// Historically Python-only; now filters via [`crate::language_indexer::path_is_indexable`].
pub fn reindex_python_paths_on_coordinator(
    coord: &WriteCoordinator,
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
    paths: Vec<String>,
    rename_config: Option<RenameConfig>,
) -> Result<IngestApplyReport, CoordinatorError> {
    let indexable = filter_indexable_paths(paths);
    reindex_paths_on_coordinator(
        coord,
        repo_root,
        kv,
        revision_index,
        branch,
        indexable,
        rename_config,
    )
}

/// Like [`reindex_python_paths`] with optional rename tunables from `.cis/ranking_policy.yaml`.
///
/// Opens a **new** coordinator each call (legacy / tests). Prefer
/// [`reindex_python_paths_on_coordinator`] with the runtime’s shared coordinator.
pub fn reindex_python_paths_with_config(
    repo_root: &str,
    kv: &Arc<MemoryKv>,
    graph: &SharedInMemoryGraph,
    vector: &InMemoryVectorStore,
    revision_index: &Arc<RevisionIndexCow>,
    branch: BranchId,
    paths: Vec<String>,
    rename_config: Option<RenameConfig>,
) -> Result<IngestApplyReport, CoordinatorError> {
    let root = PathBuf::from(repo_root);
    let indexable = filter_indexable_paths(paths);
    if indexable.is_empty() {
        return Ok(IngestApplyReport::default());
    }

    let coord = open_persisted_coordinator(&root)
        .map_err(|e| CoordinatorError::Persist(e.to_string()))?;
    let saga = MergeSagaOrchestrator::new(Arc::new(MemoryKv::new()));
    let _ = coord.reconcile_on_startup(&saga);

    let rep = reindex_python_paths_on_coordinator(
        coord.as_ref(),
        repo_root,
        kv,
        revision_index,
        branch,
        indexable,
        rename_config,
    )?;

    sync_coordinator_to_runtime(coord.as_ref(), graph, vector);

    Ok(rep)
}

/// Poll-based watcher: detect `.py` mtime changes under `repo_root`.
#[derive(Debug, Default)]
pub struct FsPollState {
    last_mtime: HashMap<String, std::time::SystemTime>,
}

impl FsPollState {
    pub fn scan_changes(&mut self, repo_root: &Path) -> Vec<String> {
        let mut current = HashMap::new();
        let mut changed = Vec::new();
        collect_watched_mtimes(repo_root, repo_root, &mut current);
        for (rel, mt) in &current {
            match self.last_mtime.get(rel) {
                Some(prev) if *prev == *mt => {}
                _ => changed.push(rel.clone()),
            }
        }
        self.last_mtime = current;
        changed
    }
}

fn watched_extensions() -> Vec<String> {
    crate::language_indexer::default_indexers()
        .iter()
        .map(|i| i.file_extension().to_string())
        .collect()
}

fn collect_watched_mtimes(
    root: &Path,
    _dir: &Path,
    out: &mut HashMap<String, std::time::SystemTime>,
) {
    let exts: Vec<String> = watched_extensions();
    let ext_refs: Vec<&str> = exts.iter().map(|s| s.as_str()).collect();
    let mut paths = Vec::new();
    crate::index_walk::collect_source_files(root, &ext_refs, &mut paths);
    for p in paths {
        if let Ok(rel) = p.strip_prefix(root) {
            if let Ok(meta) = p.metadata() {
                if let Ok(mt) = meta.modified() {
                    out.insert(rel.to_string_lossy().replace('\\', "/"), mt);
                }
            }
        }
    }
}

/// How external file changes are detected (**Phase 2**).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsWatchBackend {
    /// `notify` crate (inotify / FSEvents / ReadDirectoryChangesW).
    NativeNotify,
    /// Mtime scan under `repo_root` (fallback).
    MtimePoll,
    /// Notify plus periodic mtime poll (`CIS_FS_POLL_FALLBACK=1`).
    Both,
}

/// Resolve watch backend from compile-time features and env.
///
/// | Env | Effect |
/// |-----|--------|
/// | `CIS_FS_POLL_ONLY=1` | Force mtime poll only |
/// | `CIS_FS_NOTIFY=0` | Disable native notify (requires `fs-notify` feature) |
/// | `CIS_FS_POLL_FALLBACK=1` | Notify + mtime poll |
pub fn resolve_fs_watch_backend() -> FsWatchBackend {
    if std::env::var_os("CIS_FS_POLL_ONLY").is_some_and(|v| v == "1" || v == "true") {
        return FsWatchBackend::MtimePoll;
    }
    #[cfg(feature = "fs-notify")]
    {
        if std::env::var_os("CIS_FS_NOTIFY").is_some_and(|v| v == "0" || v == "false") {
            return FsWatchBackend::MtimePoll;
        }
        if std::env::var_os("CIS_FS_POLL_FALLBACK")
            .is_some_and(|v| v == "1" || v == "true")
        {
            return FsWatchBackend::Both;
        }
        return FsWatchBackend::NativeNotify;
    }
    #[cfg(not(feature = "fs-notify"))]
    {
        let _ = ();
        FsWatchBackend::MtimePoll
    }
}

/// Process debounce-ready FS events: promote CIS writes (token present) or revert external edits.
pub fn flush_debounced_reindex(rt: &crate::mcp_runtime::CisMcpRuntime) {
    let batch = rt.index_debouncer().drain_ready();
    if batch.is_empty() {
        return;
    }
    for ev in &batch {
        // Check whether this is a CIS-originated write by probing for a pending confirm token.
        if let Some(patch_id) = rt.pending_patch_for_path(&ev.path) {
            let nonce = format!("{}", patch_id);
            let has_token = rt
                .confirm_backend()
                .read_token(&nonce)
                .unwrap_or(None)
                .is_some();
            if has_token {
                // CIS-originated write confirmed by FS event → promote
                if let Err(e) = rt.confirm_patch_internal(patch_id) {
                    eprintln!("cis-fs-sync: promote patch {} failed: {:?}", patch_id, e);
                } else {
                    eprintln!("cis-fs-sync: promoted patch {} for {}", patch_id, ev.path);
                }
                continue;
            } else {
                // FS event without matching token → external edit → revert
                eprintln!(
                    "cis-fs-sync: external edit detected for {} (patch {}), reverting",
                    ev.path, patch_id
                );
                rt.revert_patch_internal(patch_id);
            }
        }
    }
    // Normal re-index for events that weren't handled via confirm/revert.
    // `reindex_python_paths` now accepts all registered language extensions.
    let path_refs: Vec<&str> = batch
        .iter()
        .map(|e| e.path.as_str())
        .filter(|p| crate::language_indexer::path_is_indexable(p))
        .collect();
    if path_refs.is_empty() {
        return;
    }
    match rt.reindex_python_paths(&path_refs) {
        Ok(rep) if rep.applied > 0 => {
            rt.record_reindex_batch(rep.applied);
            let paths: Vec<String> = path_refs.iter().map(|p| (*p).to_string()).collect();
            rt.mark_paths_indexed(&paths);
            eprintln!(
                "cis-fs-sync: re-indexed {} file(s) (debounced)",
                rep.applied
            );
        }
        Err(e) => eprintln!("cis-fs-sync: reindex error: {:?}", e),
        _ => {}
    }
}

/// Sample a subset of indexed paths for disk-vs-graph mtime drift (**Phase 5.1**).
fn sample_missed_events(rt: &crate::mcp_runtime::CisMcpRuntime, repo_root: &Path, debounce: Duration) {
    use std::time::UNIX_EPOCH;

    let branch = rt.active_branch();
    let paths: Vec<String> = {
        let g = rt.coordinator().graph().read();
        g.active_file_paths_on_branch(branch).into_iter().collect()
    };
    if paths.is_empty() {
        return;
    }
    let sample_k = 20.min(paths.len());
    let step = (paths.len() / sample_k).max(1);
    let slack_ms = debounce.as_millis() as u64 + 500;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    rt.watcher_metrics().set_last_sample_at_ms(now_ms);

    for (i, rel) in paths.iter().enumerate().filter(|(i, _)| i % step == 0).take(sample_k) {
        let _ = i;
        let disk_path = repo_root.join(rel);
        let Ok(meta) = std::fs::metadata(&disk_path) else {
            continue;
        };
        let Ok(disk_mtime) = meta.modified() else {
            continue;
        };
        let Some(indexed_mtime) = rt.last_indexed_mtime(rel) else {
            continue;
        };
        let Ok(disk_ms) = disk_mtime.duration_since(UNIX_EPOCH) else {
            continue;
        };
        let Ok(idx_ms) = indexed_mtime.duration_since(UNIX_EPOCH) else {
            continue;
        };
        if disk_ms.as_millis() as u64 > idx_ms.as_millis() as u64 + slack_ms {
            rt.watcher_metrics().record_missed_sample();
            eprintln!(
                "cis-fs-sync: possible missed event for {} (disk newer than last indexed)",
                rel
            );
        }
    }
}

/// Debounce drain + incremental re-index loop. When `enable_poll` is false, only the debouncer
/// is drained (events must come from **`spawn_notify_watcher`**).
pub fn run_fs_sync_loop(
    rt: Arc<crate::mcp_runtime::CisMcpRuntime>,
    config: FsSyncConfig,
    enable_poll: bool,
) {
    let root = PathBuf::from(rt.repo_root());
    let branch = rt.default_branch();
    let mut poll = FsPollState::default();
    if enable_poll {
        let _ = poll.scan_changes(&root);
    }

    let mut tick: u64 = 0;
    let mut git_head_mtime: Option<std::time::SystemTime> = None;
    let sample_every = (300_000 / config.poll_interval.as_millis().max(1)) as u64;
    loop {
        if let Some(hb) = rt.worker_heartbeats() {
            hb.tick("cis-fs-sync");
        }
        std::thread::sleep(config.poll_interval.min(config.debounce));
        if enable_poll {
            let changes = poll.scan_changes(&root);
            if !changes.is_empty() {
                rt.watcher_metrics()
                    .record_raw_events(changes.len() as u64);
            }
            for rel in changes {
                rt.index_debouncer().schedule(IndexEvent {
                    branch_id: branch,
                    path: rel,
                    kind: FsChangeKind::Modified,
                    old_path: None,
                });
            }
        }
        rt.poll_git_branch_sync(&mut git_head_mtime);
        flush_debounced_reindex(rt.as_ref());
        tick = tick.wrapping_add(1);
        if sample_every > 0 && tick % sample_every.max(1) == 0 {
            sample_missed_events(rt.as_ref(), &root, config.debounce);
        }
        let ticks_per_sweep = (30_000 / config.poll_interval.as_millis().max(1)) as u64;
        if tick % ticks_per_sweep.max(1) == 0 {
            let swept = rt.sweep_speculative_orphans(30);
            if swept > 0 {
                eprintln!("cis-fs-sync: swept {} expired speculative patch(es)", swept);
            }
        }
    }
}

/// Start native watcher thread + debounce re-index loop (**`cis-mcp`** default when `fs-notify` is enabled).
pub fn spawn_fs_sync_stack(rt: Arc<crate::mcp_runtime::CisMcpRuntime>, config: FsSyncConfig) {
    let mut backend = resolve_fs_watch_backend();
    let mut enable_poll = matches!(backend, FsWatchBackend::MtimePoll | FsWatchBackend::Both);

    #[cfg(feature = "fs-notify")]
    if matches!(backend, FsWatchBackend::NativeNotify | FsWatchBackend::Both) {
        match spawn_notify_watcher(
            rt.repo_root().to_string(),
            Arc::clone(rt.index_debouncer()),
            rt.default_branch(),
        ) {
            Ok(()) => {
                eprintln!("cis-fs-sync: native notify watcher active");
            }
            Err(e) => {
                eprintln!(
                    "cis-fs-sync: notify unavailable ({e}), falling back to mtime poll"
                );
                backend = FsWatchBackend::MtimePoll;
                enable_poll = true;
            }
        }
    }

    let mode_label = match backend {
        FsWatchBackend::NativeNotify => "notify",
        FsWatchBackend::MtimePoll => "poll",
        FsWatchBackend::Both => "notify+poll",
    };
    eprintln!(
        "cis-fs-sync: mode={mode_label} debounce={:?} interval={:?}",
        config.debounce, config.poll_interval
    );

    let rt_loop = Arc::clone(&rt);
    std::thread::Builder::new()
        .name("cis-fs-sync".into())
        .spawn(move || run_fs_sync_loop(rt_loop, config, enable_poll))
        .expect("spawn cis-fs-sync");
}

#[cfg(feature = "fs-notify")]
fn notify_kind_to_fs_change(kind: notify::EventKind) -> FsChangeKind {
    use notify::event::ModifyKind;
    match kind {
        notify::EventKind::Create(_) => FsChangeKind::Created,
        notify::EventKind::Modify(ModifyKind::Name(_)) => FsChangeKind::Renamed,
        notify::EventKind::Modify(_) => FsChangeKind::Modified,
        notify::EventKind::Remove(_) => FsChangeKind::Deleted,
        _ => FsChangeKind::Modified,
    }
}

#[cfg(feature = "fs-notify")]
fn notify_err(e: notify::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
}

#[cfg(feature = "fs-notify")]
fn run_notify_blocking(
    repo_root: String,
    debouncer: Arc<IndexDebouncer>,
    branch: BranchId,
) -> std::io::Result<()> {
    use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};

    let root = repo_root.clone();
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            let Ok(event) = res else {
                return;
            };
            let kind = notify_kind_to_fs_change(event.kind);
            let watched = watched_extensions();
            let rels: Vec<String> = event
                .paths
                .iter()
                .filter_map(|p| {
                    let ext = p.extension().and_then(|x| x.to_str())?;
                    if !watched.iter().any(|w| w == ext) {
                        return None;
                    }
                    let Ok(rel) = p.strip_prefix(&root) else {
                        return None;
                    };
                    let rel = rel.to_string_lossy().replace('\\', "/");
                    if !crate::language_indexer::path_is_indexable(&rel) {
                        return None;
                    }
                    Some(rel)
                })
                .collect();
            if matches!(kind, FsChangeKind::Renamed | FsChangeKind::Moved) && rels.len() >= 2 {
                // notify typically lists old path then new path.
                debouncer.schedule(IndexEvent {
                    branch_id: branch,
                    path: rels[1].clone(),
                    kind,
                    old_path: Some(rels[0].clone()),
                });
            } else {
                for rel in rels {
                    debouncer.schedule(IndexEvent {
                        branch_id: branch,
                        path: rel,
                        kind,
                        old_path: None,
                    });
                }
            }
        },
        Config::default(),
    )
    .map_err(notify_err)?;
    watcher
        .watch(Path::new(&repo_root), RecursiveMode::Recursive)
        .map_err(notify_err)?;
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Spawn a dedicated thread holding the **`notify`** watcher (must stay alive).
#[cfg(feature = "fs-notify")]
pub fn spawn_notify_watcher(
    repo_root: String,
    debouncer: Arc<IndexDebouncer>,
    branch: BranchId,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("cis-fs-notify".into())
        .spawn(move || {
            if let Err(e) = run_notify_blocking(repo_root, debouncer, branch) {
                eprintln!("cis-fs-notify: watcher exited: {e}");
            }
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn debouncer_coalesces_rapid_schedules() {
        let d = IndexDebouncer::new(Duration::from_millis(200));
        let branch = BranchId([0u8; 16]);
        for _ in 0..5 {
            d.schedule(IndexEvent {
                branch_id: branch,
                path: "a.py".into(),
                kind: FsChangeKind::Modified,
                old_path: None,
            });
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(d.pending_count(), 1);
        assert!(d.drain_ready().is_empty());
        thread::sleep(Duration::from_millis(220));
        let ready = d.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].path, "a.py");
    }

    #[test]
    fn reindex_persist_env_gate() {
        std::env::set_var("CIS_REINDEX_PERSIST", "0");
        assert!(!reindex_persist_snapshots_enabled());
        std::env::remove_var("CIS_REINDEX_PERSIST");
        assert!(reindex_persist_snapshots_enabled());
        std::env::set_var("CIS_REINDEX_PERSIST", "1");
        assert!(reindex_persist_snapshots_enabled());
        std::env::remove_var("CIS_REINDEX_PERSIST");
    }

    #[test]
    fn flush_all_immediate() {
        let d = IndexDebouncer::new(Duration::from_secs(60));
        let branch = BranchId([0u8; 16]);
        d.schedule(IndexEvent {
            branch_id: branch,
            path: "b.py".into(),
            kind: FsChangeKind::Modified,
            old_path: None,
        });
        let all = d.flush_all();
        assert_eq!(all.len(), 1);
        assert_eq!(d.pending_count(), 0);
    }
}
