//! **`CisMcpRuntime`** — shared graph + policy + auth/audit for MCP tools (**FR-4**, **FR-4.5**, **FR-4.11**).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use cis_wal::{BranchId, IdentityId, MergeId, MutationKind, MutationLogStore, NodeRevisionId};

use crate::body_store::BodyStore;
use crate::branch_registry::BranchRegistry;
use crate::confirm_token::{clear_token_with_retry, write_token_with_retry};
use crate::embedder::{api_embedder_configured, embedder_from_env, Embedder};
use crate::graph::{EdgeType, GraphEdge, InMemoryGraph, NodeRevision, RevisionStatus, SourceSpan};
use crate::graph_mutation::GraphMutationSet;
use crate::merge_control::{MergeCancelReport, MergeControl};
use crate::merge_gate::MergeRecoveryGate;
use crate::merge_lock::{
    merge_lock_holder, release_merge_lock, sweep_all_expired_merge_intents,
};
use crate::merge_preflight::{MergePreflight, MergePreflightError};
use crate::coordinator::{CoordinatorPersistence, WriteCoordinator};
use crate::optimistic_patcher::{OptimisticPatchError, OptimisticPatcher};
use crate::persistence::cis_dir;
use crate::path_lease::{LeaseError, PathLeaseManager, SessionId, SpeculativePathTracker};
use crate::pre_write_snapshot::PreWriteSnapshotStore;
use crate::policy_watcher::ActiveRankingPolicy;
use crate::query_context::{
    build_context_truncated, hybrid_search_rerank, CharApproxTokenizer, HybridSearchCandidate,
    QueryMeta,
};
use crate::revision_cow::RevisionIndexCow;
use crate::saga::{MergeSagaOrchestrator, SagaPhase};
use crate::security::{
    AuthError, AuthProvider, ProductionAuditSink, QuotaGuard, QuotaTracker, Session,
    verify_audit_chain,
};
use crate::daemon_handles::CisDaemonHandles;
use crate::consistency_snapshot::{ConsistencyStatusSummary, LastConsistencySnapshot};
use crate::degraded::{disk_free_percent, DiskPressureFlag, VectorDegradedController};
use crate::embedding_metrics::{EmbeddingMetrics, EmbeddingStatusSnapshot};
use crate::graph_consistency::check_consistency;
use crate::watcher_metrics::{WatcherMetrics, WatcherStatusSnapshot};
use crate::worker_heartbeats::WorkerHeartbeats;
use crate::vector_store::InMemoryVectorStore;
use crate::MemoryKv;

/// One symbol match for **`find_symbol`** (MCP text payload).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SymbolHit {
    pub identity_id_hex: String,
    pub revision_id_hex: String,
    pub qualified_name: String,
    pub file_path: String,
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
    /// Per-node retrieval confidence (**§01.1 / AC-01.1**).
    pub confidence: f64,
    /// Present when the hit comes from a graph edge (`get_dependencies`, `get_callers`, …).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct FindSymbolResponse {
    pub matches: Vec<SymbolHit>,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct BuildContextResponse {
    pub text: String,
    pub truncated: bool,
    pub omitted_tokens: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct GetSymbolBodyResponse {
    pub text: String,
    pub truncated: bool,
    pub omitted_tokens: usize,
    pub qualified_name: String,
    pub file_path: String,
    pub revision_id_hex: String,
    pub source: crate::symbol_body::BodySource,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct HybridSearchResponse {
    pub ranked: Vec<HybridSearchCandidate>,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct GoToDefinitionResponse {
    pub target: Option<SymbolHit>,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct SymbolHitsResponse {
    pub hits: Vec<SymbolHit>,
    pub meta: QueryMeta,
}

/// **`file_imports`** — import edges from a repo-relative file path (file hub).
#[derive(Debug, serde::Serialize)]
pub struct FileImportsResponse {
    pub file_path: String,
    pub revision_id_hex: String,
    pub hits: Vec<SymbolHit>,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct SemanticSearchHit {
    pub qualified_name: String,
    pub score: f64,
    pub revision_id_hex: String,
}

#[derive(Debug, serde::Serialize)]
pub struct SemanticSearchResponse {
    pub hits: Vec<SemanticSearchHit>,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct WriteFileResponse {
    pub path: String,
    pub patch_id: u64,
    pub bytes_written: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct ConfirmPatchResponse {
    pub patch_id: u64,
    pub paths_promoted: Vec<String>,
    pub revisions_activated: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct RevertPatchResponse {
    pub patch_id: u64,
    pub paths_reverted: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct AxisWeightBreakdown {
    pub structural_proximity: f64,
    pub edge_type: f64,
    pub semantic_similarity: f64,
    pub recency: f64,
}

#[derive(Debug, serde::Serialize)]
pub struct ExplainCounts {
    pub stale_count: usize,
    pub speculative_count: usize,
    pub pruned_low_confidence_count: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct ExplainContextResponse {
    pub revision_id_hex: String,
    pub qualified_name: String,
    pub budget_tokens: usize,
    pub axis_weights: AxisWeightBreakdown,
    pub hybrid_search: serde_json::Value,
    pub counts: ExplainCounts,
    pub notes: String,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct ReindexPathsResponse {
    pub branch_id_hex: String,
    pub applied: usize,
    pub parse_errors: usize,
    /// Speculative patches touching `paths` that were promoted and had confirm sidecars cleared.
    pub patches_confirmed: usize,
    /// Whether `.cis/` graph/vector/kv snapshots were written during this reindex.
    pub persisted_snapshots: bool,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct SweepConfirmSidecarsResponse {
    pub sidecars_removed: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct PurgeBranchResponse {
    pub branch_id_hex: String,
    pub keys_deleted: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct CreateBranchResponse {
    pub branch_id_hex: String,
    pub branch_name: String,
    pub parent_branch_name: String,
    pub bindings_copied: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct SwitchBranchResponse {
    pub branch_id_hex: String,
    pub branch_name: String,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct ListBranchesResponse {
    pub branches: Vec<BranchInfo>,
    pub active_branch_name: String,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct ListMergeMetricsResponse {
    pub records: Vec<crate::merge_metrics::MergeMetricsRecord>,
    pub rollup: crate::merge_metrics::MergeMetricsRollup,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct BranchInfo {
    pub name: String,
    pub branch_id_hex: String,
}

#[derive(Debug, serde::Serialize)]
pub struct SaveWorkspaceResponse {
    pub cis_dir: String,
    pub stale_confirm_sidecars_removed: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct MergeBranchResponse {
    pub merge_id_hex: String,
    pub source_branch_hex: String,
    pub target_branch_hex: String,
    pub saga_phase: String,
    /// When false, MCP progress streaming (**FR-4.8**) is not available; check **`meta.degraded_modes`**.
    pub streaming: bool,
    pub message: String,
    pub meta: QueryMeta,
    /// Phase A classification summary: how many identities were auto-resolved.
    pub resolved_count: usize,
    /// Phase A: conflicts requiring resolution.
    pub conflicts: Vec<String>,
    /// Phase A: detected renames.
    pub rename_detections: Vec<String>,
    /// Phase B: number of revisions promoted to the target branch.
    pub promoted_count: usize,
    /// Phase B: number of losing revisions orphaned.
    pub orphaned_count: usize,
    /// Phase C: signature drift warnings.
    pub signature_drift: Vec<String>,
    /// Phase C: dangling edges removed.
    pub dangling_edges_removed: usize,
    /// Phase C: cardinality violations.
    pub cardinality_violations: Vec<String>,
    /// Phase C: revisions needing edge re-extraction from source bodies.
    pub needs_edge_regen_count: usize,
    /// Phase C: revisions whose edges were rebuilt from stored bodies.
    pub edges_regenerated: usize,
    /// Phase C: caller edges reresolved after signature drift.
    pub signature_reresolved: usize,
    /// Phase timeline: ordered progress events with elapsed_ms from merge start.
    pub progress: Vec<MergeProgressEvent>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MergeProgressEvent {
    pub phase: String,
    pub elapsed_ms: u64,
    pub detail: String,
}

/// **FR-4.8** — receive live merge phase updates (e.g. MCP `notifications/progress`).
pub trait MergeProgressSink {
    fn on_progress(&mut self, event: &MergeProgressEvent, step: u32, total: u32);
}

/// Saga phases reported to progress consumers during `merge_branch`.
pub const MERGE_PROGRESS_TOTAL: u32 = 5;

fn report_merge_progress(
    progress: &mut Vec<MergeProgressEvent>,
    sink: &mut Option<&mut dyn MergeProgressSink>,
    step: u32,
    t0: Instant,
    phase: &str,
    detail: impl Into<String>,
) {
    let event = MergeProgressEvent {
        phase: phase.into(),
        elapsed_ms: t0.elapsed().as_millis() as u64,
        detail: detail.into(),
    };
    if let Some(s) = sink.as_deref_mut() {
        s.on_progress(&event, step, MERGE_PROGRESS_TOTAL);
    }
    progress.push(event);
}

#[derive(Debug, serde::Serialize)]
pub struct IngestCisConfigResponse {
    pub grammar_lock_path: String,
    pub grammar_lock_parsed: bool,
    pub grammar_python: Option<String>,
    pub grammar_lock_status: String,
    pub grammar_lock_error: Option<String>,
    pub ranking_policy_path: String,
    pub ranking_policy_applied: bool,
    pub ranking_policy_status: String,
    pub ranking_policy_error: Option<String>,
    pub meta: QueryMeta,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexStatusResponse {
    pub last_index_epoch_ms: Option<u64>,
    pub files_scanned: usize,
    pub symbols_indexed: usize,
    pub edges_indexed: usize,
    pub parse_error_count: usize,
    pub ingest_mode: String,
    pub embeddings_indexed: usize,
    pub embeddings_stale: usize,
    pub embed_model_id: String,
    pub ann_index_size: usize,
    pub embed_chunks_registered: usize,
    pub unique_vectors: usize,
    pub body_blobs_stored: usize,
    pub quota_active: u32,
    pub quota_max: u32,
    pub embedding_queue_depth: usize,
    pub embedding_queue_state: String,
    pub embedding_queue_hwm: u32,
    pub embedding_queue_lwm: u32,
    pub embedding_drains_per_minute: f64,
    pub embedding_embedded_per_minute: f64,
    pub watcher_raw_events: u64,
    pub watcher_coalesced_events: u64,
    pub watcher_reindexed_files: u64,
    pub watcher_pending_count: usize,
    pub watcher_debounce_p50_ms: Option<u64>,
    pub watcher_debounce_p99_ms: Option<u64>,
    pub watcher_missed_samples: u64,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct EmbeddingStatusResponse {
    pub embedding: EmbeddingStatusSnapshot,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct DiskStatus {
    pub under_pressure: bool,
    pub free_percent: f64,
    pub min_free_percent: f64,
}

#[derive(Debug, serde::Serialize)]
pub struct QuotaStatus {
    pub active: u32,
    pub max: u32,
}

#[derive(Debug, serde::Serialize)]
pub struct VectorStatus {
    pub degraded: bool,
    pub queue_depth: usize,
    pub queue_state: String,
}

#[derive(Debug, serde::Serialize)]
pub struct ReconciliationStatus {
    pub active_branch_pending: bool,
    pub pending_branches: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct SystemStatusResponse {
    pub healthy: bool,
    pub degraded_modes: Vec<String>,
    pub disk: DiskStatus,
    pub quota: QuotaStatus,
    pub vector: VectorStatus,
    pub embedding: EmbeddingStatusSnapshot,
    pub watcher: WatcherStatusSnapshot,
    pub reconciliation: ReconciliationStatus,
    pub consistency: ConsistencyStatusSummary,
    pub background_workers: crate::worker_heartbeats::WorkerHeartbeatSummary,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct VerifyAuditChainResponse {
    pub ok: bool,
    pub chain_length: usize,
    pub broken_at_epoch: Option<u64>,
    pub message: String,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct GraphConsistencyResponse {
    pub clean: bool,
    pub summary: String,
    pub dangling_bindings: usize,
    pub orphaned_active: usize,
    pub tombstones_still_bound: usize,
    pub duplicate_active: usize,
    pub missing_body: usize,
    pub index_desync: usize,
    pub meta: QueryMeta,
}

#[derive(Debug, serde::Serialize)]
pub struct WhyNoDefinitionResponse {
    pub revision_id_hex: String,
    pub qualified_name: String,
    pub candidate_target_identities: usize,
    pub reasons: Vec<String>,
    pub meta: QueryMeta,
}

#[derive(Debug, Clone, Default)]
struct IndexStatusSnapshot {
    last_index_epoch_ms: Option<u64>,
    files_scanned: usize,
    symbols_indexed: usize,
    edges_indexed: usize,
    parse_error_count: usize,
    embeddings_indexed: usize,
    embeddings_stale: usize,
    embed_model_id: String,
    ann_index_size: usize,
    embed_chunks_registered: usize,
    unique_vectors: usize,
    body_blobs_stored: usize,
}

fn active_ingest_mode() -> &'static str {
    if cfg!(feature = "tree-sitter") {
        "tree_sitter"
    } else {
        "regex"
    }
}

fn structural_substring_score(needle: &str, qualified_name: &str) -> f64 {
    let qn = qualified_name.to_lowercase();
    if needle.is_empty() {
        return 0.0;
    }
    if !qn.contains(needle) {
        return 0.0;
    }
    let c = qn.match_indices(needle).count() as f64;
    c + (qn.len() as f64).recip()
}

fn hex16(b: &[u8; 16]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_revision_hex(s: &str) -> Option<NodeRevisionId> {
    let t = s.trim();
    if t.len() != 32 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&t[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(NodeRevisionId(b))
}

fn parse_merge_id_hex(s: &str) -> Option<MergeId> {
    parse_revision_hex(s).map(|r| MergeId(r.0))
}

fn revision_index_for_branch(
    parent: std::sync::Arc<RevisionIndexCow>,
    branch: BranchId,
    kv: std::sync::Arc<MemoryKv>,
) -> std::sync::Arc<RevisionIndexCow> {
    if branch == parent.branch_id() {
        return parent;
    }
    RevisionIndexCow::root_hydrated(branch, kv)
}

fn parse_branch_id_hex(s: &str) -> Option<BranchId> {
    let t = s.trim();
    if t.len() != 32 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&t[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(BranchId(b))
}

fn parse_identity_hex_local(s: &str) -> Option<IdentityId> {
    let t = s.trim();
    if t.len() != 32 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&t[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(IdentityId(b))
}

fn merge_id_from_now() -> MergeId {
    let n = now_ms();
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&n.to_le_bytes());
    b[8..16].copy_from_slice(&n.to_be_bytes());
    MergeId(b)
}

fn edge_type_label(ty: EdgeType) -> String {
    serde_json::to_value(ty)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| format!("{ty:?}"))
}

fn hit_from_span(rev: &NodeRevision, span: SourceSpan, confidence: f64) -> SymbolHit {
    SymbolHit {
        identity_id_hex: hex16(&rev.identity_id.0),
        revision_id_hex: hex16(&rev.revision_id.0),
        qualified_name: rev.qualified_name.clone(),
        file_path: rev.file_path.clone(),
        start_line: span.start_line,
        start_col: span.start_col,
        end_line: span.end_line,
        end_col: span.end_col,
        confidence,
        edge_type: None,
    }
}

fn hit_from_rev(rev: &NodeRevision, confidence: f64) -> SymbolHit {
    hit_from_span(rev, rev.span, confidence)
}

fn hit_from_rev_and_edge(rev: &NodeRevision, edge: &GraphEdge, confidence: f64) -> SymbolHit {
    let span = if edge.anchor.is_unknown() {
        rev.span
    } else {
        edge.anchor
    };
    let mut hit = hit_from_span(rev, span, confidence);
    hit.edge_type = Some(edge_type_label(edge.ty));
    hit
}

fn eto_for_runtime(kv: &std::sync::Arc<MemoryKv>) -> crate::edge_target_override::EdgeTargetOverrideStore {
    crate::edge_target_override::EdgeTargetOverrideStore::new(std::sync::Arc::clone(kv))
}

fn query_now_ms() -> u64 {
    now_ms()
}

fn policy_half_life_ms(policy: &crate::ranking_policy::RankingPolicySnapshot) -> u64 {
    policy.recency.half_life_days as u64 * 24 * 60 * 60 * 1000
}

fn resolve_target_revision(
    target: &str,
    branch: BranchId,
    overlay: &RevisionIndexCow,
    g: &InMemoryGraph,
) -> Option<NodeRevisionId> {
    let t = target.trim();
    if let Some(rid) = parse_revision_hex(t) {
        return overlay
            .resolved_bindings()
            .iter()
            .any(|(_, r)| *r == rid)
            .then_some(rid);
    }
    for (_, rid) in overlay.resolved_bindings() {
        let Some(rev) = g.get_revision(rid) else {
            continue;
        };
        if rev.branch_id == branch && rev.qualified_name.contains(t) {
            return Some(rid);
        }
    }
    None
}

fn resolve_repo_path(repo_root: &str, rel: &str) -> Result<std::path::PathBuf, AuthError> {
    if rel.contains("..") {
        return Err(AuthError::Forbidden);
    }
    let root = Path::new(repo_root);
    let p = if Path::new(rel).is_absolute() {
        std::path::PathBuf::from(rel)
    } else {
        root.join(rel)
    };
    let root_canon = root.canonicalize().map_err(|_| AuthError::Forbidden)?;
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).map_err(|_| AuthError::Forbidden)?;
    }
    let abs = if p.exists() {
        p.canonicalize().map_err(|_| AuthError::Forbidden)?
    } else {
        let parent = p.parent().ok_or(AuthError::Forbidden)?;
        let parent_canon = if parent.as_os_str().is_empty() {
            root_canon.clone()
        } else {
            parent.canonicalize().map_err(|_| AuthError::Forbidden)?
        };
        if !parent_canon.starts_with(&root_canon) {
            return Err(AuthError::Forbidden);
        }
        let name = p.file_name().ok_or(AuthError::Forbidden)?;
        parent_canon.join(name)
    };
    if !abs.starts_with(&root_canon) {
        return Err(AuthError::Forbidden);
    }
    Ok(abs)
}

fn is_probably_unified_diff(s: &str) -> bool {
    let t = s.trim_start();
    t.starts_with("--- ")
        || t.starts_with("diff ")
        || t.starts_with("*** ")
        || t.contains("\n@@ ")
}

fn patch_err(e: OptimisticPatchError) -> AuthError {
    match e {
        OptimisticPatchError::Lease(LeaseError::Conflict { .. }) => AuthError::Forbidden,
        OptimisticPatchError::MergeLocked => AuthError::Forbidden,
        _ => AuthError::InvalidInput,
    }
}

pub struct CisMcpRuntime {
    repo_root: String,
    /// Shared ingest + query graph (single coordinator per MCP process).
    coordinator: std::sync::Arc<WriteCoordinator>,
    policy: ActiveRankingPolicy,
    auth: AuthProvider,
    audit: std::sync::Arc<ProductionAuditSink>,
    disk_pressure: std::sync::Arc<DiskPressureFlag>,
    vector_degraded: std::sync::Arc<VectorDegradedController>,
    quota: std::sync::Arc<QuotaTracker>,
    kv: std::sync::Arc<MemoryKv>,
    branch_registry: BranchRegistry,
    active_branch_name: std::sync::RwLock<String>,
    active_branch_id: std::sync::RwLock<BranchId>,
    revision_index: std::sync::RwLock<std::sync::Arc<RevisionIndexCow>>,
    body_store: BodyStore,
    merge: MergeControl,
    #[allow(dead_code)] // Retained for direct lease APIs; [`OptimisticPatcher`] holds clones.
    leases: std::sync::Arc<PathLeaseManager>,
    spec_paths: std::sync::Arc<SpeculativePathTracker>,
    patcher: OptimisticPatcher,
    index_status: std::sync::Mutex<IndexStatusSnapshot>,
    /// **FR-1.2 / FR-1.3** — when false, responses include **`lsp_unavailable`** in **`meta.degraded_modes`**.
    lsp_integration_active: std::sync::atomic::AtomicBool,
    /// **Phase 2** — debounced external FS index events.
    index_debouncer: std::sync::Arc<crate::fs_sync::IndexDebouncer>,
    /// **Epic 3.3** — confirm token backend chosen at startup (sidecar or xattr).
    confirm_backend: std::sync::Arc<Box<dyn crate::confirm_token::ConfirmTokenBackend>>,
    /// **Phase 0** — pre-write byte snapshots for revert disk restore.
    pre_write_snapshots: PreWriteSnapshotStore,
    /// **Phase 6** — embedding computation (API or stub).
    embedder: std::sync::Arc<dyn Embedder>,
    /// **Phase 7C** — flat ANN index over embeddings (rebuilt on commit).
    ann_index: std::sync::Mutex<crate::semantic_ann::FlatAnnIndex>,
    /// **Phase 8A** — pluggable body blob backend (file or sqlite).
    body_blob_store: std::sync::Arc<dyn crate::body_blob::BodyBlobStore>,
    reconciliation_tracker: crate::branch_reconciliation_tracker::BranchReconciliationTracker,
    embedding_metrics: Arc<EmbeddingMetrics>,
    watcher_metrics: Arc<WatcherMetrics>,
    last_indexed_mtimes: Arc<Mutex<HashMap<String, SystemTime>>>,
    last_consistency: LastConsistencySnapshot,
    worker_heartbeats: Arc<WorkerHeartbeats>,
}

impl CisMcpRuntime {
    /// Dev/test MCP runtime for `repo_root`.
    ///
    /// Environment:
    /// - `CIS_WAL_MEMORY=1` — in-memory WAL (avoids `.cis/wal.json` lock contention in parallel tests).
    /// - `CIS_SKIP_WORKSPACE_LOAD=1` — skip loading `.cis/graph.json` / `kv.json` on startup (use with bootstrap).
    /// - `CIS_SKIP_MERGE_RECOVER=1` — skip `recover_inflight_merges` during load (tests only).
    pub fn new_dev(repo_root: &str) -> Self {
        Self::new_dev_with_options(repo_root, None, None)
    }

    /// Test/dev runtime with a custom confirm-token backend (**Phase 0** tests).
    #[doc(hidden)]
    pub fn new_dev_with_confirm_backend(
        repo_root: &str,
        confirm_backend: std::sync::Arc<Box<dyn crate::confirm_token::ConfirmTokenBackend>>,
    ) -> Self {
        Self::new_dev_with_options(repo_root, Some(confirm_backend), None)
    }

    /// Test/dev runtime with fault injection (**Phase 4**).
    #[doc(hidden)]
    pub fn new_dev_with_fault_injector(
        repo_root: &str,
        injector: std::sync::Arc<dyn crate::fault_injection::FaultInjector>,
    ) -> Self {
        Self::new_dev_with_options(repo_root, None, Some(injector))
    }

    fn new_dev_with_options(
        repo_root: &str,
        confirm_backend: Option<std::sync::Arc<Box<dyn crate::confirm_token::ConfirmTokenBackend>>>,
        fault_injector: Option<std::sync::Arc<dyn crate::fault_injection::FaultInjector>>,
    ) -> Self {
        let repo_root = std::fs::canonicalize(repo_root)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| repo_root.to_string());
        let injector = fault_injector
            .or_else(crate::fault_injection::injector_from_env)
            .unwrap_or_else(|| std::sync::Arc::new(crate::fault_injection::NoOpFaultInjector));
        let kv = std::sync::Arc::new(MemoryKv::new());
        kv.set_fault_injector(std::sync::Arc::clone(&injector));
        let wal: std::sync::Arc<dyn MutationLogStore> = {
            if std::env::var_os("CIS_WAL_MEMORY").is_some_and(|v| v == "1") {
                std::sync::Arc::new(cis_wal::MutationLog::new())
            } else {
                let cis = cis_dir(std::path::Path::new(&repo_root));
                let wp = crate::persistence::wal_path(&cis);
                if wp.exists() {
                    match cis_wal::DurableMutationLog::open(&wp) {
                        Ok(d) => std::sync::Arc::new(d),
                        Err(_) => std::sync::Arc::new(cis_wal::MutationLog::new()),
                    }
                } else {
                    std::sync::Arc::new(cis_wal::MutationLog::new())
                }
            }
        };
        let root_path = std::path::PathBuf::from(&repo_root);
        let cis = cis_dir(&root_path);
        let _ = std::fs::create_dir_all(&cis);
        let coordinator = std::sync::Arc::new(WriteCoordinator::open(
            wal,
            Some(CoordinatorPersistence { cis_dir: cis }),
        ));
        coordinator.set_fault_injector(std::sync::Arc::clone(&injector));
        let saga = MergeSagaOrchestrator::new(std::sync::Arc::clone(&kv));
        let _ = coordinator.reconcile_on_startup(&saga);
        let wrapped_confirm = confirm_backend.map(|b| {
            std::sync::Arc::new(Box::new(crate::confirm_token::FaultInjectingConfirmBackend::new(
                b,
                std::sync::Arc::clone(&injector),
            )) as Box<dyn crate::confirm_token::ConfirmTokenBackend>)
        });
        let rt = Self::new_with_coordinator(
            repo_root,
            coordinator,
            kv,
            wrapped_confirm,
            None,
            Some(injector),
        );
        if !std::env::var_os("CIS_SKIP_WORKSPACE_LOAD").is_some_and(|v| v == "1") {
            let rep = rt.load_persisted_workspace();
            if rep.graph_loaded {
                rt.sync_index_status_from_graph();
            }
        }
        rt
    }

    /// MCP runtime sharing **`cisd`**'s [`WriteCoordinator`] + KV (**single process**, Phase 4).
    ///
    /// Caller must have run [`WriteCoordinator::reconcile_on_startup`] and loaded `.cis` snapshots on
    /// `coordinator` (e.g. via [`crate::open_persisted_coordinator`]). This path loads **`kv.json`** and
    /// hydrates the revision index from the coordinator graph without re-opening WAL/graph.
    pub fn attach_coordinator(
        repo_root: impl AsRef<std::path::Path>,
        coordinator: std::sync::Arc<WriteCoordinator>,
        kv: std::sync::Arc<MemoryKv>,
        handles: Option<CisDaemonHandles>,
    ) -> std::sync::Arc<Self> {
        let rt = std::sync::Arc::new(Self::new_with_coordinator(
            repo_root.as_ref(),
            coordinator,
            kv,
            None,
            handles,
            None,
        ));
        let weak = std::sync::Arc::downgrade(&rt);
        rt.coordinator.set_post_embed_hook(Some(std::sync::Arc::new(move || {
            if let Some(rt) = weak.upgrade() {
                rt.rebuild_ann_index();
            }
        })));
        if !std::env::var_os("CIS_SKIP_WORKSPACE_LOAD").is_some_and(|v| v == "1") {
            let _ = rt.load_persisted_kv_and_hydrate_revision_index();
            rt.audit.resume_from_kv(rt.kv.as_ref());
            rt.sync_index_status_from_graph();
        }
        rt
    }

    fn new_with_coordinator(
        repo_root: impl AsRef<std::path::Path>,
        coordinator: std::sync::Arc<WriteCoordinator>,
        kv: std::sync::Arc<MemoryKv>,
        confirm_backend: Option<std::sync::Arc<Box<dyn crate::confirm_token::ConfirmTokenBackend>>>,
        handles: Option<CisDaemonHandles>,
        fault_injector: Option<std::sync::Arc<dyn crate::fault_injection::FaultInjector>>,
    ) -> Self {
        let repo_root = std::fs::canonicalize(repo_root.as_ref())
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| repo_root.as_ref().to_string_lossy().into_owned());
        let auth = AuthProvider::new();
        auth.register(Session {
            id: 0,
            admin: true,
            repo_roots: vec![repo_root.clone()],
        });
        let branch_registry = BranchRegistry::new(std::sync::Arc::clone(&kv));
        let main_id = branch_registry.get_or_create_id("main");
        let revision_index = std::sync::RwLock::new(RevisionIndexCow::root_hydrated(
            main_id,
            std::sync::Arc::clone(&kv),
        ));
        let leases = std::sync::Arc::new(PathLeaseManager::new());
        let spec_paths = std::sync::Arc::new(SpeculativePathTracker::new());
        let patcher =
            OptimisticPatcher::new(std::sync::Arc::clone(&leases), std::sync::Arc::clone(&spec_paths));
        let fs_cfg = crate::fs_sync::FsSyncConfig::from_env();
        let index_debouncer =
            std::sync::Arc::new(crate::fs_sync::IndexDebouncer::new(fs_cfg.debounce));
        let watcher_metrics = Arc::new(WatcherMetrics::new());
        index_debouncer.set_metrics(Arc::clone(&watcher_metrics));
        let inj = fault_injector.unwrap_or_else(|| {
            std::sync::Arc::new(crate::fault_injection::NoOpFaultInjector)
        });
        let confirm_backend = confirm_backend.unwrap_or_else(|| {
            let inner = std::sync::Arc::new(crate::confirm_token::probe_confirm_backend(
                std::path::Path::new(&repo_root),
            ));
            std::sync::Arc::new(Box::new(
                crate::confirm_token::FaultInjectingConfirmBackend::new(
                    inner,
                    std::sync::Arc::clone(&inj),
                ),
            ) as Box<dyn crate::confirm_token::ConfirmTokenBackend>)
        });
        let cis = cis_dir(std::path::Path::new(&repo_root));
        let pre_write_snapshots = PreWriteSnapshotStore::new(cis)
            .unwrap_or_else(|e| {
                eprintln!("cis-mcp: pre_write_snapshots init: {:?}; using fallback dir", e);
                PreWriteSnapshotStore::new(std::env::temp_dir()).expect("temp pre_write_snapshots")
            });
        let body_store = BodyStore::new(std::sync::Arc::clone(&kv));
        let merge = MergeControl::new(std::sync::Arc::clone(&kv));
        let embedder = embedder_from_env();
        let body_blob_store =
            crate::body_blob::open_body_blob_store(cis_dir(std::path::Path::new(&repo_root)));
        let handles = handles.unwrap_or_else(|| {
            CisDaemonHandles::for_tests(std::path::Path::new(&repo_root))
        });
        let policy_snap = ActiveRankingPolicy::with_system_default();
        coordinator.set_embedding_queue_thresholds(
            policy_snap.snapshot().embedding_queue_hwm,
            policy_snap.snapshot().embedding_queue_lwm,
        );
        Self {
            repo_root,
            coordinator,
            policy: policy_snap,
            auth,
            audit: handles.audit,
            disk_pressure: handles.disk_pressure,
            vector_degraded: handles.vector_degraded,
            quota: handles.quota,
            kv,
            branch_registry,
            active_branch_name: std::sync::RwLock::new("main".to_string()),
            active_branch_id: std::sync::RwLock::new(main_id),
            revision_index,
            body_store,
            merge,
            leases,
            spec_paths,
            patcher,
            index_status: std::sync::Mutex::new(IndexStatusSnapshot::default()),
            lsp_integration_active: AtomicBool::new(false),
            index_debouncer,
            confirm_backend,
            pre_write_snapshots,
            embedder,
            ann_index: std::sync::Mutex::new(crate::semantic_ann::FlatAnnIndex::new()),
            body_blob_store,
            reconciliation_tracker: crate::branch_reconciliation_tracker::BranchReconciliationTracker::new(),
            embedding_metrics: handles.embedding_metrics,
            watcher_metrics,
            last_indexed_mtimes: Arc::new(Mutex::new(HashMap::new())),
            last_consistency: handles.last_consistency,
            worker_heartbeats: handles.worker_heartbeats,
        }
    }

    fn cis_path(&self) -> std::path::PathBuf {
        cis_dir(std::path::Path::new(&self.repo_root))
    }

    /// **Phase 7** — GC + persist referenced bodies; rebuild ANN index.
    pub fn sync_bodies_after_commit(&self, branch: BranchId) {
        let keep = {
            let g = self.coordinator.graph().read();
            let mut keep = crate::body_blob::referenced_body_hashes(&g, branch, true);
            for r in g.revisions() {
                if r.branch_id == branch && matches!(r.status, RevisionStatus::Tombstone) {
                    keep.insert(r.body_hash);
                    if r.qualified_name == r.file_path {
                        keep.insert(crate::ingest::file_body_hash_key(&r.file_path));
                    }
                }
            }
            keep
        };
        let cis = self.cis_path();
        let _ = crate::body_blob::gc_bodies_with_store(
            self.body_blob_store.as_ref(),
            &cis,
            &self.body_store,
            self.kv.as_ref(),
            &keep,
        );
        let _ = crate::body_blob::sync_bodies_to_store(
            self.body_blob_store.as_ref(),
            &self.body_store,
            &keep,
        );
        self.rebuild_ann_index();
    }

    /// Rebuild flat ANN index from vector store embeddings.
    pub fn rebuild_ann_index(&self) {
        let vector = self.coordinator.vector();
        let snap = vector.export_snapshot();
        let mut ann = self.ann_index.lock().unwrap();
        ann.clear();
        for v in snap.vectors {
            ann.upsert(v.body_hash, v.embedding);
        }
    }

    fn resolve_revision_for_query(
        &self,
        revision_id_hex: Option<&str>,
        symbol: Option<&str>,
        branch: BranchId,
    ) -> Result<NodeRevision, AuthError> {
        let g = self.coordinator.graph().read();
        if let Some(hex) = revision_id_hex {
            if let Some(rid) = parse_revision_hex(hex) {
                if let Some(rev) = g.get_revision(rid) {
                    if rev.branch_id == branch {
                        return Ok(rev.clone());
                    }
                }
            }
        }
        if let Some(needle) = symbol {
            for rev in g.revisions() {
                if rev.branch_id == branch && rev.qualified_name.contains(needle) {
                    return Ok(rev.clone());
                }
            }
        }
        Err(AuthError::InvalidInput)
    }

    /// **Phase 7** — live symbol body with BodyStore → cis blob → disk fallback.
    pub fn get_symbol_body(
        &self,
        session_id: u64,
        revision_id_hex: Option<&str>,
        symbol: Option<&str>,
        branch_id: Option<BranchId>,
        budget_tokens: usize,
    ) -> Result<GetSymbolBodyResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let rev = self.resolve_revision_for_query(revision_id_hex, symbol, branch)?;
        let (raw, source) = crate::symbol_body::resolve_revision_body(
            &self.body_store,
            self.body_blob_store.as_ref(),
            &self.cis_path(),
            std::path::Path::new(&self.repo_root),
            &rev,
        );
        let tok = CharApproxTokenizer;
        let (text, truncated, omitted_tokens) =
            build_context_truncated(&raw, budget_tokens.max(1), &tok);
        self.audit.record_sync(session_id, "get_symbol_body");
        let meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            1,
            None,
            false,
            None,
        );
        Ok(GetSymbolBodyResponse {
            text,
            truncated,
            omitted_tokens,
            qualified_name: rev.qualified_name.clone(),
            file_path: rev.file_path.clone(),
            revision_id_hex: hex16(&rev.revision_id.0),
            source,
            meta,
        })
    }

    /// Load **`kv.json`** and bind revision index from the coordinator graph (graph already loaded).
    pub fn load_persisted_kv_and_hydrate_revision_index(
        &self,
    ) -> crate::persistence::PersistenceLoadReport {
        let mut report = crate::persistence::PersistenceLoadReport::default();
        let cis = cis_dir(std::path::Path::new(&self.repo_root));
        if !cis.is_dir() {
            return report;
        }
        let kpath = crate::persistence::kv_snapshot_path(&cis);
        if kpath.exists() {
            match crate::persistence::load_kv_snapshot(&kpath, self.kv.as_ref()) {
                Ok(n) => {
                    report.kv_loaded = true;
                    report.kv_entries = n;
                }
                Err(e) => report.kv_error = Some(e.to_string()),
            }
        }
        let g = self.coordinator.graph().read();
        report.graph_loaded = g.revision_count() > 0;
        report.graph_identities = g.identity_count();
        let bindings: Vec<_> = g
            .revisions()
            .filter(|r| r.branch_id == self.active_branch())
            .map(|r| (r.identity_id, r.revision_id))
            .collect();
        drop(g);
        for (identity_id, revision_id) in bindings {
            self.revision_index.read().unwrap().bind(identity_id, revision_id);
        }
        report
    }

    /// Refresh MCP index counters from the in-memory graph (after snapshot load or ingest).
    pub fn sync_index_status_from_graph(&self) {
        let g = self.coordinator.graph().read();
        let symbols_indexed = g.revisions().count();
        let edges_indexed: usize = g
            .revisions()
            .map(|r| g.outbound_edges(r.revision_id).len())
            .sum();
        let files: HashSet<String> = g
            .revisions()
            .map(|r| r.file_path.clone())
            .filter(|p| {
                p.ends_with(".py") || p.ends_with(".ts") || p.ends_with(".tsx")
            })
            .collect();
        let model_id = self.embedder.model_id().to_string();
        let vector = self.coordinator.vector();
        let snap = vector.export_snapshot();
        let unique_vectors = snap
            .vectors
            .iter()
            .filter(|v| v.model_id == model_id)
            .count();
        let embed_chunks_registered = snap.chunks.len();
        let mut embedded = 0usize;
        let mut stale = 0usize;
        for r in g.revisions() {
            match vector.vector_for_body(&r.body_hash) {
                Some(entry) if entry.model_id == model_id => embedded += 1,
                Some(_) | None => stale += 1,
            }
        }
        let body_blobs_stored = self
            .body_blob_store
            .list_hashes()
            .map(|h| h.len())
            .unwrap_or(0);
        drop(g);
        let ann_size = self.ann_index.lock().unwrap().len();
        let mut st = self.index_status.lock().unwrap();
        st.symbols_indexed = symbols_indexed;
        st.edges_indexed = edges_indexed;
        st.files_scanned = files.len();
        st.embeddings_indexed = embedded;
        st.embeddings_stale = stale;
        st.embed_model_id = model_id;
        st.ann_index_size = ann_size;
        st.embed_chunks_registered = embed_chunks_registered;
        st.unique_vectors = unique_vectors;
        st.body_blobs_stored = body_blobs_stored;
        if st.last_index_epoch_ms.is_none() {
            st.last_index_epoch_ms = Some(now_ms());
        }
    }

    pub fn wal(&self) -> std::sync::Arc<dyn MutationLogStore> {
        self.coordinator.wal()
    }

    pub fn coordinator(&self) -> &std::sync::Arc<WriteCoordinator> {
        &self.coordinator
    }

    /// Lease manager for hardening / invariant checks.
    #[doc(hidden)]
    pub fn leases(&self) -> &std::sync::Arc<PathLeaseManager> {
        &self.leases
    }

    /// Speculative path tracker for hardening / invariant checks.
    #[doc(hidden)]
    pub fn spec_paths(&self) -> &std::sync::Arc<SpeculativePathTracker> {
        &self.spec_paths
    }

    /// Optimistic patcher for hardening / invariant checks.
    #[doc(hidden)]
    pub fn patcher(&self) -> &OptimisticPatcher {
        &self.patcher
    }

    /// Build invariant context for this runtime.
    #[doc(hidden)]
    pub fn invariant_context<'a>(
        &'a self,
        branches: &'a [BranchId],
    ) -> crate::invariants::InvariantContext<'a> {
        crate::invariants::InvariantContext {
            graph: self.coordinator.graph(),
            kv: self.kv.as_ref(),
            body_store: &self.body_store,
            branches,
            leases: self.leases.as_ref(),
            patcher: &self.patcher,
            spec_tracker: self.spec_paths.as_ref(),
            coordinator: Some(self.coordinator.as_ref()),
        }
    }

    /// Assert all invariants hold (test helper).
    #[doc(hidden)]
    pub fn assert_invariants(&self) {
        let branch = self.active_branch();
        let branches = [branch];
        crate::invariants::assert_invariants(&self.invariant_context(&branches));
    }

    pub fn embedder(&self) -> &std::sync::Arc<dyn Embedder> {
        &self.embedder
    }

    pub fn body_store(&self) -> &BodyStore {
        &self.body_store
    }

    /// **FR-1.5** — walk `repo_root` for `*.py`, ingest via `WriteCoordinator`, replace this runtime’s graph (**§01.4** dev path).
    pub fn bootstrap_python_index_from_repo(
        &self,
    ) -> Result<crate::ingest::IngestApplyReport, crate::coordinator::CoordinatorError> {
        let rep = crate::repo_bootstrap::bootstrap_python_workspace_on_coordinator(
            self.coordinator.as_ref(),
            &self.repo_root,
            &self.kv,
            &self.revision_index_arc(),
            self.active_branch(),
        )?;
        let g = self.coordinator.graph().read();
        let symbols_indexed = g.revisions().count();
        let edges_indexed: usize = g
            .revisions()
            .map(|r| g.outbound_edges(r.revision_id).len())
            .sum();
        drop(g);
        let mut st = self.index_status.lock().unwrap();
        st.last_index_epoch_ms = Some(now_ms());
        st.files_scanned = rep.applied
            + rep.requeued_merge_lock
            + rep.skipped_non_py
            + rep.skipped_empty_py
            + rep.skipped_delete_stub
            + rep.parse_errors;
        st.symbols_indexed = symbols_indexed;
        st.edges_indexed = edges_indexed;
        st.parse_error_count = rep.parse_errors;
        self.sync_bodies_after_commit(self.active_branch());
        Ok(rep)
    }

    /// Alias for [`Self::bootstrap_python_index_from_repo`] (multi-language bootstrap entry point).
    pub fn bootstrap_index_from_repo(
        &self,
    ) -> Result<crate::ingest::IngestApplyReport, crate::coordinator::CoordinatorError> {
        self.bootstrap_python_index_from_repo()
    }

    pub fn repo_root(&self) -> &str {
        &self.repo_root
    }

    pub fn active_branch(&self) -> BranchId {
        *self.active_branch_id.read().unwrap()
    }

    /// Policy snapshot for tests / background workers.
    #[doc(hidden)]
    pub fn policy_snapshot(&self) -> crate::ranking_policy::RankingPolicySnapshot {
        self.policy.snapshot()
    }

    pub fn default_branch(&self) -> BranchId {
        self.active_branch()
    }

    fn revision_index_arc(&self) -> std::sync::Arc<RevisionIndexCow> {
        std::sync::Arc::clone(&*self.revision_index.read().unwrap())
    }

    pub fn vector_store(&self) -> &InMemoryVectorStore {
        self.coordinator.vector()
    }

    /// Load **`.cis/graph.json`**, **`vector.json`**, **`kv.json`** without re-ingest (**Phase 1**).
    pub fn load_persisted_workspace(&self) -> crate::persistence::PersistenceLoadReport {
        let rep = crate::persistence::load_workspace_into(
            std::path::Path::new(&self.repo_root),
            self.coordinator.graph(),
            self.coordinator.vector(),
            self.kv.as_ref(),
            self.revision_index.read().unwrap().as_ref(),
            self.active_branch(),
        );
        if !std::env::var_os("CIS_SKIP_MERGE_RECOVER").is_some_and(|v| v == "1") {
            let saga = MergeSagaOrchestrator::new(std::sync::Arc::clone(&self.kv));
            let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
            let mut g = self.coordinator.graph().write();
            let report = crate::merge_engine::recover_inflight_merges(
                &mut g,
                &self.kv,
                &self.body_store,
                &saga,
                &self.merge,
                self.coordinator.vector(),
                &gate,
                Some(&self.reconciliation_tracker),
            );
            for entry in report.entries {
                self.append_merge_metrics_record(crate::merge_metrics::MergeMetricsRecord {
                    merge_id_hex: hex16(&entry.merge_id.0),
                    started_at_ms: query_now_ms(),
                    phase_durations_ms: std::collections::BTreeMap::new(),
                    edges_regenerated: entry.edges_regenerated,
                    dangling_edges_removed: entry.dangling_edges_removed,
                    signature_reresolved: entry.signature_reresolved,
                    cardinality_violations: entry.cardinality_violations,
                    resumed_from_phase: entry.resumed_from_phase,
                    compensated: entry.compensated,
                });
            }
        }
        if rep.graph_loaded {
            self.sync_index_status_from_graph();
            let keep = {
                let g = self.coordinator.graph().read();
                crate::body_blob::referenced_body_hashes(&g, self.active_branch(), true)
            };
            let cis = self.cis_path();
            let _ = crate::body_blob::hydrate_bodies_from_store(
                self.body_blob_store.as_ref(),
                &cis,
                &self.body_store,
                &keep,
            );
            self.rebuild_ann_index();
        }
        rep
    }

    pub fn set_lsp_integration_active(&self, active: bool) {
        self.lsp_integration_active.store(active, Ordering::Relaxed);
    }

    pub fn graph_store(&self) -> &crate::shared_graph::SharedInMemoryGraph {
        self.coordinator.graph()
    }

    /// Alias for [`graph_store`] (legacy name).
    pub fn graph_mutex(&self) -> &crate::shared_graph::SharedInMemoryGraph {
        self.coordinator.graph()
    }

    pub fn kv(&self) -> &std::sync::Arc<MemoryKv> {
        &self.kv
    }

    pub fn revision_index(&self) -> std::sync::Arc<RevisionIndexCow> {
        self.revision_index_arc()
    }

    pub fn index_debouncer(&self) -> &std::sync::Arc<crate::fs_sync::IndexDebouncer> {
        &self.index_debouncer
    }

    pub fn watcher_metrics(&self) -> &Arc<WatcherMetrics> {
        &self.watcher_metrics
    }

    pub fn worker_heartbeats(&self) -> Option<&Arc<WorkerHeartbeats>> {
        Some(&self.worker_heartbeats)
    }

    pub fn record_reindex_batch(&self, file_count: usize) {
        self.watcher_metrics.record_reindex_batch(file_count);
    }

    pub fn mark_paths_indexed(&self, paths: &[String]) {
        let now = SystemTime::now();
        let mut g = self.last_indexed_mtimes.lock().unwrap();
        for p in paths {
            g.insert(p.clone(), now);
        }
    }

    pub fn last_indexed_mtime(&self, rel_path: &str) -> Option<SystemTime> {
        self.last_indexed_mtimes
            .lock()
            .unwrap()
            .get(rel_path)
            .copied()
    }

    pub fn update_consistency_cache(&self, report: &crate::graph_consistency::ConsistencyReport) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_consistency.update(report, now_ms);
    }

    fn embedding_status_snapshot(&self) -> EmbeddingStatusSnapshot {
        self.embedding_metrics.snapshot(
            self.coordinator.embedding_queue_depth(),
            self.coordinator.embedding_queue_state(),
            self.coordinator.embedding_queue_hwm(),
            self.coordinator.embedding_queue_lwm(),
        )
    }

    fn watcher_status_snapshot(&self) -> WatcherStatusSnapshot {
        self.watcher_metrics
            .snapshot(self.index_debouncer.pending_count())
    }

    /// Expose confirm backend for FS sync confirm/revert logic.
    pub fn confirm_backend(&self) -> &dyn crate::confirm_token::ConfirmTokenBackend {
        self.confirm_backend.as_ref().as_ref()
    }

    /// Return the patch_id of any pending speculative patch whose paths include `rel_path`.
    pub fn pending_patch_for_path(&self, rel_path: &str) -> Option<u64> {
        self.patcher.pending_patch_for_path(rel_path, &self.repo_root)
    }

    /// Promote pending speculative patches that touch any of `paths` and clear confirm sidecars.
    pub fn confirm_pending_patches_for_paths(&self, paths: &[&str]) -> usize {
        let mut seen = HashSet::new();
        let mut n = 0usize;
        for path in paths {
            let Some(patch_id) = self.pending_patch_for_path(path) else {
                continue;
            };
            if !seen.insert(patch_id) {
                continue;
            }
            if self.confirm_patch_internal(patch_id).is_ok() {
                n += 1;
            }
        }
        n
    }

    /// Remove `.cis_confirm_*` files whose patch id is no longer tracked (orphan sidecars).
    pub fn sweep_stale_confirm_sidecars(&self) -> usize {
        let root = std::path::Path::new(&self.repo_root);
        let Ok(entries) = std::fs::read_dir(root) else {
            return 0;
        };
        let mut n = 0usize;
        for ent in entries.flatten() {
            let name = ent.file_name().to_string_lossy().into_owned();
            let Some(nonce) = name.strip_prefix(".cis_confirm_") else {
                continue;
            };
            if nonce.contains(".tmp") {
                continue;
            }
            let stale = match nonce.parse::<u64>() {
                Ok(id) => self.patcher.peek_patch_paths(id).is_empty(),
                Err(_) => true,
            };
            if stale && self.confirm_backend.clear_token(nonce).is_ok() {
                n += 1;
            }
        }
        n
    }

    fn rel_path_from_abs(&self, abs: &str) -> String {
        abs.strip_prefix(&self.repo_root)
            .and_then(|s| s.strip_prefix('/'))
            .unwrap_or(abs)
            .replace('\\', "/")
    }

    fn speculative_rids_for_paths(&self, paths: &[String], branch: BranchId) -> Vec<NodeRevisionId> {
        let g = self.coordinator.graph().read();
        paths
            .iter()
            .flat_map(|p| {
                let rel = self.rel_path_from_abs(p);
                g.speculative_revision_ids_for_file(branch, &rel)
            })
            .collect()
    }

    fn mutation_checksum(rids: &[NodeRevisionId]) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, rid) in rids.iter().enumerate() {
            out[i % 32] ^= rid.0[i % 16];
        }
        out
    }

    fn wal_promote_speculative(
        &self,
        patch_id: u64,
        paths: &[String],
        branch: BranchId,
    ) -> Result<usize, crate::coordinator::CoordinatorError> {
        let rids = self.speculative_rids_for_paths(paths, branch);
        let count = rids.len();
        if rids.is_empty() {
            return Ok(0);
        }
        let set = GraphMutationSet::new(rids.clone(), Self::mutation_checksum(&rids));
        let id = self.coordinator.begin_status_mutation(
            MutationKind::PromoteSpeculative { patch_id },
            &set,
        )?;
        self.coordinator.commit_graph(id, |g| {
            for rid in &rids {
                if let Some(rev) = g.get_revision(*rid).cloned() {
                    let mut updated = rev;
                    updated.status = RevisionStatus::Active;
                    g.put_revision(updated);
                }
            }
            Ok(())
        })?;
        self.coordinator.finalize_graph_only(id)?;
        Ok(count)
    }

    fn wal_revert_speculative(
        &self,
        patch_id: u64,
        paths: &[String],
        branch: BranchId,
    ) -> Result<usize, crate::coordinator::CoordinatorError> {
        let rids = self.speculative_rids_for_paths(paths, branch);
        let count = rids.len();
        if rids.is_empty() {
            return Ok(0);
        }
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let set = GraphMutationSet::new(rids.clone(), Self::mutation_checksum(&rids));
        let id = self.coordinator.begin_status_mutation(
            MutationKind::RevertSpeculative { patch_id },
            &set,
        )?;
        self.coordinator.commit_graph(id, |g| {
            for rid in &rids {
                if let Some(rev) = g.get_revision(*rid).cloned() {
                    let mut updated = rev;
                    updated.status = RevisionStatus::Tombstone;
                    updated.tombstoned_at_ms = Some(ts);
                    g.put_revision(updated);
                }
            }
            Ok(())
        })?;
        self.coordinator.finalize_graph_only(id)?;
        Ok(count)
    }

    fn rollback_failed_speculative_write(
        &self,
        patch_id: u64,
        session_id: u64,
        rel_path: &str,
        pre_bytes: &[u8],
    ) {
        let _ = PreWriteSnapshotStore::restore_inline(
            Path::new(&self.repo_root),
            rel_path,
            pre_bytes,
        );
        self.pre_write_snapshots.clear_patch(patch_id);
        let _ = self.patcher.revert(patch_id, SessionId(session_id));
    }

    /// Internal: promote a patch directly (used by FS sync without session check).
    pub fn confirm_patch_internal(&self, patch_id: u64) -> Result<(), ()> {
        let session_id = self.patcher.patch_session(patch_id).ok_or(())?;
        let paths = self.patcher.promote(patch_id, session_id).map_err(|_| ())?;
        let nonce = format!("{}", patch_id);
        let _ = clear_token_with_retry(self.confirm_backend(), &nonce);
        let branch = self.active_branch();
        let _ = self.wal_promote_speculative(patch_id, &paths, branch).map_err(|_| ())?;
        self.pre_write_snapshots.clear_patch(patch_id);
        Ok(())
    }

    /// Internal: revert a patch without session check (used by FS sync on external-edit detection).
    pub fn revert_patch_internal(&self, patch_id: u64) {
        let Some(session_id) = self.patcher.patch_session(patch_id) else { return };
        let paths = self.patcher.peek_patch_paths(patch_id);
        if self
            .finish_revert_patch(patch_id, session_id, &paths)
            .is_err()
        {
            eprintln!("cis-mcp: revert_patch_internal failed for patch {}", patch_id);
        }
    }

    fn reindex_reverted_paths(&self, paths: &[String], branch: BranchId) {
        let rel_paths: Vec<String> = paths
            .iter()
            .map(|p| self.rel_path_from_abs(p))
            .filter(|rel| {
                crate::language_indexer::indexer_for_path(
                    rel,
                    &crate::language_indexer::default_indexers(),
                )
                .is_some()
            })
            .collect();
        if rel_paths.is_empty() {
            return;
        }
        let refs: Vec<&str> = rel_paths.iter().map(|s| s.as_str()).collect();
        if let Err(e) = self.reindex_paths_on_branch(&refs, branch) {
            eprintln!("cis-mcp: revert reindex: {:?}", e);
            self.sync_revision_index_from_graph();
        } else {
            self.sync_bodies_after_commit(branch);
        }
    }

    fn finish_revert_patch(
        &self,
        patch_id: u64,
        session_id: SessionId,
        paths: &[String],
    ) -> Result<(), crate::coordinator::CoordinatorError> {
        self.pre_write_snapshots
            .restore_patch(patch_id, Path::new(&self.repo_root))
            .map_err(|_| crate::coordinator::CoordinatorError::Persist("restore snapshot".into()))?;
        self.patcher
            .revert(patch_id, session_id)
            .map_err(|_| crate::coordinator::CoordinatorError::Graph("patcher revert"))?;
        let nonce = format!("{}", patch_id);
        let _ = clear_token_with_retry(self.confirm_backend(), &nonce);
        let branch = self.active_branch();
        self.wal_revert_speculative(patch_id, paths, branch)?;
        self.pre_write_snapshots.clear_patch(patch_id);
        self.reindex_reverted_paths(paths, branch);
        Ok(())
    }

    /// Sweep speculative patches older than `ttl_secs` and revert them.
    /// Returns the number of patches reverted.
    pub fn sweep_speculative_orphans(&self, ttl_secs: u64) -> usize {
        let expired = self.patcher.expired_patch_ids(ttl_secs);
        let count = expired.len();
        for patch_id in expired {
            self.revert_patch_internal(patch_id);
        }
        count
    }

    /// Incrementally re-index repo-relative Python paths into this runtime (**Phase 2**).
    pub fn reindex_python_paths(
        &self,
        paths: &[&str],
    ) -> Result<crate::ingest::IngestApplyReport, crate::coordinator::CoordinatorError> {
        self.reindex_python_paths_on_branch(paths, self.active_branch())
    }

    fn reindex_python_paths_on_branch(
        &self,
        paths: &[&str],
        branch: BranchId,
    ) -> Result<crate::ingest::IngestApplyReport, crate::coordinator::CoordinatorError> {
        let rename_config =
            crate::identity_resolution::RenameConfig::from_policy(&self.policy.snapshot());
        let overlay = revision_index_for_branch(
            self.revision_index_arc(),
            branch,
            std::sync::Arc::clone(&self.kv),
        );
        crate::fs_sync::reindex_python_paths_on_coordinator(
            self.coordinator.as_ref(),
            &self.repo_root,
            &self.kv,
            &overlay,
            branch,
            paths.iter().map(|p| (*p).to_string()).collect(),
            Some(rename_config),
        )
    }

    /// MCP **`reindex_paths`**: ingest specific repo-relative paths on a branch overlay.
    pub fn reindex_paths(
        &self,
        session_id: u64,
        paths: &[&str],
        branch_id: Option<BranchId>,
    ) -> Result<ReindexPathsResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let t0 = Instant::now();
        let rep = self
            .reindex_python_paths_on_branch(paths, branch)
            .map_err(|e| {
                eprintln!("cis-mcp: reindex_paths: {:?}", e);
                AuthError::InvalidInput
            })?;
        if branch == self.active_branch() {
            self.sync_index_status_from_graph();
        }
        self.sync_bodies_after_commit(branch);
        let patches_confirmed = self.confirm_pending_patches_for_paths(paths);
        let persisted_snapshots = crate::fs_sync::reindex_persist_snapshots_enabled();
        let mut meta = self.meta_at_commit(false, t0.elapsed().as_millis() as u64, 0, None, false, None);
        meta.node_count = rep.applied;
        self.audit.record_sync(session_id, "reindex_paths");
        Ok(ReindexPathsResponse {
            branch_id_hex: hex16(&branch.0),
            applied: rep.applied,
            parse_errors: rep.parse_errors,
            patches_confirmed,
            persisted_snapshots,
            meta,
        })
    }

    /// Fork `parent` bindings into a new named branch (**Phase 1.5**).
    pub fn create_branch(
        &self,
        session_id: u64,
        name: &str,
        parent: Option<&str>,
    ) -> Result<CreateBranchResponse, AuthError> {
        self.require_session(session_id)?;
        if name.trim().is_empty() {
            return Err(AuthError::InvalidInput);
        }
        let parent_name = parent
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.active_branch_name.read().unwrap().clone());
        let parent_id = self.branch_registry.get_or_create_id(&parent_name);
        let child_id = self.branch_registry.get_or_create_id(name);
        if child_id == parent_id {
            return Err(AuthError::InvalidInput);
        }
        let bindings_copied =
            crate::revision_index::fork_branch_bindings(self.kv.as_ref(), parent_id, child_id);
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.node_count = bindings_copied;
        self.audit
            .record_sync(session_id, format!("create_branch {name} from {parent_name}"));
        Ok(CreateBranchResponse {
            branch_id_hex: hex16(&child_id.0),
            branch_name: name.to_string(),
            parent_branch_name: parent_name,
            bindings_copied,
            meta,
        })
    }

    /// Set the active branch for subsequent default-branch MCP calls (**Phase 1.5 / 2.3**).
    pub fn switch_branch(
        &self,
        session_id: u64,
        name: &str,
    ) -> Result<SwitchBranchResponse, AuthError> {
        self.require_session(session_id)?;
        if name.trim().is_empty() {
            return Err(AuthError::InvalidInput);
        }
        let id = self.branch_registry.get_or_create_id(name);
        *self.active_branch_name.write().unwrap() = name.to_string();
        *self.active_branch_id.write().unwrap() = id;
        *self.revision_index.write().unwrap() =
            RevisionIndexCow::root_hydrated(id, std::sync::Arc::clone(&self.kv));
        self.sync_index_status_from_graph();
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        self.audit.record_sync(session_id, format!("switch_branch {name}"));
        Ok(SwitchBranchResponse {
            branch_id_hex: hex16(&id.0),
            branch_name: name.to_string(),
            meta,
        })
    }

    /// List registered branch names and ids.
    pub fn list_branches(&self, session_id: u64) -> Result<ListBranchesResponse, AuthError> {
        self.require_session(session_id)?;
        let active = self.active_branch_name.read().unwrap().clone();
        let branches: Vec<BranchInfo> = self
            .branch_registry
            .list_branches()
            .into_iter()
            .map(|(name, id)| BranchInfo {
                name,
                branch_id_hex: hex16(&id.0),
            })
            .collect();
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.node_count = branches.len();
        Ok(ListBranchesResponse {
            branches,
            active_branch_name: active,
            meta,
        })
    }

    /// Read recent merge observability records from `.cis/merge_metrics.jsonl` (**Phase 2.5**).
    pub fn list_merge_metrics(
        &self,
        session_id: u64,
        limit: usize,
        since_ms: Option<u64>,
    ) -> Result<ListMergeMetricsResponse, AuthError> {
        self.require_session(session_id)?;
        let limit = limit.clamp(1, 500);
        let records = crate::merge_metrics::read_merge_metrics_filtered(
            &self.cis_path(),
            limit,
            since_ms,
        )
        .unwrap_or_default();
        let rollup = crate::merge_metrics::rollup_merge_metrics(&records);
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.node_count = records.len();
        self.audit.record_sync(session_id, "list_merge_metrics");
        Ok(ListMergeMetricsResponse {
            records,
            rollup,
            meta,
        })
    }

    /// Remove **`ri:{branch}:`** overlay keys from durable KV (post-merge cleanup).
    pub fn purge_branch(&self, session_id: u64, branch_hex: &str) -> Result<PurgeBranchResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = parse_branch_id_hex(branch_hex).ok_or(AuthError::InvalidInput)?;
        let prefix = format!("ri:{}:", hex16(&branch.0));
        let keys: Vec<String> = self
            .kv
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let n = keys.len();
        for k in keys {
            self.kv.delete(&k);
        }
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.node_count = n;
        self.audit.record_sync(session_id, "purge_branch");
        Ok(PurgeBranchResponse {
            branch_id_hex: hex16(&branch.0),
            keys_deleted: n,
            meta,
        })
    }

    /// Persist **`.cis/graph.json`**, **vector.json**, and **kv.json`** from the live runtime.
    pub fn save_workspace(&self, session_id: u64) -> Result<SaveWorkspaceResponse, AuthError> {
        self.require_session(session_id)?;
        self.sync_bodies_after_commit(self.active_branch());
        let cis = crate::persistence::cis_dir(std::path::Path::new(&self.repo_root));
        self.sync_revision_index_from_graph();
        if crate::metadata_store::metadata_backend_from_env()
            == crate::metadata_store::MetadataBackendKind::Sqlite
        {
            let epoch =
                crate::time_travel::next_ris_epoch(self.kv.as_ref(), self.active_branch());
            self.revision_index()
                .persist_ris_snapshot(epoch, Some(cis.as_path()));
        }
        let g = self.coordinator.graph().read();
        crate::persistence::save_workspace_snapshots(
            &cis,
            &g,
            self.coordinator.vector(),
            self.kv.as_ref(),
        )
            .map_err(|e| {
                eprintln!("cis-mcp: save_workspace: {:?}", e);
                AuthError::InvalidInput
            })?;
        let stale_confirm_sidecars_removed = self.sweep_stale_confirm_sidecars();
        self.audit.record_sync(session_id, "save_workspace");
        Ok(SaveWorkspaceResponse {
            cis_dir: cis.display().to_string(),
            stale_confirm_sidecars_removed,
            meta: QueryMeta::default(),
        })
    }

    /// Marks all newly-written revisions as `Speculative`
    /// (architecture SP-6 / §01.5).  The FS sync confirm path will promote them to `Active`.
    fn speculative_reindex_after_write(
        &self,
        rel_path: &str,
    ) -> Result<(), crate::coordinator::CoordinatorError> {
        if crate::language_indexer::indexer_for_path(
            rel_path,
            &crate::language_indexer::default_indexers(),
        )
        .is_none()
        {
            return Ok(());
        }
        self.reindex_paths_on_branch(&[rel_path], self.active_branch())?;
        let branch = self.active_branch();
        let path_s = rel_path.to_string();
        let rids_to_speculate: Vec<NodeRevisionId> = {
            let g = self.coordinator.graph().read();
            g.revision_ids_for_file(branch, &path_s)
                .iter()
                .filter(|rid| {
                    g.get_revision(**rid)
                        .map(|r| matches!(r.status, RevisionStatus::Active))
                        .unwrap_or(false)
                })
                .copied()
                .collect()
        };
        {
            let mut g = self.coordinator.graph().write();
            for rid in rids_to_speculate {
                if let Some(rev) = g.get_revision(rid).cloned() {
                    let mut updated = rev;
                    updated.status = RevisionStatus::Speculative;
                    g.put_revision(updated);
                }
            }
        }
        Ok(())
    }

    fn reindex_paths_on_branch(
        &self,
        paths: &[&str],
        branch: BranchId,
    ) -> Result<crate::ingest::IngestApplyReport, crate::coordinator::CoordinatorError> {
        let rename_config =
            crate::identity_resolution::RenameConfig::from_policy(&self.policy.snapshot());
        let overlay = revision_index_for_branch(
            self.revision_index_arc(),
            branch,
            std::sync::Arc::clone(&self.kv),
        );
        crate::fs_sync::reindex_paths_on_coordinator(
            self.coordinator.as_ref(),
            &self.repo_root,
            &self.kv,
            &overlay,
            branch,
            paths.iter().map(|p| (*p).to_string()).collect(),
            Some(rename_config),
        )
    }

    /// Start native notify (default when built with `fs-notify`) + debounced re-index (**Phase 2**).
    pub fn spawn_fs_sync_background(self: &std::sync::Arc<Self>) {
        let config = crate::fs_sync::FsSyncConfig::from_env();
        crate::fs_sync::spawn_fs_sync_stack(std::sync::Arc::clone(self), config);
    }

    pub fn merge_control(&self) -> &MergeControl {
        &self.merge
    }

    /// Whether `rel_path` is still tracked as speculative (tests / diagnostics).
    pub fn has_speculative_path(&self, rel_path: &str) -> bool {
        let abs = resolve_repo_path(&self.repo_root, rel_path)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| rel_path.to_string());
        self.spec_paths.intersects(&[abs])
    }

    /// Rebuild **`ri:`** overlay from the current in-memory graph (dev / tests).
    pub fn sync_revision_index_from_graph(&self) {
        let branch = self.active_branch();
        let branch_hex = branch
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let prefix = format!("ri:{branch_hex}:");
        let keys: Vec<String> = self
            .kv
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let mut actions: Vec<(IdentityId, Option<NodeRevisionId>)> = Vec::new();
        {
            let g = self.coordinator.graph().read();
            for key in keys {
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() != 3 {
                    continue;
                }
                let Some(identity) = parse_identity_hex_local(parts[2]) else {
                    continue;
                };
                let rid = match g.primary_revision_for_identity(branch, identity) {
                    Some(rev)
                        if matches!(
                            rev.status,
                            RevisionStatus::Active | RevisionStatus::Speculative
                        ) =>
                    {
                        Some(rev.revision_id)
                    }
                    _ => None,
                };
                actions.push((identity, rid));
            }
            let identities: std::collections::HashSet<_> = g
                .revisions()
                .filter(|r| r.branch_id == branch)
                .map(|r| r.identity_id)
                .collect();
            for identity in identities {
                if actions.iter().any(|(id, _)| *id == identity) {
                    continue;
                }
                if let Some(rev) = g.primary_revision_for_identity(branch, identity) {
                    if matches!(
                        rev.status,
                        RevisionStatus::Active | RevisionStatus::Speculative
                    ) {
                        actions.push((identity, Some(rev.revision_id)));
                    }
                }
            }
        }
        let ri = self.revision_index.read().unwrap();
        for (identity, rid) in actions {
            match rid {
                Some(revision_id) => ri.bind(identity, revision_id),
                None => ri.unbind(identity),
            }
        }
    }

    /// Persist a **FR-4.11** snapshot for **`wal_log_id`** after syncing `revision_index` from the graph.
    pub fn record_time_travel_checkpoint(&self, wal_log_id: u64) {
        self.sync_revision_index_from_graph();
        crate::time_travel::record_committed_snapshot(
            self.revision_index.read().unwrap().as_ref(),
            &self.kv,
            self.active_branch(),
            wal_log_id,
            Some(self.cis_path().as_path()),
        );
    }

    /// Run merge-lock TTL sweep using **`policy.merge_ttl_hours`** (**§01.6.5**).
    pub fn run_merge_ttl_sweep(&self, session_id: u64) -> Result<usize, AuthError> {
        self.require_session(session_id)?;
        let ttl = self.policy.snapshot().merge_ttl_hours;
        Ok(sweep_all_expired_merge_intents(&self.kv, now_ms(), ttl))
    }

    pub fn require_session(&self, session_id: u64) -> Result<(), AuthError> {
        self.auth.require_session(session_id).map(|_| ())
    }

    fn current_index_status(&self) -> IndexStatusSnapshot {
        self.index_status.lock().unwrap().clone()
    }

    fn meta_at_commit(
        &self,
        merge_in_progress: bool,
        latency_ms: u64,
        node_count: usize,
        commit_label: Option<&str>,
        time_travel_live_graph: bool,
        branch: Option<BranchId>,
    ) -> QueryMeta {
        let branch = branch.unwrap_or_else(|| self.active_branch());
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.merge_in_progress = merge_in_progress;
        meta.query_latency_ms = latency_ms;
        meta.node_count = node_count;
        meta.query_at_commit = commit_label.map(String::from);
        meta.time_travel_uses_live_graph = time_travel_live_graph;
        meta.background_reconciliation_pending = self.reconciliation_tracker.is_pending(branch);
        meta.git_merge_in_progress =
            crate::git_branch_sync::git_merge_in_progress(std::path::Path::new(&self.repo_root));
        let idx = self.current_index_status();
        meta.ingest_mode = active_ingest_mode().into();
        meta.indexed_symbol_count = idx.symbols_indexed;
        meta.indexed_edge_count = idx.edges_indexed;
        if !self.lsp_integration_active.load(Ordering::Relaxed) {
            meta.degraded_modes.push("lsp_unavailable".into());
        }
        if self.disk_pressure.disk_pressure() {
            meta.degraded_modes.push("disk_pressure".into());
        }
        if self.vector_degraded.is_vector_degraded() {
            meta.degraded_modes.push("vector_degraded".into());
        }
        meta
    }

    fn acquire_query_quota(&self, session_id: u64) -> Result<QuotaGuard<'_>, AuthError> {
        QuotaGuard::acquire(&self.quota, session_id).map_err(|_| AuthError::QuotaExceeded)
    }

    fn merge_reconciliation_job_id(merge_id: MergeId) -> u64 {
        u64::from_le_bytes(merge_id.0[0..8].try_into().expect("merge id is 16 bytes"))
    }

    fn phase_durations_from_timeline(timeline: &[MergeProgressEvent]) -> std::collections::BTreeMap<String, u64> {
        let mut out = std::collections::BTreeMap::new();
        let mut prev = 0u64;
        for ev in timeline {
            let dur = ev.elapsed_ms.saturating_sub(prev);
            out.insert(ev.phase.clone(), dur);
            prev = ev.elapsed_ms;
        }
        out
    }

    fn append_merge_metrics_record(&self, record: crate::merge_metrics::MergeMetricsRecord) {
        let _ = crate::merge_metrics::append_merge_metrics(&self.cis_path(), &record);
    }

    /// Poll `.git/HEAD` and sync CIS active branch when `CIS_GIT_BRANCH_SYNC=1`.
    pub fn poll_git_branch_sync(&self, last_mtime: &mut Option<std::time::SystemTime>) {
        crate::git_branch_sync::poll_git_head_and_sync(
            std::path::Path::new(&self.repo_root),
            std::sync::Arc::clone(&self.kv),
            &self.branch_registry,
            &self.active_branch_name,
            &self.active_branch_id,
            &self.revision_index,
            last_mtime,
        );
    }

    pub fn find_symbol(
        &self,
        session_id: u64,
        needle: &str,
        branch_id: Option<BranchId>,
        limit: usize,
        prefer_file_hub: bool,
    ) -> Result<FindSymbolResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let g = self.coordinator.graph().read();
        let half = policy_half_life_ms(&self.policy.snapshot());
        let now = query_now_ms();
        let mut matches = Vec::new();
        for rev in g.revisions() {
            if rev.branch_id != branch {
                continue;
            }
            if !rev.qualified_name.contains(needle) {
                continue;
            }
            if !prefer_file_hub && matches.len() >= limit {
                break;
            }
            let conf = crate::query_engine::node_hit_confidence(&g, rev, branch, now, half);
            matches.push(hit_from_rev(rev, conf));
        }
        if prefer_file_hub {
            matches.sort_by(|a, b| {
                let rank = |h: &SymbolHit| -> (u8, usize) {
                    if h.qualified_name == h.file_path {
                        (0, h.qualified_name.len())
                    } else if h.qualified_name.ends_with(needle) {
                        (1, h.qualified_name.len())
                    } else {
                        (2, h.qualified_name.len())
                    }
                };
                rank(a).cmp(&rank(b))
            });
            matches.truncate(limit);
        }
        let node_count = matches.len();
        let latency_ms = t0.elapsed().as_millis() as u64;
        self.audit.record_sync(session_id, "find_symbol");
        let mut meta = self.meta_at_commit(merge_in_progress, latency_ms, node_count, None, false, Some(branch));
        if let Some(max_conf) = matches.iter().map(|m| m.confidence).reduce(f64::max) {
            meta.retrieval_confidence = max_conf;
        }
        Ok(FindSymbolResponse { matches, meta })
    }

    /// **FR-4.11** — `commit_hash` is CIS WAL **`log_id`** (see [`crate::time_travel::parse_wal_log_anchor`]).
    pub fn find_symbol_at(
        &self,
        session_id: u64,
        needle: &str,
        wal_log_id: u64,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<FindSymbolResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let overlay = crate::time_travel::overlay_at_wal_log_at(
            &self.kv,
            branch,
            wal_log_id,
            Some(self.cis_path().as_path()),
        )
            .ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let mut matches = Vec::new();
        for (_, rid) in overlay.resolved_bindings() {
            let Some(rev) = g.get_revision(rid) else {
                continue;
            };
            if rev.branch_id != branch {
                continue;
            }
            if matches.len() >= limit {
                break;
            }
            if rev.qualified_name.contains(needle) {
                let conf = crate::query_engine::node_hit_confidence(
                    &g,
                    rev,
                    branch,
                    query_now_ms(),
                    policy_half_life_ms(&self.policy.snapshot()),
                );
                matches.push(hit_from_rev(rev, conf));
            }
        }
        let node_count = matches.len();
        let latency_ms = t0.elapsed().as_millis() as u64;
        self.audit.record_sync(session_id, "find_symbol_at");
        let label = Some(format!("{}", wal_log_id));
        let meta = self.meta_at_commit(
            merge_in_progress,
            latency_ms,
            node_count,
            label.as_deref(),
            true,
            Some(branch),
        );
        Ok(FindSymbolResponse { matches, meta })
    }

    pub fn build_context(
        &self,
        session_id: u64,
        text: &str,
        budget_tokens: usize,
    ) -> Result<BuildContextResponse, AuthError> {
        self.require_session(session_id)?;
        let _quota = self.acquire_query_quota(session_id)?;
        let t0 = Instant::now();
        let tok = CharApproxTokenizer;
        let (out, truncated, omitted_tokens) =
            build_context_truncated(text, budget_tokens.max(1), &tok);
        self.audit.record_sync(session_id, "build_context");
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.query_latency_ms = t0.elapsed().as_millis() as u64;
        meta.tokenizer_mode = "char_approximation".into();
        Ok(BuildContextResponse {
            text: out,
            truncated,
            omitted_tokens,
            meta,
        })
    }

    /// **FR-4.11** — `target` is revision id (32 hex) or substring of `qualified_name` at `wal_log_id`.
    pub fn build_context_at(
        &self,
        session_id: u64,
        target: &str,
        wal_log_id: u64,
        budget_tokens: usize,
        branch_id: Option<BranchId>,
    ) -> Result<BuildContextResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let overlay = crate::time_travel::overlay_at_wal_log_at(
            &self.kv,
            branch,
            wal_log_id,
            Some(self.cis_path().as_path()),
        )
            .ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let rid = resolve_target_revision(target, branch, overlay.as_ref(), &g).ok_or(AuthError::InvalidInput)?;
        let rev = g.get_revision(rid).ok_or(AuthError::InvalidInput)?;
        let (raw, _source) = crate::symbol_body::resolve_revision_body(
            &self.body_store,
            self.body_blob_store.as_ref(),
            &self.cis_path(),
            std::path::Path::new(&self.repo_root),
            rev,
        );
        drop(g);
        let tok = CharApproxTokenizer;
        let (text, truncated, omitted_tokens) =
            build_context_truncated(&raw, budget_tokens.max(1), &tok);
        self.audit.record_sync(session_id, "build_context_at");
        let label = Some(format!("{}", wal_log_id));
        let meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            1,
            label.as_deref(),
            true,
            Some(branch),
        );
        Ok(BuildContextResponse {
            text,
            truncated,
            omitted_tokens,
            meta,
        })
    }

    pub fn hybrid_search(
        &self,
        session_id: u64,
        candidates: Vec<HybridSearchCandidate>,
        vector_top_k: usize,
    ) -> Result<HybridSearchResponse, AuthError> {
        self.require_session(session_id)?;
        let _quota = self.acquire_query_quota(session_id)?;
        let t0 = Instant::now();
        let snap = self.policy.snapshot();
        let ranked = hybrid_search_rerank(candidates, &snap, vector_top_k);
        self.audit.record_sync(session_id, "hybrid_search");
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.query_latency_ms = t0.elapsed().as_millis() as u64;
        meta.node_count = ranked.len();
        Ok(HybridSearchResponse { ranked, meta })
    }

    pub fn go_to_definition(
        &self,
        session_id: u64,
        revision_id_hex: &str,
        branch_id: Option<BranchId>,
    ) -> Result<GoToDefinitionResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let rid = parse_revision_hex(revision_id_hex).ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let rev = g.get_revision(rid).ok_or(AuthError::InvalidInput)?;
        if rev.branch_id != branch {
            return Err(AuthError::InvalidInput);
        }
        let eto = eto_for_runtime(&self.kv);
        let policy = self.policy.snapshot();
        let now = query_now_ms();
        let half = policy_half_life_ms(&policy);
        let target = crate::query_engine::resolve_definition_target(&g, &eto, branch, rid);
        let hit = target.map(|r| {
            let conf = crate::query_engine::node_hit_confidence(&g, r, branch, now, half);
            hit_from_rev(r, conf)
        });
        self.audit.record_sync(session_id, "go_to_definition");
        let mut meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            usize::from(hit.is_some()),
            None,
            false,
            None,
        );
        if let Some(ref h) = hit {
            meta.retrieval_confidence = h.confidence;
        }
        Ok(GoToDefinitionResponse { target: hit, meta })
    }

    pub fn find_references(
        &self,
        session_id: u64,
        revision_id_hex: &str,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<SymbolHitsResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let rid = parse_revision_hex(revision_id_hex).ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let rev = g.get_revision(rid).ok_or(AuthError::InvalidInput)?;
        if rev.branch_id != branch {
            return Err(AuthError::InvalidInput);
        }
        let eto = eto_for_runtime(&self.kv);
        let now = query_now_ms();
        let half = policy_half_life_ms(&self.policy.snapshot());
        let sources = g.source_identities_targeting(rev.identity_id);
        let mut hits = Vec::new();
        for sid in sources {
            if hits.len() >= limit {
                break;
            }
            if let Some(src_rev) = g.primary_revision_for_identity(branch, sid) {
                for e in g.outbound_edges(src_rev.revision_id) {
                    let effective = eto.effective_target_identity(branch, e);
                    if effective == rev.identity_id {
                        let conf =
                            crate::confidence::edge_confidence(e, now, half);
                        hits.push(hit_from_rev_and_edge(src_rev, e, conf));
                        break;
                    }
                }
            }
        }
        let n = hits.len();
        self.audit.record_sync(session_id, "find_references");
        let mut meta = self.meta_at_commit(merge_in_progress, t0.elapsed().as_millis() as u64, n, None, false, None);
        if let Some(max_conf) = hits.iter().map(|h| h.confidence).reduce(f64::max) {
            meta.retrieval_confidence = max_conf;
        }
        Ok(SymbolHitsResponse { hits, meta })
    }

    pub fn get_callers(
        &self,
        session_id: u64,
        identity_id_hex: &str,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<SymbolHitsResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let target_id = parse_revision_hex(identity_id_hex)
            .map(|r| IdentityId(r.0))
            .ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let eto = eto_for_runtime(&self.kv);
        let now = query_now_ms();
        let half = policy_half_life_ms(&self.policy.snapshot());
        let mut hits = Vec::new();
        for r in g.revisions() {
            if hits.len() >= limit {
                break;
            }
            if r.branch_id != branch {
                continue;
            }
            for e in g.outbound_edges(r.revision_id) {
                if e.ty == EdgeType::Calls
                    && eto.effective_target_identity(branch, e) == target_id
                {
                    let conf = crate::confidence::edge_confidence(e, now, half);
                    hits.push(hit_from_rev_and_edge(r, e, conf));
                    break;
                }
            }
        }
        let n = hits.len();
        self.audit.record_sync(session_id, "get_callers");
        let meta = self.meta_at_commit(merge_in_progress, t0.elapsed().as_millis() as u64, n, None, false, None);
        Ok(SymbolHitsResponse { hits, meta })
    }

    pub fn get_dependencies(
        &self,
        session_id: u64,
        revision_id_hex: &str,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<SymbolHitsResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let rid = parse_revision_hex(revision_id_hex).ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let rev = g.get_revision(rid).ok_or(AuthError::InvalidInput)?;
        if rev.branch_id != branch {
            return Err(AuthError::InvalidInput);
        }
        let eto = eto_for_runtime(&self.kv);
        let now = query_now_ms();
        let half = policy_half_life_ms(&self.policy.snapshot());
        let mut seen: HashSet<NodeRevisionId> = HashSet::new();
        let mut hits = Vec::new();
        let mut push_dep = |e: &GraphEdge| {
            if hits.len() >= limit {
                return;
            }
            if !matches!(
                e.ty,
                EdgeType::Imports | EdgeType::Uses | EdgeType::Calls | EdgeType::Extends
            ) {
                return;
            }
            if let Some(trev) = crate::query_engine::resolve_edge_target(&g, &eto, branch, e) {
                if seen.insert(trev.revision_id) {
                    let conf = crate::query_engine::node_hit_confidence(&g, trev, branch, now, half);
                    hits.push(hit_from_rev_and_edge(trev, e, conf));
                }
            }
        };
        for e in crate::query_engine::outbound_context_edges(&g, branch, rid) {
            push_dep(e);
        }
        let n = hits.len();
        self.audit.record_sync(session_id, "get_dependencies");
        let mut meta = self.meta_at_commit(merge_in_progress, t0.elapsed().as_millis() as u64, n, None, false, None);
        if let Some(max_conf) = hits.iter().map(|h| h.confidence).reduce(f64::max) {
            meta.retrieval_confidence = max_conf;
        }
        Ok(SymbolHitsResponse { hits, meta })
    }

    /// Import edges attached to the file hub for `file_path` (repo-relative).
    pub fn file_imports(
        &self,
        session_id: u64,
        file_path: &str,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<FileImportsResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let path = file_path.trim().replace('\\', "/");
        let path = path.strip_prefix("./").unwrap_or(&path);
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let g = self.coordinator.graph().read();
        let hub = crate::query_engine::file_hub_revision_for_path(&g, branch, path)
            .ok_or(AuthError::InvalidInput)?;
        let eto = eto_for_runtime(&self.kv);
        let now = query_now_ms();
        let half = policy_half_life_ms(&self.policy.snapshot());
        let mut seen: HashSet<NodeRevisionId> = HashSet::new();
        let mut hits = Vec::new();
        for e in g.outbound_edges(hub) {
            if hits.len() >= limit {
                break;
            }
            if e.ty != EdgeType::Imports {
                continue;
            }
            if let Some(trev) = crate::query_engine::resolve_edge_target(&g, &eto, branch, e) {
                if seen.insert(trev.revision_id) {
                    let conf = crate::query_engine::node_hit_confidence(&g, trev, branch, now, half);
                    hits.push(hit_from_rev_and_edge(trev, e, conf));
                }
            }
        }
        let n = hits.len();
        self.audit.record_sync(session_id, "file_imports");
        let mut meta = self.meta_at_commit(merge_in_progress, t0.elapsed().as_millis() as u64, n, None, false, None);
        if let Some(max_conf) = hits.iter().map(|h| h.confidence).reduce(f64::max) {
            meta.retrieval_confidence = max_conf;
        }
        Ok(FileImportsResponse {
            file_path: path.to_string(),
            revision_id_hex: hex16(&hub.0),
            hits,
            meta,
        })
    }

    pub fn expand_context(
        &self,
        session_id: u64,
        revision_id_hex: &str,
        depth: u32,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<SymbolHitsResponse, AuthError> {
        self.require_session(session_id)?;
        let _quota = self.acquire_query_quota(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let start = parse_revision_hex(revision_id_hex).ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let eto = eto_for_runtime(&self.kv);
        let policy = self.policy.snapshot();
        let expanded = crate::query_engine::expand_context_bfs(
            &g,
            &eto,
            &policy,
            branch,
            start,
            depth,
            query_now_ms(),
        );
        let hits: Vec<SymbolHit> = expanded
            .hits
            .iter()
            .take(limit)
            .filter_map(|(rid, conf)| g.get_revision(*rid).map(|r| hit_from_rev(r, *conf)))
            .collect();
        let n = hits.len();
        self.audit.record_sync(session_id, "expand_context");
        let mut meta = self.meta_at_commit(merge_in_progress, t0.elapsed().as_millis() as u64, n, None, false, None);
        meta.pruned_low_confidence_count = expanded.pruned_low_confidence_count;
        if let Some(max_conf) = hits.iter().map(|h| h.confidence).reduce(f64::max) {
            meta.retrieval_confidence = max_conf;
        }
        Ok(SymbolHitsResponse { hits, meta })
    }

    pub fn semantic_search(
        &self,
        session_id: u64,
        query: &str,
        branch_id: Option<BranchId>,
        limit: usize,
    ) -> Result<SemanticSearchResponse, AuthError> {
        self.require_session(session_id)?;
        let _quota = self.acquire_query_quota(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let t0 = Instant::now();
        let needle = query.to_lowercase();
        let model_id = self.embedder.model_id();
        let vector = self.coordinator.vector();

        struct SemanticRow {
            revision_id_hex: String,
            qualified_name: String,
            body_hash: [u8; 32],
            structural_score: f64,
            vector_score: f64,
        }

        let g = self.coordinator.graph().read();
        let mut rows: Vec<SemanticRow> = Vec::new();
        let mut stale_count = 0usize;
        let mut embedded_count = 0usize;

        for r in g.revisions() {
            if r.branch_id != branch || !matches!(r.status, RevisionStatus::Active) {
                continue;
            }
            match vector.vector_for_body(&r.body_hash) {
                Some(entry) if entry.model_id == model_id => embedded_count += 1,
                Some(_) | None => stale_count += 1,
            }
            rows.push(SemanticRow {
                revision_id_hex: hex16(&r.revision_id.0),
                qualified_name: r.qualified_name.clone(),
                body_hash: r.body_hash,
                structural_score: structural_substring_score(&needle, &r.qualified_name),
                vector_score: 0.0,
            });
        }
        drop(g);

        let query_vec = if embedded_count > 0 {
            self.embedder
                .embed_batch(&[query.to_string()])
                .ok()
                .and_then(|mut v| v.pop())
        } else {
            None
        };

        let ann_scores: std::collections::HashMap<[u8; 32], f64> = if let Some(ref qv) = query_vec {
            let ann = self.ann_index.lock().unwrap();
            if ann.is_empty() {
                std::collections::HashMap::new()
            } else {
                ann.search(qv, limit.saturating_mul(10).max(50))
                    .into_iter()
                    .collect()
            }
        } else {
            std::collections::HashMap::new()
        };

        let mut hits = Vec::new();
        let mut meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            0,
            None,
            false,
            None,
        );

        if let Some(ref qv) = query_vec {
            if !ann_scores.is_empty() {
                for row in &mut rows {
                    if let Some(&score) = ann_scores.get(&row.body_hash) {
                        row.vector_score = score;
                    }
                }
                rows.retain(|r| ann_scores.contains_key(&r.body_hash));
            } else {
                for row in &mut rows {
                    if let Some(entry) = vector.vector_for_body(&row.body_hash) {
                        if entry.model_id == model_id {
                            row.vector_score =
                                crate::embedder::cosine_similarity(qv, &entry.vec);
                        }
                    }
                }
            }
            let hybrid: Vec<HybridSearchCandidate> = rows
                .iter()
                .map(|row| HybridSearchCandidate {
                    id: row.revision_id_hex.clone(),
                    vector_score: row.vector_score,
                    structural_score: row.structural_score,
                    speculative: false,
                })
                .collect();
            let ranked = hybrid_search_rerank(hybrid, &self.policy.snapshot(), limit);
            let snap = self.policy.snapshot();
            let w_sem = snap.axis_weights.semantic_similarity;
            let w_str = snap.axis_weights.structural_proximity;
            let by_hex: std::collections::HashMap<String, &SemanticRow> =
                rows.iter().map(|r| (r.revision_id_hex.clone(), r)).collect();
            for c in ranked.into_iter().take(limit) {
                if let Some(row) = by_hex.get(&c.id) {
                    hits.push(SemanticSearchHit {
                        qualified_name: row.qualified_name.clone(),
                        score: c.vector_score * w_sem + c.structural_score * w_str,
                        revision_id_hex: c.id,
                    });
                }
            }
        } else {
            meta.degraded_modes.push("semantic_degraded".into());
            meta.degraded_reason = Some(
                "no_embeddings_indexed: falling back to structural substring match".into(),
            );
            let mut scored: Vec<&SemanticRow> = rows
                .iter()
                .filter(|r| {
                    needle.is_empty()
                        || r.qualified_name.to_lowercase().contains(&needle)
                })
                .collect();
            scored.sort_by(|a, b| {
                b.structural_score
                    .partial_cmp(&a.structural_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            for row in scored.into_iter().take(limit) {
                hits.push(SemanticSearchHit {
                    qualified_name: row.qualified_name.clone(),
                    score: row.structural_score,
                    revision_id_hex: row.revision_id_hex.clone(),
                });
            }
        }

        if stale_count > 0 {
            meta.degraded_modes.push("embeddings_pending".into());
        }
        if !api_embedder_configured() {
            meta.degraded_modes.push("stub_embedder".into());
        }
        if self.vector_degraded.is_vector_degraded() {
            meta.degraded_modes.push("vector_degraded".into());
        }
        meta.stale_count = stale_count;
        meta.node_count = hits.len();

        self.audit.record_sync(session_id, "semantic_search");
        Ok(SemanticSearchResponse { hits, meta })
    }

    /// **FR-4.2** — speculative write with path lease (**§01.5**).
    pub fn write_file(
        &self,
        session_id: u64,
        path: &str,
        content: &str,
        reindex: bool,
    ) -> Result<WriteFileResponse, AuthError> {
        self.require_session(session_id)?;
        if self.disk_pressure.disk_pressure() {
            return Err(AuthError::DiskPressure);
        }
        let abs = resolve_repo_path(&self.repo_root, path)?;
        let abs_s = abs.to_string_lossy().into_owned();
        self.auth.validate_path(session_id, &abs_s)?;
        let patch_id = self
            .patcher
            .apply_speculative(
                SessionId(session_id),
                vec![abs_s.clone()],
                Some((self.kv.as_ref(), self.active_branch())),
            )
            .map_err(patch_err)?;
        let pre_bytes = std::fs::read(&abs).unwrap_or_default();
        if self.pre_write_snapshots.capture_if_absent(patch_id, path, pre_bytes.clone()).is_err() {
            let _ = self.patcher.revert(patch_id, SessionId(session_id));
            return Err(AuthError::InvalidInput);
        }
        let bytes = content.as_bytes();
        std::fs::write(&abs, bytes).map_err(|_| AuthError::InvalidInput)?;
        let nonce = format!("{}", patch_id);
        if let Err(e) = write_token_with_retry(self.confirm_backend(), &nonce, nonce.as_bytes())
        {
            eprintln!("cis-mcp: write_file confirm_token write: {:?}", e);
            self.rollback_failed_speculative_write(patch_id, session_id, path, &pre_bytes);
            return Err(AuthError::InvalidInput);
        }
        if reindex {
            if let Err(e) = self.speculative_reindex_after_write(path) {
                eprintln!("cis-mcp: write_file speculative reindex: {:?}", e);
                self.sync_revision_index_from_graph();
            }
        }
        self.audit
            .record_sync(session_id, format!("write_file {}", abs_s));
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.node_count = bytes.len();
        Ok(WriteFileResponse {
            path: abs_s,
            patch_id,
            bytes_written: bytes.len(),
            meta,
        })
    }

    /// **FR-4.2** — apply a **unified diff** (when detected) or replace full file (**§01.5**).
    pub fn apply_patch(
        &self,
        session_id: u64,
        path: &str,
        new_content_or_unified_patch: &str,
        reindex: bool,
    ) -> Result<WriteFileResponse, AuthError> {
        self.require_session(session_id)?;
        if self.disk_pressure.disk_pressure() {
            return Err(AuthError::DiskPressure);
        }
        let abs = resolve_repo_path(&self.repo_root, path)?;
        let abs_s = abs.to_string_lossy().into_owned();
        self.auth.validate_path(session_id, &abs_s)?;
        let patch_id = self
            .patcher
            .apply_speculative(
                SessionId(session_id),
                vec![abs_s.clone()],
                Some((self.kv.as_ref(), self.active_branch())),
            )
            .map_err(patch_err)?;
        let pre_bytes = std::fs::read(&abs).unwrap_or_default();
        if self.pre_write_snapshots.capture_if_absent(patch_id, path, pre_bytes.clone()).is_err() {
            let _ = self.patcher.revert(patch_id, SessionId(session_id));
            return Err(AuthError::InvalidInput);
        }
        let original = std::fs::read_to_string(&abs).unwrap_or_default();
        let merged = if is_probably_unified_diff(new_content_or_unified_patch) {
            let p = diffy::Patch::from_str(new_content_or_unified_patch).map_err(|_| AuthError::InvalidInput)?;
            diffy::apply(&original, &p).map_err(|_| AuthError::InvalidInput)?
        } else {
            new_content_or_unified_patch.to_string()
        };
        let bytes = merged.as_bytes();
        std::fs::write(&abs, bytes).map_err(|_| AuthError::InvalidInput)?;
        let nonce = format!("{}", patch_id);
        if let Err(e) = write_token_with_retry(self.confirm_backend(), &nonce, nonce.as_bytes())
        {
            eprintln!("cis-mcp: apply_patch confirm_token write: {:?}", e);
            self.rollback_failed_speculative_write(patch_id, session_id, path, &pre_bytes);
            return Err(AuthError::InvalidInput);
        }
        if reindex {
            if let Err(e) = self.speculative_reindex_after_write(path) {
                eprintln!("cis-mcp: apply_patch speculative reindex: {:?}", e);
                self.sync_revision_index_from_graph();
            }
        }
        self.audit
            .record_sync(session_id, format!("apply_patch {}", abs_s));
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.node_count = bytes.len();
        Ok(WriteFileResponse {
            path: abs_s,
            patch_id,
            bytes_written: bytes.len(),
            meta,
        })
    }

    /// **Epic 3.3 §3.3:** promote a speculative patch to Active (commit graph).
    /// Clears the confirm token, releases leases, and flips graph revisions to `Active`.
    pub fn confirm_patch(
        &self,
        session_id: u64,
        patch_id: u64,
    ) -> Result<ConfirmPatchResponse, AuthError> {
        self.require_session(session_id)?;
        let paths = self
            .patcher
            .promote(patch_id, SessionId(session_id))
            .map_err(|_| AuthError::InvalidInput)?;
        let nonce = format!("{}", patch_id);
        let _ = clear_token_with_retry(self.confirm_backend(), &nonce);
        let branch = self.active_branch();
        let count = self
            .wal_promote_speculative(patch_id, &paths, branch)
            .map_err(|_| AuthError::InvalidInput)?;
        self.pre_write_snapshots.clear_patch(patch_id);
        self.audit.record_sync(session_id, format!("confirm_patch {}", patch_id));
        Ok(ConfirmPatchResponse { patch_id, paths_promoted: paths, revisions_activated: count })
    }

    /// **Epic 3.3 §3.3:** revert a speculative patch (tombstone speculative revisions + release leases).
    pub fn revert_patch(
        &self,
        session_id: u64,
        patch_id: u64,
    ) -> Result<RevertPatchResponse, AuthError> {
        self.require_session(session_id)?;
        let paths = self.patcher.peek_patch_paths(patch_id);
        self.finish_revert_patch(patch_id, SessionId(session_id), &paths)
            .map_err(|_| AuthError::InvalidInput)?;
        self.audit.record_sync(session_id, format!("revert_patch {}", patch_id));
        Ok(RevertPatchResponse { patch_id, paths_reverted: paths })
    }

    /// **FR-4.3** — policy / graph observability for ranking (**§01.2**).
    pub fn explain_context(
        &self,
        session_id: u64,
        revision_id_hex: &str,
        budget_tokens: usize,
        branch_id: Option<BranchId>,
    ) -> Result<ExplainContextResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let t0 = Instant::now();
        let rid = parse_revision_hex(revision_id_hex).ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let rev = g.get_revision(rid).ok_or(AuthError::InvalidInput)?;
        if rev.branch_id != branch {
            return Err(AuthError::InvalidInput);
        }
        let qn = rev.qualified_name.clone();
        let snap = self.policy.snapshot();
        let axis = AxisWeightBreakdown {
            structural_proximity: snap.axis_weights.structural_proximity,
            edge_type: snap.axis_weights.edge_type,
            semantic_similarity: snap.axis_weights.semantic_similarity,
            recency: snap.axis_weights.recency,
        };
        let hybrid_search =
            serde_json::to_value(&snap.hybrid_search).unwrap_or_else(|_| serde_json::json!({}));
        let outbound_edges = g.outbound_edges(rid);
        let outbound = outbound_edges.len();
        let stale_count = g
            .revisions()
            .filter(|r| {
                r.branch_id == branch
                    && r.identity_id == rev.identity_id
                    && matches!(r.status, RevisionStatus::Tombstone)
            })
            .count();
        let speculative_count = g
            .revisions()
            .filter(|r| {
                r.branch_id == branch
                    && r.identity_id == rev.identity_id
                    && matches!(r.status, RevisionStatus::Orphaned)
            })
            .count();
        let eto = eto_for_runtime(&self.kv);
        let pruned_low_confidence_count = crate::query_engine::count_unresolved_definition_edges(
            &g,
            &eto,
            branch,
            rid,
        ) + crate::query_engine::count_pruned_expand_neighbors(
            &g,
            &eto,
            &snap,
            branch,
            rid,
            snap.max_hops.min(3),
            query_now_ms(),
        );
        let inbound = g.source_identities_targeting(rev.identity_id).len();
        let counts = ExplainCounts {
            stale_count,
            speculative_count,
            pruned_low_confidence_count,
        };
        let notes = format!(
            "axes §01.2; outbound_edges={outbound}; inbound_source_identities={inbound}; budget_tokens={budget_tokens} (char/4 tokenizer for build_context); tombstone_rows_for_identity={stale_count}; orphaned_rows_for_identity={speculative_count}; unresolved_edge_targets={pruned_low_confidence_count}",
        );
        drop(g);
        self.audit.record_sync(session_id, "explain_context");
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress =
            gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let mut meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            outbound,
            None,
            false,
            None,
        );
        meta.stale_count = stale_count;
        meta.speculative_count = speculative_count;
        meta.pruned_low_confidence_count = pruned_low_confidence_count;
        meta.dangling_edge_count = pruned_low_confidence_count;
        meta.tokenizer_mode = "char_approximation".into();
        if stale_count > 0 {
            meta.degraded_modes.push("stale_graph_nodes".into());
        }
        if pruned_low_confidence_count > 0 {
            meta.degraded_modes.push("dangling_edges".into());
        }
        if speculative_count > 0 {
            meta.degraded_modes.push("orphaned_revisions".into());
        }
        Ok(ExplainContextResponse {
            revision_id_hex: hex16(&rid.0),
            qualified_name: qn,
            budget_tokens,
            axis_weights: axis,
            hybrid_search,
            counts,
            notes,
            meta,
        })
    }

    /// **FR-4.10** — merge rollback; **`clear_edges_for`** = revisions to drop partial edges for.
    pub fn cancel_merge(
        &self,
        session_id: u64,
        merge_id_hex: &str,
        branch_id: BranchId,
        clear_edges_for: Vec<NodeRevisionId>,
    ) -> Result<MergeCancelReport, AuthError> {
        self.require_session(session_id)?;
        let merge_id = parse_merge_id_hex(merge_id_hex).ok_or(AuthError::InvalidInput)?;
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        gate.begin_rollback(branch_id);
        let mut g = self.coordinator.graph().write();
        let rep = self
            .merge
            .cancel_merge(
                merge_id,
                branch_id,
                &mut g,
                self.coordinator.vector(),
                None,
                &clear_edges_for,
            )
            .map_err(|_| AuthError::InvalidInput)?;
        drop(g);
        let saga = MergeSagaOrchestrator::new(std::sync::Arc::clone(&self.kv));
        saga.purge_merge_saga_state(merge_id);
        crate::merge_engine::MergeContext::remove(&self.kv, merge_id);
        let _ = crate::merge_engine::append_merge_cancelled_marker(
            self.coordinator.wal().as_ref(),
            merge_id,
        );
        gate.end_rollback(branch_id);
        self.audit.record_sync(session_id, "cancel_merge");
        Ok(rep)
    }

    /// Runtime indexing status for MCP observability.
    pub fn index_status(&self, session_id: u64) -> Result<IndexStatusResponse, AuthError> {
        self.require_session(session_id)?;
        let t0 = Instant::now();
        let idx = self.current_index_status();
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress = gate.is_query_blocked(self.active_branch())
            || merge_lock_holder(&self.kv, self.active_branch()).is_some();
        let mut meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            idx.symbols_indexed,
            None,
            false,
            None,
        );
        if idx.last_index_epoch_ms.is_none() {
            meta.degraded_modes.push("index_not_bootstrapped".into());
        }
        self.audit.record_sync(session_id, "index_status");
        let embed = self.embedding_status_snapshot();
        let watcher = self.watcher_status_snapshot();
        Ok(IndexStatusResponse {
            last_index_epoch_ms: idx.last_index_epoch_ms,
            files_scanned: idx.files_scanned,
            symbols_indexed: idx.symbols_indexed,
            edges_indexed: idx.edges_indexed,
            parse_error_count: idx.parse_error_count,
            ingest_mode: active_ingest_mode().into(),
            embeddings_indexed: idx.embeddings_indexed,
            embeddings_stale: idx.embeddings_stale,
            embed_model_id: idx.embed_model_id.clone(),
            ann_index_size: idx.ann_index_size,
            embed_chunks_registered: idx.embed_chunks_registered,
            unique_vectors: idx.unique_vectors,
            body_blobs_stored: idx.body_blobs_stored,
            quota_active: self.quota.active_count(),
            quota_max: self.quota.max_concurrent(),
            embedding_queue_depth: embed.queue_depth,
            embedding_queue_state: embed.queue_state.clone(),
            embedding_queue_hwm: embed.hwm,
            embedding_queue_lwm: embed.lwm,
            embedding_drains_per_minute: embed.drains_per_minute,
            embedding_embedded_per_minute: embed.embedded_per_minute,
            watcher_raw_events: watcher.raw_events,
            watcher_coalesced_events: watcher.coalesced_events,
            watcher_reindexed_files: watcher.reindexed_files,
            watcher_pending_count: watcher.pending_count,
            watcher_debounce_p50_ms: watcher.debounce_p50_ms,
            watcher_debounce_p99_ms: watcher.debounce_p99_ms,
            watcher_missed_samples: watcher.missed_event_samples,
            meta,
        })
    }

    /// Embedding queue depth, state, and drain rates (**Phase 5.3**).
    pub fn embedding_status(&self, session_id: u64) -> Result<EmbeddingStatusResponse, AuthError> {
        self.require_session(session_id)?;
        let t0 = Instant::now();
        let embedding = self.embedding_status_snapshot();
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        meta.query_latency_ms = t0.elapsed().as_millis() as u64;
        self.audit.record_sync(session_id, "embedding_status");
        Ok(EmbeddingStatusResponse { embedding, meta })
    }

    /// Consolidated operational health (**Phase 5.4**).
    pub fn system_status(&self, session_id: u64) -> Result<SystemStatusResponse, AuthError> {
        self.require_session(session_id)?;
        let t0 = Instant::now();
        let branch = self.active_branch();
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress = gate.is_query_blocked(branch)
            || merge_lock_holder(&self.kv, branch).is_some();
        let mut meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            self.current_index_status().symbols_indexed,
            None,
            false,
            Some(branch),
        );
        let min_free = self.policy.snapshot().disk_min_free_pct as f64;
        let free_pct = disk_free_percent(Path::new(&self.repo_root));
        let disk = DiskStatus {
            under_pressure: self.disk_pressure.disk_pressure(),
            free_percent: free_pct,
            min_free_percent: min_free,
        };
        let quota = QuotaStatus {
            active: self.quota.active_count(),
            max: self.quota.max_concurrent(),
        };
        let embed_snap = self.embedding_status_snapshot();
        let vector = VectorStatus {
            degraded: self.vector_degraded.is_vector_degraded(),
            queue_depth: embed_snap.queue_depth,
            queue_state: embed_snap.queue_state.clone(),
        };
        let watcher = self.watcher_status_snapshot();
        let pending_branches: Vec<String> = self
            .branch_registry
            .list_branches()
            .into_iter()
            .filter(|(_name, id)| self.reconciliation_tracker.is_pending(*id))
            .map(|(name, _)| name)
            .collect();
        let reconciliation = ReconciliationStatus {
            active_branch_pending: self.reconciliation_tracker.is_pending(branch),
            pending_branches,
        };
        let consistency = self.last_consistency.read();
        let background_workers = self.worker_heartbeats.snapshot();
        let audit_ok = verify_audit_chain(&self.kv).ok;
        let mut degraded_modes = meta.degraded_modes.clone();
        if !consistency.clean {
            if !degraded_modes.iter().any(|m| m == "consistency_violations") {
                degraded_modes.push("consistency_violations".into());
            }
        }
        if background_workers.any_stale {
            degraded_modes.push("worker_stale".into());
        }
        if !audit_ok {
            degraded_modes.push("audit_chain_broken".into());
        }
        meta.degraded_modes = degraded_modes.clone();
        let healthy = consistency.clean
            && audit_ok
            && !background_workers.any_stale
            && !disk.under_pressure;
        self.audit.record_sync(session_id, "system_status");
        Ok(SystemStatusResponse {
            healthy,
            degraded_modes,
            disk,
            quota,
            vector,
            embedding: embed_snap,
            watcher,
            reconciliation,
            consistency,
            background_workers,
            meta,
        })
    }

    /// Admin: verify durable audit hash chain in KV.
    pub fn verify_audit_chain(&self, session_id: u64) -> Result<VerifyAuditChainResponse, AuthError> {
        self.require_session(session_id)?;
        let v = verify_audit_chain(&self.kv);
        self.audit.record_sync(session_id, "verify_audit_chain");
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        Ok(VerifyAuditChainResponse {
            ok: v.ok,
            chain_length: v.chain_length,
            broken_at_epoch: v.broken_at_epoch,
            message: v.message,
            meta,
        })
    }

    /// Admin: on-demand graph consistency check.
    pub fn check_graph_consistency(
        &self,
        session_id: u64,
    ) -> Result<GraphConsistencyResponse, AuthError> {
        self.require_session(session_id)?;
        let branches = self.branch_registry.list_branches();
        let branch_ids: Vec<BranchId> = branches.into_iter().map(|(_, id)| id).collect();
        let report = check_consistency(
            self.coordinator.graph(),
            &self.kv,
            &self.body_store,
            &branch_ids,
        );
        self.update_consistency_cache(&report);
        self.audit.record_sync(session_id, "check_graph_consistency");
        let mut meta = QueryMeta::default();
        meta.policy_version = self.policy.current_version_label();
        if !report.is_clean() {
            meta.degraded_modes.push("consistency_violations".into());
        }
        Ok(GraphConsistencyResponse {
            clean: report.is_clean(),
            summary: report.summary(),
            dangling_bindings: report.dangling_bindings.len(),
            orphaned_active: report.orphaned_active_without_binding.len(),
            tombstones_still_bound: report.tombstones_still_bound.len(),
            duplicate_active: report.duplicate_active_per_identity.len(),
            missing_body: report.body_hash_missing_from_store.len(),
            index_desync: report.secondary_index_desync.len(),
            meta,
        })
    }

    /// Explain why `go_to_definition` returned no target for a given revision.
    pub fn why_no_definition(
        &self,
        session_id: u64,
        revision_id_hex: &str,
        branch_id: Option<BranchId>,
    ) -> Result<WhyNoDefinitionResponse, AuthError> {
        self.require_session(session_id)?;
        let branch = branch_id.unwrap_or(self.active_branch());
        let t0 = Instant::now();
        let rid = parse_revision_hex(revision_id_hex).ok_or(AuthError::InvalidInput)?;
        let g = self.coordinator.graph().read();
        let rev = g.get_revision(rid).ok_or(AuthError::InvalidInput)?;
        if rev.branch_id != branch {
            return Err(AuthError::InvalidInput);
        }
        let qualified_name = rev.qualified_name.clone();
        let mut reasons = Vec::new();
        let eto = eto_for_runtime(&self.kv);
        let outbound = g.outbound_edges(rid);
        let mut candidate_target_identities = 0usize;
        for e in outbound {
            if !matches!(e.ty, EdgeType::Imports | EdgeType::Extends | EdgeType::Calls) {
                continue;
            }
            candidate_target_identities += 1;
            if crate::query_engine::resolve_edge_target(&g, &eto, branch, e).is_none() {
                reasons.push("target_identity_without_active_revision".into());
            }
        }
        if candidate_target_identities == 0 {
            reasons.push("no_outbound_definition_edges".into());
        }
        if active_ingest_mode() == "regex" {
            reasons.push("index_mode_regex_limits_call_precision".into());
        }
        reasons.sort();
        reasons.dedup();
        drop(g);
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress = gate.is_query_blocked(branch) || merge_lock_holder(&self.kv, branch).is_some();
        let meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            candidate_target_identities,
            None,
            false,
            None,
        );
        self.audit.record_sync(session_id, "why_no_definition");
        Ok(WhyNoDefinitionResponse {
            revision_id_hex: hex16(&rid.0),
            qualified_name,
            candidate_target_identities,
            reasons,
            meta,
        })
    }

    /// **FR-4.8** — Full Phase A/B/C merge engine.
    ///
    /// - Phase A: Classify all identities across source/target/base branches
    /// - Phase B: Promote winning revisions; orphan losers
    /// - Phase C: Reconcile edges (dangling, drift, cardinality)
    ///
    /// When `strategy` is `None` and conflicts exist, returns `RequiresResolution` without promoting.
    ///
    /// Pass `progress` to emit live phase updates (**FR-4.8**); `MergeBranchResponse.streaming`
    /// is true when a sink was provided.
    pub fn merge_branch(
        &self,
        session_id: u64,
        source_branch_hex: &str,
        target_branch_hex: &str,
        strategy: Option<crate::merge_engine::MergeStrategy>,
        merge_id_hex: Option<&str>,
        mut progress_sink: Option<&mut dyn MergeProgressSink>,
    ) -> Result<MergeBranchResponse, AuthError> {
        let streaming = progress_sink.is_some();
        self.require_session(session_id)?;
        let _quota = self.acquire_query_quota(session_id)?;
        let source = parse_branch_id_hex(source_branch_hex).ok_or(AuthError::InvalidInput)?;
        let target = parse_branch_id_hex(target_branch_hex).ok_or(AuthError::InvalidInput)?;
        let merge_id = if let Some(hex) = merge_id_hex {
            parse_merge_id_hex(hex).ok_or(AuthError::InvalidInput)?
        } else {
            merge_id_from_now()
        };
        let continuing = merge_id_hex.is_some();
        let t0 = Instant::now();
        let saga = MergeSagaOrchestrator::new(std::sync::Arc::clone(&self.kv));
        let mut timeline = Vec::new();

        if continuing {
            if merge_lock_holder(&self.kv, target) != Some(merge_id) {
                return Err(AuthError::InvalidInput);
            }
        } else {
            let pre_bindings = crate::merge_engine::premerge_bindings_for_branch(&self.kv, target);
            let paths: Vec<String> = {
                let g = self.coordinator.graph().read();
                pre_bindings
                    .iter()
                    .filter_map(|(_, rid)| g.get_revision(*rid).map(|r| r.file_path.clone()))
                    .collect()
            };
            MergePreflight::begin_with_snapshot(
                std::sync::Arc::clone(&self.kv),
                target,
                merge_id,
                &paths,
                &self.spec_paths,
                &pre_bindings,
            )
            .map_err(|e| match e {
                MergePreflightError::Locked { .. } | MergePreflightError::SpeculativeConflict { .. } => {
                    AuthError::Forbidden
                }
                MergePreflightError::Cas(_) => AuthError::InvalidInput,
            })?;
        }

        let ctx = crate::merge_engine::MergeContext {
            merge_id,
            ours_branch: target,
            theirs_branch: source,
            base_branch: target,
            target_branch: target,
            strategy,
        };
        ctx.persist(&self.kv);

        saga.persist(merge_id, SagaPhase::Intent);
        report_merge_progress(
            &mut timeline,
            &mut progress_sink,
            1,
            t0,
            "Intent",
            "Merge initiated",
        );

        saga.persist(merge_id, SagaPhase::Classifying);
        let phase_a = {
            let g = self.coordinator.graph().read();
            crate::merge_engine::phase_a_for_merge(
                &g, &self.kv, merge_id, target, source, target, target,
            )
        };
        report_merge_progress(
            &mut timeline,
            &mut progress_sink,
            2,
            t0,
            "Classifying",
            format!(
                "Classified {} identities: {} resolved, {} conflicts, {} renames",
                phase_a.classified.len(),
                phase_a.report.resolved_count,
                phase_a.report.conflicts.len(),
                phase_a.report.rename_detections.len(),
            ),
        );

        let has_conflicts =
            phase_a.report.status == crate::merge_engine::MergeWorkflowStatus::RequiresResolution;

        if has_conflicts && strategy.is_none() {
            let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
            let merge_in_progress = gate.is_query_blocked(self.active_branch())
                || merge_lock_holder(&self.kv, self.active_branch()).is_some();
            let meta = self.meta_at_commit(
                merge_in_progress,
                t0.elapsed().as_millis() as u64,
                phase_a.classified.len(),
                None,
                false,
                Some(target),
            );
            self.audit.record_sync(session_id, "merge_branch");
            return Ok(MergeBranchResponse {
                merge_id_hex: hex16(&merge_id.0),
                source_branch_hex: source_branch_hex.to_string(),
                target_branch_hex: target_branch_hex.to_string(),
                saga_phase: "Classifying".into(),
                streaming,
                message: format!(
                    "Phase A complete: {} conflicts require resolution. Re-call with merge_id and strategy='ours' or 'theirs'.",
                    phase_a.report.conflicts.len()
                ),
                meta,
                resolved_count: phase_a.report.resolved_count,
                conflicts: phase_a.report.conflicts,
                rename_detections: phase_a.report.rename_detections,
                promoted_count: 0,
                orphaned_count: 0,
                signature_drift: vec![],
                dangling_edges_removed: 0,
                cardinality_violations: vec![],
                needs_edge_regen_count: 0,
                edges_regenerated: 0,
                signature_reresolved: 0,
                progress: timeline,
            });
        }

        saga.persist(merge_id, SagaPhase::Promoting);
        let phase_b = {
            let mut g = self.coordinator.graph().write();
            crate::merge_engine::phase_b_promote(
                &self.kv,
                &mut g,
                target,
                &phase_a.classified,
                strategy,
            )
        };
        report_merge_progress(
            &mut timeline,
            &mut progress_sink,
            3,
            t0,
            "Promoting",
            format!(
                "Promoted {} revisions, orphaned {}",
                phase_b.promoted.len(),
                phase_b.orphaned_revisions.len(),
            ),
        );

        saga.persist(merge_id, SagaPhase::EdgeBatch { seq: 0 });
        let recon_job = Self::merge_reconciliation_job_id(merge_id);
        self.reconciliation_tracker.register(target, recon_job);
        let phase_c = {
            let mut g = self.coordinator.graph().write();
            crate::merge_engine::phase_c_reconcile_edges_full(
                &mut g,
                &self.kv,
                Some(&self.body_store),
                target,
                &phase_b.promoted,
                Some(&phase_a.classified),
            )
        };
        if phase_c.needs_edge_regen.is_empty() {
            self.reconciliation_tracker.on_job_complete(target, recon_job);
        }
        for batch in &phase_c.saga_batches {
            crate::merge_saga_batch::persist_saga_edge_batch(&self.kv, merge_id, batch);
            saga.persist_batch_marker(merge_id, batch.seq);
        }
        report_merge_progress(
            &mut timeline,
            &mut progress_sink,
            4,
            t0,
            "EdgeReconciliation",
            format!(
                "Regenerated {} revisions, checked {} edges: {} dangling, {} drift, {} reresolved",
                phase_c.edges_regenerated,
                phase_c.edges_checked,
                phase_c.dangling_edges_removed,
                phase_c.signature_drifts.len(),
                phase_c.signature_reresolved,
            ),
        );

        saga.persist(merge_id, SagaPhase::Committed);
        let _ = crate::merge_engine::append_merge_committed(
            self.coordinator.wal().as_ref(),
            merge_id,
            &phase_b.promoted,
        );
        crate::merge_saga_batch::purge_saga_edge_payloads(&self.kv, merge_id);
        crate::merge_engine::MergeContext::remove(&self.kv, merge_id);
        let _ = release_merge_lock(&self.kv, target, merge_id);
        report_merge_progress(
            &mut timeline,
            &mut progress_sink,
            5,
            t0,
            "Committed",
            "Merge committed",
        );

        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress = gate.is_query_blocked(self.active_branch())
            || merge_lock_holder(&self.kv, self.active_branch()).is_some();
        let meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            phase_a.classified.len(),
            None,
            false,
            Some(target),
        );
        self.append_merge_metrics_record(crate::merge_metrics::MergeMetricsRecord {
            merge_id_hex: hex16(&merge_id.0),
            started_at_ms: query_now_ms().saturating_sub(t0.elapsed().as_millis() as u64),
            phase_durations_ms: Self::phase_durations_from_timeline(&timeline),
            edges_regenerated: phase_c.edges_regenerated,
            dangling_edges_removed: phase_c.dangling_edges_removed,
            signature_reresolved: phase_c.signature_reresolved,
            cardinality_violations: phase_c.cardinality_violations.clone(),
            resumed_from_phase: None,
            compensated: false,
        });
        self.audit.record_sync(session_id, "merge_branch");
        Ok(MergeBranchResponse {
            merge_id_hex: hex16(&merge_id.0),
            source_branch_hex: source_branch_hex.to_string(),
            target_branch_hex: target_branch_hex.to_string(),
            saga_phase: "Committed".into(),
            streaming,
            message: format!(
                "Merge complete: {} promoted, {} orphaned, {} edges regenerated, {} dangling removed",
                phase_b.promoted.len(),
                phase_b.orphaned_revisions.len(),
                phase_c.edges_regenerated,
                phase_c.dangling_edges_removed,
            ),
            meta,
            resolved_count: phase_a.report.resolved_count,
            conflicts: phase_a.report.conflicts,
            rename_detections: phase_a.report.rename_detections,
            promoted_count: phase_b.promoted.len(),
            orphaned_count: phase_b.orphaned_revisions.len(),
            signature_drift: phase_c.signature_drifts,
            dangling_edges_removed: phase_c.dangling_edges_removed,
            cardinality_violations: phase_c.cardinality_violations,
            needs_edge_regen_count: phase_c.needs_edge_regen.len(),
            edges_regenerated: phase_c.edges_regenerated,
            signature_reresolved: phase_c.signature_reresolved,
            progress: timeline,
        })
    }

    /// **FR-1.4** — load `.cis/grammar.lock` (validated) and `.cis/ranking_policy.yaml` into **`ActiveRankingPolicy`** when present.
    pub fn ingest_cis_config(&self, session_id: u64) -> Result<IngestCisConfigResponse, AuthError> {
        self.require_session(session_id)?;
        let t0 = Instant::now();
        let root = Path::new(&self.repo_root);
        let gl_path = root.join(".cis").join("grammar.lock");
        let rp_path = root.join(".cis").join("ranking_policy.yaml");
        let grammar_lock_path = gl_path.to_string_lossy().into_owned();
        let ranking_policy_path = rp_path.to_string_lossy().into_owned();

        let (grammar_lock_parsed, grammar_python, grammar_lock_status, grammar_lock_error) = if !gl_path.exists() {
            (false, None, "missing_file".to_string(), None)
        } else {
            match std::fs::read(&gl_path) {
                Ok(bytes) => match crate::grammar_lock::GrammarLock::from_yaml_bytes(&bytes) {
                    Ok(g) => (true, Some(g.python), "applied".to_string(), None),
                    Err(e) => (
                        false,
                        None,
                        "yaml_parse_error".to_string(),
                        Some(e.to_string()),
                    ),
                },
                Err(e) => (
                    false,
                    None,
                    "read_error".to_string(),
                    Some(e.to_string()),
                ),
            }
        };

        let (ranking_policy_applied, ranking_policy_status, ranking_policy_error) = if !rp_path.exists() {
            (false, "missing_file".to_string(), None)
        } else {
            match std::fs::read_to_string(&rp_path) {
                Ok(yaml) => match self.policy.try_update_from_yaml(&yaml) {
                    Ok(()) => (true, "applied".to_string(), None),
                    Err(e) => (
                        false,
                        "validation_error".to_string(),
                        Some(e.to_string()),
                    ),
                },
                Err(e) => (
                    false,
                    "read_error".to_string(),
                    Some(e.to_string()),
                ),
            }
        };

        self.audit.record_sync(session_id, "ingest_cis_config");
        let gate = MergeRecoveryGate::new(std::sync::Arc::clone(&self.kv));
        let merge_in_progress = gate.is_query_blocked(self.active_branch())
            || merge_lock_holder(&self.kv, self.active_branch()).is_some();
        let mut meta = self.meta_at_commit(
            merge_in_progress,
            t0.elapsed().as_millis() as u64,
            0,
            None,
            false,
            None,
        );
        if gl_path.exists() && !grammar_lock_parsed {
            meta.degraded_modes.push("grammar_lock_invalid".into());
        }
        if rp_path.exists() && !ranking_policy_applied {
            meta.degraded_modes.push("ranking_policy_rejected".into());
        }
        Ok(IngestCisConfigResponse {
            grammar_lock_path,
            grammar_lock_parsed,
            grammar_python,
            grammar_lock_status,
            grammar_lock_error,
            ranking_policy_path,
            ranking_policy_applied,
            ranking_policy_status,
            ranking_policy_error,
            meta,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cis_wal::IdentityId;

    use crate::graph::{Language, NodeIdentity, NodeKind, NodeRevision, RevisionStatus};
    use crate::merge_lock::acquire_merge_lock;
    use cis_wal::MergeId;

    #[test]
    fn find_symbol_at_requires_snapshot() {
        let rt = CisMcpRuntime::new_dev("/tmp");
        let r = rt.find_symbol_at(0, "x", 1, None, 10);
        assert!(matches!(r, Err(AuthError::InvalidInput)));
    }

    #[test]
    fn find_symbol_at_hits_after_checkpoint() {
        let rt = CisMcpRuntime::new_dev("/tmp");
        let branch = BranchId([0u8; 16]);
        assert_eq!(rt.revision_index().branch_id(), branch);
        {
            let mut g = rt.graph_mutex().write();
            let i = IdentityId([11u8; 16]);
            let rev_id = NodeRevisionId([22u8; 16]);
            g.put_identity(NodeIdentity {
                identity_id: i,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: rev_id,
                identity_id: i,
                branch_id: branch,
                status: RevisionStatus::Active,
                qualified_name: "p::mcp_at".into(),
                file_path: "p.py".into(),
                body_hash: [3u8; 32],
                signature_hash: [4u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: SourceSpan {
                    start_line: 1,
                    start_col: 1,
                    end_line: 1,
                    end_col: 20,
                },
            });
        }
        rt.record_time_travel_checkpoint(1);
        let resp = rt.find_symbol_at(0, "mcp_at", 1, None, 10).unwrap();
        assert_eq!(resp.matches.len(), 1);
        assert_eq!(resp.meta.query_at_commit, Some("1".into()));
        assert_eq!(resp.matches[0].start_line, 1);
        assert_eq!(resp.matches[0].start_col, 1);
    }

    #[test]
    fn write_file_writes_under_repo() {
        let dir = std::env::temp_dir().join(format!("cis_mcp_write_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rt = CisMcpRuntime::new_dev(&dir.to_string_lossy());
        let rel = "w.txt";
        let resp = rt.write_file(0, rel, "hello", true).unwrap();
        assert_eq!(resp.bytes_written, 5);
        let p = dir.join(rel);
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_and_switch_branch_mcp() {
        use crate::revision_index::revision_binding_kv_key;

        let dir = std::env::temp_dir().join(format!("cis_branch_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rt = CisMcpRuntime::new_dev(&dir.to_string_lossy());
        let main = rt.active_branch();
        let i = IdentityId([44u8; 16]);
        let rev = NodeRevisionId([55u8; 16]);
        rt.kv()
            .set(&revision_binding_kv_key(main, i), rev.0.to_vec());

        let created = rt.create_branch(0, "feature", None).unwrap();
        assert!(created.bindings_copied >= 1);

        let switched = rt.switch_branch(0, "feature").unwrap();
        assert_eq!(switched.branch_name, "feature");
        assert_eq!(rt.active_branch().0, parse_branch_id_hex(&switched.branch_id_hex).unwrap().0);

        let listed = rt.list_branches(0).unwrap();
        assert!(listed.branches.iter().any(|b| b.name == "feature"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn semantic_search_uses_vector_similarity_when_embedded() {
        let dir = std::env::temp_dir().join(format!("cis_sem_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rt = CisMcpRuntime::new_dev(&dir.to_string_lossy());
        let branch = rt.revision_index().branch_id();
        let auth_body = "def authenticate_user(password): verify_credentials(password)";
        let other_body = "def render_board(): pass";
        let auth_hash = [101u8; 32];
        let other_hash = [102u8; 32];
        rt.body_store.put(auth_hash, auth_body.as_bytes().to_vec());
        rt.body_store.put(other_hash, other_body.as_bytes().to_vec());

        let model_id = rt.embedder.model_id().to_string();
        let auth_vec = rt
            .embedder
            .embed_batch(&[auth_body.to_string()])
            .unwrap()
            .pop()
            .unwrap();
        let other_vec = rt
            .embedder
            .embed_batch(&[other_body.to_string()])
            .unwrap()
            .pop()
            .unwrap();
        rt.coordinator
            .vector()
            .set_embedding(auth_hash, auth_vec, &model_id);
        rt.coordinator
            .vector()
            .set_embedding(other_hash, other_vec, &model_id);

        let auth_i = IdentityId([31u8; 16]);
        let other_i = IdentityId([32u8; 16]);
        let auth_r = NodeRevisionId([41u8; 16]);
        let other_r = NodeRevisionId([42u8; 16]);
        {
            let mut g = rt.graph_mutex().write();
            for (i, r, name, bh) in [
                (auth_i, auth_r, "auth.authenticate_user", auth_hash),
                (other_i, other_r, "game.render_board", other_hash),
            ] {
                g.put_identity(NodeIdentity {
                    identity_id: i,
                    kind: NodeKind::Function,
                });
                g.put_revision(NodeRevision {
                    revision_id: r,
                    identity_id: i,
                    branch_id: branch,
                    status: RevisionStatus::Active,
                    qualified_name: name.into(),
                    file_path: "x.py".into(),
                    body_hash: bh,
                    signature_hash: [0u8; 32],
                    language: Language::Python,
                    parent_revision_id: None,
                    rename_source_id: None,
                    tombstoned_at_ms: None,
                    span: SourceSpan::UNKNOWN,
                });
                let cid = crate::chunk_id::chunk_id(i, r, 0);
                rt.coordinator.vector().register(cid, bh);
            }
        }

        let resp = rt.semantic_search(0, "authenticate", None, 5).unwrap();
        assert!(!resp.hits.is_empty());
        assert!(
            resp.hits[0]
                .qualified_name
                .contains("authenticate"),
            "expected auth symbol first, got {:?}",
            resp.hits
        );
        assert!(resp.hits[0].score > resp.hits.get(1).map(|h| h.score).unwrap_or(0.0));
        assert!(!resp.meta.degraded_modes.contains(&"semantic_degraded".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn semantic_search_degraded_without_embeddings() {
        let rt = CisMcpRuntime::new_dev("/tmp");
        let branch = rt.revision_index().branch_id();
        let i = IdentityId([51u8; 16]);
        let r = NodeRevisionId([61u8; 16]);
        {
            let mut g = rt.graph_mutex().write();
            g.put_identity(NodeIdentity {
                identity_id: i,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: r,
                identity_id: i,
                branch_id: branch,
                status: RevisionStatus::Active,
                qualified_name: "auth.login".into(),
                file_path: "a.py".into(),
                body_hash: [9u8; 32],
                signature_hash: [0u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: SourceSpan::UNKNOWN,
            });
            let cid = crate::chunk_id::chunk_id(i, r, 0);
            rt.coordinator.vector().register(cid, [9u8; 32]);
        }
        let resp = rt.semantic_search(0, "auth", None, 5).unwrap();
        assert!(resp
            .meta
            .degraded_modes
            .contains(&"semantic_degraded".to_string()));
        assert!(resp.meta.degraded_modes.contains(&"embeddings_pending".to_string()));
    }

    #[test]
    fn merge_ttl_sweep_clears_stale_lock() {
        let rt = CisMcpRuntime::new_dev("/tmp");
        let b = rt.revision_index().branch_id();
        let m = MergeId([8u8; 16]);
        acquire_merge_lock(rt.kv.as_ref(), b, m).unwrap();
        crate::merge_lock::record_merge_started_ms(rt.kv.as_ref(), m, 0);
        let n = rt.run_merge_ttl_sweep(0).unwrap();
        assert_eq!(n, 1);
        assert!(crate::merge_lock::merge_lock_holder(rt.kv.as_ref(), b).is_none());
    }

    fn branch_hex(b: BranchId) -> String {
        b.0.iter().map(|x| format!("{:02x}", x)).collect()
    }

    #[test]
    fn merge_branch_e2e_promotes_source_with_wal_marker() {
        use crate::merge_engine::MergeStrategy;
        use crate::revision_index::revision_binding_kv_key;
        use cis_wal::MutationKind;

        let target = BranchId([0u8; 16]);
        let source = BranchId([2u8; 16]);
        let i1 = IdentityId([11u8; 16]);
        let r_base = NodeRevisionId([21u8; 16]);
        let r_theirs = NodeRevisionId([23u8; 16]);

        let rt = CisMcpRuntime::new_dev("/tmp");
        {
            let mut g = rt.graph_mutex().write();
            g.put_identity(NodeIdentity {
                identity_id: i1,
                kind: NodeKind::Function,
            });
            g.put_revision(NodeRevision {
                revision_id: r_base,
                identity_id: i1,
                branch_id: target,
                status: RevisionStatus::Active,
                qualified_name: "f".into(),
                file_path: "f.py".into(),
                body_hash: [10u8; 32],
                signature_hash: [0u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: SourceSpan::UNKNOWN,
            });
            g.put_revision(NodeRevision {
                revision_id: r_theirs,
                identity_id: i1,
                branch_id: source,
                status: RevisionStatus::Active,
                qualified_name: "f".into(),
                file_path: "f.py".into(),
                body_hash: [30u8; 32],
                signature_hash: [0u8; 32],
                language: Language::Python,
                parent_revision_id: None,
                rename_source_id: None,
                tombstoned_at_ms: None,
                span: SourceSpan::UNKNOWN,
            });
        }
        rt.kv
            .set(&revision_binding_kv_key(target, i1), r_base.0.to_vec());
        rt.kv
            .set(&revision_binding_kv_key(source, i1), r_theirs.0.to_vec());

        let resp = rt
            .merge_branch(
                0,
                &branch_hex(source),
                &branch_hex(target),
                Some(MergeStrategy::Theirs),
                None,
                None,
            )
            .unwrap();

        assert!(!resp.streaming);
        assert_eq!(resp.saga_phase, "Committed");
        assert_eq!(resp.promoted_count, 1);
        assert_eq!(
            rt.kv.get(&revision_binding_kv_key(target, i1)),
            Some(r_theirs.0.to_vec())
        );
        assert!(rt.wal().iter_all().iter().any(|r| {
            matches!(r.kind, MutationKind::Merge { .. })
        }));
    }

    struct CountingSink(std::sync::Arc<std::sync::atomic::AtomicU32>);

    impl MergeProgressSink for CountingSink {
        fn on_progress(&mut self, _event: &MergeProgressEvent, _step: u32, _total: u32) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn merge_branch_invokes_progress_sink() {
        use crate::merge_engine::MergeStrategy;

        let target = BranchId([0u8; 16]);
        let source = BranchId([2u8; 16]);
        let rt = CisMcpRuntime::new_dev("/tmp");
        let count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut sink = CountingSink(std::sync::Arc::clone(&count));

        let _ = rt
            .merge_branch(
                0,
                &branch_hex(source),
                &branch_hex(target),
                Some(MergeStrategy::Theirs),
                None,
                Some(&mut sink),
            )
            .unwrap();

        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            MERGE_PROGRESS_TOTAL
        );
    }
}
