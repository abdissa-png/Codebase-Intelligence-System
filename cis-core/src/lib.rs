//! Core engine: graph, `WriteCoordinator`, KV, revision index, reconciliation hooks.
//!
//! Spec: `CIS-Architecture-v2.md` (repo root).
//!
//! **Follow-up (architecture):** MCP response/tool DTOs currently live in `mcp_runtime`
//! and are re-exported from this crate. Prefer moving transport-facing types into
//! `cis-mcp` in a later refactor so `cis-core` stays free of MCP surface area.

mod body_blob;
mod body_store;
mod metadata_store;
mod migration;
mod semantic_ann;
mod symbol_body;
mod branch_reconciliation_tracker;
mod branch_registry;
mod chunk_id;
mod confidence;
mod confirm_token;
mod coordinator;
mod daemon_handles;
mod degraded;
mod deletion_absence;
mod edge_target_override;
mod embedder;
mod env_file;
mod fault_injection;
mod consistency_snapshot;
mod embedding_metrics;
mod embedding_queue;
mod embedding_worker;
mod watcher_metrics;
mod worker_heartbeats;
mod fs_sync;
mod git_branch_sync;
mod grammar_lock;
mod graph;
mod graph_consistency;
mod graph_delete_queue;
mod graph_mutation;
mod graph_store;
mod identity_cas;
pub mod identity_resolution;
mod identity_resolver;
mod invariants;
mod index_walk;
mod index_model;
mod call_resolve;
mod python_indexer;
mod ingest;
mod kv;
mod language_indexer;
mod lsp_pool;
mod merge_control;
mod merge_engine;
mod merge_gate;
mod merge_lock;
mod merge_metrics;
mod merge_preflight;
mod merge_saga_batch;
mod mcp_runtime;
mod optimistic_patcher;
mod path_lease;
mod pre_write_snapshot;
mod persistence;
mod policy_watcher;
mod query_context;
mod query_engine;
mod ranking_policy;
mod repo_bootstrap;
mod reconciliation;
mod revision_cow;
mod revision_index;
mod revision_lineage;
mod shared_graph;
mod time_travel;
mod tombstone_gc;
mod typescript_indexer;
mod saga;
mod security;
mod wal_compaction;
mod vector_cleanup_queue;
mod vector_cleanup_worker;
mod vector_backend;
mod vector_store;

pub use body_blob::{
    bodies_dir, bodies_db_path, body_backend_from_env, body_blob_path, gc_bodies,
    gc_bodies_for_branch, gc_bodies_with_store, gc_body_blob_files, gc_body_blobs,
    hydrate_bodies_from_disk,
    hydrate_bodies_from_store, load_body_blob, load_body_blob_with_fallback,
    open_body_blob_store, referenced_body_hashes, save_body_blob, sync_bodies_to_disk,
    sync_bodies_to_store, walk_body_files_pub, BodyBackendKind, BodyBlobStore,
    FileBodyBlobStore,
};
#[cfg(feature = "body-sqlite")]
pub use body_blob::SqliteBodyBlobStore;
pub use metadata_store::{metadata_backend_from_env, store_db_path, MetadataBackendKind};
#[cfg(feature = "body-sqlite")]
pub use metadata_store::MetadataStore;
pub use graph_store::{
    graph_backend_from_env, graph_db_path, graph_json_export_enabled, migrate_graph_json_to_sqlite,
    open_graph_store, rebuild_graph_normalized, save_graph_delta, save_graph_with_backend,
    GraphBackendKind, GraphStore, JsonGraphStore, MigrateGraphReport,
};
#[cfg(feature = "body-sqlite")]
pub use graph_store::SqliteGraphStore;
pub use migration::{
    migrate_bodies_from_files, migrate_kv_ris_to_sqlite, verify_body_migration,
    MigrateBodiesReport, MigrateKvReport,
};
pub use body_store::BodyStore;
pub use semantic_ann::{AnnIndex, FlatAnnIndex};
pub use symbol_body::{resolve_revision_body, BodySource};
pub use branch_reconciliation_tracker::BranchReconciliationTracker;
pub use branch_registry::BranchRegistry;
pub use chunk_id::chunk_id;
pub use confirm_token::{
    clear_token_with_retry, probe_confirm_backend, write_sidecar_confirm_token,
    write_token_with_retry, ConfirmBackendMode, ConfirmTokenBackend,
    FaultInjectingConfirmBackend, SidecarConfirmBackend,
};
pub use confidence::{
    confidence_overall, edge_confidence, freshness_decay, node_confidence_from_inbound,
    path_confidence, path_floor, source_weight,
};
pub use query_engine::{
    count_pruned_expand_neighbors, count_unresolved_definition_edges, expand_context_bfs,
    expand_context_bfs_with_absence, file_hub_revision_for_path, is_opaque_traversal_gate,
    node_hit_confidence, node_hit_confidence_with_absence, outbound_context_edges,
    rename_successor_identity, resolve_definition_target, resolve_definition_target_with_absence,
    resolve_edge_target, resolve_edge_target_with_absence, resolve_identity_revision,
    resolve_identity_revision_with_absence, ExpandContextResult,
};
pub use coordinator::{CoordinatorError, CoordinatorPersistence, WriteCoordinator};
pub use daemon_handles::CisDaemonHandles;
pub use degraded::{disk_free_percent, DiskPressureFlag, VectorDegradedController};
pub use deletion_absence::{deleted_key, DeletionAbsenceStore};
pub use edge_target_override::{delete_eto_for_source_revision, eto_key, EdgeTargetOverrideStore};
pub use embedder::{
    api_embedder_configured, cosine_similarity, embedder_from_env, embeddings_endpoint_url,
    l2_normalize, EmbedError, Embedder, StubEmbedder, STUB_EMBED_DIM,
};
pub use env_file::load_env_file;
#[cfg(feature = "api-embeddings")]
pub use embedder::{ApiEmbedder, ApiEmbedderConfig};
pub use consistency_snapshot::{ConsistencyStatusSummary, LastConsistencySnapshot};
pub use embedding_metrics::{EmbeddingMetrics, EmbeddingStatusSnapshot};
pub use embedding_queue::{EmbedJob, EmbeddingQueue, EmbeddingQueueState};
pub use embedding_worker::{EmbeddingDrainReport, EmbeddingWorker};
pub use watcher_metrics::{WatcherMetrics, WatcherStatusSnapshot};
pub use worker_heartbeats::{WorkerHeartbeat, WorkerHeartbeatSummary, WorkerHeartbeats};
pub use fs_sync::{
    flush_debounced_reindex, reindex_paths, reindex_persist_snapshots_enabled, reindex_python_paths,
    reindex_paths_on_coordinator, reindex_python_paths_on_coordinator,
    reindex_python_paths_with_config,
    resolve_fs_watch_backend,
    run_fs_sync_loop, spawn_fs_sync_stack, FsPollState, FsSyncConfig, FsWatchBackend,
    IndexDebouncer, DEFAULT_DEBOUNCE_MS, DEFAULT_POLL_MS,
};
pub use git_branch_sync::{GitBranchSyncConfig, git_merge_in_progress};
pub use graph::{
    metadata_for, validate_edge_cardinality, CardinalityViolation, EdgeResolution, EdgeType,
    EdgeTypeMetadata, GraphEdge, InMemoryGraph, Language, NodeIdentity, NodeKind, NodeRevision,
    ReconciliationTier, RevisionStatus, SourceSpan, SourceType,
};
pub use graph_consistency::{check_consistency, ConsistencyReport};
pub use invariants::{
    assert_invariants, check_all_invariants, check_all_invariants_with_mode,
    check_merge_invariants, check_merge_invariants_with_mode, check_write_path_invariants,
    InvariantCheckMode, InvariantContext, InvariantReport, MergeViolation, WritePathViolation,
};
pub use fault_injection::{
    apply_fault, apply_fault_io, injector_from_env, AlwaysFail, CrashAfterNCalls, FailHooks,
    FaultAction, FaultInjected, FaultInjector, IdentityCasDelay, NoOpFaultInjector, RandomDelay,
};
pub use graph_delete_queue::{DeleteJob, DeleteReason, GraphDeleteQueue};
pub use graph_mutation::GraphMutationSet;
pub use identity_cas::{IdentityProvisionalCas, ALLOCATING_TTL_MS};
pub use language_indexer::{
    default_indexers, indexer_for_path, IndexError, LanguageIndexer, PythonIndexer,
};
pub use identity_resolution::{
    best_tombstone_rename, tombstone_all_file_symbols, tombstone_all_file_symbols_with_absence,
    tombstone_orphaned_file_symbols_with_absence, RenameConfig, ResolveOutcome,
};
pub use identity_resolver::{
    IdentityResolver, RenameEvidence, RenameSignalKind,
};
pub use ingest::{
    apply_index_events, apply_index_events_with_config, extract_python_top_level_defs,
    load_file_body, module_map_from_paths, module_map_for_paths, path_to_python_module_key,
    paths_on_branch, python_paths_on_branch,
    body_store_slot_key, branch_id_tag, content_checksum_32, file_body_hash_key,
    identity_cas_semantic_hash, regen_edges_for_file_with_graph,
    regen_edges_for_python_file, regen_edges_for_python_file_with_graph, stable_id_bytes,
    stable_rev_id_bytes, content_rev_id_bytes, symbol_identity_key, FsChangeKind,
    IdentityResolverShell,
    IngestApplyReport, IndexEvent, IndexEventQueue,
};
pub use kv::{durable_kv_subset, durable_kv_subset_for_persist, CasError, MemoryKv, DURABLE_KV_PREFIXES, KvSnapshot};
pub use lsp_pool::{
    lsp_cache_key, lsp_cache_value_with_timestamp, LspCacheSweeper, LspPoolState, LspSessionFlags,
};
pub use mcp_runtime::{
    AxisWeightBreakdown, BranchInfo, BuildContextResponse, CisMcpRuntime, ConfirmPatchResponse,
    CreateBranchResponse, ExplainContextResponse, ExplainCounts, FileImportsResponse,
    FindSymbolResponse, GetSymbolBodyResponse, GoToDefinitionResponse,
    HybridSearchResponse, IndexStatusResponse, EmbeddingStatusResponse, SystemStatusResponse,
    IngestCisConfigResponse, ListBranchesResponse,
    ListMergeMetricsResponse,
    MergeBranchResponse, MergeProgressEvent, MergeProgressSink, RevertPatchResponse,
    SemanticSearchHit, SemanticSearchResponse, SwitchBranchResponse, SymbolHit, SymbolHitsResponse,
    VerifyAuditChainResponse, GraphConsistencyResponse,
    WhyNoDefinitionResponse, WriteFileResponse, MERGE_PROGRESS_TOTAL,
};
pub use merge_control::{
    load_msnap_bindings, MergeCancelError, MergeCancelReport, MergeControl,
};
pub use merge_engine::{
    append_merge_cancelled_after_control, append_merge_cancelled_marker,
    append_merge_committed, classify_identity_stub, merge_phase_a_report, phase_a_classify,
    phase_a_classify_with_base, phase_a_for_merge, phase_b_promote, phase_c_reconcile_edges,
    phase_c_reconcile_edges_full, premerge_bindings_for_branch, merge_reconciliation_job_id,
    recover_inflight_merges,
    resume_merge, run_phase_a_classify, ClassifiedMergeIdentity, MergeContext,
    MergeIdentityClass, MergeRecoveryReport, MergeReport, MergeStrategy, MergeWorkflowStatus,
    PhaseAResult, PhaseBResult, PhaseCResult, ResumePendingResolution,
};
pub use merge_gate::MergeRecoveryGate;
pub use merge_metrics::{
    append_merge_metrics, read_merge_metrics, read_merge_metrics_filtered, rollup_merge_metrics,
    MergeMetricsRecord, MergeMetricsRollup,
};
pub use merge_lock::{
    acquire_merge_lock, merge_lock_holder, merge_lock_key, merge_started_key, merge_ttl_expired_ms,
    record_merge_started_ms, release_merge_lock, scan_merge_lock_holders,
    sweep_all_expired_merge_intents, sweep_expired_merge_intents,
};
pub use merge_preflight::{MergePreflight, MergePreflightError};
pub use merge_saga_batch::{
    apply_and_persist_saga_batches, apply_saga_edge_batch, compensate_saga_edge_batches,
    load_saga_edge_batches, persist_saga_edge_batch, purge_saga_edge_payloads, SagaEdgeBatch,
};
pub use optimistic_patcher::{OptimisticPatcher, OptimisticPatchError};
pub use path_lease::{LeaseError, PathLeaseManager, SessionId, SpeculativePathTracker};
pub use pre_write_snapshot::PreWriteSnapshotStore;
pub use persistence::{
    cis_dir, graph_snapshot_path, kv_snapshot_path, load_kv_snapshot, load_state_from_cis_dir,
    load_workspace_into, open_persisted_coordinator, save_graph_snapshot, save_kv_snapshot,
    save_vector_snapshot, save_workspace_snapshots, snapshot_persist_enabled,
    vector_snapshot_path, wal_path, PersistenceLoadReport, VectorSnapshot,
};
pub use policy_watcher::{
    ActiveRankingPolicy, PolicyBootstrapError, PolicyFileReloader, PolicyReloadOutcome,
};
pub use query_context::{
    build_context_truncated, hybrid_search_rerank, CharApproxTokenizer, ContextRanker, DedupShell,
    HybridSearchCandidate, QueryCancelFlag, QueryMeta, Tokenizer,
};
pub use ranking_policy::{
    AxisWeights, BudgetOvershootPolicy, BudgetPolicy, ConflictResolution, DedupPolicy, DedupStrategy,
    EdgeTypeWeights, HybridSearchPolicy, OverlapDefinition, PolicyLoadError, PolicyValidationError,
    RankingPolicy, RankingPolicySnapshot, RecencyPolicy, RecencySource,
};
pub use index_walk::index_respect_gitignore;
pub use repo_bootstrap::{
    bootstrap_index_from_repo, bootstrap_python_workspace_into_graph,
    bootstrap_python_workspace_on_coordinator, collect_indexable_files, collect_py_files,
    collect_source_files,
};
pub use shared_graph::{graph_rwlock_enabled, SharedInMemoryGraph};
pub use time_travel::{
    git_oid_anchor_key, overlay_at_wal_log, overlay_at_wal_log_at, parse_wal_log_anchor,
    record_committed_snapshot, record_committed_snapshot_legacy,
    record_git_oid_wal_log, resolve_commit_anchor_to_wal_log, resolve_epoch_for_wal_log,
    resolve_wal_log_from_git_oid, wal_log_anchor_key,
};
pub use reconciliation::{PeriodicReconciler, RecoveryReport, ReconciliationEngine};
pub use revision_cow::{ris_snapshot_kv_key, RevisionIndexCow};
pub use revision_index::{
    branch_ancestry, fork_branch_bindings, revision_binding_kv_key, RevisionIndex,
};
pub use revision_lineage::{
    lineage_for_identity, lineage_from_revision, merge_base_revision,
    retire_revision_to_tombstone, LineageLink, LineageOptions, LineageStep,
};
pub use saga::{MergeSagaOrchestrator, SagaPhase};
pub use security::{
    append_audit_epoch, verify_audit_chain, AuditChainVerification, AuditLog, AuditRecord,
    AuthError, AuthProvider, DurableAuditQueue, ProductionAuditSink, QuotaError, QuotaGuard,
    QuotaTracker, Session,
};
pub use tombstone_gc::{GcDrainReport, TombstoneGcWorker};
pub use wal_compaction::WalCompactionScheduler;
pub use vector_cleanup_queue::VectorCleanupQueue;
pub use vector_cleanup_worker::{VectorCleanupDrainReport, VectorCleanupWorker};
pub use vector_backend::{InMemoryVectorBackend, VectorBackend};
pub use vector_store::{
    FlakyVectorStore, InMemoryVectorStore, VectorBodyRecord, VectorChunkRecord, VectorChunkStore,
    VectorDeleteError, VectorEntry, VectorStoreSnapshot, VECTOR_STORE_SNAPSHOT_VERSION,
};
