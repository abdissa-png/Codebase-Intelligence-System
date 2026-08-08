//! **`cisd`** — CIS daemon: background policy / merge TTL / vector cleanup, optional **`--mcp`** stdio server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cis_core::{
    cis_dir, defer_vector_snapshot_load, embedder_from_env, kv_snapshot_path, load_env_file,
    load_kv_snapshot, load_vector_into, open_persisted_coordinator, ActiveRankingPolicy, BodyStore,
    CisDaemonHandles, CisMcpRuntime, EmbeddingWorker, MemoryKv, MergeRecoveryGate,
    MergeSagaOrchestrator, PolicyFileReloader, PolicyReloadOutcome, VectorCleanupQueue,
    VectorCleanupWorker, disk_free_percent, sweep_all_expired_merge_intents,
};
use cis_mcp::{build_runtime, run_stdio_slot, McpRuntimeSlot};

fn policy_path() -> PathBuf {
    std::env::var_os("CIS_POLICY_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".cis/ranking_policy.yaml"))
}

fn vector_dlq() -> Arc<VectorCleanupQueue> {
    if let Some(p) = std::env::var_os("CIS_VECTOR_DLQ_PATH") {
        match VectorCleanupQueue::open_persistent(PathBuf::from(p)) {
            Ok(q) => return Arc::new(q),
            Err(e) => {
                eprintln!(
                    "cisd: CIS_VECTOR_DLQ_PATH open failed, using in-memory DLQ: {}",
                    e
                );
            }
        }
    }
    Arc::new(VectorCleanupQueue::new())
}

#[allow(clippy::too_many_arguments)]
fn spawn_background_threads(
    coord: Arc<cis_core::WriteCoordinator>,
    kv: Arc<MemoryKv>,
    saga: Arc<MergeSagaOrchestrator>,
    reloader: Option<Arc<PolicyFileReloader>>,
    default_merge_ttl_hours: u32,
    dlq: Arc<VectorCleanupQueue>,
    handles: CisDaemonHandles,
    repo_root: PathBuf,
    merge_control: Arc<cis_core::MergeControl>,
    body_store: Arc<BodyStore>,
) {
    let cleaner = VectorCleanupWorker::new(Arc::clone(&dlq));
    let _gate = MergeRecoveryGate::new(Arc::clone(&kv));
    let audit_epoch = Arc::clone(&handles.audit);
    let kv_epoch = Arc::clone(&kv);
    let worker_hb = Arc::clone(&handles.worker_heartbeats);
    let embed_metrics = Arc::clone(&handles.embedding_metrics);
    let last_consistency = handles.last_consistency.clone();
    std::thread::Builder::new()
        .name("cis-audit-epoch".into())
        .spawn(move || loop {
            worker_hb.tick("cis-audit-epoch");
            if let Some(digest) = audit_epoch.maybe_seal_epoch(&kv_epoch) {
                eprintln!("cisd: audit epoch sealed digest={digest}");
            }
            std::thread::sleep(Duration::from_secs(300));
        })
        .expect("spawn audit epoch");

    let coord_wal = Arc::clone(&coord);
    let kv_wal = Arc::clone(&kv);
    let saga_wal = Arc::clone(&saga);
    let audit_wal = Arc::clone(&handles.audit);
    let gate_wal = MergeRecoveryGate::new(Arc::clone(&kv));
    let wal_sched = handles.wal_compaction.clone();
    let policy_wal = reloader.clone();
    let disk_flag_wal = Arc::clone(&handles.disk_pressure);
    let worker_hb_wal = Arc::clone(&handles.worker_heartbeats);
    std::thread::Builder::new()
        .name("cis-wal-compaction".into())
        .spawn(move || loop {
            worker_hb_wal.tick("cis-wal-compaction");
            let wal_max = policy_wal
                .as_ref()
                .map(|r| r.active().snapshot().wal_max_bytes)
                .unwrap_or(256 * 1024 * 1024);
            if let Some(rep) = wal_sched.run_once(
                coord_wal.wal().as_ref(),
                wal_max,
                &kv_wal,
                &saga_wal,
                &gate_wal,
                &audit_wal,
            ) {
                eprintln!(
                    "cisd: wal_compaction dropped={} freed_est={}",
                    rep.records_dropped, rep.bytes_estimated_freed
                );
            }
            let interval = if disk_flag_wal.disk_pressure() {
                Duration::from_secs(60)
            } else {
                wal_sched.interval
            };
            std::thread::sleep(interval);
        })
        .expect("spawn wal compaction");

    let coord_rec = Arc::clone(&coord);
    let kv_rec = Arc::clone(&kv);
    let saga_rec = Arc::clone(&saga);
    let audit_rec = Arc::clone(&handles.audit);
    let gate_rec = MergeRecoveryGate::new(Arc::clone(&kv));
    let periodic = handles.periodic_reconciler.clone();
    let merge_ctl = Arc::clone(&merge_control);
    let body_rec = Arc::clone(&body_store);
    let worker_hb_rec = Arc::clone(&handles.worker_heartbeats);
    let last_consistency_rec = last_consistency.clone();
    std::thread::Builder::new()
        .name("cis-periodic-reconciler".into())
        .spawn(move || loop {
            worker_hb_rec.tick("cis-periodic-reconciler");
            let branches: Vec<cis_wal::BranchId> = kv_rec
                .scan_prefix("branch_reg:")
                .into_iter()
                .filter_map(|(_k, v)| {
                    if v.len() == 16 {
                        let mut b = [0u8; 16];
                        b.copy_from_slice(&v);
                        Some(cis_wal::BranchId(b))
                    } else {
                        None
                    }
                })
                .collect();
            let rep = periodic.run_once(
                &coord_rec,
                &saga_rec,
                &kv_rec,
                &merge_ctl,
                &body_rec,
                &gate_rec,
                &audit_rec,
                &branches,
                Some(&last_consistency_rec),
            );
            if rep.sagas_compensated > 0 || rep.wal_replayed > 0 {
                eprintln!("cisd: periodic_reconcile {:?}", rep);
            }
            std::thread::sleep(periodic.interval);
        })
        .expect("spawn periodic reconciler");

    let coord_gc = Arc::clone(&coord);
    let kv_gc = Arc::clone(&kv);
    let dlq_gc = Arc::clone(&dlq);
    let delete_q = Arc::clone(&handles.graph_delete_queue);
    let audit_gc = Arc::clone(&handles.audit);
    let tombstone = handles.tombstone_gc.clone();
    let policy_gc = reloader.clone();
    let disk_flag = Arc::clone(&handles.disk_pressure);
    let worker_hb_gc = Arc::clone(&handles.worker_heartbeats);
    std::thread::Builder::new()
        .name("cis-tombstone-gc".into())
        .spawn(move || loop {
            worker_hb_gc.tick("cis-tombstone-gc");
            let policy = policy_gc
                .as_ref()
                .map(|r| r.active().snapshot())
                .unwrap_or_else(|| cis_core::RankingPolicy::default().snapshot());
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let branch = cis_wal::BranchId([0u8; 16]);
            let eligible = tombstone.scan_eligible(
                coord_gc.graph(),
                &kv_gc,
                &coord_gc,
                &policy,
                now_ms,
            );
            if !eligible.is_empty() {
                tombstone.enqueue_deletes(&eligible, &delete_q, branch);
                audit_gc.record_sync(
                    0,
                    format!("tombstone_gc_scan eligible={}", eligible.len()),
                );
            }
            let mut fail_streak = 0u32;
            loop {
                let rep = tombstone.drain_batch(
                    coord_gc.graph(),
                    &kv_gc,
                    &dlq_gc,
                    &delete_q,
                    32,
                );
                if rep.attempted == 0 {
                    break;
                }
                if rep.requeued > 0 {
                    fail_streak = fail_streak.saturating_add(1);
                    std::thread::sleep(Duration::from_millis(
                        VectorCleanupWorker::next_backoff_ms(fail_streak.min(8), 30_000),
                    ));
                } else {
                    fail_streak = 0;
                }
            }
            let interval = if disk_flag.disk_pressure() {
                Duration::from_secs(60)
            } else {
                Duration::from_secs(1800)
            };
            std::thread::sleep(interval);
        })
        .expect("spawn tombstone gc");

    let disk_flag_mon = Arc::clone(&handles.disk_pressure);
    let policy_disk = reloader.clone();
    let data_dir = repo_root.join(".cis");
    let worker_hb_disk = Arc::clone(&handles.worker_heartbeats);
    std::thread::Builder::new()
        .name("cis-disk-monitor".into())
        .spawn(move || loop {
            worker_hb_disk.tick("cis-disk-monitor");
            let min_free = policy_disk
                .as_ref()
                .map(|r| r.active().snapshot().disk_min_free_pct as f64)
                .unwrap_or(10.0);
            let free_pct = disk_free_percent(&data_dir);
            let under = free_pct < min_free;
            let was = disk_flag_mon.disk_pressure();
            disk_flag_mon.set_disk_pressure(under);
            if under && !was {
                eprintln!(
                    "cisd: disk pressure ON (free={free_pct:.1}% < min={min_free}%)"
                );
            } else if !under && was {
                eprintln!("cisd: disk pressure OFF (free={free_pct:.1}%)");
            }
            std::thread::sleep(Duration::from_secs(30));
        })
        .expect("spawn disk monitor");

    let kv_merge = Arc::clone(&kv);
    let merge_ttl_reloader = reloader.clone();
    let worker_hb_merge = Arc::clone(&handles.worker_heartbeats);
    std::thread::Builder::new()
        .name("cis-merge-ttl-sweep".into())
        .spawn(move || {
            loop {
                worker_hb_merge.tick("cis-merge-ttl-sweep");
                let ttl_hours = merge_ttl_reloader
                    .as_ref()
                    .map(|r| r.active().snapshot().merge_ttl_hours)
                    .unwrap_or(default_merge_ttl_hours);
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let n = sweep_all_expired_merge_intents(kv_merge.as_ref(), now_ms, ttl_hours);
                if n > 0 {
                    eprintln!("cisd: merge_ttl_sweep released {} stale merge lock(s)", n);
                }
                std::thread::sleep(Duration::from_secs(30));
            }
        })
        .expect("spawn merge ttl sweep");

    let coord_bg = Arc::clone(&coord);
    let worker_hb_vec = Arc::clone(&handles.worker_heartbeats);
    std::thread::Builder::new()
        .name("cis-vector-cleanup".into())
        .spawn(move || {
            let mut fail_streak: u32 = 0;
            loop {
                worker_hb_vec.tick("cis-vector-cleanup");
                let rep = cleaner.drain_batch(coord_bg.vector_chunk_store(), 64);
                let delay = if rep.attempted == 0 {
                    fail_streak = 0;
                    Duration::from_millis(500)
                } else if rep.requeued > 0 {
                    fail_streak = fail_streak.saturating_add(1);
                    Duration::from_millis(VectorCleanupWorker::next_backoff_ms(
                        fail_streak.min(8),
                        30_000,
                    ))
                } else {
                    fail_streak = 0;
                    Duration::from_millis(250)
                };
                std::thread::sleep(delay);
            }
        })
        .expect("spawn vector cleanup");

    let coord_embed = Arc::clone(&coord);
    let kv_embed = Arc::clone(&kv);
    let vector_deg = Arc::clone(&handles.vector_degraded);
    let embedder = embedder_from_env();
    eprintln!(
        "cisd: embedding worker model_id={} api={}",
        embedder.model_id(),
        cis_core::api_embedder_configured()
    );
    let worker_hb_embed = Arc::clone(&handles.worker_heartbeats);
    std::thread::Builder::new()
        .name("cis-embedding-worker".into())
        .spawn(move || {
            let body_store = BodyStore::new(Arc::clone(&kv_embed));
            let mut fail_streak: u32 = 0;
            loop {
                worker_hb_embed.tick("cis-embedding-worker");
                let rep = EmbeddingWorker::drain_batch(
                    &coord_embed,
                    embedder.as_ref(),
                    &body_store,
                    coord_embed.vector(),
                    16,
                );
                embed_metrics.record_drain(rep);
                vector_deg.set_queue_depth(coord_embed.embedding_queue_depth() as u64);
                let delay = if rep.attempted == 0 {
                    fail_streak = 0;
                    Duration::from_millis(500)
                } else if rep.requeued > 0 {
                    fail_streak = fail_streak.saturating_add(1);
                    Duration::from_millis(EmbeddingWorker::next_backoff_ms(
                        fail_streak.min(8),
                        30_000,
                    ))
                } else {
                    fail_streak = 0;
                    Duration::from_millis(250)
                };
                std::thread::sleep(delay);
            }
        })
        .expect("spawn embedding worker");

    if let Some(rel) = reloader {
        let path = rel.path().to_path_buf();
        let coord_policy = Arc::clone(&coord);
        let worker_hb_policy = Arc::clone(&handles.worker_heartbeats);
        std::thread::Builder::new()
            .name("cis-policy-watch".into())
            .spawn(move || {
                let mut last_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
                loop {
                    worker_hb_policy.tick("cis-policy-watch");
                    std::thread::sleep(Duration::from_secs(1));
                    let mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
                    if mtime == last_mtime {
                        continue;
                    }
                    last_mtime = mtime;
                    match rel.reload_now() {
                        PolicyReloadOutcome::Applied => {
                            let snap = rel.active().snapshot();
                            eprintln!(
                                "cisd: ranking policy applied (version={})",
                                rel.active().current_version_label()
                            );
                            coord_embed_policy_thresholds(&coord_policy, &snap);
                        }
                        PolicyReloadOutcome::RejectedInvalid(e) => {
                            eprintln!("cisd: policy reload rejected (keeping prior): {}", e)
                        }
                        PolicyReloadOutcome::ReadError(e) => {
                            eprintln!("cisd: policy read error: {}", e)
                        }
                    }
                }
            })
            .expect("spawn policy watch");
    }
}

fn coord_embed_policy_thresholds(coord: &cis_core::WriteCoordinator, policy: &cis_core::RankingPolicySnapshot) {
    coord.set_embedding_queue_thresholds(policy.embedding_queue_hwm, policy.embedding_queue_lwm);
}

struct PreparedDaemon {
    coord: Arc<cis_core::WriteCoordinator>,
    kv: Arc<MemoryKv>,
    handles: CisDaemonHandles,
    dlq: Arc<VectorCleanupQueue>,
    version_label: String,
}

/// Open WAL/graph, recover, spawn daemon workers. Heavy — do not block MCP handshake on this.
fn prepare_daemon(
    repo: &PathBuf,
    policy_snap: &cis_core::RankingPolicySnapshot,
    reloader: Option<Arc<PolicyFileReloader>>,
    mcp_mode: bool,
    version_label: String,
) -> Result<PreparedDaemon, String> {
    let coord = open_persisted_coordinator(repo).map_err(|e| e.to_string())?;
    if let Some(dir) = coord.persistence_dir() {
        eprintln!("cisd: persistence at {:?}", dir);
    }
    coord.set_embedding_queue_thresholds(
        policy_snap.embedding_queue_hwm,
        policy_snap.embedding_queue_lwm,
    );
    let kv = Arc::new(MemoryKv::new());
    let cis = cis_dir(repo);
    if cis.is_dir() {
        let kpath = kv_snapshot_path(&cis);
        if kpath.exists() {
            if let Err(e) = load_kv_snapshot(&kpath, kv.as_ref()) {
                eprintln!("cisd: kv snapshot load failed: {e}");
            }
        }
    }
    let saga = Arc::new(MergeSagaOrchestrator::new(Arc::clone(&kv)));
    let rep = coord.reconcile_on_startup(saga.as_ref());
    eprintln!("cisd: startup recovery {:?}", rep);
    if rep.wal_replay_failed {
        return Err("WAL replay failed — refusing to start".into());
    }

    let handles = CisDaemonHandles::open(repo, policy_snap);
    handles.audit.resume_from_kv(&kv);
    let branches: Vec<cis_wal::BranchId> = kv
        .scan_prefix("branch_reg:")
        .into_iter()
        .filter_map(|(_k, v)| {
            if v.len() == 16 {
                let mut b = [0u8; 16];
                b.copy_from_slice(&v);
                Some(cis_wal::BranchId(b))
            } else {
                None
            }
        })
        .collect();
    let body_store_startup = BodyStore::new(Arc::clone(&kv));
    {
        let merge_control_startup = cis_core::MergeControl::new(Arc::clone(&kv));
        let gate = cis_core::MergeRecoveryGate::new(Arc::clone(&kv));
        let mut g = coord.graph().write();
        let merge_rep = cis_core::recover_inflight_merges(
            &mut *g,
            kv.as_ref(),
            &body_store_startup,
            saga.as_ref(),
            &merge_control_startup,
            coord.vector_chunk_store(),
            &gate,
            None,
        );
        drop(g);
        let compensated = saga.compensate_orphans();
        eprintln!(
            "cisd: merge recovery resumed={} compensated_by_recover={} orphans_purged={}",
            merge_rep.resumed, merge_rep.compensated, compensated
        );
    }
    let run_consistency = std::env::var_os("CIS_STARTUP_CONSISTENCY").is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true")
    }) || (!mcp_mode
        && !std::env::var_os("CIS_STARTUP_CONSISTENCY").is_some_and(|v| {
            v == "0" || v.eq_ignore_ascii_case("false")
        }));
    if run_consistency {
        let consistency =
            cis_core::check_consistency(coord.graph(), &kv, &body_store_startup, &branches);
        let startup_now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        handles.last_consistency.update(&consistency, startup_now_ms);
        if !consistency.is_clean() {
            handles
                .audit
                .record_sync(0, format!("startup_consistency {}", consistency.summary()));
            eprintln!("cisd: startup consistency check: {}", consistency.summary());
        }
    } else {
        eprintln!(
            "cisd: skipping startup consistency check (set CIS_STARTUP_CONSISTENCY=1 to enable)"
        );
    }
    let merge_control = Arc::new(cis_core::MergeControl::new(Arc::clone(&kv)));
    let body_store = Arc::new(BodyStore::new(Arc::clone(&kv)));
    let dlq = vector_dlq();
    spawn_background_threads(
        Arc::clone(&coord),
        Arc::clone(&kv),
        saga,
        reloader,
        policy_snap.merge_ttl_hours,
        Arc::clone(&dlq),
        handles.clone(),
        repo.clone(),
        merge_control,
        body_store,
    );
    Ok(PreparedDaemon {
        coord,
        kv,
        handles,
        dlq,
        version_label,
    })
}

fn boot_mcp_runtime(
    repo: PathBuf,
    policy_snap: cis_core::RankingPolicySnapshot,
    reloader: Option<Arc<PolicyFileReloader>>,
    version_label: String,
) -> Result<Arc<CisMcpRuntime>, String> {
    let prepared = prepare_daemon(&repo, &policy_snap, reloader, true, version_label.clone())?;
    eprintln!(
        "cisd: MCP workspace ready for tools (policy_version={} dlq_depth={})",
        prepared.version_label,
        prepared.dlq.depth()
    );
    let rt = CisMcpRuntime::attach_coordinator(
        &repo,
        prepared.coord,
        prepared.kv,
        Some(prepared.handles),
    );
    let rt = build_runtime(Some(rt)).map_err(|e| e.to_string())?;
    if defer_vector_snapshot_load() {
        let coord_bg = Arc::clone(rt.coordinator());
        let cis_bg = cis_dir(&repo);
        std::thread::spawn(move || {
            let t = std::time::Instant::now();
            let rep = load_vector_into(&cis_bg, coord_bg.vector());
            eprintln!(
                "cisd: background vector load done in {:.1}s (loaded={} chunks={})",
                t.elapsed().as_secs_f64(),
                rep.vector_loaded,
                rep.vector_chunks
            );
        });
    }
    Ok(rt)
}

fn main() {
    load_env_file(None);
    let mcp_mode = std::env::args().any(|a| a == "--mcp" || a == "-mcp");

    let path = policy_path();
    let (_policy, reloader): (ActiveRankingPolicy, Option<Arc<PolicyFileReloader>>) =
        if path.exists() {
            match PolicyFileReloader::from_file(&path) {
                Ok(r) => {
                    let r = Arc::new(r);
                    (r.active(), Some(r))
                }
                Err(e) => {
                    eprintln!("cisd: cannot load policy {:?}: {:?}", path, e);
                    std::process::exit(1);
                }
            }
        } else {
            eprintln!(
                "cisd: no policy file at {:?}; using built-in default (override with CIS_POLICY_PATH)",
                path
            );
            (ActiveRankingPolicy::with_system_default(), None)
        };

    let version_label = _policy.current_version_label();
    let policy_snap = _policy.snapshot();

    let repo = std::env::var_os("CIS_REPO_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    // MCP clients (Cursor) time out if initialize/tools/list wait on full warm-start.
    // Defer vector.json by default; structural tools work without embeddings loaded.
    if mcp_mode && std::env::var_os("CIS_DEFER_VECTOR_LOAD").is_none() {
        std::env::set_var("CIS_DEFER_VECTOR_LOAD", "1");
    }

    // Cursor MCP IPC metadata timeout defaults to 10s — answer handshake before WAL/graph boot.
    if mcp_mode {
        let slot = McpRuntimeSlot::new();
        let slot_bg = Arc::clone(&slot);
        let repo_bg = repo.clone();
        let policy_bg = policy_snap.clone();
        let reloader_bg = reloader.clone();
        let version_bg = version_label.clone();
        std::thread::Builder::new()
            .name("cis-mcp-boot".into())
            .spawn(move || match boot_mcp_runtime(repo_bg, policy_bg, reloader_bg, version_bg)
            {
                Ok(rt) => {
                    eprintln!("cisd: MCP boot complete — tools/call unlocked");
                    slot_bg.set_ready(rt);
                }
                Err(e) => {
                    eprintln!("cisd: MCP boot failed: {e}");
                    slot_bg.set_failed(e);
                }
            })
            .expect("spawn cis-mcp-boot");
        eprintln!(
            "cisd: MCP stdio accepting initialize/tools/list (workspace loading; policy={})",
            version_label
        );
        if let Err(e) = run_stdio_slot(slot) {
            eprintln!("cisd: MCP stdio exited: {}", e);
            std::process::exit(1);
        }
        return;
    }

    let prepared = match prepare_daemon(&repo, &policy_snap, reloader, false, version_label.clone())
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("cisd: {e}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "cisd: ready policy_version={} vector_dlq_depth={} (Ctrl+C to exit; use --mcp for stdio MCP)",
        prepared.version_label,
        prepared.dlq.depth()
    );
    let _keep = (prepared.coord, prepared.kv, prepared.handles, prepared.dlq);
    std::thread::park();
}
