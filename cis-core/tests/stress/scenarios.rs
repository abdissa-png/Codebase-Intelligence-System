//! Stress scenarios (**Phase 4.2**).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cis_core::{
    apply_index_events_with_config, merge_lock_holder, release_merge_lock, CisMcpRuntime,
    CrashAfterNCalls, IndexEvent, IndexEventQueue, InvariantCheckMode, MergePreflight,
    MergeRecoveryGate, MergeSagaOrchestrator, OptimisticPatcher, PathLeaseManager,
    ProductionAuditSink, SessionId, SpeculativePathTracker, TombstoneGcWorker,
    WalCompactionScheduler,
};
use cis_wal::{BranchId, MergeId};

use crate::harness::{identity_cas_delay_injector, path_pool, seed_file, StressConfig, StressHarness};
use crate::replay;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn merge_preflight_one_wins_violation(
    h: &StressHarness,
    rel: &str,
    branch: BranchId,
    merge_id: MergeId,
) -> Option<String> {
    let lock_held = merge_lock_holder(h.rt.kv(), branch) == Some(merge_id);
    let spec_conflicts = h.rt.spec_paths().conflicting_paths(&[rel.to_string()]);
    let patch_refs = h
        .rt
        .patcher()
        .path_refcounts()
        .get(rel)
        .copied()
        .unwrap_or(0);
    let speculative_active = !spec_conflicts.is_empty() || patch_refs > 0;

    if lock_held && speculative_active {
        return Some(format!(
            "torn state: merge lock coexists with speculative patch on {rel} (lock={lock_held}, spec={spec_conflicts:?}, patch_refs={patch_refs})"
        ));
    }

    let preflight_won = lock_held && !speculative_active;
    let speculative_won = !lock_held && speculative_active;
    let both_lost_cleanly = !lock_held && !speculative_active;
    if !(preflight_won || speculative_won || both_lost_cleanly) {
        return Some(format!("race on {rel} left unresolved torn state"));
    }

    if speculative_active {
        if h.rt.leases().holder(rel).is_none() {
            return Some(format!("speculative winner must hold lease on {rel}"));
        }
        let tracker_count = h
            .rt
            .spec_paths()
            .path_counts()
            .into_iter()
            .find(|(p, _)| p == rel)
            .map(|(_, c)| c)
            .unwrap_or(0);
        if patch_refs != tracker_count {
            return Some(format!(
                "patch refcount must match speculative tracker on {rel}: patch={patch_refs} tracker={tracker_count}"
            ));
        }
    }
    None
}

fn cleanup_merge_preflight_race(h: &StressHarness, rel: &str, branch: BranchId, merge_id: MergeId) {
    for (patch_id, path) in h.rt.patcher().open_patch_paths() {
        if path == rel {
            let _ = h.rt.patcher().revert(patch_id, SessionId(1));
        }
    }
    if merge_lock_holder(h.rt.kv(), branch) == Some(merge_id) {
        let _ = release_merge_lock(h.rt.kv(), branch, merge_id);
    }
}

pub fn scenario_write_confirm_revert(h: &StressHarness, cfg: &StressConfig) {
    let paths = path_pool(cfg.n_threads.max(4));
    for (i, p) in paths.iter().enumerate() {
        seed_file(
            h.repo(),
            p,
            &format!("def fn_{i}():\n    return {i}\n"),
        );
    }
    let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
    let _ = h.rt.reindex_python_paths(&path_refs);

    h.run_barrier(cfg.n_threads, {
        let paths = paths.clone();
        let repo = h.repo().to_path_buf();
        let ops = cfg.ops_per_thread;
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            let path = &paths[thread % paths.len()];
            for op in 0..ops {
                let label = if thread % 2 == 0 {
                    let body = format!("def fn_{thread}_{op}():\n    return {op}\n");
                    let before = std::fs::read_to_string(repo.join(path)).ok();
                    match hh.rt.write_file(thread as u64, path, &body, true) {
                        Ok(resp) => {
                            if op % 3 == 0 {
                                let _ = hh.rt.confirm_patch(thread as u64, resp.patch_id);
                                format!("write+confirm:{path}")
                            } else {
                                let _ = hh.rt.revert_patch(thread as u64, resp.patch_id);
                                if let Some(ref snapshot) = before {
                                    let after = std::fs::read_to_string(repo.join(path))
                                        .unwrap_or_default();
                                    assert_eq!(
                                        after, *snapshot,
                                        "revert should restore pre-write file content for {path}"
                                    );
                                }
                                format!("write+revert:{path}")
                            }
                        }
                        Err(e) => format!("write_err:{e:?}"),
                    }
                } else {
                    let body = format!("def alt_{thread}_{op}():\n    pass\n");
                    let before = std::fs::read_to_string(repo.join(path)).ok();
                    match hh.rt.write_file(thread as u64, path, &body, true) {
                        Ok(resp) => {
                            let _ = hh.rt.revert_patch(thread as u64, resp.patch_id);
                            if let Some(ref snapshot) = before {
                                let after = std::fs::read_to_string(repo.join(path))
                                    .unwrap_or_default();
                                assert_eq!(
                                    after, *snapshot,
                                    "revert should restore pre-write file content for {path}"
                                );
                            }
                            format!("write+revert:{path}")
                        }
                        Err(e) => format!("write_err:{e:?}"),
                    }
                };
                hh.log(thread, op, label);
                hh.quiesce_then_assert_after_op(quiesce.as_ref(), sync_done.as_ref(), thread);
            }
        }
    });

    assert!(
        h.rt.leases().is_empty(),
        "leases should be empty after stress"
    );
    assert_eq!(
        h.rt.patcher().open_patch_count(),
        0,
        "no open patches after stress"
    );
    h.assert_invariants_shrink(|prefix| replay::write_confirm_revert(prefix, cfg));
}

pub fn scenario_identity_cas_converges(_h: &StressHarness, cfg: &StressConfig) {
    let seed = cfg.n_threads as u64 * 17 + cfg.ops_per_thread as u64;
    let h2 = StressHarness::with_injector(identity_cas_delay_injector(seed));
    let coord = Arc::clone(h2.rt.coordinator());
    let branch = h2.rt.active_branch();
    let path = "shared.py";
    let src = "def shared():\n    return 42\n";
    let n = cfg.n_threads.max(4);
    let src = Arc::new(src.to_string());

    h2.run_barrier(n, {
        let src = Arc::clone(&src);
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            let q = IndexEventQueue::new();
            let events = vec![IndexEvent {
                branch_id: branch,
                path: path.into(),
                kind: cis_core::FsChangeKind::Modified,
                old_path: None,
            }];
                let body = Arc::clone(&src);
            let _ = apply_index_events_with_config(
                &q,
                hh.rt.coordinator().as_ref(),
                Arc::clone(hh.rt.kv()),
                events,
                move |_| Ok((*body).clone()),
                None,
                None,
                None,
            );
            hh.log(thread, 0, "ingest");
            hh.quiesce_sync_then_check_after_op(
                quiesce.as_ref(),
                sync_done.as_ref(),
                thread,
                InvariantCheckMode::Strict,
            );
        }
    });

    let active_count = {
        let g = coord.graph().read();
        g.revisions()
            .filter(|r| r.file_path == path && r.qualified_name.ends_with("::shared"))
            .filter(|r| matches!(r.status, cis_core::RevisionStatus::Active))
            .map(|r| r.identity_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
    };
    assert_eq!(
        active_count,
        1,
        "concurrent ingest should converge to one function identity (got {})",
        active_count
    );
    h2.rt.sync_revision_index_from_graph();
    h2.assert_invariants_shrink(|prefix| replay::identity_cas(prefix, cfg));
}

pub fn scenario_merge_preflight_vs_speculative(h: &StressHarness, _cfg: &StressConfig) {
    let rel = "race.py";
    seed_file(h.repo(), rel, "def race():\n    return 0\n");
    h.rt.reindex_python_paths(&[rel]).expect("ingest");

    let kv = Arc::clone(h.rt.kv());
    let spec = Arc::clone(h.rt.spec_paths());
    let branch = h.rt.active_branch();
    let merge_id = MergeId([7u8; 16]);

    h.run_barrier(2, {
        let rel = rel.to_string();
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            if thread == 0 {
                let err = MergePreflight::begin_with_snapshot(
                    Arc::clone(&kv),
                    branch,
                    merge_id,
                    &[rel.clone()],
                    spec.as_ref(),
                    &[],
                );
                hh.log(thread, 0, format!("preflight:{err:?}"));
            } else {
                let r = hh.rt.patcher().apply_speculative(
                    SessionId(1),
                    vec![rel.clone()],
                    Some((hh.rt.kv().as_ref(), branch)),
                );
                hh.log(thread, 0, format!("speculative:{r:?}"));
            }
            quiesce.wait();
            if let Some(msg) = merge_preflight_one_wins_violation(&hh, &rel, branch, merge_id) {
                hh.record_failure(format!("t{thread} merge race: {msg}"));
            }
            hh.check_invariants_after_op(thread, InvariantCheckMode::MergePreflightWindow);
            sync_done.wait();
        }
    });
    cleanup_merge_preflight_race(h, rel, branch, merge_id);
    h.assert_invariants_shrink(replay::merge_preflight);
}

pub fn scenario_ttl_sweep_vs_confirm(h: &StressHarness, _cfg: &StressConfig) {
    let rel = "ttl.py";
    seed_file(h.repo(), rel, "def ttl():\n    return 0\n");

    h.run_barrier(2, {
        let rel = rel.to_string();
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            if thread == 0 {
                let n = hh.rt.sweep_speculative_orphans(0);
                hh.log(thread, 0, format!("sweep:{n}"));
            } else if let Ok(resp) = hh.rt.write_file(1, &rel, "def ttl():\n    return 1\n", false)
            {
                let _ = hh.rt.confirm_patch(1, resp.patch_id);
                hh.log(thread, 0, "confirm");
            }
            hh.quiesce_then_assert_after_op(quiesce.as_ref(), sync_done.as_ref(), thread);
        }
    });
    h.assert_invariants_shrink(replay::ttl_sweep);
}

pub fn scenario_wal_compaction_during_merge(h: &StressHarness, _cfg: &StressConfig) {
    let rel = "merge_me.py";
    seed_file(h.repo(), rel, "def merge_me():\n    return 0\n");
    h.rt.reindex_python_paths(&[rel]).expect("ingest");
    let created = h.rt.create_branch(0, "stress_feat", Some("main")).unwrap();
    h.rt.switch_branch(0, "stress_feat").unwrap();
    let write_resp = h.rt
        .write_file(0, rel, "def merge_me():\n    return 99\n", true)
        .unwrap();
    h.rt
        .confirm_patch(0, write_resp.patch_id)
        .expect("confirm feature write before merge");
    h.rt.switch_branch(0, "main").unwrap();

    let scheduler = WalCompactionScheduler::new(Duration::from_millis(1), 0);
    let saga = MergeSagaOrchestrator::new(Arc::clone(h.rt.kv()));
    let gate = MergeRecoveryGate::new(Arc::clone(h.rt.kv()));
    let audit = ProductionAuditSink::new(h.repo().join(".cis/audit.jsonl"));

    h.run_barrier(2, {
        let feature = created.branch_id_hex.clone();
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            if thread == 0 {
                let _ = scheduler.run_once(
                    hh.rt.wal().as_ref(),
                    1024 * 1024,
                    hh.rt.kv().as_ref(),
                    &saga,
                    &gate,
                    &audit,
                );
                hh.log(thread, 0, "wal_compact");
            } else {
                let listed = hh.rt.list_branches(0).unwrap();
                let main_hex = listed
                    .branches
                    .iter()
                    .find(|x| x.name == "main")
                    .map(|x| x.branch_id_hex.clone())
                    .unwrap_or_else(|| "00000000000000000000000000000000".into());
                let r = hh.rt.merge_branch(0, &feature, &main_hex, None, None, None);
                hh.log(thread, 0, format!("merge:{r:?}"));
            }
            hh.quiesce_sync_then_check_after_op(
                quiesce.as_ref(),
                sync_done.as_ref(),
                thread,
                InvariantCheckMode::Strict,
            );
        }
    });
    let branch = h.rt.active_branch();
    if let Some(holder) = merge_lock_holder(h.rt.kv(), branch) {
        let _ = release_merge_lock(h.rt.kv(), branch, holder);
    }
    h.rt.sync_revision_index_from_graph();
    h.assert_invariants_shrink(replay::wal_compaction_during_merge);
}

pub fn scenario_tombstone_gc_during_rename(h: &StressHarness, _cfg: &StressConfig) {
    let kv = Arc::clone(h.rt.kv());
    let coord = Arc::clone(h.rt.coordinator());
    let branch = h.rt.active_branch();
    let path = "rename.py";
    let policy = h.rt.policy_snapshot();

    apply_index_events_with_config(
        &IndexEventQueue::new(),
        coord.as_ref(),
        Arc::clone(&kv),
        vec![IndexEvent {
            branch_id: branch,
            path: path.into(),
            kind: cis_core::FsChangeKind::Modified,
            old_path: None,
        }],
        |_| Ok("def foo():\n    return 1\n".to_string()),
        None,
        None,
        None,
    )
    .unwrap();

    let worker = TombstoneGcWorker::from_policy(&policy);

    h.run_barrier(2, move |hh, thread, barrier, quiesce, sync_done| {
        barrier.wait();
        let policy = hh.rt.policy_snapshot();
        if thread == 0 {
            let eligible = worker.scan_eligible(
                hh.rt.coordinator().graph(),
                hh.rt.kv().as_ref(),
                hh.rt.coordinator(),
                &policy,
                now_ms(),
            );
            hh.log(thread, 0, format!("gc_scan:{}", eligible.len()));
        } else {
            let _ = apply_index_events_with_config(
                &IndexEventQueue::new(),
                hh.rt.coordinator().as_ref(),
                Arc::clone(hh.rt.kv()),
                vec![IndexEvent {
                    branch_id: branch,
                    path: path.into(),
                    kind: cis_core::FsChangeKind::Modified,
                    old_path: None,
                }],
        |_| Ok("def bar():\n    return 1\n".to_string()),
                None,
                None,
                None,
            );
            hh.log(thread, 0, "rename_ingest");
        }
        hh.quiesce_sync_then_check_after_op(
            quiesce.as_ref(),
            sync_done.as_ref(),
            thread,
            InvariantCheckMode::Strict,
        );
    });
    h.rt.sync_revision_index_from_graph();
    h.assert_invariants_shrink(replay::tombstone_gc_during_rename);
}

pub fn scenario_branch_switch_and_write(h: &StressHarness, cfg: &StressConfig) {
    let _ = h.rt.create_branch(0, "sw_a", Some("main"));
    let _ = h.rt.create_branch(0, "sw_b", Some("main"));

    h.run_barrier(cfg.n_threads, {
        let ops = cfg.ops_per_thread;
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            for op in 0..ops {
                let branch = if op % 2 == 0 { "sw_a" } else { "sw_b" };
                let _ = hh.rt.switch_branch(thread as u64, branch);
                let rel = format!("sw/{thread}_{op}.py");
                let body = format!("def f():\n    return {op}\n");
                if let Ok(resp) = hh.rt.write_file(thread as u64, &rel, &body, false) {
                    let _ = hh.rt.confirm_patch(thread as u64, resp.patch_id);
                }
                hh.log(thread, op, format!("switch_write:{branch}"));
                hh.quiesce_then_assert_after_op(quiesce.as_ref(), sync_done.as_ref(), thread);
            }
        }
    });
    h.assert_invariants_shrink(|prefix| replay::branch_switch(prefix, cfg));
}

pub fn scenario_lease_storm(h: &StressHarness, cfg: &StressConfig) {
    let leases = Arc::new(PathLeaseManager::new());
    let spec = Arc::new(SpeculativePathTracker::new());
    let patcher = Arc::new(OptimisticPatcher::new(
        Arc::clone(&leases),
        Arc::clone(&spec),
    ));

    h.run_barrier(cfg.n_threads, {
        let patcher = Arc::clone(&patcher);
        let leases = Arc::clone(&leases);
        let spec = Arc::clone(&spec);
        let ops = cfg.ops_per_thread;
        move |hh, thread, barrier, quiesce, sync_done| {
            barrier.wait();
            let p = format!("lease/{thread}.py");
            for op in 0..ops {
                if let Ok(id) = patcher.apply_speculative(SessionId(thread as u64), vec![p.clone()], None) {
                    let _ = patcher.revert(id, SessionId(thread as u64));
                    hh.log(thread, op, format!("lease_cycle:{id}"));
                } else {
                    hh.log(thread, op, "lease_err");
                }
                hh.check_lease_storm_after_op(
                    quiesce.as_ref(),
                    sync_done.as_ref(),
                    thread,
                    leases.as_ref(),
                    patcher.as_ref(),
                    spec.as_ref(),
                );
            }
        }
    });
    for t in 0..cfg.n_threads {
        patcher.revert_session(cis_core::SessionId(t as u64));
    }
    StressHarness::assert_lease_storm_invariants(
        leases.as_ref(),
        patcher.as_ref(),
        spec.as_ref(),
    );
    assert!(leases.is_empty(), "all leases released after storm");
    h.assert_invariants_shrink(|prefix| replay::lease_storm(prefix, cfg));
}

pub fn scenario_crash_mid_promote_recovery(h: &StressHarness, _cfg: &StressConfig) {
    let rel = "crash.py";
    seed_file(h.repo(), rel, "def crash():\n    return 0\n");
    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");

    let repo = h.repo().to_path_buf();
    let injector = Arc::new(CrashAfterNCalls::new(3));
    {
        let rt = CisMcpRuntime::new_dev_with_fault_injector(&repo.to_string_lossy(), injector);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let resp = rt.write_file(0, rel, "def crash():\n    return 1\n", true).unwrap();
            let _ = rt.confirm_patch(0, resp.patch_id);
        }));
    }

    std::env::set_var("CIS_WAL_MEMORY", "1");
    let rt2 = CisMcpRuntime::new_dev(&repo.to_string_lossy());
    let branch = rt2.active_branch();
    let branches = [branch];
    cis_core::assert_invariants(&rt2.invariant_context(&branches));
}

pub fn scenario_background_worker_chaos(h: &StressHarness, _cfg: &StressConfig) {
    let rel = "chaos.py";
    seed_file(h.repo(), rel, "def chaos():\n    return 0\n");
    h.rt.reindex_python_paths(&[rel]).expect("ingest");

    let scheduler = WalCompactionScheduler::default();
    let saga = MergeSagaOrchestrator::new(Arc::clone(h.rt.kv()));
    let gate = MergeRecoveryGate::new(Arc::clone(h.rt.kv()));
    let audit = ProductionAuditSink::with_options(
        h.repo().join(".cis/audit_chaos.jsonl"),
        Duration::from_secs(0),
        1,
    );
    let policy = h.rt.policy_snapshot();
    let worker = TombstoneGcWorker::from_policy(&policy);

    h.run_barrier(3, move |hh, thread, barrier, quiesce, sync_done| {
        barrier.wait();
        match thread {
            0 => {
                let _ = scheduler.run_once(
                    hh.rt.wal().as_ref(),
                    1024,
                    hh.rt.kv().as_ref(),
                    &saga,
                    &gate,
                    &audit,
                );
                hh.log(thread, 0, "compact");
            }
            1 => {
                let policy = hh.rt.policy_snapshot();
                let _ = worker.scan_eligible(
                    hh.rt.coordinator().graph(),
                    hh.rt.kv().as_ref(),
                    hh.rt.coordinator(),
                    &policy,
                    now_ms(),
                );
                let _ = audit.maybe_seal_epoch(hh.rt.kv().as_ref());
                hh.log(thread, 0, "gc+audit");
            }
            _ => {
                let _ = hh.rt.coordinator().reconcile_on_startup(&saga);
                hh.log(thread, 0, "reconcile");
            }
        }
        hh.quiesce_sync_then_check_after_op(
            quiesce.as_ref(),
            sync_done.as_ref(),
            thread,
            InvariantCheckMode::Strict,
        );
    });
    h.assert_invariants_shrink(replay::background_worker_chaos);
}

pub fn run_all(_h: &StressHarness, cfg: &StressConfig) {
    scenario_write_confirm_revert(&StressHarness::new(), cfg);
    scenario_identity_cas_converges(&StressHarness::new(), cfg);
    scenario_merge_preflight_vs_speculative(&StressHarness::new(), cfg);
    scenario_ttl_sweep_vs_confirm(&StressHarness::new(), cfg);
    scenario_wal_compaction_during_merge(&StressHarness::new(), cfg);
    scenario_tombstone_gc_during_rename(&StressHarness::new(), cfg);
    scenario_branch_switch_and_write(&StressHarness::new(), cfg);
    scenario_lease_storm(&StressHarness::new(), cfg);
    scenario_crash_mid_promote_recovery(&StressHarness::new(), cfg);
    scenario_background_worker_chaos(&StressHarness::new(), cfg);
}
