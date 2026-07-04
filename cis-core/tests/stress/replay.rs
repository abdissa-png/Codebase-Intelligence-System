//! Shrink reproducers: replay an op-log prefix and return whether invariants fail.

use std::sync::Arc;
use std::time::Duration;

use cis_core::{
    apply_index_events_with_config, IndexEvent, IndexEventQueue, MergePreflight,
    MergeRecoveryGate, MergeSagaOrchestrator, OptimisticPatcher, PathLeaseManager,
    ProductionAuditSink, SessionId, SpeculativePathTracker, TombstoneGcWorker,
    WalCompactionScheduler,
};
use cis_wal::MergeId;

use crate::harness::{
    identity_cas_delay_injector, path_pool, seed_file, OpRecord, StressConfig, StressHarness,
};

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn write_confirm_revert(prefix: &[OpRecord], cfg: &StressConfig) -> bool {
    let h = StressHarness::new();
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

    for op in prefix {
        if op.label.starts_with("write_err:") {
            continue;
        }
        let path = &paths[op.thread % paths.len()];
        let body = if op.label.contains("write+revert:") && op.thread % 2 != 0 {
            format!("def alt_{}_{}():\n    pass\n", op.thread, op.op_index)
        } else {
            format!("def fn_{}_{}():\n    return {}\n", op.thread, op.op_index, op.op_index)
        };
        if op.label.starts_with("write+confirm:") {
            if let Ok(resp) = h.rt.write_file(op.thread as u64, path, &body, true) {
                let _ = h.rt.confirm_patch(op.thread as u64, resp.patch_id);
            }
        } else if op.label.starts_with("write+revert:") {
            if let Ok(resp) = h.rt.write_file(op.thread as u64, path, &body, true) {
                let _ = h.rt.revert_patch(op.thread as u64, resp.patch_id);
            }
        }
    }
    h.invariants_dirty()
}

pub fn identity_cas(prefix: &[OpRecord], cfg: &StressConfig) -> bool {
    let seed = cfg.n_threads as u64 * 17 + cfg.ops_per_thread as u64;
    let h = StressHarness::with_injector(identity_cas_delay_injector(seed));
    let branch = h.rt.active_branch();
    let path = "shared.py";
    let src = "def shared():\n    return 42\n";

    for op in prefix {
        if op.label != "ingest" {
            continue;
        }
        let q = IndexEventQueue::new();
        let events = vec![IndexEvent {
            branch_id: branch,
            path: path.into(),
            kind: cis_core::FsChangeKind::Modified,
        }];
        let _ = apply_index_events_with_config(
            &q,
            h.rt.coordinator().as_ref(),
            Arc::clone(h.rt.kv()),
            events,
            |_| Ok(src.to_string()),
            None,
            None,
            None,
        );
    }
    h.rt.sync_revision_index_from_graph();
    h.invariants_dirty()
}

pub fn merge_preflight(prefix: &[OpRecord]) -> bool {
    let h = StressHarness::new();
    let rel = "race.py";
    seed_file(h.repo(), rel, "def race():\n    return 0\n");
    h.rt.reindex_python_paths(&[rel]).expect("ingest");
    let kv = Arc::clone(h.rt.kv());
    let branch = h.rt.active_branch();
    let merge_id = MergeId([7u8; 16]);

    for op in prefix {
        if op.label.starts_with("preflight:") {
            let _ = MergePreflight::begin_with_snapshot(
                Arc::clone(&kv),
                branch,
                merge_id,
                &[rel.to_string()],
                h.rt.spec_paths().as_ref(),
                &[],
            );
        } else if op.label.starts_with("speculative:") {
            let _ = h
                .rt
                .patcher()
                .apply_speculative(SessionId(1), vec![rel.to_string()]);
        }
    }
    h.invariants_dirty()
}

pub fn ttl_sweep(prefix: &[OpRecord]) -> bool {
    let h = StressHarness::new();
    let rel = "ttl.py";
    seed_file(h.repo(), rel, "def ttl():\n    return 0\n");

    for op in prefix {
        if op.label.starts_with("sweep:") {
            let _ = h.rt.sweep_speculative_orphans(0);
        } else if op.label == "confirm" {
            if let Ok(resp) = h.rt.write_file(1, rel, "def ttl():\n    return 1\n", false) {
                let _ = h.rt.confirm_patch(1, resp.patch_id);
            }
        }
    }
    h.invariants_dirty()
}

pub fn wal_compaction_during_merge(prefix: &[OpRecord]) -> bool {
    let h = StressHarness::new();
    let rel = "merge_me.py";
    seed_file(h.repo(), rel, "def merge_me():\n    return 0\n");
    h.rt.reindex_python_paths(&[rel]).expect("ingest");
    let created = h.rt.create_branch(0, "stress_feat", Some("main")).unwrap();
    h.rt.switch_branch(0, "stress_feat").unwrap();
    let write_resp = h
        .rt
        .write_file(0, rel, "def merge_me():\n    return 99\n", true)
        .unwrap();
    h.rt
        .confirm_patch(0, write_resp.patch_id)
        .expect("confirm feature write");
    h.rt.switch_branch(0, "main").unwrap();

    let scheduler = WalCompactionScheduler::new(Duration::from_millis(1), 0);
    let saga = MergeSagaOrchestrator::new(Arc::clone(h.rt.kv()));
    let gate = MergeRecoveryGate::new(Arc::clone(h.rt.kv()));
    let audit = ProductionAuditSink::new(h.repo().join(".cis/audit.jsonl"));
    let feature = created.branch_id_hex;

    for op in prefix {
        if op.label == "wal_compact" {
            let _ = scheduler.run_once(
                h.rt.wal().as_ref(),
                1024 * 1024,
                h.rt.kv().as_ref(),
                &saga,
                &gate,
                &audit,
            );
        } else if op.label.starts_with("merge:") {
            let listed = h.rt.list_branches(0).unwrap();
            let main_hex = listed
                .branches
                .iter()
                .find(|x| x.name == "main")
                .map(|x| x.branch_id_hex.clone())
                .unwrap_or_else(|| "00000000000000000000000000000000".into());
            let _ = h.rt.merge_branch(0, &feature, &main_hex, None, None, None);
        }
    }
    h.rt.sync_revision_index_from_graph();
    h.invariants_dirty()
}

pub fn tombstone_gc_during_rename(prefix: &[OpRecord]) -> bool {
    let h = StressHarness::new();
    let branch = h.rt.active_branch();
    let path = "rename.py";
    let policy = h.rt.policy_snapshot();
    let worker = TombstoneGcWorker::from_policy(&policy);

    apply_index_events_with_config(
        &IndexEventQueue::new(),
        h.rt.coordinator().as_ref(),
        Arc::clone(h.rt.kv()),
        vec![IndexEvent {
            branch_id: branch,
            path: path.into(),
            kind: cis_core::FsChangeKind::Modified,
        }],
        |_| Ok("def foo():\n    return 1\n".to_string()),
        None,
        None,
        None,
    )
    .unwrap();

    for op in prefix {
        if op.label.starts_with("gc_scan:") {
            let policy = h.rt.policy_snapshot();
            let _ = worker.scan_eligible(
                h.rt.coordinator().graph(),
                h.rt.kv().as_ref(),
                h.rt.coordinator(),
                &policy,
                now_ms(),
            );
        } else if op.label == "rename_ingest" {
            let _ = apply_index_events_with_config(
                &IndexEventQueue::new(),
                h.rt.coordinator().as_ref(),
                Arc::clone(h.rt.kv()),
                vec![IndexEvent {
                    branch_id: branch,
                    path: path.into(),
                    kind: cis_core::FsChangeKind::Modified,
                }],
                |_| Ok("def bar():\n    return 1\n".to_string()),
                None,
                None,
                None,
            );
        }
    }
    h.rt.sync_revision_index_from_graph();
    h.invariants_dirty()
}

pub fn branch_switch(prefix: &[OpRecord], _cfg: &StressConfig) -> bool {
    let h = StressHarness::new();
    let _ = h.rt.create_branch(0, "sw_a", Some("main"));
    let _ = h.rt.create_branch(0, "sw_b", Some("main"));

    for op in prefix {
        let branch = op
            .label
            .strip_prefix("switch_write:")
            .unwrap_or("sw_a");
        let _ = h.rt.switch_branch(op.thread as u64, branch);
        let rel = format!("sw/{}_{}.py", op.thread, op.op_index);
        let body = format!("def f():\n    return {}\n", op.op_index);
        if let Ok(resp) = h.rt.write_file(op.thread as u64, &rel, &body, false) {
            let _ = h.rt.confirm_patch(op.thread as u64, resp.patch_id);
        }
    }
    h.invariants_dirty()
}

pub fn lease_storm(prefix: &[OpRecord], cfg: &StressConfig) -> bool {
    let h = StressHarness::new();
    let leases = Arc::new(PathLeaseManager::new());
    let spec = Arc::new(SpeculativePathTracker::new());
    let patcher = Arc::new(OptimisticPatcher::new(
        Arc::clone(&leases),
        Arc::clone(&spec),
    ));

    for op in prefix {
        let p = format!("lease/{}.py", op.thread);
        if let Ok(id) = patcher.apply_speculative(SessionId(op.thread as u64), vec![p]) {
            let _ = patcher.revert(id, SessionId(op.thread as u64));
        }
    }
    for t in 0..cfg.n_threads {
        patcher.revert_session(SessionId(t as u64));
    }
    StressHarness::lease_storm_invariants_dirty(leases.as_ref(), patcher.as_ref(), spec.as_ref())
        || h.invariants_dirty()
}

pub fn background_worker_chaos(prefix: &[OpRecord]) -> bool {
    let h = StressHarness::new();
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

    for op in prefix {
        match op.label.as_str() {
            "compact" => {
                let _ = scheduler.run_once(
                    h.rt.wal().as_ref(),
                    1024,
                    h.rt.kv().as_ref(),
                    &saga,
                    &gate,
                    &audit,
                );
            }
            "gc+audit" => {
                let policy = h.rt.policy_snapshot();
                let _ = worker.scan_eligible(
                    h.rt.coordinator().graph(),
                    h.rt.kv().as_ref(),
                    h.rt.coordinator(),
                    &policy,
                    now_ms(),
                );
                let _ = audit.maybe_seal_epoch(h.rt.kv().as_ref());
            }
            "reconcile" => {
                let _ = h.rt.coordinator().reconcile_on_startup(&saga);
            }
            _ => {}
        }
    }
    h.invariants_dirty()
}
