//! **Phase 2** — write path + FS poll sync.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cis_core::{
    CisMcpRuntime, FsSyncConfig, IndexDebouncer, WatcherMetrics, DEFAULT_DEBOUNCE_MS,
    DEFAULT_POLL_MS,
};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-fs2-{}-{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn write_file_then_find_symbol_sees_new_definition() {
    let root = temp_repo("write-find");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());

    let py = "agent.py";
    let content = "def brand_new_symbol():\n    return 42\n";
    rt.write_file(0, py, content, true).expect("write_file");

    let hits = rt
        .find_symbol(0, "brand_new_symbol", None, 32, false)
        .expect("find_symbol");
    assert!(
        hits.matches.iter().any(|h| h.qualified_name.contains("brand_new_symbol")),
        "expected symbol from write_file reindex, got {:?}",
        hits.matches
    );
}

#[test]
fn external_edit_detected_by_poll_watcher() {
    let root = temp_repo("poll");
    let rt = Arc::new(CisMcpRuntime::new_dev(&root.to_string_lossy()));
    let cfg = FsSyncConfig {
        debounce: Duration::from_millis(100),
        poll_interval: Duration::from_millis(80),
    };

    let py_path = root.join("external.py");
    fs::write(&py_path, "# placeholder\n").unwrap();

    let rt_bg = Arc::clone(&rt);
    let handle = thread::spawn(move || cis_core::run_fs_sync_loop(rt_bg, cfg, true));
    thread::sleep(Duration::from_millis(200));
    fs::write(&py_path, "def from_disk():\n    pass\n").unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if rt
            .find_symbol(0, "from_disk", None, 32, false)
            .map(|r| !r.matches.is_empty())
            .unwrap_or(false)
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("poll watcher did not index external.py within 5s");
        }
        thread::sleep(Duration::from_millis(100));
    }
    drop(handle);
}

#[test]
fn debouncer_coalesces_before_reindex() {
    let d = IndexDebouncer::new(Duration::from_millis(DEFAULT_DEBOUNCE_MS));
    let branch = cis_wal::BranchId([0u8; 16]);
    for i in 0..8 {
        d.schedule(cis_core::IndexEvent {
            branch_id: branch,
            path: "same.py".into(),
            kind: cis_core::FsChangeKind::Modified,
        });
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(d.pending_count(), 1);
    let immediate = d.flush_all();
    assert_eq!(immediate.len(), 1);
    assert_eq!(immediate[0].path, "same.py");
    assert_eq!(d.pending_count(), 0);
}

#[test]
fn debouncer_records_watcher_metrics() {
    let metrics = Arc::new(WatcherMetrics::new());
    let d = IndexDebouncer::new(Duration::from_millis(50));
    d.set_metrics(Arc::clone(&metrics));
    let branch = cis_wal::BranchId([0u8; 16]);
    for _ in 0..5 {
        d.schedule(cis_core::IndexEvent {
            branch_id: branch,
            path: "coalesce.py".into(),
            kind: cis_core::FsChangeKind::Modified,
        });
    }
    let snap = metrics.snapshot(d.pending_count());
    assert_eq!(snap.scheduled_events, 5);
    assert_eq!(snap.coalesced_events, 4);
    let _ = d.flush_all();
    assert!(metrics.snapshot(0).debounce_p50_ms.is_some());
}

#[test]
fn poll_watcher_increments_raw_event_counter() {
    let root = temp_repo("watcher-metrics");
    let rt = Arc::new(CisMcpRuntime::new_dev(&root.to_string_lossy()));
    let cfg = FsSyncConfig {
        debounce: Duration::from_millis(80),
        poll_interval: Duration::from_millis(60),
    };
    let py_path = root.join("metric.py");
    fs::write(&py_path, "x = 1\n").unwrap();
    let rt_bg = Arc::clone(&rt);
    let _handle = thread::spawn(move || cis_core::run_fs_sync_loop(rt_bg, cfg, true));
    thread::sleep(Duration::from_millis(150));
    fs::write(&py_path, "def counted():\n    pass\n").unwrap();
    thread::sleep(Duration::from_millis(600));
    let idx = rt.index_status(0).unwrap();
    assert!(
        idx.watcher_raw_events > 0 || idx.watcher_coalesced_events > 0,
        "watcher metrics should move after external edit"
    );
}

#[test]
#[cfg(feature = "fs-notify")]
fn default_backend_is_native_notify() {
    std::env::remove_var("CIS_FS_POLL_ONLY");
    std::env::remove_var("CIS_FS_NOTIFY");
    std::env::remove_var("CIS_FS_POLL_FALLBACK");
    assert_eq!(
        cis_core::resolve_fs_watch_backend(),
        cis_core::FsWatchBackend::NativeNotify
    );
}

#[test]
fn fs_sync_config_from_env_overrides() {
    std::env::set_var("CIS_INDEX_DEBOUNCE_MS", "42");
    std::env::set_var("CIS_FS_POLL_MS", "99");
    let c = FsSyncConfig::from_env();
    assert_eq!(c.debounce, Duration::from_millis(42));
    assert_eq!(c.poll_interval, Duration::from_millis(99));
    std::env::remove_var("CIS_INDEX_DEBOUNCE_MS");
    std::env::remove_var("CIS_FS_POLL_MS");
    let _ = DEFAULT_DEBOUNCE_MS;
    let _ = DEFAULT_POLL_MS;
}

/// Confirm that `confirm_patch_internal` activates Speculative revisions without relying on the
/// full FS-sync loop (faster, deterministic).
#[test]
fn confirm_patch_internal_activates_speculative() {
    use cis_core::RevisionStatus;
    let root = temp_repo("confirm-internal");
    let rt = Arc::new(CisMcpRuntime::new_dev(&root.to_string_lossy()));
    let rel = "work.py";
    fs::write(root.join(rel), "def foo(): pass\n").unwrap();
    let write_resp = rt.write_file(0, rel, "def foo(): pass\n", true).unwrap();
    let patch_id = write_resp.patch_id;

    assert!(
        rt.graph_mutex().read().revisions()
            .any(|r| matches!(r.status, RevisionStatus::Speculative)),
        "expected Speculative revision after write_file"
    );

    rt.confirm_patch_internal(patch_id).unwrap();

    let spec_remaining: Vec<_> = rt.graph_mutex().read()
        .revisions()
        .filter(|r| matches!(r.status, RevisionStatus::Speculative))
        .map(|r| r.qualified_name.clone())
        .collect();
    assert!(spec_remaining.is_empty(), "no Speculative revisions should remain after confirm; got {:?}", spec_remaining);
}
