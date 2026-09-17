//! Boot + `find_symbol` latency: JSON hydrate vs SQLite query-in-place.
//!
//! Default CI runs the small correctness gate. Heavy timings are `#[ignore]`:
//! `cargo test -p cis-core --features tree-sitter,body-sqlite --test graph_sqlite_bench -- --ignored --nocapture`

#![cfg(feature = "body-sqlite")]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cis_core::{
    apply_index_events, cis_dir, open_persisted_coordinator, open_workspace_kv, CisMcpRuntime,
    FsChangeKind, IndexEvent, IndexEventQueue, MemoryKv, MergeSagaOrchestrator, WriteCoordinator,
    FROM_SNAPSHOT_CALLS, LOAD_INTO_CALLS,
};
use cis_wal::BranchId;

static CIS_ENV_LOCK: Mutex<()> = Mutex::new(());

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-graph-bench-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn clear_store_env() {
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");
    std::env::remove_var("CIS_FORCE_REINDEX");
    std::env::remove_var("CIS_SKIP_MERGE_RECOVER");
    std::env::remove_var("CIS_BODY_BACKEND");
    std::env::remove_var("CIS_METADATA_BACKEND");
    std::env::remove_var("CIS_GRAPH_BACKEND");
    std::env::remove_var("CIS_KV_BACKEND");
    std::env::remove_var("CIS_KV_JSON_EXPORT");
    std::env::remove_var("CIS_WAL_BACKEND");
    std::env::remove_var("CIS_WAL_JSON_EXPORT");
    std::env::remove_var("CIS_VECTOR_BACKEND");
    std::env::remove_var("CIS_VECTOR_JSON_EXPORT");
    std::env::remove_var("CIS_DEFER_VECTOR_LOAD");
    std::env::remove_var("CIS_GRAPH_JSON_EXPORT");
    std::env::remove_var("CIS_WAL_MEMORY");
}

fn set_sqlite_profile() {
    std::env::set_var("CIS_GRAPH_BACKEND", "sqlite");
    std::env::set_var("CIS_KV_BACKEND", "sqlite");
    std::env::set_var("CIS_WAL_BACKEND", "sqlite");
    std::env::set_var("CIS_VECTOR_BACKEND", "sqlite");
    std::env::set_var("CIS_BODY_BACKEND", "sqlite");
    std::env::set_var("CIS_METADATA_BACKEND", "sqlite");
}

fn set_json_profile() {
    std::env::set_var("CIS_GRAPH_BACKEND", "json");
    std::env::set_var("CIS_KV_BACKEND", "json");
    std::env::set_var("CIS_WAL_BACKEND", "json");
    std::env::set_var("CIS_VECTOR_BACKEND", "json");
    std::env::set_var("CIS_BODY_BACKEND", "file");
    std::env::set_var("CIS_METADATA_BACKEND", "json");
}

fn seed_graph(coord: &WriteCoordinator, kv: Arc<MemoryKv>, root: &PathBuf, n: usize) {
    let branch = BranchId([0u8; 16]);
    let mut events = Vec::with_capacity(n);
    for i in 0..n {
        let path = format!("sym_{i}.py");
        std::fs::write(root.join(&path), format!("def f{i}():\n    return {i}\n")).unwrap();
        events.push(IndexEvent {
            branch_id: branch,
            path,
            kind: FsChangeKind::Modified,
            old_path: None,
        });
    }
    apply_index_events(
        &IndexEventQueue::new(),
        coord,
        kv,
        events,
        |rel| std::fs::read_to_string(root.join(rel)),
        None,
        None,
    )
    .expect("ingest");
}

fn seed_workspace(root: &PathBuf, n: usize) {
    let cis = cis_dir(root);
    std::fs::create_dir_all(&cis).unwrap();
    let kv = Arc::new(open_workspace_kv(&cis));
    let coord = open_persisted_coordinator(root).expect("open for seed");
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::clone(&kv)));
    seed_graph(coord.as_ref(), kv, root, n);
    drop(coord);
}

fn vm_rss_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

fn find_via_view(coord: &WriteCoordinator, needle: &str) -> usize {
    coord.with_graph_view(|g| {
        g.find_revisions_qn_contains(&[BranchId([0u8; 16])], needle, 8)
            .len()
    })
}

/// Same-process JSON vs sqlite VmRSS is not comparable (allocator does not return pages).
/// Printed for sqlite boot only as a hint, never a CI gate.
fn print_rss(label: &str) {
    if let Some(kb) = vm_rss_kb() {
        eprintln!("{label} VmRSS={kb} kB (informational; not a gate)");
    }
}

#[test]
fn sqlite_boot_and_find_symbol_skips_hydrate() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    clear_store_env();
    set_sqlite_profile();
    let root = temp_repo("boot-find");
    let n = 24usize;
    seed_workspace(&root, n);

    let load_before = LOAD_INTO_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    let snap_before = FROM_SNAPSHOT_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    let t0 = Instant::now();
    let coord = open_persisted_coordinator(&root).expect("reopen");
    let boot_ms = t0.elapsed().as_millis();
    let load_after = LOAD_INTO_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    let snap_after = FROM_SNAPSHOT_CALLS.load(std::sync::atomic::Ordering::SeqCst);

    assert_eq!(load_after, load_before, "sqlite boot must not call load_into");
    assert_eq!(
        snap_after, snap_before,
        "sqlite boot must not call from_snapshot"
    );
    assert_eq!(
        coord.graph().read().revision_count(),
        0,
        "RAM overlay stays empty at sqlite boot"
    );
    let durable = coord.durable_revision_count();
    assert!(
        durable > 0,
        "SQL must hold revisions while overlay is empty"
    );
    let t1 = Instant::now();
    let hits = find_via_view(coord.as_ref(), "f10");
    let find_ms = t1.elapsed().as_millis();
    assert!(hits > 0, "GraphView find_symbol path must hit SQL");
    drop(coord);

    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let found = rt
        .find_symbol(0, "f10", None, 8, false)
        .expect("find_symbol");
    assert!(
        !found.matches.is_empty(),
        "MCP find_symbol must see sqlite graph"
    );
    assert_eq!(
        rt.coordinator().graph().read().revision_count(),
        0,
        "new_dev must not hydrate overlay on sqlite"
    );
    eprintln!(
        "sqlite boot overlay=0 durable={durable} open={}ms find_qn={}ms mcp_hits={}",
        boot_ms,
        find_ms,
        found.matches.len()
    );
    print_rss("sqlite boot");
    let _ = std::fs::remove_dir_all(&root);
    clear_store_env();
}

#[test]
#[ignore]
fn bench_sqlite_boot_vs_json_find_symbol() {
    let _env = CIS_ENV_LOCK.lock().unwrap();
    let n = 400usize;

    clear_store_env();
    set_json_profile();
    let json_root = temp_repo("json");
    seed_workspace(&json_root, n);
    let t0 = Instant::now();
    let json_coord = open_persisted_coordinator(&json_root).expect("json reopen");
    let json_boot_ms = t0.elapsed().as_millis();
    let json_overlay = json_coord.graph().read().revision_count();
    let json_durable = json_coord.durable_revision_count();
    let t1 = Instant::now();
    let json_hits = find_via_view(json_coord.as_ref(), "f10");
    let json_find_ms = t1.elapsed().as_millis();
    drop(json_coord);
    eprintln!(
        "json   n={n} boot={json_boot_ms}ms find_qn={json_find_ms}ms overlay={json_overlay} durable={json_durable} hits={json_hits}"
    );

    clear_store_env();
    set_sqlite_profile();
    let sql_root = temp_repo("sqlite");
    seed_workspace(&sql_root, n);
    let load_before = LOAD_INTO_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    let t2 = Instant::now();
    let sql_coord = open_persisted_coordinator(&sql_root).expect("sqlite reopen");
    let sql_boot_ms = t2.elapsed().as_millis();
    let load_after = LOAD_INTO_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    let sql_overlay = sql_coord.graph().read().revision_count();
    let sql_durable = sql_coord.durable_revision_count();
    let t3 = Instant::now();
    let sql_hits = find_via_view(sql_coord.as_ref(), "f10");
    let sql_find_ms = t3.elapsed().as_millis();
    drop(sql_coord);
    let t4 = Instant::now();
    let rt = CisMcpRuntime::new_dev(&sql_root.to_string_lossy());
    let mcp = rt.find_symbol(0, "f10", None, 8, false).expect("mcp find");
    let mcp_ms = t4.elapsed().as_millis();
    eprintln!(
        "sqlite n={n} boot={sql_boot_ms}ms find_qn={sql_find_ms}ms new_dev+find_symbol={mcp_ms}ms overlay={sql_overlay} durable={sql_durable} hits={sql_hits} mcp_hits={} load_into_delta={}",
        mcp.matches.len(),
        load_after - load_before
    );
    print_rss("sqlite boot (ignored bench)");
    assert_eq!(load_after, load_before);
    assert_eq!(sql_overlay, 0);
    assert!(sql_durable > 0);
    assert!(sql_hits > 0 && json_hits > 0);
    assert!(!mcp.matches.is_empty());

    let _ = std::fs::remove_dir_all(&json_root);
    let _ = std::fs::remove_dir_all(&sql_root);
    clear_store_env();
}
