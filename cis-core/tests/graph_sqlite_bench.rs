//! Graph commit latency: JSON vs SQLite snapshot (ignored unless `--ignored`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cis_core::{
    cis_dir, open_persisted_coordinator, save_workspace_snapshots, FsChangeKind, IndexEvent,
    IndexEventQueue, MemoryKv, WriteCoordinator,
};
use cis_core::MergeSagaOrchestrator;
use cis_wal::{BranchId, MutationLog};

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-graph-bench-{}-{}",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn seed_graph(coord: &WriteCoordinator, root: &PathBuf, n: usize) {
    let kv = Arc::new(MemoryKv::new());
    let branch = BranchId([0u8; 16]);
    let mut events = Vec::new();
    for i in 0..n {
        let path = format!("sym_{i}.py");
        std::fs::write(
            root.join(&path),
            format!("def f{i}():\n    return {i}\n"),
        )
        .unwrap();
        events.push(IndexEvent {
            branch_id: branch,
            path,
            kind: FsChangeKind::Modified,
            old_path: None,
        });
    }
    cis_core::apply_index_events(
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

#[test]
#[ignore]
fn bench_graph_json_vs_sqlite_commit() {
    let root = temp_repo("bench");
    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let n = 1000usize;

    std::env::set_var("CIS_GRAPH_BACKEND", "json");
    {
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));
        seed_graph(&coord, &root, n);
        let g = coord.graph().read();
        let kv = MemoryKv::new();
        let t0 = Instant::now();
        save_workspace_snapshots(&cis_dir(&root), &g, coord.vector(), &kv).unwrap();
        eprintln!("json snapshot {} symbols: {} ms", n, t0.elapsed().as_millis());
    }

    std::env::set_var("CIS_GRAPH_BACKEND", "sqlite");
    {
        let coord = WriteCoordinator::new(Arc::clone(&wal));
        seed_graph(&coord, &root, n);
        let g = coord.graph().read();
        let kv = MemoryKv::new();
        let t0 = Instant::now();
        save_workspace_snapshots(&cis_dir(&root), &g, coord.vector(), &kv).unwrap();
        eprintln!("sqlite snapshot {} symbols: {} ms", n, t0.elapsed().as_millis());
    }

    let _ = std::fs::remove_dir_all(&root);
}
