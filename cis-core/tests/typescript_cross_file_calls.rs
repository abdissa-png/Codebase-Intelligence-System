//! TypeScript indexer MVP — cross-file `Calls` via shared CallResolver.
//!
//! Run: `cargo test -p cis-core --test typescript_cross_file_calls`

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use cis_core::{
    apply_index_events, EdgeType, FsChangeKind, IndexEvent, IndexEventQueue, MemoryKv,
    WriteCoordinator,
};
use cis_core::MergeSagaOrchestrator;
use cis_wal::{BranchId, MutationLog};

fn temp_ts_repo() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-ts-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("util.ts"),
        "export function helper(x: number): number {\n  return x + 1;\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("main.ts"),
        "import { helper } from './util';\n\nexport function run(): number {\n  return helper(2);\n}\n",
    )
    .unwrap();
    dir
}

#[test]
fn typescript_cross_file_calls() {
    let root = temp_ts_repo();
    let wal: Arc<dyn cis_wal::MutationLogStore> = Arc::new(MutationLog::new());
    let coord = WriteCoordinator::new(Arc::clone(&wal));
    let kv = Arc::new(MemoryKv::new());
    let _ = coord.reconcile_on_startup(&MergeSagaOrchestrator::new(Arc::new(MemoryKv::new())));
    let branch = BranchId([0u8; 16]);

    let rep = apply_index_events(
        &IndexEventQueue::new(),
        &coord,
        kv,
        vec![
            IndexEvent {
                branch_id: branch,
                path: "util.ts".into(),
                kind: FsChangeKind::Modified,
            },
            IndexEvent {
                branch_id: branch,
                path: "main.ts".into(),
                kind: FsChangeKind::Modified,
            },
        ],
        |rel| fs::read_to_string(root.join(rel)),
        None,
        None,
    )
    .expect("ingest");
    assert_eq!(rep.applied, 2);

    let g = coord.graph().read();
    let mut run_calls = 0usize;
    let mut calls_helper = false;
    for r in g.revisions() {
        if !r.qualified_name.contains("main.ts::run") {
            continue;
        }
        for e in g.outbound_edges(r.revision_id) {
            if e.ty != EdgeType::Calls {
                continue;
            }
            run_calls += 1;
            let tgt = g
                .primary_revision_for_identity(branch, e.target_identity_id)
                .map(|t| t.qualified_name.clone())
                .unwrap_or_default();
            if tgt.contains("helper") {
                calls_helper = true;
            }
        }
    }
    assert!(run_calls > 0, "expected Calls from main.ts::run");
    assert!(calls_helper, "expected run() to call helper()");
    let _ = fs::remove_dir_all(&root);
}
