//! **Phase 5.4** — consolidated `system_status` MCP.

use std::fs;
use std::path::PathBuf;

use cis_core::CisMcpRuntime;

fn temp_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cis-system-status-{}-{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn system_status_returns_all_sections() {
    let root = temp_repo("sections");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let status = rt.system_status(0).unwrap();
    assert!(status.quota.max > 0);
    assert_eq!(status.quota.active, rt.index_status(0).unwrap().quota_active);
    assert!(!status.consistency.summary.is_empty());
    assert!(!status.background_workers.workers.is_empty());
}

#[test]
fn consistency_cache_updates_after_check_graph_consistency() {
    let root = temp_repo("consistency-cache");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let before = rt.system_status(0).unwrap();
    let _ = rt.check_graph_consistency(0).unwrap();
    let after = rt.system_status(0).unwrap();
    assert!(after.consistency.checked_at_ms.is_some());
    if before.consistency.checked_at_ms.is_some() {
        assert!(after.consistency.checked_at_ms >= before.consistency.checked_at_ms);
    }
}

#[test]
fn embedding_status_matches_index_status_queue_fields() {
    let root = temp_repo("embedding");
    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let embed = rt.embedding_status(0).unwrap();
    let idx = rt.index_status(0).unwrap();
    assert_eq!(embed.embedding.queue_depth, idx.embedding_queue_depth);
    assert_eq!(embed.embedding.queue_state, idx.embedding_queue_state);
    assert_eq!(embed.embedding.hwm, idx.embedding_queue_hwm);
}
