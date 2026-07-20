//! Chess semantic search with StubEmbedder (offline, no live embed server).
//!
//! Run: `cargo test -p cis-core --features tree-sitter,body-sqlite --test chess_semantic_search`

mod support;

use std::path::PathBuf;

use cis_core::CisMcpRuntime;
use cis_wal::BranchId;

use support::{chess_fixture_root, ensure_chess_fixture};

fn chess_root() -> PathBuf {
    chess_fixture_root()
}

fn embed_all_bodies(rt: &CisMcpRuntime) {
    let branch = BranchId([0u8; 16]);
    rt.sync_bodies_after_commit(branch);
    let model_id = rt.embedder().model_id().to_string();
    let g = rt.graph_mutex().read();
    let mut pairs: Vec<([u8; 32], String)> = Vec::new();
    for r in g.revisions() {
        if r.branch_id != branch {
            continue;
        }
        if let Some(body) = rt.body_store().get(&r.body_hash) {
            if let Ok(text) = String::from_utf8(body) {
                let enrich = format!("{}\n{}\n{}", r.qualified_name, r.file_path, text);
                pairs.push((r.body_hash, enrich));
            }
        }
    }
    drop(g);
    for chunk in pairs.chunks(32) {
        let texts: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();
        if let Ok(vecs) = rt.embedder().embed_batch(&texts) {
            for (i, vec) in vecs.into_iter().enumerate() {
                rt.coordinator()
                    .vector()
                    .set_embedding(chunk[i].0, vec, &model_id);
            }
        }
    }
    rt.rebuild_ann_index();
    rt.sync_index_status_from_graph();
}

fn top_names(rt: &CisMcpRuntime, query: &str, k: usize) -> Vec<String> {
    rt.semantic_search(0, query, None, k)
        .expect("semantic_search")
        .hits
        .into_iter()
        .map(|h| h.qualified_name)
        .collect()
}

#[test]
#[cfg(feature = "tree-sitter")]
fn chess_semantic_search_stub_embedder() {
    let root = chess_root();
    if !ensure_chess_fixture(&root) {
        return;
    }

    std::env::set_var("CIS_FORCE_REINDEX", "1");
    std::env::remove_var("CIS_SKIP_WORKSPACE_LOAD");

    let rt = CisMcpRuntime::new_dev(&root.to_string_lossy());
    let boot = rt
        .bootstrap_python_index_from_repo()
        .expect("full reindex");
    assert!(boot.applied > 0, "ingest: {:?}", boot);

    embed_all_bodies(&rt);

    let idx = rt.index_status(0).expect("index_status");
    assert!(
        idx.embeddings_indexed > 0,
        "expected embeddings after stub embed, got indexed={} stale={}",
        idx.embeddings_indexed,
        idx.embeddings_stale
    );
    assert!(idx.ann_index_size > 0, "ANN index should be populated");

    let minimax = top_names(&rt, "minimax", 8);
    eprintln!("minimax hits: {minimax:?}");
    assert!(
        minimax.iter().any(|n| n.contains("minimax")),
        "expected minimax symbol in top-K, got {minimax:?}"
    );

    let promo = top_names(&rt, "pawn promotion", 12);
    eprintln!("promotion hits: {promo:?}");
    assert!(
        promo.iter().any(|n| {
            n.contains("Move") || n.contains("Board") || n.contains("promotion")
        }),
        "expected promotion-related symbol in top-K, got {promo:?}"
    );

    // Stub BOW matches token overlap; natural-language "chess tile click" needs API embedder (see runbook).
    let click = top_names(&rt, "on_click", 8);
    eprintln!("on_click hits: {click:?}");
    assert!(
        click.iter().any(|n| n.contains("ChessTile.on_click")),
        "expected ChessTile.on_click in top-K for on_click query, got {click:?}"
    );

    let nl_click = top_names(&rt, "chess tile click", 12);
    eprintln!("chess tile click hits (observability): {nl_click:?}");
}
