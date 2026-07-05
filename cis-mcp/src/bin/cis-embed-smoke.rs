//! End-to-end smoke test: **`CIS_EMBED_*`** → API embed → ingest → worker → **`semantic_search`**.
//!
//! ```bash
//! cd cis
//! cargo run -p cis-mcp --features api-embeddings --bin cis-embed-smoke
//! ```

use std::sync::Arc;

use cis_core::{
    api_embedder_configured, embedder_from_env, embeddings_endpoint_url, load_env_file,
    BodyStore, CisMcpRuntime, EmbeddingWorker,
};

fn main() {
    load_env_file(None);

    let embedder = embedder_from_env();
    println!("=== CIS embedding smoke test ===");
    println!("CIS_EMBED_API_URL = {:?}", std::env::var("CIS_EMBED_API_URL").ok());
    println!(
        "resolved endpoint   = {}",
        embeddings_endpoint_url(&std::env::var("CIS_EMBED_API_URL").unwrap_or_default())
    );
    println!("api_embedder_configured = {}", api_embedder_configured());
    println!("active model_id     = {}", embedder.model_id());
    println!("expected dim        = {}", embedder.dim());

    let sample = "def authenticate_user(password: str) -> bool:\n    return verify_credentials(password)";
    print!("1) API embed_batch … ");
    let vecs = match embedder.embed_batch(&[sample.to_string()]) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("FAILED\n   {e}");
            eprintln!("\nHint: HF spaces can return 503 when cold/overloaded — retry in a minute.");
            std::process::exit(1);
        }
    };
    println!("OK (vector dim = {})", vecs[0].len());

    let dir = std::env::temp_dir().join(format!("cis-embed-smoke-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("tmpdir");
    std::fs::write(dir.join("auth.py"), sample).expect("write auth.py");
    std::fs::write(dir.join("game.py"), "def render_board():\n    pass\n").expect("write game.py");

    std::env::set_var("CIS_WAL_MEMORY", "1");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");

    print!("2) ingest Python workspace … ");
    let rt = CisMcpRuntime::new_dev(&dir.to_string_lossy());
    rt.bootstrap_python_index_from_repo()
        .expect("bootstrap_python_index_from_repo");
    println!("OK");

    let body_store = BodyStore::new(Arc::clone(rt.kv()));

    print!("3) drain embedding queue (API worker) … ");
    let mut total_embedded = 0usize;
    for _ in 0..20 {
        let rep = EmbeddingWorker::drain_batch(
            rt.coordinator().as_ref(),
            embedder.as_ref(),
            &body_store,
            rt.coordinator().vector(),
            16,
        );
        total_embedded += rep.embedded_ok;
        if rep.attempted == 0 {
            break;
        }
        if rep.embedded_ok == 0 && rep.requeued > 0 {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
    println!(
        "OK (embedded {} bodies, queue depth = {})",
        total_embedded,
        rt.coordinator().embedding_queue_depth()
    );

    print!("4) semantic_search(\"authenticate\") … ");
    let resp = rt
        .semantic_search(0, "authenticate user credentials", None, 5)
        .expect("semantic_search");
    println!("OK ({} hits)", resp.hits.len());
    for (i, h) in resp.hits.iter().enumerate() {
        println!("   [{}] score={:.4}  {}", i + 1, h.score, h.qualified_name);
    }
    println!("   meta.degraded_modes = {:?}", resp.meta.degraded_modes);
    println!("   meta.stale_count    = {}", resp.meta.stale_count);

    if resp.meta.degraded_modes.contains(&"stub_embedder".to_string()) {
        eprintln!("\nWARN: still on stub embedder — rebuild with --features api-embeddings");
        std::process::exit(2);
    }
    if resp.meta.degraded_modes.contains(&"semantic_degraded".to_string()) {
        eprintln!("\nWARN: semantic search fell back to substring (embeddings still pending?)");
        std::process::exit(3);
    }
    if resp.hits.is_empty()
        || !resp.hits[0].qualified_name.to_lowercase().contains("authenticate")
    {
        eprintln!("\nWARN: expected auth.authenticate_user near top of results");
        std::process::exit(4);
    }

    println!("\n=== PASS: real API embeddings used end-to-end ===");
    let _ = std::fs::remove_dir_all(&dir);
}
