//! CIS workspace maintenance CLI (**Phase 8B/8C**).

use std::env;
use std::path::PathBuf;
use std::process;

fn usage() {
    eprintln!("CIS workspace tools");
    eprintln!();
    eprintln!("  cis migrate-bodies [--delete-files] <repo_root>");
    eprintln!("      Copy `.cis/bodies/` shard files into the active body blob store.");
    eprintln!();
    eprintln!("  cis migrate-kv [--compact] <repo_root>");
    eprintln!("      Copy `ris:` keys from kv.json into `.cis/store.db` (requires body-sqlite).");
    eprintln!();
    eprintln!("  cis migrate-graph [--compact] [--rebuild-normalized] <repo_root>");
    eprintln!("      Copy graph.json into graph.db (requires body-sqlite).");
    eprintln!("      --rebuild-normalized rewrites normalized rows from blob snapshot.");
    eprintln!();
    eprintln!("Environment:");
    eprintln!("  CIS_BODY_BACKEND=file|sqlite   (default: file)");
    eprintln!("  CIS_METADATA_BACKEND=json|sqlite (default: json)");
    eprintln!("  CIS_GRAPH_BACKEND=json|sqlite  (default: json)");
    eprintln!("  CIS_MIGRATION_STRICT=1         fail if sqlite feature/backend missing");
}

fn repo_root_from_args(args: &[String]) -> PathBuf {
    args.iter()
        .skip(2)
        .find(|a| !a.starts_with('-'))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("missing repo_root");
            usage();
            process::exit(1);
        })
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        usage();
        process::exit(1);
    }
    match args[1].as_str() {
        "migrate-bodies" => {
            let delete_files = args.iter().any(|a| a == "--delete-files");
            let root = repo_root_from_args(&args);
            match cis_core::migrate_bodies_from_files(&root, delete_files) {
                Ok(rep) => {
                    println!(
                        "migrate-bodies: scanned={} inserted={} skipped={} verified={} deleted={}",
                        rep.files_scanned,
                        rep.inserted,
                        rep.skipped_existing,
                        rep.verified,
                        rep.files_deleted
                    );
                    if let Ok((file_n, sqlite_n)) = cis_core::verify_body_migration(&root) {
                        println!("verify: file_shards={file_n} sqlite_rows={sqlite_n}");
                    }
                }
                Err(e) => {
                    eprintln!("migrate-bodies failed: {e}");
                    process::exit(1);
                }
            }
        }
        "migrate-kv" => {
            let compact = args.iter().any(|a| a == "--compact");
            let root = repo_root_from_args(&args);
            match cis_core::migrate_kv_ris_to_sqlite(&root, compact) {
                Ok(rep) => {
                    println!(
                        "migrate-kv: ris_migrated={} stripped={}",
                        rep.ris_keys_migrated, rep.kv_json_stripped
                    );
                }
                Err(e) => {
                    eprintln!("migrate-kv failed: {e}");
                    process::exit(1);
                }
            }
        }
        "migrate-graph" => {
            let compact = args.iter().any(|a| a == "--compact");
            let rebuild = args.iter().any(|a| a == "--rebuild-normalized");
            let root = repo_root_from_args(&args);
            std::env::set_var("CIS_GRAPH_BACKEND", "sqlite");
            let result = if rebuild {
                cis_core::rebuild_graph_normalized(&cis_core::cis_dir(&root), compact)
            } else {
                cis_core::migrate_graph_json_to_sqlite(&root, compact, false)
            };
            match result {
                Ok(rep) => {
                    println!(
                        "migrate-graph: identities={} revisions={} edges={} stripped={}",
                        rep.identities, rep.revisions, rep.edges, rep.graph_json_stripped
                    );
                }
                Err(e) => {
                    eprintln!("migrate-graph failed: {e}");
                    process::exit(1);
                }
            }
        }
        "--help" | "-h" | "help" => usage(),
        other => {
            eprintln!("unknown command: {other}");
            usage();
            process::exit(1);
        }
    }
}
