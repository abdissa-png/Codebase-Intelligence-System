[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/abdissa-png/Codebase-Intelligence-System)

# CIS — Codebase Intelligence System

CIS is a Rust daemon that builds a **versioned code graph** over your repository and exposes it to AI agents through the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). Agents can navigate symbols, search semantically, apply speculative edits with confirm/revert, and merge branches — with crash-safe persistence and background maintenance.

## What it does

| Capability | Description |
|------------|-------------|
| **Symbol navigation** | Find symbols, go to definition from that symbol's own edges, list callers and dependencies, expand context. A file's imports are a separate query (`file_imports`); they are not treated as every symbol's definition. |
| **Semantic & hybrid search** | Vector search over indexed symbols; optional HTTP embedding API |
| **Speculative writes** | `write_file` / `apply_patch` create draft graph revisions; `confirm_patch` or `revert_patch` promotes or rolls back (including disk content) |
| **Branching & merge** | Explicit CIS branches (`create_branch`, `switch_branch`); three-phase merge with saga recovery |
| **Filesystem sync** | Watches the repo and re-indexes changed files; detects external edits vs agent writes |
| **Crash recovery** | WAL-backed coordinator; graph/KV snapshots survive restarts |
| **Background workers** | WAL compaction, tombstone GC, audit epochs, embedding queue, disk pressure, consistency checks |

CIS is designed for **local agent-assisted development**: one developer runs `cisd` against one repo over MCP stdio (Cursor, Antigravity, or any other stdio MCP client). It is not a multi-tenant hosted service.

## Architecture

Three crates in this workspace:

| Crate | Role |
|-------|------|
| **`cis-wal`** | Write-ahead log and mutation phase machine |
| **`cis-core`** | Graph, KV store, ingest, query, merge engine, MCP runtime |
| **`cis-mcp`** | MCP JSON-RPC server and binaries (`cisd`, `cis-mcp`, `cis`) |

State lives under `.cis/` in the repo root. `cisd --mcp` (with the default `body-sqlite` feature) sets any unset `CIS_*_BACKEND` variable to `sqlite` and, when all six are sqlite, stores them in one `.cis/cis.db`. Set a backend to `json` or `file` to keep the split snapshot files. Library tests do not apply that default.

## Binaries

| Binary | Purpose |
|--------|---------|
| **`cisd`** | Main daemon — background workers; add `--mcp` for stdio MCP server |
| **`cis-mcp`** | MCP entrypoint; execs `cisd --mcp` when available (`CIS_STANDALONE_MCP=1` to run in-process) |
| **`cis`** | Maintenance CLI — migrate bodies/KV/graph to SQLite (`body-sqlite` feature) |
| **`cis-embed-smoke`** | Smoke test for external embedding API (`api-embeddings` feature) |

## Quick start

### Prerequisites

- Rust stable (2021 edition)
- Linux or macOS (FS notify uses inotify/kqueue)

### Build

```bash
cargo build --release -p cis-mcp --features python-ast,all-languages,body-sqlite,api-embeddings
```

Binaries: `target/release/cisd`, `target/release/cis-mcp`, `target/release/cis`.

### Run the daemon with MCP

From your project root (the repo CIS should index):

```bash
export CIS_REPO_ROOT="$(pwd)"
/path/to/cisd --mcp
```

On first run, CIS walks the tree (honoring `.gitignore` unless `CIS_INDEX_RESPECT_GITIGNORE=0`), builds the graph, and writes `.cis/`. Subsequent starts load persisted state.

Rust `crate::` / `self::` / `super::` imports resolve from the importing file's crate root (`lib.rs` or `main.rs`). An ambiguous basename is kept only when one module matches, or one sibling file matches. A named import whose symbol is missing is dropped. It is not attached to a file hub.

### Cursor MCP config (example)

```json
{
  "mcpServers": {
    "cis": {
      "command": "/path/to/cis/target/release/cis-mcp",
      "env": {
        "CIS_REPO_ROOT": "/path/to/your/repo"
      }
    }
  }
}
```

Set `CIS_REPO_ROOT` to the repo to index. `cisd --mcp` fills any unset `CIS_*_BACKEND` with `sqlite`; set one to `json` or `file` only when you want the split snapshot files.

Antigravity reads the same stdio server from `~/.gemini/config/mcp_config.json` (global) or `.agents/mcp_config.json` (one workspace), under `mcpServers`. Use `command` and `env` as above. Remote HTTP servers in Antigravity use `serverUrl`, which this local binary does not.

List tool names without starting the server:

```bash
cis-mcp --tools
```

## Local embedding server (semantic search)

CIS can use an external OpenAI-compatible `/v1/embeddings` endpoint for `semantic_search`. `hybrid_search` only reranks candidates you pass in; it does not call the embedder. For offline development, a small Python server ships in `scripts/` using [fastembed](https://github.com/qdrant/fastembed) (ONNX, no GPU).

### 1. Create the Python virtual environment

From the **cis repo root**:

```bash
./scripts/setup_embed_venv.sh
```

This creates `.venv-embed/` (gitignored) and installs `scripts/requirements-embed.txt`. Requires **Python 3.10+**.

Override the venv path with `CIS_EMBED_VENV` if needed.

### 2. Start the embed server

Terminal 1:

```bash
./scripts/run_embed_server.sh
```

Listens on `http://127.0.0.1:8080`. First start downloads the `all-MiniLM-L6-v2` model (~90 MB) into the Hugging Face cache.

Health check:

```bash
curl http://127.0.0.1:8080/healthz
```

### 3. Point CIS at the server

Copy the example env file and build with `api-embeddings`:

```bash
cp .env.example .env
cargo build -p cis-mcp --features python-ast,body-sqlite,api-embeddings
```

`cisd` loads `.env` from the repo root on startup. Minimal settings:

| Variable | Value |
|----------|-------|
| `CIS_EMBED_API_URL` | `http://127.0.0.1:8080/v1` |
| `CIS_EMBED_MODEL` | `all-MiniLM-L6-v2` |
| `CIS_EMBED_DIM` | `384` |

### 4. Verify end-to-end

With the embed server running:

```bash
cargo run -p cis-mcp --features api-embeddings --bin cis-embed-smoke
```

Manual curl test:

```bash
curl -X POST http://127.0.0.1:8080/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"all-MiniLM-L6-v2","input":"Hello world"}'
```

Without the embed server, CIS falls back to a deterministic **stub embedder** (offline tests only; not meaningful semantic search).

## MCP tools

### Read (22)

`find_symbol`, `find_symbol_at`, `go_to_definition`, `find_references`, `get_callers`, `get_dependencies`, `file_imports`, `expand_context`, `semantic_search`, `hybrid_search`, `build_context`, `build_context_at`, `get_symbol_body`, `explain_context`, `index_status`, `embedding_status`, `system_status`, `why_no_definition`, `list_branches`, `list_merge_metrics`, `verify_audit_chain`, `check_graph_consistency`

### Write (15)

`write_file`, `apply_patch`, `reindex_paths`, `confirm_patch`, `revert_patch`, `sweep_confirm_sidecars`, `purge_branch`, `retarget_edge`, `save_workspace`, `cancel_merge`, `merge_ttl_sweep`, `merge_branch`, `ingest_cis_config`, `create_branch`, `switch_branch`

`go_to_definition` follows Extends, then Imports, on the revision you pass. Calls and Uses are dependencies of that revision (`get_dependencies`, `get_callers`), not another definition. It does not copy the parent file's imports onto that symbol. `why_no_definition` reports the same Extends and Imports edges: `no_outbound_definition_edges`, `target_identity_without_active_revision`, or `index_mode_regex_limits_call_precision`. `file_imports` is the file-level import list. `find_symbol` ranks a symbol whose name matches the query ahead of a file hub whose path contains it.

## Configuration

CIS reads an optional `.env` in the repo root (does not override variables already set in the environment).

| Variable | Default | Meaning |
|----------|---------|---------|
| `CIS_REPO_ROOT` | `.` | Repository root to index |
| `CIS_GRAPH_BACKEND`, `CIS_KV_BACKEND`, `CIS_WAL_BACKEND`, `CIS_VECTOR_BACKEND`, `CIS_BODY_BACKEND`, `CIS_METADATA_BACKEND` | `json` / `file` in library code; `sqlite` when unset under `cisd --mcp` | Store backend. All six `sqlite` → one `.cis/cis.db` |
| `CIS_INDEX_RESPECT_GITIGNORE` | on | Set `0` to index files that `.gitignore` excludes (builtin skip dirs such as `target/` and `.git/` still apply) |
| `CIS_FS_SYNC` | on | Set `0` to disable filesystem watcher |
| `CIS_FS_POLL_ONLY` | off | Force mtime polling instead of native notify |
| `CIS_INDEX_DEBOUNCE_MS` | — | Debounce delay before re-indexing after FS events |
| `CIS_MCP_SKIP_INDEX` | off | Skip startup source walk (all registered languages) |
| `CIS_GIT_BRANCH_SYNC` | off | Set `1` to mirror git `HEAD` into CIS branches |
| `CIS_POLICY_PATH` | `.cis/ranking_policy.yaml` | Search ranking policy file |
| `CIS_EMBED_API_URL` | — | OpenAI-compatible `/v1/embeddings` endpoint |
| `CIS_EMBED_API_KEY` | — | API key for embeddings (not needed for local server) |
| `CIS_EMBED_MODEL` | — | Embedding model name |
| `CIS_EMBED_DIM` | — | Vector dimension (384 for `all-MiniLM-L6-v2`) |
| `CIS_WAL_MEMORY` | off | In-memory WAL (testing only) |

See `cis-core` sources and `cis-mcp/src/lib.rs` module docs for the full set of tuning knobs.

## Cargo features

| Feature | Crate | Effect |
|---------|-------|--------|
| `python-ast` | cis-mcp → cis-core | Tree-sitter Python ingest (classes, methods, calls) |
| `all-languages` | cis-mcp → cis-core | Tree-sitter indexers for Python, TypeScript/TSX, JavaScript/JSX, Rust, Go, Java, C, C++, C# |
| `ts-rust` / `ts-go` / `ts-javascript` / `ts-typescript` / `ts-java` / `ts-c` / `ts-cpp` / `ts-csharp` | cis-mcp → cis-core | Enable a single language grammar |
| `body-sqlite` | cis-mcp → cis-core | SQLite body/metadata backends; enables `cis` migrate commands |
| `api-embeddings` | cis-mcp → cis-core | HTTP embedding provider for semantic search |
| `fs-notify` | cis-mcp → cis-core | Native filesystem notifications (default on) |

Python and TypeScript/TSX are always registered. Other languages activate when their `ts-*` (or `all-languages`) feature is enabled at build time.

## Testing

```bash
# Core library + integration tests
cargo test -p cis-core --features tree-sitter,body-sqlite

# All language indexers (Rust/Go/JS/TS/Java/C/C++/C#)
cargo test -p cis-core --features tree-sitter-all --lib '_indexer::'

# Real-codebase indexer recall + **cross-file graph resolution**
# (CIS + chess pygame; optional OSS corpora)
cargo test -p cis-core --features tree-sitter-all --test indexer_corpus_eval -- --nocapture
# Extra languages: ./scripts/fetch_indexer_eval_corpora.sh
# FileIndex-only (skip ingest): CIS_INDEXER_EVAL_SKIP_GRAPH=1 …

# Fast concurrency stress suite
cargo test -p cis-core --features tree-sitter,body-sqlite --test stress_fast

# WAL crate
cargo test -p cis-wal
```

CI runs on push/PR to `main` (see `.github/workflows/`). The chess_pygame fixture is cloned in CI; locally:

```bash
git clone --depth 1 https://github.com/abdissa-png/A-chess-game-using-Pygame.git fixtures/chess_pygame
```

## Project layout

```
cis/
├── cis-core/          # Engine (graph, ingest, merge, query, MCP runtime)
├── cis-wal/           # Write-ahead log
├── cis-mcp/           # MCP server + binaries
├── scripts/           # Demo/eval helpers; embed server setup (see README)
│   ├── setup_embed_venv.sh
│   ├── run_embed_server.sh
│   ├── requirements-embed.txt
│   └── local_embed_server.py
├── fixtures/          # Local test repos (gitignored; clone for tests)
└── .github/workflows/ # CI
```

## Status

**Version 0.1.0** — solid for local agent workflows on medium-sized repos. Known limits:

- SQLite graph queries are the `cisd --mcp` path; the JSON/RAM graph remains the library default and the fallback if sqlite open fails
- Flat ANN for semantic search (large monorepos will need scale work)
- MCP over stdio with a trusted local session (no network auth)
- Observability is primarily `eprintln!` plus `system_status` / merge metrics JSONL

## License

MIT
