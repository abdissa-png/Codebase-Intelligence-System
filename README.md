# CIS — Codebase Intelligence System

CIS is a Rust daemon that builds a **versioned code graph** over your repository and exposes it to AI agents through the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). Agents can navigate symbols, search semantically, apply speculative edits with confirm/revert, and merge branches — with crash-safe persistence and background maintenance.

## What it does

| Capability | Description |
|------------|-------------|
| **Symbol navigation** | Find symbols, go to definition, list callers/callees, expand context around a node |
| **Semantic & hybrid search** | Vector search over indexed symbols; optional HTTP embedding API |
| **Speculative writes** | `write_file` / `apply_patch` create draft graph revisions; `confirm_patch` or `revert_patch` promotes or rolls back (including disk content) |
| **Branching & merge** | Explicit CIS branches (`create_branch`, `switch_branch`); three-phase merge with saga recovery |
| **Filesystem sync** | Watches the repo and re-indexes changed files; detects external edits vs agent writes |
| **Crash recovery** | WAL-backed coordinator; graph/KV snapshots survive restarts |
| **Background workers** | WAL compaction, tombstone GC, audit epochs, embedding queue, disk pressure, consistency checks |

CIS is designed for **local agent-assisted development**: one developer runs `cisd` against one repo over MCP stdio (e.g. from Cursor). It is not a multi-tenant hosted service.

## Architecture

Three crates in this workspace:

| Crate | Role |
|-------|------|
| **`cis-wal`** | Write-ahead log and mutation phase machine |
| **`cis-core`** | Graph, KV store, ingest, query, merge engine, MCP runtime |
| **`cis-mcp`** | MCP JSON-RPC server and binaries (`cisd`, `cis-mcp`, `cis`) |

State lives under `.cis/` in the repo root (graph, KV, WAL, bodies, audit log, merge metrics).

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
cargo build --release -p cis-mcp --features python-ast,body-sqlite,api-embeddings
```

Binaries: `target/release/cisd`, `target/release/cis-mcp`, `target/release/cis`.

### Run the daemon with MCP

From your project root (the repo CIS should index):

```bash
export CIS_REPO_ROOT="$(pwd)"
/path/to/cisd --mcp
```

On first run, CIS walks the tree, builds the graph, and writes `.cis/`. Subsequent starts load persisted state.

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

List tool names without starting the server:

```bash
cis-mcp --tools
```

## Local embedding server (semantic search)

CIS can use an external OpenAI-compatible `/v1/embeddings` endpoint for `semantic_search` and `hybrid_search`. For offline development, a small Python server ships in `scripts/` using [fastembed](https://github.com/qdrant/fastembed) (ONNX, no GPU).

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

`write_file`, `apply_patch`, `reindex_paths`, `confirm_patch`, `revert_patch`, `sweep_confirm_sidecars`, `purge_branch`, `save_workspace`, `cancel_merge`, `merge_ttl_sweep`, `merge_branch`, `ingest_cis_config`, `create_branch`, `switch_branch`

## Configuration

CIS reads an optional `.env` in the repo root (does not override variables already set in the environment).

| Variable | Default | Meaning |
|----------|---------|---------|
| `CIS_REPO_ROOT` | `.` | Repository root to index |
| `CIS_FS_SYNC` | on | Set `0` to disable filesystem watcher |
| `CIS_FS_POLL_ONLY` | off | Force mtime polling instead of native notify |
| `CIS_INDEX_DEBOUNCE_MS` | — | Debounce delay before re-indexing after FS events |
| `CIS_MCP_SKIP_INDEX` | off | Skip startup Python walk |
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
| `body-sqlite` | cis-mcp → cis-core | SQLite body/metadata backends; enables `cis` migrate commands |
| `api-embeddings` | cis-mcp → cis-core | HTTP embedding provider for semantic search |
| `fs-notify` | cis-mcp → cis-core | Native filesystem notifications (default on) |

## Testing

```bash
# Core library + integration tests
cargo test -p cis-core --features tree-sitter,body-sqlite

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

- Full in-memory graph; flat ANN for semantic search (large monorepos will need scale work)
- MCP over stdio with a trusted local session (no network auth)
- Observability is primarily `eprintln!` plus `system_status` / merge metrics JSONL

## License

MIT
