//! CIS **MCP** server — JSON-RPC over stdio (**FR-4.1**, **FR-4.5**, **FR-4.11**).
//! Default framing is newline-delimited JSON (Cursor / `@modelcontextprotocol/sdk`);
//! `Content-Length` framing is still accepted on read (`CIS_MCP_CONTENT_LENGTH=1` to write it).
//! Prefer **`cisd --mcp`** (single process with background policy / merge TTL / vector cleanup).
//! `cis-mcp` execs sibling `cisd --mcp` unless `CIS_STANDALONE_MCP=1`.
//! Env: `CIS_REPO_ROOT` (default `.`), `CIS_MCP_SKIP_INDEX=1` skips startup Python walk.
//! **Phase 2 FS sync:** enabled by default (`CIS_FS_SYNC=0` to disable). Native **`notify`**
//! watcher is on by default (`fs-notify` feature). `CIS_FS_POLL_ONLY=1` forces mtime polling;
//! `CIS_FS_POLL_FALLBACK=1` uses notify + poll; `CIS_INDEX_DEBOUNCE_MS` / `CIS_FS_POLL_MS` tune timing.
//! Build with `--features python-ast` to enable tree-sitter Python ingest from `cis-core`.

use std::io::{self, BufRead, BufReader, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use cis_core::{
    resolve_commit_anchor_to_wal_log, CisMcpRuntime, HybridSearchCandidate, MergeProgressEvent,
    MergeProgressSink, QueryMeta,
};
use cis_wal::{BranchId, NodeRevisionId};
use serde_json::{json, Value};

/// Shared slot so MCP can answer `initialize` / `tools/list` while workspace boot runs.
enum McpRuntimeSlotState {
    Loading,
    Ready(Arc<CisMcpRuntime>),
    Failed(String),
}

/// Filled by a background boot thread; stdio handshake does not wait on it.
pub struct McpRuntimeSlot {
    state: Mutex<McpRuntimeSlotState>,
    cv: Condvar,
}

impl McpRuntimeSlot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(McpRuntimeSlotState::Loading),
            cv: Condvar::new(),
        })
    }

    pub fn set_ready(&self, rt: Arc<CisMcpRuntime>) {
        let mut g = self.state.lock().unwrap();
        *g = McpRuntimeSlotState::Ready(rt);
        self.cv.notify_all();
    }

    pub fn set_failed(&self, err: impl Into<String>) {
        let mut g = self.state.lock().unwrap();
        *g = McpRuntimeSlotState::Failed(err.into());
        self.cv.notify_all();
    }

    pub fn wait(&self, timeout: Duration) -> Result<Arc<CisMcpRuntime>, String> {
        let mut g = self.state.lock().unwrap();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match &*g {
                McpRuntimeSlotState::Ready(rt) => return Ok(Arc::clone(rt)),
                McpRuntimeSlotState::Failed(e) => return Err(e.clone()),
                McpRuntimeSlotState::Loading => {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Err(
                            "workspace still loading (CIS warm-start); retry in a moment".into(),
                        );
                    }
                    let (next, timeout_result) =
                        self.cv.wait_timeout(g, deadline.saturating_duration_since(now)).unwrap();
                    g = next;
                    if timeout_result.timed_out() {
                        if matches!(*g, McpRuntimeSlotState::Loading) {
                            return Err(
                                "workspace still loading (CIS warm-start); retry in a moment"
                                    .into(),
                            );
                        }
                    }
                }
            }
        }
    }
}

const MCP_READ_TOOLS: &[&str] = &[
    "find_symbol",
    "find_symbol_at",
    "go_to_definition",
    "find_references",
    "get_callers",
    "get_dependencies",
    "file_imports",
    "expand_context",
    "semantic_search",
    "hybrid_search",
    "build_context",
    "build_context_at",
    "get_symbol_body",
    "explain_context",
    "index_status",
    "embedding_status",
    "system_status",
    "why_no_definition",
    "list_branches",
    "list_merge_metrics",
    "verify_audit_chain",
    "check_graph_consistency",
];
const MCP_WRITE_TOOLS: &[&str] = &[
    "write_file",
    "apply_patch",
    "reindex_paths",
    "confirm_patch",
    "revert_patch",
    "sweep_confirm_sidecars",
    "purge_branch",
    "retarget_edge",
    "save_workspace",
    "cancel_merge",
    "merge_ttl_sweep",
    "merge_branch",
    "ingest_cis_config",
    "create_branch",
    "switch_branch",
];
const MCP_DIAG_TOOLS: &[&str] = &["verify_audit_chain", "check_graph_consistency"];

/// Official MCP TypeScript SDK (Cursor) uses **newline-delimited JSON** on stdio.
/// Older / LSP-style clients use `Content-Length` framing — accept both on read.
fn read_mcp_message<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        // NDJSON: one JSON object per line (Cursor / @modelcontextprotocol/sdk).
        if trimmed.starts_with('{') {
            return Ok(Some(trimmed.as_bytes().to_vec()));
        }
        // Content-Length framing (optional headers, blank line, then body).
        let mut len: Option<usize> = None;
        let rest = trimmed
            .strip_prefix("Content-Length:")
            .or_else(|| trimmed.strip_prefix("Content-Length: "));
        if let Some(r) = rest {
            len = Some(r.trim().parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Content-Length: {e}"))
            })?);
        }
        loop {
            let mut hdr = String::new();
            if reader.read_line(&mut hdr)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF in Content-Length headers",
                ));
            }
            let hdr = hdr.trim_end_matches(['\r', '\n']);
            if hdr.is_empty() {
                break;
            }
            let rest = hdr
                .strip_prefix("Content-Length:")
                .or_else(|| hdr.strip_prefix("Content-Length: "));
            if let Some(r) = rest {
                len = Some(r.trim().parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Content-Length: {e}"))
                })?);
            }
        }
        let n = len.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing Content-Length header")
        })?;
        let mut buf = vec![0u8; n];
        reader.read_exact(&mut buf)?;
        return Ok(Some(buf));
    }
}

fn mcp_write_content_length() -> bool {
    std::env::var_os("CIS_MCP_CONTENT_LENGTH").is_some_and(|v| v == "1")
}

fn write_mcp_message<W: Write>(w: &mut W, body: &[u8]) -> io::Result<()> {
    if mcp_write_content_length() {
        write!(w, "Content-Length: {}\r\n\r\n", body.len())?;
        w.write_all(body)?;
    } else {
        // Match @modelcontextprotocol/sdk serializeMessage: JSON + '\n'
        w.write_all(body)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

/// MCP **`notifications/progress`** (**FR-4.8**).
fn send_progress_notification<W: Write>(
    w: &mut W,
    progress_token: &Value,
    progress: f64,
    total: Option<f64>,
    message: &str,
) -> io::Result<()> {
    let mut params = json!({
        "progressToken": progress_token,
        "progress": progress,
        "message": message,
    });
    if let Some(t) = total {
        params["total"] = json!(t);
    }
    let note = json!({
        "jsonrpc": "2.0",
        "method": "notifications/progress",
        "params": params,
    });
    write_mcp_message(w, &serde_json::to_vec(&note).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    })?)
}

fn progress_token_from_params(params: &Value) -> Option<Value> {
    params
        .get("_meta")
        .and_then(|m| m.get("progressToken"))
        .cloned()
}

struct McpMergeProgressSink<'a, W: Write> {
    writer: &'a mut W,
    token: Value,
}

impl<W: Write> MergeProgressSink for McpMergeProgressSink<'_, W> {
    fn on_progress(&mut self, event: &MergeProgressEvent, step: u32, total: u32) {
        let message = format!("{}: {}", event.phase, event.detail);
        let _ = send_progress_notification(
            self.writer,
            &self.token,
            f64::from(step),
            Some(f64::from(total)),
            &message,
        );
    }
}

fn parse_branch_hex(s: &str) -> Option<BranchId> {
    if s.len() != 32 {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(BranchId(b))
}

fn parse_revision_hex(s: &str) -> Option<NodeRevisionId> {
    let t = s.trim();
    if t.len() != 32 || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&t[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(NodeRevisionId(b))
}

fn parse_revision_id_list(args: &Value) -> Vec<NodeRevisionId> {
    let Some(arr) = args.get("clear_edges_for").and_then(|x| x.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|v| v.as_str().and_then(parse_revision_hex))
        .collect()
}

/// Accept `revision_id` or `revision_id_hex` (matches `find_symbol` output field names).
fn revision_id_from_args(args: &Value) -> &str {
    args.get("revision_id")
        .or_else(|| args.get("revision_id_hex"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
}

/// Accept `identity_id` or `identity_id_hex`.
fn identity_id_from_args(args: &Value) -> &str {
    args.get("identity_id")
        .or_else(|| args.get("identity_id_hex"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
}

fn symbol_from_args(args: &Value) -> &str {
    args.get("symbol")
        .or_else(|| args.get("name"))
        .or_else(|| args.get("query"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
}

fn limit_from_args(args: &Value, default: usize) -> usize {
    args.get("limit")
        .and_then(|x| x.as_u64())
        .map(|n| n as usize)
        .unwrap_or(default)
}

fn prefer_file_hub_from_args(args: &Value) -> bool {
    match args.get("prefer").and_then(|x| x.as_str()) {
        Some("file") | Some("file_hub") => true,
        _ => args
            .get("prefer_file_hub")
            .and_then(|x| x.as_bool())
            .unwrap_or(false),
    }
}

fn read_tool_schema(name: &str) -> Value {
    match name {
        "find_symbol" => json!({
            "type": "object",
            "properties": {
                "symbol": { "type": "string", "description": "Substring match on qualified_name (alias: name, query)" },
                "session_id": { "type": "integer" },
                "limit": { "type": "integer", "default": 20 },
                "prefer": { "type": "string", "description": "Set to \"file\" to rank file-hub nodes (qualified_name == file_path) first" },
                "prefer_file_hub": { "type": "boolean", "default": false },
                "branch_id": { "type": "string", "description": "32-char hex BranchId" }
            },
            "required": ["symbol"]
        }),
        "find_symbol_at" => json!({
            "type": "object",
            "properties": {
                "symbol": { "type": "string" },
                "commit_hash": { "type": "string", "description": "Git OID (40 hex) if indexed, or CIS wal_log_id (hex/decimal). Results use RIS at this anchor; graph fields are live unless materialized snapshots are enabled." },
                "session_id": { "type": "integer" },
                "limit": { "type": "integer", "default": 20 },
                "branch_id": { "type": "string" }
            },
            "required": ["symbol", "commit_hash"]
        }),
        "build_context" => json!({
            "type": "object",
            "properties": {
                "text": { "type": "string" },
                "budget_tokens": { "type": "integer", "default": 4096 },
                "session_id": { "type": "integer" }
            },
            "required": ["text"]
        }),
        "build_context_at" => json!({
            "type": "object",
            "properties": {
                "target": { "type": "string", "description": "revision id (32 hex) or qualified_name substring" },
                "commit_hash": { "type": "string", "description": "Git OID (40 hex) or wal_log_id; RIS overlay at anchor; body/graph from live store unless snapshotted." },
                "budget_tokens": { "type": "integer", "default": 4096 },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" }
            },
            "required": ["target", "commit_hash"]
        }),
        "get_symbol_body" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string", "description": "32-char hex NodeRevisionId" },
                "symbol": { "type": "string", "description": "qualified_name substring (used when revision_id omitted)" },
                "budget_tokens": { "type": "integer", "default": 4096 },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" }
            }
        }),
        "hybrid_search" => json!({
            "type": "object",
            "properties": {
                "candidates": { "type": "array", "items": { "type": "object" } },
                "vector_top_k": { "type": "integer", "default": 10 },
                "session_id": { "type": "integer" }
            },
            "required": ["candidates"]
        }),
        "go_to_definition" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string", "description": "32-char hex NodeRevisionId" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" }
            },
            "required": ["revision_id"]
        }),
        "find_references" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" },
                "limit": { "type": "integer", "default": 20 }
            },
            "required": ["revision_id"]
        }),
        "get_callers" => json!({
            "type": "object",
            "properties": {
                "identity_id": { "type": "string", "description": "32-char hex IdentityId (alias: identity_id_hex)" },
                "symbol": { "type": "string", "description": "Resolve identity from first find_symbol match when identity_id omitted" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" },
                "limit": { "type": "integer", "default": 20 }
            }
        }),
        "get_dependencies" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string", "description": "32-char hex NodeRevisionId (alias: revision_id_hex)" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" },
                "limit": { "type": "integer", "default": 4096 }
            },
            "required": ["revision_id"]
        }),
        "file_imports" => json!({
            "type": "object",
            "properties": {
                "file_path": { "type": "string", "description": "Repo-relative path, e.g. src/foo/bar.py" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" },
                "limit": { "type": "integer", "default": 256 }
            },
            "required": ["file_path"]
        }),
        "expand_context" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string", "description": "32-char hex NodeRevisionId (alias: revision_id_hex)" },
                "depth": { "type": "integer", "default": 2 },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" },
                "limit": { "type": "integer", "default": 1024 }
            },
            "required": ["revision_id"]
        }),
        "semantic_search" => json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" },
                "limit": { "type": "integer", "default": 20 }
            },
            "required": ["query"]
        }),
        "explain_context" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string", "description": "32-char hex NodeRevisionId" },
                "budget_tokens": { "type": "integer", "default": 4096 },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string" }
            },
            "required": ["revision_id"]
        }),
        "index_status" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "embedding_status" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "system_status" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "why_no_definition" => json!({
            "type": "object",
            "properties": {
                "revision_id": { "type": "string", "description": "32-char hex NodeRevisionId" },
                "session_id": { "type": "integer" },
                "branch_id": { "type": "string", "description": "32-char hex BranchId" }
            },
            "required": ["revision_id"]
        }),
        "list_branches" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "list_merge_metrics" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" },
                "limit": {
                    "type": "integer",
                    "description": "Max records to return (default 50, max 500)"
                },
                "since_ms": {
                    "type": "integer",
                    "description": "Only include merges started at or after this epoch ms"
                }
            }
        }),
        "verify_audit_chain" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "check_graph_consistency" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        _ => json!({ "type": "object", "properties": {} }),
    }
}

fn write_tool_schema(name: &str) -> Value {
    match name {
        "cancel_merge" => json!({
            "type": "object",
            "properties": {
                "merge_id": { "type": "string", "description": "32-char hex MergeId" },
                "branch_id": { "type": "string", "description": "32-char hex BranchId" },
                "clear_edges_for": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "NodeRevisionId hex list (partial graph cleanup)"
                },
                "session_id": { "type": "integer" }
            },
            "required": ["merge_id", "branch_id"]
        }),
        "merge_ttl_sweep" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "write_file" => json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Repo-relative path" },
                "content": { "type": "string" },
                "reindex": { "type": "boolean", "description": "When true (default), re-index after write when the extension is registered (py/ts/tsx/rs/go/js/…)" },
                "session_id": { "type": "integer" }
            },
            "required": ["path", "content"]
        }),
        "apply_patch" => json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "new_content": { "type": "string", "description": "Full file contents, or a unified diff (---/+++ with @@ hunks) when the payload looks like a patch" },
                "reindex": { "type": "boolean", "description": "When true (default), re-index after write when the extension is registered (py/ts/tsx/rs/go/js/…)" },
                "session_id": { "type": "integer" }
            },
            "required": ["path", "new_content"]
        }),
        "reindex_paths" => json!({
            "type": "object",
            "properties": {
                "paths": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Repo-relative source paths (py/ts/tsx/js/jsx/rs/go/java/c/cpp/cs/…); non-indexable and build-artifact paths are skipped"
                },
                "branch_id": { "type": "string", "description": "32-char hex BranchId; omit for default/main" },
                "session_id": { "type": "integer" }
            },
            "required": ["paths"]
        }),
        "confirm_patch" => json!({
            "type": "object",
            "properties": {
                "patch_id": { "type": "integer", "description": "From write_file/apply_patch response" },
                "session_id": { "type": "integer" }
            },
            "required": ["patch_id"]
        }),
        "revert_patch" => json!({
            "type": "object",
            "properties": {
                "patch_id": { "type": "integer" },
                "session_id": { "type": "integer" }
            },
            "required": ["patch_id"]
        }),
        "sweep_confirm_sidecars" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "purge_branch" => json!({
            "type": "object",
            "properties": {
                "branch_id": { "type": "string", "description": "32-char hex BranchId" },
                "session_id": { "type": "integer" }
            },
            "required": ["branch_id"]
        }),
        "retarget_edge" => json!({
            "type": "object",
            "properties": {
                "branch_id": { "type": "string", "description": "32-char hex BranchId" },
                "source_revision_id": { "type": "string", "description": "32-char hex source NodeRevisionId" },
                "edge_id": { "type": "string", "description": "32-char hex edge_id" },
                "new_target_identity_id": { "type": "string", "description": "32-char hex IdentityId to redirect to" },
                "session_id": { "type": "integer" }
            },
            "required": ["branch_id", "source_revision_id", "edge_id", "new_target_identity_id"]
        }),
        "save_workspace" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "merge_branch" => json!({
            "type": "object",
            "properties": {
                "source_branch_id": { "type": "string", "description": "32-char hex BranchId" },
                "target_branch_id": { "type": "string", "description": "32-char hex BranchId" },
                "merge_id": { "type": "string", "description": "Optional 32-char hex MergeId to continue a conflicted merge" },
                "strategy": { "type": "string", "enum": ["ours", "theirs"], "description": "Conflict resolution strategy. Omit to surface conflicts without resolving." },
                "session_id": { "type": "integer" }
            },
            "required": ["source_branch_id", "target_branch_id"]
        }),
        "ingest_cis_config" => json!({
            "type": "object",
            "properties": {
                "session_id": { "type": "integer" }
            }
        }),
        "create_branch" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "New branch name" },
                "parent": { "type": "string", "description": "Parent branch name; defaults to active branch" },
                "session_id": { "type": "integer" }
            },
            "required": ["name"]
        }),
        "switch_branch" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Branch name to activate" },
                "session_id": { "type": "integer" }
            },
            "required": ["name"]
        }),
        _ => json!({ "type": "object", "properties": {} }),
    }
}

fn read_tool_description(name: &str) -> String {
    match name {
        "find_symbol" => "Search indexed symbols by substring on qualified_name (FR-4.1). Each match includes identity_id_hex, revision_id_hex, qualified_name, file_path, and 1-based start_line/start_col/end_line/end_col. Use prefer=\"file\" to surface file-hub nodes first.".into(),
        "find_symbol_at" => "find_symbol against RIS bindings at commit_hash. NodeRevision/edges come from the LIVE in-memory graph (meta.time_travel_uses_live_graph). For FR-4.11 fidelity, re-index or narrow queries to bindings/body only.".into(),
        "build_context" => "Truncate text to a token budget (FR-3.1).".into(),
        "build_context_at" => "build_context for a symbol at commit_hash using historical RIS bindings; symbol bodies and graph traversals use LIVE graph — same limitation as find_symbol_at (see meta.time_travel_uses_live_graph).".into(),
        "get_symbol_body" => "Live symbol source text from BodyStore → .cis/bodies blob → disk span slice. Provide revision_id (32 hex) or symbol substring; no WAL/commit_hash required.".into(),
        "hybrid_search" => "Policy rerank for hybrid candidates (FR-3.5).".into(),
        "go_to_definition" => "First IMPORTS/EXTENDS/CALLS target revision (graph).".into(),
        "find_references" => "Reverse-index references to symbol (graph).".into(),
        "get_callers" => "Call edges inbound to identity (graph). Provide identity_id or symbol. Response uses hits[] with optional edge_type.".into(),
        "get_dependencies" => "IMPORTS/USES/CALLS/EXTENDS outbound from revision (graph). Input revision_id or revision_id_hex; response hits[] with edge_type.".into(),
        "file_imports" => "Import edges for a repo-relative file path via its file hub (no manual hub lookup).".into(),
        "expand_context" => "BFS neighbors over Calls/Imports/Uses. Response hits[] (not nodes).".into(),
        "semantic_search" => "Vector + structural hybrid search. When no embeddings are indexed, falls back to structural match and sets meta.degraded_reason.".into(),
        "explain_context" => "Policy axis weights and hybrid ranker snapshot for a revision (FR-4.3).".into(),
        "index_status" => "Indexing observability: symbol/edge counts, embedding coverage, watcher metrics, embedding queue depth/state/drain rates.".into(),
        "embedding_status" => "Embedding queue depth, HWM/LWM, state, and per-minute drain/embed rates (Phase 5.3).".into(),
        "system_status" => "Consolidated health: disk, quota, vector degraded, reconciliation, consistency cache, worker heartbeats (Phase 5.4).".into(),
        "why_no_definition" => "Machine-readable reasons why go_to_definition has no target for a revision.".into(),
        "list_branches" => "List registered branch names, ids, and the active branch.".into(),
        "list_merge_metrics" => "Read recent merge metrics from .cis/merge_metrics.jsonl with rollups for alerting (Phase 2.5 + 5.2).".into(),
        "verify_audit_chain" => "Admin: verify durable audit epoch hash chain in KV (Phase 3.1).".into(),
        "check_graph_consistency" => "Admin: read-only graph+KV consistency report (Phase 3.7).".into(),
        _ => format!("Read tool {name}."),
    }
}

fn write_tool_description(name: &str) -> String {
    match name {
        "cancel_merge" => "FR-4.10 / FR-1.16 rollback + lock release.".into(),
        "merge_ttl_sweep" => "Release merge locks past policy.merge_ttl_hours (§01.6.5).".into(),
        "merge_branch" => "FR-4.8: three-phase merge (classify, promote, reconcile edges) with saga tracking, rename detection, conflict resolution via strategy, crash-resume, and MCP notifications/progress when _meta.progressToken is set.".into(),
        "ingest_cis_config" => "FR-1.4: load .cis/grammar.lock and .cis/ranking_policy.yaml into the runtime.".into(),
        "write_file" => "Write file under repo root with lease + audit (FR-4.2).".into(),
        "apply_patch" => "FR-4.2: write path after optional unified diff apply, or full-file replace (lease + audit).".into(),
        "reindex_paths" => "Ingest specific repo-relative source paths on a branch overlay (any registered language: py/ts/tsx/js/jsx/rs/go/java/c/cpp/cs/…). Skips build artifacts (node_modules, target, vendor, …). Confirms pending patches for those paths and clears .cis_confirm_* sidecars. Set CIS_REINDEX_PERSIST=0 to skip writing .cis/ until save_workspace.".into(),
        "confirm_patch" => "After verifying an agent write: promote speculative revisions to Active, release leases, clear .cis_confirm_{patch_id}.".into(),
        "revert_patch" => "Discard a speculative patch: tombstone revisions, release leases, clear confirm sidecar.".into(),
        "sweep_confirm_sidecars" => "Remove orphan .cis_confirm_* files with no matching pending patch.".into(),
        "purge_branch" => "Delete ri:{branch_id}:* overlay keys from KV (feature-branch cleanup).".into(),
        "retarget_edge" => "Override an edge's target identity on a branch via ETO without rewriting the shared graph (FR §01.7.1a).".into(),
        "save_workspace" => "Persist graph, vector, and KV snapshots under .cis/; also sweeps stale confirm sidecars.".into(),
        "create_branch" => "Register a new branch and copy parent ri: bindings (Phase 1 fork bootstrap). Parent (or active default) must already be registered; unknown parent names error.".into(),
        "switch_branch" => "Set the active branch for subsequent default-branch MCP calls. Branch must already exist (create_branch first); unknown names error.".into(),
        _ => format!("Write tool {name}."),
    }
}

fn tool_definitions() -> Vec<Value> {
    let mut tools = Vec::new();
    for name in MCP_READ_TOOLS {
        tools.push(json!({
            "name": name,
            "description": read_tool_description(name),
            "inputSchema": read_tool_schema(name)
        }));
    }
    for name in MCP_WRITE_TOOLS {
        tools.push(json!({
            "name": name,
            "description": write_tool_description(name),
            "inputSchema": write_tool_schema(name)
        }));
    }
    for name in MCP_DIAG_TOOLS {
        tools.push(json!({
            "name": name,
            "description": format!("Diagnostic {name} (stub)."),
            "inputSchema": { "type": "object", "properties": {} }
        }));
    }
    tools
}

/// Resolve MCP session id. Requires `arguments.session_id` or `CIS_SESSION_ID`.
/// Legacy default `0` only when `CIS_ALLOW_DEFAULT_SESSION=1`.
fn session_id_from_args(args: &Value) -> Result<u64, &'static str> {
    if let Some(id) = args.get("session_id").and_then(|x| x.as_u64()) {
        return Ok(id);
    }
    if let Ok(s) = std::env::var("CIS_SESSION_ID") {
        if let Ok(id) = s.parse::<u64>() {
            return Ok(id);
        }
    }
    if std::env::var_os("CIS_ALLOW_DEFAULT_SESSION").is_some_and(|v| v == "1") {
        return Ok(0);
    }
    Err("session_id is required (pass arguments.session_id or set CIS_SESSION_ID; CIS_ALLOW_DEFAULT_SESSION=1 for legacy default 0)")
}

fn parse_hybrid_candidates(args: &Value) -> Vec<HybridSearchCandidate> {
    let Some(arr) = args.get("candidates").and_then(|c| c.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in arr {
        let id = item
            .get("id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let vector_score = item
            .get("vector_score")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let structural_score = item
            .get("structural_score")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let speculative = item
            .get("speculative")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        out.push(HybridSearchCandidate {
            id,
            vector_score,
            structural_score,
            speculative,
        });
    }
    out
}

fn stub_tool_body(tool: &str) -> Value {
    let mut meta = QueryMeta::default();
    meta.policy_version = "stub".into();
    json!({
        "error": "not_implemented",
        "tool": tool,
        "meta": meta
    })
}

fn wants_json_content(args: &Value) -> bool {
    if args
        .get("response_mode")
        .and_then(|x| x.as_str())
        .is_some_and(|m| m.eq_ignore_ascii_case("json"))
    {
        return true;
    }
    std::env::var("CIS_MCP_CONTENT_MODE")
        .ok()
        .map(|m| m.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
}

fn content_block(text: &str, json_mode: bool) -> Value {
    if json_mode {
        if let Ok(v) = serde_json::from_str::<Value>(text) {
            return json!([{ "type": "json", "json": v }]);
        }
    }
    json!([{ "type": "text", "text": text }])
}

fn tool_err(tool: &str, msg: impl AsRef<str>, json_mode: bool) -> Value {
    let payload = serde_json::to_string(&json!({
        "error": msg.as_ref(),
        "tool": tool
    }))
    .unwrap_or_else(|_| "{}".into());
    json!({
        "content": content_block(&payload, json_mode),
        "isError": true
    })
}

fn tool_call<W: Write>(rt: &CisMcpRuntime, params: &Value, out: &mut W) -> Value {
    let name = params.get("name").and_then(|x| x.as_str()).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    let json_mode = wants_json_content(&args);
    let mut is_error = false;
    let content_text: String = match name {
        "find_symbol" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let needle = symbol_from_args(&args);
            let limit = limit_from_args(&args, 20);
            let prefer_file = prefer_file_hub_from_args(&args);
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            match rt.find_symbol(session_id, needle, branch, limit, prefer_file) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "find_symbol_at" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let needle = args.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let limit = args.get("limit").and_then(|x| x.as_u64()).unwrap_or(20) as usize;
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let wal = match args
                .get("commit_hash")
                .and_then(|x| x.as_str())
                .ok_or(())
                .and_then(|s| resolve_commit_anchor_to_wal_log(rt.kv().as_ref(), s).map_err(|_| ()))
            {
                Ok(w) => w,
                Err(()) => {
                    return tool_err(name, "invalid commit_hash (wal log id or indexed git oid)", json_mode);
                }
            };
            match rt.find_symbol_at(session_id, needle, wal, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "build_context" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let text = args.get("text").and_then(|x| x.as_str()).unwrap_or("");
            let budget = args
                .get("budget_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(4096) as usize;
            match rt.build_context(session_id, text, budget) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "build_context_at" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let target = args.get("target").and_then(|x| x.as_str()).unwrap_or("");
            let budget = args
                .get("budget_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(4096) as usize;
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let wal = match args
                .get("commit_hash")
                .and_then(|x| x.as_str())
                .ok_or(())
                .and_then(|s| resolve_commit_anchor_to_wal_log(rt.kv().as_ref(), s).map_err(|_| ()))
            {
                Ok(w) => w,
                Err(()) => {
                    return tool_err(name, "invalid commit_hash", json_mode);
                }
            };
            match rt.build_context_at(session_id, target, wal, budget, branch) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "get_symbol_body" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev_hex = revision_id_from_args(&args);
            let revision_id = if rev_hex.is_empty() {
                None
            } else {
                Some(rev_hex)
            };
            let symbol = args.get("symbol").and_then(|x| x.as_str());
            if revision_id.is_none() && symbol.is_none() {
                return tool_err(name, "revision_id or symbol required", json_mode);
            }
            let budget = args
                .get("budget_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(4096) as usize;
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            match rt.get_symbol_body(session_id, revision_id, symbol, branch, budget) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "hybrid_search" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let k = args
                .get("vector_top_k")
                .and_then(|x| x.as_u64())
                .unwrap_or(10) as usize;
            let candidates = parse_hybrid_candidates(&args);
            match rt.hybrid_search(session_id, candidates, k) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "go_to_definition" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev = revision_id_from_args(&args);
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            match rt.go_to_definition(session_id, rev, branch) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "find_references" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev = revision_id_from_args(&args);
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let limit = limit_from_args(&args, 20);
            match rt.find_references(session_id, rev, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "get_callers" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let limit = limit_from_args(&args, 20);
            let mut id_hex = identity_id_from_args(&args).to_string();
            if id_hex.is_empty() {
                let sym = symbol_from_args(&args);
                if sym.is_empty() {
                    return tool_err(name, "identity_id or symbol required", json_mode);
                }
                match rt.find_symbol(session_id, sym, branch, 1, false) {
                    Ok(resp) => {
                        id_hex = resp
                            .matches
                            .first()
                            .map(|m| m.identity_id_hex.clone())
                            .unwrap_or_default();
                    }
                    Err(e) => return tool_err(name, &format!("{e:?}"), json_mode),
                }
                if id_hex.is_empty() {
                    return tool_err(name, "symbol not found", json_mode);
                }
            }
            match rt.get_callers(session_id, &id_hex, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "get_dependencies" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev = revision_id_from_args(&args);
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let limit = limit_from_args(&args, 4096);
            match rt.get_dependencies(session_id, rev, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "file_imports" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let file_path = args
                .get("file_path")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            if file_path.is_empty() {
                return tool_err(name, "file_path required", json_mode);
            }
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let limit = limit_from_args(&args, 256);
            match rt.file_imports(session_id, file_path, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "expand_context" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev = revision_id_from_args(&args);
            let depth = args.get("depth").and_then(|x| x.as_u64()).unwrap_or(2) as u32;
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let limit = limit_from_args(&args, 1024);
            match rt.expand_context(session_id, rev, depth, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "semantic_search" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let q = args.get("query").and_then(|x| x.as_str()).unwrap_or("");
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            let limit = args.get("limit").and_then(|x| x.as_u64()).unwrap_or(20) as usize;
            match rt.semantic_search(session_id, q, branch, limit) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "explain_context" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev = revision_id_from_args(&args);
            let budget = args
                .get("budget_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(4096) as usize;
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            match rt.explain_context(session_id, rev, budget, branch) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "index_status" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.index_status(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "embedding_status" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.embedding_status(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "system_status" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.system_status(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "why_no_definition" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let rev = revision_id_from_args(&args);
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            match rt.why_no_definition(session_id, rev, branch) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "list_branches" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.list_branches(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "list_merge_metrics" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let limit = args
                .get("limit")
                .and_then(|x| x.as_u64())
                .unwrap_or(50) as usize;
            let since_ms = args.get("since_ms").and_then(|x| x.as_u64());
            match rt.list_merge_metrics(session_id, limit, since_ms) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "verify_audit_chain" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.verify_audit_chain(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "check_graph_consistency" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.check_graph_consistency(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "write_file" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let path = args.get("path").and_then(|x| x.as_str()).unwrap_or("");
            let content = args.get("content").and_then(|x| x.as_str()).unwrap_or("");
            let reindex = args.get("reindex").and_then(|x| x.as_bool()).unwrap_or(true);
            match rt.write_file(session_id, path, content, reindex) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "apply_patch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let path = args.get("path").and_then(|x| x.as_str()).unwrap_or("");
            let new_content = args.get("new_content").and_then(|x| x.as_str()).unwrap_or("");
            let reindex = args.get("reindex").and_then(|x| x.as_bool()).unwrap_or(true);
            match rt.apply_patch(session_id, path, new_content, reindex) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "reindex_paths" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let paths: Vec<&str> = args
                .get("paths")
                .and_then(|x| x.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect()
                })
                .unwrap_or_default();
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex);
            match rt.reindex_paths(session_id, &paths, branch) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "confirm_patch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let patch_id = args.get("patch_id").and_then(|x| x.as_u64()).unwrap_or(0);
            match rt.confirm_patch(session_id, patch_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "revert_patch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let patch_id = args.get("patch_id").and_then(|x| x.as_u64()).unwrap_or(0);
            match rt.revert_patch(session_id, patch_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "sweep_confirm_sidecars" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let _ = session_id;
            let n = rt.sweep_stale_confirm_sidecars();
            match serde_json::to_string(&json!({
                "sidecars_removed": n,
                "tool": name
            })) {
                Ok(s) => s,
                Err(_) => "{}".into(),
            }
        }
        "purge_branch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            match rt.purge_branch(session_id, branch) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "retarget_edge" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let branch = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let source_rev = args
                .get("source_revision_id")
                .or_else(|| args.get("source_revision_id_hex"))
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let edge_id = args
                .get("edge_id")
                .or_else(|| args.get("edge_id_hex"))
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let new_target = args
                .get("new_target_identity_id")
                .or_else(|| args.get("new_target_identity_id_hex"))
                .and_then(|x| x.as_str())
                .unwrap_or("");
            match rt.retarget_edge(session_id, branch, source_rev, edge_id, new_target) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "save_workspace" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.save_workspace(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "cancel_merge" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let merge_hex = args.get("merge_id").and_then(|x| x.as_str()).unwrap_or("");
            let Some(branch) = args
                .get("branch_id")
                .and_then(|x| x.as_str())
                .and_then(parse_branch_hex)
            else {
                return tool_err(name, "branch_id required (32 hex)", json_mode);
            };
            let edges = parse_revision_id_list(&args);
            match rt.cancel_merge(session_id, merge_hex, branch, edges) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "merge_ttl_sweep" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.run_merge_ttl_sweep(session_id) {
                Ok(n) => serde_json::to_string(&json!({
                    "cleared_merge_locks": n,
                    "tool": name
                }))
                .unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "merge_branch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let source = args
                .get("source_branch_id")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let target = args
                .get("target_branch_id")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let strategy = args
                .get("strategy")
                .and_then(|x| x.as_str())
                .and_then(|s| match s {
                    "ours" => Some(cis_core::MergeStrategy::Ours),
                    "theirs" => Some(cis_core::MergeStrategy::Theirs),
                    _ => None,
                });
            let merge_id = args.get("merge_id").and_then(|x| x.as_str());
            let token = progress_token_from_params(params);
            let mut progress_sink = token.as_ref().map(|t| McpMergeProgressSink {
                writer: out,
                token: t.clone(),
            });
            let sink = progress_sink
                .as_mut()
                .map(|s| s as &mut dyn MergeProgressSink);
            match rt.merge_branch(session_id, source, target, strategy, merge_id, sink) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "ingest_cis_config" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            match rt.ingest_cis_config(session_id) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "create_branch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let name = args.get("name").and_then(|x| x.as_str()).unwrap_or("");
            let parent = args.get("parent").and_then(|x| x.as_str());
            match rt.create_branch(session_id, name, parent) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        "switch_branch" => {
            let session_id = match session_id_from_args(&args) {
                Ok(id) => id,
                Err(msg) => {
                    is_error = true;
                    return json!({
                        "content": content_block(
                            &serde_json::to_string(&json!({ "error": msg, "tool": name }))
                                .unwrap_or_else(|_| "{}".into()),
                            json_mode
                        ),
                        "isError": true
                    });
                }
            };
            let name = args.get("name").and_then(|x| x.as_str()).unwrap_or("");
            match rt.switch_branch(session_id, name) {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
                Err(e) => {
                    is_error = true;
                    serde_json::to_string(&json!({ "error": format!("{e:?}"), "tool": name }))
                        .unwrap_or_else(|_| "{}".into())
                }
            }
        }
        _ => {
            serde_json::to_string(&stub_tool_body(name)).unwrap_or_else(|_| "{}".into())
        }
    };

    json!({
        "content": content_block(&content_text, json_mode),
        "isError": is_error
    })
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2024-11-05",
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "cis-mcp", "version": env!("CARGO_PKG_VERSION") }
    })
}

fn handle_jsonrpc_method<W: Write>(
    rt: Option<&CisMcpRuntime>,
    slot: Option<&McpRuntimeSlot>,
    req: &Value,
    out: &mut W,
) -> Option<Value> {
    let method = req.get("method")?.as_str()?;
    if method.starts_with("notifications/") {
        return None;
    }
    let id = req.get("id").cloned();

    let result = match method {
        "initialize" => initialize_result(),
        "tools/list" => json!({ "tools": tool_definitions() }),
        "ping" => json!({}),
        "tools/call" => {
            let rt = if let Some(rt) = rt {
                rt
            } else {
                let slot = slot.expect("tools/call requires runtime or slot");
                match slot.wait(Duration::from_secs(120)) {
                    Ok(rt) => {
                        return Some(json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": tool_call(
                                rt.as_ref(),
                                req.get("params").unwrap_or(&json!({})),
                                out,
                            )
                        }));
                    }
                    Err(e) => {
                        return Some(json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "content": [{ "type": "text", "text": e }],
                                "isError": true
                            }
                        }));
                    }
                }
            };
            tool_call(rt, req.get("params").unwrap_or(&json!({})), out)
        }
        _ => {
            return Some(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": format!("method not found: {method}")
                }
            }));
        }
    };

    Some(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }))
}

fn handle_jsonrpc<W: Write>(rt: &CisMcpRuntime, req: &Value, out: &mut W) -> Option<Value> {
    handle_jsonrpc_method(Some(rt), None, req, out)
}

pub fn dump_tool_names() {
    for t in MCP_READ_TOOLS {
        println!("read:{t}");
    }
    for t in MCP_WRITE_TOOLS {
        println!("write:{t}");
    }
    for t in MCP_DIAG_TOOLS {
        println!("diag:{t}");
    }
}

/// Build MCP runtime (standalone `new_dev` unless `runtime` is passed from **`cisd --mcp`**).
pub fn build_runtime(
    runtime: Option<Arc<CisMcpRuntime>>,
) -> io::Result<Arc<CisMcpRuntime>> {
    cis_core::load_env_file(None);
    if let Some(rt) = runtime {
        bootstrap_if_needed(Arc::clone(&rt))?;
        return Ok(rt);
    }
    let repo = std::env::var("CIS_REPO_ROOT").unwrap_or_else(|_| ".".into());
    let root = std::fs::canonicalize(&repo).unwrap_or_else(|_| std::path::PathBuf::from(&repo));
    let _ = std::fs::create_dir_all(root.join(".cis"));
    eprintln!(
        "cis-mcp: repo={} persistence={}/.cis (set CIS_WAL_MEMORY=1 to disable durable WAL)",
        root.display(),
        root.display()
    );
    let rt = Arc::new(CisMcpRuntime::new_dev(&root.to_string_lossy()));
    bootstrap_if_needed(Arc::clone(&rt))?;
    Ok(rt)
}

fn bootstrap_if_needed(rt: Arc<CisMcpRuntime>) -> io::Result<()> {
    let graph_loaded = rt
        .as_ref()
        .index_status(0)
        .map(|s| s.symbols_indexed > 0)
        .unwrap_or(false);
    if graph_loaded {
        if let Ok(s) = rt.as_ref().index_status(0) {
            eprintln!(
                "cis-mcp: workspace ready ({} symbols, {} edges)",
                s.symbols_indexed, s.edges_indexed
            );
        }
    }

    let skip_index = std::env::var("CIS_MCP_SKIP_INDEX").ok().as_deref() == Some("1");
    let force_reindex = std::env::var_os("CIS_FORCE_REINDEX").is_some_and(|v| v == "1");
    if !skip_index && (!graph_loaded || force_reindex) {
        if let Err(e) = rt.as_ref().bootstrap_python_index_from_repo() {
            eprintln!("cis-mcp: index bootstrap: {:?}", e);
        }
    } else if skip_index && !graph_loaded {
        eprintln!("cis-mcp: CIS_MCP_SKIP_INDEX=1 and no graph.json — starting with empty graph");
    } else if graph_loaded && !force_reindex {
        eprintln!(
            "cis-mcp: workspace graph loaded — skipping bootstrap (set CIS_FORCE_REINDEX=1 to re-walk)"
        );
    }
    if let Err(e) = rt.as_ref().ingest_cis_config(0) {
        eprintln!("cis-mcp: ingest_cis_config: {:?}", e);
    }

    let fs_sync_off = std::env::var_os("CIS_FS_SYNC").is_some_and(|v| v == "0");
    if !fs_sync_off {
        rt.spawn_fs_sync_background();
    } else {
        eprintln!("cis-mcp: CIS_FS_SYNC=0 — background FS sync disabled");
    }
    Ok(())
}

#[cfg(test)]
mod mcp_arg_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn revision_id_accepts_hex_alias() {
        let args = json!({"revision_id_hex": "abcd1234"});
        assert_eq!(revision_id_from_args(&args), "abcd1234");
        let args2 = json!({"revision_id": "deadbeef"});
        assert_eq!(revision_id_from_args(&args2), "deadbeef");
    }

    #[test]
    fn identity_id_accepts_hex_alias() {
        let args = json!({"identity_id_hex": "aabbccdd"});
        assert_eq!(identity_id_from_args(&args), "aabbccdd");
    }

    #[test]
    fn prefer_file_hub_flag_parsing() {
        assert!(!prefer_file_hub_from_args(&json!({})));
        assert!(prefer_file_hub_from_args(&json!({"prefer": "file"})));
        assert!(prefer_file_hub_from_args(&json!({"prefer_file_hub": true})));
    }
}

/// JSON-RPC MCP loop on stdio (NDJSON by default; see module docs).
pub fn run_stdio(rt: Arc<CisMcpRuntime>) -> io::Result<()> {
    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);
    let mut stdout = io::stdout().lock();

    while let Some(msg) = read_mcp_message(&mut reader)? {
        let Ok(v) = serde_json::from_slice::<Value>(&msg) else {
            continue;
        };
        if let Some(resp) = handle_jsonrpc(rt.as_ref(), &v, &mut stdout) {
            write_mcp_message(&mut stdout, &serde_json::to_vec(&resp).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })?)?;
        }
    }
    Ok(())
}

/// Like [`run_stdio`], but answers `initialize` / `tools/list` / `ping` before workspace boot finishes.
///
/// Cursor's MCP IPC metadata timeout defaults to **10s**; CIS warm-start is often ~7s and tip over.
pub fn run_stdio_slot(slot: Arc<McpRuntimeSlot>) -> io::Result<()> {
    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);
    let mut stdout = io::stdout().lock();

    while let Some(msg) = read_mcp_message(&mut reader)? {
        let Ok(v) = serde_json::from_slice::<Value>(&msg) else {
            continue;
        };
        if let Some(resp) = handle_jsonrpc_method(None, Some(slot.as_ref()), &v, &mut stdout) {
            write_mcp_message(&mut stdout, &serde_json::to_vec(&resp).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })?)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod progress_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn progress_notification_json_shape() {
        let mut buf = Cursor::new(Vec::new());
        send_progress_notification(
            &mut buf,
            &json!("tok-1"),
            2.0,
            Some(5.0),
            "Classifying: done",
        )
        .unwrap();
        let body = String::from_utf8(buf.into_inner()).unwrap();
        assert!(body.contains("notifications/progress"));
        let line = body.lines().next().expect("ndjson line");
        let v: Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["method"], "notifications/progress");
        assert_eq!(v["params"]["progressToken"], "tok-1");
        assert_eq!(v["params"]["progress"], 2.0);
        assert_eq!(v["params"]["total"], 5.0);
    }
}
