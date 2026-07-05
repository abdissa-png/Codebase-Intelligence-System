#!/usr/bin/env python3
"""
Production-style CIS eval on _tmp_chess_pygame via **`cisd --mcp`** (single process: daemon + MCP stdio).

Prerequisites:
  cargo build -p cis-mcp --features python-ast,fs-notify

Usage (from cis repo root):
  python3 scripts/chess_mcp_production_workflow.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

MAIN = "00000000000000000000000000000000"
FEATURE_A = "02020202020202020202020202020202"
FEATURE_B = "03030303030303030303030303030303"
REL_BOARD = "BoardUtils.py"
NEEDLE = "    x,y=position[0],position[1]\n    return str(x)+COLUMNS[y]"


def repo_root() -> Path:
    script = Path(__file__).resolve()
    return (script.parent.parent.parent / "_tmp_chess_pygame").resolve()


def cisd_binary() -> Path:
    manifest = Path(__file__).resolve().parent.parent
    return manifest / "target" / "debug" / "cisd"


class McpClient:
    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        self.proc = proc
        self._id = 0
        self._buf = b""

    def _read_message(self) -> dict[str, Any]:
        while b"\r\n\r\n" not in self._buf:
            chunk = self.proc.stdout.read(1)
            if not chunk:
                raise RuntimeError("cis-mcp closed stdout")
            self._buf += chunk
        header, rest = self._buf.split(b"\r\n\r\n", 1)
        self._buf = rest
        length = None
        for line in header.decode("ascii", errors="replace").split("\r\n"):
            if line.lower().startswith("content-length:"):
                length = int(line.split(":", 1)[1].strip())
        if length is None:
            raise RuntimeError(f"missing Content-Length in header: {header!r}")
        while len(self._buf) < length:
            chunk = self.proc.stdout.read(length - len(self._buf))
            if not chunk:
                raise RuntimeError("cis-mcp closed while reading body")
            self._buf += chunk
        body = self._buf[:length]
        self._buf = self._buf[length:]
        return json.loads(body.decode("utf-8"))

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self._id += 1
        msg: dict[str, Any] = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params is not None:
            msg["params"] = params
        body = json.dumps(msg).encode("utf-8")
        header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
        assert self.proc.stdin is not None
        self.proc.stdin.write(header)
        self.proc.stdin.write(body)
        self.proc.stdin.flush()
        resp = self._read_message()
        if "error" in resp:
            raise RuntimeError(f"MCP {method} error: {resp['error']}")
        return resp.get("result")

    def tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        result = self.request(
            "tools/call", {"name": name, "arguments": arguments or {}}
        )
        if result.get("isError"):
            text = result["content"][0]["text"]
            raise RuntimeError(f"tool {name} failed: {text}")
        text = result["content"][0]["text"]
        return json.loads(text)

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def sep(title: str) -> None:
    print("\n" + "=" * 72, flush=True)
    print(f"  {title}", flush=True)
    print("=" * 72, flush=True)


def timed(label: str, fn) -> Any:
    t0 = time.perf_counter()
    out = fn()
    ms = (time.perf_counter() - t0) * 1000
    print(f"  [{ms:.1f}ms] {label}", flush=True)
    return out


def main() -> int:
    root = repo_root()
    if not root.is_dir():
        print(f"skip: chess repo missing at {root}", file=sys.stderr)
        return 1

    cisd_path = cisd_binary()
    if not cisd_path.is_file():
        print(
            "Build binary first:\n"
            "  cargo build -p cis-mcp --features python-ast,fs-notify",
            file=sys.stderr,
        )
        return 1

    policy = root / ".cis" / "ranking_policy.yaml"
    env = os.environ.copy()
    env["CIS_REPO_ROOT"] = str(root)
    env["CIS_WAL_MEMORY"] = "1"
    env["CIS_MCP_SKIP_INDEX"] = "1"
    env["CIS_FS_SYNC"] = "0"
    # Defer .cis/ snapshots during reindex_paths; save_workspace persists once at the end.
    env["CIS_REINDEX_PERSIST"] = "0"
    if policy.is_file():
        env["CIS_POLICY_PATH"] = str(policy)

    sep("CIS MCP production workflow — _tmp_chess_pygame")
    print(f"Repo: {root}")
    print(f"Branches: main={MAIN} feature_a={FEATURE_A} feature_b={FEATURE_B}")

    # Avoid stale daemons from prior runs contending on `.cis/`.
    subprocess.run(["pkill", "-f", str(cisd_path)], check=False, capture_output=True)
    time.sleep(0.2)

    sep("Start cisd --mcp (single process: policy TTL + vector cleanup + MCP stdio)")
    mcp_proc = subprocess.Popen(
        [str(cisd_path), "--mcp"],
        cwd=str(root),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=0,
        start_new_session=True,
    )
    print(f"  cisd --mcp pid={mcp_proc.pid}")
    client = McpClient(mcp_proc)

    try:
        print("  waiting for cisd --mcp ready (load .cis)…", flush=True)
        timed(
            "initialize",
            lambda: client.request(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "chess_mcp_workflow", "version": "1.0"},
                },
            ),
        )

        idx = timed("index_status", lambda: client.tool("index_status"))
        print(
            f"  symbols={idx['symbols_indexed']} edges={idx['edges_indexed']} "
            f"files={idx['files_scanned']} mode={idx['ingest_mode']}"
        )

        sep("Query battery (MCP read tools)")
        find = timed(
            "find_symbol(getStrPosition)",
            lambda: client.tool("find_symbol", {"symbol": "getStrPosition", "limit": 10}),
        )
        print(f"  hits={len(find['matches'])} conf={find['meta']['retrieval_confidence']:.3f}")
        for m in find["matches"][:3]:
            print(
                f"    {m['qualified_name']} @ {m['file_path']}:{m['start_line']} "
                f"conf={m['confidence']:.3f}"
            )
        board_rev = next(
            m["revision_id_hex"]
            for m in find["matches"]
            if "BoardUtils" in m["file_path"]
        )

        sem = timed(
            "semantic_search(BoardUtils)",
            lambda: client.tool("semantic_search", {"query": "BoardUtils", "limit": 8}),
        )
        print(f"  semantic hits={len(sem['hits'])}")
        for h in sem["hits"][:3]:
            print(f"    score={h['score']:.3f} {h['qualified_name']}")

        refs = timed(
            "find_references(getStrPosition)",
            lambda: client.tool("find_references", {"revision_id": board_rev, "limit": 20}),
        )
        print(f"  references={len(refs['hits'])}")
        for h in refs["hits"][:3]:
            print(f"    {h['qualified_name']} @ {h['file_path']}")

        calc = client.tool("find_symbol", {"symbol": "calculateMoves", "limit": 5})
        caller = next((m for m in calc["matches"] if "Board.py" in m["file_path"]), None)
        if caller:
            exp = timed(
                "expand_context(Board.calculateMoves)",
                lambda: client.tool(
                    "expand_context",
                    {"revision_id": caller["revision_id_hex"], "depth": 1},
                ),
            )
            names = [h["qualified_name"] for h in exp["hits"]]
            print(f"  expand_context hits={len(exp['hits'])} neighbors={names!r}")

        gtd = timed(
            "go_to_definition",
            lambda: client.tool("go_to_definition", {"revision_id": board_rev}),
        )
        target = gtd.get("target")
        print(
            "  go_to_definition target="
            + (target["qualified_name"] if target else "(none)")
        )

        original = (root / REL_BOARD).read_text(encoding="utf-8")
        if NEEDLE not in original:
            print("skip merge: BoardUtils.py layout changed", file=sys.stderr)
            return 0

        edit_a = original.replace(
            NEEDLE,
            "    x,y=position[0],position[1]\n    # cis-feature-a\n    return str(x)+COLUMNS[y]",
        )
        edit_b = original.replace(
            NEEDLE,
            "    x,y=position[0],position[1]\n    # cis-feature-b\n    return str(x)+COLUMNS[y]",
        )

        sep("Feature branch A — apply_patch + reindex_paths (MCP)")
        timed(
            "apply_patch feature_a disk",
            lambda: client.tool(
                "apply_patch",
                {"path": REL_BOARD, "new_content": edit_a, "reindex": False},
            ),
        )
        rep_a = timed(
            "reindex_paths feature_a",
            lambda: client.tool(
                "reindex_paths",
                {"paths": [REL_BOARD], "branch_id": FEATURE_A},
            ),
        )
        print(
            f"  applied={rep_a['applied']} patches_confirmed={rep_a.get('patches_confirmed', 0)} "
            f"persisted={rep_a.get('persisted_snapshots', True)}"
        )

        sep("Feature branch B — apply_patch + reindex_paths (MCP)")
        timed(
            "apply_patch feature_b disk",
            lambda: client.tool(
                "apply_patch",
                {"path": REL_BOARD, "new_content": edit_b, "reindex": False},
            ),
        )
        rep_b = timed(
            "reindex_paths feature_b",
            lambda: client.tool(
                "reindex_paths",
                {"paths": [REL_BOARD], "branch_id": FEATURE_B},
            ),
        )
        print(
            f"  applied={rep_b['applied']} patches_confirmed={rep_b.get('patches_confirmed', 0)} "
            f"persisted={rep_b.get('persisted_snapshots', True)}"
        )

        sep("Merge feature_a → main (merge_branch MCP)")
        merge = timed(
            "merge_branch",
            lambda: client.tool(
                "merge_branch",
                {
                    "source_branch_id": FEATURE_A,
                    "target_branch_id": MAIN,
                    "strategy": "theirs",
                },
            ),
        )
        print(
            f"  saga={merge['saga_phase']} promoted={merge['promoted_count']} "
            f"edges_regen={merge['edges_regenerated']}"
        )
        if merge["saga_phase"] != "Committed":
            raise RuntimeError(f"merge not committed: {merge}")

        post = timed(
            "find_symbol after merge",
            lambda: client.tool("find_symbol", {"symbol": "getStrPosition", "limit": 5}),
        )
        print(f"  post-merge hits={len(post['matches'])}")

        sep("Cleanup — restore main, purge branches, save_workspace")
        timed(
            "apply_patch restore",
            lambda: client.tool(
                "apply_patch",
                {"path": REL_BOARD, "new_content": original, "reindex": False},
            ),
        )
        pa = timed("purge_branch feature_a", lambda: client.tool("purge_branch", {"branch_id": FEATURE_A}))
        pb = timed("purge_branch feature_b", lambda: client.tool("purge_branch", {"branch_id": FEATURE_B}))
        print(f"  purged keys: a={pa['keys_deleted']} b={pb['keys_deleted']}")
        rep_main = timed(
            "reindex_paths main",
            lambda: client.tool("reindex_paths", {"paths": [REL_BOARD]}),
        )
        print(
            f"  main reindex applied={rep_main['applied']} "
            f"patches_confirmed={rep_main.get('patches_confirmed', 0)} "
            f"persisted={rep_main.get('persisted_snapshots', True)}"
        )
        sw = timed("save_workspace", lambda: client.tool("save_workspace"))
        print(
            f"  persisted to {sw['cis_dir']} "
            f"(stale_confirm_sidecars_removed={sw.get('stale_confirm_sidecars_removed', 0)})"
        )

        sep("Summary")
        print("  cisd --mcp unified workflow: OK")
        print("  All steps used MCP tools/call (stdio JSON-RPC)")
        print('  CIS "main" = BranchId zeros (git stays on master)')
        return 0

    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
