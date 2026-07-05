#!/usr/bin/env python3
"""Read-only MCP navigation demo on _tmp_chess_pygame (structural + semantic + graph).

Usage (from cis repo root):
  # terminal 1
  ./scripts/run_embed_server.sh

  # terminal 2
  cargo build -p cis-mcp --features api-embeddings,python-ast,fs-notify
  python3 scripts/chess_navigation_demo.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# Reuse MCP client from production workflow.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from chess_mcp_production_workflow import McpClient, cisd_binary, repo_root, sep, timed  # noqa: E402


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def py_files(root: Path) -> list[str]:
    return sorted(
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if ".git" not in p.parts and ".cis" not in p.parts
    )


def print_meta(tool: str, meta: dict) -> None:
    print(
        f"  meta: nodes={meta.get('node_count')} stale={meta.get('stale_count')} "
        f"degraded={meta.get('degraded_modes', [])} conf={meta.get('retrieval_confidence', 0):.3f}"
    )


def main() -> int:
    root = repo_root()
    if not root.is_dir():
        print(f"chess repo missing: {root}", file=sys.stderr)
        return 1

    cisd_path = cisd_binary()
    if not cisd_path.is_file():
        print(
            "Build first:\n"
            "  cargo build -p cis-mcp --features api-embeddings,python-ast,fs-notify",
            file=sys.stderr,
        )
        return 1

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    env = os.environ.copy()
    env["CIS_REPO_ROOT"] = str(root)
    env["CIS_WAL_MEMORY"] = "1"
    env["CIS_FS_SYNC"] = "0"
    env["CIS_REINDEX_PERSIST"] = "0"
    policy = root / ".cis" / "ranking_policy.yaml"
    if policy.is_file():
        env["CIS_POLICY_PATH"] = str(policy)

    subprocess.run(["pkill", "-f", str(cisd_path)], check=False, capture_output=True)
    time.sleep(0.2)

    sep("Chess pygame — MCP navigation demo (embeddings + graph)")
    print(f"Repo: {root}")
    print(f"Embed: {env.get('CIS_EMBED_API_URL', '(stub)')} model={env.get('CIS_EMBED_MODEL', '?')}")

    proc = subprocess.Popen(
        [str(cisd_path), "--mcp"],
        cwd=str(root),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        start_new_session=True,
    )
    client = McpClient(proc)

    try:
        timed("initialize", lambda: client.request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "chess_nav_demo", "version": "1.0"},
        }))

        sep("Reindex all Python (populate graph + embedding queue)")
        paths = py_files(root)
        rep = timed("reindex_paths (all .py)", lambda: client.tool(
            "reindex_paths", {"paths": paths}
        ))
        print(f"  applied={rep['applied']} files={len(paths)}")

        # Let cisd embedding worker drain queue.
        print("  waiting 8s for embedding worker …", flush=True)
        time.sleep(8)

        idx = timed("index_status", lambda: client.tool("index_status"))
        print(
            f"  symbols={idx['symbols_indexed']} edges={idx['edges_indexed']} "
            f"mode={idx['ingest_mode']}"
        )

        sep("1. find_symbol — exact name lookup (structural)")
        find = timed("find_symbol(getStrPosition)", lambda: client.tool(
            "find_symbol", {"symbol": "getStrPosition", "limit": 8}
        ))
        print_meta("find_symbol", find["meta"])
        for m in find["matches"][:4]:
            print(f"    {m['qualified_name']} @ {m['file_path']}:{m['start_line']} conf={m['confidence']:.3f}")
        board_utils_rev = next(
            (m["revision_id_hex"] for m in find["matches"] if "BoardUtils" in m["file_path"]),
            find["matches"][0]["revision_id_hex"] if find["matches"] else None,
        )

        sep("2. semantic_search — conceptual discovery (vector + structural)")
        for query in ["convert board position to chess notation", "calculate legal moves", "computer AI player"]:
            sem = timed(f'semantic_search("{query[:40]}…")', lambda q=query: client.tool(
                "semantic_search", {"query": q, "limit": 5}
            ))
            print_meta("semantic_search", sem["meta"])
            for h in sem["hits"][:3]:
                print(f"    score={h['score']:.4f} {h['qualified_name']}")

        sep("3. find_references — who calls getStrPosition?")
        if board_utils_rev:
            refs = timed("find_references", lambda: client.tool(
                "find_references", {"revision_id": board_utils_rev, "limit": 15}
            ))
            print_meta("find_references", refs["meta"])
            for h in refs["hits"][:6]:
                print(f"    {h['qualified_name']} @ {h['file_path']} conf={h['confidence']:.3f}")

        sep("4. get_callers — inbound call graph")
        if find["matches"]:
            iid = find["matches"][0]["identity_id_hex"]
            callers = timed("get_callers", lambda: client.tool(
                "get_callers", {"identity_id": iid, "limit": 10}
            ))
            print_meta("get_callers", callers["meta"])
            for h in callers["hits"][:6]:
                print(f"    {h['qualified_name']} @ {h['file_path']}")

        sep("5. expand_context — 2-hop neighborhood (confidence-pruned BFS)")
        calc = client.tool("find_symbol", {"symbol": "calculateMoves", "limit": 5})
        board_calc = next((m for m in calc["matches"] if "Board.py" in m["file_path"]), None)
        if board_calc:
            exp = timed("expand_context(depth=2)", lambda: client.tool(
                "expand_context",
                {"revision_id": board_calc["revision_id_hex"], "depth": 2},
            ))
            print_meta("expand_context", exp["meta"])
            for h in exp["hits"][:10]:
                print(f"    {h['qualified_name']} @ {h['file_path']} conf={h['confidence']:.3f}")

        sep("6. get_dependencies + go_to_definition — follow an edge")
        if board_calc:
            deps = timed("get_dependencies", lambda: client.tool(
                "get_dependencies",
                {"revision_id": board_calc["revision_id_hex"], "limit": 10},
            ))
            print_meta("get_dependencies", deps["meta"])
            for h in deps["hits"][:5]:
                print(f"    dep: {h['qualified_name']} conf={h['confidence']:.3f}")
            if deps["hits"]:
                gtd = timed("go_to_definition", lambda: client.tool(
                    "go_to_definition",
                    {"revision_id": deps["hits"][0]["revision_id_hex"]},
                ))
                t = gtd.get("target")
                print(f"    go_to_definition → {t['qualified_name'] if t else '(none)'}")

        sep("7. explain_context — combined narrative")
        if board_utils_rev:
            expl = timed("explain_context", lambda: client.tool(
                "explain_context", {"revision_id": board_utils_rev, "depth": 2}
            ))
            print_meta("explain_context", expl["meta"])
            print(f"  counts: {json.dumps(expl.get('counts', {}), indent=2)}")

        sep("Done — MCP server exercised read-only navigation")
        sw = timed("save_workspace", lambda: client.tool("save_workspace"))
        print(f"  snapshots saved to {sw['cis_dir']}")
        return 0

    finally:
        client.close()
        err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        if err:
            print("\n--- cisd stderr (tail) ---", file=sys.stderr)
            print(err[-2000:], file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
