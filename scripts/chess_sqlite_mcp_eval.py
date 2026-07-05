#!/usr/bin/env python3
"""Evaluate chess pygame MCP navigation with SQLite backends + live embeddings."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from chess_mcp_production_workflow import McpClient, timed, sep  # noqa: E402


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


def repo_root() -> Path:
    if p := os.environ.get("CIS_REPO_ROOT"):
        return Path(p).resolve()
    return (Path(__file__).resolve().parent.parent / "fixtures" / "chess_pygame").resolve()


def cisd_binary() -> Path:
    return Path(__file__).resolve().parent.parent / "target" / "debug" / "cisd"


def py_files(root: Path) -> list[str]:
    return sorted(
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if ".git" not in p.parts and ".cis" not in p.parts
    )


def print_meta(meta: dict) -> None:
    print(
        f"    meta: nodes={meta.get('node_count')} stale={meta.get('stale_count')} "
        f"degraded={meta.get('degraded_modes', [])} conf={meta.get('retrieval_confidence', 0):.3f} "
        f"latency_ms={meta.get('latency_ms', '?')}"
    )


def inspect_sqlite(cis_dir: Path) -> None:
    sep("SQLite structure (.cis/)")
    for name in sorted(cis_dir.iterdir()):
        if name.is_file():
            print(f"  {name.name}: {name.stat().st_size:,} bytes")

    graph_db = cis_dir / "graph.db"
    if graph_db.is_file():
        conn = sqlite3.connect(graph_db)
        cur = conn.cursor()
        for table in ("identities", "revisions", "edges", "graph_snapshot"):
            try:
                n = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  graph.db/{table}: {n} rows")
            except sqlite3.OperationalError:
                pass
        print("  Top modules by revision count:")
        rows = cur.execute(
            """
            SELECT file_path, COUNT(*) AS n
            FROM revisions
            GROUP BY file_path
            ORDER BY n DESC
            LIMIT 8
            """
        ).fetchall()
        for fp, n in rows:
            print(f"    {fp}: {n} symbols")
        print("  Calls edge sample (Board.py callers):")
        rows = cur.execute(
            """
            SELECT r.qualified_name, e.ty
            FROM edges e
            JOIN revisions r ON r.revision_id = e.source_revision_id
            WHERE r.file_path LIKE '%Board.py' AND e.ty = 0
            LIMIT 6
            """
        ).fetchall()
        for qn, ty in rows:
            print(f"    {qn} (ty={ty})")
        conn.close()

    store_db = cis_dir / "store.db"
    if store_db.is_file():
        conn = sqlite3.connect(store_db)
        try:
            bodies = conn.execute("SELECT COUNT(*) FROM bodies").fetchone()[0]
            print(f"  store.db/bodies: {bodies} rows")
        except sqlite3.OperationalError:
            pass
        conn.close()


def main() -> int:
    root = repo_root()
    cisd = cisd_binary()
    if not root.is_dir():
        print(f"chess repo missing: {root}", file=sys.stderr)
        return 1
    if not cisd.is_file():
        print("Build cisd first", file=sys.stderr)
        return 1

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    env = os.environ.copy()
    env.update({
        "CIS_REPO_ROOT": str(root),
        "CIS_WAL_MEMORY": "1",
        "CIS_FS_SYNC": "0",
        "CIS_FORCE_REINDEX": "1",
        "CIS_REINDEX_PERSIST": "0",
        "CIS_BODY_BACKEND": "sqlite",
        "CIS_METADATA_BACKEND": "sqlite",
        "CIS_GRAPH_BACKEND": "sqlite",
        "CIS_GRAPH_JSON_EXPORT": "0",
    })

    subprocess.run(["pkill", "-f", str(cisd)], check=False, capture_output=True)
    time.sleep(0.3)

    sep("Chess pygame — SQLite + live embeddings MCP eval")
    print(f"Repo: {root}")
    print(f"Embed: {env.get('CIS_EMBED_API_URL', '(stub)')} model={env.get('CIS_EMBED_MODEL', '?')}")

    proc = subprocess.Popen(
        [str(cisd), "--mcp"],
        cwd=str(root),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        start_new_session=True,
    )
    client = McpClient(proc)
    results: dict[str, Any] = {"timings_ms": {}, "quality": {}}

    try:
        timed("initialize", lambda: client.request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "chess_sqlite_eval", "version": "1.0"},
        }))

        paths = py_files(root)
        rep = timed("reindex_paths (full .py walk)", lambda: client.tool(
            "reindex_paths", {"paths": paths}
        ))
        print(f"  applied={rep['applied']} parse_errors={rep.get('parse_errors', 0)} files={len(paths)}")

        print("  waiting for embedding worker …", flush=True)
        for i in range(30):
            time.sleep(2)
            idx = client.tool("index_status")
            emb = idx.get("embeddings_indexed", 0)
            ann = idx.get("ann_index_size", 0)
            print(f"    t+{(i+1)*2}s: embeddings={emb} ann={ann}", flush=True)
            if emb >= idx["symbols_indexed"] * 0.8:
                break

        idx = timed("index_status (final)", lambda: client.tool("index_status"))
        results["index_status"] = idx
        print(
            f"  symbols={idx['symbols_indexed']} edges={idx['edges_indexed']} "
            f"embeddings={idx.get('embeddings_indexed', '?')} "
            f"ann={idx.get('ann_index_size', '?')} "
            f"model={idx.get('embed_model_id', '?')}"
        )

        sep("Structural queries")
        find = timed("find_symbol(getStrPosition)", lambda: client.tool(
            "find_symbol", {"symbol": "getStrPosition", "limit": 8}
        ))
        print_meta(find["meta"])
        for m in find["matches"][:3]:
            print(f"    {m['qualified_name']} @ {m['file_path']}:{m['start_line']}")
        results["quality"]["find_getStrPosition"] = [m["qualified_name"] for m in find["matches"][:3]]

        board_init = timed("find_symbol(initialize)", lambda: client.tool(
            "find_symbol", {"symbol": "initialize", "limit": 10}
        ))
        board_hits = [m for m in board_init["matches"] if "Board.py" in m["file_path"]]
        print(f"  Board.initialize hits: {len(board_hits)}")
        results["quality"]["board_initialize"] = [m["qualified_name"] for m in board_hits[:2]]

        sep("Semantic search (live MiniLM embeddings)")
        queries = [
            ("convert board coordinates to chess notation", ["getStrPosition", "BoardUtils"]),
            ("calculate legal moves for a piece", ["calculateMoves", "Board"]),
            ("handle mouse click on chess tile", ["on_click", "ChessTile", "Screen"]),
            ("computer AI chooses best move", ["AI", "ComputerPlayer"]),
        ]
        for query, expect_substrings in queries:
            sem = timed(f'semantic_search("{query[:45]}…")', lambda q=query: client.tool(
                "semantic_search", {"query": q, "limit": 5}
            ))
            print_meta(sem["meta"])
            hits = sem["hits"][:3]
            for h in hits:
                print(f"    score={h['score']:.4f} {h['qualified_name']} rev={h['revision_id_hex'][:8]}…")
            top = hits[0]["qualified_name"] if hits else ""
            matched = any(s.lower() in top.lower() for s in expect_substrings) if hits else False
            results["quality"][f"semantic:{query[:30]}"] = {
                "top": top,
                "score": hits[0]["score"] if hits else 0,
                "relevant": matched,
            }
            print(f"    relevance_ok={matched}")

        sep("Graph traversal")
        if find["matches"]:
            rev = find["matches"][0]["revision_id_hex"]
            refs = timed("find_references(getStrPosition)", lambda: client.tool(
                "find_references", {"revision_id": rev, "limit": 12}
            ))
            print_meta(refs["meta"])
            for h in refs["hits"][:5]:
                print(f"    {h['qualified_name']} @ {h['file_path']}")
            results["quality"]["references_count"] = len(refs["hits"])

        calc = client.tool("find_symbol", {"symbol": "calculateMoves", "limit": 5})
        board_calc = next((m for m in calc["matches"] if "Board.py" in m["file_path"]), None)
        if board_calc:
            exp = timed("expand_context(Board.calculateMoves, depth=2)", lambda: client.tool(
                "expand_context",
                {"revision_id": board_calc["revision_id_hex"], "depth": 2},
            ))
            print_meta(exp["meta"])
            for h in exp["hits"][:8]:
                print(f"    {h['qualified_name']} @ {h['file_path']}")
            results["quality"]["expand_context_hits"] = len(exp["hits"])

        sep("Persist workspace to SQLite")
        sw = timed("save_workspace", lambda: client.tool("save_workspace"))
        cis_dir = Path(sw["cis_dir"])
        print(f"  saved: {cis_dir}")
        inspect_sqlite(cis_dir)

        sep("Summary")
        rel = sum(1 for k, v in results["quality"].items() if k.startswith("semantic:") and v.get("relevant"))
        sem_total = sum(1 for k in results["quality"] if k.startswith("semantic:"))
        print(f"  Semantic relevance: {rel}/{sem_total} queries matched expected symbols")
        print(f"  Index: {idx['symbols_indexed']} symbols, {idx['edges_indexed']} edges, "
              f"{idx.get('embeddings_indexed', 0)} embeddings")
        return 0

    finally:
        client.close()
        err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        if err:
            print("\n--- cisd stderr (tail) ---", file=sys.stderr)
            print(err[-3000:], file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
