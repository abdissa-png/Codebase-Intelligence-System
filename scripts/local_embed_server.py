#!/usr/bin/env python3
"""Minimal OpenAI-compatible /v1/embeddings server for local CIS testing.

Setup (from cis repo root):

    ./scripts/setup_embed_venv.sh

Run:

    ./scripts/run_embed_server.sh
    # listens on http://127.0.0.1:8080

Then copy embed settings from .env.example into .env:

    CIS_EMBED_API_URL=http://127.0.0.1:8080/v1
    CIS_EMBED_MODEL=all-MiniLM-L6-v2
    CIS_EMBED_DIM=384

Verify:

    curl -X POST http://127.0.0.1:8080/v1/embeddings \\
      -H 'Content-Type: application/json' \\
      -d '{"model":"all-MiniLM-L6-v2","input":"Hello world"}'

    cargo run -p cis-mcp --features api-embeddings --bin cis-embed-smoke
"""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

try:
    from fastembed import TextEmbedding
except ImportError:
    print(
        "fastembed not installed. From the cis repo root run:\n"
        "  ./scripts/setup_embed_venv.sh\n"
        "  ./scripts/run_embed_server.sh",
        file=sys.stderr,
    )
    raise

HOST = "127.0.0.1"
PORT = 8080
MODEL_ID = "all-MiniLM-L6-v2"
MODEL_PATH = "sentence-transformers/all-MiniLM-L6-v2"

print(f"Loading {MODEL_PATH} …", flush=True)
EMBEDDER = TextEmbedding(MODEL_PATH)
# Probe dim once.
DIM = len(list(EMBEDDER.embed(["probe"]))[0])
print(f"Ready: model={MODEL_ID} dim={DIM} url=http://{HOST}:{PORT}/v1/embeddings", flush=True)


def embed_texts(texts: list[str]) -> list[list[float]]:
    return [vec.tolist() for vec in EMBEDDER.embed(texts)]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_GET(self) -> None:
        if self.path in ("/healthz", "/health"):
            self._json(200, {"status": "ok"})
        elif self.path == "/v1/models":
            self._json(
                200,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": MODEL_ID,
                            "object": "model",
                            "embedding_dim": DIM,
                            "loaded": True,
                        }
                    ],
                },
            )
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/v1/embeddings":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as e:
            self._json(400, {"error": {"message": str(e)}})
            return

        model = body.get("model", MODEL_ID)
        inp = body.get("input")
        if isinstance(inp, str):
            texts = [inp]
        elif isinstance(inp, list):
            texts = [str(x) for x in inp]
        else:
            self._json(400, {"error": {"message": "input must be string or array"}})
            return

        vectors = embed_texts(texts)
        self._json(
            200,
            {
                "object": "list",
                "model": model,
                "data": [
                    {"object": "embedding", "index": i, "embedding": v}
                    for i, v in enumerate(vectors)
                ],
            },
        )

    def _json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)


if __name__ == "__main__":
    main()
