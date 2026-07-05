#!/usr/bin/env bash
# Create .venv-embed and install dependencies for scripts/local_embed_server.py.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV="${CIS_EMBED_VENV:-$ROOT/.venv-embed}"
REQ="$ROOT/scripts/requirements-embed.txt"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 not found (Python 3.10+ recommended)" >&2
  exit 1
fi

PY_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
echo "Using python3 ($PY_VERSION) at $(command -v python3)"

if [[ ! -d "$VENV" ]]; then
  echo "Creating virtual environment at $VENV"
  python3 -m venv "$VENV"
else
  echo "Virtual environment already exists at $VENV"
fi

"$VENV/bin/python" -m pip install --upgrade pip
"$VENV/bin/pip" install -r "$REQ"

echo ""
echo "Done. Next steps:"
echo ""
echo "  1. Start the local embed server (terminal 1):"
echo "       ./scripts/run_embed_server.sh"
echo ""
echo "  2. Copy embed settings into .env (repo root):"
echo "       cp .env.example .env"
echo ""
echo "  3. Build CIS with api-embeddings and verify (terminal 2):"
echo "       cargo build -p cis-mcp --features api-embeddings --bin cis-embed-smoke"
echo "       cargo run -p cis-mcp --features api-embeddings --bin cis-embed-smoke"
