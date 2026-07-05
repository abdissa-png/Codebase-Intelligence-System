#!/usr/bin/env bash
# Run the local OpenAI-compatible embedding server on http://127.0.0.1:8080
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV="${CIS_EMBED_VENV:-$ROOT/.venv-embed}"
PYTHON="$VENV/bin/python"
SERVER="$ROOT/scripts/local_embed_server.py"

if [[ ! -x "$PYTHON" ]]; then
  echo "error: $VENV not found. Run ./scripts/setup_embed_venv.sh first." >&2
  exit 1
fi

if ! "$PYTHON" -c "import fastembed" 2>/dev/null; then
  echo "error: fastembed not installed in $VENV. Run ./scripts/setup_embed_venv.sh" >&2
  exit 1
fi

exec "$PYTHON" "$SERVER" "$@"
