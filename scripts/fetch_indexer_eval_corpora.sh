#!/usr/bin/env bash
# Clone pinned real-world trees used by the indexer corpus evaluator.
# Output: cis/fixtures/indexer_eval/<name>/
#
# Usage (from cis/):
#   ./scripts/fetch_indexer_eval_corpora.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${CIS_INDEXER_EVAL_DIR:-$ROOT/fixtures/indexer_eval}"
mkdir -p "$DEST"

clone() {
  local name="$1"
  local url="$2"
  local ref="$3"
  local dir="$DEST/$name"
  if [[ -d "$dir/.git" || -f "$dir/.cis-eval-ok" ]]; then
    echo "skip $name (already present)"
    return 0
  fi
  echo "clone $name ($ref)"
  rm -rf "$dir"
  if git clone --depth 1 --branch "$ref" "$url" "$dir"; then
    touch "$dir/.cis-eval-ok"
  else
    echo "retry $name on default branch"
    rm -rf "$dir"
    git clone --depth 1 "$url" "$dir" || {
      echo "WARN: failed to clone $name" >&2
      rm -rf "$dir"
      return 0
    }
    touch "$dir/.cis-eval-ok"
  fi
}

# Compact, well-known libraries — enough surface for recall, small enough for CI.
clone gorilla-mux  https://github.com/gorilla/mux.git           v1.8.1
clone underscore   https://github.com/jashkenas/underscore.git  1.13.7
clone redux        https://github.com/reduxjs/redux.git         v5.0.1
clone zlib         https://github.com/madler/zlib.git           v1.3.1
clone fmt          https://github.com/fmtlib/fmt.git            11.0.2
clone gson         https://github.com/google/gson.git           gson-parent-2.11.0
clone dapper       https://github.com/DapperLib/Dapper.git      2.1.35

echo "corpora ready under $DEST"
ls -1 "$DEST"
