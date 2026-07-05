#!/usr/bin/env bash
# Minimal CIS MCP stdio client for exploration from bash.
# Usage:
#   export CIS_REPO_ROOT=/path/to/repo
#   ./scripts/mcp_explore.sh index_status
#   ./scripts/mcp_explore.sh find_symbol '{"symbol":"foo","prefer":"file","limit":5}'
set -euo pipefail

TOOL="${1:?tool name required}"
ARGS="${2:-\{\}}"
CIS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CISD="${CIS_DIR}/target/debug/cisd"

if [[ ! -x "$CISD" ]]; then
  echo "cisd not found at $CISD — run: cargo build -p cis-mcp --bin cisd" >&2
  exit 1
fi

export CIS_FS_SYNC="${CIS_FS_SYNC:-0}"
export CIS_MCP_SKIP_INDEX="${CIS_MCP_SKIP_INDEX:-1}"

COPROC_OUT="" COPROC_IN="" COPROC_PID=""
send_mcp() { printf 'Content-Length: %d\r\n\r\n%s' "${#1}" "$1" >&${COPROC_IN}; }
read_mcp() {
  local length="" line
  while IFS= read -r -u "${COPROC_OUT}" line; do
    line="${line//$'\r'/}"
    [[ -z "$line" ]] && break
    [[ "$line" =~ ^[Cc]ontent-[Ll]ength:[[:space:]]*([0-9]+)$ ]] && length="${BASH_REMATCH[1]}"
  done
  dd bs=1 count="$length" status=none <&"${COPROC_OUT}"
}

coproc CISDMCP { "$CISD" --mcp 2>/dev/null; }
COPROC_PID=$COPROC_PID
COPROC_OUT=${CISDMCP[0]}
COPROC_IN=${CISDMCP[1]}

send_mcp '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"mcp_explore.sh","version":"1"}}}'
read_mcp >/dev/null
send_mcp '{"jsonrpc":"2.0","method":"notifications/initialized"}'

REQ=$(jq -nc --arg tool "$TOOL" --argjson args "$ARGS" \
  '{jsonrpc:"2.0", id:2, method:"tools/call", params:{name:$tool, arguments:$args}}')
send_mcp "$REQ"
RESP=$(read_mcp)
kill "$COPROC_PID" 2>/dev/null || true

echo "$RESP" | jq -r '.result.content[0].text // .result // .error' | jq '.' 2>/dev/null || echo "$RESP" | jq '.'
