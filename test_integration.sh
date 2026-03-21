#!/bin/zsh
# Integration test for BlitzDB.
# Usage: ./test_integration.sh
# Requires: cargo, uv (for pyarrow)

set -exuo pipefail

PASS=0
FAIL=0
TMPDIR_LOCAL=$(mktemp -d)
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TMPDIR_LOCAL"
}
trap cleanup EXIT

# Colours
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC} $1"; PASS=$((PASS + 1)); }
fail() { echo -e "${RED}FAIL${NC} $1"; ((FAIL++)); }

# ── 1. Build ──────────────────────────────────────────────────────────────────
echo "==> Building workspace..."
cargo build -q --workspace 2>&1

PREPARE=./target/debug/blitzdb-prepare
SERVER=./target/debug/blitzdb-server
CLIENT=./target/debug/blitzdb-client

# ── 2. Generate test Parquet file ─────────────────────────────────────────────
echo "==> Generating test Parquet file..."
PARQUET="$TMPDIR_LOCAL/test.parquet"
uv run --with pyarrow --quiet python3 - "$PARQUET" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path = sys.argv[1]
keys   = [b"hello", b"foo",   b"blitz", b"rdma",    b"fast"]
values = [b"world", b"bar",   b"fast",  b"network", b"speed"]

table = pa.table({"key": pa.array(keys, type=pa.large_binary()),
                  "value": pa.array(values, type=pa.large_binary())})
pq.write_table(table, path)
print(f"Wrote {len(keys)} rows to {path}")
PYEOF

# ── 3. Prepare (MPH + index + heap) ──────────────────────────────────────────
echo "==> Running blitzdb-prepare..."
PREFIX="$TMPDIR_LOCAL/test"
"$PREPARE" "$PARQUET" "$PREFIX"

for ext in mph index heap; do
    if [[ -f "$PREFIX.$ext" ]]; then
        pass "output file $ext exists"
    else
        fail "output file $ext missing"
    fi
done

# ── 4. Start server ───────────────────────────────────────────────────────────
echo "==> Starting blitzdb-server..."
"$SERVER" "$PREFIX" 10000 > "$TMPDIR_LOCAL/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server to advertise itself via chitchat
sleep 2

if kill -0 "$SERVER_PID" 2>/dev/null; then
    pass "server started (pid $SERVER_PID)"
else
    fail "server exited early"
    echo "--- server log ---"
    cat "$TMPDIR_LOCAL/server.log"
    exit 1
fi

# ── 5. Key lookups ────────────────────────────────────────────────────────────
echo "==> Running key lookups..."
MPH="$PREFIX.mph"
SEED="127.0.0.1:10000"
GOSSIP_PORT=10001

declare -A EXPECTED=(
    [hello]=world
    [foo]=bar
    [blitz]=fast
    [rdma]=network
    [fast]=speed
)

for key in "${(k)EXPECTED[@]}"; do
    output=$("$CLIENT" "$MPH" "$key" "$SEED" $((GOSSIP_PORT++)) 2>/dev/null | tail -1)
    want="${key} = ${EXPECTED[$key]}"
    if [[ "$output" == "$want" ]]; then
        pass "lookup '$key' → '${EXPECTED[$key]}'"
    else
        fail "lookup '$key': got '$output', want '$want'"
    fi
done

# ── 6. Results ────────────────────────────────────────────────────────────────
echo ""
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
