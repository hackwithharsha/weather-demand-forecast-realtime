#!/usr/bin/env bash
# scripts/chaos/03_consumer_lag.sh
#
# Demonstrates consumer lag building and draining.
#
# What it does
# ────────────
# 1. Show current consumer lag (should be ~0 — ingestor is consuming live).
# 2. docker pause the ingestor — freezes all threads without disconnecting.
#    The Kafka broker keeps the group assignment alive until session.timeout.ms
#    (confluent-kafka default: 45 s), so the partitions stay assigned and lag
#    is visible as log-end-offset advances past committed offset.
# 3. Wait 12s while producers keep writing. Show lag growing via:
#      • rpk group describe (terminal)
#      • Redpanda Console API (JSON)
#      • Console UI hint (browser URL)
# 4. docker unpause — consumer resumes from committed offsets, drains lag.
# 5. Wait and assert lag returned to near zero.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

INGESTOR="weather-demand-forecast-ingestor-1"
REDPANDA="weather-demand-forecast-redpanda-1"
RPK="docker exec $REDPANDA rpk"
CONSOLE_URL="http://localhost:8082"

DEMAND_GROUP="ingestor-demand"
WEATHER_GROUP="ingestor-weather"

# ── pre-checks ───────────────────────────────────────────────────────────────
for c in "$INGESTOR" "$REDPANDA"; do
    if ! docker ps --filter "name=^/${c}$" --filter status=running --format '{{.Names}}' \
            | grep -q .; then
        echo "ERROR: $c is not running. Run 'make up-stream' first." >&2
        exit 1
    fi
done

# ── helpers ───────────────────────────────────────────────────────────────────
_rpk_lag() {
    # Print rpk group describe output (all columns).
    local group="$1"
    $RPK group describe "$group" --brokers localhost:9092 2>/dev/null || true
}

_total_lag() {
    # Read TOTAL-LAG from the rpk group describe summary line.
    local group="$1"
    $RPK group describe "$group" --brokers localhost:9092 2>/dev/null | \
        awk '/^TOTAL-LAG[[:space:]]/ { print $2; exit }'
}

_console_lag() {
    # Redpanda Console /api/consumer-groups — show lag for our groups.
    local raw
    raw=$(curl -sf "$CONSOLE_URL/api/consumer-groups" 2>/dev/null) || {
        echo "  (Console not reachable at $CONSOLE_URL)"
        return
    }
    echo "$raw" | python3 -c "
import sys, json

data = json.load(sys.stdin)

# Redpanda Console returns: {consumerGroups: [{groupId, state, members, ...}]}
# Lag is nested per-topic-partition; sum it manually if a top-level field
# is absent.
groups = (data.get('consumerGroups')
          or data.get('groups')
          or (data if isinstance(data, list) else []))

for g in groups:
    gid   = g.get('groupId') or g.get('id') or ''
    if 'ingestor' not in gid:
        continue
    state = g.get('state', '')
    # Try direct lag fields first
    lag = (g.get('lagSum')
           or g.get('totalLag')
           or g.get('lag'))
    # Fall back: sum across topics → partitions
    if lag is None:
        lag = 0
        for t in g.get('topicOffsets', g.get('topics', [])):
            for p in t.get('partitionOffsets', t.get('partitions', [])):
                lag += p.get('lag', 0)
    print(f'  {gid:<28}  totalLag={lag}  state={state}')
" 2>/dev/null || echo "  (could not parse Console response)"
}

# ── 1 · baseline lag ─────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 1 — baseline (ingestor running)"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  ── rpk: $DEMAND_GROUP ──"
_rpk_lag "$DEMAND_GROUP"
echo ""
echo "  ── rpk: $WEATHER_GROUP ──"
_rpk_lag "$WEATHER_GROUP"
echo ""
echo "  ── Console API: $CONSOLE_URL ──"
_console_lag

# ── 2 · pause ingestor ───────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 2 — docker pause ingestor"
echo "══════════════════════════════════════════════════════"
docker pause "$INGESTOR"
echo "  Ingestor frozen. Producers keep writing to Kafka."
echo "  Open Console for a live view: $CONSOLE_URL/consumer-groups"
echo ""
echo "  Sleeping 12s..."
sleep 12

# ── 3 · show lag while paused ────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 3 — lag after 12s pause"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  ── rpk: $DEMAND_GROUP ──"
_rpk_lag "$DEMAND_GROUP"

lag_paused=$(_total_lag "$DEMAND_GROUP")
echo ""
printf "  demand total lag while paused: %s\n" "$lag_paused"

echo ""
echo "  ── Console API ──"
_console_lag

# ── 4 · unpause ──────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 4 — docker unpause ingestor"
echo "══════════════════════════════════════════════════════"
docker unpause "$INGESTOR"
echo "  Consumer resumed. Waiting 10s for batch drain..."
sleep 10

# ── 5 · assert lag drained ───────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 5 — lag after resume"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  ── rpk: $DEMAND_GROUP ──"
_rpk_lag "$DEMAND_GROUP"

lag_after=$(_total_lag "$DEMAND_GROUP")
echo ""
printf "  demand total lag after resume: %s\n" "$lag_after"

echo ""
echo "  ── Console API ──"
_console_lag

# ── assertion ────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " ASSERTIONS"
echo "══════════════════════════════════════════════════════"

# Expect lag grew while paused (event-generator produces ~30 msgs/s × 12s = ~360)
GROWTH_THRESHOLD=30  # conservative: at least one batch worth should have accumulated
if [ "$lag_paused" -ge "$GROWTH_THRESHOLD" ]; then
    printf "  \033[32m[PASS]\033[0m  lag grew to %s during pause (≥%s expected)\n" \
        "$lag_paused" "$GROWTH_THRESHOLD"
else
    printf "  \033[33m[WARN]\033[0m  lag only reached %s — producers may be slow\n" \
        "$lag_paused"
fi

# Expect lag drained significantly (within 2 batch-windows ≈ 4s of the 10s we waited)
DRAIN_THRESHOLD=200  # anything above this suggests the drain stalled
if [ "$lag_after" -le "$DRAIN_THRESHOLD" ]; then
    printf "  \033[32m[PASS]\033[0m  lag drained to %s (≤%s)\n" \
        "$lag_after" "$DRAIN_THRESHOLD"
else
    printf "  \033[33m[WARN]\033[0m  lag still at %s after 10s — may need more time\n" \
        "$lag_after"
fi

echo ""
echo "Done.  Console UI: $CONSOLE_URL/consumer-groups"
