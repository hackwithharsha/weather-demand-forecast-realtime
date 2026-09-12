#!/usr/bin/env bash
# scripts/chaos/01_kill_restart.sh
#
# Demonstrates at-least-once delivery + idempotent upsert on event_id.
#
# What it does
# ────────────
# 1. Snapshot row counts for non-null event_ids in both raw tables.
# 2. SIGKILL the ingestor (simulates an OOM kill / hard crash mid-batch).
#    The final-flush block in the consumer's `finally` clause does NOT run.
# 3. Wait for Docker's restart policy to bring ingestor back up.
# 4. Give the ingestor time to re-consume the un-committed messages and flush.
# 5. Assert:
#    a) Row counts are non-decreasing (Kafka retained everything; idempotent
#       upserts handle any messages that were re-delivered after restart).
#    b) Zero duplicate event_ids — ON CONFLICT (event_id) DO NOTHING held.

set -euo pipefail

# ── container names / helpers ────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
COMPOSE="docker compose --profile stream --profile core"
INGESTOR="weather-demand-forecast-ingestor-1"
POSTGRES="weather-demand-forecast-postgres-1"

_pass() { printf "  \033[32m[PASS]\033[0m  %s\n" "$*"; }
_fail() { printf "  \033[31m[FAIL]\033[0m  %s\n" "$*"; FAILED=1; }
FAILED=0

PSQL() {
    docker exec "$POSTGRES" psql -U forecast -d forecast -t -A -c "$1" 2>/dev/null \
        | tr -d ' \n'
}

_demand_count()  { PSQL "SELECT COUNT(*) FROM raw.demand_events  WHERE event_id IS NOT NULL"; }
_weather_count() { PSQL "SELECT COUNT(*) FROM raw.weather_readings WHERE event_id IS NOT NULL"; }

_demand_dupes() {
    PSQL "SELECT COUNT(*) FROM (
            SELECT event_id FROM raw.demand_events
            WHERE event_id IS NOT NULL
            GROUP BY event_id HAVING COUNT(*) > 1
          ) x"
}

_weather_dupes() {
    PSQL "SELECT COUNT(*) FROM (
            SELECT event_id FROM raw.weather_readings
            WHERE event_id IS NOT NULL
            GROUP BY event_id HAVING COUNT(*) > 1
          ) x"
}

# ── pre-checks ───────────────────────────────────────────────────────────────
for c in "$INGESTOR" "$POSTGRES" weather-demand-forecast-redpanda-1; do
    if ! docker ps --filter "name=^/${c}$" --filter status=running --format '{{.Names}}' \
            | grep -q .; then
        echo "ERROR: $c is not running. Run 'make up-stream' first." >&2
        exit 1
    fi
done

# ── 1 · snapshot before kill ─────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 1 — snapshot before kill"
echo "══════════════════════════════════════════════════════"
d_before=$(_demand_count)
w_before=$(_weather_count)
printf "  demand_events   non-null event_id rows : %s\n" "$d_before"
printf "  weather_readings non-null event_id rows : %s\n" "$w_before"

# ── 2 · kill ─────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 2 — SIGKILL ingestor"
echo "══════════════════════════════════════════════════════"
echo "  docker kill $INGESTOR"
docker kill "$INGESTOR"

# ── 3 · wait for restart ─────────────────────────────────────────────────────
echo ""
echo "  Waiting for Docker restart policy to bring ingestor back up..."
WAITED=0
while true; do
    STATUS=$(docker inspect "$INGESTOR" --format '{{.State.Status}}' 2>/dev/null || echo gone)
    [ "$STATUS" = "running" ] && break
    sleep 1
    WAITED=$((WAITED + 1))
    if [ $WAITED -ge 20 ]; then
        echo "  Auto-restart did not fire within 20s; triggering manually."
        $COMPOSE up -d ingestor 2>/dev/null
        sleep 3
        break
    fi
done
echo "  Ingestor is running (waited ${WAITED}s)."

# Give the consumer time to re-consume the un-committed window and flush
echo "  Sleeping 10s for backlog re-consumption + batch flush..."
sleep 10

# ── 4 · snapshot after restart ───────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 3 — snapshot after restart"
echo "══════════════════════════════════════════════════════"
d_after=$(_demand_count)
w_after=$(_weather_count)
printf "  demand_events   non-null event_id rows : %s\n" "$d_after"
printf "  weather_readings non-null event_id rows : %s\n" "$w_after"

echo ""
echo "══════════════════════════════════════════════════════"
echo " PHASE 4 — duplicate scan"
echo "══════════════════════════════════════════════════════"
d_dupes=$(_demand_dupes)
w_dupes=$(_weather_dupes)
printf "  demand_events   duplicate event_ids : %s\n" "$d_dupes"
printf "  weather_readings duplicate event_ids : %s\n" "$w_dupes"

# ── 5 · assertions ───────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " ASSERTIONS"
echo "══════════════════════════════════════════════════════"

if [ "$d_after" -ge "$d_before" ]; then
    _pass "demand count non-decreasing  ($d_before → $d_after)"
else
    _fail "demand count decreased  ($d_before → $d_after)"
fi

if [ "$w_after" -ge "$w_before" ]; then
    _pass "weather count non-decreasing  ($w_before → $w_after)"
else
    _fail "weather count decreased  ($w_before → $w_after)"
fi

if [ "$d_dupes" -eq 0 ]; then
    _pass "zero duplicate demand event_ids"
else
    _fail "$d_dupes duplicate demand event_id(s) found after restart"
fi

if [ "$w_dupes" -eq 0 ]; then
    _pass "zero duplicate weather event_ids"
else
    _fail "$w_dupes duplicate weather event_id(s) found after restart"
fi

echo ""
if [ "$FAILED" -eq 0 ]; then
    echo "  All assertions passed."
else
    echo "  One or more assertions FAILED — check ingestor logs." >&2
    exit 1
fi
