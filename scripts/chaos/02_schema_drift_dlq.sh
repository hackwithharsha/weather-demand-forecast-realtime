#!/usr/bin/env bash
# scripts/chaos/02_schema_drift_dlq.sh
#
# Demonstrates DLQ routing on validation failure.
#
# What it does
# ────────────
# PART A — poller-side schema drift (silent drop, no DLQ entry)
#   1. Enable schema_drift on mock-weather (renames "temperature" → "temp").
#   2. Tail weather-poller logs for 8s to observe fetch_failed warnings.
#      The poller catches the KeyError from _flatten() and drops the message
#      before producing — nothing reaches Kafka yet.
#
# PART B — ingestor-side DLQ routing
#   3. Start a background consumer on demand.events.dlq from the current end
#      so it will capture only messages produced during this run.
#   4. Inject 3 messages with schema_version=99 directly into
#      weather.readings.v1 via rpk (simulates a producer that blindly
#      forwarded drifted data or bumped the version without coordination).
#   5. The ingestor's _validate_weather() raises ValidationError for
#      schema_version=99 → _send_to_dlq() fires immediately.
#   6. Show the DLQ messages including the "dlq-reason" header.
#
# PART C — cleanup
#   7. Disable schema_drift, verify weather-poller resumes.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
COMPOSE="docker compose --profile stream --profile core"

MOCK_URL="http://localhost:8001"
RPK="docker exec -i weather-demand-forecast-redpanda-1 rpk"
RPK_EXEC="docker exec weather-demand-forecast-redpanda-1 rpk"
WEATHER_TOPIC="weather.readings.v1"
DLQ_TOPIC="demand.events.dlq"

# ── pre-checks ───────────────────────────────────────────────────────────────
for c in weather-demand-forecast-ingestor-1 \
          weather-demand-forecast-redpanda-1 \
          weather-demand-forecast-mock-weather-1 \
          weather-demand-forecast-weather-poller-1; do
    if ! docker ps --filter "name=^/${c}$" --filter status=running --format '{{.Names}}' \
            | grep -q .; then
        echo "ERROR: $c is not running. Run 'make up-stream' first." >&2
        exit 1
    fi
done

if ! curl -sf "$MOCK_URL/health" >/dev/null; then
    echo "ERROR: mock-weather not reachable at $MOCK_URL" >&2
    exit 1
fi

# ── helper: pretty-print DLQ output ──────────────────────────────────────────
_show_dlq() {
    local file="$1"
    if [ ! -s "$file" ]; then
        echo "  (no DLQ messages captured — check ingestor logs)"
        return
    fi
    # rpk outputs pretty-printed (multi-line) JSON — accumulate lines into
    # complete objects, then parse each one.
    python3 - "$file" <<'PYEOF'
import sys, json

def _emit(buf):
    raw = "".join(buf).strip()
    if not raw:
        return
    try:
        rec = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  (unparseable record)")
        return
    key    = rec.get("key") or "(none)"
    part   = rec.get("partition", "?")
    off    = rec.get("offset", "?")
    hdrs   = {h["key"]: h["value"] for h in rec.get("headers", [])}
    reason = hdrs.get("dlq-reason", "(no dlq-reason header)")
    print(f"  partition={part}  offset={off}  key={key}")
    print(f"    dlq-reason : {reason}")
    try:
        body = json.loads(rec.get("value", "{}"))
        print(f"    schema_version={body.get('schema_version')}  "
              f"event_id={body.get('event_id')}")
    except Exception:
        pass
    print()

buf, depth = [], 0
with open(sys.argv[1]) as fh:
    for line in fh:
        buf.append(line)
        depth += line.count("{") - line.count("}")
        if depth == 0 and buf:
            _emit(buf)
            buf = []
PYEOF
}

# ── PART A · schema drift causes fetch_failed in the poller ──────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PART A — enable schema_drift on mock-weather"
echo "══════════════════════════════════════════════════════"
echo "  POST $MOCK_URL/admin/chaos  {schema_drift: true}"
curl -sf -X POST "$MOCK_URL/admin/chaos" \
    -H "Content-Type: application/json" \
    -d '{"schema_drift": true}' | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('  chaos now:', d)"

echo ""
echo "  The weather-poller polls every \${POLLER_POLL_INTERVAL_S:-30}s."
echo "  Waiting up to 35s for the next cycle — expect fetch_failed warnings:"
echo "  (mock-weather now returns 'temp' instead of 'temperature';"
echo "   _flatten() raises KeyError → poller silently drops, no Kafka msg)"
echo ""

# --tail 0 shows only lines written AFTER the command starts
$COMPOSE logs --no-log-prefix --tail 0 -f weather-poller 2>/dev/null &
TAIL_PID=$!
# Wait for one full poll cycle; kill log tail cleanly
sleep 35
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true

echo ""
echo "  Note: the poller drops the message before producing — nothing"
echo "  entered Kafka, so no DLQ entries from this path."

# ── PART B · inject bad envelopes and observe DLQ ────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PART B — inject schema_version=99 into $WEATHER_TOPIC"
echo "══════════════════════════════════════════════════════"
echo "  Simulates a producer that forwards schema-drifted data without"
echo "  coordinating a version bump."
echo ""

DLQ_OUT="$(mktemp)"

# Start DLQ consumer from the current end. -n 3 stops after receiving 3 msgs.
# rpk topic consume has no --timeout; we do a manual timed wait+kill below.
$RPK_EXEC topic consume "$DLQ_TOPIC" \
    --brokers localhost:9092 \
    -o end \
    -n 3 \
    > "$DLQ_OUT" 2>/dev/null &
DLQ_PID=$!
sleep 1   # allow the consumer to subscribe and register its starting offset

echo "  Producing 3 messages with schema_version=99 to ${WEATHER_TOPIC}..."
for i in 1 2 3; do
    NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    MSG=$(printf \
        '{"schema_version":99,"event_id":"chaos-drift-%03d","produced_at":"%s","payload":{"city":"london","polled_at":"%s"}}' \
        "$i" "$NOW" "$NOW")
    printf '%s\n' "$MSG" | \
        docker exec -i weather-demand-forecast-redpanda-1 rpk topic produce \
            "$WEATHER_TOPIC" --key london --brokers localhost:9092
    printf "  [%d/3] sent event_id=chaos-drift-%03d\n" "$i" "$i"
done

echo ""
echo "  Waiting for ingestor batch cycle + DLQ consumer (up to 15s)..."
# Poll until consumer exits (-n 3 satisfied) or 15s elapses
for _i in $(seq 1 15); do
    sleep 1
    kill -0 "$DLQ_PID" 2>/dev/null || break  # consumer already finished
done
kill "$DLQ_PID" 2>/dev/null || true
wait "$DLQ_PID" 2>/dev/null || true

echo ""
echo "══════════════════════════════════════════════════════"
echo " DLQ messages received"
echo "══════════════════════════════════════════════════════"
_show_dlq "$DLQ_OUT"
rm -f "$DLQ_OUT"

# ── PART C · cleanup ─────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo " PART C — reset schema_drift"
echo "══════════════════════════════════════════════════════"
echo "  POST $MOCK_URL/admin/chaos  {schema_drift: false}"
curl -sf -X POST "$MOCK_URL/admin/chaos" \
    -H "Content-Type: application/json" \
    -d '{"schema_drift": false}' | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('  chaos now:', d)"

echo ""
echo "  Weather-poller will resume successfully on its next poll cycle."
echo "Done."
