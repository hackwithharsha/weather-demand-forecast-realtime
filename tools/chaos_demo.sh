#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# chaos_demo.sh — End-to-end drift + alerting demo
#
# What it does
# ------------
# 1. Enables weather chaos (null_field_rate 1.0) → weather-poller stops
#    producing fresh readings, so Redis values can't self-correct.
# 2. Injects extreme weather values (temperature_c=0, precip_mm=0) directly
#    into Redis for every city.  The API reads these values when it builds
#    the feature vector for /predict.
# 3. Pumps 200 /predict calls spread across all cities so that
#    marts.prediction_features accumulates enough rows for Evidently to run.
# 4. Tells you exactly what to watch in Grafana and Alertmanager.
# 5. Tails the webhook-receiver container so you see alerts fire inline.
# 6. Restores everything on exit (Ctrl-C or normal completion).
#
# Prerequisites
# -------------
#   make up-obs up-stream   (or make up-all)
#   A Production model must exist in MLflow (run: make train && make promote)
#
# Usage
# -----
#   bash tools/chaos_demo.sh              # interactive, full demo
#   bash tools/chaos_demo.sh --fast       # lower drift check interval to 5 min
#                                         # (restarts the drift service)
# ---------------------------------------------------------------------------

set -euo pipefail

COMPOSE="docker compose"
API_URL="http://localhost:8000"
WEATHER_URL="http://localhost:8001"
CITIES=(london tokyo new_york dubai sydney moscow nairobi singapore reykjavik cape_town)
REDIS_ROUTE_KEY_PREFIX="feat:route"
DRIFT_INTERVAL_FAST=5   # minutes, used with --fast

FAST_MODE=false
if [[ "${1:-}" == "--fast" ]]; then
  FAST_MODE=true
fi

# ── Colours ─────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YLW='\033[0;33m'; GRN='\033[0;32m'; BLU='\033[0;34m'; BOLD='\033[1m'; RST='\033[0m'

info()    { echo -e "${BLU}▸${RST} $*"; }
success() { echo -e "${GRN}✓${RST} $*"; }
warn()    { echo -e "${YLW}⚠${RST}  $*"; }
header()  { echo -e "\n${BOLD}$*${RST}"; }

# ── Cleanup on exit ──────────────────────────────────────────────────────────
cleanup() {
  echo ""
  header "── Restoring baseline ──────────────────────────────────────────────"

  info "Disabling weather chaos..."
  curl -sf -X POST "$WEATHER_URL/admin/chaos" \
    -H "Content-Type: application/json" \
    -d '{"null_field_rate": 0.0}' > /dev/null 2>&1 || warn "Could not reach mock-weather"
  success "Chaos disabled"

  info "Removing injected Redis weather values..."
  for city in "${CITIES[@]}"; do
    key="$REDIS_ROUTE_KEY_PREFIX:$city"
    $COMPOSE exec -T redis redis-cli HDEL "$key" weather_temp_c weather_precip_mm > /dev/null 2>&1 || true
  done
  success "Redis weather fields removed (stream consumer will repopulate on next poll)"

  if [[ "$FAST_MODE" == true ]]; then
    info "Restarting drift service at default interval..."
    $COMPOSE --profile obs restart drift > /dev/null 2>&1 || true
    success "Drift service restored"
  fi

  echo ""
  success "Demo complete — system restored to baseline."
}
trap cleanup EXIT

# ── Helpers ─────────────────────────────────────────────────────────────────
chaos_status() {
  curl -sf "$WEATHER_URL/admin/chaos" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2))" \
    || echo "(could not reach mock-weather)"
}

drift_scores() {
  # Query Prometheus for the latest drift_score values
  curl -sf "http://localhost:9090/api/v1/query?query=drift_score" 2>/dev/null \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
rows = d.get('data', {}).get('result', [])
if not rows:
    print('  (no drift_score metrics yet)')
else:
    for r in sorted(rows, key=lambda x: float(x['value'][1]), reverse=True):
        feat = r['labels'].get('feature', '?')
        val  = float(r['value'][1])
        bar  = '█' * int(val * 20)
        print(f'  {feat:<20s}  {val:.4f}  {bar}')
" 2>/dev/null || echo "  (Prometheus not reachable)"
}

pump_predictions() {
  local total=$1
  local per_city=$(( total / ${#CITIES[@]} ))
  local ok=0 fail=0
  for city in "${CITIES[@]}"; do
    for (( i=0; i<per_city; i++ )); do
      resp=$(curl -sf -X POST "$API_URL/predict" \
        -H "Content-Type: application/json" \
        -d "{\"city\": \"$city\", \"horizon_hours\": 1}" 2>&1) && ok=$(( ok+1 )) || fail=$(( fail+1 ))
    done
    echo -n "."
  done
  echo ""
  success "$ok /predict calls succeeded  ($fail failed)"
}

# ── Pre-flight ───────────────────────────────────────────────────────────────
header "── Pre-flight checks ───────────────────────────────────────────────"

if ! curl -sf "$API_URL/health" > /dev/null 2>&1; then
  echo -e "${RED}✗${RST} API not reachable at $API_URL"
  echo "  Run: make up-all   (or: make up-obs up-stream)"
  exit 1
fi
success "API healthy ($API_URL)"

if ! curl -sf "$WEATHER_URL/health" > /dev/null 2>&1; then
  echo -e "${RED}✗${RST} mock-weather not reachable at $WEATHER_URL"
  echo "  Run: make up   (core profile includes mock-weather)"
  exit 1
fi
success "mock-weather healthy ($WEATHER_URL)"

if ! $COMPOSE ps webhook-receiver 2>/dev/null | grep -q "running\|Up"; then
  warn "webhook-receiver is not running — alerts will fire but won't be logged here"
  warn "Run: make up-obs   to start the full observability stack"
fi

# ── Baseline ─────────────────────────────────────────────────────────────────
header "── Baseline state ──────────────────────────────────────────────────"
info "Current chaos config:"
chaos_status
echo ""
info "Current drift scores:"
drift_scores

# ── Optional: fast mode ──────────────────────────────────────────────────────
if [[ "$FAST_MODE" == true ]]; then
  header "── Fast mode: drift check interval → ${DRIFT_INTERVAL_FAST} min ─────────────────"
  warn "Restarting drift service with CHECK_INTERVAL_MINUTES=$DRIFT_INTERVAL_FAST"
  CHECK_INTERVAL_MINUTES=$DRIFT_INTERVAL_FAST $COMPOSE --profile obs up -d drift > /dev/null 2>&1 \
    || warn "Could not override drift interval (check that the obs profile is running)"
  success "Drift service restarted — first check fires in up to ${DRIFT_INTERVAL_FAST} min"
fi

# ── Confirm ──────────────────────────────────────────────────────────────────
echo ""
warn "This demo will inject extreme weather values (0 °C, 0 mm) into Redis."
warn "Chaos + injected values will be removed automatically on exit (Ctrl-C)."
read -rp "$(echo -e "${BOLD}Press Enter to start the demo, or Ctrl-C to abort.${RST}") "

# ── Step 1: enable chaos ─────────────────────────────────────────────────────
header "── Step 1: Enable weather chaos ────────────────────────────────────"
info "Setting null_field_rate=1.0 on mock-weather..."
curl -sf -X POST "$WEATHER_URL/admin/chaos" \
  -H "Content-Type: application/json" \
  -d '{"null_field_rate": 1.0}' | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2))"
echo ""
success "Weather chaos enabled — poller will crash on next poll cycle (≤30 s)"
info "Weather readings to Kafka will stop.  Redis values are now frozen."

# ── Step 2: inject Redis values ───────────────────────────────────────────────
header "── Step 2: Inject extreme weather into Redis ────────────────────────"
info "Writing temperature_c=0.0, precip_mm=0.0 to feat:route:{city} for all cities..."
for city in "${CITIES[@]}"; do
  key="$REDIS_ROUTE_KEY_PREFIX:$city"
  $COMPOSE exec -T redis redis-cli HSET "$key" weather_temp_c 0.0 weather_precip_mm 0.0 > /dev/null
  echo -n "  ${city} → 0 °C, 0 mm"
  # Show reference value for contrast
  echo ""
done
success "All cities now have temperature_c=0.0 in Redis"
info "Reference distribution (X_train / city_hour_features) has real temperatures:"
info "  london ~8–12 °C  |  dubai ~35–40 °C  |  tokyo ~15–25 °C  etc."
info "Evidently will detect drift in temperature_c and precip_mm when /predict"
info "calls accumulate these 0-values in marts.prediction_features."

# ── Step 3: pump predictions ─────────────────────────────────────────────────
header "── Step 3: Pump /predict calls → populate prediction_features ───────"
info "Sending 200 requests (20 per city × 10 cities)..."
pump_predictions 200

# ── Step 4: what to watch ─────────────────────────────────────────────────────
header "── Step 4: Watch drift climb ────────────────────────────────────────"
echo ""
echo -e "  ${BOLD}Grafana Data Quality dashboard${RST}"
echo -e "  → ${BLU}http://localhost:3001/d/data-quality${RST}"
echo -e "    Panel 'Drift Score by Feature'      (temperature_c and precip_mm should climb)"
echo -e "    Panel 'Drift Detected by Feature'   (flips to 1 when score > threshold)"
echo -e "    Stat  'Features Drifting'            (increases from 0)"
echo ""
echo -e "  ${BOLD}Alertmanager${RST}"
echo -e "  → ${BLU}http://localhost:9093/#/alerts${RST}"
echo -e "    Alert 'FeatureDriftHigh' appears in FIRING state"
echo ""
echo -e "  ${BOLD}Prometheus${RST}"
echo -e "  → ${BLU}http://localhost:9090/alerts${RST}"
echo -e "    Rule 'FeatureDriftHigh' transitions from INACTIVE → PENDING → FIRING"

if [[ "$FAST_MODE" == true ]]; then
  echo ""
  info "Drift check fires every ${DRIFT_INTERVAL_FAST} min.  First check: up to ${DRIFT_INTERVAL_FAST} min from now."
else
  echo ""
  warn "Default drift check interval is 60 min.  For a faster demo run:"
  warn "  bash tools/chaos_demo.sh --fast"
fi

# ── Step 5: tail webhook logs ────────────────────────────────────────────────
header "── Step 5: Tail webhook-receiver logs (Ctrl-C to stop + clean up) ──"
echo ""
info "When FeatureDriftHigh fires, you will see lines like:"
echo -e "  ${YLW}ALERT  FIRING   FeatureDriftHigh  feature=temperature_c  sev=warning  ...${RST}"
echo -e "  ${GRN}ALERT  RESOLVED FeatureDriftHigh  feature=temperature_c  (after cleanup)${RST}"
echo ""
info "Polling Prometheus for drift_score every 30 s while waiting..."
echo "(Ctrl-C triggers automatic cleanup)"
echo ""

# Poll drift scores every 30 s alongside the webhook tail in background
(
  while true; do
    sleep 30
    echo ""
    echo -e "${BOLD}── drift scores @ $(date -u +%H:%M:%S) UTC ──${RST}"
    drift_scores
  done
) &
POLL_PID=$!

# Tail webhook logs (this blocks until Ctrl-C)
$COMPOSE logs -f webhook-receiver 2>/dev/null || true

kill "$POLL_PID" 2>/dev/null || true
