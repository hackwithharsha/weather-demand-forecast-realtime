# Demos

## SET vs HSET: feature fields disappearing under concurrent writes

**File**: `tools/demo_set_vs_hset.py`
**Run**: `python3 tools/demo_set_vs_hset.py` (no live Redis required; uses an in-process simulator)

### The scenario

Two independent jobs write to the same Redis key `feat:route:london`:

| Writer | Fields | Trigger |
|---|---|---|
| Batch job | `avg_bookings_90d`, `seasonality_mon`, `lead_time_p50`, `batch_computed_at` | Nightly at 02:30 UTC |
| Stream consumer | `searches_5m`, `bookings_15m`, `look_to_book_1h`, `stream_computed_at` | Every demand event |

The field sets are **disjoint** — neither job cares about the other's fields.
The key is the same — both jobs target `feat:route:london`.

---

### Part 1 — Broken: sequential SET writes

Each job serialises only its own fields into a JSON blob and calls `SET key blob`.
`SET` replaces the **entire value**; the previous writer's fields are gone.

```
[1] batch job  → SET key {avg_bookings_90d, seasonality_mon, lead_time_p50, batch_computed_at}
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +2 more
    state : ✓ batch   ✗ STREAM FIELDS GONE

[2] stream consumer → SET key {searches_5m, bookings_15m, look_to_book_1h, stream_computed_at}
    value : {"searches_5m": "7.0", "bookings_15m": "3.0"} … +2 more
    state : ✗ BATCH FIELDS GONE   ✓ stream
      ^^ batch fields are gone — nightly compute is overwritten

[3] batch job  → SET key again (next nightly run)
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +2 more
    state : ✓ batch   ✗ STREAM FIELDS GONE
      ^^ stream fields are gone — real-time signals are overwritten
```

The key ping-pongs between containing only batch fields and only stream fields.
It **never** holds both at the same time.

---

### Part 2 — Broken: concurrent SET (80 rounds each writer)

Two threads writing continuously.  A checker thread samples the key every 0.5 ms
and counts samples where either writer's fields are absent.

```
  Samples taken       :   159
  Batch fields gone   :    76 / 159  ( 47%)
  Stream fields gone  :    83 / 159  ( 52%)

  ✗  159 samples had missing fields — the key never holds both writers' work at once.
```

Every single sample is corrupt.  At any instant the key contains at most one
writer's fields — whichever job happened to run last.  The other's work is
silently discarded.

This is not a timing fluke.  It is **structural**: `SET` is a complete
replacement.  Two writers with disjoint field sets cannot both be right.

---

### Part 3 — Fixed: sequential HSET writes

Each job calls `HSET key mapping={only_their_fields}`.
`HSET` writes only the specified fields; all other hash fields are untouched.

```
[1] batch job  → HSET key {avg_bookings_90d, seasonality_mon, lead_time_p50, batch_computed_at}
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +2 more
    state : ✓ batch   ✗ STREAM FIELDS GONE
      (stream fields not yet written — expected)

[2] stream consumer → HSET key {searches_5m, bookings_15m, look_to_book_1h, stream_computed_at}
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +6 more
    state : ✓ batch   ✓ stream
      ^^ both field-sets coexist ✓

[3] batch job  → HSET key again (next nightly run)
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +6 more
    state : ✓ batch   ✓ stream
      ^^ stream fields untouched — real-time signals survive the nightly sync ✓
```

After the first write from each job, both field-sets coexist permanently.
Subsequent runs by either job update only their own fields.

---

### Part 4 — Fixed: concurrent HSET (80 rounds each writer)

Same two-thread / checker setup as Part 2.

```
  Samples taken       :   158
  Batch fields gone   :     0 / 158  (  0%)
  Stream fields gone  :     0 / 158  (  0%)

  ✓  Zero field loss across all samples.
```

---

### Why this matters for the feature store

The batch job and stream consumer write to `feat:route:{route_id}` on completely
different schedules — nightly vs. per-event.  They share the key because the API
reads **all** features in a single `HGETALL`.  Keeping the key unified means the
serving path needs one round-trip regardless of how many independent writers exist.

If either job used `SET`:

* Every nightly batch run would erase `searches_5m`, `bookings_15m`,
  `look_to_book_1h`, `weather_*`, and `stream_computed_at`.
* Every stream event would erase `avg_bookings_90d`, `seasonality_*`,
  `lead_time_*`, `cancellation_rate_180d`, `elasticity_estimate`, and
  `batch_computed_at`.

The model would silently receive stale or missing features — no error raised,
no alert fired.  The only symptom would be degraded forecast accuracy.

`HSET` eliminates the problem at the protocol level: Redis itself enforces
field-granularity writes, so no amount of scheduling coincidence can cause one
job to overwrite the other's fields.

See `docs/decisions.md §"HSET vs SET for the feature store"` for the full ADR.

---

### Code diff: broken → fixed

```python
# ── BROKEN ────────────────────────────────────────────────────────────────────
# Batch job
r.set(
    f"feat:route:{route_id}",
    json.dumps({f.name: str(row[f.name]) for f in ROUTE_FEATURES.by_source("batch")}),
)

# Stream consumer
r.set(
    f"feat:route:{route_id}",
    json.dumps({
        "searches_5m":        str(searches_5m),
        "bookings_15m":       str(bookings_15m),
        "stream_computed_at": datetime.now(timezone.utc).isoformat(),
    }),
)

# ── FIXED ─────────────────────────────────────────────────────────────────────
# Batch job  (feature_store.py:_sync_to_redis)
pipe.hset(
    f"feat:route:{route_id}",
    mapping={f.name: str(row[f.name]) for f in ROUTE_FEATURES.by_source("batch")},
)

# Stream consumer  (stream_features/redis_ops.py:update_demand_features)
r.hset(
    f"feat:route:{route_id}",
    mapping={
        _F_SEARCHES_5M:          str(float(searches_5m)),
        _F_BOOKINGS_15M:         str(float(bookings_15m)),
        _F_LOOK_TO_BOOK:         str(round(look_to_book_1h, 6)),
        STREAM_COMPUTED_AT_FIELD: datetime.now(timezone.utc).isoformat(),
    },
)
```

The change is **one word** per call site — `set` → `hset` and `json.dumps({...})`
→ `mapping={...}`.  The Redis hash model does the rest.

---

## Chaos → Drift → Alert: end-to-end observability demo

**Script**: `tools/chaos_demo.sh`
**Run**: `bash tools/chaos_demo.sh` (interactive) or `bash tools/chaos_demo.sh --fast` (5-min drift interval)

### Prerequisites

```bash
make up-all          # postgres, redis, redpanda, minio, mlflow, api, worker, …
make train           # train a model and push it to MLflow
make promote         # promote the best run to Production stage
make up-obs          # prometheus, grafana, alertmanager, webhook-receiver
```

All ten cities must have feature data in Redis (`feat:route:{city}`) — a few
minutes of stream traffic is enough.

---

### What the script does

| Step | Action |
|---|---|
| Pre-flight | Verifies `/health` on the API and mock-weather service |
| (opt) --fast | Restarts the drift job with `CHECK_INTERVAL_MINUTES=5` so you don't wait an hour |
| Confirm | Prints baseline chaos config + current drift scores, then prompts Enter |
| 1 | `POST /admin/chaos {"null_field_rate": 1.0}` — weather-poller crashes on next poll (≤30 s) |
| 2 | `redis-cli HSET feat:route:{city} weather_temp_c 0.0 weather_precip_mm 0.0` for all 10 cities |
| 3 | Pumps 200 `POST /predict` calls (20 per city) to populate `marts.prediction_features` |
| 4 | Prints URLs to watch (Grafana, Alertmanager, Prometheus) |
| 5 | Tails `webhook-receiver` logs; polls Prometheus `drift_score` every 30 s |
| Ctrl-C | `cleanup()` trap: disables chaos, removes injected Redis fields, restores drift interval |

---

### Why direct Redis injection?

Weather chaos (`null_field_rate=1.0`) alone does **not** cause prediction drift:

- Redis hash fields have no TTL — old weather values persist while the poller is
  down.
- `weather_temp_c` has `default_on_missing=None`, so the API falls back to the
  Postgres mart value (which matches the training distribution — no drift signal).
- `weather_precip_mm` has `default_on_missing=0.0`, but only if the hash field
  is **absent**; an explicit `0.0` value in the hash is just a normal reading.

Injecting `temperature_c=0.0` and `precip_mm=0.0` directly into Redis bypasses
these defaults and plants values that are far outside the training distribution
(London trains on ~8–12 °C; Dubai on ~35–40 °C).  Combined with chaos
preventing the poller from correcting them, the extremes stick for as long as
the demo runs.

---

### What you should see

#### Terminal (drift scores, every 30 s)

```
── drift scores @ 14:32:00 UTC ──
  temperature_c         0.0000
  precip_mm             0.0000

── drift scores @ 14:32:30 UTC ──
  temperature_c         0.0000
  precip_mm             0.0000

# … after the drift job fires (up to 60 min, or 5 min with --fast) …

── drift scores @ 14:37:00 UTC ──
  precip_mm             0.6200  ████████████
  temperature_c         0.8800  █████████████████
  event_count           0.0300
  demand_lag_1h         0.0100
```

Scores for `temperature_c` and `precip_mm` climb well above the 0.20 threshold.
Other features remain near zero (they draw from Postgres, which is unaffected).

#### Grafana — Data Quality dashboard (`http://localhost:3001/d/data-quality`)

| Panel | Before chaos | After drift job fires |
|---|---|---|
| **Drift Score by Feature** | flat near 0 | `temperature_c` and `precip_mm` bars spike |
| **Drift Detected by Feature** | all 0 | flips to 1 for `temperature_c` and `precip_mm` |
| **Features Drifting** (stat) | 0 | 2 |
| **Drift Job Duration** | last run time | updates with each check |

#### Prometheus alerts (`http://localhost:9090/alerts`)

```
FeatureDriftHigh    INACTIVE  →  PENDING  →  FIRING
  feature="temperature_c"  value=0.88
  feature="precip_mm"      value=0.62
```

The alert has `for: 0m`, so it moves from PENDING to FIRING immediately on the
first evaluation that exceeds the threshold (no wait window).

#### Alertmanager (`http://localhost:9093/#/alerts`)

Two active `FeatureDriftHigh` alerts appear with severity `warning`.
`group_wait: 30s` means the first notification fires ~30 s after Alertmanager
receives the alert from Prometheus.

#### webhook-receiver logs (`docker compose logs -f webhook-receiver`)

```
2026-01-15T14:37:31Z INFO     ALERT  FIRING    FeatureDriftHigh               sev=warning   feature=temperature_c  "Feature temperature_c drift score is 88%"
2026-01-15T14:37:31Z INFO     ALERT  FIRING    FeatureDriftHigh               sev=warning   feature=precip_mm      "Feature precip_mm drift score is 62%"
```

The script tails this output live.  You will see the lines appear within
30 s of the alert becoming FIRING in Prometheus.

After Ctrl-C the cleanup trap runs and the alerts resolve:

```
2026-01-15T14:40:05Z INFO     ALERT  RESOLVED  FeatureDriftHigh               sev=warning   feature=temperature_c
2026-01-15T14:40:05Z INFO     ALERT  RESOLVED  FeatureDriftHigh               sev=warning   feature=precip_mm
```

---

### Expected timeline (default mode, --fast in parentheses)

| Time | Event |
|---|---|
| T+0 s | Script enables chaos; Redis values injected; 200 /predict calls sent |
| T+30 s | weather-poller crashes (null_field_rate=1.0 causes TypeError) |
| T+60 min (5 min) | Drift job fires; Evidently detects drift in `temperature_c`, `precip_mm` |
| T+60 min+15 s | Prometheus scrapes worker metrics; `drift_score` time series updated |
| T+60 min+30 s | Alertmanager receives `FeatureDriftHigh`; fires webhook after `group_wait` |
| T+60 min+60 s | Webhook log lines appear in terminal |
| Ctrl-C | Chaos disabled, Redis fields removed; next drift check scores → 0 |
| Ctrl-C+60 min (5 min) | Alerts resolve; RESOLVED lines appear in webhook log |

---

### Alert rules triggered

| Alert | Threshold | Source metric |
|---|---|---|
| `FeatureDriftHigh` | `drift_score > 0.2` | `worker:9101/metrics` |

The other three rules (`KafkaConsumerLagHigh`, `PredictionLatencyHigh`,
`ModelNotTrainedRecently`) are not triggered by this demo — they require a
Redpanda consumer backlog, a slow `/predict` path, or a missing training run
respectively.

---

### Cleanup

The `cleanup()` trap fires on any exit (Ctrl-C, normal completion, or error):

1. `POST /admin/chaos {"null_field_rate": 0.0}` — re-enables weather-poller
2. `HDEL feat:route:{city} weather_temp_c weather_precip_mm` for all 10 cities
3. (--fast only) Restarts the drift service at the default 60-min interval

After the stream consumer's next poll cycle (≤30 s) the correct weather values
are written back to Redis, and the subsequent drift check will score near zero.
