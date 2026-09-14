# Operations Runbook

Diagnosing and resolving every alert defined in `infra/prometheus/alerts.yml`.

---

## Alert index

| Alert | Severity | Default threshold |
|---|---|---|
| [KafkaConsumerLagHigh](#kafkaconsumerlaghigh) | warning | > 1000 messages for 5 min |
| [FeatureDriftHigh](#featuredrif​thigh) | warning | drift_score > 0.2 (immediate) |
| [PredictionLatencyHigh](#predictionlatencyhigh) | warning | /predict p99 > 500 ms for 5 min |
| [ModelNotTrainedRecently](#modelnottrainedrecently) | warning | no training run in 48 h, or metric absent |

---

## KafkaConsumerLagHigh

**Expression:** `redpanda_kafka_consumer_group_lag > 1000` for 5 m

**What it means:** A consumer group (ingestor-demand or ingestor-weather) is
more than 1000 messages behind the producer.  Sustained lag means raw events
are not being written to Postgres, which delays staging, marts, and predictions.

### Diagnosis

```bash
# 1. Check which group(s) and partitions are lagging
make up-obs   # ensure Prometheus is running
# Open http://localhost:9090 → Graph:
#   redpanda_kafka_consumer_group_lag

# 2. Is the ingestor running?
docker compose --profile stream ps ingestor

# 3. Is the ingestor healthy?
docker compose --profile stream logs --tail=50 ingestor

# 4. Is Redpanda itself healthy?
docker compose --profile stream ps redpanda
curl -s http://localhost:9644/v1/brokers | python3 -m json.tool
```

### Common causes and fixes

| Cause | Fix |
|---|---|
| Ingestor container crashed | `docker compose --profile stream up -d ingestor` |
| Postgres connection failure | Check `POSTGRES_PASSWORD` in `.env`; `make shell-db` to verify connectivity |
| Ingestor too slow (batch write timeout) | Reduce `PARQUET_FLUSH_INTERVAL_S` in `.env`; scale ingestor replicas |
| Producer burst (load test / seed job) | Lag is transient — monitor; if sustained, restart ingestor |
| Redpanda rebalancing | Wait ~60 s for rebalancing to complete; lag should recover |

### Replay after extended downtime

If the ingestor was down long enough that messages are at risk of falling off
the retention window, or you need to reprocess a specific time range:

```bash
# Stop the ingestor first (required — group must be inactive to reset offsets)
docker compose --profile stream stop ingestor

# Reset the demand consumer group to a specific timestamp
make replay GROUP=ingestor-demand TS=2026-09-14T08:00:00

# Reset the weather consumer group
make replay GROUP=ingestor-weather TS=2026-09-14T08:00:00

# Restart ingestor
make up-stream
```

If messages have already expired from Redpanda, use the Parquet backfill path:

```bash
# Insert Parquet lake rows directly into raw.* (no Kafka needed)
make backfill DATE_START=2026-09-14 DATE_END=2026-09-14

# Then run the batch pipeline to propagate to staging + marts
make worker-pipeline
```

---

## FeatureDriftHigh

**Expression:** `drift_score > 0.2` (fires immediately — no `for` window)

**What it means:** The Evidently drift score for one or more features has
exceeded 0.2.  Scores above this threshold indicate that the distribution of
serving features has shifted significantly relative to the training set logged
in the Production MLflow run.

### Diagnosis

```bash
# 1. Which features are drifting?
#    Open the UI → Drift tab, or query Prometheus:
#    drift_score{feature="temperature_c"}
#    drift_score{feature="demand_lag_1h"}
#    ... etc.

# 2. When did drift start?
#    Look at the Drift Score over 6 h chart in the UI.
#    Or: SELECT * FROM marts.drift_reports ORDER BY checked_at DESC LIMIT 50;
make shell-db
# \c forecast
# SELECT checked_at, feature_name, drift_score, drift_detected
#   FROM marts.drift_reports
#  WHERE drift_detected = true
#  ORDER BY checked_at DESC LIMIT 20;

# 3. Check the worker logs for drift_check events
docker compose --profile stream logs --tail=100 worker | grep drift
```

### Interpreting scores

| Score range | Severity | Recommended action |
|---|---|---|
| 0.1 – 0.2 | Moderate | Monitor trend; no action needed |
| 0.2 – 0.3 | Significant | Alert fires; investigate root cause |
| > 0.3 | Severe | Auto-retrain triggers (if `AUTO_RETRAIN_ON_DRIFT=true`) |

### Root causes

**Weather features** (`temperature_c`, `humidity_pct`, `precip_mm`):
- Seasonal shift — expected over months; model refresh resolves it
- Mock-weather server returning stale/flat values — `make logs-mock-weather`
- External weather API schema change — check `services/mock-weather/`

**Demand features** (`demand_lag_1h`, `demand_lag_24h`, `event_count`):
- Genuine demand pattern change (holiday, event, marketing campaign)
- Ingestor lag causing lag features to be populated from stale data
- Check `KafkaConsumerLagHigh` alert first

### Resolution

```bash
# Option A: Let auto-retrain handle it (default when AUTO_RETRAIN_ON_DRIFT=true)
#   The worker triggers a training run in a background thread when any score
#   exceeds RETRAIN_DRIFT_THRESHOLD (default 0.3).  A new Staging version
#   appears in MLflow if the new model beats the current Production model.
#   Then promote manually via the UI → Model tab → Promote.
#
#   Cooldown: after any retrain (drift or weekly), further drift-triggered
#   runs are suppressed for RETRAIN_COOLDOWN_MINUTES (default 360 / 6 h).
#   The worker logs "retrain_cooldown_active" with remaining_minutes when
#   a trigger is rejected.  To bypass the cooldown, trigger manually:

# Option B: Trigger training manually (bypasses the cooldown)
make train

# After training completes, promote Staging → Production via the UI,
# or force-promote the API's in-memory models:
make reload
```

---

## PredictionLatencyHigh

**Expression:**
```
histogram_quantile(0.99,
  rate(http_request_duration_seconds_bucket{path="/predict"}[5m])
) > 0.5
```
fires when p99 > 500 ms for 5 consecutive minutes.

**What it means:** The 99th-percentile latency of the `/predict` endpoint is
above 500 ms.  Causes are typically: Redis miss (falling back to Postgres),
slow Postgres query, API CPU saturation, or model inference becoming slow.

### Diagnosis

```bash
# 1. What is the current p99?
#    Prometheus: histogram_quantile(0.99, rate(http_request_duration_seconds_bucket{path="/predict"}[5m]))

# 2. Check the API logs for slow request traces
docker compose --profile core logs --tail=100 api | grep predict

# 3. Is Redis healthy?
make shell-redis
# > PING   → expect PONG
# > INFO server   → check uptime_in_seconds

# 4. Redis feature store miss rate
curl -s http://localhost:8080/model/info | python3 -m json.tool | grep miss

# 5. Is the feature store populated?
make inspect-features   # dumps all feat:route:* keys
```

### Common causes and fixes

| Cause | Symptoms | Fix |
|---|---|---|
| Redis feature miss (fallback to Postgres) | `miss_rate_pct` > 50% in /model/info | Run feature store sync: `docker compose run --rm worker python -m app.feature_store` |
| Feature store outdated | Old `loaded_at` timestamps | Run `make worker-pipeline` then feature store sync |
| Redis connection failure | Latency spike + Redis errors in logs | Restart Redis: `docker compose restart redis` |
| API CPU saturation | Steady-state high latency, all endpoints slow | Scale API replicas or increase container CPU limit |
| Model inference slow (large model) | Only /predict is slow, /health is fast | Re-train with fewer features or reduced HGB iterations |
| Cold start / model reload | Brief spike during `make reload` | Normal; resolves in seconds |

### Force a feature-store sync

```bash
docker compose --profile core --profile stream run --rm worker \
    python -m app.feature_store
```

---

## ModelNotTrainedRecently

**Expression:**
```
(time() - model_last_trained_timestamp > 172800)
or absent(model_last_trained_timestamp)
```
Fires immediately when the Pushgateway metric is missing or older than 48 h.

**What it means:** No successful training run has completed in the past 48
hours (or the Pushgateway has been restarted and the metric was lost).  Stale
models diverge from the current data distribution over time.

### Diagnosis

```bash
# 1. When was the last training run?
curl -s http://localhost:9091/metrics | grep model_last_trained

# 2. Check the trainer logs
docker compose --profile ml logs --tail=100 trainer

# 3. Is the Pushgateway running?
docker compose --profile obs ps pushgateway

# 4. Is MLflow reachable?
curl -s http://localhost:5000/health

# 5. Check model registry for recent versions
curl -s http://localhost:8080/model/versions | python3 -m json.tool
```

### Common causes and fixes

| Cause | Fix |
|---|---|
| Trainer job never scheduled / misconfigured | Check `make up-ml`; trainer is a one-shot service run via `make train` |
| Pushgateway restarted (metric lost) | Run `make train` — a fresh push resets the metric |
| MLflow unavailable during trainer run | `make up-ml`; then `make train` |
| Insufficient mart data (`< val_days` rows) | Run `make worker-pipeline` to populate marts; then `make train` |
| PSI drift gate blocked promotion (Gate 1) | Check trainer logs for `promotion_rejected_drift`; inspect `drift_report.json` in MLflow UI |
| Holdout MAE gate blocked promotion (Gate 2) | New model was worse than current Production; this is expected — wait for more data or retrain |

### Manual training

```bash
# Run the full train → register → promote-to-staging pipeline
make train

# After training, if the new version is in Staging, promote to Production via
# the UI (Model tab → Promote) or force-reload the in-memory models:
make reload
```

### Weekly auto-retrain

The worker runs an automatic retraining job every Sunday at 04:00 UTC.
If the Pushgateway metric is missing and it is outside that window, run
training manually as above.

To verify the weekly job is scheduled:

```bash
docker compose --profile core --profile stream logs --tail=20 worker | grep weekly_retrain
```

---

## General diagnostic commands

```bash
# Container health overview
make ps

# Tail all logs
make logs

# Tail a specific service
make logs-worker
make logs-api
make logs-ingestor

# Postgres shell
make shell-db

# Redis shell
make shell-redis

# Inspect feature store keys
make inspect-features

# Parquet lake contents
make lake CMD="list"
make lake CMD="counts demand_events --last 6"

# Manually trigger the batch pipeline
make worker-pipeline

# Manually trigger training
make train

# Hot-reload Production + Staging models in the API
make reload

# Full infra verify (health checks + endpoint assertions)
make verify
```
