# Operations Runbook

For each alert defined in `infra/prometheus/alerts.yml`:
- **Symptom** — what you observe when the alert fires
- **Likely causes** — the most probable root causes, ordered by frequency
- **Diagnostic commands** — what to run first
- **Fix** — how to resolve each cause

Open Prometheus at http://localhost:9090 and Grafana at http://localhost:3001
to correlate metrics while following these steps.

---

## Alert index

| Alert | Severity | Fires when |
|---|---|---|
| [KafkaConsumerLagHigh](#kafkaconsumerlaghigh) | warning | consumer lag > 1 000 messages for 5 min |
| [FeatureDriftHigh](#featuredrif​thigh) | warning | any feature drift_score > 0.2 (immediate) |
| [PredictionLatencyHigh](#predictionlatencyhigh) | warning | /predict p99 > 500 ms for 5 min |
| [ModelNotTrainedRecently](#modelnottrainedrecently) | warning | no training run in 48 h, or metric absent |

---

## KafkaConsumerLagHigh

```
redpanda_kafka_consumer_group_lag > 1000   for: 5m
```

### Symptom

Alertmanager fires with the consumer group name, topic, and partition in the
labels.  The Grafana **Pipeline Health** dashboard shows the lag counter
climbing.  No new rows appear in `raw.demand_events` or `raw.weather_readings`;
the Ingestor service is falling behind.

### Likely causes

1. **Ingestor container crashed or is OOM-killed** — most common
2. **Postgres write latency spike** — batch inserts timing out or taking too long
3. **Producer burst** — load test or seed job flooded the topic temporarily
4. **Redpanda rebalancing** — group rebalance after a broker restart (transient)
5. **Ingestor never started** — stream profile not running

### Diagnostic commands

```bash
# Which group and partition is lagging?
# Prometheus → Graph: redpanda_kafka_consumer_group_lag

# Is the ingestor running?
docker compose --profile stream ps ingestor

# Is it crashing / restarting?
docker compose --profile stream logs --tail=50 ingestor

# Is Redpanda itself healthy?
docker compose --profile stream ps redpanda
curl -s http://localhost:9644/v1/brokers | python3 -m json.tool

# Is Postgres accepting connections?
make shell-db
```

### Fix

| Cause | Fix |
|---|---|
| Ingestor container crashed | `docker compose --profile stream up -d ingestor` |
| Postgres connection failure | Check `POSTGRES_PASSWORD` in `.env`; `make shell-db` to verify |
| Postgres write latency | Increase `PARQUET_FLUSH_INTERVAL_S` batch window; check Postgres disk I/O |
| Producer burst (transient) | Monitor — lag recovers automatically once burst ends |
| Redpanda rebalancing | Wait ~60 s; lag should recover as the new leader is elected |
| Stream profile not running | `make up-stream` |

#### Replay after extended downtime

If the ingestor was down long enough that messages risk falling off the
retention window, or you need to reprocess a time range:

```bash
# Stop the ingestor before resetting offsets (group must be inactive)
docker compose --profile stream stop ingestor

# Reset demand + weather consumer groups to a specific timestamp
make replay GROUP=ingestor-demand  TS=2026-09-14T08:00:00
make replay GROUP=ingestor-weather TS=2026-09-14T08:00:00

# Restart
make up-stream
```

If messages have already expired from Redpanda, use the Parquet backfill path:

```bash
make backfill DATE_START=2026-09-14 DATE_END=2026-09-14
make worker-pipeline
```

---

## FeatureDriftHigh

```
drift_score > 0.2   for: 0m  (fires immediately)
```

### Symptom

Alertmanager fires with `feature="<name>"` in the label.  The Grafana
**Data Quality** dashboard shows one or more features in orange/red.  The
`drift_detected` column in `marts.drift_reports` is `true` for recent rows.
The worker logs contain `drift_high` structured log events.

The alert fires immediately (no `for` window) because `drift_score` is already
a windowed statistic computed by Evidently — a single noisy data point cannot
flip it.

### Likely causes

**Weather features** (`temperature_c`, `humidity_pct`, `precip_mm`):
1. Seasonal distribution shift — expected over months; a model refresh resolves it
2. Mock-weather server stuck returning stale/flat values
3. Weather API schema change causing null fields to be inserted

**Demand features** (`demand_lag_1h`, `demand_lag_24h`, `event_count`):
1. Genuine demand pattern change (holiday, campaign, new market)
2. Upstream ingestor lag — lag features populated from stale/missing Postgres rows
3. Feature store not refreshed — batch features in Redis are stale

### Diagnostic commands

```bash
# Which features are drifting and by how much?
# Prometheus → Graph:  drift_score   (filter by feature label)

# When did drift start?  (Grafana: Drift Score over 6 h panel)
make shell-db
# SELECT checked_at, feature_name, drift_score, drift_detected
#   FROM marts.drift_reports
#  WHERE drift_detected = true
#  ORDER BY checked_at DESC LIMIT 20;

# Worker drift logs
docker compose --profile stream logs --tail=100 worker | grep drift

# Is the mock-weather server returning sensible values?
make logs-mock-weather

# Is the ingestor keeping up?  (check KafkaConsumerLagHigh first)
docker compose --profile stream ps ingestor
```

**Drift score interpretation:**

| Range | Severity | Action |
|---|---|---|
| 0.10 – 0.20 | Moderate | Monitor trend; no action needed |
| 0.20 – 0.30 | Significant | Alert fires; investigate root cause |
| > 0.30 | Severe | Auto-retrain triggers if `AUTO_RETRAIN_ON_DRIFT=true` |

### Fix

```bash
# Option A: let the auto-retrain loop handle it
#   When AUTO_RETRAIN_ON_DRIFT=true (default), the worker triggers a training
#   run in a background thread once any score exceeds RETRAIN_DRIFT_THRESHOLD
#   (default 0.3).  A new Staging model appears in MLflow if it beats the
#   current Production model on the holdout split.
#
#   Cooldown: after any retrain (drift-triggered or weekly), further
#   drift-triggered runs are suppressed for RETRAIN_COOLDOWN_MINUTES (default
#   360 min / 6 h).  The worker logs "retrain_cooldown_active" with
#   remaining_minutes when a trigger is rejected.

# Option B: trigger training manually (bypasses the cooldown)
make train

# After training, promote Staging → Production via the UI (Model tab → Promote)
# or hot-reload the API's in-memory models without a restart:
make reload

# If drift is caused by stale feature store data, force a batch sync first:
docker compose --profile core --profile stream run --rm worker \
    python -m app.feature_store
```

---

## PredictionLatencyHigh

```
histogram_quantile(0.99,
  rate(http_request_duration_seconds_bucket{path="/predict"}[5m])
) > 0.5   for: 5m
```

### Symptom

Alertmanager fires after the p99 `/predict` latency has been above 500 ms for
five consecutive minutes.  The Grafana **Model Performance** dashboard shows
a rising p99 curve.  `/health` and `/ready` remain fast; only `/predict` is
affected (unless it's a Redis outage, which would affect all Postgres fallback
paths).

### Likely causes

1. **Redis feature cache miss** — API falls back to Postgres for every request
2. **Feature store not populated** — batch sync never ran or failed
3. **Redis down or restarting** — full fallback to Postgres for all reads
4. **Postgres query slow** — missing index, autovacuum, or disk pressure
5. **API CPU saturation** — insufficient container resources
6. **Model inference slow** — overly large model (too many features / HGB trees)

### Diagnostic commands

```bash
# Current p99 (Prometheus)
# histogram_quantile(0.99, rate(http_request_duration_seconds_bucket{path="/predict"}[5m]))

# API logs — look for slow_request or fallback_to_postgres events
docker compose --profile core logs --tail=100 api | grep predict

# Is Redis healthy?
make shell-redis
# > PING  → expect PONG
# > INFO server  → check uptime_in_seconds

# Feature store miss rate and loaded_at timestamps
curl -s http://localhost:8080/model/info | python3 -m json.tool

# Are feature keys present?
make inspect-features   # lists all feat:route:* keys in Redis

# Postgres query latency
make shell-db
# SELECT query, calls, mean_exec_time, max_exec_time
#   FROM pg_stat_statements
#  ORDER BY mean_exec_time DESC LIMIT 10;
```

### Fix

| Cause | Fix |
|---|---|
| Redis cache miss / stale features | Force sync: `docker compose --profile core --profile stream run --rm worker python -m app.feature_store` |
| Feature store never populated | Run full pipeline first: `make worker-pipeline`, then feature store sync above |
| Redis down | `docker compose restart redis`; feature sync will repopulate on restart |
| Postgres query slow | `ANALYZE <table>;` or `VACUUM ANALYZE;` inside `make shell-db`; add missing index |
| API CPU saturation | Increase `cpus` limit in docker-compose.yml api service; or scale replicas |
| Model inference slow | Retrain with fewer features or reduced `n_estimators` in HGB |
| Cold start after `make reload` | Brief spike (seconds); normal and self-resolving |

#### Force a feature-store sync

```bash
docker compose --profile core --profile stream run --rm worker \
    python -m app.feature_store
```

---

## ModelNotTrainedRecently

```
(time() - model_last_trained_timestamp > 172800)
or absent(model_last_trained_timestamp)   for: 0m
```

### Symptom

Alertmanager fires immediately.  Either the Pushgateway metric
`model_last_trained_timestamp` is absent (Pushgateway restarted and lost the
gauge, or the trainer has never completed a run), or the metric exists but is
older than 48 hours.  The Grafana **Model Performance** dashboard shows
"Last Trained" as `N/A` or a stale timestamp.  The model in production is
potentially diverging from the current data distribution.

### Likely causes

1. **Trainer job not triggered** — `make train` was never called, or the weekly cron thread in the worker hasn't fired yet
2. **Pushgateway restarted** — the gauge is held in memory and lost on restart
3. **MLflow unreachable** — trainer completes but cannot log or register the model
4. **Insufficient mart data** — fewer rows than `val_days` required by the trainer; training aborts
5. **PSI drift gate blocked promotion** — new model's feature distributions shifted too far from training set
6. **Holdout MAE gate blocked promotion** — new model was worse than the current Production model

### Diagnostic commands

```bash
# When was the metric last pushed?
curl -s http://localhost:9091/metrics | grep model_last_trained

# Trainer logs (look for errors or "promotion_rejected_*")
docker compose --profile ml logs --tail=100 trainer

# Is the Pushgateway running?
docker compose --profile obs ps pushgateway

# Is MLflow reachable?
curl -s http://localhost:5001/health

# How many mart rows are available?
make shell-db
# SELECT COUNT(*) FROM marts.route_features_daily;

# Registered model versions in MLflow
curl -s http://localhost:8080/model/versions | python3 -m json.tool

# Weekly auto-retrain scheduler status
docker compose --profile stream logs --tail=20 worker | grep weekly_retrain
```

### Fix

| Cause | Fix |
|---|---|
| Trainer never run | `make train` |
| Pushgateway restarted (metric lost) | `make train` — a fresh run re-pushes the gauge |
| MLflow unavailable | `make up-ml`; then `make train` |
| Insufficient mart data | `make worker-pipeline` to populate marts; then `make train` |
| PSI drift gate blocked promotion | Inspect `drift_report.json` in the MLflow run artifacts; if expected, `make train` after more data accumulates |
| Holdout MAE gate blocked promotion | New model was worse; wait for more data or `make train` again |

#### Trigger training manually

```bash
# Full train → register → promote-to-staging pipeline
make train

# After training completes, promote Staging → Production via the UI
# (Model tab → Promote), or hot-reload the API's in-memory models:
make reload
```

#### Weekly auto-retrain schedule

The worker schedules a retrain every Sunday at 04:00 UTC regardless of drift.
If the alert fires on a Monday and no manual `make train` has been run, verify
the weekly job fired:

```bash
docker compose --profile stream logs worker | grep weekly_retrain
```

If the worker was down over Sunday, run `make train` manually to reset the
Pushgateway gauge.

---

## General diagnostic commands

```bash
# Container health overview
make ps

# Tail all logs
make logs

# Tail a single service
make logs-worker
make logs-api
make logs-ingestor

# Postgres shell
make shell-db

# Redis shell
make shell-redis

# Inspect all feature store keys in Redis
make inspect-features

# Parquet lake contents
make lake CMD="list"
make lake CMD="counts demand_events --last 6"

# Manually run the batch pipeline (raw → staging → marts → scaler)
make worker-pipeline

# Manually trigger model training
make train

# Hot-reload Production + Staging models in the API (no restart)
make reload

# Full infra verify (health checks + endpoint assertions)
make verify

# Reset offsets for a consumer group to a specific timestamp
make replay GROUP=ingestor-demand TS=2026-09-14T08:00:00

# Backfill Postgres raw tables from Parquet lake
make backfill DATE_START=2026-09-14 DATE_END=2026-09-14
```
