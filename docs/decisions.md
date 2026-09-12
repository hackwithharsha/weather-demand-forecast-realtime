# Architecture Decision Records

---

## Why both Postgres rows and Parquet files?

### Context

The ingestor consumes events from Kafka and currently writes every row to
Postgres (`raw.demand_events`, `raw.weather_readings`). Starting with the
storage-layer milestone, the ingestor also writes hourly Parquet partitions to
MinIO at `s3://lake/raw/{table}/dt=YYYY-MM-DD/hour=HH/data.parquet`.

### Decision

Maintain **two storage paths** from a single writer:

| Path | Technology | Purpose |
|---|---|---|
| Operational (OLTP) | Postgres | Deduplication, joins, staging/marts |
| Analytical (OLAP) | Parquet on MinIO | ML training, DuckDB/Spark queries |

### Rationale

**Postgres is the system of record.**

- `ON CONFLICT (event_id) DO NOTHING` guarantees idempotent ingestion; reprocessing Kafka partitions is safe.
- Postgres supports OLTP queries: filtering by city/time, joining demand events with weather readings, materialising staging/mart tables via future dbt models.
- Row-level granularity makes it straightforward to backfill or correct individual events.

**Parquet/MinIO is the analytical read path.**

- Columnar, Snappy-compressed files are an order of magnitude more efficient for full-table scans (ML feature extraction, batch aggregations) than Postgres heap pages.
- DuckDB can query S3 Parquet directly with zero ETL overhead; Spark and pandas also read Parquet natively.
- Offloading analytical reads to MinIO removes query pressure from Postgres during model training runs.
- S3-compatible object storage is cheap, durable, and scales horizontally without schema migrations.

**Single writer avoids dual-write inconsistencies.**

The ingestor is the only process that writes to both stores. Parquet partitions
are derived from Postgres rows *after a full hour is committed*, so the
analytical path is a consistent, delayed projection of the operational store —
not an independent write path that could diverge.

**Hourly partitions fit batch ML workflows.**

Training pipelines typically operate on hour- or day-level windows. Writing one
Parquet file per table per hour keeps partition sizes manageable (~MB range at
typical event rates) and maps naturally onto date/hour filtering in DuckDB or
Spark partition pruning.

### Trade-offs

- **~1-hour lag** between event ingestion and Parquet availability. This is acceptable for batch ML but unsuitable for near-real-time feature stores (a future concern).
- **Storage duplication**: data lives in both Postgres and MinIO. The redundancy is intentional and the cost is low relative to the read-path benefits.
- If the ingestor restarts, already-written Parquet keys are tracked only in process memory (`_written` set). On restart, the writer will re-query and re-upload affected hours harmlessly (idempotent `put_object` overwrites the same key with identical content).

---

## Why versioned SQL files instead of dbt (for now)

### Context

The batch pipeline transforms `raw.*` → `staging.*` → `marts.*` in Postgres.
The staging step (raw → staging) uses SQL-based transforms: deduplication on
`event_id`, null validation, reject logging, and hourly aggregation.  These
transforms live in `services/worker/sql/staging/001_demand.sql` and
`002_weather.sql`, executed by a thin Python `SqlRunner` inside the APScheduler
worker.

### Why not dbt right now

**1. Process model mismatch.**
dbt is a CLI tool (`dbt run`) designed to be invoked as a command.  The worker
is a long-running Python process managed by APScheduler.  Calling dbt via
`subprocess.run(["dbt", "run"])` from inside a scheduler job is possible but
operationally fragile: it requires a separate dbt installation in the container,
a `profiles.yml` on disk (with credentials), and adds a shell subprocess to
the failure surface.  The SQL runner avoids all of this — it executes SQL
directly over the existing psycopg2 connection.

**2. Project scaffolding overhead before first value.**
A dbt project requires `dbt_project.yml`, a `models/` directory tree,
`profiles.yml` (or `dbt_project.yml` env-var overrides), and at least a basic
understanding of dbt's Jinja macro system.  For two staging models, that
overhead delivers no additional correctness — it just gates the first working
pipeline on a yak-shaving exercise.

**3. Incremental strategy incompatibility.**
dbt's incremental materialisation uses `{{ this }}` and `is_incremental()`
macros that are evaluated at compile time.  The worker computes the
`window_start` / `window_end` parameters at runtime in Python and passes them
as psycopg2 bind parameters.  Bridging these two parameter models requires
dbt variables (`--vars`) or environment variables, which complicates the
scheduler-to-dbt hand-off without adding clarity.

**4. No lineage graph yet worth documenting.**
dbt's most compelling feature is its DAG — the ability to `ref()` one model
from another, automatically resolve dependencies, and render a documented
lineage graph.  With two staging tables and one mart, the DAG is a straight
line.  The cost-to-value ratio of maintaining dbt metadata for a two-node
graph is poor.

**5. Test framework duplication.**
dbt's built-in schema tests (`not_null`, `unique`, `accepted_values`) are
valuable at scale.  The `staging.rejects` table already captures the same
information in a queryable, log-friendly form.  Adding dbt tests now would
duplicate that signal rather than replace it.

### When to migrate to dbt

The right trigger is when **any two of these are true**:

| Signal | Threshold |
|---|---|
| Number of staging/marts models | ≥ 6 |
| Cross-model `ref()` needed | any model references another by name |
| Multi-environment deploys | dev schema ≠ staging schema ≠ prod schema |
| Documentation demanded | downstream teams consume the lineage graph |
| CI test coverage of SQL | > 0 schema tests wanted without writing pytest |

At that point, dbt pays for its setup cost immediately.  The versioned SQL
files in `sql/staging/` map cleanly onto dbt models: each `.sql` file becomes
a `models/staging/stg_<name>.sql` file with its `DISTINCT ON` and
`CONCAT_WS` logic intact.  The `SqlRunner` execution loop is replaced by
`dbt run --select staging`.

### What the current approach gives instead

- **SQL is the single source of truth** for transform logic — reviewable,
  diffable, and testable without running the full pipeline.
- **Execution order is explicit** via numeric filename prefixes (`001_`, `002_`).
- **Idempotency is enforced** by `ON CONFLICT` clauses, not by dbt's
  `is_incremental()` macro.
- **Rejects are first-class** — `staging.rejects` is a queryable audit trail
  that dbt's built-in tests do not produce by default.

---

## Why ClickHouse for the ML analytical read path

### Context

ML training pipelines scan the full `city_hour_features` table (all cities,
all time), compute rolling aggregates, and feed them into `fit_pipeline()`.
Running these scans against the Postgres `marts.city_hour_features` table
blocks the OLTP path and degrades ingestor throughput.  The question was
whether to add a purpose-built OLAP store or accept the Postgres scan cost.

### Decision

Add **ClickHouse** (`clickhouse/clickhouse-server:24.3-alpine`) under the `ml`
Compose profile, with a `MergeTree` table mirroring the mart schema:

```
ENGINE = MergeTree()
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (city, hour_ts)
```

The mart sync is a future worker step: after every `run_marts()` pass, a bulk
`INSERT INTO ml.city_hour_features SELECT ... FROM marts.city_hour_features`
brings ClickHouse current.  The operational Postgres table remains the source
of truth; ClickHouse holds a derived replica.

### Benchmark

`tools/bench.py` loads **10 M synthetic rows** (5 cities × 2 M hours, Gaussian
demand, 1 % lag nulls, 5 % weather nulls) into both stores and runs the
same wide aggregation three times each, reporting the minimum elapsed time.

**Query** (city × hour-of-day group-by, 10 aggregates):

```sql
SELECT city, <hour_extract>,
    AVG(total_demand), STDDEV(total_demand),
    AVG(demand_lag_1h), AVG(demand_lag_24h), AVG(demand_roll_24h),
    AVG(temperature_c), AVG(humidity_pct),
    SUM(<holiday>), COUNT(*)
FROM <table>
GROUP BY city, <hour_extract>
ORDER BY city, <hour_extract>
```

**Results** (Docker Compose, 4 vCPU / 8 GB):

| Database   | Min of 3 runs | Rows / second |
|------------|--------------|---------------|
| Postgres   | 11.2 s       | ~0.9 M / s    |
| ClickHouse | 0.38 s       | ~26 M / s     |
| **Speedup**| **~29 ×**    |               |

Run the benchmark yourself:

```bash
make bench
```

### Rationale

**ClickHouse is purpose-built for this access pattern.**

- The `ORDER BY (city, hour_ts)` primary key places all data for a city
  contiguously on disk; range scans over a city window read the minimum number
  of granules.
- `PARTITION BY toYYYYMM(hour_ts)` prunes entire monthly parts when the
  training window is restricted by date (common for rolling-window jobs).
- `LowCardinality(String)` for the 5-value city column encodes the column as a
  dictionary, halving the memory footprint and improving scan throughput.
- ClickHouse processes data in 8 192-row granules using SIMD-vectorised
  aggregation; Postgres processes rows one at a time via the executor node tree.

**Postgres is kept for writes and OLTP queries.**

- `ON CONFLICT (event_id) DO NOTHING` stays in Postgres — ClickHouse's
  MergeTree engine does not enforce uniqueness.
- Staging and mart transforms run via psycopg2 against Postgres; no SQL dialect
  porting is required.
- ClickHouse holds a derived read replica, not the source of truth.

### Trade-offs

| | Postgres | ClickHouse |
|---|---|---|
| Write semantics | ACID, `ON CONFLICT` upserts | Append-only; dedup via `ReplacingMergeTree` (not yet needed) |
| Schema changes | Alembic migrations | `ALTER TABLE … ADD COLUMN` DDL (fast) |
| Analytical scans | Slow at 10 M+ rows | Fast by design |
| Operational queries | Row-level access, joins | Expensive cross-row lookups |
| Sync lag | Real-time (source) | ~5 min batch sync (acceptable for training) |

---

## HSET vs SET for the feature store

### Context

The nightly batch sync reads `marts.route_features_daily` and writes one
feature vector per route into Redis.  The two obvious Redis data structures
are a plain string key (`SET feat:route:london <json>`) and a hash
(`HSET feat:route:london field1 v1 field2 v2 …`).

### Decision

Use **`HSET` with individual field-level writes**.  The sync calls
`pipe.hset(key, mapping={feat: value, …})` for each route inside a pipeline.
The `SET` command is explicitly forbidden in `feature_store.py` and
documented with an inline comment explaining why.

### Rationale

**Concurrent writers can safely update disjoint fields.**

Today only the nightly batch writes to `feat:route:{id}`.  In the near term
an online pipeline will compute `lead_time_p50` / `lead_time_p90` from the
live booking stream and update those two fields every 5 minutes.  With `SET`,
each writer holds the full JSON blob; a write-after-write from the online
pipeline overwrites the offline `avg_bookings_90d` computed by the batch, or
vice versa.  With `HSET`, the online pipeline calls
`HSET feat:route:london lead_time_p50 1.8 lead_time_p90 6.2` and touches
only those two fields; all other fields remain untouched.

**The API reads individual fields cheaply.**

`HGET feat:route:london avg_bookings_90d` fetches one field in O(1) without
parsing JSON.  `HMGET feat:route:london f1 f2 …` fetches a subset.  Both
are impossible with a string key without deserialising the whole blob.

**TTL semantics are per-key, not per-field — and that is correct.**

A single key-level TTL covers the entire feature vector.  If the batch job
fails, the whole key expires together rather than leaving a partial vector
where half the fields are fresh and half are stale from two nightly cycles
ago.  The `batch_computed_at` hash field gives the API a precise staleness
signal to enforce `freshness_sla` from the registry.

**No atomicity concern for the batch job.**

The batch sync writes every field for a route in a single `HSET` call inside
a Redis pipeline, so the hash is either fully updated or not at all (the
pipeline is flushed in one network round-trip after all routes are queued).

### Trade-offs

- Redis hashes have slightly higher per-key overhead than strings (one hash
  object vs one string).  At the scale of a few hundred routes with ~15
  fields each this is negligible.
- Monitoring for "all fields present" requires checking hash length or
  specific field existence; with JSON you would check the key exists.  The
  `batch_computed_at` field acts as the canary: its absence signals an
  incomplete write.

---

## Sliding-window counters: Redis sorted sets vs counter-per-minute buckets

### Context

The stream-features consumer computes three sliding-window metrics per route
from `demand.events.v1`:

| Metric | Window |
|---|---|
| `searches_5m` | 5 minutes of search events |
| `bookings_15m` | 15 minutes of booking events |
| `look_to_book_1h` | bookings ÷ searches over 1 hour |

Both must be recomputed on every incoming event and written to
`feat:route:{route_id}` as HSET fields.  The implementation choice is
between two Redis data structures.

### Option A — counter-per-minute buckets

Maintain one `INCR` key per (route, event_type, minute):

```
INCR feat:stream:{route_id}:searches:min:202401170234   # expires in 2h
```

To answer "searches in the last 5 minutes": issue 5 separate `GET` commands
and sum them.  For `look_to_book_1h`: 60 GETs for bookings + 60 GETs for
searches = 120 round-trips (or one MGET of 120 keys).

**Why this was rejected.**

- **Clock alignment.** Minute-bucket boundaries are wall-clock minutes, not
  event-time minutes.  A burst of events arriving at 14:59:59 and 15:00:01
  ends up in different buckets even though they are 2 seconds apart.  The
  window query must either accept this ±1 minute error or manage partial-
  bucket arithmetic.

- **Query overhead scales with window width.** `look_to_book_1h` requires
  reading 120 keys per event (60 search buckets + 60 booking buckets).  Any
  future addition of a wider window (e.g. `searches_24h`) would multiply that.

- **TTL management is subtle.** Each bucket key needs an independent TTL long
  enough to cover the longest window.  Setting TTLs incorrectly silently
  loses data.  Forgetting to set TTL leaks keys indefinitely.

### Option B — Redis sorted sets (chosen)

Maintain one sorted set per (route, event_type):

```
feat:stream:{route_id}:searches   ZSET — score = sim_ts epoch, member = event_id
feat:stream:{route_id}:bookings   ZSET — score = sim_ts epoch, member = event_id
```

On every demand event, run a single pipelined round-trip:

```
ZADD   feat:stream:{city}:searches   {epoch}  {event_id}   # if event_type=='search'
ZREMRANGEBYSCORE ...searches 0 (epoch-3601)  # trim to 1h
ZREMRANGEBYSCORE ...bookings 0 (epoch-3601)  # trim to 1h
ZCOUNT ...searches  (epoch-300)  epoch        # → searches_5m
ZCOUNT ...bookings  (epoch-900)  epoch        # → bookings_15m
ZCOUNT ...searches  (epoch-3600) epoch        # → searches_1h
ZCOUNT ...bookings  (epoch-3600) epoch        # → bookings_1h
```

`results[-4:]` from `pipe.execute()` yield the four counts regardless of
whether a ZADD was issued.  `look_to_book_1h = bookings_1h / max(1, searches_1h)`.

### Why sorted sets win

| Property | Counter buckets | Sorted sets |
|---|---|---|
| Window queries per event | 5 + 15 + 120 reads | 1 pipeline round-trip |
| Window accuracy | ±1 minute bucket | Exact (sim_ts to the second) |
| Clock alignment issues | Yes | No |
| New window width | N more GETs | One more ZCOUNT in same pipeline |
| Deduplication (at-least-once) | Count inflates on re-delivery | ZADD with same member is idempotent |
| Memory per route | Negligible (counters) | O(events/hour) — ~50 KB at 1 000 events/h |
| Trim mechanism | Per-key TTL | `ZREMRANGEBYSCORE` on same pipeline |

The only cost of sorted sets is memory: each member stores an event_id string
(~36 bytes for a UUID-4) plus the 8-byte score.  At a generous 1 000 events/
hour on a busy route with 100 routes, that is roughly 100 routes × 2 sets ×
1 000 members × ~50 bytes = **10 MB** — negligible against a typical Redis
instance.

**`sim_ts` as the score, not `now()`.**  Using the event's business timestamp
as the sorted-set score means the window is point-in-time correct even when
replaying backfill events.  Using wall-clock `now()` would assign future scores
to late-arriving events and silently inflate windows during replay.

**ZADD `event_id` as member = at-least-once idempotency.**  Confluent Kafka
with `enable.auto.commit=False` and per-message commits delivers at-least once.
ZADD with the same `(score, member)` pair is a no-op, so re-delivered events
do not inflate counts.

### Interaction with the batch job

The sorted sets use a separate key namespace (`feat:stream:{route_id}:…`)
from the route feature hashes (`feat:route:{route_id}`).  The stream consumer
only calls `HSET feat:route:{route_id}` with the *computed* aggregate values
(not raw event storage).  The batch job's HSET writes disjoint fields
(`avg_bookings_90d`, etc.) and never touches the sorted set keys.

### Benchmark

`tools/bench_sliding_window.py` measures end-to-end write latency of one
`update_demand_features()` call at increasing concurrency.  Each call is
exactly two Redis round-trips:

- **RT1** — `pipeline( ZADD? + ZREMRANGEBYSCORE×2 + ZCOUNT×4 ).execute()`
- **RT2** — `r.hset(route_key, mapping={4 fields})`

**Setup**: Redis 7.4.11 in Docker on macOS (localhost port-forward), 10 routes,
70/30 search/booking split, 3 s warmup + 10 s measurement per concurrency level.

| Concurrent writers | Events/s | p50 (ms) | p95 (ms) | p99 (ms) | SLA ≤ 5 ms |
|---:|---:|---:|---:|---:|:---:|
| 1  |    870 | 0.96 | 2.01 |  4.14 | ✓ |
| 2  |  1,352 | 1.17 | 2.75 |  6.16 | ✗ |
| 4  |  2,207 | 1.54 | 3.17 |  6.73 | ✗ |
| 8  |  3,429 | 2.06 | 3.61 |  7.69 | ✗ |
| 16 |  2,550 | 4.54 | 14.20 | 35.45 | ✗ |

**p99 first exceeds 5 ms at ~1,350 events/s (2 concurrent writers).**
Single-writer safe rate: **870 events/s, p99 = 4.1 ms**.

The demand and weather stream consumers run as separate threads, so production
always has at least 2 concurrent Redis writers.  At realistic event volumes
(5 routes × 10–100 events/s = 50–500 events/s total), the system operates well
inside the SLA.

The throughput collapse above 8 threads (2,550 and 2,005 ev/s at 16 and 32
threads vs 3,429 at 8) reflects the CPython GIL serialising the Python-side
pipeline construction and connection pool contention, not a Redis bottleneck.

**Hardware note**: Docker-on-macOS adds ~0.5 ms per round-trip via the Linux VM
network bridge.  On production Linux with Redis in the same pod or subnet
(RTT < 0.3 ms), expect 3–5× higher throughput before the same p99 limit.

Run the benchmark:

```bash
python3 tools/bench_sliding_window.py
# optional flags:
#   --threads 1 2 4 8 16   (default: 1 2 4 8 16 32 64)
#   --measure 10            (seconds per level, default 10)
```

---

## Partial-failure handling for the nightly Redis sync

### Context

The nightly sync job writes one Redis hash per route in sequential batches.
If the process is killed mid-run — OOM, network partition, container restart —
some routes will have the freshly-computed features from tonight and others
will still hold last night's values.  The API has no way to tell which routes
are fresh and which are stale.

Two strategies were considered.

### Option A — shadow key namespace + atomic RENAME

Write all route hashes to a shadow namespace
(`shadow:feat:route:{id}`) and, once all writes complete, flip
the shadow keys into the live namespace with `RENAME`.

**Why this was rejected.**

1. **RENAME is incompatible with the HSET multi-writer model.**
   `RENAME src dst` replaces `dst` *in its entirety*.  As documented in
   §"HSET vs SET for the feature store", the live key `feat:route:{id}` is
   intentionally shared between the nightly batch (which writes the 12 offline
   features) and a future online pipeline (which will update `lead_time_p50` /
   `lead_time_p90` in near-real-time from the booking stream).  A RENAME from
   the shadow key would atomically destroy those online-written fields.
   The problem is fundamental: no amount of engineering around RENAME recovers
   the per-field coexistence guarantee that HSET provides.

2. **There is no atomic cross-key operation in Redis without Lua.**
   `RENAME` is per-key and O(1) but not atomic *across* N keys.  A Lua script
   can rename all N keys in a single server-side pass, but it blocks the Redis
   event loop for O(N) string copies — potentially tens to hundreds of
   milliseconds for hundreds of routes.  That is a significant latency spike
   on a shared Redis instance.

3. **Cross-slot RENAME is illegal in Redis Cluster.**
   `feat:route:london` and `shadow:feat:route:london` hash to different slots
   unless both keys use the same hash tag `{route_id}`.  Adding hash tags to
   every key name is a non-trivial schema migration.

4. **Shadow keys leak on crash.**
   If the job dies between writing shadow keys and performing the renames, the
   shadow namespace accumulates indefinitely.  A cleanup job adds operational
   complexity and is itself a failure surface.

### Option B — accept partial updates; write a sync completion sentinel

Keep the existing HSET-per-key writes exactly as they are.  Add a single
lightweight sentinel key, `feat:sync:completed_at`, that is written as the
**very last operation** of a successful `_sync_to_redis()` run and holds the
`batch_computed_at` value shared by all route hashes in that run.

```
feat:sync:completed_at  →  "2024-01-17T02:34:11.203+00:00"
feat:route:london       →  {avg_bookings_90d: "312.5", ...,
                            batch_computed_at: "2024-01-17T02:34:11.203+00:00"}
feat:route:tokyo        →  {avg_bookings_90d: "198.0", ...,
                            batch_computed_at: "2024-01-17T02:34:11.203+00:00"}
```

**Failure detection.**

After a crash mid-run the sentinel is stale (it holds the previous run's
`batch_computed_at`) while some route hashes hold the new run's
`batch_computed_at`.  The comparison is exact:

| `route.batch_computed_at` vs `feat:sync:completed_at` | Meaning |
|---|---|
| Equal | Key is confirmed complete — written by a run that finished. |
| Greater (newer) | Key was written in an in-progress or failed run. |
| Less (older) | Key is older than the last complete run — extremely stale. |

The API consults the sentinel when serving features and emits a
feature-freshness alert for any route whose `batch_computed_at` does not
match `feat:sync:completed_at`.  This is a precise per-route signal, not a
blanket "something went wrong" alert.

**Why the staleness window is acceptable.**

All route features have `freshness_sla_seconds = 86 400` (24 hours).  The
sync itself completes in seconds for realistic route counts.  A partial failure
therefore leaves affected routes stale by at most one nightly SLA period, and
the APScheduler re-raises exceptions so on-call is paged before the next run.
Because the Postgres upsert uses `ON CONFLICT DO UPDATE`, a manual re-run of
the sync for the same `feature_date` is always safe and will restore
consistency.

### Decision

**Option B.**  The sentinel costs one additional `SET` call per successful
sync run and adds precise observability without touching the HSET architecture.
Option A is architecturally incompatible with the multi-writer field model and
introduces more failure modes than it removes.

### Implementation

`feat:sync:completed_at` is a plain Redis string key (not a hash).  Its value
is the ISO-8601 UTC `batch_computed_at` string that was written into every
route hash during the same run.

`_sync_to_redis()` is wrapped in a `try/finally` block so the Redis connection
is always closed.  The sentinel write lives inside the `try` block after the
last `pipe.execute()` returns — it is only reachable if every batch succeeded.

```python
try:
    for batch in batches:
        pipe.execute()          # raises on network / Redis error
    r.set(SYNC_COMPLETED_AT_KEY, batch_computed_at)   # sentinel
finally:
    r.close()                   # always runs; no connection leak on failure
```

The `SYNC_COMPLETED_AT_KEY` constant (`"feat:sync:completed_at"`) lives in
`common.features.registry` alongside the other Redis key constants so the API
can import it without depending on the worker package.

---

## Feature registry: one definition, zero literals

### Context

Features appear in four places: the Postgres mart DDL, the SQL that populates
it, the batch sync job that writes to Redis, and the API that reads from
Redis.  Without a shared contract, each service maintains its own list of
feature-name strings, leading to drift: the API silently reads a field that
was renamed in the mart, or the batch writes a field the API never reads.

### Decision

All feature metadata lives in a single **`FeatureRegistry`** instance
(`libs/common/common/feature_registry.py`):

```python
ROUTE_FEATURES = FeatureRegistry([
    FeatureDef(name="avg_bookings_90d", dtype="float64",
               source="marts.route_features_daily", freshness_sla="PT24H",
               owner="demand-team", ...),
    ...
])
```

The registry is the sole authoritative list of feature names.  Every other
file that needs a feature name imports the registry and calls
`ROUTE_FEATURES.names()` or `ROUTE_FEATURES["avg_bookings_90d"]`.  No file
that is not `feature_registry.py` is permitted to contain a feature-name
string literal (`"avg_bookings_90d"`, etc.) outside comments.

`tests/test_feature_store.py::TestNoStringLiterals` enforces this by parsing
`feature_store.py` with Python's `ast` module and failing the test suite if
any `ast.Constant` node matches a registered feature name.

### What each field in `FeatureDef` is used for

| Field | Used by |
|---|---|
| `name` | Postgres column name, Redis hash field name, registry key |
| `dtype` | API casts the Redis string back to a Python type using this |
| `source` | Documentation; batch job logs it on mismatch |
| `freshness_sla` | API compares against `batch_computed_at`; emits alert if stale |
| `owner` | Paged when a quality assertion fails for this feature |
| `description` | Surfaced in the feature catalogue and monitoring dashboards |

### Adding a feature

1. Add a `FeatureDef` to `ROUTE_FEATURES` in `feature_registry.py`.
2. Add the column to the Alembic migration (new revision).
3. Add the column to `sql/feature_store/001_route_features_daily.sql`.
4. Run `make migrate`.

No changes to `feature_store.py` or the API are needed — both iterate
`ROUTE_FEATURES.names()` dynamically.
