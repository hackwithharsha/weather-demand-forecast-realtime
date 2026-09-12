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
