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
