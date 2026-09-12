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
