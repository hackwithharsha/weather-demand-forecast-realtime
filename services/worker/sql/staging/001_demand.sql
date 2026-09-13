-- 001_demand.sql
-- raw.demand_events → staging.demand_hourly
--
-- Parameters (psycopg2 pyformat / named style)
--   %(window_start)s  TIMESTAMPTZ  start of ingested_at window (inclusive)
--   %(window_end)s    TIMESTAMPTZ  end   of ingested_at window (exclusive)
--
-- Steps inside a single data-modifying CTE (one Postgres round-trip):
--   1. source      — project and cast columns from the ingested_at window
--   2. deduped     — keep the first-ingested occurrence of each event_id;
--                    rows with NULL event_id cannot be deduplicated and are
--                    all retained
--   3. classified  — tag every row with a comma-separated reject_reason if
--                    any required column is NULL; clean rows get NULL reason
--   4. ins_rejects — INSERT tagged rows into staging.rejects;
--                    ON CONFLICT DO UPDATE refreshes reason + timestamp on
--                    re-runs so the table reflects the latest pipeline logic
--   5. ins_staging — aggregate clean rows by (city, business hour) and
--                    upsert into staging.demand_hourly
--
-- Returns one row: (rejects_written BIGINT, staging_written BIGINT)
-- The caller logs these values; no application logic branches on them.
--
-- Versioning note
-- ---------------
-- This file is executed on every pipeline run (not once like Alembic).
-- The numeric prefix controls execution order within the staging/ directory.
-- All writes use ON CONFLICT so re-runs are idempotent.

WITH

-- ── 1. source: window filter + explicit casts ─────────────────────────────
source AS (
    SELECT
        id,
        city,
        event_type,
        sim_ts,
        quantity::DOUBLE PRECISION      AS quantity,
        temperature_c::DOUBLE PRECISION AS temperature_c,
        condition,
        event_id,
        ingested_at
    FROM raw.demand_events
    WHERE ingested_at >= %(window_start)s
      AND ingested_at <  %(window_end)s
),

-- ── 2. deduped: one row per event_id (NULL event_ids kept as-is) ──────────
deduped AS (
    SELECT DISTINCT ON (event_id)
        id,
        city,
        event_type,
        sim_ts,
        quantity,
        temperature_c,
        condition,
        event_id,
        ingested_at
    FROM source
    -- For duplicate event_ids keep the earliest ingest; for NULLs the order
    -- is arbitrary but stable (PostgreSQL DISTINCT ON is deterministic when
    -- the ORDER BY is fully specified).
    ORDER BY event_id, ingested_at ASC
),

-- ── 3. classified: build reject_reason from all failing null checks ───────
classified AS (
    SELECT
        *,
        NULLIF(
            CONCAT_WS(
                ',',
                CASE WHEN city       IS NULL THEN 'null_city'       END,
                CASE WHEN event_type IS NULL THEN 'null_event_type' END,
                CASE WHEN sim_ts     IS NULL THEN 'null_sim_ts'     END,
                CASE WHEN quantity   IS NULL THEN 'null_quantity'   END
            ),
            ''
        ) AS reject_reason
    FROM deduped
),

-- ── 4. ins_rejects: log invalid rows ──────────────────────────────────────
ins_rejects AS (
    INSERT INTO staging.rejects
        (source_table, source_id, event_id, reject_reason, rejected_at)
    SELECT
        'demand_events',
        id,
        event_id,
        reject_reason,
        now()
    FROM classified
    WHERE reject_reason IS NOT NULL
    ON CONFLICT (source_table, source_id) DO UPDATE
        SET reject_reason = EXCLUDED.reject_reason,
            rejected_at   = EXCLUDED.rejected_at
    RETURNING 1
),

-- ── 5. ins_staging: aggregate clean rows and upsert ───────────────────────
ins_staging AS (
    INSERT INTO staging.demand_hourly
        (city, hour_ts, total_demand, event_count, avg_temp_c)
    SELECT
        city,
        date_trunc('hour', sim_ts)  AS hour_ts,
        SUM(quantity)               AS total_demand,
        COUNT(*)                    AS event_count,
        AVG(temperature_c)          AS avg_temp_c
    FROM classified
    WHERE reject_reason IS NULL
    GROUP BY city, date_trunc('hour', sim_ts)
    ON CONFLICT (city, hour_ts) DO UPDATE
        SET total_demand = EXCLUDED.total_demand,
            event_count  = EXCLUDED.event_count,
            avg_temp_c   = EXCLUDED.avg_temp_c
    RETURNING 1
)

SELECT
    (SELECT COUNT(*) FROM ins_rejects)::BIGINT AS rejects_written,
    (SELECT COUNT(*) FROM ins_staging)::BIGINT AS staging_written
;
