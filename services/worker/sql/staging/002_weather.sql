-- 002_weather.sql
-- raw.weather_readings → staging.weather_hourly
--
-- Parameters (psycopg2 %(name)s style)
--   %(window_start)s  TIMESTAMPTZ  start of ingested_at window (inclusive)
--   %(window_end)s    TIMESTAMPTZ  end   of ingested_at window (exclusive)
--
-- Steps mirror 001_demand.sql:
--   1. source      — project and cast columns
--   2. deduped     — keep earliest ingest per event_id
--   3. classified  — tag rows missing city or polled_at
--   4. ins_rejects — log invalid rows to staging.rejects (idempotent)
--   5. ins_staging — average weather metrics per (city, hour) and upsert
--
-- Returns one row: (rejects_written BIGINT, staging_written BIGINT)
--
-- Why AVG for temperature/humidity and SUM for precip_mm?
-- --------------------------------------------------------
-- Temperature and humidity are instantaneous readings; averaging across
-- multiple polls within an hour gives a representative hourly value.
-- Precipitation accumulates over time, so the sum of polls within an
-- hour approximates the hourly total — consistent with how weather APIs
-- report precip_mm as an accumulation figure.

WITH

-- ── 1. source ─────────────────────────────────────────────────────────────
source AS (
    SELECT
        id,
        city,
        polled_at,
        temperature_c::DOUBLE PRECISION      AS temperature_c,
        humidity_pct::DOUBLE PRECISION       AS humidity_pct,
        precip_mm::DOUBLE PRECISION          AS precip_mm,
        event_id,
        ingested_at
    FROM raw.weather_readings
    WHERE ingested_at >= %(window_start)s
      AND ingested_at <  %(window_end)s
),

-- ── 2. deduped ────────────────────────────────────────────────────────────
deduped AS (
    SELECT DISTINCT ON (event_id)
        id,
        city,
        polled_at,
        temperature_c,
        humidity_pct,
        precip_mm,
        event_id,
        ingested_at
    FROM source
    ORDER BY event_id, ingested_at ASC
),

-- ── 3. classified ─────────────────────────────────────────────────────────
classified AS (
    SELECT
        *,
        NULLIF(
            CONCAT_WS(
                ',',
                CASE WHEN city      IS NULL THEN 'null_city'      END,
                CASE WHEN polled_at IS NULL THEN 'null_polled_at' END
            ),
            ''
        ) AS reject_reason
    FROM deduped
),

-- ── 4. ins_rejects ────────────────────────────────────────────────────────
ins_rejects AS (
    INSERT INTO staging.rejects
        (source_table, source_id, event_id, reject_reason, rejected_at)
    SELECT
        'weather_readings',
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

-- ── 5. ins_staging ────────────────────────────────────────────────────────
ins_staging AS (
    INSERT INTO staging.weather_hourly
        (city, hour_ts, temperature_c, humidity_pct, precip_mm)
    SELECT
        city,
        date_trunc('hour', polled_at)  AS hour_ts,
        AVG(temperature_c)             AS temperature_c,
        AVG(humidity_pct)              AS humidity_pct,
        SUM(precip_mm)                 AS precip_mm
    FROM classified
    WHERE reject_reason IS NULL
    GROUP BY city, date_trunc('hour', polled_at)
    ON CONFLICT (city, hour_ts) DO UPDATE
        SET temperature_c = EXCLUDED.temperature_c,
            humidity_pct  = EXCLUDED.humidity_pct,
            precip_mm     = EXCLUDED.precip_mm
    RETURNING 1
)

SELECT
    (SELECT COUNT(*) FROM ins_rejects)::BIGINT AS rejects_written,
    (SELECT COUNT(*) FROM ins_staging)::BIGINT AS staging_written
;
