-- 001_route_features_daily.sql
-- Compute route-level features for CURRENT_DATE - 1 and upsert into
-- marts.route_features_daily.
--
-- This file lives in sql/feature_store/ (not sql/marts/) so run_marts()
-- never runs it.  It is executed by feature_store.run_route_mart() with
-- params={} — no substitution parameters are needed because the reference
-- date is always CURRENT_DATE - 1 (yesterday as of batch run time).
--
-- Route identity
-- --------------
-- In v1 route_id = city.  The join key to the underlying mart data is
-- therefore ``city``.  When the data model grows to include explicit
-- origin/destination pairs, change the alias here and add a migration.
--
-- Window semantics
-- ----------------
-- All lookback windows use strict ``< CURRENT_DATE`` on the upper bound so
-- today's partial-day data is never included in a historical aggregate.
-- This prevents data leakage into features that are subsequently used to
-- train models or serve forecasts.
--
-- Feature: avg_bookings_90d
-- -------------------------
-- Average of daily booking counts (event_count aggregated to date) over the
-- 90 calendar days that end one day before CURRENT_DATE.
-- NULL when no data exists in the window.
--
-- Feature: seasonality_{dow}
-- --------------------------
-- For each ISO weekday 1-Mon … 7-Sun: (avg demand on that DOW) / (route
-- overall avg daily demand).  Pivot is done with MAX(...) FILTER (WHERE dow=N)
-- which is the standard PostgreSQL conditional aggregation pattern.
-- NULL for any DOW that has never appeared in the historical data.
--
-- Feature: lead_time_p50 / lead_time_p90
-- ----------------------------------------
-- PERCENTILE_CONT applied to (ingested_at - sim_ts) in hours.  Negative
-- lead times (clock skew or SIM_SPEED artefacts) are excluded.  Window:
-- 90 days before CURRENT_DATE.
--
-- Feature: cancellation_rate_180d
-- --------------------------------
-- Fraction of events with event_type = 'cancellation' over 180 days.
-- Defaults to 0.0 when no events match (COALESCE on the LEFT JOIN).
--
-- Feature: elasticity_estimate
-- -----------------------------
-- Pearson r between temperature_c and total_demand over all history before
-- CURRENT_DATE.  CORR() is a SQL standard aggregate; it returns NULL when
-- fewer than 2 non-null pairs exist.
--
-- Returns one row:
--   routes_assembled BIGINT — rows in the assembled CTE
--   routes_upserted  BIGINT — rows written (INSERT or UPDATE)

WITH

-- ── Daily demand per route ────────────────────────────────────────────────
daily AS (
    SELECT
        city                                    AS route_id,
        (hour_ts AT TIME ZONE 'UTC')::date      AS day,
        SUM(total_demand)                       AS daily_demand,
        SUM(event_count)                        AS daily_bookings
    FROM marts.city_hour_features
    WHERE total_demand IS NOT NULL
      AND (hour_ts AT TIME ZONE 'UTC')::date < CURRENT_DATE
    GROUP BY city, (hour_ts AT TIME ZONE 'UTC')::date
),

-- ── 90-day average bookings ───────────────────────────────────────────────
avg_bookings AS (
    SELECT
        route_id,
        AVG(daily_bookings) AS avg_bookings_90d
    FROM daily
    WHERE day >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY route_id
),

-- ── Seasonality: per-DOW ratio to overall average ─────────────────────────
overall_avg AS (
    SELECT route_id, AVG(daily_bookings) AS mu
    FROM daily
    GROUP BY route_id
),

dow_avg AS (
    SELECT
        route_id,
        EXTRACT(ISODOW FROM day)::int           AS dow,   -- 1=Mon … 7=Sun
        AVG(daily_bookings)                     AS dow_mu
    FROM daily
    GROUP BY route_id, EXTRACT(ISODOW FROM day)::int
),

-- Pivot 7 rows → 1 row per route using conditional aggregation.
-- NULLIF(mu, 0) guards against division by zero on routes with all-zero demand.
seasonality AS (
    SELECT
        da.route_id,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 1) AS seasonality_mon,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 2) AS seasonality_tue,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 3) AS seasonality_wed,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 4) AS seasonality_thu,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 5) AS seasonality_fri,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 6) AS seasonality_sat,
        MAX(da.dow_mu / NULLIF(oa.mu, 0)) FILTER (WHERE da.dow = 7) AS seasonality_sun
    FROM dow_avg da
    JOIN overall_avg oa USING (route_id)
    GROUP BY da.route_id
),

-- ── Lead time percentiles (hours), last 90 days ───────────────────────────
-- Negative lead times arise from SIM_SPEED artefacts and are excluded.
lead_times AS (
    SELECT
        city AS route_id,
        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (ingested_at - sim_ts)) / 3600.0
        ) AS lead_time_p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (ingested_at - sim_ts)) / 3600.0
        ) AS lead_time_p90
    FROM raw.demand_events
    WHERE sim_ts     IS NOT NULL
      AND ingested_at IS NOT NULL
      AND ingested_at >= CURRENT_DATE - INTERVAL '90 days'
      AND ingested_at <  CURRENT_DATE
      AND EXTRACT(EPOCH FROM (ingested_at - sim_ts)) >= 0
    GROUP BY city
),

-- ── Cancellation rate, last 180 days ─────────────────────────────────────
-- event_type = 'cancellation' identifies cancelled demand events.
-- COALESCE(…, 0.0) on the outer join gives 0 for routes with no cancellations
-- rather than NULL.
cancellations AS (
    SELECT
        city AS route_id,
        SUM(CASE WHEN event_type = 'cancellation' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0)               AS cancellation_rate_180d
    FROM raw.demand_events
    WHERE ingested_at >= CURRENT_DATE - INTERVAL '180 days'
      AND ingested_at <  CURRENT_DATE
    GROUP BY city
),

-- ── Elasticity: Pearson r(temperature_c, total_demand) ───────────────────
-- Computed over all available history so the estimate is as stable as possible.
-- CORR() returns NULL with < 2 non-null pairs.
elasticity AS (
    SELECT
        city AS route_id,
        CORR(
            temperature_c::double precision,
            total_demand::double precision
        ) AS elasticity_estimate
    FROM marts.city_hour_features
    WHERE temperature_c IS NOT NULL
      AND total_demand   IS NOT NULL
      AND (hour_ts AT TIME ZONE 'UTC')::date < CURRENT_DATE
    GROUP BY city
),

-- ── Assemble one row per route ────────────────────────────────────────────
assembled AS (
    SELECT
        ab.route_id,
        (CURRENT_DATE - 1)                      AS feature_date,
        ab.avg_bookings_90d,
        s.seasonality_mon,
        s.seasonality_tue,
        s.seasonality_wed,
        s.seasonality_thu,
        s.seasonality_fri,
        s.seasonality_sat,
        s.seasonality_sun,
        lt.lead_time_p50,
        lt.lead_time_p90,
        COALESCE(c.cancellation_rate_180d, 0.0) AS cancellation_rate_180d,
        el.elasticity_estimate,
        now() AT TIME ZONE 'UTC'                AS feature_computed_at
    FROM avg_bookings  ab
    LEFT JOIN seasonality  s  USING (route_id)
    LEFT JOIN lead_times   lt USING (route_id)
    LEFT JOIN cancellations c USING (route_id)
    LEFT JOIN elasticity   el USING (route_id)
),

-- ── Upsert ────────────────────────────────────────────────────────────────
upserted AS (
    INSERT INTO marts.route_features_daily (
        route_id,              feature_date,
        avg_bookings_90d,
        seasonality_mon,       seasonality_tue,       seasonality_wed,
        seasonality_thu,       seasonality_fri,       seasonality_sat,
        seasonality_sun,
        lead_time_p50,         lead_time_p90,
        cancellation_rate_180d,
        elasticity_estimate,
        feature_computed_at
    )
    SELECT
        route_id,              feature_date,
        avg_bookings_90d,
        seasonality_mon,       seasonality_tue,       seasonality_wed,
        seasonality_thu,       seasonality_fri,       seasonality_sat,
        seasonality_sun,
        lead_time_p50,         lead_time_p90,
        cancellation_rate_180d,
        elasticity_estimate,
        feature_computed_at
    FROM assembled
    ON CONFLICT (route_id, feature_date) DO UPDATE SET
        avg_bookings_90d       = EXCLUDED.avg_bookings_90d,
        seasonality_mon        = EXCLUDED.seasonality_mon,
        seasonality_tue        = EXCLUDED.seasonality_tue,
        seasonality_wed        = EXCLUDED.seasonality_wed,
        seasonality_thu        = EXCLUDED.seasonality_thu,
        seasonality_fri        = EXCLUDED.seasonality_fri,
        seasonality_sat        = EXCLUDED.seasonality_sat,
        seasonality_sun        = EXCLUDED.seasonality_sun,
        lead_time_p50          = EXCLUDED.lead_time_p50,
        lead_time_p90          = EXCLUDED.lead_time_p90,
        cancellation_rate_180d = EXCLUDED.cancellation_rate_180d,
        elasticity_estimate    = EXCLUDED.elasticity_estimate,
        feature_computed_at    = EXCLUDED.feature_computed_at
    RETURNING 1
)

SELECT
    (SELECT COUNT(*) FROM assembled)::bigint AS routes_assembled,
    (SELECT COUNT(*) FROM upserted)::bigint  AS routes_upserted
;
