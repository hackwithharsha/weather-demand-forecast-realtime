-- 001_city_hour_features.sql
-- staging.demand_hourly + staging.weather_hourly → marts.city_hour_features
--
-- No parameters: reads the full staging tables so that lag columns at the
-- start of any lookback window are computed from genuine preceding rows rather
-- than being forced to NULL by an artificial window cutoff.
--
-- Feature inventory
-- -----------------
-- demand aggregates   total_demand, event_count  (pass-through from staging)
-- lagged demand       1 h, 24 h, 168 h           LAG window function
-- rolling means       3 h, 24 h                  AVG ROWS BETWEEN … 1 PRECEDING
-- cyclical hour       hour_sin, hour_cos          sin/cos of hour-of-day / 24
-- cyclical weekday    dow_sin,  dow_cos           sin/cos of ISO weekday / 7
-- weather join        temperature_c, humidity_pct, precip_mm  LEFT JOIN
-- is_holiday          FALSE placeholder           Python fills this in next
--
-- Non-leakage guarantee for rolling means
-- ----------------------------------------
-- ROWS BETWEEN N PRECEDING AND 1 PRECEDING excludes the current row from every
-- aggregate window.  The mean at time t uses only t-1 … t-N, never t itself.
-- This mirrors the pandas shift(1).rolling(N).mean() pattern it replaces.
--
-- Day-of-week convention
-- ----------------------
-- EXTRACT(ISODOW …) returns 1 = Monday … 7 = Sunday.
-- Subtracting 1 yields 0 = Monday … 6 = Sunday, matching Python's
-- datetime.weekday() / pandas dt.dayofweek — consistent with the previous
-- pandas implementation and with serving-time feature computation.
--
-- is_holiday placeholder
-- ----------------------
-- SQL cannot look up public-holiday calendars.  The INSERT writes FALSE for
-- every new row; the ON CONFLICT clause intentionally omits is_holiday from
-- the UPDATE set so existing values are preserved.  Python updates this column
-- via a bulk UPDATE from a temp table immediately after this query commits.
--
-- Returns one row: (staging_rows BIGINT, rows_upserted BIGINT)

WITH

-- ── Step 1: join demand and weather ───────────────────────────────────────
joined AS (
    SELECT
        d.city,
        d.hour_ts,
        d.total_demand,
        d.event_count,
        w.temperature_c,
        w.humidity_pct,
        w.precip_mm
    FROM       staging.demand_hourly  d
    LEFT JOIN  staging.weather_hourly w USING (city, hour_ts)
),

-- ── Step 2: compute all SQL-expressible features ──────────────────────────
-- Named window w supplies PARTITION BY + ORDER BY for both LAG and the
-- extended-frame AVG expressions below.
featured AS (
    SELECT
        city,
        hour_ts,
        total_demand,
        event_count::INTEGER          AS event_count,

        -- Lagged demand (NULL for the first lag_h rows per city)
        LAG(total_demand,   1) OVER w AS demand_lag_1h,
        LAG(total_demand,  24) OVER w AS demand_lag_24h,
        LAG(total_demand, 168) OVER w AS demand_lag_168h,

        -- Rolling means: ROWS BETWEEN N PRECEDING AND 1 PRECEDING
        -- → past values only; NULL when fewer than 1 preceding row exists
        AVG(total_demand) OVER (w ROWS BETWEEN  3 PRECEDING AND 1 PRECEDING)
            AS demand_roll_3h,
        AVG(total_demand) OVER (w ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
            AS demand_roll_24h,

        -- Cyclical hour-of-day encoding  (period = 24 h, range [-1, 1])
        SIN(2.0 * PI() * EXTRACT(HOUR   FROM hour_ts) / 24.0) AS hour_sin,
        COS(2.0 * PI() * EXTRACT(HOUR   FROM hour_ts) / 24.0) AS hour_cos,

        -- Cyclical day-of-week encoding  (ISODOW 1=Mon…7=Sun → 0-based / 7)
        SIN(2.0 * PI() * (EXTRACT(ISODOW FROM hour_ts) - 1)   / 7.0)  AS dow_sin,
        COS(2.0 * PI() * (EXTRACT(ISODOW FROM hour_ts) - 1)   / 7.0)  AS dow_cos,

        temperature_c,
        humidity_pct,
        precip_mm,

        FALSE  AS is_holiday,          -- Python fills this in afterwards
        now()  AS feature_computed_at

    FROM joined
    WINDOW w AS (PARTITION BY city ORDER BY hour_ts)
),

-- ── Step 3: upsert ────────────────────────────────────────────────────────
upserted AS (
    INSERT INTO marts.city_hour_features (
        city, hour_ts,
        total_demand, event_count,
        demand_lag_1h, demand_lag_24h, demand_lag_168h,
        demand_roll_3h, demand_roll_24h,
        hour_sin, hour_cos, dow_sin, dow_cos,
        temperature_c, humidity_pct, precip_mm,
        is_holiday, feature_computed_at
    )
    SELECT
        city, hour_ts,
        total_demand, event_count,
        demand_lag_1h, demand_lag_24h, demand_lag_168h,
        demand_roll_3h, demand_roll_24h,
        hour_sin, hour_cos, dow_sin, dow_cos,
        temperature_c, humidity_pct, precip_mm,
        is_holiday, feature_computed_at
    FROM featured
    ON CONFLICT (city, hour_ts) DO UPDATE SET
        total_demand        = EXCLUDED.total_demand,
        event_count         = EXCLUDED.event_count,
        demand_lag_1h       = EXCLUDED.demand_lag_1h,
        demand_lag_24h      = EXCLUDED.demand_lag_24h,
        demand_lag_168h     = EXCLUDED.demand_lag_168h,
        demand_roll_3h      = EXCLUDED.demand_roll_3h,
        demand_roll_24h     = EXCLUDED.demand_roll_24h,
        hour_sin            = EXCLUDED.hour_sin,
        hour_cos            = EXCLUDED.hour_cos,
        dow_sin             = EXCLUDED.dow_sin,
        dow_cos             = EXCLUDED.dow_cos,
        temperature_c       = EXCLUDED.temperature_c,
        humidity_pct        = EXCLUDED.humidity_pct,
        precip_mm           = EXCLUDED.precip_mm
        -- is_holiday intentionally omitted: existing rows keep their value;
        -- new rows receive FALSE and are corrected by the Python holiday step.
        -- feature_computed_at also intentionally omitted on conflict: only
        -- refresh it when the row is genuinely new.
    RETURNING 1
)

SELECT
    (SELECT COUNT(*) FROM staging.demand_hourly)::BIGINT  AS staging_rows,
    (SELECT COUNT(*) FROM upserted)::BIGINT               AS rows_upserted
;
