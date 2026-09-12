-- 01_ml_schema.sql
-- Executed once by the ClickHouse container on first start-up.
-- Mirrors the structure of marts.city_hour_features in Postgres.
--
-- Engine choices
-- -------------
-- MergeTree: the standard engine for immutable, append-mostly analytics.
-- PARTITION BY toYYYYMM(hour_ts): one part per calendar month; pruned away
--   by any WHERE clause that restricts to a time range (typical for ML
--   training queries that operate on a rolling window).
-- ORDER BY (city, hour_ts): primary sort key used to build the sparse index.
--   Queries that filter or group by city get data locality for free.
--
-- Type notes
-- ----------
-- LowCardinality(String) for city encodes the 5-value city column as a
--   dictionary, cutting memory and improving scan throughput.
-- DateTime64(0, 'UTC') stores timestamps at second precision in UTC — the
--   same resolution as the hourly mart table.
-- Nullable(Float64) matches the Postgres DOUBLE PRECISION columns that can
--   be NULL (lag/rolling windows for the first rows, missing weather joins).
-- UInt8 for is_holiday (guaranteed non-null by mart quality assertions).

CREATE DATABASE IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.city_hour_features
(
    city                LowCardinality(String),
    hour_ts             DateTime64(0, 'UTC'),

    -- demand aggregates
    total_demand        Nullable(Float64),
    event_count         Nullable(Int32),

    -- lagged demand
    demand_lag_1h       Nullable(Float64),
    demand_lag_24h      Nullable(Float64),
    demand_lag_168h     Nullable(Float64),

    -- rolling means (past values only; NULL when history is shallow)
    demand_roll_3h      Nullable(Float64),
    demand_roll_24h     Nullable(Float64),

    -- cyclical time encodings in [-1, 1]
    hour_sin            Nullable(Float64),
    hour_cos            Nullable(Float64),
    dow_sin             Nullable(Float64),
    dow_cos             Nullable(Float64),

    -- weather join (NULL when no reading exists for the hour)
    temperature_c       Nullable(Float64),
    humidity_pct        Nullable(Float64),
    precip_mm           Nullable(Float64),

    -- public-holiday flag; non-null by construction
    is_holiday          UInt8,

    -- bookkeeping
    feature_computed_at DateTime64(0, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (city, hour_ts)
SETTINGS index_granularity = 8192;
