-- Raw ingestion tables.
-- Applied automatically by postgres:16 on first volume initialisation.
-- To apply to an existing instance: make reset

CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- demand_events  (from event-generator → demand.events.v1)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.demand_events (
    id               BIGSERIAL     PRIMARY KEY,
    city             TEXT          NOT NULL,
    event_type       TEXT          NOT NULL,
    sim_ts           TIMESTAMPTZ   NOT NULL,
    quantity         NUMERIC(12,4) NOT NULL,
    temperature_c    NUMERIC(6,2),
    condition        TEXT,
    kafka_partition  SMALLINT,
    kafka_offset     BIGINT,
    schema_version   SMALLINT,
    event_id         TEXT,
    received_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_demand_events_city_sim_ts
    ON raw.demand_events (city, sim_ts);

-- ---------------------------------------------------------------------------
-- weather_readings  (from weather-poller → weather.readings.v1)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.weather_readings (
    id                     BIGSERIAL     PRIMARY KEY,
    city                   TEXT          NOT NULL,
    polled_at              TIMESTAMPTZ   NOT NULL,
    temperature_c          NUMERIC(6,2),
    feels_like_c           NUMERIC(6,2),
    dew_point_c            NUMERIC(6,2),
    humidity_pct           SMALLINT,
    wind_kph               NUMERIC(6,2),
    wind_direction_deg     NUMERIC(6,2),
    cloud_cover_pct        SMALLINT,
    precip_probability_pct SMALLINT,
    precip_mm              NUMERIC(8,3),
    condition              TEXT,
    kafka_partition        SMALLINT,
    kafka_offset           BIGINT,
    schema_version         SMALLINT,
    event_id               TEXT,
    received_at            TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_weather_readings_city_polled_at
    ON raw.weather_readings (city, polled_at);
