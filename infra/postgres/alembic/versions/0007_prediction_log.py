"""create marts.prediction_log

One row is written per POST /predict request (and per background broadcast
refresh).  It captures Production and Staging model predictions side-by-side
with per-model inference latencies and the online-feature retrieval context
that was active at request time.

Design decisions
----------------
prod_preds / staging_preds stored as DOUBLE PRECISION[]
    Arrays are cheap to store and allow full reconstruction of the forecast
    horizon without a JOIN.  Individual step values can be unnested with
    ``unnest()`` for aggregate analysis.

staging_version / staging_preds / staging_latency_ms are nullable
    A Staging model is optional.  When none is loaded the Production columns
    are still written so that every prediction is auditable.

feature_missing / feature_degraded stored as TEXT[]
    Preserves the exact field names from OnlineResult for downstream
    analysis (e.g. "how often is weather_temp_c stale?").

Index on (city, requested_at DESC)
    Covers the most common query pattern: "last N predictions for city X".

Revision ID: 0007
Revises: 0006
Create Date: 2024-01-17

"""

from __future__ import annotations

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS marts.prediction_log (
            id                   BIGSERIAL           PRIMARY KEY,

            -- Request context
            requested_at         TIMESTAMPTZ         NOT NULL,
            city                 TEXT                NOT NULL,
            horizon_hours        INTEGER             NOT NULL,

            -- Production model
            prod_version         INTEGER             NOT NULL,
            prod_preds           DOUBLE PRECISION[]  NOT NULL,
            prod_latency_ms      DOUBLE PRECISION    NOT NULL,

            -- Staging model (NULL when no Staging version is loaded)
            staging_version      INTEGER,
            staging_preds        DOUBLE PRECISION[],
            staging_latency_ms   DOUBLE PRECISION,

            -- Online-feature retrieval context at request time
            feature_hash_present BOOLEAN,
            feature_age_s        DOUBLE PRECISION,
            feature_missing      TEXT[],
            feature_degraded     TEXT[],

            -- Bookkeeping
            logged_at            TIMESTAMPTZ         NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS marts_prediction_log_city_requested_at
            ON marts.prediction_log (city, requested_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS marts.prediction_log")
