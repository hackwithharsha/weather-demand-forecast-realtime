"""create marts.drift_reports

Stores one row per feature per hourly Evidently drift check.  The drift
job reads this table to update Prometheus gauges and Grafana dashboards.

Design decisions
----------------
One row per (checked_at, feature_name)
    A single check run writes N rows — one per feature column compared.
    This allows per-feature queries ("has temperature_c drifted more than
    once this week?") without JSON parsing.

drift_score is the test statistic returned by Evidently (Wasserstein
    distance for numeric features by default in 0.4.x).  Lower is better.

reference_count / current_count
    Logged so that downstream consumers can weight or filter checks that
    ran on suspiciously small windows.

Revision ID: 0009
Revises: 0008
Create Date: 2024-01-19
"""

from __future__ import annotations

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS marts.drift_reports (
            id              BIGSERIAL        PRIMARY KEY,
            checked_at      TIMESTAMPTZ      NOT NULL,
            feature_name    TEXT             NOT NULL,
            drift_score     DOUBLE PRECISION,
            drift_detected  BOOLEAN          NOT NULL DEFAULT FALSE,
            stat_test       TEXT,
            p_value         DOUBLE PRECISION,
            reference_count INTEGER,
            current_count   INTEGER,
            logged_at       TIMESTAMPTZ      NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS marts_drift_reports_checked_at
            ON marts.drift_reports (checked_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS marts_drift_reports_feature_checked_at
            ON marts.drift_reports (feature_name, checked_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS marts.drift_reports")
