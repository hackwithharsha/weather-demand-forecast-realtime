"""create staging.rejects

Stores rows that failed validation during the raw → staging transform.

Columns
-------
source_table   Which raw table the row came from.
source_id      raw.*.id — lets us JOIN back to the original row.
event_id       The event's business UUID (may be NULL if missing in source).
reject_reason  Comma-separated list of failed checks, e.g. 'null_city,null_sim_ts'.
rejected_at    Wall-clock time the rejection was recorded.

Uniqueness
----------
UNIQUE (source_table, source_id) ensures that re-running the pipeline on the
same window does not produce duplicate reject rows.  The SQL transform uses
ON CONFLICT DO UPDATE so the reason is refreshed if the logic changes.

Revision ID: 0005
Revises: 0004
Create Date: 2024-01-16

"""

from __future__ import annotations

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.rejects (
            id            BIGSERIAL    PRIMARY KEY,
            source_table  TEXT         NOT NULL,
            source_id     BIGINT       NOT NULL,
            event_id      TEXT,
            reject_reason TEXT         NOT NULL,
            rejected_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
            UNIQUE (source_table, source_id)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS staging_rejects_rejected_at
            ON staging.rejects (rejected_at)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS staging.rejects")
