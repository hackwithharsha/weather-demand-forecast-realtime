"""
ParquetWriter — wall-clock-hour-triggered Parquet export to MinIO.

Design
------
Unit of work  : one completed wall-clock hour, per table.
Trigger       : clock rollover (hour N ends → write hour N immediately).
Source        : Postgres raw.* tables, filtered by ingested_at range.
Destination   : s3://lake/raw/{table}/dt=YYYY-MM-DD/hour=HH/data.parquet
Compression   : Snappy.

Partitioning by ingested_at (not sim_ts / polled_at)
-----------------------------------------------------
demand_events.sim_ts is a *simulated* timestamp that can be years in the past
when the generator runs with SIM_SPEED > 1.  Querying that column against a
wall-clock window returns zero rows.  ingested_at is the real commit time and
always aligns with the wall-clock trigger.  sim_ts and polled_at are preserved
as columns inside every Parquet file so consumers can filter by event time.

Crash safety
------------
On startup the writer calls list_objects_v2 under raw/ to rebuild _written
from actual S3 state. This makes the "already written?" decision persistent
across restarts:

  Crash before write   → key absent  → catch-up writes from Postgres.        ✓
  Crash after write    → key present → skipped; no duplication.               ✓
  Crash during upload  → MinIO put_object is atomic (partial uploads are
                         abandoned) → key absent → re-uploaded on restart.    ✓

Partial hours
-------------
The current (live) hour is never written mid-hour.  On shutdown or crash,
rows for that incomplete hour stay safely in Postgres.  The next startup's
catch-up phase (or the next rollover trigger, whichever comes first) will
write them once the hour is complete.
"""

from __future__ import annotations

import io
import re
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.exceptions import ClientError

from common.s3 import make_s3_client

from .settings import Settings

log = structlog.get_logger()

# How many completed hours to scan on startup during catch-up.
_LOOKBACK_HOURS = 25

# Regex that matches the canonical S3 key format written by this module.
_S3_KEY_RE = re.compile(
    r"raw/(?P<table>[^/]+)"
    r"/dt=(?P<dt>\d{4}-\d{2}-\d{2})"
    r"/hour=(?P<hour>\d{2})"
    r"/data\.parquet$"
)

# Maps table name → SELECT column list.
# Both tables are queried by ingested_at; event-time columns (sim_ts,
# polled_at) are included in the SELECT so they land in the Parquet file.
_TABLE_CFG: dict[str, str] = {
    "demand_events": (
        "id, city, event_type, sim_ts, quantity, temperature_c, condition, "
        "kafka_partition, kafka_offset, schema_version, event_id, ingested_at"
    ),
    "weather_readings": (
        "id, city, polled_at, temperature_c, feels_like_c, dew_point_c, "
        "humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct, "
        "precip_probability_pct, precip_mm, condition, kafka_partition, "
        "kafka_offset, schema_version, event_id, ingested_at"
    ),
}


def _floor_hour(dt: datetime) -> datetime:
    """Truncate a timezone-aware datetime to the start of its UTC hour."""
    return dt.replace(minute=0, second=0, microsecond=0)


def _parse_s3_key(key: str) -> tuple[str, str, int] | None:
    """
    Extract (table, dt_str, hour) from a canonical S3 key, or return None
    if the key does not match the expected format.
    """
    m = _S3_KEY_RE.search(key)
    if m is None:
        return None
    return (m.group("table"), m.group("dt"), int(m.group("hour")))


def _coerce_row(row: dict) -> dict:
    """Convert Decimal → float so PyArrow can infer a numeric schema."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}


class ParquetWriter(threading.Thread):
    """
    Background thread that writes one Snappy-compressed Parquet file per
    (table, wall-clock hour) to MinIO, triggered by hour rollover.
    """

    def __init__(self, settings: Settings, stop_event: threading.Event) -> None:
        super().__init__(name="parquet-writer", daemon=False)
        self._settings = settings
        self._stop_event = stop_event
        # Persistent across the process lifetime; populated from S3 on startup
        # so the set survives crash-and-restart.
        self._written: set[tuple[str, str, int]] = set()
        self._s3 = make_s3_client(settings)

    # ------------------------------------------------------------------
    # Thread entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info(
            "parquet_writer_started",
            poll_interval_s=self._settings.parquet_flush_interval_s,
            bucket=self._settings.lake_bucket,
        )

        # ── Startup sequence ────────────────────────────────────────────
        # 1. Rebuild _written from S3 so already-written hours are not
        #    re-uploaded after a crash.
        self._sync_written_set_from_s3()

        # 2. Back-fill any completed hours that the process missed while it
        #    was down (crash-before-write recovery).
        self._catch_up()

        # ── Steady-state loop ───────────────────────────────────────────
        tracked_hour = _floor_hour(datetime.now(timezone.utc))

        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._settings.parquet_flush_interval_s)

            now_hour = _floor_hour(datetime.now(timezone.utc))
            if now_hour > tracked_hour:
                self._flush_rolled_hours(tracked_hour, now_hour)
                tracked_hour = now_hour

        # ── Shutdown flush ──────────────────────────────────────────────
        # Handle the edge case where SIGTERM arrives just after a rollover
        # (e.g. at 15:59:59): the stop_event unblocks the wait early, we
        # exit the loop, and would skip the newly completed hour without
        # this final check.
        now_hour = _floor_hour(datetime.now(timezone.utc))
        if now_hour > tracked_hour:
            self._flush_rolled_hours(tracked_hour, now_hour)

        log.info("parquet_writer_stopped")

    # ------------------------------------------------------------------
    # Startup helpers
    # ------------------------------------------------------------------

    def _sync_written_set_from_s3(self) -> None:
        """
        List s3://lake/raw/ and record every recognised Parquet key in
        _written.

        This is the primary crash-safety mechanism: it answers "has this
        (table, hour) already been written?" by consulting S3 directly
        rather than relying on in-process state that is lost on crash.

        Non-fatal: if MinIO is unavailable the writer starts with an empty
        set and may re-upload some completed hours idempotently.
        """
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self._settings.lake_bucket, Prefix="raw/"
            ):
                for obj in page.get("Contents", []):
                    parsed = _parse_s3_key(obj["Key"])
                    if parsed:
                        self._written.add(parsed)
            log.info(
                "parquet_writer_s3_sync_done",
                known_partitions=len(self._written),
            )
        except ClientError as exc:
            log.warning("parquet_writer_s3_sync_failed", error=str(exc))

    def _catch_up(self) -> None:
        """
        Write the last _LOOKBACK_HOURS completed hours not yet in S3.

        Called once at startup to recover from the crash-before-write case:
        any hour that completed while the process was down will be missing
        from S3 and is written here from Postgres before the live loop starts.
        """
        now = datetime.now(timezone.utc)
        for hours_ago in range(1, _LOOKBACK_HOURS + 1):
            hour_start = _floor_hour(now - timedelta(hours=hours_ago))
            for table in _TABLE_CFG:
                self._maybe_write(table, hour_start)

    # ------------------------------------------------------------------
    # Rollover flush
    # ------------------------------------------------------------------

    def _flush_rolled_hours(
        self, from_hour: datetime, up_to_hour: datetime
    ) -> None:
        """
        Write every (table, hour) pair in the half-open interval
        [from_hour, up_to_hour).

        up_to_hour is the current (not yet complete) hour and is excluded.
        The loop handles the uncommon case where the process sleeps through
        more than one rollover (e.g. long GC pause, system suspend).
        """
        cursor = from_hour
        while cursor < up_to_hour:
            for table in _TABLE_CFG:
                self._maybe_write(table, cursor)
            cursor += timedelta(hours=1)

    def _maybe_write(self, table: str, hour_start: datetime) -> None:
        """Write one (table, hour) partition if it is not already in _written."""
        dt_str = hour_start.strftime("%Y-%m-%d")
        hour = hour_start.hour
        key = (table, dt_str, hour)
        if key in self._written:
            return
        if self._write_partition(table, hour_start):
            self._written.add(key)

    # ------------------------------------------------------------------
    # Core write
    # ------------------------------------------------------------------

    def _write_partition(self, table: str, hour_start: datetime) -> bool:
        """
        Query Postgres for one hour's rows by ingested_at, serialise to
        a Snappy-compressed Parquet buffer, and upload to MinIO.

        Returns True if rows existed and the upload succeeded.
        Returns False (and logs) on any Postgres or S3 error — the caller
        will not add the key to _written, so the write will be retried.
        """
        hour_end = hour_start + timedelta(hours=1)
        query = (
            f"SELECT {_TABLE_CFG[table]} FROM raw.{table} "
            f"WHERE ingested_at >= %s AND ingested_at < %s"
        )

        try:
            conn = psycopg2.connect(self._settings.postgres_dsn)
            try:
                with conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cur:
                    cur.execute(query, (hour_start, hour_end))
                    rows = cur.fetchall()
            finally:
                conn.close()
        except psycopg2.Error:
            log.exception(
                "parquet_writer_pg_error",
                table=table,
                hour=hour_start.isoformat(),
            )
            return False

        if not rows:
            return False

        coerced = [_coerce_row(dict(r)) for r in rows]
        arrow_table = pa.Table.from_pylist(coerced)

        buf = io.BytesIO()
        pq.write_table(arrow_table, buf, compression="snappy")

        dt_str = hour_start.strftime("%Y-%m-%d")
        hour = hour_start.hour
        s3_key = f"raw/{table}/dt={dt_str}/hour={hour:02d}/data.parquet"

        try:
            self._s3.put_object(
                Bucket=self._settings.lake_bucket,
                Key=s3_key,
                Body=buf.getvalue(),
            )
        except ClientError:
            log.exception(
                "parquet_writer_s3_error",
                table=table,
                s3_key=s3_key,
            )
            return False

        log.info(
            "parquet_partition_written",
            table=table,
            dt=dt_str,
            hour=hour,
            rows=len(rows),
            bytes=buf.tell(),
            s3_key=s3_key,
        )
        return True
