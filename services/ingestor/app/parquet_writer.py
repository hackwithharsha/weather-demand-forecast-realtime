"""
ParquetWriter: background thread that periodically flushes completed hourly
partitions from Postgres → Parquet → MinIO (S3-compatible).

S3 key format:
    raw/{table}/dt=YYYY-MM-DD/hour=HH/data.parquet
"""

from __future__ import annotations

import io
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from common.s3 import make_s3_client

from .settings import Settings

log = structlog.get_logger()

# Columns to query for each table, and their pyarrow type overrides where
# automatic inference is unreliable (e.g. Decimal → float64).
_TABLE_CFG: dict[str, dict] = {
    "demand_events": {
        "ts_col": "sim_ts",
        "select": (
            "id, city, event_type, sim_ts, quantity, temperature_c, condition, "
            "kafka_partition, kafka_offset, schema_version, event_id, ingested_at"
        ),
    },
    "weather_readings": {
        "ts_col": "polled_at",
        "select": (
            "id, city, polled_at, temperature_c, feels_like_c, dew_point_c, "
            "humidity_pct, wind_kph, wind_direction_deg, cloud_cover_pct, "
            "precip_probability_pct, precip_mm, condition, kafka_partition, "
            "kafka_offset, schema_version, event_id, ingested_at"
        ),
    },
}


def _coerce_row(row: dict) -> dict:
    """Convert Decimal values to float so pyarrow can infer the schema."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}


class ParquetWriter(threading.Thread):
    """
    Wakes every ``flush_interval_s`` seconds, then iterates over the past 25
    completed hours (UTC) and writes a Parquet file to MinIO for any hour that
    has not already been written in this process lifetime.
    """

    def __init__(self, settings: Settings, stop_event: threading.Event) -> None:
        super().__init__(name="parquet-writer", daemon=False)
        self._settings = settings
        self._stop_event = stop_event
        self._written: set[tuple[str, str, int]] = set()  # (table, dt_str, hour)
        self._s3 = make_s3_client(settings)

    # ------------------------------------------------------------------
    # Thread entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info(
            "parquet_writer_started",
            flush_interval_s=self._settings.parquet_flush_interval_s,
            bucket=self._settings.lake_bucket,
        )
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._settings.parquet_flush_interval_s)
            if self._stop_event.is_set():
                break
            try:
                self._flush_completed_hours()
            except Exception:
                log.exception("parquet_flush_error")
        log.info("parquet_writer_stopped")

    # ------------------------------------------------------------------
    # Core flush logic
    # ------------------------------------------------------------------

    def _flush_completed_hours(self) -> None:
        now_utc = datetime.now(timezone.utc)
        # Iterate the 25 most-recently completed hours (skip the current hour)
        for hours_ago in range(1, 26):
            hour_start = (now_utc - timedelta(hours=hours_ago)).replace(
                minute=0, second=0, microsecond=0
            )
            dt_str = hour_start.strftime("%Y-%m-%d")
            hour = hour_start.hour

            for table in _TABLE_CFG:
                key = (table, dt_str, hour)
                if key in self._written:
                    continue
                written = self._write_partition(table, dt_str, hour, hour_start)
                if written:
                    self._written.add(key)

    def _write_partition(
        self,
        table: str,
        dt_str: str,
        hour: int,
        hour_start: datetime,
    ) -> bool:
        """Query Postgres for one hour, write Parquet to MinIO. Returns True if
        any rows were written."""
        hour_end = hour_start + timedelta(hours=1)
        cfg = _TABLE_CFG[table]
        ts_col = cfg["ts_col"]
        select = cfg["select"]

        query = (
            f"SELECT {select} FROM raw.{table} "
            f"WHERE {ts_col} >= %s AND {ts_col} < %s"
        )

        try:
            conn = psycopg2.connect(self._settings.postgres_dsn)
        except psycopg2.OperationalError:
            log.exception("parquet_writer_pg_connect_error")
            return False

        try:
            with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (hour_start, hour_end))
                rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            return False

        coerced = [_coerce_row(dict(r)) for r in rows]
        table_pa = pa.Table.from_pylist(coerced)

        buf = io.BytesIO()
        pq.write_table(table_pa, buf, compression="snappy")
        buf.seek(0)

        s3_key = f"raw/{table}/dt={dt_str}/hour={hour:02d}/data.parquet"
        self._s3.put_object(
            Bucket=self._settings.lake_bucket,
            Key=s3_key,
            Body=buf.getvalue(),
        )
        log.info(
            "parquet_partition_written",
            table=table,
            dt=dt_str,
            hour=hour,
            rows=len(rows),
            s3_key=s3_key,
        )
        return True
