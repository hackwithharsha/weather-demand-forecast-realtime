"""
Thread-safe in-process Parquet buffer for the stream feature consumers.

Path convention
---------------
  stream/{table}/dt=YYYY-MM-DD/hour=HH/part_{epoch_ms}.parquet

Partitioning is by *business event time* (sim_ts for demand events,
polled_at for weather readings), not by wall-clock ingestion time.  A single
flush can therefore span multiple (dt, hour) partitions when the producer has
backfilled events across hour boundaries.

Key difference from the ingestor's ParquetWriter
-------------------------------------------------
The ingestor's writer reads completed hours back from Postgres (append-once
per hour, idempotent).  This buffer accumulates records in memory and writes
them whenever ``flush()`` is called by the consumer loop.  Multiple flushes
within the same event-time hour produce *separate* files with unique
``part_{epoch_ms}.parquet`` names — they do not clobber each other.  ML
training pipelines are expected to read all ``part_*.parquet`` files within
a given partition prefix.

Crash semantics
---------------
On restart, uncommitted Kafka offsets are re-delivered (at-least-once).
Re-delivered events write additional Parquet part files for the same
(dt, hour) partition — duplicate rows that training code must deduplicate
on event_id.  This is the same contract as the raw/ Parquet files from the
ingestor.

Flush failure
-------------
If an S3 upload fails, the affected partition's records are returned to the
front of the buffer so they are included in the next ``flush()`` call.
Nothing is silently dropped.
"""

from __future__ import annotations

import io
import threading
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.exceptions import ClientError

from common.s3 import make_s3_client

from ..settings import Settings

log = structlog.get_logger()


class ParquetBuffer:
    """Thread-safe accumulator that flushes records to MinIO as Parquet."""

    def __init__(self, settings: Settings, table: str) -> None:
        self._settings = settings
        self._table = table
        self._s3 = make_s3_client(settings)
        self._lock = threading.Lock()
        self._pending: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, record: dict[str, Any]) -> None:
        """Buffer one event record.

        *record* must contain ``_dt`` (``"YYYY-MM-DD"``) and ``_hour``
        (``int``) routing keys, which are stripped before the Parquet schema
        is inferred.  All other keys become Parquet columns.
        """
        with self._lock:
            self._pending.append(record)

    def flush(self) -> None:
        """Write all buffered records to MinIO, partitioned by (dt, hour).

        Routing keys ``_dt`` and ``_hour`` are stripped from every record
        before writing so they do not appear as Parquet columns.

        Partition uploads that fail are returned to the front of the buffer
        for retry on the next ``flush()`` call.
        """
        with self._lock:
            if not self._pending:
                return
            batch, self._pending = self._pending, []

        # Partition outside the lock — S3 uploads can be slow.
        parts: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for record in batch:
            key = (record["_dt"], record["_hour"])
            parts.setdefault(key, []).append(record)

        epoch_ms = int(time.time() * 1000)
        failed: list[dict[str, Any]] = []

        for (dt_str, hour), records in parts.items():
            # Strip routing keys before serialising.
            stripped = [
                {k: v for k, v in r.items() if not k.startswith("_")}
                for r in records
            ]
            if not self._upload_partition(dt_str, hour, stripped, epoch_ms):
                failed.extend(records)

        if failed:
            with self._lock:
                # Pre-pend so failed records are flushed first next time.
                self._pending = failed + self._pending

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _upload_partition(
        self,
        dt_str: str,
        hour: int,
        records: list[dict[str, Any]],
        epoch_ms: int,
    ) -> bool:
        """Serialise to Snappy Parquet and upload.  Returns True on success."""
        arrow_table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf, compression="snappy")

        s3_key = (
            f"stream/{self._table}/dt={dt_str}/hour={hour:02d}"
            f"/part_{epoch_ms}.parquet"
        )

        try:
            self._s3.put_object(
                Bucket=self._settings.lake_bucket,
                Key=s3_key,
                Body=buf.getvalue(),
            )
        except ClientError:
            log.exception(
                "stream_parquet_upload_failed",
                table=self._table,
                s3_key=s3_key,
            )
            return False

        log.info(
            "stream_parquet_partition_written",
            table=self._table,
            dt=dt_str,
            hour=hour,
            rows=len(records),
            bytes=buf.tell(),
            s3_key=s3_key,
        )
        return True
