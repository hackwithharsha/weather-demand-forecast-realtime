"""
replay — reset a Kafka consumer group to a point-in-time offset.

Usage
-----
  python -m tools.replay --group ingestor-demand --timestamp 2026-09-14T10:00:00
  python -m tools.replay --group ingestor-weather --timestamp 2026-09-14T10:00:00Z
  python -m tools.replay --group ingestor-demand --timestamp 2026-09-14T10:00:00 \\
      --topic demand.events.v1

The consumer group must be INACTIVE (its members disconnected) before offsets
can be reset — Kafka rejects offset commits from groups with active members.

Stop the ingestor before running this tool:

    docker compose --profile stream stop ingestor

Restart the ingestor after the reset:

    make up-stream

Offset behaviour
----------------
Sets each partition offset to the first message whose timestamp is
>= TIMESTAMP.  When no such message exists in a partition (all messages
are older than the requested time), the partition is advanced to the high
watermark so the consumer starts from the tail.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition

# Well-known consumer group → default topic map.
_GROUP_TOPICS: dict[str, str] = {
    "ingestor-demand":       "demand.events.v1",
    "ingestor-weather":      "weather.readings.v1",
    "worker-stream-demand":  "demand.events.v1",
    "worker-stream-weather": "weather.readings.v1",
}


def _default_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")


def _parse_timestamp_ms(ts_str: str) -> int:
    """Parse an ISO-8601 string and return Unix milliseconds."""
    s = ts_str.rstrip("Z")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise SystemExit(f"error: invalid timestamp {ts_str!r}: {exc}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def replay(
    group: str,
    timestamp: str,
    topic: str | None,
    servers: str,
) -> None:
    # Resolve the topic for well-known groups when --topic is not given.
    if topic is None:
        topic = _GROUP_TOPICS.get(group)
        if topic is None:
            print(
                f"error: no default topic for group '{group}'. "
                "Use --topic to specify the Kafka topic explicitly.",
                file=sys.stderr,
            )
            sys.exit(1)

    ts_ms = _parse_timestamp_ms(timestamp)
    print(f"Resetting group '{group}' on topic '{topic}' to {timestamp!r} ...")
    print(f"  bootstrap: {servers}")

    consumer = Consumer({
        "bootstrap.servers":  servers,
        "group.id":           group,
        "enable.auto.commit": "false",
    })

    try:
        # ── 1. Discover partitions ────────────────────────────────────────
        meta = consumer.list_topics(topic, timeout=15)
        if topic not in meta.topics:
            print(f"error: topic '{topic}' not found on broker.", file=sys.stderr)
            sys.exit(1)
        topic_meta = meta.topics[topic]
        if topic_meta.error is not None:
            print(f"error: topic metadata error: {topic_meta.error}", file=sys.stderr)
            sys.exit(1)

        partition_ids = sorted(topic_meta.partitions.keys())
        print(f"  {len(partition_ids)} partition(s): {partition_ids}")

        # ── 2. Build timestamp-based TopicPartition list ─────────────────
        # confluent-kafka uses the offset field to carry the target timestamp
        # when calling offsets_for_times().
        tp_with_ts = [TopicPartition(topic, p, ts_ms) for p in partition_ids]

        # ── 3. Resolve timestamps → offsets ──────────────────────────────
        resolved: list[TopicPartition] = consumer.offsets_for_times(
            tp_with_ts, timeout=15
        )

        # ── 4. Fall back to high watermark for partitions with no match ──
        final: list[TopicPartition] = []
        for tp in resolved:
            if tp.offset < 0:
                # OFFSET_INVALID means no message found at or after timestamp.
                # Advance to the partition end so consumption starts from tail.
                _low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, tp.partition), timeout=10
                )
                final.append(TopicPartition(topic, tp.partition, high))
                print(
                    f"  partition {tp.partition}: "
                    f"no message >= {timestamp}, set to end (offset {high})"
                )
            else:
                final.append(tp)
                print(f"  partition {tp.partition}: offset → {tp.offset}")

        # ── 5. Commit new offsets for the consumer group ──────────────────
        # assign() makes the consumer "own" these partitions so commit()
        # can record the group's position without actively consuming.
        consumer.assign(final)
        consumer.commit(offsets=final, asynchronous=False)

        print(f"\nGroup '{group}' reset to {timestamp!r}.")
        print("Restart the ingestor to begin consuming from the new offsets:")
        print("  make up-stream")

    except KafkaException as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset a Kafka consumer group to a point-in-time offset.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--group", required=True,
        help="Consumer group ID to reset (e.g. ingestor-demand)",
    )
    parser.add_argument(
        "--timestamp", required=True, metavar="ISO8601",
        help=(
            "ISO-8601 timestamp to seek to "
            "(e.g. 2026-09-14T10:00:00 or 2026-09-14T10:00:00Z)"
        ),
    )
    parser.add_argument(
        "--topic",
        help=(
            "Kafka topic (inferred from well-known group names; "
            "required for custom group IDs)"
        ),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=_default_servers(),
        help="Comma-separated Kafka bootstrap servers (default: redpanda:9092)",
    )
    args = parser.parse_args()

    replay(
        group=args.group,
        timestamp=args.timestamp,
        topic=args.topic,
        servers=args.bootstrap_servers,
    )


if __name__ == "__main__":
    main()
