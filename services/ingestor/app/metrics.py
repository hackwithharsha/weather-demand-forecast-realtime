"""Prometheus metrics for the ingestor service.

Exposed on port 9100 via prometheus_client.start_http_server so that
Prometheus can scrape ingestor:9100/metrics without adding a route to
the non-HTTP ingestor process.
"""
from prometheus_client import Counter, Histogram

messages_consumed_total = Counter(
    "ingestor_messages_consumed_total",
    "Messages successfully validated and buffered",
    ["topic"],
)

messages_validation_failed_total = Counter(
    "ingestor_messages_validation_failed_total",
    "Messages that failed schema / version validation",
    ["topic"],
)

messages_dlq_total = Counter(
    "ingestor_messages_dlq_total",
    "Messages routed to the dead-letter queue",
    ["topic"],
)

batch_write_duration_seconds = Histogram(
    "ingestor_batch_write_duration_seconds",
    "Time taken to bulk-insert a batch into Postgres",
    ["topic"],
    buckets=[0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

batch_write_rows_total = Counter(
    "ingestor_batch_write_rows_total",
    "Rows successfully written to Postgres",
    ["topic"],
)
