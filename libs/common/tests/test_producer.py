"""
Unit tests for common.producer.EventProducer.

No running Kafka broker required — confluent_kafka.Producer is patched.

Properties verified
-------------------
1. Envelope shape     schema_version, event_id, produced_at, payload are present
2. Key routing        message key is city.encode()
3. Uniqueness         every produce() call gets a distinct event_id (UUID4)
4. Timestamp          produced_at parses as timezone-aware ISO-8601
5. Schema version     DEMAND_EVENTS and WEATHER_READINGS constants are int >= 1
6. Delivery callback  default callback handles error / success without raising
7. Custom callback    caller-supplied on_delivery is forwarded, not default
8. Flush              flush() is delegated to the underlying producer
9. Context manager    __exit__ calls flush()
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest

from common.producer import EventProducer, _log_delivery


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_producer(MockProducer: MagicMock) -> tuple[EventProducer, MagicMock]:
    """Return (EventProducer instance, mock inner Producer)."""
    inner = MagicMock()
    MockProducer.return_value = inner
    return EventProducer("localhost:9092"), inner


# ---------------------------------------------------------------------------
# 1–4 · Envelope
# ---------------------------------------------------------------------------

class TestEnvelope:

    @patch("common.producer.Producer")
    def test_all_envelope_fields_present(self, MP):
        producer, inner = _make_producer(MP)
        payload = {"city": "london", "quantity": 1.5}
        producer.produce(topic="t", city="london", schema_version=1, payload=payload)

        _, kw = inner.produce.call_args
        env = json.loads(kw["value"].decode())

        assert env["schema_version"] == 1
        assert env["payload"] == payload
        assert "event_id" in env
        assert "produced_at" in env

    @patch("common.producer.Producer")
    def test_schema_version_stored_verbatim(self, MP):
        producer, inner = _make_producer(MP)
        producer.produce(topic="t", city="c", schema_version=7, payload={})
        _, kw = inner.produce.call_args
        assert json.loads(kw["value"].decode())["schema_version"] == 7

    @patch("common.producer.Producer")
    def test_key_is_city_encoded(self, MP):
        producer, inner = _make_producer(MP)
        producer.produce(topic="t", city="tokyo", schema_version=1, payload={})
        _, kw = inner.produce.call_args
        assert kw["key"] == b"tokyo"

    @patch("common.producer.Producer")
    def test_event_id_is_valid_uuid4(self, MP):
        producer, inner = _make_producer(MP)
        producer.produce(topic="t", city="c", schema_version=1, payload={})
        _, kw = inner.produce.call_args
        env = json.loads(kw["value"].decode())
        parsed = uuid.UUID(env["event_id"])
        assert parsed.version == 4

    @patch("common.producer.Producer")
    def test_each_call_gets_unique_event_id(self, MP):
        producer, inner = _make_producer(MP)
        for _ in range(5):
            producer.produce(topic="t", city="c", schema_version=1, payload={})
        ids = [
            json.loads(c.kwargs["value"].decode())["event_id"]
            for c in inner.produce.call_args_list
        ]
        assert len(set(ids)) == 5

    @patch("common.producer.Producer")
    def test_produced_at_is_timezone_aware_iso(self, MP):
        producer, inner = _make_producer(MP)
        producer.produce(topic="t", city="c", schema_version=1, payload={})
        _, kw = inner.produce.call_args
        env = json.loads(kw["value"].decode())
        dt = datetime.fromisoformat(env["produced_at"])
        assert dt.tzinfo is not None

    @patch("common.producer.Producer")
    def test_poll_called_after_produce(self, MP):
        """poll(0) must be called after every produce to serve delivery callbacks."""
        producer, inner = _make_producer(MP)
        producer.produce(topic="t", city="c", schema_version=1, payload={})
        inner.poll.assert_called_with(0)


# ---------------------------------------------------------------------------
# 5 · Schema version constants
# ---------------------------------------------------------------------------

class TestSchemaVersionConstants:

    def test_demand_events_is_positive_int(self):
        v = EventProducer.DEMAND_EVENTS_SCHEMA_VERSION
        assert isinstance(v, int) and v >= 1

    def test_weather_readings_is_positive_int(self):
        v = EventProducer.WEATHER_READINGS_SCHEMA_VERSION
        assert isinstance(v, int) and v >= 1


# ---------------------------------------------------------------------------
# 6–7 · Delivery callbacks
# ---------------------------------------------------------------------------

class TestDeliveryCallback:

    def test_default_error_callback_does_not_raise(self):
        err = Exception("broker gone")
        msg = MagicMock()
        msg.topic.return_value = "t"
        msg.partition.return_value = 0
        _log_delivery(err, msg)  # must not raise

    def test_default_success_callback_does_not_raise(self):
        msg = MagicMock()
        msg.topic.return_value = "t"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        _log_delivery(None, msg)  # must not raise

    @patch("common.producer.Producer")
    def test_custom_on_delivery_is_forwarded(self, MP):
        producer, inner = _make_producer(MP)
        my_cb = MagicMock()
        producer.produce(topic="t", city="c", schema_version=1, payload={},
                         on_delivery=my_cb)
        _, kw = inner.produce.call_args
        assert kw["on_delivery"] is my_cb

    @patch("common.producer.Producer")
    def test_default_on_delivery_used_when_none(self, MP):
        producer, inner = _make_producer(MP)
        producer.produce(topic="t", city="c", schema_version=1, payload={})
        _, kw = inner.produce.call_args
        assert kw["on_delivery"] is _log_delivery


# ---------------------------------------------------------------------------
# 8–9 · flush and context manager
# ---------------------------------------------------------------------------

class TestFlushAndContextManager:

    @patch("common.producer.Producer")
    def test_flush_delegates_to_inner(self, MP):
        producer, inner = _make_producer(MP)
        inner.flush.return_value = 0
        result = producer.flush(timeout=5.0)
        inner.flush.assert_called_once_with(timeout=5.0)
        assert result == 0

    @patch("common.producer.Producer")
    def test_context_manager_flushes_on_exit(self, MP):
        inner = MagicMock()
        MP.return_value = inner
        with EventProducer("localhost:9092"):
            pass
        inner.flush.assert_called_once()
