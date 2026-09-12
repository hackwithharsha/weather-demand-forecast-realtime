"""
Weather-poller: polls mock-weather /v1/current for each city on a schedule
and produces a flat JSON reading to weather.readings.v1.

Message schema (value, JSON-encoded):
  {
    "city":                   "london",
    "polled_at":              "2024-01-15T14:00:00+00:00",   # ISO-8601 UTC
    "temperature_c":          3.2,
    "feels_like_c":           1.0,
    "dew_point_c":            0.5,
    "humidity_pct":           85,
    "wind_kph":               15.2,
    "wind_direction_deg":     270.0,
    "cloud_cover_pct":        80,
    "precip_probability_pct": 40,
    "precip_mm":              0.5,
    "condition":              "RAIN"
  }

Message key: city name (bytes)
"""

from __future__ import annotations

import json
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import structlog
from confluent_kafka import KafkaException, Producer

from common.generator import CITIES
from common.log import configure_logging

from .settings import Settings

log = structlog.get_logger()

_SHUTDOWN = False


def _install_signal_handlers() -> None:
    def _handler(signum, frame):  # noqa: ANN001
        global _SHUTDOWN
        _SHUTDOWN = True
        log.info("shutdown_signal_received", signal=signum)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _on_delivery(err: Exception | None, msg: Any) -> None:
    if err is not None:
        log.error("kafka_delivery_failed", error=str(err), topic=msg.topic())


def _fetch_current(base_url: str, lat: float, lon: float) -> dict:
    url = f"{base_url}/v1/current?lat={lat}&lon={lon}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _flatten(city_key: str, payload: dict) -> dict:
    """Extract the nested camelCase currentConditions into a flat snake_case dict."""
    cc = payload["currentConditions"]
    return {
        "city":                   city_key,
        "polled_at":              datetime.now(timezone.utc).isoformat(),
        "temperature_c":          cc["temperature"]["degrees"],
        "feels_like_c":           cc["feelsLike"]["degrees"],
        "dew_point_c":            cc["dewPoint"]["degrees"],
        "humidity_pct":           cc["humidity"],
        "wind_kph":               cc["wind"]["speed"]["value"],
        "wind_direction_deg":     cc["wind"]["direction"]["degrees"],
        "cloud_cover_pct":        cc["cloudCover"],
        "precip_probability_pct": cc["precipitation"]["probability"]["percent"],
        "precip_mm":              cc["precipitation"]["qpf"]["quantity"],
        "condition":              cc["weatherCondition"]["type"],
    }


def run(settings: Settings) -> None:
    city_keys = settings.city_list()
    unknown = [k for k in city_keys if k not in CITIES]
    if unknown:
        log.warning("unknown_cities_skipped", cities=unknown)
        city_keys = [k for k in city_keys if k in CITIES]

    if not city_keys:
        log.error("no_valid_cities")
        sys.exit(1)

    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    })

    log.info(
        "poller_started",
        cities=city_keys,
        poll_interval_s=settings.poll_interval_s,
        weather_url=settings.mock_weather_url,
        topic=settings.weather_topic,
    )

    while not _SHUTDOWN:
        cycle_start = time.monotonic()

        for city_key in city_keys:
            if _SHUTDOWN:
                break
            city = CITIES[city_key]
            try:
                raw = _fetch_current(settings.mock_weather_url, city.lat, city.lon)
                reading = _flatten(city_key, raw)
            except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
                log.warning("fetch_failed", city=city_key, error=str(exc))
                continue

            producer.produce(
                topic=settings.weather_topic,
                key=city_key.encode(),
                value=json.dumps(reading).encode(),
                on_delivery=_on_delivery,
            )
            producer.poll(0)  # serve delivery callbacks without blocking

            log.info(
                "reading_produced",
                city=city_key,
                temperature_c=reading["temperature_c"],
                condition=reading["condition"],
            )

        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0.0, settings.poll_interval_s - elapsed)
        # Sleep in small chunks so SIGTERM is handled promptly
        deadline = time.monotonic() + sleep_s
        while not _SHUTDOWN and time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))

    log.info("flushing_producer")
    producer.flush(timeout=10)
    log.info("poller_stopped")


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    _install_signal_handlers()
    try:
        run(settings)
    except KafkaException as exc:
        log.error("fatal_kafka_error", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
