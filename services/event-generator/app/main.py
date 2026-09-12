"""
Event-generator main loop.

Tick loop
---------
Each real-second tick (tick_interval_s) advances the simulation clock by
  sim_delta = tick_interval_s * sim_speed  (simulated seconds)

For every tick, for every configured city, for every EventType:
  1. Call common.generator.generate_reading(city, sim_ts)
  2. Call demand.generate_quantity(event_type, reading, city_key, sim_ts)
  3. Emit one structlog JSON line

Output schema (one JSON object per line, to stdout):
  {
    "event":      "demand",
    "city":       "london",
    "event_type": "ELECTRICITY_KWH",
    "sim_ts":     "2024-01-15T14:00:00+00:00",
    "quantity":   742.18,
    "temperature_c": 3.2,
    "condition":  "MOSTLY_CLEAR"
  }
"""

from __future__ import annotations

import asyncio
import signal
import sys
from datetime import timedelta, timezone

import structlog

from common.generator import CITIES, generate_reading
from common.log import configure_logging

from .demand import DemandEvent, EventType, generate_quantity
from .settings import Settings

log = structlog.get_logger()

_SHUTDOWN = False


def _install_signal_handlers() -> None:
    def _handler(signum, frame):  # noqa: ANN001
        global _SHUTDOWN
        _SHUTDOWN = True
        log.info("shutdown_signal_received", signal=signum)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT,  _handler)


async def run(settings: Settings) -> None:
    city_keys = settings.city_list()
    unknown   = [k for k in city_keys if k not in CITIES]
    if unknown:
        log.warning("unknown_cities_skipped", cities=unknown)
        city_keys = [k for k in city_keys if k in CITIES]

    if not city_keys:
        log.error("no_valid_cities_configured")
        sys.exit(1)

    cities   = {k: CITIES[k] for k in city_keys}
    sim_ts   = settings.start_time().replace(tzinfo=timezone.utc) \
               if settings.start_time().tzinfo is None \
               else settings.start_time()
    sim_delta = timedelta(seconds=settings.tick_interval_s * settings.sim_speed)

    log.info(
        "generator_started",
        cities=city_keys,
        tick_interval_s=settings.tick_interval_s,
        sim_speed=settings.sim_speed,
        sim_start=sim_ts.isoformat(),
        events_per_tick=len(city_keys) * len(EventType),
    )

    tick = 0
    while not _SHUTDOWN:
        tick += 1
        for city_key, city in cities.items():
            reading = generate_reading(city, sim_ts)

            for event_type in EventType:
                quantity = generate_quantity(
                    event_type=event_type,
                    reading=reading,
                    city_key=city_key,
                    sim_ts=sim_ts,
                )
                log.info(
                    "demand",
                    city=city_key,
                    event_type=event_type.value,
                    sim_ts=sim_ts.isoformat(),
                    quantity=quantity,
                    temperature_c=reading.temperature_c,
                    condition=reading.condition,
                    tick=tick,
                )

        sim_ts += sim_delta
        await asyncio.sleep(settings.tick_interval_s)

    log.info("generator_stopped", ticks_completed=tick)


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    _install_signal_handlers()
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
