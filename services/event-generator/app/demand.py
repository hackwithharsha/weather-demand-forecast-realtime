"""
Pure demand model: (event_type, weather_reading, city_key, sim_ts) -> quantity.

No I/O. No framework. Same inputs always produce the same output.

Demand signal design
--------------------
Each event type has a base demand and a set of weather multipliers that
combine multiplicatively.  A small Gaussian noise term is seeded on
(city_key, event_type, year, month, day, hour) so the signal is
reproducible but varies intra-day.

Event types and their primary weather drivers:
  ELECTRICITY_KWH      high when very cold OR very hot (HVAC)
  GAS_CONSUMPTION_M3   high when cold
  RIDE_SHARE_TRIPS     high when rainy or very hot
  ICE_CREAM_SALES      high when hot and sunny
  HOT_DRINK_SALES      high when cold or rainy
  GROCERY_DELIVERIES   high when rainy (people stay indoors)
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from common.generator import WeatherReading


class EventType(str, Enum):
    ELECTRICITY_KWH    = "ELECTRICITY_KWH"
    GAS_CONSUMPTION_M3 = "GAS_CONSUMPTION_M3"
    RIDE_SHARE_TRIPS   = "RIDE_SHARE_TRIPS"
    ICE_CREAM_SALES    = "ICE_CREAM_SALES"
    HOT_DRINK_SALES    = "HOT_DRINK_SALES"
    GROCERY_DELIVERIES = "GROCERY_DELIVERIES"


# Base demand per event type (per simulated hour, per city unit)
BASE_DEMAND: dict[EventType, float] = {
    EventType.ELECTRICITY_KWH:    500.0,
    EventType.GAS_CONSUMPTION_M3: 120.0,
    EventType.RIDE_SHARE_TRIPS:    80.0,
    EventType.ICE_CREAM_SALES:     40.0,
    EventType.HOT_DRINK_SALES:     60.0,
    EventType.GROCERY_DELIVERIES:  50.0,
}

# City population/density scale relative to a notional "average" city
CITY_SCALE: dict[str, float] = {
    "london":    1.4,
    "new_york":  1.6,
    "tokyo":     1.8,
    "sydney":    1.1,
    "dubai":     1.2,
    "nairobi":   0.8,
    "moscow":    1.3,
    "singapore": 1.0,
    "reykjavik": 0.4,
    "cape_town": 0.7,
}
_DEFAULT_CITY_SCALE = 1.0


# ---------------------------------------------------------------------------
# Multiplier functions (pure, no RNG)
# ---------------------------------------------------------------------------

def _electricity_multiplier(r: WeatherReading) -> float:
    """U-shaped: high demand below 5 °C and above 28 °C (heating/cooling)."""
    t = r.temperature_c
    if t <= 5.0:
        cold_factor = 1.0 + (5.0 - t) * 0.08     # +8% per degree below 5
        return cold_factor
    if t >= 28.0:
        hot_factor = 1.0 + (t - 28.0) * 0.06     # +6% per degree above 28
        return hot_factor
    # Neutral band: mild dip around comfort zone ~18 °C
    return 0.8 + 0.2 * abs(t - 18.0) / 13.0


def _gas_multiplier(r: WeatherReading) -> float:
    """Monotonically decreasing with temperature (space heating)."""
    t = r.temperature_c
    return max(0.05, 1.0 + (15.0 - t) * 0.07)


def _ride_share_multiplier(r: WeatherReading) -> float:
    """Rainy and extreme-heat weather drives ride-share demand."""
    rain_bump = r.precip_probability_pct / 100.0 * 0.8   # up to +80%
    heat_bump = max(0.0, (r.temperature_c - 32.0) * 0.04)
    return 1.0 + rain_bump + heat_bump


def _ice_cream_multiplier(r: WeatherReading) -> float:
    """Strongly tied to high temperature and low cloud cover."""
    if r.temperature_c < 10.0:
        return 0.05
    sun_factor = 1.0 - r.cloud_cover_pct / 100.0 * 0.5   # 50% penalty at full cloud
    temp_factor = max(0.0, (r.temperature_c - 10.0) / 25.0)
    return max(0.05, sun_factor * temp_factor * 3.0)


def _hot_drink_multiplier(r: WeatherReading) -> float:
    """Cold or rainy weather drives coffee/tea demand."""
    cold_factor = max(0.3, 1.0 + (18.0 - r.temperature_c) * 0.04)
    rain_bump   = r.precip_probability_pct / 100.0 * 0.4
    return cold_factor + rain_bump


def _grocery_delivery_multiplier(r: WeatherReading) -> float:
    """People order in when it rains or when it's very cold."""
    rain_factor = 1.0 + r.precip_probability_pct / 100.0 * 1.2
    cold_bump   = max(0.0, (5.0 - r.temperature_c) * 0.03)
    return rain_factor + cold_bump


_MULTIPLIER_FN = {
    EventType.ELECTRICITY_KWH:    _electricity_multiplier,
    EventType.GAS_CONSUMPTION_M3: _gas_multiplier,
    EventType.RIDE_SHARE_TRIPS:   _ride_share_multiplier,
    EventType.ICE_CREAM_SALES:    _ice_cream_multiplier,
    EventType.HOT_DRINK_SALES:    _hot_drink_multiplier,
    EventType.GROCERY_DELIVERIES: _grocery_delivery_multiplier,
}


# ---------------------------------------------------------------------------
# Diurnal shape (hour-of-day weighting, no RNG)
# ---------------------------------------------------------------------------

# Weights indexed 0-23 represent typical activity for each hour.
# Electricity and gas peak in the morning/evening; retail peaks midday.
_DIURNAL: dict[EventType, list[float]] = {
    EventType.ELECTRICITY_KWH: [
        0.6, 0.5, 0.5, 0.5, 0.6, 0.8,  # 00-05 low overnight; morning ramp
        1.1, 1.3, 1.3, 1.2, 1.1, 1.0,  # 06-11 morning peak
        1.0, 1.0, 1.0, 1.0, 1.1, 1.3,  # 12-17 afternoon
        1.4, 1.3, 1.2, 1.0, 0.8, 0.7,  # 18-23 evening peak
    ],
    EventType.GAS_CONSUMPTION_M3: [
        0.5, 0.5, 0.4, 0.4, 0.5, 0.8,
        1.3, 1.4, 1.2, 1.0, 0.9, 0.8,
        0.8, 0.8, 0.8, 0.9, 1.1, 1.4,
        1.5, 1.3, 1.0, 0.8, 0.6, 0.5,
    ],
    EventType.RIDE_SHARE_TRIPS: [
        0.3, 0.2, 0.2, 0.2, 0.3, 0.6,
        1.2, 1.5, 1.4, 1.0, 0.9, 0.9,
        1.0, 0.9, 0.9, 1.0, 1.3, 1.5,
        1.4, 1.2, 1.0, 0.8, 0.6, 0.4,
    ],
    EventType.ICE_CREAM_SALES: [
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.1, 0.2, 0.3, 0.5, 0.8, 1.1,
        1.4, 1.5, 1.5, 1.4, 1.3, 1.1,
        0.9, 0.7, 0.4, 0.2, 0.1, 0.0,
    ],
    EventType.HOT_DRINK_SALES: [
        0.1, 0.0, 0.0, 0.0, 0.1, 0.4,
        1.2, 1.6, 1.5, 1.2, 1.0, 1.0,
        1.0, 1.0, 0.9, 0.9, 1.0, 1.1,
        1.0, 0.8, 0.6, 0.4, 0.2, 0.1,
    ],
    EventType.GROCERY_DELIVERIES: [
        0.1, 0.0, 0.0, 0.0, 0.1, 0.2,
        0.4, 0.6, 0.8, 0.9, 1.0, 1.1,
        1.3, 1.2, 1.1, 1.0, 1.1, 1.3,
        1.5, 1.4, 1.2, 0.9, 0.6, 0.3,
    ],
}


# ---------------------------------------------------------------------------
# Noise seeding (deterministic)
# ---------------------------------------------------------------------------

def _noise_seed(city_key: str, event_type: EventType, ts: datetime) -> int:
    return hash((city_key, event_type.value, ts.year, ts.month, ts.day, ts.hour))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DemandEvent:
    city:       str
    event_type: EventType
    sim_ts:     datetime
    quantity:   float


def generate_quantity(
    event_type: EventType,
    reading: WeatherReading,
    city_key: str,
    sim_ts: datetime,
) -> float:
    """
    Return the simulated demand quantity for one event type in one city-hour.

    Parameters
    ----------
    event_type : EventType
    reading    : WeatherReading for this city+hour (from common.generator)
    city_key   : key in CITY_SCALE (unknown keys use DEFAULT_CITY_SCALE)
    sim_ts     : simulation timestamp (used for diurnal index and noise seed)

    Returns
    -------
    float >= 0.0  (rounded to 2 decimal places)
    """
    base      = BASE_DEMAND[event_type]
    scale     = CITY_SCALE.get(city_key, _DEFAULT_CITY_SCALE)
    weather_m = _MULTIPLIER_FN[event_type](reading)
    diurnal_m = _DIURNAL[event_type][sim_ts.hour]

    # Gaussian noise: σ=8% of the pre-noise quantity so CV is constant
    rng       = random.Random(_noise_seed(city_key, event_type, sim_ts))
    noise_m   = math.exp(rng.gauss(0.0, 0.08))   # log-normal → always positive

    quantity  = base * scale * weather_m * diurnal_m * noise_m
    return round(max(0.0, quantity), 2)
