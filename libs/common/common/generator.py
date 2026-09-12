"""
Pure weather-reading generator.

    generate_reading(city: City, ts: datetime) -> WeatherReading

Contract
--------
- **Pure**: no I/O, no network, no global-state mutation, no side effects.
- **Deterministic**: identical (city, ts) always returns an identical WeatherReading.
  Seed = hash(round(lat, 1), round(lon, 1), year, month, day, hour).
  Minutes and seconds within an hour do not change the output.
- **RNG call order is fixed** (temperature → humidity → wind_speed → wind_dir →
  cloud_cover → precip_mm).  Changing the order breaks reproducibility.

Temperature model
-----------------
    T = base(lat) + seasonal(lat, doy) + diurnal(hour) + noise(seed)

    base(lat)       = 27 − 0.5·|lat|          equator≈27 °C, poles≈−18 °C
    seasonal        = A·sign(lat)·sin(2π(doy−80)/365)   A = 15·|lat|/90
    diurnal(hour)   = 6·sin(2π(hour−6)/24)    min at 06:00, max at 14:00
    noise           = Gaussian(0, σ=1.5), seeded

All other fields are derived from temperature + seeded RNG draws.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class City:
    name: str
    lat: float   # degrees, negative = southern hemisphere
    lon: float   # degrees, negative = west


@dataclass(frozen=True)
class WeatherReading:
    temperature_c: float          # dry-bulb temperature
    feels_like_c: float           # apparent temperature
    dew_point_c: float
    humidity_pct: int             # 0–100
    wind_kph: float               # ≥ 0
    wind_direction_deg: float     # 0–360
    cloud_cover_pct: int          # 0–100
    precip_probability_pct: int   # 0–100
    precip_mm: float              # ≥ 0, hourly accumulation
    condition: str                # see _CONDITION_LABELS


# ---------------------------------------------------------------------------
# Built-in city registry
# ---------------------------------------------------------------------------

CITIES: dict[str, City] = {
    "london":    City("London",    lat= 51.5, lon=  -0.1),
    "nairobi":   City("Nairobi",   lat= -1.3, lon=  36.8),
    "moscow":    City("Moscow",    lat= 55.8, lon=  37.6),
    "singapore": City("Singapore", lat=  1.4, lon= 103.8),
    "reykjavik": City("Reykjavik", lat= 64.1, lon= -21.9),
    "cape_town": City("Cape Town", lat=-33.9, lon=  18.4),
    "new_york":  City("New York",  lat= 40.7, lon= -74.0),
    "tokyo":     City("Tokyo",     lat= 35.7, lon= 139.7),
    "dubai":     City("Dubai",     lat= 25.2, lon=  55.3),
    "sydney":    City("Sydney",    lat=-33.9, lon= 151.2),
}


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def _seed(city: City, ts: datetime) -> int:
    """
    Deterministic integer seed.  Rounded lat/lon so that nearby coordinates
    share weather patterns; hour-granularity so the reading is stable within
    an hour regardless of when within it the caller samples.
    """
    return hash((
        round(city.lat, 1), round(city.lon, 1),
        ts.year, ts.month, ts.day, ts.hour,
    ))


# ---------------------------------------------------------------------------
# Temperature sub-model (pure, no RNG)
# ---------------------------------------------------------------------------

def _base_temp(lat: float) -> float:
    """Annual-mean temperature by latitude.  Equator ≈ 27 °C, poles ≈ −18 °C."""
    return 27.0 - 0.5 * abs(lat)


def _seasonal_offset(lat: float, ts: datetime) -> float:
    """
    Peak-to-peak swing of ±(15·|lat|/90) °C.
    Northern Hemisphere maximum around day 172 (summer solstice).
    Southern Hemisphere phase is inverted.
    """
    doy = ts.timetuple().tm_yday
    amplitude = 15.0 * abs(lat) / 90.0
    hemisphere_sign = 1.0 if lat >= 0.0 else -1.0
    return amplitude * hemisphere_sign * math.sin(2 * math.pi * (doy - 80) / 365)


def _diurnal_offset(hour: int) -> float:
    """±6 °C diurnal cycle.  Trough at 06:00, crest at 14:00."""
    return 6.0 * math.sin(2 * math.pi * (hour - 6) / 24)


# ---------------------------------------------------------------------------
# Derived-field helpers  (each consumes exactly one RNG draw in call order)
# ---------------------------------------------------------------------------

def _temperature(city: City, ts: datetime, rng: random.Random) -> float:
    # RNG draw 1
    noise = rng.gauss(0.0, 1.5)
    return round(
        _base_temp(city.lat)
        + _seasonal_offset(city.lat, ts)
        + _diurnal_offset(ts.hour)
        + noise,
        1,
    )


def _humidity(city: City, ts: datetime, rng: random.Random) -> int:
    # RNG draw 2
    doy = ts.timetuple().tm_yday
    # Base peaks around 30° latitude (trade-wind belt); seasonal modulation
    base = 70.0 - abs(abs(city.lat) - 30.0) * 0.4
    seasonal = 8.0 * math.sin(2 * math.pi * (doy - 80) / 365) * (
        1.0 if city.lat >= 0.0 else -1.0
    )
    return max(10, min(100, round(base + seasonal + rng.gauss(0.0, 12.0))))


def _wind_kph(rng: random.Random) -> float:
    # RNG draw 3
    return round(max(0.0, rng.gauss(18.0, 9.0)), 1)


def _wind_direction_deg(rng: random.Random) -> float:
    # RNG draw 4
    return round(rng.uniform(0.0, 360.0), 1)


def _cloud_cover(humidity: int, rng: random.Random) -> int:
    # RNG draw 5
    return max(0, min(100, round((humidity - 40) * 1.3 + rng.gauss(0.0, 10.0))))


def _precip_mm(precip_prob: int, rng: random.Random) -> float:
    # RNG draw 6
    if precip_prob < 20:
        return 0.0
    rate = max(0.1, precip_prob / 100.0 * 5.0)
    return round(rng.expovariate(1.0 / rate) * (precip_prob / 100.0), 2)


def _precip_probability(humidity: int, cloud_cover: int) -> int:
    return min(100, max(0, round((humidity - 55) * 1.5 + (cloud_cover - 50) * 0.4)))


def _feels_like(temp: float, wind_kph: float, humidity: int) -> float:
    wind_ms = wind_kph / 3.6
    if temp > 27.0:
        return round(temp + 0.33 * (humidity / 100.0 * 6.1) - 0.70 * wind_ms - 4.0, 1)
    if temp < 10.0:
        return round(temp - wind_kph * 0.25, 1)
    return temp


def _dew_point(temp: float, humidity: int) -> float:
    """Magnus approximation: accurate to ±0.4 °C for 50 % ≤ RH ≤ 100 %."""
    return round(temp - (100 - humidity) / 5.0, 1)


_CONDITION_LABELS: dict[str, str] = {
    "HEAVY_RAIN":        "Heavy rain",
    "RAIN":              "Rain",
    "DRIZZLE":           "Light drizzle",
    "CLOUDY":            "Overcast",
    "MOSTLY_CLOUDY":     "Mostly cloudy",
    "PARTLY_CLOUDY_DAY": "Partly cloudy",
    "MOSTLY_CLEAR":      "Mostly clear",
    "CLEAR_DAY":         "Clear",
}


def _condition(cloud_cover: int, precip_prob: int) -> str:
    if precip_prob >= 70:
        return "HEAVY_RAIN"
    if precip_prob >= 40:
        return "RAIN"
    if precip_prob >= 20:
        return "DRIZZLE"
    if cloud_cover >= 90:
        return "CLOUDY"
    if cloud_cover >= 70:
        return "MOSTLY_CLOUDY"
    if cloud_cover >= 40:
        return "PARTLY_CLOUDY_DAY"
    if cloud_cover >= 20:
        return "MOSTLY_CLEAR"
    return "CLEAR_DAY"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_reading(city: City, ts: datetime) -> WeatherReading:
    """
    Return a deterministic WeatherReading for *city* at time *ts*.

    The seed is derived from the city coordinates and the *hour* of *ts*;
    readings are therefore stable within a single clock-hour.  Callers that
    want per-minute variation must supply a different city or fabricate a
    distinct seed by subclassing City.
    """
    rng = random.Random(_seed(city, ts))

    # Fixed call order — see module docstring.
    temp        = _temperature(city, ts, rng)
    humidity    = _humidity(city, ts, rng)
    wind        = _wind_kph(rng)
    wind_dir    = _wind_direction_deg(rng)
    cloud       = _cloud_cover(humidity, rng)
    precip_prob = _precip_probability(humidity, cloud)
    precip      = _precip_mm(precip_prob, rng)

    return WeatherReading(
        temperature_c=temp,
        feels_like_c=_feels_like(temp, wind, humidity),
        dew_point_c=_dew_point(temp, humidity),
        humidity_pct=humidity,
        wind_kph=wind,
        wind_direction_deg=wind_dir,
        cloud_cover_pct=cloud,
        precip_probability_pct=precip_prob,
        precip_mm=precip,
        condition=_condition(cloud, precip_prob),
    )
