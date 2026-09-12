"""
Deterministic weather data generation.

Design rules
------------
- Seed = hash(round(lat,1), round(lon,1), year, month, day, hour)
  → identical inputs always produce identical outputs.
- Temperature follows a physics-inspired model:
    base (latitude) + seasonal offset (day-of-year) + diurnal cycle (hour) + seed noise
- Humidity, wind, and precipitation use the seeded RNG so they are
  consistent per city-hour but feel independent of temperature.
- Precipitation probability is Poisson-ish: proportional to humidity × cloud cover.
"""

import math
import random
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Seed
# ---------------------------------------------------------------------------

def _make_seed(lat: float, lon: float, dt: datetime) -> int:
    return hash((round(lat, 1), round(lon, 1), dt.year, dt.month, dt.day, dt.hour))


def _rng(lat: float, lon: float, dt: datetime) -> random.Random:
    return random.Random(_make_seed(lat, lon, dt))


# ---------------------------------------------------------------------------
# Temperature
# ---------------------------------------------------------------------------

def _base_temp(lat: float) -> float:
    """Equator ≈ 27 °C, poles ≈ −30 °C."""
    return 27.0 - 0.63 * abs(lat)


def _seasonal_offset(lat: float, dt: datetime) -> float:
    """±15 °C swing at high latitudes; negligible at the equator.
    Northern-hemisphere peak around day 172 (summer solstice)."""
    doy = dt.timetuple().tm_yday
    amplitude = 15.0 * abs(lat) / 90.0
    hemisphere = 1.0 if lat >= 0 else -1.0
    return amplitude * hemisphere * math.sin(2 * math.pi * (doy - 80) / 365)


def _diurnal_offset(hour: int) -> float:
    """Minimum at ~06:00, maximum at ~14:00, ±6 °C swing."""
    return 6.0 * math.sin(2 * math.pi * (hour - 6) / 24)


def _temperature(lat: float, lon: float, dt: datetime) -> float:
    rng = _rng(lat, lon, dt)
    noise = rng.gauss(0, 1.5)
    return round(_base_temp(lat) + _seasonal_offset(lat, dt) + _diurnal_offset(dt.hour) + noise, 1)


# ---------------------------------------------------------------------------
# Derived quantities
# ---------------------------------------------------------------------------

def _humidity(rng: random.Random, lat: float, dt: datetime) -> int:
    """Base humidity peaks around 30° latitude; higher variance at mid-latitudes."""
    base = 70.0 - abs(abs(lat) - 30) * 0.4
    # Slight seasonal boost: more humid in local summer
    doy = dt.timetuple().tm_yday
    seasonal = 8.0 * math.sin(2 * math.pi * (doy - 80) / 365) * (1 if lat >= 0 else -1)
    return max(10, min(100, round(base + seasonal + rng.gauss(0, 12))))


def _cloud_cover(humidity: int, rng: random.Random) -> int:
    base = (humidity - 40) * 1.3
    return max(0, min(100, round(base + rng.gauss(0, 10))))


def _precip_probability(humidity: int, cloud_cover: int) -> int:
    prob = max(0, (humidity - 55) * 1.5 + (cloud_cover - 50) * 0.4)
    return min(100, round(prob))


def _precip_qpf(precip_prob: int, rng: random.Random) -> float:
    """Exponential distribution scaled by probability — Poisson-ish mm/hr."""
    if precip_prob < 20:
        return 0.0
    rate = max(0.1, precip_prob / 100 * 5)
    raw = rng.expovariate(1 / rate) * (precip_prob / 100)
    return round(raw, 2)


def _wind_speed(rng: random.Random) -> float:
    return round(max(0.0, rng.gauss(18, 9)), 1)


def _wind_direction(rng: random.Random) -> float:
    return round(rng.uniform(0, 360), 1)


def _feels_like(temp: float, wind_kph: float, humidity: int) -> float:
    wind_ms = wind_kph / 3.6
    if temp > 27:
        # Simplified heat index
        hi = temp + 0.33 * (humidity / 100 * 6.1) - 0.70 * wind_ms - 4.0
        return round(hi, 1)
    if temp < 10:
        # Simplified wind chill
        return round(temp - wind_kph * 0.25, 1)
    return temp


def _dew_point(temp: float, humidity: int) -> float:
    """Magnus approximation."""
    return round(temp - (100 - humidity) / 5.0, 1)


def _visibility(cloud_cover: int, humidity: int) -> float:
    vis = 20.0 - (humidity - 50) * 0.1 - cloud_cover * 0.05
    return round(max(1.0, min(20.0, vis)), 1)


def _uv_index(dt: datetime, cloud_cover: int) -> int:
    if dt.hour < 6 or dt.hour > 20:
        return 0
    solar = math.sin(math.pi * (dt.hour - 6) / 14)
    uv = solar * 10 * (1 - cloud_cover / 150)
    return max(0, min(11, round(uv)))


# ---------------------------------------------------------------------------
# Condition label
# ---------------------------------------------------------------------------

_CONDITION_LABELS: dict[str, str] = {
    "HEAVY_RAIN":         "Heavy rain",
    "RAIN":               "Rain",
    "DRIZZLE":            "Light drizzle",
    "CLOUDY":             "Overcast",
    "MOSTLY_CLOUDY":      "Mostly cloudy",
    "PARTLY_CLOUDY_DAY":  "Partly cloudy",
    "MOSTLY_CLEAR":       "Mostly clear",
    "CLEAR_DAY":          "Clear",
}


def _condition_type(cloud_cover: int, precip_prob: int) -> str:
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


_CARDINALS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
              "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]


def _cardinal(degrees: float) -> str:
    return _CARDINALS[round(degrees / 22.5) % 16]


# ---------------------------------------------------------------------------
# Point assembly
# ---------------------------------------------------------------------------

def _point(lat: float, lon: float, dt: datetime) -> dict:
    rng = _rng(lat, lon, dt)

    temp        = _temperature(lat, lon, dt)
    humidity    = _humidity(rng, lat, dt)
    wind_spd    = _wind_speed(rng)
    wind_dir    = _wind_direction(rng)
    cloud       = _cloud_cover(humidity, rng)
    precip_prob = _precip_probability(humidity, cloud)
    qpf         = _precip_qpf(precip_prob, rng)
    ctype       = _condition_type(cloud, precip_prob)

    return {
        "temperature":      {"degrees": temp, "unit": "CELSIUS"},
        "feelsLike":        {"degrees": _feels_like(temp, wind_spd, humidity), "unit": "CELSIUS"},
        "dewPoint":         {"degrees": _dew_point(temp, humidity), "unit": "CELSIUS"},
        "humidity":         humidity,
        "wind": {
            "speed":     {"value": wind_spd, "unit": "KILOMETERS_PER_HOUR"},
            "direction": {"degrees": wind_dir, "cardinal": _cardinal(wind_dir)},
        },
        "precipitation": {
            "probability": {"percent": precip_prob, "type": "RAIN"},
            "qpf":         {"quantity": qpf, "unit": "MILLIMETERS"},
        },
        "weatherCondition": {
            "description": {"text": _CONDITION_LABELS[ctype], "languageCode": "en"},
            "type":        ctype,
            "iconBaseUri": f"https://maps.gstatic.com/weather/v1/{ctype.lower()}",
        },
        "uvIndex":    _uv_index(dt, cloud),
        "visibility": {"distance": _visibility(cloud, humidity), "unit": "KILOMETERS"},
        "cloudCover": cloud,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_current(lat: float, lon: float) -> dict:
    now = datetime.now(timezone.utc)
    dt  = now.replace(minute=0, second=0, microsecond=0)  # truncate to hour for seed
    return {"currentConditions": {"time": now.isoformat(), **_point(lat, lon, dt)}}


def generate_forecast(lat: float, lon: float, hours: int) -> dict:
    now   = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0)
    items = []
    for i in range(hours):
        dt = start + timedelta(hours=i)
        items.append({
            "interval": {
                "startTime": dt.isoformat(),
                "endTime":   (dt + timedelta(hours=1)).isoformat(),
            },
            **_point(lat, lon, dt),
        })
    return {"forecastHours": items}
