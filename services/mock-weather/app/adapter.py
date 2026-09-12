"""
Adapts the pure WeatherReading (flat, SI units) from common.generator into
the nested camelCase dict shape expected by the Google Weather API contract.

UV index and visibility are presentation-layer concerns not held in the core
WeatherReading, so they are computed here from the fields that are available.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from common.generator import City, WeatherReading, generate_reading

_CARDINALS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]

_CONDITION_TEXT: dict[str, str] = {
    "HEAVY_RAIN":        "Heavy rain",
    "RAIN":              "Rain",
    "DRIZZLE":           "Light drizzle",
    "CLOUDY":            "Overcast",
    "MOSTLY_CLOUDY":     "Mostly cloudy",
    "PARTLY_CLOUDY_DAY": "Partly cloudy",
    "MOSTLY_CLEAR":      "Mostly clear",
    "CLEAR_DAY":         "Clear",
}


def _cardinal(deg: float) -> str:
    return _CARDINALS[round(deg / 22.5) % 16]


def _uv_index(ts: datetime, cloud_pct: int) -> int:
    """0 at night; peaks at solar noon; reduced by cloud cover."""
    if ts.hour < 6 or ts.hour > 20:
        return 0
    solar = math.sin(math.pi * (ts.hour - 6) / 14)
    return max(0, min(11, round(solar * 10.0 * (1 - cloud_pct / 150.0))))


def _visibility_km(humidity: int, cloud: int) -> float:
    vis = 20.0 - (humidity - 50) * 0.1 - cloud * 0.05
    return round(max(1.0, min(20.0, vis)), 1)


def _to_point(r: WeatherReading, ts: datetime) -> dict:
    """Convert a WeatherReading to a nested API-shaped dict for one time point."""
    return {
        "temperature":   {"degrees": r.temperature_c,   "unit": "CELSIUS"},
        "feelsLike":     {"degrees": r.feels_like_c,     "unit": "CELSIUS"},
        "dewPoint":      {"degrees": r.dew_point_c,      "unit": "CELSIUS"},
        "humidity":      r.humidity_pct,
        "wind": {
            "speed":     {"value": r.wind_kph,              "unit": "KILOMETERS_PER_HOUR"},
            "direction": {"degrees": r.wind_direction_deg,  "cardinal": _cardinal(r.wind_direction_deg)},
        },
        "precipitation": {
            "probability": {"percent": r.precip_probability_pct, "type": "RAIN"},
            "qpf":         {"quantity": r.precip_mm,              "unit": "MILLIMETERS"},
        },
        "weatherCondition": {
            "description": {
                "text":         _CONDITION_TEXT.get(r.condition, r.condition),
                "languageCode": "en",
            },
            "type":        r.condition,
            "iconBaseUri": f"https://maps.gstatic.com/weather/v1/{r.condition.lower()}",
        },
        "uvIndex":    _uv_index(ts, r.cloud_cover_pct),
        "visibility": {"distance": _visibility_km(r.humidity_pct, r.cloud_cover_pct), "unit": "KILOMETERS"},
        "cloudCover": r.cloud_cover_pct,
    }


def current_payload(city: City) -> dict:
    ts = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    r  = generate_reading(city, ts)
    return {"currentConditions": {"time": datetime.now(timezone.utc).isoformat(), **_to_point(r, ts)}}


def forecast_payload(city: City, hours: int) -> dict:
    start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    items = []
    for i in range(hours):
        dt = start + timedelta(hours=i)
        r  = generate_reading(city, dt)
        items.append({
            "interval": {
                "startTime": dt.isoformat(),
                "endTime":   (dt + timedelta(hours=1)).isoformat(),
            },
            **_to_point(r, dt),
        })
    return {"forecastHours": items}
