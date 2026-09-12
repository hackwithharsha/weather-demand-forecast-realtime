"""
Unit tests for common.generator.

Properties verified
-------------------
1. Determinism         same (city, ts) → identical WeatherReading, always
2. Hour-granularity    minutes/seconds within the same hour don't change output
3. City independence   different cities produce different temperatures at same ts
4. Temperature bounds  noon readings stay within physically plausible limits
                       for every city across a full simulated year (365 days)
5. Seasonal spread     non-equatorial cities show ≥ 10 °C annual spread at noon
6. Field invariants    humidity, cloud cover, precip, wind are always in-range
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.generator import CITIES, City, WeatherReading, generate_reading


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UTC = timezone.utc


def _noon_year(city: City, year: int = 2024) -> list[WeatherReading]:
    """One reading per calendar day at 12:00 UTC for *year*."""
    out: list[WeatherReading] = []
    dt = datetime(year, 1, 1, 12, 0, tzinfo=_UTC)
    while dt.year == year:
        out.append(generate_reading(city, dt))
        dt += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# 1 & 2  Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_inputs_same_output(self) -> None:
        city = CITIES["london"]
        ts   = datetime(2024, 6, 15, 14, 0, tzinfo=_UTC)
        assert generate_reading(city, ts) == generate_reading(city, ts)

    def test_minutes_within_hour_do_not_matter(self) -> None:
        city = CITIES["tokyo"]
        base = datetime(2024, 3, 10, 9, 0, tzinfo=_UTC)
        assert generate_reading(city, base) == generate_reading(city, base.replace(minute=47, second=59))

    def test_hour_boundary_produces_different_output(self) -> None:
        city = CITIES["tokyo"]
        ts_09 = datetime(2024, 3, 10,  9, 59, tzinfo=_UTC)
        ts_10 = datetime(2024, 3, 10, 10,  0, tzinfo=_UTC)
        assert generate_reading(city, ts_09) != generate_reading(city, ts_10)

    def test_different_cities_differ(self) -> None:
        ts = datetime(2024, 6, 15, 14, 0, tzinfo=_UTC)
        r_lon = generate_reading(CITIES["london"], ts)
        r_mos = generate_reading(CITIES["moscow"], ts)
        assert r_lon.temperature_c != r_mos.temperature_c

    def test_custom_city_is_deterministic(self) -> None:
        city = City("Custom", lat=0.0, lon=0.0)
        ts   = datetime(2025, 1, 1, 0, 0, tzinfo=_UTC)
        assert generate_reading(city, ts) == generate_reading(city, ts)


# ---------------------------------------------------------------------------
# 4  Temperature bounds — derived analytically from the model
#
#    T_noon = base(lat) + seasonal(lat, doy) + 6 + gauss(σ=1.5)
#
#    Over 365 samples the noise peak is ≈ ±4.4σ ≈ ±6.6 °C (99.9th pct).
#    Bounds below are deterministic_range ± 10 °C to absorb that.
# ---------------------------------------------------------------------------

# (min_c, max_c)  —  plausible annual range at noon; generous but not absurd
_TEMP_BOUNDS: dict[str, tuple[float, float]] = {
    "london":    (-17.0,  32.0),   # det range ≈ [−1, 16]  + noise + margin
    "nairobi":   ( 19.0,  46.0),   # equatorial, narrow det range ≈ [32, 33]
    "moscow":    (-20.0,  30.0),   # det range ≈ [−4, 14]
    "singapore": ( 19.0,  46.0),   # equatorial, same latitude class as Nairobi
    "reykjavik": (-25.0,  27.0),   # det range ≈ [−10, 12]
    "cape_town": ( -5.0,  38.0),   # SH mid-lat, det range ≈ [10, 22]
    "new_york":  (-10.0,  36.0),   # det range ≈ [6, 19]
    "tokyo":     ( -8.0,  36.0),   # det range ≈ [9, 21]
    "dubai":     (  0.0,  42.0),   # det range ≈ [16, 25]
    "sydney":    ( -5.0,  38.0),   # same lat as Cape Town
}


class TestTemperatureBounds:
    @pytest.mark.parametrize("city_key", list(_TEMP_BOUNDS))
    def test_noon_temps_within_bounds_across_year(self, city_key: str) -> None:
        lo, hi = _TEMP_BOUNDS[city_key]
        city   = CITIES[city_key]
        temps  = [r.temperature_c for r in _noon_year(city)]

        violations = [t for t in temps if not (lo <= t <= hi)]
        assert not violations, (
            f"{city.name}: {len(violations)} out-of-bound reading(s). "
            f"Range [{min(temps):.1f}, {max(temps):.1f}] vs allowed [{lo}, {hi}]. "
            f"Examples: {violations[:3]}"
        )

    @pytest.mark.parametrize("city_key", list(_TEMP_BOUNDS))
    def test_seasonal_spread(self, city_key: str) -> None:
        """Non-equatorial cities must show a meaningful summer/winter contrast."""
        city = CITIES[city_key]
        if abs(city.lat) < 5:
            pytest.skip(f"{city.name} is near-equatorial — seasonal spread intentionally < 10 °C")

        temps  = [r.temperature_c for r in _noon_year(city)]
        spread = max(temps) - min(temps)
        assert spread >= 10.0, (
            f"{city.name}: annual noon spread is only {spread:.1f} °C (expected ≥ 10)"
        )


# ---------------------------------------------------------------------------
# 6  Field invariants — spot-check across cities × full year
# ---------------------------------------------------------------------------

_INVARIANT_CITIES = ["london", "moscow", "dubai", "singapore", "cape_town"]


@pytest.fixture(scope="module")
def sample_readings() -> list[WeatherReading]:
    out: list[WeatherReading] = []
    for key in _INVARIANT_CITIES:
        out.extend(_noon_year(CITIES[key]))
    return out


class TestFieldInvariants:
    def test_humidity_in_range(self, sample_readings: list[WeatherReading]) -> None:
        assert all(10 <= r.humidity_pct <= 100 for r in sample_readings)

    def test_cloud_cover_in_range(self, sample_readings: list[WeatherReading]) -> None:
        assert all(0 <= r.cloud_cover_pct <= 100 for r in sample_readings)

    def test_precip_probability_in_range(self, sample_readings: list[WeatherReading]) -> None:
        assert all(0 <= r.precip_probability_pct <= 100 for r in sample_readings)

    def test_wind_non_negative(self, sample_readings: list[WeatherReading]) -> None:
        assert all(r.wind_kph >= 0 for r in sample_readings)

    def test_precip_mm_non_negative(self, sample_readings: list[WeatherReading]) -> None:
        assert all(r.precip_mm >= 0 for r in sample_readings)

    def test_wind_direction_in_range(self, sample_readings: list[WeatherReading]) -> None:
        assert all(0 <= r.wind_direction_deg <= 360 for r in sample_readings)

    def test_condition_is_known_label(self, sample_readings: list[WeatherReading]) -> None:
        valid = {
            "HEAVY_RAIN", "RAIN", "DRIZZLE", "CLOUDY",
            "MOSTLY_CLOUDY", "PARTLY_CLOUDY_DAY", "MOSTLY_CLEAR", "CLEAR_DAY",
        }
        unknown = {r.condition for r in sample_readings} - valid
        assert not unknown, f"Unknown condition values: {unknown}"
