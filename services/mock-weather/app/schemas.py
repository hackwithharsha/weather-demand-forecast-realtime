"""
Response schemas for the Mock Weather API.

Weather schemas  —  mirror the Google Weather API shape (nested, camelCase).
Admin schemas    —  ChaosStatus and ChaosUpdate for GET/POST /admin/chaos.

Routes return plain dicts rather than these models so that chaos transforms
(null fields, schema drift) can freely mutate the payload after generation.
These classes exist for documentation and OpenAPI schema generation.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared weather point sub-models
# ---------------------------------------------------------------------------

class _TemperatureUnit(BaseModel):
    degrees: float
    unit: str = "CELSIUS"


class _WindSpeed(BaseModel):
    value: float
    unit: str = "KILOMETERS_PER_HOUR"


class _WindDirection(BaseModel):
    degrees: float
    cardinal: str


class _Wind(BaseModel):
    speed: _WindSpeed
    direction: _WindDirection


class _PrecipProbability(BaseModel):
    percent: int
    type: str = "RAIN"


class _PrecipQpf(BaseModel):
    quantity: float
    unit: str = "MILLIMETERS"


class _Precipitation(BaseModel):
    probability: _PrecipProbability
    qpf: _PrecipQpf


class _ConditionDesc(BaseModel):
    text: str
    languageCode: str = "en"


class _WeatherCondition(BaseModel):
    description: _ConditionDesc
    type: str
    iconBaseUri: str


class _Visibility(BaseModel):
    distance: float
    unit: str = "KILOMETERS"


class _ObservationPoint(BaseModel):
    """Fields present at every observation point (current and each forecast hour)."""

    temperature: _TemperatureUnit
    feelsLike: _TemperatureUnit
    dewPoint: _TemperatureUnit
    humidity: int
    wind: _Wind
    precipitation: _Precipitation
    weatherCondition: _WeatherCondition
    uvIndex: int
    visibility: _Visibility
    cloudCover: int


# ---------------------------------------------------------------------------
# Top-level weather responses
# ---------------------------------------------------------------------------

class CurrentConditions(_ObservationPoint):
    time: str


class CurrentWeatherResponse(BaseModel):
    currentConditions: CurrentConditions


class _HourInterval(BaseModel):
    startTime: str
    endTime: str


class ForecastHour(_ObservationPoint):
    interval: _HourInterval


class ForecastResponse(BaseModel):
    forecastHours: list[ForecastHour]


# ---------------------------------------------------------------------------
# Admin: chaos state
# ---------------------------------------------------------------------------

class ChaosStatus(BaseModel):
    """Current chaos configuration returned by GET and POST /admin/chaos."""

    latency_ms: int = Field(description="Milliseconds of artificial delay per response")
    error_rate: float = Field(description="Probability (0–1) of returning HTTP 503")
    null_field_rate: float = Field(description="Probability (0–1) of nulling any response field")
    schema_drift: bool = Field(description="When true, 'temperature' is renamed to 'temp'")


class ChaosUpdate(BaseModel):
    """
    Partial update for POST /admin/chaos.
    Omit a field to leave it unchanged.
    Send an empty body {} to read current state without modifying anything.
    """

    latency_ms: int | None = Field(default=None, ge=0)
    error_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    null_field_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    schema_drift: bool | None = None
