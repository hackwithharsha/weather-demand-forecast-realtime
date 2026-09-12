"""
Response schemas matching the Google Weather API shape.
Used as documentation and for type-safety in weather.py.
FastAPI routes return plain dicts so chaos transforms can freely mutate them.
"""

from pydantic import BaseModel


class _Unit(BaseModel):
    degrees: float
    unit: str


class _WindSpeed(BaseModel):
    value: float
    unit: str


class _WindDirection(BaseModel):
    degrees: float
    cardinal: str


class _Wind(BaseModel):
    speed: _WindSpeed
    direction: _WindDirection


class _PrecipProbability(BaseModel):
    percent: int
    type: str


class _PrecipQpf(BaseModel):
    quantity: float
    unit: str


class _Precipitation(BaseModel):
    probability: _PrecipProbability
    qpf: _PrecipQpf


class _ConditionDesc(BaseModel):
    text: str
    languageCode: str


class _WeatherCondition(BaseModel):
    description: _ConditionDesc
    type: str
    iconBaseUri: str


class _Visibility(BaseModel):
    distance: float
    unit: str


class _ObservationPoint(BaseModel):
    """Fields shared by current conditions and each forecast hour."""

    temperature: _Unit
    feelsLike: _Unit
    dewPoint: _Unit
    humidity: int
    wind: _Wind
    precipitation: _Precipitation
    weatherCondition: _WeatherCondition
    uvIndex: int
    visibility: _Visibility
    cloudCover: int


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
