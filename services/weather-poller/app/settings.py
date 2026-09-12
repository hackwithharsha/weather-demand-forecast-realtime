from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    mock_weather_url: str = Field(
        default="http://mock-weather:8001",
        description="Base URL of the mock-weather service",
    )

    cities: str = Field(
        default="london,new_york,tokyo,sydney,dubai",
        description="Comma-separated city keys (must exist in common.generator.CITIES)",
    )

    poll_interval_s: float = Field(
        default=30.0,
        gt=0,
        description="Seconds between polling cycles",
    )

    kafka_bootstrap_servers: str = Field(
        default="redpanda:9092",
        description="Comma-separated Kafka bootstrap servers",
    )

    weather_topic: str = Field(
        default="weather.readings.v1",
        description="Topic to produce weather readings to",
    )

    def city_list(self) -> list[str]:
        return [c.strip() for c in self.cities.split(",") if c.strip()]
