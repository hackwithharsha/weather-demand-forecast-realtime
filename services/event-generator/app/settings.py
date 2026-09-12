from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    # Comma-separated city keys drawn from common.generator.CITIES
    cities: str = Field(
        default="london,new_york,tokyo,sydney,dubai",
        description="Comma-separated list of city keys to generate events for",
    )

    # Real-time interval between simulation ticks
    tick_interval_s: float = Field(
        default=1.0,
        ge=0.05,
        description="Real seconds between ticks",
    )

    # How many simulated seconds advance per real second
    sim_speed: float = Field(
        default=3600.0,
        gt=0,
        description="Simulated seconds per real second (3600 = 1 sim-hour per tick)",
    )

    # Optional fixed start time; defaults to current UTC time when empty
    sim_start: str = Field(
        default="",
        description="ISO-8601 datetime for simulation start, e.g. 2024-01-01T00:00:00Z",
    )

    kafka_bootstrap_servers: str = Field(
        default="redpanda:9092",
        description="Comma-separated Kafka bootstrap servers",
    )

    demand_topic: str = Field(
        default="demand.events.v1",
        description="Topic to produce demand events to",
    )

    @field_validator("sim_start", mode="before")
    @classmethod
    def _coerce_none(cls, v: object) -> str:
        return "" if v is None else str(v)

    def city_list(self) -> list[str]:
        return [c.strip() for c in self.cities.split(",") if c.strip()]

    def start_time(self) -> datetime:
        if self.sim_start:
            return datetime.fromisoformat(self.sim_start)
        from datetime import timezone
        return datetime.now(timezone.utc)
