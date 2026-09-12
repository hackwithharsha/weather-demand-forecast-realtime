from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    # Postgres
    postgres_host:     str = "postgres"
    postgres_port:     int = 5432
    postgres_db:       str = "forecast"
    postgres_user:     str = "forecast"
    postgres_password: str = Field(..., description="Required — set POSTGRES_PASSWORD")

    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="redpanda:9092",
        description="Comma-separated Kafka bootstrap servers",
    )

    demand_topic:  str = "demand.events.v1"
    weather_topic: str = "weather.readings.v1"
    dlq_topic:     str = "demand.events.dlq"

    demand_group_id: str = Field(
        default="ingestor-demand",
        description="Consumer group for demand.events.v1",
    )
    weather_group_id: str = Field(
        default="ingestor-weather",
        description="Consumer group for weather.readings.v1",
    )

    # MinIO / Parquet lake
    minio_endpoint_url:       str = "http://minio:9000"
    minio_access_key:         str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key:         str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    lake_bucket:              str = "lake"
    parquet_flush_interval_s: int = 60

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )
