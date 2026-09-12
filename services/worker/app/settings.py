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

    # MinIO / Parquet lake
    minio_endpoint_url: str = "http://minio:9000"
    minio_access_key:   str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key:   str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    lake_bucket:        str = "lake"

    # Pipeline behaviour
    # How far back (in days) the staging populate and initial scaler fit look.
    training_lookback_days: int = 30
    # Minute of the hour at which the cron job fires (default: 5 past the hour,
    # allowing the ingestor's tail events for the previous hour to land in
    # raw.* before staging reads them).
    pipeline_cron_minute:   int = 5

    # S3 key inside lake_bucket where the fitted scaler is stored.
    # Increment the version suffix to force a re-fit on the next run.
    scaler_s3_key: str = "artifacts/scalers/features_v1.pkl"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
