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

    # S3 key for the full feature-engineering Pipeline (imputer+scaler+OHE).
    # Increment the version suffix to force a re-fit on the next run.
    features_pipeline_s3_key: str = "artifacts/pipelines/features_v1.pkl"

    # Kafka (stream feature consumers)
    kafka_bootstrap_servers: str = "redpanda:9092"
    demand_topic:            str = "demand.events.v1"
    weather_topic:           str = "weather.readings.v1"

    stream_demand_group_id:          str = "worker-stream-demand"
    stream_weather_group_id:         str = "worker-stream-weather"
    stream_parquet_flush_interval_s: int = 60

    # Redis (offline feature store)
    redis_host:     str = "redis"
    redis_port:     int = 6379
    redis_db:       int = 0
    redis_password: str = Field(..., description="Required — set REDIS_PASSWORD")

    # Feature store sync schedule.
    # Defaults to 02:30 UTC — after the 02:05 mart run completes but well
    # before peak serving load starts.
    feature_store_cron_hour:   int = 2
    feature_store_cron_minute: int = 30

    # MLflow (drift job — reference dataset from the Production model run)
    mlflow_tracking_uri:           str = "http://mlflow:5000"
    mlflow_registered_model_name:  str = "demand-forecaster"
    mlflow_s3_endpoint_url:        str = "http://minio:9000"
    # Use MINIO_SVC_ACCESS_KEY / MINIO_SVC_SECRET_KEY (same creds as the
    # MLflow server) so the worker can read from the s3://mlflow/ bucket.
    mlflow_s3_access_key: str = Field(default="minioadmin", alias="MINIO_SVC_ACCESS_KEY")
    mlflow_s3_secret_key: str = Field(default="minioadmin", alias="MINIO_SVC_SECRET_KEY")

    # Drift check schedule
    drift_check_interval_minutes: int = 60   # how often to run Evidently
    drift_current_hours:          int = 24   # hours of prediction_features as current window
    drift_min_current_rows:       int = 20   # skip if fewer rows than this

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
