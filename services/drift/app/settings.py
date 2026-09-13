"""Configuration for the drift detection service."""
from pydantic_settings import BaseSettings

# Numeric features to compare between serving (prediction_features) and
# training reference (city_hour_features).
# Cyclic time features (hour_sin/cos, dow_sin/cos) are excluded because
# a 24 h current window always has a different time distribution than a
# 30-day reference — that is expected, not a signal of code drift.
DRIFT_FEATURES: list[str] = [
    "event_count",
    "demand_lag_1h",
    "demand_lag_24h",
    "demand_lag_168h",
    "demand_roll_3h",
    "demand_roll_24h",
    "temperature_c",
    "humidity_pct",
    "precip_mm",
    "is_holiday",
]


class Settings(BaseSettings):
    # Postgres — holds both prediction_features and city_hour_features
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "forecast"
    postgres_user: str = "forecast"
    postgres_password: str

    # Prometheus metrics HTTP server
    metrics_port: int = 8000

    # Drift check window parameters
    reference_days: int = 30    # days of city_hour_features as reference
    current_hours: int = 24     # hours of prediction_features as current
    min_current_rows: int = 20  # minimum rows needed to run Evidently

    # Scheduler
    check_interval_minutes: int = 60

    log_level: str = "INFO"

    model_config = {"extra": "ignore"}

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )
