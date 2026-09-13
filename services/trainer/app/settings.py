from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Postgres — source of mart features
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "forecast"
    postgres_user: str = "forecast"
    postgres_password: str

    # MLflow tracking server
    mlflow_tracking_uri: str = "http://mlflow:5000"
    mlflow_experiment_name: str = "demand-forecast"
    mlflow_registered_model_name: str = "demand-forecaster"

    # Training knobs
    training_lookback_days: int = 30   # calendar days of mart history to load
    val_days: int = 7                  # last N days withheld as time-based holdout

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )
