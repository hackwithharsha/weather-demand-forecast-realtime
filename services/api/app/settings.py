from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Redis — real-time weather override (written by stream worker)
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""

    # Postgres — feature mart fallback + history + city list
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "forecast"
    postgres_user: str = "forecast"
    postgres_password: str = ""

    # MLflow — model registry
    mlflow_tracking_uri: str = "http://mlflow:5000"
    mlflow_registered_model_name: str = "demand-forecaster"

    # MinIO / S3 credentials forwarded to MLflow artifact store
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    mlflow_s3_endpoint_url: str = "http://minio:9000"

    # Prometheus — used by /pipeline/stats and /drift/* to proxy metric queries
    prometheus_url: str = "http://prometheus:9090"

    model_config = {"env_file": ".env", "extra": "ignore"}
