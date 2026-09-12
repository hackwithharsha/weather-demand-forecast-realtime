from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    port: int = 8001
    log_level: str = "INFO"

    # Chaos controls — all disabled by default
    fault_latency_ms: int = 0           # artificial delay added to every response
    fault_error_rate: float = 0.0       # 0.0–1.0 probability of returning HTTP 503
    fault_null_field_rate: float = 0.0  # 0.0–1.0 probability of nulling any field
    fault_schema_drift: bool = False    # rename "temperature" → "temp" everywhere
