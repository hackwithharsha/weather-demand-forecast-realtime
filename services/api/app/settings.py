from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""

    model_config = {"env_file": ".env", "extra": "ignore"}
