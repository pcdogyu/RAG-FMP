from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    fmp_api_key: SecretStr = Field(default=SecretStr(""))
    mcp_bearer_token: SecretStr = Field(default=SecretStr(""))
    weknora_base_url: str = "http://weknora-app:8080"
    weknora_api_key: SecretStr = Field(default=SecretStr(""))
    weknora_knowledge_base_id: str = ""
    database_url: str = "sqlite:///./data/fmp_bridge.db"
    redis_url: str = ""
    host: str = "0.0.0.0"
    port: int = 8000
    fmp_base_url: str = "https://financialmodelingprep.com/stable"
    fmp_requests_per_minute: int = 240
    fmp_daily_request_budget: int = 100000
    fmp_concurrency: int = 8
    sync_shard_size: int = 250
    sync_enabled: bool = True
    sync_hourly_minute: int = 10
    sync_catalog_hour: int = 2
    sync_bootstrap_limit: int = 0

    @field_validator("fmp_requests_per_minute", "fmp_daily_request_budget", "fmp_concurrency")
    @classmethod
    def positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be greater than zero")
        return value

    @field_validator("sync_hourly_minute")
    @classmethod
    def valid_minute(cls, value: int) -> int:
        if not 0 <= value <= 59:
            raise ValueError("must be between 0 and 59")
        return value

    @property
    def configured(self) -> bool:
        return bool(self.fmp_api_key.get_secret_value())


@lru_cache
def get_settings() -> Settings:
    return Settings()
