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
    sync_symbols: str = ""
    sync_universes: str = ""
    sync_rotation_batch_size: int = 1000

    @field_validator(
        "fmp_requests_per_minute",
        "fmp_daily_request_budget",
        "fmp_concurrency",
        "sync_shard_size",
        "sync_rotation_batch_size",
    )
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

    @field_validator("sync_symbols")
    @classmethod
    def normalize_sync_symbols(cls, value: str) -> str:
        return _normalize_csv(value)

    @field_validator("sync_universes")
    @classmethod
    def normalize_sync_universes(cls, value: str) -> str:
        universes = _normalize_csv(value).lower().split(",") if value.strip() else []
        allowed = {"crypto", "nasdaq", "forex_g10"}
        invalid = sorted(set(universes) - allowed)
        if invalid:
            raise ValueError("SYNC_UNIVERSES contains unsupported values: " + ", ".join(invalid))
        return ",".join(universes)

    @property
    def configured(self) -> bool:
        return bool(self.fmp_api_key.get_secret_value())

    @property
    def sync_symbol_list(self) -> tuple[str, ...]:
        return tuple(symbol for symbol in self.sync_symbols.split(",") if symbol)

    @property
    def sync_universe_list(self) -> tuple[str, ...]:
        return tuple(universe for universe in self.sync_universes.split(",") if universe)


def _normalize_csv(value: str) -> str:
    symbols: list[str] = []
    for raw_symbol in value.split(","):
        symbol = raw_symbol.strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return ",".join(symbols)


@lru_cache
def get_settings() -> Settings:
    return Settings()
