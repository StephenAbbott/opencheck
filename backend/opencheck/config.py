"""Runtime configuration loaded from environment (.env in dev)."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """OpenCheck environment settings.

    All keys are optional in Phase 0 because every source adapter returns
    stub responses. A key is only required once its adapter graduates to
    live mode AND ``allow_live`` is true.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Global toggle. When false, live adapters short-circuit to stubs.
    allow_live: bool = Field(default=False, alias="OPENCHECK_ALLOW_LIVE")

    # CORS origin for the frontend dev server.
    cors_origin: str = Field(
        default="http://localhost:5173", alias="OPENCHECK_CORS_ORIGIN"
    )

    # --- Source credentials ---
    companies_house_api_key: str | None = Field(default=None, alias="COMPANIES_HOUSE_API_KEY")
    opensanctions_api_key: str | None = Field(default=None, alias="OPENSANCTIONS_API_KEY")
    openaleph_api_key: str | None = Field(default=None, alias="OPENALEPH_API_KEY")
    wikidata_sparql_endpoint: str = Field(
        default="https://query.wikidata.org/sparql",
        alias="WIKIDATA_SPARQL_ENDPOINT",
    )

    # --- Optional LLM ---
    anthropic_api_key: str | None = Field(default=None, alias="ANTHROPIC_API_KEY")


@lru_cache
def get_settings() -> Settings:
    """Cached settings accessor."""
    return Settings()
