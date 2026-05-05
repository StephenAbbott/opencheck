"""Runtime configuration loaded from environment (.env in dev)."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolved_env_files() -> tuple[str, ...]:
    """Locations we check for a ``.env`` file.

    pydantic-settings interprets relative paths as CWD-relative, which
    breaks when uvicorn is launched from ``backend/`` while the actual
    ``.env`` lives at the repo root. We resolve both candidates to
    absolute paths so the lookup works regardless of CWD.

    Tests set ``OPENCHECK_DISABLE_DOTENV=1`` so monkeypatched env vars
    (and the absence of optional ones) aren't shadowed by whatever the
    developer happens to have in their real ``.env``.
    """
    if os.environ.get("OPENCHECK_DISABLE_DOTENV"):
        return ()
    here = Path(__file__).resolve()
    backend_dir = here.parents[1]   # backend/
    project_root = here.parents[2]  # repo root
    return (
        str(project_root / ".env"),
        str(backend_dir / ".env"),
        ".env",  # final CWD-relative fallback for older setups
    )


class Settings(BaseSettings):
    """OpenCheck environment settings.

    All keys are optional in Phase 0 because every source adapter returns
    stub responses. A key is only required once its adapter graduates to
    live mode AND ``allow_live`` is true.
    """

    model_config = SettingsConfigDict(
        env_file=_resolved_env_files(),
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

    # --- OpenCorporates ---
    opencorporates_api_key: str | None = Field(
        default=None, alias="OPENCORPORATES_API_KEY"
    )
    # Path to the OpenCorporates Relationships bulk CSV file.
    # When set, the OpenCorporates adapter will look up ownership relationships
    # from this file instead of (or in addition to) the live /network API
    # endpoint.  Leave unset (default) to disable bulk-file lookup entirely.
    # The file must match the OC Relationships CSV schema (columns:
    # relationship_type, oc_relationship_identifier, subject_entity_name,
    # subject_entity_company_number, subject_entity_jurisdiction_code,
    # object_entity_name, object_entity_company_number,
    # object_entity_jurisdiction_code, percentage_min_share_ownership, …).
    opencorporates_relationships_file: str | None = Field(
        default=None, alias="OPENCORPORATES_RELATIONSHIPS_FILE"
    )

    # --- BrightQuery ---
    # Path to the SQLite database produced by scripts/extract_brightquery.py.
    # When set, the BrightQuery adapter provides LEI-keyed lookup of US
    # entities and their associated executives from OpenData.org bulk data.
    brightquery_db_file: str | None = Field(
        default=None, alias="BRIGHTQUERY_DB_FILE"
    )

    # --- Zefix (Swiss Federal Commercial Registry) ---
    # HTTP Basic credentials — request via zefix@bj.admin.ch.
    zefix_username: str | None = Field(default=None, alias="ZEFIX_USERNAME")
    zefix_password: str | None = Field(default=None, alias="ZEFIX_PASSWORD")

    # --- INPI (Institut National de la Propriété Industrielle) ---
    # Bearer token auth: POST /api/sso/login with username + password.
    # Request access at https://registre-national-entreprises.inpi.fr/
    inpi_username: str | None = Field(default=None, alias="INPI_USERNAME")
    inpi_password: str | None = Field(default=None, alias="INPI_PASSWORD")

    # --- Bolagsverket (Swedish Companies Registration Office) ---
    # OAuth2 Client Credentials Grant. The client_id and client_secret are
    # issued via the developer portal. Request access at:
    #   https://portal.api.bolagsverket.se/ (production)
    #   https://portal-accept2.api.bolagsverket.se/ (test/accept2)
    # BOLAGSVERKET_API_KEY is the OAuth2 client_id (Consumer Key).
    # BOLAGSVERKET_CLIENT_SECRET is the OAuth2 client_secret (Consumer Secret).
    bolagsverket_api_key: str | None = Field(default=None, alias="BOLAGSVERKET_API_KEY")
    bolagsverket_client_secret: str | None = Field(default=None, alias="BOLAGSVERKET_CLIENT_SECRET")

    # --- Optional LLM ---
    anthropic_api_key: str | None = Field(default=None, alias="ANTHROPIC_API_KEY")

    # --- AMLA risk-rule tuning ---
    # Codes added to the built-in EU+EEA set. Comma-separated ISO 3166-1
    # alpha-2 codes — e.g. ``GB,CH,US`` to suppress NON_EU_JURISDICTION
    # for those jurisdictions.
    amla_equivalent_jurisdictions: str = Field(
        default="", alias="OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS"
    )
    # When set, replaces the entire EU+EEA default. Use sparingly — most
    # users will prefer the additive variable above. Useful only when
    # someone wants strict AMLA EU-only (no EEA) or a totally custom set.
    amla_eu_eea_override: str | None = Field(
        default=None, alias="OPENCHECK_AMLA_EU_EEA_OVERRIDE"
    )


@lru_cache
def get_settings() -> Settings:
    """Cached settings accessor."""
    return Settings()
