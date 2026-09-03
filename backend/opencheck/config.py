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
    # Public origin of THIS API, used to build absolute og:image URLs on the
    # /share/{lei} page (crawlers need absolute URLs; the request's Host
    # header is unreliable behind Render's proxy).
    public_api_base: str = Field(
        default="https://api.opencheck.world", alias="OPENCHECK_PUBLIC_API_BASE"
    )
    # Public origin of the FRONTEND, used as the /share/{lei} redirect target
    # and og:url. Deliberately separate from OPENCHECK_CORS_ORIGIN — that is a
    # CORS *policy* value and is legitimately "*" on Render, which is not a
    # navigable URL (shipping the two conflated broke share redirects with
    # a literal "*/?lei=..." target).
    frontend_origin: str = Field(
        default="https://opencheck.world", alias="OPENCHECK_FRONTEND_ORIGIN"
    )

    # --- SEO entity pages (see opencheck/entity_pages.py) ---
    # Local path of entity_pages.sqlite (built by scripts/build_entity_pages_db.py).
    # When unset but a URL is set, the DB is downloaded at boot to /tmp — the
    # arrangement for Render, whose disk is ephemeral. When neither is set the
    # /entity, /sitemaps and /browse routes answer 503 (with Retry-After).
    entity_pages_db_file: str | None = Field(
        default=None, alias="OPENCHECK_ENTITY_PAGES_DB_FILE"
    )
    # URL of a prebuilt entity_pages.sqlite (optionally .gz), e.g. a GitHub
    # Release asset published by the monthly refresh job.
    entity_pages_db_url: str | None = Field(
        default=None, alias="OPENCHECK_ENTITY_PAGES_DB_URL"
    )
    # --- Source health (Phase 161) ---
    # Where the weekly sweep's ``source-health.json`` is read from. The sweep
    # (``.github/workflows/source-health.yml``) uploads it, with the rolling
    # ``source-health-history.json`` beside it, to the ``source-health-latest``
    # GitHub release, the same arrangement the entity-pages DB uses: a URL an
    # ephemeral-filesystem host can read without a rebuild. The file wins
    # when both are set (tests, and a developer reading a local sweep).
    # Empty string disables the fetch: ``/source-health`` then answers
    # ``available: false`` and the sources page shows no health at all.
    source_health_url: str = Field(
        default=(
            "https://github.com/StephenAbbott/opencheck/releases/download/"
            "source-health-latest/source-health.json"
        ),
        alias="OPENCHECK_SOURCE_HEALTH_URL",
    )
    source_health_file: str | None = Field(
        default=None, alias="OPENCHECK_SOURCE_HEALTH_FILE"
    )
    # Directory of a built frontend (frontend/dist). When set, the backend
    # serves the SPA itself — the single-service arrangement that lets
    # opencheck.world serve /entity/* pages from one origin: Render
    # static-site rewrites cannot proxy to another service (verified against
    # Render's docs, 2026-08-03), so the fallback documented on the SEO
    # ticket applies. Unset by default: the existing static-site deploy is
    # untouched until the cutover flips this on.
    frontend_dist_dir: str | None = Field(
        default=None, alias="OPENCHECK_FRONTEND_DIST"
    )
    # GoatCounter endpoint for the server-rendered entity/browse pages
    # (Phase 89 — privacy-respecting analytics). Cookie-less; pages record
    # only fixed path buckets ("/entity", "/browse"), never LEIs or query
    # strings. Set to an empty string to disable analytics on these pages.
    # The SPA's endpoint is baked into frontend/src/lib/analytics.ts.
    goatcounter_endpoint: str = Field(
        default="https://opencheck.goatcounter.com/count",
        alias="OPENCHECK_GOATCOUNTER_ENDPOINT",
    )
    # IndexNow shared key (Phase 91 — SEO Phase D). Served back at
    # /{key}.txt so search engines can verify ownership of the
    # URLs the monthly refresh submits. Same value as the GitHub Actions
    # secret INDEXNOW_KEY. Unset = key route 404s and submission is skipped.
    indexnow_key: str | None = Field(default=None, alias="OPENCHECK_INDEXNOW_KEY")

    # --- Rate limiting / abuse protection (see opencheck/ratelimit.py) ---
    # Master switch. The test suite turns it off in conftest.py so unrelated
    # tests never trip budgets; dedicated tests re-enable it per-fixture.
    rate_limit_enabled: bool = Field(default=True, alias="OPENCHECK_RATE_LIMIT_ENABLED")
    # Per-IP budget for the fan-out endpoints (/lookup, /lookup-stream, /search,
    # /stream, /report, /export). Each request dispatches every registered
    # source adapter, so this is the budget that protects key-gated upstream
    # quotas (Companies House, NZBN, CVR, OpenSanctions, …).
    rate_limit_lookup: str = Field(default="10/minute", alias="OPENCHECK_RATE_LIMIT_LOOKUP")
    # Per-IP budget for the expensive synthesis endpoints (/narrative burns
    # Anthropic tokens; /export/pdf and /export-network burn CPU).
    rate_limit_heavy: str = Field(default="3/minute", alias="OPENCHECK_RATE_LIMIT_HEAVY")
    # Per-IP budget for everything else public (per-source retries, expansion,
    # history, securities, share pages, …). Generous: normal UI usage fires
    # several of these per lookup.
    rate_limit_default: str = Field(default="60/minute", alias="OPENCHECK_RATE_LIMIT_DEFAULT")
    # --- Outbound GLEIF budget (see opencheck/gleif_throttle.py) ---
    # GLEIF rate-limits by IP at 60 req/min, shared across everything this
    # process sends it (anchor lookups, /securities ISINs, Time Machine,
    # subsidiary reveals). This is OUR ceiling — kept under GLEIF's so bursts
    # queue here instead of burning GLEIF's sliding window with 429s (which
    # still count against it). 0 disables the throttle entirely; the test
    # suite does that in conftest.py, dedicated tests re-enable per-fixture.
    gleif_rate_limit_per_minute: int = Field(
        default=50, alias="OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE"
    )
    # How long one GLEIF request may wait for a budget slot before giving up
    # (GleifRateLimitedError → the adapter's stale-cache/snapshot fallback).
    gleif_throttle_max_wait_s: float = Field(
        default=15.0, alias="OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S"
    )
    # Phase 144: when the anchor LEI exists in the entity-pages Golden Copy,
    # don't make the user sit out the full max-wait — if the live anchor fetch
    # hasn't completed within this many seconds, serve the snapshot instead
    # (honestly badged). Only applies when a snapshot row exists, so
    # deployments without the entity-pages DB keep the full live wait.
    # 0 disables the early fallback. Measured 2026-08-29: under crawler
    # saturation the anchor stalled ~15–21s before falling back; this caps it.
    gleif_snapshot_after_s: float = Field(
        default=5.0, alias="OPENCHECK_GLEIF_SNAPSHOT_AFTER_S"
    )
    # Phase 144: refuse declared automated clients (memwatch.is_bot on the
    # User-Agent) on /lookup-stream, the interactive app's SSE endpoint.
    # robots.txt has always disallowed it; a crawler there is ignoring robots
    # and burning the shared upstream budgets (GLEIF's 60 req/min IP cap
    # first among them). Bots get a 403 pointing at the crawlable /entity
    # pages and the plain /lookup JSON API. The gate deliberately does NOT
    # cover /lookup itself — that is the promoted programmatic API, and
    # `python`/`curl`/`httpx` UAs are its legitimate callers.
    bot_gate_lookup_stream: bool = Field(
        default=True, alias="OPENCHECK_BOT_GATE_LOOKUP_STREAM"
    )

    # --- Memory + traffic instrumentation (see opencheck/memwatch.py) ---
    # Interval (seconds) between "memwatch" memory-report log lines. 0 disables
    # the reporter (the access log below is controlled separately). Added after
    # the 2026-08-05/06 Render OOM restarts so the next one is diagnosable
    # from the log stream alone.
    memwatch_interval_s: float = Field(default=30.0, alias="OPENCHECK_MEMWATCH_INTERVAL")
    # RSS as a percentage of the container memory limit above which the
    # memwatch line escalates to WARNING (and, when tracemalloc is on, dumps
    # the top Python allocation sites).
    memwatch_warn_pct: float = Field(default=85.0, alias="OPENCHECK_MEMWATCH_WARN_PCT")
    # Opt-in tracemalloc tracing for the high-water dump. Off by default:
    # tracing costs extra memory (it records every allocation's traceback),
    # which is exactly what an OOM-bound instance can't spare. Turn on
    # temporarily when the memwatch lines alone don't name the culprit.
    memwatch_tracemalloc: bool = Field(default=False, alias="OPENCHECK_MEMWATCH_TRACEMALLOC")
    # Per-request access-log lines (method, path, status, duration, UA, bot
    # flag). uvicorn's own access log has no User-Agent, which makes crawler
    # traffic invisible — this replaces it for diagnosis purposes.
    access_log_enabled: bool = Field(default=True, alias="OPENCHECK_ACCESS_LOG")
    # Override for the container memory limit (MB) used in the memwatch pct
    # calculation. Unset = read from cgroups (correct on Render/Docker).
    memory_limit_mb: float | None = Field(default=None, alias="OPENCHECK_MEMORY_LIMIT_MB")

    # --- Identifier checksum enforcement (see opencheck/identifiers.py) ---
    # Master switch for check-digit *enforcement*: LEI mod-97 fast-fail at the
    # API boundaries, checksum-aware LEI classification in the BODS mappers /
    # reconciler / source adapters, and national-ID check-digit warnings on
    # /resolve-national-id. Shape validation always applies regardless. The
    # test suite turns this off in conftest.py (long-standing fixtures use
    # deliberately fake, shape-valid LEIs); dedicated tests re-enable it —
    # the same arrangement as the rate limiter above.
    identifier_checksums_enforced: bool = Field(
        default=True, alias="OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED"
    )

    # --- Source credentials ---
    companies_house_api_key: str | None = Field(default=None, alias="COMPANIES_HOUSE_API_KEY")
    # Dedicated key for the Time Machine /history filing-history fetch, kept
    # separate from the lookup adapter's key. Falls back to the lookup key if
    # unset (see timeline/service.py).
    companies_house_history_api_key: str | None = Field(
        default=None, alias="COMPANIES_HOUSE_HISTORY_API_KEY"
    )
    # New Zealand NZBN API (Companies Office / MBIE) subscription key.
    nzbn_api_key: str | None = Field(default=None, alias="NZBN_API_KEY")
    # New Zealand Companies Entity Role Search API — separate subscription key,
    # used by the lazy /nz-associations enrichment (director/shareholder links
    # across the register).
    nzbn_role_search_api_key: str | None = Field(
        default=None, alias="NZBN_ROLE_SEARCH_API_KEY"
    )

    # --- Corporations Canada (ISED) ---
    # Public-plan API key from the ISED API Gateway.
    # Register at: https://api.ised-isde.canada.ca/corporations/api
    # Must be provided via .env (never committed to the repository).
    corporations_canada_api_key: str | None = Field(
        default=None, alias="CORPORATIONS_CANADA_API_KEY"
    )
    opensanctions_api_key: str | None = Field(default=None, alias="OPENSANCTIONS_API_KEY")
    openaleph_api_key: str | None = Field(default=None, alias="OPENALEPH_API_KEY")
    # Wikirate REST API (https://wikirate.org/use_the_API) — sent as the
    # X-API-Key header. Effectively required: anonymous server-side requests
    # are blocked by Wikirate's Cloudflare bot protection (verified
    # 2026-07-07), so the adapter skips silently when unset. 60 req/min.
    wikirate_api_key: str | None = Field(default=None, alias="WIKIRATE_API_KEY")

    # --- OpenFIGI (securities enrichment: ISIN → FIGI / security type) ---
    # Free key from https://www.openfigi.com/api — sent as the X-OPENFIGI-APIKEY
    # header. Optional: the /v3/mapping endpoint works without a key at a lower
    # rate limit (and smaller batch size). Used by the securities service to
    # type the handful of ISINs we actually display.
    openfigi_api_key: str | None = Field(default=None, alias="OPENFIGI_API_KEY")
    # Sanctioned-securities overlay: path to the compact LEI→ISIN index built
    # from the free OpenSanctions securities.csv export by
    # scripts/extract_securities.py. When unset, the securities panel runs on
    # GLEIF + OpenFIGI alone (no sanctioned banner). OpenSanctions has no live
    # securities-by-LEI API — that collection is a bulk-export wrapper.
    securities_index_file: str | None = Field(
        default=None, alias="OPENCHECK_SECURITIES_INDEX_FILE"
    )
    # Alternative to the local file: a URL (GitHub raw / release asset / S3) the
    # backend downloads at startup. Preferred on ephemeral-filesystem hosts
    # (Render) so the index can be refreshed without rebuilding the image. The
    # file wins if both are set.
    securities_index_url: str | None = Field(
        default=None, alias="OPENCHECK_SECURITIES_INDEX_URL"
    )
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

    # --- Singapore ACRA Business Registry ---
    # Pre-built SQLite index. Build with: python scripts/extract_acra.py
    # Source: https://data.gov.sg/datasets?query=acra&resultId=1
    # License: Singapore Open Data Licence 1.0 — no API key required.
    acra_singapore_db_file: str | None = Field(default=None, alias="ACRA_SINGAPORE_DB_FILE")

    # --- Cyprus DRCOR (data.gov.cy open data, CC BY 4.0) ---
    # No API key. Pre-built SQLite index. Build with: python scripts/extract_cyprus.py
    # Source (3 monthly CSVs): https://data.gov.cy/el/dataset/mitroo-eggegrammenon-etaireion-emporikon-eponymion-kai-synetairismon-stin-kypro
    cyprus_drcor_db_file: str | None = Field(default=None, alias="CYPRUS_DRCOR_DB_FILE")

    # --- Australian Business Register (ABN Lookup, CC BY 3.0 AU) ---
    # Free GUID from https://abr.business.gov.au/Documentation/WebServiceRegistration
    abn_guid: str | None = Field(default=None, alias="ABN_GUID")

    # --- India MCA Company Master Data (data.gov.in, GODL) ---
    # OGD Platform India API key: register at data.gov.in (JanParichay /
    # MeriPehchaan login), then My Account → Generate API key. Also unlocks
    # every other data.gov.in resource, not just MCA.
    data_gov_in_api_key: str | None = Field(default=None, alias="DATA_GOV_IN_API_KEY")

    # --- Belgian Crossroads Bank for Enterprises (BCE / KBO) ---
    # Pre-built SQLite index. Build with: python scripts/extract_bce.py
    # Source: https://kbopub.economie.fgov.be/kbo-open-data/
    # License: KBO reuse licence (attribution required, no API key needed).
    bce_belgium_db_file: str | None = Field(default=None, alias="BCE_BELGIUM_DB_FILE")

    # --- Estonian e-Business Register (ariregister) ---
    # No credentials required — the adapter uses the public printable-page
    # endpoint at ariregister.rik.ee without authentication.
    # ARIREGISTER_USERNAME / ARIREGISTER_PASSWORD are retained here only for
    # backward compatibility (existing .env files); they are no longer read
    # by the adapter.
    ariregister_username: str | None = Field(default=None, alias="ARIREGISTER_USERNAME")
    ariregister_password: str | None = Field(default=None, alias="ARIREGISTER_PASSWORD")

    # --- Danish Central Business Register (CVR) via Datafordeler GraphQL ---
    # Datafordeler CVR GraphQL API authentication.
    # Auth: ?apiKey=<key> query parameter — raw key, no encoding.
    # Set up at portal.datafordeler.dk:
    #   1. Create an IT-system.
    #   2. Generate an API key under the IT-system (valid 2 years, renewable).
    #   3. Set CVR_DENMARK_API_KEY to the generated API key.
    # GraphQL endpoint: https://graphql.datafordeler.dk/CVR/v2
    cvr_denmark_api_key: str | None = Field(default=None, alias="CVR_DENMARK_API_KEY")

    # --- Croatian Court Register (Sudski registar) ---
    # OAuth2 Client Credentials Grant against the public sudreg_javni v3 API.
    # Register at https://sudreg-data.gov.hr/ to obtain the credentials.
    # NOTE: both the Client ID and Client Secret end in literal dots ("..") —
    # the trailing dots are an integral part of each value and must be kept.
    # Token endpoint: https://sudreg-data.gov.hr/api/oauth/token
    sudreg_client_id: str | None = Field(default=None, alias="SUDREG_CLIENT_ID")
    sudreg_client_secret: str | None = Field(default=None, alias="SUDREG_CLIENT_SECRET")

    # --- Open Ownership BODS bulk data (GLEIF + UK PSC) ---
    # Pre-extracted Parquet files. Build with: python scripts/setup_bods_data.py
    # S3 source URLs are fetched once and extracted locally.
    # BODS_GLEIF_PARQUET_DIR   — directory containing GLEIF Parquet files
    # BODS_GLEIF_FTS_DB        — path to the FTS5 SQLite index for GLEIF
    # BODS_UK_PSC_PARQUET_DIR  — directory containing UK PSC Parquet files
    # BODS_UK_PSC_FTS_DB       — path to the FTS5 SQLite index for UK PSC
    bods_gleif_parquet_dir: str | None = Field(default=None, alias="BODS_GLEIF_PARQUET_DIR")
    bods_gleif_fts_db: str | None = Field(default=None, alias="BODS_GLEIF_FTS_DB")
    # S3 URL for a bundle zip produced by setup_bods_data.py --create-bundle.
    # On ephemeral-filesystem hosts (Render), the adapter downloads and
    # extracts this bundle at first connection if the local paths don't exist.
    bods_gleif_s3_url: str | None = Field(default=None, alias="BODS_GLEIF_S3_URL")
    # Option B: direct S3 URL for just the fts.db file (no zip, no parquet).
    # Preferred over BODS_GLEIF_S3_URL on Render — avoids the 2 GB /tmp limit
    # because only the FTS db (~500 MB) is downloaded, not the full bundle.
    # Upload with: aws s3 cp data/bods/gleif/fts.db s3://BUCKET/... --acl public-read
    bods_gleif_fts_s3_url: str | None = Field(default=None, alias="BODS_GLEIF_FTS_S3_URL")
    # Base HTTPS URL for the individual Parquet files on S3
    # (e.g. https://opencheck.s3.eu-north-1.amazonaws.com/bods/gleif/parquet).
    # DuckDB queries Parquet directly via HTTPFS — no local Parquet download.
    bods_gleif_parquet_s3_base: str | None = Field(default=None, alias="BODS_GLEIF_PARQUET_S3_BASE")
    bods_uk_psc_parquet_dir: str | None = Field(default=None, alias="BODS_UK_PSC_PARQUET_DIR")
    bods_uk_psc_fts_db: str | None = Field(default=None, alias="BODS_UK_PSC_FTS_DB")
    bods_uk_psc_s3_url: str | None = Field(default=None, alias="BODS_UK_PSC_S3_URL")
    # Option B direct fts.db download for UK PSC.
    bods_uk_psc_fts_s3_url: str | None = Field(default=None, alias="BODS_UK_PSC_FTS_S3_URL")
    # Base HTTPS URL for UK PSC Parquet files on S3.
    bods_uk_psc_parquet_s3_base: str | None = Field(default=None, alias="BODS_UK_PSC_PARQUET_S3_BASE")

    # --- Austrian Firmenbuch (commercial register) HVD API ---
    # Register for a key at: https://justizonline.gv.at/jop/web/iwg/register
    # WSDL: https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws/fbw.wsdl
    # License: CC BY 4.0 (High Value Dataset, EU Implementing Regulation 2023/138)
    firmenbuch_api_key: str | None = Field(default=None, alias="FIRMENBUCH_API_KEY")

    # --- Greek General Commercial Registry (ΓΕΜΗ) Open Data API ---
    # Request a key at: https://opendata.businessportal.gr/register/
    # Sent as an ``api_key`` request header. Rate limited to 8 requests per
    # minute; a higher limit can be requested from support@uhc.gr.
    # License: ODC-BY-1.0 (attribution only; commercial reuse permitted).
    gemi_api_key: str | None = Field(default=None, alias="GEMI_API_KEY")

    # --- SEC EDGAR fair-use contact ---
    # SEC EDGAR's automated-access policy requires a contact e-mail in the
    # User-Agent string so they can reach you if your tool misbehaves.
    # Without this, requests from cloud-hosting IPs are likely to be silently
    # blocked with a 403.  Set to any working e-mail address.
    # See: https://www.sec.gov/os/webmaster-faq#developers
    edgar_contact_email: str = Field(
        default="opencheck@example.com", alias="OPENCHECK_EDGAR_CONTACT_EMAIL"
    )

    # --- Optional LLM (narrative summaries) ---
    anthropic_api_key: str | None = Field(default=None, alias="ANTHROPIC_API_KEY")
    # Model used for narrative summaries. Overridable so we can A/B Sonnet vs
    # Opus without a code change.
    narrative_model: str = Field(default="claude-sonnet-4-6", alias="OPENCHECK_NARRATIVE_MODEL")
    # Feature flag for the /narrative endpoint. The feature also requires
    # ``anthropic_api_key``; this flag lets us disable it even when a key is set.
    narrative_enabled: bool = Field(default=True, alias="OPENCHECK_NARRATIVE_ENABLED")

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
