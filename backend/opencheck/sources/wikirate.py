"""Wikirate adapter — ESG category.

Wikirate (https://wikirate.org/) is an open, collaboratively-researched
database of corporate ESG metric answers (CC BY 4.0), covering thousands
of metrics from designers such as the World Benchmarking Alliance,
Net Zero Tracker, Fashion Revolution and the Business & Human Rights
Resource Centre. Wikirate is a GODIN member and publishes rich company
identifiers — LEI, Wikidata QID, OpenCorporates ID, UK company number,
SEC CIK, ABN/ACN, ISINs — making it a strong cross-source corroborator.

OpenCheck shows a high-level summary (total data points + a sample of
the most recent researched answers) and links out to wikirate.org for
the full record, per Wikirate's community model.

Hard-won API constraints (verified live 2026-07-07)
----------------------------------------------------
* **Cloudflare**: anonymous server-side requests get a bot-protection
  challenge (HTTP 403 "Just a moment"). The ``X-API-Key`` header
  bypasses it — the adapter therefore requires ``WIKIRATE_API_KEY``
  and skips silently without one.
* **Identifier filter**: the flat ``filter[company_identifier]=…``
  param 500s server-side ("Error rendering: Company (items view)")
  regardless of auth. The working shape is the nested
  ``filter[company_identifier[value]]=…`` that wikirate4py builds. It
  matches *any* identifier type — LEI and Wikidata QID both resolve.
* **Card paths**: company names with trailing dots ("BP plc.") break
  naive ``{slug}+Answer.json`` URLs. Always address cards by numeric id
  (``/~{card_id}+Answer.json``) — stable and encoding-proof.
* **Answers**: ``view=count`` returns a bare integer total (it ignores
  other filters). Per direct recommendation from the Wikirate team
  (2026-07-24), the answers list query sends
  ``filter[metric_type][]=researched&sort_by=year&sort_dir=desc`` —
  this restricts the list to human-researched answers (excluding
  derived/calculated ones) and orders them most-recent-year-first.
  This supersedes the earlier ``filter[year]=latest`` approach (which
  returned one answer per metric) and the earlier finding that sort
  params were ignored.
* Rate limit: 60 requests/minute; this adapter spends 3 per lookup.

Two shapes for one company (verified live 2026-10-10, Phase 321)
----------------------------------------------------------------
* ``/Companies.json`` list items carry each field **flat**:
  ``"headquarters": "United Kingdom"``, ``"legal_entity_identifier":
  "213800LH1BZH3DI6G760"``.
* The card itself, ``/~{card_id}.json`` — what ``fetch()`` reads, so every
  lookup's deepen pass and every per-source retry — wraps **every** field
  as a nested card: ``{"id", "name", "type": "Pointer", "url", "content":
  ["United Kingdom"]}``; the identifiers too (``"content":
  ["213800LH1BZH3DI6G760"]``). Until Phase 321 the adapter read the card
  as if it were a list item: ``headquarters`` / ``website`` failed the
  bundle schema (production, Eli Lilly and Rosneft, 10 Oct 2026), and the
  identifier dicts would have been stringified into asserted identifiers.
  ``_flatten_card`` reads both shapes; it is the only place either is read.

The legacy host (from 1 Nov 2026)
---------------------------------
On 1 Nov 2026 Wikirate 2.0 took over ``wikirate.org`` and the legacy
platform and its API moved to ``legacy.wikirate.org``, where they stay
until **31 Dec 2026** (Wikirate's email to API users, read 2 Nov 2026).
The legacy API answers there exactly as it did on ``wikirate.org``, with
the same ``X-API-Key``; this adapter calls it. The new ``wikirate.org``
answers every old API path — ``/Companies.json``, ``/~637.json``,
``view=count`` — with **HTTP 200 and its website's HTML**. Until
2 Nov 2026 the adapter caught the failed JSON parse and returned "no
record", so the Climate & ESG tab said Wikirate had nothing on BP, Eli
Lilly and Rosneft. ``_get_json`` now refuses a body that is not JSON
(``WikirateUnexpectedResponse``), and a failed resolution or card read
records a ``source_fetch`` degradation, so the lookup reports Wikirate as
not answering instead of as answering with no record.

The new platform keeps the legacy card ids (BP is 637 on both) and
``wikirate.org/company/{card_id}`` is the company's page there, carried as
``platform_url``. Its REST API launched with it and needs a new account
and a new token; moving onto it before the legacy API is retired is
stage 2 of the Notion ticket "Update Wikirate source for its platform
changes". The cache namespace changed with the host so no payload from
the old host is replayed.

The REST API (Wikirate 2.0, read 2 Nov 2026)
--------------------------------------------
``https://data.wikirate.org/rest/v1`` (documented at wikirate.org/api) is a
read-only PostgREST API. Every request carries three headers: ``apikey`` (the
publishable key, the same for every user), ``x-api-token`` (the personal
``wr_…`` token) and ``Accept-Profile: api`` — without the last every endpoint
answers 404 ``PGRST205``. A bad token is 401 ``PT401``; an empty match is
``200 []``; a total comes back in ``Content-Range`` (``0-0/2295``) when
``Prefer: count=exact`` is sent, and that answer is **HTTP 206**.

Measured against the docs on 2 Nov 2026, with BP (637), Eli Lilly (56477) and
Rosneft (47340):

* ``company_identifiers`` uses the legacy key names, not the docs' ``lei`` /
  ``isin``: ``legal_entity_identifier``, ``wikidata``, ``open_corporates_id``,
  ``sec_central_index_key``, ``international_security_identification_number``
  (sic), ``open_supply_id``. 22,037 companies carry an LEI, none as a list.
  ``_REST_IDENTIFIER_KEYS`` maps the two renamed keys onto the bundle's names,
  so the BODS mapper and the hit builder read both APIs alike.
* Company ids are the legacy card ids. Totals: BP 2,295 (legacy 2,290), Eli
  Lilly 2,223 (2,223), Rosneft 1,369 (1,317) — the new platform has answers
  the read-only legacy one does not.
* Every answer carries its own ``license``; all of the three companies' were
  CC BY 4.0. A sample answer under another licence is left out and counted
  (``answers_other_licence``), never shown under the source's CC BY line.
* Answers have no public page: the data-exploration page needs a login. The
  card links to the company's page, ``wikirate.org/company/{id}``.

``WIKIRATE_BACKEND`` picks the API (``auto``: REST when its credentials are
set, else legacy), so a rollback is a Render setting, not a deploy.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

import httpx

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.wikirate import WikirateBundle

log = logging.getLogger(__name__)

#: The legacy platform and its API, moved here on 1 Nov 2026; retired after
#: 31 Dec 2026 (``LEGACY_RETIRES``).
_API_BASE = "https://legacy.wikirate.org"
#: Wikirate 2.0 — same card ids; ``/company/{id}`` is the company's page.
_PLATFORM_BASE = "https://wikirate.org"
#: Changed with the host, so nothing cached from ``wikirate.org`` is replayed.
_CACHE_NS = "wikirate-legacy"
#: The last day Wikirate keeps the legacy API up.
LEGACY_RETIRES = "2026-12-31"

#: The REST API's cache namespace and path; the base URL is a setting.
_REST_CACHE_NS = "wikirate-rest"
_REST_PATH = "/rest/v1"
#: REST ``company_identifiers`` keys that differ from the bundle's names.
_REST_IDENTIFIER_KEYS = {
    "wikidata": "wikidata_id",
    "international_security_identification_number": (
        "international_securities_identification_number"
    ),
}
#: The answer licence the source is attributed under.
_SOURCE_LICENCE = "CC BY 4.0"
_API_WORDS = {"rest": "REST API", "legacy": "legacy API"}

#: Fixed sentences for the degradations (no company name can reach them).
_NOT_ANSWERED = "Wikirate's {api} did not answer ({failure})"
_CARD_NOT_READ = "Wikirate's company card could not be read ({failure})"
_ANSWERS_NOT_READ = "Wikirate's answers could not be read ({failure})"


class WikirateUnexpectedResponse(Exception):
    """Wikirate answered, but not with the JSON this endpoint returns.

    The new ``wikirate.org`` answers the legacy API paths with HTTP 200 and
    its website's HTML; a parse failure must never read as "no record".
    """


def _failure_label(exc: BaseException) -> str:
    """A short label for a failure — no URL, so no key or company can leak."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    if isinstance(exc, WikirateUnexpectedResponse):
        return str(exc)
    return type(exc).__name__


def _rest_credentials() -> tuple[str, str, str] | None:
    """``(base URL, publishable key, token)`` when the REST API is configured."""
    settings = get_settings()
    if not (settings.wikirate_publishable_key and settings.wikirate_api_token):
        return None
    base = (settings.wikirate_api_url or "https://data.wikirate.org").rstrip("/")
    return base, settings.wikirate_publishable_key, settings.wikirate_api_token


def _mode() -> str | None:
    """Which API to read — ``"rest"``, ``"legacy"`` or ``None`` (unconfigured).

    ``WIKIRATE_BACKEND`` = ``auto`` (the default) reads the REST API when its
    credentials are set and the legacy API otherwise; ``rest`` / ``legacy``
    force one, and an unconfigured forced choice reads nothing.
    """
    choice = (get_settings().wikirate_backend or "auto").strip().lower()
    rest = _rest_credentials() is not None
    legacy = bool(_api_key())
    if choice == "rest":
        return "rest" if rest else None
    if choice == "legacy":
        return "legacy" if legacy else None
    if rest:
        return "rest"
    return "legacy" if legacy else None


def _content_range_total(value: str | None) -> int | None:
    """``Content-Range: 0-0/2295`` → 2295; ``*/0`` → 0; ``*/*`` → None."""
    if not value or "/" not in value:
        return None
    total = value.rsplit("/", 1)[1].strip()
    return int(total) if total.isdigit() else None


def _rest_identifiers(raw: Any) -> dict[str, Any]:
    """REST ``company_identifiers`` → the bundle's identifier names."""
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for key, value in raw.items():
        name = _REST_IDENTIFIER_KEYS.get(key, key)
        if name in _IDENTIFIER_FIELDS and value not in (None, "", []):
            out[name] = value
    return out

#: How many latest-per-metric answers to include in the bundle.
_MAX_LATEST_ANSWERS = 12

#: Wikirate Company-card identifier fields worth carrying in the bundle.
#: Note: ``open_corporates_id`` is the OpenCorporates *company number*
#: (e.g. "00102498"), not OpenCheck's jurisdiction-scoped ``ocid``.
_IDENTIFIER_FIELDS = (
    "legal_entity_identifier",
    "wikidata_id",
    "open_corporates_id",
    "uk_company_number",
    "sec_central_index_key",
    "australian_business_number",
    "australian_company_number",
    "international_securities_identification_number",
    "open_supply_id",
)


def _api_key() -> str | None:
    """The Wikirate API key (module-level so tests can pin it)."""
    return get_settings().wikirate_api_key


def _first(value: Any) -> str | None:
    """Scalar-or-list identifier fields → first scalar as str."""
    if isinstance(value, list):
        value = value[0] if value else None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _card_content(value: Any) -> Any:
    """A card-JSON field → its value; a flat list-item field is unchanged.

    ``/~{id}.json`` nests each field as ``{"id", "name", "type", "url",
    "content": …}``. Only a dict carrying ``content`` is unwrapped — the
    card's own ``type`` object has none and is left alone.
    """
    if isinstance(value, dict) and "content" in value:
        return value["content"]
    return value


def _flatten_card(item: dict[str, Any]) -> dict[str, Any]:
    """Either company shape (list item or card JSON) → the flat list shape."""
    return {key: _card_content(value) for key, value in item.items()}


def _html_url(json_url: str | None) -> str | None:
    """Wikirate card JSON URL → its HTML page URL."""
    if not json_url:
        return None
    return json_url[:-5] if json_url.endswith(".json") else json_url


def _split_metric(metric: str | None) -> tuple[str | None, str | None]:
    """"Designer+Metric name" → (designer, metric name)."""
    if not metric:
        return None, None
    designer, sep, name = metric.partition("+")
    if not sep:
        return None, metric
    return designer or None, name or None


class WikirateAdapter(SourceAdapter):
    """Wikirate ESG metric-answers adapter — ESG category."""

    id = "wikirate"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Wikirate",
            homepage="https://wikirate.org/",
            description=(
                "Open, collaboratively-researched corporate ESG metric "
                "answers (environment, human rights, supply chains, "
                "governance) from designers such as the World Benchmarking "
                "Alliance and Net Zero Tracker. GODIN member; publishes "
                "LEI, Wikidata, OpenCorporates and other identifiers."
            ),
            license="CC-BY-4.0",
            attribution="Wikirate.org, licensed under CC BY 4.0",
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=settings.allow_live and _mode() is not None,
            category="esg",
        )

    # ------------------------------------------------------------------
    # Search — identifier-keyed source; free-text search intentionally empty
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # LEI-keyed lookup (called by the lookup pipeline)
    # ------------------------------------------------------------------

    async def fetch_by_lei(
        self, lei: str, qid: str | None = None, legal_name: str = ""
    ) -> dict[str, Any] | None:
        """Resolve LEI (falling back to Wikidata QID) to a Wikirate company.

        Returns ``None`` when the key is unset or the company is not on
        Wikirate. When Wikirate does not answer it also returns ``None``,
        after recording a ``source_fetch`` degradation, which the pipeline
        reports as the source not answering — never as "no record".
        """
        mode = _mode()
        if mode is None:
            return None
        if mode == "rest":
            return await self._rest_fetch_by_lei(lei, qid)
        item, matched_by = None, None
        try:
            if lei:
                item = await self._resolve_company(lei)
                matched_by = "lei"
            if item is None and qid:
                item = await self._resolve_company(qid)
                matched_by = "wikidata_qid"
        except Exception as exc:  # noqa: BLE001
            failure = _failure_label(exc)
            log.warning("Wikirate company resolution failed: %s", failure)
            degradation.record(
                self.id,
                _NOT_ANSWERED.format(api=_API_WORDS["legacy"], failure=failure),
                reason=degradation.reason_for_failure(failure),
            )
            return None
        if item is None:
            return None
        return await self._build_bundle(item, matched_by=matched_by or "lei")

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by Wikirate card id (deepen / retry path)."""
        mode = _mode()
        if mode is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        if mode == "rest":
            return await self._rest_fetch_card(hit_id)
        try:
            card = await self._get_json(f"/~{hit_id}.json")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                self._card_failed(exc)
            card = None
        except Exception as exc:  # noqa: BLE001
            self._card_failed(exc)
            card = None
        if not isinstance(card, dict) or not card.get("id"):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        bundle = await self._build_bundle(card, matched_by="card")
        return bundle or {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # REST API (Wikirate 2.0)
    # ------------------------------------------------------------------

    async def _rest_fetch_by_lei(
        self, lei: str, qid: str | None
    ) -> dict[str, Any] | None:
        company, matched_by = None, None
        try:
            if lei:
                company = await self._rest_company(
                    "company_identifiers->>legal_entity_identifier", lei
                )
                matched_by = "lei"
            if company is None and qid:
                company = await self._rest_company("company_identifiers->>wikidata", qid)
                matched_by = "wikidata_qid"
        except Exception as exc:  # noqa: BLE001
            failure = _failure_label(exc)
            log.warning("Wikirate REST company lookup failed: %s", failure)
            degradation.record(
                self.id,
                _NOT_ANSWERED.format(api=_API_WORDS["rest"], failure=failure),
                reason=degradation.reason_for_failure(failure),
            )
            return None
        if company is None:
            return None
        return await self._rest_bundle(company, matched_by=matched_by or "lei")

    async def _rest_fetch_card(self, hit_id: str) -> dict[str, Any]:
        stub = {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        if not str(hit_id).isdigit():
            return stub
        try:
            company = await self._rest_company("id", str(hit_id))
        except Exception as exc:  # noqa: BLE001
            self._card_failed(exc)
            return stub
        if company is None:
            return stub
        return await self._rest_bundle(company, matched_by="card") or stub

    async def _rest_company(self, column: str, value: str) -> dict[str, Any] | None:
        """One ``v1_companies`` filter → the lowest-id match, or None."""
        rows, _ = await self._rest_get(
            "v1_companies", {column: f"eq.{value}", "order": "id.asc", "limit": "5"}
        )
        return rows[0] if rows else None

    async def _rest_bundle(
        self, company: dict[str, Any], matched_by: str
    ) -> dict[str, Any] | None:
        card_id = company.get("id")
        name = _first(company.get("name")) or ""
        if not isinstance(card_id, int) or not name:
            return None
        total, (latest, other_licence) = await asyncio.gather(
            self._rest_answer_count(card_id),
            self._rest_latest_answers(card_id),
        )
        headquarters = company.get("headquarters")
        company_url = f"{_PLATFORM_BASE}/company/{card_id}"
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "api": "rest",
            "card_id": card_id,
            "name": name,
            # Answers have no public page and the data explorer needs a
            # login, so the company page is the record's link.
            "wikirate_url": company_url,
            "platform_url": company_url,
            "matched_by": matched_by,
            "identifiers": _rest_identifiers(company.get("company_identifiers")),
            "headquarters": (
                _first(headquarters.get("name")) if isinstance(headquarters, dict) else None
            ),
            "website": _first(company.get("website")),
            "total_answers": total,
            "latest_answers": latest,
            "answers_other_licence": other_licence,
            "is_stub": False,
        }
        validate_raw("wikirate", WikirateBundle, bundle)
        return bundle

    async def _rest_answer_count(self, card_id: int) -> int:
        try:
            _, total = await self._rest_get(
                "v1_answers",
                {"company_id": f"eq.{card_id}", "select": "id", "limit": "1"},
                count=True,
            )
            return total or 0
        except Exception as exc:  # noqa: BLE001
            log.warning("Wikirate REST answer count failed for %s: %s", card_id, exc)
            self._answers_failed(exc)
            return 0

    async def _rest_latest_answers(
        self, card_id: int
    ) -> tuple[list[dict[str, Any]], int]:
        try:
            rows, _ = await self._rest_get(
                "v1_answers",
                {
                    "company_id": f"eq.{card_id}",
                    "metric_type": "eq.researched",
                    "select": "id,metric_name,metric_designer,year,value,"
                    "value_numeric,unit,license",
                    "order": "year.desc,id.asc",
                    "limit": str(_MAX_LATEST_ANSWERS),
                },
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Wikirate REST answers failed for %s: %s", card_id, exc)
            self._answers_failed(exc)
            return [], 0
        answers: list[dict[str, Any]] = []
        other_licence = 0
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            licence = raw.get("license")
            abbreviation = licence.get("abbreviation") if isinstance(licence, dict) else None
            if abbreviation and abbreviation != _SOURCE_LICENCE:
                other_licence += 1
                continue
            numeric = raw.get("value_numeric")
            answers.append(
                {
                    "metric_designer": raw.get("metric_designer"),
                    "metric_name": raw.get("metric_name"),
                    "year": raw.get("year"),
                    "value": numeric if isinstance(numeric, (int, float)) else raw.get("value"),
                    "answer_url": None,
                }
            )
        return answers, other_licence

    async def _rest_get(
        self, endpoint: str, params: dict[str, str], *, count: bool = False
    ) -> tuple[list[Any], int | None]:
        """GET a REST endpoint → ``(rows, total)``, cached.

        Anything but a JSON list is ``WikirateUnexpectedResponse``; with
        ``count`` the total must be in ``Content-Range``. Nothing that failed
        is cached.
        """
        credentials = _rest_credentials()
        if credentials is None:
            raise WikirateUnexpectedResponse("REST API not configured")
        base, publishable_key, token = credentials
        cache_key = (
            f"{_REST_CACHE_NS}/{endpoint}/"
            + hashlib.sha1(
                json.dumps([sorted(params.items()), count]).encode()
            ).hexdigest()
        )
        cached = self._cache.get_payload(cache_key)
        if cached is not None and isinstance(cached[0], dict):
            return cached[0].get("rows") or [], cached[0].get("total")
        headers = {
            "apikey": publishable_key,
            "x-api-token": token,
            "Accept-Profile": "api",
            "Accept": "application/json",
        }
        if count:
            headers["Prefer"] = "count=exact"
        async with build_client() as client:
            response = await client.get(
                f"{base}{_REST_PATH}/{endpoint}", params=params, headers=headers
            )
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type.lower():
                raise WikirateUnexpectedResponse(
                    f"HTTP {response.status_code} {content_type.split(';')[0] or 'no content type'}"
                    " instead of JSON"
                )
            rows = response.json()
            if not isinstance(rows, list):
                raise WikirateUnexpectedResponse("a JSON object where rows were expected")
            total = None
            if count:
                total = _content_range_total(response.headers.get("content-range"))
                if total is None:
                    raise WikirateUnexpectedResponse("no total in Content-Range")
        self._cache.put(cache_key, {"rows": rows, "total": total})
        return rows, total

    def _card_failed(self, exc: BaseException) -> None:
        """A card read that failed (not a 404, which is an answer)."""
        failure = _failure_label(exc)
        log.warning("Wikirate card fetch failed: %s", failure)
        degradation.record(
            self.id,
            _CARD_NOT_READ.format(failure=failure),
            reason=degradation.reason_for_failure(failure),
        )

    def _answers_failed(self, exc: BaseException) -> None:
        """The company resolved but its answers could not be read in full."""
        failure = _failure_label(exc)
        degradation.record(
            self.id,
            _ANSWERS_NOT_READ.format(failure=failure),
            check=degradation.CHECK_SOURCE_READ,
            reason=degradation.reason_for_failure(failure),
        )

    async def _resolve_company(self, identifier: str) -> dict[str, Any] | None:
        """One ``filter[company_identifier[value]]`` query → first item."""
        payload = await self._get_json(
            "/Companies.json",
            params={"filter[company_identifier[value]]": identifier, "limit": 5},
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise WikirateUnexpectedResponse("a company list without items")
        items = payload["items"]
        return items[0] if items else None

    async def _build_bundle(
        self, item: dict[str, Any], matched_by: str
    ) -> dict[str, Any] | None:
        item = _flatten_card(item)
        card_id = item.get("id")
        name = _first(item.get("name")) or ""
        if not card_id or not name:
            return None

        total, latest = await asyncio.gather(
            self._fetch_answer_count(card_id),
            self._fetch_latest_answers(card_id),
        )

        identifiers = {
            field: item[field]
            for field in _IDENTIFIER_FIELDS
            if item.get(field) not in (None, "", [])
        }

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "card_id": card_id,
            "name": name,
            # ~id form is stable against renames and slug-encoding traps
            # (e.g. trailing dots in "BP plc.").
            "wikirate_url": f"{_API_BASE}/~{card_id}",
            "platform_url": f"{_PLATFORM_BASE}/company/{card_id}",
            "matched_by": matched_by,
            "identifiers": identifiers,
            # A Pointer card's content is a list ("Indiana (United States)"
            # inside one); a Phrase card's is a string.
            "headquarters": _first(item.get("headquarters")),
            "website": _first(item.get("website")),
            "total_answers": total,
            "latest_answers": latest,
            "is_stub": False,
        }
        validate_raw("wikirate", WikirateBundle, bundle)
        return bundle

    async def _fetch_answer_count(self, card_id: int) -> int:
        try:
            payload = await self._get_json(
                f"/~{card_id}+Answer.json", params={"view": "count"}
            )
            return int(payload)
        except Exception as exc:  # noqa: BLE001
            log.warning("Wikirate answer count failed for ~%s: %s", card_id, exc)
            self._answers_failed(exc)
            return 0

    async def _fetch_latest_answers(self, card_id: int) -> list[dict[str, Any]]:
        try:
            payload = await self._get_json(
                f"/~{card_id}+Answer.json",
                params={
                    "filter[metric_type][]": ["researched"],
                    "sort_by": "year",
                    "sort_dir": "desc",
                    "limit": _MAX_LATEST_ANSWERS,
                },
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Wikirate answers fetch failed for ~%s: %s", card_id, exc)
            self._answers_failed(exc)
            return []
        answers: list[dict[str, Any]] = []
        for raw in (payload or {}).get("items") or []:
            designer, metric_name = _split_metric(raw.get("metric"))
            answers.append(
                {
                    "metric_designer": designer,
                    "metric_name": metric_name,
                    "year": raw.get("year"),
                    "value": raw.get("value"),
                    "answer_url": _html_url(raw.get("answer_url") or raw.get("url")),
                }
            )
        return answers

    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET a Wikirate card JSON endpoint, cached, with the API key."""
        cache_key = f"{_CACHE_NS}{path}?{sorted((params or {}).items())!r}"
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        async with build_client() as client:
            response = await client.get(
                f"{_API_BASE}{path}",
                params=params,
                headers={"X-API-Key": _api_key() or "", "Accept": "application/json"},
            )
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type.lower():
                raise WikirateUnexpectedResponse(
                    f"HTTP {response.status_code} {content_type.split(';')[0] or 'no content type'}"
                    " instead of JSON"
                )
            payload = response.json()
        self._cache.put(cache_key, payload)
        return payload
