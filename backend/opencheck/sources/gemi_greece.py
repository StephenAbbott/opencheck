"""Greek General Commercial Registry (ΓΕΜΗ / GEMI) adapter.

Το Γενικό Εμπορικό Μητρώο is Greece's national register of commercial
publicity, maintained by the Κεντρική Υπηρεσία ΓΕΜΗ under the Κεντρική Ένωση
Επιμελητηρίων Ελλάδος. It replaced the courts' company books, the Μητρώο
Ανωνύμων Εταιρειών and the ΦΕΚ ΤΑΕ/ΕΠΕ gazette.

It publishes a developer-facing Open Data API — distinct from, and much more
open than, the ``gsis.gr`` GEMI web service, which is restricted to the public
sector::

    Base:  https://opendata-api.businessportal.gr/api/opendata/v1
    Spec:  https://opendata-api.businessportal.gr/api-docs  (Swagger 2.0, keyless)
    Docs:  https://opendata.businessportal.gr/techdocs/

Endpoints used:

* ``GET /companies/{arGemi}`` — one company's full published record.
* ``GET /companies?name=…`` — company search. **Returns complete company
  records, not summaries** (verified live 2026-08-28), so a name search needs
  no follow-up detail call.
* ``GET /companies/{arGemi}/documents`` — filed decisions and ΥΜΣ documents.
  Second-tier: only fetched when the budget allows (see below).

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000685"`` (ΓΕΜΗ) and
     ``registeredAs`` = a 12-digit zero-padded Αριθμός ΓΕΜΗ for Greek
     entities.
  2. ``routers/lookup.py`` derives ``derived["gr_argemi"]`` and calls
     ``fetch()`` here.

Rate limiting — the defining constraint
---------------------------------------
GEMI publish a hard budget of **8 requests per minute**. A FullCheck that
traverses several Greek related parties would trip that on the third or fourth
company, so this adapter is paced by a process-wide token bucket
(:mod:`opencheck.outbound_rate`) at 7/min, and each lookup may spend at most
``_LOOKUP_CALL_BUDGET`` calls on GEMI. Over budget, or on HTTP 429, the
adapter records a degradation rather than stalling or throwing — the lookup
returns what it has and says GEMI was rate-limited. An increase was requested
from support@uhc.gr on 2026-08-28.

Codelists are not optional
--------------------------
Objects embedded in a company record are truncated to ``{id, descr}``: the
``descrEn`` and ``isActive`` fields the Swagger definitions advertise are
returned **only** by ``/metadata/*``. English labels and the active/inactive
determination therefore come from the committed snapshot in
``data/gemi_metadata.json`` (refreshed by ``scripts/refresh_gemi_metadata.py``).
Note the id type mismatch: the codelists key on **strings**, company records
embed **integers**.

What the register does and does not publish
-------------------------------------------
``persons[]`` carries both officers and owners, discriminated by ``category``:

* ``Διοικητικό συμβούλιο`` (board of directors) — officers of an ΑΕ.
  ``percentage`` is always ``"-"``.
* ``Εταίροι`` (partners/members) — the actual holders of an ΙΚΕ, ΕΕ, ΟΕ or
  ΕΠΕ, **with percentages** (``"70%"``).

An **ΑΕ (société anonyme) publishes no shareholders at all** — that is the
Greek regime, not missing data: an ΑΕ's share register is not part of ΓΕΜΗ
publicity. Callers must not present that as data withheld.

ΓΕΜΗ is a *commercial* register, not a beneficial ownership regime — Greece's
Κεντρικό Μητρώο Πραγματικών Δικαιούχων is a separate, non-public register — so
this source is deliberately **not** in the mapper's ``_BO_ASSERTING_SOURCES``
and its interests carry no ``beneficialOwnershipOrControl`` flag.

Authentication: ``api_key`` request header. Free, issued on approval of the
  form at https://opendata.businessportal.gr/register/. Set ``GEMI_API_KEY``.
GLEIF RA code: RA000685 (General Commercial Registry (G.E.MI.))
License: ODC-BY-1.0 — attribution only, commercial reuse permitted.
  http://www.opendefinition.org/licenses/odc-by
Attribution: "Contains data from the Greek General Commercial Registry
  (ΓΕΜΗ), published by the Κεντρική Υπηρεσία ΓΕΜΗ / ΚΕΕΕ, ODC-BY-1.0."
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client, sanitize_name_query
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.gemi_greece import GemiGreeceBundle

_LOG = logging.getLogger(__name__)

_API_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"
_CACHE_NS = "gemi_greece"

#: GLEIF Registration Authority code for the General Commercial Registry
#: (G.E.MI.). Verified live 2026-08-28: 20 of 25 sampled Greek LEI records
#: register under it (the rest under RA000376 Athens Chamber of Commerce,
#: RA001118 pleasure-vessel shipping companies, and RA999999).
GR_RA_CODE: str = "RA000685"

#: Published budget is 8 requests/minute. Pace at 7 to leave headroom for a
#: retry and for clock skew against the server's own window.
_RATE_PER_MINUTE = 7.0

#: Most GEMI calls a single lookup may spend. A company costs 1 (detail) or 2
#: (detail + documents), so this allows a subject plus a couple of Greek
#: related parties before the lookup starts degrading rather than stalling.
_LOOKUP_CALL_BUDGET = 4

#: Search results are capped well under the API's own maximum of 200 — a large
#: page is no cheaper in requests but is much more to parse and rank.
_SEARCH_RESULT_SIZE = 10

_BUCKET = TokenBucket(_RATE_PER_MINUTE, capacity=1, name="gemi_greece")

_METADATA_PATH = Path(__file__).resolve().parents[1] / "data" / "gemi_metadata.json"

#: ``persons[].category`` values. The category — not the free-text ``role`` —
#: decides whether a person is an owner or an officer.
CATEGORY_PARTNERS = "Εταίροι"
CATEGORY_BOARD = "Διοικητικό συμβούλιο"

#: A ΓΕΜΗ number is up to 12 digits, and GLEIF stores it zero-padded to 12.
#: The API accepts either form — ``/companies/003031801000`` and
#: ``/companies/3031801000`` return byte-identical responses (verified live
#: 2026-08-28) — so nothing needs stripping.
_ARGEMI_RE = re.compile(r"^\d{1,12}$")

#: ``percentage`` arrives as a string with a percent sign (``"70%"``), or the
#: literal ``"-"`` when the role carries no holding (every board member).
_PERCENT_RE = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*%?\s*$")


def normalise_argemi(argemi: str) -> str:
    """Canonicalise an Αριθμός ΓΕΜΗ, keeping GLEIF's zero padding.

    GLEIF's ``registeredAs`` is zero-padded to 12 digits (``003031801000``)
    and that padded form is the citable identifier, so it is preserved rather
    than normalised away. The API tolerates either form, so no strip is needed
    for the request path either.

    Raises ``ValueError`` for anything that is not 1–12 digits, which the
    lookup pipeline treats as "skip this adapter for this subject".
    """
    cleaned = re.sub(r"[\s./-]", "", str(argemi).strip())
    if not _ARGEMI_RE.match(cleaned):
        raise ValueError(f"not a ΓΕΜΗ number: {argemi!r}")
    return cleaned


def parse_percentage(value: Any) -> float | None:
    """``"70%"`` → ``70.0``; ``"-"``, ``""`` and ``None`` → ``None``.

    Returns None rather than 0.0 for absent values — a board member with no
    holding must not be published as owning zero percent, which is a claim
    the register never made.
    """
    if value is None:
        return None
    match = _PERCENT_RE.match(str(value))
    if not match:
        return None
    try:
        share = float(match.group(1).replace(",", "."))
    except ValueError:
        return None
    return share if 0.0 < share <= 100.0 else None


@lru_cache(maxsize=1)
def metadata_tables() -> dict[str, dict[str, dict[str, Any]]]:
    """The committed ΓΕΜΗ codelist snapshot, keyed by table then string id."""
    try:
        blob = json.loads(_METADATA_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        _LOG.warning("ΓΕΜΗ codelist snapshot missing or unreadable at %s", _METADATA_PATH)
        return {}
    tables = blob.get("tables")
    return tables if isinstance(tables, dict) else {}


def codelist_entry(table: str, code_id: Any) -> dict[str, Any]:
    """Look one id up in a codelist table, or ``{}``.

    The snapshot keys on **strings** while company records embed **integers**,
    so the key is stringified here. Getting that wrong is a silent total miss:
    every English label would quietly fall back to the Greek one.
    """
    if code_id is None:
        return {}
    return metadata_tables().get(table, {}).get(str(code_id)) or {}


def status_is_active(status: dict[str, Any] | None) -> bool | None:
    """Whether a company's status is an operating one, per the codelist.

    Returns None when the status is unknown to the snapshot — the caller must
    not read that as "inactive". Exactly one of the twelve ΓΕΜΗ statuses is
    active (id 3, ``Ενεργή``); the other eleven are varieties of dissolved,
    deleted, merged, suspended or under forced management.
    """
    if not status:
        return None
    entry = codelist_entry("companyStatuses", status.get("id"))
    value = entry.get("isActive")
    return bool(value) if isinstance(value, bool) else None


def english_label(table: str, obj: dict[str, Any] | None) -> str:
    """The English label for an embedded codelist object, falling back to Greek."""
    if not obj:
        return ""
    entry = codelist_entry(table, obj.get("id"))
    label = entry.get("descrEn") or entry.get("descr") or obj.get("descr") or ""
    return str(label).strip()


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def company_url(argemi: str) -> str:
    """A stable, dereferenceable URL for the company's ΓΕΜΗ record."""
    return f"{_API_BASE}/companies/{argemi}"


class GemiGreeceAdapter(SourceAdapter):
    """Source adapter for the Greek General Commercial Registry Open Data API."""

    id = "gemi_greece"

    lookup_derivers = (
        LookupDeriver(frozenset({GR_RA_CODE}), "gr_argemi", normalise_argemi),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="ΓΕΜΗ — Greek General Commercial Registry",
            homepage="https://www.businessregistry.gr/",
            description=(
                "Greek company data from the General Commercial Registry "
                "(ΓΕΜΗ) Open Data API (ODC-BY-1.0). Provides entity details — "
                "name, ΑΦΜ, legal form, status, registered office and "
                "incorporation date — together with board members and, for "
                "private companies and partnerships, partners with their "
                "percentage holdings."
            ),
            license="ODC-BY-1.0",
            attribution=(
                "Contains data from the Greek General Commercial Registry "
                "(ΓΕΜΗ), published by the Κεντρική Υπηρεσία ΓΕΜΗ / Κεντρική "
                "Ένωση Επιμελητηρίων Ελλάδος under ODC-BY-1.0 via "
                "opendata.businessportal.gr."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=settings.allow_live and bool(settings.gemi_api_key),
            is_national_register=True,
            country="GR",
        )

    # ------------------------------------------------------------------
    # HTTP — every outbound call funnels through here
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any | None:
        """One rate-limited, budget-checked GET. None on any non-success.

        Returning None rather than raising is deliberate: a rate-limited or
        unavailable GEMI should cost the lookup one source, not the whole
        request. Every None path records a degradation so the reason reaches
        the user instead of looking like an empty register.
        """
        settings = get_settings()
        key = settings.gemi_api_key
        if not key:
            return None

        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"ΓΕΜΗ per-lookup call budget of {_LOOKUP_CALL_BUDGET} requests "
                    "reached; remaining Greek records were not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None

        await _BUCKET.acquire()

        url = f"{_API_BASE}{path}"
        async with build_client() as client:
            response = await client.get(url, params=params, headers={"api_key": key})

        if response.status_code == 429:
            degradation.record(
                self.id,
                "ΓΕΜΗ returned HTTP 429 (published limit is 8 requests/minute)",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if response.status_code == 404:
            return None
        if not response.is_success:
            _LOG.warning("ΓΕΜΗ returned %s for %s", response.status_code, path)
            degradation.record(
                self.id,
                f"ΓΕΜΗ returned HTTP {response.status_code}",
            )
            return None
        try:
            return response.json()
        except ValueError:
            degradation.record(self.id, "ΓΕΜΗ returned a non-JSON body")
            return None

    # ------------------------------------------------------------------
    # Search — one call; results are complete company records
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Search by name or distinctive title.

        ``GET /companies`` returns full ``Company`` objects, so each result is
        already everything ``fetch()`` would return and no follow-up call is
        made — worth half the request budget on this path.
        """
        if kind != SearchKind.ENTITY:
            return []
        cleaned = sanitize_name_query(query)
        if not cleaned:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(cleaned)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            payload = cached[0]
        else:
            payload = await self._get(
                "/companies",
                params={"name": cleaned, "resultsSize": str(_SEARCH_RESULT_SIZE)},
            )
            if payload is None:
                return []
            self._cache.put(cache_key, payload)

        results = (payload or {}).get("searchResults") or []
        hits: list[SourceHit] = []
        for company in results:
            if not isinstance(company, dict):
                continue
            argemi = str(company.get("arGemi") or "").strip()
            if not argemi:
                continue
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=argemi,
                    kind=SearchKind.ENTITY,
                    name=str(company.get("coNameEl") or "").strip(),
                    summary=self._summary(company),
                    identifiers=self._identifiers(company),
                    raw={"company": company},
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch — one company by Αριθμός ΓΕΜΗ, plus documents if affordable
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        try:
            argemi = normalise_argemi(hit_id)
        except ValueError:
            return self._bundle(None, None, hit_id, legal_name, is_stub=True)

        cache_key = f"{_CACHE_NS}/company/{argemi}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._bundle(None, None, argemi, legal_name, is_stub=True)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            stored = cached[0] or {}
            return self._bundle(
                stored.get("company"), stored.get("documents"), argemi, legal_name
            )

        company = await self._get(f"/companies/{argemi}")
        if not isinstance(company, dict):
            return self._bundle(None, None, argemi, legal_name)

        # Documents are second-tier: worth having, never worth losing the
        # company record for. Skipped silently when the budget is spent — the
        # budget path has already recorded its own degradation.
        documents = None
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is None or not budget.exhausted:
            fetched = await self._get(f"/companies/{argemi}/documents")
            if isinstance(fetched, dict):
                documents = fetched

        self._cache.put(cache_key, {"company": company, "documents": documents})
        return self._bundle(company, documents, argemi, legal_name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        company: dict[str, Any] | None,
        documents: dict[str, Any] | None,
        argemi: str,
        legal_name: str,
        *,
        is_stub: bool = False,
    ) -> dict[str, Any]:
        bundle = {
            "source_id": self.id,
            "gr_argemi": argemi,
            "company": company,
            "documents": documents,
            "legal_name": legal_name,
            "is_stub": is_stub or company is None,
        }
        if company is not None:
            validate_raw(self.id, GemiGreeceBundle, bundle)
        return bundle

    @staticmethod
    def _identifiers(company: dict[str, Any]) -> dict[str, str]:
        """Only what ΓΕΜΗ itself publishes — never the LEI it was found by."""
        identifiers: dict[str, str] = {}
        argemi = str(company.get("arGemi") or "").strip()
        if argemi:
            identifiers["gr_argemi"] = argemi
        afm = str(company.get("afm") or "").strip()
        if afm:
            identifiers["gr_afm"] = afm
        return identifiers

    @staticmethod
    def _summary(company: dict[str, Any]) -> str:
        parts = ["GR"]
        legal_type = english_label("legalTypes", company.get("legalType"))
        if legal_type:
            parts.append(legal_type)
        status = company.get("status") or {}
        status_label = english_label("companyStatuses", status)
        if status_label:
            parts.append(status_label.lower())
        return " · ".join(parts)
