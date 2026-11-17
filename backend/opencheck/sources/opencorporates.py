"""OpenCorporates adapter.

OpenCorporates is the world's largest open legal entity database,
providing a single unified set of company records from government
registries (https://opencorporates.com/).

The entry point is the ``ocid`` field that GLEIF returns on Level 1
records (``attributes.ocid``), formatted as
``{jurisdiction_code}/{company_number}`` (e.g. ``gb/00102498``).
This maps directly to the OpenCorporates API path:

    GET /v0.4/companies/{jurisdiction}/{company_number}

That one call carries the officers too, inline at
``results.company.officers``. There is no working officers child
endpoint: ``/companies/{jurisdiction}/{number}/officers`` 404s (see
``fetch``), which cost this adapter its entire officer list until
Phase 195.

Authentication: ``api_token`` query parameter. Gated on
``allow_live=true`` + key. All responses are cached.

License: OC data is openly licensed per-jurisdiction; the API terms
of service permit re-use with attribution. Corporate records
themselves are public-domain government data. Officers data
carries a similar regime.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any
from urllib.parse import quote

import httpx

from .. import degradation, names
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .lineage import NATIONAL_REGISTERS
from .oc_relationships import get_bulk_lookup_for_settings
from .schemas import validate_raw
from .schemas.opencorporates import OCBundle

_log = logging.getLogger(__name__)

# Degradation detail — a fixed sentence, never an entity name.
_NETWORK_FAILED_DETAIL = (
    "the OpenCorporates relationships (network) endpoint did not answer, so "
    "corporate relationships from it were not consulted"
)

_API_BASE = "https://api.opencorporates.com/v0.4"
_CACHE_NS = "opencorporates"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


# A company number written in scientific notation — ``1.0166E+12`` — is a
# spreadsheet's rendering of a long number, not a number OpenCorporates holds.
# GLEIF's LEI-to-OpenCorporates mapping carries some (``529900IQDQ6X3VTMWN63``,
# "Kaufland" S.R.L., Moldova: ``md/1.0166E+12`` for IDNO 1016600004811, read
# 20 Oct 2026), and asking for one is a guaranteed 404.
_SCIENTIFIC_NUMBER = re.compile(r"^\d(?:\.\d+)?E\+?\d+$", re.IGNORECASE)

#: Coverage note for a company OpenCorporates answered 404 for.
COVERAGE_404 = (
    "OpenCorporates holds no company under this identifier."
)


def repair_ocid(ocid: str | None, registered_as: str | None) -> str | None:
    """Return a usable ocid, repairing a scientific-notation company number.

    An ocid whose number is not in scientific notation is returned as it is.
    One that is gets the subject's own GLEIF ``registeredAs`` in its place —
    but only when that is all digits and rounds to exactly the mangled value,
    so the repair restores the number the spreadsheet cut and never guesses
    one. Anything else returns ``None``: an ocid that cannot name a company
    is not asked for.
    """
    if not ocid:
        return None
    parts = ocid.strip().strip("/").split("/", 1)
    if len(parts) != 2:
        return ocid
    jurisdiction, number = parts
    if not _SCIENTIFIC_NUMBER.match(number):
        return ocid
    candidate = re.sub(r"\s", "", registered_as or "")
    if not candidate.isdigit():
        return None
    mantissa = number.upper().split("E", 1)[0]
    digits = len(mantissa.replace(".", ""))
    if float(f"{int(candidate):.{digits - 1}E}") != float(number):
        return None
    return f"{jurisdiction}/{candidate}"


def _ocid_to_path(ocid: str) -> str:
    """Convert ``gb/00102498`` → ``/gb/00102498`` (URL-safe segments)."""
    parts = ocid.strip("/").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid ocid {ocid!r} — expected jurisdiction/number")
    jurisdiction, number = parts
    return f"/{quote(jurisdiction.lower())}/{quote(number)}"


#: Coverage note for a company number that names a different company on
#: OpenCorporates than the one the lookup is about.
COVERAGE_NAME_MISMATCH = (
    "OpenCorporates holds this company number for a differently named "
    "company, so its record is not shown; the number GLEIF links to this "
    "LEI may be wrong."
)


def _oc_names(company: dict[str, Any]) -> list[str]:
    """Every name OpenCorporates gives the company: current, previous and
    alternative (trading, translated)."""
    out = [str(company.get("name") or "").strip()]
    for key in ("previous_names", "alternative_names"):
        for item in company.get(key) or []:
            if isinstance(item, dict):
                out.append(str(item.get("company_name") or "").strip())
            elif isinstance(item, str):
                out.append(item.strip())
    return [n for n in out if n]


def _comparable(text: str) -> bool:
    """Can this name be compared letter by letter with a Latin one?

    ``fold_ascii`` transliterates Cyrillic and Greek; a CJK name stays as it
    is, and a GLEIF name in kanji cannot contradict an OpenCorporates name in
    romaji (or the reverse). Such a pair is never refused."""
    if names.has_dense_script(text):
        return False
    return bool(re.search(r"[a-z]", names.normalise_name(text)))


def _names_match(a: str, b: str) -> bool:
    ca, cb = names.org_comparable_name(a), names.org_comparable_name(b)
    if ca and ca == cb:
        return True
    if ca and names.despace(ca) == names.despace(cb):
        return True
    if names.distinctive_token_agreement(a, b):
        return True
    return names.name_similarity(a, b) >= names.NAME_MATCH_THRESHOLD


def names_agree(legal_name: str | None, company: dict[str, Any] | None) -> bool:
    """Does OpenCorporates' record carry the name GLEIF gave the subject?

    The lookup reaches OpenCorporates through GLEIF's LEI-to-OpenCorporates
    mapping, and a wrong company number in it names another company: GLEIF
    links American Foreign Policy Council (``549300W96W2VKSMVDF81``) to
    ``us_dc/L21249``, which is CONNIE-19 STREET LLC. True when the current,
    a previous or an alternative name agrees with ``legal_name``. A missing
    name on either side, or a pair in scripts that cannot be compared,
    cannot contradict the number and agrees.
    """
    wanted = (legal_name or "").strip()
    candidates = _oc_names(company or {})
    if not wanted or not candidates or not _comparable(wanted):
        return True
    comparable = [c for c in candidates if _comparable(c)]
    if not comparable:
        return True
    return any(_names_match(wanted, c) for c in comparable)


def publishes_lei(company: dict[str, Any] | None, lei: str | None) -> bool:
    """Does OpenCorporates' own record carry this LEI?

    Only then may the hit assert it (the identifier corroboration rule): the
    lookup arrives with the LEI, and a hit that repeats it reads as
    OpenCorporates confirming it."""
    want = (lei or "").strip().upper()
    if not want:
        return False
    for item in (company or {}).get("identifiers") or []:
        ident = item.get("identifier") if isinstance(item, dict) else None
        ident = ident if isinstance(ident, dict) else item
        if not isinstance(ident, dict):
            continue
        if str(ident.get("uid") or "").strip().upper() == want:
            return True
    return False


class OpenCorporatesAdapter(SourceAdapter):
    id = "opencorporates"

    #: The lookup hands ``fetch`` GLEIF's legal name, so a company number
    #: that names another company is refused here — in the dispatch and in
    #: every re-fetch of the hit (``routers.lookup._legal_name_kwargs``).
    lookup_pass_legal_name = True

    # OpenCorporates' terms permit OpenCheck to use the data and publish derived
    # (BODS) output, but not to redistribute the raw source records in bulk. The
    # raw payload is therefore redacted from all API responses and exports; the
    # mapped BODS statements remain available.
    republish_raw = False

    # OpenCorporates is a mirror of the official registers — its Companies
    # House officers agree with Companies House because they *are* Companies
    # House. Declared so corroboration counts read it as one observation
    # with whichever national register it copies (see sources/lineage.py).
    derived_from = frozenset({NATIONAL_REGISTERS})

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenCorporates",
            homepage="https://opencorporates.com/",
            description=(
                "The world's largest open legal-entity database, providing "
                "a single unified set of company records from government "
                "registries."
            ),
            license="OC-Terms",
            attribution=(
                "Contains company data from OpenCorporates "
                "(https://opencorporates.com/). Licensed per-jurisdiction "
                "under the source government registry license."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(
                settings.opencorporates_api_key and settings.allow_live
            ),
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """OC is entered via ocid (from GLEIF), not free-text search.

        This method is provided for protocol compliance. It returns an
        empty list because the canonical entry point is ``fetch(ocid)``
        via the GLEIF-derived ``ocid`` identifier.
        """
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(
        self, hit_id: str, legal_name: str | None = None
    ) -> dict[str, Any]:
        """Fetch company data + officers for a given ocid.

        ``hit_id`` is an OpenCorporates identifier in the format
        ``jurisdiction/company_number`` (e.g. ``gb/00102498``),
        exactly as returned by the GLEIF ``attributes.ocid`` field.

        With ``legal_name`` (a lookup passes GLEIF's), a record whose names
        do not agree with it is refused: the bundle carries no company,
        officers or network, only ``name_mismatch`` and the name
        OpenCorporates holds, for a note card.
        """
        ocid = hit_id.strip()
        path = _ocid_to_path(ocid)
        cache_key = f"{_CACHE_NS}/company/{_slug(ocid)}"

        if (
            not self.info.live_available
            and self._cache.get_payload(cache_key) is None
        ):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        company_data = await self._get(f"/companies{path}", cache_key=cache_key)
        if company_data is None:
            # A 404: OpenCorporates answered, and holds no company under this
            # ocid. That is a "no record", not a source that did not answer —
            # until this was split out, the 404 escaped as an HTTPStatusError
            # and the report said OpenCorporates "did not answer, so its
            # records were not consulted". No officers or network call: there
            # is no company for them to belong to.
            return {
                "source_id": self.id,
                "hit_id": ocid,
                "ocid": ocid,
                "is_stub": False,
                "not_found": True,
                "coverage_note": COVERAGE_404,
            }
        company = (company_data.get("results") or {}).get("company") or {}
        if not names_agree(legal_name, company):
            return {
                "source_id": self.id,
                "hit_id": ocid,
                "ocid": ocid,
                "is_stub": False,
                "not_found": True,
                "name_mismatch": True,
                "registered_name": str(company.get("name") or "").strip() or None,
                "coverage_note": COVERAGE_NAME_MISMATCH,
            }

        # Officers come from the company response, which inlines them under
        # ``results.company.officers`` — nested in an ``officer`` object, the
        # same shape the mapper already reads.
        #
        # Until Phase 195 they were fetched from ``/companies{path}/officers``
        # instead. **That endpoint 404s** (checked live on 2026-09-09 against
        # gb/00002065 and gb/00102498 with a working key), and it was fetched
        # through ``_get_optional``, which treats 404 as "this source has
        # nothing to say" — correct for an optional record, and exactly why
        # nobody saw it. So this list was empty for every company OpenCheck
        # ever looked up, from the adapter's first day: 27 cached officer
        # payloads on disk, every one of them ``null``. For a UK company
        # Companies House hid the loss by supplying the same people; for a
        # jurisdiction where OpenCorporates is the only officer source, the
        # report showed a company with no board and read as a finding.
        #
        # The data was in the response beside it the whole time — 116 officers
        # for Lloyds Bank PLC, 100 of them ended.
        officers = company.get("officers") or []

        # Network relationships — two possible sources, in priority order:
        #
        # 1. Bulk CSV file (OPENCORPORATES_RELATIONSHIPS_FILE env var).
        #    When configured, this takes precedence over the live API call.
        #    NOT activated unless the env var is set.
        #
        # 2. Live /network API endpoint (OC Relationships Supplement, premium
        #    tier). Used when the bulk file is not configured.
        #    Returns None gracefully on 402/403/404.
        network_data: dict | None = None

        bulk_lookup = get_bulk_lookup_for_settings()
        if bulk_lookup is not None:
            # Bulk file is configured — look up by jurisdiction + company number.
            jc, num = (ocid.split("/", 1) + [""])[:2]
            network_data = bulk_lookup.get_network(jc, num)
        else:
            # Fall back to live API endpoint.
            network_cache_key = f"{_CACHE_NS}/network/{_slug(ocid)}"
            raw_network = await self._get_optional(
                f"/companies{path}/network",
                cache_key=network_cache_key,
            )
            network_data = (raw_network.get("results") or {}) if raw_network else None

        bundle = {
            "source_id": self.id,
            "hit_id": ocid,
            "ocid": ocid,
            "company": company,
            "officers": officers,
            # Network payload — from bulk file or live API; None when neither
            # is available.
            "network": network_data if network_data else None,
            "raw_company": company_data,
        }
        validate_raw("opencorporates", OCBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, cache_key: str) -> dict[str, Any] | None:
        """GET a required record; ``None`` when OpenCorporates has none.

        A 404 is a definitive answer and is cached as absent (it expires
        after ``ABSENT_TTL_DAYS``, so a company added since is picked up).
        A 402/403 (a tier the key lacks), a 429, a 5xx and a network error
        still raise: each means OpenCheck could not ask, and the pipeline
        reports it as a degradation.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        settings = get_settings()
        assert settings.opencorporates_api_key, "live_available should have been false"

        params = {"api_token": settings.opencorporates_api_key}
        async with build_client() as client:
            response = await client.get(f"{_API_BASE}{path}", params=params)
            if response.status_code == 404:
                self._cache.put_absent(cache_key)
                return None
            response.raise_for_status()
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    async def _get_optional(
        self, path: str, *, cache_key: str
    ) -> dict[str, Any] | None:
        """Like ``_get`` but returns ``None`` when there is no record.

        A 404 (or a 402/403 for a tier the key lacks) is cached as absent with
        the cache's absence TTL. Any other failure returns ``None`` too — the
        record is optional — but is recorded as a degradation and NOT cached,
        so the next lookup asks again.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]
        if not self.info.live_available:
            return None

        settings = get_settings()
        assert settings.opencorporates_api_key, "live_available should have been false"

        params = {"api_token": settings.opencorporates_api_key}
        try:
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}{path}", params=params
                )
                if response.status_code in (402, 403, 404):
                    # 402/403: endpoint requires a premium API tier.
                    # 404:     no data for this company.
                    # Both are definitive answers — remembered for
                    # ABSENT_TTL_DAYS, not for ever.
                    self._cache.put_absent(cache_key)
                    return None
                response.raise_for_status()
                payload = response.json()
        except Exception as exc:  # noqa: BLE001 — optional data never fails the lookup
            # Phase 262: a 5xx, a 429, a network error or an unreadable body
            # says nothing about whether the record exists. Until now every
            # exception was cached as None with no expiry, so one outage read
            # as "no relationships" until the cache directory was wiped.
            label = (
                f"HTTP {exc.response.status_code}"
                if isinstance(exc, httpx.HTTPStatusError)
                else type(exc).__name__
            )
            _log.warning("opencorporates: optional fetch failed (%s): %s", path, label)
            degradation.record(
                self.id,
                _NETWORK_FAILED_DETAIL,
                reason=degradation.reason_for_failure(label),
            )
            return None

        self._cache.put(cache_key, payload)
        return payload
