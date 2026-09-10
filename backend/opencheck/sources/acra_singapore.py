"""Singapore ACRA adapter — live, keyed on the Unique Entity Number (UEN).

The Accounting and Corporate Regulatory Authority (ACRA) publishes its entity
register on data.gov.sg as two monthly collections, both queryable row by row
through the CKAN-style ``datastore_search`` API
(``GET https://data.gov.sg/api/action/datastore_search``):

* **Collection 1 — "UEN"**: eight columns (``uen``, issuing agency,
  ``uen_status_desc``, ``entity_name``, ``entity_type_desc``,
  ``uen_issue_date``, street name, postal code) in two datasets —
  *Entities Registered with ACRA* (~2.1M rows) and *Entities Registered with
  Other UEN Issuance Agencies* (~23k rows). Status is only ``Registered`` /
  ``Deregistered``. **Variable capital companies (VCCs) are in the second
  dataset**, not the first, even though ACRA issues their UENs.
* **Collection 2 — "ACRA Information on Corporate Entities"**: 55 columns in
  27 datasets **split by the first character of the entity's current name**
  (A–Z, and ``Others`` for digits and symbols). Adds company type, the
  incorporation date, the full registered address, SSIC activity codes,
  former names 1–15 and a detailed status ("Live Company", "Struck Off",
  "In Liquidation - …", "Gazetted To Be Struck Off" …). Missing values are
  the literal string ``na``. VCCs are not in it.

**No officers, shareholders or beneficial owners** are published in either —
``no_of_officers`` is a bare count — so the BODS mapping is a single entity
statement.

The flow
--------

GLEIF files Singapore companies under ``RA000523`` (ACRA Business Registry)
with the UEN in ``registeredAs`` — 12,292 of 13,326 active SG LEIs on
2026-09-10. :func:`normalise_uen` is the lookup deriver, so the pipeline
dispatches on an exact key and never matches on a name. One fetch:

1. Resolve the dataset ids from the two collections' metadata
   (``?withDatasetMetadata=true`` returns every child dataset with its name in
   one call). Ids can change between releases, so nothing is hard-coded; the
   answer is kept in-process for a day.
2. Look the UEN up in the collection-1 ACRA dataset; on a miss, in the
   other-agencies dataset (accepted only when ACRA issued the UEN).
3. For an ACRA-dataset row, look the UEN up again in the collection-2 file for
   the first character **of the name ACRA just returned**. Not GLEIF's legal
   name: GLEIF still calls ``201532108K`` *FRINSA SINGAPORE PTE.LTD.*, while
   the register renamed it *KIBU PTE. LTD.* — routing on GLEIF's name would
   look in the 'F' file and find nothing. A miss there retries ``Others``.

``fetch`` then compares the register's name — and its former names, when
collection 2 answered — with the GLEIF legal name, and **drops the record**
when none agree (the Phase 202 Hong Kong rule: an identifier that reaches a
different company must not be attached to the subject). Former names are
what keep a renamed company from being dropped.

VCC sub-funds (``T21VC0144D-SF001``) carry ACRA registrations in GLEIF but are
published in neither collection, so :func:`normalise_uen` rejects them and the
source is skipped. The umbrella VCC is a different legal record and is never
substituted.

There is no name search: ``datastore_search``'s full-text ``q`` takes 4–5
seconds over 2.1M rows, would be spent on every name search OpenCheck runs
for every country, and returns sole proprietorships, which are frequently a
person's name — the Singapore Open Data Licence grants no rights over personal
data. Singapore companies are reached from the LEI, or from a UEN typed into
the National ID tab (GLEIF reverse lookup on ``RA000523``).

Rate limits
-----------

Measured 2026-09-10 against the documented limits (per 10 seconds):
``datastore_search`` 4 without a key and 8 with a developer key; dataset
downloads 2 / 4. Excess is **HTTP 429** with
``{"code": 24, "name": "TOO_MANY_REQUESTS", …"try again in 10 seconds"}`` and
**no ``Retry-After`` header**. The key is sent as ``x-api-key``; **a wrong key
is not rejected** — it silently gets the keyless limit — so a configured key
proves nothing until a burst shows the higher limit (verified: 8 of 12 parallel
calls with the key, 4 of 12 without, from a residential IP). A cloud container
saw no enforcement on ``datastore_search`` at all, so enforcement varies by
vantage point; the documented figures are what this adapter paces to.

Calls go through :class:`opencheck.outbound_rate.TokenBucket` at a little under
the limit for the configured tier, and a lookup may spend at most
``_LOOKUP_CALL_BUDGET`` calls. A 429 waits the ten seconds the API asks for and
retries once; a second 429, an exhausted budget or any other failure records a
degradation rather than returning an empty register.

License: Singapore Open Data Licence v1.0 — https://data.gov.sg/open-data-licence
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from .. import degradation, names
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.acra_singapore import AcraSingaporeBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "acra_singapore"

#: GLEIF Registration Authority code for ACRA's Business Registry. Verified live
#: 2026-09-10: 12,292 of 13,326 active SG LEI records register under it, every
#: one with a UEN in ``registeredAs``.
ACRA_RA_CODE: str = "RA000523"

#: BODS identifier scheme — org-id.guide's code for ACRA.
SG_UEN_SCHEME = "SG-ACRA"
SG_UEN_SCHEME_NAME = "Unique Entity Number (UEN) — Accounting and Corporate Regulatory Authority (Singapore)"

_DATASTORE_URL = "https://data.gov.sg/api/action/datastore_search"
_COLLECTION_METADATA_URL = (
    "https://api-production.data.gov.sg/v2/public/api/collections/{collection_id}/metadata"
)

#: Collection 1 ("UEN") and its two datasets, by the names data.gov.sg gives them.
UEN_COLLECTION_ID = "1"
ACRA_DATASET_NAME = "Entities Registered with ACRA"
OTHER_AGENCIES_DATASET_NAME = "Entities Registered with Other UEN Issuance Agencies"

#: Collection 2 ("ACRA Information on Corporate Entities"), one dataset per
#: first character of the current name: ``… ('K')``, ``… ('Others')``.
DETAIL_COLLECTION_ID = "2"
_DETAIL_DATASET_NAME_RE = re.compile(r"\('([A-Za-z]+)'\)\s*$")
OTHERS_FILE = "Others"

#: How long resolved dataset ids are trusted. The collections refresh monthly.
_METADATA_TTL_S = 24 * 3600.0

#: Cached register rows are refetched after a week; the data is monthly.
_CACHE_MAX_AGE_DAYS = 7.0

#: ``datastore_search`` allows 8 calls per 10 s with a developer key and 4
#: without (48 / 24 a minute). Pace just under, so a retry and clock skew
#: against the server's window still fit.
_RATE_PER_MINUTE_KEYED = 42.0
_RATE_PER_MINUTE_KEYLESS = 21.0

#: Most calls one lookup may spend: two metadata calls on a cold process, two
#: collection-1 lookups (ACRA, then other agencies) and two collection-2 lookups
#: (the letter file, then ``Others``). A warm lookup spends 2.
_LOOKUP_CALL_BUDGET = 6

#: What the 429 body asks for, in seconds. There is no Retry-After header.
_RATE_LIMIT_BACKOFF_S = 10.0

#: Wall-clock budget for the whole source. Six calls at ~1.5 s each plus the
#: bucket's spacing on the keyless tier, and one ten-second back-off, fit.
_LOOKUP_TIMEOUT_S = 45.0

_ACRA_AGENCY_LABELS = frozenset(
    {"acra", "accounting and corporate regulatory authority"}
)

#: Singapore UEN shapes (https://www.uen.gov.sg): businesses ``nnnnnnnnX``,
#: local companies ``yyyynnnnnX``, and every other entity
#: ``[R|S|T]yyPQnnnnX`` (``T21LP0078G`` a limited partnership, ``S99FC5710J``
#: a foreign company branch, ``T23VC0219C`` a VCC).
_UEN_RE = re.compile(r"^(?:\d{8}[A-Z]|\d{9}[A-Z]|[RST]\d{2}[A-Z]{2}\d{4}[A-Z])$")
_VCC_SUB_FUND_RE = re.compile(r"^[RST]\d{2}VC\d{4}[A-Z]-SF\d+$")

_NAME_SIMILARITY_THRESHOLD = 0.88
_LATIN_LETTER_RE = re.compile(r"[A-Za-z]")

#: The register's literal "no value".
_NA = "na"


def normalise_uen(value: str) -> str:
    """Canonicalise a Singapore UEN, or raise ``ValueError``.

    Whitespace is removed and letters upper-cased; anything that is not one of
    the three UEN shapes is rejected so the lookup pipeline skips the adapter
    rather than querying a guessed number. A VCC sub-fund registration
    (``T21VC0144D-SF001``) is rejected explicitly: ACRA publishes no sub-fund
    rows, and the umbrella VCC is a different record.
    """
    text = "".join(str(value or "").split()).upper()
    if not text:
        raise ValueError("empty UEN")
    if _VCC_SUB_FUND_RE.match(text):
        raise ValueError(f"VCC sub-fund, not published by ACRA: {value!r}")
    if not _UEN_RE.match(text):
        raise ValueError(f"not a Singapore UEN: {value!r}")
    return text


def clean_field(value: Any) -> str:
    """A register text field with the literal ``na`` read as empty."""
    text = " ".join(str(value or "").split())
    return "" if text.lower() == _NA else text


def detail_file_for(name: str | None) -> str:
    """The collection-2 file a name is filed under: its first character when
    that is a Latin letter, else ``Others`` (digits, quotes, ``@``, ``$`` …)."""
    first = (name or "").strip()[:1].upper()
    return first if "A" <= first <= "Z" else OTHERS_FILE


def former_names(detail: dict[str, Any] | None) -> list[str]:
    """``former_entity_name1`` … ``15`` in order, without ``na`` or repeats."""
    if not detail:
        return []
    out: list[str] = []
    for index in range(1, 16):
        text = clean_field(detail.get(f"former_entity_name{index}"))
        if text and text not in out:
            out.append(text)
    return out


def _latin_agree(a: str, b: str) -> bool:
    ca, cb = names.org_comparable_name(a), names.org_comparable_name(b)
    if ca and ca == cb:
        return True
    if ca and names.despace(ca) == names.despace(cb):
        return True
    if names.distinctive_token_agreement(a, b):
        return True
    return names.name_similarity(a, b) >= _NAME_SIMILARITY_THRESHOLD


def names_agree(
    legal_name: str | None,
    entity: dict[str, Any],
    detail: dict[str, Any] | None = None,
) -> bool:
    """Does the register's record carry the name GLEIF gave the subject?

    True when the current name — or, where collection 2 answered, any former
    name — agrees with ``legal_name``. An empty ``legal_name``, or one with no
    Latin letters, cannot contradict the identifier and is treated as agreeing.

    Measured on 200 random active RA000523 LEI records (2026-09-10): of 181
    records the register answered, the check agreed on 180; the one it dropped
    was GLEIF's Chinese legal name against ACRA's English one, which is why a
    name with no Latin letters now passes. Former names are what let
    ``201532108K`` (GLEIF: FRINSA SINGAPORE, ACRA: KIBU) through.
    """
    wanted = (legal_name or "").strip()
    if not wanted:
        return True
    if not _LATIN_LETTER_RE.search(names.fold_ascii(wanted)):
        # ACRA writes every name in Latin script. A legal name GLEIF holds only
        # in another script (``经典商品有限公司`` for CLASSIC COMMODITIES PTE.
        # LTD., seen live) cannot contradict the identifier — a translation is
        # not a transliteration — so it is not a mismatch.
        return True
    candidates = [clean_field(entity.get("entity_name"))]
    if detail:
        candidates.append(clean_field(detail.get("entity_name")))
        candidates.extend(former_names(detail))
    return any(c and _latin_agree(wanted, c) for c in candidates)


def record_url(resource_id: str, uen: str) -> str:
    """A dereferenceable URL for the row: the ``datastore_search`` query that
    returns it. The datasets have no per-record web page."""
    query = urlencode({"resource_id": resource_id, "filters": f'{{"uen":"{uen}"}}'})
    return f"{_DATASTORE_URL}?{query}"


class _MetadataCache:
    """Resolved dataset ids per collection, trusted for ``_METADATA_TTL_S``."""

    def __init__(self) -> None:
        self._entries: dict[str, tuple[float, dict[str, str]]] = {}

    def get(self, collection_id: str) -> dict[str, str] | None:
        entry = self._entries.get(collection_id)
        if entry is None:
            return None
        stored_at, datasets = entry
        if time.monotonic() - stored_at > _METADATA_TTL_S:
            return None
        return datasets

    def put(self, collection_id: str, datasets: dict[str, str]) -> None:
        self._entries[collection_id] = (time.monotonic(), datasets)

    def clear(self) -> None:
        self._entries.clear()


@dataclass
class _Found:
    """What one register lookup established."""

    entity: dict[str, Any] | None = None
    detail: dict[str, Any] | None = None
    dataset: str | None = None
    resource_id: str | None = None
    complete: bool = True


_METADATA = _MetadataCache()
_BUCKETS: dict[bool, TokenBucket] = {}


def _bucket(keyed: bool) -> TokenBucket:
    bucket = _BUCKETS.get(keyed)
    if bucket is None:
        rate = _RATE_PER_MINUTE_KEYED if keyed else _RATE_PER_MINUTE_KEYLESS
        bucket = TokenBucket(rate, capacity=1, name="acra_singapore")
        _BUCKETS[keyed] = bucket
    return bucket


class AcraSingaporeAdapter(SourceAdapter):
    """Source adapter for Singapore's ACRA register on data.gov.sg."""

    id = "acra_singapore"

    lookup_derivers = (
        LookupDeriver(frozenset({ACRA_RA_CODE}), "sg_uen", normalise_uen),
    )
    lookup_pass_legal_name = True
    lookup_timeout_s = _LOOKUP_TIMEOUT_S

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Singapore ACRA — Accounting and Corporate Regulatory Authority",
            homepage="https://data.gov.sg/collections/2/view",
            description=(
                "Singapore company data from ACRA's registers on data.gov.sg, "
                "looked up by Unique Entity Number: name and former names, "
                "entity and company type, status, incorporation date and "
                "registered address."
            ),
            license="Singapore-OGL-1.0",
            attribution=(
                "Contains information from ACRA Information on Corporate Entities "
                "and Entities Registered with ACRA accessed from data.gov.sg, which "
                "is made available under the terms of the Singapore Open Data "
                "Licence version 1.0 https://data.gov.sg/open-data-licence "
                "(the date of access is each record's retrieval date)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="SG",
        )

    # ------------------------------------------------------------------
    # HTTP — every outbound call funnels through here
    # ------------------------------------------------------------------

    async def _get_json(self, url: str, params: dict[str, str]) -> dict[str, Any] | None:
        """One paced, budget-checked GET. The JSON object, or None on failure.

        Every None path records a degradation, so a register that did not
        answer is never mistaken for one that had nothing to say.
        """
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        key = get_settings().data_gov_sg_api_key
        headers = {"x-api-key": key} if key else {}

        for attempt in (1, 2):
            if budget is not None and not budget.take():
                degradation.record(
                    self.id,
                    (
                        f"ACRA per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                        "data.gov.sg requests reached; the record was not fetched"
                    ),
                    reason=degradation.REASON_RATE_LIMITED,
                )
                return None
            await _bucket(bool(key)).acquire()
            async with build_client() as client:
                response = await client.get(url, params=params, headers=headers)
            if response.status_code == 429 and attempt == 1:
                _LOG.info("data.gov.sg rate limit hit; backing off %.0fs", _RATE_LIMIT_BACKOFF_S)
                await asyncio.sleep(_RATE_LIMIT_BACKOFF_S)
                continue
            break

        if response.status_code == 429:
            degradation.record(
                self.id,
                (
                    "data.gov.sg returned HTTP 429 twice (limit is 8 requests per "
                    "10 seconds with an API key, 4 without)"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            _LOG.warning("data.gov.sg returned %s for %s", response.status_code, url)
            degradation.record(self.id, f"data.gov.sg returned HTTP {response.status_code}")
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "data.gov.sg returned a non-JSON body")
            return None
        if not isinstance(payload, dict):
            degradation.record(self.id, "data.gov.sg returned an unexpected shape")
            return None
        return payload

    async def _datasets(self, collection_id: str) -> dict[str, str] | None:
        """``{dataset name: dataset id}`` for a collection, or None on failure."""
        cached = _METADATA.get(collection_id)
        if cached is not None:
            return cached
        payload = await self._get_json(
            _COLLECTION_METADATA_URL.format(collection_id=collection_id),
            {"withDatasetMetadata": "true"},
        )
        if payload is None:
            return None
        rows = (payload.get("data") or {}).get("datasetMetadata")
        if not isinstance(rows, list):
            degradation.record(
                self.id,
                f"data.gov.sg collection {collection_id} metadata carried no dataset list",
            )
            return None
        datasets = {
            str(row.get("name") or "").strip(): str(row.get("datasetId") or "").strip()
            for row in rows
            if isinstance(row, dict) and row.get("name") and row.get("datasetId")
        }
        _METADATA.put(collection_id, datasets)
        return datasets

    async def _row_by_uen(self, resource_id: str, uen: str) -> dict[str, Any] | None:
        """The dataset row for ``uen``: the row, ``{}`` for a clean miss, or None
        when the call failed."""
        payload = await self._get_json(
            _DATASTORE_URL,
            {"resource_id": resource_id, "filters": f'{{"uen":"{uen}"}}'},
        )
        if payload is None:
            return None
        if payload.get("success") is not True:
            degradation.record(self.id, "data.gov.sg datastore_search reported no success")
            return None
        records = (payload.get("result") or {}).get("records")
        if not isinstance(records, list):
            degradation.record(self.id, "data.gov.sg datastore_search carried no records list")
            return None
        for row in records:
            if isinstance(row, dict) and clean_field(row.get("uen")).upper() == uen:
                return row
        return {}

    # ------------------------------------------------------------------
    # Search — none: see the module docstring
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — one entity by UEN
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        try:
            uen = normalise_uen(hit_id)
        except ValueError:
            return self._bundle(uen=str(hit_id or ""), legal_name=legal_name)

        cache_key = f"{_CACHE_NS}/entity/{uen}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._bundle(uen=uen, legal_name=legal_name)

        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None:
            stored = cached[0] or {}
            found = _Found(
                entity=stored.get("entity") or None,
                detail=stored.get("detail") or None,
                dataset=stored.get("dataset"),
                resource_id=stored.get("resource_id"),
            )
        else:
            fetched = await self._fetch_live(uen)
            if fetched is None:
                return self._bundle(uen=uen, legal_name=legal_name)
            found = fetched
            if found.complete:
                # A miss is cached too — "not in the register" is an answer.
                self._cache.put(
                    cache_key,
                    {
                        "entity": found.entity,
                        "detail": found.detail,
                        "dataset": found.dataset,
                        "resource_id": found.resource_id,
                    },
                )
        entity, detail = found.entity, found.detail

        if not entity:
            _LOG.info("ACRA: %s not in the register datasets", uen)
            return self._bundle(uen=uen, legal_name=legal_name, not_in_register=True)

        if not names_agree(legal_name, entity, detail):
            _LOG.info("ACRA: %s names a different entity — record dropped", uen)
            return self._bundle(uen=uen, legal_name=legal_name, name_mismatch=True)

        return self._bundle(
            uen=uen,
            legal_name=legal_name,
            entity=entity,
            detail=detail,
            dataset=found.dataset,
            resource_id=found.resource_id,
        )

    async def _fetch_live(self, uen: str) -> _Found | None:
        """What the register holds for ``uen``, or None when the register could
        not be asked. ``complete`` is False when collection 2 failed, so the
        partial answer is served but not cached."""
        uen_datasets = await self._datasets(UEN_COLLECTION_ID)
        if uen_datasets is None:
            return None
        acra_id = uen_datasets.get(ACRA_DATASET_NAME)
        if not acra_id:
            degradation.record(
                self.id,
                f"data.gov.sg collection {UEN_COLLECTION_ID} no longer lists the ACRA entities dataset",
            )
            return None

        entity = await self._row_by_uen(acra_id, uen)
        if entity is None:
            return None
        if entity:
            found = _Found(entity=entity, dataset="acra", resource_id=acra_id)
        else:
            found = _Found()
            other_id = uen_datasets.get(OTHER_AGENCIES_DATASET_NAME)
            if other_id:
                other = await self._row_by_uen(other_id, uen)
                if other is None:
                    return None
                agency = clean_field(other.get("issuance_agency_desc")).lower()
                if other and agency in _ACRA_AGENCY_LABELS:
                    found = _Found(entity=other, dataset="other_agencies", resource_id=other_id)

        if not found.entity or found.dataset != "acra":
            # A clean miss, or a VCC: collection 2 holds only ACRA-dataset rows.
            return found

        detail, detail_id, complete = await self._detail(
            uen, clean_field(found.entity.get("entity_name"))
        )
        if detail:
            found.detail, found.resource_id = detail, detail_id
        found.complete = complete
        return found

    async def _detail(
        self, uen: str, name: str
    ) -> tuple[dict[str, Any] | None, str | None, bool]:
        """``(row, resource_id, complete)`` from collection 2, routed by the
        register's own current name."""
        detail_datasets = await self._datasets(DETAIL_COLLECTION_ID)
        if detail_datasets is None:
            return None, None, False
        by_file: dict[str, str] = {}
        for dataset_name, dataset_id in detail_datasets.items():
            match = _DETAIL_DATASET_NAME_RE.search(dataset_name)
            if match:
                label = match.group(1)
                by_file[label.upper() if len(label) == 1 else label] = dataset_id

        files = [detail_file_for(name)]
        if files[0] != OTHERS_FILE:
            files.append(OTHERS_FILE)
        for file in files:
            resource_id = by_file.get(file)
            if not resource_id:
                degradation.record(
                    self.id,
                    f"data.gov.sg collection {DETAIL_COLLECTION_ID} lists no '{file}' dataset; detailed record not fetched",
                )
                return None, None, False
            row = await self._row_by_uen(resource_id, uen)
            if row is None:
                return None, None, False
            if row:
                return row, resource_id, True
        return None, None, True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        *,
        uen: str,
        legal_name: str,
        entity: dict[str, Any] | None = None,
        detail: dict[str, Any] | None = None,
        dataset: str | None = None,
        resource_id: str | None = None,
        not_in_register: bool = False,
        name_mismatch: bool = False,
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "uen": uen,
            "entity": entity,
            "detail": detail,
            "dataset": dataset,
            "record_resource_id": resource_id,
            "legal_name": legal_name,
            "is_stub": entity is None,
        }
        if not_in_register:
            bundle["not_in_register"] = True
        if name_mismatch:
            bundle["name_mismatch"] = True
        if entity is not None:
            validate_raw(self.id, AcraSingaporeBundle, bundle)
        return bundle
