"""Washington DC DLCP adapter — live, keyed on the Corporations Division file number.

The District of Columbia's Department of Licensing and Consumer Protection
(DLCP) publishes its Corporations Division register as three tables on one
ArcGIS FeatureServer at ``maps2.dcgis.dc.gov``, keyless and queryable row by
row with the standard ArcGIS REST ``query`` operation:

* **Table 0 — Corporate Registration** (504,036 rows on 2026-09-18): file
  number, entity status, locale (Domestic / Foreign), model type, business
  name and address, registered agent name and address, effective date.
  ``FILE_NUMBER`` is unique.
* **Table 1 — Trade Name** (62,088 rows): trade names ("doing business as")
  with their own ``TN…`` registration numbers, joined to the entity by
  ``INITIAL_FILENUMBER``.
* **Table 2 — Beneficial Owners** (491,151 rows): a name and an address per
  owner, joined to the entity by ``INITIALFILENUMBER``.

What Table 2 actually is
------------------------

**Not a FATF-style beneficial ownership list.** It is the disclosure required
by **D.C. Code § 29-102.11(a)(6)** in the biennial report: "the names,
residence and business addresses of each person whose aggregate share of
direct or indirect, legal or beneficial ownership of a **governance or total
distributional interest** of the entity: (A) Exceeds 10%; or (B) Does not
exceed 10%; provided, that the person: (i) Controls the financial or
operational decisions of the entity; or (ii) Has the ability to direct the
day-to-day operations of the entity." Form BRA-25 collects it as one combined
list — "each person (member, manager, officer, director, shareholder, partner,
trustee, etc.)".

Three consequences the mapping has to respect:

1. A **governance** interest qualifies on its own, so nonprofit boards land in
   the table wholesale: American University files 27 trustees, Children's
   National 18, the National Geographic Society 3 (measured 2026-09-18).
2. There is **no role and no percentage** on a row, and the statute covers
   direct *and* indirect holdings, so every interest is
   ``unknownInterest`` / ``directOrIndirect: "unknown"`` with **no share**.
3. Roughly **4.5%** of owner rows are legal entities rather than natural
   persons (178 of a random 4,000 rows), so each row is classified before it
   becomes a person or entity statement — see :func:`classify_owner`.

The flow
--------

GLEIF files DC companies under ``RA000601`` with the file number in
``registeredAs``: 502 of the 726 ``US-DC`` LEI records on 2026-09-18.
:func:`normalise_file_number` is the lookup deriver, so the pipeline
dispatches on an exact key. One fetch spends up to three queries:

1. Table 0 by ``FILE_NUMBER``. A miss retries the ``US-DC-`` prefixed form,
   which is one of the live file-number shapes GLEIF sometimes drops.
2. Table 1 for the entity's trade names.
3. Table 2 for the owner rows.

``fetch`` then compares the register's name — and its trade names — with the
GLEIF legal name, and **drops the record** when none agree. That gate is not
theoretical: GLEIF gives ``L21249`` as the ``registeredAs`` for *American
Foreign Policy Council* (LEI ``549300W96W2VKSMVDF81``), and DLCP's ``L21249``
is *CONNIE-19 STREET LLC*, an unrelated active company; AFPC's real file
number is ``825027``. A straight key join would publish one company's owners
under another's name. The gate must also tolerate a rename, because DLCP is
routinely **more** current than GLEIF (``STEPTOE & JOHNSON LLP`` → ``STEPTOE
LLP``, ``NRA FOUNDATION`` → ``The 1791 Foundation``), which is what the trade
names and the similarity threshold are for.

Match rate, measured over the 500 distinct ``RA000601`` ``registeredAs``
values on 2026-09-18: 486 exact, 1 more with the ``US-DC-`` prefix, 13 misses.
By LEI registration status: ``ISSUED`` 144/145 (99.3%), ``LAPSED`` 322/333,
``RETIRED`` 22/23 — the misses are almost all lapsed LEIs still carrying a
legacy identifier (old DCRA numbers, a trade-name number, bare 8-digit codes).

Scope
-----

A lookup by file number returns the register's row **whatever its status** —
a revoked or dissolved company is exactly what a due-diligence check needs to
see, and the status drives the liveness banner. Only ~23% of the register is
active (116,121 of 504,036), so the **name fallback** below is restricted to
active companies: guessing an entity from a name is the risky path, and the
cheapest way to keep it safe is to let it reach only live records.

There is no general name search. DC company names are reached from the LEI, or
from a file number typed into the National ID tab (GLEIF reverse lookup on
``RA000601``).

Freshness
---------

All three tables reload wholesale each day; on 2026-09-18 every row in all
three carried the identical ``DCS_LAST_MOD_DTTM`` of 2026-09-17 07:49–07:50.
It is a **table reload stamp, not a per-row change date** — usable as the
snapshot date for provenance, useless for change detection.

Rate limits
-----------

None documented and none observed: 20 sequential queries ran in 8.1 s with
zero errors from a cloud vantage point, and a single ``where=FILE_NUMBER='…'``
query returns in ~0.35 s (measured 2026-09-18). Calls are still paced through
a token bucket and capped by a per-lookup call budget, so an unannounced limit
degrades this source rather than the lookup.

License: CC BY 4.0 — https://creativecommons.org/licenses/by/4.0/
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlencode

import pycountry

from .. import degradation, names
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.dlcp_dc import DlcpDcBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "dlcp_dc"

#: GLEIF Registration Authority code for the DC Corporations Division.
#: Verified live 2026-09-18: 502 of the 726 ``US-DC`` LEI records register
#: under it, every one with a file number in ``registeredAs``.
DLCP_RA_CODE: str = "RA000601"

#: BODS identifier scheme for a DC Corporations Division file number. The
#: ISO 3166-2 subdivision code, deliberately the same scheme the BODS mapper
#: already stamps on a DC entity reached through GLEIF
#: (``_US_STATE_REGISTRY_NAMES``): one scheme means the two sources corroborate
#: each other in the reconciler, where two spellings of the same register would
#: read as each asserting an identifier the other lacks.
DC_FILE_NUMBER_SCHEME = "US-DC"
DC_FILE_NUMBER_SCHEME_NAME = (
    "District of Columbia Department of Licensing and Consumer Protection"
)

_FEATURE_SERVER = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
    "Business_Licensing_and_Grants_WebMercator/FeatureServer"
)

#: Table ids on the FeatureServer.
LAYER_CORPORATE_REGISTRATION = 0
LAYER_TRADE_NAME = 1
LAYER_BENEFICIAL_OWNERS = 2

#: DLCP publishes some file numbers with a jurisdiction prefix
#: (``US-DC-LL012601299``) and GLEIF sometimes files the bare form. One retry.
_US_DC_PREFIX = "US-DC-"

#: The register's own maximum page size. An entity's owner list is far below
#: it — the largest seen is 27 rows — so one page is always enough.
_MAX_RECORD_COUNT = 2000

#: Most calls one lookup may spend across every DC entity it touches.
#:
#: One entity's worst case is **seven**: two Table 0 probes (bare, then
#: ``US-DC-`` prefixed), Table 1 and Table 2 for whatever that found, one
#: name-fallback query, then Table 1 and Table 2 again for the entity the name
#: found. That is the AFPC/CONNIE-19 recovery path, and it is the reason this
#: number is not five: Phase 225 shipped a budget of 5, one short, so the last
#: call — the owner rows for the *right* company — was refused and
#: the fetch returned nothing at all. A collision recovery that
#: silently drops the record is worse than no recovery, because the card simply
#: disappears with a rate-limit note attached.
#:
#: Twelve leaves room for a second DC entity reached through the deepen path
#: (a DC parent or subsidiary in the same lookup) without ever letting one
#: ownership chain monopolise the register.
_LOOKUP_CALL_BUDGET = 12

#: No documented limit and none observed (20 calls in 8.1 s, 2026-09-18).
#: Paced anyway so an unannounced limit degrades this source, not the lookup.
_RATE_PER_MINUTE = 60.0

#: Six ~0.35 s queries plus the bucket's spacing fit comfortably.
_LOOKUP_TIMEOUT_S = 30.0

#: Register rows are refetched after a day; the tables reload daily.
_CACHE_MAX_AGE_DAYS = 1.0

#: A file number is an opaque register string: digits, letters and hyphens.
#: Every live shape fits — ``000077``, ``L00005029230``, ``L21249``,
#: ``N00008414776``, ``P00454``, ``US-DC-LL012601299`` (measured 2026-09-18).
#: Anything else is not a DLCP file number and the source is skipped rather
#: than queried on a guess.
_FILE_NUMBER_RE = re.compile(r"^[A-Z0-9][A-Z0-9-]{2,29}$")

#: Entity statuses DLCP publishes. ``Active - In Good Standing`` and
#: ``Active - Not in Good Standing`` are live; the register's own wording is
#: always carried through verbatim.
LIVE_STATUSES: tuple[str, ...] = (
    "Active - In Good Standing",
    "Active - Not in Good Standing",
    "Active - In Good Standing - Pending Domestication",
)
TERMINAL_STATUSES: tuple[str, ...] = (
    "Dissolved",
    "Revoked",
    "Terminated",
    "Withdrawn",
    "Merged",
    "Consolidated",
    "Converted",
    "Inactive - Cancelled",
    "Domesticated",
)

_NAME_SIMILARITY_THRESHOLD = 0.88

#: Legal-form markers that make an owner row a legal entity rather than a
#: natural person. Deliberately high-precision: a marker here is a form of
#: incorporation or a legal arrangement, never a word that is also a surname
#: ("Church", "Bank", "Fund"). The error this ordering prefers is filing an
#: unmarked organisation as a person's name, which shows the string the
#: register published; the reverse — turning a named individual into a
#: company — would misrepresent a person. Measured on a random 4,000 rows
#: (2026-09-18): 178 rows (4.5%) carry one of these.
_ENTITY_MARKERS: tuple[str, ...] = (
    "llc", "l l c", "llp", "l l p", "pllc", "pc", "plc",
    "lp", "l p", "inc", "incorporated", "corp", "corporation",
    "ltd", "limited", "company", "co",
    "trust", "foundation", "association", "partnership",
    "holdings", "gmbh", "nv", "bv", "sa", "sarl", "ag", "pte",
    "na",  # "…, N.A." — a national banking association
)

#: Two-letter US state / territory codes, for reading an owner address tail.
_US_STATE_RE = re.compile(r"^[A-Z]{2}$")
#: US ZIP, five digits or ZIP+4.
_US_ZIP_RE = re.compile(r"^\d{5}(?:-\d{4})?$")
#: The register writes the country as an ISO alpha-3 code (USA, GBR, CAN…).
_ISO3_RE = re.compile(r"^[A-Z]{3}$")

_WORD_RE = re.compile(r"[a-z0-9]+")

_BUCKET = TokenBucket(_RATE_PER_MINUTE, capacity=1, name="dlcp_dc")


def normalise_file_number(value: str) -> str:
    """Canonicalise a DLCP file number, or raise ``ValueError``.

    Whitespace is removed and letters upper-cased. The file number is treated
    as an **opaque string**: no zero-stripping, no prefix rewriting, no
    reshaping. DLCP issues at least six different shapes and the only thing
    they have in common is that the register stores them verbatim.
    """
    text = "".join(str(value or "").split()).upper()
    if not text:
        raise ValueError("empty file number")
    if not _FILE_NUMBER_RE.match(text):
        raise ValueError(f"not a DC Corporations Division file number: {value!r}")
    return text


def clean_field(value: Any) -> str:
    """A register text field with whitespace collapsed."""
    return " ".join(str(value or "").split())


def is_live_status(status: str | None) -> bool:
    """Does this ``ENTITY_STATUS`` label mean the company is on the register?"""
    text = clean_field(status).lower()
    return any(text == s.lower() for s in LIVE_STATUSES)


def classify_owner(name: str | None) -> str:
    """``"entity"`` or ``"person"`` for one Table 2 owner name.

    DLCP publishes no owner type, so the legal form in the name is the only
    signal. A name carrying one of ``_ENTITY_MARKERS`` as a whole token is an
    entity; everything else is filed as a person.

    The known limit: an organisation whose name has no legal-form marker —
    "The Washington Center", "Army Distaff Foundation Board" (caught, on
    "foundation"), "Lowell School, Inc." (caught, on "inc") — is filed as a
    person. The mapper's ``bo_person`` / ``bo_entity`` record kinds route the
    ``beneficialOwnershipOrControl`` flag off this classification, so it is
    recorded on the statement and auditable rather than silent.
    """
    tokens = _WORD_RE.findall(names.fold_ascii(clean_field(name)).lower())
    if not tokens:
        return "person"
    return "entity" if any(t in _ENTITY_MARKERS for t in tokens) else "person"


def parse_owner_address(
    text: str | None, *, address_type: str = "residence"
) -> dict[str, Any] | None:
    """Split a Table 2 ``ADDRESS`` into a BODS address object, or None.

    The register writes a consistent comma-separated tail — ``street[, unit],
    city, state, ZIP, ISO3`` — on every row seen (4,000 of 4,000 populated,
    93% with four or five commas, 2026-09-18). The postcode and country are
    isolated into their own fields because the BODS v0.4 Address schema says
    they SHOULD NOT be repeated inside ``address``; everything the filer wrote
    before them is carried through unchanged.

    ``address_type`` defaults to ``residence``, which is what
    § 29-102.11(a)(6) asks a natural person for and what the open dataset
    publishes. It has to be overridden for an owner row that is a legal
    entity: BODS v0.4 allows ``residence`` only on a person record and
    ``registered``/``business``/``alternative`` only on an entity record, so
    an entity owner takes ``business`` — the register does not say the address
    is the entity's registered office, only that it is the address filed.
    """
    raw = clean_field(text)
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return None

    country: dict[str, str] | None = None
    post_code = ""
    if len(parts) > 1 and _ISO3_RE.match(parts[-1]):
        country = _country_from_iso3(parts[-1])
        parts = parts[:-1]
    if len(parts) > 1 and _US_ZIP_RE.match(parts[-1]):
        post_code = parts[-1]
        parts = parts[:-1]

    address: dict[str, Any] = {"type": address_type, "address": ", ".join(parts)}
    if post_code:
        address["postCode"] = post_code
    if country:
        address["country"] = country
    return address


def _country_from_iso3(code: str) -> dict[str, str]:
    """``{"name": …, "code": …}`` for an ISO alpha-3 code.

    An alpha-3 pycountry cannot resolve still yields ``{"name": <code>}``: a
    BODS country MUST have a name and SHOULD have a 2-letter code, so the
    unresolved code is carried as the name rather than dropped.
    """
    entry = pycountry.countries.get(alpha_3=code.upper())
    if entry is None:
        return {"name": code.upper()}
    return {"name": entry.name, "code": entry.alpha_2}


def trade_name_list(rows: list[dict[str, Any]] | None) -> list[str]:
    """Distinct trade names from Table 1 rows, in the order given."""
    out: list[str] = []
    for row in rows or []:
        text = clean_field(row.get("TRADE_NAME"))
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
    company: dict[str, Any] | None,
    trade_names: list[str] | None = None,
) -> bool:
    """Does the register's record carry the name GLEIF gave the subject?

    True when the registered business name, or any trade name the entity has
    filed, agrees with ``legal_name``. An empty ``legal_name`` cannot
    contradict the identifier and is treated as agreeing.

    Trade names do the job former names do for other registers: DLCP publishes
    no name history, but a company that has traded under its older name has
    usually registered it here.
    """
    wanted = clean_field(legal_name)
    if not wanted:
        return True
    candidates = [clean_field((company or {}).get("BUSINESS_NAME"))]
    candidates.extend(trade_names or [])
    return any(c and _latin_agree(wanted, c) for c in candidates)


def record_url(layer: int, where: str) -> str:
    """A dereferenceable URL for a row: the ArcGIS query that returns it.

    The FeatureServer publishes no per-record web page.
    """
    query = urlencode(
        {"where": where, "outFields": "*", "f": "json"},
    )
    return f"{_FEATURE_SERVER}/{layer}/query?{query}"


def _quote(value: str) -> str:
    """A value as an ArcGIS SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


class DlcpDcAdapter(SourceAdapter):
    """Source adapter for the Washington DC Corporations Division register."""

    id = "dlcp_dc"

    lookup_derivers = (
        LookupDeriver(
            frozenset({DLCP_RA_CODE}), "us_dc_file_number", normalise_file_number
        ),
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
            name=(
                "DLCP — Department of Licensing and Consumer Protection "
                "Corporations Division (Washington, DC)"
            ),
            homepage="https://opendata.dc.gov/datasets/DCGIS::beneficial-owners/about",
            description=(
                "Washington DC company registrations from DLCP's Corporations "
                "Division, looked up by file number: name, status, entity "
                "type, address, registered agent, trade names, and the owners "
                "and controllers filed on the biennial report under D.C. Code "
                "§ 29-102.11(a)(6)."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Corporate Registration, Trade Name and Beneficial Owners data "
                "© District of Columbia (Department of Licensing and Consumer "
                "Protection / Office of the Chief Technology Officer), via "
                "opendata.dc.gov, licensed under Creative Commons Attribution "
                "4.0 https://creativecommons.org/licenses/by/4.0/"
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="US",
        )

    # ------------------------------------------------------------------
    # HTTP — every outbound call funnels through here
    # ------------------------------------------------------------------

    async def _query(
        self,
        layer: int,
        where: str,
        *,
        out_fields: str = "*",
        order_by: str = "",
    ) -> list[dict[str, Any]] | None:
        """Attribute rows for one ArcGIS query, or None when it failed.

        Every None path records a degradation, so a register that did not
        answer is never mistaken for one that had nothing to say.
        """
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"DLCP per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                    "ArcGIS requests reached; the record was not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "false",
            "resultRecordCount": str(_MAX_RECORD_COUNT),
            "f": "json",
        }
        if order_by:
            params["orderByFields"] = order_by

        await _BUCKET.acquire()
        async with build_client() as client:
            response = await client.get(f"{_FEATURE_SERVER}/{layer}/query", params=params)

        if response.status_code == 429:
            degradation.record(
                self.id,
                "maps2.dcgis.dc.gov returned HTTP 429",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            _LOG.warning(
                "DLCP FeatureServer returned %s for layer %s", response.status_code, layer
            )
            degradation.record(
                self.id, f"maps2.dcgis.dc.gov returned HTTP {response.status_code}"
            )
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "maps2.dcgis.dc.gov returned a non-JSON body")
            return None
        if not isinstance(payload, dict):
            degradation.record(self.id, "maps2.dcgis.dc.gov returned an unexpected shape")
            return None
        # ArcGIS reports failures inside a 200 body.
        if "error" in payload:
            message = str((payload.get("error") or {}).get("message") or "unknown error")
            degradation.record(
                self.id, f"maps2.dcgis.dc.gov rejected the query: {message}"
            )
            return None
        features = payload.get("features")
        if not isinstance(features, list):
            degradation.record(self.id, "maps2.dcgis.dc.gov returned no feature list")
            return None
        rows: list[dict[str, Any]] = []
        for feature in features:
            if isinstance(feature, dict) and isinstance(feature.get("attributes"), dict):
                rows.append(feature["attributes"])
        return rows

    # ------------------------------------------------------------------
    # Search — none: see the module docstring
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — one entity by file number
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The register's record for one file number, name-gated.

        Split deliberately into two cached reads — the file number's record,
        and (only when that one is wrong) the name's. Both caches are keyed on
        register facts alone, never on the caller's ``legal_name``, and the
        name gate runs *here*, over whatever came back. That is what makes a
        second ``fetch`` for the same entity free.

        Phase 225 folded ``legal_name`` into one cache key, which looked
        harmless: the lookup pipeline dispatches with the GLEIF legal name and
        so always hit its own entry. But its second pass — ``_count_only`` and
        ``_safe_deepen`` in ``routers/lookup.py`` — calls ``fetch(hit_id)``
        with *no* legal name, on the stated assumption that dispatch has
        already warmed the cache. With the name in the key that assumption was
        false for this source alone, so every DC entity was fetched from DLCP
        twice per lookup: six ArcGIS calls where three were needed, the last of
        them refused by the call budget.
        """
        try:
            file_number = normalise_file_number(hit_id)
        except ValueError:
            return self._bundle(file_number=str(hit_id or ""), legal_name=legal_name)

        by_number = await self._record_for_file_number(file_number)
        if by_number is None:
            # The register could not be asked — never mistaken for an answer.
            return self._bundle(file_number=file_number, legal_name=legal_name)

        company = by_number.get("company") or None
        trade_names = by_number.get("trade_names") or []
        if company is not None and names_agree(legal_name, company, trade_names):
            return self._bundle(
                file_number=file_number,
                legal_name=legal_name,
                company=company,
                owners=by_number.get("owners") or [],
                trade_names=trade_names,
                matched_by=by_number.get("matched_by") or None,
            )
        if company is not None:
            _LOG.info(
                "DLCP: %s names a different entity — trying the name instead", file_number
            )

        # Either the file number is not in the register, or it reached a
        # different company. Both are recoverable from the name, but only
        # against a single unambiguous ACTIVE match — see _company_by_name.
        by_name = await self._record_for_name(legal_name)
        if by_name is None:
            return self._bundle(file_number=file_number, legal_name=legal_name)
        if by_name.get("company"):
            return self._bundle(
                file_number=file_number,
                legal_name=legal_name,
                company=by_name["company"],
                owners=by_name.get("owners") or [],
                trade_names=by_name.get("trade_names") or [],
                matched_by="name",
            )
        if company is not None:
            return self._bundle(
                file_number=file_number, legal_name=legal_name, name_mismatch=True
            )
        return self._bundle(
            file_number=file_number, legal_name=legal_name, not_in_register=True
        )

    async def _record_for_file_number(self, file_number: str) -> dict[str, Any] | None:
        """The register's rows for a file number, cached; None if unaskable.

        An empty ``company`` means the register answered and holds no such file
        number — a fact worth caching, so a miss is not re-asked all day.
        """
        cache_key = f"{_CACHE_NS}/entity/{file_number}"
        cached = self._cached(cache_key)
        if cached is not None:
            return cached
        if not self.info.live_available:
            return None

        company, matched_by = await self._company_by_file_number(file_number)
        if company is None and matched_by is None:
            return None
        if company is None:
            return self._store(cache_key, {})

        trade_names, owners = await self._related(
            clean_field(company.get("FILE_NUMBER")) or file_number
        )
        if trade_names is None or owners is None:
            return None
        return self._store(
            cache_key,
            {
                "company": company,
                "trade_names": trade_names,
                "owners": owners,
                "matched_by": matched_by,
            },
        )

    async def _record_for_name(self, legal_name: str) -> dict[str, Any] | None:
        """The register's rows for a name, cached; None if unaskable.

        Cached under the name rather than the file number because that is what
        was asked: the whole point of this path is that the file number we hold
        is wrong, so it cannot key the answer. An empty ``company`` means the
        register answered and offered no single unambiguous active match.
        """
        name_key = names.display_name_key(legal_name)
        if not name_key:
            return {}
        cache_key = f"{_CACHE_NS}/by-name/{name_key}"
        cached = self._cached(cache_key)
        if cached is not None:
            return cached
        if not self.info.live_available:
            return None

        company = await self._company_by_name(legal_name)
        if company is None:
            return None
        if not company:
            return self._store(cache_key, {})

        found_number = clean_field(company.get("FILE_NUMBER"))
        trade_names, owners = await self._related(found_number)
        if trade_names is None or owners is None:
            return None
        record = {"company": company, "trade_names": trade_names, "owners": owners}
        if found_number:
            # The same register rows, under the register's own key. A recovery
            # asserts the file number it found, and the pipeline hops on it —
            # so without this the next fetch asks DLCP for rows already held.
            self._store(f"{_CACHE_NS}/entity/{found_number}", dict(record, matched_by="file_number"))
        return self._store(cache_key, record)

    def _cached(self, cache_key: str) -> dict[str, Any] | None:
        """A stored register answer, or None when there is none to serve."""
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is None:
            return None
        stored = cached[0]
        return stored if isinstance(stored, dict) else {}

    def _store(self, cache_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._cache.put(cache_key, payload)
        return payload

    async def _company_by_file_number(
        self, file_number: str
    ) -> tuple[dict[str, Any] | None, str | None]:
        """``(row, how)`` for a file number.

        ``(None, None)`` means the register could not be asked; ``(None,
        "miss")`` means it answered and holds no such file number.
        """
        candidates = [file_number]
        if not file_number.startswith(_US_DC_PREFIX):
            candidates.append(_US_DC_PREFIX + file_number)
        for index, candidate in enumerate(candidates):
            rows = await self._query(
                LAYER_CORPORATE_REGISTRATION, f"FILE_NUMBER = {_quote(candidate)}"
            )
            if rows is None:
                return None, None
            for row in rows:
                if clean_field(row.get("FILE_NUMBER")).upper() == candidate:
                    return row, ("file_number" if index == 0 else "file_number_prefixed")
        return None, "miss"

    async def _company_by_name(self, legal_name: str) -> dict[str, Any] | None:
        """A single unambiguous ACTIVE company for ``legal_name``.

        ``None`` means the register could not be asked; ``{}`` means no safe
        match. A name match is the weakest evidence this adapter accepts, so
        it is deliberately narrow: the register is asked only for **active**
        companies, and the answer is used only when exactly one of them has a
        comparable name equal to the one GLEIF holds. Anything ambiguous is
        declined rather than guessed — the AFPC/CONNIE-19 collision in the
        module docstring is what this path exists to recover from, and it
        would be no improvement to swap one wrong company for another.
        """
        wanted = clean_field(legal_name)
        comparable = names.org_comparable_name(wanted)
        if not wanted or not comparable:
            return {}
        # ArcGIS has no case-insensitive operator; UPPER() on both sides is
        # supported and the register writes names in mixed case.
        prefix = wanted.upper()[:40].replace("%", "")
        where = (
            f"UPPER(BUSINESS_NAME) LIKE {_quote(prefix + '%')} "
            "AND ENTITY_STATUS LIKE 'Active%'"
        )
        rows = await self._query(
            LAYER_CORPORATE_REGISTRATION,
            where,
            out_fields="FILE_NUMBER,ENTITY_STATUS,LOCALE,MODELTYPE,BUSINESS_NAME,"
            "BUSNIESS_ADDRESS_LINE1,BUSNIESS_ADDRESS_LINE2,BUSNIESS_ADDRESS_LINE3,"
            "BUSNIESS_ADDRESS_LINE4,BUSINESS_CITY,BUSINESS_STATE,ZIPCODE,"
            "BUSINESS_COUNTRY,RA_NAME,RA_CITY,RA_STATE,EFFECTIVE_DATE",
        )
        if rows is None:
            return None
        matches = [
            row
            for row in rows
            if names.org_comparable_name(clean_field(row.get("BUSINESS_NAME")))
            == comparable
        ]
        if len(matches) != 1:
            return {}
        return matches[0]

    async def _related(
        self, file_number: str
    ) -> tuple[list[str] | None, list[dict[str, Any]] | None]:
        """``(trade names, owner rows)`` for an entity, or ``(None, None)``."""
        if not file_number:
            return [], []
        trade_rows = await self._query(
            LAYER_TRADE_NAME,
            f"INITIAL_FILENUMBER = {_quote(file_number)}",
            out_fields="TRADE_NAME,TRADENAME_STATUS,FILE_NUMBER,INITIAL_FILENUMBER",
            order_by="TRADE_NAME",
        )
        if trade_rows is None:
            return None, None
        owner_rows = await self._query(
            LAYER_BENEFICIAL_OWNERS,
            f"INITIALFILENUMBER = {_quote(file_number)}",
            out_fields="NAME,ADDRESS,BUSINESSNAME,MODELTYPE,LOCALE,"
            "INITIALFILENUMBER,STATUS",
            order_by="NAME",
        )
        if owner_rows is None:
            return None, None
        return trade_name_list(trade_rows), owner_rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        *,
        file_number: str,
        legal_name: str,
        company: dict[str, Any] | None = None,
        owners: list[dict[str, Any]] | None = None,
        trade_names: list[str] | None = None,
        matched_by: str | None = None,
        not_in_register: bool = False,
        name_mismatch: bool = False,
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "file_number": file_number,
            "company": company,
            "owners": list(owners or []),
            "trade_names": list(trade_names or []),
            "matched_by": matched_by,
            "legal_name": legal_name,
            "is_stub": company is None,
        }
        if not_in_register:
            bundle["not_in_register"] = True
        if name_mismatch:
            bundle["name_mismatch"] = True
        if company is not None:
            validate_raw(self.id, DlcpDcBundle, bundle)
        return bundle


__all__ = [
    "DLCP_RA_CODE",
    "DC_FILE_NUMBER_SCHEME",
    "DC_FILE_NUMBER_SCHEME_NAME",
    "DlcpDcAdapter",
    "LIVE_STATUSES",
    "TERMINAL_STATUSES",
    "classify_owner",
    "clean_field",
    "is_live_status",
    "names_agree",
    "normalise_file_number",
    "parse_owner_address",
    "record_url",
    "trade_name_list",
]
