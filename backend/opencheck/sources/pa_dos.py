"""Pennsylvania Department of State adapter — live, keyed on the filing number.

The Department of State's Bureau of Corporations publishes its register of
**active** business registrations on data.pa.gov as the Socrata dataset
``xvd7-5r2c`` *Registered Businesses in PA Current by County* — 4,200,322
rows on 12 Nov 2026, rows last updated 3 Nov 2026 (the metadata says
monthly). Dataset licence: *Public Domain U.S. Government*; the portal's data
policy says data sets are "offered free and without restriction", asks for
credit ("while not required") and gives no warranty.

One row is one business × one listed party: business name, filing number,
registered address, type of registration (18 values: domestic and foreign
business corporations, LLCs, LPs, nonprofits, business trusts, authorities,
land banks…), creation date, and the party's title (``party_type``) and name
in three parts. A business with no party listed has one row with the party
columns empty. There is **no status column** and **no registered agent**.

What the list is, and is not
----------------------------

It holds active registrations only, and the Department warns that "due to
statutory limitations in removing businesses no longer in operation from our
database, this data shows a larger number of active businesses than
currently exist". So:

* A record found is "on the Department of State's list of active
  registrations" — **never** "active". No status is stated in BODS; a
  ``commenting`` annotation says what the list is.
* A number not on the list is a **note card**, never "dissolved": the list
  holds no dissolved, withdrawn or merged company, and is a month old.

The flow
--------

GLEIF files Pennsylvania companies under ``RA000632`` with the bare filing
number (``676862``, FORTA, LLC); a few issuers file it zero-padded to ten
digits (``0015735802``). The dataset pads every number to ten digits
(``0000676862``). :func:`normalise_filing_number` gives the bare form, which
keys the deriver and is the identifier the mapper writes; the query pads it.
Keyed on the RA code, never the jurisdiction (the ``ny_dos`` rule).

Measured on the 2,153 ISSUED LEIs under RA000632 (12 Nov 2026): 1,836 resolve
(85 %); 1,832 of those agree with GLEIF's name and 4 do not (a rename, or a
different entity on the number). Each filing number carries one name and one
type across its rows. **The name is gated** (the DC / NY rule): a record whose
name does not agree with GLEIF's is a note card.

Parties
-------

``party_type`` is free text — Governor (820k rows), President, Organizer,
Treasurer, Secretary, Vice President, General Partner, Incorporator, Member,
Owner, "Stockholder", "100% Owner", "Parent Company"… — and is carried as
filed. A row with a title and no name is no one. The Department publishes
**no address for a party**; the address on the row is the business's.

License: Public Domain U.S. Government (data.pa.gov). Attribution, which the
portal requests: "Data retrieved from data.pa.gov — Pennsylvania Department
of State, Registered Businesses in PA Current by County."
"""

from __future__ import annotations

import asyncio
import email.utils
import logging
import re
import weakref
from typing import Any
from urllib.parse import urlencode

from .. import degradation, names
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.pa_dos import PaDosBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "pa_dos"

#: GLEIF's registration authority for the Pennsylvania Department of State,
#: Bureau of Corporations and Charitable Organizations.
PA_DOS_RA_CODE = "RA000632"

#: The ISO 3166-2 subdivision code, which the GLEIF mapper's US-state fallback
#: already wrote for a Pennsylvania number (the DC / NY / FL rule).
PA_DOS_SCHEME = "US-PA"
PA_DOS_SCHEME_NAME = "Pennsylvania Department of State, Bureau of Corporations"

DATASET = "xvd7-5r2c"
_SODA_BASE = "https://data.pa.gov/resource"
DATASET_PAGE = (
    "https://data.pa.gov/Licenses-Certificates/"
    "Registered-Businesses-in-PA-Current-by-County-Depa/xvd7-5r2c"
)

#: Columns asked for — every published column but the geocoded point and the
#: presentation copy of the name.
_SELECT = (
    "business_name_source_,filing_number,address_line1,address_line2,city,"
    "state,zip,typeofbusinessregistration,creationdate,party_type,first_name,"
    "middle_name,last_name,shortcountyname"
)

_ROW_LIMIT = 1000
_SEARCH_LIMIT = 20
_LOOKUP_CALL_BUDGET = 4
_RATE_PER_MINUTE = 120.0
_BURST = 4
_LOOKUP_TIMEOUT_S = 30.0
_CACHE_MAX_AGE_DAYS = 1.0

#: The titles of the people who signed the formation filing. Neither runs
#: the company; they are carried as officer-marked roles.
FORMATION_TITLES: frozenset[str] = frozenset({"organizer", "organiser", "incorporator"})

#: The statutory word: 15 Pa.C.S. § 102 defines a "governor" as a director of
#: a corporation, a manager of a manager-managed LLC (a member of a
#: member-managed one), a general partner of a partnership or a trustee of a
#: business trust.
GOVERNOR_GLOSS = (
    "a director, manager, general partner or trustee — 15 Pa.C.S. § 102"
)

COVERAGE_NOT_ON_LIST = (
    "The filing number on the LEI record is not on the Pennsylvania Department "
    "of State's list of active registrations. The list holds no dissolved, "
    "withdrawn or merged company and is refreshed monthly, so this says "
    "nothing on its own about the company's status."
)
COVERAGE_NAME_MISMATCH = (
    "The filing number on the LEI record belongs to a business with a "
    "different name on the Pennsylvania Department of State's list, so no "
    "record is attached."
)

LIST_NOTE = (
    "Listed among the Pennsylvania Department of State's active business "
    "registrations. The Department says the list shows more active businesses "
    "than currently exist, because statutory limits stop it removing some "
    "that no longer operate; OpenCheck states no status from it."
)

_BUCKETS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, TokenBucket]" = (
    weakref.WeakKeyDictionary()
)


def _bucket() -> TokenBucket:
    loop = asyncio.get_running_loop()
    bucket = _BUCKETS.get(loop)
    if bucket is None:
        bucket = TokenBucket(_RATE_PER_MINUTE, capacity=_BURST, name="pa_dos")
        _BUCKETS[loop] = bucket
    return bucket


# ---------------------------------------------------------------------------
# Numbers and fields
# ---------------------------------------------------------------------------


def normalise_filing_number(value: Any) -> str:
    """The bare filing number (``676862``) from any spelling GLEIF or the
    dataset uses (``676862``, ``0000676862``). Raises ``ValueError`` for
    anything that is not one to ten digits with a non-zero value."""
    text = "".join(str(value or "").split())
    if not re.fullmatch(r"\d{1,10}", text):
        raise ValueError(f"not a Pennsylvania filing number: {value!r}")
    bare = text.lstrip("0")
    if not bare:
        raise ValueError(f"not a Pennsylvania filing number: {value!r}")
    return bare


def padded(filing_number: str) -> str:
    """The dataset's ten-digit form."""
    return filing_number.zfill(10)


def clean_field(value: Any) -> str:
    return " ".join(str(value or "").split())


def day_of(value: Any) -> str | None:
    """``YYYY-MM-DD`` from a SODA floating timestamp; None for a date before
    1800 (the dataset dates a few records 1 Jan 1753)."""
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", clean_field(value))
    if not match:
        return None
    if int(match.group(1)) < 1800:
        return None
    return "-".join(match.groups())


def cut_from_header(value: str | None) -> str | None:
    """``YYYY-MM-DD`` from Socrata's ``Last-Modified`` header (the cut)."""
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    return parsed.date().isoformat() if parsed else None


def is_domestic(registration_type: str | None) -> bool:
    """A Pennsylvania entity: every type but the ``Foreign …`` ones."""
    return not clean_field(registration_type).lower().startswith("foreign")


def full_name(row: dict[str, Any]) -> str:
    return " ".join(
        p for p in (clean_field(row.get(k)) for k in ("first_name", "middle_name", "last_name")) if p
    )


# US legal-form words, matched as whole tokens (dots removed, so "L.L.C."
# reads "llc"). Kept here as well as through rigour, which the base install
# lacks: without it ``has_org_form_tokens`` answers False for every name.
_PA_ORG_TOKENS = frozenset({
    "llc", "inc", "incorporated", "corp", "corporation", "co", "company",
    "lp", "llp", "lllp", "ltd", "limited", "plc", "pc", "pllc", "trust",
    "partnership", "partners", "holdings", "association", "foundation",
})


def party_is_organisation(name: str) -> bool:
    """A party named with a legal form ("RiskMetrics Group Holdings, LLC").
    The dataset splits such a name across first and last name and types no
    party, so the legal-form token is the only signal."""
    if names.has_org_form_tokens(name):
        return True
    tokens = re.findall(r"[a-z0-9]+", name.casefold().replace(".", ""))
    return any(t in _PA_ORG_TOKENS for t in tokens)


def is_formation_title(title: str | None) -> bool:
    return clean_field(title).lower() in FORMATION_TITLES


def names_agree(gleif_name: str | None, registered: str | None) -> bool:
    """Does the register's name agree with GLEIF's legal name?"""
    wanted = clean_field(gleif_name)
    candidate = clean_field(registered)
    if not wanted or not candidate:
        return True
    a, b = names.org_comparable_name(wanted), names.org_comparable_name(candidate)
    if a and a == b:
        return True
    if a and names.despace(a) == names.despace(b):
        return True
    if names.distinctive_token_agreement(wanted, candidate):
        return True
    return names.name_similarity(wanted, candidate) >= names.NAME_MATCH_THRESHOLD


def summarise(bundle: dict[str, Any]) -> dict[str, Any]:
    """Everything the mapper, the finding and the hit builder read."""
    rows = [r for r in bundle.get("rows") or [] if isinstance(r, dict)]
    first = rows[0] if rows else {}
    try:
        number = normalise_filing_number(first.get("filing_number") or bundle.get("key"))
    except ValueError:
        number = clean_field(bundle.get("key"))
    registration_type = clean_field(first.get("typeofbusinessregistration"))
    line = ", ".join(
        p for p in (clean_field(first.get(k)) for k in ("address_line1", "address_line2", "city")) if p
    )
    tail = " ".join(p for p in (clean_field(first.get(k)) for k in ("state", "zip")) if p)
    address = ", ".join(p for p in (line, tail) if p)

    parties: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = full_name(row)
        if not name:
            continue
        key = names.display_name_key(name) or name.casefold()
        party = by_key.get(key)
        if party is None:
            party = {
                "name": name,
                "given": clean_field(row.get("first_name")),
                "family": clean_field(row.get("last_name")),
                "is_entity": party_is_organisation(name),
                "titles": [],
            }
            by_key[key] = party
            parties.append(party)
        title = clean_field(row.get("party_type"))
        if title and title.casefold() not in {t.casefold() for t in party["titles"]}:
            party["titles"].append(title)
    return {
        "filing_number": number,
        "name": clean_field(first.get("business_name_source_")),
        "registration_type": registration_type,
        "domestic": is_domestic(registration_type),
        "created_on": day_of(first.get("creationdate")),
        "address": address,
        "state": clean_field(first.get("state")),
        "county": clean_field(first.get("shortcountyname")),
        "parties": parties,
        "data_cut": bundle.get("data_cut"),
    }


def record_url(filing_number: str) -> str:
    """The SODA query for the business's rows (the Department's own search is
    a form, with no addressable record page)."""
    return f"{_SODA_BASE}/{DATASET}.json?" + urlencode(
        {"$select": _SELECT, "filing_number": padded(filing_number)}
    )


class PaDosAdapter(SourceAdapter):
    """Source adapter for the Pennsylvania Department of State (data.pa.gov)."""

    id = "pa_dos"

    lookup_derivers = (
        LookupDeriver(frozenset({PA_DOS_RA_CODE}), "us_pa_filing_number", normalise_filing_number),
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
            name="Pennsylvania Department of State — registered businesses",
            homepage=DATASET_PAGE,
            description=(
                "Pennsylvania business registrations from the Department of "
                "State's list of active registrations on data.pa.gov, looked up "
                "by filing number: name, type of registration, creation date, "
                "registered address, and the governors and officers listed, by "
                "name and title. The list publishes no status and no owners."
            ),
            license="PA-Open-Data",
            attribution=(
                "Data retrieved from data.pa.gov — Pennsylvania Department of "
                "State, Registered Businesses in PA Current by County."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="US",
        )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _query(
        self, params: dict[str, str]
    ) -> tuple[list[dict[str, Any]], str | None] | None:
        """``(rows, cut)`` for one SODA query, or None when it failed."""
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"Pennsylvania per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                    "Socrata requests reached; the record was not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        headers = {"Accept": "application/json"}
        token = getattr(get_settings(), "socrata_app_token", None)
        if token:
            headers["X-App-Token"] = token

        await _bucket().acquire()
        async with build_client() as client:
            response = await client.get(
                f"{_SODA_BASE}/{DATASET}.json",
                params={"$select": _SELECT, **params},
                headers=headers,
            )
        if response.status_code == 429:
            degradation.record(
                self.id, "data.pa.gov returned HTTP 429",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            degradation.record(self.id, f"data.pa.gov returned HTTP {response.status_code}")
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "data.pa.gov returned a non-JSON body")
            return None
        if not isinstance(payload, list):
            degradation.record(self.id, "data.pa.gov returned an unexpected shape")
            return None
        cut = cut_from_header(
            response.headers.get("X-SODA2-Truth-Last-Modified")
            or response.headers.get("Last-Modified")
        )
        return [r for r in payload if isinstance(r, dict)], cut

    # ------------------------------------------------------------------
    # Search — by the start of the name, or a filing number
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not self.info.live_available:
            return []
        cleaned = clean_field(query)
        if len(cleaned) < 3:
            return []
        try:
            number = normalise_filing_number(cleaned)
        except ValueError:
            number = None
        if number:
            params = {"filing_number": padded(number), "$limit": str(_ROW_LIMIT)}
        else:
            literal = cleaned.upper().replace("'", "''")
            params = {
                "$where": f"starts_with(upper(business_name_source_), '{literal}')",
                "$limit": str(_SEARCH_LIMIT * 5),
            }
        answered = await self._query(params)
        if not answered:
            return []
        rows, _cut = answered
        by_number: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            try:
                key = normalise_filing_number(row.get("filing_number"))
            except ValueError:
                continue
            by_number.setdefault(key, []).append(row)
        hits: list[SourceHit] = []
        for key, group in list(by_number.items())[:_SEARCH_LIMIT]:
            s = summarise({"rows": group, "key": key})
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=key,
                    kind=SearchKind.ENTITY,
                    name=s["name"] or key,
                    summary=f"{PA_DOS_SCHEME} {key}"
                    + (f" · {s['registration_type']}" if s["registration_type"] else ""),
                    identifiers={"us_pa_filing_number": key},
                    raw={"filing_number": key, "registration_type": s["registration_type"]},
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The Department's rows for one filing number, name-gated."""
        try:
            number = normalise_filing_number(hit_id)
        except ValueError:
            return self._bundle(key=str(hit_id or ""), legal_name=legal_name)

        cache_key = f"{_CACHE_NS}/filing/{number}"
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], dict):
            rows = cached[0].get("rows") or []
            cut = cached[0].get("data_cut")
        else:
            if not self.info.live_available:
                return self._bundle(key=number, legal_name=legal_name)
            answered = await self._query(
                {"filing_number": padded(number), "$limit": str(_ROW_LIMIT)}
            )
            if answered is None:
                return self._bundle(key=number, legal_name=legal_name)
            rows, cut = answered
            self._cache.put(cache_key, {"rows": rows, "data_cut": cut})

        if not rows:
            return self._bundle(key=number, legal_name=legal_name, data_cut=cut, not_on_list=True)
        registered = clean_field(rows[0].get("business_name_source_"))
        if legal_name and not names_agree(legal_name, registered):
            return self._bundle(
                key=number, legal_name=legal_name, data_cut=cut,
                name_mismatch=True, registered_name=registered,
            )
        return self._bundle(key=number, legal_name=legal_name, rows=rows, data_cut=cut)

    # ------------------------------------------------------------------

    def _bundle(
        self,
        *,
        key: str,
        legal_name: str,
        rows: list[dict[str, Any]] | None = None,
        data_cut: str | None = None,
        not_on_list: bool = False,
        name_mismatch: bool = False,
        registered_name: str | None = None,
    ) -> dict[str, Any]:
        has_record = bool(rows)
        answered_empty = not_on_list or name_mismatch
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "key": key,
            "legal_name": legal_name,
            "rows": [dict(r) for r in rows or []],
            "data_cut": data_cut,
            "is_stub": not has_record and not answered_empty,
        }
        if answered_empty:
            bundle["not_found"] = True
        if not_on_list:
            bundle["not_on_list"] = True
            bundle["coverage_note"] = COVERAGE_NOT_ON_LIST
        if name_mismatch:
            bundle["name_mismatch"] = True
            bundle["coverage_note"] = COVERAGE_NAME_MISMATCH
            if registered_name:
                bundle["registered_name"] = registered_name
        if has_record:
            validate_raw(self.id, PaDosBundle, bundle)
        return bundle


__all__ = [
    "COVERAGE_NAME_MISMATCH",
    "COVERAGE_NOT_ON_LIST",
    "DATASET",
    "FORMATION_TITLES",
    "GOVERNOR_GLOSS",
    "LIST_NOTE",
    "PA_DOS_RA_CODE",
    "PA_DOS_SCHEME",
    "PA_DOS_SCHEME_NAME",
    "PaDosAdapter",
    "clean_field",
    "cut_from_header",
    "day_of",
    "full_name",
    "is_domestic",
    "is_formation_title",
    "names_agree",
    "normalise_filing_number",
    "padded",
    "party_is_organisation",
    "record_url",
    "summarise",
]
