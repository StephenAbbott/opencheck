"""Connecticut Secretary of the State adapter — live, keyed on the business ID.

The Connecticut Secretary of the State publishes its business registry on
data.ct.gov as a family of Socrata datasets, refreshed nightly. This adapter
reads three of them, one SODA query each:

* ``n7gp-d28j`` **Business Master** (1,307,547 rows on 12 Nov 2026) — one row
  per business: name, type, status and sub-status, citizenship (domestic or
  foreign), state or country of formation, registration and dissolution
  dates, NAICS code, the business address. Keys: ``accountnumber`` (the
  seven-digit business ID, zero-padded: ``0016116``) and ``id`` (the
  register's 18-character record id).
* ``ka36-64k6`` **Principals** (1,803,163 rows) — "an officer or director of
  a corporation, a member or manager of an LLC or a partner in a limited
  liability partnership", joined on ``business_id`` = Business Master ``id``.
  ``designation`` is ``Officer`` (169,405), ``Director`` (71,498),
  ``Officer;Director`` (51,968) or **blank** (1,510,292) — the LLC members and
  managers and the partners, and the register does not say which.
* ``enwv-52we`` **Name Change History** (96,966 rows) — old name, new name and
  the day, keyed on ``unique_key`` = Business Master ``id``.

Not read: **Agents** (``qh2m-n44y``: e-mail, phone and residence on most
rows), **Filing History** (``ah3s-bes7``, 10.1M rows — a History-tab
follow-up), the principals' residence addresses and street lines, the
business e-mail, and the self-reported owner flags (woman-, minority-,
veteran-, LGBTQI-owned).

The flow
--------

GLEIF files Connecticut businesses under ``RA000600`` with the seven-digit
business ID in ``registeredAs``: 2,585 records on 12 Nov 2026 (684 ISSUED,
1,619 LAPSED, 276 RETIRED, 6 DUPLICATE), every one zero-padded to seven
digits. 13 sit in other jurisdictions — companies registered to do business
in Connecticut (UBS AG, ``0597268``, CH) — so the deriver keys on the **RA
code, never the jurisdiction**. All 684 ISSUED records resolve.

The name gate
-------------

``fetch`` compares the GLEIF legal name with the business's current name and
every name the Name Change History records for it, and **drops the record**
when none agree. Measured over the 684 ISSUED LEIs: 682 agree on the current
name, 1 more on a recorded former name (Eder-Goodman, LLC → E-G Holdings,
LLC, 1 April 2026) and **1 differs** with no recorded change (``0637799``:
GLEIF 38 TOWN LINE ROAD LIMITED PARTNERSHIP, register RIVER RIDGE APARTMENTS
LIMITED PARTNERSHIP) — a **note card** (the NY DOS shape): no statements, no
identifier. The Name Change History records the name a business was formed
under, so unlike Florida no "renamed but unrecorded" answer is needed.

What the register says
----------------------

No beneficial ownership and no shareholders. Principals are carried by
**name and role only** — never a residence, never a street line, never
``beneficialOwnershipOrControl``, and a blank-designation principal (a member,
manager or partner) is an officer, **never an owner**: an LLC member may hold
the whole company or none of it, and the register does not say which role a
person holds.

Licence
-------

data.ct.gov marks Business Master **Public Domain**. The Principals and Name
Change History datasets, from the same publisher on the same portal, carry
no licence field at all; the bulk-data page lists all five datasets as
"Free". Connecticut's Freedom of Information Act makes the records public
(Conn. Gen. Stat. § 1-210). The ``CT-Open-Data`` licence row says all of
this. Whether to ask the Secretary of the State to confirm reuse terms for
the principals is Stephen's decision on the Notion ticket.

Rate limits
-----------

Socrata throttles unauthenticated requests from a shared per-IP pool;
``SOCRATA_APP_TOKEN`` (optional, shared with ``ny_dos`` and
``rues_colombia``) is sent as ``X-App-Token`` — a header, never a query
parameter.
"""

from __future__ import annotations

import asyncio
import logging
import re
import weakref
from typing import Any
from urllib.parse import urlencode

import pycountry

from .. import degradation, names
from ..bods.countries import country_code
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.ct_sots import CtSotsBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "ct_sots"

#: GLEIF Registration Authority code for the Connecticut Secretary of the
#: State. Verified live 2026-11-12: 2,585 LEI records register under it,
#: every one with the seven-digit business ID.
CT_SOTS_RA_CODE: str = "RA000600"

#: BODS identifier scheme for a business ID: the ISO 3166-2 subdivision code,
#: the DC / NY / FL rule — the GLEIF mapper stamps the same on RA000600.
CT_SCHEME = "US-CT"
CT_SCHEME_NAME = "Connecticut Secretary of the State, Business Registry"

_SODA_BASE = "https://data.ct.gov/resource"

DATASET_MASTER = "n7gp-d28j"
DATASET_PRINCIPALS = "ka36-64k6"
DATASET_NAME_CHANGES = "enwv-52we"

#: Columns asked for — an allowlist. Never the business e-mail, the mailing
#: or records addresses, or the self-reported owner flags.
_MASTER_SELECT = (
    "id,name,accountnumber,business_type,status,sub_status,citizenship,"
    "formation_place,state_or_territory_formation,country_formation,"
    "date_registration,began_transacting_in_ct,dissolution_date,naics_code,"
    "billingstreet,billing_unit,billingcity,billingstate,billingpostalcode,"
    "billingcountry"
)
#: Principals: name, designation and the city / state / country of the
#: business address the principal gave. Never a residence address, never a
#: street line.
_PRINCIPALS_SELECT = (
    "business_id,name__c,firstname,lastname,designation,business_city,"
    "business_state,business_country"
)
_NAME_CHANGES_SELECT = "unique_key,business_name_old,business_name_new,name_change_date"

#: Rows per query. EURPAC has eleven principals; 1,000 is headroom, and a
#: response that fills it is marked truncated.
_ROW_LIMIT = 1000

#: Three reads per business; six lets a second Connecticut business reached
#: through the deepen path be read too.
_LOOKUP_CALL_BUDGET = 6

_RATE_PER_MINUTE = 120.0
_BURST = 3
_LOOKUP_TIMEOUT_S = 30.0

#: The datasets are republished nightly.
_CACHE_MAX_AGE_DAYS = 1.0

_BUSINESS_ID_RE = re.compile(r"^\d{1,7}$")

_NAME_SIMILARITY_THRESHOLD = names.NAME_MATCH_THRESHOLD

STATUS_ACTIVE = "Active"

#: Statuses, by what they mean for a DOMESTIC business. Each ends the
#: business in Connecticut's own register: dissolved, forfeited (an LLC or LP
#: that failed to file), revoked, merged or consolidated into another, its
#: formation cancelled, or converted into another state's entity. Anything
#: else — the pending states, "Converted", "Domesticated", "Redomesticated",
#: "Recorded", a reservation — is left unclassified, never guessed.
LIVE_STATUSES: tuple[str, ...] = (STATUS_ACTIVE,)
TERMINAL_STATUSES: tuple[str, ...] = (
    "Dissolved",
    "Forfeited",
    "Revoked",
    "Merged",
    "Consolidation",
    "Cancelled",
    "Converted Out",
)
#: For a FOREIGN business these end only its registration in Connecticut —
#: the company may be alive at home — so they are never terminal.
FOREIGN_ENDED_STATUSES: tuple[str, ...] = (
    "Withdrawn",
    "Revoked",
    "Forfeited",
    "Cancelled",
    "Dissolved",
)

#: Whole-word tokens that mark a principal with no first or last name as a
#: company. 65,562 principal rows carry neither; most are companies ("USBA
#: LLC", "MIDDLEBURY REALTY CORP.") but some are people whose name the filer
#: typed only into the full-name field ("Mehran Moghadami").
_ORG_TOKENS = frozenset(
    {
        "LLC", "L.L.C.", "LLP", "L.L.P.", "LP", "L.P.", "INC", "INC.",
        "INCORPORATED", "CORP", "CORP.", "CORPORATION", "CO", "CO.", "COMPANY",
        "LTD", "LTD.", "LIMITED", "PLC", "TRUST", "PARTNERS", "PARTNERSHIP",
        "HOLDINGS", "GROUP", "ASSOCIATES", "FUND", "BANK", "FOUNDATION",
        "ASSOCIATION", "SOCIETY", "GMBH", "AG", "SA", "S.A.", "BV", "B.V.",
        "NV", "N.V.", "LLLP", "PC", "P.C.", "PLLC", "P.L.L.C.", "ENTERPRISES",
        "MANAGEMENT", "CAPITAL", "INVESTMENTS", "PROPERTIES", "REALTY",
    }
)

_US_STATE_NAMES = {
    sub.name.upper(): sub.code
    for sub in pycountry.subdivisions.get(country_code="US") or []
}

#: One bucket per running event loop (the ny_dos reasoning: the reads
#: contend for the lock, and a waited-on asyncio.Lock binds to its loop).
_BUCKETS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, TokenBucket]" = (
    weakref.WeakKeyDictionary()
)


def _bucket() -> TokenBucket:
    loop = asyncio.get_running_loop()
    bucket = _BUCKETS.get(loop)
    if bucket is None:
        bucket = TokenBucket(_RATE_PER_MINUTE, capacity=_BURST, name="ct_sots")
        _BUCKETS[loop] = bucket
    return bucket


COVERAGE_NOT_IN_REGISTER = (
    "The Connecticut Secretary of the State's open data holds no business "
    "with the business ID on the LEI record."
)
COVERAGE_NAME_MISMATCH = (
    "The business ID on the LEI record belongs to a different business at "
    "the Connecticut Secretary of the State, so no record is attached."
)


def normalise_business_id(value: str) -> str:
    """Canonicalise a Connecticut business ID, or raise ``ValueError``.

    Whitespace removed and zero-padded to seven digits — the register's
    ``accountnumber`` and GLEIF's ``registeredAs`` are both written that way
    (``0016116``), so a padded and an unpadded spelling are one number.
    """
    text = "".join(str(value or "").split())
    if not _BUSINESS_ID_RE.match(text) or not text.strip("0"):
        raise ValueError(f"not a Connecticut business ID: {value!r}")
    return text.zfill(7)


def clean_field(value: Any) -> str:
    """A register text field with whitespace collapsed."""
    return " ".join(str(value or "").split())


def iso_date(value: Any) -> str | None:
    """``YYYY-MM-DD`` from a SODA floating timestamp, or None."""
    text = clean_field(value)
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        return text[:10]
    return None


def is_domestic(master: dict[str, Any]) -> bool:
    return clean_field(master.get("citizenship")).lower() == "domestic"


def jurisdiction_of(master: dict[str, Any]) -> tuple[str, str] | None:
    """``(country name, ISO 3166-1 code)`` where the business was formed.

    A domestic business is United States. A foreign one reads
    ``country_formation`` (a country name) first, then
    ``state_or_territory_formation``, which the register fills with a US
    state's code or name ("DE", "DELAWARE") or a country's alpha-3 ("CHE").
    A two-letter value is read as a US state only — ``DE`` is Delaware here,
    never Germany. Anything else is None: the risk engine's contract forbids
    a stand-in code.
    """
    if is_domestic(master):
        return ("United States", "US")
    country = clean_field(master.get("country_formation"))
    if country:
        code = country_code(country)
        if code:
            entry = pycountry.countries.get(alpha_2=code)
            return (entry.name if entry else country, code)
    place = clean_field(master.get("state_or_territory_formation")).upper()
    if not place:
        place = clean_field(master.get("formation_place")).upper()
    if not place or place == "NONE":
        return None
    if len(place) == 2:
        if pycountry.subdivisions.get(code=f"US-{place}") is not None:
            return ("United States", "US")
        return None
    if place in _US_STATE_NAMES:
        return ("United States", "US")
    if len(place) == 3:
        entry = pycountry.countries.get(alpha_3=place)
        if entry is not None:
            return (entry.name, entry.alpha_2)
    return None


def formation_words(master: dict[str, Any]) -> str:
    """Where a foreign business was formed, as the register wrote it."""
    for key in ("country_formation", "state_or_territory_formation", "formation_place"):
        text = clean_field(master.get(key))
        if text and text.upper() != "NONE":
            return text
    return ""


def principal_is_entity(row: dict[str, Any]) -> bool:
    """Is this principal a company? Only a row with no first or last name is
    considered, and then only a name carrying a legal-form token."""
    if clean_field(row.get("firstname")) or clean_field(row.get("lastname")):
        return False
    words = {
        w.strip(",()")
        for w in clean_field(row.get("name__c")).upper().replace("/", " ").split()
    }
    return bool(words & _ORG_TOKENS)


def designations_of(value: Any) -> list[str]:
    """``"Officer;Director"`` → ``["Officer", "Director"]``; blank → ``[]``."""
    out: list[str] = []
    for part in clean_field(value).split(";"):
        text = part.strip().title()
        if text and text not in out:
            out.append(text)
    return out


def principals_of(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """The principals, one per party, in the register's order.

    The register repeats a principal on a row per address (Global Help Desk
    Services' STEVE WETHERELL appears twice, Middletown and Rocky Hill):
    rows with the same name fold into one, their designations united.
    """
    out: dict[tuple[bool, str], dict[str, Any]] = {}
    for row in bundle.get("principals") or []:
        if not isinstance(row, dict):
            continue
        name = clean_field(row.get("name__c")) or " ".join(
            p for p in (clean_field(row.get("firstname")), clean_field(row.get("lastname"))) if p
        )
        if not name:
            continue
        entity = principal_is_entity(row)
        key = (entity, names.display_name_key(name) or name.casefold())
        item = out.get(key)
        if item is None:
            item = {
                "name": name,
                "given": clean_field(row.get("firstname")),
                "family": clean_field(row.get("lastname")),
                "entity": entity,
                "designations": [],
            }
            out[key] = item
        for d in designations_of(row.get("designation")):
            if d not in item["designations"]:
                item["designations"].append(d)
    return list(out.values())


def name_changes_of(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """The recorded name changes, deduplicated, oldest first."""
    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for row in bundle.get("name_changes") or []:
        if not isinstance(row, dict):
            continue
        key = (
            clean_field(row.get("business_name_old")),
            clean_field(row.get("business_name_new")),
            iso_date(row.get("name_change_date")) or "",
        )
        if key in seen or not (key[0] or key[1]):
            continue
        seen.add(key)
        rows.append({"old": key[0], "new": key[1], "on": key[2] or None})
    rows.sort(key=lambda r: r["on"] or "")
    return rows


def summarise(bundle: dict[str, Any]) -> dict[str, Any]:
    """Everything the mapper, finding and hit builder read, in one place."""
    master = bundle.get("master") or {}
    name = clean_field(master.get("name"))
    changes = name_changes_of(bundle)
    former: list[str] = []
    for change in changes:
        for text in (change["old"], change["new"]):
            if text and text != name and text not in former:
                former.append(text)
    domestic = is_domestic(master)
    status = clean_field(master.get("status"))
    principals = principals_of(bundle)
    return {
        "business_id": clean_field(bundle.get("business_id") or master.get("accountnumber")),
        "record_id": clean_field(master.get("id")),
        "name": name,
        "former_names": former,
        "name_changes": changes,
        "business_type": clean_field(master.get("business_type")),
        "status": status,
        "sub_status": clean_field(master.get("sub_status")),
        "domestic": domestic,
        "formed_in": formation_words(master),
        "registered_on": iso_date(master.get("date_registration")),
        "began_in_ct_on": iso_date(master.get("began_transacting_in_ct")),
        "dissolved_on": iso_date(master.get("dissolution_date")),
        "naics": clean_field(master.get("naics_code")),
        "principals": principals,
        "officers": [p for p in principals if p["designations"]],
        "undesignated": [p for p in principals if not p["designations"]],
    }


def _latin_agree(a: str, b: str) -> bool:
    ca, cb = names.org_comparable_name(a), names.org_comparable_name(b)
    if ca and ca == cb:
        return True
    if ca and names.despace(ca) == names.despace(cb):
        return True
    if names.distinctive_token_agreement(a, b):
        return True
    return names.name_similarity(a, b) >= _NAME_SIMILARITY_THRESHOLD


def names_agree(legal_name: str | None, bundle: dict[str, Any]) -> bool:
    """Does the current name, or any recorded former name, agree with GLEIF's?

    An empty ``legal_name`` cannot contradict the identifier and agrees.
    """
    wanted = clean_field(legal_name)
    if not wanted:
        return True
    candidates = [clean_field((bundle.get("master") or {}).get("name"))]
    for change in name_changes_of(bundle):
        candidates.extend([change["old"], change["new"]])
    return any(c and _latin_agree(wanted, c) for c in candidates)


def record_url(business_id: str) -> str:
    """A dereferenceable URL for the business: the SODA query for its row.

    The register's own business search (service.ct.gov) has no per-business
    address, so the open-data query is the link that stays true.
    """
    return f"{_SODA_BASE}/{DATASET_MASTER}.json?" + urlencode({"accountnumber": business_id})


def principals_url(record_id: str) -> str:
    return f"{_SODA_BASE}/{DATASET_PRINCIPALS}.json?" + urlencode({"business_id": record_id})


def address_object(master: dict[str, Any]) -> dict[str, Any] | None:
    """The business address as a BODS ``business`` address, or None."""
    parts = [
        clean_field(master.get(k))
        for k in ("billingstreet", "billing_unit", "billingcity", "billingstate")
    ]
    text = ", ".join(p for p in parts if p)
    if not text:
        return None
    address: dict[str, Any] = {"type": "business", "address": text}
    post = clean_field(master.get("billingpostalcode"))
    if post:
        address["postCode"] = post
    country = clean_field(master.get("billingcountry"))
    if country:
        code = country_code(country)
        address["country"] = {"name": country, "code": code} if code else {"name": country}
    return address


class CtSotsAdapter(SourceAdapter):
    """Source adapter for the Connecticut Secretary of the State business registry."""

    id = "ct_sots"

    lookup_derivers = (
        LookupDeriver(frozenset({CT_SOTS_RA_CODE}), "us_ct_business_id", normalise_business_id),
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
            name="Connecticut Secretary of the State — Business Registry",
            homepage="https://portal.ct.gov/sots/business-services/bulk-data-and-images",
            description=(
                "Connecticut business records from the Secretary of the "
                "State's registry on data.ct.gov, looked up by business ID: "
                "name, type, status, formation, recorded name changes, and "
                "the principals — officers and directors of a corporation, "
                "members or managers of an LLC, partners of an LLP — by name "
                "and role."
            ),
            license="CT-Open-Data",
            attribution=(
                "Connecticut Business Registry — Business Master, Principals "
                "and Name Change History datasets — Connecticut Secretary of "
                "the State, via data.ct.gov."
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
        self, dataset: str, select: str, where: str, order: str
    ) -> list[dict[str, Any]] | None:
        """Rows for one SODA query, or None when it failed (a degradation)."""
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"Connecticut per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                    "Socrata requests reached; the record was not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        params = {
            "$select": select,
            "$where": where,
            "$order": order,
            "$limit": str(_ROW_LIMIT),
        }
        headers = {"Accept": "application/json"}
        token = getattr(get_settings(), "socrata_app_token", None)
        if token:
            headers["X-App-Token"] = token

        await _bucket().acquire()
        async with build_client() as client:
            response = await client.get(
                f"{_SODA_BASE}/{dataset}.json", params=params, headers=headers
            )

        if response.status_code == 429:
            degradation.record(
                self.id,
                "data.ct.gov returned HTTP 429",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            _LOG.warning("data.ct.gov returned %s for %s", response.status_code, dataset)
            degradation.record(self.id, f"data.ct.gov returned HTTP {response.status_code}")
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "data.ct.gov returned a non-JSON body")
            return None
        if not isinstance(payload, list):
            degradation.record(self.id, "data.ct.gov returned an unexpected shape")
            return None
        return [r for r in payload if isinstance(r, dict)]

    # ------------------------------------------------------------------
    # Search — none: the register is reached from the business ID GLEIF files
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — one business by business ID
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The register's rows for one business ID, name-gated.

        Cached on the business ID alone and gated over the cached rows (the
        Phase 228 rule: the pipeline's second pass fetches without a name).
        """
        try:
            business_id = normalise_business_id(hit_id)
        except ValueError:
            return self._bundle(business_id=str(hit_id or ""), legal_name=legal_name)

        rows = await self._rows_for(business_id)
        if rows is None:
            return self._bundle(business_id=business_id, legal_name=legal_name)
        if not rows.get("master"):
            return self._bundle(
                business_id=business_id, legal_name=legal_name, not_in_register=True
            )
        if not names_agree(legal_name, rows):
            _LOG.info("ct_sots: %s names a different business", business_id)
            return self._bundle(
                business_id=business_id,
                legal_name=legal_name,
                name_mismatch=True,
                registered_name=clean_field(rows["master"].get("name")) or None,
            )
        return self._bundle(business_id=business_id, legal_name=legal_name, **rows)

    async def _rows_for(self, business_id: str) -> dict[str, Any] | None:
        """The three datasets' rows for a business ID, cached; None if unaskable.

        Business Master first (its ``id`` keys the other two), then the
        principals and the name changes together. Cached only when every read
        answered (Phase 262); a Business Master that holds no such ID is a
        real answer and is cached like any other.
        """
        cache_key = f"{_CACHE_NS}/business/{business_id}"
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], dict):
            return cached[0]
        if not self.info.live_available:
            return None

        masters = await self._query(
            DATASET_MASTER, _MASTER_SELECT, f"accountnumber='{business_id}'", "accountnumber"
        )
        if masters is None:
            return None
        masters = [m for m in masters if clean_field(m.get("accountnumber")) == business_id]
        if not masters:
            rows: dict[str, Any] = {
                "master": None, "principals": [], "name_changes": [], "truncated": False,
            }
            self._cache.put(cache_key, rows)
            return rows
        master = masters[0]
        record_id = clean_field(master.get("id"))
        if not re.match(r"^[A-Za-z0-9]{15,18}$", record_id):
            degradation.record(self.id, "data.ct.gov returned a business row with no record id")
            return None
        principals, changes = await asyncio.gather(
            self._query(
                DATASET_PRINCIPALS, _PRINCIPALS_SELECT,
                f"business_id='{record_id}'", "name__c",
            ),
            self._query(
                DATASET_NAME_CHANGES, _NAME_CHANGES_SELECT,
                f"unique_key='{record_id}'", "name_change_date",
            ),
        )
        if principals is None or changes is None:
            return None
        rows = {
            "master": master,
            "principals": principals,
            "name_changes": changes,
            "truncated": len(principals) >= _ROW_LIMIT or len(changes) >= _ROW_LIMIT,
        }
        self._cache.put(cache_key, rows)
        return rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        *,
        business_id: str,
        legal_name: str,
        master: dict[str, Any] | None = None,
        principals: list[dict[str, Any]] | None = None,
        name_changes: list[dict[str, Any]] | None = None,
        truncated: bool = False,
        not_in_register: bool = False,
        name_mismatch: bool = False,
        registered_name: str | None = None,
    ) -> dict[str, Any]:
        has_record = bool(master)
        answered_empty = not_in_register or name_mismatch
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "business_id": business_id,
            "legal_name": legal_name,
            "master": dict(master) if master else None,
            "principals": list(principals or []),
            "name_changes": list(name_changes or []),
            "truncated": bool(truncated),
            "is_stub": not has_record and not answered_empty,
        }
        if not_in_register:
            bundle["not_in_register"] = True
            bundle["not_found"] = True
            bundle["coverage_note"] = COVERAGE_NOT_IN_REGISTER
        if name_mismatch:
            bundle["name_mismatch"] = True
            bundle["not_found"] = True
            bundle["coverage_note"] = COVERAGE_NAME_MISMATCH
            if registered_name:
                bundle["registered_name"] = registered_name
        if has_record:
            validate_raw(self.id, CtSotsBundle, bundle)
        return bundle


__all__ = [
    "COVERAGE_NAME_MISMATCH",
    "COVERAGE_NOT_IN_REGISTER",
    "CT_SCHEME",
    "CT_SCHEME_NAME",
    "CT_SOTS_RA_CODE",
    "CtSotsAdapter",
    "DATASET_MASTER",
    "DATASET_NAME_CHANGES",
    "DATASET_PRINCIPALS",
    "FOREIGN_ENDED_STATUSES",
    "LIVE_STATUSES",
    "STATUS_ACTIVE",
    "TERMINAL_STATUSES",
    "address_object",
    "clean_field",
    "designations_of",
    "is_domestic",
    "iso_date",
    "jurisdiction_of",
    "name_changes_of",
    "names_agree",
    "normalise_business_id",
    "principal_is_entity",
    "principals_of",
    "principals_url",
    "record_url",
    "summarise",
]
