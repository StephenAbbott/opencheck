"""New York Department of State adapter — live, keyed on the DOS ID.

The New York Department of State's Division of Corporations, State Records
and Uniform Commercial Code publishes its entity database on data.ny.gov as a
family of weekly Socrata datasets, every one keyed on the **DOS ID**
(``corpid_num``). This adapter reads four of them, one SODA query each:

* ``63wc-4exh`` **All Filings** (20.96M rows, 2026-09-29) — every document
  filed: incorporation, biennial statement, amendment, merger, dissolution,
  dissolution by proclamation… with ``date_filed``, ``eff_date``, the entity
  type, the law it was filed under and the jurisdiction of incorporation.
* ``3gg2-jgnp`` **Entity Status History** — the status after each filing:
  ``Active`` / ``Inactive`` / ``Suspended`` / ``Discontinued``.
* ``ekwr-p59j`` **Name Status History** — every name the entity has held,
  with the filing that gave it.
* ``2tms-hftb`` **Address** — per filing; this adapter asks only for
  ``addr_type`` 3 (**chief executive officer**) and 4 (**principal executive
  office**), per the DOS data dictionary.

Why the All Filings family and not "Active Corporations"
--------------------------------------------------------

DOS also publishes ``n9v6-gdp6`` *Active Corporations: Beginning 1800*, a
flat monthly snapshot. It holds **active entities only**, so a company that
dissolved is simply absent — and a dissolved company still carrying a live
LEI is exactly what a check needs to see. On 2026-09-29, 14 of the 3,921
ISSUED LEIs filed under ``RA000628`` pointed at companies the snapshot had
dropped (LENTOR CAPITAL LLC, ``5810913``, filed a certificate of
dissolution-cancellation on 24 July 2026). The snapshot also has no name
history, which the name gate below needs. Decision: Stephen, 29 Sept 2026.

The flow
--------

GLEIF files New York companies under ``RA000628`` (*Corporation and Business
Entity Database*) with the bare DOS ID in ``registeredAs``: 13,442 of the
16,819 ``US-NY`` LEI records, 3,921 of the 4,848 ISSUED ones (2026-09-29),
plus 53 records in other jurisdictions — foreign companies authorised to do
business in New York, such as Quantexa Inc (``US-DE``, ``5215193``). The
deriver therefore keys on the **RA code, never the jurisdiction**. The DOS ID
is 1–9 digits, stored unpadded on both sides (``8512``), so the only
normalisation is removing whitespace.

The name gate
-------------

``fetch`` compares every name DOS has recorded for the entity — current and
former, from the name history and the entity's own filings, plus a foreign
entity's fictitious name — with the GLEIF legal name, and **drops the
record** when none agree. Measured over the 3,921 ISSUED LEIs: 3,878 agree on
the current name, 8 more only on a former name (``Maybank Kim Eng Securities
USA Inc.`` → ``MAYBANK SECURITIES USA INC.``), 23 more exist only in the All
Filings family, 4 are in no DOS dataset, and **8 are wrong numbers** —
``894500KOG527QMFEK485`` Salt City Federal
Credit Union files ``8512``, which DOS holds as THE MUNICIPAL WASTE PAPER
RECEPTACLE COMPANY. Federal credit unions appear to file their NCUA charter
number under ``RA000628``. A wrong number becomes a **note card** (the
KvK/INPI shape, Stephen 29 Sept 2026): no statements, no identifier. There is
no name fallback — DOS publishes no name search worth trusting over 4.3M
active names.

What the register says
----------------------

No beneficial ownership and no shareholders. The only person DOS publishes is
the **chief executive officer** a corporation names on its biennial statement
(LLCs and partnerships name none). The mapper carries the CEO's **name and
role only**; the CEO address is not carried (Stephen, 29 Sept 2026) — it may
be a home address and adds nothing to a screening.

DOS's own caveat: the data "is not to be construed as an official record of
the Department of State, nor is the presence of the entity in this data to be
construed as the current legal status of the entity". The finding says what
DOS *records*, never what the company *is*.

Rate limits
-----------

Socrata throttles unauthenticated requests from a shared per-IP pool and does
not throttle requests carrying an application token unless they are abusive.
``SOCRATA_APP_TOKEN`` (free, optional) is sent as ``X-App-Token`` — a header,
never a query parameter, so it cannot reach an error message through a URL.
A single-ID query answers in 0.3–0.6 s (measured 2026-09-29).

License: OPEN-NY Terms of Use (last modified 8 March 2013) — "So long as you
are not doing anything malicious with NYS data, you may use it as you wish,
subject to no other requirements." No attribution is required; it is given.
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
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.ny_dos import NyDosBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "ny_dos"

#: GLEIF Registration Authority code for the DOS Division of Corporations.
#: Verified live 2026-09-29: 13,442 of the 16,819 ``US-NY`` LEI records (and
#: 53 elsewhere) register under it, every one with a bare DOS ID.
NY_DOS_RA_CODE: str = "RA000628"

#: BODS identifier scheme for a DOS ID: the ISO 3166-2 subdivision code, the
#: scheme the GLEIF mapper already stamps on a New York entity reached through
#: GLEIF (``_US_STATE_REGISTRY_NAMES``) — the DC precedent, so the two
#: sources corroborate one another in the reconciler.
NY_DOS_SCHEME = "US-NY"
NY_DOS_SCHEME_NAME = "New York Department of State, Division of Corporations"

_SODA_BASE = "https://data.ny.gov/resource"

#: The four datasets this adapter reads, by the name the bundle uses.
DATASET_FILINGS = "63wc-4exh"
DATASET_STATUS = "3gg2-jgnp"
DATASET_NAMES = "ekwr-p59j"
DATASET_ADDRESSES = "2tms-hftb"

#: Columns asked for. Everything else in All Filings is a per-document
#: ``amd_*_flag`` this adapter does not read.
_FILINGS_SELECT = (
    "corpid_num,film_num,date_filed,eff_date,dis_eff_date,for_inc_date,"
    "documenttype,entitytype,law,corp_name,fict_name,juris,cnty_prin_ofc"
)
_STATUS_SELECT = "corpid_num,film_num,date_filed,mod_cert_code,status"
_NAMES_SELECT = "corpid_num,film_num,date_filed,name_type,name_status,corp_name"
_ADDRESS_SELECT = (
    "corpid_num,film_num,date_filed,addr_type,name,addr1,addr2,city,state,zip5,"
    "zip4,country"
)

#: The DOS data dictionary's ``addr_type`` codes.
ADDR_SERVICE_OF_PROCESS = "1"
ADDR_REGISTERED_AGENT = "2"
ADDR_CHIEF_EXECUTIVE = "3"
ADDR_PRINCIPAL_OFFICE = "4"

#: Rows per query. Corning Incorporated, a 1936 consolidation, has 91 filings;
#: 5,000 is headroom, and a response that fills it is marked truncated.
_ROW_LIMIT = 5000

#: Most SODA calls one lookup may spend. One entity costs exactly four (the
#: four datasets, asked once each and cached together); eight lets a second
#: New York entity reached through the deepen path be read too.
_LOOKUP_CALL_BUDGET = 8

#: Socrata publishes no number; an unauthenticated client shares a per-IP
#: pool. Four at once, so one entity's reads go out together.
_RATE_PER_MINUTE = 120.0
_BURST = 4

_LOOKUP_TIMEOUT_S = 30.0

#: The datasets are republished weekly.
_CACHE_MAX_AGE_DAYS = 1.0

_DOS_ID_RE = re.compile(r"^\d{1,9}$")

_NAME_SIMILARITY_THRESHOLD = 0.88

#: Status labels, verbatim.
STATUS_ACTIVE = "Active"
STATUS_INACTIVE = "Inactive"

#: Documents that form a domestic entity, or admit a foreign one.
_FORMATION_DOCUMENTS: tuple[str, ...] = (
    "CERTIFICATE OF INCORPORATION",
    "ARTICLES OF ORGANIZATION",
    "CERTIFICATE OF LIMITED PARTNERSHIP",
    "CERTIFICATE OF REGISTRATION",
    "NOTICE OF REGISTRATION",
    "CERTIFICATE OF CONSOLIDATION",
)
#: Deliberately absent: CERTIFICATE OF CONVERSION. A converted entity existed
#: before, under another form, so the conversion date is not its founding.
_AUTHORITY_DOCUMENTS: tuple[str, ...] = (
    "APPLICATION OF AUTHORITY",
    "APPLICATION FOR AUTHORITY",
)

#: Assumed-name filings are **not the entity's**. They sit in All Filings
#: under ``corpid_num`` values from a separate, sequential numbering —
#: ``293750`` MILK AND HONEY PRODUCTIONS, ``293751`` JUSTALK, ``293752``
#: JUSTALK… — that collides with DOS IDs. Corning Incorporated's ``49779``
#: carries a 1983 "ASSUMED NAME CORP INITIAL FILING" for J & M COFFEE SHOP,
#: and ``8512`` (a 1900 New Jersey company) one for TOY TOWN, marked DOMESTIC
#: (measured 2026-09-29). Every reader here drops them: they never become an
#: alternate name, never decide a name, type or jurisdiction, never pass the
#: name gate and never reach the History tab.
_ASSUMED_NAME_MARKER = "ASSUMED NAME"

#: What a filer writes in the CEO name field when there is no one to name.
_NOT_A_NAME = frozenset(
    {
        "", "VACANT", "VACANT VACANT", "NONE", "N/A", "NA", "N A", "SAME",
        "SAME AS ABOVE", "THE CORPORATION", "CORPORATION", "TBD",
    }
)

#: Titles a filer sometimes appends to the CEO's name after a comma
#: ("CHAN KENG LOKE, PRESIDENT", Maybank's 1992 statement). Only a trailing
#: comma-clause made entirely of these words is removed; "CHOI, JEONG EUM"
#: (surname first) is left alone.
_TITLE_WORDS = frozenset(
    {
        "PRESIDENT", "PRES", "CEO", "C E O", "CHAIRMAN", "CHAIR", "CHAIRPERSON",
        "CHIEF", "EXECUTIVE", "OFFICER", "AND", "&", "VICE", "MANAGING",
        "DIRECTOR", "MANAGER", "TREASURER", "SECRETARY", "PRINCIPAL", "OWNER",
    }
)

_US_STATE_CODES = frozenset(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN "
    "MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA "
    "WV WI WY".split()
)
#: US territories that are their own ISO 3166-1 country.
_US_TERRITORY_ISO = {"PR": "PR", "VI": "VI", "GU": "GU", "AS": "AS", "MP": "MP"}
_CA_PROVINCES = frozenset("AB BC MB NB NL NF NS NT NU ON PE QU QC SK SN YT".split())

#: One bucket per running event loop. The four reads of one entity go out
#: together, so the bucket's lock is contended — and an ``asyncio.Lock`` that
#: has been waited on is bound to the loop it was waited on in. A single
#: module-level bucket would then fail in any second loop (a script, the
#: test suite's ``asyncio.run`` per test) with "bound to a different event
#: loop". The other adapters' buckets are uncontended, which is why they have
#: never needed this.
_BUCKETS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, TokenBucket]" = (
    weakref.WeakKeyDictionary()
)


def _bucket() -> TokenBucket:
    loop = asyncio.get_running_loop()
    bucket = _BUCKETS.get(loop)
    if bucket is None:
        bucket = TokenBucket(_RATE_PER_MINUTE, capacity=_BURST, name="ny_dos")
        _BUCKETS[loop] = bucket
    return bucket

#: The note on a card whose DOS ID the register does not hold.
COVERAGE_NOT_IN_REGISTER = (
    "The New York Department of State's open data holds no entity with the "
    "DOS ID on the LEI record."
)
#: The note on a card whose DOS ID belongs to a different entity. Said, not
#: silently dropped (Stephen, 29 Sept 2026): a wrong registration number on an
#: LEI record is a fact about the LEI record a reader may need.
COVERAGE_NAME_MISMATCH = (
    "The DOS ID on the LEI record belongs to a different entity at the New "
    "York Department of State, so no DOS record is attached."
)


def normalise_dos_id(value: str) -> str:
    """Canonicalise a DOS ID, or raise ``ValueError``.

    Whitespace removed, nothing else: DOS stores the ID unpadded and so does
    GLEIF, so a leading zero would be a different number, not a formatting
    choice.
    """
    text = "".join(str(value or "").split())
    if not _DOS_ID_RE.match(text):
        raise ValueError(f"not a New York DOS ID: {value!r}")
    return text


def clean_field(value: Any) -> str:
    """A register text field with whitespace collapsed."""
    return " ".join(str(value or "").split())


def iso_date(value: Any) -> str | None:
    """``YYYY-MM-DD`` from a SODA floating timestamp, or None."""
    text = clean_field(value)
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        return text[:10]
    return None


def us_date(value: Any) -> str | None:
    """``YYYY-MM-DD`` from DOS's ``MM/DD/YYYY`` text dates (``for_inc_date``)."""
    match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", clean_field(value))
    if not match:
        return None
    month, day, year = (int(g) for g in match.groups())
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def is_assumed_name_filing(filing: dict[str, Any]) -> bool:
    return _ASSUMED_NAME_MARKER in clean_field(filing.get("documenttype")).upper()


def entity_filings_of(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """The entity's own filings, in filing order — assumed-name rows removed."""
    return [f for f in _ordered(bundle.get("filings")) if not is_assumed_name_filing(f)]


def ceo_name(raw: Any) -> str:
    """The CEO's name as filed, minus a trailing title, or ``""``.

    Returns ``""`` for a placeholder ("VACANT VACANT", "THE CORPORATION").
    """
    text = clean_field(raw)
    if "," in text:
        head, _, tail = text.rpartition(",")
        tail_words = re.findall(r"[A-Z&]+", tail.upper())
        if tail_words and all(w in _TITLE_WORDS for w in tail_words):
            text = head.strip()
    if text.upper().strip(" .") in _NOT_A_NAME:
        return ""
    return text


def jurisdiction_of(juris: str | None) -> tuple[str, str] | None:
    """``(country name, ISO 3166-1 code)`` for a DOS jurisdiction code.

    DOS writes a two-letter code of its own: US states, Canadian provinces and
    a handful of others (``EN``, ``EW``, ``WL``, ``QU``, ``CZ``) that are not
    ISO. Only what resolves unambiguously to a country is returned; anything
    else returns None and the mapper states the code in words instead — the
    risk engine's contract forbids a stand-in code.
    """
    code = clean_field(juris).upper()
    if code in _US_STATE_CODES:
        return ("United States", "US")
    if code in _US_TERRITORY_ISO:
        entry = pycountry.countries.get(alpha_2=_US_TERRITORY_ISO[code])
        if entry is not None:
            return (entry.name, entry.alpha_2)
    if code in _CA_PROVINCES:
        return ("Canada", "CA")
    return None


def _ordered(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Rows in filing order; the API's ``$order`` is kept, dates re-checked."""
    items = [r for r in (rows or []) if isinstance(r, dict)]
    ranked = sorted(
        enumerate(items), key=lambda pair: (clean_field(pair[1].get("date_filed")), pair[0])
    )
    return [row for _, row in ranked]


def summarise(bundle: dict[str, Any]) -> dict[str, Any]:
    """Everything the mapper, finding and hit builder read, from the raw rows.

    Kept here, next to the rows' shape, so the three readers cannot each
    decide differently which filing is "the latest" or which name is current.
    """
    filings = entity_filings_of(bundle)
    statuses = _ordered(bundle.get("status_history"))
    name_rows = _ordered(bundle.get("name_history"))
    addresses = _ordered(bundle.get("addresses"))

    by_film = {clean_field(f.get("film_num")): f for f in filings if f.get("film_num")}
    entity_filings = filings

    # --- names
    current = [r for r in name_rows if clean_field(r.get("name_status")).upper() == "A"]
    all_names: list[str] = []
    for row in name_rows:
        text = clean_field(row.get("corp_name"))
        if text and text not in all_names:
            all_names.append(text)
    if current:
        name = clean_field(current[-1].get("corp_name"))
    elif all_names:
        name = all_names[-1]
    else:
        name = next(
            (clean_field(f.get("corp_name")) for f in reversed(entity_filings) if f.get("corp_name")),
            "",
        )
    former_names = [n for n in all_names if n != name]
    # A foreign entity whose name is taken in New York files under a
    # fictitious name; that is the entity's own, unlike an assumed-name row.
    assumed: list[str] = []
    for f in filings:
        text = clean_field(f.get("fict_name"))
        if text and text != name and text not in assumed:
            assumed.append(text)

    # --- type, jurisdiction, law
    latest = entity_filings[-1] if entity_filings else {}
    entity_type = next(
        (clean_field(f.get("entitytype")) for f in reversed(entity_filings) if f.get("entitytype")),
        "",
    )
    juris = next(
        (clean_field(f.get("juris")).upper() for f in reversed(entity_filings) if f.get("juris")),
        "",
    )
    domestic = entity_type.upper().startswith("DOMESTIC") or (
        not entity_type and juris == "NY"
    )

    # --- formation / authority
    formed_on = None
    authority_on = None
    for f in entity_filings:
        doc = clean_field(f.get("documenttype")).upper()
        when = iso_date(f.get("eff_date")) or iso_date(f.get("date_filed"))
        if formed_on is None and domestic and any(doc.startswith(d) for d in _FORMATION_DOCUMENTS):
            formed_on = when
        if authority_on is None and not domestic and any(doc.startswith(d) for d in _AUTHORITY_DOCUMENTS):
            authority_on = when
    incorporated_on = None
    if not domestic:
        incorporated_on = next(
            (us_date(f.get("for_inc_date")) for f in reversed(entity_filings) if us_date(f.get("for_inc_date"))),
            None,
        )

    # --- status: the last row that carries one
    status = ""
    status_row: dict[str, Any] = {}
    for row in statuses:
        text = clean_field(row.get("status"))
        if text:
            status, status_row = text, row
    # The date the current status took effect: the first row of the run of
    # rows that ends in it.
    status_since = None
    if status:
        start = status_row
        for row in reversed(statuses):
            text = clean_field(row.get("status"))
            if not text:
                continue
            if text != status:
                break
            start = row
        film = clean_field(start.get("film_num"))
        filing = by_film.get(film) or {}
        status_since = iso_date(filing.get("eff_date")) or iso_date(start.get("date_filed"))
        status_document = clean_field(filing.get("documenttype"))
    else:
        status_document = ""

    # --- the chief executive(s) on the latest filing that names one
    ceo_filings: dict[str, list[str]] = {}
    ceo_dates: dict[str, str] = {}
    office: dict[str, Any] | None = None
    for row in addresses:
        kind = clean_field(row.get("addr_type"))
        film = clean_field(row.get("film_num")) or clean_field(row.get("date_filed"))
        if kind == ADDR_CHIEF_EXECUTIVE:
            person = ceo_name(row.get("name"))
            if person and names.org_comparable_name(person) != names.org_comparable_name(name):
                bucket = ceo_filings.setdefault(film, [])
                if person not in bucket:
                    bucket.append(person)
                ceo_dates[film] = iso_date(row.get("date_filed")) or ""
        elif kind == ADDR_PRINCIPAL_OFFICE:
            office = row
    ceos: list[str] = []
    ceos_filed_on = None
    if ceo_filings:
        last_film = list(ceo_filings)[-1]
        ceos = ceo_filings[last_film]
        ceos_filed_on = ceo_dates.get(last_film) or None

    return {
        "dos_id": clean_field(bundle.get("dos_id")),
        "name": name,
        "former_names": former_names,
        "assumed_names": assumed,
        "entity_type": entity_type,
        "juris": juris,
        "domestic": domestic,
        "law": clean_field(latest.get("law")),
        "county": clean_field(latest.get("cnty_prin_ofc")),
        "formed_on": formed_on,
        "authority_on": authority_on,
        "incorporated_on": incorporated_on,
        "status": status,
        "status_since": status_since,
        "status_document": status_document,
        "ceos": ceos,
        "ceos_filed_on": ceos_filed_on,
        "principal_office": office,
        "filing_count": len(filings),
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
    """Does any name DOS holds for the entity agree with GLEIF's?

    Current and former names from the name history, the name on every
    non-assumed filing, and the assumed names. An empty ``legal_name`` cannot
    contradict the identifier and is treated as agreeing.
    """
    wanted = clean_field(legal_name)
    if not wanted:
        return True
    candidates: list[str] = []
    for row in bundle.get("name_history") or []:
        candidates.append(clean_field(row.get("corp_name")))
    for row in entity_filings_of(bundle):
        candidates.append(clean_field(row.get("corp_name")))
        candidates.append(clean_field(row.get("fict_name")))
    return any(c and _latin_agree(wanted, c) for c in candidates)


def record_url(dos_id: str, dataset: str = DATASET_FILINGS) -> str:
    """A dereferenceable URL for the entity: the SODA query for its rows.

    DOS's own public inquiry (apps.dos.ny.gov/publicInquiry) is a form with no
    per-entity address, so the open-data query is the one link that stays
    true.
    """
    return f"{_SODA_BASE}/{dataset}.json?" + urlencode({"corpid_num": dos_id})


def address_object(row: dict[str, Any] | None, *, address_type: str) -> dict[str, Any] | None:
    """A BODS address from a DOS address row, or None."""
    if not row:
        return None
    parts = [clean_field(row.get(k)) for k in ("addr1", "addr2", "city", "state")]
    text = ", ".join(p for p in parts if p)
    if not text:
        return None
    address: dict[str, Any] = {"type": address_type, "address": text}
    zip5 = clean_field(row.get("zip5"))
    if zip5:
        zip4 = clean_field(row.get("zip4"))
        address["postCode"] = f"{zip5}-{zip4}" if zip4 else zip5
    country = clean_field(row.get("country")).upper()
    if country:
        entry = pycountry.countries.get(alpha_3=country)
        address["country"] = (
            {"name": entry.name, "code": entry.alpha_2} if entry else {"name": country}
        )
    return address


class NyDosAdapter(SourceAdapter):
    """Source adapter for the New York Department of State entity database."""

    id = "ny_dos"

    lookup_derivers = (
        LookupDeriver(frozenset({NY_DOS_RA_CODE}), "us_ny_dos_id", normalise_dos_id),
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
            name="New York Department of State — Division of Corporations",
            homepage="https://data.ny.gov/Economic-Development/Corporations-and-Other-Entities-All-Filings/63wc-4exh",
            description=(
                "New York entity records from the Department of State's "
                "Division of Corporations, looked up by DOS ID: every filing, "
                "the status after each one, every name the entity has held, "
                "and the chief executive officer a corporation names on its "
                "biennial statement."
            ),
            license="OPEN-NY-Terms",
            attribution=(
                "Corporations and Other Entities: All Filings, Entity Status "
                "History, Name Status History and Address datasets — New York "
                "State Department of State, Division of Corporations, State "
                "Records and Uniform Commercial Code, via data.ny.gov, used "
                "under the OPEN-NY Terms of Use."
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
        self, dataset: str, select: str, where: str
    ) -> list[dict[str, Any]] | None:
        """Rows for one SODA query, or None when it failed.

        Every None path records a degradation, so a register that did not
        answer is never mistaken for one that had nothing to say.
        """
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"New York DOS per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                    "Socrata requests reached; the record was not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        params = {
            "$select": select,
            "$where": where,
            "$order": "date_filed,film_num",
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
                "data.ny.gov returned HTTP 429",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            detail = ""
            try:
                body = response.json()
                if isinstance(body, dict):
                    detail = clean_field(body.get("errorCode") or "")
            except ValueError:
                pass
            _LOG.warning(
                "data.ny.gov returned %s for %s %s", response.status_code, dataset, detail
            )
            degradation.record(
                self.id,
                f"data.ny.gov returned HTTP {response.status_code}"
                + (f" ({detail})" if detail else ""),
            )
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "data.ny.gov returned a non-JSON body")
            return None
        if not isinstance(payload, list):
            degradation.record(self.id, "data.ny.gov returned an unexpected shape")
            return None
        rows = [r for r in payload if isinstance(r, dict)]
        if len(rows) >= _ROW_LIMIT:
            _LOG.info("data.ny.gov: %s filled the %s-row page", dataset, _ROW_LIMIT)
        return rows

    # ------------------------------------------------------------------
    # Search — none: the register is reached from the DOS ID GLEIF files
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — one entity by DOS ID
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The register's rows for one DOS ID, name-gated.

        The cache is keyed on the DOS ID alone and the gate runs over the
        cached rows — the Phase 228 rule: the pipeline's second pass fetches
        without a legal name, and a key that folded the name in would make it
        a second trip to the register.
        """
        try:
            dos_id = normalise_dos_id(hit_id)
        except ValueError:
            return self._bundle(dos_id=str(hit_id or ""), legal_name=legal_name)

        rows = await self._rows_for(dos_id)
        if rows is None:
            return self._bundle(dos_id=dos_id, legal_name=legal_name)
        if not entity_filings_of(rows):
            # No filing of the entity's own — nothing under this ID, or only
            # assumed-name rows from the colliding numbering (see above).
            return self._bundle(dos_id=dos_id, legal_name=legal_name, not_in_register=True)
        if not names_agree(legal_name, rows):
            _LOG.info("ny_dos: %s names a different entity", dos_id)
            registered = summarise(dict(rows, dos_id=dos_id)).get("name") or None
            return self._bundle(
                dos_id=dos_id,
                legal_name=legal_name,
                name_mismatch=True,
                registered_name=registered,
            )
        return self._bundle(dos_id=dos_id, legal_name=legal_name, **rows)

    async def _rows_for(self, dos_id: str) -> dict[str, Any] | None:
        """The four datasets' rows for a DOS ID, cached; None if unaskable.

        Cached only when all four answered (Phase 262: a failure is never
        stored as "no record"). An empty ``filings`` list is a real answer —
        DOS holds no such ID — and is cached like any other.
        """
        cache_key = f"{_CACHE_NS}/entity/{dos_id}"
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], dict):
            return cached[0]
        if not self.info.live_available:
            return None

        where = f"corpid_num='{dos_id}'"
        filings, statuses, name_rows, addresses = await asyncio.gather(
            self._query(DATASET_FILINGS, _FILINGS_SELECT, where),
            self._query(DATASET_STATUS, _STATUS_SELECT, where),
            self._query(DATASET_NAMES, _NAMES_SELECT, where),
            self._query(
                DATASET_ADDRESSES,
                _ADDRESS_SELECT,
                f"{where} AND addr_type in('{ADDR_CHIEF_EXECUTIVE}','{ADDR_PRINCIPAL_OFFICE}')",
            ),
        )
        if filings is None or statuses is None or name_rows is None or addresses is None:
            return None
        rows = {
            "filings": filings,
            "status_history": statuses,
            "name_history": name_rows,
            "addresses": addresses,
            "truncated": any(
                len(r) >= _ROW_LIMIT for r in (filings, statuses, name_rows, addresses)
            ),
        }
        self._cache.put(cache_key, rows)
        return rows

    async def fetch_timeline_data(
        self, dos_id: str, *, legal_name: str = ""
    ) -> dict[str, Any] | None:
        """The same rows, for the History tab — None when there is no record.

        One read serves both: the History tab's emitter reconstructs events
        from the rows ``fetch`` already returns (the CVR shape), so nothing
        here is a second call when the lookup has warmed the cache.
        """
        bundle = await self.fetch(dos_id, legal_name=legal_name)
        if bundle.get("is_stub") or not bundle.get("filings"):
            return None
        return bundle

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        *,
        dos_id: str,
        legal_name: str,
        filings: list[dict[str, Any]] | None = None,
        status_history: list[dict[str, Any]] | None = None,
        name_history: list[dict[str, Any]] | None = None,
        addresses: list[dict[str, Any]] | None = None,
        truncated: bool = False,
        not_in_register: bool = False,
        name_mismatch: bool = False,
        registered_name: str | None = None,
    ) -> dict[str, Any]:
        has_record = bool(filings)
        answered_empty = not_in_register or name_mismatch
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "dos_id": dos_id,
            "legal_name": legal_name,
            "filings": list(filings or []),
            "status_history": list(status_history or []),
            "name_history": list(name_history or []),
            "addresses": list(addresses or []),
            "truncated": bool(truncated),
            # A register that answered "nothing here" is not a stub: it is a
            # note card (the KvK/INPI shape), so the pipeline builds a hit for
            # it and the reader is told why no record is attached.
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
            validate_raw(self.id, NyDosBundle, bundle)
        return bundle


__all__ = [
    "ADDR_CHIEF_EXECUTIVE",
    "COVERAGE_NAME_MISMATCH",
    "COVERAGE_NOT_IN_REGISTER",
    "ADDR_PRINCIPAL_OFFICE",
    "DATASET_ADDRESSES",
    "DATASET_FILINGS",
    "DATASET_NAMES",
    "DATASET_STATUS",
    "NY_DOS_RA_CODE",
    "NY_DOS_SCHEME",
    "NY_DOS_SCHEME_NAME",
    "NyDosAdapter",
    "STATUS_ACTIVE",
    "STATUS_INACTIVE",
    "address_object",
    "ceo_name",
    "clean_field",
    "entity_filings_of",
    "is_assumed_name_filing",
    "iso_date",
    "jurisdiction_of",
    "names_agree",
    "normalise_dos_id",
    "record_url",
    "summarise",
    "us_date",
]
