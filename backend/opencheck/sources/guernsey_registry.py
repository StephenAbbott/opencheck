"""Guernsey Registry adapter — live, keyed on the prefixed registration number.

The Guernsey Registry (the Registrar of Companies and of the other Bailiwick
registers of legal persons) runs its public search on an Angular portal whose
backend answers anonymous JSON requests at
``https://portal.guernseyregistry.com/api/``. The registry publishes no
documentation and no reuse terms for it (checked 30 Oct 2026); Stephen has
written to the Registrar, and the pull request that added this adapter is held
until the Registry answers. Everything here was read from the portal's own
bundle and measured live on 30 Oct 2026.

The calls
---------

Everything is keyed on an internal ``entityId``, not the register number, so a
lookup is a search first:

1. ``POST entity/quick-search`` ``{"searchText": "CMP48295"}`` → rows of
   ``{entityId, registeredNumber, registeredName, entityTypeDesc,
   entityStatusDesc}``. The search **prefix-matches** (``CMP2820`` also returns
   ``CMP28200``), so only an exact ``registeredNumber`` is accepted. It does
   not match former names, and it returns registered entities only — never an
   individual (checked with an official's surname).
2. ``GET entity/{id}`` — the record; ``GET entity/relationships/{id}`` — the
   current officials; ``GET entity/names/{id}`` — former names.
3. ``GET entity/{officialId}`` for each official: **200** is a Guernsey
   entity with its own register number (a trust company, a general partner);
   **400** "These information are not publicly available. Please refer to the
   Register." is an individual **or a body registered elsewhere** (iCON
   Infrastructure LLP, an English LLP, gets it too). The two are told apart by
   the name: an honorific is a person; an organisation word is a body; anything
   else is treated as a person — the private direction.

Rate limit: 60 requests a minute per client, from the API's own headers
(``X-Rate-Limit-Limit: 1m``). Paced by ``host_throttle``.

The number
----------

The registry writes the register's prefix into the number: ``CMP`` companies,
``LP`` limited partnerships, ``LLP``, ``FND`` foundations, ``CH`` charities,
``NP`` non-profit organisations. The digits alone are ambiguous — ``CMP2820``
is a wound-up company, ``LP2820`` a live partnership.

GLEIF files the **bare digits** under five Guernsey Registry RA codes, and the
RA code is wrong often: half the RA000668 (LLP register) records are companies
or partnerships, charities sit under RA000383, and Guernsey numbers are filed
under RA000777, Alderney's register. The legal-form (ELF) code is reliable. So
the candidates are: an explicit prefix as filed; else the ELF's register, the
RA's register, then the rest — and each is accepted only when the registry's
name for it agrees with GLEIF's (current or former). A wrong prefix silently
returns another company (``CMP5309`` Olympus Sportswear (Guernsey) Limited,
``LP5309`` Hedosophia Defence Technologies L.P.), so the name gate is not
optional. Measured on 190 GLEIF records (30 Oct 2026): every record under a
Guernsey Registry RA code resolved.

Alderney (RA000777, ELF ``NDC3`` / ``1Q2B`` / ``UY66``, numbers like
``C/563``) keeps its own register at the Court of Alderney, which this API does
not cover: a note card says so and nothing is sent. RA000886 (GFSC licence
numbers) and RA999999 are not dispatched at all (Stephen, 30 Oct 2026).

Individuals
-----------

**Only corporate officials are named** (Stephen, 30 Oct 2026). The registry
sells the officials list as a "Statement of the Register", although its search
API returns current officials' names to anyone. An individual official is kept
as a role and an appointment date only — name, honorific and the registry's
id for the person are dropped inside this module, before anything is cached
or returned, and ``GuernseyRegistryBundle`` refuses a bundle that carries one.

No shareholders and no beneficial owners: Guernsey's beneficial ownership
register is open to authorities only, and shareholders are not filed with the
registry.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import httpx

from .. import degradation, names
from ..cache import Cache
from ..config import get_settings
from ..host_throttle import HostRateLimitedError
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.guernsey_registry import GuernseyRegistryBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "guernsey_registry"
#: Bump when the cached shapes change.
_CACHE_VERSION = "v1"

API_BASE = "https://portal.guernseyregistry.com/api"
PORTAL_URL = "https://portal.guernseyregistry.com/"

#: BODS identifier scheme (org-id.guide ``GG-RCE``): the full prefixed number.
GG_SCHEME = "GG-RCE"
GG_SCHEME_NAME = "Guernsey Registry registration number"

# --- GLEIF Registration Authority codes (verified live, 30 Oct 2026) --------
RA_LIMITED_COMPANIES = "RA000383"
RA_FOUNDATIONS = "RA000666"
RA_LIMITED_PARTNERSHIPS = "RA000667"
RA_LLPS = "RA000668"
RA_CHARITIES_NPOS = "RA001134"
#: Alderney Court Office Company Registry — another register; dispatched only
#: because Guernsey numbers are misfiled under it (Apax, Partners Group…).
RA_ALDERNEY = "RA000777"

#: The Guernsey Registry's own registers.
GG_REGISTRY_RA_CODES: frozenset[str] = frozenset(
    {RA_LIMITED_COMPANIES, RA_FOUNDATIONS, RA_LIMITED_PARTNERSHIPS, RA_LLPS, RA_CHARITIES_NPOS}
)
#: Every RA code the lookup dispatches this adapter on.
DISPATCH_RA_CODES: frozenset[str] = GG_REGISTRY_RA_CODES | {RA_ALDERNEY}

#: The register prefixes, in the order tried when nothing says which.
PREFIXES: tuple[str, ...] = ("CMP", "LP", "LLP", "FND", "CH", "NP")

#: RA code → the prefix of the register it names. RA001134 holds two
#: registers (charities and non-profits), so it names no single prefix.
RA_PREFIX: dict[str, str] = {
    RA_LIMITED_COMPANIES: "CMP",
    RA_FOUNDATIONS: "FND",
    RA_LIMITED_PARTNERSHIPS: "LP",
    RA_LLPS: "LLP",
}

#: ISO 20275 ELF code → register prefix: every active Guernsey code in GLEIF's
#: ELF list (paged in full, 30 Oct 2026), plus two inactive ones.
ELF_PREFIX: dict[str, str] = {
    "E6J3": "CMP",  # company limited by shares
    "2AUN": "CMP",  # incorporated cell
    "QJBA": "CMP",  # incorporated cell company
    "4C0J": "CMP",  # protected cell company
    "OT3Y": "CMP",  # company limited by guarantee
    "NGR5": "CMP",  # mixed liability company
    "YVZD": "CMP",  # unlimited liability company
    "BVOE": "CMP",  # limited company (inactive code)
    "75XK": "LP",   # limited partnership without separate legal personality
    "MCQP": "LP",   # limited partnership with separate legal personality
    "UO3D": "LP",   # limited partnership (inactive code)
    "Q49K": "LLP",  # limited liability partnership
    "9XFK": "FND",  # foundation
}
#: Alderney's legal forms: the company is on the Court of Alderney's register.
ALDERNEY_ELF: frozenset[str] = frozenset({"NDC3", "1Q2B", "UY66"})

#: Most officials resolved with a registry call per record (Stephen: 10).
OFFICIALS_RESOLVE_CAP = 10
#: Most name-search rows returned.
_SEARCH_RESULT_CAP = 20

_RECORD_MAX_AGE_DAYS = 1.0
#: An official's kind (Guernsey entity, body elsewhere, individual) does not
#: change under its registry id.
_OFFICIAL_MAX_AGE_DAYS = 30.0

_LOOKUP_TIMEOUT_S = 45.0

_NUMBER_RE = re.compile(r"^(LLP|CMP|FND|LP|CH|NP)?0*(\d{1,7})$")
#: An Alderney registration as GLEIF files it ("C/563").
_ALDERNEY_NUMBER_RE = re.compile(r"^C/\d{1,7}$")

#: The registry's answer for an entity whose record it does not publish.
_NOT_PUBLIC_MARKER = "not publicly available"

_HONORIFIC_RE = re.compile(
    r"^(?:MR|MRS|MS|MISS|MX|DR|SIR|DAME|LADY|LORD|PROF|PROFESSOR|REV|REVD|THE HON|HON|ADV|ADVOCATE)\.?\s+",
    re.IGNORECASE,
)

#: Words that mark an official as a body rather than a person.
ORG_OFFICIAL_TOKENS: frozenset[str] = frozenset(
    {
        "LIMITED", "LTD", "LLP", "LP", "L.P.", "LLC", "INC", "INC.", "PLC", "CORP",
        "CORPORATION", "COMPANY", "CO.", "SA", "S.A.", "AG", "GMBH", "BV", "B.V.",
        "NV", "N.V.", "SARL", "S.A.R.L.", "TRUST", "TRUSTEES", "TRUSTEE", "NOMINEES",
        "NOMINEE", "SERVICES", "HOLDINGS", "PARTNERS", "PARTNERSHIP", "FUND",
        "FOUNDATION", "ASSOCIATES", "MANAGEMENT", "CAPITAL", "GROUP", "BANK",
        "FIDUCIARY", "DIRECTORS", "SECRETARIES", "SECRETARIAL", "ADMINISTRATION",
        "ADMINISTRATORS", "INVESTMENTS", "PCC", "ICC", "IC",
    }
)

#: Relationship types that are structure, not officials (a cell and its cell
#: company, a "Related Guernsey Entity"). Not mapped and not counted.
_STRUCTURAL_ROLE_WORDS = ("cell", "related guernsey entity")

COVERAGE_ALDERNEY = (
    "The LEI record files this company on Alderney's register (the Court of "
    "Alderney's Company Registry), which the Guernsey Registry does not hold, "
    "so no Guernsey Registry record is attached."
)
COVERAGE_NOT_IN_REGISTER = (
    "The Guernsey Registry holds no entity under the number on the LEI record, "
    "on any of its registers."
)
COVERAGE_NAME_MISMATCH = (
    "The number on the LEI record belongs to a different entity on the Guernsey "
    "Registry, so no registry record is attached."
)
COVERAGE_AMBIGUOUS = (
    "A Guernsey Registry number needs its register prefix (CMP, LP, LLP, FND, "
    "CH or NP): the digits alone name a different entity on each register."
)

#: Kinds of official in the bundle.
KIND_GG_ENTITY = "guernsey_entity"
KIND_BODY = "body"
KIND_PERSON = "person"


class _Unanswered(Exception):
    """A registry call that did not answer (a degradation is recorded)."""


def clean(value: Any) -> str:
    """A text field with whitespace collapsed."""
    return " ".join(str(value or "").split())


def iso_day(value: Any) -> str | None:
    """``YYYY-MM-DD`` from the registry's ``2008-01-02T00:00:00``, or None."""
    text = clean(value)
    return text[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", text) else None


def normalise_registry_number(value: str) -> str:
    """Canonicalise a Guernsey Registry number, or raise ``ValueError``.

    ``CMP48295`` / ``cmp 48295`` → ``CMP48295``; bare digits stay bare
    (``48295`` — GLEIF's form, resolved by ``fetch``); leading zeros go (the
    registry writes none). An Alderney number (``C/563``) is kept as filed so
    the lookup can say why it is not here.
    """
    text = "".join(str(value or "").split()).upper()
    if _ALDERNEY_NUMBER_RE.match(text):
        return text
    match = _NUMBER_RE.match(text)
    if not match:
        raise ValueError(f"not a Guernsey Registry number: {value!r}")
    prefix, digits = match.group(1) or "", match.group(2)
    return f"{prefix}{int(digits)}"


def split_number(number: str) -> tuple[str | None, str]:
    """``("CMP", "48295")`` for ``CMP48295``; ``(None, "48295")`` for bare digits."""
    match = _NUMBER_RE.match(number)
    if not match:
        return None, number
    return match.group(1), str(int(match.group(2)))


def is_alderney(number: str, *, legal_form: str = "") -> bool:
    """Is this an Alderney registration (not on the Guernsey Registry)?"""
    return bool(_ALDERNEY_NUMBER_RE.match(number)) or (
        clean(legal_form).upper() in ALDERNEY_ELF and split_number(number)[0] is None
    )


def candidate_numbers(number: str, *, registered_at: str = "", legal_form: str = "") -> list[str]:
    """The prefixed numbers to try for ``number``, most likely first.

    An explicit prefix is the only candidate. Bare digits: the ELF code's
    register, then the RA code's, then every other register.
    """
    prefix, digits = split_number(number)
    if prefix:
        return [f"{prefix}{digits}"]
    order: list[str] = []
    for p in (
        ELF_PREFIX.get(clean(legal_form).upper()),
        RA_PREFIX.get(clean(registered_at).upper()),
        *PREFIXES,
    ):
        if p and p not in order:
            order.append(p)
    return [f"{p}{digits}" for p in order]


def gleif_prefixed_number(registered_as: str, *, registered_at: str, legal_form: str) -> str | None:
    """The prefixed number GLEIF's record can be said to name, or None.

    Stephen's decision 4 (30 Oct 2026): GLEIF files bare digits; the BODS
    GLEIF statement writes ``GG-RCE`` with a prefix only where the RA code and
    the ELF code name the same register — the clean majority — and otherwise
    keeps the RA code as the scheme rather than assert a prefix GLEIF did not
    file. A prefix GLEIF did file is kept.
    """
    try:
        number = normalise_registry_number(registered_as)
    except ValueError:
        return None
    if _ALDERNEY_NUMBER_RE.match(number):
        return None
    prefix, digits = split_number(number)
    if prefix:
        return number
    ra_prefix = RA_PREFIX.get(clean(registered_at).upper())
    elf_prefix = ELF_PREFIX.get(clean(legal_form).upper())
    if ra_prefix and ra_prefix == elf_prefix:
        return f"{ra_prefix}{digits}"
    return None


def official_name_is_body(name: str) -> bool:
    """Does an official's name read as a body rather than a person?"""
    words = {w.strip(".,()&") for w in clean(name).upper().replace("/", " ").split()}
    words |= {w for w in clean(name).upper().split()}
    return bool(words & ORG_OFFICIAL_TOKENS)


def official_name_is_person(name: str) -> bool:
    """An honorific, or no organisation word: treated as an individual."""
    text = clean(name)
    if _HONORIFIC_RE.match(text):
        return True
    return not official_name_is_body(text)


def is_structural_role(role: str) -> bool:
    text = clean(role).lower()
    return any(w in text for w in _STRUCTURAL_ROLE_WORDS)


def _agree(a: str, b: str) -> bool:
    """Do two organisation names name the same entity?"""
    ca, cb = names.org_comparable_name(a), names.org_comparable_name(b)
    if ca and ca == cb:
        return True
    if ca and names.despace(ca) == names.despace(cb):
        return True
    if names.distinctive_token_agreement(a, b):
        return True
    return names.name_similarity(a, b) >= names.NAME_MATCH_THRESHOLD


def names_agree(legal_name: str | None, candidates: list[str]) -> bool:
    """Does any registry name for the entity agree with GLEIF's legal name?"""
    wanted = clean(legal_name)
    return any(c and _agree(wanted, clean(c)) for c in candidates)


def entity_summary(entity: dict[str, Any] | None) -> dict[str, Any]:
    """The record fields OpenCheck keeps, by allowlist.

    The registry's entity record also carries an e-mail address, a mobile
    number and, for a person, name parts; none of it is kept.
    """
    e = entity or {}
    address = e.get("entityRegAddress") or {}
    return {
        "number": clean(e.get("entityRegNumber")),
        "name": clean(e.get("entityRegName")),
        "registered_on": iso_day(e.get("entityRegDate")),
        "type_id": e.get("entityTypeId"),
        "type": clean(e.get("entityTypeDesc")),
        "status": clean(e.get("entityStatusDesc")),
        "status_detail": clean(e.get("entityStatusExtDesc")),
        "status_since": iso_day(e.get("entityStatusEffectiveDate")),
        "expires_on": iso_day(e.get("entityExpiryDate")),
        "annual_return_due": iso_day(e.get("annualReturnDueDate")),
        "address": {
            "text": clean(address.get("addressOneLine")),
            "postcode": clean(address.get("postcode")),
            "country": clean(address.get("country")),
        }
        if address
        else None,
    }


class GuernseyRegistryAdapter(SourceAdapter):
    """Source adapter for the Guernsey Registry's public search API."""

    id = "guernsey_registry"

    lookup_derivers = (
        LookupDeriver(DISPATCH_RA_CODES, "gg_registry_number", normalise_registry_number),
    )
    lookup_pass_legal_name = True
    lookup_pass_anchor = True
    lookup_timeout_s = _LOOKUP_TIMEOUT_S

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Guernsey Registry",
            homepage="https://www.guernseyregistry.com/",
            description=(
                "Guernsey companies, limited partnerships, LLPs, foundations, "
                "charities and non-profit organisations from the Guernsey "
                "Registry's public search: name, form, status, registration "
                "date, registered office, former names and current officials "
                "— corporate officials named, individuals counted by role."
            ),
            license="Guernsey-Registry-Terms-Unstated",
            attribution=(
                "Guernsey Registry (Registrar of Companies, States of Guernsey), "
                "public search at portal.guernseyregistry.com."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="GG",
        )

    # ------------------------------------------------------------------
    # HTTP — every outbound call funnels through here
    # ------------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, body: dict[str, Any] | None = None, what: str
    ) -> tuple[int, Any]:
        """One registry call: ``(status, json)`` for an answer.

        An answer is a 2xx, or a 400/404 carrying the registry's JSON envelope
        (the "not publicly available" refusal is an answer about the entity).
        Anything else raises :class:`_Unanswered` after recording a
        degradation, so a registry that did not answer is never read as one
        with nothing to say.
        """
        url = f"{API_BASE}/{path}"
        try:
            async with build_client() as client:
                if method == "POST":
                    response = await client.post(url, json=body or {})
                else:
                    response = await client.get(url)
        except HostRateLimitedError:
            degradation.record(
                self.id,
                f"Guernsey Registry {what}: not sent — OpenCheck's request pacing for the registry was spent",
                reason=degradation.REASON_RATE_LIMITED,
            )
            raise _Unanswered(what) from None
        except httpx.HTTPError as exc:
            kind = type(exc).__name__
            _LOG.warning("Guernsey Registry %s did not answer: %s", what, kind)
            degradation.record(
                self.id,
                f"Guernsey Registry {what} did not answer ({kind})",
                reason=degradation.reason_for_failure(kind),
            )
            raise _Unanswered(what) from None

        if response.status_code == 429:
            degradation.record(
                self.id,
                f"Guernsey Registry {what} returned HTTP 429",
                reason=degradation.REASON_RATE_LIMITED,
            )
            raise _Unanswered(what)
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if response.is_success and isinstance(payload, dict):
            return response.status_code, payload
        if response.status_code in (400, 404) and isinstance(payload, dict) and "success" in payload:
            return response.status_code, payload
        degradation.record(
            self.id,
            f"Guernsey Registry {what} returned HTTP {response.status_code}",
            reason=degradation.reason_for_failure(f"HTTP {response.status_code}"),
        )
        raise _Unanswered(what)

    async def _quick_search(self, text: str, *, what: str) -> list[dict[str, Any]]:
        status, payload = await self._call(
            "POST", "entity/quick-search", body={"searchText": text}, what=what
        )
        rows = payload.get("data") if status == 200 else None
        return [r for r in (rows or []) if isinstance(r, dict)]

    async def _exact(self, number: str) -> dict[str, Any] | None:
        """The search row whose ``registeredNumber`` is exactly ``number``.

        Cached a day — an empty answer too, which is the registry's own "no
        such number" (an answer, not a failure).
        """
        key = f"{_CACHE_NS}/search-{_CACHE_VERSION}/{number}"
        cached = self._cache.get_payload(key, max_age_days=_RECORD_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], dict):
            return cached[0].get("row")
        rows = await self._quick_search(number, what="number search")
        row = next((r for r in rows if clean(r.get("registeredNumber")).upper() == number), None)
        self._cache.put(key, {"row": row})
        return row

    # ------------------------------------------------------------------
    # Search — a name, or a prefixed number typed as the query
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        cleaned = clean(query)
        if not (2 <= len(cleaned) <= 100) or not self.info.live_available:
            return []
        try:
            number = normalise_registry_number(cleaned)
        except ValueError:
            number = ""
        try:
            if number and split_number(number)[0]:
                row = await self._exact(number)
                rows = [row] if row else []
            else:
                rows = await self._quick_search(cleaned, what="name search")
        except _Unanswered:
            return []
        hits: list[SourceHit] = []
        for row in rows:
            reg = clean(row.get("registeredNumber")).upper()
            name = clean(row.get("registeredName"))
            # Trade marks (GGGT…) share the search; only the six registers are kept.
            if not name or not _NUMBER_RE.match(reg) or not split_number(reg)[0]:
                continue
            status = clean(row.get("entityStatusDesc"))
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=reg,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"{GG_SCHEME} {reg}" + (f" · {status}" if status else ""),
                    identifiers={"gg_registry_number": reg},
                    raw={
                        "number": reg,
                        "name": name,
                        "type": clean(row.get("entityTypeDesc")),
                        "status": status,
                    },
                    is_stub=False,
                )
            )
            if len(hits) >= _SEARCH_RESULT_CAP:
                break
        return hits

    # ------------------------------------------------------------------
    # Fetch — one entity
    # ------------------------------------------------------------------

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
        anchor: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """The registry's record for a number, name-gated.

        ``anchor`` carries the GLEIF record's ``registered_at`` and
        ``legal_form`` (``lookup_pass_anchor``); without it a bare number is
        tried on every register in turn.
        """
        anchor = anchor or {}
        try:
            number = normalise_registry_number(hit_id)
        except ValueError:
            return self._bundle(str(hit_id or ""), legal_name)

        if is_alderney(number, legal_form=anchor.get("legal_form", "")):
            return self._note(number, legal_name, COVERAGE_ALDERNEY, alderney=True)

        prefix, _digits = split_number(number)
        if not prefix and not clean(legal_name):
            # Nothing to tell the registers apart by — never guessed.
            return self._note(number, legal_name, COVERAGE_AMBIGUOUS, ambiguous=True)

        candidates = candidate_numbers(
            number,
            registered_at=anchor.get("registered_at", ""),
            legal_form=anchor.get("legal_form", ""),
        )
        cached_any = any(self._cached_record(c) is not None for c in candidates)
        if not self.info.live_available and not cached_any:
            return self._bundle(number, legal_name)

        mismatch: str | None = None
        try:
            for candidate in candidates:
                cached = self._cached_record(candidate)
                if cached is not None:
                    if names_agree(legal_name, self._record_names(cached)) or not clean(legal_name):
                        return self._bundle(candidate, legal_name, record=cached, number_as_filed=number)
                    mismatch = mismatch or cached["entity"].get("name")
                    continue
                if not self.info.live_available:
                    continue
                row = await self._exact(candidate)
                if row is None:
                    continue
                registered_name = clean(row.get("registeredName"))
                former: list[dict[str, Any]] | None = None
                if clean(legal_name) and not names_agree(legal_name, [registered_name]):
                    former = await self._former_names(row["entityId"])
                    if not names_agree(legal_name, [f["name"] for f in former]):
                        _LOG.info("guernsey_registry: %s names a different entity", candidate)
                        mismatch = mismatch or registered_name
                        continue
                return await self._record(candidate, row, legal_name, number, former=former)
        except _Unanswered:
            return self._bundle(number, legal_name)

        if mismatch:
            return self._note(
                number, legal_name, COVERAGE_NAME_MISMATCH, name_mismatch=True, registered_name=mismatch
            )
        return self._note(number, legal_name, COVERAGE_NOT_IN_REGISTER, not_in_register=True)

    def _cached_record(self, number: str) -> dict[str, Any] | None:
        cached = self._cache.get_payload(
            f"{_CACHE_NS}/record-{_CACHE_VERSION}/{number}", max_age_days=_RECORD_MAX_AGE_DAYS
        )
        if cached is not None and isinstance(cached[0], dict):
            return cached[0]
        return None

    @staticmethod
    def _record_names(record: dict[str, Any]) -> list[str]:
        return [record["entity"].get("name") or ""] + [
            f.get("name") or "" for f in record.get("former_names") or []
        ]

    async def _former_names(self, entity_id: Any) -> list[dict[str, Any]]:
        status, payload = await self._call("GET", f"entity/names/{entity_id}", what="names read")
        rows = payload.get("data") if status == 200 else []
        out: list[dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict) or row.get("currentInd"):
                continue
            name = clean(row.get("name"))
            if name:
                out.append({"name": name, "since": iso_day(row.get("effectiveFrom"))})
        return out

    async def _record(
        self,
        number: str,
        row: dict[str, Any],
        legal_name: str,
        number_as_filed: str,
        *,
        former: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        """Read the record, its officials and former names; cache when whole."""
        entity_id = row["entityId"]
        entity_task = self._call("GET", f"entity/{entity_id}", what="record read")
        rel_task = self._call("GET", f"entity/relationships/{entity_id}", what="officials read")
        names_task = (
            self._former_names(entity_id) if former is None else asyncio.sleep(0, result=former)
        )
        entity_res, rel_res, names_res = await asyncio.gather(
            entity_task, rel_task, names_task, return_exceptions=True
        )
        if isinstance(entity_res, BaseException):
            if not isinstance(entity_res, _Unanswered):
                raise entity_res
            return self._bundle(number_as_filed, legal_name)
        status, entity_payload = entity_res
        entity = entity_payload.get("data") if status == 200 else None
        if not isinstance(entity, dict):
            degradation.record(self.id, "Guernsey Registry record read returned no record")
            return self._bundle(number_as_filed, legal_name)

        whole = True
        former_names: list[dict[str, Any]] = []
        names_read = True
        if isinstance(names_res, BaseException):
            if not isinstance(names_res, _Unanswered):
                raise names_res
            names_read = whole = False
        else:
            former_names = list(names_res)

        officials: list[dict[str, Any]] = []
        officials_read = True
        resolve_capped = False
        if isinstance(rel_res, BaseException):
            if not isinstance(rel_res, _Unanswered):
                raise rel_res
            officials_read = whole = False
            degradation.record(
                self.id,
                "Guernsey Registry answered for the entity but not for its officials",
                check=degradation.CHECK_SOURCE_READ,
            )
        else:
            rel_status, rel_payload = rel_res
            rows = rel_payload.get("data") if rel_status == 200 else []
            officials, resolve_capped, complete = await self._officials(rows or [])
            whole = whole and complete

        record = {
            "entity": entity_summary(entity),
            "former_names": former_names,
            "names_read": names_read,
            "officials": officials,
            "officials_read": officials_read,
            "officials_resolve_capped": resolve_capped,
        }
        if whole:
            self._cache.put(f"{_CACHE_NS}/record-{_CACHE_VERSION}/{number}", record)
        return self._bundle(number, legal_name, record=record, number_as_filed=number_as_filed)

    async def _officials(
        self, rows: list[Any]
    ) -> tuple[list[dict[str, Any]], bool, bool]:
        """Officials as kept: a body named, an individual as a role and date.

        Returns ``(officials, capped, complete)``. ``complete`` is False when
        an official's kind could not be read (not cached then).
        """
        resolved: dict[str, dict[str, Any]] = {}
        calls = 0
        capped = False
        complete = True
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            role = clean(row.get("relationshipTypeDesc"))
            if is_structural_role(role):
                continue
            display = clean(row.get("displayName"))
            official_id = str(row.get("relationshipToEntityId") or "")
            if official_id not in resolved:
                kind: dict[str, Any] | None = None
                if _HONORIFIC_RE.match(display):
                    kind = {"kind": KIND_PERSON}
                else:
                    kind = self._cached_official(official_id)
                    if kind is None and calls < OFFICIALS_RESOLVE_CAP and self.info.live_available:
                        calls += 1
                        try:
                            kind = await self._resolve_official(official_id, display)
                        except _Unanswered:
                            complete = False
                            kind = None
                    if kind is None:
                        if calls >= OFFICIALS_RESOLVE_CAP:
                            capped = True
                        kind = {
                            "kind": KIND_BODY if not official_name_is_person(display) else KIND_PERSON,
                            "resolved": False,
                        }
                resolved[official_id] = kind
            kind = resolved[official_id]
            entry: dict[str, Any] = {
                "relationship_id": str(row.get("relationshipId") or ""),
                "role": role,
                "role_id": row.get("relationshipTypeId"),
                "appointed": iso_day(row.get("effectiveDate")),
                "kind": kind["kind"],
            }
            if kind["kind"] != KIND_PERSON:
                # A body is named; the key keeps two rows of one body together.
                entry["name"] = kind.get("name") or display
                entry["official_key"] = official_id
                for field in ("number", "type", "status"):
                    if kind.get(field):
                        entry[field] = kind[field]
            if kind.get("resolved") is False:
                entry["resolved"] = False
            else:
                entry["resolved"] = True
            # An individual: role and date only — no name, no registry id.
            out.append(entry)
        if capped:
            degradation.record(
                self.id,
                f"Guernsey Registry: officials beyond the first {OFFICIALS_RESOLVE_CAP} were classified by name only",
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.REASON_TRUNCATED,
            )
        return out, capped, complete

    def _cached_official(self, official_id: str) -> dict[str, Any] | None:
        if not official_id:
            return None
        cached = self._cache.get_payload(
            f"{_CACHE_NS}/official-{_CACHE_VERSION}/{official_id}", max_age_days=_OFFICIAL_MAX_AGE_DAYS
        )
        if cached is not None and isinstance(cached[0], dict):
            return cached[0]
        return None

    async def _resolve_official(self, official_id: str, display: str) -> dict[str, Any]:
        """What an official is: a Guernsey entity (200), or — on the registry's
        "not publicly available" — a body elsewhere or an individual, by name."""
        status, payload = await self._call("GET", f"entity/{official_id}", what="official read")
        data = payload.get("data") if status == 200 else None
        if isinstance(data, dict) and not data.get("personInd") and clean(data.get("entityRegNumber")):
            kind: dict[str, Any] = {
                "kind": KIND_GG_ENTITY,
                "name": clean(data.get("entityRegName")) or display,
                "number": clean(data.get("entityRegNumber")).upper(),
                "type": clean(data.get("entityTypeDesc")),
                "status": clean(data.get("entityStatusDesc")),
            }
        elif status == 400 and _NOT_PUBLIC_MARKER in clean(payload.get("message")).lower():
            kind = {"kind": KIND_PERSON if official_name_is_person(display) else KIND_BODY}
        elif isinstance(data, dict) and data.get("personInd"):
            kind = {"kind": KIND_PERSON}
        else:
            degradation.record(
                self.id,
                f"Guernsey Registry official read returned HTTP {status} without a record",
                check=degradation.CHECK_SOURCE_READ,
            )
            raise _Unanswered("official read")
        if official_id:
            # A body's name is kept for the next record that names it; an
            # individual's entry holds only the kind.
            self._cache.put(f"{_CACHE_NS}/official-{_CACHE_VERSION}/{official_id}", kind)
        return kind

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _note(self, number: str, legal_name: str, note: str, **flags: Any) -> dict[str, Any]:
        """A note card: the registry answered, nothing is attached."""
        bundle = self._bundle(number, legal_name)
        bundle.update({"is_stub": False, "not_found": True, "coverage_note": note})
        for key, value in flags.items():
            if value is not None:
                bundle[key] = value
        return bundle

    def _bundle(
        self,
        number: str,
        legal_name: str,
        *,
        record: dict[str, Any] | None = None,
        number_as_filed: str | None = None,
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "gg_registry_number": number,
            "number_as_filed": number_as_filed or number,
            "legal_name": legal_name,
            "entity": None,
            "former_names": [],
            "names_read": False,
            "officials": [],
            "officials_read": False,
            "officials_resolve_capped": False,
            "is_stub": record is None,
        }
        if record is not None:
            bundle.update(record)
            validate_raw(self.id, GuernseyRegistryBundle, bundle)
        return bundle


__all__ = [
    "ALDERNEY_ELF",
    "API_BASE",
    "COVERAGE_ALDERNEY",
    "COVERAGE_AMBIGUOUS",
    "COVERAGE_NAME_MISMATCH",
    "COVERAGE_NOT_IN_REGISTER",
    "DISPATCH_RA_CODES",
    "ELF_PREFIX",
    "GG_REGISTRY_RA_CODES",
    "GG_SCHEME",
    "GG_SCHEME_NAME",
    "GuernseyRegistryAdapter",
    "KIND_BODY",
    "KIND_GG_ENTITY",
    "KIND_PERSON",
    "OFFICIALS_RESOLVE_CAP",
    "ORG_OFFICIAL_TOKENS",
    "PORTAL_URL",
    "PREFIXES",
    "RA_ALDERNEY",
    "RA_PREFIX",
    "candidate_numbers",
    "clean",
    "entity_summary",
    "gleif_prefixed_number",
    "is_alderney",
    "is_structural_role",
    "iso_day",
    "names_agree",
    "normalise_registry_number",
    "official_name_is_body",
    "official_name_is_person",
    "split_number",
]
