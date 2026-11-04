"""Search filters — jurisdiction, legal form, statuses, category; nationality
and birth year for people (the advanced-search-filters ticket).

One definition for every surface: ``/search``, ``/stream`` and the MCP tool
``opencheck_search`` parse and plan through here, and the website's search box
mirrors the GLEIF half in ``frontend/src/lib/searchFilters.ts`` (parity pinned
by ``tests/test_search_filters.py``). Stephen's decisions (3 Nov 2026):

* **A source that cannot apply an active filter is left out and named**,
  never run unfiltered beside filtered rows. ``filter_plan`` decides per
  adapter; the response carries ``applied_by`` and ``left_out``.
* **The jurisdiction filter is exact.** GLEIF's ``entity.jurisdiction`` filter
  matches each part of a code separately, so ``PA`` returns Panama *and*
  Pennsylvania (``US-PA``) — 9,291 of the 15,625 records it returned on
  3 Nov 2026. For the 31 clashing country codes (``CLASHES``, generated from
  GLEIF's accepted jurisdictions) the GLEIF read scans and keeps rows whose
  jurisdiction is the country or one of its own sub-divisions.
  ``needs_exact_jurisdiction`` says when; a country-specific legal form makes
  it unnecessary, the 8888/9999 placeholders do not.
* **Legal forms are scoped to a jurisdiction**: ``forms_for`` lists the forms
  of one country (or US state / Canadian province), most used first by issued
  LEIs in the Golden Copy mirror where it is present.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

_DATA = Path(__file__).parent / "data"

# GLEIF's enum values (field metadata, LEIREC_ENTITY_STATUS / _REGISTRATION_STATUS
# / _ENTITY_CATEGORY, read 3 Nov 2026). Words are mirrored in searchFilters.ts.
ENTITY_STATUSES: dict[str, str] = {"ACTIVE": "Active", "INACTIVE": "Inactive"}
REGISTRATION_STATUSES: dict[str, str] = {
    "ISSUED": "Issued",
    "LAPSED": "Lapsed",
    "RETIRED": "Retired",
    "MERGED": "Merged",
    "ANNULLED": "Annulled",
    "DUPLICATE": "Duplicate",
    "PENDING_TRANSFER": "Pending transfer",
    "PENDING_ARCHIVAL": "Pending archival",
}
CATEGORIES: dict[str, str] = {
    "GENERAL": "General",
    "BRANCH": "Branch",
    "FUND": "Fund",
    "SOLE_PROPRIETOR": "Sole proprietor",
    "RESIDENT_GOVERNMENT_ENTITY": "Government entity",
    "INTERNATIONAL_ORGANIZATION": "International organisation",
}
#: ELF placeholders: 8888 "not yet in the list", 9999 "no ELF code". Valid
#: filter values; used everywhere, so they never make a jurisdiction exact.
PLACEHOLDER_FORMS = ("8888", "9999")
NO_ELF_CODE = "no-elf-code"

ENTITY_FILTERS = ("jurisdiction", "legal_form", "entity_status", "registration_status", "category")
PERSON_FILTERS = ("nationality", "birth_year")

#: How far the exact-jurisdiction scan reads before it stops and says so.
EXACT_SCAN_PAGE = 200
EXACT_SCAN_MAX_PAGES = 5


class FilterError(ValueError):
    """A filter value OpenCheck cannot use — said to the caller as a 400."""


@lru_cache(maxsize=1)
def _jurisdiction_data() -> dict[str, Any]:
    return json.loads((_DATA / "gleif_jurisdictions.json").read_text(encoding="utf-8"))


def jurisdictions() -> dict[str, str]:
    """GLEIF's accepted jurisdiction codes → names."""
    return {j["code"]: j["name"] for j in _jurisdiction_data()["jurisdictions"]}


def clashes() -> dict[str, list[str]]:
    """Country code → sub-divisions GLEIF's jurisdiction filter also matches."""
    return dict(_jurisdiction_data()["collisions"])


@lru_cache(maxsize=1)
def _elf_data() -> dict[str, Any]:
    return json.loads((_DATA / "elf_forms.json").read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def elf_index() -> dict[str, dict[str, Any]]:
    return {f["code"]: f for f in _elf_data()["forms"]}


def elf_version() -> str:
    return str(_elf_data().get("version") or "")


@dataclass(frozen=True)
class SearchFilters:
    jurisdiction: str | None = None
    legal_form: tuple[str, ...] = ()
    entity_status: str | None = None
    registration_status: tuple[str, ...] = ()
    category: tuple[str, ...] = ()
    nationality: str | None = None
    birth_year: int | None = None

    def active(self) -> list[str]:
        """The names of the filters that are set, in a fixed order."""
        return [name for name in ENTITY_FILTERS + PERSON_FILTERS if getattr(self, name)]

    def __bool__(self) -> bool:
        return bool(self.active())

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name in self.active():
            value = getattr(self, name)
            out[name] = list(value) if isinstance(value, tuple) else value
        return out


def _split(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    items = value.split(",") if isinstance(value, str) else list(value)
    return [i.strip() for i in items if i and i.strip()]


def parse_filters(
    *,
    jurisdiction: str | None = None,
    legal_form: str | Iterable[str] | None = None,
    entity_status: str | None = None,
    registration_status: str | Iterable[str] | None = None,
    category: str | Iterable[str] | None = None,
    nationality: str | None = None,
    birth_year: int | str | None = None,
) -> SearchFilters:
    """Validate raw filter values; raise :class:`FilterError` naming the bad one.

    ``legal_form`` takes ELF codes (``2HBR``, comma-separated for several) or
    ``no-elf-code`` for the two placeholders. An abbreviation such as "GmbH"
    is resolved by :func:`resolve_legal_form` before this, where a
    jurisdiction can disambiguate it.
    """
    jur = None
    if jurisdiction and jurisdiction.strip():
        jur = jurisdiction.strip().upper()
        if jur not in jurisdictions():
            raise FilterError(
                f"jurisdiction {jurisdiction!r} is not a jurisdiction GLEIF records — "
                "use an ISO 3166 code such as GB, DE or US-DE"
            )

    forms: list[str] = []
    for raw in _split(legal_form):
        if raw.lower() == NO_ELF_CODE:
            forms.extend(PLACEHOLDER_FORMS)
            continue
        code = raw.upper()
        if code not in elf_index() and code not in PLACEHOLDER_FORMS:
            raise FilterError(f"legal_form {raw!r} is not an active ISO 20275 ELF code")
        forms.append(code)
    if jur and forms:
        for code in forms:
            form = elf_index().get(code)
            if form and not jurisdiction_matches(_form_scope(form), jur) and not jurisdiction_matches(jur, _form_scope(form)):
                raise FilterError(
                    f"legal_form {code} is a legal form of {_form_scope(form)}, not {jur}"
                )

    status = None
    if entity_status and entity_status.strip():
        status = entity_status.strip().upper()
        if status not in ENTITY_STATUSES:
            raise FilterError(f"entity_status must be one of {', '.join(ENTITY_STATUSES)}")

    regs = [r.upper() for r in _split(registration_status)]
    for r in regs:
        if r not in REGISTRATION_STATUSES:
            raise FilterError(
                f"registration_status {r!r} must be one of {', '.join(REGISTRATION_STATUSES)}"
            )
    cats = [c.upper() for c in _split(category)]
    for c in cats:
        if c not in CATEGORIES:
            raise FilterError(f"category {c!r} must be one of {', '.join(CATEGORIES)}")

    nat = None
    if nationality and nationality.strip():
        nat = nationality.strip().upper()
        if not re.fullmatch(r"[A-Z]{2}", nat) or nat not in jurisdictions():
            raise FilterError("nationality must be an ISO 3166-1 alpha-2 country code such as GB")

    year = None
    if birth_year not in (None, ""):
        try:
            year = int(str(birth_year).strip())
        except ValueError as exc:
            raise FilterError("birth_year must be a four-digit year") from exc
        if not 1850 <= year <= date.today().year:
            raise FilterError("birth_year must be a four-digit year")

    return SearchFilters(
        jurisdiction=jur,
        legal_form=tuple(dict.fromkeys(forms)),
        entity_status=status,
        registration_status=tuple(dict.fromkeys(regs)),
        category=tuple(dict.fromkeys(cats)),
        nationality=nat,
        birth_year=year,
    )


# ---------------------------------------------------------------------------
# Jurisdictions
# ---------------------------------------------------------------------------


def jurisdiction_matches(value: str | None, wanted: str) -> bool:
    """Whether a record's jurisdiction is ``wanted`` or one of its sub-divisions.

    ``US-DE`` matches ``US`` and ``US-DE``; it never matches ``DE``.
    """
    if not value:
        return False
    value = value.upper()
    wanted = wanted.upper()
    return value == wanted or value.startswith(wanted + "-")


def needs_exact_jurisdiction(f: SearchFilters) -> bool:
    """Whether GLEIF's jurisdiction filter would return records from elsewhere."""
    if not f.jurisdiction or f.jurisdiction not in clashes():
        return False
    # A real legal-form code belongs to one country, so it already excludes
    # the clashing state's records. The placeholders are used everywhere.
    return not f.legal_form or any(code in PLACEHOLDER_FORMS for code in f.legal_form)


def gleif_filter_params(f: SearchFilters) -> list[tuple[str, str]]:
    """The GLEIF ``lei-records`` filter parameters for the entity filters."""
    params: list[tuple[str, str]] = []
    if f.jurisdiction:
        params.append(("filter[entity.jurisdiction]", f.jurisdiction))
    if f.legal_form:
        # ``entity.legalForm`` — ``entity.legalForm.id`` is refused (HTTP 400).
        params.append(("filter[entity.legalForm]", ",".join(f.legal_form)))
    if f.entity_status:
        params.append(("filter[entity.status]", f.entity_status))
    if f.registration_status:
        params.append(("filter[registration.status]", ",".join(f.registration_status)))
    if f.category:
        params.append(("filter[entity.category]", ",".join(f.category)))
    return params


def record_matches(record: dict[str, Any], f: SearchFilters) -> bool:
    """Whether a GLEIF ``lei-records`` item satisfies the filters exactly.

    Used after an exact-jurisdiction scan, and as a guard on every filtered
    GLEIF result so a row that slipped past an upstream filter never reaches
    the reader.
    """
    attrs = record.get("attributes") or {}
    entity = attrs.get("entity") or {}
    reg = attrs.get("registration") or {}
    if f.jurisdiction and not jurisdiction_matches(entity.get("jurisdiction"), f.jurisdiction):
        return False
    if f.legal_form and ((entity.get("legalForm") or {}).get("id") or "").upper() not in f.legal_form:
        return False
    if f.entity_status and (entity.get("status") or "").upper() != f.entity_status:
        return False
    if f.registration_status and (reg.get("status") or "").upper() not in f.registration_status:
        return False
    if f.category and (entity.get("category") or "").upper() not in f.category:
        return False
    return True


# ---------------------------------------------------------------------------
# Legal forms
# ---------------------------------------------------------------------------


def _form_scope(form: dict[str, Any]) -> str:
    return form.get("subdivision") or form["country"]


def display_name(form: dict[str, Any]) -> str:
    """English where ISO 20275 publishes it, else the first local name."""
    for n in form["names"]:
        if n["lang"] == "en":
            return n["local"]
    first = form["names"][0]
    return first.get("translit") or first["local"]


def display_abbreviation(form: dict[str, Any]) -> str | None:
    """The abbreviation a reader recognises, or None.

    The code list puts several in one cell and not always the plain one
    first (``63KS``: "GmbH UG;UG (haftungsbeschränkt)"; ``8Z6G`` lists
    "GmbH & Co. KG" after "KG"). Rule: drop a parenthesised tail, prefer the
    first that starts with the displayed name's first letter, else the
    shortest — so the UK's Private Limited Company reads "Ltd", not the
    Welsh "cyf".
    """
    abbrs = [re.sub(r"\s*\(.*?\)\s*", "", a).strip() for a in form.get("abbreviations") or []]
    abbrs = [a for a in abbrs if a]
    if not abbrs:
        return None
    # Some cells hold the spelled-out form beside the abbreviation
    # ("limited liability partnership;LLP"); an abbreviation is short.
    short = [a for a in abbrs if len(a) <= 12]
    abbrs = short or abbrs
    first = display_name(form)[:1].lower()
    for a in abbrs:
        if a[:1].lower() == first:
            return a
    return min(abbrs, key=len)


def _counts_key() -> tuple[str, str] | None:
    from . import entity_pages

    store = entity_pages.get_store()
    if store is None:
        return None
    meta = store.meta()
    return (str(store.path), str(meta.get("refreshed_at") or meta.get("built_at") or ""))


_counts_lock = threading.Lock()
_counts_cache: dict[tuple[str, str], dict[str, int]] = {}


def legal_form_counts() -> dict[str, int] | None:
    """Issued LEIs per ELF code in the Golden Copy mirror, or None without one.

    One scan of the mirror (a few seconds on 3.4M rows), on its own
    read-only connection so the page reads keep theirs, cached until the
    mirror is refreshed. Call it off the event loop.
    """
    key = _counts_key()
    if key is None:
        return None
    with _counts_lock:
        if key in _counts_cache:
            return _counts_cache[key]
        try:
            conn = sqlite3.connect(f"file:{key[0]}?mode=ro", uri=True, timeout=10.0)
            try:
                rows = conn.execute(
                    "SELECT upper(legal_form), count(*) FROM entities "
                    "WHERE registration_status = 'ISSUED' AND legal_form IS NOT NULL "
                    "GROUP BY upper(legal_form)"
                ).fetchall()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            log.warning("legal-form counts unavailable: %s", exc)
            return None
        _counts_cache.clear()
        _counts_cache[key] = {code: int(n) for code, n in rows if code}
        return _counts_cache[key]


def forms_for(jurisdiction: str, counts: dict[str, int] | None = None) -> dict[str, Any]:
    """The legal forms of one jurisdiction, most used first where counts exist.

    A country's list holds its national forms (no sub-division); a US state or
    Canadian province's list holds its own forms and the national ones.
    ``has_subdivision_forms`` says a country's list is incomplete until a
    state or province is chosen.
    """
    jur = jurisdiction.strip().upper()
    if jur not in jurisdictions():
        raise FilterError(f"jurisdiction {jurisdiction!r} is not a jurisdiction GLEIF records")
    country = jur.split("-", 1)[0]
    forms: list[dict[str, Any]] = []
    has_sub = False
    for form in _elf_data()["forms"]:
        if form["country"] != country:
            continue
        sub = form.get("subdivision")
        if sub and sub != jur:
            has_sub = has_sub or jur == country
            continue
        forms.append(
            {
                "code": form["code"],
                "name": display_name(form),
                "abbreviation": display_abbreviation(form),
                "abbreviations": form.get("abbreviations") or [],
                "names": [n.get("translit") or n["local"] for n in form["names"]]
                + [n["local"] for n in form["names"] if n.get("translit")],
                "subdivision": sub,
                "issued_leis": None if counts is None else counts.get(form["code"], 0),
            }
        )
    if counts is not None:
        forms.sort(key=lambda f: (-(f["issued_leis"] or 0), f["name"].lower()))
    else:
        forms.sort(key=lambda f: ((f["abbreviation"] or f["name"]).lower()))
    return {
        "jurisdiction": jur,
        "jurisdiction_name": jurisdictions().get(jur, jur),
        "forms": forms,
        "has_subdivision_forms": has_sub,
        "counts_available": counts is not None,
        "elf_version": elf_version(),
    }


def resolve_legal_form(value: str, jurisdiction: str | None) -> tuple[list[str], list[dict[str, Any]]]:
    """Codes for a legal-form value given as a code, an abbreviation or a name.

    Returns ``(codes, candidates)``: codes when the value is unambiguous,
    otherwise no codes and the candidates (code, country, name, abbreviation)
    so the caller can choose. A code passes straight through.
    """
    raw = value.strip()
    if not raw:
        return [], []
    parts = _split(raw)
    if all(p.upper() in elf_index() or p.upper() in PLACEHOLDER_FORMS or p.lower() == NO_ELF_CODE for p in parts):
        return parts, []
    needle = _fold(raw)
    jur = jurisdiction.strip().upper() if jurisdiction else None
    matches: list[dict[str, Any]] = []
    for form in _elf_data()["forms"]:
        if jur and not (
            jurisdiction_matches(_form_scope(form), jur) or jurisdiction_matches(jur, _form_scope(form))
        ):
            continue
        texts = form.get("abbreviations", []) + [n["local"] for n in form["names"]] + [
            n["translit"] for n in form["names"] if n.get("translit")
        ]
        if any(_fold(t) == needle for t in texts):
            matches.append(form)
    candidates = [
        {
            "code": f["code"],
            "jurisdiction": _form_scope(f),
            "name": display_name(f),
            "abbreviation": display_abbreviation(f),
        }
        for f in matches
    ]
    if len(matches) == 1:
        return [matches[0]["code"]], candidates
    return [], candidates


def _fold(text: str) -> str:
    return re.sub(r"[\s.]+", "", text).casefold()


# ---------------------------------------------------------------------------
# Which source can apply what
# ---------------------------------------------------------------------------


@dataclass
class FilterPlan:
    run: bool
    filtered: bool
    reason: str | None = None
    unsupported: list[str] = field(default_factory=list)


def filter_plan(adapter: Any, kind_value: str, f: SearchFilters) -> FilterPlan:
    """Whether one adapter runs, and whether it applies the filters itself.

    * no filter for this kind of search → run as before;
    * every active filter in the adapter's ``search_filters`` → run filtered;
    * a national register asked only for a jurisdiction → run plain when its
      register covers that jurisdiction (every Companies House record is
      ``GB``), leave out when it does not;
    * otherwise → leave out, naming the filters it cannot apply.
    """
    relevant = PERSON_FILTERS if kind_value == "person" else ENTITY_FILTERS
    wanted = [name for name in f.active() if name in relevant]
    if not wanted:
        return FilterPlan(run=True, filtered=False)
    supported = set(getattr(adapter, "search_filters", frozenset()))
    missing = [name for name in wanted if name not in supported]
    if not missing:
        return FilterPlan(run=True, filtered=True)
    country = getattr(getattr(adapter, "info", None), "country", None)
    if (
        "jurisdiction" in missing
        and country
        and f.jurisdiction
        and not jurisdiction_matches(country, f.jurisdiction)
    ):
        # A register of another country: whatever else is asked, it has
        # nothing in this jurisdiction. Said first, as the plainer reason.
        return FilterPlan(run=False, filtered=False, reason="outside_jurisdiction", unsupported=["jurisdiction"])
    if "jurisdiction" in missing and country and f.jurisdiction:
        # The register covers the jurisdiction: every record it holds is in it.
        missing = [n for n in missing if n != "jurisdiction"]
        if not missing:
            filtered = [n for n in wanted if n != "jurisdiction"]
            return FilterPlan(run=True, filtered=bool(filtered))
    return FilterPlan(run=False, filtered=False, reason="cannot_apply", unsupported=missing)


# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------


def birth_year_matches(dates: Iterable[str], year: int) -> bool:
    """A birth date (any precision) in ``year``. No date is not a match."""
    return any(str(d)[:4] == str(year) for d in dates if d)


def nationality_matches(codes: Iterable[str], wanted: str) -> bool:
    wanted = wanted.upper()
    return any((c or "").strip().upper()[:2] == wanted for c in codes)
