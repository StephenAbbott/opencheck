"""New Zealand director / shareholder associations enrichment (lazy, on demand).

Shown on the BackgroundCheck tab, one person at a time
(:func:`person_associations`, ``GET /nz-associations/person``), beside the
Estonian register's "companies on the register" control — not on QuickCheck,
where it sat as a company-wide panel until the transfer ticket (19 Oct 2026).
The company-wide view (:func:`assemble_associations`, ``GET /nz-associations``)
stays in the documented API.

For each director and shareholder of the looked-up NZ company, this searches the
Companies Entity Role Search API (v3) by name and reports how many **other**
companies that person / organisation is linked to — a customer-due-diligence
indicator for nominees and mass directorships.

The Role Search API is keyed on a **name string** (there is no stable person
id), so this is fundamentally a matching problem. **Every name match counts**;
address is used to *grade* confidence, not to gate the result:

* **high**   — same PAF delivery-point id (exact registered address).
* **medium** — same / strongly-overlapping address lines.
* **low**    — name matches but no address corroboration ("same name — may be a
  different person").

Earlier versions hid the **low** band entirely, which made the panel empty for
career directors (who file different addresses across boards) — i.e. the exact
people worth surfacing. Now name-only matches are shown and counted, clearly
labelled, with the per-name register total as a common-name warning. This never
asserts that a person *is* a nominee — it reports what appears under a name in
the public register, for analyst review. Lazy and never on the main lookup.
Separate subscription key: ``NZBN_ROLE_SEARCH_API_KEY``.

**Alternative addresses (Companies (Address Information) Amendment Act 2025,
in force 18 Nov 2026.)** A director — and a shareholder who is that director or
lives with them — may show an *alternative* physical address on the public
register instead of their residential one. The NZBN side says which kind it is
(``address_type``: ``PHYSICAL`` / ``ALTERNATIVE`` / None). Entity Role Search
does **not**: for a role holder with an alternative address it returns an
empty ``physicalAddress`` (``addressLines: []``, no ``pafId``, no postcode),
and it carries no ``addressType`` field for anyone (MBIE API team, Case
00094468, 15 and 27 Oct 2026; MBIE's release notice of 27 Oct 2026, checked
in the sandbox the same day). Until 27 Oct the sandbox served the residential
address there instead, which MBIE confirmed was unintended.

So a block with no address line and no ``pafId`` is *withheld*, never
compared (a postcode alone is not matched on). For the one-person view the address is
then read from the company's own NZBN record — ``associatedCompanyNzbn`` on the
record or, for a shareholding, on the shareholding — through the
``nz_companies`` adapter's fetch and cache, for at most
``_FOLLOWUP_CAP`` companies per person; the rest are counted as not checked.
The company-wide view (up to 60 names) makes no follow-up calls.

A match where **either** side's address is ``ALTERNATIVE`` is graded at most
*medium* (Stephen, 27 Oct 2026): an alternative address is typically the
office of the accountant or agent who provides it, shared by every one of
their clients, so it corroborates a service provider, not an identity. See
``docs/nz-associations.md``.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import quote

from . import names
from .cache import Cache
from .config import get_settings
from .http import build_client
from .secret_scrub import describe_exception
from .sources import REGISTRY

_LOG = logging.getLogger(__name__)

_ROLE_SEARCH_URL = (
    "https://api.business.govt.nz/gateway/companies-office/"
    "companies-register/entity-roles/v3/search"
)
_CACHE_NS = "nz_associations"

_PAGE_SIZE = 50
_PAGE_CAP = 3              # ≤ 150 role records *fetched* per name (see totalResults)
_MAX_ROLE_HOLDERS = 60     # safety ceiling on searches per subject company
_CONCURRENCY = 5           # parallel Role Search / NZBN calls (rate-limit friendly)
_FOLLOWUP_CAP = 25         # NZBN reads per person for withheld Role Search addresses

_CONFIDENCE_BASIS = {
    "high": "Same registered address",
    "medium": "Overlapping address",
    "low": "Same name — may differ",
}

# Companies (Address Information) Amendment Act 2025 — in force 18 Nov 2026.
# Where the subject's own filed address is an **alternative** address, a shared
# address is much weaker evidence than it was: an alternative address is
# typically the office of the accountant or agent who provides it, shared by
# every one of their clients. The tier is unchanged (the counts and ordering
# the panel is built on keep their meaning), but it must not be *described* as
# a shared residence — "Same registered address" would read as corroboration
# of identity when it is corroboration of a service provider.
_ALTERNATIVE_BASIS = {
    "high": "Same alternative address — may be a shared service address",
    "medium": "Overlapping alternative address",
}

# Where the address a company was graded on came from.
#   role_search    — Role Search's own physicalAddress
#   company_record — Role Search withheld it; read from the company's NZBN record
#   not_checked    — withheld, and past the per-person cap (or the company-wide view)
#   check_failed   — withheld, and the company's NZBN record could not be read
_NOT_CHECKED_BASIS = "Same name — address not published by Role Search, not checked"
_CHECK_FAILED_BASIS = "Same name — the company's own record could not be read"
_COMPANY_RECORD_SUFFIX = " (read from the company's own record)"

# Strongest → weakest, for keeping the best confidence per company and ordering.
_CONF_ORDER = {"high": 0, "medium": 1, "low": 2}

_cache = Cache()


# --------------------------------------------------------------------------- #
# Matching helpers
# --------------------------------------------------------------------------- #

def _norm_name(s: str | None) -> str:
    """Shared comparable form (Phase B) — see ``opencheck/names.py``. Both
    sides of the exact-name key go through the same function, and the old
    lowercase+split had no diacritic handling at all."""
    return names.normalise_name(s or "")


def _norm_addr(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _tokens(s: str | None) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))


def _paf_of(a: Any) -> str | None:
    if not isinstance(a, dict):
        return None
    return str(a.get("pafId") or "").strip() or None


def _phys_addr_str(a: Any) -> str | None:
    """Join a Role Search ``physicalAddress`` block into a comparable line."""
    if not isinstance(a, dict):
        return None
    parts = [str(x).strip() for x in (a.get("addressLines") or []) if x and str(x).strip()]
    if a.get("postCode"):
        parts.append(str(a["postCode"]).strip())
    return ", ".join(parts) or None


def _address_withheld(a: Any) -> bool:
    """Role Search's block for a role holder with an alternative address:
    ``{"addressLines": []}`` — no lines, no ``pafId``, no postcode (MBIE,
    27 Oct 2026; sandbox the same day). A block with neither an address line
    nor a ``pafId`` is withheld: a postcode alone is not matched on.
    """
    if not isinstance(a, dict):
        return False
    if _paf_of(a):
        return False
    return not any(x and str(x).strip() for x in (a.get("addressLines") or []))


def _is_alternative(address_type: str | None) -> bool:
    return (address_type or "").strip().upper() == "ALTERNATIVE"


def _grade(
    rec_paf: str | None, rec_addr: str | None, rec_type: str | None,
    subj_paf: str | None, subj_addr: str | None, subj_type: str | None,
) -> tuple[str, str]:
    """``(tier, basis)`` for one company match.

    Residential (or untyped) on both sides keeps the original grading. Where
    either side is ``ALTERNATIVE`` the match is at most *medium* and is worded
    as a possibly shared service address (Stephen, 27 Oct 2026).
    """
    tier = _tier(rec_paf, rec_addr, subj_paf, subj_addr)
    if tier == "low":
        return tier, _CONFIDENCE_BASIS["low"]
    if not (_is_alternative(subj_type) or _is_alternative(rec_type)):
        return tier, _CONFIDENCE_BASIS[tier]
    # The wording follows the evidence (a shared pafId reads "same", an address
    # text match "overlapping"), the tier is capped.
    return "medium", _ALTERNATIVE_BASIS[tier]


def _tier(rec_paf: str | None, rec_addr: str | None,
          subj_paf: str | None, subj_addr: str | None) -> str:
    """Confidence that a Role Search record is the *same* person as the subject."""
    if subj_paf and rec_paf and str(subj_paf) == str(rec_paf):
        return "high"
    if subj_addr and rec_addr:
        if _norm_addr(subj_addr) == _norm_addr(rec_addr):
            return "medium"
        a, b = _tokens(subj_addr), _tokens(rec_addr)
        if a and b and len(a & b) / len(a | b) >= 0.6:
            return "medium"
    return "low"


def _basis(tier: str, subj_address_type: str | None = None) -> str:
    """Plain-English basis for a tier, honest about an alternative address.

    ``subj_address_type`` is the NZBN ``addressType`` of the *subject* role
    holder's address (``PHYSICAL`` / ``ALTERNATIVE`` / None). Only the NZBN
    side carries it — the Role Search ``physicalAddress`` has no
    ``addressType`` field at all (checked live, 1 Oct 2026) — so this grades
    the wording on what the subject filed, which is what the match is against.
    """
    if (subj_address_type or "").strip().upper() == "ALTERNATIVE":
        alt = _ALTERNATIVE_BASIS.get(tier)
        if alt:
            return alt
    return _CONFIDENCE_BASIS[tier]


def _entity_link(nzbn: str | None) -> str | None:
    return f"https://www.nzbn.govt.nz/mynzbn/nzbndetails/{nzbn}/" if nzbn else None


# The live Role Search response shape is not pinned by a public schema, so the
# record array and total can arrive under a few different keys depending on the
# API version. Extract tolerantly rather than assuming one shape (and never
# raise — this is a panel-only enrichment that must degrade, not 500).
_RECORD_KEYS = ("roles", "items", "results", "entityRoles", "entityRoleSearchResults", "entities")
_TOTAL_KEYS = ("totalResults", "total", "totalRecords", "totalItems", "numFound")


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    for k in _RECORD_KEYS:
        v = payload.get(k)
        if isinstance(v, list):
            return [r for r in v if isinstance(r, dict)]
    # Fall back to the first list-of-dicts value anywhere in the payload.
    for v in payload.values():
        if isinstance(v, list) and any(isinstance(x, dict) for x in v):
            return [r for r in v if isinstance(r, dict)]
    return []


def _extract_total(payload: Any, fallback: int) -> int:
    if isinstance(payload, dict):
        for k in _TOTAL_KEYS:
            v = payload.get(k)
            if v is not None:
                try:
                    return int(v)
                except (TypeError, ValueError):
                    continue
    return fallback


def _to_pct(v: Any) -> float | None:
    """Coerce a share-percentage of any shape (number, "50", "50.0", "50%")."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().rstrip("%").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _record_companies(rec: dict[str, Any]) -> list[dict[str, Any]]:
    """The companies a single RoleInEntity record references (director + shares)."""
    out: list[dict[str, Any]] = []
    rtype = str(rec.get("roleType") or "")
    cnum = str(rec.get("associatedCompanyNumber") or "").strip()
    # Director company — skip ceased directorships (a resignation date is set).
    if cnum and "Director" in rtype and not rec.get("resignationDate"):
        out.append({
            "number": cnum,
            "name": str(rec.get("associatedCompanyName") or "").strip() or None,
            "nzbn": str(rec.get("associatedCompanyNzbn") or "").strip() or None,
            "role": "director", "share_percentage": None,
        })
    for sh in rec.get("shareholdings") or []:
        if not isinstance(sh, dict):
            continue
        scnum = str(sh.get("associatedCompanyNumber") or "").strip()
        if not scnum:
            continue
        out.append({
            "number": scnum,
            "name": str(sh.get("associatedCompanyName") or "").strip() or None,
            "nzbn": str(sh.get("associatedCompanyNzbn") or "").strip() or None,
            "role": "shareholder", "share_percentage": _to_pct(sh.get("sharePercentage")),
        })
    return out


# --------------------------------------------------------------------------- #
# Role holders of the subject company
# --------------------------------------------------------------------------- #

def _collect_role_holders(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """Unique directors + shareholders of the subject, with name + address.

    Each holder carries a ``search_name`` — the Companies Office recommended
    "LastName FirstName MiddleName" order for the Role Search API (falling back
    to the display name, e.g. organisation shareholders, which match by fuzzy
    name regardless of order).
    """
    by_name: dict[str, dict[str, Any]] = {}

    def add(
        name: str | None, paf: str | None, addr: str | None, role_here: str,
        search_name: str | None = None, addr_type: str | None = None,
    ) -> None:
        name = (name or "").strip()
        if not name:
            return
        key = _norm_name(name)
        rh = by_name.get(key)
        if rh is None:
            rh = {
                "name": name, "search_name": (search_name or "").strip() or name,
                "paf_id": paf, "address": addr, "address_type": addr_type,
                "roles_here": set(),
            }
            by_name[key] = rh
        rh["roles_here"].add(role_here)
        if not rh.get("paf_id") and paf:
            rh["paf_id"] = paf
        if not rh.get("address") and addr:
            rh["address"] = addr
        if not rh.get("address_type") and addr_type:
            rh["address_type"] = addr_type

    for r in bundle.get("roles") or []:
        add(r.get("name"), r.get("paf_id"), r.get("address"), "director",
            r.get("search_name"), r.get("address_type"))
    for s in bundle.get("shareholders") or []:
        add(s.get("name"), s.get("paf_id"), s.get("address"), "shareholder",
            s.get("search_name"), s.get("address_type"))
    return list(by_name.values())


# --------------------------------------------------------------------------- #
# Role Search API
# --------------------------------------------------------------------------- #

async def _role_search(client, name: str, key: str) -> tuple[list[dict[str, Any]], int]:
    """Return ``(records, total_results)`` for a name (registered companies only).

    ``records`` is capped at ``_PAGE_CAP * _PAGE_SIZE``; ``total_results`` is the
    API's reported total so a prolific name isn't silently undercounted.

    ``role-type=ALL`` is **required** by the API (one of ``SHR`` / ``DIR`` /
    ``ALL`` must be sent); omitting it returns no usable result. ``ALL`` is right
    here because we want every company a name is linked to, by any role.
    """
    records: list[dict[str, Any]] = []
    total = 0
    for page in range(_PAGE_CAP):
        url = (
            f"{_ROLE_SEARCH_URL}?name={quote(name)}&role-type=ALL"
            f"&page={page}&page-size={_PAGE_SIZE}&registered-only=true"
        )
        try:
            resp = await client.get(url, headers={"Ocp-Apim-Subscription-Key": key})
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("nz_associations: role-search HTTP error: %s", exc)
            break
        if not resp.is_success:
            _LOG.warning("nz_associations: role-search HTTP %s", resp.status_code)
            break
        try:
            payload = resp.json()
        except ValueError:
            break
        page_records = _extract_records(payload)
        if page == 0:
            total = _extract_total(payload, len(page_records))
            if not page_records and isinstance(payload, dict):
                # 200 but nothing we recognise — log the keys so the live shape
                # can be mapped without guessing (no PII in key names).
                _LOG.warning(
                    "nz_associations: role-search returned no parseable records; "
                    "payload keys=%s", sorted(payload.keys()),
                )
        records.extend(page_records)
        if len(page_records) < _PAGE_SIZE:
            break
    return records, (total or len(records))


def _record_person_name(rec: dict[str, Any]) -> str:
    parts = [rec.get("firstName"), rec.get("middleName"), rec.get("lastName")]
    return " ".join(str(x).strip() for x in parts if x and str(x).strip())


def _find_record_holder(holders: list[dict[str, Any]], rec: dict[str, Any]) -> dict[str, Any] | None:
    """The role holder in another company's NZBN record that a Role Search
    record names: the full name first, then first + last name (a middle name
    is often entered on one register record and not another)."""
    found = _find_holder(holders, _record_person_name(rec))
    if found is not None:
        return found
    first = _norm_name(rec.get("firstName"))
    last = _norm_name(rec.get("lastName"))
    if not (first and last):
        return None
    for rh in holders:
        toks = _norm_name(rh.get("name")).split()
        if len(toks) >= 2 and toks[0] == first and toks[-1] == last:
            return rh
    return None


def withheld_companies(
    records: list[dict[str, Any]], subject_number: str,
) -> list[tuple[str, dict[str, Any]]]:
    """``(nzbn, record)`` for each other company whose Role Search address is
    withheld, in the order Role Search returned them, one entry per NZBN."""
    out: list[tuple[str, dict[str, Any]]] = []
    seen: set[str] = set()
    for rec in records:
        try:
            if not _address_withheld(rec.get("physicalAddress")):
                continue
            companies = _record_companies(rec)
        except Exception:  # noqa: BLE001 — summarise_person logs it
            continue
        for c in companies:
            nzbn = c["nzbn"]
            if not nzbn or c["number"] == subject_number or nzbn in seen:
                continue
            seen.add(nzbn)
            out.append((nzbn, rec))
    return out


async def follow_up_withheld(
    records: list[dict[str, Any]], subject_number: str,
    *, cap: int = _FOLLOWUP_CAP, sem: asyncio.Semaphore | None = None,
) -> dict[str, dict[str, Any]]:
    """Read the address Role Search withholds from each company's own NZBN
    record (MBIE's documented route, 27 Oct 2026), for the first ``cap``
    companies. Keyed on NZBN: ``{"status": "checked", "holder": {...} | None}``
    or ``{"status": "failed"}``. A company not in the result was not checked.

    Goes through ``REGISTRY["nz_companies"].fetch`` so the adapter's cache is
    shared with the lookup, and a failure there raises rather than reading as
    an empty record.
    """
    targets = withheld_companies(records, subject_number)[:max(0, cap)]
    if not targets:
        return {}
    adapter: Any = REGISTRY["nz_companies"]
    sem = sem or asyncio.Semaphore(_CONCURRENCY)

    async def _one(nzbn: str, rec: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        async with sem:
            try:
                bundle = await adapter.fetch(nzbn, legal_name="")
            except Exception as exc:  # noqa: BLE001 — one company must not fail the rest
                _LOG.warning("nz_associations: follow-up for %s failed: %s",
                             nzbn, describe_exception(exc))
                return nzbn, {"status": "failed"}
        if not isinstance(bundle, dict) or bundle.get("is_stub") or not bundle.get("company"):
            return nzbn, {"status": "failed"}
        holder = _find_record_holder(_collect_role_holders(bundle), rec)
        if holder is None:
            return nzbn, {"status": "checked", "holder": None}
        return nzbn, {"status": "checked", "holder": {
            "paf_id": holder.get("paf_id"), "address": holder.get("address"),
            "address_type": holder.get("address_type"),
        }}

    pairs = await asyncio.gather(*[_one(n, r) for n, r in targets])
    return dict(pairs)


def _company_grade(
    rec: dict[str, Any], c: dict[str, Any], rh: dict[str, Any],
    followups: dict[str, dict[str, Any]] | None,
) -> tuple[str, str, str]:
    """``(tier, basis, address_source)`` for one company on one record."""
    subj_paf, subj_addr, subj_type = rh.get("paf_id"), rh.get("address"), rh.get("address_type")
    phys = rec.get("physicalAddress")
    if not _address_withheld(phys):
        tier, basis = _grade(_paf_of(phys), _phys_addr_str(phys), None,
                             subj_paf, subj_addr, subj_type)
        return tier, basis, "role_search"
    fu = (followups or {}).get(c["nzbn"] or "")
    if fu is None:
        return "low", _NOT_CHECKED_BASIS, "not_checked"
    if fu.get("status") != "checked":
        return "low", _CHECK_FAILED_BASIS, "check_failed"
    holder = fu.get("holder") or {}
    tier, basis = _grade(holder.get("paf_id"), holder.get("address"), holder.get("address_type"),
                         subj_paf, subj_addr, subj_type)
    if tier != "low":
        basis += _COMPANY_RECORD_SUFFIX
    return tier, basis, "company_record"


def summarise_person(
    rh: dict[str, Any], records: list[dict[str, Any]], subject_number: str,
    total_records: int = 0, followups: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Dedup by company, exclude the subject, grade each by address confidence.

    **Every name match counts.** Address corroboration grades a company match
    high / medium / low (name-only) — it no longer excludes it. The per-company
    confidence is the strongest tier seen across that name's records.

    ``followups`` is :func:`follow_up_withheld`'s result: where Role Search
    withheld a record's address, the company's own NZBN record grades it
    instead; a withheld company without one is name-only and counted as not
    checked.
    """
    companies: dict[str, dict[str, Any]] = {}

    for rec in records:
        try:
            record_companies = _record_companies(rec)
            graded = [(c, _company_grade(rec, c, rh, followups)) for c in record_companies]
        except Exception as exc:  # noqa: BLE001 — one odd record must not break the rest
            _LOG.warning("nz_associations: skipping unparseable record: %s", exc)
            continue
        for c, (tier, basis, source) in graded:
            num = c["number"]
            if not num or num == subject_number:
                continue
            cur = companies.get(num)
            if cur is None:
                companies[num] = {
                    "number": num, "name": c["name"], "nzbn": c["nzbn"],
                    "roles": {c["role"]}, "share_percentage": c["share_percentage"],
                    "confidence": tier, "basis": basis, "address_source": source,
                }
            else:
                cur["roles"].add(c["role"])
                if c["share_percentage"] is not None and cur["share_percentage"] is None:
                    cur["share_percentage"] = c["share_percentage"]
                if not cur.get("name") and c["name"]:
                    cur["name"] = c["name"]
                if not cur.get("nzbn") and c["nzbn"]:
                    cur["nzbn"] = c["nzbn"]
                # Keep the strongest confidence seen for this company, with the
                # basis and address source that produced it.
                if _CONF_ORDER[tier] < _CONF_ORDER[cur["confidence"]]:
                    cur.update(confidence=tier, basis=basis, address_source=source)

    ordered = sorted(
        companies.values(),
        key=lambda c: (_CONF_ORDER[c["confidence"]], (c["name"] or "").lower()),
    )
    out_companies = [{
        "number": c["number"], "name": c["name"], "nzbn": c["nzbn"],
        "roles": sorted(c["roles"]), "share_percentage": c["share_percentage"],
        "confidence": c["confidence"], "basis": c["basis"],
        "address_source": c["address_source"],
        "link": _entity_link(c["nzbn"]),
    } for c in ordered]

    vals = companies.values()

    def _sourced(kind: str) -> int:
        return sum(1 for c in vals if c["address_source"] == kind)

    return {
        "name": rh["name"],
        "role_here": sorted(rh.get("roles_here") or []),
        "other_company_count": len(companies),
        "high_confidence_count": sum(1 for c in vals if c["confidence"] == "high"),
        # Address-corroborated (high + medium) vs name-only (low) — so the UI can
        # lead with the credible subset while still showing every name match.
        "address_match_count": sum(1 for c in vals if c["confidence"] in ("high", "medium")),
        "name_only_count": sum(1 for c in vals if c["confidence"] == "low"),
        "as_director": sum(1 for c in vals if "director" in c["roles"]),
        "as_shareholder": sum(1 for c in vals if "shareholder" in c["roles"]),
        # The register holds more role records under this name than we fetched
        # (≤ 150) — surface the true magnitude so a prolific name isn't capped.
        "total_records_under_name": total_records,
        "truncated": total_records > len(records),
        # Role Search withheld the address (an alternative address is on file):
        # read from the company's record, past the cap, or the read failed.
        "address_withheld_count": _sourced("company_record") + _sourced("not_checked")
        + _sourced("check_failed"),
        "withheld_checked": _sourced("company_record"),
        "withheld_not_checked": _sourced("not_checked"),
        "withheld_check_failed": _sourced("check_failed"),
        "companies": out_companies,
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

async def assemble_associations(company_number: str) -> dict[str, Any]:
    """Build the associations view for a subject NZ company number."""
    number = (company_number or "").strip()
    settings = get_settings()
    key = settings.nzbn_role_search_api_key

    if not number:
        return {"company_number": "", "available": False,
                "reason": "no company number", "people": []}
    if not settings.allow_live or not key:
        return {"company_number": number, "available": False,
                "reason": "Companies Entity Role Search not configured", "people": []}

    cache_key = f"{_CACHE_NS}/v2/{number}"
    cached = _cache.get_payload(cache_key)
    if cached is not None:
        return cached[0]

    # This is a panel-only enrichment over a third-party API whose response shape
    # is not pinned by a public schema. Anything unexpected must degrade to
    # "unavailable", never bubble a 500 onto the results page.
    try:
        adapter = REGISTRY["nz_companies"]
        bundle = await adapter.fetch(number, legal_name="")
        subject_number = str(bundle.get("nz_company_number") or number)

        all_role_holders = _collect_role_holders(bundle)
        # Directors first — control matters more than minority shareholdings for
        # nominee detection — so any ceiling trims shareholder-only holders first.
        all_role_holders.sort(key=lambda rh: "director" not in (rh.get("roles_here") or set()))
        role_holders = all_role_holders[:_MAX_ROLE_HOLDERS]
        not_checked = max(0, len(all_role_holders) - len(role_holders))

        sem = asyncio.Semaphore(_CONCURRENCY)

        async def _one(client, rh: dict[str, Any]) -> dict[str, Any]:
            async with sem:
                try:
                    records, total = await _role_search(
                        client, rh.get("search_name") or rh["name"], key
                    )
                    return summarise_person(rh, records, subject_number, total_records=total)
                except Exception as exc:  # noqa: BLE001 — one holder must not fail all
                    _LOG.warning("nz_associations: role-holder %r failed: %s", rh.get("name"), exc)
                    return summarise_person(rh, [], subject_number)

        async with build_client() as client:
            people = list(
                await asyncio.gather(*[_one(client, rh) for rh in role_holders])
            )

        # Lead with the most credible (address-corroborated) and most connected.
        people.sort(
            key=lambda p: (
                p["address_match_count"],
                p["other_company_count"],
                p["high_confidence_count"],
            ),
            reverse=True,
        )
        result = {
            "company_number": subject_number,
            "available": True,
            "subject_name": (bundle.get("company") or {}).get("name"),
            "checked": len(role_holders),
            "not_checked": not_checked,
            "people": people,
        }
    except Exception as exc:  # noqa: BLE001
        _LOG.exception("nz_associations: assembly failed for %s: %s", number, exc)
        return {"company_number": number, "available": False,
                "reason": "associations lookup failed", "people": []}

    _cache.put(cache_key, result)
    return result


def _holder_key(name: str | None) -> str:
    """Order-insensitive comparable form of a role holder's name.

    The BackgroundCheck card passes the name as the NZ person statement files
    it — the bundle's own spelling — so the plain normalised form matches; the
    sorted-token form also accepts "DOE, John" for "John Doe".
    """
    return " ".join(sorted(_norm_name(name).split()))


def _find_holder(holders: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    want = _norm_name(name)
    for rh in holders:
        if _norm_name(rh.get("name")) == want:
            return rh
    want_sorted = _holder_key(name)
    for rh in holders:
        if _holder_key(rh.get("name")) == want_sorted:
            return rh
    return None


async def person_associations(company_number: str, name: str) -> dict[str, Any]:
    """One role holder's other NZ companies — the BackgroundCheck card's view.

    The same search and the same address grading as
    :func:`assemble_associations`, for one person: the name must be a director
    or shareholder of ``company_number`` (the register's filing is what gives
    the address the matches are graded against), so this never becomes a free
    name search. One Role Search call per person, cached on company + name;
    a company-wide result already cached answers it without a call.
    """
    number = (company_number or "").strip()
    name = (name or "").strip()
    settings = get_settings()
    key = settings.nzbn_role_search_api_key
    base: dict[str, Any] = {"company_number": number, "name": name, "person": None}

    if not number or not name:
        return {**base, "available": False, "reason": "no company number or name"}
    if not settings.allow_live or not key:
        return {**base, "available": False,
                "reason": "Companies Entity Role Search not configured"}

    cache_key = f"{_CACHE_NS}/person-v2/{number}/{_holder_key(name)}"
    cached = _cache.get_payload(cache_key)
    if cached is not None:
        return cached[0]
    whole = _cache.get_payload(f"{_CACHE_NS}/v2/{number}")
    if whole is not None and whole[0].get("available"):
        for p in whole[0].get("people") or []:
            # The company-wide view makes no NZBN follow-ups, so it answers for
            # one person only when nothing was left unchecked.
            if (_holder_key(p.get("name")) == _holder_key(name)
                    and not p.get("withheld_not_checked")):
                return {**base, "available": True,
                        "company_number": whole[0].get("company_number") or number,
                        "subject_name": whole[0].get("subject_name"), "person": p}

    try:
        adapter: Any = REGISTRY["nz_companies"]  # NzCompaniesAdapter.fetch takes legal_name
        bundle = await adapter.fetch(number, legal_name="")
        subject_number = str(bundle.get("nz_company_number") or number)
        rh = _find_holder(_collect_role_holders(bundle), name)
        if rh is None:
            # Not cached: the register's list of directors changes.
            return {**base, "company_number": subject_number, "available": False,
                    "reason": "not a director or shareholder of this company on the register"}
        async with build_client() as client:
            records, total = await _role_search(client, rh.get("search_name") or rh["name"], key)
        followups = await follow_up_withheld(records, subject_number)
        person = summarise_person(rh, records, subject_number, total_records=total,
                                  followups=followups)
        result = {
            **base,
            "company_number": subject_number,
            "available": True,
            "subject_name": (bundle.get("company") or {}).get("name"),
            "person": person,
        }
    except Exception as exc:  # noqa: BLE001 — a card must degrade, never 500
        _LOG.exception("nz_associations: person lookup failed for %s: %s", number, exc)
        return {**base, "available": False, "reason": "associations lookup failed"}

    # A failed NZBN read is never cached as the answer (the Phase 262 rule):
    # the next ask reads it again.
    if not person.get("withheld_check_failed"):
        _cache.put(cache_key, result)
    return result


__all__ = [
    "assemble_associations", "follow_up_withheld", "person_associations",
    "summarise_person", "withheld_companies",
]
