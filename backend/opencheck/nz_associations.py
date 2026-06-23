"""New Zealand director / shareholder associations enrichment (lazy, panel-only).

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
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import quote

from .cache import Cache
from .config import get_settings
from .http import build_client
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
_CONCURRENCY = 5           # parallel Role Search calls (rate-limit friendly)

_CONFIDENCE_BASIS = {
    "high": "Same registered address",
    "medium": "Overlapping address",
    "low": "Same name — may differ",
}

# Strongest → weakest, for keeping the best confidence per company and ordering.
_CONF_ORDER = {"high": 0, "medium": 1, "low": 2}

_cache = Cache()


# --------------------------------------------------------------------------- #
# Matching helpers
# --------------------------------------------------------------------------- #

def _norm_name(s: str | None) -> str:
    return " ".join((s or "").lower().split())


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


def _entity_link(nzbn: str | None) -> str | None:
    return f"https://www.nzbn.govt.nz/mynzbn/nzbndetails/{nzbn}/" if nzbn else None


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
            "role": "shareholder", "share_percentage": sh.get("sharePercentage"),
        })
    return out


# --------------------------------------------------------------------------- #
# Role holders of the subject company
# --------------------------------------------------------------------------- #

def _collect_role_holders(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """Unique directors + shareholders of the subject, with name + address."""
    by_name: dict[str, dict[str, Any]] = {}

    def add(name: str | None, paf: str | None, addr: str | None, role_here: str) -> None:
        name = (name or "").strip()
        if not name:
            return
        key = _norm_name(name)
        rh = by_name.get(key)
        if rh is None:
            rh = {"name": name, "paf_id": paf, "address": addr, "roles_here": set()}
            by_name[key] = rh
        rh["roles_here"].add(role_here)
        if not rh.get("paf_id") and paf:
            rh["paf_id"] = paf
        if not rh.get("address") and addr:
            rh["address"] = addr

    for r in bundle.get("roles") or []:
        add(r.get("name"), r.get("paf_id"), r.get("address"), "director")
    for s in bundle.get("shareholders") or []:
        add(s.get("name"), s.get("paf_id"), s.get("address"), "shareholder")
    return list(by_name.values())


# --------------------------------------------------------------------------- #
# Role Search API
# --------------------------------------------------------------------------- #

async def _role_search(client, name: str, key: str) -> tuple[list[dict[str, Any]], int]:
    """Return ``(records, total_results)`` for a name (registered companies only).

    ``records`` is capped at ``_PAGE_CAP * _PAGE_SIZE``; ``total_results`` is the
    API's reported total so a prolific name isn't silently undercounted."""
    records: list[dict[str, Any]] = []
    total = 0
    for page in range(_PAGE_CAP):
        url = (
            f"{_ROLE_SEARCH_URL}?name={quote(name)}"
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
        if page == 0:
            try:
                total = int(payload.get("totalResults") or 0)
            except (TypeError, ValueError):
                total = 0
        roles = payload.get("roles") or []
        records.extend(r for r in roles if isinstance(r, dict))
        if len(roles) < _PAGE_SIZE:
            break
    return records, (total or len(records))


def summarise_person(
    rh: dict[str, Any], records: list[dict[str, Any]], subject_number: str,
    total_records: int = 0,
) -> dict[str, Any]:
    """Dedup by company, exclude the subject, grade each by address confidence.

    **Every name match counts.** Address corroboration grades a company match
    high / medium / low (name-only) — it no longer excludes it. The per-company
    confidence is the strongest tier seen across that name's records.
    """
    subj_paf, subj_addr = rh.get("paf_id"), rh.get("address")
    companies: dict[str, dict[str, Any]] = {}

    for rec in records:
        phys = rec.get("physicalAddress")
        tier = _tier(_paf_of(phys), _phys_addr_str(phys), subj_paf, subj_addr)
        for c in _record_companies(rec):
            num = c["number"]
            if not num or num == subject_number:
                continue
            cur = companies.get(num)
            if cur is None:
                companies[num] = {
                    "number": num, "name": c["name"], "nzbn": c["nzbn"],
                    "roles": {c["role"]}, "share_percentage": c["share_percentage"],
                    "confidence": tier,
                }
            else:
                cur["roles"].add(c["role"])
                if c["share_percentage"] is not None and cur["share_percentage"] is None:
                    cur["share_percentage"] = c["share_percentage"]
                if not cur.get("name") and c["name"]:
                    cur["name"] = c["name"]
                if not cur.get("nzbn") and c["nzbn"]:
                    cur["nzbn"] = c["nzbn"]
                # Keep the strongest confidence seen for this company.
                if _CONF_ORDER[tier] < _CONF_ORDER[cur["confidence"]]:
                    cur["confidence"] = tier

    ordered = sorted(
        companies.values(),
        key=lambda c: (_CONF_ORDER[c["confidence"]], (c["name"] or "").lower()),
    )
    out_companies = [{
        "number": c["number"], "name": c["name"], "nzbn": c["nzbn"],
        "roles": sorted(c["roles"]), "share_percentage": c["share_percentage"],
        "confidence": c["confidence"], "basis": _CONFIDENCE_BASIS[c["confidence"]],
        "link": _entity_link(c["nzbn"]),
    } for c in ordered]

    vals = companies.values()
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

    cache_key = f"{_CACHE_NS}/{number}"
    cached = _cache.get_payload(cache_key)
    if cached is not None:
        return cached[0]

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
                records, total = await _role_search(client, rh["name"], key)
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("nz_associations: %s", exc)
                records, total = [], 0
        return summarise_person(rh, records, subject_number, total_records=total)

    async with build_client() as client:
        people = list(
            await asyncio.gather(*[_one(client, rh) for rh in role_holders])
        )

    # Lead with the most credible (address-corroborated) and the most connected.
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
    _cache.put(cache_key, result)
    return result


__all__ = ["assemble_associations", "summarise_person"]
