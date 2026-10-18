"""Every company a legal person has held a role in, from RIK's REST API.

``GET /company/companyRelations?legalPersonCode=…&legalPersonCodeCountry=…
&onlyValid=false`` lists every company a **legal person** has ever been named
on — shareholder, founder, AS shareholder, partner, pledgee, document
holder — with dates and shares, **deleted companies included**. Eesti Energia
AS on 17 Oct 2026: 25 companies, 13 of them deleted, 113 ended relations.
The index's reverse holdings (the Subsidiaries tab's fifth list) show only
current holdings in live companies.

Stephen's decisions (17 Oct 2026, the legal-person-relations ticket):

* **Legal persons only.** A natural person stays on the index's person key;
  REST's name + birthday search would be a name match.
* **On demand, never per lookup** — the contract's daily allowance is shared
  and capped. Fetched on click (the person-appointments pattern), from the
  Subsidiaries tab and the Data drawer, and by the MCP tool.
* **Current holdings are shown too**, beside the former and deleted ones —
  to be tested in production and taken out if the repeat reads wrong.
* **Listed, never drawn.** No BODS, no graph node; hidden on a saved report.

What is kept: the company's name, registry code, status, legal form (where
the code is one the register's own answer has confirmed), and per relation
the role (in the mapper's words), dates and share. Everything else in the
answer — the company's e-mail, phone, web site, address history — is
dropped before anything is cached. The answer carries no personal codes
(it names the legal person itself); the reduced rows are checked anyway.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime
from typing import Any

from ..cache import Cache
from . import ariregister_rest as rik

logger = logging.getLogger(__name__)

#: One call, one page. The answer's ``total`` says how many companies there
#: are; more than this is cut and said (Nasdaq CSD SE: 8,562).
PAGE_SIZE = 500

CACHE_MAX_AGE_DAYS = 1.0
_CACHE_KEY = "ariregister/relations-v1/{country}/{code}"

#: REST's ``legalForm`` codes, as the register's own ``/company`` answer
#: confirmed them (``generalData.legalForm[].contentNumber``, 17 Oct 2026).
#: Any other code is left unworded rather than guessed.
LEGAL_FORMS: dict[str, str] = {
    "1": "Public limited company (AS)",
    "5": "Private limited company (OÜ)",
}

#: Founder roles (``A`` founder, ``B`` founder without a contribution).
FOUNDER_ROLES = frozenset({"A", "B"})

_COUNTRY = re.compile(r"^[A-Z]{3}$")
_EE_CODE = re.compile(r"^\d{8}$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}")
_PERSONAL_CODE = re.compile(r"(?<!\d)[1-6]\d{10}(?!\d)")


class BadCode(ValueError):
    """Not an Estonian registry code, or not a foreign code with a country."""


def normalise(code: str, country: str | None) -> tuple[str, str]:
    """``(code, country)`` as the API takes them. An Estonian code is eight
    digits (zero-filled); a foreign one is the register's own spelling of
    the code with an ISO alpha-3 country."""
    code = (code or "").strip()
    country = (country or "EST").strip().upper()
    if not _COUNTRY.match(country):
        raise BadCode("country must be an ISO 3166-1 alpha-3 code (EST, LVA, …)")
    if country == "EST":
        digits = re.sub(r"\D", "", code)
        if not digits or len(digits) > 8:
            raise BadCode("an Estonian registry code is eight digits")
        return digits.zfill(8), country
    if not code or len(code) > 40 or not re.match(r"^[A-Za-z0-9 .\-/]+$", code):
        raise BadCode("a foreign registry code is letters and digits")
    return code, country


def _day(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:10] if _DAY.match(text) else None


def _num(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return f"{number:.2f}".rstrip("0").rstrip(".") if number != int(number) else str(int(number))


def role_label(role: str | None, register_text: str | None) -> str:
    """The relation's role in the words the Estonian mapper uses for the same
    code on a company's own lists; the register's English text otherwise."""
    from ..bods.mappers.estonia import ee_person_role_label

    role = (role or "").strip()
    if role == "O":
        return ee_person_role_label("shareholders", role)
    for list_name in ("off_card", "registry_card"):
        label = ee_person_role_label(list_name, role)
        if label != role:
            return label
    return (register_text or role or "Listed").strip()


def reduce_answer(body: Any, *, today: date | None = None) -> dict[str, Any]:
    """The rows OpenCheck shows, from one ``companyRelations`` answer:
    one per company, each with its relations. Nothing else is kept."""
    from ..bods.mappers.estonia import EE_STATUS_LABELS

    today = today or datetime.now(UTC).date()
    body = body if isinstance(body, dict) else {}
    companies: list[dict[str, Any]] = []
    for item in body.get("list") or []:
        if not isinstance(item, dict):
            continue
        code = str(item.get("registryCode") or "").strip()
        if code.isdigit():
            code = code.zfill(8)
        status = str(item.get("status") or "").strip()
        relations = []
        for rel in item.get("searchRelatedPersons") or []:
            if not isinstance(rel, dict):
                continue
            raw_share = rel.get("share")
            share: dict[str, Any] = raw_share if isinstance(raw_share, dict) else {}
            end = _day(rel.get("endDate"))
            role_code = str(rel.get("role") or "").strip()
            relations.append({
                "role": role_label(rel.get("role"), rel.get("roleAsText")),
                "role_code": role_code or None,
                # A founder is how the company began, never a current holding
                # (the off-card rule): it has no end date and is not "current".
                "founder": role_code in FOUNDER_ROLES,
                "since": _day(rel.get("startDate")),
                "until": end,
                "ended": bool(end and end <= today.isoformat()),
                "share_percent": _num(share.get("sharePercentage")),
                "share_size": _num(share.get("shareSize")),
                "share_currency": (share.get("shareCurrencyAsText") or None),
            })
        # Current relations first, then the most recent.
        relations.sort(key=lambda r: r["until"] or r["since"] or "", reverse=True)
        relations.sort(key=lambda r: (r["ended"], r["founder"]))
        companies.append({
            "name": (str(item.get("legalEntityName") or "").strip() or "Unnamed company"),
            "code": code or None,
            "status_code": status or None,
            "status": EE_STATUS_LABELS.get(status) or (item.get("statusAsText") or None),
            "legal_form": LEGAL_FORMS.get(str(item.get("legalForm") or "").strip()),
            "current": status in ("R", "L", "N") and any(not r["ended"] and not r["founder"] for r in relations),
            "relations": relations,
        })
    # A company with a current relation first, then by its latest date.
    def latest(c: dict[str, Any]) -> str:
        return max((r["until"] or r["since"] or "" for r in c["relations"]), default="")

    companies.sort(key=latest, reverse=True)
    companies.sort(key=lambda c: not c["current"])
    total = body.get("total")
    total = total if isinstance(total, int) else len(companies)
    reduced = {"total": total, "companies": companies, "truncated": total > len(companies)}
    if _PERSONAL_CODE.search(str(reduced)):
        # The answer names the legal person and its companies only; a code
        # of the personal shape here is something new and is not shown.
        raise ValueError("a personal-code-shaped value reached the reduced relations")
    return reduced


async def legal_person_relations(code: str, country: str | None = "EST") -> dict[str, Any]:
    """The reduced relations for one legal person, from the day's cache or
    one REST call. Raises :class:`BadCode`, :class:`ariregister_rest.RikNotConfigured`
    or :class:`ariregister_rest.RikUnavailable`; a code with no relations
    (RIK's 204) is an empty list, and is cached like any answer."""
    code, country = normalise(code, country)
    cache = Cache()
    key = _CACHE_KEY.format(country=country, code=re.sub(r"[^A-Za-z0-9]", "_", code))
    hit = cache.get_payload(key, max_age_days=CACHE_MAX_AGE_DAYS)
    if hit is not None and isinstance(hit[0], dict) and "companies" in hit[0]:
        return {**hit[0], "cached": True}
    body = await rik.get_json(
        "/company/companyRelations",
        service="company_relations",
        params={
            "legalPersonCode": code,
            "legalPersonCodeCountry": country,
            "onlyValid": "false",
            "pageSize": str(PAGE_SIZE),
        },
    )
    reduced = reduce_answer(body)
    reduced.update({
        "code": code,
        "country": country,
        "retrieved_at": datetime.now(UTC).isoformat(timespec="seconds"),
    })
    cache.put(key, reduced)
    return {**reduced, "cached": False}


__all__ = [
    "BadCode", "CACHE_MAX_AGE_DAYS", "LEGAL_FORMS", "PAGE_SIZE",
    "legal_person_relations", "normalise", "reduce_answer", "role_label",
]
