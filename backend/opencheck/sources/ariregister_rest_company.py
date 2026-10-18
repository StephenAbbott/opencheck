"""A company the open-data index lacks, read from RIK's REST API.

An index miss is almost always a **deleted** company (no open-data file
carries one) or one **entered after the files' cut**. Until this module the
miss went to the printable page, which rate-limits datacentre callers
(Cloudflare 1015), redirects when a company is unknown, and has no
``kirje_id``. ``GET /company/{code}`` answers both cases in one call —
deletion date, every name with its dates, successions, rulings, the
registry card and every person the card or the lists have ever named — and
a clean ``404 code 4043`` for an unknown code.

:func:`bundle_from_rest` turns that answer into the bundle shape the index
and the page already produce (the files' row vocabulary: ``kirje_id``,
``isiku_tyyp``, ``isiku_roll``, ``osaluse_protsent``…), so
``map_ariregister`` and ``finding_ariregister`` read it unchanged. Rules,
each the index path's or the full-history phase's:

* **Current rows are the lists, ended rows are history.** Officers and the
  supervisory board are serving-only (Phase 317); an ended shareholding is
  a closed relationship, through the same ``history.former_shareholders``
  the SOAP history feeds. Founders are ownership history whatever their
  end date (the off-card phase).
* **Personal codes join, then go.** A natural person's ``identityCode``
  groups that person's rows into one node, keyed ``rest-<first recordId>``
  (never anything derived from the code), and is dropped. A natural
  person's foreign code is dropped too; a legal person's registry or
  foreign code is kept — it is the company. E-mail, street addresses,
  telephone numbers and the representation-rights list are never copied:
  the translation is an allowlist.
* **No beneficial owners.** The REST beneficial-owner service needs a named
  viewer and a stated purpose, and is logged; it is not read here, and the
  bundle says so (``bo_not_read``) so the finding cannot read an empty list
  as "none declared".
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from . import ariregister_rest as rik

#: The query the index-miss read sends: every section, full history.
COMPANY_QUERY = {
    "includeGeneralData": "true",
    "includeRelatedPersons": "true",
    "includeRulings": "true",
    "includeRegistryCards": "true",
    "includeCommercialPledgeData": "true",
    "onlyValid": "false",
}

#: The REST answer's status codes, as the index and the page word them.
STATUS_WORDS = {
    "R": "Entered into the register",
    "L": "In liquidation",
    "N": "Bankrupt",
    "K": "Deleted",
}

#: Off-card roles that are a holding (an AS shareholder's register entry).
_AS_SHAREHOLDER_ROLES = frozenset({"S"})
#: Founders: ownership history at founding, kept whatever their end date.
_FOUNDER_ROLES = frozenset({"A", "B"})

#: A registry code named in a succession entry, as the index builder reads
#: it (``scripts/build_ariregister_index.py::_SUCCESSION_CODE``).
_SUCCESSION_CODE = re.compile(
    r"(?:registrikood\w*|reg\.?\s*(?:nr|kood)\.?)\D{0,6}?(\d{8})(?!\d)", re.IGNORECASE,
)
_ANY_CODE = re.compile(r"(?<!\d)(\d{8})(?!\d)")
_COMPANY_CODE = re.compile(r"^\d{8}$")

#: An eleven-digit number that could be a personal identification code, and
#: the words that make one a registry code instead — the index builder's
#: ``scrub_text`` rule (``scripts/build_ariregister_index.py``), copied
#: because the builder is a script; ``test_ariregister_rest_company`` pins
#: that the two agree.
_PERSONAL_CODE_IN_TEXT = re.compile(r"(?<!\d)[1-6]\d{10}(?!\d)")
_REGISTRY_WORD = re.compile(
    r"(registrikood\w*|reg\.?\s*(nr|kood)\.?|registreerimisnumb\w*)\W{0,6}$", re.IGNORECASE,
)
PERSONAL_CODE_REMOVED = "[personal code removed]"


def scrub_text(text: Any) -> str:
    """A register text with whitespace collapsed and every number shaped like
    a personal identification code removed, unless the words before it call
    it a registry code (a Latvian company is eleven digits too)."""
    text = " ".join(str(text or "").split())

    def keep(m: re.Match[str]) -> str:
        before = text[max(0, m.start() - 24):m.start()]
        return m.group(0) if _REGISTRY_WORD.search(before) else PERSONAL_CODE_REMOVED

    return _PERSONAL_CODE_IN_TEXT.sub(keep, text)


def _day(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw[:10] if len(raw) >= 10 and raw[4] == "-" else None


def _ended(row: dict[str, Any], today: date) -> bool:
    end = _day(row.get("endDate"))
    return end is not None and end <= today.isoformat()


def _current(rows: Any, today: date) -> list[dict[str, Any]]:
    return [r for r in rows or [] if isinstance(r, dict) and not _ended(r, today)]


def _latest(rows: Any) -> dict[str, Any] | None:
    rows = [r for r in rows or [] if isinstance(r, dict)]
    if not rows:
        return None
    return max(rows, key=lambda r: (_day(r.get("startDate")) or "", str(r.get("recordId") or "")))


def _text(value: Any) -> str | None:
    s = " ".join(str(value or "").split())
    return s or None


def _num(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return _text(value)
    return f"{f:.2f}".rstrip("0").rstrip(".") if f != int(f) else str(int(f))


# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------


def _person_keys(related: dict[str, Any]) -> tuple[dict[str, str], set[str]]:
    """``recordId`` → person key for every natural-person row, grouped on
    the personal code (one key per person, ``rest-<smallest recordId>``),
    and the ``recordId``s of natural-person rows with no code at all."""
    by_code: dict[str, list[str]] = {}
    keys: dict[str, str] = {}
    codeless: set[str] = set()
    for lst in ("personsOnCard", "nonCardRelatedPersons", "shareholders"):
        for row in related.get(lst) or []:
            person = row.get("person") or {}
            if (person.get("type") or "F") == "J":
                continue
            rid = str(row.get("recordId") or "")
            code = str(person.get("identityCode") or "").strip()
            if code:
                by_code.setdefault(code, []).append(rid)
            else:
                keys[rid] = f"rest-{rid}"
                codeless.add(rid)
    for rids in by_code.values():
        key = "rest-" + min(rids, key=lambda r: (len(r), r))
        for rid in rids:
            keys[rid] = key
    return keys, codeless


def _row(row: dict[str, Any], keys: dict[str, str]) -> dict[str, Any]:
    """One REST row in the files' vocabulary, by allowlist. The personal
    code, e-mail, street address and telephone never leave."""
    person = row.get("person") or {}
    kind = (person.get("type") or "F").strip() or "F"
    rid = str(row.get("recordId") or "")
    out: dict[str, Any] = {
        "kirje_id": rid,
        "isiku_tyyp": kind,
        "isiku_roll": _text(person.get("role")),
        "isiku_roll_tekstina": _text(person.get("roleAsText")),
        "eesnimi": _text(person.get("firstName")),
        "nimi_arinimi": _text(person.get("surnameLegalEntityName")),
        "algus_kpv": _day(row.get("startDate")),
        "lopp_kpv": _day(row.get("endDate")),
    }
    if kind == "J":
        code = str(person.get("identityCode") or "").strip()
        country = str(person.get("identityCodeCountry") or person.get("foreignCodeCountry") or "").strip().upper()
        if _COMPANY_CODE.match(code) and country in ("", "EST"):
            out["isikukood_registrikood"] = code
        foreign = _text(person.get("foreignCode"))
        if foreign:
            out["valis_kood"] = foreign
            out["valis_kood_riik"] = country or None
    else:
        out["person_key"] = keys.get(rid) or f"rest-{rid}"
        country = str(person.get("foreignCodeCountry") or "").strip().upper()
        if country and country != "EST":
            # The country of a foreign person's identifier is said, as the
            # files say it; the identifier itself is a personal number.
            out["valis_kood_riik"] = country
    # The holding, where the row is one.
    for src, dst in (
        ("sharePercentage", "osaluse_protsent"),
        ("shareSize", "osaluse_suurus"),
    ):
        val = _num(row.get(src))
        if val is not None:
            out[dst] = val
    for src, dst in (
        ("shareCurrency", "osaluse_valuuta"),
        ("shareOwnershipType", "osaluse_omandiliik"),
        ("shareOwnershipTypeAsText", "osaluse_omandiliik_tekstina"),
        ("authoritySource", "kontrolli_allikas"),
        ("authoritySourceAsText", "kontrolli_allikas_tekstina"),
    ):
        val = _text(row.get(src))
        if val:
            out[dst] = val
    if _day(row.get("authoritySourceDate")):
        out["kontrolli_allika_kpv"] = _day(row.get("authoritySourceDate"))
    return {k: v for k, v in out.items() if v not in (None, "")}


def _people(
    related: dict[str, Any], today: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """(officers, shareholders, off-card persons, former shareholders)."""
    keys, codeless = _person_keys(related)

    officers = [_row(r, keys) for r in _current(related.get("personsOnCard"), today)]

    shareholders: list[dict[str, Any]] = []
    off_card: list[dict[str, Any]] = []
    ended_holdings: list[dict[str, Any]] = []
    for raw in related.get("shareholders") or []:
        if not isinstance(raw, dict):
            continue
        row = _row(raw, keys)
        if _ended(raw, today):
            ended_holdings.append(row)
        elif (row.get("isiku_roll") or "") in _AS_SHAREHOLDER_ROLES:
            row.pop("lopp_kpv", None)
            off_card.append(row)
        else:
            row.pop("lopp_kpv", None)
            shareholders.append(row)

    for raw in related.get("nonCardRelatedPersons") or []:
        if not isinstance(raw, dict):
            continue
        row = _row(raw, keys)
        role = row.get("isiku_roll") or ""
        if role in _FOUNDER_ROLES:
            # A founding is a moment: the register's end date on a founder
            # row is the company's deletion, not the end of a holding.
            row.pop("lopp_kpv", None)
            off_card.append(row)
        elif not _ended(raw, today):
            off_card.append(row)

    # Who is still on a list, by the person key (natural persons) or the
    # registry code (legal persons): the full-history phase's join words.
    current_keys = {
        r.get("person_key") or r.get("isikukood_registrikood")
        for r in officers + shareholders + off_card
        if (r.get("isiku_roll") or "") not in _FOUNDER_ROLES
    } - {None}
    former: list[dict[str, Any]] = []
    for row in ended_holdings:
        key = row.get("person_key") or row.get("isikukood_registrikood")
        row["history_key"] = row.get("person_key")
        if row.get("isiku_tyyp") == "J":
            # A legal person is keyed on its registry code by the mapper and
            # carries no join note — the SOAP path's shape.
            pass
        elif key in current_keys:
            row["history_join"] = "register"
        elif row.get("isiku_tyyp") != "J" and row.get("kirje_id") in codeless:
            row["history_join"] = "row"
        else:
            row["history_join"] = "former"
        former.append({k: v for k, v in row.items() if v is not None})
    return officers, shareholders, off_card, former


# ---------------------------------------------------------------------------
# Rulings, pledges, general data
# ---------------------------------------------------------------------------


def _rulings(body: dict[str, Any], last_report_end: str | None) -> list[dict[str, Any]]:
    from .ariregister import RULING_TYPES, rulings_in_force

    rows = []
    for r in body.get("rulings") or []:
        if not isinstance(r, dict):
            continue
        kind = str(r.get("type") or "").strip()
        if kind not in RULING_TYPES or str(r.get("status") or "") != "J":
            continue
        rows.append({
            "type": kind,
            "type_text": _text(r.get("typeAsText")),
            "day": _day(r.get("date")) or _day(r.get("effectiveDate")),
            "number": _text(r.get("number")),
        })
    rows.sort(key=lambda r: r.get("day") or "", reverse=True)
    return rulings_in_force(rows, last_report_end)


def _pledges(body: dict[str, Any], today: date) -> list[dict[str, Any]]:
    out = []
    for p in body.get("commercialPledgeData") or []:
        if not isinstance(p, dict) or str(p.get("status") or "") == "K" or _ended(p, today):
            continue
        pledgees = []
        for h in p.get("pledgees") or []:
            if not isinstance(h, dict) or _ended(h, today):
                continue
            if (h.get("personType") or "") == "J":
                pledgees.append({
                    "kind": "J",
                    "name": _text(h.get("name")),
                    "code": str(h.get("code") or "").strip() if _COMPANY_CODE.match(str(h.get("code") or "").strip()) else None,
                    "country": _text(h.get("country")),
                    "country_text": _text(h.get("countryAsText")),
                })
            else:
                pledgees.append({"kind": "F"})  # a private individual; the code is never read
        rank = str(p.get("rank") or "")
        out.append({
            "amount": _num(p.get("amount")),
            "currency": _text(p.get("currency")),
            "rank": int(rank) if rank.isdigit() else None,
            "since": _day(p.get("startDate")),
            "pledgees": [{k: v for k, v in h.items() if v is not None} for h in pledgees],
        })
    out.sort(key=lambda p: (p.get("rank") or 99, p.get("since") or ""))
    return out


def _succession(general: dict[str, Any], subject: str) -> list[dict[str, Any]]:
    from .ariregister import entity_record

    out = []
    for s in general.get("successions") or []:
        if not isinstance(s, dict):
            continue
        text = scrub_text(s.get("content"))
        # The entry names the company itself too ("…, registrikood 11336737
        # ühendatav…"); only the other party is a partner.
        codes = [c for c in dict.fromkeys(_SUCCESSION_CODE.findall(text)) if c != subject]
        for code in _ANY_CODE.findall(text):
            if code in codes or code == subject:
                continue
            try:
                if entity_record(code) is not None:
                    codes.append(code)
            except Exception:  # noqa: BLE001 — no index, nothing to add
                pass
        partners = []
        for code in codes:
            try:
                hit = entity_record(code)
            except Exception:  # noqa: BLE001
                hit = None
            partners.append({"code": code, "name": (hit or {}).get("name")})
        out.append({
            "type": _text(s.get("type")),
            "day": _day(s.get("startDate")),
            "text": text,
            "codes": codes,
            "partners": partners,
        })
    return out


def _general(general: dict[str, Any], today: date, subject: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    capital = _latest(_current(general.get("capitals"), today))
    if capital:
        out["capital"] = _num(capital.get("size"))
        out["capital_currency"] = _text(capital.get("currency"))
        out["capital_since"] = _day(capital.get("startDate"))
    reports = [r for r in general.get("infoFinancialYearFromReports") or [] if isinstance(r, dict)]
    if reports:
        last = max(reports, key=lambda r: _day(r.get("periodEndDate")) or "")
        out["last_report_end"] = _day(last.get("periodEndDate"))
        out["reports_filed"] = len(reports)
        if last.get("activityAreaEmtakCode"):
            out["activity"] = _text(last.get("activityAreaEmtakCode"))
            out["activity_nace"] = _text(last.get("activityAreaNaceCode"))
            out["activity_text"] = _text(last.get("activityAreaEmtakAsText"))
    # Whether the company must file beneficial owners is stated only for a
    # company still in the register: a deleted one reads false whatever it
    # once was, and "not obliged" would say something untrue of its past.
    if general.get("deletionDate") in (None, "") and isinstance(general.get("isRequiredtoProvideBeneficiaries"), bool):
        out["bo_obliged"] = general["isRequiredtoProvideBeneficiaries"]
    succession = _succession(general, subject)
    if succession:
        out["succession"] = succession
    predecessors = [
        scrub_text(n.get("content"))
        for n in general.get("registryCardNotes") or []
        if isinstance(n, dict) and str(n.get("type") or "") == "EVREG" and n.get("content")
    ]
    if predecessors:
        out["predecessor_notes"] = predecessors
    return {k: v for k, v in out.items() if v not in (None, "", [])}


def _names(general: dict[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    """The current name and the former names, each with its dates."""
    rows = sorted(
        (r for r in general.get("legalEntityNames") or [] if isinstance(r, dict) and _text(r.get("content"))),
        key=lambda r: (_day(r.get("startDate")) or "", str(r.get("recordId") or "")),
    )
    if not rows:
        return None, []
    current = rows[-1]
    former = [
        {"name": _text(r.get("content")), "from": _day(r.get("startDate")), "until": _day(r.get("endDate"))}
        for r in rows[:-1]
        if _text(r.get("content")) != _text(current.get("content"))
    ]
    return _text(current.get("content")), former


# ---------------------------------------------------------------------------
# The bundle
# ---------------------------------------------------------------------------


def bundle_from_rest(body: dict[str, Any], code: str, *, today: date | None = None) -> dict[str, Any]:
    """The REST answer for one company as an ``AriregisterBundle``."""
    today = today or date.today()
    general = body.get("generalData") or {}
    status = general.get("status") or {}
    status_code = str(status.get("status") or "").strip() or None
    current_name, former_names = _names(general)
    name = _text(body.get("legalEntityName")) or current_name or ""
    form = _latest(_current(general.get("legalForm"), today)) or _latest(general.get("legalForm"))
    address = _latest(_current(general.get("addresses"), today))
    address_text = None
    if address:
        a = address.get("address") or {}
        address_text = _text(a.get("adsNormalizedFullAddress")) or ", ".join(
            p for p in (_text(a.get("address")), _text(a.get("ehakAsText"))) if p
        ) or None

    officers, shareholders, off_card, former = _people(body.get("relatedPersons") or {}, today)
    general_out = _general(general, today, code)
    bundle: dict[str, Any] = {
        "source_id": "ariregister",
        "registry_code": code,
        "name": name,
        "legal_form": _text((form or {}).get("contentAsText")) or _text((form or {}).get("content")),
        "vat_number": _text(body.get("vatID")),
        "status": STATUS_WORDS.get(status_code or "", _text(status.get("statusAsText"))),
        "status_code": status_code,
        "status_text": _text(status.get("statusAsText")),
        "registration_date": _day(general.get("firstEntryTime")),
        "dissolution_date": _day(general.get("deletionDate")) if status_code == "K" else None,
        "address": address_text,
        "link": f"https://ariregister.rik.ee/eng/company/{code}",
        "former_names": [n for n in former_names if n.get("name")],
        "shareholders": shareholders,
        "officers": officers,
        "beneficial_owners": [],
        "bo_not_read": True,
        "off_card_persons": off_card,
        "general": general_out or None,
        "rulings": _rulings(body, general_out.get("last_report_end")),
        "pledges": _pledges(body, today),
        "history": {
            "read": True,
            "former_shareholders": former,
            "former_beneficial_owners": [],
            "card_entries": [],
            "bo_read": False,
        },
        "tier": "rest",
        "is_stub": False,
    }
    leaks = list(rik.personal_data_paths(bundle))
    if leaks:  # an allowlist that let one through is a defect, never data
        raise ValueError(f"personal data in a REST bundle at {leaks[:3]}")
    return bundle


__all__ = ["COMPANY_QUERY", "STATUS_WORDS", "bundle_from_rest"]
