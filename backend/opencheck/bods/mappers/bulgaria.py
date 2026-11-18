"""Bulgaria — the Commercial Register's open data → BODS.

The bundle is ``BgTrAdapter.fetch``'s, built from an index of the Registry
Agency's daily files (entries since 1 January 2021). ``mapper.py`` re-exports
every name here.

What each list becomes
----------------------

* **Beneficial owners** (``ActualOwners``) → a person (or, rarely, an entity)
  and one relationship whose interests are the register's own rights codes:
  control through ownership → ``shareholding``, voting rights →
  ``votingRights``, the rest → ``otherInfluenceOrControl``; each asserts
  ``beneficialOwnershipOrControl`` (record kind ``actual_owner``). The one
  exception is ``SENIOR_MANAGEMENT_POSITION``: a person named because no owner
  was identified is a ``seniorManagingOfficial`` with no BO flag (record kind
  ``actual_owner_senior_manager``, the Estonian J/C rule). The extent is the
  percentage a right's own size states, ``exact`` or as a lower bound, and
  nothing else: an owner with no rights code is ``otherInfluenceOrControl``
  with their words quoted, because the free text often states another
  party's holding. Name, nationality, country of residence and the control —
  never a birth date, address or the register's person hash (Stephen,
  18 Nov 2026).
* **Partners and the sole owner** of a company with capital → a
  ``shareholding``: 100 % for the sole owner; for partners a percentage only
  where the contributions the register files add up to the capital within
  1 %, annotated as OpenCheck's arithmetic. Partners of a general
  partnership and unlimited-liability partners are ``otherInfluenceOrControl``.
* **Managers and representatives** → ``seniorManagingOfficial``; board
  members of any board → ``boardMember`` with the board named, the chair →
  ``boardChair``; liquidators and procurators → ``otherInfluenceOrControl``
  marked as an officer (Phase 340).

A person appearing on two lists of one company is one party: the builder
gives each a company-scoped key derived from the register's hash, and a
person without one is matched by name within the company only.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

from .. import liveness as _liveness
from ..annotations import annotate, commenting, pointer, transformation
from ..officer_roles import mark_officer
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _country_obj,
    content_addressed,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    record_date,
    set_beneficial_ownership,
)

_BG_SCHEME = "BG-EIK"
_BG_SCHEME_NAME = "ЕИК — Unified Identification Code (Bulgaria, Registry Agency)"
_BG_SOURCE = "bg_tr"

#: The register's list → (BODS interest type, English role, officer marker?).
_BG_ROLE_INTERESTS: dict[str, tuple[str, str, bool]] = {
    "Managers": ("seniorManagingOfficial", "Manager (управител)", False),
    "Representatives": ("seniorManagingOfficial", "Representative (представител)", False),
    "Representatives101": ("seniorManagingOfficial", "Representative (представител)", False),
    "Representatives103": ("seniorManagingOfficial", "Representative (представител)", False),
    "ChairMan": ("boardChair", "Chair (председател)", False),
    "BoardOfDirectors": ("boardMember", "Member of the board of directors (съвет на директорите)", False),
    "BoardOfManagers": ("boardMember", "Member of the management board (управителен съвет)", False),
    "BoardOfManagers2": ("boardMember", "Member of the management board (управителен съвет)", False),
    "SupervisingBoard": ("boardMember", "Member of the supervisory board (надзорен съвет)", False),
    "ControllingBoard": ("boardMember", "Member of the control board (контролен съвет)", False),
    "ManagementBodies12d": ("boardMember", "Member of a management body", False),
    "Liquidators": ("otherInfluenceOrControl", "Liquidator (ликвидатор)", True),
    "Procurators": ("otherInfluenceOrControl", "Procurator (прокурист)", True),
    "UnlimitedLiabilityPartners": (
        "otherInfluenceOrControl", "Unlimited-liability partner (неограничено отговорен съдружник)", False),
    "PhysicalPersonTrader": ("otherInfluenceOrControl", "Sole trader (физическо лице търговец)", False),
}

#: A beneficial owner's rights code → BODS interest type.
_BG_RIGHT_TYPES: dict[str, str] = {
    "CONTROL_THROUGH_OWNERSHIP": "shareholding",
    "VOTING_RIGHTS": "votingRights",
    "OTHER_MEANS": "otherInfluenceOrControl",
    "NATURAL_PERSON_FOUNDER": "otherInfluenceOrControl",
    "MGMNT_WITH_BENEFICIARY_TO_NATURAL_PERSON": "otherInfluenceOrControl",
}
_BG_SENIOR_MANAGER = "SENIOR_MANAGEMENT_POSITION"

BG_COVERAGE_NOTE = (
    "The Commercial Register's open data holds the entries made since "
    "1 January 2021. A field not changed since then is not in it, so a list "
    "that is absent here is unknown, not empty."
)
_BG_PARTNER_SHARE_NOTE = (
    "Share percentage derived by OpenCheck from the contribution the register "
    "files for the partner and the company's capital; the register publishes "
    "no percentage."
)


def _bg_amount(text: Any) -> float | None:
    raw = str(text or "").replace(" ", "").replace(" ", "")
    if not raw:
        return None
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".") if raw.rfind(",") > raw.rfind(".") else raw.replace(",", "")
    else:
        raw = raw.replace(",", ".")
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def bg_partner_shares(bundle: dict[str, Any]) -> dict[int, float]:
    """Partner index → percentage, only when the filed contributions add up
    to the company's capital within 1 % (the ЄДР rule)."""
    from ...sources.bg_tr import CAPITAL_FORMS

    if bundle.get("legal_form_code") not in CAPITAL_FORMS:
        return {}
    capital = _bg_amount((bundle.get("capital") or {}).get("amount"))
    partners = [
        (i, _bg_amount(p.get("share")))
        for i, p in enumerate(bundle.get("parties") or [])
        if p.get("field") == "Partners"
    ]
    if not capital or not partners or any(a is None for _i, a in partners):
        return {}
    total = sum(a for _i, a in partners if a is not None)
    if abs(total - capital) > capital * 0.01:
        return {}
    return {i: round(a / capital * 100, 2) for i, a in partners if a is not None}


def _bg_share_from_percent(percent: dict[str, Any] | None) -> dict[str, float] | None:
    if not percent:
        return None
    if "exact" in percent:
        return {"exact": float(percent["exact"])}
    if "more_than" in percent:
        return {"exclusiveMinimum": float(percent["more_than"])}
    return None


def _bg_party_key(party: dict[str, Any]) -> str:
    if party.get("eik"):
        return f"eik:{party['eik']}"
    if party.get("key"):
        return f"key:{party['key']}"
    return f"{party.get('kind')}:" + " ".join(str(party.get("name") or "").casefold().split())


def _bg_party_statement(eik: str, party: dict[str, Any], source_url: str | None) -> dict[str, Any]:
    key = _bg_party_key(party)
    if party.get("kind") == "entity":
        if party.get("eik"):
            stmt = make_entity_statement(
                source_id=_BG_SOURCE,
                local_id=f"eik:{party['eik']}",
                name=party["name"],
                jurisdiction=("Bulgaria", "BG"),
                identifiers=[{"id": party["eik"], "scheme": _BG_SCHEME, "schemeName": _BG_SCHEME_NAME}],
                source_url=source_url,
            )
            return stmt
        country = _country_obj(party.get("country") or "") or (
            _country_obj(party["country_name"]) if party.get("country_name") else None
        )
        jurisdiction = (country["name"], country.get("code")) if country else None
        digest = hashlib.sha256(key.encode()).hexdigest()[:16]
        stmt = make_entity_statement(
            source_id=_BG_SOURCE,
            local_id=f"{eik}:entity:{digest}",
            name=party["name"],
            jurisdiction=jurisdiction,
            source_url=source_url,
        )
        foreign = party.get("foreign") or {}
        bits = [
            f"register {foreign['register']}" if foreign.get("register") else None,
            f"number {foreign['number']}" if foreign.get("number") else None,
            f"registered by {foreign['authority']}" if foreign.get("authority") else None,
        ]
        said = ", ".join(b for b in bits if b)
        if said:
            annotate(stmt, commenting(
                pointer("recordDetails"),
                f"The Bulgarian Commercial Register files this foreign entity as {said}; "
                "OpenCheck does not assert it as an identifier, because the register names no scheme.",
            ))
        return stmt
    nationalities = []
    nat = _country_obj(party.get("country") or "")
    if nat and nat.get("code"):
        nationalities.append(nat)
    addresses: list[dict[str, Any]] = []
    residence = _country_obj(party.get("residence") or "")
    if residence and residence.get("code"):
        # The country of residence only; the register's street address is
        # never read (Stephen, 18 Nov 2026).
        addresses.append({"type": "residence", "country": residence})
    local = f"{eik}:person:{hashlib.sha256(key.encode()).hexdigest()[:16]}"
    return make_person_statement(
        source_id=_BG_SOURCE,
        local_id=local,
        full_name=party["name"],
        nationalities=nationalities,
        addresses=addresses,
        source_url=source_url,
    )


def _bg_role_interest(party: dict[str, Any], form: str | None) -> tuple[dict[str, Any], str]:
    """(interest, record kind) for one party of a non-BO list."""
    from ...sources.bg_tr import CAPITAL_FORMS

    field = party.get("field") or ""
    position = (party.get("position") or "").strip()
    if field == "SoleCapitalOwner":
        interest: dict[str, Any] = {
            "type": "shareholding",
            "directOrIndirect": "direct",
            "share": {"exact": 100.0},
            "details": "Sole owner of the capital (едноличен собственик на капитала)",
        }
        return interest, "sole_owner"
    if field == "Partners":
        if form in CAPITAL_FORMS:
            interest = {"type": "shareholding", "directOrIndirect": "direct",
                        "details": "Partner (съдружник)"}
        else:
            interest = {"type": "otherInfluenceOrControl", "directOrIndirect": "direct",
                        "details": "Partner (съдружник)"}
        if party.get("share"):
            interest["details"] += f"; contribution filed as {party['share']}"
        return interest, "partner"
    kind, label, officer = _BG_ROLE_INTERESTS.get(field, ("otherInfluenceOrControl", field or "Role", True))
    details = f"{label}; {position}" if position and position not in label else label
    interest = {"type": kind, "directOrIndirect": "direct", "details": details}
    if officer:
        mark_officer(interest, label.split(" (")[0])
    record_kind = "member" if kind in ("boardMember", "boardChair") else (
        "trader" if field == "PhysicalPersonTrader" else "officer")
    return interest, record_kind


def _bg_owner_interests(owner: dict[str, Any]) -> list[dict[str, Any]]:
    """The interests of one beneficial owner, from the register's rights."""
    rights = owner.get("rights") or []
    text = owner.get("owned_rights")
    out: list[dict[str, Any]] = []
    codes = [r.get("code") for r in rights if r.get("code")]
    if codes and all(c == _BG_SENIOR_MANAGER for c in codes):
        interest: dict[str, Any] = {
            "type": "seniorManagingOfficial",
            "directOrIndirect": "unknown",
            "details": (
                "Named as beneficial owner because they hold a senior management "
                "position (no owner was identified), as filed in the Commercial Register"
            ),
        }
        set_beneficial_ownership(interest, _BG_SOURCE, record_kind="actual_owner_senior_manager")
        return [interest]
    for right in rights or [{}]:
        code = right.get("code")
        if code == _BG_SENIOR_MANAGER:
            continue
        # A share only from the right's own size: the owner's free text
        # often states another party's holding (UBB's KBC directors' entries
        # say "KBC Bank holds 99.92 %"), so it is quoted, never read as one.
        share = _bg_share_from_percent(right.get("percent"))
        kind = _BG_RIGHT_TYPES.get(code, "otherInfluenceOrControl") if code else "otherInfluenceOrControl"
        words = [w for w in (right.get("name"), right.get("size")) if w]
        if not words and text:
            words = [text]
        interest = {
            "type": kind,
            "directOrIndirect": "unknown",
            "details": "Beneficial owner as filed in the Commercial Register"
            + (f": {'; '.join(words)}" if words else ""),
        }
        if share and kind in ("shareholding", "votingRights"):
            interest["share"] = dict(share)
        set_beneficial_ownership(interest, _BG_SOURCE, record_kind="actual_owner")
        out.append(interest)
    return out


@content_addressed
def map_bg_tr(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a ``BgTrAdapter`` bundle to BODS v0.4 statements."""
    from ...sources.bg_tr import NON_PROFIT_FORMS, form_label

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    eik = str(bundle.get("eik") or "").strip()
    if not eik:
        return
    source_url = bundle.get("link") or None
    statement_date = record_date(bundle.get("last_entry"))
    form = bundle.get("legal_form_code")

    addresses: list[dict[str, Any]] = []
    if bundle.get("seat_text"):
        addresses.append(_addr("registered", bundle["seat_text"], bundle.get("seat_country") or "BG"))
    alternate = [bundle["transliteration"]] if bundle.get("transliteration") else []
    company = make_entity_statement(
        source_id=_BG_SOURCE,
        local_id=f"eik:{eik}",
        name=bundle.get("name") or f"BG {eik}",
        jurisdiction=("Bulgaria", "BG"),
        identifiers=[{"id": eik, "scheme": _BG_SCHEME, "schemeName": _BG_SCHEME_NAME}],
        founding_date=bundle.get("registered_on") or None,
        addresses=addresses,
        alternate_names=alternate,
        entity_details=form_label(form, bundle.get("legal_form_text")),
        source_url=source_url,
        statement_date=statement_date,
    )
    cls = (bundle.get("status_class") or "unknown").strip()
    _liveness.apply_register_status(
        company,
        source_label=SOURCE_NAMES[_BG_SOURCE],
        liveness=_liveness.classify(cls, live=("live",), pending=("pending",), terminal=("terminal",)),
        raw=bundle.get("status") or None,
        since=bundle.get("status_since") or None,
    )
    annotate(company, commenting(pointer("recordDetails"), BG_COVERAGE_NOTE))
    yield company
    company_id = company["recordId"]

    seen: dict[str, tuple[str, str]] = {}
    # One person on several lists is one statement, built from everything
    # the lists say: the residence is filed on the beneficial-owner entry
    # only, the nationality on some lists and not others.
    merged: dict[str, dict[str, Any]] = {}
    for listed in [*(bundle.get("parties") or []), *(bundle.get("beneficial_owners") or [])]:
        whole = merged.setdefault(_bg_party_key(listed), {})
        for k, v in listed.items():
            if v not in (None, "", [], {}) and k not in whole:
                whole[k] = v

    def party_ref(party: dict[str, Any]) -> Iterable[dict[str, Any]]:
        key = _bg_party_key(party)
        if key not in seen:
            stmt = _bg_party_statement(eik, merged.get(key, party), source_url)
            seen[key] = (stmt["recordId"], "entity" if party.get("kind") == "entity" else "person")
            yield stmt

    shares = bg_partner_shares(bundle)
    for idx, party in enumerate(bundle.get("parties") or []):
        if party.get("eik") == eik:
            continue
        yield from party_ref(party)
        ref, ptype = seen[_bg_party_key(party)]
        interest, record_kind = _bg_role_interest(party, form)
        if form in NON_PROFIT_FORMS and interest["type"] == "shareholding":
            interest["type"] = "otherInfluenceOrControl"
        pct = shares.get(idx)
        notes: list[dict[str, Any]] = []
        if pct is not None:
            interest["share"] = {"exact": pct}
            notes.append(transformation(target=pointer("recordDetails", "interests", 0, "share"),
                                        description=_BG_PARTNER_SHARE_NOTE))
        set_beneficial_ownership(interest, _BG_SOURCE, record_kind=record_kind)
        rel = make_relationship_statement(
            source_id=_BG_SOURCE,
            local_id=f"{eik}:{party.get('field')}:{_bg_party_key(party)}",
            subject_statement_id=company_id,
            interested_party_statement_id=ref,
            interested_party_type=ptype,
            interests=[interest],
            source_url=source_url,
            statement_date=statement_date,
        )
        annotate(rel, *notes)
        yield rel

    for owner in bundle.get("beneficial_owners") or []:
        interests = _bg_owner_interests(owner)
        if not interests:
            continue
        yield from party_ref(owner)
        ref, ptype = seen[_bg_party_key(owner)]
        yield make_relationship_statement(
            source_id=_BG_SOURCE,
            local_id=f"{eik}:ActualOwners:{_bg_party_key(owner)}",
            subject_statement_id=company_id,
            interested_party_statement_id=ref,
            interested_party_type=ptype,
            interests=interests,
            source_url=source_url,
            statement_date=statement_date,
        )

    parent = bundle.get("foreign_trader") or {}
    if form == "KCHT" and parent.get("name"):
        country = _country_obj(parent.get("country") or "") or (
            _country_obj(parent["country_name"]) if parent.get("country_name") else None
        )
        stmt = make_entity_statement(
            source_id=_BG_SOURCE,
            local_id=f"{eik}:foreign-trader",
            name=parent["name"],
            jurisdiction=(country["name"], country.get("code")) if country else None,
            source_url=source_url,
        )
        bits = [str(parent[k]) for k in ("register", "number") if parent.get(k)]
        if bits:
            annotate(stmt, commenting(
                pointer("recordDetails"),
                "The Bulgarian Commercial Register files the foreign trader's registration as "
                + ", ".join(bits) + "; OpenCheck does not assert it as an identifier.",
            ))
        yield stmt
        yield make_relationship_statement(
            source_id=_BG_SOURCE,
            local_id=f"{eik}:foreign-trader-rel",
            subject_statement_id=company_id,
            interested_party_statement_id=stmt["recordId"],
            interested_party_type="entity",
            interests=[{
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct",
                "details": "The branch registered in Bulgaria is part of this foreign trader",
            }],
            source_url=source_url,
            statement_date=statement_date,
        )
