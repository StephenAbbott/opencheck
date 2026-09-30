"""Botswana — CIPA company and beneficial ownership register → BODS v0.4.

Reads the records ``scripts/build_cipa_botswana_index.py`` harvests from
CIPA's public register pages: beneficial owners, shareholders and directors,
name and role only (Stephen, 30 Sept 2026). Not CIPA's own BODS v0.3 export,
which drops the subject's UIN, every interest date and the free-text detail of
an "other" interest, and leaves out legal shareholders.

One party per person or organisation
------------------------------------
The same person is often a director, a shareholder and a beneficial owner of
one company. They get **one** person statement and **one** relationship whose
interests say each role, each with its own dates and its own
``beneficialOwnershipOrControl``. People are matched by name within one
company only — the register gives no identifier for a person, so the same name
at two companies is not claimed to be one person. An organisation with a CIPA
UIN is one party wherever it appears; without one it is matched by name, legal
form ignored, within the company.

Dates and endings
-----------------
Every role carries the register's start date and, for a former holder, its
end date. An ended role keeps its relationship with ``endDate`` on the
interest; OpenCheck draws it faint (``bods/lifecycle.py``) rather than dropping
it.

Countries are carried as filed, including ``IO`` (British Indian Ocean
Territory), which is its own territory, distinct from the British Virgin
Islands and the United Kingdom (Stephen, 30 Sept 2026).
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from ... import names as _names
from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _country_obj,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)

_SOURCE_ID = "cipa_botswana"
_SOURCE_URL = "https://www.cipa.co.bw"
_SCHEME = "BW-CIPA"
_SCHEME_NAME = "Companies and Intellectual Property Authority (Botswana)"

#: CIPA's nature-of-interest codelist (``DC_natureOfInterestType``) → BODS
#: v0.4 interest types. Most are already v0.4 codes; the rest are CIPA's own
#: and keep CIPA's wording in ``details``.
_CIPA_INTEREST = {
    "other": ("otherInfluenceOrControl", None),
    "financier": ("otherInfluenceOrControl", "Financier"),
    "shareholderBotswanaCompany": (
        "shareholding", "Recorded shareholder is a Botswana company",
    ),
    "shareholderForeignCompany": (
        "shareholding", "Recorded shareholder is a foreign company",
    ),
    # v0.3 spelling; v0.4 is singular.
    "rightsToProfitOrIncomeFromAssets": ("rightToProfitOrIncomeFromAssets", None),
}
_V04_INTEREST = frozenset({
    "shareholding", "votingRights", "appointmentOfBoard", "otherInfluenceOrControl",
    "seniorManagingOfficial", "settlor", "trustee", "protector",
    "beneficiaryOfLegalArrangement", "rightsToSurplusAssetsOnDissolution",
    "rightsToProfitOrIncome", "rightsGrantedByContract",
    "conditionalRightsGrantedByContract", "controlViaCompanyRulesOrArticles",
    "controlByLegalFramework", "boardMember", "boardChair",
    "enjoymentAndUseOfAssets", "rightToProfitOrIncomeFromAssets",
})

#: CIPA's own entity types (``bodsOtherEntityType``, a BODS v0.3 list) →
#: v0.4. CIPA defines ``legalEntity`` as "uniquely identified in an official
#: register", which is v0.4's ``registeredEntity``.
_ENTITY_TYPES = {
    "legalEntity": "registeredEntity",
    "legalOther": "legalEntity",
    "arrangement": "arrangement",
    "state": "state",
    "stateBody": "stateBody",
}

_COMPANY_TYPES = {
    "private": "Private company",
    "public": "Public company",
    "external": "External company",
    "close": "Close company",
}
_SUB_TYPES = {
    "limitedByShares": "limited by shares",
    "limitedByGuarantee": "limited by guarantee",
    "unlimited": "unlimited",
}
_SUB_TYPE_WORDS = {
    "privateCompany": "Private company",
    "publicCompany": "Public company",
    "externalCompany": "External company",
    "stateOwnedCompany": "State-owned company",
    "nonProfitCompany": "Non-profit company",
}

#: Legal-form words ignored when matching two spellings of one organisation
#: ("Mining And Power Resources Company (Pty)Ltd" and "… Proprietary Limited").
_FORM_WORDS = frozenset({"pty", "proprietary", "ltd", "limited", "plc"})


def _org_key(name: str) -> str:
    tokens = (_names.org_name_residue(name) or _names.normalise_name(name)).split()
    while tokens and tokens[-1] in _FORM_WORDS:
        tokens.pop()
    return " ".join(tokens)


def _person_key(name: str) -> str:
    return _names.normalise_name(name)


def _jurisdiction(code: str | None) -> tuple[str, str | None] | None:
    obj = _country_obj(code or "")
    if not obj:
        return None
    return obj["name"], obj.get("code")


def _dated(interest: dict[str, Any], start: str | None, end: str | None) -> dict[str, Any]:
    if start:
        interest["startDate"] = start
    if end:
        interest["endDate"] = end
    return interest


def _bo_interests(bo: dict[str, Any], record_kind: str) -> list[dict[str, Any]]:
    """A beneficial owner's declared interests, in v0.4 terms."""
    out: list[dict[str, Any]] = []
    for raw in bo.get("interests") or [{"type": None}]:
        code = raw.get("type")
        if code in _CIPA_INTEREST:
            itype, label = _CIPA_INTEREST[code]
        elif code in _V04_INTEREST:
            itype, label = code, None
        else:
            itype, label = "unknownInterest", None
        details = raw.get("other_reason") or label
        interest: dict[str, Any] = {"type": itype, "directOrIndirect": "unknown"}
        share = raw.get("voting") if itype == "votingRights" else raw.get("share")
        if _valid_share(share):
            interest["share"] = {"exact": share}
        if details:
            interest["details"] = details
        out.append(_dated(
            set_beneficial_ownership(interest, _SOURCE_ID, record_kind=record_kind),
            bo.get("start"), bo.get("end"),
        ))
        voting = raw.get("voting")
        if itype != "votingRights" and _valid_share(voting):
            out.append(_dated(
                set_beneficial_ownership(
                    {"type": "votingRights", "directOrIndirect": "unknown",
                     "share": {"exact": voting}},
                    _SOURCE_ID, record_kind=record_kind,
                ),
                bo.get("start"), bo.get("end"),
            ))
    return out


def _valid_share(value: Any) -> bool:
    return (
        isinstance(value, (int, float)) and not isinstance(value, bool)
        and 0 < value <= 100
    )


def _shareholder_interests(
    sh: dict[str, Any], total: Any, record_kind: str,
) -> list[dict[str, Any]]:
    interest: dict[str, Any] = {"type": "shareholding", "directOrIndirect": "direct"}
    if _valid_share(sh.get("percentage")):
        interest["share"] = {"exact": sh["percentage"]}
    if sh.get("shares"):
        of = f" of {int(total):,}" if isinstance(total, (int, float)) and total else ""
        interest["details"] = f"Registered shareholder: {int(sh['shares']):,}{of} shares"
    else:
        interest["details"] = "Registered shareholder"
    out = [_dated(
        set_beneficial_ownership(interest, _SOURCE_ID, record_kind=record_kind),
        sh.get("start"), sh.get("end"),
    )]
    if sh.get("nominee"):
        out.append(_dated(
            set_beneficial_ownership(
                {"type": "nominee", "directOrIndirect": "direct",
                 "details": _nominee_details("Holds these shares as a nominee", sh)},
                _SOURCE_ID, record_kind=record_kind,
            ),
            sh.get("start"), sh.get("end"),
        ))
    return out


def _nominee_details(lead: str, party: dict[str, Any]) -> str:
    nominator = party.get("nominator")
    if nominator:
        return f"{lead}; CIPA records {nominator} as the nominator"
    return f"{lead}; CIPA records that a nominator exists but publishes no name"


def _director_interests(d: dict[str, Any]) -> list[dict[str, Any]]:
    out = [_dated(
        set_beneficial_ownership(
            {"type": "boardMember", "directOrIndirect": "direct", "details": "Director"},
            _SOURCE_ID, record_kind="director",
        ),
        d.get("start"), d.get("end"),
    )]
    if d.get("nominee"):
        out.append(_dated(
            set_beneficial_ownership(
                {"type": "nominee", "directOrIndirect": "direct",
                 "details": _nominee_details("Acts as a nominee director", d)},
                _SOURCE_ID, record_kind="director",
            ),
            d.get("start"), d.get("end"),
        ))
    return out


def _company_details(record: dict[str, Any]) -> str | None:
    kind = _COMPANY_TYPES.get(record.get("entity_type") or "")
    sub = _SUB_TYPES.get(record.get("sub_type") or "")
    if kind and sub:
        return f"{kind}, {sub}"
    return kind or None


class _Party:
    """One person or organisation and everything it holds in the company."""

    def __init__(self, kind: str, name: str, local_id: str) -> None:
        self.kind = kind          # "person" | "entity"
        self.name = name
        self.local_id = local_id
        self.nationalities: list[str] = []
        self.country: str | None = None
        self.uin: str | None = None
        self.entity_type: str | None = None
        self.sub_type: str | None = None
        self.company_number: str | None = None
        self.interests: list[dict[str, Any]] = []

    def add_nationalities(self, codes: Iterable[str | None]) -> None:
        for code in codes:
            if code and code not in self.nationalities:
                self.nationalities.append(code)


def _parties(record: dict[str, Any]) -> list[_Party]:
    uin = record["uin"]
    total = record.get("total_shares")
    people: dict[str, _Party] = {}
    orgs: dict[str, _Party] = {}
    order: list[_Party] = []

    def person(name: str) -> _Party:
        key = _person_key(name)
        if key not in people:
            people[key] = _Party("person", name, f"{uin}:person:{key}")
            order.append(people[key])
        return people[key]

    def org(name: str, org_uin: str | None) -> _Party:
        key = f"uin:{org_uin}" if org_uin else f"name:{_org_key(name)}"
        name_key = f"name:{_org_key(name)}"
        party = orgs.get(key) or orgs.get(name_key)
        if party is None:
            local = f"entity:{org_uin}" if org_uin else f"{uin}:entity:{_org_key(name)}"
            party = _Party("entity", name, local)
            order.append(party)
        orgs[key] = orgs[name_key] = party
        if org_uin and not party.uin:
            party.uin = org_uin
        return party

    # Shareholders first: the share register names a Botswana company with its
    # UIN, so a beneficial owner declared under a looser spelling of the same
    # name joins the party that carries the identifier.
    for sh in record.get("shareholders") or []:
        name = (sh.get("name") or "").strip()
        if not name:
            continue
        if sh.get("kind") == "IndividualShareholder":
            p = person(name)
            p.add_nationalities([sh.get("nationality")])
            kind = "shareholder"
        else:
            p = org(name, sh.get("uin"))
            if sh.get("kind") == "EntityShareholder":
                # A company on CIPA's register — which includes external
                # companies (De Beers UK Limited holds a UIN), so the UIN says
                # nothing about where the company was incorporated.
                p.entity_type = p.entity_type or "legalEntity"
            else:
                p.country = p.country or sh.get("country")
            kind = "corporate_shareholder"
        p.interests.extend(_shareholder_interests(sh, total, kind))
        if sh.get("nominee") and sh.get("nominator"):
            nom = person(sh["nominator"])
            nom.interests.append(_dated(
                set_beneficial_ownership(
                    {"type": "nominator", "directOrIndirect": "indirect",
                     "details": f"Recorded by CIPA as the nominator of shareholder {name}"},
                    _SOURCE_ID, record_kind="nominator",
                ),
                sh.get("start"), sh.get("end"),
            ))

    for bo in record.get("beneficial_owners") or []:
        name = (bo.get("name") or "").strip()
        if not name:
            continue
        if bo.get("kind") == "IndividualBeneficialOwner":
            p = person(name)
            p.add_nationalities(bo.get("nationalities") or [])
            p.interests.extend(_bo_interests(bo, "bo_individual"))
        else:
            p = org(name, bo.get("uin"))
            p.country = p.country or bo.get("country")
            p.entity_type = bo.get("entity_type") or p.entity_type
            p.sub_type = p.sub_type or bo.get("sub_type")
            p.company_number = p.company_number or bo.get("company_number")
            p.interests.extend(_bo_interests(bo, "bo_entity"))

    for d in record.get("directors") or []:
        name = (d.get("name") or "").strip()
        if not name:
            continue
        p = person(name)
        p.add_nationalities([d.get("nationality")])
        p.interests.extend(_director_interests(d))
        if d.get("nominee") and d.get("nominator"):
            nom = person(d["nominator"])
            nom.interests.append(_dated(
                set_beneficial_ownership(
                    {"type": "nominator", "directOrIndirect": "indirect",
                     "details": f"Recorded by CIPA as the nominator of director {name}"},
                    _SOURCE_ID, record_kind="nominator",
                ),
                d.get("start"), d.get("end"),
            ))
    return order


def _entity_identifiers(p: _Party) -> list[dict[str, str]]:
    idents: list[dict[str, str]] = []
    if p.uin:
        idents.append({"id": p.uin, "scheme": _SCHEME, "schemeName": _SCHEME_NAME})
    if p.company_number and p.country == "ZA":
        # A foreign beneficial owner's own registration number, as filed with
        # CIPA. Only South Africa's is given a scheme: it is the one country
        # the harvest meets and CIPC's number shape is unambiguous.
        idents.append({
            "id": p.company_number,
            "scheme": "ZA-CIPC",
            "schemeName": "Companies and Intellectual Property Commission (South Africa)",
        })
    return idents


def map_cipa_botswana(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CipaBotswanaAdapter bundle to BODS v0.4 statements.

    One entity statement for the Botswana company; per beneficial owner,
    shareholder, director or recorded nominator, one person or entity
    statement and one relationship statement carrying every role that party
    holds (see the module docstring).
    """
    if not bundle or bundle.get("is_stub"):
        return
    record: dict[str, Any] = bundle.get("record") or {}
    uin = str(record.get("uin") or "").strip()
    name = (record.get("company") or "").strip()
    if not uin or not name:
        return

    identifiers = [{"id": uin, "scheme": _SCHEME, "schemeName": _SCHEME_NAME}]
    old = (record.get("old_number") or "").strip()
    if old:
        identifiers.append({
            "id": old,
            "scheme": _SCHEME,
            "schemeName": f"{_SCHEME_NAME} — company number before re-registration",
        })
    subject = make_entity_statement(
        source_id=_SOURCE_ID,
        local_id=uin,
        name=name,
        jurisdiction=("Botswana", "BW"),
        identifiers=identifiers,
        founding_date=record.get("incorporated"),
        entity_details=_company_details(record),
        source_url=_SOURCE_URL,
    )
    status = (record.get("status") or "").strip()
    _liveness.apply_register_status(
        subject,
        source_label=SOURCE_NAMES[_SOURCE_ID],
        liveness=_liveness.classify(
            status,
            live=("registered",),
            pending=("liquidation", "judicialManagement"),
            terminal=("removed", "amalgamated"),
        ),
        raw=status or None,
        since=record.get("status_since"),
    )
    yield subject
    subject_id = subject["statementId"]

    for p in _parties(record):
        if not p.interests:
            continue
        if p.kind == "person":
            nationalities = [c for c in (_country_obj(n) for n in p.nationalities) if c]
            party = make_person_statement(
                source_id=_SOURCE_ID,
                local_id=p.local_id,
                full_name=p.name,
                nationalities=nationalities,
                source_url=_SOURCE_URL,
            )
        else:
            details = _SUB_TYPE_WORDS.get(p.sub_type or "")
            party = make_entity_statement(
                source_id=_SOURCE_ID,
                local_id=p.local_id,
                name=p.name,
                jurisdiction=_jurisdiction(p.country),
                identifiers=_entity_identifiers(p),
                entity_type=_ENTITY_TYPES.get(p.entity_type or "", "legalEntity"),
                entity_details=details,
                source_url=_SOURCE_URL,
            )
        yield party
        yield make_relationship_statement(
            source_id=_SOURCE_ID,
            local_id=f"{uin}:{p.local_id}",
            subject_statement_id=subject_id,
            interested_party_statement_id=party["statementId"],
            interested_party_type=p.kind,
            interests=p.interests,
            source_url=_SOURCE_URL,
        )


_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def cipa_counts(record: dict[str, Any], today: str) -> dict[str, int]:
    """Current beneficial owners, shareholders and directors — a role is
    current when it has no end date, or one after *today*."""
    def current(items: list[dict[str, Any]]) -> int:
        return sum(
            1 for i in items
            if not (i.get("end") and _ISO.match(i["end"]) and i["end"] <= today)
        )
    return {
        "beneficial_owners": current(record.get("beneficial_owners") or []),
        "shareholders": current(record.get("shareholders") or []),
        "directors": current(record.get("directors") or []),
    }
