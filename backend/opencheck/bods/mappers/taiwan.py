"""Taiwan — Ministry of Economic Affairs company register (GCIS open data) → BODS.

One entity statement for the company, and for every holder on the register of
directors and supervisors a party statement and one relationship. Stephen's
decisions of 29 Oct 2026 (the Notion ticket "Taiwan / MOEA GCIS company
registration open data"), which are what this module implements:

* **A. Holdings are shareholdings.** ``Person_Shareholding`` above zero is a
  ``shareholding`` interest beside the seat, with a percentage computed
  against the issued shares (``Equity_Amt``, a 股份有限公司) or the total
  capital (``Capital_Stock_Amount``, a 有限公司), and the filed figure and
  the denominator in ``details``. No percentage when there is no denominator
  (a foreign company's Taiwan branch has none) or when the filed holdings add
  up to more than it. The details always say these are directors' and
  supervisors' holdings — the register publishes no list of shareholders.
* **B. A legal-person director holds the seat.** When ``Juristic_Person_Name``
  is set the legal person (法人董事 / 法人代表) is an entity statement with
  the seat — one interest per seat its representatives fill — and **one**
  shareholding: the register repeats its holding on every representative's
  row, so it is taken once. Each representative is a person statement whose
  one interest names the legal person it represents, never a second
  shareholding.
* **C. Supervisors and representatives are officers.** BODS has no
  supervisor role, and a 監察人 is an individual statutory auditor, not a
  board, so it is ``otherInfluenceOrControl`` with the Phase 340 officer
  marker. So are a foreign company's representative in Taiwan and its
  litigation agent.
* **E.** A state holder (行政院國家發展基金管理會, a ministry) stays a
  ``legalEntity``: Phase 240 forbids classifying the state by name.

``beneficialOwnershipOrControl`` is never set (the GEMI rule): this is a
commercial register, and Taiwan's 10%-shareholder filings (Company Act art.
22-1) are restricted to the authorities.

The register publishes the company name in Chinese only. GLEIF's English and
transliterated names are on GLEIF's own statement, which merges with this one
on the ``TW-CR`` identifier; they are not copied here, because the register
did not publish them.
"""

from __future__ import annotations

from typing import Any

from .. import liveness as _liveness
from ..annotations import annotate, commenting, pointer, transformation
from ..officer_roles import mark_officer
from ..statements import (
    BODSBundle,
    SOURCE_NAMES,
    _addr,
    content_addressed,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    record_date,
    set_beneficial_ownership,
)


def _tw() -> Any:
    from ...sources import gcis_taiwan  # local import avoids circular

    return gcis_taiwan


#: 公司狀態 wording → liveness. The 33 descriptions of the platform's status
#: code table (``/od/cmpStatusCodeData``, read 29 Oct 2026) plus 核准登記,
#: which foreign companies' Taiwan branches carry and the table omits. A
#: dissolution, revocation or bankruptcy opens a liquidation (清算) that the
#: company survives until it is completed, so those are pending; only a
#: completed liquidation, a merger or division dissolution, or a cancelled
#: incorporation is terminal.
TW_STATUS_LIVENESS: dict[str, _liveness.LivenessClass] = {
    "核准設立": _liveness.LIVE,
    "核准登記": _liveness.LIVE,
    "核准設立，但已命令解散": _liveness.PENDING,
    "重整": _liveness.PENDING,
    "解散": _liveness.PENDING,
    "撤銷": _liveness.PENDING,
    "破產": _liveness.PENDING,
    "合併解散": _liveness.TERMINAL,
    "撤回認許": _liveness.PENDING,
    "廢止": _liveness.PENDING,
    "廢止認許": _liveness.PENDING,
    "解散已清算完結": _liveness.TERMINAL,
    "撤銷已清算完結": _liveness.TERMINAL,
    "廢止已清算完結": _liveness.TERMINAL,
    "撤回認許已清算完結": _liveness.TERMINAL,
    "撤銷認許已清算完結": _liveness.TERMINAL,
    "廢止認許已清算完結": _liveness.TERMINAL,
    "撤銷認許": _liveness.PENDING,
    "分割解散": _liveness.TERMINAL,
    "終止破產": _liveness.UNKNOWN,
    "中止破產": _liveness.UNKNOWN,
    "塗銷破產": _liveness.UNKNOWN,
    "破產程序終結(終止)": _liveness.UNKNOWN,
    "破產程序終結(終止)清算中": _liveness.PENDING,
    "破產已清算完結": _liveness.TERMINAL,
    "接管": _liveness.PENDING,
    "撤銷無需清算": _liveness.TERMINAL,
    "撤銷許可": _liveness.PENDING,
    "廢止許可": _liveness.PENDING,
    "撤銷許可已清算完結": _liveness.TERMINAL,
    "廢止許可已清算完結": _liveness.TERMINAL,
    "清理": _liveness.PENDING,
    "撤銷公司設立": _liveness.TERMINAL,
    "清理完結": _liveness.TERMINAL,
}

#: A foreign company's name in Taiwan starts with its nationality (Company Act
#: art. 370): 英商 (British), 英屬開曼群島商 (Cayman Islands)… The prefix is the
#: register's own statement of the home jurisdiction. Longest first, so
#: 英屬維京群島商 is not read as 英商. Only prefixes that name one country.
TW_FOREIGN_PREFIXES: tuple[tuple[str, str, str], ...] = tuple(sorted(
    (
        ("英屬開曼群島商", "KY", "Cayman Islands"),
        ("開曼群島商", "KY", "Cayman Islands"),
        ("英屬維京群島商", "VG", "British Virgin Islands"),
        ("英屬維爾京群島商", "VG", "British Virgin Islands"),
        ("英屬百慕達商", "BM", "Bermuda"),
        ("百慕達商", "BM", "Bermuda"),
        ("英屬安圭拉商", "AI", "Anguilla"),
        ("英屬澤西島商", "JE", "Jersey"),
        ("英屬根西島商", "GG", "Guernsey"),
        ("英屬曼島商", "IM", "Isle of Man"),
        ("英商", "GB", "United Kingdom"),
        ("美商", "US", "United States"),
        ("日商", "JP", "Japan"),
        ("德商", "DE", "Germany"),
        ("法商", "FR", "France"),
        ("荷蘭商", "NL", "Netherlands"),
        ("瑞士商", "CH", "Switzerland"),
        ("瑞典商", "SE", "Sweden"),
        ("丹麥商", "DK", "Denmark"),
        ("挪威商", "NO", "Norway"),
        ("芬蘭商", "FI", "Finland"),
        ("比利時商", "BE", "Belgium"),
        ("盧森堡商", "LU", "Luxembourg"),
        ("奧地利商", "AT", "Austria"),
        ("義大利商", "IT", "Italy"),
        ("西班牙商", "ES", "Spain"),
        ("愛爾蘭商", "IE", "Ireland"),
        ("新加坡商", "SG", "Singapore"),
        ("香港商", "HK", "Hong Kong"),
        ("澳門商", "MO", "Macao"),
        ("韓商", "KR", "Korea, Republic of"),
        ("澳商", "AU", "Australia"),
        ("澳大利亞商", "AU", "Australia"),
        ("紐西蘭商", "NZ", "New Zealand"),
        ("加拿大商", "CA", "Canada"),
        ("馬來西亞商", "MY", "Malaysia"),
        ("泰國商", "TH", "Thailand"),
        ("菲律賓商", "PH", "Philippines"),
        ("印尼商", "ID", "Indonesia"),
        ("越南商", "VN", "Viet Nam"),
        ("印度商", "IN", "India"),
        ("以色列商", "IL", "Israel"),
        ("薩摩亞商", "WS", "Samoa"),
        ("模里西斯商", "MU", "Mauritius"),
        ("賴比瑞亞商", "LR", "Liberia"),
        ("巴拿馬商", "PA", "Panama"),
        ("塞席爾商", "SC", "Seychelles"),
        ("貝里斯商", "BZ", "Belize"),
        ("馬紹爾群島商", "MH", "Marshall Islands"),
        ("南非商", "ZA", "South Africa"),
        ("巴西商", "BR", "Brazil"),
        ("墨西哥商", "MX", "Mexico"),
    ),
    key=lambda row: -len(row[0]),
))

#: The legal-form suffixes of the Company Act, longest first (股份有限公司
#: before 有限公司).
TW_LEGAL_FORMS: tuple[tuple[str, str], ...] = (
    ("股份有限公司", "Company limited by shares (股份有限公司)"),
    ("有限公司", "Limited company (有限公司)"),
    ("無限公司", "Unlimited company (無限公司)"),
    ("兩合公司", "Unlimited company with limited liability shareholders (兩合公司)"),
)

#: 職稱 on the directors and supervisors register → (BODS interest type,
#: the position in words, officer marker or None). Board positions are board
#: roles; everything else is an officer (decision C), never a board member.
TW_POSITIONS: dict[str, tuple[str, str, bool]] = {
    "董事長": ("boardChair", "Chair of the board (董事長)", False),
    "副董事長": ("boardMember", "Vice-chair of the board (副董事長)", False),
    "常務董事": ("boardMember", "Managing director (常務董事)", False),
    "獨立董事": ("boardMember", "Independent director (獨立董事)", False),
    "董事": ("boardMember", "Director (董事)", False),
    "監察人": ("otherInfluenceOrControl", "Supervisor (監察人)", True),
    "在中華民國境內負責人/代表人": (
        "otherInfluenceOrControl",
        "Representative in Taiwan (在中華民國境內負責人/代表人)",
        True,
    ),
    "訴訟代理人及非訴訟代理人": (
        "otherInfluenceOrControl",
        "Litigation and non-litigation agent (訴訟代理人及非訴訟代理人)",
        True,
    ),
    "清算人": ("otherInfluenceOrControl", "Liquidator (清算人)", True),
    "臨時管理人": ("otherInfluenceOrControl", "Provisional administrator (臨時管理人)", True),
    "重整人": ("otherInfluenceOrControl", "Reorganiser (重整人)", True),
    "重整監督人": ("otherInfluenceOrControl", "Reorganisation supervisor (重整監督人)", True),
}

#: What every shareholding's details end with.
TW_HOLDINGS_CAVEAT = (
    "Directors' and supervisors' holdings only, as Company Act art. 393 "
    "requires them to be published; the register publishes no list of "
    "shareholders."
)


def tw_foreign_prefix(name: str) -> tuple[str, str, str] | None:
    """The nationality prefix a foreign company's Taiwanese name opens with:
    ``(prefix, ISO code, country name)``, or None."""
    text = (name or "").strip()
    for prefix, code, country in TW_FOREIGN_PREFIXES:
        if text.startswith(prefix):
            return prefix, code, country
    return None


def tw_legal_form(name: str) -> str | None:
    text = (name or "").strip()
    for suffix, words in TW_LEGAL_FORMS:
        if text.endswith(suffix):
            return words
    return None


def tw_is_limited_company(name: str) -> bool:
    """A 有限公司 (the holding is a capital contribution), not a 股份有限公司."""
    text = (name or "").strip()
    return text.endswith("有限公司") and not text.endswith("股份有限公司")


def tw_position(raw: str) -> tuple[str, str, bool]:
    text = (raw or "").strip()
    if text in TW_POSITIONS:
        return TW_POSITIONS[text]
    return ("otherInfluenceOrControl", text or "Position not stated", True)


def tw_holding_denominator(company: dict[str, Any]) -> tuple[int | None, str]:
    """What a holding is a share of: ``(denominator, unit)`` where unit is
    ``"shares"`` or ``"NT$"``, or ``(None, unit)`` when the register gives
    nothing to divide by."""
    int_field = _tw().int_field
    name = str(company.get("Company_Name") or "")
    if tw_is_limited_company(name):
        capital = int_field(company.get("Capital_Stock_Amount"))
        return (capital if capital and capital > 0 else None), "NT$"
    issued = int_field(company.get("Equity_Amt"))
    if issued and issued > 0:
        return issued, "shares"
    paid, par = int_field(company.get("Paid_In_Capital_Amount")), int_field(company.get("Share_Val"))
    if paid and par and paid > 0 and par > 0 and paid % par == 0:
        return paid // par, "shares"
    return None, "shares"


def tw_holders(directors: list[dict[str, Any]]) -> dict[str, int]:
    """Each holder once — a legal person by its name, a natural person by
    theirs — with its filed holding (the register repeats a legal person's
    holding on every representative's row, so the largest reading is taken,
    never a sum)."""
    int_field = _tw().int_field
    out: dict[str, int] = {}
    for row in directors:
        key = str(row.get("Juristic_Person_Name") or "").strip() or str(
            row.get("Person_Name") or ""
        ).strip()
        if not key:
            continue
        value = int_field(row.get("Person_Shareholding")) or 0
        out[key] = max(out.get(key, 0), max(value, 0))
    return out


def tw_percentages(company: dict[str, Any], directors: list[dict[str, Any]]) -> dict[str, float] | None:
    """Each holder's percentage, or None when none can be given: no
    denominator, or the filed holdings add up to more than it (a stale issued
    share count — 創維風力發電 on 29 Oct 2026)."""
    denominator, _unit = tw_holding_denominator(company)
    if not denominator:
        return None
    held = tw_holders(directors)
    if sum(held.values()) > denominator:
        return None
    return {key: round(value * 100.0 / denominator, 4) for key, value in held.items() if value > 0}


def _tw_holding_details(value: int, denominator: int | None, unit: str) -> str:
    if unit == "NT$":
        figure = f"Capital contribution of NT${value:,}"
        if denominator:
            figure += f" of NT${denominator:,} total capital"
    else:
        figure = f"{value:,} shares"
        if denominator:
            figure += f" of {denominator:,} issued"
    return f"{figure}, as filed on the register of directors and supervisors. {TW_HOLDINGS_CAVEAT}"


def _tw_seat_interest(raw_position: str, source_id: str, *, through: str | None = None) -> dict[str, Any]:
    interest_type, words, officer = tw_position(raw_position)
    details = words if not through else f"{words}; seat filled by its representative {through}"
    interest: dict[str, Any] = {"type": interest_type, "directOrIndirect": "direct", "details": details}
    if officer:
        mark_officer(interest, words)
    return set_beneficial_ownership(interest, source_id, asserted=None)


def _tw_shareholding_interest(
    value: int, denominator: int | None, unit: str, pct: float | None, source_id: str
) -> dict[str, Any]:
    interest: dict[str, Any] = {
        "type": "shareholding",
        "directOrIndirect": "direct",
        "details": _tw_holding_details(value, denominator, unit),
    }
    if pct is not None:
        interest["share"] = {"exact": pct}
    return set_beneficial_ownership(interest, source_id, asserted=None)


def _tw_percentage_note(index: int, unit: str) -> dict[str, Any]:
    basis = (
        "the company's total capital (Capital_Stock_Amount)"
        if unit == "NT$"
        else "the company's issued shares (Equity_Amt)"
    )
    return transformation(
        pointer("recordDetails", "interests", index, "share"),
        f"Percentage computed by OpenCheck from the filed holding and {basis}; "
        "the register publishes no percentage.",
    )


def _tw_no_percentage_note(company: dict[str, Any], directors: list[dict[str, Any]]) -> str | None:
    denominator, unit = tw_holding_denominator(company)
    total = sum(tw_holders(directors).values())
    if not total:
        return None
    if not denominator:
        return (
            "The register gives no capital or issued-share figure for this "
            "company, so the filed holdings are stated without a percentage."
        )
    if total > denominator:
        figure = "NT$" if unit == "NT$" else ""
        return (
            f"The holdings filed on the register of directors and supervisors add "
            f"up to {figure}{total:,}, more than the {figure}{denominator:,} the "
            "company's record gives, so no percentage is stated."
        )
    return None


def _tw_entity_details(name: str, company: dict[str, Any]) -> str | None:
    parts: list[str] = []
    prefix = tw_foreign_prefix(name)
    if prefix:
        parts.append(f"Foreign company registered in Taiwan ({prefix[0]}, {prefix[2]})")
    form = tw_legal_form(name)
    if form:
        parts.append(form)
    authority = str(company.get("Register_Organization_Desc") or "").strip()
    if authority:
        parts.append(f"registered with {authority}")
    return " · ".join(parts) or None


def _tw_suspension_note(company: dict[str, Any]) -> str | None:
    parse = _tw().parse_roc_date
    begin, end = parse(company.get("Sus_Beg_Date")), parse(company.get("Sus_End_Date"))
    case = str(company.get("Case_Status_Desc") or "").strip()
    parts: list[str] = []
    if begin:
        span = f"from {begin}" + (f" to {end}" if end else "")
        parts.append(f"The register records a suspension of business (停業) {span}.")
    if case:
        parts.append(f"The register records a case in progress: {case}.")
    return " ".join(parts) or None


@content_addressed
def map_gcis_taiwan(bundle: dict[str, Any]) -> BODSBundle:
    """Map a GcisTaiwanAdapter fetch bundle to BODS v0.4 statements."""
    statements = BODSBundle()
    company = bundle.get("company")
    if not bundle or bundle.get("is_stub") or not isinstance(company, dict):
        return statements
    tw = _tw()

    ubn = str(company.get("Business_Accounting_NO") or bundle.get("tw_ubn") or "").strip()
    name = str(company.get("Company_Name") or bundle.get("legal_name") or "").strip()
    if not ubn or not name:
        return statements

    source_id = bundle.get("source_id") or "gcis_taiwan"
    url = tw.company_url(ubn)
    directors_url = tw.dataset_url(tw.DIRECTORS_DATASET, f"Business_Accounting_NO eq {ubn}")
    as_of = record_date(tw.parse_roc_date(company.get("Change_Of_Approval_Data")))

    prefix = tw_foreign_prefix(name)
    jurisdiction = (prefix[2], prefix[1]) if prefix else ("Taiwan", "TW")
    address_text = str(company.get("Company_Location") or "").strip()

    entity = make_entity_statement(
        source_id=source_id,
        local_id=ubn,
        name=name,
        jurisdiction=jurisdiction,
        identifiers=[{"id": ubn, "scheme": tw.TW_CR_SCHEME, "schemeName": tw.TW_CR_SCHEME_NAME}],
        founding_date=tw.parse_roc_date(company.get("Company_Setup_Date")),
        addresses=[_addr("registered", address_text, "TW")] if address_text else [],
        entity_details=_tw_entity_details(name, company),
        source_url=url,
        statement_date=as_of,
    )
    status = str(company.get("Company_Status_Desc") or "").strip()
    liveness = TW_STATUS_LIVENESS.get(status, _liveness.UNKNOWN)
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["gcis_taiwan"],
        liveness=liveness,
        raw=status or None,
        since=tw.parse_roc_date(company.get("Revoke_App_Date")) if liveness != _liveness.LIVE else None,
    )
    suspension = _tw_suspension_note(company)
    if suspension:
        annotate(entity, commenting(pointer("recordDetails"), suspension))
    if not bundle.get("directors_read", True):
        annotate(entity, commenting(
            pointer("recordDetails"),
            "The register's directors and supervisors could not be read for "
            "this lookup; none are shown.",
        ))
    statements.extend([entity])

    directors = [row for row in (bundle.get("directors") or []) if isinstance(row, dict)]
    if not directors:
        return statements

    denominator, unit = tw_holding_denominator(company)
    pcts = tw_percentages(company, directors)
    no_pct = None if pcts is not None else _tw_no_percentage_note(company, directors)
    held = tw_holders(directors)

    # Group the rows: a legal person and its representatives, or a natural
    # person on their own account. Order is the register's.
    legal_persons: dict[str, list[dict[str, Any]]] = {}
    natural: dict[str, list[dict[str, Any]]] = {}
    for row in directors:
        person_name = str(row.get("Person_Name") or "").strip()
        juristic = str(row.get("Juristic_Person_Name") or "").strip()
        if juristic:
            legal_persons.setdefault(juristic, []).append(row)
        elif person_name:
            natural.setdefault(person_name, []).append(row)

    people: dict[str, dict[str, Any]] = {}

    def _person(person_name: str) -> dict[str, Any]:
        if person_name not in people:
            stmt = make_person_statement(
                source_id=source_id,
                local_id=f"{ubn}|person|{person_name}",
                full_name=person_name,
                source_url=directors_url,
                statement_date=as_of,
            )
            people[person_name] = stmt
            statements.extend([stmt])
        return people[person_name]

    def _holding(key: str, interests: list[dict[str, Any]], rel_notes: list[dict[str, Any]]) -> None:
        value = held.get(key, 0)
        if value <= 0:
            return
        pct = (pcts or {}).get(key)
        interests.append(_tw_shareholding_interest(value, denominator, unit, pct, source_id))
        if pct is not None:
            rel_notes.append(_tw_percentage_note(len(interests) - 1, unit))

    for juristic, rows in legal_persons.items():
        jp_prefix = tw_foreign_prefix(juristic)
        party = make_entity_statement(
            source_id=source_id,
            local_id=f"{ubn}|legal-person|{juristic}",
            name=juristic,
            jurisdiction=(jp_prefix[2], jp_prefix[1]) if jp_prefix else None,
            entity_type="legalEntity",
            entity_details=(
                f"Legal person holding a seat (法人董事/法人監察人) on {name}"
            ),
            source_url=directors_url,
            statement_date=as_of,
        )
        statements.extend([party])
        interests = [
            _tw_seat_interest(
                str(row.get("Person_Position_Name") or ""),
                source_id,
                through=str(row.get("Person_Name") or "").strip() or None,
            )
            for row in rows
        ]
        notes: list[dict[str, Any]] = []
        _holding(juristic, interests, notes)
        rel = make_relationship_statement(
            source_id=source_id,
            local_id=f"{ubn}|legal-person-seat|{juristic}",
            subject_statement_id=entity["recordId"],
            interested_party_statement_id=party["recordId"],
            interested_party_type="entity",
            interests=interests,
            source_url=directors_url,
            statement_date=as_of,
        )
        annotate(rel, *notes)
        if no_pct and held.get(juristic, 0) > 0:
            annotate(rel, commenting(pointer("recordDetails", "interests"), no_pct))
        statements.extend([rel])

        for row in rows:
            rep_name = str(row.get("Person_Name") or "").strip()
            if not rep_name:
                continue
            rep = _person(rep_name)
            _type, words, _officer = tw_position(str(row.get("Person_Position_Name") or ""))
            label = f"Representative of {juristic}: {words}"
            rep_interest: dict[str, Any] = {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct",
                "details": (
                    f"Represents the legal person {juristic}, which holds the seat; "
                    "the register files the legal person's holding, not the "
                    "representative's."
                ),
            }
            mark_officer(rep_interest, label)
            statements.extend([make_relationship_statement(
                source_id=source_id,
                local_id=f"{ubn}|representative|{juristic}|{rep_name}|{words}",
                subject_statement_id=entity["recordId"],
                interested_party_statement_id=rep["recordId"],
                interested_party_type="person",
                interests=[set_beneficial_ownership(rep_interest, source_id, asserted=None)],
                source_url=directors_url,
                statement_date=as_of,
            )])

    for person_name, rows in natural.items():
        person = _person(person_name)
        interests = [
            _tw_seat_interest(str(row.get("Person_Position_Name") or ""), source_id) for row in rows
        ]
        notes = []
        _holding(person_name, interests, notes)
        rel = make_relationship_statement(
            source_id=source_id,
            local_id=f"{ubn}|seat|{person_name}",
            subject_statement_id=entity["recordId"],
            interested_party_statement_id=person["recordId"],
            interested_party_type="person",
            interests=interests,
            source_url=directors_url,
            statement_date=as_of,
        )
        annotate(rel, *notes)
        if no_pct and held.get(person_name, 0) > 0:
            annotate(rel, commenting(pointer("recordDetails", "interests"), no_pct))
        statements.extend([rel])

    return statements
