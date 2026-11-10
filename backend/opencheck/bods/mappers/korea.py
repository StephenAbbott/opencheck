"""South Korea — Open DART disclosure filings → BODS.

One entity statement for the filer, and three kinds of party from its
filings. Stephen's decisions of 10 Nov 2026 (the Notion ticket "South Korea /
Open DART disclosure data"), which are what this module implements:

* **Largest shareholder and related persons** (최대주주 및 특수관계인,
  ``hyslrSttus``) from the latest periodic report: one relationship per
  holder, one ``shareholding`` interest per class of shares, with the
  period-end percentage of that class (``trmend_posesn_stock_qota_rt``) and
  the relation the filing gives. A holding at zero at the period end ended
  within the period: its interest carries ``endDate`` (the period end), so a
  holder with nothing left is a closed record (Phase 317). The filing's
  total rows (계) are not parties.
* **Executives** (임원 현황, ``exctvSttus``) from the same report (decision D):
  name, position, registered or not, full-time or not, term end — and the
  birth year-month as a partial ``birthDate`` (the Companies House
  precedent). Sex and career history are not carried. Registered directors
  are ``boardMember`` (``boardChair`` for the board's chair), a
  representative director (대표이사) also ``seniorManagingOfficial``,
  unregistered executives ``seniorManagingOfficial``, a statutory auditor
  (감사) an officer-marked ``otherInfluenceOrControl``. Serving-only, like
  every officer list.
* **5% holding reports** (대량보유, ``majorstock``): the latest report per
  reporter. Its percentage is the reporter's **and its specially related
  persons'** combined holding (Samsung C&T reports 19.69% of Samsung
  Electronics where its own line in the largest-shareholder table is 5.11%),
  so it is said in ``details`` and never put in ``share``. A latest report
  under 5% is an exit: the interest ends on the report's day.

Korea has no public beneficial-ownership register:
``beneficialOwnershipOrControl`` is never set, and nothing here says owner.

**Person or entity.** The filings type no party. A largest-shareholder row is
an entity when its name carries a legal-form token (㈜, 주식회사, 재단 …) or
its relation names an affiliate or a foundation; a 5% reporter is a person
only when its name is two or three Hangul syllables with no such token (the
shape of a Korean personal name), and takes the type of a largest-shareholder
row of the same name when there is one. Either reading is said in a
``commenting`` annotation. A person who is on both the shareholder table and
the executives table of the same report is one node (joined by name within
the one filing, and said).

Names are in Hangul as filed; no romanisation is invented.
"""

from __future__ import annotations

import re
from typing import Any

from ..annotations import annotate, commenting, pointer
from ..officer_roles import mark_officer
from ..statements import (
    BODSBundle,
    _addr,
    content_addressed,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    record_date,
    set_beneficial_ownership,
)


def _kr() -> Any:
    from ...sources import dart_korea  # local import avoids circular

    return dart_korea


#: Legal-form and organisation tokens that make a party an entity.
KR_ORG_TOKENS = (
    "㈜", "(주)", "주식회사", "유한회사", "(유)", "유한책임회사", "합자회사", "합명회사",
    "재단", "법인", "공단", "공사", "은행", "보험", "증권", "투자", "자산운용", "펀드",
    "조합", "기금", "연금", "협회", "신탁", "캐피탈", "홀딩스", "지주",
)
_LATIN_ORG = re.compile(
    r"\b(inc|corp|corporation|co|ltd|limited|llc|lp|plc|fund|trust|bank|"
    r"capital|holdings?|group|management|advisors?|partners|sa|ag|gmbh|bv|nv)\b\.?",
    re.IGNORECASE,
)

#: Relations (``relate``) that make a largest-shareholder row an entity.
_ENTITY_RELATIONS = ("계열회사", "출연 재단", "출연재단", "재단")
#: Relation words → English. Anything else is said as filed.
KR_RELATION_WORDS = {
    "최대주주 본인": "the largest shareholder itself",
    "본인": "the largest shareholder itself",
    "최대주주의 특수관계인": "a person specially related to the largest shareholder",
    "특수관계인": "a person specially related to the largest shareholder",
    "계열회사": "an affiliate in the same business group",
    "계열회사 임원": "an officer of an affiliate",
    "출연 재단": "a foundation endowed by the group",
    "친인척": "a relative of the largest shareholder",
    "임원": "an officer",
    "자기주식": "the company's own shares",
}
#: Rows of the shareholder table that are no party.
_TOTAL_ROWS = frozenset({"계", "합계", "소계", "총계"})
#: The note a special account (특별계정) carries in the holder's name.
_SPECIAL_ACCOUNT = re.compile(r"\s*\(?\s*특별계정\s*\)?\s*$")

#: ``rgist_exctv_at`` → (English, registered director?).
KR_EXECUTIVE_KINDS = {
    "사내이사": ("inside director", True),
    "사외이사": ("outside director", True),
    "독립이사": ("independent director", True),
    "기타비상무이사": ("other non-executive director", True),
    "미등기": ("unregistered executive", False),
    "감사": ("statutory auditor", False),
}

KR_SHAREHOLDER_CAVEAT = (
    "Filed in the company's periodic report as the largest shareholder and its "
    "specially related persons (최대주주 및 특수관계인). Korea has no public "
    "beneficial-ownership register; this is a disclosure of holdings, not of "
    "beneficial ownership."
)


def kr_number(value: Any) -> float | None:
    """``"497,660,185"`` / ``"8.51"`` → a number; ``"-"`` or blank → None."""
    text = str(value or "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def kr_has_org_token(name: str) -> bool:
    text = str(name or "")
    return any(token in text for token in KR_ORG_TOKENS) or bool(_LATIN_ORG.search(text))


def kr_holder_name(raw: str) -> tuple[str, bool]:
    """A holder's name without the special-account note, and whether the row
    was the special account (an insurer's 특별계정)."""
    text = " ".join(str(raw or "").split())
    stripped = _SPECIAL_ACCOUNT.sub("", text).strip()
    return stripped, stripped != text


def kr_shareholder_is_entity(name: str, relate: str) -> bool:
    if kr_has_org_token(name):
        return True
    relation = " ".join(str(relate or "").split())
    return relation in _ENTITY_RELATIONS or relation.startswith("계열회사") and "임원" not in relation


def kr_reporter_is_person(name: str) -> bool:
    """A 5% reporter is read as a person only when its name has the shape of a
    Korean personal name: two or three Hangul syllables, no other characters."""
    text = str(name or "").strip()
    return bool(re.fullmatch(r"[가-힣]{2,3}", text)) and not kr_has_org_token(text)


def kr_name_key(name: str) -> str:
    """A party name for joining within one filing: legal-form tokens,
    brackets and spaces removed (삼성물산㈜ and 삼성물산 are one name)."""
    text = str(name or "")
    for token in ("㈜", "(주)", "주식회사", "(유)", "유한회사"):
        text = text.replace(token, "")
    return re.sub(r"[\s()]", "", text)


def kr_relation_words(relate: str) -> str:
    relation = " ".join(str(relate or "").split())
    if not relation:
        return "relation not stated"
    words = KR_RELATION_WORDS.get(relation)
    return f"{words} ({relation})" if words else f"relation as filed: {relation}"


def kr_birth_month(value: Any) -> str | None:
    """``"1960년 12월"`` → ``"1960-12"`` (a partial date), else None."""
    match = re.search(r"(\d{4})\D+(\d{1,2})", str(value or ""))
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    if not 1900 <= year <= 2100 or not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"


def kr_korean_date(value: Any) -> str | None:
    """``"2028년 03월 18일"`` → ``"2028-03-18"``."""
    match = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", str(value or ""))
    if not match:
        return None
    return _kr().parse_ymd("".join(f"{int(p):02d}" for p in match.groups()).rjust(8, "0"))


def kr_executive_kind(row: dict[str, Any]) -> tuple[str, bool]:
    raw = " ".join(str(row.get("rgist_exctv_at") or "").split())
    if raw in KR_EXECUTIVE_KINDS:
        return KR_EXECUTIVE_KINDS[raw]
    if "감사" in str(row.get("ofcps") or ""):
        return KR_EXECUTIVE_KINDS["감사"]
    return (f"executive ({raw})" if raw else "executive", "등기" in raw and "미등기" not in raw)


def kr_executive_interests(row: dict[str, Any], source_id: str) -> list[dict[str, Any]]:
    """The interests one executive row is mapped to."""
    kind, registered = kr_executive_kind(row)
    position = " ".join(str(row.get("ofcps") or "").split())
    duties = " ".join(str(row.get("chrg_job") or "").replace("ㆍ", " ").split())
    full_time = {"상근": "full-time", "비상근": "part-time"}.get(
        " ".join(str(row.get("fte_at") or "").split())
    )
    term_end = kr_korean_date(row.get("tenure_end_on"))
    parts = [f"{kind[0].upper()}{kind[1:]} ({' '.join(str(row.get('rgist_exctv_at') or '').split())})"]
    if position:
        parts.append(f"position {position}")
    if full_time:
        parts.append(full_time)
    if term_end:
        parts.append(f"term ends {term_end}")
    details = ", ".join(parts) + ", as filed in the periodic report's executives table (임원 현황)"

    interests: list[dict[str, Any]] = []
    if kind == "statutory auditor":
        interest = {"type": "otherInfluenceOrControl", "directOrIndirect": "direct", "details": details}
        mark_officer(interest, "Statutory auditor (감사)")
        interests.append(interest)
    elif registered:
        chair = "이사회 의장" in duties or "의장" == position
        interests.append({
            "type": "boardChair" if chair else "boardMember",
            "directOrIndirect": "direct",
            "details": details,
        })
        if "대표이사" in duties or "대표이사" in position:
            interests.append({
                "type": "seniorManagingOfficial",
                "directOrIndirect": "direct",
                "details": "Representative director (대표이사), as filed in the executives table",
            })
    else:
        interests.append({"type": "seniorManagingOfficial", "directOrIndirect": "direct", "details": details})
    return [set_beneficial_ownership(i, source_id, asserted=None) for i in interests]


def kr_shareholder_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The party rows of a shareholder table: total rows (계) and rows with no
    name dropped."""
    out = []
    for row in rows:
        name, _special = kr_holder_name(str(row.get("nm") or ""))
        if not name or name in _TOTAL_ROWS or row.get("relate") is None:
            continue
        out.append(row)
    return out


def kr_largest_shareholder(rows: list[dict[str, Any]]) -> tuple[str, float] | None:
    """The largest shareholder itself (최대주주 본인) and its period-end
    percentage of common shares, as filed on its own row (an insurer's special
    account (특별계정) holds for policyholders and is not added in)."""
    totals: dict[str, float] = {}
    for row in kr_shareholder_rows(rows):
        relation = " ".join(str(row.get("relate") or "").split())
        if relation not in ("최대주주 본인", "본인"):
            continue
        if "보통" not in str(row.get("stock_knd") or "보통주"):
            continue
        name, special = kr_holder_name(str(row.get("nm") or ""))
        if special:
            continue
        totals[name] = max(totals.get(name, 0.0), kr_number(row.get("trmend_posesn_stock_qota_rt")) or 0.0)
    if not totals:
        return None
    name, pct = max(totals.items(), key=lambda item: item[1])
    return name, round(pct, 2)


def kr_latest_major_reports(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The latest 5% report per reporter, newest first."""
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        reporter = " ".join(str(row.get("repror") or "").split())
        if not reporter:
            continue
        key = (str(row.get("rcept_dt") or ""), str(row.get("rcept_no") or ""))
        held = latest.get(reporter)
        if held is None or key > (str(held.get("rcept_dt") or ""), str(held.get("rcept_no") or "")):
            latest[reporter] = row
    return sorted(latest.values(), key=lambda r: (str(r.get("rcept_dt") or ""), str(r.get("rcept_no") or "")), reverse=True)


def kr_listing_words(company: dict[str, Any]) -> str | None:
    words = _kr().LISTING_CLASSES.get(str(company.get("corp_cls") or ""))
    stock = str(company.get("stock_code") or "").strip()
    if words and stock and words.startswith("listed"):
        return f"{words} (stock code {stock})"
    return words


def _type_note(index_name: str, *, entity: bool, reason: str) -> dict[str, Any]:
    kind = "an organisation" if entity else "a natural person"
    return commenting(
        pointer("recordDetails"),
        f"The filing does not say whether 「{index_name}」 is a person or an "
        f"organisation; OpenCheck reads it as {kind} ({reason}).",
    )


@content_addressed
def map_dart_korea(bundle: dict[str, Any]) -> BODSBundle:
    """Map a DartKoreaAdapter fetch bundle to BODS v0.4 statements."""
    statements = BODSBundle()
    company = bundle.get("company")
    if not bundle or bundle.get("is_stub") or bundle.get("not_found") or not isinstance(company, dict):
        return statements
    kr = _kr()
    corp_code = str(bundle.get("corp_code") or company.get("corp_code") or "").strip()
    name = str(company.get("corp_name") or bundle.get("legal_name") or "").strip()
    if not corp_code or not name:
        return statements

    source_id = bundle.get("source_id") or "dart_korea"
    company_url = kr.api_url("company", corp_code=corp_code)
    identifiers = []
    bizr = kr._digits(company.get("bizr_no"))
    jurir = kr._digits(company.get("jurir_no"))
    if len(bizr) == 10:
        identifiers.append({"id": kr.format_bizr_no(bizr), "scheme": kr.KR_BRN_SCHEME, "schemeName": kr.KR_BRN_SCHEME_NAME})
    if len(jurir) == 13:
        identifiers.append({"id": kr.format_jurir_no(jurir), "scheme": kr.KR_CRN_SCHEME, "schemeName": kr.KR_CRN_SCHEME_NAME})
    address = " ".join(str(company.get("adres") or "").split())
    english = " ".join(str(company.get("corp_name_eng") or "").split())

    entity = make_entity_statement(
        source_id=source_id,
        local_id=corp_code,
        name=name,
        jurisdiction=("Republic of Korea", "KR"),
        identifiers=identifiers,
        founding_date=kr.parse_ymd(company.get("est_dt")),
        addresses=[_addr("registered", address, "KR")] if address else [],
        alternate_names=[english] if english else [],
        entity_details=kr_listing_words(company),
        source_url=company_url,
    )
    if not bundle.get("profile_read", True):
        annotate(entity, commenting(
            pointer("recordDetails"),
            "Open DART did not answer for the company profile in this lookup; "
            "the names, numbers and listing are from OpenCheck's crosswalk of DART filers.",
        ))
    unread = [
        words for flag, words in (
            ("shareholders_read", "the largest-shareholder table"),
            ("executives_read", "the executives table"),
            ("major_holders_read", "the 5% holding reports"),
        )
        if bundle.get("report") is not None or flag == "major_holders_read"
        if not bundle.get(flag, True)
    ]
    if unread:
        annotate(entity, commenting(
            pointer("recordDetails"),
            "Open DART did not answer for " + ", ".join(unread) + " in this lookup; none are shown.",
        ))
    statements.extend([entity])

    report = bundle.get("report") or None
    report_date = record_date((report or {}).get("rcept_dt"))
    report_url = kr.filing_url(report["rcept_no"]) if report else company_url
    report_words = ""
    if report:
        kind = kr.REPORT_CODES.get(str(report.get("reprt_code")), "periodic report")
        report_words = f"{kind} ({report.get('report_nm')}, filed {report.get('rcept_dt')})"

    parties: dict[str, dict[str, Any]] = {}  # name key → statement
    person_lists: dict[str, set[str]] = {}

    def _party(raw_name: str, *, entity_party: bool, reason: str | None, local: str) -> dict[str, Any]:
        key = kr_name_key(raw_name)
        if key in parties:
            return parties[key]
        if entity_party:
            stmt = make_entity_statement(
                source_id=source_id,
                local_id=f"{corp_code}|entity|{key}",
                name=raw_name,
                entity_type="legalEntity",
                source_url=report_url if local != "major" else kr.api_url("majorstock", corp_code=corp_code),
                statement_date=report_date,
            )
        else:
            stmt = make_person_statement(
                source_id=source_id,
                local_id=f"{corp_code}|person|{key}",
                full_name=raw_name,
                source_url=report_url if local != "major" else kr.api_url("majorstock", corp_code=corp_code),
                statement_date=report_date,
            )
        if reason:
            annotate(stmt, _type_note(raw_name, entity=entity_party, reason=reason))
        parties[key] = stmt
        return stmt

    # --- largest shareholder and related persons --------------------------
    holder_rows: dict[str, list[dict[str, Any]]] = {}
    holder_names: dict[str, str] = {}
    for row in kr_shareholder_rows(bundle.get("shareholders") or []):
        holder, _special = kr_holder_name(str(row.get("nm") or ""))
        key = kr_name_key(holder)
        holder_rows.setdefault(key, []).append(row)
        holder_names.setdefault(key, holder)
    holder_types: dict[str, bool] = {}
    for key, rows in holder_rows.items():
        holder = holder_names[key]
        relate = str(rows[0].get("relate") or "")
        is_entity = kr_shareholder_is_entity(holder, relate)
        holder_types[key] = is_entity
        # Said only where the reading rests on the relation alone: a legal-form
        # token is the filer's own word, and a holder with neither is a person.
        reason = (
            f"its relation is filed as {' '.join(relate.split())}"
            if is_entity and not kr_has_org_token(holder) else None
        )
        party = _party(holder, entity_party=is_entity, reason=reason, local="holder")
        if not is_entity:
            person_lists.setdefault(key, set()).add("shareholders")
        interests: list[dict[str, Any]] = []
        for row in rows:
            _name, special = kr_holder_name(str(row.get("nm") or ""))
            stock = " ".join(str(row.get("stock_knd") or "").split()) or "shares"
            count = kr_number(row.get("trmend_posesn_stock_co"))
            pct = kr_number(row.get("trmend_posesn_stock_qota_rt"))
            period_end = kr.parse_ymd(row.get("stlm_dt"))
            figure = f"{int(count):,} {stock}" if count is not None else stock
            if pct is not None and pct == 0 and count:
                pct_words = ", less than 0.01% of the class"
            elif pct is not None:
                pct_words = f", {pct:g}% of the class"
            else:
                pct_words = ""
            parts = [
                f"Holding at the period end{f' ({period_end})' if period_end else ''}: {figure}{pct_words}",
                kr_relation_words(str(row.get("relate") or "")),
            ]
            if special:
                parts.append("held in the insurer's special account (특별계정)")
            if report_words:
                parts.append(f"from the {report_words}")
            interest: dict[str, Any] = {
                "type": "shareholding",
                "directOrIndirect": "direct",
                "details": "; ".join(parts) + ". " + KR_SHAREHOLDER_CAVEAT,
            }
            if count == 0 or (count is None and pct == 0):
                if period_end:
                    interest["endDate"] = period_end
            elif pct is not None and 0 < pct <= 100:
                interest["share"] = {"exact": pct}
            interests.append(set_beneficial_ownership(interest, source_id, asserted=None))
        statements.extend([party])
        statements.extend([make_relationship_statement(
            source_id=source_id,
            local_id=f"{corp_code}|largest-shareholder-group|{key}",
            subject_statement_id=entity["recordId"],
            interested_party_statement_id=party["recordId"],
            interested_party_type="entity" if is_entity else "person",
            interests=interests,
            source_url=report_url,
            statement_date=report_date,
        )])

    # --- executives -------------------------------------------------------
    for row in bundle.get("executives") or []:
        person_name = " ".join(str(row.get("nm") or "").split())
        if not person_name:
            continue
        key = kr_name_key(person_name)
        if holder_types.get(key):
            continue  # an organisation of the same name — never merge a person into it
        known = key in parties
        person = _party(person_name, entity_party=False, reason=None, local="executive")
        birth = kr_birth_month(row.get("birth_ym"))
        if birth and "birthDate" not in person["recordDetails"]:
            person["recordDetails"]["birthDate"] = birth
        person_lists.setdefault(key, set()).add("executives")
        if not known:
            statements.extend([person])
        statements.extend([make_relationship_statement(
            source_id=source_id,
            local_id=f"{corp_code}|executive|{key}|{' '.join(str(row.get('ofcps') or '').split())}",
            subject_statement_id=entity["recordId"],
            interested_party_statement_id=person["recordId"],
            interested_party_type="person",
            interests=kr_executive_interests(row, source_id),
            source_url=report_url,
            statement_date=report_date,
        )])
    for key, lists in person_lists.items():
        if lists == {"shareholders", "executives"}:
            annotate(parties[key], commenting(
                pointer("recordDetails"),
                "Named in both the largest-shareholder table and the executives table "
                "of the same periodic report; OpenCheck reads the two as one person by "
                "the name as filed.",
            ))

    # --- 5% holding reports ------------------------------------------------
    major_url = kr.api_url("majorstock", corp_code=corp_code)
    for row in kr_latest_major_reports(bundle.get("major_holders") or []):
        reporter = " ".join(str(row.get("repror") or "").split())
        key = kr_name_key(reporter)
        if key in holder_types:
            is_entity = holder_types[key]
            reason = None
        else:
            is_entity = not kr_reporter_is_person(reporter)
            reason = (
                "its name has the shape of a Korean personal name" if not is_entity
                else None if kr_has_org_token(reporter)
                else "its name does not have the shape of a Korean personal name"
            )
        known = key in parties
        party = _party(reporter, entity_party=is_entity, reason=reason, local="major")
        if not known:
            statements.extend([party])
        day = kr.parse_ymd(row.get("rcept_dt"))
        pct = kr_number(row.get("stkrt"))
        count = kr_number(row.get("stkqy"))
        rcept_no = str(row.get("rcept_no") or "").strip()
        reported: list[str] = []
        if pct is not None:
            reported.append(f"{pct:g}%")
        if count is not None:
            reported.append(f"{int(count):,} shares")
        details = (
            "Latest 5% holding report (주식등의 대량보유상황보고서)"
            + (f" filed {day}" if day else "")
            + (f": {', '.join(reported)}" if reported else "")
            + ", the reporter's holding together with its specially related persons "
            "(a combined figure, not the reporter's own share)"
            + (f"; filing {rcept_no}" if rcept_no else "")
            + ". Korea has no public beneficial-ownership register."
        )
        major: dict[str, Any] = {"type": "shareholding", "directOrIndirect": "unknown", "details": details}
        if pct is not None and pct < 5 and day:
            major["endDate"] = day
            major["details"] = details + " The report is below 5%: the reporter left the reporting threshold."
        statements.extend([make_relationship_statement(
            source_id=source_id,
            local_id=f"{corp_code}|major-holder|{key}",
            subject_statement_id=entity["recordId"],
            interested_party_statement_id=party["recordId"],
            interested_party_type="entity" if is_entity else "person",
            interests=[set_beneficial_ownership(major, source_id, asserted=None)],
            source_url=kr.filing_url(rcept_no) if rcept_no else major_url,
            statement_date=record_date(day),
        )])

    return statements
