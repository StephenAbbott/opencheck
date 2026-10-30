"""United States, Florida — Department of State, Division of Corporations
(Sunbiz) → BODS v0.4.

The Florida adapter's index carries the register's record, its kept events
and up to six officers. ``mapper.py`` re-exports every name defined here.

Decisions (Stephen, 30 Oct 2026 — the Florida ticket, D4/D5/D9):

* **Officers are managing officials, never owners.** Every title — including
  an LLC's "authorized member" and "managing member" — maps to
  ``seniorManagingOfficial`` with the filed title in ``details``; the filing
  lists who is authorised or manages, not what anyone holds, and Florida has
  no beneficial-ownership register. No ``beneficialOwnershipOrControl`` is
  set.
* **Name, city and state only** for an officer — never a street address; a
  corporate officer is an entity with no jurisdiction (Phase 295: the
  register files none).
* **The registered agent is not a party.** It is named in the finding and the
  bundle; it holds no role in the company.
* **The FEI number is an identifier** (``US-EIN``), as the register publishes
  it.
* A **merger or conversion partner** the index holds is an entity statement
  with a ``commenting`` note — **no relationship** (the ClimateTRACE
  successor rule): the event says two records were joined, not who owns whom.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .. import former_names as _former_names
from .. import liveness as _liveness
from ..annotations import annotate, commenting, pointer
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _stable_id,
    content_addressed,
    latest_record_date,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    record_date,
)

_FL_SOURCE = "sunbiz_florida"


def _fl_address(parts: dict[str, Any] | None, address_type: str) -> dict[str, Any] | None:
    if not parts:
        return None
    line = ", ".join(
        str(parts[k]).strip()
        for k in ("addr1", "addr2", "city")
        if parts.get(k) and str(parts[k]).strip()
    )
    tail = " ".join(str(parts[k]).strip() for k in ("state", "zip") if parts.get(k))
    text = ", ".join(p for p in (line, tail) if p)
    if not text:
        return None
    country = str(parts.get("country") or "").strip().upper()
    # The layout's country is two letters — "US" on most rows, blank on many.
    return _addr(address_type, text, "US" if country in ("", "US") else country)


def _fl_entity_details(bundle: dict[str, Any]) -> str:
    from ...sources.sunbiz_florida import filing_type_label, is_foreign, jurisdiction_of

    code = (bundle.get("filing_type") or "").strip()
    label = filing_type_label(code) or "Filing"
    text = f"{label[0].upper()}{label[1:]}"
    if code and code.upper() != label.upper():
        text += f" ({code})"
    text += ", filed with the Florida Division of Corporations"
    if is_foreign(code):
        filed = bundle.get("file_date")
        if filed:
            text += f"; authorised to transact business in Florida on {filed}"
        home = (bundle.get("state_country") or "").strip()
        if home and jurisdiction_of(home) is None:
            text += f"; state or country of incorporation filed as “{home}”"
    return text


def _fl_officer_interest(title: str | None, filed_on: str | None) -> dict[str, Any]:
    from ...sources.sunbiz_florida import title_label

    label = title_label(title)
    raw = (title or "").strip()
    details = f"{label[0].upper()}{label[1:]}"
    if raw and raw.lower() != label.lower():
        details += f" (filed as “{raw}”)"
    details += (
        f", as listed on the Florida Division of Corporations record of {filed_on}"
        if filed_on else ", as listed on the Florida Division of Corporations record"
    )
    return {
        "type": "seniorManagingOfficial",
        "directOrIndirect": "direct",
        "details": details,
    }


@content_addressed
def map_sunbiz_florida(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a SunbizFloridaAdapter fetch bundle to BODS v0.4 statements.

    One entityStatement for the company (former names as ``alternateNames``),
    a statement per merger or conversion partner the index holds, and per
    officer a person or entity statement and a ``seniorManagingOfficial``
    relationship. A note card (``not_found``) and a stub yield nothing.
    """
    from ...sources.sunbiz_florida import (
        EIN_SCHEME,
        EIN_SCHEME_NAME,
        FL_SCHEME,
        FL_SCHEME_NAME,
        LINK_EVENTS,
        format_ein,
        is_foreign,
        jurisdiction_of,
        record_url,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    doc = (bundle.get("doc") or "").strip()
    if not doc:
        return
    name = bundle.get("name") or bundle.get("legal_name") or f"US-FL {doc}"
    source_url = bundle.get("link") or record_url(doc)
    foreign = is_foreign(bundle.get("filing_type"))

    identifiers = [{"id": doc, "scheme": FL_SCHEME, "schemeName": FL_SCHEME_NAME}]
    fei = bundle.get("fei")
    if fei:
        identifiers.append({"id": format_ein(fei), "scheme": EIN_SCHEME, "schemeName": EIN_SCHEME_NAME})

    if foreign:
        jurisdiction = jurisdiction_of(bundle.get("state_country"))
        # The Florida date is when it was authorised here, not founded.
        founding = None
    else:
        jurisdiction = ("United States", "US")
        founding = bundle.get("file_date") or None

    addresses = [
        a for a in (
            _fl_address(bundle.get("principal_address"), "business"),
            _fl_address(bundle.get("mailing_address"), "alternative"),
        ) if a
    ]
    if len(addresses) == 2 and addresses[0]["address"] == addresses[1]["address"]:
        addresses = addresses[:1]

    former = [(f["name"], f.get("since")) for f in bundle.get("former_names") or [] if f.get("name")]
    alternate_names = [n for n, _ in former]

    reports = bundle.get("annual_reports") or []
    latest_report = latest_record_date(r.get("filed") for r in reports)
    company = make_entity_statement(
        source_id=_FL_SOURCE,
        local_id=doc,
        statement_date=record_date(bundle.get("last_transaction")),
        name=name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        founding_date=founding,
        addresses=addresses,
        alternate_names=alternate_names,
        entity_details=_fl_entity_details(bundle),
        source_url=source_url,
    )

    cls = (bundle.get("status_class") or "unknown").strip()
    words = (bundle.get("status_words") or "").strip()
    raw_status = (bundle.get("status") or "").strip()
    raw = f"{raw_status} — {words}" if raw_status and words else (raw_status or words or None)
    _liveness.apply_register_status(
        company,
        source_label=SOURCE_NAMES[_FL_SOURCE],
        liveness=_liveness.classify(cls, live=("live",), terminal=("terminal",)),
        raw=raw,
        since=bundle.get("status_since") if cls == "terminal" else None,
    )

    # A name GLEIF holds that the register no longer records: the events
    # file carries the name after each change, never the name an entity was
    # formed under, so a renamed entity's first name is in no file.
    if bundle.get("name_check") == "renamed" and bundle.get("legal_name"):
        annotate(company, commenting(
            pointer("recordDetails", "name"),
            f"GLEIF names this record “{bundle['legal_name']}”, which is not a "
            "name the Florida Division of Corporations records for it. The "
            "register records that the entity was renamed; its events file "
            "carries each name after a change but not the name it was formed "
            "under.",
        ))
    if bundle.get("more_than_six_officers"):
        annotate(company, commenting(
            pointer("recordDetails"),
            "The Florida Division of Corporations records more than six "
            "officers for this entity; its bulk data carries six, so the list "
            "here is incomplete.",
        ))
    if foreign and cls == "inactive_foreign":
        when = f" on {bundle['status_since']}" if bundle.get("status_since") else ""
        annotate(company, commenting(
            pointer("recordDetails"),
            f"Inactive in Florida{when} ({words or 'inactive'}). For a foreign "
            "filing this ends its authority to transact business in Florida; "
            "it says nothing about the entity in its home jurisdiction.",
        ))
    if fei:
        annotate(company, commenting(
            pointer("recordDetails", "identifiers", 1),
            "The FEI (federal employer identification) number as filed with "
            "the Florida Division of Corporations; the register does not "
            "verify it with the IRS.",
        ))
    annotate(company, *_former_names.former_name_annotations(
        alternate_names, [(n, None, since) for n, since in former]
    ))

    links = {(e.get("other_doc") or "").upper(): e for e in bundle.get("events") or []
             if (e.get("code") or "") in LINK_EVENTS and e.get("other_doc")}
    yield company
    company_id = company["recordId"]

    for partner in bundle.get("linked_records") or []:
        pdoc = (partner.get("doc") or "").strip().upper()
        if not pdoc:
            continue
        event = links.get(pdoc) or {}
        kind = LINK_EVENTS.get(event.get("code") or "", "link")
        on_day = event.get("effective") or event.get("filed")
        sentence = (
            f"The Florida Division of Corporations records a {kind} between "
            f"this record and {name} (document {doc})"
            + (f" on {on_day}" if on_day else "")
            + ". No ownership or control between them is stated."
        )
        if partner.get("name"):
            linked = make_entity_statement(
                source_id=_FL_SOURCE,
                local_id=pdoc,
                name=partner["name"],
                identifiers=[{"id": pdoc, "scheme": FL_SCHEME, "schemeName": FL_SCHEME_NAME}],
                source_url=record_url(pdoc),
            )
            annotate(linked, commenting(pointer("recordDetails"), sentence))
            yield linked
        else:
            annotate(company, commenting(
                pointer("recordDetails"),
                f"The Florida Division of Corporations records a {kind} with "
                f"document {pdoc}" + (f" on {on_day}" if on_day else "") + ", a "
                "record outside the index (inactive and named by no LEI).",
            ))

    officer_date = latest_report or record_date(bundle.get("last_transaction"))
    seen: dict[str, str] = {}
    for idx, officer in enumerate(bundle.get("officers") or []):
        full_name = (officer.get("name") or "").strip()
        if not full_name:
            continue
        seq = officer.get("seq", idx)
        is_org = (officer.get("type") or "").strip().upper() == "C"
        party_key = ("entity" if is_org else "person") + ":" + full_name.casefold()
        if party_key not in seen:
            party_local = f"{doc}:officer:{seq}"
            if is_org:
                party = make_entity_statement(
                    source_id=_FL_SOURCE,
                    local_id=party_local,
                    name=full_name,
                    jurisdiction=None,
                    source_url=source_url,
                )
            else:
                party = make_person_statement(
                    source_id=_FL_SOURCE,
                    local_id=party_local,
                    full_name=full_name,
                    source_url=source_url,
                )
                given = (officer.get("given") or "").strip()
                family = (officer.get("surname") or "").strip()
                if given or family:
                    legal = party["recordDetails"]["names"][0]
                    if family:
                        legal["familyName"] = family
                    if given:
                        legal["givenName"] = given
            yield party
            seen[party_key] = party["recordId"]
        yield make_relationship_statement(
            source_id=_FL_SOURCE,
            local_id=f"{doc}:officer-rel:{seq}",
            statement_date=officer_date,
            subject_statement_id=company_id,
            interested_party_statement_id=seen[party_key],
            interested_party_type="entity" if is_org else "person",
            interests=[_fl_officer_interest(officer.get("title"), officer_date)],
            source_url=source_url,
        )


def sunbiz_officer_record_id(doc: str, seq: int) -> str:
    """The recordId of an officer's party statement (for tests and joins)."""
    return _stable_id(_FL_SOURCE, "person", f"{doc}:officer:{seq}")
