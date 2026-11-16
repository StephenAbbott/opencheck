"""Japan — the National Tax Agency's Corporate Number register → BODS.

The NTA publishes a body's identity and standing — name, kind, head-office
address, the closing of its registration record and a merger's successor —
and nothing about who owns or runs it. So this mapper yields one entity
statement for the body and, after a merger, one for the successor it names,
**with no relationship between them**: a successor is the company that
absorbed this one, not an owner of it (the ClimateTRACE / Estonia successor
rule). ``mapper.py`` re-exports every name here.

Rules (the NTA Japan wiring, 16 Nov 2026):

* Kinds 101 (national organs) and 201 (local public bodies) are
  ``stateBody``; 499 (bodies not registered by incorporation, some without
  legal personality) is ``legalEntity``; every other kind, the foreign 401
  included, is ``registeredEntity``. The register's Japanese wording and
  OpenCheck's gloss go to ``entityType.details``. No ELF or ISO 5009 code is
  written: the register's kind does not name an ELF form precisely enough
  (a 株式会社 and a 特例有限会社 are both ELF-coded, a 399 is fifty forms).
* Jurisdiction JP, except a foreign body (401), whose home country the
  register does not file — its address abroad is free text.
* ``assignmentDate`` is never a founding date (5 Oct 2015 for every body that
  existed then). A closed record's ``closeDate`` is the dissolution date.
* The English name a body registered (≈0.3 %) is an alternate name.
* ``hihyoji`` (the address no longer exists) is a note, never a status.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .. import liveness as _liveness
from ..annotations import annotate, commenting, pointer
from ..statements import (
    SOURCE_NAMES,
    _addr,
    content_addressed,
    make_entity_statement,
    record_date,
)

_JP_STATE_BODY_KINDS = frozenset({"101", "201"})
_JP_LEGAL_ENTITY_KINDS = frozenset({"499"})


def _jp_entity_type(kind: str | None) -> str:
    if kind in _JP_STATE_BODY_KINDS:
        return "stateBody"
    if kind in _JP_LEGAL_ENTITY_KINDS:
        return "legalEntity"
    return "registeredEntity"


def _jp_identifier(number: str) -> dict[str, str]:
    from ...sources.nta_japan import JP_JCN_SCHEME, JP_JCN_SCHEME_NAME

    return {"id": number, "scheme": JP_JCN_SCHEME, "schemeName": JP_JCN_SCHEME_NAME}


@content_addressed
def map_nta_japan(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an NtaJapanAdapter fetch bundle to BODS v0.4 statements.

    One entityStatement for the body; one for a merger's successor when the
    record names one. A note card (``not_found``) and a stub yield nothing.
    """
    from ...sources.nta_japan import (
        close_cause_label,
        corporate_number,
        kind_japanese,
        kind_label,
        record_url,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    number = corporate_number(bundle.get("number"))
    if not number:
        return

    kind = (bundle.get("kind") or "").strip() or None
    name = bundle.get("name") or bundle.get("legal_name") or f"JP {number}"
    source_url = bundle.get("link") or record_url(number)

    gloss, japanese = kind_label(kind), kind_japanese(kind)
    entity_details = (
        f"{gloss[0].upper()}{gloss[1:]} ({japanese})" if gloss and japanese else None
    )

    addresses: list[dict[str, Any]] = []
    domestic = "".join(
        str(bundle.get(k) or "").strip() for k in ("prefecture", "city", "street")
    )
    outside = (bundle.get("address_outside") or "").strip()
    if outside:
        # A foreign body: its head office abroad is the registered address,
        # and the place in Japan the register files is where it does business.
        # The register files the address abroad as free text, with no country.
        addresses.append(_addr("registered", outside))
        if domestic:
            addresses.append(_addr("business", domestic, "JP"))
    elif domestic:
        addresses.append(_addr("registered", domestic, "JP"))
    post_code = (bundle.get("post_code") or "").strip()
    if post_code and domestic:
        for address in addresses:
            if address.get("country"):
                address["postCode"] = post_code

    en_name = (bundle.get("en_name") or "").strip()
    company = make_entity_statement(
        source_id="nta_japan",
        local_id=number,
        name=name,
        jurisdiction=None if kind == "401" else ("Japan", "JP"),
        identifiers=[_jp_identifier(number)],
        addresses=addresses,
        alternate_names=[en_name] if en_name and en_name != name else [],
        entity_type=_jp_entity_type(kind),
        entity_details=entity_details,
        source_url=source_url,
        statement_date=record_date(bundle.get("update_date")),
    )

    status_cls = (bundle.get("status_class") or "unknown").strip()
    cause = (bundle.get("close_cause") or "").strip()
    raw_status = None
    if status_cls == "terminal":
        label = close_cause_label(cause)
        raw_status = f"registration record closed ({label})" if label else "registration record closed"
    elif status_cls == "live":
        raw_status = "registration record open"
    _liveness.apply_register_status(
        company,
        source_label=SOURCE_NAMES["nta_japan"],
        liveness=_liveness.classify(
            status_cls, live=("live",), terminal=("terminal",)
        ),
        raw=raw_status,
        since=bundle.get("close_date") or None,
    )

    if bundle.get("hihyoji") and addresses:
        annotate(company, commenting(
            pointer("recordDetails", "addresses", 0),
            "The National Tax Agency marks this address as no longer existing "
            "(a renamed or renumbered street, 検索対象除外) and leaves the record "
            "out of its site's search. It says nothing about the body's status.",
        ))

    successor = corporate_number(bundle.get("successor"))
    successor_stmt: dict[str, Any] | None = None
    if successor and successor != number:
        successor_name = (bundle.get("successor_name") or "").strip()
        annotate(company, commenting(
            pointer("recordDetails"),
            f"The National Tax Agency names corporate number {successor}"
            + (f" ({successor_name})" if successor_name else "")
            + " as this body's successor after a merger (承継先法人). A "
            "successor is the company that absorbed this one, not an owner "
            "of it, so OpenCheck draws no relationship.",
        ))
        successor_stmt = make_entity_statement(
            source_id="nta_japan",
            local_id=successor,
            name=successor_name or f"JP {successor}",
            # Not assumed: a foreign body (401) holds a corporate number too.
            jurisdiction=None,
            identifiers=[_jp_identifier(successor)],
            source_url=record_url(successor),
        )
        annotate(successor_stmt, commenting(
            pointer("recordDetails"),
            f"Named by the National Tax Agency as the successor of {name} "
            f"(corporate number {number}) after a merger. Not a relationship.",
        ))

    yield company
    if successor_stmt is not None:
        yield successor_stmt
