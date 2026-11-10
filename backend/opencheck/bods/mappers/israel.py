"""Israel — the Corporations Authority's registers (data.gov.il) → BODS.

One entity statement per company or partnership. The open data carries no
directors, shareholders or partners, so there are no relationships.
``mapper.py`` re-exports every name here.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .. import liveness as _liveness
from ..annotations import annotate, commenting
from ..countries import country_code
from ..statements import (
    SOURCE_NAMES,
    _addr,
    content_addressed,
    make_entity_statement,
)

#: org-id.guide's code for the Registrar of Companies, the scheme
#: ``mapper._GLEIF_RA_TO_ORG_ID`` already gives RA000406 — so the GLEIF and
#: register statements corroborate on the bare 9-digit number.
_IL_SCHEME = "IL-ROC"
_IL_SCHEME_NAME = "Registrar of Companies (Israel Corporations Authority)"

#: ``קוד סוג חברה`` 2: a foreign company or partnership registered in
#: Israel. Its home country is not published, so no jurisdiction is given.
_FOREIGN_TYPE_CODE = 2


def _il_address(addr: dict[str, Any]) -> dict[str, Any] | None:
    street = " ".join(
        str(addr[k]).strip() for k in ("street", "house") if addr.get(k)
    )
    parts = [p for p in (street, addr.get("city"), addr.get("postcode")) if p]
    if addr.get("po_box"):
        parts.append(f"P.O.B. {addr['po_box']}")
    country = str(addr.get("country") or "").strip()
    if not parts:
        return None
    code = country_code(country)
    if country and not code:
        parts.append(country)
    return _addr("registered", ", ".join(str(p) for p in parts), code)


@content_addressed
def map_ica_israel(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an IcaIsraelAdapter fetch bundle to BODS v0.4 statements.

    Yields one entityStatement. A note card (``not_found``) and a stub yield
    nothing.
    """
    from ...sources.ica_israel import (
        PRIVATE_COMPANY_WORDS,
        form_label,
        status_label,
        sub_status_label,
        violating_label,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    number = str(bundle.get("number") or "").strip()
    if len(number) != 9:
        return

    name_he = (bundle.get("name") or "").strip()
    name_en = (bundle.get("name_en") or "").strip()
    name = name_he or name_en or bundle.get("legal_name") or f"IL {number}"
    alternate = [name_en] if name_en and name_en != name else []

    foreign = bundle.get("type_code") == _FOREIGN_TYPE_CODE
    type_words = (bundle.get("type_words") or "").strip()
    gloss = form_label(type_words)
    details_parts: list[str] = []
    if gloss and type_words and gloss != type_words:
        details_parts.append(f"{gloss[0].upper()}{gloss[1:]} ({type_words})")
    elif gloss or type_words:
        details_parts.append(gloss or type_words)
    if bundle.get("government"):
        # A government company stays registeredEntity (the Phase 240 rule):
        # it is a company the state owns, not a state body.
        details_parts.append("government company (חברה ממשלתית)")

    addresses = []
    address = _il_address(bundle.get("address") or {})
    if address:
        addresses.append(address)

    entity = make_entity_statement(
        source_id="ica_israel",
        local_id=number,
        name=name,
        jurisdiction=None if foreign else ("Israel", "IL"),
        identifiers=[{"id": number, "scheme": _IL_SCHEME, "schemeName": _IL_SCHEME_NAME}],
        founding_date=bundle.get("incorporated") or None,
        addresses=addresses,
        alternate_names=alternate,
        entity_details="; ".join(details_parts) or None,
        source_url=bundle.get("link") or None,
    )

    status_words = (bundle.get("status_words") or "").strip()
    sub = sub_status_label(bundle.get("sub_status"))
    raw = status_words or None
    if raw and sub:
        raw = f"{raw} ({bundle.get('sub_status')})"
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["ica_israel"],
        liveness=_liveness.classify(
            bundle.get("status_class") or "unknown",
            live=("live",), pending=("pending",), terminal=("terminal",),
        ),
        raw=raw,
    )

    notes: list[dict[str, Any] | None] = []
    gloss_status = status_label(status_words)
    # The register-status note above already quotes the words; a gloss is
    # added where it says more than "active".
    live_plain = bundle.get("status_class") == "live" and not sub
    if gloss_status and gloss_status != status_words and not live_plain:
        text = f"The register's status is {gloss_status.lower()} ({status_words})"
        if sub:
            text += f"; sub-status: {sub}"
        notes.append(commenting("/recordDetails", text + "."))
    violating = violating_label(bundle.get("violating"))
    if violating:
        notes.append(
            commenting(
                "/recordDetails",
                f"The register marks this company as a {violating} "
                f"({bundle.get('violating')}): a company that has not paid its annual fees or filed its annual "
                "reports (Companies Law, section 362A).",
            )
        )
    year = bundle.get("last_report_year")
    if year and type_words == PRIVATE_COMPANY_WORDS:
        notes.append(
            commenting(
                "/recordDetails",
                f"The last annual report the register records as filed is "
                f"for {year}.",
            )
        )
    if foreign:
        notes.append(
            commenting(
                "/recordDetails",
                "Registered in Israel as a foreign company or partnership; "
                "the register does not publish its home jurisdiction.",
            )
        )
    annotate(entity, *notes)
    yield entity
