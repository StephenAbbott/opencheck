"""Slovenia — the Slovenian Business Register (AJPES PRS) → BODS.

One entity statement per unit. The open file carries no officers, members,
shareholders or beneficial owners, so there are no relationships; it carries
no status and no dates either, so none is stated. ``mapper.py`` re-exports
every name here.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..annotations import annotate, commenting
from ..countries import country_code
from ..statements import _addr, content_addressed, make_entity_statement

#: org-id.guide's code for the Slovenian Business Register; GLEIF's RA000529
#: maps to the same scheme (``mapper._GLEIF_RA_TO_ORG_ID``), so the GLEIF and
#: register statements corroborate on the bare ten-digit number.
_SI_SCHEME = "SI-PRS"
_SI_SCHEME_NAME = "Slovenian Business Register (Poslovni register Slovenije, AJPES)"


def _si_address(addr: dict[str, Any]) -> dict[str, Any] | None:
    """The registered address. A natural person's street never reaches the
    bundle (the builder drops it), so a sole trader's is settlement and
    postcode only."""
    street = " ".join(
        str(addr[k]).strip() for k in ("street", "house", "house_suffix") if addr.get(k)
    )
    place = " ".join(str(addr[k]).strip() for k in ("postcode", "post_office") if addr.get(k))
    settlement = str(addr.get("settlement") or "").strip()
    if settlement and settlement.casefold() == str(addr.get("post_office") or "").strip().casefold():
        settlement = ""
    parts = [p for p in (street, settlement, place) if p]
    if not parts:
        return None
    country = str(addr.get("country") or "").strip()
    code = country_code(country)
    if country and not code:
        parts.append(country)
    return _addr("registered", ", ".join(parts), code)


@content_addressed
def map_si_prs(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a SiPrsAdapter fetch bundle to BODS v0.4 statements.

    Yields one entityStatement. A note card (``not_found``) and a stub yield
    nothing.
    """
    from ...sources.si_prs import form_label, organ_label

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    number = str(bundle.get("number") or "").strip()
    if len(number) != 10 or not number.isdigit():
        return

    name = (bundle.get("name") or bundle.get("legal_name") or f"SI {number}").strip()
    form = (bundle.get("form") or "").strip()
    gloss = form_label(form)
    if gloss and form:
        details: str | None = f"{gloss[0].upper()}{gloss[1:]} ({form})"
    else:
        details = gloss or form or None

    addresses = []
    address = _si_address(bundle.get("address") or {})
    if address:
        addresses.append(address)

    entity = make_entity_statement(
        source_id="si_prs",
        local_id=number,
        name=name,
        jurisdiction=("Slovenia", "SI"),
        identifiers=[{"id": number, "scheme": _SI_SCHEME, "schemeName": _SI_SCHEME_NAME}],
        addresses=addresses,
        entity_details=details,
        source_url=bundle.get("link") or None,
    )

    notes: list[dict[str, Any] | None] = []
    organ = (bundle.get("organ") or "").strip()
    if organ:
        label = organ_label(organ)
        said = f"{label} ({organ})" if label and label != organ and organ not in label else (label or organ)
        notes.append(commenting("/recordDetails", f"Entered in the register by {said}."))
    notes.append(
        commenting(
            "/recordDetails",
            "The register's open file publishes no status and no dates: being "
            "listed says the unit is on the register, not whether it trades.",
        )
    )
    if not bundle.get("legal_person"):
        notes.append(
            commenting(
                "/recordDetails/addresses",
                "A natural person's unit (a sole trader, for instance): OpenCheck "
                "keeps the settlement and postcode of its registered address and "
                "not the street, which is often a home.",
            )
            if addresses
            else None
        )
    annotate(entity, *notes)
    yield entity
