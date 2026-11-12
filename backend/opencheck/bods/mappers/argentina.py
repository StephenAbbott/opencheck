"""Argentina — the Registro Nacional de Sociedades and the IGJ → BODS.

One entity statement per company. Neither dataset publishes shareholders or
beneficial owners, and the people named in IGJ filings are counted, never
mapped (Stephen's decision B, 11 Nov 2026), so there are no relationships.
``mapper.py`` re-exports every name here.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable
from typing import Any

from ...identifiers import format_cuit
from .. import liveness as _liveness
from ..annotations import annotate, commenting
from ..statements import (
    SOURCE_NAMES,
    _addr,
    content_addressed,
    make_entity_statement,
    record_date,
)

_AR_SOURCE = "registro_sociedades_argentina"

#: ARCA's wording for a fiscal address it could not find.
_AR_ADDRESS_UNKNOWN = "INEXISTENTE - DESCONOCIDO"


def _ar_fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()


def _ar_address_text(addr: dict[str, Any] | None) -> str | None:
    if not addr:
        return None
    street = " ".join(str(addr[k]).strip() for k in ("street", "number") if addr.get(k))
    unit = " ".join(
        f"{label} {addr[k]}".strip()
        for k, label in (("floor", "piso"), ("unit", "dto."))
        if addr.get(k)
    )
    parts = [p for p in (street, unit, addr.get("locality"), addr.get("province"), addr.get("postcode")) if p]
    return ", ".join(str(p) for p in parts) or None


def _ar_igj_note(igj: dict[str, Any]) -> str:
    form = igj.get("form")
    text = f"The Inspección General de Justicia (City of Buenos Aires) registers this entity as entry {igj.get('correlativo')}"
    if form:
        text += f" ({form.strip().lower()})"
    text += "."
    words = igj.get("baja_words")
    cls = igj.get("baja_class")
    if igj.get("deregistered"):
        reason = f'"{words}"' if words else f"code {igj.get('baja_code') or 'not given'}"
        if cls == "moved":
            text += (
                f" The IGJ removed the entry when the company moved to another "
                f"jurisdiction ({reason}); this does not mean it ceased to exist."
            )
        elif cls == "transformed":
            text += (
                f" The IGJ removed the entry on a transformation ({reason}); the "
                "company may continue under another legal form."
            )
        elif cls == "terminal":
            text += f" The IGJ removed the entry: {reason}."
        else:
            text += f" The IGJ marks the entry as removed ({reason}) without saying why."
    else:
        text += (
            " The IGJ's open data marks removed entries only; an entry without "
            "the mark is not a statement that the company is active."
        )
    return text


@content_addressed
def map_registro_sociedades_argentina(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a RegistroSociedadesArgentinaAdapter bundle to BODS v0.4 statements.

    Yields one entityStatement. A note card (``not_found``) and a stub yield
    nothing.
    """
    from ...sources.registro_sociedades_argentina import (
        AR_CUIT_SCHEME,
        AR_CUIT_SCHEME_NAME,
        IGJ_FOREIGN_FORMS,
        RNS_FOREIGN_FORMS,
        STATE_FORMS,
        form_label,
        name_key,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    rns = bundle.get("rns") or {}
    igj = bundle.get("igj") or {}
    if not rns and not igj:
        return
    cuit = str(bundle.get("cuit") or "").strip()
    if cuit and len(cuit) != 11:
        return
    local_id = cuit or f"igj-{igj.get('correlativo')}"

    name = (rns.get("name") or igj.get("name") or bundle.get("legal_name") or "").strip()
    if not name:
        return
    alternate = []
    if igj.get("name") and rns.get("name") and name_key(igj["name"]) != name_key(rns["name"]):
        alternate.append(igj["name"].strip())

    form_words = (rns.get("form") or "").strip()
    gloss = form_label(form_words)
    details_parts: list[str] = []
    if form_words and gloss and _ar_fold(form_words) not in _ar_fold(gloss):
        details_parts.append(f"{gloss[0].upper()}{gloss[1:]} ({form_words.lower()})")
    elif gloss:
        details_parts.append(f"{gloss[0].upper()}{gloss[1:]}")
    elif igj.get("form"):
        form = str(igj["form"]).strip().lower()
        details_parts.append(f"{form[0].upper()}{form[1:]}" if form else form)
    if form_words in STATE_FORMS:
        # A state company stays registeredEntity (the Phase 240 rule).
        details_parts.append("state-owned company form")

    foreign = form_words in RNS_FOREIGN_FORMS or (not rns and igj.get("form_code") in IGJ_FOREIGN_FORMS)

    addresses = []
    legal = _ar_address_text(rns.get("legal_address")) if rns else _ar_address_text(igj.get("address"))
    if legal:
        addresses.append(_addr("registered", legal, "AR"))
    fiscal = _ar_address_text(rns.get("fiscal_address"))
    if fiscal and fiscal != legal:
        addresses.append(_addr("alternative", fiscal, "AR"))

    entity = make_entity_statement(
        source_id=_AR_SOURCE,
        local_id=local_id,
        name=name,
        jurisdiction=None if foreign else ("Argentina", "AR"),
        identifiers=(
            [{"id": format_cuit(cuit), "scheme": AR_CUIT_SCHEME, "schemeName": AR_CUIT_SCHEME_NAME}]
            if cuit else []
        ),
        founding_date=rns.get("founded") or None,
        addresses=addresses,
        alternate_names=alternate,
        entity_details="; ".join(details_parts) or None,
        source_url=bundle.get("link") or None,
        statement_date=record_date(rns.get("updated")),
    )

    if igj.get("baja_class") == "terminal":
        _liveness.apply_register_status(
            entity,
            source_label=SOURCE_NAMES[_AR_SOURCE],
            liveness=_liveness.TERMINAL,
            raw=igj.get("baja_words") or f"IGJ code {igj.get('baja_code')}",
        )

    notes: list[dict[str, Any] | None] = []
    if entity["recordDetails"].get("foundingDate"):
        notes.append(
            commenting(
                "/recordDetails/foundingDate",
                "The founding date is the date of the company's founding "
                "contract (fecha del contrato social) in the Registro Nacional "
                "de Sociedades.",
            )
        )
    if not rns and cuit:
        notes.append(
            commenting(
                "/recordDetails",
                "This CUIT is not in the Registro Nacional de Sociedades as "
                "indexed; the record is the IGJ's alone.",
            )
        )
    if igj:
        notes.append(commenting("/recordDetails", _ar_igj_note(igj)))
        people = int(igj.get("people_named") or 0)
        if people:
            notes.append(
                commenting(
                    "/recordDetails",
                    f"{people} {'person is' if people == 1 else 'people are'} named "
                    "in this entity's IGJ filings since its registration. The open "
                    "data adds names and never removes them, gives no dates and no "
                    "specific role, so it does not say who serves today; OpenCheck "
                    "does not list them.",
                )
            )
        balances = int(igj.get("balances") or 0)
        if balances and igj.get("last_balance"):
            text = (
                f"The IGJ records {balances} balance sheet{'' if balances == 1 else 's'} "
                f"filed, the latest for the period ending {igj['last_balance']}"
            )
            if igj.get("last_balance_filed"):
                text += f" (filed {igj['last_balance_filed']})"
            notes.append(commenting("/recordDetails", text + "."))
        others = int(igj.get("other_entries") or 0)
        if others:
            notes.append(
                commenting(
                    "/recordDetails",
                    f"The IGJ holds {others} other entr{'y' if others == 1 else 'ies'} "
                    "under this CUIT (an earlier name, a transformation or a "
                    "re-registration); the one shown is the newest that is not "
                    "cancelled.",
                )
            )
    if rns.get("activity_code"):
        words = rns.get("activity")
        notes.append(
            commenting(
                "/recordDetails",
                f"Main activity registered with ARCA: {rns['activity_code']}"
                + (f" — {str(words).strip().lower()}" if words else "")
                + ".",
            )
        )
    fiscal_state = ((rns.get("fiscal_address") or {}).get("state") or "").strip().upper()
    if fiscal_state == _AR_ADDRESS_UNKNOWN and entity["recordDetails"].get("addresses"):
        notes.append(
            commenting(
                "/recordDetails/addresses",
                "ARCA records the fiscal address as non-existent or unknown "
                f"({_AR_ADDRESS_UNKNOWN}).",
            )
        )
    if foreign:
        notes.append(
            commenting(
                "/recordDetails",
                "Registered in Argentina as a foreign company or its branch; "
                "the register does not publish its home jurisdiction.",
            )
        )
    annotate(entity, *notes)
    yield entity
