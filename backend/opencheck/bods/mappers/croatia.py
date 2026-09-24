"""Croatia — Sudski registar (Court Register) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from ..statements import _addr, make_entity_statement


# ----------------------------------------------------------------------
# Croatian Court Register (Sudski registar) → BODS
# ----------------------------------------------------------------------


def map_sudreg_croatia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Sudski registar fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the registered company. Officer and
    beneficial-ownership data are not exposed by the public API, so no
    person or relationship statements are emitted.

    Returns an empty iterable for stub bundles or missing subject data.
    """
    if not bundle or bundle.get("is_stub"):
        return

    subject: dict[str, Any] = bundle.get("subject") or {}
    if not subject:
        return

    mbs: str = bundle.get("mbs") or ""
    if not mbs:
        return

    # Name: tvrtka.ime (full legal name); fall back to the short name, then
    # to the GLEIF-supplied legal_name.
    name: str = ((subject.get("tvrtka") or {}).get("ime") or "").strip()
    if not name:
        name = ((subject.get("skracena_tvrtka") or {}).get("ime") or "").strip()
    if not name:
        name = (bundle.get("legal_name") or "").strip()
    if not name:
        return

    short_name = ((subject.get("skracena_tvrtka") or {}).get("ime") or "").strip()
    alternate_names = [short_name] if short_name and short_name != name else []

    # Founding date: datum_osnivanja is an ISO timestamp ("1990-10-31T00:00:00");
    # keep the YYYY-MM-DD prefix only.
    founding_date: str | None = None
    raw_date = subject.get("datum_osnivanja") or ""
    if isinstance(raw_date, str) and len(raw_date) >= 10:
        candidate = raw_date[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
            founding_date = candidate

    # Identifiers: MBS (court register number) and OIB. Both are
    # independently published by the Sudski registar for this entity, so
    # both are asserted on the statement.
    mbs_display = (subject.get("potpuni_mbs") or mbs).strip()
    identifiers: list[dict[str, str]] = [
        {
            "id": mbs_display,
            "scheme": "HR-MBS",
            "schemeName": "Sudski registar — Croatian Court Register (MBS)",
        }
    ]
    oib = (bundle.get("oib") or subject.get("potpuni_oib") or "").strip()
    if oib:
        identifiers.append(
            {
                "id": oib,
                "scheme": "HR-OIB",
                "schemeName": "OIB — Croatian personal/company identification number",
            }
        )

    addresses = _sudreg_address(subject.get("sjediste") or {})

    entity = make_entity_statement(
        source_id="sudreg_croatia",
        local_id=mbs,
        name=name,
        jurisdiction=("Croatia", "HR"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        alternate_names=alternate_names,
        source_url=_SUDREG_SOURCE_URL,
    )
    yield entity


_SUDREG_SOURCE_URL = "https://sudreg.pravosudje.hr"


def _sudreg_address(block: dict[str, Any]) -> list[dict[str, str]]:
    """Build a BODS address list from a Sudski registar ``sjediste`` block.

    Fields: ulica (street), kucni_broj (house number), naziv_naselja
    (settlement), naziv_opcine (municipality), naziv_zupanije (county).
    """
    if not block:
        return []
    street = (block.get("ulica") or "").strip()
    house = block.get("kucni_broj")
    if street and house not in (None, ""):
        street = f"{street} {house}"
    parts = [
        street,
        block.get("naziv_naselja"),
        block.get("naziv_opcine"),
        block.get("naziv_zupanije"),
    ]
    joined = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
    if not joined:
        return []
    return [_addr("registered", joined, "HR")]
