"""Switzerland — Zefix (Federal Commercial Registry) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, _addr, make_entity_statement


# ----------------------------------------------------------------------
# Zefix (Swiss Federal Commercial Registry) → BODS
# ----------------------------------------------------------------------
#
# Zefix exposes the ``CompanyFull`` schema via ``GET /api/v1/company/uid/{uid}``.
# We map the entity-level fields to a BODS entity statement.  Zefix does not
# expose natural persons through this API, so only entity statements are emitted.
#
# Identifier scheme: ``CH-FDJP`` for the Swiss Unternehmens-Identifikationsnummer
# (UID), issued by the Federal Department of Justice and Police (FDJP) via
# the commercial register.  Format used in BODS identifiers is ``CHE-NNN.NNN.NNN``
# (the official display format).
#
# The ``bundle`` shape expected here matches what ZefixAdapter.fetch() returns:
#   {
#     "source_id": "zefix",
#     "uid": "CHE313550547",        # normalised (no separators)
#     "company": {<CompanyFull>},   # from Zefix API
#     "is_stub": False,
#   }

import re as _re

_ZEFIX_UID_RE = _re.compile(r"CHE(\d{3})(\d{3})(\d{3})", _re.IGNORECASE)

_ZEFIX_CANTON_TO_NAME: dict[str, str] = {
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Geneva", "GL": "Glarus", "GR": "Graubünden",
    "JU": "Jura", "LU": "Lucerne", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen", "SO": "Solothurn",
    "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino", "UR": "Uri",
    "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zurich",
}


def _zefix_format_uid(uid: str) -> str:
    """``CHE313550547`` → ``CHE-313.550.547`` (official display format)."""
    m = _ZEFIX_UID_RE.match(uid.strip())
    if m:
        return f"CHE-{m.group(1)}.{m.group(2)}.{m.group(3)}"
    return uid


def map_zefix(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a Zefix fetch bundle to BODS v0.4 entity statements.

    Returns an empty iterable for stub bundles or missing company data.
    Only entity statements are emitted — Zefix does not expose natural persons.
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    uid_raw: str = company.get("uid") or bundle.get("uid") or ""
    name: str = company.get("name") or ""
    if not uid_raw or not name:
        return

    uid_display = _zefix_format_uid(uid_raw)
    canton: str = company.get("canton") or ""

    # Jurisdiction: use canton subdivision code where available (e.g. CH-ZH),
    # falling back to country-level CH.
    if canton and canton.upper() in _ZEFIX_CANTON_TO_NAME:
        jur_code = f"CH-{canton.upper()}"
        jur_name = f"{_ZEFIX_CANTON_TO_NAME[canton.upper()]}, Switzerland"
    else:
        jur_code = "CH"
        jur_name = "Switzerland"

    # Identifiers — Swiss UID is the primary cross-reference.
    identifiers: list[dict[str, str]] = [
        {
            "id": uid_display,
            "scheme": "CH-FDJP",
            "schemeName": "Swiss commercial register (Federal Department of Justice and Police)",
        }
    ]
    # EHRA-ID (internal Zefix identifier) as a secondary cross-reference.
    # Uses CH-COA (generic Swiss company register) since the EHRAID is a
    # Zefix-internal key distinct from the publicly-issued UID.
    ehraid = company.get("ehraid")
    if ehraid is not None:
        identifiers.append(
            {
                "id": str(ehraid),
                "scheme": "CH-COA",
                "schemeName": "Zefix (FCRO/EHRA) internal ID",
            }
        )

    # Address
    addr_block = company.get("address") or {}
    addresses = _zefix_address(addr_block)

    source_url = (
        ((company.get("zefixDetailWeb") or {}).get("en"))
        or company.get("cantonalExcerptWeb")
        or f"https://www.zefix.ch/en/search/entity/list/firm/{company.get('ehraid', '')}"
    )

    entity = make_entity_statement(
        source_id="zefix",
        local_id=uid_raw,
        name=name,
        jurisdiction=(jur_name, jur_code),
        identifiers=identifiers,
        addresses=addresses,
        source_url=source_url or None,
    )
    # Zefix ``status``: ACTIVE / BEING_CANCELLED / CANCELLED (Phase 151).
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["zefix"],
        liveness=_liveness.classify(
            company.get("status"),
            live=("ACTIVE",),
            pending=("BEING_CANCELLED",),
            terminal=("CANCELLED",),
        ),
        raw=company.get("status") or None,
    )

    # Carry the Swiss legal form (e.g. "Foundation"/"Stiftung", "Corporation")
    # as the non-schema `legalFormLabel` annotation. This is what the AMLA
    # trust/arrangement risk signal keys off — matching the *legal form*, not
    # the entity name (a name like "…Foundation" must not trip the signal on
    # its own). BODS v0.4 entityType.subtype is a restricted enum that does not
    # accept arbitrary legal-form text, so the label lives alongside it.
    legal_form = company.get("legalForm") or {}
    lf_names = legal_form.get("name") if isinstance(legal_form, dict) else None
    legal_form_text = ""
    if isinstance(lf_names, dict):
        legal_form_text = (lf_names.get("en") or lf_names.get("de") or "").strip()
    if legal_form_text:
        entity["recordDetails"]["legalFormLabel"] = legal_form_text

    yield entity


def _zefix_address(block: dict[str, Any]) -> list[dict[str, str]]:
    if not block:
        return []
    parts = [
        block.get("organisation"),
        block.get("careOf"),
        " ".join(filter(None, [block.get("street"), block.get("houseNumber")])),
        block.get("addon"),
        block.get("poBox"),
        " ".join(filter(None, [block.get("swissZipCode"), block.get("city")])),
    ]
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return []
    return [_addr("registered", joined, "CH")]
