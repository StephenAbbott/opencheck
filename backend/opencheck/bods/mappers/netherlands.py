"""Netherlands — KvK (Chamber of Commerce) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, make_entity_statement


# ----------------------------------------------------------------------
# KvK (Netherlands Chamber of Commerce) → BODS
# ----------------------------------------------------------------------
#
# The KvK open-data endpoint returns limited fields: registration status,
# legal form code (rechtsvormCode), SBI activity codes, start date, and a
# 2-digit postal-code region.  Company name is NOT available from this API
# tier; it is passed via bundle["legal_name"] (sourced from GLEIF).
#
# Identifier scheme: "NL-KVK"  (follows the GB-COH / CH-FDJP pattern)
# Jurisdiction: Netherlands ("NL")
# Source: https://developers.kvk.nl/nl/documentation/open-dataset-basis-bedrijfsgegevens-api


def map_kvk(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a KvK fetch bundle to a BODS v0.4 entity statement.

    Returns an empty iterable for stub bundles, missing company data,
    or missing entity name.  KvK open data does not expose natural
    persons, so only entity statements are emitted.

    Bundle shape (as returned by KvKAdapter.fetch):

    .. code-block:: python

        {
            "source_id": "kvk",
            "kvk_number": "96332751",
            "company": {          # raw KvK open-data API response
                "datumAanvang": "20250202",
                "actief": "J",
                "rechtsvormCode": "BV",
                "postcodeRegio": 10,
                "activiteiten": [{"sbiCode": "6201", "soortActiviteit": "Hoofdactiviteit"}],
                "lidstaat": "NL",
            },
            "legal_name": "Splitty B.V.",   # from GLEIF, may be empty
            "is_stub": False,
        }
    """
    if not bundle or bundle.get("is_stub"):
        return

    company: dict[str, Any] = bundle.get("company") or {}
    if not company:
        return

    kvk_number: str = bundle.get("kvk_number") or ""
    name: str = bundle.get("legal_name") or ""
    if not kvk_number or not name:
        return

    # Founding date: datumAanvang is YYYYMMDD — convert to ISO format.
    raw_date = str(company.get("datumAanvang") or "").strip()
    founding_date: str | None = None
    if len(raw_date) == 8 and raw_date.isdigit():
        founding_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    identifiers: list[dict[str, str]] = [
        {
            "id": kvk_number,
            "scheme": "NL-KVK",
            "schemeName": "Netherlands Chamber of Commerce (KvK) registration number",
        }
    ]

    entity = make_entity_statement(
        source_id="kvk",
        local_id=kvk_number,
        name=name,
        jurisdiction=("Netherlands", "NL"),
        identifiers=identifiers,
        founding_date=founding_date,
        source_url=f"https://www.kvk.nl/zoeken/handelsnaam/?q={kvk_number}",
    )
    # datumEinde (YYYYMMDD) is the end of the registration — the register's
    # only liveness signal in the open-data profile (Phase 151). No end date
    # says nothing either way, so no annotation is written.
    raw_end = str(company.get("datumEinde") or "").strip()
    if len(raw_end) == 8 and raw_end.isdigit():
        _liveness.apply_register_status(
            entity,
            source_label=SOURCE_NAMES["kvk"],
            liveness=_liveness.TERMINAL,
            raw="datumEinde",
            since=f"{raw_end[:4]}-{raw_end[4:6]}-{raw_end[6:]}",
        )
    yield entity
