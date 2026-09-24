"""Serbia — APR company register → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import SOURCE_NAMES, make_entity_statement


# ---------------------------------------------------------------------------
# Serbia — APR company register (Phase 222)
# ---------------------------------------------------------------------------
# One entity statement per company: the register publishes no people.

_RS_JURISDICTION = ("Serbia", "RS")


def map_apr_serbia(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AprSerbiaAdapter bundle to a single BODS v0.4 entity statement.

    * The business name is primary **as registered**, in whichever script the
      company filed it. A Cyrillic name adds its Serbian Latin form to
      ``alternateNames`` — the official one-to-one letter table, not a guess.
    * The matični broj is identified as ``RS-APR`` (org-id.guide).
    * The register's legal form goes to ``entityType.details``.
    * APR publishes the **municipality** and no street address, so the
      registered address is the municipality as filed and nothing finer.
    * Status: *Активан* is ``live``; liquidation, bankruptcy and forced
      liquidation are ``pending`` — the company still exists. APR publishes
      no date for the status, so none is written. Deleted companies are not in
      the feed, so ``terminal`` never arises.
    """
    from ...sources.apr_serbia import (  # local import avoids a cycle
        RS_APR_SCHEME,
        RS_APR_SCHEME_NAME,
        STATUSES,
        has_cyrillic,
        to_latin,
    )

    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    company: dict[str, Any] = bundle.get("company") or {}
    mb = str(company.get("mb") or bundle.get("mb") or "").strip()
    name = str(company.get("name") or "").strip()
    if not mb or not name:
        return

    latin = (company.get("name_latin") or (to_latin(name) if has_cyrillic(name) else "")).strip()
    municipality = str(company.get("municipality") or "").strip()
    entity = make_entity_statement(
        source_id="apr_serbia",
        local_id=mb,
        name=name,
        jurisdiction=_RS_JURISDICTION,
        identifiers=[{"id": mb, "scheme": RS_APR_SCHEME, "schemeName": RS_APR_SCHEME_NAME}],
        founding_date=(company.get("founded_on") or None),
        addresses=(
            [{"type": "registered", "address": municipality, "country": {"name": "Serbia", "code": "RS"}}]
            if municipality
            else []
        ),
        alternate_names=[latin] if latin and latin != name else [],
        entity_type="registeredEntity",
        entity_details=(company.get("legal_form") or None),
        source_url=bundle.get("link"),
    )
    status = str(company.get("status") or "").strip()
    klass = STATUSES.get(status, ("unknown", ""))[0]
    _liveness.apply_register_status(
        entity,
        source_label=SOURCE_NAMES["apr_serbia"],
        liveness={"live": _liveness.LIVE, "pending": _liveness.PENDING}.get(klass, _liveness.UNKNOWN),
        raw=status or None,
    )
    yield entity
