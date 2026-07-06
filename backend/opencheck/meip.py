"""OECD-UNSD MEIP signpost lookup.

MEIP (the OECD-UNSD Multinational Enterprise Information Platform) publishes an
annual register of the subsidiaries of the world's 500 largest multinational
enterprises. OpenCheck treats it as a **signpost**, not a data provider: when the
subject LEI matches, we surface a card that proves the entity is in MEIP, shows
its identifiers + MNE context, and points users to the OECD site to download and
reuse the full register. Nothing here is mapped to BODS or added to the graph.

Two match modes (the LEI key sets are disjoint):

* ``subsidiary`` — the subject is one of the ~30k LEI-carrying subsidiaries; we
  show its immediate parent and its ultimate parent MNE.
* ``mne_head``   — the subject is one of the 500 MNE heads; we show a subsidiary
  count instead.

Corroboration: the identifiers MEIP publishes (OpenCorporates, S&P Capital IQ)
are cross-checked against the identifiers GLEIF already publishes for the LEI, so
the card doubles as a light cross-source trust signal.

Data: ``data/meip_subsidiaries.json`` + ``data/meip_mne_heads.json``, generated
from the annual Global Register CSV by ``scripts/build_meip.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

_DATA = Path(__file__).parent / "data"

#: Where users go to download / reuse the full register.
MEIP_URL = (
    "https://www.oecd.org/en/data/dashboards/"
    "oecd-unsd-multinational-enterprise-information-platform.html"
)

# Display metadata for the identifiers we surface. GLEIF independently publishes
# OpenCorporates ids (as ``ocid``) and S&P Capital IQ ids (as ``spglobal``), so
# those two can be corroborated; Refinitiv PermID is informational only.
_ID_LABELS: dict[str, str] = {
    "lei": "LEI",
    "opencorporates": "OpenCorporates",
    "permid": "Refinitiv PermID",
    "capiq": "S&P Capital IQ",
}
# MEIP identifier key → the key it corroborates against in ``known_ids``.
_CORROBORATABLE: dict[str, str] = {
    "opencorporates": "opencorporates",
    "capiq": "capiq",
}


class MeipIdentifier(BaseModel):
    scheme: str  # "lei" | "opencorporates" | "permid" | "capiq"
    label: str
    value: str
    #: True when GLEIF independently publishes the same identifier for this LEI.
    corroborated: bool = False


class MeipMatch(BaseModel):
    mode: Literal["subsidiary", "mne_head"]
    lei: str
    name: str
    iso3: str = ""
    parent_mne: str = ""
    #: Subsidiary mode only — the entity's immediate parent in the MEIP tree.
    immediate_parent: str | None = None
    alt_names: list[str] = []
    address: str = ""
    identifiers: list[MeipIdentifier] = []
    #: MNE-head mode only.
    subsidiaries_total: int | None = None
    subsidiaries_with_lei: int | None = None
    source_url: str = MEIP_URL


def _load(name: str) -> dict[str, dict]:
    return json.loads((_DATA / name).read_text(encoding="utf-8"))


MEIP_SUBSIDIARIES: dict[str, dict] = _load("meip_subsidiaries.json")
MEIP_MNE_HEADS: dict[str, dict] = _load("meip_mne_heads.json")


def _norm(v: str | None) -> str:
    return (v or "").strip().casefold()


def _identifiers(lei: str, raw_ids: dict, known_ids: dict[str, str] | None) -> list[MeipIdentifier]:
    known = {k: _norm(v) for k, v in (known_ids or {}).items()}
    out = [
        # The LEI is the match key — it is the subject's own GLEIF-verified LEI.
        MeipIdentifier(scheme="lei", label=_ID_LABELS["lei"], value=lei, corroborated=True)
    ]
    for key in ("opencorporates", "permid", "capiq"):
        val = str(raw_ids.get(key) or "").strip()
        if not val:
            continue
        corro_key = _CORROBORATABLE.get(key)
        corroborated = bool(corro_key and known.get(corro_key) == _norm(val))
        out.append(
            MeipIdentifier(
                scheme=key, label=_ID_LABELS[key], value=val, corroborated=corroborated
            )
        )
    return out


def meip_lookup(lei: str | None, known_ids: dict[str, str] | None = None) -> MeipMatch | None:
    """Return the MEIP signpost match for a subject LEI, or ``None``.

    ``known_ids`` carries the subject's identifiers already known from GLEIF
    (keys ``opencorporates`` / ``capiq``) so matching MEIP identifiers can be
    flagged as corroborated."""
    if not lei:
        return None
    key = lei.strip().upper()

    head = MEIP_MNE_HEADS.get(key)
    if head is not None:
        return MeipMatch(
            mode="mne_head",
            lei=key,
            name=head.get("name", ""),
            iso3=head.get("iso3", ""),
            parent_mne=head.get("parent_mne", ""),
            alt_names=head.get("alt_names", []),
            address=head.get("address", ""),
            identifiers=_identifiers(key, head.get("identifiers", {}), known_ids),
            subsidiaries_total=head.get("subsidiaries_total"),
            subsidiaries_with_lei=head.get("subsidiaries_with_lei"),
        )

    sub = MEIP_SUBSIDIARIES.get(key)
    if sub is not None:
        return MeipMatch(
            mode="subsidiary",
            lei=key,
            name=sub.get("name", ""),
            iso3=sub.get("iso3", ""),
            parent_mne=sub.get("parent_mne", ""),
            immediate_parent=sub.get("immediate_parent") or None,
            alt_names=sub.get("alt_names", []),
            address=sub.get("address", ""),
            identifiers=_identifiers(key, sub.get("identifiers", {}), known_ids),
        )

    return None
