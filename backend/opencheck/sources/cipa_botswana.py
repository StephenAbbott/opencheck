"""Botswana CIPA company and beneficial ownership register — CDD category.

Serves the beneficial owners, shareholders and directors that Botswana's
**Companies and Intellectual Property Authority** (CIPA) publishes on its
public register at ``www.cipa.co.bw`` (the Online Business Registration System,
built and run by Foster Moore on its Verne platform). GLEIF Registration
Authority code ``RA000035``.

Offline / curated (why)
-----------------------
CIPA publishes no API, no bulk file and no reuse licence, and its register
pages are session-bound. So — the ``cac_nigeria`` pattern — a curated set of
LEI-anchored companies is harvested **offline** by
``scripts/build_cipa_botswana_index.py harvest`` and committed as
``opencheck/data/cipa_botswana.json``. At runtime this adapter answers
``fetch_by_lei`` as a dict lookup — no network. ``bods.map_cipa_botswana``
maps each record to BODS v0.4.

The set is every LEI GLEIF files under ``RA000035`` (plus the ``RA000821``
records that file a CIPA UIN) that the register resolves to one company. It
holds names, roles, nationalities or countries, dates and share counts — no
addresses, documents or contact details (Stephen, 30 Sept 2026), although the
register pages publish addresses for every party.

CIPA's own BODS export
----------------------
Each company page carries a hidden "Download BODS JSON" button whose data also
drives the page's ownership diagram. It is BODS v0.3 and drops the subject's
UIN, every interest date and the free-text interest detail, and it leaves out
legal shareholders. OpenCheck builds its statements from the page's structured
records instead (see ``bods/mappers/botswana.py``).

Identifier corroboration
------------------------
The register publishes the UIN (``BW00000466545``) and, for companies
re-registered in 2019, the old company number (``CO1969/660``). It does not
publish the LEI, which the harvest derives from GLEIF. So this adapter asserts
``bw_cipa_uin`` / ``bw_cipa_old_number`` and never ``lei``.

Licence: public register; CIPA states no reuse terms. Attribution is given.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .. import provenance
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

log = logging.getLogger(__name__)

#: GLEIF Registration Authority code for CIPA's Register of Companies
#: (verified 29 Sept 2026: 38 LEIs, every one filing a UIN or an old number).
CIPA_RA_CODE = "RA000035"

#: Committed, LEI-keyed index (built by scripts/build_cipa_botswana_index.py).
_INDEX_PATH = Path(
    os.environ.get("CIPA_BOTSWANA_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "cipa_botswana.json")
)

_index: dict[str, dict[str, Any]] | None = None
#: The harvest date from the file's own ``meta.harvested`` — a real retrieval
#: date, unlike the file's mtime.
_harvested_at: datetime | None = None


def _get_index() -> dict[str, dict[str, Any]]:
    """Load the committed LEI-keyed CIPA index (cached in a module singleton)."""
    global _index, _harvested_at
    if _index is None:
        try:
            with open(_INDEX_PATH, encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("index") or {}
            _index = {
                str(k).strip().upper(): v
                for k, v in raw.items()
                if len(str(k).strip()) == 20
            }
            meta = data.get("meta") or {}
            harvested = str(meta.get("harvested") or "").strip()
            _harvested_at = None
            if harvested:
                try:
                    _harvested_at = datetime.fromisoformat(harvested).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    _harvested_at = None
            log.info(
                "CIPA Botswana index loaded: %s entities (%s harvest)",
                len(_index), meta.get("harvested"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("CIPA Botswana index unavailable: %s", exc)
            _index = {}
    return _index


def _load_and_declare() -> dict[str, dict[str, Any]]:
    """Load the index and declare the payload curated, never live."""
    index = _get_index()
    provenance.record_curated(
        "Curated CIPA register example set committed to the repository",
        harvested_at=_harvested_at,
    )
    return index


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singleton so a fresh index is loaded."""
    global _index
    _index = None


def cipa_identifiers(record: dict[str, Any]) -> dict[str, str]:
    """The identifiers CIPA itself publishes for a company — never the LEI."""
    out: dict[str, str] = {}
    if record.get("uin"):
        out["bw_cipa_uin"] = str(record["uin"])
    if record.get("old_number"):
        out["bw_cipa_old_number"] = normalise_cipa_number(str(record["old_number"]))
    return out


def normalise_cipa_number(value: str) -> str:
    """Canonical form of a CIPA number: no spaces, upper case, and the
    ``C0`` typo some LEI records carry (``C02015/11750``) read as ``CO``."""
    text = "".join(str(value or "").split()).upper()
    if text.startswith("C0"):
        text = "CO" + text[2:]
    return text


#: The shapes GLEIF files for a CIPA company: the UIN, and the number a company
#: carried before the 2019 re-registration (``CO1988/1163``).
_UIN_SHAPE = re.compile(r"^BW\d{11}$")
_OLD_SHAPE = re.compile(r"^CO\d{4}/\d+$")
#: NBFIRA's code: a few of its records file the company's CIPA UIN.
NBFIRA_RA_CODE = "RA000821"


def gleif_cipa_identifier(ra_code: str | None, registered_as: str | None) -> tuple[str, str] | None:
    """The CIPA identifier a GLEIF record files, as ``(key, value)``, or None.

    Lets the reconciler bridge GLEIF and CIPA on the number both publish.
    ``RA000035`` files a UIN or an old company number; ``RA000821`` is NBFIRA,
    whose records count only when the number is a CIPA UIN.
    """
    number = normalise_cipa_number(registered_as or "")
    if ra_code not in (CIPA_RA_CODE, NBFIRA_RA_CODE) or not number:
        return None
    if _UIN_SHAPE.match(number):
        return "bw_cipa_uin", number
    if ra_code == CIPA_RA_CODE and _OLD_SHAPE.match(number):
        return "bw_cipa_old_number", number
    return None


class CipaBotswanaAdapter(SourceAdapter):
    """Botswana CIPA register adapter — CDD category."""

    id = "cipa_botswana"

    #: LEI-keyed source, dispatched directly in routers/lookup.py alongside
    #: the other curated sets (cac_nigeria, eiti_soe), not via an RA deriver.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Botswana CIPA — company and beneficial ownership register",
            homepage="https://www.cipa.co.bw",
            description=(
                "Beneficial owners, shareholders and directors from the public "
                "register of Botswana's Companies and Intellectual Property "
                "Authority (CIPA), whose beneficial ownership register is "
                "structured in line with BODS. Curated example set: the "
                "companies GLEIF records under CIPA's Register of Companies, "
                "harvested from the register's public pages and mapped to BODS "
                "v0.4. Not a live feed — CIPA publishes no API or bulk file."
            ),
            license="Public register (www.cipa.co.bw); no reuse terms stated",
            attribution=(
                "Companies and Intellectual Property Authority (CIPA), "
                "Republic of Botswana — www.cipa.co.bw"
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(_get_index()),
            category="cdd",
            is_national_register=True,
            country="BW",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # Identifier-keyed (LEI) source; free-text search intentionally empty.
        return []

    def covers_lei(self, lei: str) -> bool:
        """Whether the curated CIPA set committed to this repository holds this LEI.

        The lookup pipeline asks before dispatching, so a company the file
        cannot describe is never announced as a source being queried. The set
        is the LEI-anchored companies only, not the whole Botswana register —
        this governs whether the source is *applicable*, and says nothing about
        the company.
        """
        return (lei or "").strip().upper() in _get_index()

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the CIPA bundle for a LEI, or ``None`` when not in the set."""
        lei_norm = (lei or "").strip().upper()
        index = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return None
        return self._build_bundle(lei_norm, record)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        lei_norm = (hit_id or "").strip().upper()
        index = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return self._build_bundle(lei_norm, record)

    def _build_bundle(self, lei: str, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": lei,
            "lei": lei,
            "is_stub": False,
            "record": record,
            "identifiers": cipa_identifiers(record),
        }
