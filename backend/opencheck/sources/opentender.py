"""OpenTender (DIGIWHIST) adapter.

EU-wide procurement data from `https://opentender.eu`_. Built by the
DIGIWHIST project, the dataset uses the DIGIWHIST Public Procurement
Data Standard — a richer-than-OCDS schema covering tender, lot, bid
and body. Released under **CC BY-NC-SA 4.0**, which means:

* Re-use is permitted with attribution,
* But not for commercial purposes, and
* Derivatives must be re-licensed under the same terms.

This non-commercial-share-alike clause propagates: any /report or
/export bundle that includes OpenTender data must carry the same
restriction. ``app._NC_LICENSES`` already understands that prefix.

Mapping into BODS v0.4 (see ``bods/mapper.map_opentender``):

* Each ``Body`` (buyer / bidder / subcontractor) becomes an
  ``entityStatement``.
* Each ``BodyIdentifier`` is surfaced as a BODS ``identifier`` plus a
  cross-source bridge key when one applies (VAT → ``vat``, HEADER_ICO
  → ``registration_number``, ``ORGANIZATION_ID`` with GB scope →
  ``gb_coh``).
* Each winning bid produces a ``relationshipStatement`` linking the
  winning bidder (interestedParty) to the buyer (subject) with a
  ``otherInfluenceOrControl`` interest annotated with the tender id,
  award date, and contract value. This is **not** beneficial ownership
  — it's a commercial engagement — but representing it as a BODS
  relationship makes procurement data composable with the existing
  reconciler and risk service.

Live integration is parked behind ``allow_live`` + a future env var.
For now the adapter ships demo fixtures only.

.. _https://opentender.eu: https://opentender.eu/
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from ..cache import Cache
from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_CACHE_NS = "opentender"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


class OpenTenderAdapter(SourceAdapter):
    id = "opentender"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="OpenTender (DIGIWHIST)",
            homepage="https://opentender.eu/",
            license="CC-BY-NC-SA-4.0",
            attribution=(
                "Procurement data from OpenTender (DIGIWHIST), "
                "licensed CC BY-NC-SA 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            # Live mode is not wired yet — the adapter currently serves
            # only demo fixtures. Toggling allow_live alone won't enable
            # network calls until the live HTTP path lands.
            live_available=False,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self._cache.has(cache_key):
            return self._stub_search(query)

        cached = self._cache.get_payload(cache_key)
        assert cached is not None  # _cache.has just told us so
        payload = cached[0]

        return [self._tender_hit(item) for item in payload.get("tenders", [])]

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        cache_key = f"{_CACHE_NS}/tender/{_slug(hit_id)}"
        if not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        cached = self._cache.get_payload(cache_key)
        assert cached is not None
        tender = cached[0]
        return {
            "source_id": self.id,
            "tender_id": hit_id,
            "tender": tender,
        }

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _tender_hit(item: dict[str, Any]) -> SourceHit:
        tender_id = item.get("id") or ""
        title = item.get("title") or item.get("titleEnglish") or "Tender"
        country = item.get("country") or ""
        buyers = item.get("buyers") or []
        buyer_name = (buyers[0] or {}).get("name") if buyers else ""

        summary_bits: list[str] = []
        if buyer_name:
            summary_bits.append(f"buyer: {buyer_name}")
        if country:
            summary_bits.append(country)
        proc_type = item.get("procedureType")
        if proc_type:
            summary_bits.append(proc_type.replace("_", " ").lower())

        # Surface every BodyIdentifier we can reach as a flat identifier
        # map so the cross-source reconciler can bridge to GLEIF / CH /
        # OS on shared VAT, registration numbers, or LEI.
        identifiers: dict[str, str] = {"opentender_id": tender_id}
        for body in _walk_bodies(item):
            for ident in body.get("bodyIds") or []:
                key, value = _bridge_identifier(ident)
                if not (key and value):
                    continue
                # Don't let a buyer's identifier overwrite a bidder's
                # (or vice versa). Reconciler matches on equality, not
                # role — so the first seen wins.
                identifiers.setdefault(key, value)

        return SourceHit(
            source_id="opentender",
            hit_id=tender_id,
            kind=SearchKind.ENTITY,
            name=title,
            summary=" · ".join(summary_bits) or "Procurement record",
            identifiers=identifiers,
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="OT-stub-0001",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub OpenTender record — drop a fixture under "
                    "data/cache/demos/opentender/ to see real procurement "
                    "data flow through. Live opentender.eu integration "
                    "is a TODO."
                ),
                identifiers={"opentender_id": "OT-stub-0001"},
                raw={
                    "id": "OT-stub-0001",
                    "title": f"{query} stub tender",
                    "buyers": [{"name": "Stub Authority"}],
                    "bidders": [{"name": f"{query} (stub)"}],
                },
            )
        ]


_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


def _walk_bodies(tender: dict[str, Any]):
    """Yield every Body referenced by a DIGIWHIST tender.

    Buyers + onBehalfOf live at the top level; bidders + subcontractors
    live nested under lots → bids. We flatten both so the search-time
    identifier sweep doesn't miss the supplier-side bridges.
    """
    for body in tender.get("buyers") or []:
        if isinstance(body, dict):
            yield body
    for body in tender.get("onBehalfOf") or []:
        if isinstance(body, dict):
            yield body
    for lot in tender.get("lots") or []:
        for bid in lot.get("bids") or []:
            for body in bid.get("bidders") or []:
                if isinstance(body, dict):
                    yield body
            for body in bid.get("subcontractors") or []:
                if isinstance(body, dict):
                    yield body


def _looks_like_lei(value: str) -> bool:
    """LEIs are 20-character ISO 17442 alphanumeric strings.

    DIGIWHIST has no dedicated ``LEI`` BodyIdentifierType — practitioners
    record LEIs under ``ETALON_ID`` (scope=GLOBAL). We detect them by
    shape so the cross-source reconciler can bridge to GLEIF on the LEI
    regardless of how the publisher tagged it.
    """
    return bool(_LEI_SHAPE.match(value.upper()))


def _bridge_identifier(ident: dict[str, Any]) -> tuple[str | None, str | None]:
    """Map a DIGIWHIST ``BodyIdentifier`` to a cross-source bridge key.

    Returns ``(scheme, value)`` or ``(None, None)`` if we don't have a
    strong-bridge equivalent. The scheme uses the same names the rest
    of OpenCheck uses: ``vat``, ``registration_number``, ``gb_coh``,
    ``lei``.
    """
    type_ = (ident.get("type") or "").upper()
    scope = (ident.get("scope") or "").upper()
    value = ident.get("id")
    if not value:
        return None, None
    value = str(value).strip()
    if not value:
        return None, None

    # LEI detection trumps the declared type — if it walks like an LEI…
    if _looks_like_lei(value):
        return "lei", value.upper()

    if type_ == "VAT":
        return "vat", value
    if type_ == "ORGANIZATION_ID" and scope == "GB":
        return "gb_coh", value
    if type_ in {"HEADER_ICO", "STATISTICAL", "TAX_ID", "TRADE_REGISTER"}:
        # These are country-specific national registry IDs; surface
        # them under a generic key so reconciler doesn't lose them.
        return "registration_number", value
    if type_ == "BVD_ID":
        return "bvd_id", value
    return None, None
