"""Single source of truth for which OpenTender (DIGIWHIST) tender fields are
actually consumed downstream — and a projection that slims a raw tender blob
down to exactly those fields.

The finalised ``opentender.db`` is dominated by the ``tenders.data`` column: the
raw DIGIWHIST tender JSON (~8.5 KB/record), of which only a handful of fields
are ever read. Keeping the keep-list here (rather than duplicated across the
build script and the standalone slimmer) means the two stay in lock-step, and
the behaviour tests can pin it against the real consumers.

Consumers the keep-list is derived from (verified against the source, which is
authoritative over any summary):

* ``opencheck.sources.opentender.OpenTenderAdapter._tender_hit`` — reads
  ``persistentId``/``id``, ``title``/``titleEnglish``, ``country``,
  ``buyers[].name``, ``procedureType``, ``ot.integrity``, and every body's
  ``bodyIds[]`` (``type``/``scope``/``id``) reached via ``_walk_bodies``.
* ``opencheck.sources.opentender._walk_bodies`` — walks ``buyers``,
  ``onBehalfOf``, and ``lots[].bids[].bidders``/``subcontractors`` for their
  bodies.
* ``opencheck.sources.opentender._bridge_identifier`` — reads each identifier's
  ``type``/``scope``/``id``.
* ``opencheck.bods.mapper.map_opentender`` (+ ``_opentender_body_statement`` and
  ``_format_award_details``) — reads ``id``/``persistentId``,
  ``publications[].humanReadableURL``, ``country``, ``title``,
  ``awardDecisionDate`` (tender- and lot-level), ``buyers``, ``lots[].bids[]``
  (``isWinning``, ``price.netAmount``/``currency``, ``bidders``), and each
  body's ``name``, ``bodyIds[]`` (``type``/``scope``/``id``) and ``address``
  (``street``/``city``/``postcode``/``country``).

The extractor (``scripts/extract_opentender.py``) additionally reads
``ot.transparency`` and ``isAwarded`` from the *full* tender to populate
dedicated SQLite columns at build time — those reads happen before projection,
so they are unaffected. The whole ``ot`` scores object is kept regardless (it is
small and preserves both integrity and transparency).

``project_tender`` is idempotent: projecting an already-projected record yields
an equal record.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# The keep-list — one place, imported by both the build script and the slimmer.
# ---------------------------------------------------------------------------

#: Scalar tender-level fields copied verbatim when present.
TENDER_SCALAR_FIELDS: tuple[str, ...] = (
    "id",
    "persistentId",
    "title",
    "titleEnglish",
    "country",
    "procedureType",
    "awardDecisionDate",
)

#: Per-publication fields kept (only the human-readable URL is consumed).
PUBLICATION_FIELDS: tuple[str, ...] = ("humanReadableURL",)

#: Per-body scalar fields kept (identifiers/address handled structurally).
BODY_SCALAR_FIELDS: tuple[str, ...] = ("name",)

#: Per-identifier (``bodyIds[]``) fields kept.
IDENTIFIER_FIELDS: tuple[str, ...] = ("type", "scope", "id")

#: Per-body ``address`` fields kept.
ADDRESS_FIELDS: tuple[str, ...] = ("street", "city", "postcode", "country")

#: Per-lot scalar fields kept.
LOT_SCALAR_FIELDS: tuple[str, ...] = ("awardDecisionDate",)

#: Per-bid scalar fields kept.
BID_SCALAR_FIELDS: tuple[str, ...] = ("isWinning",)

#: Per-bid ``price`` fields kept.
PRICE_FIELDS: tuple[str, ...] = ("netAmount", "currency")


def _pick(source: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    """Copy the present ``fields`` from ``source`` into a fresh dict."""
    return {k: source[k] for k in fields if k in source}


def _project_body(body: dict[str, Any]) -> dict[str, Any]:
    """Slim a DIGIWHIST ``Body`` to its consumed fields."""
    out = _pick(body, BODY_SCALAR_FIELDS)

    if "bodyIds" in body:
        ids = body["bodyIds"]
        if isinstance(ids, list):
            out["bodyIds"] = [
                _pick(ident, IDENTIFIER_FIELDS) if isinstance(ident, dict) else ident
                for ident in ids
            ]
        else:
            out["bodyIds"] = ids

    if "address" in body:
        address = body["address"]
        out["address"] = (
            _pick(address, ADDRESS_FIELDS) if isinstance(address, dict) else address
        )

    return out


def _project_bodies(bodies: Any) -> Any:
    """Slim a list of bodies, leaving non-list / non-dict shapes untouched."""
    if not isinstance(bodies, list):
        return bodies
    return [_project_body(b) if isinstance(b, dict) else b for b in bodies]


def _project_bid(bid: dict[str, Any]) -> dict[str, Any]:
    """Slim a lot's bid to its consumed fields."""
    out = _pick(bid, BID_SCALAR_FIELDS)

    if "price" in bid:
        price = bid["price"]
        out["price"] = (
            _pick(price, PRICE_FIELDS) if isinstance(price, dict) else price
        )

    for key in ("bidders", "subcontractors"):
        if key in bid:
            out[key] = _project_bodies(bid[key])

    return out


def _project_lot(lot: dict[str, Any]) -> dict[str, Any]:
    """Slim a lot to its consumed fields."""
    out = _pick(lot, LOT_SCALAR_FIELDS)

    if "bids" in lot:
        bids = lot["bids"]
        if isinstance(bids, list):
            out["bids"] = [
                _project_bid(b) if isinstance(b, dict) else b for b in bids
            ]
        else:
            out["bids"] = bids

    return out


def project_tender(tender: dict[str, Any]) -> dict[str, Any]:
    """Project a raw DIGIWHIST tender down to the consumed fields.

    Returns a fresh dict; the input is never mutated. Idempotent —
    ``project_tender(project_tender(t)) == project_tender(t)`` — because every
    kept container is itself reduced to a stable subset of keys.
    """
    out = _pick(tender, TENDER_SCALAR_FIELDS)

    # The whole ``ot`` scores object is small and covers both integrity (read at
    # query time) and transparency (read by the extractor into a column).
    if "ot" in tender:
        out["ot"] = tender["ot"]

    if "publications" in tender:
        pubs = tender["publications"]
        if isinstance(pubs, list):
            out["publications"] = [
                _pick(pub, PUBLICATION_FIELDS) if isinstance(pub, dict) else pub
                for pub in pubs
            ]
        else:
            out["publications"] = pubs

    for key in ("buyers", "onBehalfOf"):
        if key in tender:
            out[key] = _project_bodies(tender[key])

    if "lots" in tender:
        lots = tender["lots"]
        if isinstance(lots, list):
            out["lots"] = [
                _project_lot(lot) if isinstance(lot, dict) else lot for lot in lots
            ]
        else:
            out["lots"] = lots

    return out
