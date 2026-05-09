"""Wikidata adapter.

Wikidata is OpenCheck's primary cross-source identifier bridge: a
Q-ID resolved from any one source can be reconciled against every
other source. Two endpoints are used:

* ``wbsearchentities`` (MediaWiki Action API) — cheap fuzzy name
  lookup, returns Q-IDs + labels + descriptions. Used for search.
* Wikidata Query Service (SPARQL) at
  ``WIKIDATA_SPARQL_ENDPOINT`` — used for fetch, a single query
  returns everything the BODS mapper and risk-signal layer need
  about a Q-ID (P31 instance, DOB/DOD, citizenship, positions held,
  LEI/OpenCorporates/ISIN identifiers, jurisdiction and inception).

Live-available whenever ``OPENCHECK_ALLOW_LIVE=true`` — Wikidata is
public, CC0, and requires no key. WDQS asks for a descriptive
User-Agent, which our shared ``build_client()`` already sets.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import httpx

# 20-char ISO 17442 LEI — same shape as ``opentender._LEI_SHAPE`` but
# kept local so wikidata can be imported without pulling in opentender.
_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_SEARCH_API = "https://www.wikidata.org/w/api.php"
_CACHE_NS = "wikidata"

# Single SPARQL query that yields labels, classifying P31 (instance of),
# personal details, corporate identifiers, and incorporation info in one
# round-trip. OPTIONAL blocks mean rows are produced per combination of
# present values — the caller groups them client-side.
_FETCH_QUERY = """
SELECT ?label ?description ?instance ?instanceLabel
       ?dob ?dod ?citizenship ?citizenshipLabel
       ?position ?positionLabel ?positionStart ?positionEnd
       ?lei ?openCorporates ?isin
       ?country ?countryLabel ?inception
WHERE {
  BIND(wd:%(qid)s AS ?qid)
  OPTIONAL { ?qid rdfs:label ?label FILTER(LANG(?label) = "en") }
  OPTIONAL { ?qid schema:description ?description FILTER(LANG(?description) = "en") }
  OPTIONAL { ?qid wdt:P31 ?instance }
  OPTIONAL { ?qid wdt:P569 ?dob }
  OPTIONAL { ?qid wdt:P570 ?dod }
  OPTIONAL { ?qid wdt:P27 ?citizenship }
  OPTIONAL {
    ?qid p:P39 ?positionStmt .
    ?positionStmt ps:P39 ?position .
    OPTIONAL { ?positionStmt pq:P580 ?positionStart }
    OPTIONAL { ?positionStmt pq:P582 ?positionEnd }
  }
  OPTIONAL { ?qid wdt:P1278 ?lei }
  OPTIONAL { ?qid wdt:P1320 ?openCorporates }
  OPTIONAL { ?qid wdt:P946 ?isin }
  OPTIONAL { ?qid wdt:P17 ?country }
  OPTIONAL { ?qid wdt:P571 ?inception }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


class WikidataAdapter(SourceAdapter):
    id = "wikidata"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Wikidata",
            homepage="https://www.wikidata.org/wiki/Wikidata:Main_Page",
            description=(
                "A free and open knowledge base that can be read and "
                "edited by both humans and machines."
            ),
            license="CC0-1.0",
            attribution="Wikidata structured data, CC0 1.0.",
            supports=[SearchKind.ENTITY, SearchKind.PERSON],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        cache_key = f"{_CACHE_NS}/search/{kind.value}/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query, kind)

        payload = await self._mediawiki_get(
            {
                "action": "wbsearchentities",
                "search": query,
                "language": "en",
                "format": "json",
                "type": "item",
                "limit": "10",
            },
            cache_key=cache_key,
        )

        hits: list[SourceHit] = []
        for item in payload.get("search", []):
            hit = self._hit(item, kind)
            if hit is not None:
                hits.append(hit)
        return hits

    # ------------------------------------------------------------------
    # LEI → Q-ID lookup (used by the LEI-anchored /lookup endpoint)
    # ------------------------------------------------------------------

    async def find_qid_by_lei(self, lei: str) -> str | None:
        """Find the Wikidata Q-ID for an entity carrying a given LEI.

        Wikidata stores LEIs under property P1278. We run a tiny SPARQL
        query and return the first matching Q-ID — or ``None`` when
        Wikidata has no LEI/QID mapping for this entity yet.

        Result is cached under ``wikidata/by_lei/<LEI>`` so repeat
        lookups for the same LEI don't hit the SPARQL endpoint.
        """
        lei = lei.strip().upper()
        if not _LEI_SHAPE.match(lei):
            return None

        cache_key = f"{_CACHE_NS}/by_lei/{lei}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return None

        query = (
            'SELECT ?item WHERE { ?item wdt:P1278 "%s" } LIMIT 1' % lei
        )
        payload = await self._sparql(query, cache_key=cache_key)
        bindings = payload.get("results", {}).get("bindings") or []
        if not bindings:
            return None
        item_uri = bindings[0].get("item", {}).get("value", "")
        # The binding is a full Wikidata URI; the QID is the last segment.
        qid = item_uri.rsplit("/", 1)[-1] if item_uri else ""
        return qid or None

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Return bindings + a normalised summary for a Q-ID."""
        qid = hit_id.strip().upper()
        cache_key = f"{_CACHE_NS}/fetch/{qid}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        if not qid.startswith("Q") or not qid[1:].isdigit():
            # Defensively reject anything that isn't a Q-ID — prevents
            # SPARQL injection via the ``%(qid)s`` interpolation below.
            return {
                "source_id": self.id,
                "qid": qid,
                "bindings": [],
                "summary": {},
            }

        query = _FETCH_QUERY % {"qid": qid}
        payload = await self._sparql(query, cache_key=cache_key)
        bindings = payload.get("results", {}).get("bindings", [])

        return {
            "source_id": self.id,
            "qid": qid,
            "bindings": bindings,
            "summary": _summarise_bindings(qid, bindings),
        }

    # ------------------------------------------------------------------
    # HTTP with caching
    # ------------------------------------------------------------------

    async def _mediawiki_get(
        self, params: dict[str, str], *, cache_key: str
    ) -> dict[str, Any]:
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        async with build_client() as client:
            response = await client.get(_SEARCH_API, params=params)
            if not response.is_success:
                import logging
                logging.getLogger(__name__).warning(
                    "Wikidata MediaWiki API returned %s — skipping", response.status_code
                )
                return {}
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    async def _sparql(
        self, query: str, *, cache_key: str
    ) -> dict[str, Any]:
        """Run a SPARQL query against the Wikidata Query Service.

        Returns an empty dict (rather than raising) on any HTTP error so
        that a Wikidata rate-limit or outage doesn't crash the entire
        /lookup response.  A 429 ("1 req / min") is the most common
        failure mode on the free Render tier.
        """
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        settings = get_settings()
        async with build_client() as client:
            response = await client.get(
                settings.wikidata_sparql_endpoint,
                params={"query": query},
                headers={"Accept": "application/sparql-results+json"},
            )
            if not response.is_success:
                import logging
                logging.getLogger(__name__).warning(
                    "Wikidata SPARQL returned %s — skipping (url=%s)",
                    response.status_code,
                    response.url,
                )
                return {}
            payload = response.json()

        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _hit(item: dict[str, Any], kind: SearchKind) -> SourceHit | None:
        qid = item.get("id")
        if not qid:
            return None
        label = item.get("label") or qid
        description = item.get("description") or "Wikidata item"
        summary_bits = [description]
        if item.get("match", {}).get("type"):
            summary_bits.append(f"matched on {item['match']['type']}")

        return SourceHit(
            source_id="wikidata",
            hit_id=qid,
            kind=kind,
            name=label,
            summary=" · ".join(summary_bits),
            identifiers={"wikidata_qid": qid},
            raw=item,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="Q0",
                kind=kind,
                name=f"{query} (stub)",
                summary=(
                    "Stub Wikidata item — set OPENCHECK_ALLOW_LIVE=true to query live. "
                    "Q-IDs bridge this hit to Companies House, GLEIF, OpenSanctions "
                    "and EveryPolitician."
                ),
                identifiers={"wikidata_qid": "Q0"},
                raw={
                    "id": "Q0",
                    "label": f"{query} (stub)",
                    "sitelink": "https://www.wikidata.org/wiki/Q0",
                },
            )
        ]


# ---------------------------------------------------------------------
# Bindings → summary
# ---------------------------------------------------------------------


def _bv(row: dict[str, Any], key: str) -> str | None:
    """Return the ``value`` field of a SPARQL binding, if present."""
    cell = row.get(key)
    if not cell:
        return None
    value = cell.get("value")
    return value if value else None


def _qid_from_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    # Wikidata entity URIs look like http://www.wikidata.org/entity/Q42
    return uri.rsplit("/", 1)[-1] if "/entity/" in uri else uri


def _summarise_bindings(qid: str, bindings: list[dict[str, Any]]) -> dict[str, Any]:
    """Collapse many-row SPARQL bindings into a single normalised record.

    The SPARQL query in ``_FETCH_QUERY`` produces a row per combination
    of OPTIONAL values, so one person-QID with two citizenships and
    three positions yields six rows. This flattens them back to one
    object with deduplicated lists.
    """
    if not bindings:
        return {
            "qid": qid,
            "label": None,
            "description": None,
            "is_person": False,
            "is_entity": False,
            "instance_of": [],
            "citizenships": [],
            "positions": [],
            "identifiers": {},
            "country": None,
            "dob": None,
            "dod": None,
            "inception": None,
        }

    label = None
    description = None
    dob = None
    dod = None
    inception = None
    country_qid = None
    country_label = None

    instance_of: dict[str, str] = {}
    citizenships: dict[str, str] = {}
    positions: dict[str, dict[str, Any]] = {}
    identifiers: dict[str, str] = {}

    for row in bindings:
        label = label or _bv(row, "label")
        description = description or _bv(row, "description")
        dob = dob or _bv(row, "dob")
        dod = dod or _bv(row, "dod")
        inception = inception or _bv(row, "inception")

        c_uri = _bv(row, "country")
        c_lbl = _bv(row, "countryLabel")
        if c_uri and not country_qid:
            country_qid = _qid_from_uri(c_uri)
            country_label = c_lbl

        inst_uri = _bv(row, "instance")
        inst_lbl = _bv(row, "instanceLabel")
        if inst_uri:
            i_qid = _qid_from_uri(inst_uri)
            if i_qid:
                instance_of[i_qid] = inst_lbl or i_qid

        ctz_uri = _bv(row, "citizenship")
        ctz_lbl = _bv(row, "citizenshipLabel")
        if ctz_uri:
            ctz_qid = _qid_from_uri(ctz_uri)
            if ctz_qid:
                citizenships[ctz_qid] = ctz_lbl or ctz_qid

        pos_uri = _bv(row, "position")
        pos_lbl = _bv(row, "positionLabel")
        if pos_uri:
            pos_qid = _qid_from_uri(pos_uri)
            if pos_qid:
                positions.setdefault(
                    pos_qid,
                    {
                        "qid": pos_qid,
                        "label": pos_lbl or pos_qid,
                        "start": _bv(row, "positionStart"),
                        "end": _bv(row, "positionEnd"),
                    },
                )

        for key, scheme in (
            ("lei", "lei"),
            ("openCorporates", "opencorporates"),
            ("isin", "isin"),
        ):
            value = _bv(row, key)
            if value and scheme not in identifiers:
                identifiers[scheme] = value

    is_person = "Q5" in instance_of
    # "is_entity" is true for everything that's not a natural person —
    # companies, organisations, government bodies, geographic places,
    # etc. The mapper uses this to choose entityStatement vs personStatement.
    is_entity = bool(instance_of) and not is_person

    return {
        "qid": qid,
        "label": label,
        "description": description,
        "is_person": is_person,
        "is_entity": is_entity,
        "instance_of": [
            {"qid": i, "label": lbl} for i, lbl in instance_of.items()
        ],
        "citizenships": [
            {"qid": c, "label": lbl} for c, lbl in citizenships.items()
        ],
        "positions": list(positions.values()),
        "identifiers": identifiers,
        "country": (
            {"qid": country_qid, "label": country_label}
            if country_qid else None
        ),
        "dob": dob,
        "dod": dod,
        "inception": inception,
    }
