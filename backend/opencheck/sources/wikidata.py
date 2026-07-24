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

import asyncio
import hashlib
from typing import Any

import httpx

from .. import identifiers
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

# 20-char ISO 17442 LEI shape (shared; see opencheck/identifiers.py).
_LEI_SHAPE = identifiers.LEI_PATH_SHAPE

_SEARCH_API = "https://www.wikidata.org/w/api.php"
_CACHE_NS = "wikidata"

# Single SPARQL query that yields labels, classifying P31 (instance of),
# personal details, corporate identifiers, and incorporation info in one
# round-trip. OPTIONAL blocks mean rows are produced per combination of
# present values — the caller groups them client-side.
#
# P749 (parent organization) and P127 (owned by) are included so that the
# BODS mapper can emit lightweight relationship statements for entity subjects
# that declare a corporate parent on Wikidata.  Both are treated as "owning /
# controlling entity" relationships; the mapper emits ``otherInfluenceOrControl``
# interests with ``beneficialOwnershipOrControl: false``.
_FETCH_QUERY = """
SELECT ?label ?labelLang ?description ?instance ?instanceLabel
       ?dob ?dod ?citizenship ?citizenshipLabel
       ?position ?positionLabel ?positionStart ?positionEnd
       ?lei ?openCorporates ?isin
       ?country ?countryLabel ?inception
       ?parentOrg ?parentOrgLabel ?ownedBy ?ownedByLabel
WHERE {
  BIND(wd:%(qid)s AS ?qid)
  OPTIONAL {
    ?qid rdfs:label ?label
    FILTER(LANG(?label) IN ("en", "de", "fr", "es", "pt", "ru", "uk", "el", "zh", "ar"))
    BIND(LANG(?label) AS ?labelLang)
  }
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
  OPTIONAL { ?qid wdt:P749 ?parentOrg }
  OPTIONAL { ?qid wdt:P127 ?ownedBy }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""


# SPARQL query for *current* roleholders of an entity — people who hold
# well-known corporate governance roles (P169 CEO, P488 chair, P3320 board
# member, P6346 treasurer, P1037 director/manager) with no end-date qualifier.
# Run concurrently with _FETCH_QUERY for entity subjects only.
_ROLEHOLDER_QUERY = """
SELECT ?roleLabel ?person ?personLabel ?start WHERE {
  {
    wd:%(qid)s p:P169 ?stmt .
    ?stmt ps:P169 ?person .
    BIND("chief executive officer" AS ?roleLabel)
    OPTIONAL { ?stmt pq:P580 ?start }
    OPTIONAL { ?stmt pq:P582 ?end }
  } UNION {
    wd:%(qid)s p:P488 ?stmt .
    ?stmt ps:P488 ?person .
    BIND("chairperson" AS ?roleLabel)
    OPTIONAL { ?stmt pq:P580 ?start }
    OPTIONAL { ?stmt pq:P582 ?end }
  } UNION {
    wd:%(qid)s p:P3320 ?stmt .
    ?stmt ps:P3320 ?person .
    BIND("board member" AS ?roleLabel)
    OPTIONAL { ?stmt pq:P580 ?start }
    OPTIONAL { ?stmt pq:P582 ?end }
  } UNION {
    wd:%(qid)s p:P6346 ?stmt .
    ?stmt ps:P6346 ?person .
    BIND("treasurer" AS ?roleLabel)
    OPTIONAL { ?stmt pq:P580 ?start }
    OPTIONAL { ?stmt pq:P582 ?end }
  } UNION {
    wd:%(qid)s p:P1037 ?stmt .
    ?stmt ps:P1037 ?person .
    BIND("director/manager" AS ?roleLabel)
    OPTIONAL { ?stmt pq:P580 ?start }
    OPTIONAL { ?stmt pq:P582 ?end }
  }
  FILTER(!BOUND(?end))
  FILTER(ISIRI(?person))
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""


# Controlling-owner extraction (prototype — see docs/wikidata-ownership.md).
# One statement node per owner so qualifiers (P1107 proportion) and references
# (prov:wasDerivedFrom → P248 stated-in / P854 reference URL / P813 retrieved)
# come back alongside each ownership edge. P127 (owned by) and P749 (parent
# organization) are queried as two UNION branches so we can record which.
_OWNERSHIP_QUERY = """
SELECT ?owner ?ownerLabel ?via ?ownerClass ?proportion
       ?statedIn ?statedInLabel ?refUrl ?retrieved
WHERE {
  {
    wd:%(qid)s p:P127 ?st . ?st ps:P127 ?owner . BIND("P127" AS ?via)
  } UNION {
    wd:%(qid)s p:P749 ?st . ?st ps:P749 ?owner . BIND("P749" AS ?via)
  }
  OPTIONAL { ?owner wdt:P31 ?ownerClass }
  OPTIONAL { ?st pq:P1107 ?proportion }
  OPTIONAL {
    ?st prov:wasDerivedFrom ?ref .
    OPTIONAL { ?ref pr:P248 ?statedIn }
    OPTIONAL { ?ref pr:P854 ?refUrl }
    OPTIONAL { ?ref pr:P813 ?retrieved }
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""


def _parse_roleholders(bindings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse roleholder SPARQL rows into a deduplicated per-person list.

    Returns a list of::

        {
          "qid":   "Q12345",
          "name":  "Jane Smith",
          "roles": [{"label": "chief executive officer", "start": "2021-04-01"}, ...]
        }

    Multiple rows for the same person (one per role) are merged into a
    single entry whose ``roles`` list carries each distinct role once.
    """
    by_person: dict[str, dict[str, Any]] = {}
    for row in bindings:
        person_uri = _bv(row, "person")
        if not person_uri:
            continue
        person_qid = _qid_from_uri(person_uri)
        if not person_qid or not person_qid.startswith("Q"):
            continue
        name = _bv(row, "personLabel") or person_qid
        role_label = _bv(row, "roleLabel") or "officeholder"
        start = _bv(row, "start")

        if person_qid not in by_person:
            by_person[person_qid] = {"qid": person_qid, "name": name, "roles": []}

        existing_labels = {r["label"] for r in by_person[person_qid]["roles"]}
        if role_label not in existing_labels:
            by_person[person_qid]["roles"].append(
                {"label": role_label, "start": start}
            )

    return list(by_person.values())


# ---------------------------------------------------------------------
# Controlling-owner classification + parsing (prototype)
# ---------------------------------------------------------------------

# Wikidata P31 class QIDs → owner category. Direct-P31 based (subclass-aware
# refinement via P279* is a follow-up); a name-hint fallback handles the common
# cases where Wikidata's P31 is generic. Tuneable — see docs/wikidata-ownership.md.
_PERSON_CLASSES = frozenset({"Q5"})                                 # human
_FOUNDATION_CLASSES = frozenset({"Q157031", "Q708676", "Q163740"})  # foundation, charity, nonprofit
_ARRANGEMENT_CLASSES = frozenset({"Q193076", "Q2992826"})           # trust / fiduciary arrangement
_STATE_CLASSES = frozenset({"Q3624078", "Q7275"})                   # sovereign state, state
_STATEBODY_CLASSES = frozenset({"Q7188", "Q327333", "Q192350", "Q2659904"})  # govt, agency, ministry
_GLIE_CLASSES = frozenset({"Q1808582"})                             # sovereign wealth fund
_FAMILY_CLASSES = frozenset({"Q8436", "Q17304012"})                 # family, noble family

_FOUNDATION_HINTS = ("foundation", "stiftung", "fondation", "fondazione", "stichting")
_ARRANGEMENT_HINTS = ("treuhand", " trust", "fiducie", "fideicomiso")

# category → (BODS statement kind, entityType.type). Persons have no entityType.
_CATEGORY_BODS: dict[str, tuple[str, str | None]] = {
    "person": ("person", None),
    "foundation": ("entity", "registeredEntity"),
    "arrangement": ("entity", "arrangement"),
    "company": ("entity", "registeredEntity"),
    "glie": ("entity", "registeredEntity"),
    "state": ("entity", "state"),
    "statebody": ("entity", "stateBody"),
}


def _classify_owner(classes: set[str], name: str | None) -> str:
    """Map an owner's P31 class set (+ name hints) to a controlling-owner category."""
    name_l = (name or "").lower()
    if classes & _FAMILY_CLASSES:
        return "family"
    if classes & _PERSON_CLASSES:
        return "person"
    if classes & _STATEBODY_CLASSES:
        return "statebody"
    if classes & _STATE_CLASSES:
        return "state"
    if classes & _GLIE_CLASSES:
        return "glie"
    if classes & _FOUNDATION_CLASSES or any(h in name_l for h in _FOUNDATION_HINTS):
        return "foundation"
    if classes & _ARRANGEMENT_CLASSES or any(h in name_l for h in _ARRANGEMENT_HINTS):
        return "arrangement"
    return "company"


def _proportion_to_pct(value: str | None) -> float | None:
    """Wikidata P1107 is stored as a ratio (0.92) or, rarely, a percent. Normalise
    to a percentage. Indicative only — Wikidata conflates capital / voting / time."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    pct = f * 100 if f <= 1.0 else f
    return round(pct, 4)


def _parse_ownership(bindings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse ownership SPARQL rows into a per-owner list ready for BODS mapping.

    Each entry carries the owner's category (person / foundation / arrangement /
    company / glie / state / stateBody), an **indicative** share percentage, the
    relating property/properties (P127 / P749), and the statement's references.
    **Family-typed owners are dropped** — a "family" is neither a legal entity nor
    a single natural person, so we do not fabricate a person or invent a group.
    """
    by: dict[str, dict[str, Any]] = {}
    for row in bindings:
        oqid = _qid_from_uri(_bv(row, "owner"))
        if not oqid or not oqid.startswith("Q"):
            continue
        rec = by.get(oqid)
        if rec is None:
            rec = {
                "qid": oqid, "name": _bv(row, "ownerLabel") or oqid,
                "via": set(), "classes": set(), "proportion": None,
                "references": [], "_refseen": set(),
            }
            by[oqid] = rec
        via = _bv(row, "via")
        if via:
            rec["via"].add(via)
        cls = _qid_from_uri(_bv(row, "ownerClass"))
        if cls:
            rec["classes"].add(cls)
        prop = _bv(row, "proportion")
        if prop and rec["proportion"] is None:
            rec["proportion"] = prop
        url, stated, retrieved = _bv(row, "refUrl"), _bv(row, "statedInLabel"), _bv(row, "retrieved")
        if url or stated:
            key = (stated or "", url or "")
            if key not in rec["_refseen"]:
                rec["_refseen"].add(key)
                rec["references"].append(
                    {"stated_in": stated, "url": url, "retrieved": retrieved}
                )

    out: list[dict[str, Any]] = []
    for rec in by.values():
        category = _classify_owner(rec["classes"], rec["name"])
        if category == "family":
            continue  # decided: drop family owners (see docs/wikidata-ownership.md)
        bods_kind, entity_type = _CATEGORY_BODS[category]
        share = _proportion_to_pct(rec["proportion"])
        out.append({
            "qid": rec["qid"],
            "name": rec["name"],
            "category": category,
            "bods_kind": bods_kind,        # "person" | "entity"
            "entity_type": entity_type,    # BODS entityType.type, or None for persons
            "via": sorted(rec["via"]),     # ["P127"] owned-by / ["P749"] parent
            "share_percent": share,        # INDICATIVE only — never render as fact alone
            "references": rec["references"],
            "has_reference": bool(rec["references"]),
        })
    return out


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
        if kind == SearchKind.PERSON and hits:
            # wbsearchentities matches labels/aliases with no type filter,
            # so a person query returns paintings, songs and films named
            # after people ("Tony Blair", 1999 single by Chumbawamba) —
            # exact-name noise that a person screen must not rank as a
            # match. Keep only instance-of-human (Q5) items.
            hits = await self._filter_to_humans(hits)
        return hits

    async def _filter_to_humans(self, hits: list[SourceHit]) -> list[SourceHit]:
        """Keep only hits whose item is an instance of human (P31 → Q5).

        One batched SPARQL VALUES query for the whole hit list. Fails
        OPEN: if the query errors or times out (``_sparql`` returns an
        empty dict), the unfiltered hits are returned — a Wikidata outage
        must degrade to "noisier results", never to "person vanished".
        """
        qids = [h.hit_id for h in hits if h.hit_id.startswith("Q")]
        if not qids:
            return hits
        cache_key = f"{_CACHE_NS}/humans/{_slug('|'.join(sorted(qids)))}"
        values = " ".join(f"wd:{q}" for q in qids)
        query = (
            "SELECT ?item WHERE { VALUES ?item { %s } ?item wdt:P31 wd:Q5 }"
            % values
        )
        try:
            payload = await self._sparql(query, cache_key=cache_key)
        except Exception:  # noqa: BLE001 — transport errors also fail open
            return hits
        bindings = payload.get("results", {}).get("bindings")
        if bindings is None:
            return hits  # SPARQL failure — fail open
        humans = {
            b["item"]["value"].rsplit("/", 1)[-1]
            for b in bindings
            if isinstance(b.get("item"), dict) and b["item"].get("value")
        }
        return [h for h in hits if not h.hit_id.startswith("Q") or h.hit_id in humans]

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
        # Additionally drop check-digit-invalid LEIs when enforcement is on —
        # P1278 values are hand-entered and typos happen.
        if identifiers.checksums_enforced() and not identifiers.is_valid_lei(
            lei, checksum=True
        ):
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
        """Return bindings + a normalised summary for a Q-ID.

        For entity subjects, the roleholder query (P169/P488/P3320/P6346/P1037)
        is run concurrently with the main fetch query so we add only one
        SPARQL round-trip rather than two sequential ones.
        """
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

        main_query = _FETCH_QUERY % {"qid": qid}
        rh_query   = _ROLEHOLDER_QUERY % {"qid": qid}
        own_query  = _OWNERSHIP_QUERY % {"qid": qid}
        rh_cache_key  = f"{_CACHE_NS}/roleholders/{qid}"
        own_cache_key = f"{_CACHE_NS}/ownership/{qid}"

        # Run all three SPARQL queries concurrently — roleholders (P169/P488…)
        # and controlling owners (P127/P749) are separate queries because they
        # carry statement-level qualifiers/references the main query can't.
        main_payload, rh_payload, own_payload = await asyncio.gather(
            self._sparql(main_query, cache_key=cache_key),
            self._sparql(rh_query,   cache_key=rh_cache_key),
            self._sparql(own_query,  cache_key=own_cache_key),
        )

        bindings     = main_payload.get("results", {}).get("bindings", [])
        rh_bindings  = rh_payload.get("results",  {}).get("bindings", [])
        own_bindings = own_payload.get("results", {}).get("bindings", [])

        if not bindings:
            # The main SPARQL query returned nothing — most likely a WDQS
            # timeout or transient error that wasn't cached.  Return a stub
            # so the lookup router suppresses this source card rather than
            # showing an entity named "Q157062" with all-null fields.
            import logging
            logging.getLogger(__name__).warning(
                "Wikidata SPARQL returned empty bindings for %s — marking stub",
                qid,
            )
            return {
                "source_id": self.id,
                "qid": qid,
                "bindings": [],
                "summary": {},
                "is_stub": True,
            }

        summary = _summarise_bindings(qid, bindings)
        # Only parse roleholders + controlling owners for entity subjects
        # (companies/orgs). Person pages don't carry P169/P488 or P127/P749 so
        # both queries return nothing.
        is_entity = summary.get("is_entity")
        summary["roleholders"] = _parse_roleholders(rh_bindings) if is_entity else []
        summary["controlling_owners"] = _parse_ownership(own_bindings) if is_entity else []

        return {
            "source_id": self.id,
            "qid": qid,
            "bindings": bindings,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Controlling-owner extraction (prototype — not yet on the main fetch path)
    # ------------------------------------------------------------------

    async def fetch_ownership(self, hit_id: str) -> list[dict[str, Any]]:
        """Return the entity's classified controlling owners (P127 / P749).

        Prototype for the unified Wikidata ownership enrichment (see
        docs/wikidata-ownership.md): foundation / family / SOE owners come from
        the same extraction, classified and mapped toward BODS. Family owners are
        dropped. Shares are indicative; references are captured for provenance.
        """
        qid = hit_id.strip().upper()
        if not qid.startswith("Q") or not qid[1:].isdigit():
            return []
        cache_key = f"{_CACHE_NS}/ownership/{qid}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []
        payload = await self._sparql(_OWNERSHIP_QUERY % {"qid": qid}, cache_key=cache_key)
        return _parse_ownership(payload.get("results", {}).get("bindings", []))

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

        # WDQS returns HTTP 200 for timeouts and internal errors, with an
        # "error" key and no "results" key.  Caching these poisons the cache
        # permanently (the empty bindings get served forever).  Only cache
        # genuine SPARQL result sets.
        if "error" in payload or "results" not in payload:
            import logging
            logging.getLogger(__name__).warning(
                "Wikidata SPARQL returned error/unexpected JSON — not caching "
                "(url=%s): %s",
                response.url,
                str(payload)[:300],
            )
            return {}

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
            "parent_orgs": [],
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
    parent_orgs: dict[str, str] = {}  # qid → label, merged from P749 + P127

    labels: dict[str, str] = {}  # BCP-47 lang → label (Phase E)
    for row in bindings:
        row_label = _bv(row, "label")
        row_lang = _bv(row, "labelLang")
        if row_label and row_lang and row_lang not in labels:
            labels[row_lang] = row_label
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

        # P749 (parent organization) and P127 (owned by) — merged into one
        # deduplicated collection keyed by QID so we emit one stub entity +
        # one relationship statement per distinct parent regardless of which
        # property contributed it.
        for parent_key, label_key in (
            ("parentOrg", "parentOrgLabel"),
            ("ownedBy", "ownedByLabel"),
        ):
            parent_uri = _bv(row, parent_key)
            if parent_uri:
                parent_qid = _qid_from_uri(parent_uri)
                if parent_qid and parent_qid not in parent_orgs:
                    parent_label = _bv(row, label_key) or parent_qid
                    parent_orgs[parent_qid] = parent_label

    is_person = "Q5" in instance_of
    # "is_entity" is true for everything that's not a natural person —
    # companies, organisations, government bodies, geographic places,
    # etc. The mapper uses this to choose entityStatement vs personStatement.
    is_entity = bool(instance_of) and not is_person

    # Prefer the English label for display; any other captured language is
    # surfaced via ``labels`` and mapped to alternateNames downstream.
    if labels:
        label = labels.get("en") or label or next(iter(labels.values()))
    return {
        "qid": qid,
        "label": label,
        "labels": [
            {"language": lang, "label": lbl} for lang, lbl in sorted(labels.items())
        ],
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
        "parent_orgs": [
            {"qid": p, "label": lbl} for p, lbl in parent_orgs.items()
        ],
    }
