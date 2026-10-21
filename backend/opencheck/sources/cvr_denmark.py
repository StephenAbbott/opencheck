"""Danish Central Business Register (CVR) adapter.

CVR (Det Centrale Virksomhedsregister) is Denmark's statutory register for
all registered businesses, maintained by Erhvervsstyrelsen (the Danish
Business Authority).

This adapter uses the Datafordeler GraphQL API (register: CVR, version: 2).

  Endpoint:  https://graphql.datafordeler.dk/CVR/v2
  Auth:      ``?apiKey=<key>`` — raw API key as query parameter (no encoding required).
  Method:    POST application/json with {"query": "...", "variables": {...}}

Access model
------------
  All CVR entities except CVRPerson are publicly accessible with only an
  API key (no special access request required).
  CVRPerson (natural persons with CPR-number) is a restricted entity that
  requires OAuth + register approval — it is NOT fetched here. A person's
  CVR unit (``CVR_CVREnhed``: type and participant number) and name
  (``CVR_Navn``) are open, and are read for fully liable participants.

Fetch strategy
--------------
  1. CVR_Virksomhed — fetch by CVRNummer (Long) to get the CVREnhedsId (UUID-
     style string that joins all other entity tables).
  2. One request per table (DAF-GQL-0010), in parallel: CVR_Navn,
     CVR_Adressering, CVR_Branche, CVR_Virksomhedsform,
     CVR_FuldtAnsvarligDeltagerRelation, keyed by CVREnhedsId. Each pages:
     Datafordeler answers 100 rows a page unless asked for more.
  3. Only when the firm has fully liable participants (I/S, K/S, P/S):
     CVR_CVREnhed and CVR_Navn for all of them, with an ``in`` filter of at
     most 100 ids (DAF-GQL-0016). Two requests per 100 participants.

  Current-state records are selected by filtering for nodes where
  ``virkningTil`` is null in Python (these are the still-effective rows).

GLEIF integration
-----------------
  GLEIF Registration Authority code: RA000170
    (Det Centrale Virksomhedsregister / Erhvervsstyrelsen)
  The ``registeredAs`` field in GLEIF records for Danish entities contains
  the CVR number (8-digit, possibly zero-padded). ``gleif.py`` extracts
  ``dk_cvr`` and passes it to ``fetch()`` here.

Bitemporal note
---------------
  CVR data is bitemporal (registrering + virkning). This adapter reads the
  current effective state without a ``virkningstid`` argument, then filters
  Python-side for null ``virkningTil``. Historical rows are preserved in the
  raw bundle for completeness.

Privacy note
------------
  CVRPerson (natural persons linked via CPR number) is not read: no CPR
  number and no birth date ever reach the bundle. A fully liable participant
  who is a person is named and keyed on CVR's participant number
  (enhedsnummer) — the number datacvr.virk.dk itself shows. That is the only
  relation to people Datafordeler's CVR data carries: directors, boards,
  legal owners and beneficial owners are not in it (they are in
  Erhvervsstyrelsen's System-til-System distribution, a separate access).

Datafordeler account setup
--------------------------
  1. Register at portal.datafordeler.dk
  2. Create an IT-system
  3. Generate an API key under the IT-system (valid 2 years, renewable)
  4. Set CVR_DENMARK_API_KEY=<key> in .env
  The key is passed verbatim as the ``apiKey`` query parameter — no encoding required.
  API keys give access to all non-restricted CVR entities (no extra access application needed).

Attribution
-----------
  "Indeholder data fra Det Centrale Virksomhedsregister (CVR),
   Erhvervsstyrelsen / Danish Business Authority.
   Data distribueret via Datafordelerens CVR GraphQL API."
  License: Danish open government data (CVR brugervilkår)
  Source:  https://graphql.datafordeler.dk/CVR/v2
  Portal:  https://portal.datafordeler.dk/
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..secret_scrub import describe_exception
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.cvr_denmark import CVRBundle

_log = logging.getLogger(__name__)

# GLEIF Registration Authority code for Denmark's CVR.
DK_CVR_RA_CODE: str = "RA000170"

_GRAPHQL_URL = "https://graphql.datafordeler.dk/CVR/v2"
_CVR_PORTAL = "https://datacvr.virk.dk/enhed/virksomhed/{cvr}"

_CACHE_NS = "cvr_denmark"

# CVR number: exactly 8 digits.
_CVR_LEN = 8

# Danish legal form codes → English description.
# Source: https://datacvr.virk.dk/artikel/virksomhedsformer
_LEGAL_FORM_MAP: dict[str, str] = {
    "10": "Enkeltmandsvirksomhed (sole proprietorship)",
    "15": "Dødsbo (estate of deceased)",
    "20": "I/S — Interessentskab (general partnership)",
    "30": "A/S — Aktieselskab (public limited company)",
    "40": "ApS — Anpartsselskab (private limited company)",
    "45": "Iværksætterselskab (IVS) — entrepreneur company",
    "50": "Andelsselskab / A.m.b.A. (cooperative society)",
    "55": "Andelsselskab begrænset ansvar (limited cooperative)",
    "60": "K/S — Kommanditselskab (limited partnership)",
    "70": "Fonde og øvrige selvejende institutioner (foundation)",
    "80": "Forening (association)",
    "81": "Frivillig forening (voluntary association)",
    "90": "P/S — Partnerselskab (limited liability partnership)",
    "95": "SE — Europæisk selskab (Societas Europaea)",
    "100": "EEIG — Europæisk økonomisk firmagruppe",
    "110": "SCE — Europæisk andelsselskab",
    "115": "Filial af udenlandsk aktieselskab (branch of foreign company)",
    "130": "Statslig administrativ enhed (state administrative unit)",
    "140": "Folkekirkelig institution (church institution)",
    "150": "Primærkommune (municipality)",
    "155": "Amtskommune (county)",
    "160": "Statsvirksomhed (state enterprise)",
    "170": "Region",
    "180": "Kommunalt fællesskab (municipal community)",
    "190": "Statslig forvaltningsmyndighed (state authority)",
    "195": "Kirkelig forening (religious association)",
    "200": "Udenlandsk enhed, beliggende i Danmark (foreign entity in DK)",
    "210": "Udenlandsk enhed, beliggende i udlandet (foreign entity abroad)",
    "220": "Europæisk selskab (European company)",
    "230": "Stiftelse (foundation/endowment)",
    "235": "Udenlandsk statslig enhed (foreign state entity)",
    "245": "Offentlig forening (public association)",
    "250": "Sparekasse (savings bank)",
    "260": "Andelskasse (cooperative bank)",
    "270": "Konverteret fra anden type",
    "280": "Erhvervsdrivende fond (business foundation)",
    "285": "Ikke-erhvervsdrivende fond (non-business foundation)",
    "290": "Investeringsforening (investment association)",
    "291": "Professionel forening",
    "295": "Special-investeringsforening",
    "300": "Hedgeforening",
    "305": "Registreret alternativ investeringsfond",
    "310": "Elektricitetsleverandør",
    "315": "Gasleverandør",
    "320": "Vandforsyning (water supply)",
    "325": "Varmeleverandør (heat supplier)",
    "330": "Spildevandsanlæg (sewage works)",
    "335": "Fjernvarmecentral",
    "340": "Havnevirksomhed (port enterprise)",
    "345": "Luftfartsselskab (airline)",
    "350": "Jernbaneselskab (railway company)",
    "355": "Postvirksomhed (postal service)",
    "360": "Uoplyst (not stated)",
}

# CVR status values (from virksomhed.status field).
_STATUS_MAP: dict[str, str] = {
    "AKTIV": "active",
    "OPHOERT": "dissolved",
    "OPLØST": "dissolved",
    "UNDER_KONKURS": "in bankruptcy",
    "UNDER_TVANGSOPLOSNING": "in forced dissolution",
    "UNDER_FRIVILLIG_LIKVIDATION": "in voluntary liquidation",
    "TVANGSOPLOEST_FEJLREGISTRERING": "dissolved (error registration)",
    "SLETTET": "deleted",
}

# GraphQL queries -------------------------------------------------------

_Q_VIRKSOMHED = """
query($cvr: Long!) {
  CVR_Virksomhed(where: {CVRNummer: {eq: $cvr}}) {
    nodes {
      CVRNummer
      id
      status
      virksomhedStartdato
      virksomhedOphoersdato
      virkningFra
      virkningTil
    }
  }
}
""".strip()

# Datafordeler GraphQL rules (DAF-GQL-0008, DAF-GQL-0010):
#   - Aliases are NOT allowed
#   - Multiple root fields in a single operation are NOT allowed
# Each entity type must be fetched in a separate HTTP request.

_Q_NAVN = """
query($id: String!, $first: Int, $after: String) {
  CVR_Navn(where: {CVREnhedsId: {eq: $id}}, first: $first, after: $after) {
    nodes {
      vaerdi
      sekvens
      virkningFra
      virkningTil
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

_Q_ADRESSERING = """
query($id: String!, $first: Int, $after: String) {
  CVR_Adressering(where: {CVREnhedsId: {eq: $id}}, first: $first, after: $after) {
    nodes {
      AdresseringAnvendelse
      CVRAdresse_vejnavn
      CVRAdresse_husnummerFra
      CVRAdresse_postnummer
      CVRAdresse_postdistrikt
      CVRAdresse_kommunenavn
      CVRAdresse_landekode
      virkningFra
      virkningTil
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

_Q_BRANCHE = """
query($id: String!, $first: Int, $after: String) {
  CVR_Branche(where: {CVREnhedsId: {eq: $id}}, first: $first, after: $after) {
    nodes {
      vaerdi
      sekvens
      virkningFra
      virkningTil
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

_Q_FORM = """
query($id: String!, $first: Int, $after: String) {
  CVR_Virksomhedsform(where: {CVREnhedsId: {eq: $id}}, first: $first, after: $after) {
    nodes {
      vaerdi
      vaerdiTekst
      virkningFra
      virkningTil
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

_Q_DELTAGER = """
query($id: String!, $first: Int, $after: String) {
  CVR_FuldtAnsvarligDeltagerRelation(where: {CVREnhedsId: {eq: $id}}, first: $first, after: $after) {
    nodes {
      deltagendeEnhedsId
      virkningFra
      virkningTil
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

# A participant's own unit (Phase F, 21 Oct 2026): what kind of unit it is
# (``CVRPerson`` or ``Virksomhed``) and its business key — a company's CVR
# number; for a person the key is the CVREnhedsId itself, the participant
# number CVR shows on datacvr.virk.dk/enhed/person/<number>. One request for
# every participant: both tables take an ``in`` filter.
_Q_PARTICIPANT_UNITS = """
query($ids: [String!], $first: Int, $after: String) {
  CVR_CVREnhed(where: {id: {in: $ids}}, first: $first, after: $after) {
    nodes {
      id
      enhedsType
      forretningsnoegletype
      forretningsnoegle
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

_Q_PARTICIPANT_NAMES = """
query($ids: [String!], $first: Int, $after: String) {
  CVR_Navn(where: {CVREnhedsId: {in: $ids}}, first: $first, after: $after) {
    nodes {
      CVREnhedsId
      vaerdi
      sekvens
      virkningFra
      virkningTil
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

#: Datafordeler answers 100 rows a page unless asked for more; until 21 Oct
#: 2026 no query asked, so a firm with more partners (Willis Towers Watson
#: Consultancy Services I/S: 132) was silently cut at 100. Every query now
#: pages, PAGE_SIZE rows at a time, up to MAX_PAGES pages.
PAGE_SIZE = 500
MAX_PAGES = 10

#: DAF-GQL-0016: an ``in`` list may hold at most 100 elements (measured on
#: Willis Towers Watson Consultancy Services I/S's 132 partners, 21 Oct 2026).
IN_LIST_MAX = 100


#: The degradation detail when the partners could not be read. A fixed
#: sentence: no name or number can reach it (``test_degraded_sources``).
PARTICIPANTS_NOT_READ = (
    "named the firm's fully liable participants but did not answer who they "
    "are, so its partners are missing from this report"
)


class CvrPageLimitError(RuntimeError):
    """A query still had pages left after MAX_PAGES — never read as complete."""


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_cvr(cvr: str | int) -> str:
    """Return CVR number normalised to an 8-digit zero-padded string."""
    return str(int(str(cvr).strip())).zfill(_CVR_LEN)


def _current(nodes: list[dict]) -> list[dict]:
    """Return nodes where virkningTil is null (currently effective)."""
    current = [n for n in nodes if n.get("virkningTil") is None]
    # Fallback: if nothing is current, return all (data might be historic)
    return current or nodes


def _best_navn(nodes: list[dict]) -> str | None:
    """Pick the best current name.

    The Datafordeler API uses sekvens=0 for the primary name (the only name
    for most entities). sekvens=1 indicates a secondary/alternate name.
    We prefer sekvens=0; if absent fall back to any current record.
    """
    if not nodes:
        return None
    current = _current(nodes)
    primary = [n for n in current if n.get("sekvens") == 0]
    pool = primary or current
    return pool[0].get("vaerdi") if pool else None


def _best_address(nodes: list[dict]) -> dict | None:
    """Pick the best current address. Prefer BELIGGENHEDSADRESSE (registered seat)."""
    if not nodes:
        return None
    current = _current(nodes)
    # Prefer registered seat address (API returns lowercase, so compare case-insensitively)
    seat = [n for n in current if "beliggenhed" in (n.get("AdresseringAnvendelse") or "").lower()]
    pool = seat or current
    return pool[0] if pool else None


def _best_branche(nodes: list[dict]) -> str | None:
    """Pick the primary industry code.

    In Datafordeler CVR, sekvens=0 is the primary (main) industry code;
    higher sekvens values are secondary/tertiary codes.
    """
    if not nodes:
        return None
    current = _current(nodes)
    primary = [n for n in current if n.get("sekvens") == 0]
    pool = primary or current
    return pool[0].get("vaerdi") if pool else None


def _best_form(nodes: list[dict]) -> tuple[str | None, str | None]:
    """Return (code, text) for the current legal form."""
    if not nodes:
        return None, None
    current = _current(nodes)
    if not current:
        return None, None
    node = current[0]
    return node.get("vaerdi"), node.get("vaerdiTekst")


def _latest_name(rows: list[dict]) -> str | None:
    """The participant's name now, else the last one CVR held.

    The primary (``sekvens`` 0) current row wins; with none current, the row
    that ended last — a dissolved company keeps the name it ended under.
    """
    if not rows:
        return None
    current = [r for r in rows if r.get("virkningTil") is None]
    if current:
        primary = [r for r in current if r.get("sekvens") == 0] or current
        return (primary[0].get("vaerdi") or "").strip() or None
    ended = sorted(rows, key=lambda r: str(r.get("virkningTil") or ""), reverse=True)
    return (ended[0].get("vaerdi") or "").strip() or None


def build_participants(
    relation_nodes: list[dict], unit_nodes: list[dict], name_nodes: list[dict]
) -> list[dict[str, Any]]:
    """One record per fully liable participant: who, what kind, and when.

    ``kind`` is ``person`` for a ``CVRPerson`` or ``AndreDeltagerePerson``
    unit, ``entity`` for a ``Virksomhed`` (with its CVR number) or another
    ``AndreDeltagere*`` unit, ``unknown`` otherwise. ``periods``
    are the relation rows, oldest first; ``current`` is any row with no
    ``virkningTil``. CVR publishes no birth date and no CPR number for a
    person here — the participant number is the only key.
    """
    units = {str(u.get("id")): u for u in unit_nodes if u.get("id")}
    names: dict[str, list[dict]] = {}
    for n in name_nodes:
        if n.get("CVREnhedsId"):
            names.setdefault(str(n["CVREnhedsId"]), []).append(n)
    periods: dict[str, list[dict[str, str | None]]] = {}
    for r in relation_nodes:
        pid = r.get("deltagendeEnhedsId")
        if not pid:
            continue
        periods.setdefault(str(pid), []).append(
            {"from": r.get("virkningFra"), "to": r.get("virkningTil")}
        )
    out: list[dict[str, Any]] = []
    for pid in sorted(periods):
        unit = units.get(pid) or {}
        etype = unit.get("enhedsType")
        etype = str(etype or "")
        # CVRPerson: a person CVR holds with a CPR number; AndreDeltagerePerson:
        # one it holds without (typically a foreign person, 1 of Willis's 132).
        # Both are keyed on their CVREnhedsId. AndreDeltagere* that is not a
        # person is a non-Danish or unregistered company: an entity, no CVR.
        if etype.endswith("Person"):
            kind = "person"
        elif etype == "Virksomhed" or etype.startswith("AndreDeltagere"):
            kind = "entity"
        else:
            kind = "unknown"
        cvr = None
        if kind == "entity" and unit.get("forretningsnoegletype") == "CVRNummer":
            key = str(unit.get("forretningsnoegle") or "").strip()
            cvr = normalise_cvr(key) if key.isdigit() else None
        rows = sorted(periods[pid], key=lambda p: str(p["from"] or ""))
        out.append(
            {
                "participant_id": pid,
                "kind": kind,
                "cvr_number": cvr,
                "name": _latest_name(names.get(pid, [])),
                "periods": rows,
                "current": any(p["to"] is None for p in rows),
            }
        )
    return out



def _entity_url(cvr: str) -> str:
    return _CVR_PORTAL.format(cvr=cvr)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class CvrDenmarkAdapter(SourceAdapter):
    """Danish CVR adapter using the Datafordeler GraphQL API."""

    id = "cvr_denmark"

    lookup_derivers = (
        LookupDeriver(frozenset({DK_CVR_RA_CODE}), "dk_cvr", normalise_cvr),
    )
    lookup_pass_legal_name = True
    # Datafordeler is slow by design: one virksomhed lookup + 5 detail
    # queries, each with an explicit 45 s timeout. Needs a bigger budget.
    lookup_timeout_s = 90.0


    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="CVR — Det Centrale Virksomhedsregister",
            homepage="https://datacvr.virk.dk/",
            description=(
                "Danish Central Business Register (CVR) — the authoritative register "
                "of all Danish businesses, maintained by Erhvervsstyrelsen (the "
                "Danish Business Authority). Accessed via the Datafordeler GraphQL "
                "API (non-restricted entity data)."
            ),
            license="Danish Open Government Data (CVR brugervilkår)",
            attribution=(
                "Indeholder data fra Det Centrale Virksomhedsregister (CVR), "
                "Erhvervsstyrelsen / Danish Business Authority. "
                "Data distribueret via Datafordelerens CVR GraphQL API."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(settings.cvr_denmark_api_key) and settings.allow_live,
            is_national_register=True,
            country="DK",
        )

    async def search(
        self,
        query: str,
        kind: SearchKind = SearchKind.ENTITY,
    ) -> list[SourceHit]:
        # CVR GraphQL has no name-search endpoint; identifier-keyed only.
        return []

    async def fetch(
        self,
        cvr_number: str,
        *,
        client: httpx.AsyncClient | None = None,
        cache: Cache | None = None,
        legal_name: str | None = None,
    ) -> dict[str, Any]:
        settings = get_settings()
        api_key = settings.cvr_denmark_api_key
        if not api_key:
            raise RuntimeError("CVR_DENMARK_API_KEY is not configured")

        cvr_norm = normalise_cvr(cvr_number)
        # v2 (Phase F): bundles carry the participants and every list is paged;
        # a v1 bundle has neither and must not answer for one.
        cache_key = f"{_CACHE_NS}:v2:{cvr_norm}"

        if cache is not None:
            cached = await cache.get(cache_key)
            if cached is not None:
                return cached  # type: ignore[return-value]

        params = {"apiKey": api_key}
        own_client = client is None
        if own_client:
            client = build_client()

        try:
            bundle = await self._fetch_bundle(
                client, int(cvr_norm), cvr_norm, params, legal_name=legal_name
            )
        finally:
            if own_client:
                await client.aclose()

        validate_raw("cvr_denmark", CVRBundle, bundle)

        # A bundle whose partners could not be read is never cached (the
        # Phase 262 rule): the next lookup asks again.
        if cache is not None and bundle.get("participants_read", True):
            await cache.set(cache_key, bundle)

        return bundle

    async def _fetch_bundle(
        self,
        client: httpx.AsyncClient,
        cvr_int: int,
        cvr_norm: str,
        params: dict[str, str],
        *,
        legal_name: str | None = None,
    ) -> dict[str, Any]:
        # --- Request 1: fetch CVR_Virksomhed by CVRNummer ---
        r1 = await client.post(
            _GRAPHQL_URL,
            params=params,
            json={"query": _Q_VIRKSOMHED, "variables": {"cvr": cvr_int}},
            timeout=45.0,  # Datafordeler CVR can be slow; override default 15 s
        )
        r1.raise_for_status()
        data1 = r1.json()

        if errors := data1.get("errors"):
            raise RuntimeError(f"CVR GraphQL error: {errors}")

        nodes_v = (
            data1.get("data", {})
            .get("CVR_Virksomhed", {})
            .get("nodes", [])
        )
        if not nodes_v:
            raise LookupError(f"CVR number {cvr_norm!r} not found in CVR")

        # Use the most recently effective virksomhed record
        virksomhed = _current(nodes_v)[0]
        enhed_id: str = virksomhed["id"]
        cvr_status: str = virksomhed.get("status", "")
        start_date: str | None = virksomhed.get("virksomhedStartdato")
        end_date: str | None = virksomhed.get("virksomhedOphoersdato")

        # --- Requests 2–6: one request per entity type (aliases and multi-root
        #     fields are forbidden by DAF-GQL-0008 / DAF-GQL-0010) ---
        id_vars = {"id": enhed_id}

        async def _gql(
            query: str, root_key: str, variables: dict[str, Any] | None = None
        ) -> list[dict]:
            """Fire one GraphQL query, following its pages, and return the nodes.

            A GraphQL error on the first page is logged and read as no rows,
            as it always has been. A page limit that is reached is an error:
            a truncated list must never read as the whole list.
            """
            nodes: list[dict] = []
            after: str | None = None
            for _page in range(MAX_PAGES):
                resp = await client.post(
                    _GRAPHQL_URL,
                    params=params,
                    json={
                        "query": query,
                        "variables": {
                            **(variables or id_vars),
                            "first": PAGE_SIZE,
                            "after": after,
                        },
                    },
                    timeout=45.0,  # Datafordeler CVR can be slow; override default 15 s
                )
                resp.raise_for_status()
                data = resp.json()
                if errs := data.get("errors"):
                    _log.warning(
                        "CVR GraphQL errors (%s) for %s: %s", root_key, cvr_norm, errs
                    )
                    if variables is not None:
                        # Participant lookups have no older behaviour to keep:
                        # an error there is a failed read, never "no one".
                        raise RuntimeError(f"CVR GraphQL error on {root_key}")
                    return nodes
                block = (data.get("data") or {}).get(root_key) or {}
                nodes.extend(block.get("nodes") or [])
                page_info = block.get("pageInfo") or {}
                if not page_info.get("hasNextPage"):
                    return nodes
                after = page_info.get("endCursor")
                if not after:
                    return nodes
            raise CvrPageLimitError(
                f"{root_key} had more than {MAX_PAGES * PAGE_SIZE} rows for {cvr_norm}"
            )

        (
            navn_nodes,
            addr_nodes,
            branch_nodes,
            form_nodes,
            deltager_nodes,
        ) = await asyncio.gather(
            _gql(_Q_NAVN, "CVR_Navn"),
            _gql(_Q_ADRESSERING, "CVR_Adressering"),
            _gql(_Q_BRANCHE, "CVR_Branche"),
            _gql(_Q_FORM, "CVR_Virksomhedsform"),
            _gql(_Q_DELTAGER, "CVR_FuldtAnsvarligDeltagerRelation"),
        )

        name = _best_navn(navn_nodes)
        if not name and legal_name:
            name = legal_name

        addr = _best_address(addr_nodes)
        branche_code = _best_branche(branch_nodes)
        form_code, form_text = _best_form(form_nodes)

        # Fully liable participants (I/S, K/S, P/S partners). Strictly the
        # rows with no virkningTil: ``_current``'s fall-back to every row
        # would read a firm whose partners have all left as still having them.
        current_deltagere = sorted(
            {
                n["deltagendeEnhedsId"]
                for n in deltager_nodes
                if n.get("deltagendeEnhedsId") and n.get("virkningTil") is None
            }
        )

        # Who each participant is: two requests for all of them (Phase F).
        # A failure is said, never read as "no partners": the bundle carries
        # participants_read False and fetch() does not cache it.
        participant_ids = sorted(
            {n["deltagendeEnhedsId"] for n in deltager_nodes if n.get("deltagendeEnhedsId")}
        )
        participants: list[dict[str, Any]] = []
        participants_read = True
        if participant_ids:
            try:
                unit_nodes: list[dict] = []
                name_nodes: list[dict] = []
                for start in range(0, len(participant_ids), IN_LIST_MAX):
                    chunk = participant_ids[start : start + IN_LIST_MAX]
                    units, names = await asyncio.gather(
                        _gql(_Q_PARTICIPANT_UNITS, "CVR_CVREnhed", {"ids": chunk}),
                        _gql(_Q_PARTICIPANT_NAMES, "CVR_Navn", {"ids": chunk}),
                    )
                    unit_nodes.extend(units)
                    name_nodes.extend(names)
                participants = build_participants(deltager_nodes, unit_nodes, name_nodes)
            except (httpx.HTTPError, RuntimeError) as exc:
                participants_read = False
                _log.warning(
                    "CVR participant read failed for %s: %s",
                    cvr_norm,
                    describe_exception(exc),
                )
                degradation.record(
                    "cvr_denmark",
                    PARTICIPANTS_NOT_READ,
                    check=degradation.CHECK_SOURCE_READ,
                    reason=degradation.reason_for_failure(describe_exception(exc)),
                )

        status_norm = _STATUS_MAP.get(cvr_status.upper(), cvr_status.lower() or "unknown")
        # Prefer the API's own vaerdiTekst label; our local map is a fallback
        # only when the API returns no text (the Datafordeler code numbering
        # does not match the datacvr.virk.dk documentation we used to build it).
        form_label = form_text or (_LEGAL_FORM_MAP.get(str(form_code)) if form_code else None)

        source_url = _entity_url(cvr_norm)

        bundle: dict[str, Any] = {
            "cvr_number": cvr_norm,
            "cvr_enhed_id": enhed_id,
            "name": name or cvr_norm,
            "status": status_norm,
            "start_date": start_date,
            "end_date": end_date,
            "legal_form_code": str(form_code) if form_code else None,
            "legal_form_text": form_label,
            "branche_code": branche_code,
            "address": addr,
            "source_url": source_url,
            "fully_liable_participant_ids": current_deltagere,
            "participants": participants,
            "participants_read": participants_read,
            # Raw for debugging / future use. CVR is bitemporal — these lists carry
            # every virkning period (virkningFra/virkningTil), so the Time Machine
            # emitter reconstructs change events from them with no extra API calls.
            # `_raw_virksomhed` is the full virksomhed list (all status periods),
            # not just the current row, so status / lifecycle changes are derivable.
            "_raw_virksomhed": nodes_v,
            "_raw_navn": navn_nodes,
            "_raw_adressering": addr_nodes,
            "_raw_branche": branch_nodes,
            "_raw_form": form_nodes,
            "_raw_deltager": deltager_nodes,
        }
        return bundle
