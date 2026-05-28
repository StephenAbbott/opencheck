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
  requires MitID Erhverv + OAuth — it is NOT fetched here.

Fetch strategy
--------------
  Two HTTP requests per lookup:
  1. CVR_Virksomhed — fetch by CVRNummer (Long) to get the CVREnhedsId (UUID-
     style string that joins all other entity tables).
  2. Batch query — fetch CVR_Navn, CVR_Adressering, CVR_Branche,
     CVR_Virksomhedsform, CVR_FuldtAnsvarligDeltagerRelation in a single
     GraphQL request keyed by CVREnhedsId.

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
  CVRPerson (natural persons linked via CPR number) is explicitly excluded
  from this adapter. The CVR GraphQL API requires a separate access
  application and MitID Erhverv authentication for CVRPerson data. This
  adapter therefore produces entity statements only — no person or
  ownership-or-control statements.

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

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
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
query($id: String!) {
  CVR_Navn(where: {CVREnhedsId: {eq: $id}}) {
    nodes {
      vaerdi
      sekvens
      virkningFra
      virkningTil
    }
  }
}
""".strip()

_Q_ADRESSERING = """
query($id: String!) {
  CVR_Adressering(where: {CVREnhedsId: {eq: $id}}) {
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
  }
}
""".strip()

_Q_BRANCHE = """
query($id: String!) {
  CVR_Branche(where: {CVREnhedsId: {eq: $id}}) {
    nodes {
      vaerdi
      sekvens
      virkningFra
      virkningTil
    }
  }
}
""".strip()

_Q_FORM = """
query($id: String!) {
  CVR_Virksomhedsform(where: {CVREnhedsId: {eq: $id}}) {
    nodes {
      vaerdi
      vaerdiTekst
      virkningFra
      virkningTil
    }
  }
}
""".strip()

_Q_DELTAGER = """
query($id: String!) {
  CVR_FuldtAnsvarligDeltagerRelation(where: {CVREnhedsId: {eq: $id}}) {
    nodes {
      deltagendeEnhedsId
      virkningFra
      virkningTil
    }
  }
}
""".strip()


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


def _entity_url(cvr: str) -> str:
    return _CVR_PORTAL.format(cvr=cvr)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class CvrDenmarkAdapter(SourceAdapter):
    """Danish CVR adapter using the Datafordeler GraphQL API."""

    id = "cvr_denmark"

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
        cache_key = f"{_CACHE_NS}:{cvr_norm}"

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

        if cache is not None:
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

        async def _gql(query: str, root_key: str) -> list[dict]:
            """Fire one GraphQL query and return the nodes list."""
            resp = await client.post(
                _GRAPHQL_URL,
                params=params,
                json={"query": query, "variables": id_vars},
                timeout=45.0,  # Datafordeler CVR can be slow; override default 15 s
            )
            resp.raise_for_status()
            data = resp.json()
            if errs := data.get("errors"):
                _log.warning("CVR GraphQL errors (%s) for %s: %s", root_key, cvr_norm, errs)
            return data.get("data", {}).get(root_key, {}).get("nodes", [])

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

        # Fully liable participant entity IDs (for K/S, I/S, etc.)
        current_deltagere = [
            n["deltagendeEnhedsId"]
            for n in _current(deltager_nodes)
            if n.get("deltagendeEnhedsId")
        ]

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
            # Raw for debugging / future use
            "_raw_navn": navn_nodes,
            "_raw_adressering": addr_nodes,
            "_raw_branche": branch_nodes,
            "_raw_form": form_nodes,
            "_raw_deltager": deltager_nodes,
        }
        return bundle
