"""Czech ARES (Administrativní registr ekonomických subjektů) adapter.

ARES is Czechia's authoritative business register, operated by the
Ministry of Finance.  It aggregates data from multiple sub-registers:

  • ROS  — Registr osob (base register of persons/entities)
  • VR   — Veřejný rejstřík (commercial register, Ministry of Justice)
  • RES  — Registr ekonomických subjektů (statistical register)
  • RZP  — Živnostenský rejstřík (trade licence register)

This adapter uses two ARES REST endpoints:

1. Aggregate endpoint  GET /ekonomicke-subjekty/{ico}
   Returns entity basics: name, address, legal form, registration date, VAT
   number, status per sub-register.  No auth required.

2. VR endpoint  GET /ekonomicke-subjekty-vr/{ico}
   Returns commercial-register data: shareholders (akcionari / spolecnici),
   directors (statutarniOrgany), share capital.  Returns 404 for entities
   not registered in the commercial register; handled gracefully.

Search: POST /ekonomicke-subjekty/vyhledat
  Body: {"obchodniJmeno": "<name>", "start": 0, "pocet": N}

GLEIF integration
-----------------
GLEIF Registration Authority code for the Czech Obchodní rejstřík:
  RA000163  (Ministerstvo spravedlnosti / Commercial Register)

The ``registeredAs`` field in the GLEIF record contains the IČO (8-digit
Identifikační číslo osoby, zero-padded).  ``app.py`` extracts ``cz_ico``
from this and passes it to ``fetch()``.

Authentication: none — fully public API.
License: CC BY 4.0  https://creativecommons.org/licenses/by/4.0/
Attribution: "Obsahuje data z ARES (Administrativní registr ekonomických
  subjektů), Ministerstvo financí ČR.  Licence CC BY 4.0."
ARES portal: https://ares.gov.cz/
Open-data catalogue entry: https://data.mf.gov.cz/
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.ares import AresBundle

_log = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Czech Commercial Register.
CZ_RA_CODE: str = "RA000163"

_BASE = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest"
_SEARCH_URL = f"{_BASE}/ekonomicke-subjekty/vyhledat"
_AGGREGATE_URL = f"{_BASE}/ekonomicke-subjekty"
_VR_URL = f"{_BASE}/ekonomicke-subjekty-vr"

_CACHE_NS = "ares"

# Phase 262: the assembled-bundle cache moved to a new key. Bundles under the
# old ``ares/bundle/`` key never expired and could have been built while the
# VR endpoint was failing — a company with no shareholders or directors
# because the register was down, remembered for good. Nothing is migrated:
# the old entries are simply never read again, and the next lookup rebuilds
# from the (still valid) aggregate cache plus a fresh VR answer.
_BUNDLE_NS = f"{_CACHE_NS}/bundle-v2"

# Degradation details — fixed sentences, never an entity name (the
# ``DegradedSource`` privacy contract).
_AGG_FAILED_DETAIL = (
    "the ARES register did not answer, so the Czech register record was not consulted"
)
_VR_FAILED_DETAIL = (
    "the ARES commercial-register (VR) endpoint did not answer, so shareholders "
    "and directors were not consulted"
)


def _failure_label(exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return type(exc).__name__

# Czech legal-form codes → English description.
_LEGAL_FORMS: dict[str, str] = {
    "101": "Veřejná obchodní společnost (v.o.s.) — general partnership",
    "105": "Komanditní společnost (k.s.) — limited partnership",
    "112": "Společnost s ručením omezeným (s.r.o.) — LLC",
    "121": "Akciová společnost (a.s.) — joint-stock company",
    "141": "Družstvo — cooperative",
    "145": "Bytové družstvo — housing cooperative",
    "151": "Zapsaný spolek — registered association",
    "161": "Obecně prospěšná společnost",
    "205": "Státní podnik — state enterprise",
    "231": "Příspěvková organizace — contributory organisation",
    "301": "Státní organizace — state organisation",
    "325": "Organizační složka státu — organisational unit of state",
    "331": "Příspěvková organizace zřízená územním samosprávným celkem",
    "421": "Zahraniční fyzická osoba — foreign natural person",
    "422": "Zahraniční právnická osoba — foreign legal entity",
    "501": "Fyzická osoba podnikající — sole trader",
    "601": "Sdružení (bez právní subjektivity)",
    "711": "Obecní úřad — municipal office",
    "721": "Krajský úřad — regional authority",
    "801": "Nadace — foundation",
    "805": "Nadační fond — endowment fund",
}

# ARES status codes → normalised label.
_STATUS_MAP: dict[str, str] = {
    "AKTIVNI": "active",
    "AKTIVNÍ": "active",
    "ZANIKLÝ": "dissolved",
    "ZANIKLÝ-FÚZE": "dissolved-merger",
    "LIKVIDACE": "liquidation",
    "NEEXISTUJICI": "not-registered",
    "NEEXISTUJÍCÍ": "not-registered",
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def normalise_ico(ico: str | int) -> str:
    """Return IČO normalised to an 8-digit zero-padded string."""
    return str(ico).strip().zfill(8)


def _extract_latest(items: list[dict]) -> str | None:
    """Extract the most recent ``hodnota`` from a timestamped-value list.

    VR endpoint returns some fields (obchodniJmeno, pravniForma) as a list
    of ``{datumZapisu, datumVymazu?, hodnota}`` records.  We take the entry
    with no ``datumVymazu`` — if multiple, pick the latest ``datumZapisu``.
    """
    if not items:
        return None
    if isinstance(items, str):
        return items
    current = [i for i in items if "datumVymazu" not in i]
    pool = current if current else items
    latest = sorted(pool, key=lambda x: x.get("datumZapisu", ""), reverse=True)
    return str(latest[0].get("hodnota", "")) if latest else None


def _resolve_status(aggregate: dict) -> str:
    """Derive a normalised status string from the aggregate response."""
    reg = aggregate.get("seznamRegistraci", {})
    # Prefer VR status if the entity is registered there.
    for key in ("stavZdrojeVr", "stavZdrojeRos", "stavZdrojeRes"):
        val = reg.get(key, "")
        if val and val != "NEEXISTUJICI" and val != "NEEXISTUJÍCÍ":
            return _STATUS_MAP.get(val.upper(), val.lower())
    # Fall back: check if VR says AKTIVNI at all
    for val in reg.values():
        mapped = _STATUS_MAP.get(str(val).upper())
        if mapped == "active":
            return "active"
    return _STATUS_MAP.get(str(list(reg.values())[0]).upper(), "unknown") if reg else "unknown"


def _entity_url(ico: str) -> str:
    return f"https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ui/rejstrik-firem/vrDetail/{ico}"


def _or_url(ico: str) -> str:
    """Public OR (Obchodní rejstřík) page for an entity."""
    return f"https://or.justice.cz/ias/ui/rejstrik-firma.vysledky?subjektId={ico}&typ=PLATNY"


def _extract_person(member: dict) -> dict[str, Any] | None:
    """Extract a person dict from a VR clenOrganu / spolecnik entry."""
    fo = member.get("fyzickaOsoba")
    if fo:
        jmeno = fo.get("jmeno", "")
        prijmeni = fo.get("prijmeni", "")
        full_name = f"{jmeno} {prijmeni}".strip()
        addr = fo.get("adresa", {})
        return {
            "type": "person",
            "name": full_name,
            "given_name": jmeno,
            "family_name": prijmeni,
            "birth_date": fo.get("datumNarozeni"),
            "nationality": fo.get("statniObcanstvi"),
            "address": addr.get("textovaAdresa"),
        }
    po = member.get("pravnickaOsoba")
    if po:
        ico = po.get("ico")
        # obchodniJmeno can be a string or a list of timestamped values in VR
        jmeno_raw = po.get("obchodniJmeno", "")
        name = _extract_latest(jmeno_raw) if isinstance(jmeno_raw, list) else jmeno_raw
        addr = po.get("adresa", {})
        return {
            "type": "entity",
            "name": name or "",
            "ico": normalise_ico(ico) if ico else None,
            "address": addr.get("textovaAdresa"),
            "country": addr.get("kodStatu"),
        }
    return None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class AresAdapter(SourceAdapter):
    """Source adapter for the Czech ARES business register."""

    id = "ares"

    lookup_derivers = (
        LookupDeriver(frozenset({CZ_RA_CODE}), "cz_ico", normalise_ico),
    )
    lookup_pass_legal_name = True


    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="ARES (Czechia)",
            homepage="https://ares.gov.cz/",
            description=(
                "Czech ARES business register (Administrativní registr "
                "ekonomických subjektů), aggregating data from the commercial "
                "register (Obchodní rejstřík), trade licence register, and "
                "other sub-registers.  Published by the Ministry of Finance "
                "under CC BY 4.0."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from ARES (Administrativní registr ekonomických "
                "subjektů), published by the Ministry of Finance of the Czech "
                "Czechia (Ministerstvo financí ČR) under CC BY 4.0. "
                "Source: ares.gov.cz."
            ),
            supports=[SearchKind.ENTITY],
            live_available=settings.allow_live,
            is_national_register=True,
            country="CZ",
            requires_api_key=False,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _stub_hit(self, name: str) -> SourceHit:
        """Return a stub hit for use when live search is unavailable."""
        return SourceHit(
            source_id=self.id,
            hit_id=name,
            kind=SearchKind.ENTITY,
            name=name,
            summary="ARES (Czechia)",
            identifiers={},
            raw={},
            is_stub=True,
        )

    async def search(self, query: str, kind: SearchKind = SearchKind.ENTITY, *, limit: int = 10) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        settings = get_settings()
        cache = Cache()
        cache_key = f"{_CACHE_NS}/search/{query.lower().strip()}"

        cached = cache.get_payload(cache_key)
        if cached is not None:
            results: list[dict] = cached[0]
        elif not settings.allow_live:
            return [self._stub_hit(query)]
        else:
            async with build_client() as client:
                try:
                    resp = await client.post(
                        _SEARCH_URL,
                        json={"obchodniJmeno": query, "start": 0, "pocet": limit},
                        timeout=15,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    results = data.get("ekonomickeSubjekty", [])
                    cache.put(cache_key, results)
                except httpx.HTTPError as exc:
                    _log.warning("ares: search failed for %r: %s", query, exc)
                    return [self._stub_hit(query)]

        hits: list[SourceHit] = []
        for subj in results[:limit]:
            ico = normalise_ico(subj.get("ico", ""))
            name_raw = subj.get("obchodniJmeno", "")
            name = (
                _extract_latest(name_raw) if isinstance(name_raw, list) else name_raw
            ) or ""
            pf_raw = subj.get("pravniForma", "")
            pf = _extract_latest(pf_raw) if isinstance(pf_raw, list) else str(pf_raw)
            entity_type = _LEGAL_FORMS.get(str(pf), "")
            addr = subj.get("sidlo", {}).get("textovaAdresa", "")
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=ico,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"IČO {ico}" + (f" · {entity_type}" if entity_type else ""),
                    identifiers={"cz_ico": ico},
                    raw={
                        "ico": ico,
                        "name": name,
                        "entity_type": entity_type,
                        "address": addr,
                    },
                    is_stub=True,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str | None = None) -> dict[str, Any]:
        ico = normalise_ico(hit_id)
        bundle_key = f"{_BUNDLE_NS}/{ico}"
        settings = get_settings()
        cache = Cache()

        # --- 1. Bundle cache ---
        # Only bundles built from a VR answer carrying data are stored here
        # (see below), so a hit never hides a VR outage or an expired absence.
        cached_bundle = cache.get_payload(bundle_key)
        if cached_bundle is not None:
            return cached_bundle[0]

        if not settings.allow_live:
            return self._stub(ico, legal_name)

        # --- 2. Fetch aggregate endpoint ---
        agg_key = f"{_CACHE_NS}/aggregate/{ico}"
        cached_agg = cache.get_payload(agg_key)
        if cached_agg is not None:
            aggregate = cached_agg[0]
        else:
            async with build_client() as client:
                try:
                    resp = await client.get(f"{_AGGREGATE_URL}/{ico}", timeout=15)
                    resp.raise_for_status()
                    aggregate = resp.json()
                    cache.put(agg_key, aggregate)
                except httpx.HTTPError as exc:
                    label = _failure_label(exc)
                    _log.warning("ares: aggregate fetch failed for %s: %s", ico, label)
                    if label != "HTTP 404":
                        # Phase 262: a register that did not answer is not a
                        # register with nothing on file. Nothing is cached.
                        degradation.record(
                            self.id,
                            _AGG_FAILED_DETAIL,
                            reason=degradation.reason_for_failure(label),
                        )
                    return self._stub(ico, legal_name)

        # --- 3. Fetch VR endpoint (404 is normal for non-VR entities) ---
        vr_key = f"{_CACHE_NS}/vr/{ico}"
        vr_data: dict | None
        cached_vr = cache.get_payload(vr_key)
        if cached_vr is not None:
            vr_data = cached_vr[0]
        else:
            vr_data = None
            async with build_client() as client:
                try:
                    vr_resp = await client.get(f"{_VR_URL}/{ico}", timeout=15)
                    vr_resp.raise_for_status()
                    vr_data = vr_resp.json()
                    cache.put(vr_key, vr_data)
                except httpx.HTTPError as exc:
                    label = _failure_label(exc)
                    if label == "HTTP 404":
                        # Not in the commercial register — a definitive answer,
                        # remembered for ABSENT_TTL_DAYS only.
                        cache.put_absent(vr_key)
                    else:
                        # Phase 262: a 5xx, a 429 or a network error says
                        # nothing about the record. Until now a 500/503/429
                        # was cached as "not in VR" with no expiry, and the
                        # bundle built from it was cached too.
                        _log.warning("ares: VR fetch failed for %s: %s", ico, label)
                        degradation.record(
                            self.id,
                            _VR_FAILED_DETAIL,
                            reason=degradation.reason_for_failure(label),
                        )

        bundle = self._build_bundle(ico, aggregate, vr_data)
        if vr_data:
            # Without VR data the bundle is rebuilt on each lookup from the two
            # cached parts (no network), so the VR absence's TTL — or the next
            # attempt after a failure — decides, not a bundle that never expires.
            cache.put(bundle_key, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Bundle construction
    # ------------------------------------------------------------------

    def _stub(self, ico: str, legal_name: str | None) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": ico,
            "cz_ico": ico,
            "name": legal_name or "",
            "is_stub": True,
        }

    def _build_bundle(
        self,
        ico: str,
        aggregate: dict,
        vr_data: dict | None,
    ) -> dict[str, Any]:
        # --- Entity basics from aggregate ---
        name_raw = aggregate.get("obchodniJmeno", "")
        name = _extract_latest(name_raw) if isinstance(name_raw, list) else name_raw
        sidlo = aggregate.get("sidlo", {})
        address = sidlo.get("textovaAdresa")
        pf_raw = aggregate.get("pravniForma")
        pf_code = (
            str(_extract_latest(pf_raw) if isinstance(pf_raw, list) else pf_raw or "")
        )
        entity_type = _LEGAL_FORMS.get(pf_code, f"Legal form {pf_code}" if pf_code else "")
        incorporation_date = aggregate.get("datumVzniku")
        # datumAktualizace — when ARES last refreshed this record. Verified
        # live 2026-08-14 on ekonomicke-subjekty/27082440, alongside
        # datumVzniku (which is the company's founding date, not a
        # declaration). The record-level update date is the register's own
        # assertion date, so it becomes the BODS statementDate.
        last_updated = aggregate.get("datumAktualizace")
        vat_number = aggregate.get("dic")
        status = _resolve_status(aggregate)

        entity: dict[str, Any] = {
            "ico": ico,
            "name": name or "",
            "address": address,
            "entity_type": entity_type,
            "legal_form_code": pf_code,
            "status": status,
            "incorporation_date": incorporation_date,
            "last_updated": last_updated,
            "vat_number": vat_number,
            "link": _or_url(ico),
        }

        owners: list[dict[str, Any]] = []
        #: Struck-off shareholders and partners (Phase 317): ended ownership,
        #: kept with the register's deletion date. Directors struck off stay
        #: out — the graph draws serving officers only (Phase 192).
        former_owners: list[dict[str, Any]] = []
        directors: list[dict[str, Any]] = []

        if vr_data:
            zaznamy = vr_data.get("zaznamy", [])
            zaznam = zaznamy[0] if zaznamy else {}

            # Override status with VR stavSubjektu if present.
            vr_status = zaznam.get("stavSubjektu")
            if vr_status:
                entity["status"] = _STATUS_MAP.get(vr_status.upper(), vr_status.lower())

            # --- Shareholders: a.s. akcionari ---
            for group in zaznam.get("akcionari", []):
                group_ended = group.get("datumVymazu")
                for member in group.get("clenoveOrganu", []):
                    if member.get("typAngazma") != "AKCIONAR":
                        continue
                    person = _extract_person(member)
                    if not person:
                        continue
                    ended = member.get("datumVymazu") or group_ended
                    row = {
                        **person,
                        "role": "shareholder",
                        "role_label": "Akcionář",
                        "start_date": member.get("datumZapisu"),
                    }
                    if ended:
                        former_owners.append({**row, "end_date": ended})
                    else:
                        owners.append(row)

            # --- Partners: s.r.o. spolecnici ---
            for group in zaznam.get("spolecnici", []):
                for sp in group.get("spolecnik", []):
                    osoba = sp.get("osoba", {})
                    person = _extract_person(osoba)
                    if person:
                        # Extract stake if available.
                        podily = sp.get("podil", [])
                        stake: str | None = None
                        for p in podily:
                            if "datumVymazu" not in p:
                                vp = p.get("velikostPodilu", {})
                                if vp.get("typObnos") == "PROCENTA":
                                    stake = vp.get("hodnota")
                                elif vp.get("typObnos") == "TEXT":
                                    stake = vp.get("hodnota")
                                break
                        row = {
                            **person,
                            "role": "partner",
                            "role_label": "Společník",
                            "stake_percent": stake,
                            "start_date": sp.get("datumZapisu"),
                        }
                        if sp.get("datumVymazu"):
                            former_owners.append({**row, "end_date": sp["datumVymazu"]})
                        else:
                            owners.append(row)

            # --- Directors: statutarniOrgany ---
            for organ in zaznam.get("statutarniOrgany", []):
                organ_name = organ.get("nazevOrganu", "")
                for member in organ.get("clenoveOrganu", []):
                    if "datumVymazu" in member:
                        continue
                    person = _extract_person(member)
                    if person:
                        clenstvi = member.get("clenstvi", {})
                        funkce = clenstvi.get("funkce", {})
                        role_label = funkce.get("nazev") or organ_name or "Director"
                        directors.append({
                            **person,
                            "role": "director",
                            "role_label": role_label,
                            "start_date": member.get("datumZapisu"),
                        })

        bundle = {
            "source_id": self.id,
            "hit_id": ico,
            "cz_ico": ico,
            "name": name or "",
            "is_stub": False,
            "entity": entity,
            "owners": owners,
            "former_owners": former_owners,
            "directors": directors,
        }
        validate_raw("ares", AresBundle, bundle)
        return bundle
