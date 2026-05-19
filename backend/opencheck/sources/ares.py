"""Czech ARES (Administrativní registr ekonomických subjektů) adapter.

ARES is the Czech Republic's authoritative business register, operated by the
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

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Czech Commercial Register.
CZ_RA_CODE: str = "RA000163"

_BASE = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest"
_SEARCH_URL = f"{_BASE}/ekonomicke-subjekty/vyhledat"
_AGGREGATE_URL = f"{_BASE}/ekonomicke-subjekty"
_VR_URL = f"{_BASE}/ekonomicke-subjekty-vr"

_CACHE_NS = "ares"

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

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="ARES (Czech Republic)",
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
                "Republic (Ministerstvo financí ČR) under CC BY 4.0. "
                "Source: ares.gov.cz."
            ),
            supports=[SearchKind.ENTITY],
            live_available=settings.allow_live,
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
            summary="ARES (Czech Republic)",
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
        bundle_key = f"{_CACHE_NS}/bundle/{ico}"
        settings = get_settings()
        cache = Cache()

        # --- 1. Bundle cache ---
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
                except httpx.HTTPStatusError as exc:
                    _log.warning("ares: aggregate 404/error for %s: %s", ico, exc)
                    return self._stub(ico, legal_name)
                except httpx.HTTPError as exc:
                    _log.warning("ares: aggregate fetch error for %s: %s", ico, exc)
                    return self._stub(ico, legal_name)

        # --- 3. Fetch VR endpoint (404 is normal for non-VR entities) ---
        vr_key = f"{_CACHE_NS}/vr/{ico}"
        cached_vr = cache.get_payload(vr_key)
        if cached_vr is not None:
            vr_data: dict | None = cached_vr[0]
        else:
            async with build_client() as client:
                try:
                    vr_resp = await client.get(f"{_VR_URL}/{ico}", timeout=15)
                    vr_resp.raise_for_status()
                    vr_data = vr_resp.json()
                    cache.put(vr_key, vr_data)
                except httpx.HTTPStatusError:
                    # 404 = entity not in VR; store None to avoid retrying
                    vr_data = None
                    cache.put(vr_key, None)
                except httpx.HTTPError as exc:
                    _log.warning("ares: VR fetch error for %s: %s", ico, exc)
                    vr_data = None

        bundle = self._build_bundle(ico, aggregate, vr_data)
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
            "vat_number": vat_number,
            "link": _or_url(ico),
        }

        owners: list[dict[str, Any]] = []
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
                if "datumVymazu" in group:
                    continue  # historic group
                for member in group.get("clenoveOrganu", []):
                    if "datumVymazu" in member:
                        continue
                    if member.get("typAngazma") != "AKCIONAR":
                        continue
                    person = _extract_person(member)
                    if person:
                        owners.append({
                            **person,
                            "role": "shareholder",
                            "role_label": "Akcionář",
                            "start_date": member.get("datumZapisu"),
                        })

            # --- Partners: s.r.o. spolecnici ---
            for group in zaznam.get("spolecnici", []):
                for sp in group.get("spolecnik", []):
                    if "datumVymazu" in sp:
                        continue
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
                        owners.append({
                            **person,
                            "role": "partner",
                            "role_label": "Společník",
                            "stake_percent": stake,
                            "start_date": sp.get("datumZapisu"),
                        })

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

        return {
            "source_id": self.id,
            "hit_id": ico,
            "cz_ico": ico,
            "name": name or "",
            "is_stub": False,
            "entity": entity,
            "owners": owners,
            "directors": directors,
        }
