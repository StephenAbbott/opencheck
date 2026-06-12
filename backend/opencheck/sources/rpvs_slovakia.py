"""Slovakia Public Sector Partners Register (RPVS) adapter.

The Register partnerov verejného sektora (RPVS) is published by the Ministry
of Justice of the Slovak Republic.  It lists entities and individuals that
receive money or other assets from the Slovak state above a legal threshold —
so-called "Partners of Public Sector" — and requires them to disclose their
ultimate beneficial owners (konečný užívateľ výhod, KUV).

This is a procurement-linked beneficial ownership register: participation is
mandatory for suppliers to public bodies that meet the value thresholds, and
the disclosed KUVs are verified by an authorised person (lawyer / notary).

API: https://rpvs.gov.sk/opendatav2/
Swagger: https://rpvs.gov.sk/opendatav2/swagger/index.html
Portal: https://rpvs.gov.sk/rpvs

OData endpoints used
---------------------
* ``GET /PartneriVerejnehoSektora?$filter=contains(tolower(ObchodneMeno),'{q}')&$expand=Partner``
  — free-text name search; returns partner entries with their register ``Id``.
* ``GET /PartneriVerejnehoSektora?$filter=Ico+eq+'{ico}'&$expand=Partner``
  — IČO-keyed lookup for the GLEIF bridge fetch path.
* ``GET /Partneri({id})?$expand=KonecniUzivateliaVyhod,PartneriVerejnehoSektora,OpravneneOsoby``
  — full register entry with all beneficial owners and related data.

Data model
----------
Each **Partner** (``Partneri``) is the root register entry, identified by
``CisloVlozky`` (entry number).  A Partner links to:

* **PartneriVerejnehoSektora** — one or more versioned entries describing the
  entity (company / natural person) that must disclose its owners.  Fields:
  ``ObchodneMeno`` (company name), ``Ico`` (IČO), ``FormaOsoby``
  (PravnickaOsoba / FyzickaOsoba), ``PlatnostOd`` / ``PlatnostDo``.

* **KonecniUzivateliaVyhod** — one or more versioned beneficial-owner (KUV)
  entries.  For individuals: ``Meno`` (first name), ``Priezvisko`` (surname),
  ``DatumNarodenia`` (DoB), ``TitulPred`` / ``TitulZa`` (titles),
  ``JeVerejnyCinitel`` (public official flag).
  For legal-person KUVs: ``ObchodneMeno`` + ``Ico``.
  ``PlatnostOd`` / ``PlatnostDo`` indicate the validity window; a null
  ``PlatnostDo`` means the record is currently active.

* **OpravneneOsoby** — the authorised person (law firm / notary) who verified
  the KUV declarations.

The fetch bundle emitted by this adapter is consumed by ``map_rpvs_slovakia``
in ``bods/mapper.py`` to produce BODS v0.4 entity, person, and ownership
relationship statements.

Authentication: none — fully public OData API.
License: Creative Commons Attribution 4.0 International (CC BY 4.0)
Attribution: "Contains data from the Slovak Public Sector Partners Register
  (RPVS), published by the Ministry of Justice of the Slovak Republic,
  CC BY 4.0."
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any
from urllib.parse import quote

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

_BASE = "https://rpvs.gov.sk/opendatav2"
_PORTAL = "https://rpvs.gov.sk/rpvs"

_CACHE_NS = "rpvs_slovakia"

# OData expand for a full Partner record.
_PARTNER_EXPAND = "KonecniUzivateliaVyhod,PartneriVerejnehoSektora,OpravneneOsoby"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


def _normalise_ico(ico: str | int | None) -> str | None:
    """Return an 8-digit zero-padded IČO string, or None if input is empty."""
    if ico is None:
        return None
    s = str(ico).strip().lstrip("0") or ""
    if not s:
        return None
    return s.zfill(8)


def _current_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return active (PlatnostDo == null) entries, falling back to all if none."""
    active = [e for e in entries if not e.get("PlatnostDo")]
    return active if active else entries


def _current_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    active = _current_entries(entries)
    if not active:
        return None
    # Prefer most recently valid.
    return sorted(active, key=lambda e: e.get("PlatnostOd") or "", reverse=True)[0]


def _person_name(kuv: dict[str, Any]) -> str:
    """Build a display name from a KUV record."""
    parts = []
    if kuv.get("TitulPred"):
        parts.append(kuv["TitulPred"].strip())
    if kuv.get("Meno"):
        parts.append(kuv["Meno"].strip())
    if kuv.get("Priezvisko"):
        parts.append(kuv["Priezvisko"].strip())
    if kuv.get("TitulZa"):
        parts.append(kuv["TitulZa"].strip())
    return " ".join(p for p in parts if p) or kuv.get("ObchodneMeno") or "Unknown"


def _partner_link(partner_id: int) -> str:
    return f"{_PORTAL}/{partner_id}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class RpvsSlovakiaAdapter(SourceAdapter):
    """Adapter for the Slovak Public Sector Partners Register (RPVS).

    **Search** uses an OData ``contains`` filter on the company name field
    (``ObchodneMeno``) of the ``PartneriVerejnehoSektora`` endpoint.

    **Fetch** is IČO-keyed: given an IČO it first resolves the internal
    ``CisloVlozky`` (entry number) and then fetches the full Partner record
    with all beneficial-owner (KUV) entries expanded.
    """

    id = "rpvs_slovakia"

    # Dispatches on the sk_ico identifier derived by the RPO adapter.
    lookup_dispatch_keys = ("sk_ico",)


    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="RPVS Slovakia",
            homepage="https://rpvs.gov.sk/rpvs",
            description=(
                "Slovak Public Sector Partners Register (Register partnerov "
                "verejného sektora), published by the Ministry of Justice of "
                "the Slovak Republic.  Lists entities supplying goods or "
                "services to public bodies above statutory thresholds, with "
                "verified ultimate beneficial owner (KUV) disclosures."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Slovak Public Sector Partners Register "
                "(RPVS), published by the Ministry of Justice of the Slovak "
                "Republic, CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
        )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []

        cache_key = f"{_CACHE_NS}/search/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        q = quote(query.lower(), safe="")
        url = (
            f"{_BASE}/PartneriVerejnehoSektora"
            f"?$filter=contains(tolower(ObchodneMeno),'{q}')"
            f"&$expand=Partner"
        )
        items = await self._get_list(url, cache_key=cache_key)

        # Deduplicate by Partner.CisloVlozky — multiple versioned PVS records
        # may share the same partner entry.
        seen_partner_ids: set[int] = set()
        hits: list[SourceHit] = []
        for item in items:
            partner = item.get("Partner") or {}
            partner_id: int | None = partner.get("Id")
            if partner_id is None or partner_id in seen_partner_ids:
                continue
            seen_partner_ids.add(partner_id)

            hit = self._search_hit(item, partner_id)
            if hit is not None:
                hits.append(hit)

        return hits

    # ------------------------------------------------------------------
    # Fetch (identifier-keyed — by IČO)
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch the full RPVS record for a given IČO.

        Two-step:
        1. Resolve IČO → internal ``CisloVlozky`` (Partner.Id).
        2. Fetch ``Partneri({id})`` with KUVs expanded.

        Returns a bundle dict or ``{"is_stub": True}`` when live mode is off
        and no cache entry exists.
        """
        ico = _normalise_ico(hit_id)
        if not ico:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}

        # Step 1 — resolve IČO → Partner.Id
        resolve_key = f"{_CACHE_NS}/resolve/{ico}"
        partner_id: int | None = None

        if self._cache.has(resolve_key):
            cached = self._cache.get_payload(resolve_key)
            if cached is not None:
                partner_id = cached[0]

        if partner_id is None:
            if not self.info.live_available:
                return {"source_id": self.id, "hit_id": ico, "is_stub": True}

            resolve_url = (
                f"{_BASE}/PartneriVerejnehoSektora"
                f"?$filter=Ico+eq+'{ico}'&$expand=Partner"
            )
            pvs_items = await self._get_list(resolve_url, cache_key=resolve_key)
            if not pvs_items:
                return {"source_id": self.id, "hit_id": ico, "is_stub": True}

            # Pick the most recent entry (highest Id) and extract Partner.Id.
            for pvs in sorted(pvs_items, key=lambda x: x.get("Id") or 0, reverse=True):
                p = pvs.get("Partner") or {}
                if p.get("Id"):
                    partner_id = p["Id"]
                    break

            if partner_id is None:
                return {"source_id": self.id, "hit_id": ico, "is_stub": True}

            self._cache.put(resolve_key, partner_id)

        # Step 2 — fetch full Partner record
        detail_key = f"{_CACHE_NS}/partner/{partner_id}"
        if self._cache.has(detail_key):
            cached = self._cache.get_payload(detail_key)
            if cached is not None:
                raw = cached[0]
                return self._build_bundle(raw, ico, partner_id)

        if not self.info.live_available:
            return {"source_id": self.id, "hit_id": ico, "is_stub": True}

        detail_url = (
            f"{_BASE}/Partneri({partner_id})"
            f"?$expand={_PARTNER_EXPAND}"
        )
        try:
            async with build_client() as client:
                resp = await client.get(detail_url)
                resp.raise_for_status()
                raw = resp.json()
        except Exception as exc:  # noqa: BLE001
            _log.warning("RPVS fetch failed for partner %s: %s", partner_id, exc)
            return {"source_id": self.id, "hit_id": ico, "is_stub": True}

        self._cache.put(detail_key, raw)
        return self._build_bundle(raw, ico, partner_id)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _get_list(self, url: str, *, cache_key: str) -> list[dict[str, Any]]:
        """GET a URL that returns an OData ``value`` array."""
        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            data = cached[0]
            return data if isinstance(data, list) else []

        try:
            async with build_client() as client:
                resp = await client.get(url)
                resp.raise_for_status()
                payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            _log.warning("RPVS request failed for %s: %s", url, exc)
            return []

        items: list[dict[str, Any]] = []
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            items = payload.get("value") or []

        self._cache.put(cache_key, items)
        return items

    # ------------------------------------------------------------------
    # Bundle builder
    # ------------------------------------------------------------------

    def _build_bundle(
        self,
        raw: dict[str, Any],
        ico: str,
        partner_id: int,
    ) -> dict[str, Any]:
        """Normalise a raw ``Partneri`` response into a fetch bundle."""
        pvs_entries: list[dict] = raw.get("PartneriVerejnehoSektora") or []
        kuvs: list[dict] = raw.get("KonecniUzivateliaVyhod") or []
        opravnene_osoby: list[dict] = raw.get("OpravneneOsoby") or []

        current_pvs = _current_entry(pvs_entries)
        name = (current_pvs or {}).get("ObchodneMeno") or ""
        platnost_od = (current_pvs or {}).get("PlatnostOd")
        platnost_do = (current_pvs or {}).get("PlatnostDo")
        status = "active" if not platnost_do else "deleted"

        # Active KUVs are those with PlatnostDo == null.
        active_kuvs = [k for k in kuvs if not k.get("PlatnostDo")]

        return {
            "source_id": self.id,
            "hit_id": ico,
            "sk_ico": ico,
            "partner_id": partner_id,
            "cislo_vlozky": raw.get("CisloVlozky") or partner_id,
            "name": name.strip(),
            "platnost_od": platnost_od,
            "platnost_do": platnost_do,
            "status": status,
            "kuvs": kuvs,
            "active_kuvs": active_kuvs,
            "pvs_entries": pvs_entries,
            "opravnene_osoby": opravnene_osoby,
            "link": _partner_link(partner_id),
            "raw": raw,
            "is_stub": False,
        }

    # ------------------------------------------------------------------
    # Hit factory (search path)
    # ------------------------------------------------------------------

    def _search_hit(
        self,
        pvs: dict[str, Any],
        partner_id: int,
    ) -> SourceHit | None:
        name = (pvs.get("ObchodneMeno") or "").strip()
        if not name:
            return None
        ico = _normalise_ico(pvs.get("Ico"))
        platnost_do = pvs.get("PlatnostDo")
        status = "active" if not platnost_do else "deleted"

        summary_bits = []
        if ico:
            summary_bits.append(f"IČO {ico}")
        summary_bits.append(f"RPVS #{partner_id}")
        summary_bits.append(status)

        return SourceHit(
            source_id=self.id,
            hit_id=ico or str(partner_id),
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(summary_bits),
            identifiers={
                **({"sk_ico": ico} if ico else {}),
                "rpvs_id": str(partner_id),
            },
            raw=pvs,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub path
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="00000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub RPVS Slovakia record — set OPENCHECK_ALLOW_LIVE=true "
                    "to query live."
                ),
                identifiers={"sk_ico": "00000000"},
                raw={"ObchodneMeno": f"{query} (stub)", "Ico": "00000000"},
            )
        ]
