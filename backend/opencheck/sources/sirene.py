"""France — INSEE's Sirene register, with the Répertoire national des associations.

Sirene (Système d'identification du répertoire des entreprises) is INSEE's
register of every legal unit in France that has a SIREN: companies, but also
associations, foundations, public bodies and sole traders. The Registre
National des Entreprises that ``inpi`` reads holds only units with a
commercial, craft, agricultural or independent activity, so an association or
foundation without one answers 404 there — of the 3,070 ISSUED French LEIs
with an association or foundation legal form (11 Nov 2026), INPI holds 25 and
Sirene 3,069. Investigated on the Notion ticket "France / Sirene + RNA for
associations and foundations"; Stephen's decisions A–G (11 Nov 2026) are what
this module implements:

* **A separate source** (A), dispatched on every French SIREN (B) — the same
  ``siren`` key INPI derives from RA000189 / RA000192 / RA001129.
* **INSEE's API Sirene 3.11** (C): ``GET /siren/{siren}`` for the legal unit
  and its current period, then ``GET /siret/{siren}{nic}`` for the head
  office's address. The key travels in the ``X-INSEE-Api-Key-Integration``
  header (``INSEE_SIRENE_API_KEY``), never the URL; 30 requests a minute per
  key, paced by :mod:`opencheck.host_throttle`.
* **The RNA, read from a monthly index** (D, :mod:`.rna_index`), joined on the
  RNA number Sirene carries (``identifiantAssociationUniteLegale``): the
  association's own creation and dissolution dates, its title as declared,
  its purpose and its recognition of public utility.
* ``FR-RNA`` for the RNA number (E) — org-id has no code for it.
* The FR-INSEE register hop asks INPI first and Sirene only when INPI has no
  record (F, :mod:`opencheck.register_hops`).

Traps measured on 11 Nov 2026
-----------------------------

* ``dateCreationUniteLegale = 1900-01-01`` is a placeholder (204,732 units,
  963 of the LEI set), never a founding date: the RNA's ``date_creat`` is used
  where there is one, otherwise no date — said in an annotation.
* A unit whose diffusion status is not ``O`` (``P``, partially public: the
  person asked INSEE not to publish) is a note card. Nothing from it is
  shown, cached or mapped.
* INSEE answers 404 for a SIREN it does not hold (definitive, cached as an
  absence) and 403 with "non diffusable" for a unit it will not publish at
  all; a 401 or another 403 is the key refused (``not_configured``).

Licence: Licence Ouverte / Open Licence 2.0 (Sirene and the RNA both).
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

import httpx

from .. import degradation, provenance
from ..cache import Cache
from ..config import get_settings
from ..host_throttle import HostRateLimitedError
from ..http import build_client
from . import rna_index
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .inpi import normalise_siren
from .schemas import validate_raw
from .schemas.sirene import SireneBundle

logger = logging.getLogger(__name__)

API_BASE = "https://api.insee.fr/api-sirene/3.11"
API_KEY_HEADER = "X-INSEE-Api-Key-Integration"
HOMEPAGE = "https://www.sirene.fr/"
LICENCE = "Licence Ouverte / Open Licence 2.0"

#: The public annuaire's page for one unit — the record a reader can open.
ANNUAIRE_URL = "https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"

FR_INSEE_SCHEME = "FR-INSEE"
FR_INSEE_SCHEME_NAME = "SIREN (Sirene, INSEE)"
FR_RNA_SCHEME = "FR-RNA"
FR_RNA_SCHEME_NAME = "Répertoire national des associations"

#: The placeholder INSEE writes when it has no creation date.
PLACEHOLDER_CREATION_DATE = "1900-01-01"

#: ``etatAdministratifUniteLegale``: A active, C ceased (cessée).
ETATS = {"A": "active", "C": "ceased"}

_CACHE_NS = "sirene"
#: Bump when the cached payload's shape changes.
#: v2 keeps the unit's history periods beside the flattened unit (the Sirene
#: history ticket); a v1 entry has none, so it is not read.
_CACHE_VERSION = "v2"
_REQUEST_TIMEOUT = httpx.Timeout(20.0, connect=10.0)

COVERAGE_NOT_FOUND = (
    "INSEE's Sirene register has no unit under this SIREN. Sirene holds every "
    "legal unit in France that has a SIREN, so the number is probably not "
    "a SIREN."
)
COVERAGE_NOT_DIFFUSIBLE = (
    "INSEE does not publish this unit's record: its diffusion status says the "
    "unit, or the person behind it, has asked INSEE not to make it public. "
    "OpenCheck shows nothing from it."
)


def unit_url(siren: str) -> str:
    """The API's URL for one unit, **without** the key — a statement's source URL."""
    return f"{API_BASE}/siren/{siren}"


def annuaire_url(siren: str) -> str:
    return ANNUAIRE_URL.format(siren=siren)


def current_period(unit: dict[str, Any]) -> dict[str, Any]:
    """The unit's open period (``dateFin`` null), else its first listed one —
    INSEE lists the latest first."""
    periods = [p for p in unit.get("periodesUniteLegale") or [] if isinstance(p, dict)]
    for period in periods:
        if not period.get("dateFin"):
            return period
    return periods[0] if periods else {}


def flatten_unit(unit: dict[str, Any]) -> dict[str, Any]:
    """The unit's own fields plus its current period's, one flat dict — the
    shape the bundle carries. Earlier periods are not kept (Sirene history is
    the follow-up ticket, decision G) — :func:`history_periods` keeps them
    for the History tab."""
    flat = {k: v for k, v in unit.items() if k != "periodesUniteLegale" and v not in (None, "")}
    for key, value in current_period(unit).items():
        if value not in (None, ""):
            flat[key] = value
    flat["nombrePeriodesUniteLegale"] = unit.get("nombrePeriodesUniteLegale")
    return flat


#: The period fields the History tab reads (``timeline/sirene.py``) — an
#: allowlist. A sole trader's ``nomUniteLegale`` / ``nomUsageUniteLegale`` and
#: their ``changement…`` flags are a person's names and never kept (decision E
#: of the Sirene history ticket); nor is the employer flag, which INSEE does
#: not historise (no period of the 72.3M in the 1 Nov 2026 file changes it).
PERIOD_FIELDS = (
    "dateDebut",
    "dateFin",
    "etatAdministratifUniteLegale",
    "denominationUniteLegale",
    "denominationUsuelle1UniteLegale",
    "denominationUsuelle2UniteLegale",
    "denominationUsuelle3UniteLegale",
    "categorieJuridiqueUniteLegale",
    "activitePrincipaleUniteLegale",
    "nomenclatureActivitePrincipaleUniteLegale",
    "nicSiegeUniteLegale",
    "economieSocialeSolidaireUniteLegale",
    "societeMissionUniteLegale",
)


def history_periods(unit: dict[str, Any]) -> list[dict[str, Any]]:
    """Every period of the unit, oldest first, reduced to :data:`PERIOD_FIELDS`.

    INSEE lists the latest first; a period with no ``dateDebut`` (the start of
    the record) sorts first.
    """
    periods = [
        {k: p.get(k) for k in PERIOD_FIELDS if p.get(k) not in (None, "")}
        for p in unit.get("periodesUniteLegale") or []
        if isinstance(p, dict)
    ]
    return sorted(periods, key=lambda p: str(p.get("dateDebut") or ""))


def head_office_address(etablissement: dict[str, Any]) -> dict[str, Any]:
    """The head office's address block, empty fields dropped."""
    block = etablissement.get("adresseEtablissement") or {}
    return {k: v for k, v in block.items() if v not in (None, "", "[ND]")}


def address_line(block: dict[str, Any]) -> str:
    """``1 RUE DE LA PAIX 75002 PARIS 2`` from an ``adresseEtablissement``."""
    def g(field: str) -> str:
        return str(block.get(f"{field}Etablissement") or "").strip()

    street = " ".join(
        p for p in (g("numeroVoie"), g("indiceRepetition"), g("typeVoie"), g("libelleVoie")) if p
    )
    city = g("libelleCommune") or g("libelleCommuneEtranger")
    parts = [g("complementAdresse"), street, " ".join(p for p in (g("codePostal"), city) if p)]
    country = g("libellePaysEtranger")
    if country:
        parts.append(country)
    return ", ".join(p for p in parts if p)


def _insee_message(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return ""
    if isinstance(body, dict):
        header = body.get("header") or {}
        if isinstance(header, dict):
            return str(header.get("message") or "")
    return ""


class SireneUnanswered(Exception):
    """A request that did not answer: the degradation it would record."""

    def __init__(self, detail: str, reason: str = degradation.REASON_UPSTREAM_ERROR):
        super().__init__(detail)
        self.detail = detail
        self.reason = reason


class _NotFound(Exception):
    pass


class _NotDiffusible(Exception):
    pass


async def insee_get(client: httpx.AsyncClient, path: str, *, what: str) -> dict[str, Any]:
    """One API Sirene call. Raises :class:`_NotFound` on 404,
    :class:`_NotDiffusible` on a 403 that says so, and
    :class:`SireneUnanswered` on anything else that is not a JSON object."""
    key = get_settings().insee_sirene_api_key or ""
    try:
        response = await client.get(
            f"{API_BASE}{path}",
            headers={API_KEY_HEADER: key, "Accept": "application/json"},
            timeout=_REQUEST_TIMEOUT,
        )
    except HostRateLimitedError:
        raise SireneUnanswered(
            f"INSEE Sirene {what}: not sent — the API's 30-a-minute pacing is spent",
            degradation.REASON_RATE_LIMITED,
        ) from None
    except httpx.HTTPError as exc:
        kind = type(exc).__name__
        raise SireneUnanswered(
            f"INSEE Sirene {what} did not answer ({kind})", degradation.reason_for_failure(kind)
        ) from None
    status = response.status_code
    if status == 404:
        raise _NotFound()
    if status == 403 and "diffusable" in _insee_message(response).lower():
        raise _NotDiffusible()
    if status in (401, 403):
        raise SireneUnanswered(
            f"INSEE Sirene {what}: the API key was refused (HTTP {status})",
            degradation.REASON_NOT_CONFIGURED,
        )
    if not response.is_success:
        raise SireneUnanswered(
            f"INSEE Sirene {what} returned HTTP {status}",
            degradation.reason_for_failure(f"HTTP {status}"),
        )
    try:
        body = response.json()
    except ValueError:
        raise SireneUnanswered(f"INSEE Sirene {what} returned a body that is not JSON") from None
    if not isinstance(body, dict):
        raise SireneUnanswered(f"INSEE Sirene {what} returned an unexpected shape")
    return body


class SireneAdapter(SourceAdapter):
    """Source adapter for INSEE's Sirene register, with the RNA for associations."""

    id = "sirene"

    #: The SIREN INPI's deriver builds from RA000189 / RA000192 / RA001129
    #: (decision B: every French SIREN reaches both registers).
    lookup_dispatch_keys = ("siren",)
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Sirene — INSEE register of legal units (France), with the RNA",
            homepage=HOMEPAGE,
            description=(
                "INSEE's Sirene register, read through API Sirene: every French "
                "legal unit with a SIREN — companies, but also associations, "
                "foundations and public bodies, which the Registre National des "
                "Entreprises does not hold. Name, legal category, status, "
                "creation date and head-office address; for an association, its "
                "record in the Répertoire national des associations (title, "
                "purpose, declaration and dissolution dates). Sirene publishes "
                "no officers, shareholders or beneficial owners."
            ),
            license=LICENCE,
            attribution=(
                "Source: INSEE, Sirene (api.insee.fr), and the Ministry of the "
                "Interior, Répertoire national des associations (data.gouv.fr). "
                "Licence Ouverte / Open Licence 2.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(settings.allow_live and settings.insee_sirene_api_key),
            is_national_register=True,
            country="FR",
        )

    def covers_lei(self, lei: str) -> bool:
        """Announced only with a key: without one every French lookup would
        announce a source that answers nothing."""
        return bool(get_settings().insee_sirene_api_key)

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """No name search in v1: the GLEIF and INPI paths find French
        companies by name, and API Sirene's multi-criteria search spends the
        30-a-minute allowance a lookup needs."""
        return []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The legal unit, its head office, for an association its RNA record
        and, for a company on a trade register, its latest BODACC
        collective-procedure notice — by SIREN."""
        bundle = await self._fetch_register(hit_id, legal_name=legal_name)
        if isinstance(bundle.get("unit"), dict) and not bundle.get("is_stub"):
            await self._attach_bodacc(bundle)
        return bundle

    async def _attach_bodacc(self, bundle: dict[str, Any]) -> None:
        """The QuickCheck clause (the BODACC ticket's decision E, 11 Nov 2026):
        the latest collective-procedure notice about the company, said in the
        card's sentence. Context only — never a risk signal, never a verdict
        clause. A legal category with no trade-register filing (public bodies,
        associations: first digit 4, 7, 8 or 9) is not asked. BODACC not
        answering is a ``source_read`` degradation, never "no procedure"."""
        from .. import bodacc

        category = str((bundle.get("unit") or {}).get("categorieJuridiqueUniteLegale") or "")
        if category[:1] in {"4", "7", "8", "9"} or not get_settings().allow_live:
            return
        try:
            payload = await bodacc.notices_for(bundle.get("siren"))
        except bodacc.BodaccUnavailable as exc:
            bundle["bodacc_read"] = False
            degradation.record(
                self.id,
                f"{exc.detail} The card does not say whether a collective procedure was published.",
                check=degradation.CHECK_SOURCE_READ,
                reason=exc.reason,
            )
            return
        except ValueError:
            return
        bundle["bodacc_read"] = True
        latest = bodacc.latest_collective(payload)
        bundle["bodacc_collective"] = (
            {k: latest.get(k) for k in ("id", "url", "kind", "label", "effective", "published")}
            if latest
            else None
        )

    async def _fetch_register(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        try:
            siren = normalise_siren(str(hit_id or ""))
        except ValueError:
            return self._stub(str(hit_id or ""), legal_name)

        cache_key = f"{_CACHE_NS}/unit-{_CACHE_VERSION}/{siren}"
        cached = self._cache.get_payload(cache_key, max_age_days=1)
        if cached is not None:
            held = cached[0]
            if held is None:
                return self._note(siren, legal_name, "not_found", COVERAGE_NOT_FOUND)
            if isinstance(held, dict) and isinstance(held.get("unit"), dict):
                return self._bundle(siren, legal_name, held)

        settings = get_settings()
        if not (settings.allow_live and settings.insee_sirene_api_key):
            if not settings.insee_sirene_api_key:
                degradation.record(
                    self.id,
                    "INSEE Sirene: no API key is configured, so the register was not read",
                    reason=degradation.REASON_NOT_CONFIGURED,
                )
            return self._stub(siren, legal_name)

        async with build_client() as client:
            try:
                body = await insee_get(client, f"/siren/{siren}", what="legal unit")
            except _NotFound:
                self._cache.put_absent(cache_key)
                return self._note(siren, legal_name, "not_found", COVERAGE_NOT_FOUND)
            except _NotDiffusible:
                return self._note(siren, legal_name, "not_diffusible", COVERAGE_NOT_DIFFUSIBLE)
            except SireneUnanswered as exc:
                degradation.record(self.id, exc.detail, reason=exc.reason)
                return self._stub(siren, legal_name)
            unit = body.get("uniteLegale")
            if not isinstance(unit, dict):
                degradation.record(
                    self.id, "INSEE Sirene returned no legal unit in its answer",
                    reason=degradation.REASON_UPSTREAM_ERROR,
                )
                return self._stub(siren, legal_name)
            flat = flatten_unit(unit)
            if str(flat.get("statutDiffusionUniteLegale") or "O") != "O":
                return self._note(siren, legal_name, "not_diffusible", COVERAGE_NOT_DIFFUSIBLE)

            payload: dict[str, Any] = {
                "unit": flat,
                "periods": history_periods(unit),
                "head_office": None,
                "head_office_read": True,
            }
            nic = re.sub(r"\D", "", str(flat.get("nicSiegeUniteLegale") or ""))
            if len(nic) == 5:
                try:
                    siret_body = await insee_get(client, f"/siret/{siren}{nic}", what="head office")
                    etab = siret_body.get("etablissement")
                    if isinstance(etab, dict):
                        payload["head_office"] = {
                            "siret": etab.get("siret") or f"{siren}{nic}",
                            "address": head_office_address(etab),
                        }
                except (_NotFound, _NotDiffusible):
                    pass
                except SireneUnanswered as exc:
                    payload["head_office_read"] = False
                    degradation.record(
                        self.id,
                        exc.detail + "; the card shows the legal unit without its address",
                        check=degradation.CHECK_SOURCE_READ,
                        reason=exc.reason,
                    )
        if payload["head_office_read"]:
            self._cache.put(cache_key, payload)
        return self._bundle(siren, legal_name, payload)

    # ------------------------------------------------------------------
    # Bundles
    # ------------------------------------------------------------------

    def _rna(self, unit: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """The RNA record the unit names, and how the read went: ``read``,
        ``not_in_index``, ``no_index``, ``failed`` or ``none`` (the unit names
        no association)."""
        raw = unit.get("identifiantAssociationUniteLegale")
        if not raw:
            return None, "none"
        try:
            rna = rna_index.normalise_rna(raw)
        except ValueError:
            return None, "none"
        if not rna_index.index_available():
            return None, "no_index"
        try:
            record = rna_index.association(rna)
        except (sqlite3.Error, ValueError) as exc:
            degradation.record(
                self.id,
                "The Répertoire national des associations index could not be read; "
                "the association's RNA record is not shown",
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return None, "failed"
        return record, ("read" if record else "not_in_index")

    def _stub(self, siren: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": siren,
            "siren": siren,
            "name": legal_name or "",
            "link": annuaire_url(siren) if siren else HOMEPAGE,
            "is_stub": True,
        }

    def _note(self, siren: str, legal_name: str, kind: str, note: str) -> dict[str, Any]:
        """A note card: INSEE answered and holds nothing it will show. Not a
        degradation — nothing failed."""
        return {
            "source_id": self.id,
            "hit_id": siren,
            "siren": siren,
            "name": legal_name or "",
            "link": annuaire_url(siren),
            "is_stub": False,
            "not_found": True,
            "not_found_kind": kind,
            "coverage_note": note,
        }

    def _bundle(self, siren: str, legal_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        unit = dict(payload.get("unit") or {})
        rna_record, rna_status = self._rna(unit)
        meta = rna_index.read_meta() if rna_status in ("read", "not_in_index") else {}
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": siren,
            "siren": siren,
            "legal_name": legal_name or "",
            "unit": unit,
            "periods": list(payload.get("periods") or []),
            "head_office": payload.get("head_office"),
            "head_office_read": bool(payload.get("head_office_read", True)),
            "rna": rna_record,
            "rna_status": rna_status,
            "rna_cut": meta.get("cut") or None,
            "link": annuaire_url(siren),
            "is_stub": False,
        }
        validate_raw(self.id, SireneBundle, bundle)
        return bundle


__all__ = [
    "FR_INSEE_SCHEME",
    "FR_RNA_SCHEME",
    "SireneAdapter",
]
