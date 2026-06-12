"""Lookup endpoints — /lookup, /lookup-stream, /deepen, /report."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field as dc_field
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from .. import __version__
from ..bods import (
    BODSBundle,
    map_acra_singapore,
    map_ares,
    map_ariregister,
    map_abr_australia,
    map_bce_belgium,
    map_corporations_canada,
    map_bolagsverket,
    map_brreg,
    map_climatetrace,
    map_companies_house,
    map_cro,
    map_cvr_denmark,
    map_everypolitician,
    map_gleif,
    map_inpi,
    map_openaleph,
    map_opencorporates,
    map_opensanctions,
    map_prh,
    map_jar_lithuania,
    map_krs_poland,
    map_firmenbuch,
    map_rpo_slovakia,
    map_rpvs_slovakia,
    map_sec_edgar,
    map_sudreg_croatia,
    map_ur_latvia,
    map_wikidata,
    map_kvk,
    map_zefix,
    validate_shape,
)
from ..sources.abr_australia import (
    ABR_ASIC_RA_CODE as _ABR_ASIC_RA_CODE,
    ABR_ABR_RA_CODE as _ABR_ABR_RA_CODE,
    normalise_acn as _normalise_acn,
    normalise_abn as _normalise_abn,
)
from ..sources.ares import CZ_RA_CODE as _CZ_RA_CODE, normalise_ico as _normalise_ico
from ..sources.bce_belgium import BCE_RA_CODE as _BCE_RA_CODE, normalise_enterprise_number as _normalise_enterprise_number
from ..sources.corporations_canada import CA_CORP_RA_CODE as _CA_CORP_RA_CODE, normalise_corp_id as _normalise_corp_id
from ..sources.krs_poland import PL_KRS_RA_CODE as _PL_KRS_RA_CODE, normalise_krs as _normalise_krs
from ..sources.firmenbuch import AT_FB_RA_CODE as _AT_FB_RA_CODE, normalise_fn as _normalise_fn
from ..sources.rpo_slovakia import SK_RPO_RA_CODE as _SK_RPO_RA_CODE, normalise_ico as _normalise_sk_ico
from ..sources.ariregister import EE_RA_CODE as _EE_RA_CODE
from ..sources.bolagsverket import BV_RA_CODE as _BV_RA_CODE, normalise_org_number as _normalise_org_number
from ..sources.brreg import NO_RA_CODE as _BRREG_RA_CODE, normalise_orgnr as _normalise_orgnr
from ..sources.cro import IE_RA_CODE as _CRO_RA_CODE, normalise_crn as _normalise_crn
from ..sources.prh import FI_RA_CODE as _PRH_RA_CODE, normalise_ytunnus as _normalise_ytunnus
from ..sources.ur_latvia import LV_RA_CODE as _LV_RA_CODE, normalise_regcode as _normalise_lv_regcode
from ..sources.jar_lithuania import LT_RA_CODE as _LT_RA_CODE, normalise_code as _normalise_lt_code
from ..sources.inpi import INPI_RA_CODE as _INPI_RA_CODE, normalise_siren as _normalise_siren
from ..sources.kvk import KVK_RA_CODE as _KVK_RA_CODE, normalise_kvk as _normalise_kvk
from ..sources.zefix import CH_RA_CODES as _ZEFIX_RA_CODES, normalise_uid as _zefix_normalise_uid
from ..sources.cvr_denmark import DK_CVR_RA_CODE as _DK_CVR_RA_CODE, normalise_cvr as _normalise_cvr
from ..sources.sudreg_croatia import SUDREG_RA_CODE as _SUDREG_RA_CODE, normalise_mbs as _normalise_mbs
from .. import bods_data
from ..cross_check import assess_cross_source_names
from ..icij_check import assess_icij_names
from ..reconcile import reconcile
from ..risk import RiskSignal, assess_bundle, assess_hits
from ..sources import REGISTRY, SearchKind, SourceHit, SourceInfo
from ..sources.schemas import SourceSchemaError

router = APIRouter()


def _fmt_source_error(exc: Exception) -> str:
    """Format a source fetch exception for the errors dict and SSE events."""
    if isinstance(exc, SourceSchemaError):
        return f"Source API changed — {exc}"
    return f"{type(exc).__name__}: {exc}"


_MAPPERS = {
    "acra_singapore": map_acra_singapore,
    "ariregister": map_ariregister,
    "bce_belgium": map_bce_belgium,
    "bolagsverket": map_bolagsverket,
    "brreg": map_brreg,
    "corporations_canada": map_corporations_canada,
    "climatetrace": map_climatetrace,
    "companies_house": map_companies_house,
    "cro": map_cro,
    "gleif": map_gleif,
    "inpi": map_inpi,
    "opencorporates": map_opencorporates,
    "opensanctions": map_opensanctions,
    "openaleph": map_openaleph,
    "sec_edgar": map_sec_edgar,
    "wikidata": map_wikidata,
    "everypolitician": map_everypolitician,
    "kvk": map_kvk,
    "prh": map_prh,
    "ares": map_ares,
    "krs_poland": map_krs_poland,
    "firmenbuch": map_firmenbuch,
    "rpo_slovakia": map_rpo_slovakia,
    "rpvs_slovakia": map_rpvs_slovakia,
    "jar_lithuania": map_jar_lithuania,
    "ur_latvia": map_ur_latvia,
    "zefix": map_zefix,
    "cvr_denmark": map_cvr_denmark,
    "sudreg_croatia": map_sudreg_croatia,
    "abr_australia": map_abr_australia,
}

_NC_LICENSES = {"CC-BY-NC-4.0", "CC-BY-NC-SA-4.0"}

# 20-char ISO 17442 LEI, alphanumeric uppercase.
_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


class SearchResponse(BaseModel):
    query: str
    kind: SearchKind
    hits: list[SourceHit]
    errors: dict[str, str]
    cross_source_links: list[dict[str, Any]]
    risk_signals: list[dict[str, Any]]


class DeepenResponse(BaseModel):
    source_id: str
    hit_id: str
    raw: dict[str, Any]
    bods: list[dict[str, Any]]
    bods_issues: list[str]
    license: str
    license_notice: str | None = None
    risk_signals: list[dict[str, Any]] = []


class ReportResponse(BaseModel):
    """Aggregate post-search synthesis for a single subject."""

    query: str
    kind: SearchKind
    hits: list[SourceHit]
    errors: dict[str, str]
    cross_source_links: list[dict[str, Any]]
    risk_signals: list[dict[str, Any]]
    bods: list[dict[str, Any]]
    bods_issues: list[str]
    license_notices: list[dict[str, str]]


class LookupResponse(ReportResponse):
    """Same shape as /report, with the LEI echoed back and the GLEIF
    bundle surfaced separately so the UI doesn't have to dig for it."""

    lei: str
    legal_name: str | None = None
    jurisdiction: str | None = None
    derived_identifiers: dict[str, str] = {}


@router.get("/deepen", response_model=DeepenResponse)
async def deepen(
    source: str = Query(..., description="Adapter id, e.g. 'companies_house'"),
    hit_id: str = Query(..., description="Adapter-local hit id"),
) -> DeepenResponse:
    """Fetch the full record for a single hit and map to BODS v0.4."""

    adapter = REGISTRY.get(source)
    if adapter is None:
        raise HTTPException(status_code=404, detail=f"unknown source {source!r}")

    raw = await adapter.fetch(hit_id)

    override = _bods_data_override(source, hit_id)
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if override is not None:
        bods = override
        issues = validate_shape(bods)
    else:
        mapper = _MAPPERS.get(source)
        if mapper and not raw.get("is_stub"):
            bundle: BODSBundle = mapper(raw)
            bods = list(bundle)
            issues = validate_shape(bods)

    info = adapter.info
    license_notice = _license_notice_for(info, raw)
    signals = [s.to_dict() for s in assess_bundle(source, raw, bods, hit_id=hit_id)]

    return DeepenResponse(
        source_id=source,
        hit_id=hit_id,
        raw=raw,
        bods=bods,
        bods_issues=issues,
        license=info.license,
        license_notice=license_notice,
        risk_signals=signals,
    )


@router.get("/report", response_model=ReportResponse)
async def report(
    q: str = Query(..., min_length=1),
    kind: SearchKind = Query(SearchKind.ENTITY),
    deepen_top: int = Query(
        3, ge=0, le=10, description="How many top hits to deepen+map+assess."
    ),
) -> ReportResponse:
    """One-shot synthesis: search, reconcile, deepen top N, assess risk."""
    return await _build_report(q, kind, deepen_top)


async def _build_report(
    q: str, kind: SearchKind, deepen_top: int
) -> ReportResponse:
    """Shared by /report and /export. Same algorithm; same response shape."""
    from .search import _run_adapters  # avoid circular at module level
    results, errors = await _run_adapters(q, kind)
    hits = [hit for adapter_hits in results.values() for hit in adapter_hits]
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]

    deep_hits = [h for h in hits if not h.is_stub][:deepen_top]
    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    deepen_signals: list[dict[str, Any]] = []
    license_notices: list[dict[str, str]] = []

    deepen_tasks = {
        (h.source_id, h.hit_id): asyncio.create_task(
            _safe_deepen(h.source_id, h.hit_id)
        )
        for h in deep_hits
    }
    for (source_id, hit_id), task in deepen_tasks.items():
        try:
            bundle = await task
        except Exception as exc:  # noqa: BLE001
            errors.setdefault(source_id, f"{type(exc).__name__}: {exc}")
            continue
        if bundle is None:
            continue
        bods_all.extend(bundle["bods"])
        bods_issues.extend(bundle["bods_issues"])
        deepen_signals.extend(bundle["risk_signals"])
        if bundle.get("license_notice"):
            license_notices.append(
                {
                    "source_id": source_id,
                    "hit_id": hit_id,
                    "notice": bundle["license_notice"],
                }
            )

    cross_signals = [
        s.to_dict() for s in await assess_cross_source_names(bods_all)
    ]
    icij_signals = [
        s.to_dict() for s in await assess_icij_names(bods_all)
    ]

    all_signals = _merge_signals(
        search_signals, deepen_signals, cross_signals, icij_signals
    )

    return ReportResponse(
        query=q,
        kind=kind,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=all_signals,
        bods=bods_all,
        bods_issues=bods_issues,
        license_notices=license_notices,
    )


# ---------------------------------------------------------------------------
# LEI-anchored lookup — one pipeline drives both /lookup and /lookup-stream
# ---------------------------------------------------------------------------
#
# ``_lookup_pipeline()`` is the ONLY place that resolves the GLEIF anchor,
# builds derived identifiers, dispatches adapters, converts results to
# SourceHits, deepens, and assesses risk. It yields ``(event, payload)``
# tuples; /lookup-stream serialises them as SSE and /lookup collects them
# into a LookupResponse. Until this refactor the two endpoints were
# hand-synchronised copies of each other — forgetting to edit both was a
# recurring bug (see the Corporations Canada regression fixed in 603c086).
#
# Wiring a new national-register adapter now means exactly two entries here:
# one in ``_RA_DERIVERS`` (RA code → derived identifier) and one in
# ``_REGISTRY_SOURCES`` (dispatch + hit builder).

LookupEvent = tuple[str, Any]


@dataclass
class _LookupCtx:
    """Mutable context threaded through one lookup run."""

    lei: str
    legal_name: str = ""
    jurisdiction: str = ""
    registered_as: str = ""
    derived: dict[str, str] = dc_field(default_factory=dict)
    ocid: str | None = None
    qid: str | None = None


def _zfill8(value: str) -> str:
    return value.strip().zfill(8)


# RA code(s) on the GLEIF record's ``registeredAt.id`` → (derived-identifier
# key, normaliser). GB is special-cased on jurisdiction in _build_derived()
# because UK records reliably carry registeredAs. Normalisers may raise
# ValueError for malformed local IDs — the source is then skipped.
_RA_DERIVERS: list[tuple[frozenset[str], str, Any]] = [
    (frozenset(_ZEFIX_RA_CODES), "che_uid", _zefix_normalise_uid),
    (frozenset({_KVK_RA_CODE}), "kvk_number", _normalise_kvk),
    (frozenset({_INPI_RA_CODE}), "siren", _normalise_siren),
    (frozenset({_BV_RA_CODE}), "se_org_number", _normalise_org_number),
    (frozenset({_EE_RA_CODE}), "ee_registry_code", _zfill8),
    (frozenset({_BRREG_RA_CODE}), "no_orgnr", _normalise_orgnr),
    (frozenset({_CRO_RA_CODE}), "ie_crn", _normalise_crn),
    (frozenset({_PRH_RA_CODE}), "fi_ytunnus", _normalise_ytunnus),
    (frozenset({_LV_RA_CODE}), "lv_regcode", _normalise_lv_regcode),
    (frozenset({_LT_RA_CODE}), "lt_code", _normalise_lt_code),
    (frozenset({_CZ_RA_CODE}), "cz_ico", _normalise_ico),
    (frozenset({_PL_KRS_RA_CODE}), "pl_krs", _normalise_krs),
    (frozenset({_AT_FB_RA_CODE}), "at_fn", _normalise_fn),
    (frozenset({_SK_RPO_RA_CODE}), "sk_ico", _normalise_sk_ico),
    (frozenset({_BCE_RA_CODE}), "be_enterprise_number", _normalise_enterprise_number),
    (frozenset({_CA_CORP_RA_CODE}), "ca_corp_id", _normalise_corp_id),
    (frozenset({_DK_CVR_RA_CODE}), "dk_cvr", _normalise_cvr),
    (frozenset({_SUDREG_RA_CODE}), "hr_mbs", _normalise_mbs),
    (frozenset({_ABR_ASIC_RA_CODE}), "au_acn", _normalise_acn),
    (frozenset({_ABR_ABR_RA_CODE}), "au_abn", _normalise_abn),
]
# NOTE: ACRA Singapore (RA000523) adapter is implemented but not wired into
# lookup dispatch. The data.gov.sg dataset is bulk CSV only (no live API),
# which doesn't fit the fast-API pattern used by the other national registers.
# To enable: add an _RA_DERIVERS / _REGISTRY_SOURCES entry and build the DB
# with scripts/extract_acra.py.


def _build_derived(ctx: _LookupCtx, registered_at_id: str) -> None:
    """Populate ctx.derived from the GLEIF anchor record."""
    ctx.derived["lei"] = ctx.lei
    if ctx.jurisdiction.upper() == "GB" and ctx.registered_as:
        ctx.derived["gb_coh"] = ctx.registered_as
    if ctx.registered_as and registered_at_id:
        for codes, key, norm in _RA_DERIVERS:
            if registered_at_id in codes:
                try:
                    ctx.derived[key] = norm(ctx.registered_as)
                except ValueError:
                    pass  # malformed local ID on the LEI record — skip source
                break


def _hit(
    source_id: str,
    hit_id: str,
    *,
    name: str,
    summary: str,
    identifiers: dict[str, str],
    raw: dict[str, Any],
    is_stub: bool = False,
) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=SearchKind.ENTITY,
        name=name,
        summary=summary,
        identifiers=identifiers,
        raw=raw,
        is_stub=is_stub,
    )


# --- per-source hit builders (dict-result registry adapters) ---------------
# Each takes (result, local_id, ctx) and returns a SourceHit. They are only
# called for non-stub dict results; stub/None results yield no hit.


def _bh_companies_house(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    p = r.get("profile") or {}
    # wikidata_qid is intentionally omitted: the QID is sourced exclusively
    # from Wikidata; Companies House does not publish Wikidata mappings, so
    # including it would falsely imply CH corroborates the identifier.
    return _hit(
        "companies_house", local_id,
        name=p.get("company_name", ctx.legal_name or ""),
        summary=f"GB-COH {local_id}",
        identifiers={"gb_coh": local_id}, raw=p,
    )


def _bh_zefix(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "zefix", local_id,
        name=c.get("name") or ctx.legal_name or "",
        summary=f"CHE {local_id}",
        identifiers={"che_uid": local_id}, raw=c,
    )


def _bh_kvk(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "kvk", local_id,
        name=ctx.legal_name or "",
        summary=f"KvK {local_id}",
        identifiers={"kvk_number": local_id}, raw=r.get("company") or {},
    )


def _bh_inpi(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    name = (
        (((c.get("identite") or {}).get("entreprise") or {}).get("denomination"))
        or ctx.legal_name or ""
    )
    return _hit(
        "inpi", local_id,
        name=name, summary=f"FR-SIREN {local_id}",
        identifiers={"siren": local_id}, raw=c,
    )


def _bh_bolagsverket(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    display = (
        f"{local_id[:6]}-{local_id[6:]}" if len(local_id) == 10 else local_id
    )
    return _hit(
        "bolagsverket", local_id,
        name=c.get("namn") or c.get("name") or ctx.legal_name or "",
        summary=f"SE-BLV {display}",
        identifiers={"se_org_number": local_id}, raw=c,
    )


def _bh_ariregister(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "ariregister", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"EE-ARIREGISTER {local_id}",
        identifiers={"ee_registry_code": local_id}, raw=r,
    )


def _bh_brreg(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    e = r.get("entity") or {}
    return _hit(
        "brreg", local_id,
        name=e.get("navn") or ctx.legal_name or "",
        summary=f"NO-ORGNR {local_id}",
        identifiers={"no_orgnr": local_id}, raw=e,
    )


def _bh_cro(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "cro", local_id,
        name=(c.get("company_name") or "").strip() or ctx.legal_name or "",
        summary=f"IE-CRN {local_id}",
        identifiers={"ie_crn": local_id}, raw=c,
    )


def _bh_prh(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    name = ""
    for n in (c.get("names") or []):
        if not n.get("endDate") and n.get("order") == 0:
            name = (n.get("name") or "").strip()
            break
    return _hit(
        "prh", local_id,
        name=name or ctx.legal_name or "",
        summary=f"FI-YTUNNUS {local_id}",
        identifiers={"fi_ytunnus": local_id}, raw=c,
    )


def _bh_ur_latvia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    e = r.get("entity") or {}
    return _hit(
        "ur_latvia", local_id,
        name=(e.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"LV-UR {local_id}",
        identifiers={"lv_regcode": local_id}, raw=e,
    )


def _bh_jar_lithuania(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "jar_lithuania", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"LT-JAR {local_id}",
        identifiers={"lt_code": local_id}, raw=r,
    )


def _bh_ares(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    e = r.get("entity") or {}
    return _hit(
        "ares", local_id,
        name=(e.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"CZ-ARES IČO {local_id}",
        identifiers={"cz_ico": local_id}, raw=e,
    )


def _bh_krs_poland(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "krs_poland", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"KRS {local_id}",
        identifiers={"pl_krs": local_id}, raw=r,
    )


def _bh_firmenbuch(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "firmenbuch", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"FN {local_id}",
        identifiers={"at_fn": local_id}, raw=r,
    )


def _bh_rpo_slovakia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "rpo_slovakia", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"SK-IČO {local_id}",
        identifiers={"sk_ico": local_id}, raw=r,
    )


def _bh_rpvs_slovakia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "rpvs_slovakia", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"SK-IČO {local_id} · RPVS #{r.get('partner_id', '')}",
        identifiers={
            "sk_ico": local_id,
            **({"rpvs_id": str(r["partner_id"])} if r.get("partner_id") else {}),
        },
        raw=r,
    )


def _bh_bce_belgium(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "bce_belgium", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"BE {r.get('dotted') or local_id}",
        identifiers={"be_enterprise_number": local_id}, raw=r,
    )


def _bh_corporations_canada(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    corp = r.get("corporation") or {}
    name = ""
    for entry in (corp.get("corporationNames") or []):
        cn = entry.get("CorporationName") or {}
        if cn.get("current"):
            name = (cn.get("name") or "").strip()
            if (cn.get("nameType") or "").lower() == "primary":
                break
    return _hit(
        "corporations_canada", local_id,
        name=name or ctx.legal_name or "",
        summary=f"CA-CORP {local_id}",
        identifiers={"ca_corp_id": local_id}, raw=corp,
    )


def _bh_cvr_denmark(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "cvr_denmark", local_id,
        name=r.get("name") or ctx.legal_name or "",
        summary=f"DK-CVR {local_id}",
        identifiers={"dk_cvr": local_id}, raw=r,
    )


def _bh_sudreg_croatia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    subject = r.get("subject") or {}
    return _hit(
        "sudreg_croatia", local_id,
        name=(subject.get("tvrtka") or {}).get("ime") or ctx.legal_name or "",
        summary=f"HR-MBS {local_id}",
        identifiers={
            "hr_mbs": local_id,
            **({"hr_oib": r["oib"]} if r.get("oib") else {}),
        },
        raw=subject,
    )


def _bh_abr_australia(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "abr_australia", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"AU-ABN {r.get('abn') or local_id}".strip(),
        identifiers={
            **({"au_abn": r["abn"]} if r.get("abn") else {}),
            **({"au_acn": r["acn"]} if r.get("acn") else {}),
        },
        raw=r,
    )


@dataclass(frozen=True)
class _RegistrySource:
    """Dispatch + hit-build spec for a derived-identifier registry adapter."""

    source_id: str
    derived_keys: tuple[str, ...]  # first present key wins (ABR: ACN over ABN)
    pass_legal_name: bool
    build: Any  # Callable[[dict, str, _LookupCtx], SourceHit]


_REGISTRY_SOURCES: list[_RegistrySource] = [
    _RegistrySource("companies_house", ("gb_coh",), False, _bh_companies_house),
    _RegistrySource("zefix", ("che_uid",), False, _bh_zefix),
    _RegistrySource("kvk", ("kvk_number",), True, _bh_kvk),
    _RegistrySource("inpi", ("siren",), False, _bh_inpi),
    _RegistrySource("bolagsverket", ("se_org_number",), True, _bh_bolagsverket),
    _RegistrySource("ariregister", ("ee_registry_code",), True, _bh_ariregister),
    _RegistrySource("brreg", ("no_orgnr",), True, _bh_brreg),
    _RegistrySource("cro", ("ie_crn",), True, _bh_cro),
    _RegistrySource("prh", ("fi_ytunnus",), True, _bh_prh),
    _RegistrySource("ur_latvia", ("lv_regcode",), True, _bh_ur_latvia),
    _RegistrySource("jar_lithuania", ("lt_code",), True, _bh_jar_lithuania),
    _RegistrySource("ares", ("cz_ico",), True, _bh_ares),
    _RegistrySource("krs_poland", ("pl_krs",), True, _bh_krs_poland),
    _RegistrySource("firmenbuch", ("at_fn",), True, _bh_firmenbuch),
    _RegistrySource("rpo_slovakia", ("sk_ico",), False, _bh_rpo_slovakia),
    _RegistrySource("rpvs_slovakia", ("sk_ico",), False, _bh_rpvs_slovakia),
    _RegistrySource("bce_belgium", ("be_enterprise_number",), True, _bh_bce_belgium),
    _RegistrySource("corporations_canada", ("ca_corp_id",), True, _bh_corporations_canada),
    _RegistrySource("cvr_denmark", ("dk_cvr",), True, _bh_cvr_denmark),
    _RegistrySource("sudreg_croatia", ("hr_mbs",), True, _bh_sudreg_croatia),
    _RegistrySource("abr_australia", ("au_acn", "au_abn"), True, _bh_abr_australia),
]

_REGISTRY_SOURCE_INDEX: dict[str, _RegistrySource] = {
    s.source_id: s for s in _REGISTRY_SOURCES
}


def _local_id_for(spec: _RegistrySource, derived: dict[str, str]) -> str | None:
    for key in spec.derived_keys:
        if key in derived:
            return derived[key]
    return None


# --- special hit builders ---------------------------------------------------


def _bh_opencorporates(r: dict, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "opencorporates", ctx.ocid or "",
        name=c.get("name") or ctx.legal_name or "",
        summary=f"OC {ctx.ocid} · {c.get('current_status', '')}",
        identifiers={
            "ocid": ctx.ocid or "",
            "lei": ctx.lei,
            **({"gb_coh": ctx.derived["gb_coh"]} if "gb_coh" in ctx.derived else {}),
        },
        raw=c,
    )


def _extract_edgar_cik(oc_company: dict[str, Any]) -> str | None:
    """Pull a SEC EDGAR CIK out of an OpenCorporates company payload."""
    data = oc_company.get("data") or {}
    for entry in (data.get("most_recent") or []):
        datum = (entry.get("datum") or {}) if isinstance(entry, dict) else {}
        if datum.get("title") == "SEC Edgar entry" and datum.get("description"):
            desc: str = datum["description"]
            if "register id:" in desc:
                raw_cik = desc.split("register id:")[-1].strip()
                if raw_cik.isdigit():
                    return raw_cik.lstrip("0") or "0"
            break
    return None


def _bh_wikidata(r: dict, ctx: _LookupCtx) -> SourceHit:
    s = r.get("summary") or {}
    return _hit(
        "wikidata", ctx.qid or "",
        name=s.get("label") or ctx.qid or "",
        summary=s.get("description") or "",
        identifiers={
            "wikidata_qid": ctx.qid or "",
            "lei": ctx.lei,
            **({"gb_coh": ctx.registered_as} if "gb_coh" in ctx.derived else {}),
        },
        raw=s,
    )


def _bh_climatetrace(r: dict, ctx: _LookupCtx) -> SourceHit:
    entity_id = r.get("entity_id") or ctx.lei
    emissions = r.get("emissions") or {}
    total_co2e = emissions.get("total_co2e_tonnes")
    parts = [f"GEM entity {entity_id}"]
    if total_co2e is not None and total_co2e > 0:
        if total_co2e >= 1_000_000:
            parts.append(f"{total_co2e / 1_000_000:.1f} Mt CO₂e (2024)")
        else:
            parts.append(f"{total_co2e:,.0f} t CO₂e (2024)")
    return _hit(
        "climatetrace", entity_id,
        name=r.get("entity_name") or ctx.legal_name or entity_id,
        summary=" · ".join(parts),
        identifiers={"gem_entity_id": entity_id},
        raw=r, is_stub=bool(r.get("is_stub")),
    )


def _bh_bods_gleif(r: dict, ctx: _LookupCtx) -> SourceHit:
    statement_id = r.get("hit_id") or ctx.lei
    name = ctx.legal_name or ctx.lei
    for stmt in r.get("bods_statements", []):
        if stmt.get("statementType") == "entityStatement":
            name = stmt.get("recordDetails", {}).get("name") or name
            break
    return _hit(
        "bods_gleif", statement_id,
        name=name,
        summary="Open Ownership BODS v0.4 (bulk) · LEI match",
        identifiers={"lei": ctx.lei, "bods_gleif_statementid": statement_id},
        raw=r,
    )


def _edgar_hit(cik: str, legal_name: str) -> SourceHit:
    return _hit(
        "sec_edgar", cik,
        name=legal_name or "",
        summary=f"CIK {cik} · US listed company",
        identifiers={"edgar_cik": cik},
        raw={"cik": cik, "name": legal_name or ""},
    )


def _build_gleif_hit(ctx: _LookupCtx, gleif_bundle: dict[str, Any]) -> SourceHit:
    # wikidata_qid (and ocid / edgar_cik) are intentionally omitted from the
    # GLEIF hit identifiers: they are sourced from Wikidata / OpenCorporates,
    # not GLEIF. Including them would make the reconciler show "gleif" as a
    # confirmer of identifiers it does not actually publish.
    identifiers = {"lei": ctx.lei}
    for key, value in ctx.derived.items():
        if key not in ("lei", "ocid", "wikidata_qid", "edgar_cik"):
            identifiers[key] = value
    return _hit(
        "gleif", ctx.lei,
        name=ctx.legal_name or f"LEI {ctx.lei}",
        summary=f"LEI {ctx.lei} · {ctx.jurisdiction}",
        identifiers=identifiers,
        raw={
            **(gleif_bundle.get("record") or {}),
            # Children metadata — read by the frontend to display
            # "Showing X of N direct subsidiaries (GLEIF)".
            "direct_children_total": gleif_bundle.get("direct_children_total", 0),
            "direct_children_fetched": len(gleif_bundle.get("direct_children") or []),
        },
    )


async def _openaleph_strategies(ctx: _LookupCtx) -> list[SourceHit]:
    """OpenAleph cascade: LEI → OC URL → registration numbers → name."""
    oa_adapter = REGISTRY.get("openaleph")
    if oa_adapter is None:
        return []
    oa: list[SourceHit] = await oa_adapter.fetch_by_lei(ctx.lei)  # type: ignore[attr-defined]
    if not oa and "ocid" in ctx.derived:
        oa = await oa_adapter.fetch_by_oc_url(ctx.derived["ocid"])  # type: ignore[attr-defined]
    if not oa:
        for jur, reg in [
            ("gb", ctx.derived.get("gb_coh")),
            ("fr", ctx.derived.get("siren")),
            ("nl", ctx.derived.get("kvk_number")),
            ("se", ctx.derived.get("se_org_number")),
            ("ch", ctx.derived.get("che_uid")),
        ]:
            if reg:
                oa = await oa_adapter.fetch_by_registration(jur, reg)  # type: ignore[attr-defined]
                if oa:
                    break
    if not oa and ctx.legal_name:
        oa = await oa_adapter.fetch_by_name(ctx.legal_name)  # type: ignore[attr-defined]
    # OpenAleph can index the same entity under multiple collection aliases,
    # causing duplicate hit_ids — deduplicate before returning.
    seen: set[str] = set()
    deduped: list[SourceHit] = []
    for h in oa:
        if h.hit_id not in seen:
            seen.add(h.hit_id)
            deduped.append(h)
    return deduped


def _dispatch(ctx: _LookupCtx) -> list[tuple[str, Any]]:
    """Build the (source_id, awaitable) dispatch list for this lookup."""
    tasks: list[tuple[str, Any]] = []
    for spec in _REGISTRY_SOURCES:
        local_id = _local_id_for(spec, ctx.derived)
        if not local_id:
            continue
        adapter = REGISTRY[spec.source_id]
        if spec.pass_legal_name:
            tasks.append((spec.source_id, adapter.fetch(local_id, legal_name=ctx.legal_name)))
        else:
            tasks.append((spec.source_id, adapter.fetch(local_id)))
    if ctx.ocid:
        tasks.append(("opencorporates", REGISTRY["opencorporates"].fetch(ctx.ocid)))
    if ctx.qid:
        tasks.append(("wikidata", REGISTRY["wikidata"].fetch(ctx.qid)))
    os_adapter = REGISTRY.get("opensanctions")
    if os_adapter and SearchKind.ENTITY in os_adapter.info.supports:
        tasks.append(("opensanctions", os_adapter.search(ctx.lei, SearchKind.ENTITY)))
    if REGISTRY.get("openaleph") is not None:
        tasks.append(("openaleph", _openaleph_strategies(ctx)))
    ct_adapter = REGISTRY.get("climatetrace")
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei"):
        tasks.append(("climatetrace", ct_adapter.fetch_by_lei(ctx.lei)))
    bg_adapter = REGISTRY.get("bods_gleif")
    if bg_adapter is not None and hasattr(bg_adapter, "fetch_by_lei"):
        tasks.append(("bods_gleif", bg_adapter.fetch_by_lei(ctx.lei)))
    return tasks


def _build_result_hit(source_id: str, result: Any, ctx: _LookupCtx) -> SourceHit | None:
    """Convert one adapter result to a SourceHit (None → no hit)."""
    if not isinstance(result, dict) or not result:
        return None
    if source_id == "climatetrace":
        # Climate TRACE stubs still carry GEM CSV data worth showing.
        return _bh_climatetrace(result, ctx) if result.get("entity_id") else None
    if result.get("is_stub"):
        return None
    if source_id == "opencorporates":
        return _bh_opencorporates(result, ctx) if ctx.ocid else None
    if source_id == "wikidata":
        return _bh_wikidata(result, ctx) if ctx.qid else None
    if source_id == "bods_gleif":
        return _bh_bods_gleif(result, ctx)
    spec = _REGISTRY_SOURCE_INDEX.get(source_id)
    if spec is None:
        return None
    local_id = _local_id_for(spec, ctx.derived)
    if not local_id:
        return None
    return spec.build(result, local_id, ctx)


_STRUCTURAL_SIGNAL_CODES = {
    "TRUST_OR_ARRANGEMENT",
    "NON_EU_JURISDICTION",
    "NOMINEE",
    "COMPLEX_OWNERSHIP_LAYERS",
    "COMPLEX_CORPORATE_STRUCTURE",
    "POSSIBLE_OBFUSCATION",
}
_STATEMENT_SCOPED_SIGNAL_CODES = {"RELATED_PEP", "RELATED_SANCTIONED"}


def _merge_signals(*signal_lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate risk signals: structural codes collapse globally,
    statement-scoped codes key on the subject statement, the rest on
    (code, source, hit)."""
    merged: dict[tuple, dict[str, Any]] = {}
    for signals in signal_lists:
        for sig in signals:
            if sig["code"] in _STRUCTURAL_SIGNAL_CODES:
                key: tuple = (sig["code"],)
            elif sig["code"] in _STATEMENT_SCOPED_SIGNAL_CODES or (
                sig["code"] == "OFFSHORE_LEAKS" and sig.get("source_id") == "icij"
            ):
                key = (
                    sig["code"],
                    sig["source_id"],
                    sig["hit_id"],
                    sig.get("evidence", {}).get("subject_statement_id", ""),
                )
            else:
                key = (sig["code"], sig["source_id"], sig["hit_id"])
            merged[key] = sig
    return list(merged.values())


# --- the pipeline -----------------------------------------------------------


async def _lookup_pipeline(
    lei: str, deepen_top: int = 5
) -> AsyncIterator[LookupEvent]:
    """Single source of truth for the LEI-anchored lookup.

    Yields ``(event, payload)`` tuples. Events mirror the SSE vocabulary
    (source_started, gleif_done, hit, source_completed, source_error,
    sources_applicable, cross_source_links, bods_counts, risk_signals,
    done, error) plus two internal events consumed only by the sync
    collector: deepen_result and deepen_error. ``hit`` payloads are
    SourceHit objects; everything else is JSON-serialisable dicts.
    """
    lei = lei.strip().upper()
    if not _LEI_SHAPE.match(lei):
        yield ("error", {
            "status": 400,
            "detail": (
                f"{lei!r} is not a valid LEI. ISO 17442 LEIs are "
                "20-character alphanumeric strings (e.g. "
                "213800LH1BZH3DI6G760)."
            ),
        })
        return

    gleif = REGISTRY["gleif"]
    yield ("source_started", {"source_id": "gleif", "source_name": gleif.info.name})

    ctx = _LookupCtx(lei=lei)
    registered_at_id = ""
    gleif_bundle: dict[str, Any] = {}
    override_bundle = bods_data.gleif_bundle_for_lei(lei)
    try:
        if override_bundle:
            ctx.legal_name, ctx.jurisdiction, ctx.registered_as = (
                _subject_metadata_from_bundle(override_bundle, lei)
            )
            if not ctx.legal_name:
                yield ("error", {
                    "status": 404,
                    "detail": (
                        f"Found a BODS bundle for {lei} but couldn't locate "
                        "the subject entity statement. Re-run the extraction "
                        "script."
                    ),
                })
                return
            gleif_bundle = {"source_id": "gleif", "lei": lei, "_from_bundle": True}
        else:
            gleif_bundle = await gleif.fetch(lei)
            if gleif_bundle.get("is_stub") or not gleif_bundle.get("record"):
                yield ("error", {
                    "status": 404,
                    "detail": (
                        f"No GLEIF record found for {lei}. Either the LEI is "
                        "not registered, live mode is disabled, or no Open "
                        "Ownership bundle has been extracted for this LEI "
                        "(see backend/scripts/extract_bods_subgraphs.py)."
                    ),
                })
                return
            record_attrs = (gleif_bundle.get("record") or {}).get("attributes") or {}
            entity_block = record_attrs.get("entity") or {}
            ctx.legal_name = (entity_block.get("legalName") or {}).get("name") or ""
            ctx.jurisdiction = entity_block.get("jurisdiction") or ""
            ctx.registered_as = entity_block.get("registeredAs") or ""
            registered_at_id = (entity_block.get("registeredAt") or {}).get("id") or ""
    except Exception as exc:  # noqa: BLE001
        yield ("error", {
            "status": 502,
            "detail": f"GLEIF fetch failed: {type(exc).__name__}: {exc}",
        })
        return

    _build_derived(ctx, registered_at_id)

    # OpenCorporates ID from the GLEIF Level-1 record.
    if gleif.info.live_available:
        try:
            gleif_src = (
                gleif_bundle
                if not gleif_bundle.get("_from_bundle")
                else await gleif.fetch(lei)
            )
            if not gleif_src.get("is_stub"):
                attrs = (gleif_src.get("record") or {}).get("attributes") or {}
                ctx.ocid = attrs.get("ocid") or None
        except Exception:  # noqa: BLE001
            pass
    if ctx.ocid:
        ctx.derived["ocid"] = ctx.ocid

    wikidata_adapter = REGISTRY["wikidata"]
    if hasattr(wikidata_adapter, "find_qid_by_lei"):
        ctx.qid = await wikidata_adapter.find_qid_by_lei(lei)  # type: ignore[attr-defined]
    if ctx.qid:
        ctx.derived["wikidata_qid"] = ctx.qid

    yield ("gleif_done", {
        "lei": lei,
        "legal_name": ctx.legal_name or None,
        "jurisdiction": ctx.jurisdiction or None,
        "derived_identifiers": ctx.derived,
    })

    gleif_hit = _build_gleif_hit(ctx, gleif_bundle)
    hits: list[SourceHit] = [gleif_hit]
    deepened_bundles: list[tuple[str, str]] = [("gleif", lei)]
    yield ("hit", gleif_hit)
    yield ("source_completed", {"source_id": "gleif", "hit_count": 1})

    dispatch = _dispatch(ctx)
    se_adapter = REGISTRY.get("sec_edgar")
    sec_applicable = bool(
        ctx.jurisdiction.upper().startswith("US")
        and (ctx.derived.get("edgar_cik") or ctx.legal_name)
        and se_adapter
        and se_adapter.info.live_available
    )
    applicable_ids = [sid for sid, _ in dispatch] + (
        ["sec_edgar"] if sec_applicable else []
    )
    yield ("sources_applicable", {"source_ids": applicable_ids})
    for sid in applicable_ids:
        if sid == "sec_edgar":
            continue  # announced only once a CIK has actually been resolved
        src_name = REGISTRY[sid].info.name if sid in REGISTRY else sid
        yield ("source_started", {"source_id": sid, "source_name": src_name})

    async def _run(src_id: str, coro: Any) -> tuple[str, Any]:
        try:
            return src_id, await coro
        except Exception as exc:  # noqa: BLE001
            return src_id, exc

    errors: dict[str, str] = {}
    oc_result_processed = False
    pending = {asyncio.create_task(_run(sid, coro)) for sid, coro in dispatch}
    while pending:
        done_set, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        for task in done_set:
            source_id, result = task.result()

            if isinstance(result, Exception):
                errors[source_id] = _fmt_source_error(result)
                yield ("source_error", {
                    "source_id": source_id,
                    "error": errors[source_id],
                    "error_type": (
                        "schema_changed"
                        if isinstance(result, SourceSchemaError)
                        else "fetch_error"
                    ),
                })
                continue

            # List-result sources (search-style adapters).
            if source_id in ("opensanctions", "openaleph"):
                list_hits = (
                    [h for h in result if not h.is_stub]
                    if isinstance(result, list)
                    else []
                )
                for sh in list_hits:
                    hits.append(sh)
                    deepened_bundles.append((source_id, sh.hit_id))
                    yield ("hit", sh)
                yield ("source_completed", {
                    "source_id": source_id, "hit_count": len(list_hits),
                })
                continue

            hit = _build_result_hit(source_id, result, ctx)
            if hit is not None:
                hits.append(hit)
                deepened_bundles.append((source_id, hit.hit_id))
                yield ("hit", hit)
                yield ("source_completed", {"source_id": source_id, "hit_count": 1})
            else:
                yield ("source_completed", {"source_id": source_id, "hit_count": 0})

            # OpenCorporates may reveal a SEC EDGAR CIK — surface immediately.
            if (
                source_id == "opencorporates"
                and hit is not None
                and not oc_result_processed
            ):
                cik = _extract_edgar_cik(result.get("company") or {})
                if cik:
                    ctx.derived["edgar_cik"] = cik
                if ctx.jurisdiction.upper().startswith("US"):
                    oc_result_processed = True
                    if cik and se_adapter and se_adapter.info.live_available:
                        edgar_hit = _edgar_hit(cik, ctx.legal_name)
                        hits.append(edgar_hit)
                        deepened_bundles.append(("sec_edgar", cik))
                        yield ("source_started", {
                            "source_id": "sec_edgar",
                            "source_name": se_adapter.info.name,
                        })
                        yield ("hit", edgar_hit)
                        yield ("source_completed", {
                            "source_id": "sec_edgar", "hit_count": 1,
                        })

    # SEC EDGAR fallback: resolve the CIK from the legal name.
    if (
        ctx.jurisdiction.upper().startswith("US")
        and not ctx.derived.get("edgar_cik")
        and ctx.legal_name
        and se_adapter
        and se_adapter.info.live_available
    ):
        try:
            cik2 = await se_adapter.resolve_cik(ctx.legal_name)  # type: ignore[attr-defined]
            if cik2:
                edgar_hit = _edgar_hit(cik2, ctx.legal_name)
                hits.append(edgar_hit)
                deepened_bundles.append(("sec_edgar", cik2))
                yield ("source_started", {
                    "source_id": "sec_edgar",
                    "source_name": se_adapter.info.name,
                })
                yield ("hit", edgar_hit)
                yield ("source_completed", {"source_id": "sec_edgar", "hit_count": 1})
        except Exception as exc:  # noqa: BLE001
            errors["sec_edgar"] = _fmt_source_error(exc)
            yield ("source_error", {
                "source_id": "sec_edgar",
                "error": errors["sec_edgar"],
                "error_type": (
                    "schema_changed"
                    if isinstance(exc, SourceSchemaError)
                    else "fetch_error"
                ),
            })

    # Reconcile + search-time risk.
    links = [link.to_dict() for link in reconcile(hits)]
    search_signals = [s.to_dict() for s in assess_hits(hits)]
    yield ("cross_source_links", {"links": links})

    # Deepen the top N bundles (BODS mapping + per-bundle risk).
    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    deepen_signals: list[dict[str, Any]] = []
    license_notices: list[dict[str, str]] = []
    bods_counts: dict[str, int] = {}

    deepen_pairs = deepened_bundles[:deepen_top]
    deepen_raw = await asyncio.gather(
        *[_safe_deepen(dsrc, dhit) for dsrc, dhit in deepen_pairs],
        return_exceptions=True,
    )
    for (dsrc, dhit), deep in zip(deepen_pairs, deepen_raw):
        if isinstance(deep, Exception):
            yield ("deepen_error", {
                "source_id": dsrc,
                "error": f"{type(deep).__name__}: {deep}",
            })
            continue
        if deep is None:
            continue
        bods_all.extend(deep["bods"])
        bods_issues.extend(deep["bods_issues"])
        deepen_signals.extend(deep["risk_signals"])
        if deep.get("license_notice"):
            license_notices.append({
                "source_id": dsrc, "hit_id": dhit, "notice": deep["license_notice"],
            })
        bods_counts[f"{dsrc}:{dhit}"] = len(deep["bods"])
        yield ("deepen_result", {
            "source_id": dsrc, "hit_id": dhit, "bods": deep["bods"],
        })

    yield ("bods_counts", {"counts": bods_counts})

    cross_raw, icij_raw = await asyncio.gather(
        assess_cross_source_names(bods_all),
        assess_icij_names(bods_all),
    )
    merged = _merge_signals(
        search_signals,
        deepen_signals,
        [s.to_dict() for s in cross_raw],
        [s.to_dict() for s in icij_raw],
    )
    yield ("risk_signals", {"signals": merged})

    yield ("done", {
        "lei": lei,
        "bods_issues": bods_issues,
        "license_notices": license_notices,
    })


# --- endpoints ---------------------------------------------------------------


@router.get("/lookup", response_model=LookupResponse)
async def lookup(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
) -> LookupResponse:
    """Driver endpoint: LEI in, full cross-source synthesis out.

    Collects the events of :func:`_lookup_pipeline` into one response —
    identical data to /lookup-stream, without the streaming.
    """
    norm_lei = lei.strip().upper()
    hits: list[SourceHit] = []
    errors: dict[str, str] = {}
    links: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    bods_all: list[dict[str, Any]] = []
    bods_issues: list[str] = []
    license_notices: list[dict[str, str]] = []
    legal_name: str | None = None
    jurisdiction: str | None = None
    derived: dict[str, str] = {}

    async for event, payload in _lookup_pipeline(norm_lei, deepen_top=deepen_top):
        if event == "error":
            raise HTTPException(
                status_code=payload["status"], detail=payload["detail"]
            )
        elif event == "gleif_done":
            legal_name = payload["legal_name"]
            jurisdiction = payload["jurisdiction"]
            derived = payload["derived_identifiers"]
        elif event == "hit":
            hits.append(payload)
        elif event == "source_error":
            errors[payload["source_id"]] = payload["error"]
        elif event == "deepen_error":
            errors.setdefault(payload["source_id"], payload["error"])
        elif event == "deepen_result":
            bods_all.extend(payload["bods"])
        elif event == "cross_source_links":
            links = payload["links"]
        elif event == "risk_signals":
            signals = payload["signals"]
        elif event == "done":
            bods_issues = payload["bods_issues"]
            license_notices = payload["license_notices"]

    return LookupResponse(
        query=norm_lei,
        kind=SearchKind.ENTITY,
        hits=hits,
        errors=errors,
        cross_source_links=links,
        risk_signals=signals,
        bods=bods_all,
        bods_issues=bods_issues,
        license_notices=license_notices,
        lei=norm_lei,
        legal_name=legal_name,
        jurisdiction=jurisdiction,
        derived_identifiers=derived,
    )


@router.get("/lookup-stream")
async def lookup_stream(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
) -> EventSourceResponse:
    """LEI-anchored lookup streamed as SSE — same pipeline as /lookup."""
    return EventSourceResponse(_lookup_sse_events(lei, deepen_top=deepen_top))


async def _lookup_sse_events(
    lei: str, deepen_top: int = 5
) -> AsyncIterator[dict[str, Any]]:
    """Serialise pipeline events as SSE frames."""
    async for event, payload in _lookup_pipeline(lei, deepen_top=deepen_top):
        if event in ("deepen_result", "deepen_error"):
            continue  # internal events for the sync collector only
        if event == "hit":
            yield {"event": "hit", "data": payload.model_dump_json()}
        else:
            yield {"event": event, "data": json.dumps(payload)}


def _subject_metadata_from_bundle(
    bundle: list[dict[str, Any]], lei: str
) -> tuple[str, str, str]:
    """Extract ``(legal_name, jurisdiction_code, registered_as)`` from the entity statement."""
    target = lei.strip().upper()
    for stmt in bundle:
        if (stmt.get("recordType") or "") != "entity":
            continue
        rd = stmt.get("recordDetails") or {}
        ids = rd.get("identifiers") or []
        has_lei = any(
            (i.get("scheme") == "XI-LEI" and (i.get("id") or "").upper() == target)
            for i in ids
            if isinstance(i, dict)
        )
        if not has_lei:
            continue
        legal_name = rd.get("name") or ""
        # v0.4 field is "jurisdiction"; OO bulk BODS pass-through still uses
        # the legacy "incorporatedInJurisdiction" — accept both.
        jur = rd.get("jurisdiction") or rd.get("incorporatedInJurisdiction") or {}
        jurisdiction = (jur.get("code") or "").upper() if isinstance(jur, dict) else ""
        registered_as = ""
        for i in ids:
            if not isinstance(i, dict):
                continue
            scheme = (i.get("scheme") or "").upper()
            if scheme == "GB-COH":
                registered_as = i.get("id") or ""
                break
        return legal_name, jurisdiction, registered_as
    return "", "", ""


def _bods_data_override(source_id: str, hit_id: str) -> list[dict[str, Any]] | None:
    """Return the Open Ownership canonical BODS bundle for this (source_id, hit_id) pair."""
    if source_id == "gleif":
        return bods_data.gleif_bundle_for_lei(hit_id)
    if source_id == "companies_house":
        if hit_id.isalnum() and len(hit_id) == 8:
            return bods_data.uk_bundle_for_company_number(hit_id)
    return None


async def _safe_deepen(source_id: str, hit_id: str) -> dict[str, Any] | None:
    """Internal helper — does what /deepen does, returns plain dict."""
    adapter = REGISTRY.get(source_id)
    if adapter is None:
        return None
    raw = await adapter.fetch(hit_id)

    override = _bods_data_override(source_id, hit_id)
    bods: list[dict[str, Any]] = []
    issues: list[str] = []
    if override is not None:
        bods = override
        issues = validate_shape(bods)
    else:
        mapper = _MAPPERS.get(source_id)
        if mapper and not raw.get("is_stub"):
            bundle: BODSBundle = mapper(raw)
            bods = list(bundle)
            issues = validate_shape(bods)

    license_notice = _license_notice_for(adapter.info, raw)
    signals = [s.to_dict() for s in assess_bundle(source_id, raw, bods, hit_id=hit_id)]
    return {
        "raw": raw,
        "bods": bods,
        "bods_issues": issues,
        "license_notice": license_notice,
        "risk_signals": signals,
    }


def _license_notice_for(
    info: SourceInfo, raw: dict[str, Any]
) -> str | None:
    """Return a human-readable warning when the payload is NC-licensed."""
    if info.license in _NC_LICENSES:
        return (
            f"{info.name} is licensed under {info.license}. Commercial "
            "re-use of this data is not permitted under the source license."
        )
    if info.id == "openaleph":
        collection = raw.get("collection") or {}
        license_ = (
            collection.get("license")
            or (collection.get("data") or {}).get("license")
            or ""
        ).upper().replace(" ", "-")
        if license_ and any(nc in license_ for nc in ("NC", "NON-COMMERCIAL")):
            label = collection.get("label") or collection.get("foreign_id") or "collection"
            return (
                f"OpenAleph collection '{label}' is licensed under "
                f"{collection.get('license') or license_}. Commercial re-use "
                "is not permitted under the source license."
            )
    return None
