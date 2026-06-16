"""Lookup endpoints — /lookup, /lookup-stream, /deepen, /report."""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field as dc_field
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from .. import __version__
from .. import bods as _bods
from ..bods import BODSBundle, validate_shape
from ..sources.base import LookupDeriver, raw_redaction_notice
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


def _mapper_for(source_id: str) -> Any | None:
    """BODS mapper for a source, by convention: ``opencheck.bods.map_<id>``.

    Adding ``map_<name>()`` to bods/mapper.py (exported via bods/__init__)
    is all it takes to wire a mapper — there is no hand-maintained dict.
    """
    return getattr(_bods, f"map_{source_id}", None)

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
        mapper = _mapper_for(source)
        if mapper and not raw.get("is_stub"):
            bundle: BODSBundle = mapper(raw)
            bods = list(bundle)
            issues = validate_shape(bods)

    info = adapter.info
    license_notice = _license_notice_for(info, raw)
    signals = [s.to_dict() for s in assess_bundle(source, raw, bods, hit_id=hit_id)]

    # Sources whose licence forbids raw re-publication (OpenCorporates) return a
    # redaction notice in place of the raw bundle; the BODS output is unaffected.
    response_raw = raw if adapter.republish_raw else raw_redaction_notice(source)

    return DeepenResponse(
        source_id=source,
        hit_id=hit_id,
        raw=response_raw,
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
# Adapters are self-describing: each national-register adapter declares its
# RA-code derivers (``lookup_derivers``), dispatch keys and legal-name flag
# on its class (see sources/base.py). The deriver table and dispatch specs
# below are built from the REGISTRY at import time, so wiring a new adapter
# into the lookup flow means declaring the spec on the adapter class and
# adding a ``_bh_<id>()`` hit builder here — nothing else.

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


# RA-code derivers declared by the adapters themselves, collected from the
# registry. GB is special-cased on jurisdiction in _build_derived() because
# UK records reliably carry registeredAs. Normalisers may raise ValueError
# for malformed local IDs — the source is then skipped.
_RA_DERIVERS: list[LookupDeriver] = [
    deriver
    for adapter in REGISTRY.values()
    for deriver in adapter.lookup_derivers
]
# NOTE: ACRA Singapore (RA000523) adapter is implemented but not wired into
# lookup dispatch. The data.gov.sg dataset is bulk CSV only (no live API),
# which doesn't fit the fast-API pattern used by the other national registers.
# To enable: declare lookup_derivers on AcraSingaporeAdapter, add a
# _bh_acra_singapore() builder, and build the DB with scripts/extract_acra.py.


def _build_derived(ctx: _LookupCtx, registered_at_id: str) -> None:
    """Populate ctx.derived from the GLEIF anchor record."""
    ctx.derived["lei"] = ctx.lei
    if ctx.jurisdiction.upper() == "GB" and ctx.registered_as:
        ctx.derived["gb_coh"] = ctx.registered_as
    if ctx.registered_as and registered_at_id:
        for deriver in _RA_DERIVERS:
            if registered_at_id in deriver.ra_codes:
                try:
                    ctx.derived[deriver.derived_key] = deriver.normalise(
                        ctx.registered_as
                    )
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


def _bh_cnpj_brazil(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "cnpj_brazil", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"BR-CNPJ {local_id}",
        identifiers={"br_cnpj": local_id}, raw=c,
    )


def _bh_malta_mbr(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "malta_mbr", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"MT-MBR {local_id}",
        identifiers={"mt_crn": local_id}, raw=c,
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


def _collect_registry_sources() -> list[_RegistrySource]:
    """Build dispatch specs from the adapters' own lookup declarations.

    Any adapter that declares lookup keys (via ``lookup_derivers`` or
    ``lookup_dispatch_keys``) MUST have a matching ``_bh_<id>()`` hit
    builder in this module — enforced here at import time so a missing
    builder fails the whole test suite, not one lookup at runtime.
    """
    specs: list[_RegistrySource] = []
    for source_id, adapter in REGISTRY.items():
        keys = adapter.lookup_keys()
        if not keys:
            continue
        builder = globals().get(f"_bh_{source_id}")
        if builder is None:
            raise RuntimeError(
                f"adapter {source_id!r} declares lookup keys {keys} but "
                f"routers/lookup.py has no _bh_{source_id}() hit builder"
            )
        specs.append(
            _RegistrySource(source_id, keys, adapter.lookup_pass_legal_name, builder)
        )
    return specs


_REGISTRY_SOURCES: list[_RegistrySource] = _collect_registry_sources()

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


def _dispatch(ctx: _LookupCtx, only: str | None = None) -> list[tuple[str, Any]]:
    """Build the (source_id, awaitable) dispatch list for this lookup.

    ``only`` restricts dispatch to a single source — used by the
    /lookup-source per-source retry endpoint.
    """
    tasks: list[tuple[str, Any]] = []

    def _want(source_id: str) -> bool:
        return only is None or source_id == only

    for spec in _REGISTRY_SOURCES:
        if not _want(spec.source_id):
            continue
        local_id = _local_id_for(spec, ctx.derived)
        if not local_id:
            continue
        adapter = REGISTRY[spec.source_id]
        if spec.pass_legal_name:
            tasks.append((spec.source_id, adapter.fetch(local_id, legal_name=ctx.legal_name)))
        else:
            tasks.append((spec.source_id, adapter.fetch(local_id)))
    if ctx.ocid and _want("opencorporates"):
        tasks.append(("opencorporates", REGISTRY["opencorporates"].fetch(ctx.ocid)))
    if ctx.qid and _want("wikidata"):
        tasks.append(("wikidata", REGISTRY["wikidata"].fetch(ctx.qid)))
    os_adapter = REGISTRY.get("opensanctions")
    if os_adapter and SearchKind.ENTITY in os_adapter.info.supports and _want("opensanctions"):
        tasks.append(("opensanctions", os_adapter.search(ctx.lei, SearchKind.ENTITY)))
    if REGISTRY.get("openaleph") is not None and _want("openaleph"):
        tasks.append(("openaleph", _openaleph_strategies(ctx)))
    ct_adapter = REGISTRY.get("climatetrace")
    if ct_adapter is not None and hasattr(ct_adapter, "fetch_by_lei") and _want("climatetrace"):
        tasks.append(("climatetrace", ct_adapter.fetch_by_lei(ctx.lei)))
    bg_adapter = REGISTRY.get("bods_gleif")
    if bg_adapter is not None and hasattr(bg_adapter, "fetch_by_lei") and _want("bods_gleif"):
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


def _source_budget(source_id: str) -> float:
    """Wall-clock budget for one source inside a lookup (adapter-declared)."""
    adapter = REGISTRY.get(source_id)
    return getattr(adapter, "lookup_timeout_s", 30.0) if adapter else 30.0


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


# --- anchor resolution --------------------------------------------------------


class _LookupAbort(Exception):
    """Fatal lookup failure: HTTP status for /lookup, error event for SSE."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


async def _resolve_ctx(lei: str) -> tuple[_LookupCtx, dict[str, Any]]:
    """Resolve the GLEIF anchor and build the lookup context.

    Returns ``(ctx, gleif_bundle)`` with derived identifiers, OpenCorporates
    ID and Wikidata QID populated. Raises :class:`_LookupAbort` when the LEI
    cannot be resolved. Shared by the pipeline and /lookup-source.
    """
    gleif = REGISTRY["gleif"]
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
                raise _LookupAbort(
                    404,
                    (
                        f"Found a BODS bundle for {lei} but couldn't locate "
                        "the subject entity statement. Re-run the extraction "
                        "script."
                    ),
                )
            gleif_bundle = {"source_id": "gleif", "lei": lei, "_from_bundle": True}
        else:
            gleif_bundle = await gleif.fetch(lei)
            if gleif_bundle.get("is_stub") or not gleif_bundle.get("record"):
                raise _LookupAbort(
                    404,
                    (
                        f"No GLEIF record found for {lei}. Either the LEI is "
                        "not registered, live mode is disabled, or no Open "
                        "Ownership bundle has been extracted for this LEI "
                        "(see backend/scripts/extract_bods_subgraphs.py)."
                    ),
                )
            record_attrs = (gleif_bundle.get("record") or {}).get("attributes") or {}
            entity_block = record_attrs.get("entity") or {}
            ctx.legal_name = (entity_block.get("legalName") or {}).get("name") or ""
            ctx.jurisdiction = entity_block.get("jurisdiction") or ""
            ctx.registered_as = entity_block.get("registeredAs") or ""
            registered_at_id = (entity_block.get("registeredAt") or {}).get("id") or ""
    except _LookupAbort:
        raise
    except Exception as exc:  # noqa: BLE001
        raise _LookupAbort(
            502, f"GLEIF fetch failed: {type(exc).__name__}: {exc}"
        ) from exc

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

    return ctx, gleif_bundle


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

    try:
        ctx, gleif_bundle = await _resolve_ctx(lei)
    except _LookupAbort as abort:
        yield ("error", {"status": abort.status, "detail": abort.detail})
        return

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
        budget = _source_budget(src_id)
        try:
            return src_id, await asyncio.wait_for(coro, timeout=budget)
        except asyncio.TimeoutError:
            return src_id, TimeoutError(
                f"source exceeded its {budget:.0f}s time budget"
            )
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
                if isinstance(result, SourceSchemaError):
                    _err_type = "schema_changed"
                elif isinstance(result, TimeoutError):
                    _err_type = "timeout"
                else:
                    _err_type = "fetch_error"
                yield ("source_error", {
                    "source_id": source_id,
                    "error": errors[source_id],
                    "error_type": _err_type,
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
            cik2 = await asyncio.wait_for(
                se_adapter.resolve_cik(ctx.legal_name),  # type: ignore[attr-defined]
                timeout=_source_budget("sec_edgar"),
            )
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
        *[
            # Deepen usually replays the adapter's cached fetch, but give it
            # the same wall-clock protection as dispatch (+ mapping headroom).
            asyncio.wait_for(
                _safe_deepen(dsrc, dhit), timeout=_source_budget(dsrc) + 15.0
            )
            for dsrc, dhit in deepen_pairs
        ],
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


# --- replay cache --------------------------------------------------------------
#
# Completed lookup runs are kept in memory for a short window so a page
# refresh, a shared URL, or an SSE reconnect replays instantly instead of
# re-querying every source. Only runs that reached the "done" event are
# cached; per-source retries and ?refresh=true invalidate/bypass.

_REPLAY_TTL_SECONDS = 15 * 60.0
_REPLAY_MAX_ENTRIES = 64
_REPLAY_CACHE: dict[str, tuple[float, list[LookupEvent]]] = {}


def _invalidate_replay(lei: str) -> None:
    prefix = f"{lei.strip().upper()}:"
    for key in [k for k in _REPLAY_CACHE if k.startswith(prefix)]:
        _REPLAY_CACHE.pop(key, None)


async def _lookup_pipeline_cached(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> AsyncIterator[LookupEvent]:
    """Replay a cached completed run, or run the pipeline and cache it."""
    key = f"{lei.strip().upper()}:{deepen_top}"
    now = time.monotonic()

    if not refresh:
        entry = _REPLAY_CACHE.get(key)
        if entry is not None and now - entry[0] < _REPLAY_TTL_SECONDS:
            for event in entry[1]:
                yield event
            return

    buffer: list[LookupEvent] = []
    completed = False
    async for event in _lookup_pipeline(lei, deepen_top=deepen_top):
        buffer.append(event)
        if event[0] == "done":
            completed = True
        yield event

    if completed:
        while len(_REPLAY_CACHE) >= _REPLAY_MAX_ENTRIES:
            _REPLAY_CACHE.pop(next(iter(_REPLAY_CACHE)), None)
        _REPLAY_CACHE[key] = (now, buffer)


# --- endpoints ---------------------------------------------------------------


@router.get("/lookup", response_model=LookupResponse)
async def lookup(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
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

    async for event, payload in _lookup_pipeline_cached(
        norm_lei, deepen_top=deepen_top, refresh=refresh
    ):
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
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> EventSourceResponse:
    """LEI-anchored lookup streamed as SSE — same pipeline as /lookup."""
    return EventSourceResponse(
        _lookup_sse_events(lei, deepen_top=deepen_top, refresh=refresh)
    )


async def _lookup_sse_events(
    lei: str, deepen_top: int = 5, refresh: bool = False
) -> AsyncIterator[dict[str, Any]]:
    """Serialise pipeline events as SSE frames."""
    async for event, payload in _lookup_pipeline_cached(
        lei, deepen_top=deepen_top, refresh=refresh
    ):
        if event in ("deepen_result", "deepen_error"):
            continue  # internal events for the sync collector only
        if event == "hit":
            yield {"event": "hit", "data": payload.model_dump_json()}
        else:
            yield {"event": event, "data": json.dumps(payload)}


class LookupSourceResponse(BaseModel):
    """Result of re-running a single source within an existing lookup."""

    lei: str
    source_id: str
    hits: list[SourceHit]
    error: str | None = None


@router.get("/lookup-source", response_model=LookupSourceResponse)
async def lookup_source(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    source_id: str = Query(..., description="Adapter id to re-run, e.g. 'kvk'."),
) -> LookupSourceResponse:
    """Re-run one source for a LEI — powers the per-source retry button.

    Resolves the GLEIF anchor (cheap — adapter-cached), dispatches just the
    requested source, and invalidates the replay cache so the next full
    lookup reflects the fresh result.
    """
    norm_lei = lei.strip().upper()
    if not _LEI_SHAPE.match(norm_lei):
        raise HTTPException(
            status_code=400,
            detail=(
                f"{norm_lei!r} is not a valid LEI. ISO 17442 LEIs are "
                "20-character alphanumeric strings (e.g. "
                "213800LH1BZH3DI6G760)."
            ),
        )

    try:
        ctx, _gleif_bundle = await _resolve_ctx(norm_lei)
    except _LookupAbort as abort:
        raise HTTPException(status_code=abort.status, detail=abort.detail)

    tasks = _dispatch(ctx, only=source_id)
    if not tasks:
        raise HTTPException(
            status_code=404,
            detail=(
                f"source {source_id!r} is not applicable to {norm_lei} "
                "(no derived identifier for it on this LEI record)"
            ),
        )

    hits: list[SourceHit] = []
    error: str | None = None
    for sid, coro in tasks:
        try:
            result = await asyncio.wait_for(coro, timeout=_source_budget(sid))
        except asyncio.TimeoutError:
            error = (
                f"TimeoutError: source exceeded its "
                f"{_source_budget(sid):.0f}s time budget"
            )
            continue
        except Exception as exc:  # noqa: BLE001
            error = _fmt_source_error(exc)
            continue
        if sid in ("opensanctions", "openaleph"):
            if isinstance(result, list):
                hits.extend(h for h in result if not h.is_stub)
        else:
            hit = _build_result_hit(sid, result, ctx)
            if hit is not None:
                hits.append(hit)

    _invalidate_replay(norm_lei)
    return LookupSourceResponse(
        lei=norm_lei, source_id=source_id, hits=hits, error=error
    )


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
        mapper = _mapper_for(source_id)
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
