"""Turning one source's payload into one ``SourceHit``.

Split out of ``routers/lookup.py`` in Phase 168, unchanged. That file was
3,186 lines and every phase touches it; this is the layer the "add a source"
checklist in ``CLAUDE.md`` actually sends people to — *"routers/lookup.py —
``_bh_<name>()`` hit builder (only this)"* — so it is now a file where only
that is true.

The contract, all of it here: a builder takes ``(result, local_id, ctx)``
and returns a ``SourceHit`` built through ``_hit``; it is called for
non-stub dict results, and a stub or ``None`` yields no hit. ``_LookupCtx``
is the per-lookup context every builder reads (the GLEIF anchor, the derived
local identifiers, the legal name), and lives here because it is half of
that signature.

``lookup.py`` imports the lot back, so nothing that referred to these by
name has to change.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any

from ..findings import (
    finding_bods_gleif,
    finding_climatetrace,
    finding_companies_house,
    finding_opencorporates,
    finding_ted_eu,
    finding_wikidata,
)
from ..provenance import Provenance
from ..sources import REGISTRY, SearchKind, SourceHit


@dataclass
class _LookupCtx:
    """Mutable context threaded through one lookup run."""

    lei: str
    legal_name: str = ""
    jurisdiction: str = ""
    registered_as: str = ""
    derived: dict[str, str] = dc_field(default_factory=dict)
    ocid: str | None = None
    #: GLEIF-published S&P Global / Capital IQ id — corroborates MEIP's CapIQ id.
    spglobal: str | None = None
    qid: str | None = None
    #: Where the GLEIF anchor payload actually came from. The anchor is
    #: resolved *before* the dispatch loop that fills ``provenances``, so
    #: without carrying it here GLEIF is the one source with no entry in
    #: ``source_liveness`` — and the one row on the report with no freshness
    #: note, which reads as "we don't know" for the source everything else is
    #: anchored to.
    provenance: Provenance | None = None


def _hit(
    source_id: str,
    hit_id: str,
    *,
    name: str,
    summary: str,
    identifiers: dict[str, str],
    raw: dict[str, Any],
    is_stub: bool = False,
    finding: str | None = None,
) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=SearchKind.ENTITY,
        name=name,
        summary=summary,
        finding=finding,
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
        # The finding reads the whole bundle (PSCs, PSC statements, officers),
        # not just the profile that becomes ``raw``.
        finding=finding_companies_house(r),
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
    raw = dict(r.get("company") or {})
    note = r.get("coverage_note")
    if note and "coverage_note" not in raw:
        # Not in the KvK open-data set (BV/NV-only 404) — pass the note through
        # so the card explains the gap instead of looking empty/broken.
        raw["coverage_note"] = note
    return _hit(
        "kvk", local_id,
        name=ctx.legal_name or "",
        summary=f"KvK {local_id}",
        identifiers={"kvk_number": local_id}, raw=raw,
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


def _bh_nz_companies(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = dict(r.get("company") or {})
    identifiers = {"nz_company_number": local_id}
    if r.get("nzbn"):
        identifiers["nzbn"] = str(r["nzbn"])
    if r.get("link") and "link" not in c:
        # Surface the public NZBN entity page so the source card links out.
        c["link"] = r["link"]
    return _hit(
        "nz_companies", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"NZ-COH {local_id}",
        identifiers=identifiers, raw=c,
    )


def _bh_malta_mbr(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    c = r.get("company") or {}
    return _hit(
        "malta_mbr", local_id,
        name=(c.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"MT-MBR {local_id}",
        identifiers={"mt_crn": local_id}, raw=c,
    )


def _bh_gemi_greece(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    """Hit builder for the Greek General Commercial Registry (ΓΕΜΗ).

    Asserts only the two identifiers ΓΕΜΗ itself publishes — the Αριθμός ΓΕΜΗ
    and the ΑΦΜ — never the LEI the lookup arrived by (see the identifier
    corroboration rule in CLAUDE.md).
    """
    from ..findings import finding_gemi_greece
    from ..sources.gemi_greece import english_label

    c = r.get("company") or {}
    identifiers = {"gr_argemi": local_id}
    afm = str(c.get("afm") or "").strip()
    if afm:
        identifiers["gr_afm"] = afm

    status = english_label("companyStatuses", c.get("status"))
    summary = f"GR-GEMI {local_id}" + (f" · {status.lower()}" if status else "")

    return _hit(
        "gemi_greece", local_id,
        name=(c.get("coNameEl") or "").strip() or ctx.legal_name or "",
        summary=summary,
        identifiers=identifiers, raw=c,
        finding=finding_gemi_greece(r),
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


def _bh_mca_india(r: dict, local_id: str, ctx: _LookupCtx) -> SourceHit:
    return _hit(
        "mca_india", local_id,
        name=(r.get("name") or "").strip() or ctx.legal_name or "",
        summary=f"IN-CIN {r.get('cin') or local_id}".strip(),
        identifiers={"in_cin": r.get("cin") or local_id},
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
                f"routers/hit_builders.py has no _bh_{source_id}() hit builder"
            )
        specs.append(
            _RegistrySource(source_id, keys, adapter.lookup_pass_legal_name, builder)
        )
    return specs


_REGISTRY_SOURCES: list[_RegistrySource] = _collect_registry_sources()

_REGISTRY_SOURCE_INDEX: dict[str, _RegistrySource] = {
    s.source_id: s for s in _REGISTRY_SOURCES
}

# Official company registers emit officers / PSCs / beneficial owners into the
# entity bundle; OpenCorporates likewise contributes officer person statements.
# These "person-capable" sources are always deepened, even past the deepen_top
# cap — otherwise the connected-people list depends on a nondeterministic
# completion-order race (issue #73). The leak/sanctions list-search sources
# (OpenSanctions, OpenAleph, EveryPolitician) are deliberately NOT here: they
# have no registry hit builder, can be numerous/slow, and are not authoritative
# registers.
_PERSON_CAPABLE_SOURCES: frozenset[str] = frozenset(_REGISTRY_SOURCE_INDEX) | {
    "opencorporates",
    # Wikidata is not a company register but carries person identity data
    # (Q-ids, the identifier spine for people) and contributes person records
    # to the bundle, so it is kept in the always-deepen set too.
    "wikidata",
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
        finding=finding_opencorporates(r),
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
        finding=finding_wikidata(s),
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
        finding=finding_climatetrace(r),
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
        finding=finding_bods_gleif(r, statement_id),
        identifiers={"lei": ctx.lei, "bods_gleif_statementid": statement_id},
        raw=r,
    )


# Countries where the EITI identification format has been verified to match
# an OpenCheck derived-identifier key — lets the reconciler show legitimate
# cross-source corroboration (EITI independently publishes these numbers).
_EITI_IDENTIFIER_KEY_BY_COUNTRY = {
    "GB": "gb_coh",
    "NO": "no_orgnr",
    "NL": "kvk_number",
    # US EITI identifications are federal EINs; matching a US subject means an
    # EIN was derived for it, so the corroboration key is that EIN.
    "US": "us_ein",
}


def _bh_eiti(r: dict, ctx: _LookupCtx) -> SourceHit:
    country = r.get("country") or ""
    ident = r.get("identification") or ""
    years = r.get("years") or []
    total_usd = r.get("total_usd") or 0.0
    parts = [f"EITI {country}"]
    if years:
        parts.append(
            f"{len(years)} reporting year{'s' if len(years) != 1 else ''} "
            f"({years[-1]}–{years[0]})" if len(years) > 1 else f"reported {years[0]}"
        )
    if total_usd > 0:
        if total_usd >= 1_000_000:
            parts.append(f"${total_usd / 1_000_000:.1f}M USD to governments")
        else:
            parts.append(f"${total_usd:,.0f} USD to governments")
    ident_key = _EITI_IDENTIFIER_KEY_BY_COUNTRY.get(country, "eiti_identification")
    return _hit(
        "eiti", f"{country}:{ident}",
        name=r.get("entity_name") or ctx.legal_name or ident,
        summary=" · ".join(parts),
        identifiers={ident_key: ident},
        raw=r,
    )


def _bh_eiti_soe(r: dict, ctx: _LookupCtx) -> SourceHit:
    parts = ["State-owned enterprise"]
    if r.get("sector"):
        parts.append(str(r["sector"]))
    commodities = r.get("commodities") or []
    if commodities:
        parts.append(", ".join(commodities[:3]))
    if r.get("country"):
        parts.append(str(r["country"]))
    if (r.get("match_confidence") or "").lower() == "low":
        parts.append("possible name match")
    # Corroboration rule: the SOE database does NOT publish the LEI (OpenCheck
    # derives it at index-build time), so `lei` is intentionally omitted from
    # identifiers. Only the identifiers EITI itself publishes are asserted.
    # `eiti_soe_id` is informational (EITI's own id). `ocid` is intentionally
    # NOT asserted from the SOE database's opencorporates_id: like the Wikirate
    # precedent, its format may differ from OpenCheck's jurisdiction-scoped
    # `ocid`, and a mismatched assert would create a false corroboration.
    identifiers: dict[str, str] = {}
    if r.get("eiti_id_company"):
        identifiers["eiti_soe_id"] = str(r["eiti_id_company"])
    return _hit(
        "eiti_soe", ctx.lei,
        name=r.get("entity_name") or ctx.legal_name or ctx.lei,
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


def _bh_cac_nigeria(r: dict, ctx: _LookupCtx) -> SourceHit:
    record = r.get("record") or {}
    pscs = record.get("pscs") or []
    n = len(pscs)
    # Count of PSC declaration rows in the register, not distinct owners: the
    # declared parties may be people or companies, and may be listed by virtue
    # of control rather than ownership, so "filings" is the accurate framing
    # (matches the CAC's own `numberOfPsc` field). The BODS diagram may show
    # fewer nodes because map_cac_nigeria dedupes owners by canonical name.
    parts = [f"{n} PSC filing{'s' if n != 1 else ''}"]
    parts.append("Nigeria CAC public register")
    # Corroboration rule: the CAC BOR publishes the RC number, NOT the LEI
    # (OpenCheck derives the LEI via GLEIF at build time). Assert only the RC —
    # the identifier the register itself publishes — never `lei`.
    identifiers: dict[str, str] = {}
    rc = (r.get("identifiers") or {}).get("ng_cac_rc") or record.get("rc")
    if rc:
        identifiers["ng_cac_rc"] = str(rc)
    return _hit(
        "cac_nigeria", ctx.lei,
        name=record.get("company") or ctx.legal_name or ctx.lei,
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


def _bh_eiti_bo(r: dict, ctx: _LookupCtx) -> SourceHit:
    record = r.get("record") or {}
    register_id = str(record.get("register_id") or "")
    parts: list[str] = []
    if register_id == "drc_itie":
        owners = (record.get("drc") or {}).get("owners") or []
        peps = sum(1 for o in owners if o.get("pep"))
        parts.append(f"{len(owners)} beneficial owner{'s' if len(owners) != 1 else ''}")
        if peps:
            parts.append(f"{peps} PEP{'s' if peps != 1 else ''}")
        parts.append("ITIE-RDC register")
    elif register_id == "armenia_eregister":
        arm = record.get("armenia") or {}
        n = len(arm.get("bods_v02") or [])
        parts.append(f"BODS v0.2 declaration, {n} statements")
        parts.append("Armenia State Register")
    elif register_id == "nigeria_cac":
        pscs = (record.get("nigeria") or {}).get("pscs") or []
        parts.append(f"{len(pscs)} PSC filing{'s' if len(pscs) != 1 else ''}")
        parts.append("Nigeria CAC (NEITI solid-minerals subset)")
    if record.get("source_date"):
        parts.append(f"register data {str(record['source_date'])[:10]}")
    if (record.get("match") or {}).get("confidence") == "medium":
        parts.append("possible name match")
    # Corroboration rule: no pooled register publishes the LEI (OpenCheck
    # derives it at index-build time), so `lei` is intentionally omitted —
    # only the identifiers the register itself publishes are asserted.
    identifiers = {k: str(v) for k, v in (r.get("identifiers") or {}).items() if v}
    return _hit(
        "eiti_bo", ctx.lei,
        name=record.get("company_latin")
        or record.get("company")
        or ctx.legal_name
        or ctx.lei,
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )


def _bh_ted_eu(r: dict, ctx: _LookupCtx) -> SourceHit:
    total = int(r.get("total_notice_count") or 0)
    wins = int(r.get("confirmed_wins") or 0)
    notices = r.get("notices") or []
    parts = [f"{total} EU award notice{'s' if total != 1 else ''}"]
    if wins:
        parts.append(f"{wins} confirmed win{'s' if wins != 1 else ''}")
    latest = next(
        (n.get("publication_date") for n in notices if n.get("publication_date")),
        "",
    )
    if latest:
        parts.append(f"latest {latest}")
    parts.append("eForms era (≈2024+) only")
    # Corroboration rule: TED publishes whatever identifier the buyer entered
    # (eForms BT-501) — usually a national registration number whose scheme is
    # not machine-readable, so national ids are NOT asserted back. The LEI is
    # asserted only when a matched notice actually carried the LEI string
    # (fill rate is zero as of 2026-08, so today this never fires — it exists
    # for when LEI adoption in eForms materialises).
    identifiers: dict[str, str] = {}
    matched = [str(v) for v in (r.get("matched_company_ids") or [])]
    if ctx.lei and any(v.strip().upper() == ctx.lei.upper() for v in matched):
        identifiers["lei"] = ctx.lei
    hit_id = "|".join(r.get("identifiers_queried") or []) or ctx.lei
    return _hit(
        "ted_eu", hit_id,
        name=r.get("legal_name") or ctx.legal_name or ctx.lei,
        summary=" · ".join(parts),
        finding=finding_ted_eu(r),
        identifiers=identifiers,
        raw=r,
    )


# Wikirate Company-card identifier fields → OpenCheck identifier keys, for
# reconciler corroboration. Wikirate independently publishes these on the
# card, so asserting them is legitimate under the corroboration rule.
# ``open_corporates_id`` is deliberately excluded: it is the bare OC company
# number, not OpenCheck's jurisdiction-scoped ``ocid``.
_WIKIRATE_IDENTIFIER_KEYS = {
    "legal_entity_identifier": "lei",
    "wikidata_id": "wikidata_qid",
    "uk_company_number": "gb_coh",
    "sec_central_index_key": "edgar_cik",
}


def _bh_wikirate(r: dict, ctx: _LookupCtx) -> SourceHit:
    card_id = r.get("card_id")
    total = r.get("total_answers") or 0
    parts = ["Wikirate ESG metrics"]
    if total > 0:
        parts.append(f"{total:,} data point{'s' if total != 1 else ''}")
    years = [a.get("year") for a in r.get("latest_answers") or [] if a.get("year")]
    if years:
        parts.append(f"latest {max(years)}")
    identifiers: dict[str, str] = {}
    for field, key in _WIKIRATE_IDENTIFIER_KEYS.items():
        value = (r.get("identifiers") or {}).get(field)
        if isinstance(value, list):
            value = value[0] if value else None
        if value:
            identifiers[key] = str(value)
    return _hit(
        "wikirate", str(card_id),
        name=r.get("name") or ctx.legal_name or str(card_id),
        summary=" · ".join(parts),
        identifiers=identifiers,
        raw=r,
    )
