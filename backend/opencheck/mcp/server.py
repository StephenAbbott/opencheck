"""OpenCheck MCP server.

Exposes OpenCheck's LEI-driven due-diligence pipeline as Model Context Protocol
tools so AI agents can invoke it directly (the most agent-native counterpart to
the OpenAPI surface advertised via ARD / ai-catalog).

The tools call the *same* in-process pipeline functions the REST routes use —
``routers.lookup.lookup``, ``routers.search.search``, etc. — so the MCP path
shares the 15-minute replay cache and the startup cache warm-up, and can never
diverge from the REST path. Responses are flattened by ``shaping`` into compact,
agent-readable structures; licence notices are preserved end to end.

Transport is streamable HTTP (``stateless_http=True``) so the server mounts onto
the existing FastAPI app and is reachable remotely at ``/mcp``.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from . import shaping

_INSTRUCTIONS = (
    "OpenCheck performs customer due diligence on legal entities using the "
    "Legal Entity Identifier (LEI) as the key. Typical flow: if you only have a "
    "company name, call opencheck_search to get its LEI (or "
    "opencheck_resolve_national_id if you have a national registration number); "
    "then call opencheck_lookup with the LEI for owners, controllers and risk "
    "signals. A company with no LEI — most UK LLPs, most small companies "
    "anywhere — is looked up by its register number instead: pass a search "
    "candidate's identifiers[].scheme and id (e.g. GB-COH, OC346224) to "
    "opencheck_register_lookup; call opencheck_export_bods for the full machine-readable ownership "
    "graph in BODS v0.4 (include_subsidiaries=true folds in the GLEIF subsidiary "
    "network), or opencheck_subsidiaries for what the company consolidates. "
    "To ask who owns a company above a threshold, call opencheck_owners "
    "(calculated owners per source, declared beneficial owners, and who is in "
    "both); opencheck_ownership_map draws the structure as an SVG image with a "
    "text table. "
    "For a list of companies call opencheck_batch_lookup "
    "with up to 20 LEIs and read each row's verdict, counts and degraded flag. "
    "To keep a check as a record the reader can share and verify, call "
    "opencheck_save_report with the LEI; it returns a link and a SHA-256. "
    "Data is open; some sources are CC-BY-NC — respect the license_notices in "
    "responses."
)

# DNS-rebinding protection guards localhost-bound dev servers from malicious
# browser pages; OpenCheck's MCP runs as a public, read-only API behind a reverse
# proxy with its own CORS, where the Host/Origin allowlist isn't known ahead of
# time — so disable it rather than hard-code hosts that would 421 in production.
_TRANSPORT_SECURITY = TransportSecuritySettings(enable_dns_rebinding_protection=False)

# The route is published at ``/mcp`` and registered directly on the FastAPI app
# (see app.py) rather than mounted under a prefix — a ``Mount`` at ``/mcp`` would
# 307-redirect a bare ``POST /mcp`` to ``/mcp/``, and many MCP clients don't
# replay the POST across that redirect, so the connector silently fails.
mcp = FastMCP(
    "opencheck",
    instructions=_INSTRUCTIONS,
    stateless_http=True,
    streamable_http_path="/mcp",
    transport_security=_TRANSPORT_SECURITY,
)


def _err(exc: HTTPException) -> dict[str, Any]:
    return {"error": exc.detail, "status": exc.status_code}


@mcp.tool()
async def opencheck_search(
    query: str, kind: str = "entity", limit: int = 15
) -> dict[str, Any]:
    """Find a company's Legal Entity Identifier (LEI) from a name or free text.

    Args:
        query: Company name or free-text query (e.g. "Rosneft").
        kind: "entity" (default) or "person".
        limit: How many ranked candidates to return (default 15, max 50).

    Returns candidate matches ranked best-first across every source — exact
    name matches first, each tagged with ``match`` — with ``total`` and
    ``truncated`` saying whether more were found. Feed a candidate's ``lei`` to
    ``opencheck_lookup``. A candidate with ``lei: null`` and an entry in
    ``identifiers`` (a register scheme and number, e.g. GB-COH OC346224) is
    looked up with ``opencheck_register_lookup`` instead.
    """
    from ..sources import SearchKind
    from ..routers.search import _search_impl as _search

    try:
        k = SearchKind(kind)
    except ValueError:
        return {"error": f"kind must be 'entity' or 'person', got {kind!r}"}
    resp = await _search(q=query, kind=k)
    return shaping.shape_search(resp, limit=limit)


@mcp.tool()
async def opencheck_resolve_national_id(
    number: str, country: str = "", ra_code: str = ""
) -> dict[str, Any]:
    """Resolve a national company-registration number to its LEI(s).

    Use when you have a local registry number but not the LEI.

    Args:
        number: National registration number (e.g. UK Companies House "00102498").
        country: ISO 3166-1 alpha-2 code (e.g. "GB"), resolved to a GLEIF RA code.
        ra_code: GLEIF Registration Authority code (e.g. "RA000585"); overrides country.
    """
    from ..routers.lookup import _resolve_national_id_impl as _resolve

    try:
        resp = await _resolve(number=number, country=country, ra_code=ra_code)
    except HTTPException as exc:
        return _err(exc)
    return resp.model_dump()


@mcp.tool()
async def opencheck_lookup(lei: str, deepen_top: int = 5) -> dict[str, Any]:
    """Run customer due diligence on a legal entity by its LEI.

    Returns the entity's identity, cross-reference identifiers, risk signals
    (sanctions / PEP / debarment / FATF / complex structure), and what each
    applicable source said — with a record, with no record, or no answer. Call opencheck_export_bods for the full ownership graph, and
    opencheck_save_report to keep this check as a shareable, verifiable record.

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        deepen_top: How many top sources to deepen (0-10, default 5).
    """
    from ..routers.lookup import _lookup_impl as _lookup

    try:
        resp = await _lookup(lei=lei, deepen_top=deepen_top)
    except HTTPException as exc:
        return _err(exc)
    return shaping.shape_lookup(resp)


@mcp.tool()
async def opencheck_register_lookup(
    scheme: str, id: str, deepen_top: int = 5, name: str = ""
) -> dict[str, Any]:
    """Run due diligence on a company that has no LEI, by its register number.

    The register that owns the scheme (UK Companies House for GB-COH, the
    Dutch KvK for NL-KVK, …) is read for the company's record, officers and
    beneficial owners / PSCs; that bundle is then screened and assessed as
    opencheck_lookup's is — sanctions / PEP / debarment / offshore-leaks
    signals, the verdict, the register profile and what is knowable in the
    jurisdiction. There is no GLEIF anchor: ``lei`` is null, no listing line,
    and the company's own name is screened by name rather than through an
    LEI-keyed record. Same rate limits and lookup budget as opencheck_lookup.

    Args:
        scheme: Register identifier scheme, as a search candidate's
            ``identifiers[].scheme`` gives it (e.g. "GB-COH", "NL-KVK",
            "REG-GB"); the full list is GET /expand-schemes.
        id: The company's number on that register (e.g. "OC346224").
        deepen_top: How many top sources to deepen (0-10, default 5).
        name: The company's name as you know it (a search candidate's name),
            optional. Some registers search by name behind the number, and
            the Dutch KvK open data publishes no names at all — pass it when
            you have it.
    """
    from ..routers.lookup import _register_lookup_impl as _lookup

    try:
        resp = await _lookup(scheme=scheme, ident=id, deepen_top=deepen_top, name=name)
    except HTTPException as exc:
        return _err(exc)
    return shaping.shape_register_lookup(resp)


@mcp.tool()
async def opencheck_batch_lookup(leis: list[str], deepen_top: int = 5) -> dict[str, Any]:
    """Screen a list of companies by LEI — one compact row per company.

    Portfolio / counterparty screening for agents (Phase 164). Each row is
    exactly what opencheck_lookup would return for that LEI, reduced to
    the table a list reader scans: name, jurisdiction, register status,
    the one-sentence verdict, risk and structural-context counts, coverage
    (how many of OpenCheck's sources apply and how many answered) and a
    ``degraded`` flag. Rows run two at a time against the shared upstream
    budgets, so twenty companies not seen recently take about two minutes.

    Args:
        leis: ISO 17442 Legal Entity Identifiers. At most 20 are screened;
            duplicates and malformed values are returned in ``rejected``,
            valid ones beyond the cap are counted in ``overflow``.
        deepen_top: How many top sources to deepen per company (0-10, default 5).

    A ``failed`` row (unknown LEI, upstream rate-limited) means that company
    was NOT screened — never read the batch as clean while ``failed`` or a
    row's ``degraded`` flag is non-empty. Rows are never ranked by
    severity: OpenCheck does not grade companies.
    """
    from .. import batch as _batch

    parsed = _batch.parse_lei_list("\n".join(str(v) for v in (leis or [])))
    rows: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    totals: dict[str, Any] = {}
    if parsed.leis:
        async for event, payload in _batch.run_batch(parsed.leis, deepen_top=deepen_top):
            if event == "row_done":
                rows.append(payload)
            elif event == "row_failed":
                failed.append(payload)
            else:
                totals = payload
    # Paste order, so the caller can line rows up with its own list.
    order = {lei: i for i, lei in enumerate(parsed.leis)}
    rows.sort(key=lambda r: order.get(r["lei"], len(order)))
    failed.sort(key=lambda r: order.get(r["lei"], len(order)))
    return {
        "cap": parsed.cap,
        "accepted": parsed.leis,
        "rejected": [r.to_dict() for r in parsed.rejected],
        "overflow": parsed.overflow,
        "rows": rows,
        "failed": failed,
        "counts": {
            "requested": totals.get("requested", len(parsed.leis)),
            "done": totals.get("done", len(rows)),
            "failed": totals.get("failed", len(failed)),
            "degraded": sum(1 for r in rows if r.get("degraded")) + len(failed),
        },
        "hint": (
            "Each row's risk_count counts findings (kind='risk'); context_count "
            "is structural context, not a finding. A failed row or degraded=true "
            "means that company was not fully screened. Call opencheck_lookup "
            "with a row's lei for the full report."
        ),
    }


@mcp.tool()
async def opencheck_export_bods(
    lei: str, format: str = "json", deepen_top: int = 5, include_subsidiaries: bool = False
) -> dict[str, Any]:
    """Export an entity's beneficial-ownership graph as BODS v0.4 statements.

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        format: "json" (list of BODS statement objects), "jsonl" (newline-delimited
            BODS string), "senzing" (Senzing JSON entity records, ready to load
            into Senzing for entity resolution), or "ftm" (FollowTheMoney
            entities, ready for OpenSanctions / OpenAleph / ftm-CLI workflows).
        deepen_top: How many top sources to deepen (0-10, default 5 — the same
            as opencheck_lookup, so an export after a lookup reuses its run).
        include_subsidiaries: Also fold in the GLEIF subsidiary network (direct
            and ultimate children, accounting consolidation) — the same merge as
            ``GET /export?subsidiaries=true``. Off by default: a large group adds
            hundreds of statements.

    For ``json`` / ``jsonl`` the result also carries ``integrated_ownership``:
    each party above the subject with how much of it that party holds,
    directly and through companies it holds, one figure per source and never
    added across sources (T-Rank's integrated-ownership method). Context, not
    a risk finding — no threshold is applied.
    """
    from ..bods import map_to_ftm, map_to_senzing
    from ..gleif_throttle import discretionary as gleif_discretionary
    from ..routers.export import _merge_subsidiaries
    from ..routers.lookup import _lookup_impl as _lookup

    if format not in ("json", "jsonl", "senzing", "ftm"):
        return {
            "error": f"format must be 'json', 'jsonl', 'senzing' or 'ftm', got {format!r}"
        }
    try:
        resp = await _lookup(lei=lei, deepen_top=deepen_top)
    except HTTPException as exc:
        return _err(exc)

    subsidiary_statements = 0
    if include_subsidiaries:
        # Phase 255: the REST export's merge, deduplicated by statementId.
        with gleif_discretionary():
            resp, subsidiary_statements = await _merge_subsidiaries(resp)

    if format in ("senzing", "ftm"):
        records = (map_to_senzing if format == "senzing" else map_to_ftm)(resp.bods)
        return {
            "lei": resp.lei,
            "format": format,
            "record_count": len(records),
            "records": records,
            "bods_issues": resp.bods_issues,
            "license_notices": resp.license_notices,
        }

    statements: Any = resp.bods
    if format == "jsonl":
        statements = "\n".join(json.dumps(s) for s in resp.bods)
    return {
        "lei": resp.lei,
        "format": format,
        "statement_count": len(resp.bods),
        "subsidiary_statement_count": subsidiary_statements,
        "statements": statements,
        # Phase 339: each party above the subject with its integrated
        # ownership, per source — context, never a signal. The same figures
        # ride in the statements as a ``commenting`` annotation on the subject.
        "integrated_ownership": _integrated_ownership_block(resp),
        "bods_issues": resp.bods_issues,
        "license_notices": resp.license_notices,
    }


def _integrated_ownership_block(resp: Any) -> dict[str, Any] | None:
    from .shaping import _integrated_ownership

    return _integrated_ownership(resp)


async def _owners_network(
    lei: str, depth: int, deepen_top: int, threshold: float | None, rule_name: str | None
) -> tuple[Any, Any, Any]:
    """Phase 340: the lookup, the rule for its jurisdiction, then (``depth`` > 1)
    FullCheck's owner-ward hop on each corporate owner at or above the
    threshold. Raises HTTPException (lookup) or ValueError (rule)."""
    from .. import integrated_ownership as _io
    from .. import owners as _owners
    from ..routers.expand import _expand_one_layer
    from ..routers.lookup import _lookup_impl as _lookup
    from ..subject_profile import subject_statements

    resp = await _lookup(lei=lei, deepen_top=deepen_top)
    rule = _owners.resolve_rule(resp.jurisdiction, threshold, rule_name)
    bods = list(resp.bods or [])
    subject_ids = [
        s["statementId"] for s in subject_statements(resp.lei or lei, bods)
        if isinstance(s.get("statementId"), str)
    ]
    payload = getattr(resp, "integrated_ownership", None)
    as_of = (payload or {}).get("as_of") or _owners.default_as_of()
    if payload is None or payload.get("subject_ids") != subject_ids:
        payload = _io.compute(bods, subject_ids, as_of, _io.lookup_canon(bods))
        payload["subject_ids"] = subject_ids

    async def hop(owner_lei: str, anchor: str) -> list[dict[str, Any]]:
        layer, _signals, _degraded = await _expand_one_layer(owner_lei, anchor)
        return layer

    net = await _owners.grow(
        bods, subject_ids, payload, rule, depth, hop, as_of, subject_lei=resp.lei
    )
    return resp, net, rule


def _depth_error(depth: int) -> dict[str, Any] | None:
    from .. import owners as _owners

    if not isinstance(depth, int) or not 1 <= depth <= _owners.MAX_DEPTH:
        return {"error": f"depth must be 1, 2 or {_owners.MAX_DEPTH}", "status": 400}
    return None


@mcp.tool()
async def opencheck_owners(
    lei: str,
    threshold: float | None = None,
    rule: str | None = None,
    depth: int = 1,
    deepen_top: int = 5,
) -> dict[str, Any]:
    """Who owns this company above a threshold — calculated and declared.

    Returns three lists:
    - ``calculated_owners``: every person or company whose integrated ownership
      of the company (shares multiplied along each path, added across paths)
      meets the threshold, one figure per source — never added across sources.
      ``position`` is "above", or "may_be_above" when a band straddles the
      threshold or a share on the path is not stated (read that as unknown).
    - ``declared_beneficial_owners``: the people the registers themselves flag
      as beneficial owners of the company, with the nature of their interest.
    - ``in_both``: the parties in both lists (same record or register
      identifier, never a name alone).
    Plus ``rule`` (the threshold applied, and why), ``chain_stops`` (companies
    above the threshold whose own owners were not reached) and
    ``listed_without_a_figure`` (voting rights only, or formerly). Calculated
    figures are arithmetic on filings, never a determination that anyone is a
    beneficial owner; nothing here is a risk signal. The one finding is
    ``sanctions_ownership_test`` (null when none applies): the lookup's own
    computed sanctions 50% rule for OFAC, the EU and the UK, side by side —
    from parties designated under each regime and matched on an identifier
    or with corroboration, ownership only (control tests not computed), and
    never a determination that the company is sanctioned.

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        threshold: Percentage (0-100). Default: the company's national rule
            (e.g. Estonia more than 25%, Ukraine 25% or more), or 25% or more
            where OpenCheck records none.
        rule: "national" (the jurisdiction's "more than" / "or more" reading)
            or "amlr" (25% or more, EU AML Regulation 2024/1624). Optional.
        depth: 1 (default, the lookup's own reach — same cost as
            opencheck_lookup), 2 or 3: also look up each corporate owner at or
            above the threshold, up to 10 extra lookups per call.
        deepen_top: How many top sources to deepen (0-10, default 5).
    """
    from .. import identifiers
    from .. import owners as _owners

    bad = _depth_error(depth)
    if bad:
        return bad
    norm = (lei or "").strip().upper()
    if not identifiers.LEI_PATH_SHAPE.match(norm):
        return {"error": f"{norm!r} is not a valid LEI (20-character alphanumeric).", "status": 400}
    try:
        resp, net, the_rule = await _owners_network(norm, depth, deepen_top, threshold, rule)
    except HTTPException as exc:
        return _err(exc)
    except ValueError as exc:
        return {"error": str(exc), "status": 400}
    return _owners.owners_answer(
        lei=resp.lei,
        legal_name=resp.legal_name,
        jurisdiction=resp.jurisdiction,
        net=net,
        rule=the_rule,
        depth=depth,
        as_of=net.payload.get("as_of") or _owners.default_as_of(),
        license_notices=list(resp.license_notices or []),
        risk_signals=list(getattr(resp, "risk_signals", None) or []),
    )


# Phase 341: ``structured_output=False``. With a ``list[...]`` return type
# FastMCP builds an output schema and serialises the result as structured
# content, which cannot hold an ``Image`` — every call failed in production
# with "Unable to serialize unknown type". Unstructured, the list is returned
# as the content blocks themselves: the JSON text and the SVG image.
@mcp.tool(structured_output=False)
async def opencheck_ownership_map(
    lei: str, source: str | None = None, depth: int = 1, deepen_top: int = 5
) -> list[Any]:
    """Map who owns a company: one SVG image plus a text table of every edge.

    Draws the company and every party above it — ownership and control edges
    from every source, each edge labelled with its source (officers and board
    roles, company secretaries included, are left out). Each holder shows its
    integrated ownership figure, one per source, never added. The JSON part
    opens with a one-sentence ``summary``, carries the same edges as rows
    (owner, interest, owned, source, current/ended), lists GLEIF reporting
    exceptions under ``reporting_exceptions`` (a filing that names no parent,
    so never drawn as one), and links to the interactive FullCheck view; the
    image draws at most 16 edges.

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        source: Optional — draw one source only (its id, e.g. "companies_house",
            or its name).
        depth: 1 (default), 2 or 3 — as opencheck_owners: also look up each
            corporate owner at or above the company's national threshold.
        deepen_top: How many top sources to deepen (0-10, default 5).
    """
    from mcp.server.fastmcp import Image
    from mcp.types import TextContent

    from .. import identifiers
    from .. import owners as _owners

    def _text(obj: dict[str, Any]) -> TextContent:
        return TextContent(type="text", text=json.dumps(obj, ensure_ascii=False))

    bad = _depth_error(depth)
    if bad:
        return [_text(bad)]
    norm = (lei or "").strip().upper()
    if not identifiers.LEI_PATH_SHAPE.match(norm):
        return [_text({"error": f"{norm!r} is not a valid LEI (20-character alphanumeric).", "status": 400})]
    try:
        resp, net, _rule = await _owners_network(norm, depth, deepen_top, None, None)
    except HTTPException as exc:
        return [_text(_err(exc))]
    answer, svg = _owners.ownership_map(
        lei=resp.lei, legal_name=resp.legal_name, net=net, source=source,
        as_of=net.payload.get("as_of") or _owners.default_as_of(),
    )
    answer["license_notices"] = list(resp.license_notices or [])
    out: list[Any] = [_text(answer)]
    if svg:
        out.append(Image(data=svg.encode("utf-8"), format="svg+xml"))
    return out


@mcp.tool()
async def opencheck_person_check(
    name: str, birth_year: int | None = None
) -> dict[str, Any]:
    """Screen one person for PEP / sanctions / offshore-leaks signals.

    BackgroundCheck for agents: fans the name out across every
    person-capable source (Companies House officers, OpenSanctions,
    EveryPolitician, Wikidata, OpenAleph), scores each hit against the
    queried name, and derives risk signals from strong matches only.

    Args:
        name: Person name to screen (e.g. an officer or beneficial owner
            from an opencheck_lookup result).
        birth_year: Known birth year — corroborates name matches where
            the source record carries a date of birth.

    Screening is name-based: results are potential matches with their
    evidence, never confirmed identities, and a non-hit is not proof of
    absence. Heed the caveats in the response.
    """
    from ..routers.person_check import _person_check_impl as _check

    if len(name.strip()) < 2:
        return {"error": "name must be at least 2 characters"}
    if birth_year is not None and not 1900 <= birth_year <= 2026:
        return {"error": "birth_year must be between 1900 and 2026"}
    resp = await _check(name=name.strip(), birth_year=birth_year)
    return shaping.shape_person_check(resp)


@mcp.tool()
async def opencheck_save_report(lei: str, deepen_top: int = 5) -> dict[str, Any]:
    """Keep a check of an LEI as a saved report: a link to exactly what
    OpenCheck found, on this date, that does not change afterwards.

    Runs (or reuses, if it finished in the last 15 minutes) the same check as
    opencheck_lookup, then saves the server's own copy of that run. Returns the
    report ``url``, the ``content_hash`` (SHA-256 of the saved JSON at
    ``json_url``), ``saved_at`` / ``expires_at`` and a ``manage_token`` — the
    only way to extend or delete the report; keep it private, it is not shown
    again. The saved report holds QuickCheck and FullCheck findings only
    (no summary, Background check, Subsidiaries, History or ESG).

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        deepen_top: How many top sources to deepen (0-10, default 5).

    Tell the reader the report is a record from ``saved_at`` and is not
    re-checked. Some sources are CC-BY-NC: see ``licensing``.
    """
    import asyncio

    from .. import saved_reports as _sr
    from ..licensing import assess as _assess
    from ..routers.lookup import _lookup_impl as _lookup
    from ..routers.saved_reports import _api_base, _with_url

    store = _sr.get_store()
    if store is None:
        return {"error": "Saved reports are not enabled on this instance.", "status": 503}
    try:
        # Phase 234: the same per-address save quota as POST /saved-reports,
        # checked before the lookup so a refused save runs nothing.
        _sr.check_client_quota()
    except _sr.SavedReportError as exc:
        return {"error": str(exc), "status": exc.status, "code": exc.code}
    try:
        resp = await _lookup(lei=lei, deepen_top=deepen_top)
    except HTTPException as exc:
        return _err(exc)
    if not resp.run_completed_at:
        return {"error": "The check did not finish, so there is nothing to save. Run it again.", "status": 409}
    try:
        out = await asyncio.to_thread(
            _sr.save_from_replay,
            store,
            lei=resp.lei or lei,
            run_completed_at=resp.run_completed_at,
            deepen_top=deepen_top,
        )
    except _sr.SavedReportError as exc:
        return {"error": str(exc), "status": exc.status, "code": exc.code}
    _sr.spend_client_quota()
    meta = _with_url(out)
    return {
        "report_id": meta["report_id"],
        "url": meta["url"],
        "json_url": f"{_api_base()}/saved-reports/{meta['report_id']}.json",
        "content_hash": meta["content_hash"],
        "lei": meta["lei"],
        "legal_name": meta.get("legal_name"),
        "run_completed_at": resp.run_completed_at,
        "saved_at": meta["saved_at"],
        "expires_at": meta["expires_at"],
        "manage_token": meta["manage_token"],
        "verdict": resp.verdict,
        "licensing": _assess(_sr._contributing_ids(resp)).headline,
        "note": (
            "A record of this check as it stood when saved; nothing in it is re-checked. "
            "Verify with: curl -s <json_url> | shasum -a 256"
        ),
    }


@mcp.tool()
async def opencheck_subsidiaries(
    lei: str, format: str = "summary", include_declared: bool = False
) -> dict[str, Any]:
    """What a legal entity consolidates: its GLEIF subsidiary network.

    GLEIF Level 2 direct and ultimate children (accounting consolidation — a
    consolidating parent need not be a shareholder, and GLEIF publishes no
    percentages). Returns counts, per-jurisdiction and per-country breakdowns
    and one row per child; ``format="bods"`` adds the network as BODS v0.4
    statements (one relationship per parent–child pair, dated from GLEIF's
    relationship records, with the direct parent drawn where GLEIF names one).

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        format: "summary" (default) or "bods" (adds ``bods``: the statements).
        include_declared: Also return the subsidiary lists OpenCheck holds from
            other sources — the OECD-UNSD MEIP register, EITI and Global Energy
            Monitor — kept apart per source. These are rows, not BODS, and they
            measure different things from GLEIF and from each other.
    """
    from .. import identifiers
    from ..gleif_throttle import discretionary as gleif_discretionary
    from ..sources import REGISTRY
    from ..subsidiaries import assemble_subsidiaries
    from ..subsidiaries_declared import assemble_declared

    if format not in ("summary", "bods"):
        return {"error": f"format must be 'summary' or 'bods', got {format!r}"}
    norm = (lei or "").strip().upper()
    if not identifiers.LEI_PATH_SHAPE.match(norm):
        return {"error": f"{norm!r} is not a valid LEI (20-character alphanumeric).", "status": 400}
    check_digit_error = identifiers.lei_check_digit_error(norm)
    if check_digit_error:
        return {"error": check_digit_error, "status": 400}

    with gleif_discretionary():
        network = await assemble_subsidiaries(norm, include_bods=(format == "bods"))
    out: dict[str, Any] = {**network}
    if format != "bods":
        out.pop("bods", None)
    else:
        out["statement_count"] = len(network.get("bods") or [])
    out["measures"] = (
        "GLEIF Level 2 accounting consolidation: entities that hold an LEI and "
        "report this entity as their direct or ultimate consolidating parent. "
        "Subsidiaries without an LEI, or that report no parent, are not here."
    )
    notices = [_license_notice(REGISTRY.get("gleif"))]
    if include_declared:
        declared = await assemble_declared(norm)
        out["declared"] = declared
        for src in declared.get("sources") or []:
            if src.get("covered"):
                notices.append(_license_notice(REGISTRY.get(src.get("id"))))
    out["license_notices"] = [n for n in notices if n]
    return out


def _license_notice(adapter: Any) -> dict[str, str] | None:
    if adapter is None:
        return None
    info = adapter.info
    return {
        "source_id": info.id,
        "license": info.license,
        "attribution": info.attribution,
    }


@mcp.tool()
async def opencheck_list_sources() -> dict[str, Any]:
    """List the data sources OpenCheck consults, with licence and live status."""
    from ..routers.health import sources as _sources

    resp = await _sources()
    return shaping.shape_sources(resp)


# Tool names in declaration order — reused by the /.well-known/mcp.json descriptor.
TOOL_NAMES = [
    "opencheck_search",
    "opencheck_resolve_national_id",
    "opencheck_lookup",
    "opencheck_register_lookup",
    "opencheck_batch_lookup",
    "opencheck_export_bods",
    "opencheck_subsidiaries",
    "opencheck_owners",
    "opencheck_ownership_map",
    "opencheck_person_check",
    "opencheck_save_report",
    "opencheck_list_sources",
]
