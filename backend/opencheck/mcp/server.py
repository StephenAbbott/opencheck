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
    "signals; call opencheck_export_bods for the full machine-readable ownership "
    "graph in BODS v0.4. Data is open; some sources are CC-BY-NC — respect the "
    "license_notices in responses."
)

# ``streamable_http_path="/"`` so the streamable-HTTP app serves at its own root;
# mounting it at ``/mcp`` on the FastAPI app then exposes the endpoint at ``/mcp``
# (otherwise FastMCP's default ``/mcp`` path would land at ``/mcp/mcp``).
# DNS-rebinding protection guards localhost-bound dev servers from malicious
# browser pages; OpenCheck's MCP runs as a public, read-only API behind a reverse
# proxy with its own CORS, where the Host/Origin allowlist isn't known ahead of
# time — so disable it rather than hard-code hosts that would 421 in production.
_TRANSPORT_SECURITY = TransportSecuritySettings(enable_dns_rebinding_protection=False)

mcp = FastMCP(
    "opencheck",
    instructions=_INSTRUCTIONS,
    stateless_http=True,
    streamable_http_path="/",
    transport_security=_TRANSPORT_SECURITY,
)


def _err(exc: HTTPException) -> dict[str, Any]:
    return {"error": exc.detail, "status": exc.status_code}


@mcp.tool()
async def opencheck_search(query: str, kind: str = "entity") -> dict[str, Any]:
    """Find a company's Legal Entity Identifier (LEI) from a name or free text.

    Args:
        query: Company name or free-text query (e.g. "Rosneft").
        kind: "entity" (default) or "person".

    Returns candidate matches with their LEIs — feed a candidate's ``lei`` to
    ``opencheck_lookup``.
    """
    from ..sources import SearchKind
    from ..routers.search import search as _search

    try:
        k = SearchKind(kind)
    except ValueError:
        return {"error": f"kind must be 'entity' or 'person', got {kind!r}"}
    resp = await _search(q=query, kind=k)
    return shaping.shape_search(resp)


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
    from ..routers.lookup import resolve_national_id as _resolve

    try:
        resp = await _resolve(number=number, country=country, ra_code=ra_code)
    except HTTPException as exc:
        return _err(exc)
    return resp.model_dump()


@mcp.tool()
async def opencheck_lookup(lei: str, deepen_top: int = 5) -> dict[str, Any]:
    """Run customer due diligence on a legal entity by its LEI.

    Returns the entity's identity, cross-reference identifiers, risk signals
    (sanctions / PEP / debarment / FATF / complex structure), and which sources
    returned data. Call opencheck_export_bods for the full ownership graph.

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        deepen_top: How many top sources to deepen (0-10, default 5).
    """
    from ..routers.lookup import lookup as _lookup

    try:
        resp = await _lookup(lei=lei, deepen_top=deepen_top)
    except HTTPException as exc:
        return _err(exc)
    return shaping.shape_lookup(resp)


@mcp.tool()
async def opencheck_export_bods(
    lei: str, format: str = "json", deepen_top: int = 3
) -> dict[str, Any]:
    """Export an entity's beneficial-ownership graph as BODS v0.4 statements.

    Args:
        lei: ISO 17442 Legal Entity Identifier (20 chars).
        format: "json" (list of statement objects) or "jsonl" (newline-delimited string).
        deepen_top: How many top sources to deepen (0-10, default 3).
    """
    from ..routers.lookup import lookup as _lookup

    if format not in ("json", "jsonl"):
        return {"error": f"format must be 'json' or 'jsonl', got {format!r}"}
    try:
        resp = await _lookup(lei=lei, deepen_top=deepen_top)
    except HTTPException as exc:
        return _err(exc)

    statements: Any = resp.bods
    if format == "jsonl":
        statements = "\n".join(json.dumps(s) for s in resp.bods)
    return {
        "lei": resp.lei,
        "format": format,
        "statement_count": len(resp.bods),
        "statements": statements,
        "bods_issues": resp.bods_issues,
        "license_notices": resp.license_notices,
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
    "opencheck_export_bods",
    "opencheck_list_sources",
]
