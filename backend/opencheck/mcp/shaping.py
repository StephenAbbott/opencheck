"""Response-shaping for the MCP tools.

MCP tools must return compact, agent-readable structured content — not the full
``LookupResponse`` blob the REST API serves to the rich web UI. These helpers
flatten the pipeline output into the minimum an agent needs to reason about an
entity, while preserving licence notices (several sources are CC-BY-NC) so an
agent never redistributes restricted data unknowingly. The full machine-readable
ownership graph is available on demand via the ``opencheck_export_bods`` tool.
"""

from __future__ import annotations

from typing import Any

from ..sources import REGISTRY


def _subject_identifiers(bods: list[dict[str, Any]], lei: str) -> list[dict[str, str]]:
    """Pull the cross-reference identifiers off the subject's GLEIF entity statement.

    The GLEIF entity statement for the queried LEI carries the richest identifier
    set (LEI, BIC, MIC, ISIN, OpenCorporates, S&P CIQ, QCC, national register id)
    — the "LEI as a connector" payload. Find that statement by matching an
    ``XI-LEI`` identifier equal to ``lei``; fall back to any entity statement
    that contains the LEI.
    """
    for stmt in bods:
        if stmt.get("recordType") != "entity":
            continue
        idents = (stmt.get("recordDetails") or {}).get("identifiers") or []
        if any(i.get("scheme") == "XI-LEI" and i.get("id") == lei for i in idents):
            return [
                {
                    k: v
                    for k, v in (
                        ("scheme", i.get("scheme")),
                        ("schemeName", i.get("schemeName")),
                        ("id", i.get("id")),
                        ("uri", i.get("uri")),
                    )
                    if v
                }
                for i in idents
            ]
    return []


def _sources_summary(
    hits: list[Any], errors: dict[str, str]
) -> list[dict[str, Any]]:
    """One row per source that participated: did it return data, under what licence."""
    by_source: dict[str, dict[str, Any]] = {}
    for h in hits:
        row = by_source.setdefault(h.source_id, {"id": h.source_id, "found": False})
        if not h.is_stub:
            row["found"] = True
    for sid, msg in (errors or {}).items():
        by_source.setdefault(sid, {"id": sid, "found": False})["error"] = msg

    for sid, row in by_source.items():
        adapter = REGISTRY.get(sid)
        if adapter is not None:
            row["name"] = adapter.info.name
            row["license"] = adapter.info.license
    return sorted(by_source.values(), key=lambda r: (not r["found"], r["id"]))


def _shape_risk(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in signals or []:
        row = {
            k: s.get(k)
            for k in ("code", "severity", "confidence", "summary")
            if s.get(k) is not None
        }
        if row:
            out.append(row)
    return out


def shape_lookup(payload: Any) -> dict[str, Any]:
    """Flatten a ``LookupResponse`` into a compact MCP tool result."""
    bods = payload.bods or []
    relationships = sum(1 for s in bods if s.get("recordType") == "relationship")
    risk = _shape_risk(payload.risk_signals)
    codes = ", ".join(dict.fromkeys(r["code"] for r in risk if r.get("code"))) or "none"
    sources = _sources_summary(payload.hits, payload.errors)
    found = sum(1 for s in sources if s.get("found"))

    summary = (
        f"{payload.legal_name or 'Entity'} (LEI {payload.lei}"
        f"{', ' + payload.jurisdiction if payload.jurisdiction else ''}). "
        f"Risk signals: {codes}. "
        f"{found} of {len(sources)} sources returned data; "
        f"{len(bods)} BODS statements ({relationships} ownership/control relationships)."
    )

    return {
        "lei": payload.lei,
        "legal_name": payload.legal_name,
        "jurisdiction": payload.jurisdiction,
        "summary": summary,
        "identifiers": _subject_identifiers(bods, payload.lei),
        "derived_identifiers": payload.derived_identifiers or {},
        "risk_signals": risk,
        "sources": sources,
        "counts": {
            "bods_statements": len(bods),
            "relationships": relationships,
            "sources_with_data": found,
        },
        "license_notices": payload.license_notices or [],
        "hint": "Call opencheck_export_bods for the full machine-readable ownership graph.",
    }


def shape_search(payload: Any) -> dict[str, Any]:
    """Flatten a ``SearchResponse`` into a ranked candidate list with LEIs."""
    candidates: list[dict[str, Any]] = []
    for h in payload.hits:
        if h.is_stub:
            continue
        candidates.append(
            {
                "name": h.name,
                "lei": h.identifiers.get("lei") or (h.hit_id if h.source_id == "gleif" else None),
                "source": h.source_id,
                "summary": h.summary,
            }
        )
    return {
        "query": payload.query,
        "kind": payload.kind.value if hasattr(payload.kind, "value") else str(payload.kind),
        "count": len(candidates),
        "candidates": candidates,
        "hint": "Pass a candidate's lei to opencheck_lookup to run due diligence.",
    }


def shape_sources(payload: Any) -> dict[str, Any]:
    """Flatten a ``SourcesResponse`` into an adapter inventory."""
    rows = [
        {
            "id": s.id,
            "name": s.name,
            "license": s.license,
            "live_available": s.live_available,
            "homepage": s.homepage,
        }
        for s in payload.sources
    ]
    return {"count": len(rows), "sources": rows}
