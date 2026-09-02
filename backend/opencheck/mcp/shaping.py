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


#: A signal without a ``kind`` is a risk finding — the same default
#: ``lib/signalKind.ts`` applies on the web, so the two surfaces cannot split
#: the same list differently.
_DEFAULT_KIND = "risk"


def _shape_risk(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The signal list as an agent should read it.

    Phase 153. Two things the first version dropped, and both mattered:

    * **``kind``.** Phases 111/116 split every signal into a *risk finding*
      and a *structural context* observation, and the results page, the PDF
      and the share card all render the two apart. The MCP row carried
      ``code``/``severity``/``confidence``/``summary`` and nothing else, so
      an agent reading ``"Risk signals: GLEIF_REPORTING_EXCEPTION,
      NON_EU_JURISDICTION, ..."`` on Shell plc reported four risk signals,
      two of which the product itself says are not findings — the Phase 111
      failure, on the surface most likely to be quoted verbatim.
    * **Merging.** Per-bundle context signals (``NON_EU_JURISDICTION`` fires
      once per source that deepened) crossed the wire as four identical rows.
      Rows that say the same thing collapse into one, and the sources that
      said it ride along as ``sources`` — so "N sources report X" is still
      answerable without four copies of X.
    """
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for s in signals or []:
        code = s.get("code")
        if not code:
            continue
        kind = s.get("kind") or _DEFAULT_KIND
        summary = s.get("summary") or ""
        key = (str(code), str(kind), str(summary))
        row = merged.get(key)
        if row is None:
            row = {
                k: s.get(k)
                for k in ("code", "severity", "confidence", "summary")
                if s.get(k) is not None
            }
            row["kind"] = kind
            row["sources"] = []
            merged[key] = row
        sid = s.get("source_id")
        if sid and sid not in row["sources"]:
            row["sources"].append(sid)
    return list(merged.values())


def _codes_by_kind(risk: list[dict[str, Any]], kind: str) -> str:
    return ", ".join(dict.fromkeys(r["code"] for r in risk if r.get("kind") == kind))


def _licensing(sources: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The composite licence verdict over the sources that contributed.

    ``license_notices`` carries only the per-bundle notices adapters attach
    themselves, which is usually nothing — a Shell plc lookup holding
    CC-BY-NC OpenSanctions and share-alike OpenCorporates statements shipped
    ``license_notices: []`` to agents while the web Download panel said
    "NOT for commercial use". The same ``licensing.assess`` the panel calls
    (via ``/license-matrix?sources=``) answers here, over the sources that
    returned data, so the two surfaces cannot disagree.
    """
    from ..licensing import assess

    ids = [s["id"] for s in sources if s.get("found")]
    if not ids:
        return None
    a = assess(ids)
    return {
        "commercial_use": a.commercial_use,
        "attribution_required": a.attribution_required,
        "share_alike": a.share_alike,
        "headline": a.headline,
        "warnings": list(a.warnings),
    }


def shape_lookup(payload: Any) -> dict[str, Any]:
    """Flatten a ``LookupResponse`` into a compact MCP tool result."""
    bods = payload.bods or []
    relationships = sum(1 for s in bods if s.get("recordType") == "relationship")
    risk = _shape_risk(payload.risk_signals)
    risk_codes = _codes_by_kind(risk, "risk") or "none"
    context_codes = _codes_by_kind(risk, "context")
    sources = _sources_summary(payload.hits, payload.errors)
    found = sum(1 for s in sources if s.get("found"))
    degraded = getattr(payload, "degraded_sources", None) or []
    licensing = _licensing(sources)

    # An AI consumer must never read "Risk signals: none" as a clean screen
    # when a screening check didn't fully run — say so in the summary line
    # it is most likely to quote.
    degraded_note = (
        f" CAUTION: {len(degraded)} screening check(s) did not fully run "
        "(see degraded_sources) — the absence of their signals is not a "
        "clean screen."
        if degraded
        else ""
    )
    # Structural context is named in the same sentence, in its own clause,
    # and labelled for what it is — never folded into the risk list.
    context_note = (
        f" Structural context (not risk findings): {context_codes}."
        if context_codes
        else ""
    )
    licence_note = (
        f" Licensing: {licensing['headline']}" if licensing else ""
    )
    summary = (
        f"{payload.legal_name or 'Entity'} (LEI {payload.lei}"
        f"{', ' + payload.jurisdiction if payload.jurisdiction else ''}). "
        f"Risk signals: {risk_codes}.{context_note} "
        f"{found} of {len(sources)} sources returned data; "
        f"{len(bods)} BODS statements ({relationships} ownership/control relationships)."
        f"{degraded_note}{licence_note}"
    )

    return {
        "lei": payload.lei,
        "legal_name": payload.legal_name,
        "jurisdiction": payload.jurisdiction,
        "summary": summary,
        # The same one-line sentence the results page opens with
        # (``opencheck.verdict``) — deterministic, already split on kind.
        "verdict": getattr(payload, "verdict", None),
        # What the registers say the company is — legal form, register
        # status, founding date, registered address — with the sources that
        # state each (Phase 154). Facts, never findings.
        "profile": getattr(payload, "subject_profile", None),
        "identifiers": _subject_identifiers(bods, payload.lei),
        "derived_identifiers": payload.derived_identifiers or {},
        "risk_signals": risk,
        "degraded_sources": degraded,
        "sources": sources,
        "counts": {
            "bods_statements": len(bods),
            "relationships": relationships,
            "sources_with_data": found,
            "risk_signals": sum(1 for r in risk if r.get("kind") == "risk"),
            "context_signals": sum(1 for r in risk if r.get("kind") == "context"),
        },
        "licensing": licensing,
        "license_notices": payload.license_notices or [],
        "hint": (
            "risk_signals rows carry kind='risk' (a finding) or kind='context' "
            "(how the company is put together, not a finding against it) — "
            "never report a context row as a risk. Call opencheck_export_bods "
            "for the full machine-readable ownership graph."
        ),
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
            # Which sources this one republishes — an agent counting "N
            # sources agree" needs it for the same reason the UI does.
            "derived_from": list(s.derived_from),
        }
        for s in payload.sources
    ]
    return {"count": len(rows), "sources": rows}


def shape_person_check(payload: Any) -> dict[str, Any]:
    """Flatten a ``PersonCheckResponse`` into an agent-friendly report.

    Raw hit payloads are dropped (they can be large and carry
    licence-suppressed content); everything a consumer needs to act —
    scores, signals with their match evidence, per-source outcomes,
    identifier bridges and the caveats — is kept.
    """

    def _match(m: Any) -> dict[str, Any]:
        return {
            "source": m.hit.source_id,
            "hit_id": m.hit.hit_id,
            "name": m.hit.name,
            "summary": m.hit.summary,
            "identifiers": m.hit.identifiers,
            "name_score": m.name_score,
            "birth_year_compatible": m.birth_year_compatible,
            "is_stub": m.hit.is_stub,
        }

    strong = [_match(m) for m in payload.matches if m.strong]
    return {
        "query": payload.query,
        "birth_year": payload.birth_year,
        "risk_signals": payload.risk_signals,
        "strong_matches": strong,
        "weak_match_count": payload.weak_match_count,
        "cross_source_links": payload.cross_source_links,
        "sources_checked": [
            {
                "id": s.source_id,
                "name": s.name,
                "license": s.license,
                "live": s.live,
                "hit_count": s.hit_count,
                "error": s.error,
            }
            for s in payload.sources
        ],
        "caveats": payload.caveats,
        "hint": (
            "Risk signals derive from strong matches only (name similarity "
            ">= 0.88 with compatible birth year); each carries its match "
            "evidence. A failed source means that person was NOT screened "
            "there — never treat this as a clean screen."
        ),
    }
