"""Assemble the due-diligence report as portable Markdown.

Mirrors ``html_report.build_report_html`` section by section, over the same
lookup-result dict and the same optional already-generated narrative — but
renders plain CommonMark instead of print HTML. Two deliberate divergences
from the PDF pipeline:

- **Diagrams** are replaced by their text-equivalent relationship tables
  (the same rows ``diagram.source_diagram`` computes for the PDF) — Markdown
  consumers (wikis, git, LLM pipelines) get more value from the table than an
  embedded image.
- **The QR code** is replaced by the plain canonical live-check URL.

No WeasyPrint dependency: this renderer is always available and doubles as
the report fallback when the PDF toolchain is not installed.
"""

from __future__ import annotations

from typing import Any

from .diagram import source_diagram
from .html_report import (
    _DISPOSITION_LABELS,
    _ID_LABELS,
    LIVE_BASE,
    _cite_labels,
    _generated_line,
    _name,
    _registry,
    _subject_entity,
    _summary_sources,
)

# ---- helpers ----------------------------------------------------------------


def _cell(value: Any) -> str:
    """Make a value safe inside a Markdown table cell."""
    text = str(value if value is not None else "")
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """A CommonMark pipe table (with the leading blank line CommonMark wants)."""
    lines = [""]
    lines.append("| " + " | ".join(_cell(h) for h in headers) + " |")
    lines.append("|" + "|".join(" --- " for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(_cell(c) for c in row) + " |")
    lines.append("")
    return lines


# ---- section builders -------------------------------------------------------


def _cover(report: dict[str, Any], subject: dict[str, Any] | None) -> list[str]:
    name = report.get("legal_name") or (subject and _name(subject)) or "Unknown entity"
    jur = report.get("jurisdiction") or ""
    identity = "Identity confirmed by LEI" if report.get("lei") else "Name match — unconfirmed"
    meta = " · ".join(b for b in [jur, identity] if b)
    return [
        f"# OpenCheck due-diligence report — {name}",
        "",
        "*Entity due-diligence report*" + (f" — {meta}" if meta else ""),
        "",
    ]


def _identifiers(report: dict[str, Any], subject: dict[str, Any] | None) -> list[str]:
    rows: list[list[str]] = []
    if report.get("lei"):
        rows.append(["Legal Entity Identifier (LEI)", f"`{report['lei']}`"])
    for k, v in (report.get("derived_identifiers") or {}).items():
        if not v:
            continue
        rows.append([_ID_LABELS.get(k, k.replace("_", " ").capitalize()), f"`{v}`"])
    seen = {r[1] for r in rows}
    for i in ((subject or {}).get("recordDetails") or {}).get("identifiers") or []:
        val = i.get("id")
        if val and f"`{val}`" not in seen:
            rows.append([i.get("scheme", "Identifier"), f"`{val}`"])
            seen.add(f"`{val}`")
    if report.get("jurisdiction"):
        rows.append(["Jurisdiction", report["jurisdiction"]])
    return [
        "## Identifiers",
        "",
        "Registered identifiers for this entity, each as published by the issuing register.",
        *_table(["Identifier", "Value"], rows),
    ]


def _live_check(lei: str | None) -> list[str]:
    if not lei:
        return []
    url = f"https://{LIVE_BASE}/?lei={lei}"
    return [
        "## Run a live check",
        "",
        "This is a point-in-time snapshot. Open the live, always-current profile — re-run "
        "every source, explore the interactive ownership graph and download the underlying "
        f"BODS data: <{url}>",
        "",
    ]


def _claims_block(narrative: dict[str, Any], dispositions: dict[str, Any] | None) -> list[str]:
    """The per-claim record: claim text, citations, and the analyst's decision."""
    claims = narrative.get("claims") or []
    if not claims:
        return []
    packet = narrative.get("packet") or {}
    disp_by_claim: dict[str, dict[str, Any]] = {}
    for d in (dispositions or {}).get("dispositions") or []:
        if d.get("claim_id"):
            disp_by_claim[d["claim_id"]] = d

    lines: list[str] = ["### Claims and analyst dispositions", ""]
    if disp_by_claim:
        counts: dict[str, int] = {}
        for d in disp_by_claim.values():
            counts[d.get("status", "")] = counts.get(d.get("status", ""), 0) + 1
        bits = [
            f"{counts[s]} {label.lower()}"
            for s, label in _DISPOSITION_LABELS.items()
            if counts.get(s)
        ]
        undecided = len(claims) - len(disp_by_claim)
        if undecided > 0:
            bits.append(f"{undecided} undecided")
        lines += [f"**Analyst review:** {' · '.join(bits)}", ""]

    for c in claims:
        cites = _cite_labels(packet, list(c.get("fact_ids") or []))
        cite_txt = f" — *{', '.join(cites)}*" if cites else ""
        disp = disp_by_claim.get(c.get("id", ""))
        disp_txt = ""
        if disp:
            status = disp.get("status", "")
            label = _DISPOSITION_LABELS.get(status, status)
            decided = str(disp.get("decided_at") or "")[:10]
            disp_txt = f" **[{label}{f' · {decided}' if decided else ''}]**"
            if disp.get("comment"):
                disp_txt += f" — Analyst note: {disp['comment']}"
        lines.append(f"- {c.get('text', '')}{cite_txt}{disp_txt}")
    lines.append("")
    return lines


def _gaps_block(narrative: dict[str, Any]) -> list[str]:
    """"Not verified in this check" — always rendered when the packet has gaps.

    Built from the evidence packet directly (not the model's prose), so a
    narrative that failed to mention a gap cannot hide it from the record.
    """
    gaps = (narrative.get("packet") or {}).get("gaps") or []
    if not gaps:
        return []
    lines = [
        "### Not verified in this check",
        "",
        "The following could not be verified from the sources consulted. A clean summary "
        "does not imply these are resolved.",
        "",
    ]
    lines += [f"- {g.get('statement', '')}" for g in gaps]
    lines.append("")
    return lines


def _summary(
    narrative: dict[str, Any] | None,
    dispositions: dict[str, Any] | None = None,
) -> list[str]:
    if not narrative or not narrative.get("summary"):
        return []
    conf = narrative.get("overall_confidence", "low")
    cites = _summary_sources(narrative)
    model = narrative.get("model", "Claude")
    pv = narrative.get("prompt_version", "")
    run_id = narrative.get("run_id", "")
    generated_at = str(narrative.get("generated_at", ""))[:19]
    updated_at = str((dispositions or {}).get("updated_at") or "")[:19]
    run_bits = [b for b in [
        f"run {run_id}" if run_id else "",
        f"generated {generated_at}" if generated_at else "",
        f"dispositions updated {updated_at}" if updated_at else "",
    ] if b]
    lines = [
        f"## Summary ({conf} confidence)",
        "",
        narrative["summary"],
        "",
    ]
    if cites:
        lines += [f"**Evidence:** {', '.join(cites)}", ""]
    lines += _claims_block(narrative, dispositions)
    lines += _gaps_block(narrative)
    lines += [
        "> AI-generated from OpenCheck’s data. Every statement is grounded in the cited "
        f"sources; nothing is added beyond what they state. Generated by {model}"
        + (f" · prompt {pv}" if pv else "")
        + (f" · {' · '.join(run_bits)}" if run_bits else "")
        + ".",
        "",
    ]
    return lines


def _risk(report: dict[str, Any]) -> list[str]:
    signals = report.get("risk_signals") or []
    lines = [
        "## Risk signals",
        "",
        "Risk signals are structural and jurisdictional indicators for further review, computed "
        "deterministically across the assembled statements and aligned with the EU AMLA draft "
        "due-diligence standards. They are not determinations of wrongdoing.",
        "",
    ]
    if not signals:
        lines += [
            "**No risk signals were raised** for this entity. The checks below were applied and "
            "returned clear: sanctions and PEP screening, FATF-listed jurisdictions, non-EU/EEA "
            "jurisdiction, trust or nominee arrangements, and complex ownership layers.",
            "",
        ]
        return lines
    reg = _registry()
    for sig in signals:
        conf = sig.get("confidence", "medium")
        src = reg.get(sig.get("source_id", ""))
        src_name = src.info.name if src else (sig.get("source_id") or "OpenCheck risk engine")
        label = sig.get("code", "").replace("_", " ").title()
        lines.append(f"- **{label}** ({conf}) — {sig.get('summary') or ''} — *{src_name}*")
    lines.append("")
    return lines


def _sources_found(report: dict[str, Any]) -> list[str]:
    reg = _registry()
    by_src: dict[str, list[dict[str, Any]]] = {}
    for h in report.get("hits") or []:
        if h.get("is_stub"):
            continue
        by_src.setdefault(h.get("source_id", ""), []).append(h)
    if not by_src:
        return []
    lines = ["## What each source found", ""]
    for sid, hits in by_src.items():
        adapter = reg.get(sid)
        if adapter is None:
            continue
        info = adapter.info
        lines += [f"### {info.name} ({info.license})", ""]
        lines += [f"- {h.get('summary') or h.get('name') or ''}" for h in hits]
        lines.append("")
    return lines


def _relationships(report: dict[str, Any]) -> list[str]:
    """Per-source relationship tables — the text equivalent of the PDF diagrams."""
    bods = report.get("bods") or []
    by_id = {s.get("statementId"): s for s in bods if s.get("statementId")}
    order: list[str] = []
    for s in bods:
        name = (s.get("source") or {}).get("description") or "OpenCheck source"
        if name not in order:
            order.append(name)
    if not order:
        return []
    lines = [
        "## Ownership & control structure",
        "",
        "Each source’s findings are listed as an ownership-and-control table — the interested "
        "party, the interest they hold, and the subject it is held in.",
        "",
    ]
    rendered = 0
    for name in order:
        rels = [
            s for s in bods
            if s.get("recordType") == "relationship"
            and ((s.get("source") or {}).get("description") or "OpenCheck source") == name
        ]
        has_entity = any(
            s.get("recordType") == "entity"
            and ((s.get("source") or {}).get("description") or "OpenCheck source") == name
            for s in bods
        )
        if not rels and not has_entity:
            continue
        diagram = source_diagram(rels, by_id, source_name=name)
        rendered += 1
        lines += [f"### {name}", ""]
        if diagram.rows:
            lines += _table(
                ["Interested party", "Interest", "Subject"],
                [[p, i, s] for p, i, s in diagram.rows],
            )
        else:
            lines += ["No ownership or control relationships reported; entity record only.", ""]
    return lines if rendered else []


def _licensing(report: dict[str, Any]) -> list[str]:
    from ..licensing import assess as assess_licensing

    reg = _registry()
    contributing = sorted({h.get("source_id") for h in (report.get("hits") or []) if not h.get("is_stub")})
    contributing = [c for c in contributing if c]
    assessment = assess_licensing(contributing)
    rows = []
    for sid in contributing:
        adapter = reg.get(sid)
        if adapter is None:
            continue
        info = adapter.info
        rows.append([info.name, info.license, info.attribution])
    lines = [
        "## Licensing & attribution",
        "",
        "This report is assembled from open data. Combined commercial-use assessment: "
        f"**{assessment.headline}**",
        "",
    ]
    notices = [n.get("notice", "") for n in (report.get("license_notices") or []) if n.get("notice")]
    if notices:
        lines += [f"- {n}" for n in notices]
        lines.append("")
    lines += _table(["Source", "Licence", "Attribution"], rows)
    lines += [
        "Ownership data structured to the "
        "[Beneficial Ownership Data Standard (BODS) v0.4]"
        "(https://standard.openownership.org/en/0.4.0/). "
        + _generated_line(),
        "",
    ]
    return lines


# ---- assembly ---------------------------------------------------------------


def build_report_markdown(
    report: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    dispositions: dict[str, Any] | None = None,
) -> str:
    """Assemble the full report as portable Markdown for a lookup result."""
    bods = report.get("bods") or []
    subject = _subject_entity(bods, report.get("lei"))
    sections: list[str] = []
    sections += _cover(report, subject)
    sections += _identifiers(report, subject)
    sections += _live_check(report.get("lei"))
    sections += _summary(narrative, dispositions)
    sections += _risk(report)
    sections += _sources_found(report)
    sections += _relationships(report)
    sections += _licensing(report)
    sections += [
        "---",
        "",
        "OpenCheck aggregates open corporate and beneficial-ownership data and maps it to "
        "BODS v0.4. It is an information tool, not a substitute for regulated due diligence. "
        "Always confirm findings against the primary registers before relying on them. "
        "Not legal or financial advice — verify against primary sources.",
        "",
    ]
    return "\n".join(sections)
