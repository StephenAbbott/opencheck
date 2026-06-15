"""Assemble the full report HTML from a lookup result.

Produces the semantic, accessible HTML that ``pdf.render`` converts to a tagged
PDF. Every section is built from the lookup result and attributed to its source;
the AI summary is only included when an already-generated narrative is passed in
(per the product decision — no model call on download).
"""

# This module is one big HTML/CSS template; wrapping the markup strings to
# 100 cols would hurt readability more than it helps.
# ruff: noqa: E501

from __future__ import annotations

from datetime import UTC, datetime
from html import escape
from typing import Any

from .diagram import source_diagram

LIVE_BASE = "opencheck.world"

# ---- CSS (A4, tagged-PDF friendly; mirrors the approved template) -----------

_CSS = """
@page { size: A4; margin: 22mm;
  @bottom-left  { content: "OpenCheck \\00b7 opencheck.world"; font: 8pt sans-serif; color:#595959; }
  @bottom-center{ content: "Not legal or financial advice \\2014 verify against primary sources"; font: 7.5pt sans-serif; color:#767676; }
  @bottom-right { content: "Page " counter(page) " of " counter(pages); font: 8pt sans-serif; color:#595959; }
}
* { box-sizing: border-box; }
body { font-family: "DejaVu Sans", sans-serif; color:#1a1a1a; font-size:10pt; line-height:1.5; margin:0; }
.band { background:#191d23; color:#fff; padding:11mm 12mm 9mm; margin:0 0 8mm; border-radius:8px; }
.brand { display:flex; align-items:center; gap:8px; }
.brand svg { width:26px; height:26px; }
.brand .word { font-size:14pt; font-weight:bold; }
.brand .word .k { color:#93c5fd; }
.kicker { font-size:8.5pt; letter-spacing:1.2px; text-transform:uppercase; color:#c7c9d6; margin:9mm 0 0; }
h1 { font-size:21pt; line-height:1.15; margin:2mm 0 1mm; color:#fff; }
.band .meta { font-size:9pt; color:#cdd0dc; margin:0; }
h2 { font-size:12.5pt; color:#191d23; margin:7mm 0 2mm; padding-bottom:1.5mm; border-bottom:1.5px solid #3d30d4; }
h3 { font-size:10.5pt; color:#191d23; margin:4mm 0 1mm; }
p { margin:0 0 2mm; }
a { color:#3d30d4; }
table { width:100%; border-collapse:collapse; font-size:9pt; margin:1mm 0 3mm; }
/* break-after:avoid keeps the caption glued to the first row. Without it a
   table that splits right after its caption leaves the wrapper with no table
   box on the first page — WeasyPrint then raises "Table wrapper without a
   table". (Reproduced and regression-tested.) */
caption { text-align:left; font-size:8.5pt; color:#595959; margin-bottom:1mm; break-after:avoid; }
th, td { text-align:left; vertical-align:top; padding:2mm 3mm; border-bottom:0.5px solid #d9d9de; }
th[scope="row"] { width:38%; font-weight:normal; color:#595959; }
thead th { background:#f5f5f7; color:#191d23; border-bottom:1px solid #d9d9de; }
.mono { font-family:"DejaVu Sans Mono", monospace; font-size:8.5pt; }
.live { display:flex; gap:6mm; align-items:center; border:1px solid #3d30d4; background:#f3f1fb; border-radius:6px; padding:4mm 5mm; margin:3mm 0; }
.live .qr { width:26mm; height:26mm; flex:none; }
.live .qr svg { width:100%; height:100%; }
.live .url { font-family:"DejaVu Sans Mono", monospace; font-size:9pt; color:#3d30d4; word-break:break-all; }
.summary { background:#f5f5f7; border-radius:6px; padding:4mm 5mm; }
.summary .lead { font-size:10.5pt; line-height:1.6; }
.badge { display:inline-block; font-size:8pt; border:1px solid; border-radius:10px; padding:0.5mm 2.5mm; }
.b-green{ color:#1f7a44; border-color:#9bd3ae; background:#eef8f1; }
.b-amber{ color:#8a5a00; border-color:#e3c890; background:#fdf6e7; }
.cites { font-size:8pt; color:#595959; margin-top:2mm; }
.cite { display:inline-block; background:#eef1fb; color:#27348b; border:0.5px solid #cfd6f5; border-radius:8px; padding:0 2mm; margin:0 1mm 1mm 0; }
.disclaimer { font-size:7.5pt; color:#595959; margin-top:2mm; border-top:0.5px solid #d9d9de; padding-top:1.5mm; }
.source { border:0.5px solid #d9d9de; border-radius:6px; padding:3mm 4mm; margin:2.5mm 0; break-inside:avoid; }
.source .head { display:flex; justify-content:space-between; align-items:baseline; gap:4mm; }
.source .name { font-weight:bold; color:#191d23; font-size:10pt; }
.lic { font-size:7.5pt; font-family:"DejaVu Sans Mono", monospace; border:0.5px solid; border-radius:4px; padding:0 1.5mm; }
.lic.ok { color:#1f7a44; border-color:#9bd3ae; background:#eef8f1; }
.lic.nc { color:#8a5a00; border-color:#e3c890; background:#fdf6e7; }
.source ul { margin:1.5mm 0 0; padding-left:5mm; }
figure { margin:2mm 0; padding:3mm; border:0.5px solid #d9d9de; border-radius:6px; break-inside:avoid; }
figure svg { width:100%; height:auto; }
figcaption { font-size:8pt; color:#595959; margin-top:1.5mm; }
.signal { border-left:3px solid #d9d9de; padding:1mm 0 1mm 4mm; margin:2mm 0; }
.signal.high { border-left-color:#9f1239; }
.signal.medium { border-left-color:#8a5a00; }
.signal .h { font-weight:bold; color:#191d23; }
.signal .src { font-size:8pt; color:#595959; }
"""

_LOGO = (
    '<svg viewBox="0 0 200 200" aria-hidden="true">'
    '<line x1="127" y1="127" x2="186" y2="186" stroke="#fff" stroke-width="14" stroke-linecap="round"/>'
    '<circle cx="80" cy="80" r="70" fill="none" stroke="#fff" stroke-width="13"/>'
    '<circle cx="48" cy="28" r="11" fill="#22c55e"/><circle cx="18" cy="76" r="11" fill="#3b82f6"/>'
    '<circle cx="48" cy="124" r="11" fill="#7c3aed"/>'
    '<line x1="48" y1="28" x2="18" y2="76" stroke="#93c5fd" stroke-width="4.5" stroke-linecap="round"/>'
    '<line x1="18" y1="76" x2="48" y2="124" stroke="#93c5fd" stroke-width="4.5" stroke-linecap="round"/>'
    '<line x1="48" y1="28" x2="48" y2="124" stroke="#93c5fd" stroke-width="4.5" stroke-linecap="round"/>'
    "</svg>"
)

_ID_LABELS = {
    "company_number": "Company registration number",
    "lei": "Legal Entity Identifier (LEI)",
    "vat": "VAT number",
}


def _registry():
    from ..sources import REGISTRY  # lazy to avoid import cycles

    return REGISTRY


def _qr_svg(url: str) -> str:
    try:
        import io

        import segno

        buf = io.BytesIO()
        segno.make(url, error="m").save(buf, kind="svg", xmldecl=False, svgns=True,
                                        omitsize=True, dark="#191d23", border=0)
        return buf.getvalue().decode("utf-8")
    except Exception:
        return ""  # QR is non-essential; degrade gracefully


# ---- section builders -------------------------------------------------------


def _subject_entity(bods: list[dict[str, Any]], lei: str | None) -> dict[str, Any] | None:
    for s in bods:
        if s.get("recordType") != "entity":
            continue
        idents = (s.get("recordDetails") or {}).get("identifiers") or []
        if lei and any(i.get("id") == lei for i in idents):
            return s
    return next((s for s in bods if s.get("recordType") == "entity"), None)


def _cover(report: dict[str, Any], subject: dict[str, Any] | None) -> str:
    name = report.get("legal_name") or (subject and _name(subject)) or "Unknown entity"
    jur = report.get("jurisdiction") or ""
    meta_bits = [b for b in [jur, ("Identity confirmed by LEI" if report.get("lei") else "Name match — unconfirmed")] if b]
    return (
        '<div class="band">'
        f'<div class="brand">{_LOGO}<span class="word">Open<span class="k">Check</span></span></div>'
        '<p class="kicker">Entity due-diligence report</p>'
        f"<h1>{escape(name)}</h1>"
        f'<p class="meta">{escape(" · ".join(meta_bits))}</p>'
        "</div>"
    )


def _name(stmt: dict[str, Any]) -> str:
    return (stmt.get("recordDetails") or {}).get("name") or "an entity"


def _identifiers(report: dict[str, Any], subject: dict[str, Any] | None) -> str:
    rows: list[tuple[str, str]] = []
    if report.get("lei"):
        rows.append(("Legal Entity Identifier (LEI)", report["lei"]))
    for k, v in (report.get("derived_identifiers") or {}).items():
        if not v:
            continue
        rows.append((_ID_LABELS.get(k, k.replace("_", " ").capitalize()), v))
    # Any extra schemes the subject entity itself publishes.
    seen = {v for _, v in rows}
    for i in ((subject or {}).get("recordDetails") or {}).get("identifiers") or []:
        val = i.get("id")
        if val and val not in seen:
            rows.append((f"{i.get('scheme', 'Identifier')}", val))
            seen.add(val)
    if report.get("jurisdiction"):
        rows.append(("Jurisdiction", report["jurisdiction"]))
    body = "".join(
        f'<tr><th scope="row">{escape(label)}</th><td class="mono">{escape(str(val))}</td></tr>'
        for label, val in rows
    )
    return (
        '<section aria-labelledby="ids"><h2 id="ids">Identifiers</h2>'
        '<table><caption>Registered identifiers for this entity, each as published by the issuing register.</caption>'
        f"<tbody>{body}</tbody></table></section>"
    )


def _live_check(lei: str | None) -> str:
    if not lei:
        return ""
    url = f"{LIVE_BASE}/?lei={lei}"
    qr = _qr_svg(f"https://{url}")
    qr_block = f'<div class="qr" aria-hidden="true">{qr}</div>' if qr else ""
    return (
        '<section aria-labelledby="live"><h2 id="live">Run a live check</h2>'
        f'<div class="live">{qr_block}<div>'
        "<h3>This is a point-in-time snapshot</h3>"
        "<p>Open the live, always-current profile — re-run every source, explore the interactive "
        "ownership graph and download the underlying BODS data:</p>"
        f'<p class="url">{escape(url)}</p>'
        "</div></div></section>"
    )


def _summary(narrative: dict[str, Any] | None) -> str:
    if not narrative or not narrative.get("summary"):
        return ""
    conf = narrative.get("overall_confidence", "low")
    badge_cls = {"high": "b-green", "medium": "b-amber"}.get(conf, "b-amber")
    cites = "".join(
        f'<span class="cite">{escape(c)}</span>'
        for c in _summary_sources(narrative)
    )
    model = escape(narrative.get("model", "Claude"))
    pv = escape(narrative.get("prompt_version", ""))
    return (
        f'<section aria-labelledby="sum"><h2 id="sum">Summary '
        f'<span class="badge {badge_cls}">{escape(conf)} confidence</span></h2>'
        f'<div class="summary"><p class="lead">{escape(narrative["summary"])}</p>'
        + (f'<p class="cites"><strong>Evidence:</strong> {cites}</p>' if cites else "")
        + '<p class="disclaimer">AI-generated from OpenCheck’s data. Every statement is grounded in '
        f"the cited sources; nothing is added beyond what they state. Generated by {model}"
        + (f" · prompt {pv}" if pv else "")
        + ".</p></div></section>"
    )


def _summary_sources(narrative: dict[str, Any]) -> list[str]:
    packet = narrative.get("packet") or {}
    names: list[str] = []
    for f in packet.get("facts") or []:
        n = f.get("source_name")
        if n and n not in names:
            names.append(n)
    return names[:6]


def _risk(report: dict[str, Any]) -> str:
    signals = report.get("risk_signals") or []
    head = (
        '<section aria-labelledby="risk"><h2 id="risk">Risk signals</h2>'
        "<p>Risk signals are structural and jurisdictional indicators for further review, computed "
        "deterministically across the assembled statements and aligned with the EU AMLA draft "
        "due-diligence standards. They are not determinations of wrongdoing.</p>"
    )
    if not signals:
        return head + (
            "<p><strong>No risk signals were raised</strong> for this entity. The checks below were "
            "applied and returned clear: sanctions and PEP screening, FATF-listed jurisdictions, "
            "non-EU/EEA jurisdiction, trust or nominee arrangements, and complex ownership layers.</p>"
            "</section>"
        )
    reg = _registry()
    items = []
    for sig in signals:
        conf = sig.get("confidence", "medium")
        src = reg.get(sig.get("source_id", ""))
        src_name = src.info.name if src else (sig.get("source_id") or "OpenCheck risk engine")
        label = sig.get("code", "").replace("_", " ").title()
        items.append(
            f'<div class="signal {escape(conf)}"><span class="h">{escape(label)}</span> '
            f'<span class="badge">{escape(conf)}</span><br>'
            f'{escape(sig.get("summary") or "")} '
            f'<span class="src">— {escape(src_name)}</span></div>'
        )
    return head + "".join(items) + "</section>"


def _sources_found(report: dict[str, Any]) -> str:
    reg = _registry()
    by_src: dict[str, list[dict[str, Any]]] = {}
    for h in report.get("hits") or []:
        if h.get("is_stub"):
            continue
        by_src.setdefault(h.get("source_id", ""), []).append(h)
    if not by_src:
        return ""
    blocks = []
    for sid, hits in by_src.items():
        adapter = reg.get(sid)
        if adapter is None:
            continue
        info = adapter.info
        lic = info.license
        lic_cls = "nc" if "nc" in lic.lower() else "ok"
        bullets = []
        for h in hits:
            summary = h.get("summary") or h.get("name") or ""
            bullets.append(f"<li>{escape(summary)}</li>")
        blocks.append(
            '<div class="source"><div class="head">'
            f'<span class="name">{escape(info.name)}</span>'
            f'<span class="lic {lic_cls}">{escape(lic)}</span></div>'
            f"<ul>{''.join(bullets)}</ul></div>"
        )
    return (
        '<section aria-labelledby="src"><h2 id="src">What each source found</h2>'
        + "".join(blocks)
        + "</section>"
    )


def _diagrams(report: dict[str, Any]) -> str:
    bods = report.get("bods") or []
    by_id = {s.get("statementId"): s for s in bods if s.get("statementId")}
    # Ordered unique source names that produced statements.
    order: list[str] = []
    for s in bods:
        name = (s.get("source") or {}).get("description") or "OpenCheck source"
        if name not in order:
            order.append(name)
    if not order:
        return ""
    fig_no = 0
    blocks = []
    for name in order:
        rels = [
            s for s in bods
            if s.get("recordType") == "relationship"
            and ((s.get("source") or {}).get("description") or "OpenCheck source") == name
        ]
        # Only render sources that have an entity or relationships for this name.
        has_entity = any(
            s.get("recordType") == "entity"
            and ((s.get("source") or {}).get("description") or "OpenCheck source") == name
            for s in bods
        )
        if not rels and not has_entity:
            continue
        diagram = source_diagram(rels, by_id, source_name=name)
        fig_no += 1
        table = _diagram_table(diagram.rows, fig_no)
        blocks.append(
            f"<h3>{escape(name)}</h3>"
            f'<figure>{diagram.svg}<figcaption>Figure {fig_no}. '
            f"Ownership and control as found by {escape(name)}.</figcaption></figure>{table}"
        )
    return (
        '<section aria-labelledby="viz"><h2 id="viz">Ownership &amp; control structure</h2>'
        "<p>Each source’s findings are shown as a separate ownership-and-control diagram in the "
        "OpenCheck (BOVS) visual style — person and entity icons, interest type by edge colour, "
        "and the interest described on each edge. A text-equivalent table follows every diagram.</p>"
        + "".join(blocks)
        + "</section>"
    )


def _diagram_table(rows: list[tuple[str, str, str]], fig_no: int) -> str:
    if not rows:
        body = '<tr><td colspan="3">No ownership or control relationships reported; entity record only.</td></tr>'
    else:
        body = "".join(
            f"<tr><td>{escape(p)}</td><td>{escape(i)}</td><td>{escape(s)}</td></tr>"
            for p, i, s in rows
        )
    return (
        f"<table><caption>Relationships in Figure {fig_no}, as a table for non-visual access.</caption>"
        '<thead><tr><th scope="col">Interested party</th><th scope="col">Interest</th>'
        '<th scope="col">Subject</th></tr></thead>'
        f"<tbody>{body}</tbody></table>"
    )


def _licensing(report: dict[str, Any]) -> str:
    from ..licensing import assess as assess_licensing

    reg = _registry()
    contributing = sorted({h.get("source_id") for h in (report.get("hits") or []) if not h.get("is_stub")})
    contributing = [c for c in contributing if c]
    assessment = assess_licensing(contributing)
    colour = {"green": "b-green", "amber": "b-amber", "red": "b-amber"}.get(assessment.color, "b-amber")
    rows = []
    for sid in contributing:
        adapter = reg.get(sid)
        if adapter is None:
            continue
        info = adapter.info
        rows.append(
            f"<tr><td>{escape(info.name)}</td><td>{escape(info.license)}</td>"
            f"<td>{escape(info.attribution)}</td></tr>"
        )
    notices = "".join(
        f"<li>{escape(n.get('notice', ''))}</li>" for n in (report.get("license_notices") or [])
    )
    notice_block = f"<ul>{notices}</ul>" if notices else ""
    return (
        '<section aria-labelledby="lic"><h2 id="lic">Licensing &amp; attribution</h2>'
        f'<p>This report is assembled from open data. Combined commercial-use assessment: '
        f'<span class="badge {colour}">{escape(assessment.headline)}</span></p>'
        + notice_block
        + "<table><caption>Licence and required attribution for each contributing source.</caption>"
        '<thead><tr><th scope="col">Source</th><th scope="col">Licence</th>'
        '<th scope="col">Attribution</th></tr></thead>'
        f"<tbody>{''.join(rows)}</tbody></table>"
        '<p class="cites">Ownership data structured to the '
        '<a href="https://standard.openownership.org/en/0.4.0/">Beneficial Ownership Data Standard '
        "(BODS) v0.4</a>. " + escape(_generated_line()) + "</p></section>"
    )


def _generated_line() -> str:
    from .. import __version__

    stamp = datetime.now(UTC).strftime("%d %B %Y")
    return f"Report generated by OpenCheck v{__version__} on {stamp}."


def build_report_html(report: dict[str, Any], *, narrative: dict[str, Any] | None = None) -> str:
    """Assemble the full, accessible report HTML for a lookup result."""
    bods = report.get("bods") or []
    subject = _subject_entity(bods, report.get("lei"))
    name = report.get("legal_name") or (subject and _name(subject)) or "Unknown entity"
    title = f"OpenCheck due-diligence report — {name}"
    return (
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"/>"
        f"<title>{escape(title)}</title><style>{_CSS}</style></head><body>"
        + _cover(report, subject)
        + _identifiers(report, subject)
        + _live_check(report.get("lei"))
        + _summary(narrative)
        + _risk(report)
        + _sources_found(report)
        + _diagrams(report)
        + _licensing(report)
        + "<footer><p style=\"font-size:8pt;color:#595959\">OpenCheck aggregates open corporate and "
        "beneficial-ownership data and maps it to BODS v0.4. It is an information tool, not a substitute "
        "for regulated due diligence. Always confirm findings against the primary registers before "
        "relying on them.</p></footer>"
        "</body></html>"
    )
