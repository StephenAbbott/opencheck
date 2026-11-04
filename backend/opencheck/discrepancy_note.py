"""A discrepancy note: hand a register-record disagreement back to the register.

The "Discrepancy report export for registers" ticket (Nov 2026, from the Open
Ownership usable-data check). OpenCheck already finds that two published
records of one company disagree — ``RECORD_CONSISTENCY`` (Phase 337) — and
the reader had no way to give that finding back to the register that holds
the stale record. This module turns the finding into a one-page note per
register, in Markdown or as a PDF, addressed to the register and saying where
to send it.

Stephen's decisions (3 Nov 2026), each of which this module implements and
none of which should be re-derived:

* **Only what ``RECORD_CONSISTENCY`` already shows qualifies** — the
  ``disagree`` items on its evidence, which the Phase 337 allowlist admitted
  (register status and jurisdiction today). Nothing is compared here; a
  field that is counted but not shown (founding date, identifier clash) is
  not in a note, and name / address comparisons are their own consistency
  ticket with the Phase 337 two-week gate first. A lapsed LEI is GLEIF's
  record of the LEI, not of the company, and a GLEIF parent is accounting
  consolidation, not a shareholder: neither is a discrepancy a register can
  act on, so neither is here.
* **One note per register that holds a disputed value, GLEIF included.** A
  disagreement has two sides and either can be the stale one. Every
  ``officialRegister`` source is an addressee, and GLEIF is a register with
  its own public challenge facility (:data:`GLEIF_CHALLENGE`). A side that is
  not a register (an aggregator, a leak archive) is quoted as the conflicting
  record but never addressed.
* **Honest framing.** Nothing compared is beneficial-ownership information,
  so the note says in its own words that it is a *register record*
  discrepancy note and not an AMLR / PSC beneficial-ownership discrepancy
  report (:data:`SCOPE_SENTENCE`). The reporting routes the jurisdictions
  table records are those duties' routes; the note points at them and lets
  the reader decide whether their duty applies.
* **It describes and never alleges** — the two values, each register's own
  words, the dates each side was stated and retrieved — and every fixed
  sentence passes the knowability ``BANNED_VOCABULARY`` guard
  (:func:`wording_issues`, pinned by a test). Values are the registers' own.
* **Where to send it comes from the jurisdictions table** (``bo_register.
  discrepancy_route`` / ``discrepancy_note``, ``company_register.
  discrepancy_route`` — synced from Notion, never typed here), keyed on the
  addressee adapter's ``SourceInfo.country``. A register with no recorded
  route says so.
* **The footer carries the saved-report link and SHA-256 only when the run
  is saved** (``saved`` from ``open_for_export``); otherwise one line says
  how to get one. Saving is a deliberate act with a daily quota; the export
  never saves on the reader's behalf.
* **The reporter is blank lines** (:data:`REPORTER_FIELDS`): a note a
  register accepts names who sent it, and nothing about the reader is
  stored or asked for.
* **A saved report exports its own day** — every date is read off the
  frozen BODS (``statementDate``, ``source.retrievedAt``) and the frozen
  signal; nothing here reads a clock except the generated-on line, which
  the saved block overrides.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from . import knowability
from .bods.refs import statement_index
from .bods.source_ids import source_id_of
from .bods.statements import OFFICIAL_REGISTER_SOURCES, SOURCE_NAMES
from .consistency import DISAGREE, FIELD_NOUNS, RECORD_CONSISTENCY
from .lei_registration import format_date
from .licensing import source_licensing

#: GLEIF is addressed like a register: its data challenge facility is open to
#: any user of LEI data and is forwarded to the LEI issuer that manages the
#: record (challenge manual v1.3, Jan 2025). Checked 3 Nov 2026.
GLEIF_CHALLENGE: dict[str, str] = {
    "url": "https://www.gleif.org/en/lei-data/gleif-data-quality-management/challenge-lei-and-vlei-data",
    "note": (
        "Any user of LEI data may challenge an LEI record through GLEIF's online "
        "form; GLEIF forwards the challenge to the LEI issuer that manages the "
        "record, which aims to answer within ten business days."
    ),
}

#: Which sources a note can be addressed to: every official register, plus
#: GLEIF. The set is derived, not typed — a new register adapter joins by
#: being in ``OFFICIAL_REGISTER_SOURCES``.
ADDRESSEE_SOURCES: frozenset[str] = OFFICIAL_REGISTER_SOURCES | {"gleif"}

#: GLEIF's public record page, used when the GLEIF statement carries no URL.
GLEIF_RECORD_URL = "https://search.gleif.org/#/record/{lei}"

TITLE = "Register record discrepancy note"

#: The honest-framing sentence (Stephen, 3 Nov 2026). The fields compared are
#: the company register's, not beneficial ownership; the AMLR Article 24 and
#: UK PSC duties are about beneficial-ownership information, and the reader
#: decides whether a duty applies.
SCOPE_SENTENCE = (
    "This note describes a difference between two published records of the "
    "same company. It is not a beneficial-ownership discrepancy report under "
    "the EU AMLR or the UK PSC regulations: the fields compared are the "
    "register's status and jurisdiction, not who owns or controls the company."
)

PURPOSE_SENTENCE = (
    "Each item names the field, the value this register holds, the value "
    "another published source holds, and the dates on which each was stated "
    "and retrieved. It says which records differ; it does not say which is right."
)

#: Blank lines for the reader to fill in. Nothing about them is stored.
REPORTER_FIELDS: tuple[str, ...] = (
    "Reported by",
    "Organisation",
    "Capacity (for example, an obliged entity under the AML rules of …)",
    "Contact",
    "Date sent",
)

NO_ROUTE_SENTENCE = (
    "OpenCheck holds no recorded reporting route for this register; use the "
    "register's general contact."
)
NO_SAVED_SENTENCE = (
    "Save the report on OpenCheck to add a verifiable link and SHA-256 of "
    "what was seen."
)

_WORD_RE = re.compile(r"[a-z]+")


@dataclass
class Side:
    """One record's value in a disagreement, with its provenance."""

    source_id: str
    source_name: str
    value: str
    statement_id: str | None
    statement_date: str | None
    retrieved_at: str | None
    record_url: str | None
    licence: str | None
    licence_name: str | None


@dataclass
class Discrepancy:
    field: str
    field_noun: str
    register: Side
    other: Side


@dataclass
class Route:
    url: str | None
    note: str | None
    company_register_url: str | None = None


@dataclass
class Note:
    source_id: str
    register_name: str
    jurisdiction: str | None
    subject_name: str
    lei: str | None
    identifiers: list[dict[str, str]]
    record_url: str | None
    discrepancies: list[Discrepancy]
    route: Route
    saved: dict[str, Any] | None = None
    generated_at: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Qualifying findings
# ---------------------------------------------------------------------------


def consistency_signal(report: dict[str, Any]) -> dict[str, Any] | None:
    for sig in report.get("risk_signals") or []:
        if isinstance(sig, dict) and sig.get("code") == RECORD_CONSISTENCY:
            return sig
    return None


def qualifying_items(report: dict[str, Any]) -> list[dict[str, Any]]:
    """The ``disagree`` items of the lookup's ``RECORD_CONSISTENCY`` signal —
    the only findings a note carries. Every item here passed the Phase 337
    allowlist when the run was made; nothing is re-compared."""
    sig = consistency_signal(report)
    if sig is None:
        return []
    items = (sig.get("evidence") or {}).get("items") or []
    out = []
    for item in items:
        if not isinstance(item, dict) or item.get("relation") != DISAGREE:
            continue
        sources = item.get("sources") or []
        ids = item.get("statement_ids") or []
        if len(sources) != 2 or len(ids) != 2:
            continue
        out.append(item)
    return out


def qualifies(report: dict[str, Any]) -> bool:
    return bool(qualifying_items(report))


# ---------------------------------------------------------------------------
# Building the notes
# ---------------------------------------------------------------------------


def _side(source_id: str, value: Any, stmt: dict[str, Any] | None) -> Side:
    src = (stmt or {}).get("source") or {}
    lic = source_licensing(source_id)
    return Side(
        source_id=source_id,
        source_name=SOURCE_NAMES.get(source_id, source_id),
        value=str(value),
        statement_id=(stmt or {}).get("statementId"),
        statement_date=(stmt or {}).get("statementDate"),
        retrieved_at=src.get("retrievedAt"),
        record_url=src.get("url"),
        licence=lic.license if lic else None,
        licence_name=lic.terms.name if lic else None,
    )


def _subject_identifiers(stmt: dict[str, Any] | None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for ident in ((stmt or {}).get("recordDetails") or {}).get("identifiers") or []:
        scheme, ident_id = ident.get("scheme"), ident.get("id")
        if scheme and ident_id and scheme != "XI-LEI":
            out.append({"scheme": str(scheme), "id": str(ident_id)})
    return out


def _country_of(source_id: str) -> str | None:
    from .sources import REGISTRY

    adapter = REGISTRY.get(source_id)
    info = getattr(adapter, "info", None)
    code = getattr(info, "country", None)
    return str(code).upper() if code else None


def route_for(source_id: str, jurisdiction: str | None) -> Route:
    """Where to send a note addressed to this register — GLEIF's challenge
    facility, else the jurisdictions table's recorded routes."""
    if source_id == "gleif":
        return Route(url=GLEIF_CHALLENGE["url"], note=GLEIF_CHALLENGE["note"])
    j = knowability.get_jurisdiction(jurisdiction) if jurisdiction else None
    if j is None:
        return Route(url=None, note=None)
    return Route(
        url=j.bo_register.discrepancy_route,
        note=j.bo_register.discrepancy_note,
        company_register_url=j.company_register.discrepancy_route,
    )


def _subject_name(report: dict[str, Any], stmt: dict[str, Any] | None) -> str:
    name = (report.get("legal_name") or "").strip()
    if not name and stmt:
        name = str(((stmt.get("recordDetails") or {}).get("name") or "")).strip()
    return name or (report.get("lei") or f"{report.get('scheme')}:{report.get('id')}" or "the subject")


def build_notes(
    report: dict[str, Any],
    *,
    saved: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> list[Note]:
    """One note per register that holds a disputed value, in the order the
    signal's items name them. An empty list means nothing qualifies."""
    items = qualifying_items(report)
    if not items:
        return []
    bods = report.get("bods") or []
    by_id = statement_index(bods)
    generated = (now or datetime.now(UTC)).replace(microsecond=0).isoformat()
    notes: dict[str, Note] = {}
    for item in items:
        fld = str(item.get("field"))
        noun = FIELD_NOUNS.get(fld, fld.replace("_", " "))
        sources = [str(s) for s in item["sources"]]
        ids = [str(i) for i in item["statement_ids"]]
        said = item.get("said") if isinstance(item.get("said"), list) and len(item.get("said") or []) == 2 else None
        values = said or [item.get("values", ["", ""])[0], item.get("values", ["", ""])[1]]
        stmts = [by_id.get(i) for i in ids]
        for me in (0, 1):
            source_id = sources[me]
            if source_id not in ADDRESSEE_SOURCES:
                continue
            other = 1 - me
            # The statement's own stamped id outranks the item's label when
            # both are present and agree; the item's source is what the
            # comparison keyed on.
            stmt = stmts[me]
            if stmt is not None and source_id_of(stmt) not in (None, source_id):
                stmt = None
            disc = Discrepancy(
                field=fld,
                field_noun=noun,
                register=_side(source_id, values[me], stmt),
                other=_side(sources[other], values[other], stmts[other]),
            )
            note = notes.get(source_id)
            if note is None:
                jurisdiction = None if source_id == "gleif" else _country_of(source_id)
                note = Note(
                    source_id=source_id,
                    register_name=SOURCE_NAMES.get(source_id, source_id),
                    jurisdiction=jurisdiction,
                    subject_name=_subject_name(report, stmt),
                    lei=report.get("lei"),
                    identifiers=_subject_identifiers(stmt),
                    record_url=disc.register.record_url
                    or (GLEIF_RECORD_URL.format(lei=report["lei"]) if source_id == "gleif" and report.get("lei") else None),
                    discrepancies=[],
                    route=route_for(source_id, jurisdiction),
                    saved=saved,
                    generated_at=generated,
                )
                notes[source_id] = note
            note.discrepancies.append(disc)
    return list(notes.values())


# ---------------------------------------------------------------------------
# Wording guard
# ---------------------------------------------------------------------------


def wording_issues(text: str) -> list[str]:
    """Every knowability banned word a text contains — the fixed sentences
    of a note must contain none (a test pins it). Register values are the
    registers' own and are not guarded here."""
    words = set(_WORD_RE.findall(text.lower()))
    return sorted(w for w in knowability.BANNED_VOCABULARY if w in words)


FIXED_TEXT: tuple[str, ...] = (
    TITLE,
    SCOPE_SENTENCE,
    PURPOSE_SENTENCE,
    NO_ROUTE_SENTENCE,
    NO_SAVED_SENTENCE,
    GLEIF_CHALLENGE["note"],
    *REPORTER_FIELDS,
)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _day(iso: str | None) -> str:
    if not iso:
        return "not stated"
    return format_date(str(iso)[:10])


def _stamp(iso: str | None) -> str:
    """A retrieval moment, to the minute, UTC."""
    if not iso:
        return "not stated"
    s = str(iso)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return s
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC)
    return f"{format_date(dt.date().isoformat())} {dt.strftime('%H:%M')} UTC"


def _licence_phrase(side: Side) -> str:
    if side.licence_name and side.licence:
        return f"{side.licence_name} ({side.licence})"
    return side.licence or side.licence_name or "licence not recorded"


def _side_lines(label: str, side: Side) -> list[str]:
    lines = [
        f"- **{label}:** {side.source_name} records *{side.value}*.",
        f"  Stated {_day(side.statement_date)}; retrieved by OpenCheck {_stamp(side.retrieved_at)}; "
        f"licence {_licence_phrase(side)}.",
    ]
    if side.record_url:
        lines.append(f"  Record: {side.record_url}")
    return lines


def _route_lines(note: Note) -> list[str]:
    r = note.route
    lines: list[str] = []
    if r.url:
        lines.append(f"- Reporting route: {r.url}")
    if r.company_register_url and r.company_register_url != r.url:
        lines.append(f"- Company register route: {r.company_register_url}")
    if r.note:
        lines.append(f"- {r.note}")
    if not lines:
        lines.append(f"- {NO_ROUTE_SENTENCE}")
    return lines


def _footer_lines(note: Note) -> list[str]:
    saved = note.saved
    if saved:
        return [
            f"OpenCheck saved report: {saved.get('report_url', '')}",
            f"SHA-256 of the saved record: `{saved.get('content_hash', '')}`",
            f"Run completed {_stamp(saved.get('run_completed_at'))}; saved {_stamp(saved.get('saved_at'))}.",
        ]
    return [NO_SAVED_SENTENCE, f"Generated by OpenCheck {_stamp(note.generated_at)}."]


def render_markdown(notes: list[Note]) -> str:
    """One Markdown document, one note per register, separated by a rule."""
    parts: list[str] = []
    for note in notes:
        lines = [f"# {TITLE}", "", f"**To:** {note.register_name}", ""]
        lines += [SCOPE_SENTENCE, "", PURPOSE_SENTENCE, ""]
        lines += ["## Subject", "", f"- Name: {note.subject_name}"]
        if note.lei:
            lines.append(f"- LEI: {note.lei}")
        for ident in note.identifiers:
            lines.append(f"- {ident['scheme']}: {ident['id']}")
        if note.record_url:
            lines.append(f"- Record on this register: {note.record_url}")
        lines.append("")
        lines += ["## Discrepancies", ""]
        for n, d in enumerate(note.discrepancies, 1):
            lines.append(f"### {n}. {d.field_noun.capitalize()}")
            lines.append("")
            lines += _side_lines("This register", d.register)
            lines += _side_lines("Conflicting record", d.other)
            lines.append("")
        lines += ["## Where to send this", ""] + _route_lines(note) + [""]
        lines += ["## Reported by", ""]
        lines += [f"- {f}: ________________________" for f in REPORTER_FIELDS]
        lines.append("")
        lines += ["---", ""] + _footer_lines(note)
        parts.append("\n".join(lines).rstrip() + "\n")
    return "\n\n---\n\n".join(parts)


_CSS = """
@page { size: A4; margin: 18mm 18mm 20mm 18mm; }
body { font-family: "DM Sans", Helvetica, Arial, sans-serif; font-size: 10.5pt; color: #191d23; line-height: 1.4; }
h1 { font-family: Bitter, Georgia, serif; font-size: 18pt; margin: 0 0 4pt 0; }
h2 { font-size: 11.5pt; margin: 14pt 0 4pt 0; border-bottom: 1px solid #cfd6f5; padding-bottom: 2pt; }
h3 { font-size: 10.5pt; margin: 8pt 0 2pt 0; }
p, li { margin: 0 0 3pt 0; }
ul { padding-left: 14pt; margin: 0; }
.to { font-size: 12pt; margin-bottom: 8pt; }
.meta { color: #595959; font-size: 9pt; }
.note + .note { page-break-before: always; }
.footer { margin-top: 14pt; border-top: 1px solid #cfd6f5; padding-top: 6pt; font-size: 9pt; color: #595959; }
code { font-family: Menlo, monospace; font-size: 8.5pt; }
"""


def _e(text: Any) -> str:
    return html.escape(str(text), quote=True)


def _link(url: str) -> str:
    return f'<a href="{_e(url)}">{_e(url)}</a>'


def _side_html(label: str, side: Side) -> str:
    rec = f"<br><span class=\"meta\">Record: {_link(side.record_url)}</span>" if side.record_url else ""
    return (
        f"<li><strong>{_e(label)}:</strong> {_e(side.source_name)} records <em>{_e(side.value)}</em>."
        f"<br><span class=\"meta\">Stated {_e(_day(side.statement_date))}; retrieved by OpenCheck "
        f"{_e(_stamp(side.retrieved_at))}; licence {_e(_licence_phrase(side))}.</span>{rec}</li>"
    )


def render_html(notes: list[Note]) -> str:
    """The same notes as a tagged-PDF-ready HTML document, one page each."""
    sections: list[str] = []
    for note in notes:
        subject = [f"<li>Name: {_e(note.subject_name)}</li>"]
        if note.lei:
            subject.append(f"<li>LEI: {_e(note.lei)}</li>")
        subject += [f"<li>{_e(i['scheme'])}: {_e(i['id'])}</li>" for i in note.identifiers]
        if note.record_url:
            subject.append(f"<li>Record on this register: {_link(note.record_url)}</li>")
        discs = []
        for n, d in enumerate(note.discrepancies, 1):
            discs.append(
                f"<h3>{n}. {_e(d.field_noun.capitalize())}</h3><ul>"
                + _side_html("This register", d.register)
                + _side_html("Conflicting record", d.other)
                + "</ul>"
            )
        r = note.route
        routes = []
        if r.url:
            routes.append(f"<li>Reporting route: {_link(r.url)}</li>")
        if r.company_register_url and r.company_register_url != r.url:
            routes.append(f"<li>Company register route: {_link(r.company_register_url)}</li>")
        if r.note:
            routes.append(f"<li>{_e(r.note)}</li>")
        if not routes:
            routes.append(f"<li>{_e(NO_ROUTE_SENTENCE)}</li>")
        reporter = "".join(f"<li>{_e(f)}: ________________________</li>" for f in REPORTER_FIELDS)
        if note.saved:
            s = note.saved
            footer = (
                f"OpenCheck saved report: {_link(str(s.get('report_url', '')))}<br>"
                f"SHA-256 of the saved record: <code>{_e(s.get('content_hash', ''))}</code><br>"
                f"Run completed {_e(_stamp(s.get('run_completed_at')))}; saved {_e(_stamp(s.get('saved_at')))}."
            )
        else:
            footer = f"{_e(NO_SAVED_SENTENCE)}<br>Generated by OpenCheck {_e(_stamp(note.generated_at))}."
        sections.append(
            f'<section class="note"><h1>{_e(TITLE)}</h1>'
            f'<p class="to"><strong>To:</strong> {_e(note.register_name)}</p>'
            f"<p>{_e(SCOPE_SENTENCE)}</p><p>{_e(PURPOSE_SENTENCE)}</p>"
            f"<h2>Subject</h2><ul>{''.join(subject)}</ul>"
            f"<h2>Discrepancies</h2>{''.join(discs)}"
            f"<h2>Where to send this</h2><ul>{''.join(routes)}</ul>"
            f"<h2>Reported by</h2><ul>{reporter}</ul>"
            f'<div class="footer">{footer}</div></section>'
        )
    return (
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        f"<title>{_e(TITLE)}</title><style>{_CSS}</style></head><body>"
        + "".join(sections)
        + "</body></html>"
    )


def build_discrepancy_note(
    report: dict[str, Any],
    *,
    fmt: str = "markdown",
    saved: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> str | bytes | None:
    """The note in the format asked for, or ``None`` when nothing qualifies.
    ``pdf`` renders through the report PDF toolchain and raises its
    ``PdfUnavailable`` where WeasyPrint is missing."""
    notes = build_notes(report, saved=saved, now=now)
    if not notes:
        return None
    if fmt == "pdf":
        from .reporting.pdf import render_html_to_pdf

        return render_html_to_pdf(render_html(notes))
    if fmt == "html":
        return render_html(notes)
    return render_markdown(notes)


def summary(notes: list[Note]) -> dict[str, Any]:
    """The MCP tool's companion to the Markdown: who is addressed and on
    what, so a caller can route the note without parsing it."""
    return {
        "notes": [
            {
                "to": n.source_id,
                "register": n.register_name,
                "jurisdiction": n.jurisdiction,
                "fields": [d.field for d in n.discrepancies],
                "route": n.route.url,
                "company_register_route": n.route.company_register_url,
                "saved_report": (n.saved or {}).get("report_url"),
            }
            for n in notes
        ]
    }
