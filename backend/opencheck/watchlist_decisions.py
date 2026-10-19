"""watchlist_decisions — the review record: change → review → decision (pKYC stage D).

Moody's defensibility argument: a monitoring programme has to be able to say
*why A was reviewed and B was not*. The watchlist log says what changed, how
much it mattered (stage A), what the watcher expected (stage B) and when
OpenCheck last looked (stage C). This module records what someone **decided**
about an entry, and pins the evidence they decided on.

Stephen's decisions on the stage D ticket (18 Oct 2026):

- **A decision, not a disposition.** ``dispositions.py`` is the narrative
  claim sign-off (accepted / disputed / needs review per claim, keyed on a
  narrative run id). This is a different object with a different key; the
  word *decision* is used everywhere a reader sees it so the two cannot be
  confused.
- **Three closed statuses** (:data:`STATUSES`): *Reviewed, no action* /
  *Re-checked* / *Escalated*. The status is the reader's statement; it is
  never set automatically — choosing a fresh re-check as evidence does not
  make the status "re-checked".
- **Append-only.** A later decision on the same entry is a new row, never an
  edit; the entry's current decision is the latest and the history is kept.
  Nothing is deleted except with the list (stage C's rule — a list with
  decisions follows the same notice-then-delete).
- **The note** carries no personal data: stage B's rationale rule
  (``watchlist_context.validate_rationale``) is reused as it stands.
- **Evidence is opt-in, never automatic.** A saved report (Phases 216–218)
  of what was reviewed: the entry's own re-run while the server still holds
  it (``entry_run``), or a fresh re-check saved in the same action
  (``recheck``) — worded as evidence of *now*, pinned to the entry as *the
  evidence reviewed on …*. Every save goes through ``save_from_replay`` and
  the per-client saved-report quota; a refused save writes no decision.
- **No reviewer identity.** No accounts, so none: the record says a decision
  was taken by whoever held the token, when.

Words shared with the page are :data:`STATUS_LABELS` / :data:`STATUS_MEANINGS`
/ :data:`EVIDENCE_WORDS`, mirrored in ``frontend/src/lib/watchlist.ts`` and
pinned by ``tests/test_watchlist_decisions.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from . import watchlist_context as context_module

#: The three statuses, closed. Order is the order the page offers them.
REVIEWED_NO_ACTION = "reviewed_no_action"
RECHECKED = "rechecked"
ESCALATED = "escalated"
STATUSES: tuple[str, ...] = (REVIEWED_NO_ACTION, RECHECKED, ESCALATED)

STATUS_LABELS: dict[str, str] = {
    REVIEWED_NO_ACTION: "Reviewed, no action",
    RECHECKED: "Re-checked",
    ESCALATED: "Escalated",
}

STATUS_MEANINGS: dict[str, str] = {
    REVIEWED_NO_ACTION: "Looked at; nothing to do.",
    RECHECKED: "Looked at and the company was re-run.",
    ESCALATED: "Routed to whoever decides; what they decided is not recorded here.",
}

#: What a pinned saved report is *of*.
EVIDENCE_ENTRY_RUN = "entry_run"
EVIDENCE_RECHECK = "recheck"
EVIDENCE_KINDS: tuple[str, ...] = (EVIDENCE_ENTRY_RUN, EVIDENCE_RECHECK)

EVIDENCE_WORDS: dict[str, str] = {
    EVIDENCE_ENTRY_RUN: "the re-run that produced this entry",
    EVIDENCE_RECHECK: "a fresh re-check made when the decision was recorded",
    "none": "no evidence pinned",
    "expired": "the pinned saved report has since expired",
    "run_not_held": "That re-run is no longer held (a finished run is held for 15 minutes); re-check and keep instead.",
}

#: The Atom category scheme for a decided entry.
ATOM_SCHEME = "https://opencheck.world/watchlist/decision"

#: How long the server holds a finished run — the replay window the
#: ``entry_run`` evidence depends on. Mirrors ``lookup_replay``'s TTL.
RUN_HELD_S = 15 * 60


class DecisionError(ValueError):
    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field
        self.message = message


def validate_status(value: Any) -> str:
    if value not in STATUSES:
        raise DecisionError("status", f"A decision is one of: {', '.join(STATUSES)}.")
    return str(value)


def validate_note(value: Any) -> str | None:
    """Stage B's rationale rule, unchanged: one line about the company, no
    e-mail, no foreign link, no personal-code-shaped digit run."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return context_module.validate_rationale(value)
    except context_module.ContextError as exc:
        raise DecisionError("note", exc.message.replace("rationale", "note")) from exc


def validate_evidence(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if value not in EVIDENCE_KINDS:
        raise DecisionError("evidence", f"Evidence is one of: {', '.join(EVIDENCE_KINDS)}, or none.")
    return str(value)


def _parse(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def run_still_held(entry: dict[str, Any], *, now: datetime | None = None) -> bool:
    """Whether the entry's own re-run can still be saved: it recorded a
    ``run_completed_at`` and that was within the replay window. A guess the
    page uses to word the evidence option; the save itself is the proof."""
    done = _parse(entry.get("run_completed_at"))
    if done is None:
        return False
    return ((now or datetime.now(UTC)) - done).total_seconds() < RUN_HELD_S


def decision_sentence(decision: dict[str, Any]) -> str:
    """The line on an entry, in the feed's words: "Decision: reviewed, no
    action — 18 Oct 2026; evidence: saved report {id} ({kind})"."""
    label = STATUS_LABELS.get(decision.get("status", ""), decision.get("status", ""))
    day = str(decision.get("decided_at") or "")[:10]
    out = f"Decision: {label.lower()} — {day}"
    if decision.get("note"):
        out += f"; note: {decision['note']}"
    report = decision.get("saved_report_id")
    if report:
        kind = EVIDENCE_WORDS.get(decision.get("evidence_kind") or "", "")
        out += f"; evidence: saved report {report}" + (f" ({kind})" if kind else "")
        if decision.get("report") and decision["report"].get("available") is False:
            out += f" — {EVIDENCE_WORDS['expired']}"
    return out + "."


def is_undecided_material(entry: dict[str, Any]) -> bool:
    """The "Needs review" filter: material (recheck or escalate) and no decision."""
    from . import watchlist_materiality as materiality

    return materiality.is_material(entry.get("materiality") or "") and not entry.get("decision")


# ---------------------------------------------------------------------------
# The Markdown record
# ---------------------------------------------------------------------------


def record_markdown(
    *,
    watches: list[dict[str, Any]],
    entries: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    headline: Any,
    list_label: str,
) -> str:
    """The per-list review record as Markdown — what a reader hands to an
    auditor. One section per company (and one for the list itself), one
    line per decision with its entry's headline and materiality, the status,
    the note, and the pinned saved report's id and SHA-256 when it still
    exists. Reads no clock and runs no pipeline: every date is one the store
    recorded; the generated-at line is the newest decision, so the same
    record renders byte-identical on any day."""
    by_entry: dict[int, dict[str, Any]] = {e["id"]: e for e in entries}
    by_lei: dict[str, list[dict[str, Any]]] = {}
    for d in decisions:
        by_lei.setdefault(d.get("lei") or "", []).append(d)
    names = {w["lei"]: w.get("legal_name") or w["lei"] for w in watches}
    latest = max((d.get("decided_at") or "" for d in decisions), default=None)
    lines = [
        f"# Review record — {list_label}",
        "",
        f"{len(decisions)} decision{'s' if len(decisions) != 1 else ''} on "
        f"{len({d.get('entry_id') for d in decisions})} entr{'ies' if len({d.get('entry_id') for d in decisions}) != 1 else 'y'}"
        + (f"; latest {latest[:10]}" if latest else "")
        + ". Each decision was recorded by whoever held the list token, at the time shown; a change of mind is a later line, never an edit.",
        "",
    ]
    for lei in sorted(by_lei, key=lambda k: (k == "", names.get(k, k))):
        title = "This list" if lei == "" else f"{names.get(lei, lei)} — LEI {lei}"
        lines += [f"## {title}", ""]
        for d in sorted(by_lei[lei], key=lambda x: (x.get("decided_at") or "", x.get("id") or 0)):
            entry = by_entry.get(int(d["entry_id"])) if d.get("entry_id") is not None else None
            head = headline(entry) if entry else f"entry {d.get('entry_id')}"
            tier = (entry or {}).get("materiality") or "—"
            line = f"- {str(d.get('decided_at') or '')[:16].replace('T', ' ')} UTC · **{STATUS_LABELS.get(d['status'], d['status'])}** · {head} (materiality: {tier})"
            if d.get("note"):
                line += f" · note: {d['note']}"
            if d.get("saved_report_id"):
                rep = d.get("report") or {}
                kind = EVIDENCE_WORDS.get(d.get("evidence_kind") or "", "")
                line += f" · evidence: saved report `{d['saved_report_id']}`"
                if rep.get("content_hash"):
                    line += f" sha256 `{rep['content_hash']}`"
                if rep.get("available") is False:
                    line += f" ({EVIDENCE_WORDS['expired']})"
                elif kind:
                    line += f" ({kind})"
            lines.append(line)
        lines.append("")
    if not decisions:
        lines += ["No decisions have been recorded on this list.", ""]
    return "\n".join(lines)
