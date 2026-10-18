"""watchlist_context — what the watcher expected (pKYC stage B).

Moody's "reuse onboarding context". A watch may carry four optional fields
written by whoever holds the list token: the jurisdiction the watcher
expects the company to be in, the LEI they expect to see as GLEIF's
*ultimate* parent, a one-line rationale for watching, and the saved report
that was the onboarding record. A later entry then compares what changed
with what was expected — "GLEIF ultimate parent: X → Y — your watch
expected X" — and the watch row says at once when the baseline already
disagrees with a freshly set expectation.

Rules (Stephen's answers on the stage B ticket, 17 Oct 2026):

- **Two expectations, no more**: jurisdiction and one parent LEI. The parent
  is compared with GLEIF's ``ultimate_parent_lei`` always, and with
  ``direct_parent_lei`` only when the expected LEI appears on either side of
  that change (a one-layer group's direct parent *is* its ultimate parent).
- **Graded once, kept.** :func:`annotate` runs at entry-write time and the
  result is stored on the change; editing the context afterwards does not
  touch an entry. The entry is the record of the day.
- **Setting context writes no entry** — a watcher's belief changing is not a
  change to the company. A baseline that already contradicts the new
  expectation is said on the watch row (:func:`baseline_disagrees`),
  computed on read, never stored.
- **The materiality tier is unchanged** by a contradicted expectation. The
  contradiction is said beside the chip, not folded into it.
- **No personal data.** The columns hold a jurisdiction code, an LEI, a
  report id and one line of free text. The free text is capped at
  :data:`RATIONALE_MAX`, single-line, and refused when it carries an e-mail
  address, a URL other than an OpenCheck saved-report link, or a run of
  eleven or more digits (a personal code's shape — the Estonian scrub's
  rule). The form says "about the company, not a person"; this is the
  enforcement.
- **A pinned report must be of the same LEI, on this instance, unexpired.**
  Only the id is stored; the report's own expiry governs it, and its date is
  read on request (``saved_at``), never copied.
- **No names stored.** The expected parent's name is resolved on read from
  the Golden Copy mirror (:func:`expected_parent_name`), the
  ``name_children`` arrangement — a rename cannot stale it.

The wording the feed and the page share is :data:`EXPECTATION_WORDS`,
mirrored in ``frontend/src/lib/watchlist.ts`` and pinned by
``tests/test_watchlist_context.py``.
"""

from __future__ import annotations

import re
from typing import Any

from . import identifiers
from .bods.validator import is_iso_jurisdiction_code

#: The four fields a watcher may set, in the order the page shows them.
CONTEXT_FIELDS: tuple[str, ...] = (
    "expected_jurisdiction",
    "expected_parent_lei",
    "rationale",
    "onboarding_report_id",
)

#: The rationale's cap — one line, a sentence or two.
RATIONALE_MAX = 280

#: The clause after a change's sentence. ``{expected}`` is the expected
#: value as the watcher wrote it (a jurisdiction code or an LEI, never a
#: name — the feed is plain text and a name could go stale).
EXPECTATION_WORDS: dict[str, str] = {
    "contradicted": "your watch expected {expected}",
    "met": "as your watch expected",
}

#: The Atom category scheme for an entry carrying a contradicted expectation,
#: beside the tier and materiality categories.
ATOM_SCHEME = "https://opencheck.world/watchlist/expectation"
ATOM_TERM_CONTRADICTED = "contradicted"

#: Which change shapes an expectation compares with.
PARENT_FIELDS: frozenset[str] = frozenset({"ultimate_parent_lei", "direct_parent_lei"})


class ContextError(ValueError):
    """A context field that cannot be stored, naming the field."""

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field
        self.message = message


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")
_URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
_SAVED_REPORT_URL_RE = re.compile(
    r"^https?://(?:[a-z0-9-]+\.)*opencheck\.world/(?:report|share/saved)/[A-Za-z0-9_-]+/?$",
    re.IGNORECASE,
)
_LONG_DIGITS_RE = re.compile(r"\d(?:[ .-]?\d){10,}")


def validate_jurisdiction(value: Any) -> str:
    """ISO 3166-1 alpha-2 or 3166-2, upper-cased. Anything else is refused —
    a code OpenCheck cannot compare with GLEIF's is not an expectation."""
    if not isinstance(value, str):
        raise ContextError("expected_jurisdiction", "A jurisdiction is a code such as GB or US-DE.")
    code = value.strip().upper()
    if not is_iso_jurisdiction_code(code):
        raise ContextError(
            "expected_jurisdiction",
            f"{value.strip()!r} is not an ISO 3166 code (GB, DE, US-DE).",
        )
    return code


def validate_parent_lei(value: Any) -> str:
    """An LEI by shape and check digits. Not resolved against GLEIF: a lapsed
    or unknown LEI is still the watcher's claim about who the parent is."""
    if not isinstance(value, str):
        raise ContextError("expected_parent_lei", "The expected parent is an LEI.")
    lei = identifiers.normalise_lei(value)
    if not identifiers.is_valid_lei(lei):
        raise ContextError(
            "expected_parent_lei",
            "That is not a valid LEI (20 characters, ISO 17442 check digits).",
        )
    return lei


def validate_rationale(value: Any) -> str:
    """One line about the company, never about a person."""
    if not isinstance(value, str):
        raise ContextError("rationale", "The rationale is one line of text.")
    text = " ".join(value.split())
    if not text:
        raise ContextError("rationale", "The rationale is empty.")
    if len(text) > RATIONALE_MAX:
        raise ContextError(
            "rationale", f"The rationale is {len(text)} characters; the most is {RATIONALE_MAX}."
        )
    if _EMAIL_RE.search(text):
        raise ContextError(
            "rationale", "The rationale cannot carry an e-mail address — it describes a company, not a person."
        )
    for m in _URL_RE.finditer(text):
        if not _SAVED_REPORT_URL_RE.match(m.group(0).rstrip(".,;)")):
            raise ContextError(
                "rationale",
                "The rationale cannot carry a link, other than an OpenCheck saved-report link.",
            )
    if _LONG_DIGITS_RE.search(text):
        raise ContextError(
            "rationale",
            "The rationale cannot carry a run of eleven or more digits — the shape of a personal code.",
        )
    return text


def validate_report(report_id: Any, lei: str, *, store: Any) -> dict[str, Any]:
    """The saved report named must exist on this instance, be unexpired and be
    a report of ``lei``. Returns its public meta; only the id is stored."""
    from . import saved_reports as sr

    if not isinstance(report_id, str) or not sr.REPORT_ID_RE.match(report_id.strip()):
        raise ContextError("onboarding_report_id", "That is not a saved report id.")
    rid = report_id.strip()
    if store is None:
        raise ContextError("onboarding_report_id", "Saved reports are not enabled on this instance.")
    try:
        meta, _ = sr.load_report(store, rid)
    except sr.SavedReportError as exc:
        raise ContextError("onboarding_report_id", str(exc)) from exc
    if (meta.get("lei") or "").upper() != lei.upper():
        raise ContextError(
            "onboarding_report_id",
            f"Saved report {rid} is a report of {meta.get('lei')}, not of this company.",
        )
    return meta


def validate(fields: dict[str, Any], lei: str, *, report_store: Any) -> dict[str, str | None]:
    """Every field present in ``fields`` validated — ``None`` clears, anything
    else must pass its rule. Unknown keys are refused. The result holds only
    the keys given, so a caller can leave the others alone."""
    out: dict[str, str | None] = {}
    for key, value in fields.items():
        if key not in CONTEXT_FIELDS:
            raise ContextError(key, "Not a context field.")
        if value is None:
            out[key] = None
        elif key == "expected_jurisdiction":
            out[key] = validate_jurisdiction(value)
        elif key == "expected_parent_lei":
            out[key] = validate_parent_lei(value)
        elif key == "rationale":
            out[key] = validate_rationale(value)
        else:
            out[key] = validate_report(value, lei, store=report_store)["report_id"]
    return out


# ---------------------------------------------------------------------------
# The comparison
# ---------------------------------------------------------------------------


def has_context(row: dict[str, Any]) -> bool:
    ctx = row.get("context") or {}
    return any(ctx.get(k) for k in CONTEXT_FIELDS)


def jurisdiction_met(actual: Any, expected: str) -> bool:
    """A country expectation is met by any of its subdivisions (``US`` by
    ``US-DE``); a subdivision expectation is exact."""
    if not isinstance(actual, str) or not actual:
        return False
    a, e = actual.strip().upper(), expected.strip().upper()
    if a == e:
        return True
    return len(e) == 2 and "-" in a and a.split("-", 1)[0] == e


def expectation_for(change: dict[str, Any], context: dict[str, Any] | None) -> dict[str, Any] | None:
    """The ``expectation`` block a change carries, or ``None`` when the
    watcher said nothing this change can be compared with."""
    if not context:
        return None
    kind = change.get("kind")
    new = change.get("new")
    if kind == "jurisdiction" or (kind == "gleif_field" and change.get("field") == "jurisdiction"):
        expected = context.get("expected_jurisdiction")
        if not expected:
            return None
        return {"field": "jurisdiction", "expected": expected, "met": jurisdiction_met(new, expected)}
    if kind == "gleif_field" and change.get("field") in PARENT_FIELDS:
        expected = context.get("expected_parent_lei")
        if not expected:
            return None
        field = change.get("field")
        if field == "direct_parent_lei" and expected not in (change.get("old"), new):
            return None
        return {"field": field, "expected": expected, "met": new == expected}
    return None


def annotate(changes: list[dict[str, Any]], row: dict[str, Any]) -> list[dict[str, Any]]:
    """Copies of ``changes`` with an ``expectation`` on each the watch's
    context can be compared with. A change with nothing to compare carries
    no key (absent, not ``False``)."""
    context = row.get("context") or {}
    out = []
    for c in changes:
        copy = dict(c)
        exp = expectation_for(copy, context)
        if exp is not None:
            copy["expectation"] = exp
        out.append(copy)
    return out


def contradicted(changes: list[dict[str, Any]] | None) -> bool:
    """True when at least one change carries ``met: false``."""
    return any((c.get("expectation") or {}).get("met") is False for c in changes or [])


def baseline_disagrees(row: dict[str, Any]) -> list[dict[str, Any]]:
    """What the watch's baseline says that its context did not expect, for
    the watch row — computed on read, never stored. Each item: ``field``,
    ``expected``, ``actual``. A baseline with no value for the field (GLEIF
    records no parent) is not a disagreement; it is said as such only when
    the baseline positively names something else."""
    context = row.get("context") or {}
    facts = row.get("gleif_facts") or {}
    out: list[dict[str, Any]] = []
    expected_j = context.get("expected_jurisdiction")
    if expected_j:
        actual = facts.get("jurisdiction") or row.get("jurisdiction")
        if actual and not jurisdiction_met(actual, expected_j):
            out.append({"field": "jurisdiction", "expected": expected_j, "actual": actual})
    expected_p = context.get("expected_parent_lei")
    if expected_p:
        actual_p = facts.get("ultimate_parent_lei")
        if actual_p and actual_p != expected_p:
            out.append({"field": "ultimate_parent_lei", "expected": expected_p, "actual": actual_p})
    return out


def expected_parent_name(lei: str | None) -> str | None:
    """The expected parent's current legal name from the Golden Copy
    mirror; ``None`` without a mirror or a row. Nothing is stored."""
    if not lei:
        return None
    from . import entity_pages as ep

    mirror = ep.get_store()
    if mirror is None:
        return None
    try:
        row = mirror.get_many([lei]).get(lei)
    except Exception:  # pragma: no cover - a mirror fault must not fail the page
        return None
    return getattr(row, "name", None) or None


def expectation_clause(expectation: dict[str, Any] | None) -> str:
    """The clause after a change's sentence, in the feed's words."""
    if not expectation:
        return ""
    if expectation.get("met"):
        return " — " + EXPECTATION_WORDS["met"] + "."
    return " — " + EXPECTATION_WORDS["contradicted"].format(expected=expectation.get("expected")) + "."
