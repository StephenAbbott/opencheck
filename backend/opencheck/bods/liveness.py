"""Register liveness — one vocabulary for "does the register still treat this
entity as live?", applied by every adapter the same way.

Why this exists (Phase 151)
---------------------------

Nearly every register OpenCheck reads publishes a company status — Companies
House ``company_status``, OpenCorporates ``current_status`` and
``dissolution_date``, GLEIF ``entity.status``, CVR ``virksomhedOphoersdato``,
ARES ``dissolved``/``liquidation``, MCA ``CompanyStatus``, and so on. Before
this module the mappers handled it in six different ways: five set
``dissolutionDate`` (three of them to the literal string ``"unknown"`` and one
to JSON ``null``, neither of which the BODS schema allows — the field MUST be
``YYYY-MM-DD``), one recorded a ``commenting`` annotation and nothing else,
and the other twenty-odd read the status into the bundle and dropped it. An
entity that Companies House has struck off therefore arrived in the results
page with no trace of it, next to a GLEIF record that still said ACTIVE, and
nothing could compare the two because nothing had written either down.

This module is the single path. A mapper classifies the register's own words
into one of four classes and calls :func:`apply_register_status`; the
statement then carries:

* ``recordDetails.dissolutionDate`` — **only** when the class is
  ``terminal`` *and* the register gave a date. Partial dates are rounded per
  the BODS dates guidance and the rounding is annotated (the existing
  ``date_rounding_annotation``). No date → no field; never a sentinel.
* a ``commenting`` annotation on ``/recordDetails`` whose description follows
  a fixed grammar — the register's class, the date if any, and the register's
  verbatim status label — so that a consumer (and Phase C's consistency
  check) can read the class back with :func:`read_register_status` without
  a second copy of every register's vocabulary. Writer and reader live here
  together and a round-trip test pins the grammar.

The four classes
----------------

``live``      — the register treats the entity as existing and operating.
``pending``   — a terminal process is under way but not complete
                (under liquidation, being struck off, in bankruptcy,
                proposal to strike off). Not dissolved; must not set
                ``dissolutionDate``; worth showing.
``terminal``  — dissolved, struck off, cancelled, liquidated, amalgamated
                into another entity, deregistered. The entity no longer
                exists as a legal person in that register's eyes.
``unknown``   — the register said something this mapper does not classify,
                or said nothing. Nothing is written for ``unknown``: absence
                of a status is not a finding, and an unclassified status must
                not be guessed either way (the GEMI precedent).

Absence of an annotation therefore means "this source did not say", which is
different from ``live`` ("this source said it is live") — the distinction
Phase C needs to tell "register dissolved, GLEIF active" from "register
dissolved, GLEIF silent".
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Literal

from .annotations import (
    annotate,
    commenting,
    date_rounding_annotation,
    pointer,
    round_partial_date,
)

LivenessClass = Literal["live", "pending", "terminal", "unknown"]

LIVE: LivenessClass = "live"
PENDING: LivenessClass = "pending"
TERMINAL: LivenessClass = "terminal"
UNKNOWN: LivenessClass = "unknown"

#: The word the annotation uses for each class — a controlled term, so the
#: reader can key on it. The register's own label travels separately, verbatim.
_CLASS_WORD: dict[str, str] = {
    LIVE: "active",
    PENDING: "in a terminal process",
    TERMINAL: "dissolved",
}
_WORD_CLASS: dict[str, LivenessClass] = {v: k for k, v in _CLASS_WORD.items()}  # type: ignore[misc]

#: Pointer every register-status annotation targets. ``/recordDetails`` rather
#: than ``/recordDetails/dissolutionDate`` because the latter does not exist
#: on a live entity and ``validate_annotations`` requires pointers to resolve.
STATUS_TARGET = pointer("recordDetails")

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Grammar, kept deliberately rigid:
#   "<Source> records this entity as <word>[ since <YYYY-MM-DD>]
#    [ — register status: “<raw>”]."
_DESCRIPTION_RE = re.compile(
    r"^(?P<source>.+?) records this entity as (?P<word>active|in a terminal process|dissolved)"
    r"(?: since (?P<date>\d{4}-\d{2}-\d{2}))?"
    r"(?: — register status: “(?P<raw>.*)”)?\.$",
    re.S,
)


def classify(
    raw: str | None,
    *,
    live: Iterable[str] = (),
    pending: Iterable[str] = (),
    terminal: Iterable[str] = (),
) -> LivenessClass:
    """Classify a register's status label against per-register vocabularies.

    Matching is case-insensitive on the whole label after whitespace
    collapsing; ``terminal`` and ``pending`` are checked before ``live`` so a
    register whose "live" vocabulary is a substring of a terminal one
    ("active" / "inactive") is handled by listing exact labels, not prefixes.
    Anything unmatched is ``unknown`` — never guessed.
    """
    text = " ".join(str(raw or "").split()).lower()
    if not text:
        return UNKNOWN
    norm = lambda vocab: {" ".join(v.split()).lower() for v in vocab}  # noqa: E731
    if text in norm(terminal):
        return TERMINAL
    if text in norm(pending):
        return PENDING
    if text in norm(live):
        return LIVE
    return UNKNOWN


def status_description(
    source_label: str,
    liveness: LivenessClass,
    *,
    since: str | None = None,
    raw: str | None = None,
) -> str:
    """Render the annotation text for a classified status (see grammar above)."""
    if liveness not in _CLASS_WORD:
        raise ValueError(f"no annotation for liveness {liveness!r}")
    text = f"{source_label} records this entity as {_CLASS_WORD[liveness]}"
    if since:
        text += f" since {since}"
    raw_text = " ".join(str(raw or "").split())
    if raw_text:
        text += f" — register status: “{raw_text}”"
    return text + "."


def apply_register_status(
    stmt: dict[str, Any],
    *,
    source_label: str,
    liveness: LivenessClass,
    raw: str | None = None,
    since: str | None = None,
    creation_date: str | None = None,
) -> dict[str, Any]:
    """Write a register's view of the entity's liveness onto its statement.

    ``since`` is the date the status took effect (a dissolution date for
    ``terminal``, the start of liquidation for ``pending``); partial dates are
    accepted and rounded with an annotation. ``dissolutionDate`` is set only
    for ``terminal`` with a usable date. ``unknown`` writes nothing.

    Idempotent per statement: a second call replaces the earlier status
    annotation rather than stacking one.
    """
    if liveness == UNKNOWN:
        return stmt
    if liveness not in _CLASS_WORD:
        raise ValueError(f"unknown liveness class {liveness!r}")

    record_details = stmt.setdefault("recordDetails", {})
    iso, precision = round_partial_date(since)
    if iso and not _ISO_DATE.match(iso):
        iso, precision = None, None  # not a date we can state
    if liveness == TERMINAL and iso:
        record_details["dissolutionDate"] = iso
        if precision:
            annotate(
                stmt,
                date_rounding_annotation(
                    pointer("recordDetails", "dissolutionDate"),
                    str(since),
                    precision,
                    creation_date=creation_date,
                ),
            )
    elif liveness != TERMINAL:
        # A pending or live status must never leave a stale dissolution date
        # from an earlier, wrongly-classified call.
        record_details.pop("dissolutionDate", None)

    description = status_description(source_label, liveness, since=iso, raw=raw)
    existing = stmt.get("annotations") or []
    stmt["annotations"] = [a for a in existing if not _is_status_annotation(a)]
    annotate(stmt, commenting(STATUS_TARGET, description, creation_date=creation_date))
    return stmt


def _is_status_annotation(annotation: dict[str, Any]) -> bool:
    return (
        annotation.get("motivation") == "commenting"
        and annotation.get("statementPointerTarget") == STATUS_TARGET
        and bool(_DESCRIPTION_RE.match(str(annotation.get("description") or "")))
    )


def read_register_status(stmt: dict[str, Any]) -> dict[str, Any] | None:
    """Read back what :func:`apply_register_status` wrote, or ``None``.

    Returns ``{"source": ..., "liveness": ..., "since": ..., "raw": ...}``.
    A ``dissolutionDate`` with no status annotation (a bulk dataset that
    carried the field verbatim, or a pre-Phase-151 cache entry) is reported
    as ``terminal`` with ``source`` taken from the statement's own source
    block, so a consumer sees one shape either way.
    """
    for annotation in stmt.get("annotations") or []:
        if not _is_status_annotation(annotation):
            continue
        m = _DESCRIPTION_RE.match(str(annotation.get("description") or ""))
        assert m is not None  # _is_status_annotation matched the same regex
        return {
            "source": m.group("source"),
            "liveness": _WORD_CLASS[m.group("word")],
            "since": m.group("date"),
            "raw": m.group("raw"),
        }
    dissolution = (stmt.get("recordDetails") or {}).get("dissolutionDate")
    if isinstance(dissolution, str) and _ISO_DATE.match(dissolution):
        return {
            "source": str(((stmt.get("source") or {}).get("description")) or ""),
            "liveness": TERMINAL,
            "since": dissolution,
            "raw": None,
        }
    return None
