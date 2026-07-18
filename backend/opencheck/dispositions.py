"""Analyst claim dispositions — the human half of the audit trail.

The narrative pipeline produces machine-grounded claims; this module records
what the *analyst* did with each one (accepted / disputed / needs review, plus
an optional comment). A disposition set is bound to one exact narrative via a
deterministic ``run_id`` — regenerating the narrative produces a new run and a
fresh, empty disposition sheet, so a sign-off can never silently apply to text
the analyst didn't read.

Persistence is filesystem JSON under ``data/dispositions/<LEI>/<run_id>.json``
(same ``data_root()`` convention as the adapter cache): durable across
restarts, trivially inspectable in an audit, and no new dependencies.
Writes are whole-record overwrites (last-write-wins; single-analyst v1 —
``reviewer`` is reserved for when identity lands).
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from .cache import data_root

# 3.10-compatible alias for datetime.UTC.
UTC = timezone.utc

DispositionStatus = Literal["accepted", "disputed", "needs_review"]

_LEI_RE = re.compile(r"^[A-Z0-9]{20}$")
_RUN_ID_RE = re.compile(r"^[0-9a-f]{16}$")

MAX_COMMENT_LENGTH = 2000


class ClaimDisposition(BaseModel):
    """One analyst decision about one narrative claim."""

    claim_id: str
    status: DispositionStatus
    comment: str | None = Field(default=None, max_length=MAX_COMMENT_LENGTH)
    decided_at: datetime | None = None  # server-set; preserved when unchanged


class DispositionRecord(BaseModel):
    """The full disposition sheet for one narrative run of one LEI."""

    lei: str
    run_id: str  # binds the decisions to one exact narrative (see compute_run_id)
    prompt_version: str = ""
    model: str = ""
    reviewer: str | None = None  # reserved for v2 (reviewer identity)
    dispositions: list[ClaimDisposition] = Field(default_factory=list)
    updated_at: datetime | None = None  # server-set on save


def compute_run_id(
    lei: str | None,
    prompt_version: str,
    model: str,
    summary: str,
    claim_texts: list[str],
) -> str:
    """Deterministic id for one generated narrative.

    Hashes exactly what the analyst signs off on (the summary and claim texts)
    plus the provenance that produced it. Re-serving the same narrative yields
    the same id; a regenerate (different text) yields a new id, so stored
    dispositions can never attach to words the analyst didn't review.
    """
    h = hashlib.sha256()
    for part in [lei or "", prompt_version, model, summary, *sorted(claim_texts)]:
        h.update(part.encode("utf-8"))
        h.update(b"\x00")  # unambiguous field separator
    return h.hexdigest()[:16]


def validate_keys(lei: str, run_id: str) -> None:
    """Reject anything that isn't a well-formed (and so path-safe) key pair."""
    if not _LEI_RE.match(lei):
        raise ValueError("lei must be a 20-character alphanumeric LEI")
    if not _RUN_ID_RE.match(run_id):
        raise ValueError("run_id must be 16 lowercase hex characters")


def _record_path(lei: str, run_id: str) -> Path:
    return data_root() / "dispositions" / lei / f"{run_id}.json"


def load_dispositions(lei: str, run_id: str) -> DispositionRecord | None:
    """Return the stored record for ``(lei, run_id)``, or ``None``."""
    validate_keys(lei, run_id)
    path = _record_path(lei, run_id)
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return DispositionRecord.model_validate(json.load(fh))


def save_dispositions(record: DispositionRecord) -> DispositionRecord:
    """Persist ``record``, stamping ``updated_at`` and per-claim ``decided_at``.

    ``decided_at`` is preserved from the previously stored record when a claim's
    (status, comment) pair is unchanged — so the timestamp reflects when the
    decision was actually made, not when the sheet was last touched.
    """
    validate_keys(record.lei, record.run_id)
    now = datetime.now(UTC)

    previous = load_dispositions(record.lei, record.run_id)
    prev_by_claim = {d.claim_id: d for d in previous.dispositions} if previous else {}

    stamped: list[ClaimDisposition] = []
    for d in record.dispositions:
        old = prev_by_claim.get(d.claim_id)
        if old is not None and old.status == d.status and old.comment == d.comment:
            decided_at = old.decided_at or now
        else:
            decided_at = now
        stamped.append(d.model_copy(update={"decided_at": decided_at}))

    out = record.model_copy(update={"dispositions": stamped, "updated_at": now})
    path = _record_path(out.lei, out.run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        fh.write(out.model_dump_json(indent=2))
    return out
