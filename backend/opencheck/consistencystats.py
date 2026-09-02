"""consistencystats — how often independent sources agree, per field and pair.

Phase 152, shadow mode. The record-consistency check (``consistency.py``)
computes, for every entity that several sources described, whether they agree
on liveness, jurisdiction, founding date and one-per-entity identifiers.
Before any of that reaches the results page, the question is: *for which
(field, source A, source B) is disagreement rare enough to be informative?*
A pair-field where the sources disagree a third of the time is two sources
answering different questions and must be re-aligned or dropped; one where
they disagree once in a hundred lookups is a finding when it fires.

This is the counter that answers it, in the same shape as ``signalstats``:
in-process, aggregate only, bounded, failing soft, served at
``/consistencystats``. Keys are closed vocabularies — field names, adapter
ids, relation names — so no entity name, LEI, identifier value or date can
appear. The ``values`` an ``Item`` carries are never recorded here.

Exit criterion for Phase D (written on the Notion plan): a pair-field enters
the UI only if its measured ``disagree / (agree + disagree)`` is under 10 %
**and** it has fired at least once, after at least two weeks of traffic.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from typing import Any

from .consistency import RELATIONS, ConsistencyResult

log = logging.getLogger("opencheck.consistencystats")

_MAX_KEYS = 5_000


class _Counters:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.lookups = 0
        self.lookups_with_groups = 0
        #: (field, source_a, source_b, relation) → n; the pair is sorted so
        #: (gleif, companies_house) and (companies_house, gleif) are one key.
        self.pairs: Counter[tuple[str, str, str, str]] = Counter()
        self.truncated = False


totals = _Counters()


def reset() -> None:
    """Clear all counters. Tests only."""
    global totals
    totals = _Counters()


def record(result: ConsistencyResult) -> None:
    """Count one lookup's comparison outcomes."""
    try:
        with totals.lock:
            totals.lookups += 1
            if result.groups:
                totals.lookups_with_groups += 1
            for item in result.items:
                if item.relation not in RELATIONS:
                    continue
                a, b = sorted(item.sources)
                key = (item.field, a, b, item.relation)
                if key not in totals.pairs and len(totals.pairs) >= _MAX_KEYS:
                    totals.truncated = True
                    continue
                totals.pairs[key] += 1
    except Exception as exc:  # noqa: BLE001
        log.debug("consistencystats.record failed, ignoring: %s", exc)


def stats() -> dict[str, Any]:
    """Aggregate-only snapshot for ``/consistencystats``.

    ``pairs`` is a flat ``"field|source_a|source_b"`` → ``{relation: n}``
    map, plus a ``disagree_rate`` (``disagree / (agree + disagree)``,
    ``None`` when neither happened) so the base-rate gate can be read
    straight off the endpoint.
    """
    with totals.lock:
        rows: dict[str, dict[str, Any]] = {}
        for (field, a, b, relation), n in totals.pairs.items():
            row = rows.setdefault(f"{field}|{a}|{b}", {r: 0 for r in RELATIONS})
            row[relation] = n
        for row in rows.values():
            decided = row["agree"] + row["disagree"]
            row["disagree_rate"] = (row["disagree"] / decided) if decided else None
        return {
            "since": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(totals.started)),
            "uptime_s": int(time.time() - totals.started),
            "lookups": totals.lookups,
            "lookups_with_groups": totals.lookups_with_groups,
            "pairs": dict(sorted(rows.items())),
            "truncated": totals.truncated,
            "note": (
                "Shadow-mode counters for the record-consistency check (Phase 152). "
                "Aggregate only; keys are field names, adapter ids and relation "
                "names. 'stale'/'mirror' are pairs where one source republishes the "
                "other (see /sources derived_from). Resets on deploy."
            ),
        }
