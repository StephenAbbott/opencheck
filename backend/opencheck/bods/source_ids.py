"""Which registered source a BODS statement came from (Phase 267).

Every licence decision OpenCheck makes about a statement — the
``bods:license`` triple in the RDF export, Senzing's ``DATA_LICENSE`` /
``ATTRIBUTION``, and the contributing sources behind a FullCheck network's
``LICENSES.md`` — needs the adapter id behind it. Until Phase 267 a
statement carried only ``source.description``, a display name, and the id
was recovered by matching that string back against the registry. A miss
returned an empty set: no error, no log line, and a licence file that
understated what was in the bundle. It missed on every OECD-UNSD MEIP
statement (the OECD writes its own description), and it would have missed
on every statement already stamped the day anyone edited a display name in
``SOURCE_NAMES``.

Now ``statements._source_block`` writes the id beside the description, as
``source.opencheckSourceId``. BODS v0.4's Source object does not close its
properties, so the key is an extension field: libcovebods lists it among
"additional fields", which is information, not a validation error.
``map_meip`` adds the same key to the OECD's source blocks — one added key,
none of the OECD's values or ids changed.

Resolution order, in :func:`source_ids_of`:

1. ``source.opencheckSourceId``, when it names a registered adapter.
2. ``source.description`` matched against the registry's display names — for
   statements stamped before Phase 267, which saved reports and caches still
   hold — then against :data:`_LEGACY_DESCRIPTIONS`, the descriptions other
   publishers wrote.
3. Otherwise nothing, **and it says so**: a warning is logged once per
   distinct description, and the export routes count what they could not
   attribute on ``/signalstats`` (:func:`record_unresolved`).
"""

from __future__ import annotations

import logging
import threading
from collections import Counter
from functools import lru_cache
from typing import Any, Iterable, Mapping

log = logging.getLogger("opencheck.bods.source_ids")

#: The extension key on a BODS ``source`` block carrying the adapter id.
SOURCE_ID_KEY = "opencheckSourceId"

#: Descriptions written by someone other than ``_source_block`` that still
#: name one registered source. Consulted only when a statement carries no
#: usable ``opencheckSourceId`` — i.e. for statements saved before Phase 267.
#: The MEIP string is the one the OECD's Global Register (BODS edition,
#: 31 December 2024) writes on every statement, as read from a production
#: export on 30 Sept 2026.
_LEGACY_DESCRIPTIONS: dict[str, str] = {
    "OECD-UNSD Multinational Enterprise Information Platform (MEIP), 'Group Register' sheet.": "meip",
}

#: Why a statement's source could not be attributed. A closed vocabulary —
#: these are the only reason strings that reach ``/signalstats``.
REASON_NO_SOURCE = "no_source"
REASON_UNRECOGNISED = "unrecognised"

_warned_lock = threading.Lock()
_warned: set[str] = set()
_MAX_WARNED = 256


def _registry_ids() -> frozenset[str]:
    from ..sources import REGISTRY

    return frozenset(REGISTRY)


@lru_cache(maxsize=1)
def description_index() -> dict[str, str]:
    """``source.description`` → adapter id, for every registered source.

    Built from ``statements.SOURCE_NAMES`` directly, the same table
    ``_source_block`` stamps from. Cached: the registry is fixed at import.
    """
    from .statements import SOURCE_NAMES

    registered = _registry_ids()
    out: dict[str, str] = {}
    for sid in registered:
        desc = (SOURCE_NAMES.get(sid, sid) or "").strip()
        if desc:
            out[desc] = sid
    for desc, sid in _LEGACY_DESCRIPTIONS.items():
        if sid in registered:
            out.setdefault(desc, sid)
    return out


def _source(stmt: Mapping[str, Any]) -> Mapping[str, Any] | None:
    src = stmt.get("source") if isinstance(stmt, Mapping) else None
    return src if isinstance(src, Mapping) else None


def _resolve(stmt: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """``(source_id, None)`` on success, ``(None, reason)`` on a miss."""
    src = _source(stmt)
    if not src:
        return None, REASON_NO_SOURCE
    stamped = src.get(SOURCE_ID_KEY)
    if isinstance(stamped, str) and stamped in _registry_ids():
        return stamped, None
    desc = str(src.get("description") or "").strip()
    if desc:
        sid = description_index().get(desc)
        if sid:
            return sid, None
    if not desc and not stamped:
        return None, REASON_NO_SOURCE
    return None, REASON_UNRECOGNISED


def _warn_once(stmt: Mapping[str, Any]) -> None:
    src = _source(stmt) or {}
    label = str(src.get(SOURCE_ID_KEY) or src.get("description") or "").strip()
    if not label:
        return  # a statement with no source block at all: nothing to name
    with _warned_lock:
        if label in _warned or len(_warned) >= _MAX_WARNED:
            return
        _warned.add(label)
    log.warning(
        "BODS statement source %r matches no registered OpenCheck source; "
        "no licence can be attached to it",
        label,
    )


def source_id_of(stmt: Mapping[str, Any]) -> str | None:
    """The registered adapter id behind a statement, or ``None``."""
    sid, reason = _resolve(stmt)
    if sid is None and reason == REASON_UNRECOGNISED:
        _warn_once(stmt)
    return sid


def source_ids_of(stmt: Mapping[str, Any]) -> set[str]:
    """As :func:`source_id_of`, as a set — the shape the licensing helpers take."""
    sid = source_id_of(stmt)
    return {sid} if sid else set()


def contributing_source_ids(statements: Iterable[Mapping[str, Any]]) -> list[str]:
    """Every registered source behind a bundle, sorted."""
    out: set[str] = set()
    for stmt in statements:
        sid = source_id_of(stmt)
        if sid:
            out.add(sid)
    return sorted(out)


def unresolved(statements: Iterable[Mapping[str, Any]]) -> Counter[str]:
    """How many statements could not be attributed, by reason."""
    counts: Counter[str] = Counter()
    for stmt in statements:
        sid, reason = _resolve(stmt)
        if sid is None and reason:
            counts[reason] += 1
    return counts


def record_unresolved(statements: Iterable[Mapping[str, Any]], surface: str) -> Counter[str]:
    """Count a bundle's unattributable statements on ``/signalstats``.

    Called once per export request, by the export routes, so the count reads
    "statements shipped without a licence attribution". ``surface`` and the
    reasons are closed vocabularies; nothing from a statement's content is
    recorded. Never raises.
    """
    counts = unresolved(statements)
    try:
        from .. import signalstats

        signalstats.record_unresolved_sources(surface, counts)
    except Exception as exc:  # noqa: BLE001 — instrumentation must never fail an export
        log.debug("record_unresolved failed, ignoring: %s", exc)
    return counts


def _reset_warnings() -> None:
    """Tests only."""
    with _warned_lock:
        _warned.clear()
