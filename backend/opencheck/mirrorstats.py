"""mirrorstats — how the GLEIF mirror is actually being used (Phase 179).

The Golden Copy ticket gates the flip of ``OPENCHECK_GLEIF_MIRROR_FIRST`` on
numbers, not on the argument for it: what share of looked-up LEIs the mirror
holds (target above 99 %), how many live GLEIF calls a lookup no longer spends,
and — with ``OPENCHECK_GLEIF_LIVE_CONFIRM`` on — how often a mirror-served
record disagrees with the live one on a field the mapper reads. Counting those
server-side is the same argument ``signalstats`` makes: the traffic was going
to happen anyway, the count is exact rather than sampled, and nobody has to
run a sweep that would itself burn the budget being measured.

What is counted:

* ``anchor`` — the adapter's ``fetch``: served from the mirror, or a miss that
  went live, or ``no_mirror`` (the flag is on but the configured file is not a
  Phase 178 mirror, so the live path ran as before). Each mirror hit records
  the live calls it displaced — the record, both parents (and the exception
  probe each parent that is absent would have cost), and the children page —
  so ``live_calls_saved`` is what the budget gained, not an estimate; the
  ``topup_*`` counters say what the cross-reference top-up spent against it
  (one live call, a cache hit, skipped for lack of headroom, or failed).
* ``entity`` — ``fetch_entity`` (the Phase 162 dispatch-drift check).
* ``subsidiaries`` — the subsidiary network, served from the mirror or live.
* ``confirm`` — the optional live-confirm, riding on the top-up's record:
  ``same`` / ``differs`` on the mapper-read paths; the paths that differed
  are counted by name so a systematically stale field shows.

**Privacy.** Aggregate only. Keys are a closed vocabulary — counter names and
the fixed list of record paths in ``entity_pages.MAPPER_READ_PATHS`` — so no
LEI, entity name or address can reach a counter. ``/mirror`` also reports the
store's own ``meta`` (watermark, row counts, schema version), which is about
the file, not about any entity. In-process, reset on deploy, like ``memwatch``.

Everything fails soft: a counter must never take down a lookup.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from typing import Any

log = logging.getLogger("opencheck.mirrorstats")

_lock = threading.Lock()
_started_at = time.time()
_counts: Counter[str] = Counter()
_differing_paths: Counter[str] = Counter()

#: Closed vocabulary of counter names. Anything else is dropped rather than
#: recorded, so a caller cannot smuggle a value into a key.
_KEYS = frozenset(
    {
        "anchor.mirror",
        "anchor.miss_live",
        "anchor.no_mirror",
        "anchor.live_calls_saved",
        "anchor.topup_live",
        "anchor.topup_cached",
        "anchor.topup_skipped",
        "anchor.topup_failed",
        "entity.mirror",
        "entity.miss_live",
        "subsidiaries.mirror",
        "subsidiaries.miss_live",
        "subsidiaries.no_mirror",
        "confirm.same",
        "confirm.differs",
    }
)


def record(key: str, n: int = 1) -> None:
    try:
        if key not in _KEYS:
            log.debug("mirrorstats: dropping unknown key %r", key)
            return
        with _lock:
            _counts[key] += int(n)
    except Exception:  # noqa: BLE001 — instrumentation never raises
        log.debug("mirrorstats: record failed", exc_info=True)


def record_differing_paths(paths: list[str]) -> None:
    """The mapper-read paths on which live and mirror disagreed. Only paths
    from the fixed list are accepted, so the keys stay a closed vocabulary."""
    try:
        from .entity_pages import MAPPER_READ_PATHS

        allowed = {"/".join(p) for p in MAPPER_READ_PATHS}
        with _lock:
            for path in paths:
                if path in allowed:
                    _differing_paths[path] += 1
    except Exception:  # noqa: BLE001
        log.debug("mirrorstats: record_differing_paths failed", exc_info=True)


def stats() -> dict[str, Any]:
    """The ``/mirror`` payload: counters, derived rates, and the store's meta."""
    try:
        with _lock:
            counts = dict(_counts)
            differing = dict(_differing_paths)
        anchor_mirror = counts.get("anchor.mirror", 0)
        anchor_miss = counts.get("anchor.miss_live", 0)
        anchor_total = anchor_mirror + anchor_miss
        confirmed = counts.get("confirm.same", 0) + counts.get("confirm.differs", 0)
        out: dict[str, Any] = {
            "since": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_started_at)),
            "counts": {k: counts.get(k, 0) for k in sorted(_KEYS)},
            "anchor_mirror_hit_rate": (
                round(anchor_mirror / anchor_total, 4) if anchor_total else None
            ),
            "confirm_differ_rate": (
                round(counts.get("confirm.differs", 0) / confirmed, 4) if confirmed else None
            ),
            "differing_paths": dict(sorted(differing.items(), key=lambda kv: -kv[1])),
        }
        out.update(_store_summary())
        return out
    except Exception:  # noqa: BLE001
        log.debug("mirrorstats: stats failed", exc_info=True)
        return {"error": "mirrorstats unavailable"}


def _store_summary() -> dict[str, Any]:
    """What the configured entity-pages file is: a v2 mirror or not, its
    watermark and row counts (from ``meta`` — about the file, never an entity),
    and the two flags. ``None`` for the store when none is configured."""
    from .config import get_settings
    from .entity_pages import get_store

    settings = get_settings()
    flags = {
        "mirror_first": bool(settings.gleif_mirror_first),
        "live_confirm": bool(settings.gleif_live_confirm),
    }
    store = get_store()
    if store is None:
        return {"flags": flags, "store": None}
    meta = store.meta()
    return {
        "flags": flags,
        "store": {
            "is_mirror": store.is_mirror,
            "schema_version": store.schema_version,
            "watermark": meta.get("source_publish_datetime") or meta.get("source_publish_date"),
            "built_at": meta.get("built_at"),
            "record_count": _int(meta.get("record_count")),
            "relationship_count": _int(meta.get("relationship_count")),
            "exception_count": _int(meta.get("exception_count")),
            "detail_encoding": meta.get("detail_encoding"),
        },
    }


def _int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def reset_for_tests() -> None:
    global _started_at
    with _lock:
        _counts.clear()
        _differing_paths.clear()
        _started_at = time.time()
