"""mirror_refresh — keep the GLEIF mirror within a day of GLEIF, in-process.

Phase 180. The mirror (``entity_pages.sqlite``, see ``entity_pages``) is built
in full once a month by ``refresh-entity-pages-db.yml`` and, since Phase 179,
can answer the anchor first. Between rebuilds it would drift by up to a month;
GLEIF publishes the delta that closes the gap three times a day. This module
applies it: a lifespan task polls the Golden Copy publish API, compares the
latest publish with the file's watermark (``meta.source_publish_datetime``),
picks the smallest delta whose window covers the gap, streams the three delta
files (LEI2, RR, REPEX) straight from ``goldencopy.gleif.org`` and upserts
them with the same code the full build uses (``mirror_build``), then advances
the watermark. A LastDay delta is ~0.8 MB + 40 KB + 26 KB, so this is seconds
of work; a LastMonth delta (the file was offline for a week or more) is a few
minutes in a background thread.

Rules that keep it honest and safe:

* **The watermark advances only on success.** Every failure — network, a CSV
  column that moved, a locked file — leaves ``meta`` untouched, logs, and is
  counted on ``/mirror``; the next tick tries again, and the monthly full
  rebuild remains the self-heal for anything the deltas cannot express.
* **Idempotent.** The loads are upserts keyed on LEI / (child, type) /
  (lei, kind), so re-applying a delta changes nothing — a crash between the
  load and the watermark write costs one repeated delta, not a wrong file.
* **A gap the deltas cannot cover** (over 31 days: the file was built long ago
  or the instance was down) is closed by downloading the release asset again,
  the same way boot does — never by a full rebuild on the app instance, which
  needs 4.8 GB of scratch and twelve minutes of CPU the web service does not
  have to spare.
* **Only a Phase 178 mirror is refreshed.** A v1 file has no detail column to
  fill and no Level 2 tables; a delta would give it a month of partial detail
  and the reader would then treat it as a mirror. It waits for a full asset.
* **Readers keep reading.** The store's connection is read-only but not
  immutable (Phase 180 changed that), so it sees the upserts as they commit;
  the writer's commit windows are short and readers wait them out. A file
  replaced wholesale is reopened via ``entity_pages.reload_store``.
* **Off when it cannot help.** ``OPENCHECK_MIRROR_REFRESH_INTERVAL_S=0``
  disables the task; it also does nothing when no store is configured.

Everything the task does is aggregate and about the file — no LEI is logged
or counted.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import entity_pages as ep
from . import mirror_build as mb

log = logging.getLogger("opencheck.mirror_refresh")

#: The delta windows GLEIF publishes, smallest first, with the widest gap each
#: can be trusted to cover. IntraDay carries the changes since the previous
#: publish (about eight hours apart); the rest are rolling windows. A delta is a
#: superset of every narrower one, so choosing wide is always safe — the
#: thresholds only decide how much to download.
DELTA_WINDOWS: tuple[tuple[str, timedelta], ...] = (
    ("IntraDay", timedelta(hours=8)),
    ("LastDay", timedelta(days=1)),
    ("LastWeek", timedelta(days=7)),
    ("LastMonth", timedelta(days=31)),
)

#: What a run concluded. Closed vocabulary — these are counter keys.
OUTCOMES = (
    "up_to_date",
    "applied",
    "asset_replaced",
    "skipped_no_store",
    "skipped_not_mirror",
    "skipped_no_watermark",
    "skipped_gap_too_large",
    "failed",
)


def choose_delta(gap: timedelta) -> str | None:
    """The smallest delta window covering ``gap``; ``None`` when none does."""
    for name, window in DELTA_WINDOWS:
        if gap <= window:
            return name
    return None


def parse_publish_datetime(raw: str | None) -> datetime | None:
    """GLEIF's ``publish_date`` (``2026-09-07 16:00:00``) as an aware UTC datetime."""
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip()[: len("2026-09-07 16:00:00")], fmt).replace(
                tzinfo=UTC
            )
        except ValueError:
            continue
    return None


@dataclass
class RefreshState:
    """What ``/mirror`` reports about the task. Aggregate only."""

    enabled: bool = False
    last_checked_at: str | None = None
    last_outcome: str | None = None
    last_applied_at: str | None = None
    last_delta: str | None = None
    last_publish: str | None = None
    last_gap_hours: float | None = None
    last_error: str | None = None
    runs: int = 0
    applied: int = 0
    failures: int = 0
    rows_applied: dict[str, int] = field(default_factory=dict)
    outcomes: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "last_checked_at": self.last_checked_at,
            "last_outcome": self.last_outcome,
            "last_applied_at": self.last_applied_at,
            "last_delta": self.last_delta,
            "last_publish": self.last_publish,
            "last_gap_hours": self.last_gap_hours,
            "last_error": self.last_error,
            "runs": self.runs,
            "applied": self.applied,
            "failures": self.failures,
            "rows_applied": dict(self.rows_applied),
            "outcomes": {k: self.outcomes.get(k, 0) for k in OUTCOMES},
        }


_state = RefreshState()
_state_lock = threading.Lock()
#: One refresh at a time: the lifespan task and a manual trigger must never
#: both write the file.
_run_lock = threading.Lock()


def state() -> dict[str, Any]:
    with _state_lock:
        return _state.to_dict()


def reset_for_tests() -> None:
    global _state
    with _state_lock:
        _state = RefreshState()


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _record(outcome: str, **fields: Any) -> str:
    with _state_lock:
        _state.runs += 1
        _state.last_checked_at = _now_iso()
        _state.last_outcome = outcome
        _state.outcomes[outcome] = _state.outcomes.get(outcome, 0) + 1
        if outcome == "failed":
            _state.failures += 1
        if outcome in ("applied", "asset_replaced"):
            _state.applied += 1
            _state.last_applied_at = _state.last_checked_at
        for key, value in fields.items():
            setattr(_state, key, value)
    return outcome


# ---------------------------------------------------------------------------
# One refresh
# ---------------------------------------------------------------------------


def refresh_once(*, publish: dict | None = None, now: datetime | None = None) -> str:
    """Bring the configured mirror up to the latest Golden Copy publish.

    Returns one of :data:`OUTCOMES`. ``publish`` (a publish-API entry) and
    ``now`` are injectable for tests; production reads the API.
    """
    from .config import get_settings

    if not _run_lock.acquire(blocking=False):
        log.info("mirror refresh: a run is already in progress")
        return "failed"
    try:
        return _refresh_once(get_settings(), publish, now)
    finally:
        _run_lock.release()


def _refresh_once(settings: Any, publish: dict | None, now: datetime | None) -> str:
    store = ep.get_store()
    if store is None:
        return _record("skipped_no_store")
    if not store.is_mirror:
        return _record("skipped_not_mirror")
    watermark = store.watermark()
    if watermark is None:
        return _record("skipped_no_watermark")

    try:
        publish = publish or mb._latest_publish()
        publish_at = parse_publish_datetime(publish.get("publish_date"))
        if publish_at is None:
            raise ValueError(f"unreadable publish_date {publish.get('publish_date')!r}")
    except Exception as exc:  # noqa: BLE001 — a failed poll is a counted, retried outcome
        log.warning("mirror refresh: could not read the publish API: %s", exc)
        return _record("failed", last_error=f"publish API: {type(exc).__name__}: {exc}")

    gap = publish_at - watermark
    gap_hours = round(gap.total_seconds() / 3600, 2)
    publish_label = publish_at.strftime("%Y-%m-%d %H:%M:%S")
    if gap <= timedelta(0):
        return _record("up_to_date", last_gap_hours=gap_hours, last_publish=publish_label)

    delta = choose_delta(gap)
    if delta is None:
        # Beyond the widest delta: the release asset is the self-heal.
        url = settings.entity_pages_db_url
        if not url:
            log.warning(
                "mirror refresh: %.0f h behind GLEIF and no release asset URL to reload from",
                gap_hours,
            )
            return _record(
                "skipped_gap_too_large", last_gap_hours=gap_hours, last_publish=publish_label
            )
        try:
            ep.download_db(url, store.path)
            ep.reload_store()
        except Exception as exc:  # noqa: BLE001
            log.warning("mirror refresh: release asset re-download failed: %s", exc)
            return _record(
                "failed", last_gap_hours=gap_hours, last_publish=publish_label,
                last_error=f"asset: {type(exc).__name__}: {exc}",
            )
        return _record(
            "asset_replaced", last_gap_hours=gap_hours, last_publish=publish_label,
            last_delta="asset",
        )

    started = time.monotonic()
    try:
        counts = apply_delta(store.path, publish, delta)
    except (Exception, SystemExit) as exc:  # noqa: BLE001 — the watermark stays where it was
        # SystemExit too: the loaders were written for the CLI and refuse a
        # CSV whose header moved with it — in-process that is a failed run,
        # not a reason to take the server down.
        log.warning("mirror refresh: %s delta failed: %s: %s", delta, type(exc).__name__, exc)
        return _record(
            "failed", last_gap_hours=gap_hours, last_publish=publish_label,
            last_delta=delta, last_error=f"{delta}: {type(exc).__name__}: {exc}",
        )
    log.info(
        "mirror refresh: applied %s delta to %s (%s) in %.1fs — %s",
        delta, publish_label, ", ".join(f"{k} {v:,}" for k, v in counts.items()),
        time.monotonic() - started, "watermark advanced",
    )
    return _record(
        "applied", last_gap_hours=gap_hours, last_publish=publish_label,
        last_delta=delta, last_error=None, rows_applied=counts,
    )


def apply_delta(db_path: Path, publish: dict, delta: str) -> dict[str, int]:
    """Download the ``delta`` files named by the publish entry and upsert them
    into ``db_path``; write the watermark last. Returns the row counts.

    Separated from :func:`refresh_once` so a test can drive it with local
    files (``publish`` may name ``file://`` URLs — see ``mb._download``) and so
    the meta writes are visibly the final step: nothing before them commits
    the watermark.
    """
    with tempfile.TemporaryDirectory(prefix="gleif-delta-") as tmp:
        tmp_dir = Path(tmp)
        paths = {
            kind: mb._download(
                publish[kind]["delta_files"][delta]["csv"]["url"], tmp_dir, f"{kind} {delta}"
            )
            for kind in ("lei2", "rr", "repex")
        }
        conn = sqlite3.connect(db_path, timeout=60.0)
        try:
            mb.ensure_schema(conn)
            zdict = mb.stored_zdict(conn)
            counts = {
                "entities": mb.load_lei2(conn, paths["lei2"], zdict=zdict),
                "relationships": mb.load_rr(conn, paths["rr"], full=False),
                "exceptions": mb.load_repex(conn, paths["repex"]),
            }
            # Rows written as text (a file without a dictionary yet) are
            # compressed here; with a dictionary this finds nothing to do.
            counts["compressed"] = mb.compress_detail_column(conn)
            mb.write_meta(
                conn,
                source_publish_date=publish["publish_date"],
                source_publish_datetime=publish["publish_date"],
                mode=f"delta:{delta}",
                refreshed_at=_now_iso(),
                record_count=str(conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]),
                relationship_count=str(
                    conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]
                ),
                exception_count=str(
                    conn.execute("SELECT COUNT(*) FROM reporting_exceptions").fetchone()[0]
                ),
            )
        finally:
            conn.close()
    return counts


# ---------------------------------------------------------------------------
# The lifespan task
# ---------------------------------------------------------------------------


async def refresh_loop(interval_s: float) -> None:
    """Run :func:`refresh_once` every ``interval_s`` seconds until cancelled,
    the work itself on a thread so the event loop never waits on a download.
    The first run is delayed by one interval: boot already downloaded or
    warmed the file, and GLEIF's three publishes a day leave nothing to gain
    from polling in the first hour."""
    with _state_lock:
        _state.enabled = True
    try:
        while True:
            await asyncio.sleep(interval_s)
            try:
                await asyncio.to_thread(refresh_once)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — the loop must outlive any one run
                log.exception("mirror refresh: unexpected error")
    finally:
        with _state_lock:
            _state.enabled = False
