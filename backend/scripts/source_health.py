#!/usr/bin/env python
"""Weekly source-health sweep — exercise every adapter and report what broke.

Runs the probe table in ``opencheck.sources.probes`` against the real upstream
sources, one known-good subject per adapter, and writes a JSON + Markdown
report. Driven by ``.github/workflows/source-health.yml`` on Mondays; runnable
by hand for a single source while debugging.

What it asserts, and why that is not the obvious thing
------------------------------------------------------

A sweep that asks "did the adapter return data?" would have gone green every
week on the Ariregister bug (PR #153): the adapter reached the Estonian
register, parsed it, and returned real officers and beneficial owners, but
recorded no provenance observation, so OpenCheck badged genuinely live data
"Placeholder data". Each probe therefore runs inside a
``provenance.recording()`` scope and checks the **resolved liveness** as well
as reachability, emptiness and shape.

Statuses
--------

``ok``        everything asserted held.
``degraded``  answered, but not the way it should have — served from cache
              rather than the upstream, or a stale ``retrieved_at``.
``fail``      unreachable, empty when it should not be, wrong provenance, or a
              missing expected field.
``skipped``   not exercised: no credential configured, a required local
              artifact absent, or the source is registered but env-gated off.

Exit code is non-zero when anything failed or degraded — which is what makes
GitHub email a red scheduled run — or when more sources skipped for want of a
credential than ``probes.MAX_SKIPPED_FOR_CREDENTIALS`` allows, so an expired
or deleted secret cannot quietly shrink coverage behind a green tick.

Reporting hygiene
-----------------

The report carries ids, statuses, latencies, liveness values, result sizes and
observed field *names* only — no payloads, no personal names, no subject
identifiers beyond the adapter id. Error text is truncated and credential-like
query parameters are redacted before anything is written. This is the same
contract ``degraded_sources`` and ``/signalstats`` already keep, and it matters
here because several sources are CC BY-NC or carry personal data.

Usage::

    uv run python scripts/source_health.py --out ../source-health
    uv run python scripts/source_health.py --only ariregister,gleif --no-retry
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Live mode must be on before the settings cache is first populated.
os.environ.setdefault("OPENCHECK_ALLOW_LIVE", "true")

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from opencheck import provenance  # noqa: E402
from opencheck.cache import data_root  # noqa: E402
from opencheck.config import get_settings  # noqa: E402
from opencheck.sources import REGISTRY  # noqa: E402
from opencheck.sources.probes import (  # noqa: E402
    MAX_SKIPPED_FOR_CREDENTIALS,
    PROBES,
    SourceProbe,
    missing_env,
)

OK = "ok"
DEGRADED = "degraded"
FAIL = "fail"
SKIPPED = "skipped"

#: A live answer older than this was not really fetched now.
_FRESH_WITHIN = timedelta(hours=1)
_ERROR_CHARS = 200
_SECRETISH = re.compile(
    r"((?:api[_-]?key|key|token|secret|password|guid|subscription-key)=)[^&\s]+",
    re.IGNORECASE,
)


def _redact(text: str) -> str:
    """Truncate and strip anything credential-shaped out of error text."""
    cleaned = _SECRETISH.sub(r"\1<redacted>", " ".join(str(text).split()))
    return cleaned[:_ERROR_CHARS] + ("…" if len(cleaned) > _ERROR_CHARS else "")


def _size_of(result: Any) -> int:
    if result is None:
        return 0
    if isinstance(result, dict):
        return len(result)
    if isinstance(result, (list, tuple, set)):
        return len(result)
    return 1


def _observed_fields(result: Any) -> list[str]:
    """Top-level key *names* only — never values."""
    if isinstance(result, dict):
        return sorted(str(k) for k in result)[:40]
    if isinstance(result, (list, tuple)) and result and isinstance(result[0], dict):
        return sorted(str(k) for k in result[0])[:40]
    return []


@dataclass
class Result:
    source_id: str
    tier: str
    status: str
    reason: str = ""
    liveness: str | None = None
    retrieved_at: str | None = None
    latency_ms: int | None = None
    result_size: int | None = None
    observed_fields: list[str] = field(default_factory=list)
    attempts: int = 0
    known_gap: str = ""
    statement_counts: dict[str, int] | None = None


async def _run_probe(source_id: str, probe: SourceProbe, timeout: float) -> Result:
    """One attempt. Returns ok / degraded / fail — never raises."""
    adapter = REGISTRY[source_id]
    call = getattr(adapter, probe.method)
    started = time.monotonic()

    try:
        with provenance.recording() as recorder:
            result = await asyncio.wait_for(call(*probe.args, **dict(probe.kwargs)), timeout=timeout)
        prov = recorder.resolve()
    except asyncio.TimeoutError:
        return Result(
            source_id,
            probe.tier,
            FAIL,
            reason=f"timed out after {timeout:.0f}s",
            latency_ms=int((time.monotonic() - started) * 1000),
            known_gap=probe.known_gap,
        )
    except Exception as exc:  # noqa: BLE001 — a sweep must survive any adapter
        return Result(
            source_id,
            probe.tier,
            FAIL,
            reason=f"{type(exc).__name__}: {_redact(exc)}",
            latency_ms=int((time.monotonic() - started) * 1000),
            known_gap=probe.known_gap,
        )

    latency_ms = int((time.monotonic() - started) * 1000)
    out = Result(
        source_id,
        probe.tier,
        OK,
        statement_counts=statement_counts(source_id, probe, result),
        liveness=prov.liveness,
        retrieved_at=prov.retrieved_at_iso(),
        latency_ms=latency_ms,
        result_size=_size_of(result),
        observed_fields=_observed_fields(result),
        known_gap=probe.known_gap,
    )

    # 1. Emptiness — for a register lookup of a company that exists, nothing
    #    back means the parser stopped understanding the response.
    if out.result_size == 0 and not probe.allow_empty:
        out.status = FAIL
        out.reason = "empty result — upstream shape may have changed"
        return out

    # 2. Provenance — the Ariregister class. Assert this even when the content
    #    looks perfect, because that is exactly how the bug presented.
    if prov.liveness not in probe.expect_liveness:
        expected = "/".join(sorted(probe.expect_liveness))
        out.status = FAIL
        out.reason = f"provenance: expected {expected}, resolved '{prov.liveness}'"
        return out

    # 3. Freshness — a 'live' claim with an old timestamp is not a live fetch.
    if prov.liveness == "live" and prov.retrieved_at is not None:
        age = datetime.now(timezone.utc) - prov.retrieved_at
        if age > _FRESH_WITHIN:
            out.status = DEGRADED
            out.reason = f"claims live but retrieved_at is {age} old"
            return out

    # 3b. Snapshot ageing — the failure mode of a committed index is silence,
    #     not an error. Uses the date the index declares, never a file mtime.
    if probe.snapshot_max_age_days is not None and prov.retrieved_at is not None:
        age_days = (datetime.now(timezone.utc) - prov.retrieved_at).days
        if age_days > probe.snapshot_max_age_days:
            out.status = DEGRADED
            out.reason = (
                f"snapshot is {age_days} days old (limit {probe.snapshot_max_age_days}) "
                "— refresh due"
            )
            return out
    if probe.snapshot_max_age_days is not None and prov.retrieved_at is None:
        out.status = DEGRADED
        out.reason = "snapshot declares no build date, so its age cannot be checked"
        return out

    # 4. A live-tier source answered from cache: the upstream was never
    #    contacted, so this run proves nothing about it.
    if probe.tier == "live" and prov.liveness == "cached":
        out.status = DEGRADED
        out.reason = "served from OpenCheck's cache — upstream not contacted"
        return out

    # 5. Shape.
    if isinstance(result, dict):
        absent = [f for f in probe.expect_fields if not result.get(f)]
        if absent:
            out.status = FAIL
            out.reason = f"missing/empty expected field(s): {', '.join(absent)}"
            return out

    return out


def disable_cache_reads() -> None:
    """Make every probe go to the upstream, including the retry.

    Two things go wrong without this. A sweep run twice in a row grades
    OpenCheck's own cache — several sources come back ``cached``, having
    contacted nobody. Worse, the single retry poisons itself: the first
    attempt writes its (possibly wrong) answer to the cache and the retry
    reads it straight back, so a real failure is re-reported as a cache hit
    and the retry proves nothing.

    Reads are stubbed out rather than the cache being deleted, so nothing the
    developer has cached locally is destroyed by running the sweep. Writes
    still happen, into the scratch root below.
    """
    from opencheck import cache as cache_module

    cache_module.Cache.get = lambda self, key: None  # type: ignore[method-assign]
    cache_module.Cache.get_payload = lambda self, key, max_age_days=None: None  # type: ignore[method-assign]


def build_scratch_data_root(real_root: Path, scratch: Path) -> Path:
    """A data root that keeps every committed artifact but starts with an empty
    live cache.

    Without this the sweep quietly grades OpenCheck's own cache instead of the
    upstream: run it twice in a row locally and half the sources come back
    ``cached``, having contacted nobody. CI never sees this — ``data/cache/live``
    is gitignored, so a fresh checkout starts empty — which is precisely why it
    would go unnoticed until someone trusted a local run.

    Everything is symlinked except ``cache/live``, which is created empty, so
    the demo fixtures, ``bods_data`` subgraphs and GEM artifacts stay reachable.
    """
    scratch.mkdir(parents=True, exist_ok=True)
    for entry in real_root.iterdir():
        if entry.name == "cache":
            continue
        link = scratch / entry.name
        if not link.exists():
            link.symlink_to(entry, target_is_directory=entry.is_dir())

    cache = scratch / "cache"
    cache.mkdir(exist_ok=True)
    real_cache = real_root / "cache"
    if real_cache.is_dir():
        for entry in real_cache.iterdir():
            if entry.name == "live":
                continue
            link = cache / entry.name
            if not link.exists():
                link.symlink_to(entry, target_is_directory=entry.is_dir())
    (cache / "live").mkdir(exist_ok=True)
    return scratch


def _skip_reason(probe: SourceProbe, root: Path) -> str | None:
    absent_env = missing_env(probe, dict(os.environ))  # .env counts too — see probes.configured_credentials
    if absent_env:
        label = "not configured" if probe.tier != "inactive" else "env-gated off"
        return f"{label}: {', '.join(absent_env)}"
    absent_files = [p for p in probe.requires_files if not (root / p).exists()]
    if absent_files:
        return f"required local artifact absent: {', '.join(absent_files)}"
    return None


async def _probe_with_retry(
    source_id: str,
    probe: SourceProbe,
    *,
    timeout: float,
    retry_delay: float,
    retry: bool,
    semaphore: asyncio.Semaphore,
) -> Result:
    async with semaphore:
        result = await _run_probe(source_id, probe, timeout)
        result.attempts = 1
        if result.status == FAIL and retry:
            # Upstream blips are common and a sweep that cries wolf gets
            # ignored within a month. One retry, then it counts.
            await asyncio.sleep(retry_delay)
            retried = await _run_probe(source_id, probe, timeout)
            retried.attempts = 2
            return retried
        return result


async def sweep(
    source_ids: list[str],
    *,
    timeout: float,
    concurrency: int,
    retry_delay: float,
    retry: bool,
) -> list[Result]:
    root = data_root()
    results: list[Result] = []
    runnable: list[str] = []

    for source_id in source_ids:
        probe = PROBES[source_id]
        reason = _skip_reason(probe, root)
        if reason:
            results.append(
                Result(source_id, probe.tier, SKIPPED, reason=reason, known_gap=probe.known_gap)
            )
        else:
            runnable.append(source_id)

    # Capped so 39 sources do not look like an attack to anyone's WAF.
    semaphore = asyncio.Semaphore(concurrency)
    live = await asyncio.gather(
        *(
            _probe_with_retry(
                sid,
                PROBES[sid],
                timeout=timeout,
                retry_delay=retry_delay,
                retry=retry,
                semaphore=semaphore,
            )
            for sid in runnable
        )
    )
    results.extend(live)
    results.sort(key=lambda r: (r.status != FAIL, r.status != DEGRADED, r.source_id))
    return results


# ---------------------------------------------------------------------------
# BODS statement counts, and the week-over-week diff
# ---------------------------------------------------------------------------

#: Below this, week-to-week wobble is ordinary (a company files a change, an
#: officer resigns). Only a collapse past it is reported.
_COLLAPSE_RATIO = 0.5


def statement_counts(source_id: str, probe: SourceProbe, result: Any) -> dict[str, int] | None:
    """Map a probe's answer through its BODS mapper and count by record type.

    Counts, not content: the artifact carries integers, never statements, so
    nothing licence-restricted or personal is written down.

    Returns None when there is nothing to count — no mapper declared, or the
    probe returns a list of hits rather than a mappable bundle.
    """
    if probe.bods_mapper is None or not isinstance(result, dict):
        return None
    try:
        from opencheck.bods import mapper as mapper_module

        mapper = getattr(mapper_module, probe.bods_mapper)
        statements = list(mapper(result))
    except Exception:  # noqa: BLE001 — a mapping failure must not fail the sweep
        return None

    counts: dict[str, int] = {"entity": 0, "person": 0, "relationship": 0}
    for statement in statements:
        if not isinstance(statement, dict):
            continue
        record_type = statement.get("recordType")
        if record_type in counts:
            counts[record_type] += 1
        if record_type == "relationship":
            # The interest-type histogram rather than a single "beneficial
            # ownership" bucket: which interest type carries BO varies by
            # source (Estonia's beneficial owners map to
            # otherInfluenceOrControl, not beneficialOwnershipOrControl), so
            # counting each type is both more honest and more informative than
            # guessing which one to watch.
            for interest in (statement.get("recordDetails") or {}).get("interests") or []:
                if isinstance(interest, dict) and interest.get("type"):
                    key = f"interest:{interest['type']}"
                    counts[key] = counts.get(key, 0) + 1
    return counts


def diff_statement_counts(
    current: dict[str, Any], previous: dict[str, Any] | None
) -> dict[str, dict[str, Any]]:
    """Report *collapses* against last week's counts, not movement.

    A company genuinely filing a new PSC must read as normal variation, or the
    check becomes noise and stops being read. A drop to zero, or past
    ``_COLLAPSE_RATIO``, is what earns a report: for a BO-carrying source that
    is the earliest machine-detectable signal of an access change — the shape
    of Estonia's postponed legitimate-interest switch, which has no announced
    date to schedule a check against, so the data has to be the alarm.
    """
    if not previous:
        return {}
    findings: dict[str, dict[str, Any]] = {}
    for source_id, now_row in current.items():
        now_counts = now_row.get("statement_counts")
        then_counts = (previous.get("sources") or {}).get(source_id, {}).get("statement_counts")
        if not now_counts or not then_counts:
            continue
        collapsed = {}
        for kind, then_value in then_counts.items():
            now_value = now_counts.get(kind, 0)
            if then_value > 0 and now_value < then_value * _COLLAPSE_RATIO:
                collapsed[kind] = {"was": then_value, "now": now_value}
        if collapsed:
            findings[source_id] = collapsed
    return findings


def load_previous_report(path: str) -> dict[str, Any] | None:
    """Last run's report, when the workflow managed to fetch it.

    The workflow downloads the previous successful run's artifact through the
    Actions API. That can legitimately come back empty — 90-day retention
    expiring, or the last run having failed — and the report says
    "no comparison available" rather than letting a missing baseline read as
    "nothing changed".
    """
    if not path:
        return None
    candidate = Path(path)
    if not candidate.exists():
        return None
    try:
        return json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# GLEIF dispatch drift
# ---------------------------------------------------------------------------


@dataclass
class DriftResult:
    source_id: str
    status: str
    reason: str = ""
    registered_at: str | None = None
    derived: str | None = None


async def _gleif_entity_block(lei: str, cache: dict[str, Any]) -> dict[str, Any]:
    if lei not in cache:
        bundle = await REGISTRY["gleif"].fetch(lei)
        attrs = (bundle.get("record") or {}).get("attributes") or {}
        cache[lei] = attrs.get("entity") or {}
    return cache[lei]


async def check_dispatch_drift(source_ids: list[str]) -> list[DriftResult]:
    """Is each national register still reachable *through GLEIF*?

    Every national register is entered via a key derived from the GLEIF anchor
    record: the pipeline reads ``registeredAt.id``, matches it against the
    adapter's ``ra_codes``, and runs the adapter's ``normalise(registeredAs)``
    to build the local identifier. If GLEIF renames a registration-authority
    code, or a registrar changes its number formatting, the source stops being
    dispatched **at all** — while its own probe stays green, because its own
    endpoint is fine. No per-source probe can see this: it is a failure of the
    join, not of either side.

    That the formats genuinely differ is not hypothetical — GLEIF returns
    Norway's as ``'923 609 016'`` with spaces and Croatia's zero-padded to nine
    digits, which is exactly what the normalisers exist to absorb. This check
    runs them for real rather than assuming they still fit.
    """
    results: list[DriftResult] = []
    cache: dict[str, Any] = {}

    for source_id in source_ids:
        probe = PROBES[source_id]
        adapter = REGISTRY[source_id]
        derivers = getattr(adapter, "lookup_derivers", ()) or ()
        if not derivers:
            continue
        if not probe.anchor_lei:
            results.append(
                DriftResult(
                    source_id,
                    SKIPPED,
                    reason="no anchor LEI on the probe — dispatch is not covered",
                )
            )
            continue

        try:
            entity = await _gleif_entity_block(probe.anchor_lei, cache)
        except Exception as exc:  # noqa: BLE001
            results.append(
                DriftResult(source_id, FAIL, reason=f"GLEIF anchor fetch failed: {_redact(exc)}")
            )
            continue

        registered_as = (entity.get("registeredAs") or "").strip()
        registered_at = (entity.get("registeredAt") or {}).get("id") or ""

        matching = [d for d in derivers if registered_at in d.ra_codes]
        if not matching:
            expected = sorted({code for d in derivers for code in d.ra_codes})
            results.append(
                DriftResult(
                    source_id,
                    FAIL,
                    reason=(
                        f"dispatch drift: GLEIF anchor is registered at {registered_at or '(none)'}, "
                        f"adapter dispatches on {', '.join(expected)} — this source would never be reached"
                    ),
                    registered_at=registered_at,
                )
            )
            continue

        deriver = matching[0]
        try:
            derived = deriver.normalise(registered_as)
        except ValueError as exc:
            results.append(
                DriftResult(
                    source_id,
                    FAIL,
                    reason=f"normalise({registered_as!r}) rejected it: {_redact(exc)}",
                    registered_at=registered_at,
                )
            )
            continue

        expected_id = str(probe.args[0]) if probe.args else ""
        if derived != expected_id:
            results.append(
                DriftResult(
                    source_id,
                    FAIL,
                    reason=(
                        f"derived identifier {derived!r} no longer matches the probe subject "
                        f"{expected_id!r} — registrar formatting may have changed"
                    ),
                    registered_at=registered_at,
                    derived=derived,
                )
            )
            continue

        results.append(
            DriftResult(source_id, OK, registered_at=registered_at, derived=derived)
        )

    results.sort(key=lambda r: (r.status != FAIL, r.source_id))
    return results


def _credential_skips(results: list[Result]) -> list[str]:
    return [r.source_id for r in results if r.status == SKIPPED and "not configured" in r.reason]


def build_report(
    results: list[Result],
    drift: list[DriftResult] | None = None,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    drift = drift or []
    counts = {status: sum(1 for r in results if r.status == status) for status in (OK, DEGRADED, FAIL, SKIPPED)}
    return {
        "dispatch_drift": {d.source_id: asdict(d) for d in drift},
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "registry_size": len(REGISTRY),
        "probed": len(results),
        "counts": counts,
        "credential_skips": sorted(_credential_skips(results)),
        "max_credential_skips": MAX_SKIPPED_FOR_CREDENTIALS,
        "sources": {r.source_id: asdict(r) for r in results},
        "statement_collapses": diff_statement_counts(
            {r.source_id: asdict(r) for r in results}, previous
        ),
        "compared_against": (previous or {}).get("generated_at"),
    }


_ICON = {OK: "✅", DEGRADED: "⚠️", FAIL: "❌", SKIPPED: "⏭️"}


def render_markdown(report: dict[str, Any]) -> str:
    counts = report["counts"]
    lines = [
        "## OpenCheck source health",
        "",
        f"Swept {report['probed']} of {report['registry_size']} registry sources at "
        f"{report['generated_at']} — "
        f"{counts[OK]} ok, {counts[DEGRADED]} degraded, {counts[FAIL]} failed, "
        f"{counts[SKIPPED]} skipped.",
        "",
        "| Source | Status | Liveness | Latency | Detail |",
        "|---|---|---|---|---|",
    ]
    for source_id, row in report["sources"].items():
        latency = f"{row['latency_ms']} ms" if row["latency_ms"] is not None else "—"
        detail = row["reason"] or ""
        if row["attempts"] == 2 and row["status"] == OK:
            detail = "passed on retry"
        lines.append(
            f"| `{source_id}` | {_ICON[row['status']]} {row['status']} | "
            f"{row['liveness'] or '—'} | {latency} | {detail} |"
        )

    drift = report.get("dispatch_drift") or {}
    drift_bad = {sid: row for sid, row in drift.items() if row["status"] == FAIL}
    drift_uncovered = [sid for sid, row in drift.items() if row["status"] == SKIPPED]
    if drift:
        checked = sum(1 for row in drift.values() if row["status"] == OK)
        lines += [
            "",
            "### GLEIF dispatch drift",
            "",
            f"{checked} of {len(drift)} identifier-dispatched sources still resolve from their GLEIF "
            "anchor. A source can pass its own probe and still be unreachable in production if the "
            "anchor no longer derives its identifier.",
            "",
        ]
        if drift_bad:
            lines += [f"- ❌ `{sid}` — {row['reason']}" for sid, row in sorted(drift_bad.items())]
        if drift_uncovered:
            lines.append(
                f"- ⏭️ not covered (no anchor LEI): {', '.join(f'`{s}`' for s in sorted(drift_uncovered))}"
            )
        if not drift_bad and not drift_uncovered:
            lines.append("- ✅ no drift")

    collapses = report.get("statement_collapses") or {}
    compared = report.get("compared_against")
    lines += ["", "### BODS statement counts, week over week", ""]
    if not compared:
        lines.append(
            "- ⏭️ no comparison available — the previous run's artifact could not be fetched "
            "(retention expired, or the last run failed). This is **not** the same as "
            "\"nothing changed\"."
        )
    elif collapses:
        lines.append(f"Compared against the run of {compared}.")
        lines.append("")
        for sid, kinds in sorted(collapses.items()):
            detail = ", ".join(
                f"{kind} {v['was']} → {v['now']}" for kind, v in sorted(kinds.items())
            )
            lines.append(f"- ❌ `{sid}` — {detail}")
        lines += [
            "",
            "A source that still answers, still resolves live and still has every expected field "
            "can still have stopped carrying ownership edges. For a beneficial ownership source, "
            "this is the earliest machine-detectable sign of an access change.",
        ]
    else:
        lines.append(f"- ✅ no collapse against the run of {compared}.")

    gaps = {sid: row["known_gap"] for sid, row in report["sources"].items() if row["known_gap"]}
    if gaps:
        lines += ["", "### Known provenance gaps (tolerated, not fixed)", ""]
        lines += [f"- `{sid}` — {gap}" for sid, gap in sorted(gaps.items())]

    skips = report["credential_skips"]
    if skips:
        lines += [
            "",
            f"### Not exercised for want of a credential ({len(skips)} of "
            f"{report['max_credential_skips']} allowed)",
            "",
            "Green above does **not** mean these are healthy — they were not tested.",
            "",
        ]
        lines += [f"- `{sid}`" for sid in skips]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="source-health", help="output path prefix (writes .json and .md)")
    parser.add_argument("--only", default="", help="comma-separated source ids to probe")
    parser.add_argument("--timeout", type=float, default=60.0, help="per-source timeout in seconds")
    parser.add_argument("--concurrency", type=int, default=6, help="max simultaneous probes")
    parser.add_argument("--retry-delay", type=float, default=30.0, help="seconds to wait before the single retry")
    parser.add_argument("--no-retry", action="store_true", help="do not retry a failing probe")
    parser.add_argument("--data-root", default="", help="override OPENCHECK_DATA_ROOT")
    parser.add_argument(
        "--previous",
        default="",
        help="path to the previous run's source-health.json, for the week-over-week diff",
    )
    parser.add_argument(
        "--no-drift-check",
        action="store_true",
        help="skip the GLEIF dispatch-drift check (which needs GLEIF to be reachable)",
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="probe against the existing live cache instead of a clean one (debugging only — "
        "a cached answer proves nothing about the upstream)",
    )
    args = parser.parse_args()

    if args.data_root:
        os.environ["OPENCHECK_DATA_ROOT"] = args.data_root
    get_settings.cache_clear()

    if not args.use_cache:
        scratch = Path(tempfile.mkdtemp(prefix="opencheck-source-health-"))
        os.environ["OPENCHECK_DATA_ROOT"] = str(
            build_scratch_data_root(data_root(), scratch)
        )
        get_settings.cache_clear()
        disable_cache_reads()

    if args.only:
        wanted = [s.strip() for s in args.only.split(",") if s.strip()]
        unknown = sorted(set(wanted) - set(PROBES))
        if unknown:
            parser.error(f"unknown source id(s): {', '.join(unknown)}")
    else:
        wanted = sorted(PROBES)

    results = asyncio.run(
        sweep(
            wanted,
            timeout=args.timeout,
            concurrency=args.concurrency,
            retry_delay=args.retry_delay,
            retry=not args.no_retry,
        )
    )
    drift = asyncio.run(check_dispatch_drift(wanted)) if not args.no_drift_check else []
    previous = load_previous_report(args.previous)
    report = build_report(results, drift, previous)
    markdown = render_markdown(report)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")
    out.with_suffix(".md").write_text(markdown, encoding="utf-8")
    print(markdown)

    counts = report["counts"]
    exit_code = 0
    if counts[FAIL] or counts[DEGRADED]:
        exit_code = 1
    if report.get("statement_collapses"):
        print(
            "::error::BODS statement counts collapsed for "
            f"{', '.join(sorted(report['statement_collapses']))} — a source may still be "
            "answering while no longer carrying the statements OpenCheck depends on.",
            file=sys.stderr,
        )
        exit_code = 1
    if any(row["status"] == FAIL for row in (report.get("dispatch_drift") or {}).values()):
        print(
            "::error::GLEIF dispatch drift — one or more sources would no longer be reached "
            "from their anchor record, however healthy their own endpoint is.",
            file=sys.stderr,
        )
        exit_code = 1
    if len(report["credential_skips"]) > MAX_SKIPPED_FOR_CREDENTIALS:
        print(
            f"::error::{len(report['credential_skips'])} sources skipped for want of a "
            f"credential, more than the {MAX_SKIPPED_FOR_CREDENTIALS} allowed — a secret "
            "has expired or been removed, and coverage has shrunk.",
            file=sys.stderr,
        )
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
