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
    absent_env = missing_env(probe, dict(os.environ))
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


def _credential_skips(results: list[Result]) -> list[str]:
    return [r.source_id for r in results if r.status == SKIPPED and "not configured" in r.reason]


def build_report(results: list[Result]) -> dict[str, Any]:
    counts = {status: sum(1 for r in results if r.status == status) for status in (OK, DEGRADED, FAIL, SKIPPED)}
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "registry_size": len(REGISTRY),
        "probed": len(results),
        "counts": counts,
        "credential_skips": sorted(_credential_skips(results)),
        "max_credential_skips": MAX_SKIPPED_FOR_CREDENTIALS,
        "sources": {r.source_id: asdict(r) for r in results},
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
    report = build_report(results)
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
