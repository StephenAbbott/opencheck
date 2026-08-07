"""memwatch — lightweight memory + traffic instrumentation for OOM diagnosis.

Motivation (2026-08-06): the Render instance (512 MB Starter, since upgraded)
hit repeated "Ran out of memory" restarts within ~48h of the SEO entity pages
going live — but Render's own metrics only show *that* memory climbed, not
*which traffic* was in flight when it did, and GoatCounter is JS-based so
crawler traffic is invisible to it. This module makes the next OOM
self-diagnosing from the Render log stream alone:

* **Access log middleware** — one line per request with method, path, status,
  duration, response size and the User-Agent (plus a ``bot=`` flag), so the
  human/crawler split and the hot paths are readable straight from the logs.
* **Periodic memory report** — every ``OPENCHECK_MEMWATCH_INTERVAL`` seconds
  (default 30), one ``memwatch`` line with current RSS, the container memory
  limit (read from cgroups — Render enforces the plan limit there), the
  in-process cache sizes (OG-card cache, lookup replay cache), and the
  request counts per path bucket since the previous line. When RSS crosses
  ``OPENCHECK_MEMWATCH_WARN_PCT`` (default 85%) the line is logged at
  WARNING and — when ``OPENCHECK_MEMWATCH_TRACEMALLOC=1`` — followed by the
  top Python allocation sites, so the culprit is named *before* the kernel
  kills the process.

Reading the logs after an OOM::

    memwatch rss_mb=489.2 limit_mb=512 pct=95.5 og_cache=128 replay_cache=7 \
        reqs=412 bots=397 inflight_peak=14 top=/og:230,/entity:140,/sitemaps:12

→ RSS at 95% of the limit while 397 of 412 requests in the window were bots
and ``/og`` dominated: the OG-card renderer under crawler load is the suspect.

Everything here is deliberately dependency-free (``/proc`` + ``resource``,
no psutil) and fails soft: an instrumentation error must never take down or
slow the API. Log lines go through a dedicated ``opencheck.memwatch`` /
``opencheck.access`` logger with its own stderr handler, because neither
uvicorn nor the app configures the root logger — without this, INFO lines
would vanish in production.
"""

from __future__ import annotations

import asyncio
import logging
import os
import resource
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request, Response

log = logging.getLogger("opencheck.memwatch")
access_log = logging.getLogger("opencheck.access")


def _ensure_handler(logger: logging.Logger) -> None:
    """INFO lines must reach stderr even with an unconfigured root logger.

    Python's last-resort handler only emits WARNING+, and neither uvicorn nor
    the app calls ``basicConfig`` — so without an explicit handler the whole
    point of this module (INFO-level forensics in the Render log stream) is
    silently lost. Propagation stays on so pytest's ``caplog`` still works;
    duplicate lines would only occur if someone later configures the root
    logger with its own handler, which is an acceptable trade.
    """
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)


_ensure_handler(log)
_ensure_handler(access_log)


# ---------------------------------------------------------------------------
# Memory readings
# ---------------------------------------------------------------------------


def current_rss_mb() -> float:
    """Current resident set size in MB.

    Linux (production): field 2 of ``/proc/self/statm`` is resident pages.
    macOS (dev): no /proc — fall back to ``ru_maxrss``, which is the *peak*,
    not current, RSS (and in bytes rather than KB). Good enough for dev.
    """
    try:
        with open("/proc/self/statm", encoding="ascii") as fh:
            pages = int(fh.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, ValueError, IndexError):
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return maxrss / (1024 * 1024) if sys.platform == "darwin" else maxrss / 1024


# Values this large mean "no limit set" (cgroup v1 reports LONG_MAX-ish).
_CGROUP_NO_LIMIT = 1 << 60

# cgroup v2 first (Render / modern Docker), then v1.
_CGROUP_LIMIT_FILES = (
    "/sys/fs/cgroup/memory.max",
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
)


def memory_limit_mb() -> float | None:
    """The container's memory limit in MB, or ``None`` when unlimited/unknown.

    ``OPENCHECK_MEMORY_LIMIT_MB`` overrides (useful in dev and in tests);
    otherwise the cgroup limit is read — on Render this is the plan's RAM.
    """
    from .config import get_settings

    override = get_settings().memory_limit_mb
    if override:
        return float(override)
    for path in _CGROUP_LIMIT_FILES:
        try:
            with open(path, encoding="ascii") as fh:
                raw = fh.read().strip()
        except OSError:
            continue
        if raw == "max":
            return None
        try:
            value = int(raw)
        except ValueError:
            continue
        if value >= _CGROUP_NO_LIMIT:
            return None
        return value / (1024 * 1024)
    return None


# ---------------------------------------------------------------------------
# Request window counters (reset every memwatch interval)
# ---------------------------------------------------------------------------

#: Substrings that mark a User-Agent as automated. Deliberately broad: for
#: OOM forensics a false "bot" on an obscure UA is harmless, while missing
#: the actual crawler fleet defeats the purpose. Matched lowercase.
_BOT_MARKERS = (
    "bot",  # Googlebot, Bingbot, DuckDuckBot, ClaudeBot, GPTBot, AhrefsBot…
    "crawl",
    "spider",
    "slurp",
    "bingpreview",
    "headless",
    "python",
    "curl",
    "wget",
    "go-http-client",
    "okhttp",
    "java/",
    "libwww",
    "facebookexternalhit",
    "meta-externalagent",
    "yandex",
    "baidu",
    "petal",
    "semrush",
    "mj12",
    "dataforseo",
    "scrapy",
    "httpx",
    "aiohttp",
)


def is_bot(user_agent: str | None) -> bool:
    """Best-effort automated-client detection from the User-Agent string.

    An *empty* UA also counts as a bot — real browsers always send one.
    """
    if not user_agent:
        return True
    ua = user_agent.lower()
    return any(marker in ua for marker in _BOT_MARKERS)


def bucket(path: str) -> str:
    """Collapse a request path to its first segment (``/entity/529900…`` →
    ``/entity``) so per-window counters stay tiny and the memwatch line
    stays one line. The full path still appears on the access-log line."""
    segment = path.split("/", 2)[1] if path.startswith("/") and len(path) > 1 else ""
    return f"/{segment}" if segment else "/"


class _Window:
    """Counters for the current memwatch interval. Event-loop-only mutation
    (the middleware is async), so no locking is needed."""

    def __init__(self) -> None:
        self.requests = 0
        self.bots = 0
        self.by_bucket: Counter[str] = Counter()
        self.bot_by_bucket: Counter[str] = Counter()
        self.inflight = 0
        self.inflight_peak = 0

    def snapshot_and_reset(self) -> dict[str, Any]:
        snap = {
            "requests": self.requests,
            "bots": self.bots,
            "by_bucket": dict(self.by_bucket),
            "bot_by_bucket": dict(self.bot_by_bucket),
            "inflight_peak": self.inflight_peak,
        }
        self.requests = 0
        self.bots = 0
        self.by_bucket.clear()
        self.bot_by_bucket.clear()
        self.inflight_peak = self.inflight  # carry current in-flight forward
        return snap


window = _Window()


class _Totals:
    """Cumulative counters since process start (i.e. since the last deploy —
    Render restarts reset them, which is fine for share-of-traffic reading).
    These back the public ``/memstats`` endpoint so the weekly ``/og``-share
    check can run against production without credentials or log access."""

    def __init__(self) -> None:
        self.started = time.time()
        self.requests = 0
        self.bots = 0
        self.by_bucket: Counter[str] = Counter()
        self.bot_by_bucket: Counter[str] = Counter()


totals = _Totals()


def stats() -> dict[str, Any]:
    """Aggregate-only snapshot for ``/memstats``.

    Deliberately contains NO per-request data: no IPs, no User-Agents, no
    LEIs, no query strings — only counts per first-path-segment bucket and
    the same memory figures the memwatch log line reports. That keeps a
    public, unauthenticated endpoint consistent with the project's
    privacy posture (cf. the GoatCounter path-bucket contract)."""
    rss = current_rss_mb()
    limit = memory_limit_mb()
    og, replay = _cache_sizes()
    return {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(totals.started)),
        "uptime_s": int(time.time() - totals.started),
        "rss_mb": round(rss, 1),
        "limit_mb": round(limit, 0) if limit else None,
        "pct": round(rss / limit * 100, 1) if limit else None,
        "og_cache": og,
        "replay_cache": replay,
        "totals": {
            "requests": totals.requests,
            "bots": totals.bots,
            "by_bucket": dict(totals.by_bucket),
            "bot_by_bucket": dict(totals.bot_by_bucket),
        },
    }

#: Paths whose access-log lines are pure noise (Render pings /health every
#: few seconds; hashed SPA assets are static file serving). They still count
#: in the window buckets, so a burst would show on the memwatch line.
_ACCESS_LOG_SKIP = ("/health", "/assets")

_UA_MAX = 140  # keep lines greppable; crawler UAs identify themselves early


async def access_log_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Count every request into the window and log one access line.

    Registered LAST in app.py so it is the outermost middleware — it must
    see requests even when an inner middleware (the SPA view negotiation)
    answers without calling further in. ``dur_ms`` is time to response
    *start*: for SSE streams the body continues after the line is logged.
    """
    from .config import get_settings

    path = request.url.path
    ua = request.headers.get("user-agent", "")
    bot = is_bot(ua)
    b = bucket(path)

    window.requests += 1
    window.by_bucket[b] += 1
    totals.requests += 1
    totals.by_bucket[b] += 1
    if bot:
        window.bots += 1
        window.bot_by_bucket[b] += 1
        totals.bots += 1
        totals.bot_by_bucket[b] += 1
    window.inflight += 1
    window.inflight_peak = max(window.inflight_peak, window.inflight)

    start = time.perf_counter()
    try:
        response = await call_next(request)
    finally:
        window.inflight -= 1

    try:  # logging must never break a response
        if get_settings().access_log_enabled and b not in _ACCESS_LOG_SKIP:
            query = request.url.query
            target = f"{path}?{query}" if query else path
            client = request.headers.get("x-forwarded-for", "")
            client = client.split(",")[0].strip() or (
                request.client.host if request.client else "-"
            )
            access_log.info(
                'access method=%s path="%s" status=%d dur_ms=%.0f bytes=%s '
                'bot=%d ip=%s ua="%s"',
                request.method,
                target[:400],
                response.status_code,
                (time.perf_counter() - start) * 1000,
                response.headers.get("content-length", "-"),
                int(bot),
                client,
                ua[:_UA_MAX].replace('"', "'"),
            )
    except Exception:  # noqa: BLE001
        log.exception("access log line failed")
    return response


# ---------------------------------------------------------------------------
# Periodic memory report
# ---------------------------------------------------------------------------


def _cache_sizes() -> tuple[int, int]:
    """(OG-card cache entries, lookup replay cache entries) — the two
    unbounded-ish in-process caches worth watching. Import lazily and fail
    soft: instrumentation must not couple module import order."""
    og = replay = 0
    try:
        from .routers.share import _OG_CACHE

        og = len(_OG_CACHE)
    except Exception:  # noqa: BLE001
        pass
    try:
        from .routers.lookup import _REPLAY_CACHE

        replay = len(_REPLAY_CACHE)
    except Exception:  # noqa: BLE001
        pass
    return og, replay


def _top_buckets(by_bucket: dict[str, int], n: int = 5) -> str:
    ranked = sorted(by_bucket.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{b}:{c}" for b, c in ranked) or "-"


def tick() -> None:
    """Emit one memwatch line (and the tracemalloc top on high water)."""
    from .config import get_settings

    settings = get_settings()
    rss = current_rss_mb()
    limit = memory_limit_mb()
    snap = window.snapshot_and_reset()
    og, replay = _cache_sizes()

    pct = (rss / limit * 100) if limit else None
    line = (
        f"memwatch rss_mb={rss:.1f} limit_mb={limit:.0f} pct={pct:.1f}"
        if limit
        else f"memwatch rss_mb={rss:.1f} limit_mb=? pct=?"
    )
    line += (
        f" og_cache={og} replay_cache={replay}"
        f" reqs={snap['requests']} bots={snap['bots']}"
        f" inflight_peak={snap['inflight_peak']}"
        f" top={_top_buckets(snap['by_bucket'])}"
        f" bot_top={_top_buckets(snap['bot_by_bucket'])}"
    )

    high_water = pct is not None and pct >= settings.memwatch_warn_pct
    log.log(logging.WARNING if high_water else logging.INFO, "%s", line)

    if high_water and settings.memwatch_tracemalloc:
        import tracemalloc

        if tracemalloc.is_tracing():
            for stat in tracemalloc.take_snapshot().statistics("lineno")[:8]:
                log.warning("memwatch tracemalloc %s", stat)


async def run() -> None:
    """The periodic reporter, started from the app lifespan.

    ``OPENCHECK_MEMWATCH_INTERVAL=0`` disables it entirely. tracemalloc
    tracing (opt-in — it costs memory and a little CPU) starts here so the
    high-water snapshot in :func:`tick` has data to report.
    """
    from .config import get_settings

    settings = get_settings()
    interval = settings.memwatch_interval_s
    if interval <= 0:
        log.info("memwatch disabled (OPENCHECK_MEMWATCH_INTERVAL=0)")
        return
    if settings.memwatch_tracemalloc:
        import tracemalloc

        if not tracemalloc.is_tracing():
            tracemalloc.start(10)
    limit = memory_limit_mb()
    log.info(
        "memwatch started interval_s=%s limit_mb=%s warn_pct=%s tracemalloc=%s",
        interval,
        f"{limit:.0f}" if limit else "?",
        settings.memwatch_warn_pct,
        int(settings.memwatch_tracemalloc),
    )
    while True:
        await asyncio.sleep(interval)
        try:
            tick()
        except Exception:  # noqa: BLE001 — the reporter must survive anything
            log.exception("memwatch tick failed")
