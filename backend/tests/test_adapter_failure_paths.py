"""A register that does not answer is never read as "no record", for every adapter.

The codebase review of 20 Nov 2026 found adapters that returned an empty
stub, or an empty card, when their upstream answered 5xx or timed out —
without recording a degradation. ``_source_outcome`` turns a stub into "Did
not answer" only when a ``source_fetch`` degradation says so, so each of those
outages read as "Answered with no record", which is a statement about the
company. PR 653 fixed Latvia; the review fixed KRS, ABR and Firmenbuch; this
module looks for the rest and stops the next one.

Each registered adapter's own probe read path (``sources/probes.py``, the one
the weekly sweep and the provenance audit run) is called with every outbound
request answered **503**, then again with every request **timing out**.
Acceptable outcomes:

* the adapter raises — the pipeline reports a ``source_error``;
* it returns a stub and records a ``source_fetch`` degradation for itself;
* it returns a card and records a degradation (``source_read`` beside what
  it did read).

And whatever it returns: nothing is written to the response cache (the
Phase 262 rule — a failure is never cached), and the exception's text never
reaches the bundle (the ``secret_scrub`` rule; the marker below is in every
failure's message and body).

Not exercised, and said so rather than counted as clean: a source whose
credential or bulk store is absent (``skip_reason``) and a source that sends
no request without its store. A 404 — "no record" — is answered differently
by every register and is each adapter's own test.

``KNOWN_GAPS`` names the adapters that fail today, each with what it does.
Fixing one means deleting its entry; ``test_known_gaps_are_still_gaps`` fails
on an entry that has started to pass, so the list only shrinks.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from opencheck import cache, degradation, provenance
from opencheck.config import get_settings
from opencheck.sources import REGISTRY
from opencheck.sources.probes import PROBES, skip_reason
from opencheck.sources.provenance_audit import is_stub_bundle

MARKER = "OPENCHECK-FAILURE-AUDIT-MARKER"
MODES = ("http_503", "timeout")

#: Adapters that fail this check today, with what they do. Delete an entry in
#: the commit that fixes it.
KNOWN_GAPS: dict[str, str] = {}

#: How many adapters must actually be graded, so the check cannot hollow out
#: quietly as sources gain stores or credentials. 45 are graded today.
MIN_GRADED = 35


@dataclass
class Outcome:
    source_id: str
    mode: str
    requests: int = 0
    raised: str = ""
    stub: bool = False
    fetch_degraded: bool = False
    degraded: bool = False
    cached: list[str] = field(default_factory=list)
    leaked: bool = False

    @property
    def graded(self) -> bool:
        return self.requests > 0

    def problems(self) -> list[str]:
        out = []
        if not self.raised:
            if self.stub and not self.fetch_degraded:
                out.append("stub with no source_fetch degradation")
            if not self.stub and not self.degraded:
                out.append("card with no degradation (counted as answered)")
        if self.cached:
            out.append(f"wrote to the cache: {', '.join(self.cached)}")
        if self.leaked:
            out.append("the failure's text reached the bundle")
        return out


async def _run(source_id: str, mode: str, puts: list[str]) -> Outcome:
    probe = PROBES[source_id]
    outcome = Outcome(source_id, mode)

    def answer(request: httpx.Request) -> httpx.Response:
        outcome.requests += 1
        if mode == "http_503":
            return httpx.Response(503, text=MARKER, request=request)
        raise httpx.ReadTimeout(MARKER, request=request)

    async def _async_send(self, request, **kwargs):  # type: ignore[no-untyped-def]
        return answer(request)

    def _sync_send(self, request, **kwargs):  # type: ignore[no-untyped-def]
        return answer(request)

    original = httpx.AsyncClient.send, httpx.Client.send
    httpx.AsyncClient.send = _async_send  # type: ignore[method-assign]
    httpx.Client.send = _sync_send  # type: ignore[method-assign]
    puts.clear()
    result: Any = None
    try:
        with degradation.recording() as records, provenance.recording():
            try:
                call = getattr(REGISTRY[source_id], probe.method)
                result = await asyncio.wait_for(
                    call(*probe.args, **dict(probe.kwargs)), timeout=60
                )
            except Exception as exc:  # noqa: BLE001 — raising is an answer
                outcome.raised = type(exc).__name__
    finally:
        httpx.AsyncClient.send, httpx.Client.send = original  # type: ignore[method-assign]

    own = [d for d in records if d.source_id == source_id]
    outcome.degraded = bool(own)
    outcome.fetch_degraded = any(d.check == degradation.CHECK_SOURCE_FETCH for d in own)
    outcome.stub = result is None or is_stub_bundle(result)
    outcome.cached = list(puts)
    outcome.leaked = MARKER in json.dumps(result, default=str)
    return outcome


@pytest.fixture(scope="module")
def outcomes() -> dict[tuple[str, str], Outcome]:
    """Every exercisable adapter, under both failures, once."""
    saved = dict(os.environ)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    # A fake value for every credential a probe needs: the request is answered
    # by the failure either way, and without it the adapter never sends one.
    for probe in PROBES.values():
        for name in probe.requires_env:
            os.environ.setdefault(name, "failure-audit")
    get_settings.cache_clear()

    puts: list[str] = []
    original_put, original_absent = cache.Cache.put, cache.Cache.put_absent

    def _put(self, key, payload):  # type: ignore[no-untyped-def]
        puts.append(key)
        return self._path(key) if hasattr(self, "_path") else None

    def _put_absent(self, key):  # type: ignore[no-untyped-def]
        puts.append(f"{key} (absent)")
        return self._path(key) if hasattr(self, "_path") else None

    cache.Cache.put = _put  # type: ignore[method-assign]
    cache.Cache.put_absent = _put_absent  # type: ignore[method-assign]

    async def _all() -> dict[tuple[str, str], Outcome]:
        table: dict[tuple[str, str], Outcome] = {}
        for source_id in sorted(PROBES):
            if source_id not in REGISTRY or skip_reason(PROBES[source_id]):
                continue
            for mode in MODES:
                table[(source_id, mode)] = await _run(source_id, mode, puts)
        return table

    try:
        return asyncio.run(_all())
    finally:
        cache.Cache.put, cache.Cache.put_absent = original_put, original_absent  # type: ignore[method-assign]
        os.environ.clear()
        os.environ.update(saved)
        get_settings.cache_clear()


def _failing(outcomes: dict[tuple[str, str], Outcome], source_id: str) -> list[str]:
    return [
        f"{mode}: {problem}"
        for mode in MODES
        if (o := outcomes.get((source_id, mode))) and o.graded
        for problem in o.problems()
    ]


def test_no_adapter_reads_a_failed_upstream_as_an_answer(outcomes) -> None:
    graded = {s for (s, _), o in outcomes.items() if o.graded}
    failures = {
        s: _failing(outcomes, s) for s in sorted(graded) if s not in KNOWN_GAPS
    }
    failures = {s: p for s, p in failures.items() if p}
    assert not failures, "\n".join(f"{s}: {'; '.join(p)}" for s, p in failures.items())


def test_known_gaps_are_still_gaps(outcomes) -> None:
    fixed = [s for s in KNOWN_GAPS if not _failing(outcomes, s)]
    assert not fixed, (
        "these adapters now pass — delete them from KNOWN_GAPS: " + ", ".join(fixed)
    )


def test_enough_adapters_are_graded(outcomes) -> None:
    graded = {s for (s, _), o in outcomes.items() if o.graded}
    assert len(graded) >= MIN_GRADED, sorted(graded)


def test_every_known_gap_names_a_registered_source() -> None:
    assert set(KNOWN_GAPS) <= set(REGISTRY)
