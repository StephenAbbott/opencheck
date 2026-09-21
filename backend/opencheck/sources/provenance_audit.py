"""Does each adapter actually *record* where its answer came from?

Two places declare how current a source's payload is, and an adapter can do
one without the other:

* ``SourceHit.liveness`` — the field on the hit.
* ``provenance.record_*()`` — the recorder, which is what the pipeline
  resolves and the UI renders, and which defaults to ``stub`` when nothing is
  recorded.

``onrc_romania`` set the first and never the second, so every ONRC card in
production badged real Trade Register rows "Placeholder data — no live source
was contacted" (PR #275). ``ariregister`` was the same bug in Phase 45.

**A static check is not enough, and the reason is specific.** Grepping each
adapter module for ``provenance.record_``, an HTTP helper or a cache helper
reports 0 of 47 adapters missing a path — and ONRC would have passed that
check for the wrong reason, because it *did* set the hit field and it *did*
call a cache helper elsewhere in the module. The check has to run the read
path and look at what the recorder holds afterwards.

What lives here
---------------

``check_provenance`` is the verdict, shared with ``scripts/source_health.py``
so the weekly live sweep and the offline audit cannot disagree about what
counts as a provenance failure. It distinguishes three things the older
one-line check collapsed into "resolved 'stub'":

* the adapter returned a **stub bundle**, which production resolves to stub
  whatever was recorded (``Recorder.resolve(is_stub=True)`` short-circuits —
  the trap that makes a harness passing the wrong ``is_stub`` report a false
  clean, in both directions);
* the adapter **recorded nothing** on a real answer — the bug class above;
* the adapter recorded something, and it is **not what the probe expects**.

``audit`` runs every probe's read path offline, with the network blocked and
counted, and reports one row per source. It is the behavioural half: it never
reads ``SourceHit.liveness``, only the recorder.

What an offline run can and cannot conclude
-------------------------------------------

For a source that answers from a local store or a committed fixture, the whole
assertion holds offline: the read path runs for real and the resolved liveness
is comparable against ``expect_liveness``.

For a source that goes to the network, it is not — a blocked request means the
adapter takes its own failure path, and grading the liveness of that would be
grading the block. One rule still applies, and it is the one that failed in
Estonia: ``build_client()`` records a live observation **when the client is
constructed**, and the adapters that build their own client record one
explicitly *before the request*, by documented convention. So a request that
leaves an adapter while the recorder is still empty is a defect regardless of
what the upstream would have replied — and that is checkable with no network
at all.

The sources the offline run cannot grade fully are exactly the ones the weekly
live sweep does grade, so the two halves compose rather than overlap.
"""

from __future__ import annotations

import asyncio
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import httpx

from .. import provenance
from ..provenance import Provenance
from .probes import PROBES, SourceProbe, skip_reason

#: Functions that go to the network but are **not** answering a lookup, so
#: they have no provenance observation of their own to record: they fetch a
#: bulk artifact to disk, and the lookup that later reads it records its own.
#: Keyed by ``(module filename, function name)`` with the reason, so every
#: hole in the guard is visible and reviewable rather than implicit.
#:
#: Read by both halves of the guard — the AST check in
#: ``tests/test_source_probes.py`` and the request-time rule below, which
#: walks the stack for these frames. One list, because an exemption that is
#: true of one check and not the other is how a hole opens quietly. Keep it
#: short, and ``test_exemptions_are_all_still_needed`` deletes stale entries.
BULK_ARTIFACT_FETCHERS: dict[tuple[str, str], str] = {
    ("climatetrace.py", "_download_gem_csvs_from_gcs"): (
        "downloads the GEM bulk CSVs to disk; the lookup that later reads them "
        "records its own provenance"
    ),
    ("climatetrace.py", "_ensure_gem_data"): "bulk artifact refresh, not a lookup",
    ("climatetrace.py", "_ensure_gleif_gem_data"): "bulk artifact refresh, not a lookup",
}

# --- the verdict, shared with the weekly sweep ------------------------------

PROVENANCE_OK = "ok"
STUB_BUNDLE = "stub_bundle"
UNRECORDED = "unrecorded"
MISMATCH = "mismatch"


@dataclass(frozen=True)
class ProvenanceVerdict:
    outcome: str
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.outcome == PROVENANCE_OK


def is_stub_bundle(result: Any) -> bool:
    """The pipeline's own rule, in one place.

    ``routers/lookup.py`` resolves provenance as
    ``recorder.resolve(is_stub=bool(raw.get("is_stub")))``. A harness that
    resolves without it grades something production never renders: an adapter
    that records a live observation and then returns a stub bundle reads
    "live" to the grader and "Placeholder data" to the reader.
    """
    return bool(result.get("is_stub")) if isinstance(result, dict) else False


def check_provenance(
    probe: SourceProbe,
    prov: Provenance,
    *,
    observations: tuple[provenance.Observation, ...],
    is_stub: bool,
) -> ProvenanceVerdict:
    """Compare what the adapter recorded against what the probe expects."""
    expected = "/".join(sorted(probe.expect_liveness))
    if is_stub and "stub" not in probe.expect_liveness:
        return ProvenanceVerdict(
            STUB_BUNDLE,
            "the adapter returned a stub bundle, which production resolves to "
            "'Placeholder data — no live source was contacted' whatever was "
            f"recorded — so nothing about {expected} was exercised",
        )
    if not observations and not is_stub:
        return ProvenanceVerdict(
            UNRECORDED,
            f"recorded no provenance observation at all; expected {expected}. "
            "Real data from this source is badged 'Placeholder data' in "
            "production (the PR #153 / #275 bug class)",
        )
    if prov.liveness not in probe.expect_liveness:
        recorded = ", ".join(sorted({o.liveness for o in observations})) or "nothing"
        return ProvenanceVerdict(
            MISMATCH,
            f"provenance: expected {expected}, resolved '{prov.liveness}' "
            f"(recorded: {recorded})",
        )
    return ProvenanceVerdict(PROVENANCE_OK)


# --- the offline audit ------------------------------------------------------

AGREES = "agrees"
DISAGREES = "disagrees"
UNRECORDED_ROW = "unrecorded"
NOT_COMPARABLE = "not_comparable"
ERRORED = "errored"
SKIPPED = "skipped"


@dataclass
class AuditRow:
    """One adapter's behaviour, as the recorder saw it."""

    source_id: str
    outcome: str
    reason: str = ""
    liveness: str | None = None
    expected: tuple[str, ...] = ()
    requests: int = 0
    recorded: tuple[str, ...] = ()
    is_stub: bool = False

    @property
    def is_defect(self) -> bool:
        return self.outcome in {DISAGREES, UNRECORDED_ROW}


@dataclass
class _Network:
    """What left the adapter while the network was blocked."""

    requests: int = 0
    unrecorded: list[str] = field(default_factory=list)
    artifact_fetches: int = 0
    off_scope: int = 0


@contextmanager
def block_network() -> Iterator[_Network]:
    """Refuse every outbound HTTP request, counting them and noting any that
    left an adapter before it had recorded anything.

    Patched at ``Client.send`` rather than at the transport, so an adapter
    that builds its own client with its own transport is intercepted too —
    which is the population this audit is about. Hosts only, never a full URL:
    several probe subjects are identifiers, and those belong in no report.

    Not re-entrant and not concurrency-safe: it patches httpx for the process,
    so ``audit`` runs its sources one at a time.

    What the rule cannot see, said plainly: a request issued from a thread
    outside the provenance scope — an index sync started in the background, as
    ``apr_serbia`` and ``asp_moldova`` do — has no recorder to check, because
    a new thread starts with an empty context. Those are counted as
    ``off_scope`` rather than judged. They are also not the bug class: such a
    request is not the one answering the lookup, and the read that does answer
    records for itself on the main path.
    """
    state = _Network()
    original_async = httpx.AsyncClient.send
    original_sync = httpx.Client.send

    def _from_a_bulk_artifact_fetcher() -> bool:
        return any(
            (Path(frame.filename).name, frame.name) in BULK_ARTIFACT_FETCHERS
            for frame in traceback.extract_stack()
        )

    def _note(request: httpx.Request) -> None:
        state.requests += 1
        recorder = provenance.current_recorder()
        if recorder is None:
            state.off_scope += 1
            return
        if recorder.observations:
            return
        if _from_a_bulk_artifact_fetcher():
            state.artifact_fetches += 1
            return
        state.unrecorded.append(request.url.host or "(no host)")

    async def _async_send(self: httpx.AsyncClient, request: httpx.Request, **kwargs: Any):
        _note(request)
        raise httpx.ConnectError("blocked by the provenance audit", request=request)

    def _sync_send(self: httpx.Client, request: httpx.Request, **kwargs: Any):
        _note(request)
        raise httpx.ConnectError("blocked by the provenance audit", request=request)

    httpx.AsyncClient.send = _async_send  # type: ignore[method-assign]
    httpx.Client.send = _sync_send  # type: ignore[method-assign]
    try:
        yield state
    finally:
        httpx.AsyncClient.send = original_async  # type: ignore[method-assign]
        httpx.Client.send = original_sync  # type: ignore[method-assign]


async def audit_source(
    source_id: str, probe: SourceProbe, *, timeout: float = 30.0
) -> AuditRow:
    """Run one probe's read path offline and report what the recorder held.

    Never raises: an adapter that blows up with no network is reported, not
    propagated — the point of the harness is the whole table, and a single
    adapter that cannot survive a ``ConnectError`` would otherwise hide the
    rest.
    """
    from . import REGISTRY

    expected = tuple(sorted(probe.expect_liveness))
    # Not exercised is not the same as clean. A source whose credential or
    # bulk store is absent answers from nothing and records nothing, which
    # looks exactly like the defect this harness hunts — so it is separated
    # out first, by the same rule the weekly sweep uses.
    skip = skip_reason(probe)
    if skip is not None:
        return AuditRow(source_id, SKIPPED, reason=skip[1], expected=expected)

    adapter = REGISTRY[source_id]
    call = getattr(adapter, probe.method)

    error = ""
    result: Any = None
    with block_network() as net, provenance.recording() as recorder:
        try:
            result = await asyncio.wait_for(
                call(*probe.args, **dict(probe.kwargs)), timeout=timeout
            )
        except asyncio.TimeoutError:
            error = f"did not return within {timeout:.0f}s with the network blocked"
        except Exception as exc:  # noqa: BLE001 — the table matters more
            error = f"{type(exc).__name__}: {exc}"[:200]

    observations = recorder.observations
    recorded = tuple(o.liveness for o in observations)

    # 1. The Estonia rule, and the only one an offline run can apply to a
    #    network source: a request left the adapter with nothing recorded.
    #    Reported first — it is a defect whatever else the call did.
    if net.unrecorded:
        return AuditRow(
            source_id,
            UNRECORDED_ROW,
            reason=(
                f"a request to {net.unrecorded[0]} left the adapter before any "
                "provenance observation was recorded — build_client() records one "
                "on construction, and an adapter building its own client must "
                "record one itself, before the request"
            ),
            expected=expected,
            requests=net.requests,
            recorded=recorded,
        )

    if error:
        return AuditRow(
            source_id,
            ERRORED,
            reason=error,
            expected=expected,
            requests=net.requests,
            recorded=recorded,
        )

    stub = is_stub_bundle(result)
    prov = recorder.resolve(is_stub=stub)
    row = AuditRow(
        source_id,
        NOT_COMPARABLE,
        liveness=prov.liveness,
        expected=expected,
        requests=net.requests,
        recorded=recorded,
        is_stub=stub,
    )

    # 2. The adapter took its own "nothing to say" path — refused upstream, or
    #    a store it reads is not here. Production resolves that to stub
    #    whatever was recorded, so there is no provenance claim to grade.
    if stub:
        row.reason = (
            "the adapter returned a stub bundle, so nothing about "
            f"{'/'.join(expected)} was exercised"
        )
        return row

    # 3. Nothing recorded on a real answer: the whole bug class, and the one
    #    conclusion that needs no upstream at all.
    if not observations:
        row.outcome = UNRECORDED_ROW
        row.reason = check_provenance(
            probe, prov, observations=observations, is_stub=stub
        ).reason
        return row

    # 4. A local cache answered where the probe expects something else. True
    #    on a developer's machine with a warm cache and never in CI, where
    #    data/cache/live is gitignored — reported rather than graded, because
    #    the alternative is a check that passes or fails by local history.
    if prov.liveness == "cached" and "cached" not in probe.expect_liveness:
        row.reason = "served from OpenCheck's local cache — nothing upstream was read"
        return row

    verdict = check_provenance(probe, prov, observations=observations, is_stub=stub)
    if verdict.ok:
        # Worth stating why this counts even when a request was blocked.
        # ``build_client()`` records ``live`` when the client is constructed,
        # not when the response arrives, and resolution takes the *worst*
        # liveness across a fetch — so a source that still resolves to what
        # its probe expects, having been refused the network, resolves the
        # same way when the network answers.
        row.outcome = AGREES
        return row

    row.reason = verdict.reason
    if net.requests:
        # A mismatch here may describe the block rather than the adapter: an
        # adapter denied its upstream can skip the store read that would have
        # recorded the snapshot. Reported, not failed — and the pinned set in
        # ``OFFLINE_COMPARED`` is what stops a source quietly sliding into
        # this bucket instead of agreeing.
        row.reason += (
            " — with the network blocked this may describe the block rather "
            "than the adapter; the weekly sweep grades it for real"
        )
        return row

    row.outcome = DISAGREES
    return row


async def audit(
    source_ids: list[str] | None = None, *, timeout: float = 30.0
) -> list[AuditRow]:
    """Every probe's read path, one at a time (``block_network`` is global)."""
    wanted = source_ids if source_ids is not None else sorted(PROBES)
    rows: list[AuditRow] = []
    for source_id in wanted:
        rows.append(await audit_source(source_id, PROBES[source_id], timeout=timeout))
    return rows


#: The sources whose provenance an offline run grades in full, pinned by id
#: rather than counted.
#:
#: Coverage here is a property of the checkout, not of the adapters: a source
#: skips when its credential or its bulk store is absent, and the rest are
#: graded only as far as a blocked request allows. A bare count would let a
#: source slide from ``agrees`` into ``not_comparable`` while another arrived
#: and kept the total level — silently trading a graded assertion for an
#: ungraded one, which is the failure shape this whole area is about. A named
#: set says which.
#:
#: Adding a source here is a gain and needs no ceremony. **Removing one is a
#: statement** that its provenance can no longer be checked without the
#: network, and belongs in the commit that made that true.
OFFLINE_COMPARED = frozenset(
    {
        "anaf_romania",
        "cac_nigeria",
        "cnpj_brazil",
        "eiti",
        "eiti_assessment",
        "eiti_bo",
        "eiti_soe",
        "jar_lithuania",
    }
)

#: How many adapters must actually issue a request under the block, so the
#: request-time rule is exercised rather than merely present. 26 do today; the
#: floor leaves room for a couple to change shape without becoming a ceiling.
MIN_REQUEST_TIME_CHECKS = 20


def render(rows: list[AuditRow]) -> str:
    """A short table, for a one-off run and for a test's failure message."""
    width = max((len(r.source_id) for r in rows), default=10)
    lines = []
    for row in sorted(rows, key=lambda r: (not r.is_defect, r.outcome, r.source_id)):
        liveness = row.liveness or "—"
        lines.append(
            f"{row.source_id:<{width}}  {row.outcome:<14} {liveness:<9} "
            f"expect {'/'.join(row.expected) or '—'}"
            + (f"  — {row.reason}" if row.reason else "")
        )
    return "\n".join(lines)
