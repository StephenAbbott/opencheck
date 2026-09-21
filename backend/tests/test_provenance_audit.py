"""The behavioural half of the provenance guard.

``tests/test_source_probes.py`` reads the adapters' *source code*: an httpx
client built without a ``record_`` call nearby. That check is worth keeping and
it is not enough, for a reason worth stating precisely: ``onrc_romania`` would
have passed it. The module did call a provenance helper, and the adapter did
set ``SourceHit.liveness`` — but the read path the lookup takes recorded
nothing, so every ONRC card in production badged real Trade Register rows
"Placeholder data — no live source was contacted" (PR #275). The same bug was
``ariregister`` in Phase 45, and both were found by reading live production
output rather than by any test.

So this module runs each adapter's own probe read path inside a
``provenance.recording()`` scope, with the network blocked and counted, and
looks at what the recorder holds. It never reads ``SourceHit.liveness``, which
is the field that made the static check pass for the wrong reason.

Offline, and in about three seconds, because it gates every PR — the weekly
sweep catches this once a week and only for the sources that ran.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from opencheck import provenance
from opencheck.config import get_settings
from opencheck.provenance import Observation, Provenance
from opencheck.sources import provenance_audit as audit_mod
from opencheck.sources.probes import PROBES, SourceProbe

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

_had_allow_live = os.environ.get("OPENCHECK_ALLOW_LIVE")
import source_health as sweep  # noqa: E402

if _had_allow_live is None:
    os.environ.pop("OPENCHECK_ALLOW_LIVE", None)


@pytest.fixture(scope="module")
def rows(tmp_path_factory: pytest.TempPathFactory) -> list[audit_mod.AuditRow]:
    """Every adapter, once, against a data root with a cold live cache.

    Live mode is on: with it off most adapters short-circuit before touching
    the network, and the request-time rule would then be exercised by nobody.
    The network is blocked throughout, so "on" costs no round-trip.

    The data root is the sweep's own scratch root — every committed artifact
    symlinked, ``cache/live`` empty — for the sweep's own reason. Without it a
    developer's warm cache answers for several sources, which both reduces
    what is graded and writes this run's answers into the cache for the next
    one.
    """
    from opencheck.cache import data_root

    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    previous_live = os.environ.get("OPENCHECK_ALLOW_LIVE")
    scratch = sweep.build_scratch_data_root(
        data_root(), tmp_path_factory.mktemp("provenance-audit-root")
    )
    os.environ["OPENCHECK_DATA_ROOT"] = str(scratch)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    get_settings.cache_clear()
    try:
        return asyncio.run(audit_mod.audit(timeout=20.0))
    finally:
        for name, value in (
            ("OPENCHECK_DATA_ROOT", previous_root),
            ("OPENCHECK_ALLOW_LIVE", previous_live),
        ):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()


def _table(rows: list[audit_mod.AuditRow]) -> str:
    return "\n" + audit_mod.render(rows)


# --- the guard ---------------------------------------------------------------


def test_no_adapter_goes_to_the_network_before_recording_provenance(rows) -> None:
    """``build_client()`` records a live observation when the client is
    constructed, and the adapters that build their own client record one
    explicitly *before* the request — ``ariregister`` says so in a comment at
    the call site. A request that leaves an adapter while the recorder is
    still empty is therefore a defect whatever the upstream would have
    answered, which is why this needs no network to decide."""
    offenders = [r for r in rows if r.outcome == audit_mod.UNRECORDED_ROW]
    assert not offenders, (
        "an adapter answered without recording where the answer came from — "
        "its live data resolves as 'stub' and renders as 'Placeholder data' "
        "(PR #153, PR #275):" + _table(offenders)
    )


def test_no_adapter_disagrees_with_the_liveness_its_probe_expects(rows) -> None:
    """The comparison the weekly sweep makes, for the sources that can make it
    with no network: a local store or a committed fixture."""
    offenders = [r for r in rows if r.outcome == audit_mod.DISAGREES]
    assert not offenders, "resolved liveness is not what the probe asserts:" + _table(
        offenders
    )


def test_the_offline_run_still_grades_the_sources_it_used_to(rows) -> None:
    """A source sliding from graded to ungraded is the thing to notice.

    Skips and blocked requests both read as "no news", so coverage can shrink
    without a single test going red — the same shape as a secret quietly
    expiring, which ``MAX_SKIPPED_FOR_CREDENTIALS`` exists to catch on the
    sweep. Pinned by id, so the failure names the source rather than a total.
    """
    graded = {r.source_id for r in rows if r.outcome == audit_mod.AGREES}
    lost = sorted(audit_mod.OFFLINE_COMPARED - graded)
    by_id = {r.source_id: r for r in rows}
    detail = "\n".join(
        f"  {sid}: {by_id[sid].outcome} — {by_id[sid].reason}" for sid in lost if sid in by_id
    )
    assert not lost, (
        f"these sources no longer have their provenance graded offline: {lost}\n"
        f"{detail}\n"
        "If that is deliberate, remove them from OFFLINE_COMPARED in this "
        "commit and say why; if not, the adapter stopped recording what it used to."
    )


def test_the_request_time_rule_is_actually_exercised(rows) -> None:
    """A guard nobody reaches is a guard that passes. The block only proves
    something for adapters that try to go out under it."""
    exercised = [r.source_id for r in rows if r.requests]
    assert len(exercised) >= audit_mod.MIN_REQUEST_TIME_CHECKS, (
        f"only {len(exercised)} adapters issued a request under the block "
        f"({audit_mod.MIN_REQUEST_TIME_CHECKS} expected) — either credentials "
        "or artifacts went missing, or the block stopped intercepting: "
        f"{sorted(exercised)}"
    )


def test_every_source_is_accounted_for(rows) -> None:
    """One row per probe, and every row an outcome we know how to read."""
    assert {r.source_id for r in rows} == set(PROBES)
    known = {
        audit_mod.AGREES,
        audit_mod.DISAGREES,
        audit_mod.UNRECORDED_ROW,
        audit_mod.NOT_COMPARABLE,
        audit_mod.ERRORED,
        audit_mod.SKIPPED,
    }
    assert {r.outcome for r in rows} <= known


# --- proof that the harness can fail -----------------------------------------
#
# A harness that has never been seen to fail is a harness nobody has tested.
# Both defects below are reintroduced deliberately, one per bug as it actually
# happened.


async def test_it_catches_an_adapter_that_stops_recording_a_local_read(monkeypatch) -> None:
    """The ONRC shape: a real answer from a local store, recorded nowhere.

    ``cac_nigeria`` stands in for it because its curated set is committed, so
    the read path runs on any checkout; ONRC's own index is a release asset
    and skips offline.
    """
    from opencheck.sources import cac_nigeria

    monkeypatch.setattr(cac_nigeria.provenance, "record_curated", lambda *a, **k: None)
    row = await audit_mod.audit_source("cac_nigeria", PROBES["cac_nigeria"])

    assert row.outcome == audit_mod.UNRECORDED_ROW, row
    assert "recorded no provenance observation" in row.reason


async def test_it_catches_a_request_that_outruns_its_observation(monkeypatch) -> None:
    """The Ariregister shape: the round-trip happens, the observation doesn't.

    Dropping ``record_live`` is exactly what bypassing ``build_client()`` did,
    and the adapter is otherwise untouched — it still reaches for the network.
    """
    monkeypatch.setattr(provenance, "record_live", lambda *a, **k: None)
    row = await audit_mod.audit_source("ariregister", PROBES["ariregister"])

    assert row.outcome == audit_mod.UNRECORDED_ROW, row
    assert "ariregister.rik.ee" in row.reason


def test_a_bulk_artifact_fetch_is_not_a_lookup(monkeypatch) -> None:
    """The exemption, exercised rather than asserted.

    ClimateTRACE downloads the GEM bulk CSVs before it can answer anything,
    and that request carries no provenance claim — the lookup that later reads
    them records its own. The exemption is the same table the AST guard in
    ``test_source_probes.py`` reads, so an entry cannot be true for one check
    and not the other; here it is proved to work by frame, and proved not to
    cover the identical request made from anywhere else.
    """
    import httpx

    monkeypatch.setitem(
        audit_mod.BULK_ARTIFACT_FETCHERS,
        ("test_provenance_audit.py", "_fetch_the_bulk_artifact"),
        "the test's stand-in for climatetrace's GEM download",
    )
    url = "https://storage.googleapis.com/gem/ownership.zip"

    def _get(client: httpx.Client) -> None:
        with pytest.raises(httpx.ConnectError):
            client.send(httpx.Request("GET", url))

    def _fetch_the_bulk_artifact(client: httpx.Client) -> None:
        _get(client)

    with audit_mod.block_network() as net, provenance.recording():
        client = httpx.Client()
        try:
            _fetch_the_bulk_artifact(client)
            assert net.artifact_fetches == 1
            assert net.unrecorded == []
            # The same request, the same empty recorder, an ordinary frame.
            _get(client)
        finally:
            client.close()

    assert net.requests == 2
    assert net.unrecorded == ["storage.googleapis.com"]


# --- the verdict, which the weekly sweep shares ------------------------------


def _probe(**kwargs) -> SourceProbe:
    return SourceProbe(tier="live", subject="x", args=("1",), **kwargs)


def _observed(liveness: str) -> tuple[Observation, ...]:
    return (Observation(liveness, None, None),)


def test_a_stub_bundle_is_its_own_verdict() -> None:
    """``resolve(is_stub=True)`` short-circuits to stub whatever was recorded,
    so "expected live, resolved stub" describes the bundle, not the recorder —
    and the difference is what the reader needs."""
    verdict = audit_mod.check_provenance(
        _probe(),
        Provenance(),
        observations=_observed("live"),
        is_stub=True,
    )
    assert verdict.outcome == audit_mod.STUB_BUNDLE
    assert "stub bundle" in verdict.reason


def test_recording_nothing_is_not_the_same_as_recording_the_wrong_thing() -> None:
    nothing = audit_mod.check_provenance(
        _probe(), Provenance(), observations=(), is_stub=False
    )
    wrong = audit_mod.check_provenance(
        _probe(),
        Provenance(liveness="snapshot"),
        observations=_observed("snapshot"),
        is_stub=False,
    )
    assert nothing.outcome == audit_mod.UNRECORDED
    assert "no provenance observation at all" in nothing.reason
    assert wrong.outcome == audit_mod.MISMATCH
    assert "expected live, resolved 'snapshot'" in wrong.reason


def test_a_match_is_a_pass() -> None:
    verdict = audit_mod.check_provenance(
        _probe(),
        Provenance(liveness="live"),
        observations=_observed("live"),
        is_stub=False,
    )
    assert verdict.ok


def test_the_pipelines_stub_rule_is_shared_not_repeated() -> None:
    """``routers/lookup.py`` resolves with ``bool(raw.get("is_stub"))``; the
    sweep and the audit have to use the same rule or they grade something the
    reader never sees."""
    assert audit_mod.is_stub_bundle({"is_stub": True}) is True
    assert audit_mod.is_stub_bundle({"is_stub": False}) is False
    assert audit_mod.is_stub_bundle({}) is False
    assert audit_mod.is_stub_bundle([{"is_stub": True}]) is False
