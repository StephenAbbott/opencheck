"""What the weekly sweep says about provenance, and about what it never ran.

Two changes, one ticket (Phase 229). The sweep resolved provenance without
``is_stub`` while the pipeline resolves with it, so an adapter that recorded a
live observation and then handed back a stub bundle read "live" here and
"Placeholder data" to the reader — the sweep being the last place that would
notice. And a probe skipped for want of a bulk store left its
``expect_liveness`` assertion evaluated by nobody, reported as a dash in a
table of dashes.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

# See test_source_health_drift.py: the script enables live mode at import and
# a test process must not inherit that.
_had_allow_live = "OPENCHECK_ALLOW_LIVE" in os.environ
import source_health as sweep  # noqa: E402

if not _had_allow_live:
    os.environ.pop("OPENCHECK_ALLOW_LIVE", None)

from opencheck import provenance  # noqa: E402
from opencheck.sources.probes import SKIP_ARTIFACT, SKIP_CREDENTIAL, SourceProbe  # noqa: E402
from opencheck.sources.provenance_audit import STUB_BUNDLE, UNRECORDED  # noqa: E402


class _Adapter:
    """An adapter whose answer and provenance the test dictates."""

    def __init__(self, result: Any, *, records: str | None = "live") -> None:
        self._result = result
        self._records = records

    async def fetch(self, *args: Any, **kwargs: Any) -> Any:
        if self._records == "live":
            provenance.record_live("test")
        elif self._records == "snapshot":
            provenance.record_snapshot(None, "test")
        return self._result


def _probe(**kwargs: Any) -> SourceProbe:
    return SourceProbe(tier="live", subject="a subject", args=("1",), **kwargs)


@pytest.fixture
def registry(monkeypatch):
    """Put one adapter in the registry under a scratch id."""

    def _install(adapter: _Adapter) -> str:
        monkeypatch.setitem(sweep.REGISTRY, "_probe_subject", adapter)
        return "_probe_subject"

    return _install


async def test_a_stub_bundle_fails_even_when_a_live_fetch_was_recorded(registry) -> None:
    """The divergence this closes. ``build_client()`` records live on
    construction, so an adapter that goes out, gets a 404 and returns its stub
    has a recorder full of ``live`` — and production still renders the card as
    'Placeholder data', because ``resolve(is_stub=True)`` short-circuits."""
    source_id = registry(_Adapter({"source_id": "x", "is_stub": True}, records="live"))

    result = await sweep._run_probe(source_id, _probe(), timeout=5)

    assert result.status == sweep.FAIL
    assert result.provenance_defect == STUB_BUNDLE
    assert "stub bundle" in result.reason
    assert result.liveness == "stub"


async def test_recording_nothing_reads_differently_from_recording_the_wrong_thing(
    registry,
) -> None:
    """The ONRC row: a real answer from a local store, recorded nowhere. The
    older message — "expected live, resolved 'stub'" — was true and pointed at
    the wrong half of the system."""
    source_id = registry(_Adapter({"company": {"name": "x"}}, records=None))

    result = await sweep._run_probe(source_id, _probe(), timeout=5)

    assert result.status == sweep.FAIL
    assert result.provenance_defect == UNRECORDED
    assert "recorded no provenance observation" in result.reason


async def test_a_real_answer_with_the_expected_liveness_still_passes(registry) -> None:
    source_id = registry(_Adapter({"company": {"name": "x"}}, records="live"))

    result = await sweep._run_probe(source_id, _probe(), timeout=5)

    assert result.status == sweep.OK
    assert result.liveness == "live"
    assert result.provenance_defect == ""


async def test_the_expected_liveness_rides_on_every_row(registry) -> None:
    """So a row can name the assertion it was making, passed or skipped."""
    source_id = registry(_Adapter({"company": {"name": "x"}}, records="snapshot"))

    result = await sweep._run_probe(
        source_id, _probe(expect_liveness=frozenset({"snapshot"})), timeout=5
    )

    assert result.expect_liveness == ["snapshot"]


# --- what was never exercised ------------------------------------------------


def _skipped(source_id: str, kind: str, reason: str, expect: list[str]) -> sweep.Result:
    return sweep.Result(
        source_id,
        "index",
        sweep.SKIPPED,
        reason=reason,
        skipped_for=kind,
        expect_liveness=expect,
    )


def test_an_absent_artifact_is_reported_apart_from_an_absent_credential() -> None:
    """Both mean untested and each is fixed somewhere else — a repository
    secret, or the warm-up step. One list would say neither."""
    results = [
        _skipped("meip", SKIP_ARTIFACT, "required local artifact absent: meip.sqlite", ["snapshot"]),
        _skipped("mca_india", SKIP_CREDENTIAL, "not configured: DATA_GOV_IN_API_KEY", ["live"]),
    ]

    report = sweep.build_report(results)

    assert report["credential_skips"] == ["mca_india"]
    assert [row["source_id"] for row in report["artifact_skips"]] == ["meip"]
    assert report["artifact_skips"][0]["unevaluated"] == ["snapshot"]


def test_the_report_names_the_assertion_a_skip_left_unevaluated() -> None:
    """``onrc_romania`` and ``meip`` both skipped every week carrying
    ``expect_liveness={"snapshot"}`` — the assertion the sweep exists to make
    — and the report said only "not tested"."""
    report = sweep.build_report(
        [_skipped("onrc_romania", SKIP_ARTIFACT, "required local artifact absent: onrc_romania.sqlite", ["snapshot"])]
    )

    markdown = sweep.render_markdown(report)

    assert "Not exercised for want of a local artifact (1)" in markdown
    assert "`expect_liveness=snapshot` not evaluated" in markdown


def test_an_artifact_skip_does_not_count_against_the_credential_budget() -> None:
    """``MAX_SKIPPED_FOR_CREDENTIALS`` guards a secret quietly expiring. An
    absent bulk store is a different failure with a different fix, and
    charging it to that budget would red the run for the wrong reason."""
    results = [
        _skipped("meip", SKIP_ARTIFACT, "required local artifact absent: meip.sqlite", ["snapshot"]),
        _skipped("onrc_romania", SKIP_ARTIFACT, "required local artifact absent: onrc_romania.sqlite", ["snapshot"]),
        _skipped("climatetrace", SKIP_ARTIFACT, "required local artifact absent: gem/ownership.zip", ["live"]),
    ]

    report = sweep.build_report(results)

    assert report["credential_skips"] == []


def test_the_warm_up_script_writes_where_requires_files_looks() -> None:
    """One path, two mechanisms. The warm-up calls each module's own
    ``db_path()`` and ``requires_files`` resolves against ``data_root()`` — if
    those ever disagree the download succeeds and the probe skips anyway,
    which reads as "the asset is missing" and is not."""
    import importlib

    warm = importlib.import_module("warm_bulk_stores")
    from opencheck.cache import data_root
    from opencheck.sources.probes import PROBES

    root = data_root()
    declared = {
        "onrc_romania": PROBES["onrc_romania"].requires_files,
        "meip": PROBES["meip"].requires_files,
    }
    for name, _warm_fn, path_of in warm.WARMERS:
        assert declared[name], f"{name}: the probe declares no required file"
        assert path_of() == root / declared[name][0], (
            f"{name}: the warm-up writes to {path_of()} and the probe looks for "
            f"{root / declared[name][0]}"
        )
