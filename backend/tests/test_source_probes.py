"""Offline guards for the source-health sweep.

Two failure modes make a health sweep quietly worthless, and neither needs the
network to catch — so both are gated here, on every push and pull request,
rather than in the weekly job:

1. **A source with no probe.** "Green" then means "green for the sources
   someone remembered", which is how a broken adapter sits unnoticed.
2. **An adapter that reaches the network without recording provenance.** That
   is the Ariregister bug (PR #153): correct data, resolved as ``stub``, badged
   "Placeholder data" in the UI. A live sweep only catches it once a week and
   only if that adapter's probe runs; this catches it in review.

The rest of the probe metadata (adapter method names, mapper names, settings
aliases) is checked here too, because a typo in the table would otherwise
surface as a spurious red in the weekly sweep, days later and off the change
that caused it.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from opencheck.config import Settings
from opencheck.sources import REGISTRY
from opencheck.sources.probes import PROBES, SourceProbe

SOURCES_DIR = Path(inspect.getfile(REGISTRY["gleif"].__class__)).parent


# --- 1. every source has a probe --------------------------------------------


def test_every_registry_source_has_a_probe():
    """A new adapter must arrive with a probe, or the sweep silently ignores it."""
    missing = sorted(set(REGISTRY) - set(PROBES))
    extra = sorted(set(PROBES) - set(REGISTRY))
    assert not missing, (
        f"sources with no health probe: {missing} — add an entry to "
        "opencheck/sources/probes.py so the weekly sweep exercises them"
    )
    assert not extra, f"probes for ids that are not in REGISTRY: {extra}"


@pytest.mark.parametrize("source_id", sorted(PROBES))
def test_probe_method_exists_on_the_adapter(source_id: str):
    probe: SourceProbe = PROBES[source_id]
    adapter = REGISTRY[source_id]
    assert hasattr(adapter, probe.method), (
        f"{source_id}: probe calls {probe.method}(), which the adapter does not define"
    )


@pytest.mark.parametrize("source_id", sorted(PROBES))
def test_probe_mapper_name_resolves(source_id: str):
    probe: SourceProbe = PROBES[source_id]
    if probe.bods_mapper is None:
        return
    from opencheck.bods import mapper

    assert hasattr(mapper, probe.bods_mapper), (
        f"{source_id}: bods_mapper {probe.bods_mapper!r} is not in opencheck.bods.mapper"
    )


@pytest.mark.parametrize("source_id", sorted(PROBES))
def test_probe_env_names_are_real_settings(source_id: str):
    """A misspelled credential name would skip the source forever, silently."""
    probe: SourceProbe = PROBES[source_id]
    aliases = {
        f.alias for f in Settings.model_fields.values() if f.alias
    } | set(Settings.model_fields)
    for name in probe.requires_env:
        assert name in aliases, (
            f"{source_id}: requires_env names {name!r}, which is not a Settings alias"
        )


def test_probe_subject_and_args_are_populated():
    for source_id, probe in sorted(PROBES.items()):
        assert probe.subject, f"{source_id}: probe has no subject label"
        assert probe.args, f"{source_id}: probe has no arguments to call {probe.method} with"
        assert probe.expect_liveness, f"{source_id}: probe expects no liveness value"


# --- 2. no network call without a provenance observation ---------------------


def _records_provenance(node: ast.AST) -> bool:
    """Does this subtree record a provenance observation (directly or via the
    shared client factory, which records one on construction)?"""
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name.startswith("record_") or name == "build_client":
            return True
    return False


def _builds_a_raw_client(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
            if child.func.attr in {"AsyncClient", "Client"}:
                return True
    return False


#: Functions that build their own client but are *not* answering a lookup, so
#: they have no provenance observation of their own to record. Keyed by
#: (module, function) with the reason, so every hole in the guard is visible and
#: reviewable rather than implicit. Keep this list short.
_EXEMPT: dict[tuple[str, str], str] = {
    ("climatetrace.py", "_download_gem_csvs_from_gcs"): (
        "downloads the GEM bulk CSVs to disk; the lookup that later reads them "
        "records its own provenance"
    ),
    ("climatetrace.py", "_ensure_gem_data"): "bulk artifact refresh, not a lookup",
    ("climatetrace.py", "_ensure_gleif_gem_data"): "bulk artifact refresh, not a lookup",
}


def test_exemptions_are_all_still_needed():
    """A stale exemption is a hole in the guard. If an exempt function no
    longer builds its own client, the entry must go."""
    stale: list[str] = []
    for (module, func_name), _reason in sorted(_EXEMPT.items()):
        path = SOURCES_DIR / module
        if not path.exists():
            stale.append(f"{module} (module gone)")
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == func_name
            and _builds_a_raw_client(node)
        ]
        if not found:
            stale.append(f"{module}:{func_name}")
    assert not stale, f"stale provenance-guard exemptions, delete them: {stale}"


def _adapter_modules() -> list[Path]:
    return sorted(
        path
        for path in SOURCES_DIR.glob("*.py")
        if path.name not in {"__init__.py", "base.py", "probes.py"}
    )


@pytest.mark.parametrize("path", _adapter_modules(), ids=lambda p: p.stem)
def test_raw_http_client_records_provenance(path: Path):
    """Constructing an httpx client is the moment an adapter commits to the
    network. ``http.build_client()`` records a live observation for you; an
    adapter that builds its own client must record one itself, in the same
    function, or its genuinely live data resolves as ``stub``.

    This is the guard that makes the PR #153 bug class unrepeatable rather
    than merely fixed.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if (path.name, node.name) in _EXEMPT:
            continue
        if _builds_a_raw_client(node) and not _records_provenance(node):
            offenders.append(f"{path.name}:{node.lineno} {node.name}()")
    assert not offenders, (
        "raw httpx client built without recording provenance — live data from "
        "these functions will be badged 'Placeholder data' (see PR #153): "
        + ", ".join(offenders)
    )


# --- 3. adapters can say "the source did not answer" -------------------------


def test_degradation_recorder_collects_and_closes():
    """The scope adapters write into, and the pipeline reads from."""
    from opencheck import degradation

    degradation.record("jar_lithuania", "should be dropped — no scope open")

    degradation.begin()
    degradation.record("jar_lithuania", "the register did not answer (HTTP 403)")
    collected = degradation.collect()

    assert [d.source_id for d in collected] == ["jar_lithuania"]
    assert collected[0].check == degradation.CHECK_SOURCE_FETCH
    assert "403" in collected[0].detail

    # Scope closed: further records are dropped rather than leaking into the
    # next lookup.
    degradation.record("jar_lithuania", "after collect")
    assert degradation.collect() == []


def test_degradation_reasons_mirror_risk():
    """The adapter-side constants duplicate risk's because risk imports
    sources and an adapter cannot import risk at module level. If they ever
    drift, degraded_sources gains a reason the rest of the system cannot
    classify."""
    from opencheck import degradation, risk

    assert degradation.REASON_UPSTREAM_ERROR == risk.DEGRADED_UPSTREAM_ERROR
    assert degradation.REASON_TIMEOUT == risk.DEGRADED_TIMEOUT
    assert degradation.REASON_NOT_CONFIGURED == risk.DEGRADED_NOT_CONFIGURED
    assert degradation.REASON_RATE_LIMITED == risk.DEGRADED_RATE_LIMITED
    assert degradation.reason_for_failure("HTTP 429") == risk.DEGRADED_RATE_LIMITED
    assert degradation.reason_for_failure("HTTP 403") == risk.DEGRADED_UPSTREAM_ERROR
    assert degradation.reason_for_failure("ConnectTimeout") == risk.DEGRADED_TIMEOUT


async def test_jar_lithuania_marks_an_unanswered_register(monkeypatch, tmp_path):
    """A register that refuses us must not look like one that answered.

    This is the shape that made Lithuania read as healthy: a bundle carrying
    only the GLEIF legal name, with `is_stub: False` and every register field
    null, is indistinguishable from a successful lookup of a company the
    register says little about.
    """
    from opencheck import degradation
    from opencheck.config import get_settings
    from opencheck.sources.jar_lithuania import JarLithuaniaAdapter

    # A clean data root: a cached bundle from an earlier real fetch would
    # short-circuit the path under test and quietly pass.
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    adapter = JarLithuaniaAdapter()

    async def _refuse(self, url, *, cache_key):
        self._last_failure = "HTTP 403"
        return None

    monkeypatch.setattr(JarLithuaniaAdapter, "_fetch_html", _refuse, raising=True)

    degradation.begin()
    bundle = await adapter.fetch("301844044", legal_name="AB Ignitis grupė")
    recorded = degradation.collect()

    assert bundle["register_unavailable"] is True
    assert bundle["register_unavailable_detail"] == "HTTP 403"
    assert bundle["status"] is None
    assert [d.source_id for d in recorded] == ["jar_lithuania"]
    assert "403" in recorded[0].detail
    # Privacy: the detail names the source and the failure, never the subject.
    assert "Ignitis" not in recorded[0].detail
    get_settings.cache_clear()
