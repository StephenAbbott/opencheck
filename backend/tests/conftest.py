"""Top-level pytest configuration.

Disables ``.env`` file loading for the entire test session. The runtime
``Settings`` class resolves an absolute path to the repo's real
``.env`` so that ``uv run uvicorn`` from ``backend/`` still picks up
``OPENCHECK_ALLOW_LIVE=true`` and the API keys the user has set. That
behaviour is great for the dev server, but tests rely on monkeypatched
env vars and shouldn't have their setup quietly shadowed by whatever
the developer happens to have on disk.

Setting ``OPENCHECK_DISABLE_DOTENV=1`` *before any test imports
opencheck.config* tells the Settings class to skip the env-file
lookup entirely. The flag is checked at class-definition time, so
this conftest must run before any test module imports the package —
pytest collects ``conftest.py`` first by design.
"""

from __future__ import annotations

import os

import pytest

# Set the flag at import time, before any test code runs and before
# opencheck.config is imported. No fixture wrapping needed.
os.environ.setdefault("OPENCHECK_DISABLE_DOTENV", "1")

# Rate limiting is off for the whole suite: tests hammer the same endpoints
# from the same fake client and would otherwise trip the per-IP budgets.
# tests/test_rate_limit.py re-enables the limiter per-fixture (by flipping
# ``limiter.enabled``), which is why endpoint functions called directly in
# tests (history, nz_associations, …) work without a Request object — the
# slowapi wrapper is a pass-through while disabled.
os.environ.setdefault("OPENCHECK_RATE_LIMIT_ENABLED", "0")

# Identifier check-digit enforcement is off for the whole suite: dozens of
# long-standing fixtures use deliberately fake, shape-valid LEIs
# ("2138000000000000A001", "LEI0000000000000ACME", …) that would fail the
# ISO 17442 mod-97 gate. tests/test_identifiers.py re-enables it per-fixture
# (env var + get_settings.cache_clear()) to pin the enforced behaviour.
os.environ.setdefault("OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED", "0")

# The process-wide GLEIF throttle (Phase 143) is off for the whole suite:
# respx-mocked GLEIF calls go through the real transport layer, and a shared
# 50/min budget would make unrelated tests sleep once enough of them have
# fired. tests/test_gleif_throttle.py re-enables it per-fixture (env var +
# get_settings.cache_clear() + reset_throttle_for_tests()).
os.environ.setdefault("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "0")

# GLEIF's ISIN-to-LEI table (Phase 258) is never downloaded or built by the
# suite, and never read from wherever a developer's dev server left one: the
# path points at a file that does not exist unless a test builds it there.
# tests/test_isin_index.py builds small tables per test under tmp_path.
import tempfile as _tempfile  # noqa: E402

os.environ.setdefault("OPENCHECK_ISIN_INDEX_SYNC", "0")
os.environ.setdefault(
    "OPENCHECK_ISIN_INDEX_DB_FILE",
    os.path.join(_tempfile.mkdtemp(prefix="opencheck-test-isin-"), "absent.sqlite"),
)

# Registry-wide entityType.subtype guard (Phase 214). Installed here, at
# conftest import time and after the env flags above, so every test module's
# ``from opencheck.bods.mapper import map_x`` binds the guarded mapper. See
# tests/_entity_subtype_guard.py for why it records instead of raising.
from tests import _entity_subtype_guard  # noqa: E402

_entity_subtype_guard.install()

# The suite is offline by construction (Phase 266): no lifespan warm-up
# downloads, and a socket guard that fails any test reaching past loopback.
# See tests/_network_guard.py. ``--run-live`` / OPENCHECK_RUN_LIVE=1 leaves
# the network alone for the @pytest.mark.live tier.
os.environ.setdefault("OPENCHECK_WARM_CACHES_ON_START", "0")

from tests import _network_guard  # noqa: E402


def _live_run(config) -> bool:
    return bool(config.getoption("--run-live")) or os.environ.get("OPENCHECK_RUN_LIVE") == "1"


def pytest_configure(config):
    if not _live_run(config):
        _network_guard.install()


def pytest_addoption(parser):
    parser.addoption(
        "--run-live",
        action="store_true",
        default=False,
        help="run @pytest.mark.live smoke tests that hit real external APIs (GLEIF, Wikidata).",
    )


def pytest_collection_modifyitems(config, items):
    """Skip @pytest.mark.live tests unless explicitly opted in. Keeps the
    default suite (and CI) fully offline; run live with `pytest --run-live`
    or `OPENCHECK_RUN_LIVE=1`.

    Also moves the mapper-coverage check (Phase 239) to the end of the run: it
    asserts every registered source's mapper went through the contract guard,
    which is only true once every other test has run."""
    last = [i for i in items if i.name == "test_every_registered_source_mapper_was_checked"]
    for item in last:
        items.remove(item)
        items.append(item)
    if _live_run(config):
        return
    skip_live = pytest.mark.skip(
        reason="live API smoke test — run with --run-live or OPENCHECK_RUN_LIVE=1"
    )
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


@pytest.fixture(autouse=True)
def _clear_lookup_replay_cache():
    """The lookup replay cache is keyed by LEI only; tests reuse the same
    demo LEIs with different fixtures, so cached events must never leak
    across tests."""
    from opencheck.routers import lookup as _lookup_mod

    from opencheck import lookup_budget as _budget

    from opencheck import pipelinestats as _pstats

    from opencheck import gleifstats as _gstats
    from opencheck import isin_index as _isin_index

    _lookup_mod._REPLAY_CACHE.clear()
    _lookup_mod._IN_FLIGHT.clear()
    _budget.reset_for_tests()
    _pstats.reset()
    _gstats.reset_for_tests()
    _isin_index.reset_for_tests()
    yield
    _lookup_mod._REPLAY_CACHE.clear()
    _lookup_mod._IN_FLIGHT.clear()
    _budget.reset_for_tests()
    _pstats.reset()
    _gstats.reset_for_tests()
    _isin_index.reset_for_tests()


@pytest.fixture(autouse=True)
def _entity_subtype_guard_check():
    """Fail any test during which a mapper emitted an entity statement whose
    ``entityType.subtype`` is outside the closed BODS v0.4 codelist, or does
    not align with ``entityType.type`` (Phase 214)."""
    _entity_subtype_guard.drain()
    yield
    violations = _entity_subtype_guard.drain()
    if violations:
        lines = "\n".join(f"  {m} → {sid}: {issue}" for m, sid, issue in violations)
        pytest.fail(
            "A mapper broke the BODS v0.4 mapper contract — an invalid "
            "entityType.subtype (local wording belongs in entityType.details), "
            "or a jurisdiction / identifier input the risk engine cannot read "
            "(Phase 239):\n" + lines,
            pytrace=False,
        )


@pytest.fixture(autouse=True)
def _network_guard_check():
    """Fail any test during which something tried to reach the network
    (Phase 266). Code under test catches the refusal — that is its job — so
    the recorded attempt, not the exception, is what fails the test."""
    _network_guard.drain()
    yield
    attempts = _network_guard.drain()
    if attempts:
        pytest.fail(
            "The test suite is offline (tests/_network_guard.py), and this test "
            "tried to reach the network — mock it with respx, or mark it "
            "@pytest.mark.live:\n  " + "\n  ".join(dict.fromkeys(attempts)),
            pytrace=False,
        )
