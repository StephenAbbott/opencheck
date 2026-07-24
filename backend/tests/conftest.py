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
    or `OPENCHECK_RUN_LIVE=1`."""
    if config.getoption("--run-live") or os.environ.get("OPENCHECK_RUN_LIVE") == "1":
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

    _lookup_mod._REPLAY_CACHE.clear()
    yield
    _lookup_mod._REPLAY_CACHE.clear()
